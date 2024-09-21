use crate::{
    mem::{DropPolicy, PinnedSlabChain},
    metrics::{Event, EventBuilder},
};
use core::slice;
use negative_impl::negative_impl;
use std::{
    cell::{RefCell, UnsafeCell},
    fmt,
    mem::{self, MaybeUninit},
    ops::Range,
    pin::Pin,
    ptr,
};

/// A buffer of bytes for reading from or writing to as part of low level I/O operations. This is
/// typically not visible to user code, rather it is used as the primitive inside the Folo I/O API.
///
/// The buffer has an active region that is used for I/O operations (by default, the entire buffer).
/// You can adjust the start/len fields as appropriate to adjust the active region (e.g. to fill
/// or consume the buffer in multiple pieces).
///
/// This is a single threaded type - these buffers cannot move between threads, all I/O performed
/// on these buffers stays within the same thread from start to finish.
///
/// For a thread-safe variant, see `PinnedBufferShared`.
#[derive(Debug)]
pub struct PinnedBuffer {
    mode: Mode,

    // We might not always use the full capacity (though we do by default).
    //
    // This is the length of the "active" slice of the buffer, the one we are using for I/O.
    len: usize,

    // We might not always use the full capacity (though we do by default).
    //
    // This is the start offset at which we begin reading or writing. The region of the buffer
    // before this point is treated as not part of the buffer for the purposes of I/O but you
    // may access it later by resetting the start offset (e.g. when the buffer has been filled by
    // multiple I/O operations).
    start: usize,
}

enum Mode {
    Pooled {
        // This is the real storage of the bytes and determines the capacity.
        //
        // We use 'static as the lifetime because in practice this is backed by storage that will
        // life as long as the buffer lives, despite being a reference.
        inner: Pin<&'static mut [u8]>,

        index_in_pool: usize,
    },
    BoxedSlice {
        // We allow the caller to retrieve the inner value from the buffer via
        // `.into_inner_boxed_slice()` if they wish to reuse the storage later.
        inner: Pin<Box<[u8]>>,
    },
    Ptr {
        inner: *mut u8,
        capacity: usize,
    },
}

impl fmt::Debug for Mode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pooled { index_in_pool, .. } => f
                .debug_struct("Pooled")
                .field("index_in_pool", index_in_pool)
                .finish(),
            Self::BoxedSlice { .. } => f.debug_struct("BoxedSlice").finish(),
            Self::Ptr { inner, capacity } => f
                .debug_struct("Ptr")
                .field("inner", &format_args!("{:p}", inner))
                .field("capacity", capacity)
                .finish(),
        }
    }
}

impl PinnedBuffer {
    /// Obtains a new buffer from the current thread's buffer pool.
    pub fn from_pool() -> Self {
        POOL.with(|pool| {
            let mut pool = pool.borrow_mut();
            let inserter = pool.begin_insert();
            let index = inserter.index();

            // We do not initialize the buffer when we take it from the pool. It has whatever data
            // it had at the start (maybe zeroes, maybe old I/O operation data).
            let storage = inserter.insert_uninit();

            // SAFETY: UnsafeCell<T> is layout-compatible with T in most cases (and definitely in
            // this case), so we can convert that freely. Likewise, MaybeUninit<T> is layout-
            // compatible with T. Finally, we do not care what bit patterns our buffers are
            // initialized with because they will be overwritten by new data anyway as part of some
            // I/O operation. We assume we do not need to worry about dirty contents being somehow
            // dangerous/sensitive here (perhaps might want to consider zeroing per-usecase but that
            // would be the responsibility of whoever put sensitive data in there, before dropping).
            let storage = unsafe { (*storage).assume_init_mut() };

            POOL_ALLOCATED.with(Event::observe_unit);

            // SAFETY: The chain guarantees pinning, we just re-wrap Pin around the inner bytes.
            // We only ever hand out references derived from UnsafeCell, which are always valid
            // to hand out as long as we do not create multiple `&mut` references (which we do not
            // as Buffer holds the only reference and protects it via standard borrow mechanics).
            let inner = unsafe {
                Pin::new_unchecked(slice::from_raw_parts_mut(
                    storage.get() as *mut u8,
                    POOL_BUFFER_CAPACITY_BYTES,
                ))
            };

            let len = inner.len();

            PinnedBuffer {
                mode: Mode::Pooled {
                    inner,
                    index_in_pool: index,
                },
                len,
                start: 0,
            }
        })
    }

    /// Creates a new buffer from a slice of bytes provided by the caller. Once the buffer has been
    /// used up, the caller may get the inner slice back via `.into_inner_boxed_slice()`.
    pub fn from_boxed_slice(slice: Box<[u8]>) -> Self {
        CALLER_BUFFERS_REFERENCED.with(Event::observe_unit);

        let len = slice.len();

        PinnedBuffer {
            mode: Mode::BoxedSlice {
                inner: Pin::new(slice),
            },
            len,
            start: 0,
        }
    }

    /// Creates a new buffer from a pinned pointer with a specified capacity.
    ///
    /// # Safety
    ///
    /// The caller is responsible for ensuring that the provided pointer remains valid for the
    /// entire lifetime of the PinnedBuffer (including any I/O operations started that reference
    /// the PinnedBuffer, including after the operation is canceled, up to the moment the completion
    /// or cancellation notification is received from the operating system).
    ///
    /// The caller is responsible for ensuring that the pointer is actually to pinned memory.
    pub unsafe fn from_ptr(ptr: *mut u8, capacity: usize) -> Self {
        CALLER_POINTERS_REFERENCED.with(Event::observe_unit);

        PinnedBuffer {
            mode: Mode::Ptr {
                inner: ptr,
                capacity,
            },
            len: capacity,
            start: 0,
        }
    }

    pub fn capacity(&self) -> usize {
        match &self.mode {
            Mode::Pooled { inner, .. } => inner.len(),
            Mode::BoxedSlice { inner } => inner.len(),
            Mode::Ptr { capacity, .. } => *capacity,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn set_len(&mut self, value: usize) {
        assert!(self.start + value <= self.capacity());

        self.len = value;
    }

    pub fn start(&self) -> usize {
        self.start
    }

    pub fn set_start(&mut self, value: usize) {
        assert!(value + self.len <= self.capacity());

        self.start = value;
    }

    /// Sets the length and start offset to cover the region of the buffer that is not yet used.
    /// If the current active region extends to the end of the buffer, the result will be a zero-
    /// sized buffer.
    pub fn use_remainder(mut self) -> Self {
        self.start += self.len;
        self.len = self.capacity() - self.start;
        self
    }

    /// Marks the buffer as used up to the end of the currently active area (extending it
    /// maximally toward the start).
    pub fn use_all_until_current(mut self) -> Self {
        self.len += self.start;
        self.start = 0;
        self
    }

    /// Marks the entire buffer as the active area.
    pub fn use_all(mut self) -> Self {
        self.start = 0;
        self.len = self.capacity();
        self
    }

    /// Obtains a mutable view over the contents of the buffer.
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        match &mut self.mode {
            Mode::Pooled { inner, .. } => &mut inner[self.start..(self.start + self.len)],
            Mode::BoxedSlice { inner } => &mut inner[self.start..(self.start + self.len)],
            Mode::Ptr { inner, .. } => unsafe {
                slice::from_raw_parts_mut(inner.add(self.start), self.len)
            },
        }
    }

    /// Sets the length and obtains a mutable view over the contents of the buffer.
    /// Shorthand to easily fill the buffer and set the length in one go for write operations.
    pub fn as_mut_slice_with_len(&mut self, length: usize) -> &mut [u8] {
        self.set_len(length);

        self.as_mut_slice()
    }

    /// Obtains multiple mutable fixed-length views over the contents of the buffer's active region
    /// and constrains the length of the active region to match the total of the requested slices.
    pub fn as_mut_slices<const COUNT: usize>(&mut self, lengths: &[usize]) -> [&mut [u8]; COUNT] {
        assert_eq!(lengths.len(), COUNT);

        // Convert it to a ptr to disconnect from "self". While we leave the returned lifetimes
        // connected to the lifetime of "self", we do not keep "self" itself borrowed in this scope.
        let mut remainder = self.as_mut_slice().as_mut_ptr();

        let remainder_len = lengths.iter().sum::<usize>();
        assert!(remainder_len <= self.len);
        self.len = remainder_len;

        let mut slices = [const { MaybeUninit::<&mut [u8]>::uninit() }; COUNT];

        for (i, &len) in lengths.iter().enumerate() {
            // SAFETY: We are splitting into non-overlapping slices and hooking them up with the
            // lifetime of "self". All is well.
            let slice = unsafe { slice::from_raw_parts_mut(remainder, len) };
            slices[i].write(slice);

            // SAFETY: We validated above that all slices fit into our total buffer active region.
            remainder = unsafe { remainder.add(len) };
        }

        // SAFETY: Everything is initialized and MaybeUninit is layout-compatible, so this is safe.
        unsafe {
            mem::transmute_copy::<[MaybeUninit<&mut [u8]>; COUNT], [&mut [u8]; COUNT]>(&slices)
        }
    }

    /// Obtains an immutable view over the contents of the buffer.
    pub fn as_slice(&self) -> &[u8] {
        match &self.mode {
            Mode::Pooled { inner, .. } => &inner[self.start..(self.start + self.len)],
            Mode::BoxedSlice { inner } => &inner[self.start..(self.start + self.len)],
            Mode::Ptr { inner, .. } => unsafe {
                slice::from_raw_parts(inner.add(self.start), self.len)
            },
        }
    }

    pub fn active_region(&self) -> Range<usize> {
        Range {
            start: self.start,
            end: self.start + self.len,
        }
    }

    /// Consumes the buffer and returns the inner boxed slice that was used to create the object.
    /// Note that the inner boxed slice will be returned in its full extent, ignoring active region.
    ///
    /// # Panics
    ///
    /// Panics if the buffer was not created from a caller-provided boxed slice.
    pub fn into_inner_boxed_slice(self) -> Box<[u8]> {
        assert!(matches!(self.mode, Mode::BoxedSlice { .. }));

        // We are destroying the buffer without going through the usual drop logic.
        // SAFETY: We are forgetting self, so nobody should mind that we stole its contents.
        let mode = unsafe { ptr::read(&self.mode) };
        mem::forget(self);

        match mode {
            Mode::Pooled { .. } | Mode::Ptr { .. } => {
                unreachable!("we already asserted that this is a boxed slice")
            }
            Mode::BoxedSlice { inner } => Pin::into_inner(inner),
        }
    }
}

impl Drop for PinnedBuffer {
    fn drop(&mut self) {
        if let Mode::Pooled { index_in_pool, .. } = self.mode {
            POOL.with(|pool| {
                let mut pool = pool.borrow_mut();
                pool.remove(index_in_pool);
                POOL_DROPPED.with(Event::observe_unit);
            });
        }
    }
}

#[negative_impl]
impl !Send for PinnedBuffer {}
#[negative_impl]
impl !Sync for PinnedBuffer {}

// 64 KB is the default "stream to stream" copy size in .NET, so we use that as a default buffer
// size, as well. Note that this is not necessarily the best for high throughput single-stream I/O
// and larger buffers will often provide better throughput for a single high throughput stream.
const POOL_BUFFER_CAPACITY_BYTES: usize = 64 * 1024;

thread_local! {
    // This is the simplest possible buffer pool implementation - a collection of fixed-size buffers
    // on every thread. Could we be better and more efficient? Sure, but this is fine for now.
    //
    // We use MustNotDropItems policy because buffers are often referenced via raw pointers, so if
    // some items still exist in the collection, we have a high probability of dangling pointers,
    // which can be a big safety problem.
    static POOL: RefCell<PinnedSlabChain<UnsafeCell<[u8; POOL_BUFFER_CAPACITY_BYTES]>>> =
        RefCell::new(PinnedSlabChain::new(DropPolicy::MustNotDropItems));

    static CALLER_BUFFERS_REFERENCED: Event = EventBuilder::new("isolated_caller_buffers_referenced")
        .build();

    static CALLER_POINTERS_REFERENCED: Event = EventBuilder::new("isolated_caller_pointers_referenced")
        .build();

    static POOL_ALLOCATED: Event = EventBuilder::new("isolated_pool_buffers_allocated")
        .build();

    static POOL_DROPPED: Event = EventBuilder::new("isolated_pool_buffers_dropped")
        .build();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_sliced_up() {
        let mut buffer = PinnedBuffer::from_boxed_slice([0_u8; 10].into());

        let [slice1, slice2, slice3] = buffer.as_mut_slices(&[2, 3, 5]);
        assert_eq!(slice1.len(), 2);
        assert_eq!(slice2.len(), 3);
        assert_eq!(slice3.len(), 5);

        slice1.fill(2);
        slice2.fill(3);
        slice3.fill(5);

        let original = buffer.into_inner_boxed_slice();

        assert_eq!(original, [2, 2, 3, 3, 3, 5, 5, 5, 5, 5].into());
    }
}
