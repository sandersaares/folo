use crate::{
    linked::link_ref,
    mem::{PooledArrayLease, SharedArrayPool},
    metrics::{Event, EventBuilder},
};
use core::slice;
use std::{
    fmt,
    mem::{self},
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
/// This is a thread-safe type - these buffers can move between threads and I/O performed
/// on these buffers may occur on any thread, including multiple threads at different points in a
/// buffer's lifetime.
///
/// For a single-threaded variant, see `PinnedBuffer`.
#[derive(Debug)]
pub struct PinnedBufferShared {
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
        inner: PooledArrayLease<POOL_BUFFER_CAPACITY_BYTES>,
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
            Self::Pooled { inner, .. } => f.debug_struct("Pooled").field("inner", inner).finish(),
            Self::BoxedSlice { .. } => f.debug_struct("BoxedSlice").finish(),
            Self::Ptr { inner, capacity } => f
                .debug_struct("Ptr")
                .field("inner", &format_args!("{:p}", inner))
                .field("capacity", capacity)
                .finish(),
        }
    }
}

unsafe impl Send for PinnedBufferShared {}

impl PinnedBufferShared {
    /// Obtains a new buffer from a global array pool.
    pub fn from_pool() -> Self {
        let lease = POOL.with(|pool| pool.get());

        PinnedBufferShared {
            mode: Mode::Pooled { inner: lease },
            len: POOL_BUFFER_CAPACITY_BYTES,
            start: 0,
        }
    }

    /// Creates a new buffer from a slice of bytes provided by the caller. Once the buffer has been
    /// used up, the caller may get the inner slice back via `.into_inner_boxed_slice()`.
    pub fn from_boxed_slice(slice: Box<[u8]>) -> Self {
        CALLER_BUFFERS_REFERENCED.with(Event::observe_unit);

        let len = slice.len();

        PinnedBufferShared {
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

        PinnedBufferShared {
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
            Mode::Pooled { .. } => POOL_BUFFER_CAPACITY_BYTES,
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
            Mode::Pooled { inner, .. } => {
                // SAFETY: Yeah it is always pinned, we just need to remove it for the caller.
                // TODO: Why are we removing Pin here, exactly?
                let unpinned = unsafe { Pin::into_inner_unchecked(inner.to_mut_slice()) };
                &mut unpinned[self.start..(self.start + self.len)]
            }
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

    /// Obtains an immutable view over the contents of the buffer.
    pub fn as_slice(&self) -> &[u8] {
        match &self.mode {
            Mode::Pooled { inner, .. } => {
                // SAFETY: Yeah it is always pinned, we just need to remove it for the caller.
                // TODO: Why are we removing Pin here, exactly?
                let unpinned = unsafe { Pin::into_inner_unchecked(inner.to_slice()) };
                &unpinned[self.start..(self.start + self.len)]
            }
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

// 64 KB is the default "stream to stream" copy size in .NET, so we use that as a default buffer
// size, as well. Note that this is not necessarily the best for high throughput single-stream I/O
// and larger buffers will often provide better throughput for a single high throughput stream.
const POOL_BUFFER_CAPACITY_BYTES: usize = 64 * 1024;

link_ref!(static POOL: SharedArrayPool<POOL_BUFFER_CAPACITY_BYTES> = SharedArrayPool::new());

thread_local! {
    static CALLER_BUFFERS_REFERENCED: Event = EventBuilder::new("shared_caller_buffers_referenced")
        .build();

    static CALLER_POINTERS_REFERENCED: Event = EventBuilder::new("shared_caller_pointers_referenced")
        .build();

    static POOL_RENTED: Event = EventBuilder::new("shared_pool_buffers_rented")
        .build();
}
