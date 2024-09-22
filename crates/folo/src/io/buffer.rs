use crate::linked::link_ref;
use crate::mem::isolation::{markers, Isolated, Shared};
use crate::mem::{DropPolicy, PinnedSlabChain, PooledArrayLease, SharedArrayPool};
use std::cell::{RefCell, UnsafeCell};
use std::mem::MaybeUninit;
use std::ops::Range;
use std::{fmt, mem, ptr, slice};
use std::{marker::PhantomData, pin::Pin, ptr::NonNull};

/// A buffer suitable for I/O operations, with the contents permanently pinned. You can obtain
/// pooled instances or provide your own. I/O operations typically return the provided buffer when
/// they complete, allowing simple reuse of buffers based on caller's determination. Pooled buffers
/// are automatically reused, of course.
///
/// Depending on where/how the bytes are stored, buffers may either be thread-isolated (only usable
/// within a single thread) or shared (usable across threads). The default is thread-isolated. To
/// obtain a buffer from a shared pool, use `Buffer::<Shared>::from_pool()`.
///
/// Some backing storages may be compatible with both isolated and shared isolation modes. For
/// example, if you are creating the buffer from a caller-provided boxed slice, this can be used
/// in either shared or thread-isolated modes. The generic parameter on the buffer is a promise that
/// it can be used in the declared mode, not that it is exclusively for use in this mode.
#[derive(Debug)]
pub struct Buffer<IsolationMode = Isolated>
where
    IsolationMode: markers::IsolationMode,
{
    // Provides references to the actual bytes in the buffer.
    storage: Storage,

    // We might not always use the full buffer capacity (though we do by default).
    // This is the length of the "active" range of the buffer, the one we are using for I/O.
    len: usize,

    // We might not always use the full buffer capacity (though we do by default).
    // This is the start offset of the active range of the buffer.
    start: usize,

    _isolation_mode: PhantomData<IsolationMode>,
}

impl<IsolationMode> Buffer<IsolationMode>
where
    IsolationMode: markers::IsolationMode,
{
    /// Creates a new buffer from a slice of bytes provided by the caller. Once the buffer has been
    /// used up, the caller may get the inner slice back via `.try_into_inner_boxed_slice()`.
    pub fn from_boxed_slice(slice: Box<[u8]>) -> Self {
        let len = slice.len();

        Buffer {
            storage: Storage::BoxedSlice {
                inner: Pin::new(slice),
            },
            len,
            start: 0,
            _isolation_mode: PhantomData,
        }
    }

    /// Consumes the buffer and returns the inner boxed slice that was used to create the buffer.
    /// Note that the inner boxed slice will be returned in its full extent, ignoring active region.
    ///
    /// Returns `None` if the buffer was not created from a caller-provided boxed slice.
    pub fn try_into_inner_boxed_slice(self) -> Option<Box<[u8]>> {
        // We are destroying the buffer without going through the usual drop logic.
        // SAFETY: We are forgetting self, so nobody should mind that we stole its contents.
        let storage = unsafe { ptr::read(&self.storage) };
        mem::forget(self);

        if let Storage::BoxedSlice { inner } = storage {
            Some(Pin::into_inner(inner))
        } else {
            None
        }
    }

    pub fn capacity(&self) -> usize {
        match &self.storage {
            Storage::IsolatedPool { .. } | Storage::SharedPool { .. } => {
                POOLED_BUFFER_CAPACITY_BYTES
            }
            Storage::BoxedSlice { inner } => inner.len(),
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// # Panics
    ///
    /// If the active range of the buffer would exceed the buffer's capacity.
    pub fn set_len(&mut self, new_len: usize) {
        assert!(self.start + new_len <= self.capacity());

        self.len = new_len;
    }

    pub fn start(&self) -> usize {
        self.start
    }

    /// # Panics
    ///
    /// If the active range of the buffer would exceed the buffer's capacity.
    pub fn set_start(&mut self, new_start: usize) {
        assert!(new_start + self.len <= self.capacity());

        self.start = new_start;
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
    pub fn as_mut_slice(&mut self) -> Pin<&mut [u8]> {
        let active_range = self.active_range();

        let slice: &mut [u8] = match &mut self.storage {
            // SAFETY: The type guarantees that the active region never goes out of bounds.
            Storage::IsolatedPool { inner, .. } => unsafe { &mut inner.as_mut()[active_range] },
            // SAFETY: The type guarantees that the active region never goes out of bounds.
            Storage::SharedPool { inner, .. } => unsafe { &mut inner.as_mut()[active_range] },
            Storage::BoxedSlice { inner } => &mut inner[active_range],
        };

        // SAFETY: The storage is always pinned, we are simply restating that promise here.
        unsafe { Pin::new_unchecked(slice) }
    }

    /// Obtains an immutable view over the contents of the buffer.
    pub fn as_slice(&self) -> Pin<&[u8]> {
        let active_range = self.active_range();

        let slice: &[u8] = match &self.storage {
            // SAFETY: The type guarantees that the active region never goes out of bounds.
            Storage::IsolatedPool { inner, .. } => unsafe { &inner.as_ref()[active_range] },
            // SAFETY: The type guarantees that the active region never goes out of bounds.
            Storage::SharedPool { inner, .. } => unsafe { &inner.as_ref()[active_range] },
            Storage::BoxedSlice { inner } => &inner[active_range],
        };

        // SAFETY: The storage is always pinned, we are simply restating that promise here.
        unsafe { Pin::new_unchecked(slice) }
    }

    pub fn active_range(&self) -> Range<usize> {
        Range {
            start: self.start,
            end: self.start + self.len,
        }
    }

    /// Obtains multiple mutable fixed-length views over the contents of the buffer's active range
    /// and constrains the length of the active range to match the total of the requested slices.
    pub fn as_mut_slices<const COUNT: usize>(
        &mut self,
        lengths: &[usize],
    ) -> [Pin<&mut [u8]>; COUNT] {
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
        // We are also re-stating the pinning guarantee the type always provides here.
        unsafe {
            mem::transmute_copy::<[MaybeUninit<&mut [u8]>; COUNT], [Pin<&mut [u8]>; COUNT]>(&slices)
        }
    }
}

impl Buffer<Isolated> {
    /// Obtains a new thread-isolated buffer from the current thread's buffer pool.
    pub fn from_pool() -> Self {
        THREAD_ISOLATED_POOL.with(|pool| {
            let mut pool = pool.borrow_mut();
            let inserter = pool.begin_insert();
            let index = inserter.index();

            // We do not initialize the buffer when we take it from the pool. It has whatever data
            // it had at the start (maybe zeroes, maybe old I/O operation data). If someone wants
            // to ensure that data is not reused, they need to clear it before returning the buffer.
            let storage = inserter.insert_uninit();

            // SAFETY: UnsafeCell<T> is layout-compatible with T in most cases (and definitely in
            // this case), so we can convert that freely. Likewise, MaybeUninit<T> is layout-
            // compatible with T. Finally, we do not care what bit patterns our buffers are
            // initialized with because they will be overwritten by new data anyway as part of some
            // I/O operation. At no point is there any violation of borrowing rules, so all is well.
            let storage = unsafe { (*storage).assume_init_mut() };

            // SAFETY: Obviously it is not going to be null, as we just got it from our storage.
            let storage_ptr = unsafe { NonNull::new_unchecked(storage.get() as *mut [u8]) };

            Buffer {
                storage: Storage::IsolatedPool {
                    inner: storage_ptr,
                    index_in_pool: index,
                },
                len: POOLED_BUFFER_CAPACITY_BYTES,
                start: 0,
                _isolation_mode: PhantomData,
            }
        })
    }
}

impl Buffer<Shared> {
    /// Obtains a new thread-safe buffer from the shared buffer pool.
    pub fn from_pool() -> Self {
        let mut lease = SHARED_POOL.with(|pool| pool.get());

        // SAFETY: This pointer is the only way to access the buffer, with the borrow checker making
        // sure no aliasing rules are violated by callers. Obviously, it is not going to be null as
        // we just got it from the pool.
        let slice_ptr = unsafe {
            NonNull::new_unchecked(slice::from_raw_parts_mut(
                lease.as_mut_ptr(),
                POOLED_BUFFER_CAPACITY_BYTES,
            ))
        };

        Buffer {
            storage: Storage::SharedPool {
                inner: slice_ptr,
                lease,
            },
            len: POOLED_BUFFER_CAPACITY_BYTES,
            start: 0,
            _isolation_mode: PhantomData,
        }
    }
}

impl<I> Drop for Buffer<I>
where
    I: markers::IsolationMode,
{
    fn drop(&mut self) {
        if let Storage::IsolatedPool { index_in_pool, .. } = self.storage {
            THREAD_ISOLATED_POOL.with(|pool| {
                let mut pool = pool.borrow_mut();
                pool.remove(index_in_pool);
            });
        }
    }
}

// SAFETY: We pinky promise that if the Shared marker is present, only thread-safe buffer storage
// will be used. That's just part of the service received when you shop at Folo.
unsafe impl Send for Buffer<Shared> {}

enum Storage {
    IsolatedPool {
        inner: NonNull<[u8]>,
        index_in_pool: usize,
    },
    SharedPool {
        inner: NonNull<[u8]>,
        lease: PooledArrayLease<POOLED_BUFFER_CAPACITY_BYTES>,
    },
    BoxedSlice {
        // We allow the caller to retrieve the inner value from the buffer via
        // `.into_inner_boxed_slice()` if they wish to reuse the storage later.
        inner: Pin<Box<[u8]>>,
    },
}

impl fmt::Debug for Storage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IsolatedPool {
                inner,
                index_in_pool,
            } => f
                .debug_struct("IsolatedPool")
                .field("inner", inner)
                .field("index_in_pool", index_in_pool)
                .finish(),
            Self::SharedPool { inner, lease } => f
                .debug_struct("SharedPool")
                .field("inner", inner)
                .field("lease", lease)
                .finish(),
            Self::BoxedSlice { inner } => {
                f.debug_struct("BoxedSlice").field("inner", inner).finish()
            }
        }
    }
}

/// Pooled buffers are always of a constant size defined here. In principle, we should one day
/// allow callers to request a specific minimum size (and fall back to manual alloc for unrealistic
/// sizes).
const POOLED_BUFFER_CAPACITY_BYTES: usize = 64 * 1024;

link_ref!(static SHARED_POOL: SharedArrayPool<POOLED_BUFFER_CAPACITY_BYTES> = SharedArrayPool::new());

thread_local! {
    // This is the simplest possible isolated pool implementation - a collection of fixed-size
    // buffers on every thread.
    //
    // We use MustNotDropItems policy because buffers are often referenced via raw pointers, so if
    // some items still exist in the collection, we have a high probability of dangling pointers,
    // which can be a big safety problem. All I/O buffers must be dropped before the thread exits.
    static THREAD_ISOLATED_POOL: RefCell<PinnedSlabChain<UnsafeCell<[u8; POOLED_BUFFER_CAPACITY_BYTES]>>> =
        RefCell::new(PinnedSlabChain::new(DropPolicy::MustNotDropItems));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_sliced_up() {
        let mut buffer = Buffer::<Shared>::from_boxed_slice([0_u8; 10].into());

        let [mut slice1, mut slice2, mut slice3] = buffer.as_mut_slices(&[2, 3, 5]);
        assert_eq!(slice1.len(), 2);
        assert_eq!(slice2.len(), 3);
        assert_eq!(slice3.len(), 5);

        slice1.fill(2);
        slice2.fill(3);
        slice3.fill(5);

        let original = buffer.try_into_inner_boxed_slice().unwrap();

        assert_eq!(original, [2, 2, 3, 3, 3, 5, 5, 5, 5, 5].into());
    }
}
