use crate::{
    metrics::{Event, EventBuilder},
    util::PinnedSlabChain,
};
use core::slice;
use negative_impl::negative_impl;
use std::{
    cell::{RefCell, UnsafeCell},
    pin::Pin,
};

/// A buffer of bytes for reading from or writing to as part of low level I/O operations. This is
/// typically not visible to user code, rather it is used as the primitive inside the Folo I/O API.
///
/// This is a single threaded type - buffers cannot move between threads, all I/O stays within the
/// same threat from start to finish.
#[derive(Debug)]
pub struct Buffer<'a> {
    // This is the real storage of the bytes and determines the capacity.
    inner: Pin<&'a mut [u8]>,

    // We might not always use the full capacity (though we do by default).
    length: usize,

    // For the time being, we only support pooled buffers (though later we can also do custom).
    index_in_pool: usize,
}

impl<'a> Buffer<'a> {
    /// Obtains a new buffer from the current thread's buffer pool.
    pub fn from_pool() -> Self {
        POOL.with(|pool| {
            let mut pool = pool.borrow_mut();
            let inserter = pool.begin_insert();
            let index = inserter.index();

            let storage = inserter.insert(UnsafeCell::new([0; BUFFER_CAPACITY_BYTES]));

            ALLOCATED.with(Event::observe_unit);

            // SAFETY: The chain guarantees pinning, we just re-wrap Pin around the inner bytes.
            // We only ever hand out references derived from UnsafeCell, which are always valid
            // to hand out as long as we do not create multiple `&mut` references (which we do not
            // as Buffer holds the only reference and protects it via standard borrow mechanics).
            let inner = unsafe {
                Pin::new_unchecked(slice::from_raw_parts_mut(
                    storage.get() as *mut u8,
                    BUFFER_CAPACITY_BYTES,
                ))
            };

            Buffer {
                inner,
                length: BUFFER_CAPACITY_BYTES,
                index_in_pool: index,
            }
        })
    }

    pub fn capacity(&self) -> usize {
        self.inner.len()
    }

    pub fn length(&self) -> usize {
        self.length
    }

    pub fn set_length(&mut self, value: usize) {
        assert!(value <= self.capacity());

        self.length = value;
    }

    /// Obtains a mutable view over the contents of the buffer.
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.inner[..self.length]
    }

    /// Sets the length and obtains a mutable view over the contents of the buffer.
    /// Shorthand to easily fill the buffer and set the length in one go for write operations.
    pub fn as_mut_slice_with_length(&mut self, length: usize) -> &mut [u8] {
        self.set_length(length);

        &mut self.inner[..self.length]
    }

    /// Obtains an immutable view over the contents of the buffer.
    pub fn as_slice(&self) -> &[u8] {
        &self.inner[..self.length]
    }
}

impl Drop for Buffer<'_> {
    fn drop(&mut self) {
        POOL.with(|pool| {
            let mut pool = pool.borrow_mut();
            pool.remove(self.index_in_pool);
            DROPPED.with(Event::observe_unit);
        });
    }
}

#[negative_impl]
impl !Send for Buffer<'_> {}
#[negative_impl]
impl !Sync for Buffer<'_> {}

const BUFFER_CAPACITY_BYTES: usize = 64 * 1024;

thread_local! {
    static POOL: RefCell<PinnedSlabChain<UnsafeCell<[u8; BUFFER_CAPACITY_BYTES]>>> = RefCell::new(PinnedSlabChain::new());

    static ALLOCATED: Event = EventBuilder::new()
        .name("pool_buffers_allocated")
        .build()
        .unwrap();

    static DROPPED: Event = EventBuilder::new()
        .name("pool_buffers_dropped")
        .build()
        .unwrap();
}
