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
    mode: Mode<'a>,

    // We might not always use the full capacity (though we do by default).
    length: usize,
}

#[derive(Debug)]
enum Mode<'a> {
    Pooled {
        // This is the real storage of the bytes and determines the capacity.
        inner: Pin<&'a mut [u8]>,

        // For the time being, we only support pooled buffers (though later we can also do custom).
        index_in_pool: usize,
    },
    CallerProvided {
        // This is the real storage of the bytes and determines the capacity.
        inner: &'a mut [u8],
    },
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
                mode: Mode::Pooled {
                    inner,
                    index_in_pool: index,
                },
                length: BUFFER_CAPACITY_BYTES,
            }
        })
    }

    /// References an existing buffer provided by the caller.
    pub fn from_slice(slice: &'a mut [u8]) -> Self {
        CALLER_BUFFERS_REFERENCED.with(Event::observe_unit);

        let length = slice.len();

        Buffer {
            mode: Mode::CallerProvided { inner: slice },
            length,
        }
    }

    pub fn capacity(&self) -> usize {
        match &self.mode {
            Mode::Pooled { inner, .. } => inner.len(),
            Mode::CallerProvided { inner } => inner.len(),
        }
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
        match &mut self.mode {
            Mode::Pooled { inner, .. } => &mut inner[..self.length],
            Mode::CallerProvided { inner } => &mut inner[..self.length],
        }
    }

    /// Sets the length and obtains a mutable view over the contents of the buffer.
    /// Shorthand to easily fill the buffer and set the length in one go for write operations.
    pub fn as_mut_slice_with_length(&mut self, length: usize) -> &mut [u8] {
        self.set_length(length);

        match &mut self.mode {
            Mode::Pooled { inner, .. } => &mut inner[..self.length],
            Mode::CallerProvided { inner } => &mut inner[..self.length],
        }
    }

    /// Obtains an immutable view over the contents of the buffer.
    pub fn as_slice(&self) -> &[u8] {
        match &self.mode {
            Mode::Pooled { inner, .. } => &inner[..self.length],
            Mode::CallerProvided { inner } => &inner[..self.length],
        }
    }
}

impl Drop for Buffer<'_> {
    fn drop(&mut self) {
        if let Mode::Pooled { index_in_pool, .. } = self.mode {
            POOL.with(|pool| {
                let mut pool = pool.borrow_mut();
                pool.remove(index_in_pool);
                DROPPED.with(Event::observe_unit);
            });
        }
    }
}

#[negative_impl]
impl !Send for Buffer<'_> {}
#[negative_impl]
impl !Sync for Buffer<'_> {}

const BUFFER_CAPACITY_BYTES: usize = 64 * 1024;

thread_local! {
    static POOL: RefCell<PinnedSlabChain<UnsafeCell<[u8; BUFFER_CAPACITY_BYTES]>>> = RefCell::new(PinnedSlabChain::new());

    static CALLER_BUFFERS_REFERENCED: Event = EventBuilder::new()
        .name("caller_buffers_referenced")
        .build()
        .unwrap();

    static ALLOCATED: Event = EventBuilder::new()
        .name("pool_buffers_allocated")
        .build()
        .unwrap();

    static DROPPED: Event = EventBuilder::new()
        .name("pool_buffers_dropped")
        .build()
        .unwrap();
}
