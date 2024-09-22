use super::Storage;
use crate::mem::PooledArrayLease;
use std::{mem, pin::Pin, ptr::NonNull};

// The backing storage of a thread-safe Buffer.
#[derive(Debug)]
pub(super) enum SharedStorage {
    Pooled {
        inner: NonNull<[u8]>,

        lease: PooledArrayLease<POOL_BUFFER_CAPACITY_BYTES>,
    },
    BoxedSlice {
        // We allow the caller to retrieve the inner value from the buffer via
        // `.into_inner_boxed_slice()` if they wish to reuse the storage later.
        inner: Pin<Box<[u8]>>,
    },
}

impl Storage for SharedStorage {
    fn as_slice(&self) -> Pin<&[u8]> {
        match self {
            // SAFETY: Both NonNull and Pin are compile time decorators and have the same layout
            // as the inner value at runtime, so this transmutation is safe as long as we ensure no
            // borrow rules are violated (which we do - normal borrow checker logic applies because
            // the returned slice reference is tied to the lifetime of the parent type).
            Self::Pooled { inner, .. } => unsafe {
                mem::transmute::<NonNull<[u8]>, Pin<&[u8]>>(*inner)
            },
            Self::BoxedSlice { inner } => inner.as_ref(),
        }
    }

    fn as_mut_slice(&mut self) -> Pin<&mut [u8]> {
        match self {
            // SAFETY: Both NonNull and Pin are compile time decorators and have the same layout
            // as the inner value at runtime, so this transmutation is safe as long as we ensure no
            // borrow rules are violated (which we do - normal borrow checker logic applies because
            // the returned slice reference is tied to the lifetime of the parent type).
            Self::Pooled { inner, .. } => unsafe {
                mem::transmute::<NonNull<[u8]>, Pin<&mut [u8]>>(*inner)
            },
            Self::BoxedSlice { inner } => inner.as_mut(),
        }
    }
}

// 64 KB is the default "stream to stream" copy size in .NET, so we use that as a default buffer
// size, as well. Note that this is not necessarily the best for high throughput single-stream I/O
// and larger buffers will often provide better throughput for a single high throughput stream.
const POOL_BUFFER_CAPACITY_BYTES: usize = 64 * 1024;
