use std::mem;
use std::ops::Deref;
use windows::core::Free;
use windows::{core::Owned, Win32::Foundation::HANDLE};

/// An owned thread-safe HANDLE from the `windows` crate, which we release on a background
/// worker thread in case closing the handle incurs synchronous work due to flushing caches etc.
pub struct OwnedHandle {
    inner: HANDLE,
}

impl From<HANDLE> for OwnedHandle {
    fn from(handle: HANDLE) -> Self {
        Self { inner: handle }
    }
}

impl From<Owned<HANDLE>> for OwnedHandle {
    fn from(value: Owned<HANDLE>) -> Self {
        let inner = *value;

        // Forget the value so that the handle is not closed on drop of the original.
        mem::forget(value);

        inner.into()
    }
}

impl From<OwnedHandle> for HANDLE {
    fn from(value: OwnedHandle) -> HANDLE {
        let inner = value.inner;

        // Forget the value so that the handle is not closed on drop of the original.
        mem::forget(value);

        inner
    }
}

impl Deref for OwnedHandle {
    type Target = HANDLE;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl Drop for OwnedHandle {
    fn drop(&mut self) {
        let thread_safe = ThreadSafeHandle(self.inner);

        _ = crate::rt::spawn_blocking(move || {
            let mut thread_safe = thread_safe;

            unsafe {
                thread_safe.0.free();
            }
        });
    }
}

unsafe impl Send for OwnedHandle {}

struct ThreadSafeHandle(HANDLE);
unsafe impl Send for ThreadSafeHandle {}
