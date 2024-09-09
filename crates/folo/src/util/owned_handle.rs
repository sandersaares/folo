use super::ThreadSafe;
use crate::rt::SynchronousTaskType;
use std::mem;
use std::ops::Deref;
use windows::{core::Free, core::Owned, Win32::Foundation::HANDLE};

/// An owned HANDLE from the `windows` crate, which we release on a background worker thread in case
/// closing the handle incurs synchronous work due to flushing caches etc.
///
/// # Safety
///
/// Must be a type of handle that is valid to close from any thread.
#[derive(Debug)]
pub struct OwnedHandle {
    inner: HANDLE,
}

impl OwnedHandle {
    /// # Safety
    ///
    /// The caller must ensure that the handle is valid to close from any thread.
    pub unsafe fn new(handle: HANDLE) -> Self {
        Self { inner: handle }
    }
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
        // We require that this type is only used with thread-safe handles.
        let mut thread_safe = unsafe { ThreadSafe::new(self.inner) };

        // It is possible that we are dropping this handle *after* the Folo runtime itself has been
        // shut down (e.g. because it is one of the handles that a runtime client was holding onto).
        // In this case we just perform the drop synchronously because there is not much else to do.
        if !crate::rt::current_runtime::is_some() {
            unsafe {
                (*thread_safe).free();
            }

            return;
        }

        // We select the high priority option because closing a handle could release valuable
        // resources that the app can put to use for other things.
        _ = crate::rt::spawn_sync(SynchronousTaskType::HighPrioritySyscall, move || {
            let mut thread_safe = thread_safe;

            unsafe {
                (*thread_safe).free();
            }
        });
    }
}

unsafe impl Send for OwnedHandle {}
unsafe impl Sync for OwnedHandle {}
