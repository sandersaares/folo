use crate::rt::SynchronousTaskType;
use crate::util::ThreadSafe;
use std::mem;
use std::ops::Deref;
use windows::{
    core::{Free, Owned},
    Win32::{Foundation::HANDLE, Networking::WinSock::SOCKET},
};

/// An owned HANDLE/SOCKET or other type of reference from the `windows` crate, which we release on
/// a background worker thread in case closing the handle incurs synchronous work due to flushing
/// caches etc.
///
/// # Safety
///
/// Must be used with a type of reference handle that is valid to close from any thread.
#[derive(Debug)]
pub struct OwnedHandle<T>
where
    T: Free + Copy + 'static,
{
    inner: T,
}

impl<T> OwnedHandle<T>
where
    T: Free + Copy + 'static,
{
    /// # Safety
    ///
    /// The caller must ensure that the reference handle is valid to close from any thread.
    pub unsafe fn new(handle: T) -> Self {
        Self { inner: handle }
    }
}

impl<T> From<T> for OwnedHandle<T>
where
    T: Free + Copy + 'static,
{
    fn from(handle: T) -> Self {
        Self { inner: handle }
    }
}

impl<T> From<Owned<T>> for OwnedHandle<T>
where
    T: Free + Copy + 'static,
{
    fn from(value: Owned<T>) -> Self {
        let inner = *value;

        // Forget the value so that the handle is not closed on drop of the original.
        mem::forget(value);

        inner.into()
    }
}

impl<T> Deref for OwnedHandle<T>
where
    T: Free + Copy + 'static,
{
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> Drop for OwnedHandle<T>
where
    T: Free + Copy + 'static,
{
    fn drop(&mut self) {
        // We require that this type is only used with thread-safe handles,
        // even if they do not always say so by implementing Send/Sync.
        let mut thread_safe = unsafe { ThreadSafe::new(self.inner) };

        // It is possible that we are dropping this handle *after* the Folo runtime itself has been
        // shut down (e.g. because it is one of the handles that a runtime client was holding onto).
        // Alternatively, we may be in the middle of shutting down but the runtime might still exist
        // in a semi-functional state (reference exists but runtime is not accepting any more tasks).
        // In this case we just perform the drop synchronously because there is not much else to do.
        if !crate::rt::current_runtime::is_some()
            || crate::rt::current_runtime::with(|x| x.is_stopping())
        {
            unsafe {
                (*thread_safe).free();
            }

            return;
        }

        // TODO: I am not convinced the above check saves us, there still feels like a race here
        // where we can stick our task into a queue where it is never picked up. Need a more
        // foolproof mechanism? Triple-check this logic, it is Very Suspect.

        // We select the high priority option because closing a handle could release valuable
        // resources that the app can put to use for other things. Furthermore, the high priority
        // queue is processed even when shutting down the runtime, as these resources can block
        // the shutdown process itself.
        _ = crate::rt::spawn_sync_on_any(SynchronousTaskType::HighPrioritySyscall, move || {
            let mut thread_safe = thread_safe;

            unsafe {
                (*thread_safe).free();
            }
        });
    }
}

unsafe impl<T> Send for OwnedHandle<T> where T: Free + Copy + 'static {}
unsafe impl<T> Sync for OwnedHandle<T> where T: Free + Copy + 'static {}

impl From<OwnedHandle<HANDLE>> for HANDLE {
    fn from(value: OwnedHandle<HANDLE>) -> HANDLE {
        let inner = value.inner;

        // Forget the value so that the handle is not closed on drop of the original.
        mem::forget(value);

        inner
    }
}

impl From<OwnedHandle<SOCKET>> for SOCKET {
    fn from(value: OwnedHandle<SOCKET>) -> SOCKET {
        let inner = value.inner;

        // Forget the value so that the handle is not closed on drop of the original.
        mem::forget(value);

        inner
    }
}
