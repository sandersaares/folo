use crate::rt::runtime_client::RuntimeClient;
use std::cell::OnceCell;

/// Executes a closure that receives the Folo runtime client for the runtime that owns the current
/// thread.
///
/// # Panics
///
/// Panics if the current thread is not owned by the Folo runtime.
pub fn with<F, R>(f: F) -> R
where
    F: FnOnce(&RuntimeClient) -> R,
{
    CURRENT.with(|runtime| {
        f(runtime
            .get()
            .expect("thread is not owned by the Folo runtime"))
    })
}

/// Attempts to get a new shared reference to the Folo runtime client for the runtime that owns the
/// current thread.
pub fn try_get() -> Option<RuntimeClient> {
    CURRENT.with(|runtime| runtime.get().cloned())
}

pub fn is_some() -> bool {
    CURRENT.with(|runtime| runtime.get().is_some())
}

pub fn set(value: RuntimeClient) {
    CURRENT.with(|runtime| {
        runtime
            .set(value)
            .expect("thread is already registered to a Folo runtime");
    });
}

thread_local!(
    static CURRENT: OnceCell<RuntimeClient> = OnceCell::new();
);
