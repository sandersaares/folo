use crate::rt::runtime_client::RuntimeClient;
use std::{cell::RefCell, sync::Arc};

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
    CURRENT.with_borrow(|runtime| {
        f(runtime
            .as_ref()
            .expect("thread is not owned by the Folo runtime"))
    })
}

/// Attempts to get a new shared reference to the Folo runtime client for the runtime that owns the
/// current thread.
pub fn try_get() -> Option<Arc<RuntimeClient>> {
    CURRENT.with_borrow(|runtime| runtime.clone())
}

pub fn set(value: Arc<RuntimeClient>) {
    CURRENT.with_borrow_mut(|runtime| {
        assert!(
            runtime.is_none(),
            "thread is already registered to a Folo runtime"
        );

        *runtime = Some(value);
    });
}

thread_local!(
    static CURRENT: RefCell<Option<Arc<RuntimeClient>>> = const { RefCell::new(None) }
);
