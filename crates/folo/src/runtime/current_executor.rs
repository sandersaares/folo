use crate::runtime::executor_client::ExecutorClient;
use std::{cell::RefCell, sync::Arc};

/// Gets the executor client for the Folo executor that owns the current thread.
///
/// # Panics
///
/// Panics if the current thread is not owned by a Folo executor.
pub fn get() -> Arc<ExecutorClient> {
    CURRENT_EXECUTOR.with_borrow(|current_executor| {
        Arc::clone(
            current_executor
                .as_ref()
                .expect("this thread is not owned by a Folo executor"),
        )
    })
}

/// Optionally gets the executor client for the Folo executor that owns the current thread.
pub fn try_get() -> Option<Arc<ExecutorClient>> {
    CURRENT_EXECUTOR.with_borrow(|current_executor| current_executor.clone())
}

pub fn set(executor: Arc<ExecutorClient>) {
    CURRENT_EXECUTOR.with_borrow_mut(|current_executor| {
        if current_executor.is_some() {
            panic!("this thread is already registered to a Folo executor");
        }

        *current_executor = Some(executor);
    });
}

thread_local!(
    static CURRENT_EXECUTOR: RefCell<Option<Arc<ExecutorClient>>> = const { RefCell::new(None) }
);
