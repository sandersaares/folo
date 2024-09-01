//! Top-level free functions that can be called to manipulate the Folo runtime.

use crate::rt::{
    current_agent, current_executor, ready_after_poll::ReadyAfterPoll, LocalJoinHandle,
    RemoteJoinHandle,
};
use std::future::Future;

/// Spawns a task to execute a future on the current async worker thread.
///
/// # Panics
///
/// Panics if the current thread is not an async worker thread owned by a Folo runtime.
pub fn spawn<F, R>(future: F) -> LocalJoinHandle<R>
where
    F: Future<Output = R> + 'static,
    R: 'static,
{
    current_agent::get().spawn(future)
}

/// Spawns a task to execute a future on any worker thread owned by the same Folo runtime
/// as the current thread.
///
/// # Panics
///
/// Panics if the current thread is not owned by a Folo runtime.
pub fn spawn_on_any<F, R>(future: F) -> RemoteJoinHandle<R>
where
    F: Future<Output = R> + Send + 'static,
    R: Send + 'static,
{
    current_executor::get().spawn_on_any(future)
}

/// Yields control back to the async task runtime to allow other tasks to run.
/// There is no guarantee that other tasks will run in any particular order.
/// Even the same task that called this may be scheduled again immediately.
pub fn yield_now() -> impl Future<Output = ()> {
    ReadyAfterPoll::default()
}
