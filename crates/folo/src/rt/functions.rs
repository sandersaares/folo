//! Top-level free functions that can be called to manipulate the Folo runtime.

use crate::rt::{
    current_async_agent, current_runtime, ready_after_poll::ReadyAfterPoll, LocalJoinHandle,
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
    current_async_agent::with(|agent| agent.spawn(future))
}

/// Spawns a task to execute a future on any worker thread owned by the same Folo runtime
/// as the current thread. The future is provided by a closure.
///
/// The future itself does not have to be thread-safe. However, the closure must be.
///
/// # Panics
///
/// Panics if the current thread is not owned by a Folo runtime.
pub fn spawn_on_any<FN, F, R>(future_fn: FN) -> RemoteJoinHandle<R>
where
    FN: FnOnce() -> F + Send + 'static,
    F: Future<Output = R> + 'static,
    R: Send + 'static,
{
    current_runtime::with(|runtime| runtime.spawn_on_any(future_fn))
}

/// Spawns a blocking task on any synchronous worker thread suitable for blocking, returning
/// the result via a join handle suitable for use in asynchronous tasks.
pub fn spawn_blocking<F, R>(f: F) -> RemoteJoinHandle<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    current_runtime::with(|runtime| runtime.spawn_blocking(f))
}

/// Yields control back to the async task runtime to allow other tasks to run.
/// There is no guarantee that other tasks will run in any particular order.
/// Even the same task that called this may be scheduled again immediately.
pub fn yield_now() -> impl Future<Output = ()> {
    ReadyAfterPoll::default()
}
