//! Top-level free functions that can be called to manipulate the Folo runtime.

use super::SynchronousTaskType;
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

/// Spawns a task to execute a future on every worker thread.
///
/// There are two layers of callbacks involved here, with the overall sequence being:
/// 1. The first layer will be called on the originating thread, to create a callback for each
///    worker thread we will be scheduling the task on.
/// 2. The result from the first callback will be a closure that we move to the target worker
///    thread and execute.
/// 3. The second callback will be called on the target thread and return the future that
///    becomes the subject of the task.
///
/// So essentially you are providing a "give me one more clone of the task-creator" function.
pub fn spawn_on_all<FC, FN, F, R>(clone_future_fn: FC) -> Box<[RemoteJoinHandle<R>]>
where
    FC: FnMut() -> FN,
    FN: FnOnce() -> F + Send + 'static,
    F: Future<Output = R> + 'static,
    R: Send + 'static,
{
    current_runtime::with(|runtime| runtime.spawn_on_all(clone_future_fn))
}

/// Spawns a task on a synchronous worker thread suitable for the specific type of synchronous
/// work requested, returning the result via a join handle suitable for use in asynchronous
/// tasks.
pub fn spawn_sync<F, R>(task_type: SynchronousTaskType, f: F) -> RemoteJoinHandle<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    current_runtime::with(|runtime| runtime.spawn_sync(task_type, f))
}

/// Spawns a task on a synchronous worker thread suitable for the specific type of synchronous
/// work requested, returning the result via a join handle suitable for use in asynchronous
/// tasks.
pub fn spawn_sync_on_any<F, R>(task_type: SynchronousTaskType, f: F) -> RemoteJoinHandle<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    current_runtime::with(|runtime| runtime.spawn_sync_on_any(task_type, f))
}

/// Yields control back to the async task runtime to allow other tasks to run.
/// There is no guarantee that other tasks will run in any particular order.
/// Even the same task that called this may be scheduled again immediately.
pub fn yield_now() -> impl Future<Output = ()> {
    ReadyAfterPoll::default()
}
