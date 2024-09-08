use std::future::Future;

/// An asyncronous task whose return type has been erased - we do not know what exactly the future
/// it executes is, we just know how to execute and handle it.
pub trait ErasedResultAsyncTask: Future<Output = ()> + 'static {
    /// Returns true if it is safe to drop the task. If not, there are still references to it and
    /// the caller must wait before dropping the task.
    fn is_inert(&self) -> bool;

    /// Clears all references this task holds to other tasks on the same worker thread. After this,
    /// the task must not be polled again.
    fn clear(&self);
}
