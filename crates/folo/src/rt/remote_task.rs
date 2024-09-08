use crate::{
    io::IoWaker,
    rt::{
        erased_async_task::ErasedResultAsyncTask, remote_result_box::RemoteResultBox,
        RemoteJoinHandle,
    },
};
use std::{cell::RefCell, future::Future, pin::Pin, sync::Arc, task};

/// This is the core essence of a task, relating a future to some result where everything up to and
/// including consuming the result may take place on a number of different threads.
///
/// The task is created as soon as its scheduling is requested. After initialization, details of the
/// task type are erased and it is exposed only as a `dyn Future<Output = ()>` used to progress the
/// task, pinned as soon as it reaches the async task engine of the worker selected to execute it.
///
/// Compare with `LocalTask` which is the single-threaded variant of this.
#[derive(Debug)]
pub(crate) struct RemoteTask<F, R>
where
    F: Future<Output = R> + Send + 'static,
    R: Send + 'static,
{
    // We drop this on `ErasedResultAsyncTask::clear()` to ensure that any captured state in the
    // future is dropped, releasing critical references that may be blocking runtime shutdown.
    future: RefCell<Option<F>>,

    // This is an Arc because we need to share it both with the task and with the JoinHandle, each
    // of which has an independent lifetime (runtime-defined and caller-defined, respectively).
    result: Arc<RemoteResultBox<R>>,
}

impl<F, R> RemoteTask<F, R>
where
    F: Future<Output = R> + Send + 'static,
    R: Send + 'static,
{
    pub fn new(future: F) -> Self {
        Self {
            future: RefCell::new(Some(future)),
            result: Arc::new(RemoteResultBox::new()),
        }
    }

    pub fn join_handle(&self, io_waker: Option<IoWaker>) -> RemoteJoinHandle<R> {
        // TODO: Protect this so only one join handle can be taken.
        RemoteJoinHandle::new(Arc::clone(&self.result), io_waker)
    }
}

impl<F, R> ErasedResultAsyncTask for RemoteTask<F, R>
where
    F: Future<Output = R> + Send + 'static,
    R: Send + 'static,
{
    fn is_inert(&self) -> bool {
        // A remote task is always fine to clean up - we hold no resources that need special care.
        true
    }

    fn clear(&self) {
        *self.future.borrow_mut() = None;
    }
}

impl<F, R> Future for RemoteTask<F, R>
where
    F: Future<Output = R> + Send + 'static,
    R: Send + 'static,
{
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> task::Poll<Self::Output> {
        let poll_result = {
            let self_as_mut = self.as_mut();
            let mut borrowed_future = self_as_mut.future.borrow_mut();
            let future = borrowed_future
                .as_mut()
                .expect("a task is never going to be polled after its future is removed");

            // SAFETY: It is actually pinned, the RefCell layer just makes it hard to preserve the
            // annotation, so we add it back manually.
            let future = unsafe { Pin::new_unchecked(future) };
            future.poll(cx)
        };

        match poll_result {
            task::Poll::Ready(result) => {
                // The only purpose of this is to send the real result onto its own path while
                // we simply report the poll status back to the caller who has no information about
                // the specific type of the result.
                self.result.set(result);
                task::Poll::Ready(())
            }
            task::Poll::Pending => task::Poll::Pending,
        }
    }
}
