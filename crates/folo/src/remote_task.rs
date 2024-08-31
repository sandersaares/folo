use crate::{remote_result_box::RemoteResultBox, RemoteJoinHandle};
use pin_project::pin_project;
use std::{future::Future, pin::Pin, sync::Arc, task};

/// This is the core essence of a task, relating a future to some result where everything up to and
/// including consuming the result may take place on a number of different threads.
///
/// The task is created as soon as its scheduling is requested. After initialization, details of the
/// task type are erased and it is exposed only as a `dyn Future<Output = ()>` used to progress the
/// task, pinned as soon as it reaches the async task engine of the worker selected to execute it.
///
/// Compare with `LocalTask` which is the single-threaded variant of this.
#[pin_project]
#[derive(Debug)]
pub(crate) struct RemoteTask<F, R>
where
    F: Future<Output = R> + Send + 'static,
    R: Send + 'static,
{
    #[pin]
    future: F,

    // This is an Rc because we need to share it both with the task and with the JoinHandle, each
    // of which has an independent lifetime (executor-defined and caller-defined, respectively).
    result: Arc<RemoteResultBox<R>>,
}

impl<F, R> RemoteTask<F, R>
where
    F: Future<Output = R> + Send + 'static,
    R: Send + 'static,
{
    pub fn new(future: F) -> Self {
        Self {
            future,
            result: Arc::new(RemoteResultBox::new()),
        }
    }

    pub fn join_handle(&self) -> RemoteJoinHandle<R> {
        RemoteJoinHandle::new(Arc::clone(&self.result))
    }
}

impl<F, R> Future for RemoteTask<F, R>
where
    F: Future<Output = R> + Send + 'static,
    R: Send + 'static,
{
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> task::Poll<Self::Output> {
        match self.as_mut().project().future.poll(cx) {
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
