use crate::rt::{local_result_box::LocalResultBox, LocalJoinHandle};
use negative_impl::negative_impl;
use pin_project::pin_project;
use std::{future::Future, pin::Pin, rc::Rc, task};

/// This is the core essence of a task, relating a future to some result where everything up to and
/// including consuming the result takes place on a single thread.
///
/// The task is created as soon as its scheduling is requested. After initialization, details of the
/// task type are erased and it is exposed only as a `dyn Future<Output = ()>` used to progress the
/// task, pinned as soon as it reaches the async task engine of the worker selected to execute it.
///
/// Compare with `RemoteTask` which is the multithreaded variant of this.
#[pin_project]
#[derive(Debug)]
pub(crate) struct LocalTask<F, R>
where
    F: Future<Output = R> + 'static,
    R: 'static,
{
    #[pin]
    future: F,

    // This is an Rc because we need to share it both with the task and with the JoinHandle, each
    // of which has an independent lifetime (executor-defined and caller-defined, respectively).
    result: Rc<LocalResultBox<R>>,
}

impl<F, R> LocalTask<F, R>
where
    F: Future<Output = R> + 'static,
    R: 'static,
{
    pub fn new(future: F) -> Self {
        Self {
            future,
            result: Rc::new(LocalResultBox::new()),
        }
    }

    pub fn join_handle(&self) -> LocalJoinHandle<R> {
        LocalJoinHandle::new(Rc::clone(&self.result))
    }
}

impl<F, R> Future for LocalTask<F, R>
where
    F: Future<Output = R> + 'static,
    R: 'static,
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

// Perhaps already implied but let's be super explicit here.
#[negative_impl]
impl<F, R> !Send for LocalTask<F, R>
where
    F: Future<Output = R> + 'static,
    R: 'static,
{
}
#[negative_impl]
impl<F, R> !Sync for LocalTask<F, R>
where
    F: Future<Output = R> + 'static,
    R: 'static,
{
}
