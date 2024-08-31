use crate::{remote_result_box::RemoteResultBox, LocalJoinHandle};
use futures::{channel::oneshot, FutureExt};
use std::{future::Future, pin::Pin, sync::Arc, task};

/// Allows a unit of work to be awaited and its result to be observed on any thread.
///
/// You can convert a `LocalJoinHandle` into a `RemoteJoinHandle` using `Into::into`. Note that this
/// requires you to already be on the thread where the `LocalJoinHandle` was created, which is not
/// always going to be the case.
///
/// Awaiting this is optional - the task will continue even if you drop the join handle.
#[derive(Debug)]
pub struct RemoteJoinHandle<R>
where
    R: Send + 'static,
{
    model: ImplementationModel<R>,
}

impl<R> RemoteJoinHandle<R>
where
    R: Send + 'static,
{
    pub(crate) fn new(result: Arc<RemoteResultBox<R>>) -> Self {
        println!("remote join handle created from remote result box");

        Self {
            model: ImplementationModel::RemoteTask { result },
        }
    }

    pub(crate) fn from_local(local: LocalJoinHandle<R>) -> Self {
        println!("remote join handle created from local join handle");

        // We add a new task to await the result on the current thread, after which we publish
        // it in a thread-safe manner to whoever wants to consume this object.

        let (tx, rx) = oneshot::channel::<R>();

        _ = crate::spawn(async {
            let result = local.await;

            println!("local join handle completed, publishing remote result");

            // If the join handle was dropped, this will return an error, which is fine.
            _ = tx.send(result);
        });

        Self {
            model: ImplementationModel::LocalJoinHandle { result_rx: rx },
        }
    }
}

impl<R> Future for RemoteJoinHandle<R>
where
    R: Send + 'static,
{
    type Output = R;

    fn poll(mut self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> task::Poll<Self::Output> {
        println!("remote join handle polled");

        match &mut self.model {
            ImplementationModel::LocalJoinHandle { ref mut result_rx } => {
                match result_rx.poll_unpin(cx) {
                    task::Poll::Ready(Ok(result)) => task::Poll::Ready(result),
                    // An error result may be returned if, for example, the sender was dropped before
                    // sending. When that may happen is up to the implementation of the executor. For
                    // example, this may happen when the executor is shutting down and dropping queued
                    // tasks. We take no strong dependencies here on the design of the executor - if no
                    // result has arrived, we simply treat this as pending forever. The caller is expected
                    // to apply a suitable abandonment timeout if there is a risk of it awaiting forever.
                    task::Poll::Ready(Err(_)) | task::Poll::Pending => task::Poll::Pending,
                }
            }
            ImplementationModel::RemoteTask { result } => match result.poll(cx.waker()) {
                Some(result) => task::Poll::Ready(result),
                None => task::Poll::Pending,
            },
        }
    }
}

#[derive(Debug)]
enum ImplementationModel<R> {
    // We are wrapping a `LocalJoinHandle`, which will send the result via oneshot channel.
    LocalJoinHandle { result_rx: oneshot::Receiver<R> },

    // We are observing a `RemoteTask` to obtain the result from it.
    RemoteTask { result: Arc<RemoteResultBox<R>> },
}

impl<R> From<LocalJoinHandle<R>> for RemoteJoinHandle<R>
where
    R: Send + 'static,
{
    fn from(value: LocalJoinHandle<R>) -> Self {
        Self::from_local(value)
    }
}
