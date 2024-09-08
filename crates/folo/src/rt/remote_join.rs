use super::remote_waker::RemoteWaker;
use crate::{
    io::IoWaker,
    rt::{remote_result_box::RemoteResultBox, LocalJoinHandle},
};
use futures::{channel::oneshot, FutureExt};
use std::future::Future;
use std::sync::Arc;
use std::{pin::Pin, task};

/// Allows a unit of work to be awaited and its result to be observed on any thread.
///
/// You can convert a `LocalJoinHandle` into a `RemoteJoinHandle` using `Into::into`.
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
    pub(crate) fn new(result: Arc<RemoteResultBox<R>>, io_waker: Option<IoWaker>) -> Self {
        Self {
            model: ImplementationModel::RemoteTask { result, io_waker },
        }
    }

    pub(crate) fn from_local(local: LocalJoinHandle<R>) -> Self {
        // We add a new task to await the result on the current thread, after which we publish
        // it in a thread-safe manner to whoever wants to consume this object.

        // TODO: This is probably not the most efficient way to do this, what with spawning
        // a new task here and allocating a channel and so forth. We could probably improve this
        // with some "direct wiring" between the two endpoints. Worry about it later - it works.

        let (tx, rx) = oneshot::channel::<R>();

        _ = crate::rt::spawn(async {
            let result = local.await;

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
        match &mut self.model {
            ImplementationModel::LocalJoinHandle { ref mut result_rx } => {
                match result_rx.poll_unpin(cx) {
                    task::Poll::Ready(Ok(result)) => task::Poll::Ready(result),
                    // An error result may be returned if, for example, the sender was dropped before
                    // sending. When that may happen is up to the implementation of the runtime. For
                    // example, this may happen when the runtime is shutting down and dropping queued
                    // tasks. We take no strong dependencies here on the design of the runtime - if no
                    // result has arrived, we simply treat this as pending forever. The caller is expected
                    // to apply a suitable abandonment timeout if there is a risk of it awaiting forever.
                    task::Poll::Ready(Err(_)) | task::Poll::Pending => task::Poll::Pending,
                }
            }
            ImplementationModel::RemoteTask { result, io_waker } => {
                let poll_result = match io_waker {
                    None => result.poll(cx.waker()),
                    Some(io_waker) => {
                        let composite_waker =
                            RemoteWaker::new(io_waker.clone(), cx.waker().clone());
                        result.poll(&composite_waker.into())
                    }
                };

                match poll_result {
                    Some(result) => task::Poll::Ready(result),
                    None => task::Poll::Pending,
                }
            }
        }
    }
}

#[derive(Debug)]
enum ImplementationModel<R> {
    // We are wrapping a `LocalJoinHandle`, which will send the result via oneshot channel.
    LocalJoinHandle {
        result_rx: oneshot::Receiver<R>,
    },

    // We are observing a `RemoteTask` to obtain the result from it. We use a special waker to
    // also wake up our thread from I/O sleep if it is sleeping.
    RemoteTask {
        result: Arc<RemoteResultBox<R>>,
        io_waker: Option<IoWaker>,
    },
}

impl<R> From<LocalJoinHandle<R>> for RemoteJoinHandle<R>
where
    R: Send + 'static,
{
    fn from(value: LocalJoinHandle<R>) -> Self {
        Self::from_local(value)
    }
}
