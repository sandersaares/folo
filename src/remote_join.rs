use futures::{channel::oneshot, FutureExt};
use std::{future::Future, pin::Pin, task};

/// Allows a unit of work to be awaited and its result to be observed on any thread.
///
/// Awaiting this is optional - the task will continue even if you drop the join handle.
pub struct RemoteJoinHandle<R>
where
    R: Send + 'static,
{
    rx: oneshot::Receiver<R>,
}

impl<R> RemoteJoinHandle<R>
where
    R: Send + 'static,
{
    pub fn new(rx: oneshot::Receiver<R>) -> Self {
        Self { rx }
    }
}

impl<R> Future for RemoteJoinHandle<R>
where
    R: Send + 'static,
{
    type Output = R;

    fn poll(mut self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> task::Poll<Self::Output> {
        match self.rx.poll_unpin(cx) {
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
}

/// Wraps a future with the wiring required to support remote joins.
pub(crate) fn wrap_for_remote_join<F, R>(future: F) -> (impl Future<Output = ()>, RemoteJoinHandle<R>)
where
    F: Future<Output = R> + Send + 'static,
    R: Send + 'static,
{
    let (tx, rx) = oneshot::channel::<R>();

    let wrapped = async {
        let result = future.await;

        // If the join handle was dropped, this will return an error, which is fine.
        _ = tx.send(result);
    };

    let join_handle = RemoteJoinHandle::new(rx);

    (wrapped, join_handle)
}
