use std::{future::Future, pin::Pin, task};

/// A unit-result future that becomes ready immediately after the first time it is polled.
/// We use this to implement yield_now().
#[derive(Debug, Default)]
pub(crate) struct ReadyAfterPoll {
    first_poll_completed: bool,
}

impl Future for ReadyAfterPoll {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> task::Poll<Self::Output> {
        if self.first_poll_completed {
            task::Poll::Ready(())
        } else {
            self.first_poll_completed = true;
            cx.waker().wake_by_ref();
            task::Poll::Pending
        }
    }
}
