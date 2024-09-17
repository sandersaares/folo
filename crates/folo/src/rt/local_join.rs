use crate::sync::once_event;
use futures::FutureExt;
use negative_impl::negative_impl;
use std::{future::Future, pin::Pin, task};

/// Allows a unit of work to be awaited and its result to be observed on the same thread as it is
/// scheduled on.
///
/// Awaiting this is optional - the task will continue even if you drop the join handle.
#[derive(Debug)]
pub struct LocalJoinHandle<R> {
    rx: once_event::EmbeddedReceiver<R>,
}

impl<R> LocalJoinHandle<R> {
    pub(crate) fn new(rx: once_event::EmbeddedReceiver<R>) -> Self {
        Self { rx }
    }
}

impl<R> Future for LocalJoinHandle<R> {
    type Output = R;

    fn poll(mut self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> task::Poll<Self::Output> {
        self.rx.poll_unpin(cx)
    }
}

// Perhaps already implied but let's be super explicit here.
#[negative_impl]
impl<R> !Send for LocalJoinHandle<R> {}
#[negative_impl]
impl<R> !Sync for LocalJoinHandle<R> {}
