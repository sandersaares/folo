use crate::runtime::local_result_box::LocalResultBox;
use negative_impl::negative_impl;
use std::{future::Future, pin::Pin, rc::Rc, task};

/// Allows a unit of work to be awaited and its result to be observed on the same thread as it is
/// scheduled on.
///
/// Awaiting this is optional - the task will continue even if you drop the join handle.
#[derive(Debug)]
pub struct LocalJoinHandle<R> {
    result: Rc<LocalResultBox<R>>,
}

impl<R> LocalJoinHandle<R> {
    pub(crate) fn new(result: Rc<LocalResultBox<R>>) -> Self {
        Self { result }
    }
}

impl<R> Future for LocalJoinHandle<R> {
    type Output = R;

    fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> task::Poll<Self::Output> {
        match self.result.poll(cx.waker()) {
            Some(result) => task::Poll::Ready(result),
            None => task::Poll::Pending,
        }
    }
}

// Perhaps already implied but let's be super explicit here.
#[negative_impl]
impl<R> !Send for LocalJoinHandle<R> {}
#[negative_impl]
impl<R> !Sync for LocalJoinHandle<R> {}
