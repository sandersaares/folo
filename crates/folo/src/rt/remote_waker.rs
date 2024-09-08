use crate::io::IoWaker;
use std::{
    sync::Arc,
    task::{Wake, Waker},
};

/// Wraps a Waker to not only wake up a task but also to wake up a thread (in addition to doing
/// whatever the original waker was doing).
pub struct RemoteWaker {
    io_waker: IoWaker,
    inner: Waker,
}

impl RemoteWaker {
    pub fn new(io_waker: IoWaker, inner: Waker) -> Arc<Self> {
        Arc::new(Self { io_waker, inner })
    }
}

impl Wake for RemoteWaker {
    fn wake(self: std::sync::Arc<Self>) {
        self.inner.wake_by_ref();
        self.io_waker.wake();
    }
}
