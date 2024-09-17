use negative_impl::negative_impl;
use std::{
    cell::{Cell, RefCell},
    collections::VecDeque,
    future::Future,
    pin::Pin,
    rc::Rc,
    task::{self, Waker},
};

/// Controls access to a thread-local resource, granting only a limited number of concurrent tasks
/// access.
pub struct LocalSemaphore<const MAX: usize> {
    current: Cell<usize>,
    awaiting: Rc<RefCell<VecDeque<Waker>>>,
}

impl<const MAX: usize> LocalSemaphore<MAX> {
    pub fn new() -> Self {
        Self {
            current: Cell::new(0),
            awaiting: Rc::new(RefCell::new(VecDeque::new())),
        }
    }

    pub fn acquire(&self) -> impl Future<Output = LocalSemaphoreGuard<'_, MAX>> {
        Acquire { semaphore: self }
    }

    fn release_one(&self) {
        self.current.set(self.current.get() - 1);

        if let Some(waker) = self.awaiting.borrow_mut().pop_front() {
            waker.wake();
        }
    }
}

impl<const MAX: usize> Default for LocalSemaphore<MAX> {
    fn default() -> Self {
        Self::new()
    }
}

#[negative_impl]
impl<const MAX: usize> !Send for LocalSemaphore<MAX> {}
#[negative_impl]
impl<const MAX: usize> !Sync for LocalSemaphore<MAX> {}

struct Acquire<'s, const MAX: usize> {
    semaphore: &'s LocalSemaphore<MAX>,
}

impl<'s, const MAX: usize> Future for Acquire<'s, MAX> {
    type Output = LocalSemaphoreGuard<'s, MAX>;

    fn poll(self: Pin<&mut Self>, cx: &mut task::Context) -> task::Poll<Self::Output> {
        if self.semaphore.current.get() < MAX {
            self.semaphore.current.set(self.semaphore.current.get() + 1);
            task::Poll::Ready(LocalSemaphoreGuard {
                semaphore: self.semaphore,
            })
        } else {
            // When woken up, we now expect `current < MAX`.
            self.semaphore
                .awaiting
                .borrow_mut()
                .push_back(cx.waker().clone());
            task::Poll::Pending
        }
    }
}

pub struct LocalSemaphoreGuard<'s, const MAX: usize> {
    semaphore: &'s LocalSemaphore<MAX>,
}

impl<'s, const MAX: usize> Drop for LocalSemaphoreGuard<'s, MAX> {
    fn drop(&mut self) {
        self.semaphore.release_one();
    }
}
