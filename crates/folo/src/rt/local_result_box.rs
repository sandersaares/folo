use negative_impl::negative_impl;
use std::{cell::RefCell, mem, task::Waker};

#[derive(Debug)]
enum TaskResult<R> {
    // The task has not completed and nobody has started awaiting for the result yet.
    Pending,

    // The task has not completed but someone is awaiting the result.
    Awaiting(Waker),

    // The task has completed and the result is ready for consumption.
    Ready(R),

    // The task has completed and the result has been consumed.
    Consumed,
}

/// A box that holds the result of a task, optionally waking up a future when the result is ready.
///
/// This is the single-threaded variant of the type. See `RemoteResultBox` for a thread-safe variant.
#[derive(Debug)]
pub(crate) struct LocalResultBox<R> {
    result: RefCell<TaskResult<R>>,
}

impl<R> LocalResultBox<R> {
    pub fn new() -> Self {
        Self {
            result: RefCell::new(TaskResult::Pending),
        }
    }

    pub fn set(&self, result: R) {
        let mut self_result = self.result.borrow_mut();

        match &*self_result {
            TaskResult::Pending => {
                *self_result = TaskResult::Ready(result);
            }
            TaskResult::Awaiting(_) => {
                let existing_result = mem::replace(&mut *self_result, TaskResult::Ready(result));

                match existing_result {
                    TaskResult::Awaiting(waker) => waker.wake(),
                    _ => unreachable!("we are re-matching an already matched pattern"),
                }
            }
            TaskResult::Ready(_) => {
                panic!("result already set");
            }
            TaskResult::Consumed => {
                panic!("result already consumed");
            }
        }
    }

    // We expose a poll-like API for getting the result, as the ResultBox is only intended to be
    // read from a future's poll() function (via a join handle).
    pub fn poll(&self, waker: &Waker) -> Option<R> {
        let mut self_result = self.result.borrow_mut();

        match &*self_result {
            TaskResult::Pending => {
                *self_result = TaskResult::Awaiting(waker.clone());
                None
            }
            TaskResult::Awaiting(_) => {
                // This is permitted by the Future API contract, in which case only the waker
                // from the most recent poll should be woken up when the result is available.
                *self_result = TaskResult::Awaiting(waker.clone());
                None
            }
            TaskResult::Ready(_) => {
                let existing_result = mem::replace(&mut *self_result, TaskResult::Consumed);

                match existing_result {
                    TaskResult::Ready(result) => Some(result),
                    _ => unreachable!("we are re-matching an already matched pattern"),
                }
            }
            TaskResult::Consumed => {
                // We do not want to keep a copy of the result around, so we can only return it once.
                // The futures API contract allows us to panic in this situation.
                panic!("ResultBox polled after result was already consumed");
            }
        }
    }
}

// Perhaps already implied but let's be super explicit here.
#[negative_impl]
impl<R> !Send for LocalResultBox<R> {}
#[negative_impl]
impl<R> !Sync for LocalResultBox<R> {}
