use crate::{
    constants::{self, GENERAL_MILLISECONDS_BUCKETS},
    metrics::{Event, EventBuilder},
    time::UltraLowPrecisionInstant,
};
use std::{mem, sync::Mutex, task::Waker};

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
/// This is the thread-safe variant of the type. See `LocalResultBox` for a single-threaded variant.
#[derive(Debug)]
pub(crate) struct RemoteResultBox<R> {
    result: Mutex<TaskResult<R>>,
    created: UltraLowPrecisionInstant,
}

impl<R> RemoteResultBox<R> {
    pub fn new() -> Self {
        Self {
            result: Mutex::new(TaskResult::Pending),
            created: UltraLowPrecisionInstant::now(),
        }
    }

    pub fn set(&self, result: R) {
        FILL_DURATION.with(|x| {
            x.observe_millis(self.created.elapsed());
        });

        let mut waker: Option<Waker> = None;

        {
            let mut self_result = self.result.lock().expect(constants::POISONED_LOCK);

            match &*self_result {
                TaskResult::Pending => {
                    *self_result = TaskResult::Ready(result);
                }
                TaskResult::Awaiting(_) => {
                    let existing_result =
                        mem::replace(&mut *self_result, TaskResult::Ready(result));

                    match existing_result {
                        TaskResult::Awaiting(w) => waker = Some(w),
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

        // We perform the wakeup outside the lock to avoid unnecessary contention if the receiver
        // of the result wakes up instantly and we have not released our lock yet.
        if let Some(waker) = waker {
            waker.wake()
        }
    }

    // We expose a poll-like API for getting the result, as the ResultBox is only intended to be
    // read from a future's poll() function (via a join handle).
    pub fn poll(&self, waker: &Waker) -> Option<R> {
        POLL_COUNT.with(|x| x.observe_unit());

        let mut self_result = self.result.lock().expect(constants::POISONED_LOCK);

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
                CONSUME_DURATION.with(|x| {
                    x.observe_millis(self.created.elapsed());
                });

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

thread_local! {
    static FILL_DURATION: Event = EventBuilder::new("result_box_remote_time_to_fill_millis")
        .buckets(GENERAL_MILLISECONDS_BUCKETS)
        .build();

    static CONSUME_DURATION: Event = EventBuilder::new("result_box_remote_time_to_consume_millis")
        .buckets(GENERAL_MILLISECONDS_BUCKETS)
        .build();

    static POLL_COUNT: Event = EventBuilder::new("result_box_remote_poll_count")
        .build();
}
