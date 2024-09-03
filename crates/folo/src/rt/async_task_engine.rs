use crate::rt::LocalErasedTask;
use negative_impl::negative_impl;
use std::{
    collections::VecDeque,
    fmt::{self, Debug, Formatter},
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    task::{self, Wake},
};

/// The engine incrementally executes async tasks on a single thread when polled. It is not active
/// on its own and requires an external actor to poll it to make progress.
#[derive(Debug)]
pub struct AsyncTaskEngine {
    // The active set contains all the futures we want to poll. This is where all futures start.
    active: VecDeque<Task>,

    // The inactive set contains all the futures that are sleeping. We will move them back to the
    // active set once a waker notifies us that a future needs to wake up. Note that the wakeup
    // may arrive from within the poll itself, which implies that we need to consider a future part
    // of the inactive set immediately before polling it, and be ready to move it back to the active
    // set during the poll if a waker is signaled during a poll. Also implies engine is not locked
    // during a poll, so new activity can occur (not only wakes but also new tasks being added).
    inactive: VecDeque<Task>,
}

impl AsyncTaskEngine {
    pub fn new() -> Self {
        Self {
            active: VecDeque::new(),
            inactive: VecDeque::new(),
        }
    }

    /// Enqueues a future whose return type has been erased. It will be polled but no result
    /// will be made available by the async task engine - it is expected that some other mechanism
    /// is used to observe the result.
    pub fn enqueue_erased(&mut self, erased_task: LocalErasedTask) {
        let task = Task::new(erased_task);
        self.active.push_back(task);
    }

    pub fn execute_cycle(&mut self) -> CycleResult {
        // If we have no activity in the cycle, we indicate that we should suspend.
        // In the future we should refactor this to a more direct signaling system.
        let mut had_activity = false;

        while let Some(mut task) = self.active.pop_front() {
            had_activity = true;

            match task.poll() {
                task::Poll::Ready(()) => {}
                task::Poll::Pending => {
                    self.inactive.push_back(task);
                }
            }
        }

        if self.activate_awakened_tasks() {
            had_activity = true;
        }

        if had_activity {
            CycleResult::Continue
        } else {
            CycleResult::Suspend
        }
    }

    // Moves any awakened tasks into the active set. Returns whether any tasks were moved.
    fn activate_awakened_tasks(&mut self) -> bool {
        let mut had_activity = false;

        let mut index = 0;

        while index < self.inactive.len() {
            let task = &self.inactive[index];

            if task.waker.consume_awakened() {
                had_activity = true;

                let task = self.inactive.remove(index).unwrap();
                self.active.push_back(task);
            } else {
                index += 1;
            }
        }

        had_activity
    }
}

impl Default for AsyncTaskEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[negative_impl]
impl !Send for AsyncTaskEngine {}
#[negative_impl]
impl !Sync for AsyncTaskEngine {}

/// The result of executing one cycle of the async task engine.
#[derive(Debug, PartialEq, Eq)]
pub enum CycleResult {
    /// The cycle was completed and we are ready to start the next cycle immediately.
    Continue,

    /// The cycle completed without performing any work - we should suspend until there is reason
    /// to suspect additional work has arrived (e.g. new tasks enqueued or on IO completions).
    Suspend,
}

struct Task {
    erased_task: LocalErasedTask,
    waker: Arc<TaskWaker>,
}

impl Task {
    fn new(erased_task: LocalErasedTask) -> Self {
        Self {
            erased_task,
            waker: Arc::new(TaskWaker::new()),
        }
    }

    fn poll(&mut self) -> task::Poll<()> {
        // TODO: This allocation is gross. To be fixed later (requires unsafe code).
        let waker = Arc::clone(&self.waker).into();
        let mut context = task::Context::from_waker(&waker);

        self.erased_task.as_mut().poll(&mut context)
    }
}

impl Debug for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Task").field("waker", &self.waker).finish()
    }
}

#[repr(transparent)]
struct TaskWaker {
    awakened: Pin<Arc<AtomicBool>>,
}

impl Wake for TaskWaker {
    fn wake(self: Arc<Self>) {
        self.awakened.store(true, Ordering::Relaxed)
    }
}

impl TaskWaker {
    fn new() -> Self {
        Self {
            awakened: Pin::new(Arc::new(AtomicBool::new(false))),
        }
    }

    /// Checks whether the waker has woken up. If so, resets the waker.
    fn consume_awakened(&self) -> bool {
        let is_awakened = self.awakened.load(Ordering::Relaxed);

        if is_awakened {
            self.awakened.store(false, Ordering::Relaxed);
        }

        is_awakened
    }
}

impl Debug for TaskWaker {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("TaskWaker")
            .field("awakened", &self.awakened)
            .finish()
    }
}
