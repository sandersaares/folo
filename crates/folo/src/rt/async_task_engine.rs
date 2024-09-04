use crate::rt::LocalErasedTask;
use negative_impl::negative_impl;
use std::{
    cell::RefCell,
    collections::VecDeque,
    fmt::{self, Debug, Formatter},
    task,
};

use super::waker::WakeSignal;

type Key = usize;

/// The engine incrementally executes async tasks on a single thread when polled. It is not active
/// on its own and requires an external actor to poll it to make progress.
#[derive(Debug)]
pub struct AsyncTaskEngine {
    // We use a pinned slab here to allocate the tasks in-place and avoid a layer of Arc-indirection.
    tasks: pinned_slab::Slab<Task>,

    // The active set contains all the tasks we want to poll. This is where all futures start.
    active: VecDeque<Key>,

    // The inactive set contains all the tasks that are sleeping. We will move them back to the
    // active set after a waker notifies us that a future needs to wake up. Note that the wakeup
    // may arrive from within the poll itself, which implies that we need to consider a future part
    // of the inactive set immediately before polling it, and be ready to move it back to the active
    // set during/after the poll if a waker is signaled during a poll. Also implies engine is not'
    // locked during a poll, so new activity can occur (not only wakes but also new tasks being added).
    inactive: VecDeque<Key>,

    // These tasks have completed and we are waiting for the wake signals to become inert before we
    // can remove them from the slab.
    completed: VecDeque<Key>,
}

impl AsyncTaskEngine {
    pub fn new() -> Self {
        Self {
            tasks: pinned_slab::Slab::new(),
            active: VecDeque::new(),
            inactive: VecDeque::new(),
            completed: VecDeque::new(),
        }
    }

    /// Enqueues a future whose return type has been erased. It will be polled but no result
    /// will be made available by the async task engine - it is expected that some other mechanism
    /// is used to observe the result.
    pub fn enqueue_erased(&mut self, erased_task: LocalErasedTask) {
        let task = Task::new(erased_task);

        // We just have a normal `&` to it, but the task is now pinned.
        let (key, _) = self.tasks.insert(task);

        self.active.push_back(key);
    }

    pub fn execute_cycle(&mut self) -> CycleResult {
        // If we have no activity in the cycle, we indicate that we should suspend.
        // In the future we should refactor this to a more direct signaling system.
        let mut had_activity = false;

        while let Some(key) = self.active.pop_front() {
            had_activity = true;

            // SAFETY: This is marked unsafe because it returns a plain reference to a pinned value.
            // As long as we still treat it as pinned (we do), all is well.
            let task = unsafe {
                self.tasks
                    .get_mut(key)
                    .expect("if we have the ID for a task, we must also have the task")
            };

            // SAFETY: Requires `&self` to be pinned, which we guarantee by always keeping the task
            // pinned in the slab.
            match unsafe { task.poll() } {
                task::Poll::Ready(()) => {
                    self.completed.push_back(key);
                }
                task::Poll::Pending => {
                    self.inactive.push_back(key);
                }
            }
        }

        if self.activate_awakened_tasks() {
            had_activity = true;
        }

        self.drop_completed_tasks();

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
            let key = &self.inactive[index];

            // SAFETY: This is marked unsafe because it returns a plain reference to a pinned value.
            // As long as we still treat it as pinned (we do), all is well.
            let task = unsafe {
                self.tasks
                    .get_mut(*key)
                    .expect("if we have the ID for a task, we must also have the task")
            };

            // SAFETY: Requires `&self` to be pinned, which we guarantee by always keeping the task
            // pinned in the slab.
            if unsafe { task.wake_signal.consume_awakened() } {
                had_activity = true;

                let key = self
                    .inactive
                    .remove(index)
                    .expect("key must exist - we just got it from the same data structure");

                self.active.push_back(key);
            } else {
                index += 1;
            }
        }

        had_activity
    }

    fn drop_completed_tasks(&mut self) {
        self.completed.retain(|key| {
            // SAFETY: This is marked unsafe because it returns a plain reference to a pinned value.
            // As long as we still treat it as pinned (we do), all is well.
            let task = unsafe {
                self.tasks
                    .get_mut(*key)
                    .expect("if we have the ID for a task, we must also have the task")
            };

            // SAFETY: Requires `&self` to be pinned, which we guarantee by always keeping the task
            // pinned in the slab.
            let is_inert = unsafe { task.wake_signal.is_inert() };

            if is_inert {
                self.tasks.remove(*key);
            }

            !is_inert
        });
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
    erased_task: RefCell<LocalErasedTask>,
    wake_signal: WakeSignal,
}

impl Task {
    fn new(erased_task: LocalErasedTask) -> Self {
        Self {
            erased_task: RefCell::new(erased_task),
            wake_signal: WakeSignal::new(),
        }
    }

    /// # Safety
    ///
    /// Requires `&self` to be pinned.
    unsafe fn poll(&self) -> task::Poll<()> {
        // SAFETY: Requires `&self` to be pinned, which we guarantee by always keeping the task
        // itself pinned.
        let waker = unsafe { self.wake_signal.waker() };

        let mut context = task::Context::from_waker(waker);

        // SAFETY: We are only mut-borrowing in poll() which is only called by the current thread
        // and never recursively.
        self.erased_task.borrow_mut().as_mut().poll(&mut context)
    }
}

impl Debug for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Task")
            .field("wake_signal", &self.wake_signal)
            .finish()
    }
}
