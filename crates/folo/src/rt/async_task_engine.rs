use crate::{
    rt::{waker::WakeSignal, LocalErasedTask},
    util::PinnedSlabChain,
};
use negative_impl::negative_impl;
use pin_project::pin_project;
use std::{
    cell::RefCell,
    collections::VecDeque,
    fmt::{self, Debug, Formatter},
    pin::Pin,
    task,
};

type TaskKey = usize;

/// The engine incrementally executes async tasks on a single thread when polled. It is not active
/// on its own and requires an external actor to poll it to make progress.
#[derive(Debug)]
pub struct AsyncTaskEngine {
    // We use a pinned slab here to allocate the tasks in-place and avoid a layer of Arc-indirection.
    tasks: PinnedSlabChain<Task>,

    // The active set contains all the tasks we want to poll. This is where all futures start.
    active: VecDeque<TaskKey>,

    // The inactive set contains all the tasks that are sleeping. We will move them back to the
    // active set after a waker notifies us that a future needs to wake up. Note that the wakeup
    // may arrive from within the poll itself, which implies that we need to consider a future part
    // of the inactive set immediately before polling it, and be ready to move it back to the active
    // set during/after the poll if a waker is signaled during a poll. Also implies engine is not'
    // locked during a poll, so new activity can occur (not only wakes but also new tasks being added).
    inactive: VecDeque<TaskKey>,

    // These tasks have completed and we are waiting for the wake signals to become inert before we
    // can remove them from the slab.
    //
    // TODO: Well what if the wakers never wake up? There could be a "task deadlock" between two
    // threads, couldn't there? Or the waker is for something that happens 2 hours from now, or
    // even never because task processing has stopped? Well, dropping the tasks would presumably
    // help but we cannot drop tasks until their wakers are inert! Ah but can we hollow out the
    // tasks and just drop the erased task and any captured state (==wakers) that it holds?
    completed: VecDeque<TaskKey>,
}

impl AsyncTaskEngine {
    pub fn new() -> Self {
        Self {
            tasks: PinnedSlabChain::new(),
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
        let inserter = self.tasks.begin_insert();
        let key = inserter.index();
        inserter.insert(task);

        self.active.push_back(key);
    }

    pub fn execute_cycle(&mut self) -> CycleResult {
        // If we have no activity in the cycle, we indicate that the caller does not need to
        // immediately call us again and may suspend until it has a reason to call us again.
        let mut had_activity = false;

        // There may be tasks that just prior woke up due to I/O activity, so go through the tasks
        // to activate any we need to activate, ensuring they get processed immediately.
        if self.activate_awakened_tasks() {
            had_activity = true;
        }

        while let Some(key) = self.active.pop_front() {
            had_activity = true;

            let task = self.tasks.get(key);

            // SAFETY: TODO - how do we ensure we keep the task alive until the waker is inert?
            match unsafe { task.poll() } {
                task::Poll::Ready(()) => {
                    self.completed.push_back(key);
                }
                task::Poll::Pending => {
                    self.inactive.push_back(key);
                }
            }
        }

        // It may be that some activity within our cycle awakened some tasks, so there may be
        // more work to do immediately - if so, we ask the caller to continue. We skip this check
        // if we already know that we need to come back again (as we activate above, as well).
        if !had_activity && self.activate_awakened_tasks() {
            had_activity = true;
        }

        self.drop_completed_tasks();

        #[cfg(test)]
        self.tasks.integrity_check();

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

            let task = self.tasks.get(*key);

            if task.wake_signal.consume_awakened() {
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
            let task = self.tasks.get(*key);

            let is_inert = task.wake_signal.is_inert();

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

#[pin_project]
struct Task {
    erased_task: RefCell<LocalErasedTask>,

    #[pin]
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
    /// Once you call this, the task must not be dropped until the wake signal indicates that it
    /// has become inert.
    unsafe fn poll(self: Pin<&Self>) -> task::Poll<()> {
        let waker = self.project_ref().wake_signal.waker();

        let mut context = task::Context::from_waker(waker);

        // We are only accessing the erased task in poll() which is only called by the current
        // thread and never recursively, so we are not at risk of conflicting borrows.
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
