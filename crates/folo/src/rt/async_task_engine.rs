use crate::{
    metrics::{Event, EventBuilder},
    rt::{waker::WakeSignal, LocalErasedAsyncTask},
    util::PinnedSlabChain,
};
use negative_impl::negative_impl;
use pin_project::pin_project;
use std::{
    cell::RefCell,
    collections::VecDeque,
    fmt::{self, Debug, Formatter},
    future,
    pin::Pin,
    task,
};

type TaskKey = usize;

/// The engine incrementally executes async tasks on a single thread when polled. It is not active
/// on its own and requires an external actor to poll it to make progress.
///
/// # Shutdown
///
/// The engine must be shut down by the caller before being dropped or it will panic in drop().
///
/// A critical component of the shutdown process is the safe destruction of the wakers, some of
/// which may still be held by foreign threads or arbitrary entities outside the Folo runtime who
/// have chosen to poll the tasks we manage. We cannot control when these wakers are dropped, so
/// we must keep alive the data structures until all wakers have become inert.
///
/// This means the shutdown process is divided into two phases:
///
/// 1. We stop processing work and drop any state that we can drop (e.g. we drop futures that we
///    will never poll again, so any data they reference gets dropped ASAP).
/// 2. We wait for all wakers to become inert, after which we drop the data structures required
///    by the wakers (which is hopefully a small subset).
///
/// The second phase happens in the background - publicly the Folo runtime is considered shut down
/// already when the first phase completes. The second phase is merely a delayed memory release
/// while the worker thread sticks around to clean up after itself and should have no functional
/// side-effects.
#[derive(Debug)]
pub struct AsyncTaskEngine {
    // We use a pinned slab here to allocate the tasks in-place and avoid allocation churn.
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
    // can remove them from the slab. During engine execution, we regularly perform this cleanup,
    // although we do not bother with it in the shutdown phase because we will drop the whole
    // container at the end anyway.
    completed: VecDeque<TaskKey>,

    // In shutdown mode, all tasks are considered completed and the only thing we do is wait for
    // wakers to become inert (which may be driven by uncontrollable actions of foreign threads).
    shutting_down: bool,
}

impl AsyncTaskEngine {
    pub fn new() -> Self {
        Self {
            tasks: PinnedSlabChain::new(),
            active: VecDeque::new(),
            inactive: VecDeque::new(),
            completed: VecDeque::new(),
            shutting_down: false,
        }
    }

    /// Enqueues a future whose return type has been erased. It will be polled but no result
    /// will be made available by the async task engine - it is expected that some other mechanism
    /// is used to observe the result.
    pub fn enqueue_erased(&mut self, erased_task: LocalErasedAsyncTask) {
        // It is possible due to the eventually consistent nature between worker commands that a
        // worker will receive a new task after shutdown has already begun. We expect the worker
        // to perform the necessary filtering to prevent that from ever reaching the task engine.
        assert!(
            !self.shutting_down,
            "cannot enqueue tasks after shutdown has begun"
        );

        let task = Task::new(erased_task);

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

            TASK_POLLED.with(Event::observe_unit);

            // SAFETY: After this, we are required to keep the task alive until the wakers are all
            // inert. We do this in the async task agent by keeping the thread alive until all
            // wakers have become inert.
            match unsafe { task.poll() } {
                task::Poll::Ready(()) => {
                    TASKS_COMPLETED.with(Event::observe_unit);
                    self.completed.push_back(key);
                }
                task::Poll::Pending => {
                    TASK_INACTIVATED.with(Event::observe_unit);
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

        if self.shutting_down && self.completed.is_empty() {
            // Shutdown is finished if all completed tasks (== all tasks) have been removed from the
            // completed list after their wakers became inert.
            CycleResult::Shutdown
        } else if had_activity {
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

                TASK_ACTIVATED.with(Event::observe_unit);

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
                TASKS_DROPPED.with(Event::observe_unit);
                self.tasks.remove(*key);
            }

            !is_inert
        });
    }

    /// Enters shutdown mode. No new tasks can be enqueued and all existing tasks are considered
    /// completed. We will only wait for wakers to become inert, no other activity will occur now.
    pub fn begin_shutdown(&mut self) {
        assert!(
            !self.shutting_down,
            "begin_shutdown() called twice on the same engine"
        );

        self.shutting_down = true;

        // All tasks are considered completed - we never poll them again.
        TASKS_CANCELED_ON_SHUTDOWN
            .with(|x| x.observe((self.active.len() + self.inactive.len()) as f64));

        self.completed.append(&mut self.active);
        self.completed.append(&mut self.inactive);

        // We drop the futures that are not yet completed. This eager cleanup is critical because
        // these futures themselves may hold some of the wakers we want to make inert, creating a
        // reference cycle. We must drop all futures (on all worker threads) to break the cycles.
        // We implement this by replacing all the futures with an already-completed future (to
        // avoid having to check for "empty" tasks on every iteration).
        for key in &self.completed {
            let task = self.tasks.get(*key);
            _ = task.erased_task.replace(Box::pin(future::ready(())));
        }
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

    /// The engine has completed shutdown and is ready to be dropped.
    Shutdown,
}

#[pin_project]
struct Task {
    erased_task: RefCell<LocalErasedAsyncTask>,

    #[pin]
    wake_signal: WakeSignal,
}

impl Task {
    fn new(erased_task: LocalErasedAsyncTask) -> Self {
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

thread_local! {
    static TASKS_CANCELED_ON_SHUTDOWN: Event = EventBuilder::new()
        .name("rt_async_tasks_canceled_on_shutdown")
        .build()
        .unwrap();

    static TASK_POLLED: Event = EventBuilder::new()
        .name("rt_async_task_polled")
        .build()
        .unwrap();

    static TASK_ACTIVATED: Event = EventBuilder::new()
        .name("rt_async_task_activated")
        .build()
        .unwrap();

    static TASK_INACTIVATED: Event = EventBuilder::new()
        .name("rt_async_task_inactivated")
        .build()
        .unwrap();

    static TASKS_COMPLETED: Event = EventBuilder::new()
        .name("rt_async_tasks_completed")
        .build()
        .unwrap();

    static TASKS_DROPPED: Event = EventBuilder::new()
        .name("rt_async_tasks_dropped")
        .build()
        .unwrap();
}
