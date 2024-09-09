use crate::{
    constants::GENERAL_LOW_PRECISION_SECONDS_BUCKETS,
    metrics::{Event, EventBuilder},
    rt::{erased_async_task::ErasedResultAsyncTask, waker::WakeSignal},
    util::{LowPrecisionInstant, PinnedSlabChain},
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
///
/// # Shutdown
///
/// The engine must be shut down by the caller before being dropped or it will panic in drop().
///
/// A critical component of the shutdown process is the safe destruction of cross-references between
/// tasks, both remote and local. These can exist in different forms:
///
/// * A local task may hold a reference to the join handle of another local task.
/// * A local or remote task may hold a reference to the wake signal of a local task.
///
/// Some of these references may still be held by foreign threads or arbitrary entities outside the
/// Folo runtime who have chosen to poll the tasks we manage (e.g. in case of wakers). We cannot
/// control when these wakers are dropped, so we must keep alive the data structures until all
/// wakers have become inert. For other data structures, we can control it to some extent.
///
/// This means the shutdown process is divided into two phases:
///
/// 1. We stop processing work and call `clear()` on all local tasks to drop any state that may
///    potentially hold references to other local tasks. For example, join handles and wakers.
/// 2. We wait for all cross-referenceable data structures like wakers and erased tasks to become
///    inert, after which we drop them. Ideally, this is a small subset of the total tasks.
///
/// The second phase happens independently in the background after a `RuntimeClient::stop()` is
/// issued. Callers that call `RuntimeClient::wait()` will wait for the cleanup to complete, though.
///
/// # Safety
///
/// As we need to ensure a proper cleanup process for each task anyway and cannot just drop them,
/// we might as well directly reference them via pointers instead of using lookup keys and
/// repeatedly looking them up. They are pinned forever, so there is no possibility of them moving
/// in memory!
///
/// This means it is not safe to drop the engine while any tasks are still active. You must wait for
/// the `CycleResult::Shutdown` result before it is safe to drop the engine.
#[derive(Debug)]
pub struct AsyncTaskEngine {
    // We use a pinned slab here to allocate the tasks in-place and avoid allocation churn.
    tasks: PinnedSlabChain<Task>,

    // The active set contains all the tasks we want to poll. This is where all futures start.
    // The items are pinned pointers into the `tasks` collection.
    active: VecDeque<*mut Task>,

    // The inactive set contains all the tasks that are sleeping. We will move them back to the
    // active set after a waker notifies us that a future needs to wake up. Note that the wakeup
    // may arrive from within the poll itself, which implies that we need to consider a future part
    // of the inactive set immediately before polling it, and be ready to move it back to the active
    // set during/after the poll if a waker is signaled during a poll. Also implies engine is not'
    // locked during a poll, so new activity can occur (not only wakes but also new tasks being
    // added).
    // The items are pinned pointers into the `tasks` collection.
    inactive: VecDeque<*mut Task>,

    // These tasks have completed and we are waiting for the references to them to be dropped (for
    // the tasks to become inert) so we can finish releasing resources.
    // The items are pinned pointers into the `tasks` collection.
    completed: VecDeque<*mut Task>,

    // In shutdown mode, all tasks are considered completed and the only thing we do is wait for
    // them to become inert (which may be driven by uncontrollable actions of foreign threads).
    shutting_down: bool,

    // Used to report interval between cycles.
    last_cycle_ended: Option<LowPrecisionInstant>,
}

impl AsyncTaskEngine {
    /// # Safety
    ///
    /// You must receive the `CycleResult::Shutdown` result before it is safe to drop the engine.
    pub unsafe fn new() -> Self {
        Self {
            tasks: PinnedSlabChain::new(),
            active: VecDeque::new(),
            inactive: VecDeque::new(),
            completed: VecDeque::new(),
            shutting_down: false,
            last_cycle_ended: None,
        }
    }

    /// Enqueues a future whose return type has been erased. It will be polled but no result
    /// will be made available by the async task engine - it is expected that some other mechanism
    /// is used to observe the result.
    pub fn enqueue_erased(&mut self, erased_task: Pin<Box<dyn ErasedResultAsyncTask>>) {
        // It is possible due to the eventually consistent nature between worker commands that a
        // worker will receive a new task after shutdown has already begun. We expect the worker
        // to perform the necessary filtering to prevent that from ever reaching the task engine.
        assert!(
            !self.shutting_down,
            "cannot enqueue tasks after shutdown has begun"
        );

        let inserter = self.tasks.begin_insert();

        // SAFETY: We are responsible for not dropping the task until it is inert. We accomplish
        // this by only removing tasks after they pass through the `completed` list and indicate
        // that they have become inert.
        let task = unsafe { Task::new(inserter.index(), erased_task) };

        self.active.push_back(inserter.insert_raw(task));
    }

    pub fn execute_cycle(&mut self) -> CycleResult {
        let cycle_start = LowPrecisionInstant::now();

        if let Some(last_end) = self.last_cycle_ended {
            CYCLE_INTERVAL.with(|x| x.observe(cycle_start.duration_since(last_end).as_secs_f64()));
        }

        // If we have no activity in the cycle, we indicate that the caller does not need to
        // immediately call us again and may suspend until it has a reason to call us again.
        let mut had_activity = false;

        // There may be tasks that just prior woke up due to I/O activity, so go through the tasks
        // to activate any we need to activate, ensuring they get processed immediately.
        if self.activate_awakened_tasks() {
            had_activity = true;
        }

        let had_active_tasks = !self.active.is_empty();

        while let Some(task_ptr) = self.active.pop_front() {
            had_activity = true;

            // SAFETY: This comes from a pinned slab and we are responsible for dropping tasks, which
            // we never do until they progress through the lifecycle into the `completed` list.
            let task = unsafe { Pin::new_unchecked(&*task_ptr) };

            let poll_result =
                TASK_POLL_DURATION.with(|x| x.observe_duration_low_precision(|| task.poll()));

            match poll_result {
                task::Poll::Ready(()) => {
                    TASKS_COMPLETED.with(Event::observe_unit);
                    self.completed.push_back(task_ptr);
                }
                task::Poll::Pending => {
                    TASK_INACTIVATED.with(Event::observe_unit);
                    self.inactive.push_back(task_ptr);
                }
            }
        }

        // It may be that some activity within our cycle awakened some tasks, so there may be
        // more work to do immediately - if so, we ask the caller to continue. We skip this check
        // if we already know that we need to come back again or if there was no task activity.
        if !had_activity && had_active_tasks && self.activate_awakened_tasks() {
            had_activity = true;
        }

        self.drop_inert_tasks();

        let cycle_end = LowPrecisionInstant::now();
        self.last_cycle_ended = Some(cycle_end);

        CYCLE_DURATION.with(|x| x.observe(cycle_end.duration_since(cycle_start).as_secs_f64()));

        if self.shutting_down && self.completed.is_empty() {
            // Shutdown is finished if all completed tasks (== all tasks) have been removed from the
            // completed list after their wakers became inert.
            CycleResult::Shutdown
        } else if had_activity {
            // We want to be immediately called again because we may have more work to do.
            CycleResult::Continue
        } else {
            // We have no work to do, feel free to take a while before coming back to us.
            CycleResult::Suspend
        }
    }

    // Moves any awakened tasks into the active set. Returns whether any tasks were moved.
    fn activate_awakened_tasks(&mut self) -> bool {
        let mut had_activity = false;

        let mut index = 0;

        while index < self.inactive.len() {
            let task_ptr = &self.inactive[index];

            // SAFETY: This comes from a pinned slab and we are responsible for dropping tasks, which
            // we never do until they progress through the lifecycle into the `completed` list.
            let task = unsafe { Pin::new_unchecked(&**task_ptr) };

            if task.wake_signal.consume_awakened() {
                had_activity = true;

                TASK_ACTIVATED.with(Event::observe_unit);

                let task_ptr = self
                    .inactive
                    .remove(index)
                    .expect("key must exist - we just got it from the same data structure");

                self.active.push_back(task_ptr);
            } else {
                index += 1;
            }
        }

        had_activity
    }

    fn drop_inert_tasks(&mut self) {
        self.completed.retain(|task_ptr| {
            // SAFETY: This comes from a pinned slab and we are responsible for dropping tasks, which
            // we never do until they progress through the lifecycle into the `completed` list.
            let task = unsafe { Pin::new_unchecked(&**task_ptr) };

            let is_inert = task.is_inert();

            if is_inert {
                TASKS_DROPPED.with(Event::observe_unit);
                self.tasks.remove(task.index);
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

        // We call `clear()` on all tasks that we are canceling. This will drop the maximum amount
        // of internal state such as any captured variables that may be holding on to join handles
        // and/or wakers, making it possible to start dropping the tasks. Not all tasks become inert
        // because of this - there may also be callers on other threads holding on to our wakers but
        // as shutdown happens simultaneously on all threads, we know they will also soon be
        // canceled and permit us to release resources.

        // We call .count() to force the iterator to be evaluated. We do not care about the count.
        _ = self
            .active
            .drain(..)
            .chain(self.inactive.drain(..))
            .map(|task_ptr| {
                // SAFETY: This comes from a pinned slab and we are responsible for dropping tasks, which
                // we never do until they progress through the lifecycle into the `completed` list.
                let task = unsafe { Pin::new_unchecked(&*task_ptr) };

                task.project_ref().inner.borrow().clear();

                self.completed.push_back(task_ptr);
            })
            .count();
    }
}

impl Drop for AsyncTaskEngine {
    fn drop(&mut self) {
        assert!(
            self.shutting_down,
            "async task engine dropped without safe shutdown process"
        );

        // We must ensure that all tasks are completed before we drop the engine. This is a safety
        // requirement of the engine - if it is not inert, we are violating memory safety.
        assert!(
            self.tasks.is_empty(),
            "async task engine dropped while some tasks still exist"
        );
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
    // Behind this may be either a local or a remote task - we do not know or care which.
    inner: RefCell<Pin<Box<dyn ErasedResultAsyncTask>>>,

    // Used for dropping the task once we are done with it.
    index: usize,

    #[pin]
    wake_signal: WakeSignal,
}

impl Task {
    /// # Safety
    ///
    /// The task must not be dropped until it is inert.
    unsafe fn new(index: usize, inner: Pin<Box<dyn ErasedResultAsyncTask>>) -> Self {
        Self {
            inner: RefCell::new(inner),
            index,
            wake_signal: WakeSignal::new(),
        }
    }

    fn poll(self: Pin<&Self>) -> task::Poll<()> {
        // SAFETY: We rely on the caller not to drop the task until it signals it is inert.
        let waker = unsafe { self.project_ref().wake_signal.waker() };

        let mut context = task::Context::from_waker(waker);

        // We are only accessing the erased task in poll() which is only called by the current
        // thread and never recursively, so we are not at risk of conflicting borrows.
        self.inner.borrow_mut().as_mut().poll(&mut context)
    }

    fn is_inert(&self) -> bool {
        self.wake_signal.is_inert() && self.inner.borrow().is_inert()
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

    static CYCLE_INTERVAL: Event = EventBuilder::new()
        .name("rt_async_cycle_interval_seconds")
        .buckets(GENERAL_LOW_PRECISION_SECONDS_BUCKETS)
        .build()
        .unwrap();

    static CYCLE_DURATION: Event = EventBuilder::new()
        .name("rt_async_cycle_duration_seconds")
        .buckets(GENERAL_LOW_PRECISION_SECONDS_BUCKETS)
        .build()
        .unwrap();

    static TASK_POLL_DURATION: Event = EventBuilder::new()
        .name("rt_async_task_poll_duration_seconds")
        .buckets(GENERAL_LOW_PRECISION_SECONDS_BUCKETS)
        .build()
        .unwrap();
}
