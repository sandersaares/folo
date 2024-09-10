use crate::{
    constants::{GENERAL_LOW_PRECISION_SECONDS_BUCKETS, POISONED_LOCK},
    metrics::{Event, EventBuilder},
    rt::{erased_async_task::ErasedResultAsyncTask, waker::WakeSignal},
    util::{LowPrecisionInstant, PinnedSlabChain},
};
use negative_impl::negative_impl;
use pin_project::pin_project;
use std::{
    cell::RefCell,
    collections::{HashSet, VecDeque},
    fmt::{self, Debug, Formatter},
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
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
    active: HashSet<*mut Task>,

    // The inactive set contains all the tasks that are sleeping. We will move them back to the
    // active set after a waker notifies us that a future needs to wake up. Note that the wakeup
    // may arrive from within the poll itself, which implies that we need to consider a future part
    // of the inactive set immediately before polling it, and be ready to move it back to the active
    // set during/after the poll if a waker is signaled during a poll. Also implies engine is not'
    // locked during a poll, so new activity can occur (not only wakes but also new tasks being
    // added).
    // The items are pinned pointers into the `tasks` collection.
    inactive: HashSet<*mut Task>,

    // The primary mechanism used to signal that a task has awoken and needs to be moved from the
    // inactive queue to the active queue. We ONLY add entries to this list if we can do so without
    // waiting on the lock, to minimize time we spend blocked on cross-thread synchronization. We
    // also only add entries if we do not need to increase the capacity, to avoid allocating the new
    // data structure on a different thread from the consuming thread (and therefore potentially in
    // a different memory region, which would lead to inefficiency). If an entry cannot be added to
    // this queue for any reason, the probe_embedded_wake_signals is set instead and the next cycle
    // of the engine will probe the awakened status of every inactive task to synchronize statuses.
    awakened: Arc<Mutex<HashSet<*mut Task>>>,

    // When a waker cannot lock the `awakened` queue or when the queue is full, it will set this
    // flag to indicate that the awakened status of every inactive task should be directly probed.
    probe_embedded_wake_signals: Arc<AtomicBool>,

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

const AWAKENED_CAPACITY: usize = 1024;

impl AsyncTaskEngine {
    /// # Safety
    ///
    /// You must receive the `CycleResult::Shutdown` result before it is safe to drop the engine.
    pub unsafe fn new() -> Self {
        Self {
            tasks: PinnedSlabChain::new(),
            active: HashSet::new(),
            inactive: HashSet::new(),
            awakened: Arc::new(Mutex::new(HashSet::with_capacity(AWAKENED_CAPACITY))),
            probe_embedded_wake_signals: Arc::new(AtomicBool::new(false)),
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
        // that they have become inert. We must also initialize the task with ::initialize() before
        // it is used.
        let task = unsafe {
            Task::new(
                inserter.index(),
                erased_task,
                Arc::clone(&self.awakened),
                Arc::clone(&self.probe_embedded_wake_signals),
            )
        };

        let task_ptr = inserter.insert_raw(task);

        // We must initialize it once pinned, to set up the self-referential pointer.
        // SAFETY: We know it is pinned because all tasks are always pinned once in `self.tasks`.
        let task_pin = unsafe { Pin::new_unchecked(&mut *task_ptr) };
        task_pin.initialize();

        self.active.insert(task_ptr);
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

        while let Some(task_ptr) = self.active.iter().copied().next() {
            self.active.remove(&task_ptr);
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
                    self.inactive.insert(task_ptr);
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

        // There are two ways to activate tasks:
        // 1. by probing the embedded wake signals.
        // 2. by receiving an explicit wake signal via the `awakened` set.
        //
        // Note that the same task may be awakened via both channels simultaneously, and that
        // explicit wake signals may be sent when the task is already active (the signal
        // may come from some caller who has no idea if it is already awake or not).

        {
            // Hard lock here - hopefully any competing threads do not hold it too long.
            // 99% of notifications will be from the same thread, so this should be low cost.
            let mut awakened = self.awakened.lock().expect(POISONED_LOCK);

            // We copy here so the loop does not keep a reference to the awakened set.
            while let Some(task_ptr) = awakened.iter().copied().next() {
                awakened.remove(&task_ptr);

                // It is theoretically possible for a completed task to be awakened, in which case
                // we do nothing. We detect this by ensuring that the task was in the "inactive" set
                // before we react to the wake notification. This also eliminates spurious wakes.
                if self.inactive.remove(&task_ptr) {
                    self.active.insert(task_ptr);

                    had_activity = true;
                    TASK_ACTIVATED_VIA_SET.with(Event::observe_unit);
                } else {
                    TASK_ACTIVATED_SPURIOUS.with(Event::observe_unit);
                }
            }
        }

        if self
            .probe_embedded_wake_signals
            .swap(false, Ordering::Acquire)
        {
            self.inactive.retain(|task_ptr| {
                // SAFETY: This comes from a pinned slab and we are responsible for dropping tasks, which
                // we never do until they progress through the lifecycle into the `completed` list.
                let task = unsafe { Pin::new_unchecked(&**task_ptr) };

                if task.wake_signal.consume_awakened() {
                    had_activity = true;

                    TASK_ACTIVATED_VIA_SIGNAL.with(Event::observe_unit);
                    self.active.insert(*task_ptr);
                    false
                } else {
                    true
                }
            });
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
            .drain()
            .chain(self.inactive.drain())
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
pub(super) struct Task {
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
    /// The task must be initialized via ::initialize() once it has been pinned.
    /// The task must not be dropped until it is inert.
    unsafe fn new(
        index: usize,
        inner: Pin<Box<dyn ErasedResultAsyncTask>>,
        awakened_set: Arc<Mutex<HashSet<*mut Task>>>,
        probe_embedded_wake_signals: Arc<AtomicBool>,
    ) -> Self {
        Self {
            inner: RefCell::new(inner),
            index,
            wake_signal: WakeSignal::new(awakened_set, probe_embedded_wake_signals),
        }
    }

    /// The task is self-referential, so must be initialized once pinned.
    fn initialize(self: Pin<&mut Self>) {
        // SAFETY: We are not unpinning anything here, just writing some harmless pointers.
        let self_mut: &mut Self = unsafe { Pin::into_inner_unchecked(self) };
        let self_ptr = self_mut as *mut _;

        self_mut.wake_signal.set_task_ptr(self_ptr);
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

    static TASK_ACTIVATED_VIA_SET: Event = EventBuilder::new()
        .name("rt_async_task_activated_via_set")
        .build()
        .unwrap();

    static TASK_ACTIVATED_VIA_SIGNAL: Event = EventBuilder::new()
        .name("rt_async_task_activated_via_signal")
        .build()
        .unwrap();

    static TASK_ACTIVATED_SPURIOUS: Event = EventBuilder::new()
        .name("rt_async_task_activated_spurious")
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
