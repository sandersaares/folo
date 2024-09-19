use crate::{
    collections::BuildPointerHasher,
    constants::{GENERAL_MILLISECONDS_BUCKETS, POISONED_LOCK},
    io::IO_DEQUEUE_BATCH_SIZE,
    mem::{DropPolicy, PinnedSlabChain},
    metrics::{Event, EventBuilder},
    rt::{erased_async_task::ErasedResultAsyncTask, waker::WakeSignal},
    time::LowPrecisionInstant,
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
    //
    // This is a VedDeque because we do not require set characteristics and a deque is faster.
    active: VecDeque<*mut Task>,

    // The inactive set contains all the tasks that are sleeping. We will move them back to the
    // active set after a waker notifies us that a future needs to wake up. Note that the wakeup
    // may arrive from within the poll itself, which implies that we need to consider a future part
    // of the inactive set immediately before polling it, and be ready to move it back to the active
    // set during/after the poll if a waker is signaled during a poll. Also implies engine is not'
    // locked during a poll, so new activity can occur (not only wakes but also new tasks being
    // added).
    // The items are pinned pointers into the `tasks` collection.
    inactive: HashSet<*mut Task, BuildPointerHasher>,

    // The primary mechanism used to signal that a task has awoken and needs to be moved from the
    // inactive queue to the active queue. We ONLY add entries to this list if we can do so without
    // waiting on the lock, to minimize time we spend blocked on cross-thread synchronization. We
    // also only add entries if we do not need to increase the capacity, to avoid allocating the new
    // data structure on a different thread from the consuming thread (and therefore potentially in
    // a different memory region, which would lead to inefficiency). If an entry cannot be added to
    // this queue for any reason, the probe_embedded_wake_signals is set instead and the next cycle
    // of the engine will probe the awakened status of every inactive task to synchronize statuses.
    //
    // This is a HashSet because we need to be able to preallocate the capacity (insertions are
    // always allocation-free because they may come from a different thread, so we cannot allocate).
    awakened: Arc<Mutex<VecDeque<*mut Task>>>,

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

// We prefer to get wakeup notifications via the "awakened" queue. This may not always be possible
// because the queue may be full or it may be locked (if the wakeup is coming from another thread).
//
// It is just a VecDeque so the size does not significantly impact wakeup processing time. The
// current size is chosen to accept "all reasonable" wakeups without needing to allocate, by
// accounting for the possibility that we have a huge batch of IO completions + some random wakes.
const AWAKENED_CAPACITY: usize = IO_DEQUEUE_BATCH_SIZE + 100;

impl AsyncTaskEngine {
    /// # Safety
    ///
    /// You must receive the `CycleResult::Shutdown` result before it is safe to drop the engine.
    pub unsafe fn new() -> Self {
        Self {
            // We use MustNotDropItems because the tasks contain elements referenced via raw
            // pointers (e.g. the wake signal) which means their lifetime must be carefully managed.
            // If items are still in the tasks list when the engine is dropped, this indicates that
            // proper cleanup did not happen and other threads may still hold dangling pointers.
            tasks: PinnedSlabChain::new(DropPolicy::MustNotDropItems),
            active: VecDeque::new(),
            inactive: HashSet::with_hasher(BuildPointerHasher::default()),
            #[allow(clippy::arc_with_non_send_sync)] // Clippy false positive? That's a big fat mutex!
            awakened: Arc::new(Mutex::new(VecDeque::with_capacity(AWAKENED_CAPACITY))),
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

        self.active.push_back(task_ptr);
    }

    pub fn execute_cycle(&mut self) -> CycleResult {
        let cycle_start = LowPrecisionInstant::now();

        if let Some(last_end) = self.last_cycle_ended {
            CYCLE_INTERVAL.with(|x| x.observe_millis(cycle_start.duration_since(last_end)));
        }

        // This is the moment we wake up any tasks that were signaled to wake up. The signals may
        // have been due to things like:
        //
        // 1. A waker was signaled from a different thread because some remote result is ready.
        // 2. A waker was signaled from the current thread because some local result is ready.
        // 3. I/O operation completed and the result is ready for processing.
        //
        // We do not really care why/how the wake signal was sent - same handling for all cases.
        self.activate_awakened_tasks();

        while let Some(task_ptr) = self.active.pop_front() {
            // SAFETY: This comes from a pinned slab and we are responsible for dropping tasks, which
            // we never do until they progress through the lifecycle into the `completed` list.
            let task = unsafe { Pin::new_unchecked(&*task_ptr) };

            let poll_result = task.poll();

            match poll_result {
                task::Poll::Ready(()) => {
                    TASKS_COMPLETED.with(Event::observe_unit);

                    // This ensures that any state held by the task is dropped. Most importantly, it
                    // may be holding a clone of a waker, which could create a circular reference
                    // that prevents the task from ever being cleaned up. This should help.
                    task.project_ref().inner.borrow().clear();

                    self.completed.push_back(task_ptr);
                }
                task::Poll::Pending => {
                    TASK_INACTIVATED.with(Event::observe_unit);
                    self.inactive.insert(task_ptr);
                }
            }
        }

        self.drop_inert_tasks();

        let cycle_end = LowPrecisionInstant::now();
        self.last_cycle_ended = Some(cycle_end);

        CYCLE_DURATION.with(|x| x.observe_millis(cycle_end.duration_since(cycle_start)));

        if self.shutting_down && self.completed.is_empty() {
            // Shutdown is finished if all completed tasks (== all tasks) have been removed from the
            // completed list after their wakers became inert.
            CycleResult::Shutdown
        } else if self.has_work_to_do() {
            // We want to be immediately called again because we may have more work to do.
            CycleResult::Continue
        } else {
            // We have no work to do, feel free to take a while before coming back to us.
            CycleResult::Suspend
        }
    }

    /// Returns whether there is any work to do in the engine. This is used to determine if the
    /// engine should be polled again immediately or if it should be suspended until new work
    /// arrives.
    fn has_work_to_do(&self) -> bool {
        // Work for us means either a) some task is active; b) a wakeup signal has been received.
        !self.active.is_empty()
            || !self.awakened.lock().expect(POISONED_LOCK).is_empty()
            || self.probe_embedded_wake_signals.load(Ordering::Relaxed)
    }

    // Moves any awakened tasks into the active set. Returns whether any tasks were moved.
    fn activate_awakened_tasks(&mut self) {
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
            while let Some(task_ptr) = awakened.pop_front() {
                // It is theoretically possible for a completed task to be awakened, in which case
                // we do nothing. We detect this by ensuring that the task was in the "inactive" set
                // before we react to the wake notification. This also eliminates spurious wakes.
                if self.inactive.remove(&task_ptr) {
                    self.active.push_back(task_ptr);

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
                    TASK_ACTIVATED_VIA_SIGNAL.with(Event::observe_unit);
                    self.active.push_back(*task_ptr);
                    false
                } else {
                    true
                }
            });
        }
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
            .with(|x| x.observe((self.active.len() + self.inactive.len()) as i64));

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
        awakened_queue: Arc<Mutex<VecDeque<*mut Task>>>,
        probe_embedded_wake_signals: Arc<AtomicBool>,
    ) -> Self {
        Self {
            inner: RefCell::new(inner),
            index,
            wake_signal: WakeSignal::new(awakened_queue, probe_embedded_wake_signals),
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
    static TASKS_CANCELED_ON_SHUTDOWN: Event = EventBuilder::new("rt_async_tasks_canceled_on_shutdown")
        .build();

    static TASK_ACTIVATED_VIA_SET: Event = EventBuilder::new("rt_async_task_activated_via_set")
        .build();

    static TASK_ACTIVATED_VIA_SIGNAL: Event = EventBuilder::new("rt_async_task_activated_via_signal")
        .build();

    static TASK_ACTIVATED_SPURIOUS: Event = EventBuilder::new("rt_async_task_activated_spurious")
        .build();

    static TASK_INACTIVATED: Event = EventBuilder::new("rt_async_task_inactivated")
        .build();

    static TASKS_COMPLETED: Event = EventBuilder::new("rt_async_tasks_completed")
        .build();

    static TASKS_DROPPED: Event = EventBuilder::new("rt_async_tasks_dropped")
        .build();

    static CYCLE_INTERVAL: Event = EventBuilder::new("rt_async_cycle_interval_millis")
        .buckets(GENERAL_MILLISECONDS_BUCKETS)
        .build();

    static CYCLE_DURATION: Event = EventBuilder::new("rt_async_cycle_duration_millis")
        .buckets(GENERAL_MILLISECONDS_BUCKETS)
        .build();
}
