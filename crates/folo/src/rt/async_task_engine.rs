use crate::rt::LocalErasedTask;
use negative_impl::negative_impl;
use std::{
    cell::{OnceCell, RefCell},
    collections::VecDeque,
    fmt::{self, Debug, Formatter},
    mem,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Weak,
    },
    task::{self, RawWaker, Waker},
};

/// The engine incrementally executes async tasks on a single thread when polled. It is not active
/// on its own and requires an external actor to poll it to make progress.
#[derive(Debug)]
pub struct AsyncTaskEngine {
    // The active set contains all the futures we want to poll. This is where all futures start.
    active: VecDeque<Pin<Arc<Task>>>,

    // The inactive set contains all the futures that are sleeping. We will move them back to the
    // active set once a waker notifies us that a future needs to wake up. Note that the wakeup
    // may arrive from within the poll itself, which implies that we need to consider a future part
    // of the inactive set immediately before polling it, and be ready to move it back to the active
    // set during the poll if a waker is signaled during a poll. Also implies engine is not locked
    // during a poll, so new activity can occur (not only wakes but also new tasks being added).
    inactive: VecDeque<Pin<Arc<Task>>>,
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
        self.active.push_back(Arc::pin(Task::new(erased_task)));
    }

    pub fn execute_cycle(&mut self) -> CycleResult {
        // If we have no activity in the cycle, we indicate that we should suspend.
        // In the future we should refactor this to a more direct signaling system.
        let mut had_activity = false;

        while let Some(task) = self.active.pop_front() {
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

            if task.consume_awakened() {
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
    erased_task: RefCell<LocalErasedTask>,
    awakened: AtomicBool,
    waker: OnceCell<Waker>,
}

impl Task {
    fn new(erased_task: LocalErasedTask) -> Self {
        Self {
            erased_task: RefCell::new(erased_task),
            awakened: AtomicBool::new(false),
            waker: OnceCell::new(),
        }
    }

    fn poll(self: &Pin<Arc<Self>>) -> task::Poll<()> {
        let mut context = task::Context::from_waker(self.waker());

        // SAFETY: We are only mut-borrowing in poll() which is only called by the current thread
        // and never recursively.
        self.erased_task.borrow_mut().as_mut().poll(&mut context)
    }

    fn waker<'a>(self: &'a Pin<Arc<Self>>) -> &'a Waker {
        self.waker.get_or_init(|| {
            // SAFETY: ???
            unsafe { Waker::from_raw(self.raw_waker()) }
        })
    }

    fn consume_awakened(&self) -> bool {
        self.awakened.swap(false, Ordering::Relaxed)
    }

    fn raw_waker(self: &Pin<Arc<Self>>) -> RawWaker {
        // SAFETY: We are not moving anything, simply extracting a pointer to the pinned value.
        let inner = unsafe { Pin::into_inner_unchecked(self.clone()) };
        let weak = Arc::downgrade(&inner);
        let ptr_to_weak = weak.into_raw();

        RawWaker::new(ptr_to_weak as *const (), &TASK_WAKER_VTABLE)
    }
}

impl Debug for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Task").field("waker", &self.waker).finish()
    }
}

static TASK_WAKER_VTABLE: task::RawWakerVTable = task::RawWakerVTable::new(
    waker_clone_waker,
    waker_wake,
    waker_wake_by_ref,
    waker_drop_waker,
);

fn waker_clone_waker(ptr: *const ()) -> RawWaker {
    let task = unsafe { resurrect_waker_ptr(ptr) };

    // The existing one we resurrected is still in use! By default if we leave it dangling here it
    // will be dropped and decrement the ref count, so leak a clone to account for the one we keep.
    mem::forget(task.clone());

    let ptr = task.into_raw();
    RawWaker::new(ptr as *const (), &TASK_WAKER_VTABLE)
}

fn waker_wake(ptr: *const ()) {
    let task = unsafe { resurrect_waker_ptr(ptr) };

    // This one consumes the waker - after we have done our thing, we drop the pointer to clean up.

    let Some(task) = task.upgrade() else {
        // The task was dropped already - nothing to wake up.
        return;
    };
    task.awakened.store(true, Ordering::Relaxed);
}

fn waker_wake_by_ref(ptr: *const ()) {
    let task = unsafe { resurrect_waker_ptr(ptr) };

    // The existing one we resurrected is still in use! By default if we leave it dangling here it
    // will be dropped and decrement the ref count, so leak a clone to account for the one we keep.
    mem::forget(task.clone());

    let Some(task) = task.upgrade() else {
        // The task was dropped already - nothing to wake up.
        return;
    };
    task.awakened.store(true, Ordering::Relaxed);
}

fn waker_drop_waker(ptr: *const ()) {
    // Resurrect it and immediately drop it to let the ref count decrement.
    unsafe { resurrect_waker_ptr(ptr) };
}

/// # Safety
///
/// Do not move the value - it is pinned (if it still exists).
unsafe fn resurrect_waker_ptr(ptr: *const ()) -> Weak<Task> {
    Weak::from_raw(ptr as *const Task)
}
