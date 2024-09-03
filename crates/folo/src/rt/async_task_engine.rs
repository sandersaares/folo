use crate::rt::LocalErasedTask;
use negative_impl::negative_impl;
use slab::Slab;
use std::{
    cell::{OnceCell, RefCell},
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
    tasks: Slab<Pin<Arc<Task>>>,
}

impl AsyncTaskEngine {
    pub fn new() -> Self {
        Self { tasks: Slab::new() }
    }

    /// Enqueues a future whose return type has been erased. It will be polled but no result
    /// will be made available by the async task engine - it is expected that some other mechanism
    /// is used to observe the result.
    pub fn enqueue_erased(&mut self, erased_task: LocalErasedTask) {
        self.tasks.insert(Arc::pin(Task::new(erased_task)));
    }

    pub fn execute_cycle(&mut self) -> CycleResult {
        // If we have no activity in the cycle, we indicate that we should suspend.
        // In the future we should refactor this to a more direct signaling system.
        let mut had_activity = false;

        self.tasks.retain(|_, task| {
            if !task.consume_awakened() {
                // Not active, let it lie.
                return true;
            }

            had_activity = true;

            match task.poll() {
                task::Poll::Ready(()) => false,
                task::Poll::Pending => true,
            }
        });

        if had_activity {
            CycleResult::Continue
        } else {
            CycleResult::Suspend
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
}

struct Task {
    erased_task: RefCell<LocalErasedTask>,
    active: AtomicBool,
    waker: OnceCell<Waker>,
}

impl Task {
    fn new(erased_task: LocalErasedTask) -> Self {
        Self {
            erased_task: RefCell::new(erased_task),
            active: AtomicBool::new(true),
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
        self.active.swap(false, Ordering::Relaxed)
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
        f.debug_struct("Task")
            .field("active", &self.active)
            .field("waker", &self.waker)
            .finish()
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
    task.active.store(true, Ordering::Relaxed);
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
    task.active.store(true, Ordering::Relaxed);
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
