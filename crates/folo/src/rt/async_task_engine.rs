use crate::rt::LocalErasedTask;
use negative_impl::negative_impl;
use std::{
    collections::{BTreeSet, VecDeque},
    fmt::{self, Debug, Formatter},
    sync::{Arc, Mutex},
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

    // The awakened set contains all the tasks that have been woken up and need to be moved back to
    // the active set. Note that our wakers may be called from any thread, so this must be guarded.
    //
    // TODO: Ewww, a set implies allocations from foreign thread are permissible (because wakers
    // are thread-safe, so we may be woken up by who knows what). We need an alloc-free mechanism.
    // We probably need to make our tasks Arc instead of Box, so awakening becomes updating a flag.
    awakened: Arc<Mutex<BTreeSet<usize>>>,

    next_task_id: usize,
}

impl AsyncTaskEngine {
    pub fn new() -> Self {
        Self {
            active: VecDeque::new(),
            inactive: VecDeque::new(),
            awakened: Arc::new(Mutex::new(BTreeSet::new())),
            next_task_id: 0,
        }
    }

    /// Enqueues a future whose return type has been erased. It will be polled but no result
    /// will be made available by the async task engine - it is expected that some other mechanism
    /// is used to observe the result.
    pub fn enqueue_erased(&mut self, erased_task: LocalErasedTask) {
        let task = Task::new(
            self.next_task_id,
            erased_task,
            Arc::new(TaskWaker::new(
                self.next_task_id,
                Arc::clone(&self.awakened),
            )),
        );

        self.next_task_id += 1;

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

        let mut awakened = self.awakened.lock().unwrap();

        while let Some(task_id) = awakened.pop_first() {
            had_activity = true;

            awakened.remove(&task_id);

            let Some(index) = self.inactive.iter().position(|x| x.id == task_id) else {
                // There is no limit on the lifetime of a waker, so one may trigger wakeups
                // of tasks that no longer exist. Just ignore that if it happens.
                println!("Task {} not found in inactive set.", task_id);
                continue;
            };

            let task = self.inactive.remove(index).unwrap();
            self.active.push_back(task);
        }

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
    id: usize,
    erased_task: LocalErasedTask,
    waker: Arc<TaskWaker>,
}

impl Task {
    fn new(id: usize, erased_task: LocalErasedTask, waker: Arc<TaskWaker>) -> Self {
        Self {
            id,
            erased_task,
            waker,
        }
    }

    fn poll(&mut self) -> task::Poll<()> {
        // TODO: This allocation is gross. To be fixed later (requires unsafe code).
        let waker = self.waker.clone().into();
        let mut context = task::Context::from_waker(&waker);

        self.erased_task.as_mut().poll(&mut context)
    }
}

impl Debug for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Task")
            .field("id", &self.id)
            .field("waker", &self.waker)
            .finish()
    }
}

struct TaskWaker {
    task_id: usize,
    awakened: Arc<Mutex<BTreeSet<usize>>>,
}

impl Wake for TaskWaker {
    fn wake(self: Arc<Self>) {
        self.awakened.lock().unwrap().insert(self.task_id);
    }
}

impl TaskWaker {
    fn new(task_id: usize, awakened: Arc<Mutex<BTreeSet<usize>>>) -> Self {
        Self { task_id, awakened }
    }
}

impl Debug for TaskWaker {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("TaskWaker")
            .field("task_id", &self.task_id)
            .field("awakened", &self.awakened)
            .finish()
    }
}
