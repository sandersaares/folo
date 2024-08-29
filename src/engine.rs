use std::{
    cell::RefCell,
    collections::{BTreeSet, VecDeque},
    future::Future,
    mem,
    pin::Pin,
    rc::Rc,
    sync::{Arc, Mutex},
    task::{self, Wake, Waker},
};

// When we accept futures into the executor, we need to pin them before using them. Pinning
// implies that the future itself lives in some place that will not change, although we can
// shuffle around the `Pin` wrapper type itself freely. This suggests we cannot move around
// futures between our data structures - rather, the futures must be in place and we must
// work with pinned references to these futures. The future itself may be anything (it is just
// some type that implements Future), created by user code and typically not pinned before
// they reach the executor (though conceivably, one could imagine pre-pinned futures existing?).

// TODO: Should we allocate the boxed futures on some convenient slab to keep them together for
//   more cache friendliness when we are iterating through them all? This does seem to assume
//   enabling the allocator API (unstable feature).

/// The engine executes tasks on a single thread. It collaborates with other engines on other
/// threads that are part of the same executor.
pub struct Engine {
    // The active set contains all the futures we want to poll. This is where all futures start.
    active: VecDeque<Box<dyn Task>>,

    // The inactive set contains all the futures that are sleeping. We will move them back to the
    // active set once a waker notifies us that a future needs to wake up. Note that the wakeup
    // may arrive from within the poll itself, which implies that we need to consider a future part
    // of the inactive set immediately before polling it, and be ready to move it back to the active
    // set during the poll if a waker is signaled during a poll. Also implies executor is not locked
    // during a poll, so new activity can occur (not only wakes but also new tasks being added).
    inactive: VecDeque<Box<dyn Task>>,

    // The awakened set contains all the tasks that have been woken up and need to be moved back to
    // the active set. Note that our wakers may be called from any thread, so this must be guarded.
    //
    // TODO: Ewww, a set implies allocations from foreign thread are permissible. We need an alloc-
    // free awakening mechanism.
    awakened: Arc<Mutex<BTreeSet<usize>>>,

    next_task_id: usize,
}

impl Engine {
    pub fn new() -> Self {
        Self {
            active: VecDeque::new(),
            inactive: VecDeque::new(),
            awakened: Arc::new(Mutex::new(BTreeSet::new())),
            next_task_id: 0,
        }
    }

    pub fn enqueue<F, R>(&mut self, future: F) -> JoinHandle<R>
    where
        F: Future<Output = R> + 'static,
        R: 'static,
    {
        let task = MyTask::new(
            self.next_task_id,
            Box::pin(future),
            Arc::new(MyWaker::new(self.next_task_id, Arc::clone(&self.awakened))),
        );

        self.next_task_id += 1;

        let result_clone = Rc::clone(&task.result);

        self.active.push_back(Box::new(task));

        JoinHandle {
            result: result_clone,
        }
    }

    pub fn run(&mut self) {
        while !self.active.is_empty() || !self.inactive.is_empty() {
            if self.active.is_empty() {
                println!("No active tasks. Yielding to other threads.");
                std::thread::yield_now();
            }

            while let Some(mut task) = self.active.pop_front() {
                match task.poll() {
                    task::Poll::Ready(()) => {
                        println!("Future completed.");
                    }
                    task::Poll::Pending => {
                        println!("Future returned Pending. Will poll again after wakeup.");
                        self.inactive.push_back(task);
                    }
                }
            }

            println!("Ran out of active tasks. Looking for awakened tasks.");

            {
                let mut awakened = self.awakened.lock().unwrap();

                while let Some(task_id) = awakened.pop_first() {
                    println!("Found awakened task {}.", task_id);
                    awakened.remove(&task_id);

                    let Some(index) = self.inactive.iter().position(|x| x.id() == task_id) else {
                        // There is no limit on the lifetime of a waker, so one may trigger wakeups
                        // of tasks that no longer exist. Just ignore that if it happens.
                        println!("Task {} not found in inactive set.", task_id);
                        continue;
                    };

                    let task = self.inactive.remove(index).unwrap();
                    self.active.push_back(task);
                }
            }
        }

        println!("All futures completed.");
    }
}

impl Default for Engine {
    fn default() -> Self {
        Self::new()
    }
}

/// Allows a unit of work to be awaited and its result to be observed.
pub struct JoinHandle<R> {
    result: Rc<RefCell<TaskResult<R>>>,
}

impl<R> Future for JoinHandle<R> {
    type Output = R;

    fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> task::Poll<Self::Output> {
        let mut result = self.result.borrow_mut();

        match &*result {
            TaskResult::Pending => {
                *result = TaskResult::Awaiting(cx.waker().clone());
                task::Poll::Pending
            }
            TaskResult::Awaiting(_) => {
                // This is permitted by the Future API contract. We can only hope that the waker
                // is the same waker because we are not going to re-register it if it is not.
                println!(
                    "JoinHandle polled again before result is ready. Ignoring duplicate poll."
                );
                task::Poll::Pending
            }
            TaskResult::Ready(_) => {
                let result = mem::replace(&mut *result, TaskResult::Consumed);

                match result {
                    TaskResult::Ready(result) => task::Poll::Ready(result),
                    _ => unreachable!(),
                }
            }
            TaskResult::Consumed => {
                // We do not want to keep a copy of the result around, so we can only return it once.
                panic!("JoinHandle polled after result was already consumed.");
            }
        }
    }
}

trait Task {
    fn id(&self) -> usize;
    fn poll(&mut self) -> task::Poll<()>;
}

enum TaskResult<T> {
    // The task has not completed and nobody has started awaiting for the result yet.
    Pending,

    // The task has not completed but someone is awaiting the result.
    Awaiting(Waker),

    // The task has completed and the result is ready for consumption.
    Ready(T),

    // The task has completed and the result has been consumed.
    Consumed,
}

struct MyTask<R> {
    id: usize,
    future: Pin<Box<dyn Future<Output = R>>>,
    waker: Arc<MyWaker>,
    result: Rc<RefCell<TaskResult<R>>>,
}

impl<R> MyTask<R> {
    fn new(id: usize, future: Pin<Box<dyn Future<Output = R>>>, waker: Arc<MyWaker>) -> Self {
        Self {
            id,
            future,
            waker,
            result: Rc::new(RefCell::new(TaskResult::Pending)),
        }
    }

    fn set_result(&mut self, result: R) {
        let mut self_result = self.result.borrow_mut();

        // Temporarily shove Pending in there so we can move the old value out and have easy life.
        let old_result = mem::replace(&mut *self_result, TaskResult::Pending);

        match old_result {
            TaskResult::Pending => {
                *self_result = TaskResult::Ready(result);
            }
            TaskResult::Awaiting(waker) => {
                *self_result = TaskResult::Ready(result);
                waker.wake();
            }
            TaskResult::Ready(_) => {
                panic!("Result already set.");
            }
            TaskResult::Consumed => {
                panic!("Result already consumed.");
            }
        }
    }
}

impl<R> Task for MyTask<R> {
    fn id(&self) -> usize {
        self.id
    }

    fn poll(&mut self) -> task::Poll<()> {
        // TODO: This allocation is gross. To be fixed later (requires unsafe code).
        let waker = self.waker.clone().into();
        let mut context = task::Context::from_waker(&waker);

        match self.future.as_mut().poll(&mut context) {
            task::Poll::Ready(result) => {
                println!("Future completed.");
                self.set_result(result);
                task::Poll::Ready(())
            }
            task::Poll::Pending => {
                println!("Future returned Pending. Will poll again after wakeup.");
                task::Poll::Pending
            }
        }
    }
}

struct MyWaker {
    task_id: usize,
    awakened: Arc<Mutex<BTreeSet<usize>>>,
}

impl Wake for MyWaker {
    fn wake(self: Arc<Self>) {
        println!("Waker for task {} called.", self.task_id);
        self.awakened.lock().unwrap().insert(self.task_id);
    }
}

impl MyWaker {
    fn new(task_id: usize, awakened: Arc<Mutex<BTreeSet<usize>>>) -> Self {
        Self { task_id, awakened }
    }
}
