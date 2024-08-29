use std::{
    collections::{BTreeSet, VecDeque},
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{self, Wake},
};

fn main() {
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

    // TODO: How are we going to handle futures with different output types? It is not obvious how
    //   the type system works this out here. We "care" about the output type but only in the sense
    //   that we ultimately need to return the output value to someone somewhere in some cases. How
    //   do we treat all futures the same and only differentiate when handling the return value?

    let mut engine = Engine::new();

    // Each of these should get polled twice - first time returning Pending, next time Ready.
    // We move these into a heap-allocated box and pin the box before adding to the active set.
    engine.enqueue(yield_now());
    engine.enqueue(yield_now());

    // TODO: The above is scheduling work that does not expose its return value. What mechanisms
    // do we need to expose the return value once ready?

    engine.run();
}

/// The engine executes tasks on a single thread. It collaborates with other engines on other
/// threads that are part of the same executor.
struct Engine {
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
    fn new() -> Self {
        Self {
            active: VecDeque::new(),
            inactive: VecDeque::new(),
            awakened: Arc::new(Mutex::new(BTreeSet::new())),
            next_task_id: 0,
        }
    }

    fn enqueue(&mut self, future: impl Future<Output = ()> + 'static) {
        self.active.push_back(Box::new(MyTask::new(
            self.next_task_id,
            Box::pin(future),
            Arc::new(MyWaker::new(self.next_task_id, Arc::clone(&self.awakened))),
        )));

        self.next_task_id += 1;
    }

    fn run(&mut self) {
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

trait Task {
    fn id(&self) -> usize;
    fn poll(&mut self) -> task::Poll<()>;
}

struct MyTask<R> {
    id: usize,
    future: Pin<Box<dyn Future<Output = R>>>,
    waker: Arc<MyWaker>,
}

impl<R> MyTask<R> {
    fn new(id: usize, future: Pin<Box<dyn Future<Output = R>>>, waker: Arc<MyWaker>) -> Self {
        Self { id, future, waker }
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

        // TODO: Deliver the output value to the join handle if we have finished.
        match self.future.as_mut().poll(&mut context) {
            task::Poll::Ready(_) => {
                println!("Future completed.");
                task::Poll::Ready(())
            }
            task::Poll::Pending => {
                println!("Future returned Pending. Will poll again after wakeup.");
                task::Poll::Pending
            }
        }
    }
}

fn yield_now() -> impl Future<Output = ()> {
    ReadyAfterPoll::default()
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

/// An unit-result future that becomes ready immediately after the first time it is polled.
#[derive(Debug, Default)]
struct ReadyAfterPoll {
    first_poll_completed: bool,
}

impl Future for ReadyAfterPoll {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> task::Poll<Self::Output> {
        if self.first_poll_completed {
            println!("Nth poll completed.");
            task::Poll::Ready(())
        } else {
            println!("First poll completed.");
            self.first_poll_completed = true;
            cx.waker().wake_by_ref();
            task::Poll::Pending
        }
    }
}
