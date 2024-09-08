use super::erased_async_task::ErasedResultAsyncTask;
use crate::{
    io,
    metrics::{self, Event, EventBuilder, ReportPage},
    rt::{
        async_task_engine::{AsyncTaskEngine, CycleResult},
        local_task::LocalTask,
        LocalJoinHandle,
    },
};
use core_affinity::CoreId;
use std::{
    any::type_name,
    cell::{Cell, RefCell},
    collections::VecDeque,
    fmt::{self, Debug, Formatter},
    future::Future,
    pin::Pin,
    sync::mpsc,
};
use tracing::{event, Level};

/// Coordinates the operations of the Folo runtime on a single thread. There may be different
/// types of agents assigned to different threads (e.g. async worker versus sync worker). This is
/// the async agent.
///
/// An agent has multiple lifecycle stages:
/// 1. Starting up - the agent is interacting with the runtime builder to set up the mutual state
///    and communication channels so all the pieces can operate together.
/// 2. Running - the agent is executing tasks and processing commands from other threads.
/// 3. Shutting down - the agent is cleaning up and preparing to terminate.
///
/// The shutdown process is not exposed to user code for the most part - after each worker thread
/// receives the shutdown command, it acknowledges it, after which the public API of the runtime
/// becomes inactive (it is no longer possible to schedule new tasks, etc) but the worker threads
/// themselves may still continue operating in the background to finish up any pending cleanup.
/// This takes place independently on each task - there is no "runtime" anymore and the worker
/// threads are disconnected from each other by this point, though the runtime client can still
/// be used to wait for them to complete.
pub struct AsyncAgent {
    command_rx: mpsc::Receiver<AsyncAgentCommand>,
    metrics_tx: Option<mpsc::Sender<ReportPage>>,
    processor_id: CoreId,

    engine: RefCell<AsyncTaskEngine>,

    io: RefCell<io::Driver>,

    // Tasks that have been enqueued but have not yet been handed over to the async task engine.
    // Includes both locally queued tasks and tasks enqueued from another thread, which are both
    // unified to the `ErasedResultAsyncTask` type.
    new_tasks: RefCell<VecDeque<Pin<Box<dyn ErasedResultAsyncTask>>>>,

    // If we are shutting down, we try ignore requests to schedule new tasks and do our best to
    // cleanup ASAP.
    shutting_down: Cell<bool>,
}

impl AsyncAgent {
    pub fn new(
        command_rx: mpsc::Receiver<AsyncAgentCommand>,
        metrics_tx: Option<mpsc::Sender<ReportPage>>,
        processor_id: CoreId,
    ) -> Self {
        Self {
            command_rx,
            metrics_tx,
            processor_id,
            engine: RefCell::new(AsyncTaskEngine::new()),
            io: RefCell::new(io::Driver::new()),
            new_tasks: RefCell::new(VecDeque::new()),
            shutting_down: Cell::new(false),
        }
    }

    pub fn processor_id(&self) -> CoreId {
        self.processor_id
    }

    pub fn io(&self) -> &RefCell<io::Driver> {
        &self.io
    }

    /// Spawns a task to execute a future on the current async worker thread.
    ///
    /// # Panics
    ///
    /// Panics if the current thread is not an async worker thread. This is possible because there
    /// are more types of runtime threads than async worker threads - e.g. sync worker threads.
    pub fn spawn<F, R>(&self, future: F) -> LocalJoinHandle<R>
    where
        F: Future<Output = R> + 'static,
        R: 'static,
    {
        let future_type = type_name::<F>();
        event!(Level::TRACE, future_type);

        assert!(
            !self.shutting_down.get(),
            "local tasks can only be spawned by the current thread; no call path takes us to spawning local tasks when we have already started local shutdown"
        );

        LOCAL_TASKS.with(Event::observe_unit);

        // SAFETY: We must ensure that the LocalTask is not dropped while any references to its
        // outcome exist (i.e. as long as the join handle is referenced by someone). The join handle
        // is returned from this function and may be referenced by any other task owned by the same
        // agent (that is what most user-initiated tasks will do - wait on other tasks).
        //
        // The specific type of the LocalTask is erased immediately after this function and it is
        // mixed together with RemoteTasks in `self.new_tasks` as `Pin<Box<dyn ErasedTask>>`, after
        // which they are all given to the async task engine which does not differentiate.
        //
        // `ErasedTask::is_inert()` informs the async task engine when the task is safe to drop.
        // This is always true for remote tasks but for local tasks it is true when references to
        // The embedded data structures have been dropped.
        //
        // But wait, if we are shutting down and there is another task that depends on it, what will
        // even cause the reference to drop? After all, we are not executing tasks anymore during
        // shutdown! The answer is `ErasedTask::clear()` which drops the future and any captured
        // state such as the join handle! The task engine also relies on that capability to drop
        // wakers and break waker reference cycles in a similar manner.
        let mut task = unsafe { LocalTask::new(future) };
        let join_handle = task.as_mut().join_handle();

        // We queue up the tasks because we may be being called from within the async task engine
        // itself, so we cannot call back into it immediately.
        self.new_tasks.borrow_mut().push_back(task);
        join_handle
    }

    pub fn run(&self) {
        event!(Level::TRACE, "Started");

        // We want to do useful work in this loop as much as possible, yet without burning CPU on
        // just pinning and waiting for work.
        //
        // There are multiple direction from which work can arrive:
        // * I/O completion arrived on the I/O driver.
        // * An "enqueue new task" command was received from an arbitrary thread.
        // * Some task on the current thread enqueued another task.
        // * A timer expired (not implemented yet).
        // * A sleeping task was woken up
        //     If it wakes up due to current thread activity, we can just think of it as a
        //     consequence of that activity (e.g. I/O completion). However, a task can also be woken
        //     up by some event on another thread!
        //
        // It is not feasible to (cheaply) react immediately to all of these events, so we need to
        // use periodic polling with some of them. The I/O driver is the most important one, as it
        // is time-critical and often going to be the main source of incoming work, so we focus on
        // that as our key driver of work. Therefore, when we have nothing else to do, we will just
        // go to sleep waiting on the I/O driver to wake us up.
        //
        // As a fallback, to ensure that other events are not missed during stretches of time when
        // there is no I/O activity, we wake up every N milliseconds to check for new work from
        // other sources.
        //
        // Note that our timers are I/O driven, so are not affected by our polling slowness. In
        // effect, this means that we only react slowly to activity arriving from other threads.
        //
        // We also have an explicit wakeup when a remote task is queued, so we do not just wait
        // doing nothing when another thread gives us a new task. This is accomplished by signaling
        // the I/O driver - "new task was added" is treated as an I/O completion event, which wakes
        // us up.

        // If we have any reason to believe that we have non-I/O work to do, we set this to false,
        // which only dequeues already existing I/O completions and does not wait for new ones.
        let mut allow_io_sleep = false;

        loop {
            match self.process_commands() {
                ProcessCommandsResult::ContinueAfterCommand => {
                    // Commands were received. We probably have non-I/O work to do.
                    allow_io_sleep = false;
                }
                ProcessCommandsResult::ContinueWithoutCommands => {
                    // No commands received - we have no information saying we have non-I/O work to do.
                }
                ProcessCommandsResult::Terminate => {
                    // Given various eventual consistency scenarios that may apply to the
                    // coordination of worker threads, it is conceivable that somehow we might get
                    // multiple shutdown commands. Just ignore any extra ones - we cannot be
                    // shutting down any harder than we already are.
                    if !self.shutting_down.get() {
                        // This *starts* our shutdown - we still need to wait for the async task
                        // engine to clean up.
                        event!(
                            Level::TRACE,
                            "received terminate command; shutdown process starting"
                        );

                        self.shutting_down.set(true);

                        // The tasks in this list may own resources that are already referenced by other
                        // tasks or external entities. We need to accept them into our regular process
                        // before dropping them - they are not safe to drop just because they are new.
                        while let Some(erased_task) = self.new_tasks.borrow_mut().pop_front() {
                            self.engine.borrow_mut().enqueue_erased(erased_task);
                        }

                        // Start cleaning up the async task engine. This may require some time if there
                        // are foreign threads holding our wakers. We wait for all wakers to be dropped.
                        self.engine.borrow_mut().begin_shutdown();

                        // TODO: Does I/O driver need shutdown logic as well? How do they relate?
                    }
                }
            }

            let io_wait_time_ms = if allow_io_sleep {
                CYCLES_WITH_SLEEP.with(Event::observe_unit);

                CROSS_THREAD_WORK_POLL_INTERVAL_MS
            } else {
                CYCLES_WITHOUT_SLEEP.with(Event::observe_unit);

                0
            };

            self.io.borrow_mut().process_completions(io_wait_time_ms);

            // TODO: Process timers.

            while let Some(erased_task) = self.new_tasks.borrow_mut().pop_front() {
                self.engine.borrow_mut().enqueue_erased(erased_task);
            }

            match self.engine.borrow_mut().execute_cycle() {
                CycleResult::Continue => {
                    // The async task engine believes there may be more work to do, so no sleep.
                    allow_io_sleep = false;
                    continue;
                }
                CycleResult::Suspend => {
                    // The async task engine had nothing to do, so it thinks we can sleep now. OK.
                    allow_io_sleep = true;
                }
                CycleResult::Shutdown => {
                    // The async task engine has finished shutting down, so we can now exit.
                    break;
                }
            };
        }

        event!(Level::TRACE, "shutdown completed");

        if let Some(tx) = &self.metrics_tx {
            _ = tx.send(metrics::report_page());
        }
    }

    fn process_commands(&self) -> ProcessCommandsResult {
        let mut received_commands = false;
        let mut received_terminate = false;

        loop {
            match self.command_rx.try_recv() {
                Ok(AsyncAgentCommand::EnqueueTask { erased_task }) => {
                    // This is how remote tasks arrive at us. If we are shutting down then we do NOT
                    // want to process them and will simply drop them on the floor. This is safe
                    // because remote tasks are expected to always be inert (they hold no resources
                    // that need special cleanup, at least not yet, because they have no local
                    // presence yet).
                    if self.shutting_down.get() || received_terminate {
                        assert!(
                            erased_task.is_inert(),
                            "all remote tasks must be always inert"
                        );
                        continue;
                    }

                    received_commands = true;
                    REMOTE_TASKS.with(Event::observe_unit);
                    self.new_tasks.borrow_mut().push_back(erased_task);
                }
                Ok(AsyncAgentCommand::Terminate) => {
                    // We continue processing commands even after the terminate signal because
                    // we need to clean up any messages received during the shutdown process,
                    // which can take some time and it feels better to avoid any data buildup.
                    received_terminate = true;
                    continue;
                }
                Err(mpsc::TryRecvError::Empty) => {
                    if received_terminate {
                        return ProcessCommandsResult::Terminate;
                    } else if received_commands {
                        return ProcessCommandsResult::ContinueAfterCommand;
                    } else {
                        return ProcessCommandsResult::ContinueWithoutCommands;
                    }
                }
                Err(mpsc::TryRecvError::Disconnected) => {
                    // Each worker thread holds a reference to the runtime client, so this
                    // should be impossible - the only way to drop the runtime is to first
                    // terminate all the worker threads. Otherwise it runs forever.
                    unreachable!("runtime was dropped before terminating worker threads");
                }
            }
        }
    }
}

/// How often to poll for cross-thread work, in milliseconds. We do not have cross-thread real time
/// signals and use polling to check for arriving work. This sets our maximum sleep time, although
/// we will often check much more often if activity on the current thread wakes us up.
const CROSS_THREAD_WORK_POLL_INTERVAL_MS: u32 = 10;

impl Debug for AsyncAgent {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Agent")
            .field("command_rx", &self.command_rx)
            .field("engine", &self.engine)
            .field("io", &self.io)
            .field("shutting_down", &self.shutting_down)
            .finish()
    }
}

pub enum AsyncAgentCommand {
    EnqueueTask {
        erased_task: Pin<Box<dyn ErasedResultAsyncTask + Send>>,
    },

    /// Shuts down the worker thread immediately, without waiting for any pending operations to
    /// complete. The worker will still complete the current task and perform necessary cleanup
    /// to avoid resource leaks, which may take some time.
    Terminate,
}

impl Debug for AsyncAgentCommand {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::EnqueueTask { .. } => write!(f, "EnqueueTask"),
            Self::Terminate => write!(f, "Terminate"),
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
enum ProcessCommandsResult {
    // At least one command processed, keep going.
    ContinueAfterCommand,

    // There were no commands queued, keep going.
    ContinueWithoutCommands,

    // We received a terminate command - stop the worker ASAP, drop everything on the floor (but
    // still clean up as needed so no dangling resources are left behind).
    Terminate,
}

thread_local! {
    static LOCAL_TASKS: Event = EventBuilder::new()
        .name("rt_async_tasks_local")
        .build()
        .unwrap();

    static REMOTE_TASKS: Event = EventBuilder::new()
        .name("rt_async_tasks_remote")
        .build()
        .unwrap();

    static CYCLES_WITH_SLEEP: Event = EventBuilder::new()
        .name("rt_async_cycles_with_sleep")
        .build()
        .unwrap();

    static CYCLES_WITHOUT_SLEEP: Event = EventBuilder::new()
        .name("rt_async_cycles_without_sleep")
        .build()
        .unwrap();
}
