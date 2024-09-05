use crate::{
    io,
    metrics::{self, Event, EventBuilder, ReportPage},
    rt::{
        async_task_engine::{AsyncTaskEngine, CycleResult},
        local_task::LocalTask,
        LocalErasedTask, LocalJoinHandle, RemoteErasedTask,
    },
};
use std::{
    any::type_name,
    cell::RefCell,
    collections::VecDeque,
    fmt::{self, Debug, Formatter},
    future::Future,
    sync::mpsc,
};
use tracing::{event, Level};

/// Coordinates the operations of the Folo runtime on a single thread. There may be different
/// types of agents assigned to different threads (e.g. async worker versus sync worker).
pub struct Agent {
    command_rx: mpsc::Receiver<AgentCommand>,
    metrics_tx: Option<mpsc::Sender<ReportPage>>,

    engine: RefCell<AsyncTaskEngine>,

    io: RefCell<io::Driver>,

    // Tasks that have been enqueued but have not yet been handed over to the async task engine.
    // Includes both locally queued tasks and tasks enqueued from another thread, which are both
    // unified to the `LocalErasedTask` type.
    new_tasks: RefCell<VecDeque<LocalErasedTask>>,
}

// TODO: This RefCell business is ugly, can we do better?
// It is how it is because it is legal to reference the current thread's agent via `Rc` to
// command it. So in principle, when the engine is processing tasks, one of those tasks could
// call back into the agent and enqueue more tasks. By separating everything via RefCell we make
// that possible. However, it is a bit error-prone (you have to be careful that no publicly
// accessible methods lead to conflicting RefCell borrows or we hit a panic).

impl Agent {
    pub fn new(
        command_rx: mpsc::Receiver<AgentCommand>,
        metrics_tx: Option<mpsc::Sender<ReportPage>>,
    ) -> Self {
        Self {
            command_rx,
            metrics_tx,
            engine: RefCell::new(AsyncTaskEngine::new()),
            io: RefCell::new(io::Driver::new()),
            new_tasks: RefCell::new(VecDeque::new()),
        }
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
        event!(Level::TRACE, message = "Agent::spawn", future_type);

        LOCAL_TASKS.with(|x| x.observe_unit());

        let task = LocalTask::new(future);
        let join_handle = task.join_handle();

        self.new_tasks.borrow_mut().push_back(Box::pin(task));

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
                    break;
                }
            }

            let io_wait_time_ms = if allow_io_sleep {
                CYCLES_WITH_SLEEP.with(|x| x.observe_unit());

                CROSS_THREAD_WORK_POLL_INTERVAL_MS
            } else {
                CYCLES_WITHOUT_SLEEP.with(|x| x.observe_unit());

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
            };
        }

        event!(Level::TRACE, "Terminating");

        if let Some(tx) = &self.metrics_tx {
            _ = tx.send(metrics::report_page());
        }
    }

    fn process_commands(&self) -> ProcessCommandsResult {
        let mut received_commands = false;

        loop {
            match self.command_rx.try_recv() {
                Ok(AgentCommand::EnqueueTask { erased_task }) => {
                    received_commands = true;
                    REMOTE_TASKS.with(|x| x.observe_unit());
                    self.new_tasks.borrow_mut().push_back(erased_task);
                }
                Ok(AgentCommand::Terminate) => {
                    return ProcessCommandsResult::Terminate;
                }
                Err(mpsc::TryRecvError::Empty) => {
                    if received_commands {
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

impl Debug for Agent {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Agent")
            .field("command_rx", &self.command_rx)
            .field("engine", &self.engine)
            .finish()
    }
}

pub enum AgentCommand {
    EnqueueTask {
        erased_task: RemoteErasedTask,
    },

    /// Shuts down the worker thread immediately, without waiting for any pending operations to
    /// complete. The worker will still perform necessary cleanup to avoid resource leaks, because
    /// this is not necessarily called at process termination time when cleanup is not needed (e.g.
    /// it may be one of many test runtimes used in a test run by different unit tests).
    Terminate,
}

impl Debug for AgentCommand {
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
        .name("rt_tasks_local")
        .build()
        .unwrap();

    static REMOTE_TASKS: Event = EventBuilder::new()
        .name("rt_tasks_remote")
        .build()
        .unwrap();

    static CYCLES_WITH_SLEEP: Event = EventBuilder::new()
        .name("rt_cycles_with_sleep")
        .build()
        .unwrap();

    static CYCLES_WITHOUT_SLEEP: Event = EventBuilder::new()
        .name("rt_cycles_without_sleep")
        .build()
        .unwrap();
}
