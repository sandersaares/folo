use tracing::{event, Level};

use crate::runtime::{
    async_task_engine::{AsyncTaskEngine, CycleResult},
    local_task::LocalTask,
    LocalErasedTask, LocalJoinHandle, RemoteErasedTask,
};
use std::{
    any::type_name,
    cell::RefCell,
    collections::VecDeque,
    fmt::{self, Debug, Formatter},
    future::Future,
    sync::mpsc,
};

/// Coordinates the operations of the Folo executor on a single thread. There may be different
/// types of agents assigned to different threads (e.g. async worker versus sync worker).
pub struct Agent {
    command_rx: mpsc::Receiver<AgentCommand>,

    engine: RefCell<AsyncTaskEngine>,

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
    pub fn new(command_rx: mpsc::Receiver<AgentCommand>) -> Self {
        Self {
            command_rx,
            engine: RefCell::new(AsyncTaskEngine::new()),
            new_tasks: RefCell::new(VecDeque::new()),
        }
    }

    /// Spawns a task to execute a future on the current async worker thread.
    ///
    /// # Panics
    ///
    /// Panics if the current thread is not an async worker thread. This is possible because there
    /// are more types of executor threads than async worker threads - e.g. sync worker threads.
    pub fn spawn<F, R>(&self, future: F) -> LocalJoinHandle<R>
    where
        F: Future<Output = R> + 'static,
        R: 'static,
    {
        let future_type = type_name::<F>();
        event!(Level::TRACE, key = "Agent::spawn", future_type);

        let task = LocalTask::new(future);
        let join_handle = task.join_handle();

        self.new_tasks.borrow_mut().push_back(Box::pin(task));

        join_handle
    }

    pub fn run(&self) {
        event!(Level::TRACE, "Started");

        loop {
            if self.process_commands() == ProcessCommandsResult::Terminate {
                event!(Level::TRACE, "Terminating");
                break;
            }

            // TODO: Process IO.
            // TODO: Process timers.

            while let Some(erased_task) = self.new_tasks.borrow_mut().pop_front() {
                self.engine.borrow_mut().enqueue_erased(erased_task);
            }

            match self.engine.borrow_mut().execute_cycle() {
                CycleResult::Continue => continue,
                CycleResult::Suspend => {
                    // TODO: We need to suspend until IO or task scheduling wakes us up, to avoid just
                    // burning CPU when nothing is happening.
                    std::thread::yield_now();
                }
            };
        }
    }

    fn process_commands(&self) -> ProcessCommandsResult {
        loop {
            match self.command_rx.try_recv() {
                Ok(AgentCommand::EnqueueTask { erased_task }) => {
                    self.new_tasks.borrow_mut().push_back(erased_task);
                }
                Ok(AgentCommand::Terminate) => {
                    return ProcessCommandsResult::Terminate;
                }
                Err(mpsc::TryRecvError::Empty) => {
                    return ProcessCommandsResult::Continue;
                }
                Err(mpsc::TryRecvError::Disconnected) => {
                    // Each worker thread holds a reference to the executor client, so this
                    // should be impossible - the only way to drop the executor is to first
                    // terminate all the worker threads. Otherwise it runs forever.
                    unreachable!("executor was dropped before terminating worker threads");
                }
            }
        }
    }
}

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
    /// it may be one of many test executors used in a test run by different unit tests).
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
    // All commands processed, keep going.
    Continue,

    // We received a terminate command - stop the worker ASAP, drop everything on the floor (but
    // still clean up as needed so no dangling resources are left behind).
    Terminate,
}
