use super::erased_async_task::ErasedResultAsyncTask;
use crate::{
    io,
    metrics::{self, Event, EventBuilder, ReportPage},
    rt::{
        async_task_engine::{AsyncTaskEngine, CycleResult},
        current_runtime,
        local_task::LocalTask,
        LocalJoinHandle,
    },
    time::{advance_local_timers, UltraLowPrecisionInstant},
};
use core_affinity::CoreId;
use crossbeam::channel;
use std::{
    cell::{Cell, RefCell},
    collections::VecDeque,
    fmt::{self, Debug, Formatter},
    future::Future,
    pin::Pin,
    sync::Arc,
    time::Instant,
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
/// 4. Resources are released (I/O driver, async task engine).
///
/// The shutdown process is not exposed to user code for the most part - after each worker thread
/// receives the shutdown command, it acknowledges it, after which the public API of the runtime
/// becomes inactive (it is no longer possible to schedule new tasks, etc) but the worker threads
/// themselves may still continue operating in the background to finish up any pending cleanup.
/// This takes place independently on each task - there is no "runtime" anymore and the worker
/// threads are disconnected from each other by this point, though the runtime client can still
/// be used to wait for them to complete.
///
/// Importantly, the shutdown process must have completed by the time the agent is dropped because
/// it may be too late to release resources by the time the agent is dropped due to the need to
/// access thread-local data during resource release (which is a problem because thread-local data
/// is not released in a guaranteed order).
pub struct AsyncAgent {
    command_rx: channel::Receiver<AsyncAgentCommand>,
    metrics_tx: Option<channel::Sender<ReportPage>>,
    processor_id: CoreId,

    // Becomes None when `run()` has finished and we are safe top drop the AsyncAgent.
    engine: RefCell<Option<AsyncTaskEngine>>,

    // Becomes None when `run()` has finished and we are safe top drop the AsyncAgent.
    io: RefCell<Option<io::Driver>>,

    // Becomes None when `run()` has finished and we are safe top drop the AsyncAgent.
    io_shared: RefCell<Option<Arc<io::DriverShared>>>,

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
        command_rx: channel::Receiver<AsyncAgentCommand>,
        metrics_tx: Option<channel::Sender<ReportPage>>,
        io_shared: Arc<io::DriverShared>,
        processor_id: CoreId,
    ) -> Self {
        Self {
            command_rx,
            metrics_tx,
            processor_id,
            // SAFETY: The async task engine must not be dropped until we get a
            // `CycleResult::Shutdown` from it. We do wait for this in `run()`.
            engine: RefCell::new(Some(unsafe { AsyncTaskEngine::new() })),
            // SAFETY: The I/O driver must not be dropped while there are pending I/O operations.
            // We ensure this by waiting for I/O to complete before returning from `run()`.
            io: RefCell::new(Some(unsafe { io::Driver::new() })),
            io_shared: RefCell::new(Some(io_shared)),
            new_tasks: RefCell::new(VecDeque::new()),
            shutting_down: Cell::new(false),
        }
    }

    pub fn processor_id(&self) -> CoreId {
        self.processor_id
    }

    pub fn with_io<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut io::Driver) -> R,
    {
        let mut io = self.io.borrow_mut();
        let io_ref = io
            .as_mut()
            .expect("value is only cleared when agent shuts down, so must be set now");

        f(io_ref)
    }

    pub fn with_io_shared<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&io::DriverShared) -> R,
    {
        let io = self.io_shared.borrow();
        let io_ref = io
            .as_ref()
            .expect("value is only cleared when agent shuts down, so must be set now");

        f(io_ref)
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
        // wakers and break waker reference cycles and we use this to also cancel pending I/O.
        let mut task = unsafe { LocalTask::new(future) };
        let join_handle = task.as_mut().join_handle();

        // We queue up the tasks because we may be being called from within the async task engine
        // itself, so we cannot call back into it immediately.
        self.new_tasks.borrow_mut().push_back(task);
        join_handle
    }

    pub fn run(&self) {
        event!(Level::TRACE, "Started");

        // Any I/O wakeups from async task threads are batched and submitted at the end of each
        // loop cycle, to avoid double-dispatch when a loop processes many I/O wakeups for the same
        // target.
        io::IoWaker::enable_batching();

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
        // We try to react fast to all these events. The I/O driver is the most important one, as it
        // is time-critical and often going to be the main source of incoming work, so we focus on
        // that as our key driver of work. Therefore, when we have nothing else to do, we will just
        // go to sleep waiting on the I/O driver to wake us up.
        //
        // All the other sources of work are subordinated to the I/O driver - the never wake us up
        // themselves but we can often identify that a particular completion event might require a
        // particular thread's I/O driver to wake up. In these cases we send a special wakeup packet
        // to the I/O driver, which will then wake up the thread.
        //
        // The wakeup packet is not a guarantee - there are situations where we cannot identify the
        // correct thread to wake up, or possibly cannot schedule a wakeup for other reasons. As a
        // fallback, we wake up every N milliseconds to check for new work from other sources even
        // if there is no activity on the I/O driver. This may add some latency to the cases where
        // we cannot immediately trigger an I/O wakeup (typically up to 20 milliseconds).

        // If we have any reason to believe that we have non-I/O work to do, we set this to false,
        // which only dequeues already existing I/O completions and does not wait for new ones.
        let mut allow_io_sleep = false;

        // We are the only one referencing the engine, so just keep the reference around for good.
        let mut engine_guard = self.engine.borrow_mut();
        let engine = engine_guard
            .as_mut()
            .expect("the engine is only removed on shutdown so it must still be there");

        loop {
            // At the start of each iteration, we update the ultra-low precision clock. All
            // observations of its value during this cycle will use the value we set here.
            UltraLowPrecisionInstant::update();

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
                        // engine to clean up and for pending I/O operations to complete.
                        event!(
                            Level::TRACE,
                            "received terminate command; shutdown process starting"
                        );

                        self.shutting_down.set(true);

                        // The tasks in this list may own resources that are already referenced by other
                        // tasks or external entities. We need to accept them into our regular process
                        // before dropping them - they are not safe to drop just because they are new.
                        while let Some(erased_task) = self.new_tasks.borrow_mut().pop_front() {
                            engine.enqueue_erased(erased_task);
                        }

                        // Start cleaning up the async task engine. This may require some time if there
                        // are foreign threads holding our wakers. We wait for all wakers to be dropped.
                        engine.begin_shutdown();

                        // The I/O driver itself does not have a shutdown process - we simply need
                        // to wait for all pending operations to complete. This will occur naturally
                        // over time, speeded up by the fact that the async task engine dropped a
                        // bunch of tasks that were hopefully holding I/O handles that now got
                        // closed and resulted in pending I/O being canceled (which we still need to
                        // wait for - a cancellation is just a regular I/O completion for us).
                    }
                }
            }

            // If new tasks have been enqueued but not yet handed over to the engine, we inhibit I/O
            // sleep to get to processing those new tasks ASAP after any pending I/O is completed.
            allow_io_sleep &= self.new_tasks.borrow().is_empty();

            let io_wait_time_ms = if allow_io_sleep {
                CYCLES_WITH_SLEEP.with(Event::observe_unit);

                CROSS_THREAD_WORK_POLL_INTERVAL_MS
            } else {
                CYCLES_WITHOUT_SLEEP.with(Event::observe_unit);

                0
            };

            self.io
                .borrow_mut()
                .as_mut()
                .expect("the I/O driver is only removed on shutdown so it must still be there")
                .process_completions(io_wait_time_ms);

            // We always only poll this, never wait on it - any waiting occurs above. One
            // implication of this is that if a completion arrives here, we may still end up waiting
            // on the above for some milliseconds. That's OK - this is shared so there are many
            // threads polling it all the time, the delay is negligible in the big picture.
            self.io_shared
                .borrow()
                .as_ref()
                .expect(
                    "the shared I/O driver is only removed on shutdown so it must still be there",
                )
                .process_completions();

            // TODO: Timers require that we provide an instant value. Some additional work we can explore:
            //
            // - What are the perf implications of this call?
            // - Shall we pass the current instant to `execute_cycle` and get rid of low-resolution watch?
            // - Shall we introduce some cached sink for current time (both relative and absolute) that is updated with each cycle?
            let now = Instant::now();
            advance_local_timers(now);

            {
                let mut new_tasks = self.new_tasks.borrow_mut();

                while let Some(erased_task) = new_tasks.pop_front() {
                    engine.enqueue_erased(erased_task);
                }
            }

            let execute_cycle_result = engine.execute_cycle();

            // The async task engine may have scheduled some runtime commands to be sent out.
            // Deliver them to runtime agents now so we ensure commands are sent every cycle.
            current_runtime::with(|runtime| runtime.submit_pending_tasks());

            // Now is a good time to submit any I/O wakeups for other threads.
            io::IoWaker::submit_batch();

            match execute_cycle_result {
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
                    event!(
                        Level::TRACE,
                        "async tasks engine reported it is safe to shut down"
                    );
                    break;
                }
            };
        }

        // Release resources before we finish shutdown, as now is a good time to clean up.
        // We can start by cleaning up the task engine because we know all tasks have been dropped
        // and no more can be scheduled. There is nothing for the task engine to do anymore.
        *engine_guard = None;

        {
            let mut io_guard = self.io.borrow_mut();
            let io = io_guard.as_mut().expect(
                "the I/O driver must still be there because it is only removed on shutdown",
            );

            let mut io_shared_guard = self.io_shared.borrow_mut();
            let io_shared = io_shared_guard.as_ref().expect(
                "the shared I/O driver must still be there because it is only removed on shutdown",
            );

            event!(
                Level::TRACE,
                "waiting for isolated and shared I/O drivers to complete pending operations"
            );

            // This is a replica of the main loop, except we only wait for I/O, nothing else.
            // Really, there is nothing else to do because task execution logic has been shut down
            // already.
            while !io.is_inert() || !io_shared.is_inert() {
                io.process_completions(CROSS_THREAD_WORK_POLL_INTERVAL_MS);
                io_shared.process_completions();

                // I/O completions could trigger wakeups of other threads.
                io::IoWaker::submit_batch();
            }

            // We are shutting down, so we need to drop the I/O driver now and release any resources
            // it holds (such as the completion port). We assume that `RuntimeClient::is_stopping()`
            // will prevent any of these drops from trying to schedule background release tasks.
            *io_guard = None;
            *io_shared_guard = None;
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
                Err(channel::TryRecvError::Empty) => {
                    if received_terminate {
                        return ProcessCommandsResult::Terminate;
                    } else if received_commands {
                        return ProcessCommandsResult::ContinueAfterCommand;
                    } else {
                        return ProcessCommandsResult::ContinueWithoutCommands;
                    }
                }
                Err(channel::TryRecvError::Disconnected) => {
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
/// signals for everything (though we have for most things, just not always guaranteed) and use
/// polling to check for arriving work. This sets our maximum sleep time, although
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

impl Drop for AsyncAgent {
    fn drop(&mut self) {
        // AsyncAgent is stored in a thread-local variable. However, when it is dropped it is
        // already too late to do any non-trivial cleanup because this may depend on other thread-
        // local variables (e.g. calling out to the runtime via current_runtime) and thread-local
        // variables may be destroyed in any order. Therefore, the agent must already be shut down
        // by the time it is dropped, so no nontrivial logic runs here.
        assert!(self.shutting_down.get());
        assert!(self.new_tasks.borrow().is_empty());
        assert!(self.io.borrow().is_none());
        assert!(self.engine.borrow().is_none());
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
    static LOCAL_TASKS: Event = EventBuilder::new("rt_async_tasks_local")
        .build();

    static REMOTE_TASKS: Event = EventBuilder::new("rt_async_tasks_remote")
        .build();

    static CYCLES_WITH_SLEEP: Event = EventBuilder::new("rt_async_cycles_with_sleep")
        .build();

    static CYCLES_WITHOUT_SLEEP: Event = EventBuilder::new("rt_async_cycles_without_sleep")
        .build();
}
