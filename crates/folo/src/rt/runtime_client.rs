use std::any::type_name;
use std::cell::Cell;
use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::{fmt, thread};

use core_affinity::CoreId;
use crossbeam::channel;
use crossbeam::queue::SegQueue;
use tracing::{event, Level};

use crate::constants::{self, GENERAL_MILLISECONDS_BUCKETS};
use crate::io::IoWaker;
use crate::metrics::{Event, EventBuilder};
use crate::rt::async_agent::AsyncAgentCommand;
use crate::rt::remote_result_box::RemoteResultBox;
use crate::rt::remote_task::RemoteTask;
use crate::rt::sync_agent::SyncAgentCommand;
use crate::rt::{current_async_agent, ErasedSyncTask, RemoteJoinHandle};
use crate::time::UltraLowPrecisionInstant;

// TODO: In a real implementation we should split this up into multiple layers:
// 1) Validation and input processing (what is the command, is it valid in context, etc).
// 2) Passing to an actual command channel.
//    - May be a buffering command channel in case we are commanding it from an owned thread.
//    - May be a direct command channel in case we are commanding it from an unowned thread.
// 3) The command sender implementation that actually communicates with agents.
// Potentially even split the first layer in two for a separate public and private API layer?
// We should also reduce the weirdness of the design in general - do we really need it to be
// interior mutable? Do we really need it to be thread-safe? There are some situations today where
// the answer is "yes" but they are something like 1% of the use cases - can we do better?

#[derive(Clone)]
pub(super) struct CoreClient {
    processor_id: CoreId,

    async_command_tx: channel::Sender<AsyncAgentCommand>,
    async_io_waker: IoWaker,

    // We often prefer to give work to the same processor, so we split
    // the sync command architecture up by the processor ID.
    sync_command_txs: Box<[channel::Sender<SyncAgentCommand>]>,
    sync_task_queue: Arc<SegQueue<ErasedSyncTask>>,
    sync_priority_task_queue: Arc<SegQueue<ErasedSyncTask>>,

    // We do not submit tasks directly to the sync agents. Instead, we submit tasks to the queues
    // and flush the queues once per agent loop. This avoids excessive cross-thread chatter when
    // submitting multiple tasks into the same queue.
    // We use Arc here simply to avoid the value having to implement `Clone` as required by HashMap.
    // Note that runtime clients are passed to worker threads from another thread, so thread-safe.
    pub pending_sync_tasks: Arc<SegQueue<ErasedSyncTask>>,
    pub pending_sync_priority_tasks: Arc<SegQueue<ErasedSyncTask>>,
}

impl CoreClient {
    pub(super) fn new(
        processor_id: CoreId,
        async_command_tx: channel::Sender<AsyncAgentCommand>,
        async_io_waker: IoWaker,
        sync_command_txs: Box<[channel::Sender<SyncAgentCommand>]>,
        sync_task_queue: Arc<SegQueue<ErasedSyncTask>>,
        sync_priority_task_queue: Arc<SegQueue<ErasedSyncTask>>,
    ) -> Self {
        Self {
            processor_id,
            async_command_tx,
            async_io_waker,
            sync_command_txs,
            sync_task_queue,
            sync_priority_task_queue,
            pending_sync_tasks: Arc::new(SegQueue::new()),
            pending_sync_priority_tasks: Arc::new(SegQueue::new()),
        }
    }

    fn enqueue_async_task<F, R>(&self, task: RemoteTask<F, R>)
    where
        F: Future<Output = R> + Send + 'static,
        R: Send + 'static,
    {
        // We ignore the return value because it is theoretically possible that something is trying
        // to schedule new work when we are in the middle of a shutdown process.
        _ = self.async_command_tx.send(AsyncAgentCommand::EnqueueTask {
            erased_task: Box::pin(task),
        });

        // Wake up the agent if it might be sleeping and waiting for I/O.
        self.async_io_waker.wake();
    }

    fn terminate(&self) {
        // We ignore the return value because if the worker has already stopped, the channel
        // may be closed in which case the send may simply fail.
        let _ = self.async_command_tx.send(AsyncAgentCommand::Terminate);

        for tx in &self.sync_command_txs {
            // We ignore the return value because if the worker has already stopped, the channel
            // may be closed in which case the send may simply fail.
            _ = tx.send(crate::rt::sync_agent::SyncAgentCommand::Terminate);
        }
    }

    fn submit_pending_sync_tasks(&self) {
        let mut had_any_tasks = false;

        while let Some(task) = self.pending_sync_tasks.pop() {
            let task_addr = format!("{:p}", &*task);
            event!(
                Level::TRACE,
                message = "submitting task",
                ?self.processor_id,
                task_addr
            );
            had_any_tasks = true;
            self.sync_task_queue.push(task);
        }

        if had_any_tasks {
            for tx in &self.sync_command_txs {
                // We ignore the return value because it is theoretically possible that something is trying
                // to schedule new work when we are in the middle of a shutdown process.
                _ = tx.send(SyncAgentCommand::CheckForTasks);
            }
        }
    }

    fn submit_pending_sync_priority_tasks(&self) {
        let mut had_any_tasks = false;

        while let Some(task) = self.pending_sync_priority_tasks.pop() {
            let task_addr = format!("{:p}", &*task);
            event!(
                Level::TRACE,
                message = "submitting priority task",
                ?self.processor_id,
                task_addr
            );
            had_any_tasks = true;
            self.sync_task_queue.push(task);
        }

        if had_any_tasks {
            for tx in &self.sync_command_txs {
                // We ignore the return value because it is theoretically possible that something is trying
                // to schedule new work when we are in the middle of a shutdown process.
                _ = tx.send(SyncAgentCommand::CheckForTasks);
            }
        }
    }
}

impl fmt::Debug for CoreClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CoreClient")
            .field("processor_id", &self.processor_id)
            .field("async_command_tx", &self.async_command_tx)
            .field("async_io_waker", &self.async_io_waker)
            .field("sync_command_txs", &self.sync_command_txs)
            .field("sync_task_queue", &self.sync_task_queue)
            .field("sync_priority_task_queue", &self.sync_priority_task_queue)
            .field("pending_sync_tasks", &self.pending_sync_tasks.len())
            .field(
                "pending_sync_priority_tasks",
                &self.pending_sync_priority_tasks.len(),
            )
            .finish()
    }
}

/// The multithreaded entry point for the Folo runtime, used for operations that affect more than
/// the current thread.
///
/// This type is thread-safe.
#[derive(Clone)]
pub struct RuntimeClient {
    core_clients: HashMap<CoreId, CoreClient>,

    processor_ids: Box<[CoreId]>,

    // This is None if `.wait()` has already been called - the field can be consumed only once,
    // typically done by the runtime client provided to the entry point thread.
    #[allow(clippy::type_complexity)] // One day we may refactor this but not today.
    join_handles: Arc<Mutex<Option<Box<[thread::JoinHandle<()>]>>>>,

    // This can be used by cleanup logic to detect that the runtime is not usable anymore.
    is_stopping: Arc<AtomicBool>,
}

impl RuntimeClient {
    #[allow(clippy::too_many_arguments)] // Ssssshhhhh, sleep little Clippy!
    pub(super) fn new(
        core_clients: HashMap<CoreId, CoreClient>,
        processor_ids: Box<[CoreId]>,
        join_handles: Box<[thread::JoinHandle<()>]>,
        is_stopping: Arc<AtomicBool>,
    ) -> Self {
        Self {
            core_clients,
            processor_ids,
            join_handles: Arc::new(Mutex::new(Some(join_handles))),
            is_stopping,
        }
    }

    /// Spawns a task to execute a future on any worker thread, creating the future via closure.
    pub fn spawn_on_any<FN, F, R>(&self, future_fn: FN) -> RemoteJoinHandle<R>
    where
        FN: FnOnce() -> F + Send + 'static,
        F: Future<Output = R> + 'static,
        R: Send + 'static,
    {
        let started = UltraLowPrecisionInstant::now();

        // Just because we are spawning a future on another thread does not mean it has to be a
        // thread-safe future (although the return value has to be). Therefore, we kajigger it
        // around via a remote join handle from the same thread, to allow a single-threaded future
        // to execute, as long as the closure that creates it is thread-safe.
        let thread_safe_wrapper_future = async move {
            REMOTE_SPAWN_DELAY.with(|x| x.observe_millis(started.elapsed()));

            // TODO: This seems inefficient. Surely we can do better?
            // This is: RemoteJoinHandle -> RemoteJoinHandle -> LocalJoinHandle -> LocalJoinHandle
            // Desired is: RemoteJoinHandle -> LocalJoinHandle
            let join_handle: RemoteJoinHandle<R> = crate::rt::spawn(future_fn()).into();
            join_handle.await
        };

        let task = RemoteTask::new(thread_safe_wrapper_future);
        let join_handle = task.join_handle(self.current_thread_io_waker());

        let processor_id = self.processor_ids[next_async_worker(self.processor_ids.len())];
        self.core_clients[&processor_id].enqueue_async_task(task);

        join_handle
    }

    /// Spawns a task to execute a future on every worker thread.
    ///
    /// There are two layers of callbacks involved here, with the overall sequence being:
    /// 1. The first layer will be called on the originating thread, to create a callback for each
    ///    worker thread we will be scheduling the task on.
    /// 2. The result from the first callback will be a closure that we move to the target worker
    ///    thread and execute.
    /// 3. The second callback will be called on the target thread and return the future that
    ///    becomes the subject of the task.
    ///
    /// So essentially you are providing a "give me one more clone of the task-creator" function.
    pub fn spawn_on_all<FC, FN, F, R>(&self, mut clone_future_fn: FC) -> Box<[RemoteJoinHandle<R>]>
    where
        FC: FnMut() -> FN,
        FN: FnOnce() -> F + Send + 'static,
        F: Future<Output = R> + 'static,
        R: Send + 'static,
    {
        let started = UltraLowPrecisionInstant::now();

        let mut join_handles = Vec::with_capacity(self.core_clients.len());

        for proc in self.core_clients.values() {
            let future_fn = clone_future_fn();

            // Just because we are spawning a future on another thread does not mean it has to be a
            // thread-safe future (although the return value has to be). Therefore, we kajigger it
            // around via a remote join handle from the same thread, to allow a single-threaded future
            // to execute, as long as the closure that creates it is thread-safe.
            let thread_safe_wrapper_future = async move {
                REMOTE_SPAWN_DELAY.with(|x| x.observe_millis(started.elapsed()));

                // TODO: This seems inefficient. Surely we can do better?
                // This is: RemoteJoinHandle -> RemoteJoinHandle -> LocalJoinHandle -> LocalJoinHandle
                // Desired is: RemoteJoinHandle -> LocalJoinHandle
                let join_handle: RemoteJoinHandle<R> = crate::rt::spawn(future_fn()).into();
                join_handle.await
            };

            let task = RemoteTask::new(thread_safe_wrapper_future);
            let join_handle = task.join_handle(self.current_thread_io_waker());
            proc.enqueue_async_task(task);
            join_handles.push(join_handle);
        }

        join_handles.into_boxed_slice()
    }

    /// Spawns a task on a synchronous worker thread suitable for the specific type of synchronous
    /// work requested, returning the result via a join handle suitable for use in asynchronous
    /// tasks.
    pub fn spawn_sync<F, R>(&self, task_type: SynchronousTaskType, f: F) -> RemoteJoinHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        if task_type == SynchronousTaskType::Compute {
            panic!("SynchronousTaskType::Compute is not yet supported");
        }

        // This is mostly just an implementation limitation because we depend on a pending+flush
        // pattern and have not needed to implement multiple patterns nor come up with the suitable
        // abstractions to cover both patterns as part of this research implementation.
        assert!(
            current_async_agent::is_some(),
            "synchronous tasks can only be spawned from async worker threads"
        );

        let result_box_rx = Arc::new(RemoteResultBox::new());
        let result_box_tx = Arc::clone(&result_box_rx);

        let started = UltraLowPrecisionInstant::now();

        let task = move || {
            match task_type {
                SynchronousTaskType::Syscall => {
                    SYNC_SPAWN_DELAY_LOW_PRIORITY.with(|x| x.observe_millis(started.elapsed()))
                }
                SynchronousTaskType::HighPrioritySyscall => {
                    SYNC_SPAWN_DELAY_HIGH_PRIORITY.with(|x| x.observe_millis(started.elapsed()))
                }
                _ => unreachable!(),
            };

            result_box_tx.set(f())
        };

        // TODO: Support this from arbitrary threads, not just async worker threads.
        // While not relevant for private I/O (first/current motivation for this to exist), it
        // would be relevant for user workloads.
        let processor_id = current_async_agent::with(|x| x.processor_id());

        // We just add it to the pending task queue for now, to be submitted at the end of the cycle.
        match task_type {
            SynchronousTaskType::Syscall => {
                self.core_clients[&processor_id]
                    .pending_sync_tasks
                    .push(Box::new(task));
                event!(
                    Level::TRACE,
                    message = "queued task",
                    ?processor_id,
                    type_name = type_name::<F>()
                );
            }
            SynchronousTaskType::HighPrioritySyscall => {
                self.core_clients[&processor_id]
                    .pending_sync_priority_tasks
                    .push(Box::new(task));
                event!(
                    Level::TRACE,
                    message = "queued priority task",
                    ?processor_id,
                    type_name = type_name::<F>()
                );
            }
            _ => unreachable!(),
        }

        RemoteJoinHandle::new(result_box_rx, self.current_thread_io_waker())
    }

    /// Spawns a task on a synchronous worker thread suitable for the specific type of synchronous
    /// work requested, returning the result via a join handle suitable for use in asynchronous
    /// tasks.
    ///
    /// The task will be spawned on an arbitrary synchronous worker thread.
    pub fn spawn_sync_on_any<F, R>(
        &self,
        task_type: SynchronousTaskType,
        f: F,
    ) -> RemoteJoinHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        if task_type == SynchronousTaskType::Compute {
            panic!("SynchronousTaskType::Compute is not yet supported");
        }

        // This is mostly just an implementation limitation because we depend on a pending+flush
        // pattern and have not needed to implement multiple patterns nor come up with the suitable
        // abstractions to cover both patterns as part of this research implementation.
        assert!(
            current_async_agent::is_some(),
            "synchronous tasks can only be spawned from async worker threads"
        );

        let result_box_rx = Arc::new(RemoteResultBox::new());
        let result_box_tx = Arc::clone(&result_box_rx);

        let started = UltraLowPrecisionInstant::now();

        let task = move || {
            match task_type {
                SynchronousTaskType::Syscall => {
                    SYNC_SPAWN_DELAY_LOW_PRIORITY.with(|x| x.observe_millis(started.elapsed()))
                }
                SynchronousTaskType::HighPrioritySyscall => {
                    SYNC_SPAWN_DELAY_HIGH_PRIORITY.with(|x| x.observe_millis(started.elapsed()))
                }
                _ => unreachable!(),
            };

            result_box_tx.set(f())
        };

        // We pick an arbitrary processor. The assumption being that whoever is calling this has
        // so much work that it is unlikely to scale on one one processor, so they want to spread
        // the load around.
        let processor_id = self.processor_ids[next_sync_processor(self.processor_ids.len())];

        // We just add it to the pending task queue for now, to be submitted at the end of the cycle.
        let boxed_task = Box::new(task);
        let task_addr = format!("{:p}", &*boxed_task);
        
        match task_type {
            SynchronousTaskType::Syscall => {
                self.core_clients[&processor_id]
                    .pending_sync_tasks
                    .push(boxed_task);
                event!(
                    Level::TRACE,
                    message = "queued task",
                    ?processor_id,
                    type_name = type_name::<F>(),
                    task_addr
                );
            }
            SynchronousTaskType::HighPrioritySyscall => {
                self.core_clients[&processor_id]
                    .pending_sync_priority_tasks
                    .push(boxed_task);
                event!(
                    Level::TRACE,
                    message = "queued priority task",
                    ?processor_id,
                    type_name = type_name::<F>(),
                    task_addr
                );
            }
            _ => unreachable!(),
        }

        RemoteJoinHandle::new(result_box_rx, self.current_thread_io_waker())
    }

    /// Submits any tasks that have been queued for submission. We expect this to be called by
    /// the current thread's agent at the end of every execution loop that could potentially have
    /// added some tasks.
    pub fn submit_pending_tasks(&self) {
        for proc in self.core_clients.values() {
            proc.submit_pending_sync_tasks();
        }
        for proc in self.core_clients.values() {
            proc.submit_pending_sync_priority_tasks();
        }
    }

    /// Commands the runtime to stop processing tasks and shut down. Safe to call multiple times.
    ///
    /// This returns immediately. To wait for the runtime to stop, use `wait()`.
    pub fn stop(&self) {
        self.is_stopping.store(true, Ordering::Relaxed);

        for proc in self.core_clients.values() {
            proc.terminate();
        }
    }

    /// Returns `true` if the runtime has been asked to stop.
    /// If so, enqueued tasks are unlikely to actually execute.
    pub fn is_stopping(&self) -> bool {
        self.is_stopping.load(Ordering::Relaxed)
    }

    /// Returns `true` if the runtime has stopped (i.e. calling `wait()` would not block).
    ///
    /// # Panics
    ///
    /// If called after `wait()`.
    pub fn is_stopped(&self) -> bool {
        let join_handles = self.join_handles.lock().expect(constants::POISONED_LOCK);

        for join_handle in join_handles
            .as_ref()
            .expect("wait() has already been called - cannot access runtime state any more")
        {
            if !join_handle.is_finished() {
                return false;
            }
        }

        true
    }

    /// Waits for the runtime to stop. Blocks the thread until all runtime owned threads have
    /// terminated in response to a call to `stop()`. This can only be called once.
    ///
    /// If tasks on this runtime started external calls (e.g. awaited a future driven by a different
    /// runtime for interop purposes) then this will block until those external calls complete.
    ///
    /// # Panics
    ///
    /// If called more than once.
    pub fn wait(&self) {
        let mut join_handles = self.join_handles.lock().expect(constants::POISONED_LOCK);

        for join_handle in join_handles
            .take()
            .expect("RuntimeClient::wait() called multiple times")
        {
            join_handle.join().expect("worker thread panicked");
        }
    }

    fn current_thread_io_waker(&self) -> Option<IoWaker> {
        current_async_agent::try_with_io(|io| io.waker())
    }
}

impl fmt::Debug for RuntimeClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RuntimeClient")
            .field("core_clients", &self.core_clients)
            .field("processor_ids", &self.processor_ids)
            .field("join_handles", &self.join_handles)
            .field("is_stopping", &self.is_stopping)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SynchronousTaskType {
    /// Some syscall that the runtime needs to perform synchronously and which may take an unknown
    /// amount of time. We try to keep the time to a minimum by always using asynchronous I/O and
    /// never intentionally blocking on this but the system is a wild west and for I/O operations
    /// you may have things like antivirus drivers that freeze your syscall for many seconds.
    Syscall,

    /// A high-priority syscall whose execution we benefit from in some way. For example, this may
    /// release resources, thereby improving our overall efficiency. The high-priority tasks are
    /// always executed, even if the runtime is shutting down. This is because they may be cleanup
    /// tasks to release resources that are blocking shutdown (e.g. because the operating system is
    /// referencing memory owned by us and this is the task to remove that reference).
    HighPrioritySyscall,

    /// The task may occupy a thread with a compute workload for a nontrivial duration (> 10 ms)
    /// and requires a safe space to execute without interfering with non-compute workloads.
    Compute,
}

// Basic round-robin implementation for distributing work across async workers.
thread_local! {
    static NEXT_ASYNC_WORKER_INDEX: Cell<usize> = const { Cell::new(0) };
}

thread_local! {
    static NEXT_SYNC_PROCESSOR_INDEX: Cell<usize> = const { Cell::new(0) };
}

fn next_async_worker(max: usize) -> usize {
    let next = NEXT_ASYNC_WORKER_INDEX.get();
    NEXT_ASYNC_WORKER_INDEX.set((next + 1) % max);
    next
}

fn next_sync_processor(max: usize) -> usize {
    let next = NEXT_SYNC_PROCESSOR_INDEX.get();
    NEXT_SYNC_PROCESSOR_INDEX.set((next + 1) % max);
    next
}

thread_local! {
    static REMOTE_SPAWN_DELAY: Event = EventBuilder::new("rt_remote_spawn_delay_millis")
        .buckets(GENERAL_MILLISECONDS_BUCKETS)
        .build();

    static SYNC_SPAWN_DELAY_HIGH_PRIORITY: Event = EventBuilder::new("rt_sync_spawn_delay_high_priority_millis")
        .buckets(GENERAL_MILLISECONDS_BUCKETS)
        .build();

    static SYNC_SPAWN_DELAY_LOW_PRIORITY: Event = EventBuilder::new("rt_sync_spawn_delay_low_priority_millis")
        .buckets(GENERAL_MILLISECONDS_BUCKETS)
        .build();
}
