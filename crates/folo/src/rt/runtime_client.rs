use crate::constants::{self, GENERAL_MILLISECONDS_BUCKETS};
use crate::io::IoWaker;
use crate::metrics::{Event, EventBuilder};
use crate::rt::{
    async_agent::AsyncAgentCommand, current_async_agent, remote_result_box::RemoteResultBox,
    remote_task::RemoteTask, sync_agent::SyncAgentCommand, ErasedSyncTask, RemoteJoinHandle,
};
use crate::time::UltraLowPrecisionInstant;
use core_affinity::CoreId;
use crossbeam::channel;
use crossbeam::queue::SegQueue;
use std::any::type_name;
use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::{cell::Cell, future::Future, sync::Mutex, thread};
use tracing::{event, Level};

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

/// The multithreaded entry point for the Folo runtime, used for operations that affect more than
/// the current thread.
///
/// This type is thread-safe.
#[derive(Clone)]
pub struct RuntimeClient {
    async_command_txs: Box<[channel::Sender<AsyncAgentCommand>]>,
    async_io_wakers: Box<[IoWaker]>,

    // We often prefer to give work to the same processor, so we split
    // the sync command architecture up by the processor ID.
    sync_command_txs_by_processor: HashMap<CoreId, Box<[channel::Sender<SyncAgentCommand>]>>,
    sync_task_queues_by_processor: HashMap<CoreId, Arc<SegQueue<ErasedSyncTask>>>,
    sync_priority_task_queues_by_processor: HashMap<CoreId, Arc<SegQueue<ErasedSyncTask>>>,

    // We do not submit tasks directly to the sync agents. Instead, we submit tasks to the queues
    // and flush the queues once per agent loop. This avoids excessive cross-thread chatter when
    // submitting multiple tasks into the same queue.
    // We use Arc here simply to avoid the value having to implement `Clone` as required by HashMap.
    // Note that runtime clients are passed to worker threads from another thread, so thread-safe.
    pending_sync_tasks_by_processor: HashMap<CoreId, Arc<SegQueue<ErasedSyncTask>>>,
    pending_sync_priority_tasks_by_processor: HashMap<CoreId, Arc<SegQueue<ErasedSyncTask>>>,

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
        async_command_txs: Box<[channel::Sender<AsyncAgentCommand>]>,
        async_io_wakers: Box<[IoWaker]>,
        sync_command_txs_by_processor: HashMap<CoreId, Box<[channel::Sender<SyncAgentCommand>]>>,
        sync_task_queues_by_processor: HashMap<CoreId, Arc<SegQueue<ErasedSyncTask>>>,
        sync_priority_task_queues_by_processor: HashMap<CoreId, Arc<SegQueue<ErasedSyncTask>>>,
        processor_ids: Box<[CoreId]>,
        join_handles: Box<[thread::JoinHandle<()>]>,
        is_stopping: Arc<AtomicBool>,
    ) -> Self {
        let pending_sync_tasks_by_processor = processor_ids
            .iter()
            .map(|key| (*key, Arc::new(SegQueue::new())))
            .collect();
        let pending_sync_priority_tasks_by_processor = processor_ids
            .iter()
            .map(|key| (*key, Arc::new(SegQueue::new())))
            .collect();

        Self {
            async_command_txs,
            async_io_wakers,
            sync_command_txs_by_processor,
            sync_task_queues_by_processor,
            sync_priority_task_queues_by_processor,
            pending_sync_tasks_by_processor,
            pending_sync_priority_tasks_by_processor,
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

        let worker_index = next_async_worker(self.async_command_txs.len());

        // We ignore the return value because it is theoretically possible that something is trying
        // to schedule new work when we are in the middle of a shutdown process.
        _ = self.async_command_txs[worker_index].send(AsyncAgentCommand::EnqueueTask {
            erased_task: Box::pin(task),
        });

        // Wake up the agent if it might be sleeping and waiting for I/O.
        self.async_io_wakers[worker_index].wake();

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
        let worker_count = self.async_command_txs.len();

        let mut join_handles = Vec::with_capacity(worker_count);

        for worker_index in 0..worker_count {
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

            // We ignore the return value because it is theoretically possible that something is trying
            // to schedule new work when we are in the middle of a shutdown process.
            _ = self.async_command_txs[worker_index].send(AsyncAgentCommand::EnqueueTask {
                erased_task: Box::pin(task),
            });

            // Wake up the agent if it might be sleeping and waiting for I/O.
            self.async_io_wakers[worker_index].wake();

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
                self.pending_sync_tasks_by_processor[&processor_id].push(Box::new(task));
                event!(
                    Level::TRACE,
                    message = "queued task",
                    ?processor_id,
                    type_name = type_name::<F>()
                );
            }
            SynchronousTaskType::HighPrioritySyscall => {
                self.pending_sync_priority_tasks_by_processor[&processor_id].push(Box::new(task));
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
        if task_type != SynchronousTaskType::Syscall {
            panic!("spawn_sync_on_any only supports SynchronousTaskType::Syscall");
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
            SYNC_SPAWN_DELAY_LOW_PRIORITY.with(|x| x.observe_millis(started.elapsed()));
            result_box_tx.set(f())
        };

        // We pick an arbitrary processor. The assumption being that whoever is calling this has
        // so much work that it is unlikely to scale on one one processor, so they want to spread
        // the load around.
        let processor_id = self.processor_ids[next_sync_processor(self.processor_ids.len())];

        // We just add it to the pending task queue for now, to be submitted at the end of the cycle.
        let boxed_task = Box::new(task);
        let task_addr = format!("{:p}", &*boxed_task);
        self.pending_sync_tasks_by_processor[&processor_id].push(boxed_task);
        event!(
            Level::TRACE,
            message = "queued task",
            ?processor_id,
            type_name = type_name::<F>(),
            task_addr
        );

        RemoteJoinHandle::new(result_box_rx, self.current_thread_io_waker())
    }

    /// Submits any tasks that have been queued for submission. We expect this to be called by
    /// the current thread's agent at the end of every execution loop that could potentially have
    /// added some tasks.
    pub fn submit_pending_tasks(&self) {
        for (processor_id, pending_tasks) in &self.pending_sync_tasks_by_processor {
            let sync_task_queue = &self.sync_task_queues_by_processor[processor_id];

            let mut had_any_tasks = false;

            while let Some(task) = pending_tasks.pop() {
                let task_addr = format!("{:p}", &*task);
                event!(
                    Level::TRACE,
                    message = "submitting task",
                    ?processor_id,
                    task_addr
                );
                had_any_tasks = true;
                sync_task_queue.push(task);
            }

            if had_any_tasks {
                for tx in &self.sync_command_txs_by_processor[processor_id] {
                    // We ignore the return value because it is theoretically possible that something is trying
                    // to schedule new work when we are in the middle of a shutdown process.
                    _ = tx.send(SyncAgentCommand::CheckForTasks);
                }
            }
        }

        for (processor_id, pending_tasks) in &self.pending_sync_priority_tasks_by_processor {
            let sync_task_queue = &self.sync_priority_task_queues_by_processor[processor_id];

            let mut had_any_tasks = false;

            while let Some(task) = pending_tasks.pop() {
                let task_addr = format!("{:p}", &*task);
                event!(
                    Level::TRACE,
                    message = "submitting priority task",
                    ?processor_id,
                    task_addr
                );
                had_any_tasks = true;
                sync_task_queue.push(task);
            }

            if had_any_tasks {
                for tx in &self.sync_command_txs_by_processor[processor_id] {
                    // We ignore the return value because it is theoretically possible that something is trying
                    // to schedule new work when we are in the middle of a shutdown process.
                    _ = tx.send(SyncAgentCommand::CheckForTasks);
                }
            }
        }
    }

    /// Commands the runtime to stop processing tasks and shut down. Safe to call multiple times.
    ///
    /// This returns immediately. To wait for the runtime to stop, use `wait()`.
    pub fn stop(&self) {
        for tx in &self.async_command_txs {
            // We ignore the return value because if the worker has already stopped, the channel
            // may be closed in which case the send may simply fail.
            _ = tx.send(AsyncAgentCommand::Terminate);
        }

        for txs in self.sync_command_txs_by_processor.values() {
            for tx in txs {
                // We ignore the return value because if the worker has already stopped, the channel
                // may be closed in which case the send may simply fail.
                _ = tx.send(crate::rt::sync_agent::SyncAgentCommand::Terminate);
            }
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
        self.is_stopping.store(true, Ordering::Relaxed);

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
            .field("async_command_txs", &self.async_command_txs)
            .field("async_io_wakers", &self.async_io_wakers)
            .field(
                "sync_command_txs_by_processor",
                &self.sync_command_txs_by_processor,
            )
            .field(
                "sync_task_queues_by_processor",
                &self.sync_task_queues_by_processor,
            )
            .field(
                "sync_priority_task_queues_by_processor",
                &self.sync_priority_task_queues_by_processor,
            )
            .field(
                "pending_sync_tasks_by_processor",
                &self
                    .pending_sync_tasks_by_processor
                    .iter()
                    .map(|x| x.1.len())
                    .sum::<usize>(),
            )
            .field(
                "pending_sync_priority_tasks_by_processor",
                &self
                    .pending_sync_priority_tasks_by_processor
                    .iter()
                    .map(|x| x.1.len())
                    .sum::<usize>(),
            )
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
    static REMOTE_SPAWN_DELAY: Event = EventBuilder::new()
        .name("rt_remote_spawn_delay_millis")
        .buckets(GENERAL_MILLISECONDS_BUCKETS)
        .build()
        .unwrap();

    static SYNC_SPAWN_DELAY_HIGH_PRIORITY: Event = EventBuilder::new()
        .name("rt_sync_spawn_delay_high_priority_millis")
        .buckets(GENERAL_MILLISECONDS_BUCKETS)
        .build()
        .unwrap();

    static SYNC_SPAWN_DELAY_LOW_PRIORITY: Event = EventBuilder::new()
        .name("rt_sync_spawn_delay_low_priority_millis")
        .buckets(GENERAL_MILLISECONDS_BUCKETS)
        .build()
        .unwrap();
}
