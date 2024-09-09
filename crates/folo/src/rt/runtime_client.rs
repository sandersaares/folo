use super::remote_result_box::RemoteResultBox;
use super::sync_agent::SyncAgentCommand;
use super::{current_async_agent, ErasedSyncTask};
use crate::constants::{self, GENERAL_LOW_PRECISION_SECONDS_BUCKETS};
use crate::io::IoWaker;
use crate::metrics::{Event, EventBuilder};
use crate::rt::{async_agent::AsyncAgentCommand, remote_task::RemoteTask, RemoteJoinHandle};
use crate::util::LowPrecisionInstant;
use concurrent_queue::ConcurrentQueue;
use core_affinity::CoreId;
use std::collections::HashMap;
use std::sync::{mpsc, Arc};
use std::{cell::Cell, future::Future, sync::Mutex, thread};

/// The multithreaded entry point for the Folo runtime, used for operations that affect more than
/// the current thread.
///
/// This type is thread-safe.
#[derive(Clone, Debug)]
pub struct RuntimeClient {
    async_command_txs: Box<[mpsc::Sender<AsyncAgentCommand>]>,
    async_io_wakers: Box<[IoWaker]>,

    // We often prefer to give work to the same processor, so we split
    // the sync command architecture up by the processor ID.
    sync_command_txs_by_processor: HashMap<CoreId, Box<[mpsc::Sender<SyncAgentCommand>]>>,
    sync_task_queues_by_processor: HashMap<CoreId, Arc<ConcurrentQueue<ErasedSyncTask>>>,

    // This is None if `.wait()` has already been called - the field can be consumed only once,
    // typically done by the runtime client provided to the entry point thread.
    join_handles: Arc<Mutex<Option<Box<[thread::JoinHandle<()>]>>>>,
}

impl RuntimeClient {
    pub(super) fn new(
        async_command_txs: Box<[mpsc::Sender<AsyncAgentCommand>]>,
        async_io_wakers: Box<[IoWaker]>,
        sync_command_txs_by_processor: HashMap<CoreId, Box<[mpsc::Sender<SyncAgentCommand>]>>,
        sync_task_queues_by_processor: HashMap<CoreId, Arc<ConcurrentQueue<ErasedSyncTask>>>,
        join_handles: Box<[thread::JoinHandle<()>]>,
    ) -> Self {
        Self {
            async_command_txs,
            async_io_wakers,
            sync_command_txs_by_processor,
            sync_task_queues_by_processor,
            join_handles: Arc::new(Mutex::new(Some(join_handles))),
        }
    }

    /// Spawns a task to execute a future on any worker thread, creating the future via closure.
    pub fn spawn_on_any<FN, F, R>(&self, future_fn: FN) -> RemoteJoinHandle<R>
    where
        FN: FnOnce() -> F + Send + 'static,
        F: Future<Output = R> + 'static,
        R: Send + 'static,
    {
        let started = LowPrecisionInstant::now();

        // Just because we are spawning a future on another thread does not mean it has to be a
        // thread-safe future (although the return value has to be). Therefore, we kajigger it
        // around via a remote join handle from the same thread, to allow a single-threaded future
        // to execute, as long as the closure that creates it is thread-safe.
        let thread_safe_wrapper_future = async move {
            REMOTE_SPAWN_DELAY.with(|x| x.observe(started.elapsed().as_secs_f64()));

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

    /// Spawns a blocking task on any synchronous worker thread suitable for blocking, returning
    /// the result via a join handle suitable for use in asynchronous tasks.
    pub fn spawn_blocking<F, R>(&self, f: F) -> RemoteJoinHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let result_box_rx = Arc::new(RemoteResultBox::new());
        let result_box_tx = Arc::clone(&result_box_rx);

        let started = LowPrecisionInstant::now();

        let task = move || {
            SYNC_SPAWN_DELAY.with(|x| x.observe(started.elapsed().as_secs_f64()));

            result_box_tx.set(f())
        };

        // TODO: Support spawn_blocking from arbitrary threads, not just async worker threads.
        let processor_id = current_async_agent::with(|x| x.processor_id());

        // We ignore the return value because it is theoretically possible that something is trying
        // to schedule new work when we are in the middle of a shutdown process.
        _ = self.sync_task_queues_by_processor[&processor_id].push(Box::new(task));

        for tx in &self.sync_command_txs_by_processor[&processor_id] {
            // We ignore the return value because it is theoretically possible that something is trying
            // to schedule new work when we are in the middle of a shutdown process.
            _ = tx.send(SyncAgentCommand::CheckForTasks);
        }

        RemoteJoinHandle::new(result_box_rx, self.current_thread_io_waker())
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

// Basic round-robin implementation for distributing work across async workers.
thread_local! {
    static NEXT_ASYNC_WORKER_INDEX: Cell<usize> = const { Cell::new(0) };
}

fn next_async_worker(max: usize) -> usize {
    let next = NEXT_ASYNC_WORKER_INDEX.get();
    NEXT_ASYNC_WORKER_INDEX.set((next + 1) % max);
    next
}

thread_local! {
    static REMOTE_SPAWN_DELAY: Event = EventBuilder::new()
        .name("rt_remote_spawn_delay_seconds")
        .buckets(GENERAL_LOW_PRECISION_SECONDS_BUCKETS)
        .build()
        .unwrap();

    static SYNC_SPAWN_DELAY: Event = EventBuilder::new()
        .name("rt_sync_spawn_delay_seconds")
        .buckets(GENERAL_LOW_PRECISION_SECONDS_BUCKETS)
        .build()
        .unwrap();
}
