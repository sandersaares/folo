use super::remote_result_box::RemoteResultBox;
use super::sync_agent::SyncAgentCommand;
use crate::constants::{self, GENERAL_SECONDS_BUCKETS};
use crate::metrics::{Event, EventBuilder};
use crate::rt::{
    async_agent::AsyncAgentCommand, remote_task::RemoteTask, runtime::Runtime, RemoteJoinHandle,
};
use std::sync::Arc;
use std::time::Instant;
use std::{cell::Cell, future::Future, sync::Mutex, thread};

/// The multithreaded entry point for the Folo runtime, used for operations that affect more than
/// the current thread.
///
/// This type is thread-safe.
#[derive(Debug)]
pub struct RuntimeClient {
    // This is a silly implementation of a thread-safe runtime client because it uses a global
    // mutex to do any work. A better implementation would avoid locking and make each thread have
    // an independently operating client. However, this is a nice and simple starting point.
    runtime: Mutex<Runtime>,
}

impl RuntimeClient {
    pub(crate) fn new(runtime: Runtime) -> Self {
        Self {
            runtime: Mutex::new(runtime),
        }
    }

    /// Spawns a task to execute a future on any worker thread, creating the future via closure.
    pub fn spawn_on_any<FN, F, R>(&self, future_fn: FN) -> RemoteJoinHandle<R>
    where
        FN: FnOnce() -> F + Send + 'static,
        F: Future<Output = R> + 'static,
        R: Send + 'static,
    {
        let started = Instant::now();

        // Just because we are spawning a future on another thread does not mean it has to be a
        // thread-safe future (although the return value has to be). Therefore, we kajigger it
        // around via a remote join handle from the same thread, to allow a single-threaded future
        // to execute, as long as the closure that creates it is thread-safe.
        let thread_safe_wrapper_future = async move {
            REMOTE_SPAWN_DELAY.with(|x| x.observe(started.elapsed().as_secs_f64()));

            let join_handle: RemoteJoinHandle<R> = crate::rt::spawn(future_fn()).into();
            join_handle.await
        };

        let task = RemoteTask::new(thread_safe_wrapper_future);
        let join_handle = task.join_handle();

        let runtime = self.runtime.lock().expect(constants::POISONED_LOCK);
        let worker_index = next_async_worker(runtime.async_command_txs.len());

        runtime.async_command_txs[worker_index]
            .send(AsyncAgentCommand::EnqueueTask {
                erased_task: Box::pin(task),
            })
            .expect("runtime agent thread terminated unexpectedly");

        // Wake up the agent if it might be sleeping and waiting for I/O.
        runtime.async_io_wakers[worker_index].wake();

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

        let task = move || result_box_tx.set(f());

        let runtime = self.runtime.lock().expect(constants::POISONED_LOCK);
        let worker_index = next_sync_worker(runtime.sync_command_txs.len());

        runtime.sync_command_txs[worker_index]
            .send(SyncAgentCommand::ExecuteTask {
                erased_task: Box::new(task),
            })
            .expect("runtime agent thread terminated unexpectedly");

        RemoteJoinHandle::new(result_box_rx)
    }

    /// Commands the runtime to stop processing tasks and shut down. Safe to call multiple times.
    ///
    /// This returns immediately. To wait for the runtime to stop, use `wait()`.
    pub fn stop(&self) {
        let runtime = self.runtime.lock().expect(constants::POISONED_LOCK);

        for tx in &runtime.async_command_txs {
            // We ignore the return value because if the worker has already stopped, the channel
            // may be closed in which case the send may simply fail.
            _ = tx.send(AsyncAgentCommand::Terminate);
        }

        for tx in &runtime.sync_command_txs {
            // We ignore the return value because if the worker has already stopped, the channel
            // may be closed in which case the send may simply fail.
            _ = tx.send(crate::rt::sync_agent::SyncAgentCommand::Terminate);
        }
    }

    /// Returns `true` if the runtime has stopped (i.e. calling `wait()` would not block).
    ///
    /// # Panics
    ///
    /// If called after `wait()`.
    pub fn is_stopped(&self) -> bool {
        let runtime = self.runtime.lock().expect(constants::POISONED_LOCK);

        for join_handle in runtime
            .join_handles
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
        let join_handles = self.get_join_handles();

        for join_handle in join_handles {
            join_handle.join().expect("worker thread panicked");
        }
    }

    fn get_join_handles(&self) -> Box<[thread::JoinHandle<()>]> {
        let mut runtime = self.runtime.lock().expect(constants::POISONED_LOCK);

        runtime
            .join_handles
            .take()
            .expect("RuntimeClient::wait() called multiple times")
    }
}

// Basic round-robin implementation for distributing work across workers.
thread_local! {
    static NEXT_ASYNC_WORKER_INDEX: Cell<usize> = const { Cell::new(0) };
    static NEXT_SYNC_WORKER_INDEX: Cell<usize> = const { Cell::new(0) };
}

fn next_async_worker(max: usize) -> usize {
    let next = NEXT_ASYNC_WORKER_INDEX.get();
    NEXT_ASYNC_WORKER_INDEX.set((next + 1) % max);
    next
}

fn next_sync_worker(max: usize) -> usize {
    let next = NEXT_SYNC_WORKER_INDEX.get();
    NEXT_SYNC_WORKER_INDEX.set((next + 1) % max);
    next
}

thread_local! {
    static REMOTE_SPAWN_DELAY: Event = EventBuilder::new()
        .name("rt_remote_spawn_delay_seconds")
        .buckets(GENERAL_SECONDS_BUCKETS)
        .build()
        .unwrap();
}
