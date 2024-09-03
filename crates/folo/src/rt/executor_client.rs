use crate::constants;
use crate::rt::{
    agent::AgentCommand, executor::Executor, remote_task::RemoteTask, RemoteJoinHandle,
};
use std::{cell::Cell, future::Future, sync::Mutex, thread};

/// The multithreaded entry point for the Folo executor, used for operations that affect more than
/// the current thread.
///
/// This type is thread-safe.
#[derive(Debug)]
pub struct ExecutorClient {
    // This is a silly implementation of a thread-safe executor client because it uses a global
    // mutex to do any work. A better implementation would avoid locking and make each thread have
    // an independently operating client. However, this is a nice and simple starting point.
    executor: Mutex<Executor>,
}

impl ExecutorClient {
    pub(crate) fn new(executor: Executor) -> Self {
        Self {
            executor: Mutex::new(executor),
        }
    }

    /// Spawns a task to execute a future on any worker thread, creating the future via closure.
    pub fn spawn_on_any<FN, F, R>(&self, future_fn: FN) -> RemoteJoinHandle<R>
    where
        FN: FnOnce() -> F + Send + 'static,
        F: Future<Output = R> + 'static,
        R: Send + 'static,
    {
        // Just because we are spawning a future on another thread does not mean it has to be a
        // thread-safe future (although the return value has to be). Therefore, we kajigger it
        // around via a remote join handle from the same thread, to allow a single-threaded future
        // to execute, as long as the closure that creates it is thread-safe.
        let thread_safe_wrapper_future = async {
            let join_handle: RemoteJoinHandle<R> = crate::rt::spawn(future_fn()).into();
            join_handle.await
        };

        let task = RemoteTask::new(thread_safe_wrapper_future);
        let join_handle = task.join_handle();

        let executor = self.executor.lock().expect(constants::POISONED_LOCK);
        let worker_index = random_worker_index(executor.agent_command_txs.len());

        executor.agent_command_txs[worker_index]
            .send(AgentCommand::EnqueueTask {
                erased_task: Box::pin(task),
            })
            .expect("executor agent thread terminated unexpectedly");

        join_handle
    }

    /// Commands the executor to stop processing tasks and shut down. Safe to call multiple times.
    ///
    /// This returns immediately. To wait for the executor to stop, use `wait()`.
    pub fn stop(&self) {
        let executor = self.executor.lock().expect(constants::POISONED_LOCK);

        for tx in &executor.agent_command_txs {
            tx.send(AgentCommand::Terminate)
                .expect("executor agent thread terminated unexpectedly");
        }
    }

    /// Waits for the executor to stop. Blocks the thread until all executor owned threads have
    /// terminated in response to a call to `stop()`. This can only be called once.
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
        let mut executor = self.executor.lock().expect(constants::POISONED_LOCK);

        executor
            .agent_join_handles
            .take()
            .expect("ExecutorClient::wait() called multiple times")
    }
}

// Basic round-robin implementation for distributing work across workers.
thread_local!(static NEXT_WORKER_INDEX: Cell<usize> = const { Cell::new(0) });

fn random_worker_index(max: usize) -> usize {
    let next = NEXT_WORKER_INDEX.get();
    NEXT_WORKER_INDEX.set((next + 1) % max);
    next
}
