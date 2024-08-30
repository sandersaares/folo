use std::{
    future::Future, sync::{
        mpsc::{self, Receiver, Sender},
        Mutex,
    }, thread
};

use crate::{engine, remote_join};

// For now, we use a hardcoded number of threads - the main point here is to verify that the design
// works with multiple threads. We do not (yet?) care about actually using threads optimally.
const THREAD_COUNT: usize = 2;

/// Owns a bunch of threads on which it runs engines for executing tasks. This is the "root" of the
/// Folf runtime, although it is largely hidden from the public API, which exposes free functions
/// that look up the current thread's assigned executor on each call, for optimal convenience.
///
/// This type is thread-safe.
pub(crate) struct Executor {
    async_workers: Vec<AsyncWorkerClient>,
}

impl Executor {
    pub fn new() -> Self {
        Self {
            async_workers: (0..THREAD_COUNT)
                .map(|_| AsyncWorkerClient::new())
                .collect(),
        }
    }

    /// Starts the executor and blocks until all scheduled tasks have completed. If you wish
    /// to terminate the executor before all tasks complete, you must call `stop()` on the executor.
    ///
    /// This function may only be called once.
    pub fn run(&self) {
        for worker in &self.async_workers {
            worker.join_handle().join().unwrap();
        }
    }

    /// Stops the executor. This signals the executor that no new tasks should be processed and
    /// that `run()` should exit as soon as possible, abandoning any tasks that are still in
    /// progress.
    ///
    /// This function may be called any number of times and returns immediately.
    pub fn stop(&self) {
        for worker in &self.async_workers {
            // We do not care about the result - it is OK if the worker is already stopped.
            _ = worker.command_sender.send(AsyncWorkerCommand::Terminate);
        }
    }

    /// Enqueues a task on any worker thread (the specific thread will be chosen by the executor).
    pub fn enqueue<F, R>(&self, future: F) -> crate::RemoteJoinHandle<R>
    where
        F: Future<Output = R> + Send + 'static,
        R: Send + 'static,
    {
        let (wrapped, join_handle) = remote_join::wrap_for_remote_join(future);

        // TODO: Pick worker, schedule wrapped task.
        join_handle
    }
}

/// This is the control surface of an async worker thread owned by the executor. This type is used
/// by the executor to communicate with the worker thread - the worker itself does not use this.
///
/// This type is thread-safe.
struct AsyncWorkerClient {
    // The join handle may only be obtained once, after which this becomes `None` and future
    // attempts to obtain the join handle will panic.
    join_handle: Mutex<Option<thread::JoinHandle<()>>>,

    command_sender: Sender<AsyncWorkerCommand>,
}

impl AsyncWorkerClient {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel::<AsyncWorkerCommand>();

        let join_handle = thread::spawn(move || {
            async_worker_entrypoint(rx);
        });

        Self {
            join_handle: Mutex::new(Some(join_handle)),
            command_sender: tx,
        }
    }

    pub fn join_handle(&self) -> thread::JoinHandle<()> {
        self.join_handle
            .lock()
            .expect("poisoned lock")
            .take()
            .expect("it is invalid to obtain multiple join handles from the same worker thread")
    }
}

enum AsyncWorkerCommand {
    /// Register a new task to be executed on this worker.
    // TODO: We should already erase the return type here, which has some implications.
    NewTask,

    /// Stop processing tasks and exit immediately.
    Terminate,
}

fn async_worker_entrypoint(command_receiver: Receiver<AsyncWorkerCommand>) {
    todo!();
}
