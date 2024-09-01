use crate::runtime::{
    agent::{Agent, AgentCommand},
    current_agent, current_executor,
    executor::Executor,
    ExecutorClient,
};
use std::{
    fmt::{self, Debug, Formatter},
    rc::Rc,
    sync::{mpsc, Arc},
    thread,
};

// For now, we use a hardcoded number of workers - the main point here is to verify that the design
// works with multiple threads. We do not yet care about actually using threads optimally.
const ASYNC_WORKER_COUNT: usize = 2;

pub struct ExecutorBuilder {
    worker_init: Option<Arc<dyn Fn() + Send + Sync + 'static>>,
    ad_hoc_entrypoint: bool,
}

impl ExecutorBuilder {
    pub fn new() -> Self {
        Self {
            worker_init: None,
            ad_hoc_entrypoint: false,
        }
    }

    /// Registers a function to call when initializing every created worker thread.
    pub fn worker_init<F>(mut self, f: F) -> Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.worker_init = Some(Arc::new(f));
        self
    }

    /// Registers the Folo executor as the owner of the entrypoint thread. This may be useful for
    /// interoperability purposes when using custom entry points (such as benchmarking logic).
    ///
    /// In the scenarios where this is set, we are often using the executor in a transient manner,
    /// for example for every benchmark iteration. Because of this, when this is enabled, the
    /// builder first checks if an executor is already registered on the current thread and returns
    /// that instead of creating a new one. Note that in this case, all other builder options are
    /// ignored - it is assumed that you are calling the same builder multiple times with the same
    /// configuration.
    pub fn ad_hoc_entrypoint(mut self) -> Self {
        self.ad_hoc_entrypoint = true;
        self
    }

    pub fn build(self) -> Result<Arc<ExecutorClient>> {
        if self.ad_hoc_entrypoint {
            // With ad-hoc entrypoints we reuse the executor if it is already set.
            if let Some(executor) = current_executor::try_get() {
                return Ok(executor);
            }
        }

        let mut join_handles = Vec::with_capacity(ASYNC_WORKER_COUNT);
        let mut command_txs = Vec::with_capacity(ASYNC_WORKER_COUNT);
        let mut start_txs = Vec::with_capacity(ASYNC_WORKER_COUNT);

        let worker_init = self.worker_init.unwrap_or(Arc::new(|| {}));

        for _ in 0..ASYNC_WORKER_COUNT {
            let (command_tx, command_rx) = mpsc::channel::<AgentCommand>();
            let (start_tx, start_rx) = oneshot::channel::<AgentStartCommand>();
            start_txs.push(start_tx);
            command_txs.push(command_tx);

            let worker_init = worker_init.clone();

            let join_handle = thread::spawn(move || {
                (worker_init)();

                // We first wait for the startup signal, which indicates that all agents have been
                // created and registered with the executor, and the executor is ready to be used.
                let start = start_rx
                    .recv()
                    .expect("executor startup process failed in infallible code");

                let agent = Rc::new(Agent::new(command_rx));

                current_agent::set(Rc::clone(&agent));
                current_executor::set(start.executor_client);

                agent.run();
            });

            join_handles.push(join_handle);
        }

        // Now we have all the info we need to construct the executor and client. We do so and then
        // send a message to all the agents that they are now attached to an executor and can start
        // doing their job.

        // This is just a convenient package the executor client uses to organize things internally.
        let executor = Executor {
            agent_join_handles: Some(join_handles.into_boxed_slice()),
            agent_command_txs: command_txs.into_boxed_slice(),
        };

        let client = Arc::new(ExecutorClient::new(executor));

        // In most cases, the entrypoint thread is merely parked. However, for interoperability
        // purposes, the caller may wish to register the Folo executor as the owner of the
        // entrypoint thread, as well. This allows custom entrypoint logic to execute code
        // that calls `spawn_on_any()` to schedule work on the Folo executor, while not being truly
        // on a Folo owned thread.
        if self.ad_hoc_entrypoint {
            current_executor::set(Arc::clone(&client));
        }

        // Tell all the agents to start.
        for tx in start_txs {
            tx.send(AgentStartCommand {
                executor_client: Arc::clone(&client),
            })
            .expect("executor agent thread failed before it could be started");
        }

        // All the agents are now running and the executor is ready to be used.
        Ok(client)
    }
}

impl Default for ExecutorBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl Debug for ExecutorBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExecutorBuilder").finish()
    }
}

/// A signal that the executor has been initialized and agents are permitted to start.
#[derive(Debug)]
struct AgentStartCommand {
    executor_client: Arc<ExecutorClient>,
}

#[derive(thiserror::Error, Debug)]
#[error("failed to build the executor for some mysterious reason")]
pub struct Error {}

pub type Result<T> = std::result::Result<T, Error>;
