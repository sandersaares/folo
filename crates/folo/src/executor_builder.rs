use crate::{
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
}

impl ExecutorBuilder {
    pub fn new() -> Self {
        Self { worker_init: None }
    }

    pub fn worker_init<F>(mut self, f: F) -> Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.worker_init = Some(Arc::new(f));
        self
    }

    pub fn build(self) -> Result<Arc<ExecutorClient>> {
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
