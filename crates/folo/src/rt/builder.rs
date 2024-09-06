use core_affinity::CoreId;
use tracing::{event, Level};

use crate::{
    io::IoWaker,
    metrics::ReportPage,
    rt::{
        async_agent::{AsyncAgent, AsyncAgentCommand},
        current_async_agent, current_runtime,
        runtime::Runtime,
        RuntimeClient,
    },
};
use std::{
    fmt::{self, Debug, Formatter},
    rc::Rc,
    sync::{mpsc, Arc},
    thread,
};

use super::{
    current_sync_agent,
    sync_agent::{SyncAgent, SyncAgentCommand},
};

pub struct RuntimeBuilder {
    worker_init: Option<Arc<dyn Fn() + Send + Sync + 'static>>,
    ad_hoc_entrypoint: bool,
    metrics_tx: Option<mpsc::Sender<ReportPage>>,
}

impl RuntimeBuilder {
    pub fn new() -> Self {
        Self {
            worker_init: None,
            ad_hoc_entrypoint: false,
            metrics_tx: None,
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

    /// Registers the Folo runtime as the owner of the entrypoint thread. This may be useful for
    /// interoperability purposes when using custom entry points (such as benchmarking logic).
    ///
    /// In the scenarios where this is set, we are often using the runtime in a transient manner,
    /// for example for every benchmark iteration. Because of this, when this is enabled, the
    /// builder first checks if a runtime is already registered on the current thread and returns
    /// that instead of creating a new one. Note that in this case, all other builder options are
    /// ignored - it is assumed that you are calling the same builder multiple times with the same
    /// configuration.
    pub fn ad_hoc_entrypoint(mut self) -> Self {
        self.ad_hoc_entrypoint = true;
        self
    }

    /// Sets the channel that is to receive the end-of-life metrics from the runtime.
    /// Each worker thread will send a report page to this channel when it is shutting down.
    pub fn metrics_tx(mut self, tx: mpsc::Sender<ReportPage>) -> Self {
        self.metrics_tx = Some(tx);
        self
    }

    pub fn build(self) -> Result<Arc<RuntimeClient>> {
        if self.ad_hoc_entrypoint {
            // With ad-hoc entrypoints we reuse the runtime if it is already set.
            if let Some(runtime) = current_runtime::try_get() {
                return Ok(runtime);
            }
        }

        let processor_ids =
            core_affinity::get_core_ids().expect("must always be able to identify processor IDs");

        // We will spawn one agent of each type (async + sync) for each processor.
        let processor_count = processor_ids.len();

        event!(Level::INFO, processor_count);

        let worker_init = self.worker_init.unwrap_or(Arc::new(|| {}));

        let mut join_handles = Vec::with_capacity(processor_count * 2);

        // # Async workers

        let mut async_command_txs = Vec::with_capacity(processor_count);
        let mut async_start_txs = Vec::with_capacity(processor_count);
        let mut async_ready_rxs = Vec::with_capacity(processor_count);

        for _ in 0..processor_count {
            let (start_tx, start_rx) = oneshot::channel::<AgentStartArguments>();
            async_start_txs.push(start_tx);

            let (ready_tx, ready_rx) = oneshot::channel::<AsyncAgentReady>();
            async_ready_rxs.push(ready_rx);

            let (command_tx, command_rx) = mpsc::channel::<AsyncAgentCommand>();
            async_command_txs.push(command_tx);

            let worker_init = worker_init.clone();

            let metrics_tx = match self.metrics_tx {
                Some(ref tx) => Some(tx.clone()),
                None => None,
            };

            let join_handle = thread::spawn(move || {
                (worker_init)();

                let agent = Rc::new(AsyncAgent::new(command_rx, metrics_tx));

                // Signal that we are ready to start.
                ready_tx
                    .send(AsyncAgentReady {
                        io_waker: agent.io().borrow().waker(),
                    })
                    .expect("runtime startup process failed in infallible code");

                // We first wait for the startup signal, which indicates that all agents have been
                // created and registered with the runtime, and the runtime is ready to be used.
                let start = start_rx
                    .recv()
                    .expect("runtime startup process failed in infallible code");

                core_affinity::set_for_current(start.processor_id);

                current_async_agent::set(Rc::clone(&agent));
                current_runtime::set(start.runtime_client);

                agent.run();
            });

            join_handles.push(join_handle);
        }

        let mut async_io_wakers = Vec::with_capacity(processor_count);

        for start_ack_rx in async_ready_rxs {
            let start_ack = start_ack_rx
                .recv()
                .expect("async worker thread failed before even starting");

            async_io_wakers.push(start_ack.io_waker);
        }

        // # Sync workers

        let mut sync_command_txs = Vec::with_capacity(processor_count);
        let mut sync_start_txs = Vec::with_capacity(processor_count);
        let mut sync_ready_rxs = Vec::with_capacity(processor_count);

        for _ in 0..processor_count {
            let (start_tx, start_rx) = oneshot::channel::<AgentStartArguments>();
            sync_start_txs.push(start_tx);

            let (ready_tx, ready_rx) = oneshot::channel::<SyncAgentReady>();
            sync_ready_rxs.push(ready_rx);

            let (command_tx, command_rx) = mpsc::channel::<SyncAgentCommand>();
            sync_command_txs.push(command_tx);

            let worker_init = worker_init.clone();

            let metrics_tx = match self.metrics_tx {
                Some(ref tx) => Some(tx.clone()),
                None => None,
            };

            let join_handle = thread::spawn(move || {
                (worker_init)();

                let agent = Rc::new(SyncAgent::new(command_rx, metrics_tx));

                // Signal that we are ready to start.
                ready_tx
                    .send(SyncAgentReady {})
                    .expect("runtime startup process failed in infallible code");

                // We first wait for the startup signal, which indicates that all agents have been
                // created and registered with the runtime, and the runtime is ready to be used.
                let start = start_rx
                    .recv()
                    .expect("runtime startup process failed in infallible code");

                core_affinity::set_for_current(start.processor_id);

                current_sync_agent::set(Rc::clone(&agent));
                current_runtime::set(start.runtime_client);

                agent.run();
            });

            join_handles.push(join_handle);
        }

        for start_ack_rx in sync_ready_rxs {
            _ = start_ack_rx
                .recv()
                .expect("sync worker thread failed before even starting");

            // For now we just want to make sure we see the ACK. No actual state fanster needed.
        }

        // # Start

        // Now we have all the info we need to construct the runtime and client. We do so and then
        // send a message to all the agents that they are now attached to a runtime and can start
        // doing their job.

        // This is just a convenient package the runtime client uses to organize things internally.
        let runtime = Runtime {
            async_command_txs: async_command_txs.into_boxed_slice(),
            sync_command_txs: sync_command_txs.into_boxed_slice(),

            async_io_wakers: async_io_wakers.into_boxed_slice(),

            join_handles: Some(join_handles.into_boxed_slice()),
        };

        let client = Arc::new(RuntimeClient::new(runtime));

        // In most cases, the entrypoint thread is merely parked. However, for interoperability
        // purposes, the caller may wish to register the Folo runtime as the owner of the
        // entrypoint thread, as well. This allows custom entrypoint logic to execute code
        // that calls `spawn_on_any()` to schedule work on the Folo runtime, while not being truly
        // on a Folo owned thread.
        if self.ad_hoc_entrypoint {
            current_runtime::set(Arc::clone(&client));
        }

        // Tell all the agents to start.
        for (index, tx) in async_start_txs.into_iter().enumerate() {
            tx.send(AgentStartArguments {
                runtime_client: Arc::clone(&client),
                processor_id: processor_ids[index],
            })
            .expect("runtime async agent thread failed before it could be started");
        }

        for (index, tx) in sync_start_txs.into_iter().enumerate() {
            tx.send(AgentStartArguments {
                runtime_client: Arc::clone(&client),
                processor_id: processor_ids[index],
            })
            .expect("runtime sync agent thread failed before it could be started");
        }

        // All the agents are now running and the runtime is ready to be used.
        Ok(client)
    }
}

impl Default for RuntimeBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl Debug for RuntimeBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("RuntimeBuilder").finish()
    }
}

/// A signal that an async agent is ready to start, providing inputs required for the runtime start.
#[derive(Debug)]
struct AsyncAgentReady {
    io_waker: IoWaker,
}

/// A signal that a sync agent is ready to start, providing inputs required for the runtime start.
#[derive(Debug)]
struct SyncAgentReady {}

/// A signal that the runtime has been initialized and agents are permitted to start,
/// providing relevant arguments to the agent.
#[derive(Debug)]
struct AgentStartArguments {
    runtime_client: Arc<RuntimeClient>,
    processor_id: CoreId,
}

#[derive(thiserror::Error, Debug)]
#[error("failed to build the runtime for some mysterious reason")]
pub struct Error {}

pub type Result<T> = std::result::Result<T, Error>;
