use super::{
    current_sync_agent,
    sync_agent::{SyncAgent, SyncAgentCommand},
};
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
use concurrent_queue::ConcurrentQueue;
use std::{
    collections::HashMap,
    fmt::{self, Debug, Formatter},
    rc::Rc,
    sync::{mpsc, Arc},
    thread,
};
use tracing::{event, Level};

/// The thing with synchronous worker threads is that they often get blocked and spend time doing
/// essentially nothing due to offloading blocking I/O onto these threads. Therefore, we spawn many
/// of them to ensure that we can keep processing synchronous work when a large batch comes in.
/// In the future we might replace this with a more dynamically sizing thread pool but for now the
/// fixed size might be acceptable.
const SYNC_WORKERS_PER_PROCESSOR: usize = 1;

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

        let async_worker_count = processor_count;
        let sync_worker_count = SYNC_WORKERS_PER_PROCESSOR * processor_count;

        event!(Level::INFO, processor_count);

        let worker_init = self.worker_init.unwrap_or(Arc::new(|| {}));

        let mut join_handles = Vec::with_capacity(sync_worker_count + async_worker_count);

        // # Async workers

        let mut async_command_txs = Vec::with_capacity(async_worker_count);
        let mut async_start_txs = Vec::with_capacity(async_worker_count);
        let mut async_ready_rxs = Vec::with_capacity(async_worker_count);

        for worker_index in 0..async_worker_count {
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

            let processor_id = processor_ids[worker_index];

            let join_handle = thread::spawn(move || {
                (worker_init)();

                let agent = Rc::new(AsyncAgent::new(command_rx, metrics_tx, processor_id));

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

                core_affinity::set_for_current(processor_id);

                current_async_agent::set(Rc::clone(&agent));
                current_runtime::set(start.runtime_client);

                agent.run();
            });

            join_handles.push(join_handle);
        }

        let mut async_io_wakers = Vec::with_capacity(async_worker_count);

        for start_ack_rx in async_ready_rxs {
            let start_ack = start_ack_rx
                .recv()
                .expect("async worker thread failed before even starting");

            async_io_wakers.push(start_ack.io_waker);
        }

        // # Sync workers

        let mut sync_command_txs_by_processor = HashMap::new();
        let mut sync_start_txs = Vec::with_capacity(sync_worker_count);
        let mut sync_ready_rxs = Vec::with_capacity(sync_worker_count);

        let mut sync_task_queues_by_processor = HashMap::new();

        for processor_id in &processor_ids {
            // There is a single queue of synchronous tasks per processor, shared by all the sync
            // workers assigned to that processor, to try balance out the load given that these may
            // often block for unequal amounts of time and end up imbalanced.
            let sync_task_queue = Arc::new(ConcurrentQueue::unbounded());
            sync_task_queues_by_processor.insert(*processor_id, Arc::clone(&sync_task_queue));

            for _ in 0..SYNC_WORKERS_PER_PROCESSOR {
                let processor_id = processor_id.clone();

                let (start_tx, start_rx) = oneshot::channel::<AgentStartArguments>();
                sync_start_txs.push(start_tx);

                let (ready_tx, ready_rx) = oneshot::channel::<SyncAgentReady>();
                sync_ready_rxs.push(ready_rx);

                let sync_command_txs = sync_command_txs_by_processor
                    .entry(processor_id)
                    .or_insert_with(|| Vec::with_capacity(sync_worker_count));

                let (command_tx, command_rx) = mpsc::channel::<SyncAgentCommand>();
                sync_command_txs.push(command_tx);

                let worker_init = worker_init.clone();

                let metrics_tx = match self.metrics_tx {
                    Some(ref tx) => Some(tx.clone()),
                    None => None,
                };

                let sync_task_queue = Arc::clone(&sync_task_queue);

                let join_handle = thread::spawn(move || {
                    (worker_init)();

                    let agent = Rc::new(SyncAgent::new(command_rx, metrics_tx, sync_task_queue));

                    // Signal that we are ready to start.
                    ready_tx
                        .send(SyncAgentReady {})
                        .expect("runtime startup process failed in infallible code");

                    // We first wait for the startup signal, which indicates that all agents have been
                    // created and registered with the runtime, and the runtime is ready to be used.
                    let start = start_rx
                        .recv()
                        .expect("runtime startup process failed in infallible code");

                    core_affinity::set_for_current(processor_id);

                    current_sync_agent::set(Rc::clone(&agent));
                    current_runtime::set(start.runtime_client);

                    agent.run();
                });

                join_handles.push(join_handle);
            }
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
            async_io_wakers: async_io_wakers.into_boxed_slice(),

            sync_command_txs_by_processor: sync_command_txs_by_processor
                .into_iter()
                .map(|(k, v)| (k, v.into_boxed_slice()))
                .collect(),
            sync_task_queues_by_processor,

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
        for tx in async_start_txs {
            tx.send(AgentStartArguments {
                runtime_client: Arc::clone(&client),
            })
            .expect("runtime async agent thread failed before it could be started");
        }

        for tx in sync_start_txs {
            tx.send(AgentStartArguments {
                runtime_client: Arc::clone(&client),
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
}

#[derive(thiserror::Error, Debug)]
#[error("failed to build the runtime for some mysterious reason")]
pub struct Error {}

pub type Result<T> = std::result::Result<T, Error>;
