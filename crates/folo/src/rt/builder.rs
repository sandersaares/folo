use std::collections::HashMap;
use std::fmt::{self, Debug, Formatter};
use std::rc::Rc;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::thread;

use crossbeam::channel;
use crossbeam::queue::SegQueue;
use tracing::{event, Level};

use super::sync_agent::{SyncAgent, SyncAgentCommand};
use super::{current_sync_agent, ErasedSyncTask};
use crate::io::{self, IoWaker};
use crate::metrics::ReportPage;
use crate::rt::async_agent::{AsyncAgent, AsyncAgentCommand};
use crate::rt::{current_async_agent, current_runtime, CoreClient, RuntimeClient};

/// The thing with synchronous worker threads is that they often get blocked and spend time doing
/// essentially nothing due to offloading blocking I/O onto these threads. Therefore, we spawn many
/// of them to ensure that we can keep processing synchronous work when a large batch comes in.
/// In the future we might replace this with a more dynamically sizing thread pool but for now the
/// fixed size might be acceptable.
const SYNC_WORKERS_PER_PROCESSOR: usize = 2;

struct ThreadStartResult<AgentReady, R> {
    join_handle: std::thread::JoinHandle<()>,
    start_tx: oneshot::Sender<AgentStartArguments>,
    ready_rx: oneshot::Receiver<AgentReady>,
    result: R,
}

pub struct RuntimeBuilder {
    worker_init: Arc<dyn Fn() + Send + Sync + 'static>,
    ad_hoc_entrypoint: bool,
    metrics_tx: Option<channel::Sender<ReportPage>>,
    max_processors: Option<usize>,
}

impl RuntimeBuilder {
    pub fn new() -> Self {
        Self {
            worker_init: Arc::new(|| {}),
            ad_hoc_entrypoint: false,
            metrics_tx: None,
            max_processors: None,
        }
    }

    /// Registers a function to call when initializing every created worker thread.
    pub fn worker_init<F>(mut self, f: F) -> Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.worker_init = Arc::new(f);
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
    pub fn metrics_tx(mut self, tx: channel::Sender<ReportPage>) -> Self {
        self.metrics_tx = Some(tx);
        self
    }

    /// Limits the number of processors the runtime will use. This may be useful in testing to get
    /// a closer look at some behavior without 99 different worker threads going wild. Not super
    /// valuable in real usage because it does not specify which processor (actually, it will use
    /// the first N).
    pub fn max_processors(mut self, max_processors: usize) -> Self {
        self.max_processors = Some(max_processors);
        self
    }

    fn start_async_agent(
        &self,
        processor_id: core_affinity::CoreId,
        io_shared: Arc<io::DriverShared>,
        worker_index: usize,
    ) -> std::io::Result<ThreadStartResult<AsyncAgentReady, channel::Sender<AsyncAgentCommand>>>
    {
        let worker_init = Arc::clone(&self.worker_init);
        let metrics_tx = self.metrics_tx.clone();
        let (start_tx, start_rx) = oneshot::channel::<AgentStartArguments>();
        let (ready_tx, ready_rx) = oneshot::channel::<AsyncAgentReady>();
        let (command_tx, command_rx) = channel::unbounded::<AsyncAgentCommand>();

        let join_handle = thread::Builder::new()
            .name(format!("async-{}", worker_index))
            .spawn(move || {
                worker_init();

                let agent = Rc::new(AsyncAgent::new(
                    command_rx,
                    metrics_tx,
                    io_shared,
                    processor_id,
                ));

                // Signal that we are ready to start.
                ready_tx
                    .send(AsyncAgentReady {
                        io_waker: agent.with_io(|io| io.waker()),
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
            })?;

        Ok(ThreadStartResult {
            join_handle,
            start_tx,
            ready_rx,
            result: command_tx,
        })
    }

    fn start_sync_agent(
        &self,
        processor_id: core_affinity::CoreId,
        worker_index: usize,
        task_queue: Arc<SegQueue<ErasedSyncTask>>,
        priority_task_queue: Arc<SegQueue<ErasedSyncTask>>,
    ) -> std::io::Result<ThreadStartResult<SyncAgentReady, channel::Sender<SyncAgentCommand>>> {
        let worker_init = Arc::clone(&self.worker_init);
        let metrics_tx = self.metrics_tx.clone();
        let (start_tx, start_rx) = oneshot::channel::<AgentStartArguments>();
        let (ready_tx, ready_rx) = oneshot::channel::<SyncAgentReady>();
        let (command_tx, command_rx) = channel::unbounded::<SyncAgentCommand>();

        let join_handle = thread::Builder::new()
            .name(format!("sync-{}-{}", processor_id.id, worker_index))
            .spawn(move || {
                (worker_init)();

                let agent = Rc::new(SyncAgent::new(
                    command_rx,
                    metrics_tx,
                    task_queue,
                    priority_task_queue,
                ));

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
            })?;

        Ok(ThreadStartResult {
            join_handle,
            start_tx,
            ready_rx,
            result: command_tx,
        })
    }

    pub fn build(self) -> io::Result<RuntimeClient> {
        if self.ad_hoc_entrypoint {
            // With ad-hoc entrypoints we reuse the runtime if it is already set.
            if let Some(runtime) = current_runtime::try_get() {
                return Ok(runtime);
            }
        }

        let mut processor_ids =
            core_affinity::get_core_ids().expect("must always be able to identify processor IDs");

        if let Some(max_processors) = self.max_processors {
            processor_ids.truncate(max_processors);
        }

        let processor_ids = processor_ids.into_boxed_slice();

        // We will spawn one agent of each type (async + sync) for each processor.
        let processor_count = processor_ids.len();

        let async_worker_count = processor_count;
        let sync_worker_count = SYNC_WORKERS_PER_PROCESSOR * processor_count;

        event!(Level::INFO, processor_count);

        let mut join_handles = Vec::with_capacity(sync_worker_count + async_worker_count);
        let mut core_processors = HashMap::new();

        // SAFETY: The shared I/O driver must be shut down only after all operations have been
        // shut down. The async worker agents guarantee this by ensuring they do not shut down
        // and release the Arc until the driver signals that it has become inert.
        let io_shared = Arc::new(unsafe { io::DriverShared::new() });

        // # Async workers & Sync workers

        let mut async_start_txs = Vec::with_capacity(async_worker_count);
        let mut sync_start_txs = Vec::with_capacity(sync_worker_count);

        for worker_index in 0..async_worker_count {
            let processor_id = processor_ids[worker_index];
            let ThreadStartResult {
                join_handle: async_join_handle,
                start_tx: async_start_tx,
                ready_rx: async_ready_rx,
                result: async_command_tx,
            } = self.start_async_agent(processor_id, Arc::clone(&io_shared), worker_index)?;

            async_start_txs.push(async_start_tx);
            join_handles.push(async_join_handle);

            // There is a single queue of synchronous tasks per processor, shared by all the sync
            // workers assigned to that processor, to try balance out the load given that these may
            // often block for unequal amounts of time and end up imbalanced.
            let sync_task_queue = Arc::new(SegQueue::new());

            // Same, but for higher-priority tasks (e.g. releasing resources).
            let sync_priority_task_queue = Arc::new(SegQueue::new());

            let mut sync_command_txs = Vec::with_capacity(sync_worker_count);
            let mut sync_ready_rxs = Vec::with_capacity(sync_worker_count);

            for worker_index in 0..SYNC_WORKERS_PER_PROCESSOR {
                let ThreadStartResult {
                    join_handle,
                    start_tx,
                    ready_rx,
                    result: command_tx,
                } = self.start_sync_agent(
                    processor_id,
                    worker_index,
                    Arc::clone(&sync_task_queue),
                    Arc::clone(&sync_priority_task_queue),
                )?;

                sync_start_txs.push(start_tx);
                sync_ready_rxs.push(ready_rx);

                sync_command_txs.push(command_tx);
                join_handles.push(join_handle);
            }

            let async_io_waker = async_ready_rx
                .recv()
                .expect("async worker thread failed before even starting")
                .io_waker;

            sync_ready_rxs.into_iter().for_each(|ready_rx| {
                ready_rx
                    .recv()
                    .expect("sync worker thread failed before even starting");
                // For now we just want to make sure we see the ACK. No actual state fanster needed.
            });

            let proc = CoreClient::new(
                processor_id,
                async_command_tx,
                async_io_waker,
                sync_command_txs.into_boxed_slice(),
                sync_task_queue,
                sync_priority_task_queue,
            );

            core_processors.insert(processor_id, proc);
        }

        // # Start

        // Now we have all the info we need to construct the runtime and client. We do so and then
        // send a message to all the agents that they are now attached to a runtime and can start
        // doing their job.

        let is_stopping = Arc::new(AtomicBool::new(false));

        let client = RuntimeClient::new(
            core_processors,
            processor_ids.clone(),
            join_handles.into_boxed_slice(),
            Arc::clone(&is_stopping),
        );

        // In most cases, the entrypoint thread is merely parked. However, for interoperability
        // purposes, the caller may wish to register the Folo runtime as the owner of the
        // entrypoint thread, as well. This allows custom entrypoint logic to execute code
        // that calls `spawn_on_any()` to schedule work on the Folo runtime, while not being truly
        // on a Folo owned thread.
        if self.ad_hoc_entrypoint {
            current_runtime::set(client.clone());
        }

        // Tell all the agents to start.
        for tx in async_start_txs {
            tx.send(AgentStartArguments {
                runtime_client: client.clone(),
            })
            .expect("runtime async agent thread failed before it could be started");
        }

        for tx in sync_start_txs {
            tx.send(AgentStartArguments {
                runtime_client: client.clone(),
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
    runtime_client: RuntimeClient,
}
