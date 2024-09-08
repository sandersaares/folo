use super::ErasedSyncTask;
use crate::{
    constants::GENERAL_LOW_PRECISION_SECONDS_BUCKETS,
    metrics::{self, Event, EventBuilder, ReportPage},
};
use concurrent_queue::ConcurrentQueue;
use std::{
    fmt::Debug,
    sync::{mpsc, Arc},
};
use tracing::{event, Level};

#[derive(Debug)]
pub struct SyncAgent {
    command_rx: mpsc::Receiver<SyncAgentCommand>,
    metrics_tx: Option<mpsc::Sender<ReportPage>>,

    // When the command queue says "you may have a task", we check here. There might not always be
    // a task waiting for us because another sync agent sharing the same queue may have taken it.
    task_queue: Arc<ConcurrentQueue<ErasedSyncTask>>,
}

impl SyncAgent {
    pub fn new(
        command_rx: mpsc::Receiver<SyncAgentCommand>,
        metrics_tx: Option<mpsc::Sender<ReportPage>>,
        task_queue: Arc<ConcurrentQueue<ErasedSyncTask>>,
    ) -> Self {
        Self {
            command_rx,
            metrics_tx,
            task_queue,
        }
    }

    pub fn run(&self) {
        event!(Level::TRACE, "Started");

        // We simply process commands one by one until we receive a terminate command.
        // There is a risk of a huge buildup of commands with a pending terminate at the very end
        // but we are not going to worry about that for now.
        while let Ok(command) = self.command_rx.recv() {
            match command {
                SyncAgentCommand::CheckForTasks => {
                    let Ok(task) = self.task_queue.pop() else {
                        // Some other worker cleared the queue already.
                        continue;
                    };

                    TASKS.with(Event::observe_unit);
                    TASK_DURATION.with(|x| x.observe_duration_low_precision(|| (task)()));
                }
                SyncAgentCommand::Terminate => {
                    event!(Level::TRACE, "Shutting down");
                    break;
                }
            }
        }

        event!(Level::TRACE, "shutdown completed");

        if let Some(tx) = &self.metrics_tx {
            _ = tx.send(metrics::report_page());
        }
    }
}

#[derive(Debug)]
pub enum SyncAgentCommand {
    /// Indicates that there may be new tasks available in the task queue. This command may be sent
    /// to multiple agents for the same task, so not every agent will find a task in the queue.
    CheckForTasks,

    /// Shuts down the worker thread immediately, without waiting for any pending operations to
    /// complete. The worker will still complete the current task and perform necessary cleanup
    /// to avoid resource leaks, which may take some time.
    Terminate,
}

thread_local! {
    static TASKS: Event = EventBuilder::new()
        .name("rt_sync_tasks")
        .build()
        .unwrap();

    static TASK_DURATION: Event = EventBuilder::new()
        .name("rt_sync_task_duration_seconds")
        .buckets(GENERAL_LOW_PRECISION_SECONDS_BUCKETS)
        .build()
        .unwrap();
}
