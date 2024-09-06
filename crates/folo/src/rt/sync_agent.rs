use super::ErasedSyncTask;
use crate::metrics::{self, Event, EventBuilder, ReportPage};
use std::{
    fmt::{self, Debug, Formatter},
    sync::mpsc,
};
use tracing::{event, Level};

pub struct SyncAgent {
    command_rx: mpsc::Receiver<SyncAgentCommand>,
    metrics_tx: Option<mpsc::Sender<ReportPage>>,
}

impl SyncAgent {
    pub fn new(
        command_rx: mpsc::Receiver<SyncAgentCommand>,
        metrics_tx: Option<mpsc::Sender<ReportPage>>,
    ) -> Self {
        Self {
            command_rx,
            metrics_tx,
        }
    }

    pub fn run(&self) {
        event!(Level::TRACE, "Started");

        // We simply process commands one by one until we receive a terminate command.
        // There is a risk of a huge buildup of commands with a pending terminate at the very end
        // but we are not going to worry about that for now.
        while let Ok(command) = self.command_rx.recv() {
            match command {
                SyncAgentCommand::ExecuteTask { erased_task } => {
                    event!(Level::TRACE, "ExecuteTask");
                    TASKS.with(Event::observe_unit);
                    (erased_task)();
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

impl Debug for SyncAgent {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Agent")
            .field("command_rx", &self.command_rx)
            .field("metrics_tx", &self.metrics_tx)
            .finish()
    }
}

pub enum SyncAgentCommand {
    ExecuteTask {
        erased_task: ErasedSyncTask,
    },

    /// Shuts down the worker thread immediately, without waiting for any pending operations to
    /// complete. The worker will still complete the current task and perform necessary cleanup
    /// to avoid resource leaks, which may take some time.
    Terminate,
}

impl Debug for SyncAgentCommand {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::ExecuteTask { .. } => write!(f, "EnqueueTask"),
            Self::Terminate => write!(f, "Terminate"),
        }
    }
}

thread_local! {
    static TASKS: Event = EventBuilder::new()
        .name("rt_sync_tasks")
        .build()
        .unwrap();
}
