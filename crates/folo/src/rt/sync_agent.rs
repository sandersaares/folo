use super::ErasedSyncTask;
use crate::{
    constants::GENERAL_MILLISECONDS_BUCKETS,
    metrics::{self, Event, EventBuilder, Magnitude, ReportPage},
};
use crossbeam::{channel, queue::SegQueue};
use std::{fmt::Debug, sync::Arc};
use tracing::{event, Level};

#[derive(Debug)]
pub struct SyncAgent {
    command_rx: channel::Receiver<SyncAgentCommand>,
    metrics_tx: Option<channel::Sender<ReportPage>>,

    // When the command queue says "you may have a task", we check here. There might not always be
    // a task waiting for us because another sync agent sharing the same queue may have taken it.
    task_queue: Arc<SegQueue<ErasedSyncTask>>,

    // When the command queue says "you may have a task", we check here. There might not always be
    // a task waiting for us because another sync agent sharing the same queue may have taken it.
    // Tasks in this queue are executed first, over `task_queue`. This is usually because they are
    // of a "beneficial" nature such as releasing resources, so doing them first will help the
    // process overall work more efficiently. These tasks are also executed even when we are
    // shutting down because they may be used to release critical resources that are blocking
    // shutdown.
    priority_task_queue: Arc<SegQueue<ErasedSyncTask>>,
}

impl SyncAgent {
    pub fn new(
        command_rx: channel::Receiver<SyncAgentCommand>,
        metrics_tx: Option<channel::Sender<ReportPage>>,
        task_queue: Arc<SegQueue<ErasedSyncTask>>,
        priority_task_queue: Arc<SegQueue<ErasedSyncTask>>,
    ) -> Self {
        Self {
            command_rx,
            metrics_tx,
            task_queue,
            priority_task_queue,
        }
    }

    pub fn run(&self) {
        event!(Level::TRACE, "Started");

        // We simply process commands one by one until we receive a terminate command.
        // There is a risk of a huge buildup of commands with a pending terminate at the very end
        // but we are not going to worry about that for now.
        while let Ok(command) =
            TASK_INTERVAL.with(|x| x.observe_duration_millis(|| self.command_rx.recv()))
        {
            match command {
                SyncAgentCommand::CheckForTasks => {
                    let Some(task) = self.next_task() else {
                        // Some other worker cleared the queue already.
                        continue;
                    };

                    TASKS.with(Event::observe_unit);
                    TASK_DURATION.with(|x| x.observe_duration_millis(|| (task)()));
                }
                SyncAgentCommand::Terminate => {
                    event!(
                        Level::TRACE,
                        "shutting down after executing high-priority tasks"
                    );
                    break;
                }
            }
        }

        // During shutdown, high priority tasks are still executed! This is because these will often
        // be cleanup tasks that are required to release resources owned by the operating system,
        // without which we cannot safely shut down (because the OS is holding references into our
        // memory).
        while let Some(task) = self.priority_task_queue.pop() {
            TASKS.with(Event::observe_unit);
            TASK_DURATION.with(|x| x.observe_duration_millis(|| (task)()));
        }

        event!(
            Level::TRACE,
            "shutdown completed - no high-priority tasks remaining"
        );

        if let Some(tx) = &self.metrics_tx {
            _ = tx.send(metrics::report_page());
        }
    }

    fn next_task(&self) -> Option<ErasedSyncTask> {
        LOW_PRIORITY_QUEUE_SIZE.with(|x| x.observe(self.task_queue.len() as i64));
        HIGH_PRIORITY_QUEUE_SIZE.with(|x| x.observe(self.priority_task_queue.len() as i64));

        self.priority_task_queue
            .pop()
            .or_else(|| self.task_queue.pop())
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

const QUEUE_SIZE_BUCKETS: &[Magnitude] = &[0, 1, 10, 100, 1000];

thread_local! {
    static TASKS: Event = EventBuilder::new()
        .name("rt_sync_tasks")
        .build()
        .unwrap();

    static TASK_DURATION: Event = EventBuilder::new()
        .name("rt_sync_task_duration_millis")
        .buckets(GENERAL_MILLISECONDS_BUCKETS)
        .build()
        .unwrap();

    static TASK_INTERVAL: Event = EventBuilder::new()
        .name("rt_sync_task_interval_millis")
        .buckets(GENERAL_MILLISECONDS_BUCKETS)
        .build()
        .unwrap();

    static LOW_PRIORITY_QUEUE_SIZE: Event = EventBuilder::new()
        .name("rt_sync_low_priority_queue_size")
        .buckets(QUEUE_SIZE_BUCKETS)
        .build()
        .unwrap();

    static HIGH_PRIORITY_QUEUE_SIZE: Event = EventBuilder::new()
        .name("rt_sync_high_priority_queue_size")
        .buckets(QUEUE_SIZE_BUCKETS)
        .build()
        .unwrap();

}
