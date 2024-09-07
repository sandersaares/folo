use super::{sync_agent::SyncAgentCommand, ErasedSyncTask};
use crate::{io::IoWaker, rt::async_agent::AsyncAgentCommand};
use concurrent_queue::ConcurrentQueue;
use core_affinity::CoreId;
use std::{
    collections::HashMap,
    sync::{mpsc, Arc},
    thread,
};

#[derive(Debug)]
pub(crate) struct Runtime {
    pub async_command_txs: Box<[mpsc::Sender<AsyncAgentCommand>]>,
    pub async_io_wakers: Box<[IoWaker]>,

    // We often prefer to give work to the same processor, so we split
    // the sync command architecture up by the processor ID.
    pub sync_command_txs_by_processor: HashMap<CoreId, Box<[mpsc::Sender<SyncAgentCommand>]>>,
    pub sync_task_queues_by_processor: HashMap<CoreId, Arc<ConcurrentQueue<ErasedSyncTask>>>,

    // This is None if `.wait()` has already been called - the field can be consumed only once.
    pub join_handles: Option<Box<[thread::JoinHandle<()>]>>,
}
