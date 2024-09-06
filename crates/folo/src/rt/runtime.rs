use super::sync_agent::SyncAgentCommand;
use crate::{io::IoWaker, rt::async_agent::AsyncAgentCommand};
use std::{sync::mpsc, thread};

#[derive(Debug)]
pub(crate) struct Runtime {
    pub async_command_txs: Box<[mpsc::Sender<AsyncAgentCommand>]>,
    pub sync_command_txs: Box<[mpsc::Sender<SyncAgentCommand>]>,

    pub async_io_wakers: Box<[IoWaker]>,

    // This is None if `.wait()` has already been called - the field can be consumed only once.
    pub join_handles: Option<Box<[thread::JoinHandle<()>]>>,
}
