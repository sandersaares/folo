use crate::rt::agent::AgentCommand;
use std::{sync::mpsc, thread};

#[derive(Debug)]
pub(crate) struct Runtime {
    pub agent_command_txs: Box<[mpsc::Sender<AgentCommand>]>,

    // This is None if `.wait()` has already been called - the field can be consumed only once.
    pub agent_join_handles: Option<Box<[thread::JoinHandle<()>]>>,
}
