use crate::agent::AgentCommand;
use std::{sync::mpsc, thread};

pub(crate) struct Executor {
    pub agent_command_txs: Box<[mpsc::Sender<AgentCommand>]>,

    // This is None if `.wait()` has already been called - the field can be consumed only once.
    pub agent_join_handles: Option<Box<[thread::JoinHandle<()>]>>,
}
