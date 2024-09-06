use crate::rt::sync_agent::SyncAgent;
use std::{cell::RefCell, rc::Rc};

/// Executes a closure that receives the current thread's sync agent for the runtime that owns the
/// current thread. The agent provides low-level access to Folo runtime internals for this thread.
///
/// # Panics
///
/// Panics if the current thread is not an sync worker thread owned by the Folo runtime.
pub fn with<F, R>(f: F) -> R
where
    F: FnOnce(&SyncAgent) -> R,
{
    CURRENT_AGENT.with_borrow(|agent| {
        f(agent
            .as_ref()
            .expect("this thread is not an sync worker thread owned by the Folo runtime"))
    })
}

pub fn set(value: Rc<SyncAgent>) {
    CURRENT_AGENT.with_borrow_mut(|agent| {
        if agent.is_some() {
            panic!("this thread is already registered to a Folo runtime");
        }

        *agent = Some(value);
    });
}

thread_local!(
    static CURRENT_AGENT: RefCell<Option<Rc<SyncAgent>>> = const { RefCell::new(None) }
);
