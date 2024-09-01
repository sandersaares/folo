use crate::runtime::agent::Agent;
use std::{cell::RefCell, rc::Rc};

/// Gets the current thread's agent for the Folo executor.
///
/// # Panics
///
/// Panics if the current thread is not owned by a Folo executor.
pub fn get() -> Rc<Agent> {
    CURRENT_AGENT.with_borrow(|current_agent| {
        Rc::clone(
            current_agent
                .as_ref()
                .expect("this thread is not owned by a Folo executor"),
        )
    })
}

pub fn set(executor: Rc<Agent>) {
    CURRENT_AGENT.with_borrow_mut(|current_agent| {
        if current_agent.is_some() {
            panic!("this thread is already registered to a Folo executor");
        }

        *current_agent = Some(executor);
    });
}

thread_local!(
    static CURRENT_AGENT: RefCell<Option<Rc<Agent>>> = const { RefCell::new(None) }
);
