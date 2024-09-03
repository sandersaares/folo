use crate::{io, rt::agent::Agent};
use std::{cell::RefCell, rc::Rc};

/// Executes a closure in the context of the current thread's agent for the Folo executor.
///
/// # Panics
///
/// Panics if the current thread is not owned by a Folo executor.
pub fn with<F, R>(f: F) -> R
where
    F: FnOnce(&Agent) -> R,
{
    CURRENT_AGENT.with_borrow(|current_agent| {
        f(current_agent
            .as_ref()
            .expect("this thread is not owned by a Folo executor"))
    })
}

/// Executes a closure in the context of the current thread's IO driver for the Folo executor.
///
/// # Panics
///
/// Panics if the current thread is not owned by a Folo executor.
pub fn with_io<F, R>(f: F) -> R
where
    F: FnOnce(&mut io::Driver) -> R,
{
    CURRENT_AGENT.with_borrow(|current_agent| {
        f(&mut current_agent
            .as_ref()
            .expect("this thread is not owned by a Folo executor")
            .io()
            .borrow_mut())
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
