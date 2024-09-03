use crate::{io, rt::agent::Agent};
use std::{cell::RefCell, rc::Rc};

/// Executes a closure that receives the current thread's agent for the runtime that owns the
/// current thread. The agent provides low-level access to Folo runtime internals for this thread.
///
/// # Panics
///
/// Panics if the current thread is not owned by the Folo runtime.
pub fn with<F, R>(f: F) -> R
where
    F: FnOnce(&Agent) -> R,
{
    CURRENT_AGENT.with_borrow(|agent| {
        f(agent
            .as_ref()
            .expect("this thread is not owned by the Folo runtime"))
    })
}

/// Executes a closure that receives the current thread's I/O driver for the runtime that owns the
/// current thread. This is the mechanism used to start I/O operations.
///
/// # Panics
///
/// Panics if the current thread is not owned by the Folo runtime.
pub fn with_io<F, R>(f: F) -> R
where
    F: FnOnce(&mut io::Driver) -> R,
{
    CURRENT_AGENT.with_borrow(|agent| {
        f(&mut agent
            .as_ref()
            .expect("this thread is not owned by the Folo runtime")
            .io()
            .borrow_mut())
    })
}

pub fn set(value: Rc<Agent>) {
    CURRENT_AGENT.with_borrow_mut(|agent| {
        if agent.is_some() {
            panic!("this thread is already registered to a Folo runtime");
        }

        *agent = Some(value);
    });
}

thread_local!(
    static CURRENT_AGENT: RefCell<Option<Rc<Agent>>> = const { RefCell::new(None) }
);
