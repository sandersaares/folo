use crate::{io, rt::async_agent::AsyncAgent};
use std::{cell::RefCell, rc::Rc};

/// Executes a closure that receives the current thread's async agent for the runtime that owns the
/// current thread. The agent provides low-level access to Folo runtime internals for this thread.
///
/// # Panics
///
/// Panics if the current thread is not an async worker thread owned by the Folo runtime.
pub fn with<F, R>(f: F) -> R
where
    F: FnOnce(&AsyncAgent) -> R,
{
    CURRENT_AGENT.with_borrow(|agent| {
        f(agent
            .as_ref()
            .expect("this thread is not an async worker thread owned by the Folo runtime"))
    })
}

/// Executes a closure that receives the current thread's I/O driver for the runtime that owns the
/// current thread. This is the mechanism used to start I/O operations. Only available on async
/// worker threads because only those threads can perform I/O using the Folo runtime.
///
/// # Panics
///
/// Panics if the current thread is not an async worker thread owned by the Folo runtime.
pub fn with_io<F, R>(f: F) -> R
where
    F: FnOnce(&mut io::Driver) -> R,
{
    CURRENT_AGENT.with_borrow(|agent| {
        f(&mut agent
            .as_ref()
            .expect("this thread is not an async worker thread owned by the Folo runtime")
            .io()
            .borrow_mut())
    })
}

/// Executes a closure that receives the current thread's I/O driver for the runtime that owns the
/// current thread. This is the mechanism used to start I/O operations. Only available on async
/// worker threads because only those threads can perform I/O using the Folo runtime.
pub fn try_with_io<F, R>(f: F) -> Option<R>
where
    F: FnOnce(&mut io::Driver) -> R,
{
    CURRENT_AGENT.with_borrow(|agent| agent.as_ref().map(|agent| f(&mut agent.io().borrow_mut())))
}

pub fn is_some() -> bool {
    CURRENT_AGENT.with_borrow(|agent| agent.is_some())
}

pub fn set(value: Rc<AsyncAgent>) {
    CURRENT_AGENT.with_borrow_mut(|agent| {
        if agent.is_some() {
            panic!("this thread is already registered to a Folo runtime");
        }

        *agent = Some(value);
    });
}

thread_local!(
    static CURRENT_AGENT: RefCell<Option<Rc<AsyncAgent>>> = const { RefCell::new(None) }
);
