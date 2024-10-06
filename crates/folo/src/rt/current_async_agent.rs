use crate::{io, rt::async_agent::AsyncAgent};
use std::{cell::OnceCell, rc::Rc};

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
    CURRENT_AGENT.with(|agent| {
        f(agent
            .get()
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
    CURRENT_AGENT.with(|agent| {
        agent
            .get()
            .expect("this thread is not an async worker thread owned by the Folo runtime")
            .with_io(f)
    })
}

/// Executes a closure that receives the multithreaded I/O driver for the runtime that owns the
/// current thread. This is the mechanism used to start I/O operations. Only available on async
/// worker threads because only those threads can perform I/O using the Folo runtime.
///
/// # Panics
///
/// Panics if the current thread is not an async worker thread owned by the Folo runtime.
pub fn with_io_shared<F, R>(f: F) -> R
where
    F: FnOnce(&io::DriverShared) -> R,
{
    CURRENT_AGENT.with(|agent| {
        agent
            .get()
            .expect("this thread is not an async worker thread owned by the Folo runtime")
            .with_io_shared(f)
    })
}

/// Executes a closure that receives the current thread's I/O driver for the runtime that owns the
/// current thread. This is the mechanism used to start I/O operations. Only available on async
/// worker threads because only those threads can perform I/O using the Folo runtime.
pub fn try_with_io<F, R>(f: F) -> Option<R>
where
    F: FnOnce(&mut io::Driver) -> R,
{
    CURRENT_AGENT.with(|agent| agent.get().map(|agent| agent.with_io(f)))
}

pub fn is_some() -> bool {
    CURRENT_AGENT.with(|agent| agent.get().is_some())
}

pub fn set(value: Rc<AsyncAgent>) {
    CURRENT_AGENT.with(|agent| {
        agent
            .set(value)
            .expect("this thread is already registered to a Folo runtime");
    });
}

thread_local!(
    static CURRENT_AGENT: OnceCell<Rc<AsyncAgent>> = OnceCell::new();
);
