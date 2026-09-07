//! Facade over transport PAL implementations.

use std::fmt;
use std::time::Duration;

use crate::pal::error::PalError;
use crate::pal::ids::{ConnId, ListenerId};
#[cfg(test)]
use crate::pal::transport::MemoryTransport;
use crate::pal::transport::Transport;
use crate::pal::transport::windows::BuildTargetTransport;
use crate::protocol::Message;

/// Dispatches transport calls to the real PAL or an in-memory test transport.
#[derive(Clone)]
pub(crate) enum TransportFacade {
    /// Real platform implementation.
    Target(&'static BuildTargetTransport),
    /// In-memory transport for unit tests.
    #[cfg(test)]
    Memory(MemoryTransport),
}

static TARGET: BuildTargetTransport = BuildTargetTransport;

#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg_attr(test, mutants::skip)]
impl fmt::Debug for TransportFacade {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Target(_) => f.debug_struct(stringify!(Target)).finish(),
            #[cfg(test)]
            Self::Memory(_) => f.debug_struct(stringify!(Memory)).finish(),
        }
    }
}

impl TransportFacade {
    pub(crate) const fn target() -> Self {
        Self::Target(&TARGET)
    }

    #[cfg(test)]
    // Facade pass-through logic is not worth testing.
    #[cfg_attr(coverage_nightly, coverage(off))]
    #[cfg_attr(test, mutants::skip)]
    pub(crate) fn from_memory(transport: MemoryTransport) -> Self {
        Self::Memory(transport)
    }
}

#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg_attr(test, mutants::skip)]
impl Transport for TransportFacade {
    fn listen(&self, name: &str) -> Result<ListenerId, PalError> {
        match self {
            Self::Target(inner) => inner.listen(name),
            #[cfg(test)]
            Self::Memory(inner) => inner.listen(name),
        }
    }

    fn accept(&self, listener: ListenerId) -> Result<ConnId, PalError> {
        match self {
            Self::Target(inner) => inner.accept(listener),
            #[cfg(test)]
            Self::Memory(inner) => inner.accept(listener),
        }
    }

    fn accept_timeout(&self, listener: ListenerId, timeout: Duration) -> Result<ConnId, PalError> {
        match self {
            Self::Target(inner) => inner.accept_timeout(listener, timeout),
            #[cfg(test)]
            Self::Memory(inner) => inner.accept_timeout(listener, timeout),
        }
    }

    fn connect(&self, name: &str, timeout: Duration) -> Result<ConnId, PalError> {
        match self {
            Self::Target(inner) => inner.connect(name, timeout),
            #[cfg(test)]
            Self::Memory(inner) => inner.connect(name, timeout),
        }
    }

    fn send(&self, conn: ConnId, message: &Message) -> Result<(), PalError> {
        match self {
            Self::Target(inner) => inner.send(conn, message),
            #[cfg(test)]
            Self::Memory(inner) => inner.send(conn, message),
        }
    }

    fn recv(&self, conn: ConnId) -> Result<Message, PalError> {
        match self {
            Self::Target(inner) => inner.recv(conn),
            #[cfg(test)]
            Self::Memory(inner) => inner.recv(conn),
        }
    }

    fn recv_timeout(&self, conn: ConnId, timeout: Duration) -> Result<Message, PalError> {
        match self {
            Self::Target(inner) => inner.recv_timeout(conn, timeout),
            #[cfg(test)]
            Self::Memory(inner) => inner.recv_timeout(conn, timeout),
        }
    }

    fn disconnect(&self, conn: ConnId) {
        match self {
            Self::Target(inner) => inner.disconnect(conn),
            #[cfg(test)]
            Self::Memory(inner) => inner.disconnect(conn),
        }
    }

    fn close_listener(&self, listener: ListenerId) {
        match self {
            Self::Target(inner) => inner.close_listener(listener),
            #[cfg(test)]
            Self::Memory(inner) => inner.close_listener(listener),
        }
    }

    fn pipe_name(&self, nonce: &str) -> String {
        match self {
            Self::Target(inner) => inner.pipe_name(nonce),
            #[cfg(test)]
            Self::Memory(inner) => inner.pipe_name(nonce),
        }
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::hint::black_box;

    use super::*;

    #[test]
    fn target_constructor_executes_at_runtime() {
        assert!(matches!(
            black_box(TransportFacade::target()),
            TransportFacade::Target(_)
        ));
    }

    #[test]
    fn from_memory_dispatches_pipe_name() {
        let transport = MemoryTransport::new();
        let facade = TransportFacade::from_memory(transport);
        assert_eq!(facade.pipe_name("n"), "memory:n");
        _ = format!("{facade:?}");
    }
}
