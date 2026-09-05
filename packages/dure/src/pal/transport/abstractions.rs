//! Client-supervisor transport PAL.

use std::fmt;
use std::time::Duration;

use crate::pal::error::PalError;
use crate::pal::ids::{ConnId, ListenerId};
use crate::protocol::Message;

/// Byte-stream between one supervisor and its attaching clients.
///
/// Steal is "accept a new connection while an old one still exists."
/// Ref: docs/implementation.md, "PAL slicing"; docs/transport.md.
#[cfg_attr(test, mockall::automock)]
pub(crate) trait Transport: Send + Sync + fmt::Debug + 'static {
    /// Create a first-instance listener for `name`.
    fn listen(&self, name: &str) -> Result<ListenerId, PalError>;

    /// Block until a client connects.
    fn accept(&self, listener: ListenerId) -> Result<ConnId, PalError>;

    /// Block until a client connects or `timeout` elapses.
    fn accept_timeout(&self, listener: ListenerId, timeout: Duration) -> Result<ConnId, PalError>;

    /// Connect to `name`, failing with [`crate::pal::error::PalErrorKind::Timeout`]
    /// if the wait elapses.
    fn connect(&self, name: &str, timeout: Duration) -> Result<ConnId, PalError>;

    /// Send one framed message.
    fn send(&self, conn: ConnId, message: &Message) -> Result<(), PalError>;

    /// Receive one framed message, blocking until one arrives or the peer drops.
    fn recv(&self, conn: ConnId) -> Result<Message, PalError>;

    /// Receive one framed message, failing if `timeout` elapses first.
    fn recv_timeout(&self, conn: ConnId, timeout: Duration) -> Result<Message, PalError>;

    /// End the connection, releasing work blocked on it at both ends.
    ///
    /// A send or receive this side already has in flight is aborted and reports
    /// a failure rather than waiting for a peer that will never answer. Shutdown
    /// paths depend on that: a supervisor abandoning a client that stopped
    /// reading has no other way to free the thread blocked writing to it.
    fn disconnect(&self, conn: ConnId);

    /// Stop accepting. Unblocks a thread waiting in [`Transport::accept`].
    fn close_listener(&self, listener: ListenerId);

    /// Build a per-session pipe name containing `nonce`.
    fn pipe_name(&self, nonce: &str) -> String;
}
