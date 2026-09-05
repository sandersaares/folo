//! Local console PAL used by the client process.

use std::fmt;

use crate::pal::error::PalError;
use crate::pal::ids::RelayLeaseId;
use crate::pal::pseudoconsole::WindowSize;

/// One console event observed while a client is attached.
///
/// The relay reads these for the whole life of the attached session, not just
/// during the handshake. Keys that are not characters — arrows, function keys,
/// modifier chords — are encoded by the console as VT sequences, so they arrive
/// in the same byte stream as typed text; a window change is the one event that
/// is not bytes at all.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ConsoleInput {
    /// The forwarded input byte stream, relayed as
    /// [`crate::protocol::Message::Input`].
    Bytes(Vec<u8>),
    /// Window size changed; forward as [`crate::protocol::Message::Resize`].
    Resize(WindowSize),
}

/// Detect a console, take it over for a relay, and exchange bytes.
///
/// Ref: docs/implementation.md, PAL slicing.
#[cfg_attr(test, mockall::automock)]
pub(crate) trait LocalConsole: Send + Sync + fmt::Debug + 'static {
    /// Whether this process has a console the relay can run on.
    ///
    /// The relay drives console input and console output, so an attach is
    /// possible only when both of them are consoles.
    fn has_console(&self) -> bool;

    /// Whether stdin can be used for an interactive id prompt.
    fn stdin_is_terminal(&self) -> bool;

    /// Take the console over for the duration of a relay.
    ///
    /// Switches it to a raw VT relay, converts its encoding, and suppresses the
    /// Ctrl+C handling that would otherwise act on a key belonging to the app.
    /// These are one operation because they are one takeover: the returned
    /// lease owns whatever this call changed and is the only thing that can
    /// change it back. A second lease is refused while one is outstanding.
    ///
    /// Ref: docs/implementation.md, "Console modes".
    fn begin_raw_relay(&self) -> Result<RelayLeaseId, PalError>;

    /// Hand the console back, undoing exactly what `lease` took over.
    ///
    /// Every recorded change is attempted even when an earlier one fails, so a
    /// console is never left half-restored in order to report an error sooner.
    fn end_raw_relay(&self, lease: RelayLeaseId) -> Result<(), PalError>;

    /// Current console size.
    fn window_size(&self) -> Result<WindowSize, PalError>;

    /// Blocking read of console input bytes or a window-size change.
    fn read_input(&self) -> Result<ConsoleInput, PalError>;

    /// Wake a blocked [`LocalConsole::read_input`] so its reader can stop.
    ///
    /// A relay reads the console from its own thread, and that read outlives
    /// the relay unless something ends it: a console that is handed back while
    /// a read is still outstanding would take the next thing the user types.
    /// Reading becomes possible again with the next takeover.
    fn cancel_input(&self) -> Result<(), PalError>;

    /// Write console output bytes.
    fn write_output(&self, data: &[u8]) -> Result<(), PalError>;

    /// Read one line from stdin for the resume-id prompt.
    fn read_prompt_line(&self) -> Result<String, PalError>;
}
