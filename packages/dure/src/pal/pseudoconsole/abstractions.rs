//! Pseudoconsole PAL: create, resize, close, and byte handles.

use std::fmt;
use std::num::NonZero;

use crate::pal::error::PalError;
use crate::pal::ids::PtyId;

/// Console size applied to the app pseudoconsole.
///
/// Both dimensions are non-zero because a console of no width or no height is
/// not a console anyone can render into. Establishing that here is what lets
/// every layer below stop deciding what to do about a zero: without it, the
/// Windows implementations quietly repaired such a size while the in-memory one
/// kept it, so the same value meant different things depending on who held it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WindowSize {
    /// Width in columns.
    pub cols: NonZero<u16>,
    /// Height in rows.
    pub rows: NonZero<u16>,
}

impl WindowSize {
    /// A size, if `cols` and `rows` describe a console that can be rendered.
    #[must_use]
    pub(crate) const fn new(cols: u16, rows: u16) -> Option<Self> {
        let (Some(cols), Some(rows)) = (NonZero::new(cols), NonZero::new(rows)) else {
            return None;
        };
        Some(Self { cols, rows })
    }
}

/// Create and relay a Windows pseudoconsole.
///
/// One pseudoconsole is one console the app is attached to: it has a size, it
/// carries the app's input and output as bytes, and it ends in two steps so a
/// reader can finish the app's output before the console is released.
/// Ref: docs/implementation.md, "PAL slicing"; docs/console.md.
#[cfg_attr(test, mockall::automock)]
pub(crate) trait Pseudoconsole: Send + Sync + fmt::Debug + 'static {
    /// Create a pseudoconsole with the given initial size.
    fn create(&self, size: WindowSize) -> Result<PtyId, PalError>;

    /// Apply a new size to the app console.
    fn resize(&self, pty: PtyId, size: WindowSize) -> Result<(), PalError>;

    /// Write bytes to the app's console input. Must not send EOF on client drop.
    fn write_input(&self, pty: PtyId, data: &[u8]) -> Result<(), PalError>;

    /// Read bytes from the app's console output. Blocks until some data arrives.
    ///
    /// `None` is the end of the app's output, and the only clean way for the
    /// stream to end. An error is a failed read and leaves the output
    /// incomplete, which callers must not mistake for the app having finished
    /// writing.
    fn read_output(&self, pty: PtyId) -> Result<Option<Vec<u8>>, PalError>;

    /// End the app console, leaving its remaining output readable.
    ///
    /// Reads keep delivering what the app already wrote and then report the end
    /// of the stream, so a reader can finish the output before the session
    /// reports the exit. The pseudoconsole is released by `close`.
    fn finish(&self, pty: PtyId);

    /// Release the pseudoconsole, abandoning output nobody has read yet.
    fn close(&self, pty: PtyId);
}
