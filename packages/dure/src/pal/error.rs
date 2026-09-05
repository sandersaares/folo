//! PAL failure classification.
//!
//! Every PAL operation reports failure as a [`PalError`]: a [`PalErrorKind`]
//! that says which of the outcomes logic distinguishes occurred, and, where the
//! platform gave one, the underlying error as its source.

use std::error::Error;
use std::{fmt, io};

/// Failure produced by a PAL operation.
pub(crate) struct PalError {
    kind: PalErrorKind,
    source: Option<Box<dyn Error + Send + Sync>>,
}

/// Distinguishes PAL failures that logic handles differently.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub(crate) enum PalErrorKind {
    /// A bounded connect wait elapsed.
    Timeout,
    /// Job breakaway was denied.
    BreakawayDenied,
    /// Opening or querying a process handle failed.
    InspectFailed,
    /// The requested object does not exist.
    NotFound,
    /// The peer closed the connection.
    Disconnected,
    /// Any other platform failure.
    Other,
}

impl PalError {
    pub(crate) fn new(kind: PalErrorKind) -> Self {
        Self { kind, source: None }
    }

    /// A failure that keeps whatever the platform said about it.
    ///
    /// The source is erased rather than typed, because a slice of a Win32-backed
    /// PAL fails through several unrelated error types — process, console,
    /// filesystem, and text decoding — and collapsing them all to a kind leaves
    /// a maintainer with nothing to read but the call stack.
    pub(crate) fn with_source(
        kind: PalErrorKind,
        source: impl Into<Box<dyn Error + Send + Sync>>,
    ) -> Self {
        Self {
            kind,
            source: Some(source.into()),
        }
    }

    pub(crate) fn kind(&self) -> PalErrorKind {
        self.kind
    }

    pub(crate) fn from_io(error: io::Error) -> Self {
        let kind = match error.kind() {
            io::ErrorKind::BrokenPipe
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::UnexpectedEof => PalErrorKind::Disconnected,
            _ => PalErrorKind::Other,
        };
        Self::with_source(kind, error)
    }
}

// Error text is not an API contract.
#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg_attr(test, mutants::skip)]
impl fmt::Debug for PalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PalError")
            .field("kind", &self.kind)
            .field("source", &self.source)
            .finish()
    }
}

// Error text is not an API contract.
#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg_attr(test, mutants::skip)]
impl fmt::Display for PalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let label = match self.kind {
            PalErrorKind::Timeout => "timed out",
            PalErrorKind::BreakawayDenied => "breakaway denied",
            PalErrorKind::InspectFailed => "failed to inspect the process",
            PalErrorKind::NotFound => "not found",
            PalErrorKind::Disconnected => "disconnected",
            PalErrorKind::Other => "platform error",
        };
        f.write_str(label)
    }
}

// Source chaining is not an API contract.
#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg_attr(test, mutants::skip)]
impl Error for PalError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source.as_deref().map(|error| -> &(dyn Error + 'static) { error })
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;

    #[test]
    fn from_io_maps_broken_pipe_to_disconnected() {
        let error = PalError::from_io(io::Error::new(io::ErrorKind::BrokenPipe, "closed"));
        assert_eq!(error.kind(), PalErrorKind::Disconnected);
    }

    #[test]
    fn from_io_maps_other_kinds_to_other() {
        let error = PalError::from_io(io::Error::other("platform"));
        assert_eq!(error.kind(), PalErrorKind::Other);
    }

    #[test]
    fn a_platform_failure_keeps_what_the_platform_said() {
        let error = PalError::with_source(PalErrorKind::Other, io::Error::other("CreateProcessW"));
        assert!(
            error
                .source()
                .is_some_and(|source| source.to_string().contains("CreateProcessW"))
        );
    }
}
