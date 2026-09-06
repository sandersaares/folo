//! What the supervisor could establish about its own lifetime.

use std::fmt;

/// What inspecting the supervisor's immediate job said about its lifetime.
///
/// Only the supervisor can ask this: Windows reports a process's job membership
/// to that process alone, and it exposes only the job the process is directly
/// in. An outer job that would also end the session therefore cannot be ruled
/// out by any of these answers (docs/job-breakaway.md), which is
/// why this names what was observed rather than promising an outcome.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LauncherTie {
    /// The immediate job is a kill-on-close job, so it ends this session when
    /// the launcher's job closes.
    Confirmed,
    /// The immediate job could not be inspected, so nothing was established.
    Unknown,
    /// No immediate job would end this session. An ancestor job still might.
    NoneDetected,
}

impl LauncherTie {
    /// Whether the user should be told the session may not survive.
    pub(crate) const fn warrants_warning(self) -> bool {
        !matches!(self, Self::NoneDetected)
    }
}

// The trace and warning wording is not a behavioral contract; whether a warning
// is emitted at all is, and `warrants_warning` carries that.
#[cfg_attr(coverage_nightly, coverage(off))]
#[cfg_attr(test, mutants::skip)]
impl fmt::Display for LauncherTie {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let note = match self {
            Self::Confirmed => "tied to the launcher, so it will not survive it",
            Self::Unknown => "could not be established",
            Self::NoneDetected => "no immediate tie to the launcher found",
        };
        f.write_str(note)
    }
}
