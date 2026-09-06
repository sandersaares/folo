//! Explanatory `--verbose` tracing.
//!
//! Ref: docs/design.md, "Diagnostics"; the workspace convention in
//! `docs/standalone-binaries.md` at the repository root.

use std::fmt::Arguments;

use crate::output::note_line;

/// Whether this invocation explains itself, and how.
///
/// Passed down to every command so a trace can state the inputs behind a
/// decision at the point the decision is made. Notes go to stderr, which keeps
/// them out of anything a caller pipes from stdout, and are worded so a reader
/// can reconstruct the reasoning rather than only learn the conclusion.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct Trace {
    enabled: bool,
}

impl Trace {
    pub(crate) fn new(enabled: bool) -> Self {
        Self { enabled }
    }

    /// Whether a caller should do the work of assembling a multi-line trace.
    pub(crate) fn is_enabled(self) -> bool {
        self.enabled
    }

    /// Writes one explanatory note when this trace is enabled.
    ///
    /// Callers reach this through [`trace!`], which is what keeps the arguments
    /// unevaluated on a quiet run; the check here is what makes a direct call
    /// behave the same way.
    // Trace wording is not a behavioral contract.
    #[cfg_attr(test, mutants::skip)]
    pub(crate) fn note(self, message: Arguments<'_>) {
        if self.enabled {
            note_line(format_args!("dure: {message}"));
        }
    }
}

/// Writes one explanatory note when tracing is on.
///
/// Takes `format!` arguments and evaluates none of them when tracing is off:
/// several call sites render paths and command lines, and a quiet run must not
/// pay to build text nobody reads.
macro_rules! trace {
    ($trace:expr, $($arg:tt)*) => {{
        let trace = $trace;
        if trace.is_enabled() {
            trace.note(::std::format_args!($($arg)*));
        }
    }};
}

pub(crate) use trace;

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;

    #[test]
    fn quiet_by_default() {
        assert!(!Trace::default().is_enabled());
    }

    #[test]
    fn reports_what_it_was_built_with() {
        assert!(Trace::new(true).is_enabled());
        assert!(!Trace::new(false).is_enabled());
    }

    #[test]
    fn a_note_is_accepted_in_both_states() {
        trace!(Trace::new(true), "enabled {}", 1);
        trace!(Trace::new(false), "disabled {}", 2);
    }
}
