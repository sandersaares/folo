//! The result of executing a `Command`: the [`RunOutcome`] a successful `run`
//! returns.

use crate::AnalysisOutcome;

/// The outcome of a successful `run`.
#[doc(hidden)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RunOutcome {
    /// The command completed; `message` is a human-readable summary.
    Completed {
        /// Human-readable summary of what happened.
        message: String,
    },
    /// The `analyze` command produced an analysis report.
    Analyzed {
        /// The rendered findings report for the requested output format.
        report: String,
        /// Number of flagged regressions across all analyzed series, for
        /// informational use. It never affects the process exit code: findings
        /// are advisory, so the machine-readable signal lives in the report's
        /// JSON (`notable`), not in the exit status.
        regressions: usize,
        /// The primary verdict of the successful analysis.
        outcome: AnalysisOutcome,
    },
}

impl RunOutcome {
    /// Whether the command should be considered successful (exit code zero).
    ///
    /// Every outcome is successful: a finding is never a build-failing condition.
    /// Only an actual failure to *run* yields a non-zero exit code. Downstream
    /// automation reads notable findings from the report JSON rather than from the
    /// exit status.
    #[must_use]
    // Every outcome is successful, so this is effectively a constant `true`; a
    // `false` mutant is unkillable because no failing outcome exists to assert
    // against. The constant is the deliberate contract, not a coverage gap.
    #[cfg_attr(test, mutants::skip)]
    pub fn is_success(&self) -> bool {
        match self {
            Self::Completed { .. } | Self::Analyzed { .. } => true,
        }
    }

    /// The text to print to standard output, or `None` when there is nothing to
    /// print.
    ///
    /// `--no-text` suppresses the text report, leaving the message/report empty;
    /// in that case this returns `None` so the caller emits no output at all
    /// rather than a blank line (the requested Markdown/JSON files carry the
    /// result instead).
    #[must_use]
    pub fn stdout_text(&self) -> Option<&str> {
        let text = match self {
            Self::Completed { message } => message,
            Self::Analyzed { report, .. } => report,
        };
        (!text.is_empty()).then_some(text.as_str())
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;

    #[test]
    fn completed_outcome_is_successful() {
        assert!(
            RunOutcome::Completed {
                message: "done".to_owned(),
            }
            .is_success()
        );
    }

    #[test]
    fn analyzed_outcome_is_always_successful() {
        // Findings are advisory and never fail a build: an Analyzed outcome is
        // successful regardless of how many regressions it flagged.
        assert!(
            RunOutcome::Analyzed {
                report: "r".to_owned(),
                regressions: 3,
                outcome: AnalysisOutcome::Findings,
            }
            .is_success()
        );
        assert!(
            RunOutcome::Analyzed {
                report: "r".to_owned(),
                regressions: 0,
                outcome: AnalysisOutcome::Clean,
            }
            .is_success()
        );
    }

    #[test]
    fn stdout_text_returns_nonempty_message_and_report() {
        assert_eq!(
            RunOutcome::Completed {
                message: "done".to_owned(),
            }
            .stdout_text(),
            Some("done")
        );
        assert_eq!(
            RunOutcome::Analyzed {
                report: "report body".to_owned(),
                regressions: 2,
                outcome: AnalysisOutcome::Findings,
            }
            .stdout_text(),
            Some("report body")
        );
    }

    #[test]
    fn stdout_text_is_suppressed_when_the_report_is_empty() {
        // `--no-text` leaves the message/report empty; the caller must print
        // nothing rather than a blank line.
        assert_eq!(
            RunOutcome::Completed {
                message: String::new(),
            }
            .stdout_text(),
            None
        );
        assert_eq!(
            RunOutcome::Analyzed {
                report: String::new(),
                regressions: 0,
                outcome: AnalysisOutcome::NothingInScope,
            }
            .stdout_text(),
            None
        );
    }
}
