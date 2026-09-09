use std::path::PathBuf;

use ohno::AppError;

use crate::apply::run_apply;
use crate::check::{CheckFormat, run_check};
use crate::expand::run_expand;
use crate::report::run_report;
use crate::verbose::Verbose;

/// Input parameters for [`run`].
#[doc(hidden)]
#[derive(Debug)]
#[expect(
    clippy::exhaustive_enums,
    reason = "Hidden enum for internal/test use only"
)]
pub enum RunInput {
    /// `report` — write `report.json` and per-package diffs.
    Report {
        /// Directory that receives `report.json` and `diffs/`.
        out_dir: PathBuf,
        /// Release baseline whose first-parent line supplies anchors.
        ///
        /// `None` defers to the default branch of the `origin` remote.
        base: Option<String>,
        /// Workspace manifest to classify. Used verbatim.
        manifest_path: PathBuf,
        /// When set, print explanatory decision notes to stderr.
        verbose: bool,
    },
    /// `check` — fail on a release the workspace's manifests cannot support.
    ///
    /// Covers a package needing an increment, a version group disagreeing with
    /// itself, a requirement not naming the version its target declares, malformed
    /// exact workspace requirements, and a package that exposes a public dependency
    /// releasing a breaking change without one of its own.
    Check {
        /// Release baseline whose first-parent line supplies anchors.
        ///
        /// `None` defers to the default branch of the `origin` remote.
        base: Option<String>,
        /// Workspace manifest to classify. Used verbatim.
        manifest_path: PathBuf,
        /// How to render diagnostics.
        format: CheckFormat,
        /// When set, warn on divergence from `cargo package --list` without failing.
        verify_packaging: bool,
        /// When set, print explanatory decision notes to stderr.
        verbose: bool,
    },
    /// `expand` — resolve version groups into an explicit per-package plan.
    Expand {
        /// Path to the plan JSON file to expand.
        plan: PathBuf,
        /// Path that receives the expanded plan JSON.
        out: PathBuf,
        /// Workspace manifest supplying members and dependency-derived groups. Used verbatim.
        manifest_path: PathBuf,
        /// When set, print explanatory decision notes to stderr.
        verbose: bool,
    },
    /// `apply` — rewrite manifests according to an approved plan.
    Apply {
        /// Path to the plan JSON file.
        plan: PathBuf,
        /// When set, compute edits without writing files or refreshing the lockfile.
        dry_run: bool,
        /// Workspace manifest to edit. Used verbatim.
        manifest_path: PathBuf,
        /// When set, print explanatory decision notes to stderr.
        verbose: bool,
    },
}

/// The successful outcome of a run.
#[doc(hidden)]
#[derive(Clone, Debug, Eq, PartialEq)]
#[expect(
    clippy::exhaustive_enums,
    reason = "Hidden enum for internal/test use only"
)]
pub enum RunOutcome {
    /// `report` finished and wrote its artifacts.
    Report {
        /// Human-readable summary for stdout. Empty when there is nothing to say.
        message: String,
    },
    /// `check` finished. `passed` is the process-level verdict.
    Check {
        /// Whether every release and workspace-version check passed.
        passed: bool,
        /// Rendered gating diagnostics or a success summary.
        message: String,
        /// Non-gating advisory lines for stderr.
        warnings: String,
    },
    /// `expand` finished and wrote the expanded plan.
    Expand {
        /// Human-readable summary for stdout.
        message: String,
    },
    /// `apply` finished (including `--dry-run`).
    Apply {
        /// Human-readable summary for stdout.
        message: String,
    },
}

/// Executes one requested operation and reports its outcome.
///
/// Selects the command named by `input` and returns what that command produced:
/// the report summary, the check verdict and its diagnostics, the expanded-plan
/// summary, or the apply summary.
///
/// # Errors
///
/// Returns an application error when the requested operation cannot be
/// completed. A failing check is a [`RunOutcome::Check`] with
/// `passed: false`, not an error.
#[doc(hidden)]
pub fn run(input: &RunInput) -> Result<RunOutcome, AppError> {
    match input {
        RunInput::Report {
            out_dir,
            base,
            manifest_path,
            verbose,
        } => {
            let message = run_report(
                out_dir,
                base.as_deref(),
                manifest_path,
                Verbose::new(*verbose),
            )?;
            Ok(RunOutcome::Report { message })
        }
        RunInput::Check {
            base,
            manifest_path,
            format,
            verify_packaging,
            verbose,
        } => {
            let (passed, message, warnings) = run_check(
                base.as_deref(),
                manifest_path,
                *format,
                *verify_packaging,
                Verbose::new(*verbose),
            )?;
            Ok(RunOutcome::Check {
                passed,
                message,
                warnings,
            })
        }
        RunInput::Expand {
            plan,
            out,
            manifest_path,
            verbose,
        } => {
            let message = run_expand(plan, out, manifest_path, Verbose::new(*verbose))?;
            Ok(RunOutcome::Expand { message })
        }
        RunInput::Apply {
            plan,
            dry_run,
            manifest_path,
            verbose,
        } => {
            let message = run_apply(plan, *dry_run, manifest_path, Verbose::new(*verbose))?;
            Ok(RunOutcome::Apply { message })
        }
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::panic::{RefUnwindSafe, UnwindSafe};

    use static_assertions::assert_impl_all;

    use super::*;

    assert_impl_all!(RunInput: UnwindSafe, RefUnwindSafe);
    assert_impl_all!(RunOutcome: UnwindSafe, RefUnwindSafe);
}
