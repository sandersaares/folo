use std::path::PathBuf;

use ohno::AppError;

use crate::analysis_order::run_analysis_order;
use crate::apply::run_apply;
use crate::check::{CheckFormat, run_check};
use crate::expand::run_expand;
use crate::inspect_plan::run_inspect_plan;
use crate::preview::{run_prepare, run_preview};
use crate::propose::run_propose;
use crate::report::run_report;
use crate::resolved::run_verify_preview;
use crate::semver_targets::run_semver_targets;
use crate::verbose::Verbose;

/// Input parameters for [`run`].
#[doc(hidden)]
#[derive(Debug)]
#[expect(
    clippy::exhaustive_enums,
    reason = "Hidden enum for internal/test use only"
)]
pub enum RunInput {
    /// Inspect validated expanded-plan facts for external tooling.
    InspectPlan {
        /// Expanded plan artifact.
        plan: PathBuf,
        /// Require a captured preview valid for application.
        require_resolved: bool,
        /// Workspace supplying tracked membership and publication eligibility.
        manifest_path: PathBuf,
        /// Print explanatory validation decisions.
        verbose: bool,
    },
    /// Order report packages for semantic assessment.
    AnalysisOrder {
        /// Report file or its containing directory.
        report: PathBuf,
        /// Print explanatory ordering decisions.
        verbose: bool,
    },
    /// Select consumer-contract packages for compatibility assessment.
    SemverTargets {
        /// Report file or its containing directory.
        report: PathBuf,
        /// Print explanatory target decisions.
        verbose: bool,
    },
    /// Complete caller-supplied semantic decisions using captured report evidence.
    Propose {
        /// Report file or its containing directory.
        report: PathBuf,
        /// Caller-supplied change decisions.
        decisions: PathBuf,
        /// Destination for the proposed plan.
        out: PathBuf,
        /// Print explanatory version-resolution decisions.
        verbose: bool,
    },
    /// Refresh the live workspace lockfile offline before semantic grading.
    Prepare {
        /// Directory receiving report evidence and prepared.json.
        output: PathBuf,
        /// Release baseline; defaults to the remote default branch.
        base: Option<String>,
        /// Workspace manifest to prepare.
        manifest_path: PathBuf,
        /// Print explanatory resolver decisions.
        verbose: bool,
    },
    /// Resolve a proposed plan to a complete, captured state for application.
    Preview {
        /// Semantic release proposal.
        plan: PathBuf,
        /// Prepared artifact whose report supplied semantic grading evidence.
        prepared: PathBuf,
        /// Directory receiving the final report, plan, and compatibility workspace.
        output: PathBuf,
        /// Workspace manifest whose inputs must match preparation.
        manifest_path: PathBuf,
        /// Print explanatory expansion and resolver decisions.
        verbose: bool,
    },
    /// Check that compatibility evidence uses the captured final workspace unchanged.
    VerifyPreview {
        /// Resolved plan whose captured state must match.
        plan: PathBuf,
        /// Required retained candidate manifest; the original workspace is not accepted.
        manifest_path: PathBuf,
        /// Print explanatory verification notes.
        verbose: bool,
    },
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
        /// Protect input aliases and stage output before replacing the destination.
        preserve_input: bool,
        /// When set, print explanatory decision notes to stderr.
        verbose: bool,
    },
    /// `apply` — install captured files or perform proposed manifest-only edits.
    Apply {
        /// Path to the plan JSON file.
        plan: PathBuf,
        /// When set, validate and describe planned writes without changing files.
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
    /// A JSON-producing query completed.
    ArtifactQuery {
        /// JSON document for stdout.
        message: String,
    },
    /// A proposed release plan was written.
    Propose {
        /// Human-readable summary.
        message: String,
    },
    /// Preparation completed and wrote frozen evidence.
    Prepare {
        /// Human-readable summary.
        message: String,
    },
    /// Preview completed and wrote the resolved release artifact.
    Preview {
        /// Human-readable summary.
        message: String,
    },
    /// The retained compatibility workspace matches the resolved plan.
    VerifyPreview {
        /// Human-readable summary.
        message: String,
    },
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
/// Selects the command named by `input` and returns its summary or the check
/// verdict and diagnostics.
///
/// # Errors
///
/// Returns an application error when the requested operation cannot be
/// completed. A failing check is a [`RunOutcome::Check`] with
/// `passed: false`, not an error.
#[doc(hidden)]
pub fn run(input: &RunInput) -> Result<RunOutcome, AppError> {
    match input {
        RunInput::InspectPlan {
            plan,
            require_resolved,
            manifest_path,
            verbose,
        } => {
            let message = run_inspect_plan(
                plan,
                *require_resolved,
                manifest_path,
                Verbose::new(*verbose),
            )?;
            Ok(RunOutcome::ArtifactQuery { message })
        }
        RunInput::AnalysisOrder { report, verbose } => {
            let message = run_analysis_order(report, Verbose::new(*verbose))?;
            Ok(RunOutcome::ArtifactQuery { message })
        }
        RunInput::SemverTargets { report, verbose } => {
            let message = run_semver_targets(report, Verbose::new(*verbose))?;
            Ok(RunOutcome::ArtifactQuery { message })
        }
        RunInput::Propose {
            report,
            decisions,
            out,
            verbose,
        } => {
            let message = run_propose(report, decisions, out, Verbose::new(*verbose))?;
            Ok(RunOutcome::Propose { message })
        }
        RunInput::VerifyPreview {
            plan,
            manifest_path,
            verbose,
        } => {
            let message = run_verify_preview(plan, manifest_path, Verbose::new(*verbose))?;
            Ok(RunOutcome::VerifyPreview { message })
        }
        RunInput::Prepare {
            output,
            base,
            manifest_path,
            verbose,
        } => {
            let message = run_prepare(
                output,
                base.as_deref(),
                manifest_path,
                Verbose::new(*verbose),
            )?;
            Ok(RunOutcome::Prepare { message })
        }
        RunInput::Preview {
            plan,
            prepared,
            output,
            manifest_path,
            verbose,
        } => {
            let message = run_preview(
                plan,
                prepared,
                output,
                manifest_path,
                Verbose::new(*verbose),
            )?;
            Ok(RunOutcome::Preview { message })
        }
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
            preserve_input,
            verbose,
        } => {
            let message = run_expand(
                plan,
                out,
                manifest_path,
                *preserve_input,
                Verbose::new(*verbose),
            )?;
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
