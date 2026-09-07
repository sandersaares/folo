//! Thin wrappers over the `cbh_analyze` reporting commands (`analyze`, `list`,
//! `examine`, `prune`, `bless`, `unbless`).
//!
//! `cbh_analyze` renders each requested report into a [`RenderedReports`] and returns
//! it (plus the regression count for `analyze`, or a message string for
//! `bless`/`unbless`); it no longer writes files. These wrappers own the binary side
//! of that handoff: they carry an [`AnalyzeError`](cbh_analyze::AnalyzeError) onward
//! unchanged, write the rendered reports to the requested paths through the
//! [`OutputWriter`](crate::output) edge (announcing each on the verbose trail), and
//! build the [`RunOutcome`] the dispatcher returns.

use std::path::Path;

use cbh_analyze::{AutoDiscriminants, RenderedReports};
use cbh_diag::StderrReporter;
use cbh_storage::StorageFacade;
use ohno::AppError;
use tick::Clock;

use crate::output::{TokioOutputWriter, write_reports};
use crate::{
    AnalyzeOptions, BlessOptions, ExamineOptions, ListOptions, PruneOptions, RunOutcome,
    UnblessOptions,
};

/// Runs `analyze`, writes its rendered reports, and reports the regression count.
///
/// # Errors
///
/// Returns an error if the analysis or a report write fails.
pub(crate) async fn analyze(
    options: &AnalyzeOptions,
    workspace_dir: &Path,
    clock: Option<Clock>,
    storage_override: Option<StorageFacade>,
    auto_discriminants: Option<AutoDiscriminants>,
) -> Result<RunOutcome, AppError> {
    let (rendered, regressions) = cbh_analyze::analyze(
        options,
        workspace_dir,
        clock,
        storage_override,
        auto_discriminants,
    )
    .await?;
    write_rendered(
        workspace_dir,
        options.verbose,
        options.markdown.as_deref(),
        options.json.as_deref(),
        options.markdown_summary.as_deref(),
        options.outcome.as_deref(),
        &rendered,
    )
    .await?;
    let outcome = rendered
        .outcome
        .expect("analyze always renders its primary outcome");
    Ok(RunOutcome::Analyzed {
        report: rendered.text.unwrap_or_default(),
        regressions,
        outcome,
    })
}

/// Runs `list` and writes its rendered reports.
///
/// # Errors
///
/// Returns an error if the listing or a report write fails.
pub(crate) async fn list(
    options: &ListOptions,
    workspace_dir: &Path,
    clock: Option<Clock>,
    storage_override: Option<StorageFacade>,
    auto_discriminants: Option<AutoDiscriminants>,
) -> Result<RunOutcome, AppError> {
    let rendered = cbh_analyze::list(
        options,
        workspace_dir,
        clock,
        storage_override,
        auto_discriminants,
    )
    .await?;
    write_rendered(
        workspace_dir,
        options.verbose,
        options.markdown.as_deref(),
        options.json.as_deref(),
        None,
        None,
        &rendered,
    )
    .await?;
    Ok(RunOutcome::Completed {
        message: rendered.text.unwrap_or_default(),
    })
}

/// Runs `examine` and writes its rendered reports.
///
/// # Errors
///
/// Returns an error if the examination or a report write fails.
pub(crate) async fn examine(
    options: &ExamineOptions,
    workspace_dir: &Path,
    clock: Option<Clock>,
    storage_override: Option<StorageFacade>,
    auto_discriminants: Option<AutoDiscriminants>,
) -> Result<RunOutcome, AppError> {
    let rendered = cbh_analyze::examine(
        options,
        workspace_dir,
        clock,
        storage_override,
        auto_discriminants,
    )
    .await?;
    write_rendered(
        workspace_dir,
        options.verbose,
        options.markdown.as_deref(),
        options.json.as_deref(),
        None,
        None,
        &rendered,
    )
    .await?;
    Ok(RunOutcome::Completed {
        message: rendered.text.unwrap_or_default(),
    })
}

/// Runs `prune` and writes its rendered reports.
///
/// # Errors
///
/// Returns an error if the prune or a report write fails.
pub(crate) async fn prune(
    options: &PruneOptions,
    workspace_dir: &Path,
    clock: Option<Clock>,
    storage_override: Option<StorageFacade>,
    auto_discriminants: Option<AutoDiscriminants>,
) -> Result<RunOutcome, AppError> {
    let rendered = cbh_analyze::prune(
        options,
        workspace_dir,
        clock,
        storage_override,
        auto_discriminants,
    )
    .await?;
    write_rendered(
        workspace_dir,
        options.verbose,
        options.markdown.as_deref(),
        options.json.as_deref(),
        None,
        None,
        &rendered,
    )
    .await?;
    Ok(RunOutcome::Completed {
        message: rendered.text.unwrap_or_default(),
    })
}

/// Runs `bless`. It writes no report files, so its message becomes the outcome
/// directly.
///
/// # Errors
///
/// Returns an error if a blessing precondition fails.
pub(crate) async fn bless(
    options: &BlessOptions,
    workspace_dir: &Path,
    clock: Option<Clock>,
    auto_discriminants: Option<AutoDiscriminants>,
) -> Result<RunOutcome, AppError> {
    let message = cbh_analyze::bless(options, workspace_dir, clock, auto_discriminants).await?;
    Ok(RunOutcome::Completed { message })
}

/// Runs `unbless`. It writes no report files, so its message becomes the outcome
/// directly.
///
/// # Errors
///
/// Returns an error if a blessing precondition fails.
pub(crate) async fn unbless(
    options: &UnblessOptions,
    workspace_dir: &Path,
    auto_discriminants: Option<AutoDiscriminants>,
) -> Result<RunOutcome, AppError> {
    let message = cbh_analyze::unbless(options, workspace_dir, auto_discriminants).await?;
    Ok(RunOutcome::Completed { message })
}

/// Writes the rendered reports to the requested paths, resolving relative paths
/// against `workspace_dir` and announcing each write on the verbose trail.
// Trivial production-adapter forwarder; `write_reports` is unit-tested and report-file
// wiring is covered end-to-end.
#[cfg_attr(test, mutants::skip)]
async fn write_rendered(
    workspace_dir: &Path,
    verbose: bool,
    markdown: Option<&Path>,
    json: Option<&Path>,
    markdown_summary: Option<&Path>,
    outcome: Option<&Path>,
    rendered: &RenderedReports,
) -> Result<(), AppError> {
    let writer = TokioOutputWriter::new(workspace_dir.to_path_buf());
    let reporter = StderrReporter::new(verbose);
    write_reports(
        &writer,
        &reporter,
        markdown,
        json,
        markdown_summary,
        outcome,
        rendered,
    )
    .await
}
