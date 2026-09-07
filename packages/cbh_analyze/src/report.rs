//! The per-format reports a single analysis pass produces: the [`ReportRequest`]
//! that decides which formats to render and the [`RenderedReports`] it renders into.
//!
//! Text is the default and is destined for standard output; `--no-text` suppresses
//! it. `--markdown <path>` and `--json <path>` each request that format. `analyze`
//! additionally offers `--markdown-summary <path>`: a condensed Markdown report
//! carrying only the most significant findings, so a large analysis still fits within
//! a GitHub issue body, plus `--outcome <path>` for the stable one-line verdict.
//! Both are analyze-only, so neither is one of the three shared formats.
//!
//! These commands no longer write files themselves: they render each requested format
//! into a [`RenderedReports`] and return it, and the binary writes the `Some` fields to
//! the paths the user gave. The `Some`-ness of each [`RenderedReports`] field is
//! therefore the single source of truth for what gets written.

use std::path::Path;

use cbh_render::{AnalysisOutcome, ReportFormat};

use crate::{AnalyzeError, NoOutputSelectedError};

/// The rendered reports a single analysis pass produced, one `Some` per requested
/// format.
///
/// `text` is `Some` unless `--no-text` suppressed it; `markdown`/`json` are `Some`
/// only when their `--markdown <path>`/`--json <path>` option was given;
/// `markdown_summary` is analyze-only and requested on demand; `outcome` is always
/// present for analyze so the shell can expose it in process or write it on request.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RenderedReports {
    /// The primary verdict, present only for `analyze`.
    pub outcome: Option<AnalysisOutcome>,
    /// The text report destined for standard output, unless `--no-text` suppressed it.
    pub text: Option<String>,
    /// The Markdown report, when `--markdown <path>` was requested.
    pub markdown: Option<String>,
    /// The JSON report, when `--json <path>` was requested.
    pub json: Option<String>,
    /// The condensed Markdown summary, when `--markdown-summary <path>` was requested
    /// (analyze-only).
    pub markdown_summary: Option<String>,
}

/// Which report formats a single analysis pass should render.
///
/// Built once, early in each orchestrator, so the "nothing to render" case (text
/// suppressed and no file requested) fails before any expensive work.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ReportRequest {
    /// Whether the text report to standard output is suppressed (`--no-text`).
    no_text: bool,
    /// Whether the Markdown report was requested (`--markdown <path>`).
    markdown: bool,
    /// Whether the JSON report was requested (`--json <path>`).
    json: bool,
    /// Whether the condensed Markdown summary was requested (`--markdown-summary
    /// <path>`). Only `analyze` sets this; the other reporting commands leave it
    /// `false` (they do not rank findings).
    markdown_summary: bool,
}

impl ReportRequest {
    /// Resolves and validates the requested outputs for the commands that render only
    /// the three shared formats (`list`, `prune`, `examine`).
    ///
    /// # Errors
    ///
    /// Returns a [`NoOutputSelectedError`] when nothing would be produced —
    /// `--no-text` suppresses the only default output and neither file format was
    /// requested — so a run that would produce nothing is rejected up front rather
    /// than completing silently.
    pub(crate) fn resolve(
        no_text: bool,
        markdown: Option<&Path>,
        json: Option<&Path>,
    ) -> Result<Self, AnalyzeError> {
        if no_text && markdown.is_none() && json.is_none() {
            return Err(NoOutputSelectedError::new(
                "--no-text suppresses the text report, so request at least one of \
                 --markdown <path> or --json <path>",
            )
            .into());
        }
        Ok(Self {
            no_text,
            markdown: markdown.is_some(),
            json: json.is_some(),
            markdown_summary: false,
        })
    }

    /// Resolves and validates the requested outputs for `analyze`, which additionally
    /// offers `--markdown-summary <path>`.
    ///
    /// # Errors
    ///
    /// Returns a [`NoOutputSelectedError`] when nothing would be produced —
    /// `--no-text` suppresses the text report and none of `--markdown`,
    /// `--markdown-summary`, `--json`, or `--outcome` was requested — so a run that
    /// would produce nothing is rejected up front rather than completing silently.
    pub(crate) fn resolve_analyze(
        no_text: bool,
        markdown: Option<&Path>,
        json: Option<&Path>,
        markdown_summary: Option<&Path>,
        outcome: Option<&Path>,
    ) -> Result<Self, AnalyzeError> {
        if no_text
            && markdown.is_none()
            && json.is_none()
            && markdown_summary.is_none()
            && outcome.is_none()
        {
            return Err(NoOutputSelectedError::new(
                "--no-text suppresses the text report, so request at least one of \
                 --markdown <path>, --markdown-summary <path>, --json <path>, or \
                 --outcome <path>",
            )
            .into());
        }
        Ok(Self {
            no_text,
            markdown: markdown.is_some(),
            json: json.is_some(),
            markdown_summary: markdown_summary.is_some(),
        })
    }

    /// Renders each requested shared format into a [`RenderedReports`], leaving the
    /// summary unset (the shared commands do not rank findings).
    ///
    /// `render` produces the report for a given format; it is invoked once per
    /// requested format (and once for the text report unless suppressed), so a single
    /// analysis backs all outputs.
    pub(crate) fn render<F>(self, render: F) -> RenderedReports
    where
        F: Fn(ReportFormat) -> String,
    {
        RenderedReports {
            outcome: None,
            text: (!self.no_text).then(|| render(ReportFormat::Text)),
            markdown: self.markdown.then(|| render(ReportFormat::Markdown)),
            json: self.json.then(|| render(ReportFormat::Json)),
            markdown_summary: None,
        }
    }

    /// Renders each requested format for `analyze`, including the condensed summary
    /// when requested.
    ///
    /// The `render_summary` closure runs only when `--markdown-summary` was requested,
    /// so an unrequested summary is never rendered.
    pub(crate) fn render_analyze<F, S>(
        self,
        outcome: AnalysisOutcome,
        render: F,
        render_summary: S,
    ) -> RenderedReports
    where
        F: Fn(ReportFormat) -> String,
        S: FnOnce() -> String,
    {
        RenderedReports {
            outcome: Some(outcome),
            text: (!self.no_text).then(|| render(ReportFormat::Text)),
            markdown: self.markdown.then(|| render(ReportFormat::Markdown)),
            json: self.json.then(|| render(ReportFormat::Json)),
            markdown_summary: self.markdown_summary.then(render_summary),
        }
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::path::PathBuf;

    use ohno::ErrorExt as _;

    use super::*;

    /// A render stub that names the format it was asked for, so a test can tell which
    /// format backs each rendered field.
    fn render_label(format: ReportFormat) -> String {
        format!("{format:?}")
    }

    #[test]
    fn resolve_rejects_a_run_that_would_render_nothing() {
        let error = ReportRequest::resolve(true, None, None).unwrap_err();
        assert!(error.find_source::<NoOutputSelectedError>().is_some());
    }

    #[test]
    fn resolve_allows_the_default_text_output() {
        ReportRequest::resolve(false, None, None).unwrap();
    }

    #[test]
    fn resolve_allows_no_text_with_a_requested_file() {
        let json = PathBuf::from("report.json");
        ReportRequest::resolve(true, None, Some(&json)).unwrap();
    }

    #[test]
    fn render_returns_the_text_report_by_default() {
        let request = ReportRequest::resolve(false, None, None).unwrap();
        let rendered = request.render(render_label);
        assert_eq!(rendered.text.as_deref(), Some("Text"));
        assert!(rendered.markdown.is_none());
        assert!(rendered.json.is_none());
        assert!(rendered.markdown_summary.is_none());
    }

    #[test]
    fn render_suppresses_the_text_report_under_no_text() {
        let json = PathBuf::from("report.json");
        let request = ReportRequest::resolve(true, None, Some(&json)).unwrap();
        let rendered = request.render(render_label);
        assert!(rendered.text.is_none());
        assert_eq!(rendered.json.as_deref(), Some("Json"));
    }

    #[test]
    fn render_produces_both_files_from_one_pass_and_still_returns_text() {
        let markdown = PathBuf::from("report.md");
        let json = PathBuf::from("report.json");
        let request = ReportRequest::resolve(false, Some(&markdown), Some(&json)).unwrap();
        let rendered = request.render(render_label);
        assert_eq!(rendered.text.as_deref(), Some("Text"));
        assert_eq!(rendered.markdown.as_deref(), Some("Markdown"));
        assert_eq!(rendered.json.as_deref(), Some("Json"));
    }

    #[test]
    fn resolve_analyze_allows_only_the_summary() {
        let summary = PathBuf::from("summary.md");
        ReportRequest::resolve_analyze(true, None, None, Some(&summary), None).unwrap();
    }

    #[test]
    fn resolve_analyze_allows_only_the_outcome() {
        let outcome = PathBuf::from("outcome.txt");
        ReportRequest::resolve_analyze(true, None, None, None, Some(&outcome)).unwrap();
    }

    #[test]
    fn resolve_analyze_rejects_a_run_that_would_render_nothing() {
        let error = ReportRequest::resolve_analyze(true, None, None, None, None).unwrap_err();
        assert!(error.find_source::<NoOutputSelectedError>().is_some());
    }

    #[test]
    fn render_analyze_renders_the_summary_when_requested() {
        let summary = PathBuf::from("summary.md");
        let request =
            ReportRequest::resolve_analyze(true, None, None, Some(&summary), None).unwrap();
        let rendered = request.render_analyze(AnalysisOutcome::Clean, render_label, || {
            "SUMMARY".to_owned()
        });
        assert!(rendered.text.is_none());
        assert_eq!(rendered.outcome, Some(AnalysisOutcome::Clean));
        assert_eq!(rendered.markdown_summary.as_deref(), Some("SUMMARY"));
    }

    #[test]
    fn render_analyze_is_a_noop_for_the_summary_without_a_requested_path() {
        // The summary is unset: the renderer must not run and the summary stays `None`.
        let json = PathBuf::from("report.json");
        let request = ReportRequest::resolve_analyze(true, None, Some(&json), None, None).unwrap();
        let mut rendered = false;
        let reports = request.render_analyze(AnalysisOutcome::Clean, render_label, || {
            rendered = true;
            "SUMMARY".to_owned()
        });

        assert!(
            !rendered,
            "the summary renderer must not run when no path is requested"
        );
        assert!(reports.markdown_summary.is_none());
    }
}
