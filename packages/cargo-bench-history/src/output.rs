//! The filesystem edge that writes the per-format reports the reporting commands
//! (`analyze`, `list`, `prune`, `examine`) render.
//!
//! `cbh_analyze` renders each requested format into a [`RenderedReports`] and
//! returns it; this module writes the `Some` fields to the paths the user gave.
//! Text is the default and goes to standard output (the caller prints it), so it is
//! never written here; `--markdown <path>` and `--json <path>` each write that
//! format to a file, and `analyze` additionally offers `--markdown-summary <path>`
//! plus a one-line `--outcome <path>`.
//! The file writes go through the [`OutputWriter`] port (mirroring the `ConfigWriter`
//! used by `install`) so the write path stays filesystem-agnostic: production uses
//! [`TokioOutputWriter`], while tests drive an in-memory fake.
//!
//! A relative `--markdown`/`--markdown-summary`/`--json` path resolves against the
//! working directory (the same base as `--config`), so the resolution happens at the
//! IO edge inside [`TokioOutputWriter`].

use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};

use cbh_analyze::RenderedReports;
use cbh_config::rebase;
use cbh_diag::{Reporter, ReporterExt};
use ohno::AppError;

use crate::errors::WriteReportFailedError;

/// Writes a rendered report to a destination path, overwriting any existing file.
///
/// This is the filesystem edge of the per-format output model: `cbh_analyze` renders
/// strings and the binary hands them here, so the report rendering stays Miri-safe
/// and an in-memory fake can stand in under test.
pub(crate) trait OutputWriter {
    /// Writes `contents` to `path`, creating parent directories as needed and
    /// replacing any existing file (a re-run refreshes the report in place).
    fn write(&self, path: &Path, contents: &str) -> impl Future<Output = io::Result<()>>;
}

/// The production [`OutputWriter`], backed by `tokio::fs`.
///
/// A relative destination resolves against `base` (the workspace directory, which
/// is the working directory in production), so `--markdown report.md` lands beside
/// the other working-directory-relative paths the tool accepts.
#[derive(Clone, Debug)]
pub(crate) struct TokioOutputWriter {
    /// The directory a relative destination path is resolved against.
    base: PathBuf,
}

impl TokioOutputWriter {
    /// Creates a writer that resolves relative paths against `base`.
    #[must_use]
    pub(crate) fn new(base: PathBuf) -> Self {
        Self { base }
    }
}

impl OutputWriter for TokioOutputWriter {
    async fn write(&self, path: &Path, contents: &str) -> io::Result<()> {
        let resolved = rebase(&self.base, path.to_path_buf());
        if let Some(parent) = resolved.parent()
            && !parent.as_os_str().is_empty()
        {
            tokio::fs::create_dir_all(parent).await?;
        }
        // `write` truncates an existing file, so re-running analysis refreshes the
        // report in place rather than appending to or failing on a stale one.
        tokio::fs::write(&resolved, contents.as_bytes()).await
    }
}

/// Writes each rendered report to its requested destination path.
///
/// The `Some`-ness of each [`RenderedReports`] field is the single source of truth
/// for what gets written for the optional report formats: `cbh_analyze` renders a
/// format exactly when the user requested its path, so a rendered field and its
/// destination path always agree. The outcome is different: analysis always computes
/// it for the in-process caller and only writes it when `--outcome` supplied a path.
/// Each file write is announced on the verbose trail with its path and size, so a
/// `--verbose` run records exactly what landed where.
///
/// # Errors
///
/// Returns a [`WriteReportFailedError`] if writing a requested file fails.
pub(crate) async fn write_reports<W: OutputWriter>(
    writer: &W,
    reporter: &dyn Reporter,
    markdown: Option<&Path>,
    json: Option<&Path>,
    markdown_summary: Option<&Path>,
    outcome: Option<&Path>,
    rendered: &RenderedReports,
) -> Result<(), AppError> {
    debug_assert_eq!(
        markdown.is_some(),
        rendered.markdown.is_some(),
        "a --markdown path and a rendered Markdown report must accompany each other"
    );
    debug_assert_eq!(
        json.is_some(),
        rendered.json.is_some(),
        "a --json path and a rendered JSON report must accompany each other"
    );
    debug_assert_eq!(
        markdown_summary.is_some(),
        rendered.markdown_summary.is_some(),
        "a --markdown-summary path and a rendered summary must accompany each other"
    );
    if let (Some(path), Some(contents)) = (markdown, rendered.markdown.as_deref()) {
        write_report(writer, reporter, path, contents, "Markdown").await?;
    }
    if let (Some(path), Some(contents)) = (json, rendered.json.as_deref()) {
        write_report(writer, reporter, path, contents, "JSON").await?;
    }
    if let (Some(path), Some(contents)) = (markdown_summary, rendered.markdown_summary.as_deref()) {
        write_report(writer, reporter, path, contents, "Markdown summary").await?;
    }
    if let Some(path) = outcome {
        let value = rendered
            .outcome
            .expect("an --outcome path is valid only for analyze, which always has an outcome");
        write_report(writer, reporter, path, value.as_str(), "analysis outcome").await?;
    }
    Ok(())
}

/// Writes one rendered report to `path` and records the result on the verbose
/// trail.
async fn write_report<W: OutputWriter>(
    writer: &W,
    reporter: &dyn Reporter,
    path: &Path,
    contents: &str,
    label: &str,
) -> Result<(), AppError> {
    writer
        .write(path, contents)
        .await
        .map_err(|error| WriteReportFailedError::caused_by(label, path, error))?;
    reporter.note_with(|| {
        format!(
            "wrote the {label} report to {} ({})",
            path.display(),
            cbh_diag::count_noun(contents.len(), "byte")
        )
    });
    Ok(())
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod fake {
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};
    use std::sync::Mutex;

    use super::{OutputWriter, io};

    /// An in-memory [`OutputWriter`] that records written files without touching
    /// the filesystem, so write-path tests run under Miri.
    ///
    /// A later write to the same path overwrites the recorded contents, mirroring
    /// the real writer.
    #[derive(Debug, Default)]
    pub(crate) struct MemoryOutputWriter {
        files: Mutex<HashMap<PathBuf, String>>,
    }

    impl MemoryOutputWriter {
        /// An empty writer.
        pub(crate) fn new() -> Self {
            Self::default()
        }

        /// The contents recorded for `path`, if any.
        pub(crate) fn written(&self, path: &Path) -> Option<String> {
            self.files.lock().unwrap().get(path).cloned()
        }
    }

    impl OutputWriter for MemoryOutputWriter {
        async fn write(&self, path: &Path, contents: &str) -> io::Result<()> {
            self.files
                .lock()
                .unwrap()
                .insert(path.to_path_buf(), contents.to_owned());
            Ok(())
        }
    }

    /// An [`OutputWriter`] whose every write fails, so the write error path is
    /// exercised under Miri without touching the filesystem.
    #[derive(Debug, Default)]
    pub(crate) struct FailingOutputWriter;

    impl OutputWriter for FailingOutputWriter {
        async fn write(&self, _path: &Path, _contents: &str) -> io::Result<()> {
            Err(io::Error::other("write refused"))
        }
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use cbh_diag::RecordingReporter;
    use futures::executor::block_on;

    use super::fake::{FailingOutputWriter, MemoryOutputWriter};
    use super::*;
    use crate::AnalysisOutcome;

    #[test]
    fn write_reports_writes_both_files_and_still_announces_them() {
        let markdown = PathBuf::from("report.md");
        let json = PathBuf::from("report.json");
        let rendered = RenderedReports {
            outcome: None,
            text: Some("Text".to_owned()),
            markdown: Some("Markdown".to_owned()),
            json: Some("Json".to_owned()),
            markdown_summary: None,
        };
        let writer = MemoryOutputWriter::new();
        let reporter = RecordingReporter::new();

        block_on(write_reports(
            &writer,
            &reporter,
            Some(&markdown),
            Some(&json),
            None,
            None,
            &rendered,
        ))
        .unwrap();

        assert_eq!(writer.written(&markdown).as_deref(), Some("Markdown"));
        assert_eq!(writer.written(&json).as_deref(), Some("Json"));
        // Each written file is announced on the verbose trail.
        assert!(
            reporter.contains("wrote the Markdown report"),
            "{:?}",
            reporter.notes()
        );
        assert!(
            reporter.contains("wrote the JSON report"),
            "{:?}",
            reporter.notes()
        );
    }

    #[test]
    fn write_reports_writes_nothing_for_a_text_only_render() {
        // The default text-only render carries no file destinations, so nothing is
        // written and no note fires.
        let rendered = RenderedReports {
            text: Some("Text".to_owned()),
            ..RenderedReports::default()
        };
        let writer = MemoryOutputWriter::new();
        let reporter = RecordingReporter::new();

        block_on(write_reports(
            &writer, &reporter, None, None, None, None, &rendered,
        ))
        .unwrap();

        assert!(reporter.notes().is_empty(), "{:?}", reporter.notes());
    }

    #[test]
    fn write_reports_writes_the_summary_and_announces_it() {
        let summary = PathBuf::from("summary.md");
        let rendered = RenderedReports {
            markdown_summary: Some("SUMMARY".to_owned()),
            ..RenderedReports::default()
        };
        let writer = MemoryOutputWriter::new();
        let reporter = RecordingReporter::new();

        block_on(write_reports(
            &writer,
            &reporter,
            None,
            None,
            Some(&summary),
            None,
            &rendered,
        ))
        .unwrap();

        assert_eq!(writer.written(&summary).as_deref(), Some("SUMMARY"));
        assert!(
            reporter.contains("wrote the Markdown summary report"),
            "{:?}",
            reporter.notes()
        );
    }

    #[test]
    fn write_reports_writes_the_analysis_outcome_and_announces_it() {
        let outcome = PathBuf::from("outcome.txt");
        let rendered = RenderedReports {
            outcome: Some(AnalysisOutcome::InsufficientBaseline),
            ..RenderedReports::default()
        };
        let writer = MemoryOutputWriter::new();
        let reporter = RecordingReporter::new();

        block_on(write_reports(
            &writer,
            &reporter,
            None,
            None,
            None,
            Some(&outcome),
            &rendered,
        ))
        .unwrap();

        assert_eq!(
            writer.written(&outcome).as_deref(),
            Some("insufficient_baseline")
        );
        assert!(
            reporter.contains("wrote the analysis outcome report"),
            "{:?}",
            reporter.notes()
        );
    }

    #[test]
    fn write_reports_names_the_report_that_could_not_be_written() {
        let json = PathBuf::from("report.json");
        let rendered = RenderedReports {
            json: Some("Json".to_owned()),
            ..RenderedReports::default()
        };
        let writer = FailingOutputWriter;
        let reporter = RecordingReporter::new();

        let error = block_on(write_reports(
            &writer,
            &reporter,
            None,
            Some(&json),
            None,
            None,
            &rendered,
        ))
        .unwrap_err();
        let write_error = error.find_source::<WriteReportFailedError>().unwrap();
        assert_eq!(write_error.label, "JSON");
        assert_eq!(write_error.path, json);
        assert!(error.find_source::<io::Error>().is_some());
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod real_writer_tests {
    use tempfile::tempdir;

    use super::{OutputWriter, TokioOutputWriter};

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // Touches the real filesystem, which Miri cannot access.
    async fn write_creates_missing_parent_directories() {
        let dir = tempdir().unwrap();
        let writer = TokioOutputWriter::new(dir.path().to_path_buf());

        writer
            .write("nested/report.md".as_ref(), "payload")
            .await
            .unwrap();

        let written = std::fs::read_to_string(dir.path().join("nested/report.md")).unwrap();
        assert_eq!(written, "payload");
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // Touches the real filesystem, which Miri cannot access.
    async fn write_overwrites_an_existing_file() {
        let dir = tempdir().unwrap();
        let writer = TokioOutputWriter::new(dir.path().to_path_buf());
        writer.write("report.json".as_ref(), "stale").await.unwrap();

        writer.write("report.json".as_ref(), "fresh").await.unwrap();

        let written = std::fs::read_to_string(dir.path().join("report.json")).unwrap();
        assert_eq!(written, "fresh");
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)] // Touches the real filesystem, which Miri cannot access.
    async fn write_resolves_an_absolute_path_without_rebasing() {
        let base = tempdir().unwrap();
        let elsewhere = tempdir().unwrap();
        let writer = TokioOutputWriter::new(base.path().to_path_buf());
        let absolute = elsewhere.path().join("report.json");

        writer.write(&absolute, "payload").await.unwrap();

        assert_eq!(std::fs::read_to_string(&absolute).unwrap(), "payload");
        // The base directory is untouched, confirming the absolute path was not
        // rebased onto it.
        assert!(!base.path().join("report.json").exists());
    }
}
