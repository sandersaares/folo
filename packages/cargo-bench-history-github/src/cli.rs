use std::num::NonZero;
use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

use crate::model::{CommitSha, Instance, Repository};

/// GitHub lifecycle operations for `cargo-bench-history` reports.
#[derive(Debug, Parser)]
#[command(version, about)]
pub struct Cli {
    #[command(flatten)]
    common: CommonArgs,

    #[command(subcommand)]
    command: Command,
}

impl Cli {
    pub(crate) fn repository(&self) -> Option<Repository> {
        self.common.repository.clone()
    }

    pub(crate) fn instance(&self) -> Instance {
        self.common.instance.clone()
    }

    pub(crate) fn verbose(&self) -> bool {
        self.common.verbose
    }

    pub(crate) fn into_command(self) -> Command {
        self.command
    }
}

/// Arguments shared by every lifecycle operation.
#[derive(Args, Debug)]
struct CommonArgs {
    /// Repository in `owner/name` form; defaults to `GITHUB_REPOSITORY`.
    #[arg(long)]
    repository: Option<Repository>,

    /// Namespace separating independent action instances.
    #[arg(long, default_value = "default")]
    instance: Instance,

    /// Emit explanatory diagnostics to standard error.
    #[arg(long)]
    verbose: bool,
}

/// One semantic GitHub lifecycle operation.
#[derive(Debug, Subcommand)]
pub(crate) enum Command {
    /// Mark an open regression issue stale before a new history run.
    IssuePreflight {
        /// Commit the new run is analyzing.
        #[arg(long)]
        head: CommitSha,
    },
    /// Create or update the rolling regression issue.
    PublishIssue {
        /// Displayed issue title.
        #[arg(long)]
        title: String,
        /// Markdown summary rendered by cargo-bench-history.
        #[arg(long)]
        body_file: PathBuf,
        /// Commit the summary describes.
        #[arg(long)]
        analyzed_sha: CommitSha,
        /// URL of the complete report artifact.
        #[arg(long)]
        artifact_url: Option<String>,
        /// Repository-specific introductory sentence.
        #[arg(long)]
        intro: Option<String>,
        /// Repository-specific report-reading documentation.
        #[arg(long)]
        docs_url: Option<String>,
    },
    /// Replace a recovered regression issue with an all-clear state.
    IssueCleanup {
        /// Commit that analyzed cleanly.
        #[arg(long)]
        clean_commit: CommitSha,
        /// Close the issue after writing the all-clear state.
        #[arg(long)]
        auto_close: bool,
        /// Repository-specific introductory sentence.
        #[arg(long)]
        intro: Option<String>,
        /// Repository-specific report-reading documentation.
        #[arg(long)]
        docs_url: Option<String>,
    },
    /// Create or update the rolling automation-failure issue.
    Alert {
        /// Displayed issue title.
        #[arg(long)]
        title: String,
        /// URL of the failed workflow run.
        #[arg(long)]
        run_url: String,
        /// Repository-specific introductory sentence.
        #[arg(long)]
        intro: Option<String>,
        /// Repository-specific runbook.
        #[arg(long)]
        docs_url: Option<String>,
    },
    /// Close the rolling automation-failure issue.
    ResolveAlert {
        /// URL of the successful workflow run.
        #[arg(long)]
        run_url: String,
    },
    /// Seed or mark stale the rolling pull-request comment.
    PrCommentPreflight {
        /// Pull-request number.
        #[arg(long)]
        pull_request: NonZero<u64>,
        /// Comma-separated benchmarked packages.
        #[arg(long)]
        packages: String,
    },
    /// Create or update the rolling pull-request results comment.
    PublishPrComment {
        /// Pull-request number.
        #[arg(long)]
        pull_request: NonZero<u64>,
        /// Commit the summary describes.
        #[arg(long)]
        analyzed_sha: CommitSha,
        /// Markdown summary rendered by cargo-bench-history.
        #[arg(long)]
        body_file: PathBuf,
        /// Comma-separated benchmarked packages.
        #[arg(long)]
        packages: String,
        /// URL of the complete report artifact.
        #[arg(long)]
        artifact_url: Option<String>,
        /// Repository-specific introductory sentence.
        #[arg(long)]
        intro: Option<String>,
        /// Repository-specific report-reading documentation.
        #[arg(long)]
        docs_url: Option<String>,
    },
    /// Replace or remove a rolling comment when nothing benchmarkable changed.
    PrCommentCleanup {
        /// Pull-request number.
        #[arg(long)]
        pull_request: NonZero<u64>,
        /// Delete instead of leaving the standard explanatory note.
        #[arg(long)]
        delete: bool,
    },
    /// Retire an in-progress placeholder after a genuine workflow failure.
    PrCommentFinalize {
        /// Pull-request number.
        #[arg(long)]
        pull_request: NonZero<u64>,
        /// URL of the failed workflow run.
        #[arg(long)]
        run_url: String,
    },
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::panic::{RefUnwindSafe, UnwindSafe};

    use clap::CommandFactory as _;
    use static_assertions::assert_impl_all;

    use super::*;

    assert_impl_all!(Cli: Send, Sync, Unpin, UnwindSafe, RefUnwindSafe);

    fn parse(args: &[&str]) -> Cli {
        Cli::try_parse_from(
            std::iter::once("cargo-bench-history-github").chain(args.iter().copied()),
        )
        .unwrap()
    }

    #[test]
    fn every_subcommand_is_present_in_help() {
        let help = Cli::command().render_long_help().to_string();
        for name in [
            "issue-preflight",
            "publish-issue",
            "issue-cleanup",
            "alert",
            "resolve-alert",
            "pr-comment-preflight",
            "publish-pr-comment",
            "pr-comment-cleanup",
            "pr-comment-finalize",
        ] {
            assert!(help.contains(name), "{help}");
        }
    }

    #[test]
    fn common_arguments_apply_to_every_command() {
        let cli = parse(&[
            "--repository",
            "folo-rs/folo",
            "--instance",
            "nightly",
            "--verbose",
            "resolve-alert",
            "--run-url",
            "https://example.test/run",
        ]);
        assert_eq!(
            cli.repository()
                .as_ref()
                .map(ToString::to_string)
                .as_deref(),
            Some("folo-rs/folo")
        );
        assert_eq!(cli.instance().as_str(), "nightly");
        assert!(cli.verbose());

        let cli = parse(&[
            "--repository",
            "folo-rs/folo",
            "resolve-alert",
            "--run-url",
            "https://example.test/run",
        ]);
        assert!(!cli.verbose());
    }

    #[test]
    fn pull_request_number_must_be_nonzero() {
        Cli::try_parse_from([
            "cargo-bench-history-github",
            "pr-comment-cleanup",
            "--pull-request",
            "0",
        ])
        .unwrap_err();
    }
}
