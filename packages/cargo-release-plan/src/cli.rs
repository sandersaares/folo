//! Command-line argument parsing for `cargo-release-plan`, built on `clap`.
//!
//! Parsing accepts the argument vector Cargo passes to a subcommand, including
//! the injected `release-plan` token, and yields either a typed [`RunInput`] or
//! an [`EarlyExit`].

use std::ffi::OsString;
use std::path::PathBuf;

use clap::error::ErrorKind;
use clap::{Error as ClapError, Parser, Subcommand, ValueEnum};

use crate::{CheckFormat, RunInput};

/// Parsed command line, before defaults are resolved.
///
/// This is the tool's argument model: the binary parses argv into it and then
/// converts it into the [`RunInput`] the core logic runs on, so `clap` types do
/// not reach the rest of the crate.
#[derive(Debug, Parser)]
#[command(
    name = "cargo-release-plan",
    about = "Classify publishable packages against version anchors and apply increment plans.",
    // Cargo subcommands are versioned by the crate that ships them, and a
    // `--version` flag here would report this binary's own version as if it
    // were a property of the workspace being planned.
    disable_version_flag = true
)]
pub struct Cli {
    #[command(subcommand)]
    command: Command,
}

impl Cli {
    /// Parses OS arguments, stripping the `release-plan` token Cargo injects.
    ///
    /// # Errors
    ///
    /// Returns an [`EarlyExit`] when the arguments request help/usage or fail to
    /// parse.
    pub fn from_args_os<I, T>(args: I) -> Result<Self, EarlyExit>
    where
        I: IntoIterator<Item = T>,
        T: Into<OsString> + Clone,
    {
        let mut argv: Vec<OsString> = args.into_iter().map(Into::into).collect();
        if argv.get(1).is_some_and(|arg| arg == "release-plan") {
            argv.remove(1);
        }
        Self::try_parse_from(argv).map_err(|error| EarlyExit::from_clap(&error))
    }

    /// Translates the parsed arguments into the [`RunInput`] the core logic consumes.
    ///
    /// CLI-owned defaults such as the workspace manifest path are resolved here.
    /// An absent base remains `None` so execution can use the repository's
    /// recorded remote default branch, falling back to `origin/main`.
    #[must_use]
    pub fn into_input(self) -> RunInput {
        match self.command {
            Command::VerifyPreview(args) => RunInput::VerifyPreview {
                plan: args.plan,
                manifest_path: args.manifest_path,
                verbose: args.verbose,
            },
            Command::Prepare(args) => RunInput::Prepare {
                output: args.output,
                base: args.base,
                manifest_path: args
                    .manifest_path
                    .unwrap_or_else(|| PathBuf::from("Cargo.toml")),
                verbose: args.verbose,
            },
            Command::Preview(args) => RunInput::Preview {
                plan: args.plan,
                prepared: args.prepared,
                output: args.output,
                manifest_path: args
                    .manifest_path
                    .unwrap_or_else(|| PathBuf::from("Cargo.toml")),
                verbose: args.verbose,
            },
            Command::Report(args) => RunInput::Report {
                out_dir: args.out_dir,
                base: args.base,
                manifest_path: args
                    .manifest_path
                    .unwrap_or_else(|| PathBuf::from("Cargo.toml")),
                verbose: args.verbose,
            },
            Command::Check(args) => RunInput::Check {
                base: args.base,
                manifest_path: args
                    .manifest_path
                    .unwrap_or_else(|| PathBuf::from("Cargo.toml")),
                format: args.format.into(),
                verify_packaging: args.verify_packaging,
                verbose: args.verbose,
            },
            Command::Expand(args) => RunInput::Expand {
                plan: args.plan,
                out: args.out,
                manifest_path: args
                    .manifest_path
                    .unwrap_or_else(|| PathBuf::from("Cargo.toml")),
                verbose: args.verbose,
            },
            Command::Apply(args) => RunInput::Apply {
                plan: args.plan,
                dry_run: args.dry_run,
                manifest_path: args
                    .manifest_path
                    .unwrap_or_else(|| PathBuf::from("Cargo.toml")),
                verbose: args.verbose,
            },
        }
    }
}

/// A parse outcome that should terminate the program before execution.
///
/// This is either a help/usage request (success, printed to stdout) or a parse
/// error (failure, printed to stderr).
#[derive(Debug)]
#[expect(
    clippy::exhaustive_structs,
    reason = "handoff struct read directly by the in-crate binary and integration tests"
)]
pub struct EarlyExit {
    /// The rendered message (help text or error) to print.
    pub output: String,
    /// `Ok` for a help/usage request (exit success), `Err` for a parse error.
    pub status: Result<(), ()>,
}

impl EarlyExit {
    /// Classifies a `clap` parse error into the success/failure early-exit shape.
    fn from_clap(error: &ClapError) -> Self {
        let success = matches!(
            error.kind(),
            ErrorKind::DisplayHelp
                | ErrorKind::DisplayVersion
                | ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand
        );
        Self {
            output: error.to_string(),
            status: if success { Ok(()) } else { Err(()) },
        }
    }
}

/// Clap grammar for the subcommands.
#[derive(Debug, Subcommand)]
enum Command {
    /// Refresh the live lockfile offline and capture evidence before semantic grading.
    Prepare(PrepareArgs),
    /// Resolve all prospective plan effects offline and capture the state for application.
    Preview(PreviewArgs),
    /// Verify that the retained compatibility workspace still matches the resolved plan.
    VerifyPreview(VerifyPreviewArgs),
    /// Write report.json and per-package diffs for the changes needing a release.
    Report(ReportArgs),
    /// Fail on a release the workspace's manifests cannot support.
    ///
    /// Fails when a publishable package has unreleased changes without a version increment, when
    /// a version group disagrees with itself, when a requirement on another workspace package
    /// does not name the version that package declares, when an exact workspace requirement is
    /// malformed, or when a package whose public API exposes a workspace dependency stays
    /// compatible while that dependency releases a breaking change.
    Check(CheckArgs),
    /// Expand groups without resolution; pass the result through preview before apply.
    Expand(ExpandArgs),
    /// Install captured files without resolution, or make proposed manifest-only edits.
    Apply(ApplyArgs),
}

/// Arguments for explicit pre-grading preparation.
#[derive(Debug, Parser)]
struct PrepareArgs {
    /// Directory receiving report.json, diffs/, and prepared.json.
    #[arg(long, visible_alias = "out-dir")]
    output: PathBuf,
    /// Release baseline, defaulting to the remote default branch.
    #[arg(long)]
    base: Option<String>,
    /// Path to the workspace Cargo.toml.
    #[arg(long)]
    manifest_path: Option<PathBuf>,
    /// Print explanatory preparation notes.
    #[arg(long)]
    verbose: bool,
}

/// Arguments for proposal-specific offline resolution.
#[derive(Debug, Parser)]
struct PreviewArgs {
    /// Proposed version decisions.
    #[arg(long)]
    plan: PathBuf,
    /// Prepared artifact whose report was assessed.
    #[arg(long)]
    prepared: PathBuf,
    /// Directory receiving plan.json, report.json, diffs/, and retained workspace/.
    #[arg(long)]
    output: PathBuf,
    /// Path to the workspace Cargo.toml.
    #[arg(long)]
    manifest_path: Option<PathBuf>,
    /// Print explanatory expansion and resolver notes.
    #[arg(long)]
    verbose: bool,
}

/// Explicit candidate selection prevents compatibility checks from using the original tree.
#[derive(Debug, Parser)]
struct VerifyPreviewArgs {
    /// Path to the resolved plan JSON.
    #[arg(long)]
    plan: PathBuf,
    /// The `resolved.evidence_manifest_path` emitted by preview.
    #[arg(long)]
    manifest_path: PathBuf,
    /// Print explanatory verification notes.
    #[arg(long)]
    verbose: bool,
}

/// Arguments for `report`.
#[derive(Debug, Parser)]
struct ReportArgs {
    /// Directory that receives `report.json` and `diffs/`.
    #[arg(long)]
    out_dir: PathBuf,

    /// Release baseline whose first-parent line supplies anchors.
    ///
    /// Defaults to the default branch the `origin` remote advertises.
    #[arg(long)]
    base: Option<String>,

    /// Path to the workspace `Cargo.toml`.
    #[arg(long)]
    manifest_path: Option<PathBuf>,

    /// Print explanatory notes for each classification decision.
    #[arg(long)]
    verbose: bool,
}

/// Arguments for `check`.
#[derive(Debug, Parser)]
struct CheckArgs {
    /// Release baseline whose first-parent line supplies anchors.
    ///
    /// Defaults to the default branch the `origin` remote advertises.
    #[arg(long)]
    base: Option<String>,

    /// Path to the workspace `Cargo.toml`.
    #[arg(long)]
    manifest_path: Option<PathBuf>,

    /// How to render diagnostics.
    #[arg(long, value_enum, default_value_t = CliCheckFormat::Text)]
    format: CliCheckFormat,

    /// Warn when released-content rules diverge from `cargo package --list`.
    ///
    /// Non-gating: a mismatch is printed and the check verdict is unchanged.
    #[arg(long)]
    verify_packaging: bool,

    /// Print explanatory notes for each classification decision.
    #[arg(long)]
    verbose: bool,
}

/// Arguments for `expand`.
#[derive(Debug, Parser)]
struct ExpandArgs {
    /// Path to the plan JSON file to expand.
    #[arg(long)]
    plan: PathBuf,

    /// Path that receives the expanded plan JSON.
    #[arg(long)]
    out: PathBuf,

    /// Path to the workspace `Cargo.toml`.
    #[arg(long)]
    manifest_path: Option<PathBuf>,

    /// Print explanatory notes for each expansion decision.
    #[arg(long)]
    verbose: bool,
}

/// Arguments for `apply`.
#[derive(Debug, Parser)]
struct ApplyArgs {
    /// Path to the plan JSON file to apply.
    #[arg(long)]
    plan: PathBuf,

    /// Validate and describe planned writes without changing files.
    #[arg(long)]
    dry_run: bool,

    /// Path to the workspace `Cargo.toml`.
    #[arg(long)]
    manifest_path: Option<PathBuf>,

    /// Print explanatory notes for each edit decision.
    #[arg(long)]
    verbose: bool,
}

/// Clap value for `--format`; converted to [`CheckFormat`].
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum CliCheckFormat {
    Text,
    Github,
}

impl From<CliCheckFormat> for CheckFormat {
    fn from(value: CliCheckFormat) -> Self {
        match value {
            CliCheckFormat::Text => Self::Text,
            CliCheckFormat::Github => Self::Github,
        }
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::panic::{RefUnwindSafe, UnwindSafe};

    use static_assertions::assert_impl_all;

    use super::{Cli, EarlyExit};
    use crate::RunInput;

    assert_impl_all!(Cli: UnwindSafe, RefUnwindSafe);
    assert_impl_all!(EarlyExit: UnwindSafe, RefUnwindSafe);

    #[test]
    fn from_args_os_strips_cargo_injected_subcommand() {
        let cli = Cli::from_args_os(["cargo-release-plan", "release-plan", "check"]).unwrap();
        match cli.into_input() {
            RunInput::Check { .. } => {}
            other => panic!("expected check, got {other:?}"),
        }
    }
}
