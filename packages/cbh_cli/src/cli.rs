//! Argument parsing: the `clap` subcommand surface and its translation into the
//! typed [`Command`](cbh_command::Command) model.
//!
//! The wide option surface is organized into named help groups (Environment,
//! Output, Discriminant selection, Commit selection, Data filtering, Benchmark
//! scope) so `--help` makes the relationship between options legible. Shared
//! groups are factored into [`clap::Args`] structs and `#[command(flatten)]`ed
//! into each subcommand so the same option means the same thing everywhere.

use std::num::NonZeroUsize;
use std::path::PathBuf;

use cbh_command::{
    AnalyzeOptions, BackfillOptions, BlessOptions, CacheSelection, CollectOptions, Command,
    ExamineOptions, ImportOptions, InstallOptions, ListOptions, ListSubject, LocalStorageSelection,
    MachineKeyOptions, PruneOptions, UnblessOptions,
};
use cbh_model::BenchmarkIdPrefix;
use clap::{ArgGroup, Args, Parser, Subcommand as ClapSubcommand, ValueEnum};

const HEADING_ENV: &str = "Environment and execution";
const HEADING_OUTPUT: &str = "Output";
const HEADING_DISCRIMINANT: &str = "Discriminant selection";
const HEADING_COMMIT: &str = "Commit selection";
const HEADING_FILTER: &str = "Data filtering";
const HEADING_SCOPE: &str = "Benchmark scope";
const HEADING_FEATURES: &str = "Feature selection";

/// Maintain a history of benchmark results over time and analyze it for trends.
#[derive(Debug, Parser)]
#[command(
    name = "cargo-bench-history",
    about = "Maintain a history of benchmark results over time and analyze it for trends.",
    disable_help_subcommand = true,
    disable_version_flag = true
)]
pub struct Cli {
    /// The subcommand to execute.
    #[command(subcommand)]
    command: Subcommand,
}

/// A parse outcome that should terminate the program before execution.
///
/// This is either a help/usage request (success, printed to stdout) or a parse
/// error (failure, printed to stderr). Mirrors the shape the binary entry point
/// consumes.
#[derive(Debug)]
pub struct EarlyExit {
    /// The rendered message (help text or error) to print.
    pub output: String,
    /// `Ok` for a help/usage request (exit success), `Err` for a parse error.
    pub status: Result<(), ()>,
}

impl EarlyExit {
    /// Classifies a `clap` parse error into the success/failure early-exit shape.
    fn from_clap(error: &clap::Error) -> Self {
        use clap::error::ErrorKind;
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

impl Cli {
    /// Parses an argument vector (program name followed by its arguments) into the
    /// typed CLI, returning an [`EarlyExit`] for a help request or a parse error.
    ///
    /// # Errors
    ///
    /// Returns an [`EarlyExit`] when the arguments request help/usage or fail to
    /// parse.
    pub fn from_args(command_name: &[&str], args: &[&str]) -> Result<Self, EarlyExit> {
        let argv: Vec<&str> = command_name.iter().chain(args).copied().collect();
        Self::try_parse_from(argv).map_err(|error| EarlyExit::from_clap(&error))
    }

    /// Translates the parsed arguments into the typed command model.
    #[must_use]
    pub fn into_command(self) -> Command {
        match self.command {
            Subcommand::Analyze(command) => Command::Analyze(command.into_options()),
            Subcommand::Backfill(command) => Command::Backfill(command.into_options()),
            Subcommand::Bless(command) => Command::Bless(command.into_options()),
            Subcommand::Collect(command) => Command::Collect(command.into_options()),
            Subcommand::Examine(command) => Command::Examine(command.into_options()),
            Subcommand::Import(command) => Command::Import(command.into_options()),
            Subcommand::Install(command) => Command::Install(command.into_options()),
            Subcommand::List(command) => Command::List(command.into_options()),
            Subcommand::MachineKey(command) => Command::MachineKey(command.into_options()),
            Subcommand::Prune(command) => Command::Prune(command.into_options()),
            Subcommand::Unbless(command) => Command::Unbless(command.into_options()),
        }
    }

    /// The top-level help text, including the command list with descriptions.
    ///
    /// Shown when the tool is invoked with no subcommand, so the available
    /// commands and what each one does are immediately visible.
    #[must_use]
    pub fn help(program_name: &str) -> String {
        // `--help` is always reported as an early exit carrying the rendered
        // help text, so the `Ok` arm is never taken in practice.
        Self::from_args(&[program_name], &["--help"])
            .err()
            .map(|early_exit| early_exit.output)
            .unwrap_or_default()
    }
}

#[derive(ClapSubcommand, Debug)]
enum Subcommand {
    /// Analyze stored history for notable patterns.
    Analyze(AnalyzeCommand),
    /// Replay `collect` across a range of historical commits.
    Backfill(BackfillCommand),
    /// Accept a benchmark's current level on the base branch as intentional.
    Bless(BlessCommand),
    /// Run the workspace benchmarks (`cargo bench`) and store the results.
    Collect(CollectCommand),
    /// Import pre-existing engine output into storage without running a benchmark
    /// engine (the collect pipeline minus `cargo bench`). Internal and hidden: it
    /// exists to validate the storage/analysis pipeline against curated engine
    /// output (for example, output produced by `cargo-bench-history-faker`), so it
    /// is kept out of the public help.
    #[command(hide = true)]
    Import(ImportCommand),
    /// Generate a starter configuration file.
    Install(InstallCommand),
    /// List the data set a matching `analyze` would include, without analyzing it.
    List(ListCommand),
    /// Print this machine's hardware fingerprint (the machine key).
    MachineKey(MachineKeyCommand),
    /// Show the raw per-commit data points of one `(benchmark, metric)` series.
    Examine(ExamineCommand),
    /// Delete stored runs (and their blessing sidecars) from the resolved data set.
    Prune(PruneCommand),
    /// Remove blessings recorded at the current commit.
    Unbless(UnblessCommand),
}

/// Environment and execution options shared by every command that reads a
/// repository's git state.
#[derive(Args, Debug)]
#[command(next_help_heading = HEADING_ENV)]
struct EnvArgs {
    /// Path to the configuration file (defaults to `.cargo/bench_history.toml`).
    /// A relative path is resolved against the working directory (not the target
    /// repository).
    #[arg(long, value_name = "PATH")]
    config: Option<PathBuf>,

    /// Repository to resolve git state from (defaults to the working directory).
    #[arg(long, value_name = "PATH")]
    repo: Option<PathBuf>,

    /// Use local filesystem storage instead of a configured cloud backend.
    ///
    /// `--local=<path>` stores history under `<path>` (a relative path resolves
    /// against the target repository — the working directory by default, or
    /// `--repo`). A bare `--local` (no value) reads the path from the
    /// `CARGO_BENCH_HISTORY_STORAGE` environment variable. Either form overrides
    /// the cloud backend in the configuration file; without `--local`, the
    /// configured cloud backend is used.
    #[arg(long, value_name = "PATH", num_args = 0..=1, require_equals = true)]
    #[expect(
        clippy::option_option,
        reason = "clap's representation of a three-state optional-value flag: absent (None), bare \
                  --local (Some(None) -> read the env var), or --local=<path> (Some(Some(path)))"
    )]
    local: Option<Option<PathBuf>>,

    /// Emit detailed diagnostic notes to standard error describing each step.
    #[arg(long)]
    verbose: bool,
}

/// Translates the `--local` flag into the typed [`LocalStorageSelection`].
///
/// `None` (flag absent) selects the configured cloud backend; `Some(Some(p))`
/// (`--local=<path>`) selects an explicit local path; `Some(None)` (bare
/// `--local`) selects the path from `CARGO_BENCH_HISTORY_STORAGE`.
#[expect(
    clippy::option_option,
    reason = "mirrors the clap field's three-state optional-value representation, mapped here to \
              the typed LocalStorageSelection"
)]
fn local_selection(local: Option<Option<PathBuf>>) -> Option<LocalStorageSelection> {
    match local {
        None => None,
        Some(Some(path)) => Some(LocalStorageSelection::Path(path)),
        Some(None) => Some(LocalStorageSelection::FromEnv),
    }
}

/// Translates the `--cache` flag into the typed [`CacheSelection`].
///
/// Mirrors [`local_selection`]'s three-state mapping: `None` (flag absent) selects
/// no cache; `Some(Some(p))` (`--cache=<path>`) selects an explicit cache
/// directory; `Some(None)` (bare `--cache`) selects the directory from
/// `CARGO_BENCH_HISTORY_CACHE`.
#[expect(
    clippy::option_option,
    reason = "mirrors the clap field's three-state optional-value representation, mapped here to \
              the typed CacheSelection"
)]
fn cache_selection(cache: Option<Option<PathBuf>>) -> Option<CacheSelection> {
    match cache {
        None => None,
        Some(Some(path)) => Some(CacheSelection::Path(path)),
        Some(None) => Some(CacheSelection::FromEnv),
    }
}

/// The `--cache` read-through cache selection, shared by the read commands
/// `analyze`, `list`, and `prune` (the write commands take no cache: they do not
/// read the bulk history). Kept out of [`EnvArgs`] so only the read commands carry
/// it, but placed in the same **Environment and execution** help group.
#[derive(Args, Debug)]
#[command(next_help_heading = HEADING_ENV)]
struct CacheArg {
    /// Mirror fetched cloud objects under a local directory so repeated reads avoid
    /// re-downloading the whole history.
    ///
    /// `--cache=<path>` mirrors under `<path>` (a relative path resolves against
    /// the target repository — the working directory by default, or `--repo`). A
    /// bare `--cache` (no value) reads the directory from the
    /// `CARGO_BENCH_HISTORY_CACHE` environment variable. The cache applies only to
    /// the cloud backend, so it conflicts with `--local` (a local backend's reads
    /// are already on disk).
    #[arg(
        long,
        value_name = "PATH",
        num_args = 0..=1,
        require_equals = true,
        conflicts_with = "local"
    )]
    #[expect(
        clippy::option_option,
        reason = "clap's representation of a three-state optional-value flag: absent (None), bare \
                  --cache (Some(None) -> read the env var), or --cache=<path> (Some(Some(path)))"
    )]
    cache: Option<Option<PathBuf>>,
}

/// The repeatable, `all`-aware discriminant filters used by every query command.
///
/// Each filter auto-detects the current machine when omitted (`--engine` has no
/// machine-derived value, so it auto-detects to every engine); repeating a filter
/// unions its values; the literal `all` removes the filter for that dimension.
#[derive(Args, Debug)]
#[command(next_help_heading = HEADING_DISCRIMINANT)]
struct QueryDiscriminantArgs {
    /// Restrict to these engines, e.g. `criterion`/`callgrind` (repeatable; `all`
    /// matches every engine; default: every engine).
    #[arg(long, value_name = "NAME")]
    engine: Vec<String>,

    /// Restrict to these full target triples, e.g. `x86_64-unknown-linux-gnu`
    /// (repeatable; `all` matches every triple; default: this machine's triple).
    #[arg(long, value_name = "TRIPLE")]
    target_triple: Vec<String>,

    /// Restrict to these machine keys (repeatable; `all` matches every
    /// machine; default: this machine's fingerprint).
    #[arg(long, value_name = "KEY")]
    machine_key: Vec<String>,
}

/// Commit selection shared by `analyze`/`list`.
#[derive(Args, Debug)]
#[command(next_help_heading = HEADING_COMMIT)]
struct TimelineArgs {
    /// Target ref whose history is used (defaults to HEAD).
    #[arg(long, value_name = "REF")]
    context: Option<String>,

    /// Base ref the target branched off from (defaults to the default branch).
    #[arg(long, value_name = "REF")]
    base: Option<String>,

    /// Only consider commits made on or after this cutoff: an RFC 3339 timestamp,
    /// a `YYYY-MM-DD` date, or a relative duration such as `6 months ago`.
    #[arg(long, value_name = "WHEN")]
    since: Option<String>,
}

/// Per-format report output shared by `analyze`/`list`/`prune`.
///
/// The text report prints to standard output by default; `--no-text` suppresses
/// it, while `--markdown` and `--json` each write that format to a file. A single
/// analysis pass backs every requested format. At least one output must remain
/// selected, so `--no-text` requires at least one of `--markdown`/`--json`.
#[derive(Args, Debug)]
#[command(next_help_heading = HEADING_OUTPUT)]
struct OutputArgs {
    /// Suppress the text report on standard output. Pair with `--markdown` and/or
    /// `--json` to direct the report to files instead.
    #[arg(long)]
    no_text: bool,

    /// Also write the Markdown report to this path (a relative path resolves
    /// against the working directory).
    #[arg(long, value_name = "PATH")]
    markdown: Option<PathBuf>,

    /// Also write the JSON report to this path (a relative path resolves against
    /// the working directory).
    #[arg(long, value_name = "PATH")]
    json: Option<PathBuf>,
}

/// Run the workspace benchmarks (`cargo bench`) and store the results.
#[derive(Args, Debug)]
struct CollectCommand {
    #[command(flatten)]
    env: EnvArgs,

    /// Benchmark the entire workspace (the default when no `--package` is given);
    /// conflicts with `--package`.
    #[arg(long, help_heading = HEADING_SCOPE, conflicts_with = "package")]
    workspace: bool,

    /// Benchmark only this package; repeatable, e.g. `-p nm -p many_cpus`
    /// (default: the whole workspace).
    #[arg(long = "package", short = 'p', value_name = "NAME", help_heading = HEADING_SCOPE)]
    package: Vec<String>,

    /// Exclude a package from a whole-workspace run; repeatable, e.g.
    /// `--exclude nm --exclude many_cpus`. Conflicts with `--package`.
    #[arg(long, value_name = "NAME", help_heading = HEADING_SCOPE, conflicts_with = "package")]
    exclude: Vec<String>,

    /// Benchmark only this bench target; repeatable (default: every bench target).
    #[arg(long, value_name = "NAME", help_heading = HEADING_SCOPE)]
    bench: Vec<String>,

    /// Activate cargo features for the benchmark build; space- or comma-separated,
    /// repeatable. Forwarded verbatim to `cargo bench` as `--features`.
    #[arg(long, value_name = "FEATURES", help_heading = HEADING_FEATURES)]
    features: Vec<String>,

    /// Activate all cargo features of all selected packages (`--all-features`).
    #[arg(long, help_heading = HEADING_FEATURES)]
    all_features: bool,

    /// Do not activate the `default` cargo feature (`--no-default-features`).
    #[arg(long, help_heading = HEADING_FEATURES)]
    no_default_features: bool,

    /// Harvest and build results without storing them.
    #[arg(long, help_heading = HEADING_ENV)]
    no_store: bool,

    /// Replace an already-stored result for this run instead of refusing it as a
    /// duplicate.
    #[arg(long, help_heading = HEADING_ENV)]
    overwrite: bool,

    /// Treat an already-stored result for this run as a success that writes
    /// nothing, instead of refusing it as a duplicate. Mutually exclusive with
    /// `--overwrite`; the append-only mode the CI collection uses.
    #[arg(long, help_heading = HEADING_ENV, conflicts_with = "overwrite")]
    skip_existing: bool,

    /// Run the whole suite this many times and keep, per metric, the best
    /// (minimum) observed value — a noise-reduction pass for jittery runners.
    /// Every run must produce the same benchmark cases and the same metrics
    /// per case or collection fails.
    #[arg(long = "best-of", value_name = "N", default_value_t = NonZeroUsize::MIN, help_heading = HEADING_ENV)]
    best_of: NonZeroUsize,

    /// Arguments after `--` forwarded verbatim to `cargo bench` after the scope
    /// flags.
    #[arg(last = true, value_name = "ARGS")]
    passthrough: Vec<String>,
}

impl CollectCommand {
    fn into_options(self) -> CollectOptions {
        CollectOptions {
            config_path: self.env.config,
            repo: self.env.repo,
            local: local_selection(self.env.local),
            packages: resolve_packages(self.workspace, self.package),
            excludes: self.exclude,
            benches: self.bench,
            features: self.features,
            all_features: self.all_features,
            no_default_features: self.no_default_features,
            no_store: self.no_store,
            overwrite: self.overwrite,
            skip_existing: self.skip_existing,
            passthrough: self.passthrough,
            verbose: self.env.verbose,
            best_of: self.best_of,
        }
    }
}

/// Import pre-existing engine output into storage (the collect pipeline minus the
/// `cargo bench` run). Internal and hidden.
///
/// It keeps collect's environment/storage flags and adds the required scan
/// directory plus the metadata overrides that let one host synthesize data for
/// another target triple or commit. It has no benchmark-scope or feature flags
/// because it never runs a build.
#[derive(Args, Debug)]
struct ImportCommand {
    #[command(flatten)]
    env: EnvArgs,

    /// The directory tree to scan for pre-existing engine output. Required, with
    /// no default: an ungated harvest of the shared `target/` directory would
    /// sweep stale leftovers from unrelated runs into one import, so the caller
    /// must name the tree it curated.
    #[arg(long, value_name = "PATH", help_heading = HEADING_ENV)]
    target_dir: PathBuf,

    /// Override the target triple the results are partitioned under (and recorded
    /// against), so a single host can synthesize data for another target.
    #[arg(long, value_name = "TRIPLE", help_heading = HEADING_DISCRIMINANT)]
    target_triple: Option<String>,

    /// Override the commit the run is keyed under (default: the current commit).
    /// Resolved through git, so it must name a real commit; it avoids checking the
    /// commit out, not the requirement that it exist.
    #[arg(long, value_name = "COMMIT", help_heading = HEADING_COMMIT)]
    commit: Option<String>,

    /// Store a dirty snapshot keyed by the import-time second instead of a clean
    /// object, as if the working tree had uncommitted changes.
    #[arg(long, help_heading = HEADING_DISCRIMINANT)]
    dirty: bool,

    /// Replace an already-stored result for this run instead of refusing it as a
    /// duplicate.
    #[arg(long, help_heading = HEADING_ENV)]
    overwrite: bool,

    /// Treat an already-stored result for this run as a success that writes
    /// nothing, instead of refusing it as a duplicate. Mutually exclusive with
    /// `--overwrite`.
    #[arg(long, help_heading = HEADING_ENV, conflicts_with = "overwrite")]
    skip_existing: bool,
}

impl ImportCommand {
    fn into_options(self) -> ImportOptions {
        ImportOptions {
            config_path: self.env.config,
            repo: self.env.repo,
            local: local_selection(self.env.local),
            target_dir: self.target_dir,
            target_triple: self.target_triple,
            commit: self.commit,
            dirty: self.dirty,
            overwrite: self.overwrite,
            skip_existing: self.skip_existing,
            verbose: self.env.verbose,
        }
    }
}

/// Generate a starter configuration file.
#[derive(Args, Debug)]
struct InstallCommand {
    /// Path to the configuration file to generate.
    #[arg(long, value_name = "PATH", help_heading = HEADING_ENV)]
    config: Option<PathBuf>,

    /// Emit detailed diagnostic notes to standard error (which path is written,
    /// or that an existing configuration was left unchanged).
    #[arg(long, help_heading = HEADING_ENV)]
    verbose: bool,
}

impl InstallCommand {
    fn into_options(self) -> InstallOptions {
        InstallOptions {
            config_path: self.config,
            verbose: self.verbose,
        }
    }
}

/// Print this machine's hardware fingerprint (the machine key).
#[derive(Args, Debug)]
struct MachineKeyCommand {
    /// Also emit the individual hardware factors that make up the fingerprint to
    /// standard error (the fingerprint version, processor count, memory-region
    /// count and processor models), so a change in the key can be traced to which
    /// factor changed. Per-processor speeds are recorded with collected runs as
    /// hardware provenance, but they are not factors and so are not reported here.
    /// The key itself always goes to standard output.
    #[arg(long, help_heading = HEADING_ENV)]
    verbose: bool,
}

impl MachineKeyCommand {
    fn into_options(self) -> MachineKeyOptions {
        MachineKeyOptions {
            verbose: self.verbose,
        }
    }
}

/// Analyze stored history for notable patterns.
#[derive(Args, Debug)]
struct AnalyzeCommand {
    /// Benchmark-id prefixes to analyze, matched against the qualified identity
    /// (for example, `all_the_time/read_cell` or a family prefix
    /// `overhead/groups_`); repeatable (default: every benchmark).
    #[arg(value_name = "PREFIX")]
    prefixes: Vec<BenchmarkIdPrefix>,

    #[command(flatten)]
    env: EnvArgs,

    #[command(flatten)]
    cache: CacheArg,

    #[command(flatten)]
    output: OutputArgs,

    #[command(flatten)]
    discriminants: QueryDiscriminantArgs,

    #[command(flatten)]
    timeline: TimelineArgs,

    /// Exclude dirty (uncommitted-tree) snapshots from the analysis.
    #[arg(long, help_heading = HEADING_FILTER)]
    no_dirty: bool,

    /// Also write a condensed Markdown summary — only the most significant findings
    /// — to this path (a relative path resolves against the working directory). The
    /// full `--markdown` report carries every finding; this summary is capped so a
    /// large analysis still fits within a GitHub issue body.
    #[arg(long, value_name = "PATH", help_heading = HEADING_OUTPUT)]
    markdown_summary: Option<PathBuf>,
}

impl AnalyzeCommand {
    fn into_options(self) -> AnalyzeOptions {
        AnalyzeOptions {
            config_path: self.env.config,
            repo: self.env.repo,
            local: local_selection(self.env.local),
            cache: cache_selection(self.cache.cache),
            context: self.timeline.context,
            base: self.timeline.base,
            no_dirty: self.no_dirty,
            since: self.timeline.since,
            engine: self.discriminants.engine,
            target_triple: self.discriminants.target_triple,
            machine_key: self.discriminants.machine_key,
            prefixes: self.prefixes,
            no_text: self.output.no_text,
            markdown: self.output.markdown,
            json: self.output.json,
            markdown_summary: self.markdown_summary,
            verbose: self.env.verbose,
            timing: false,
        }
    }
}

/// What a `list` invocation enumerates.
#[derive(Clone, Copy, Debug, ValueEnum)]
enum ListSubjectArg {
    /// The runs that would enter a matching `analyze` pass.
    Runs,
    /// Every discriminant set present in storage (no repository required); a
    /// discovery catalog that lists all partitions regardless of the current
    /// machine. Pass a discriminant filter to narrow it.
    Discriminants,
    /// The blessings recorded at the current commit (or, with `--all`, across the
    /// whole analysis window).
    Blessings,
}

impl From<ListSubjectArg> for ListSubject {
    fn from(subject: ListSubjectArg) -> Self {
        match subject {
            ListSubjectArg::Runs => Self::Runs,
            ListSubjectArg::Discriminants => Self::Discriminants,
            ListSubjectArg::Blessings => Self::Blessings,
        }
    }
}

/// List the data set a matching `analyze` would include, without analyzing it.
#[derive(Args, Debug)]
struct ListCommand {
    /// What to list: `runs`, `discriminants`, or `blessings`.
    #[arg(value_name = "runs|discriminants|blessings")]
    subject: ListSubjectArg,

    #[command(flatten)]
    env: EnvArgs,

    #[command(flatten)]
    cache: CacheArg,

    #[command(flatten)]
    output: OutputArgs,

    #[command(flatten)]
    discriminants: QueryDiscriminantArgs,

    #[command(flatten)]
    timeline: TimelineArgs,

    /// Exclude dirty (uncommitted-tree) snapshots from the listing.
    #[arg(long, help_heading = HEADING_FILTER)]
    no_dirty: bool,

    /// With the `blessings` subject, list the most recent blessing of every
    /// benchmark across the whole analysis window rather than only those at the
    /// current commit. Errors if given with any other subject.
    #[arg(long, help_heading = HEADING_FILTER)]
    all: bool,
}

impl ListCommand {
    fn into_options(self) -> ListOptions {
        ListOptions {
            subject: self.subject.into(),
            config_path: self.env.config,
            repo: self.env.repo,
            local: local_selection(self.env.local),
            cache: cache_selection(self.cache.cache),
            context: self.timeline.context,
            base: self.timeline.base,
            no_dirty: self.no_dirty,
            since: self.timeline.since,
            engine: self.discriminants.engine,
            target_triple: self.discriminants.target_triple,
            machine_key: self.discriminants.machine_key,
            no_text: self.output.no_text,
            markdown: self.output.markdown,
            json: self.output.json,
            all: self.all,
            verbose: self.env.verbose,
        }
    }
}

/// Show the raw per-commit data points of one `(benchmark, metric)` series.
///
/// A drill-down sibling of `list runs`: it resolves exactly the data set a matching
/// `analyze`/`list` would, then pivots one named series into a per-commit listing —
/// every commit from the earliest one carrying the series in any matching set up to
/// the analyzed tip, in git first-parent order, pairing the value with the short
/// commit id and the start of the commit's title. A commit with several observations
/// contributes one row per observation, clean before dirty; a commit with no data
/// point reads `n/a`. Both `--benchmark` and `--metric` are required.
#[derive(Args, Debug)]
struct ExamineCommand {
    #[command(flatten)]
    env: EnvArgs,

    #[command(flatten)]
    cache: CacheArg,

    #[command(flatten)]
    output: OutputArgs,

    #[command(flatten)]
    discriminants: QueryDiscriminantArgs,

    #[command(flatten)]
    timeline: TimelineArgs,

    /// The exact qualified benchmark id to examine, e.g.
    /// `nm/nm::observe/pull` (required). Copy it from an `analyze` finding.
    #[arg(long, value_name = "ID", help_heading = HEADING_SCOPE)]
    benchmark: String,

    /// The metric to examine by its stable name, e.g. `instruction_count` or
    /// `wall_time` (required). Copy it from an `analyze` finding.
    #[arg(long, value_name = "NAME", help_heading = HEADING_SCOPE)]
    metric: String,

    /// Exclude dirty (uncommitted-tree) snapshots from the pivot.
    #[arg(long, help_heading = HEADING_FILTER)]
    no_dirty: bool,
}

impl ExamineCommand {
    fn into_options(self) -> ExamineOptions {
        ExamineOptions {
            config_path: self.env.config,
            repo: self.env.repo,
            local: local_selection(self.env.local),
            cache: cache_selection(self.cache.cache),
            context: self.timeline.context,
            base: self.timeline.base,
            no_dirty: self.no_dirty,
            since: self.timeline.since,
            engine: self.discriminants.engine,
            target_triple: self.discriminants.target_triple,
            machine_key: self.discriminants.machine_key,
            benchmark: self.benchmark,
            metric: self.metric,
            no_text: self.output.no_text,
            markdown: self.output.markdown,
            json: self.output.json,
            verbose: self.env.verbose,
        }
    }
}

/// Delete stored runs from the data set a matching `analyze`/`list` would resolve.
///
/// You must say what to delete with `--clean`, `--dirty`, `--all`, or
/// `--include-blessings`. Pruning runs never removes a blessing on its own;
/// `--include-blessings` additionally deletes blessing sidecars in the selected
/// range (including on commits with no recorded run) and may be given alone.
/// Pruning walks the selected commits from `--context` back to `--base`; deleting
/// the base branch's own data set requires the `--prune-base` guard.
#[derive(Args, Debug)]
#[command(
    group(
        ArgGroup::new("prune-action")
            .args(["clean", "dirty", "all", "include_blessings"])
            .required(true)
            .multiple(true)
    ),
    group(
        ArgGroup::new("prune-run-kind")
            .args(["clean", "dirty", "all"])
    )
)]
struct PruneCommand {
    /// Restrict removal to these commits (a full or short commit ID, prefix-matched);
    /// repeatable (default: every one of the selected commits).
    #[arg(value_name = "COMMIT")]
    commit: Vec<String>,

    #[command(flatten)]
    env: EnvArgs,

    #[command(flatten)]
    cache: CacheArg,

    /// Preview what would be removed without deleting anything.
    #[arg(long, help_heading = HEADING_ENV)]
    dry_run: bool,

    /// Confirm pruning the base branch's own data set (required when `--context`
    /// resolves to the same commit as `--base`).
    #[arg(long, help_heading = HEADING_ENV)]
    prune_base: bool,

    #[command(flatten)]
    output: OutputArgs,

    #[command(flatten)]
    discriminants: QueryDiscriminantArgs,

    #[command(flatten)]
    commit_selection: PruneCommitArgs,

    /// Remove only clean runs.
    #[arg(long, help_heading = HEADING_FILTER)]
    clean: bool,

    /// Remove only dirty (uncommitted-tree) snapshots.
    #[arg(long, help_heading = HEADING_FILTER)]
    dirty: bool,

    /// Remove both clean and dirty runs (the same as `--clean --dirty`).
    #[arg(long, help_heading = HEADING_FILTER)]
    all: bool,

    /// Also remove blessing sidecars in the selected range, including on commits
    /// with no recorded run. May be given alone to remove only blessings.
    #[arg(long, help_heading = HEADING_FILTER)]
    include_blessings: bool,
}

/// Commit selection for `prune`: the range of commits whose data is removed.
#[derive(Args, Debug)]
#[command(next_help_heading = HEADING_COMMIT)]
struct PruneCommitArgs {
    /// Target ref whose data set is pruned, walking back until the base ref
    /// (defaults to HEAD).
    #[arg(long, value_name = "REF")]
    context: Option<String>,

    /// Base ref that the context branched off from; pruning stops on reaching it
    /// (defaults to the default branch).
    #[arg(long, value_name = "REF")]
    base: Option<String>,

    /// Only prune commits made on or after this cutoff: an RFC 3339 timestamp, a
    /// `YYYY-MM-DD` date, or a relative duration such as `6 months ago`.
    #[arg(long, value_name = "WHEN")]
    since: Option<String>,
}

impl PruneCommand {
    fn into_options(self) -> PruneOptions {
        // `--all` is the union of the two specific kinds.
        let clean = self.clean || self.all;
        let dirty = self.dirty || self.all;
        PruneOptions {
            config_path: self.env.config,
            repo: self.env.repo,
            local: local_selection(self.env.local),
            cache: cache_selection(self.cache.cache),
            context: self.commit_selection.context,
            base: self.commit_selection.base,
            commit: self.commit,
            since: self.commit_selection.since,
            engine: self.discriminants.engine,
            target_triple: self.discriminants.target_triple,
            machine_key: self.discriminants.machine_key,
            clean,
            dirty,
            include_blessings: self.include_blessings,
            prune_base: self.prune_base,
            dry_run: self.dry_run,
            no_text: self.output.no_text,
            markdown: self.output.markdown,
            json: self.output.json,
            verbose: self.env.verbose,
        }
    }
}

/// Replay `collect` across a range of historical commits.
///
/// The range is walked newest commit first, so a run that is cut short has filled
/// the most recent — and most comparison-relevant — gaps.
#[derive(Args, Debug)]
struct BackfillCommand {
    /// Oldest commit of the range to backfill, inclusive; a commit ID, tag, or ref such
    /// as `HEAD~20`.
    #[arg(value_name = "FROM")]
    from: String,

    /// Newest commit of the range to backfill, inclusive; a commit ID, tag, or ref such
    /// as `HEAD`. Must be reachable from `<FROM>` along first-parent history.
    #[arg(value_name = "TO")]
    to: String,

    #[command(flatten)]
    env: EnvArgs,

    /// Benchmark the entire workspace (the default when no `--package` is given);
    /// conflicts with `--package`.
    #[arg(long, help_heading = HEADING_SCOPE, conflicts_with = "package")]
    workspace: bool,

    /// Benchmark only this package; repeatable, e.g. `-p nm -p many_cpus`
    /// (default: the whole workspace).
    #[arg(long = "package", short = 'p', value_name = "NAME", help_heading = HEADING_SCOPE)]
    package: Vec<String>,

    /// Exclude a package from a whole-workspace run; repeatable, e.g.
    /// `--exclude nm --exclude many_cpus`. Conflicts with `--package`.
    #[arg(long, value_name = "NAME", help_heading = HEADING_SCOPE, conflicts_with = "package")]
    exclude: Vec<String>,

    /// Benchmark only this bench target; repeatable (default: every bench target).
    #[arg(long, value_name = "NAME", help_heading = HEADING_SCOPE)]
    bench: Vec<String>,

    /// Activate cargo features for the benchmark build; space- or comma-separated,
    /// repeatable. Forwarded verbatim to `cargo bench` as `--features`.
    #[arg(long, value_name = "FEATURES", help_heading = HEADING_FEATURES)]
    features: Vec<String>,

    /// Activate all cargo features of all selected packages (`--all-features`).
    #[arg(long, help_heading = HEADING_FEATURES)]
    all_features: bool,

    /// Do not activate the `default` cargo feature (`--no-default-features`).
    #[arg(long, help_heading = HEADING_FEATURES)]
    no_default_features: bool,

    /// Replace already-stored results for the backfilled commits instead of
    /// skipping them as duplicates.
    #[arg(long, help_heading = HEADING_ENV)]
    overwrite: bool,

    /// Continue past commits whose build or benchmark fails instead of stopping at
    /// the newest failing one.
    #[arg(long, help_heading = HEADING_ENV)]
    ignore_errors: bool,

    /// Run the whole suite this many times per commit and keep, per metric, the
    /// best (minimum) observed value — a noise-reduction pass for jittery runners.
    /// Every run must produce the same benchmark cases and the same metrics
    /// per case or collection fails.
    #[arg(long = "best-of", value_name = "N", default_value_t = NonZeroUsize::MIN, help_heading = HEADING_ENV)]
    best_of: NonZeroUsize,

    /// Arguments after `--` forwarded verbatim to `cargo bench` after the scope
    /// flags.
    #[arg(last = true, value_name = "ARGS")]
    passthrough: Vec<String>,
}

impl BackfillCommand {
    fn into_options(self) -> BackfillOptions {
        BackfillOptions {
            config_path: self.env.config,
            repo: self.env.repo,
            local: local_selection(self.env.local),
            from: self.from,
            to: self.to,
            packages: resolve_packages(self.workspace, self.package),
            excludes: self.exclude,
            benches: self.bench,
            features: self.features,
            all_features: self.all_features,
            no_default_features: self.no_default_features,
            overwrite: self.overwrite,
            ignore_errors: self.ignore_errors,
            passthrough: self.passthrough,
            verbose: self.env.verbose,
            best_of: self.best_of,
        }
    }
}

/// Accept a benchmark's current level on the base branch as intentional.
#[derive(Args, Debug)]
struct BlessCommand {
    /// Benchmark-id prefixes to accept, matched against the qualified identity
    /// (for example, `all_the_time/read_cell` or a family prefix
    /// `overhead/groups_`). At least one is required unless `--all` is given.
    #[arg(value_name = "PREFIX")]
    prefixes: Vec<BenchmarkIdPrefix>,

    #[command(flatten)]
    env: EnvArgs,

    /// Accept every benchmark recorded at the context commit, with no prefixes.
    #[arg(long, conflicts_with = "prefixes")]
    all: bool,

    /// Commit to bless (defaults to HEAD). Use this to bless a commit other than
    /// the one currently checked out.
    #[arg(long, value_name = "REF", help_heading = HEADING_COMMIT)]
    context: Option<String>,

    /// Base ref the context commit must be on (defaults to the default branch).
    #[arg(long, value_name = "REF", help_heading = HEADING_COMMIT)]
    base: Option<String>,

    #[command(flatten)]
    discriminants: QueryDiscriminantArgs,
}

impl BlessCommand {
    fn into_options(self) -> BlessOptions {
        BlessOptions {
            config_path: self.env.config,
            repo: self.env.repo,
            local: local_selection(self.env.local),
            context: self.context,
            base: self.base,
            engine: self.discriminants.engine,
            target_triple: self.discriminants.target_triple,
            machine_key: self.discriminants.machine_key,
            prefixes: self.prefixes,
            all: self.all,
            verbose: self.env.verbose,
        }
    }
}

/// Remove blessings recorded at the context commit.
///
/// Only blessings recorded at the context commit are removed. Blessings issued
/// at later commits remain in effect, so the timeline may still be blessed past
/// the context commit.
#[derive(Args, Debug)]
struct UnblessCommand {
    #[command(flatten)]
    env: EnvArgs,

    /// Commit to unbless (defaults to HEAD). Use this to unbless a commit other
    /// than the one currently checked out.
    #[arg(long, value_name = "REF", help_heading = HEADING_COMMIT)]
    context: Option<String>,

    /// Base ref the context commit must be on (defaults to the default branch).
    #[arg(long, value_name = "REF", help_heading = HEADING_COMMIT)]
    base: Option<String>,

    #[command(flatten)]
    discriminants: QueryDiscriminantArgs,
}

impl UnblessCommand {
    fn into_options(self) -> UnblessOptions {
        UnblessOptions {
            config_path: self.env.config,
            repo: self.env.repo,
            local: local_selection(self.env.local),
            context: self.context,
            base: self.base,
            engine: self.discriminants.engine,
            target_triple: self.discriminants.target_triple,
            machine_key: self.discriminants.machine_key,
            verbose: self.env.verbose,
        }
    }
}

/// Resolves the benchmark scope. `--workspace` and `--package` are mutually
/// exclusive at the CLI level, so `--workspace` simply yields the empty package
/// list that means "the whole workspace" — the same as passing no scope at all.
fn resolve_packages(workspace: bool, package: Vec<String>) -> Vec<String> {
    if workspace { Vec::new() } else { package }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    #[cfg(miri)]
    use clap::{Command as ClapCommand, FromArgMatches};

    use super::*;

    fn from_args(command_name: &[&str], args: &[&str]) -> Result<Cli, EarlyExit> {
        #[cfg(not(miri))]
        {
            Cli::from_args(command_name, args)
        }
        #[cfg(miri)]
        {
            // Each option test needs only its selected subcommand. Use its real Args schema and
            // the real Cli conversion, avoiding construction of every unrelated schema in Miri.
            // Native runs retain the full root parser, including subcommand registration.
            let command = match args.first().copied() {
                Some("analyze") => AnalyzeCommand::augment_args(ClapCommand::new("analyze")),
                Some("backfill") => BackfillCommand::augment_args(ClapCommand::new("backfill")),
                Some("bless") => BlessCommand::augment_args(ClapCommand::new("bless")),
                Some("collect") => CollectCommand::augment_args(ClapCommand::new("collect")),
                Some("examine") => ExamineCommand::augment_args(ClapCommand::new("examine")),
                Some("import") => ImportCommand::augment_args(ClapCommand::new("import")),
                Some("list") => ListCommand::augment_args(ClapCommand::new("list")),
                Some("machine-key") => {
                    MachineKeyCommand::augment_args(ClapCommand::new("machine-key"))
                }
                Some("prune") => PruneCommand::augment_args(ClapCommand::new("prune")),
                Some("unbless") => UnblessCommand::augment_args(ClapCommand::new("unbless")),
                // A small registered command also lets root-level error/help tests run.
                _ => InstallCommand::augment_args(ClapCommand::new("install")),
            };
            ClapCommand::new("cargo-bench-history")
                .disable_help_subcommand(true)
                .disable_version_flag(true)
                .subcommand_required(true)
                .arg_required_else_help(true)
                .subcommand(command)
                .try_get_matches_from(command_name.iter().chain(args).copied())
                .and_then(|matches| Cli::from_arg_matches(&matches))
                .map_err(|error| EarlyExit::from_clap(&error))
        }
    }

    fn parse(args: &[&str]) -> Command {
        from_args(&["cargo-bench-history"], args)
            .unwrap()
            .into_command()
    }

    #[test]
    fn cli_is_debug_formatted() {
        let cli = from_args(&["cargo-bench-history"], &["collect"]).unwrap();
        assert!(format!("{cli:?}").contains("Collect"), "{cli:?}");
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "renders the full command catalog; option schemas run individually under Miri"
    )]
    fn help_lists_every_command() {
        let help = Cli::help("cargo-bench-history");
        assert!(!help.is_empty(), "help text is non-empty");
        for command in [
            "analyze", "backfill", "bless", "collect", "examine", "install", "list", "prune",
            "unbless",
        ] {
            assert!(help.contains(command), "help lists {command}: {help}");
        }
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "renders the full command catalog; option schemas run individually under Miri"
    )]
    fn import_is_hidden_from_help() {
        // `import` parses (it is registered below) but is deliberately kept out of
        // the public help, so it must not appear as an entry in the command list.
        // Match the subcommand name as the first token of a help line rather than a
        // bare substring, so an unrelated word like "important" in a description
        // cannot mask a regression that re-exposes the command.
        let help = Cli::help("cargo-bench-history");
        let listed = help
            .lines()
            .any(|line| line.split_whitespace().next() == Some("import"));
        assert!(!listed, "import is hidden from help: {help}");
    }

    #[test]
    fn import_parses_target_dir_and_metadata_overrides() {
        let command = parse(&[
            "import",
            "--target-dir",
            "curated/target",
            "--target-triple",
            "aarch64-apple-darwin",
            "--commit",
            "release-1.0",
            "--dirty",
            "--overwrite",
        ]);
        let Command::Import(options) = command else {
            panic!("expected import command");
        };
        assert_eq!(options.target_dir, PathBuf::from("curated/target"));
        assert_eq!(
            options.target_triple.as_deref(),
            Some("aarch64-apple-darwin")
        );
        assert_eq!(options.commit.as_deref(), Some("release-1.0"));
        assert!(options.dirty);
        assert!(options.overwrite);
        assert!(!options.skip_existing);
    }

    #[test]
    fn import_requires_target_dir() {
        // The harvest is ungated, so the tree to scan must be named explicitly
        // rather than defaulting to the shared `target/` directory.
        let error = from_args(&["cargo-bench-history"], &["import"]).unwrap_err();
        assert_eq!(error.status, Err(()));
        assert!(error.output.contains("--target-dir"), "{}", error.output);
    }

    #[test]
    fn import_overwrite_and_skip_existing_conflict() {
        let error = from_args(
            &["cargo-bench-history"],
            &[
                "import",
                "--target-dir",
                "t",
                "--overwrite",
                "--skip-existing",
            ],
        )
        .unwrap_err();
        assert_eq!(error.status, Err(()));
        assert!(
            error.output.contains("cannot be used with"),
            "{}",
            error.output
        );
    }

    #[test]
    fn collect_parses_scope_and_passthrough() {
        let command = parse(&[
            "collect",
            "--package",
            "nm",
            "-p",
            "many_cpus",
            "--bench",
            "nm_observe",
            "--",
            "--noplot",
        ]);
        let Command::Collect(options) = command else {
            panic!("expected collect command");
        };
        assert_eq!(
            options.packages,
            vec!["nm".to_owned(), "many_cpus".to_owned()]
        );
        assert_eq!(options.benches, vec!["nm_observe".to_owned()]);
        assert_eq!(options.passthrough, vec!["--noplot".to_owned()]);
        assert!(!options.overwrite);
    }

    #[test]
    fn collect_workspace_and_package_conflict() {
        let error = from_args(
            &["cargo-bench-history"],
            &["collect", "--workspace", "-p", "nm"],
        )
        .unwrap_err();
        assert_eq!(error.status, Err(()));
        assert!(
            error.output.contains("cannot be used with"),
            "{}",
            error.output
        );
    }

    #[test]
    fn collect_parses_exclude_filters() {
        let command = parse(&["collect", "--exclude", "nm", "--exclude", "many_cpus"]);
        let Command::Collect(options) = command else {
            panic!("expected collect command");
        };
        assert!(
            options.packages.is_empty(),
            "exclude implies workspace scope"
        );
        assert_eq!(
            options.excludes,
            vec!["nm".to_owned(), "many_cpus".to_owned()]
        );
    }

    #[test]
    fn collect_parses_feature_selection() {
        let command = parse(&[
            "collect",
            "--features",
            "foo,bar",
            "--features",
            "baz",
            "--no-default-features",
        ]);
        let Command::Collect(options) = command else {
            panic!("expected collect command");
        };
        assert_eq!(
            options.features,
            vec!["foo,bar".to_owned(), "baz".to_owned()]
        );
        assert!(!options.all_features);
        assert!(options.no_default_features);
    }

    #[test]
    fn collect_parses_all_features() {
        let command = parse(&["collect", "--all-features"]);
        let Command::Collect(options) = command else {
            panic!("expected collect command");
        };
        assert!(options.all_features);
        assert!(options.features.is_empty());
    }

    #[test]
    fn collect_best_of_defaults_to_one() {
        let Command::Collect(options) = parse(&["collect"]) else {
            panic!("expected collect command");
        };
        assert_eq!(
            options.best_of.get(),
            1,
            "--best-of defaults to a single run"
        );
    }

    #[test]
    fn collect_best_of_parses_a_value() {
        let Command::Collect(options) = parse(&["collect", "--best-of", "5", "--no-store"]) else {
            panic!("expected collect command");
        };
        assert_eq!(options.best_of.get(), 5);
        assert!(options.no_store, "--best-of coexists with --no-store");
    }

    #[test]
    fn collect_best_of_rejects_zero() {
        let parsed = from_args(&["cargo-bench-history"], &["collect", "--best-of", "0"]);
        assert!(parsed.is_err(), "--best-of 0 must be rejected");
    }

    #[test]
    fn collect_exclude_and_package_conflict() {
        let error = from_args(
            &["cargo-bench-history"],
            &["collect", "--exclude", "nm", "-p", "many_cpus"],
        )
        .unwrap_err();
        assert_eq!(error.status, Err(()));
        assert!(
            error.output.contains("cannot be used with"),
            "{}",
            error.output
        );
    }

    #[test]
    fn backfill_workspace_and_package_conflict() {
        let error = from_args(
            &["cargo-bench-history"],
            &["backfill", "abc", "def", "--workspace", "-p", "nm"],
        )
        .unwrap_err();
        assert_eq!(error.status, Err(()));
        assert!(
            error.output.contains("cannot be used with"),
            "{}",
            error.output
        );
    }

    #[test]
    fn backfill_collects_exclude_filters() {
        let command = parse(&["backfill", "abc", "def", "--exclude", "nm"]);
        let Command::Backfill(options) = command else {
            panic!("expected backfill command");
        };
        assert!(
            options.packages.is_empty(),
            "exclude implies workspace scope"
        );
        assert_eq!(options.excludes, vec!["nm".to_owned()]);
    }

    #[test]
    fn backfill_collects_feature_selection() {
        let command = parse(&[
            "backfill",
            "abc",
            "def",
            "--features",
            "foo",
            "--all-features",
        ]);
        let Command::Backfill(options) = command else {
            panic!("expected backfill command");
        };
        assert_eq!(options.features, vec!["foo".to_owned()]);
        assert!(options.all_features);
        assert!(!options.no_default_features);
    }

    #[test]
    fn backfill_exclude_and_package_conflict() {
        let error = from_args(
            &["cargo-bench-history"],
            &[
                "backfill",
                "abc",
                "def",
                "--exclude",
                "nm",
                "-p",
                "many_cpus",
            ],
        )
        .unwrap_err();
        assert_eq!(error.status, Err(()));
        assert!(
            error.output.contains("cannot be used with"),
            "{}",
            error.output
        );
    }

    #[test]
    fn collect_parses_overwrite_switch() {
        let command = parse(&["collect", "--overwrite"]);
        let Command::Collect(options) = command else {
            panic!("expected collect command");
        };
        assert!(options.overwrite);
    }

    #[test]
    fn collect_parses_skip_existing_switch() {
        let command = parse(&["collect", "--skip-existing"]);
        let Command::Collect(options) = command else {
            panic!("expected collect command");
        };
        assert!(options.skip_existing);
        assert!(!options.overwrite);
    }

    #[test]
    fn collect_rejects_skip_existing_with_overwrite() {
        let parsed = from_args(
            &["cargo-bench-history"],
            &["collect", "--overwrite", "--skip-existing"],
        );
        assert!(
            parsed.is_err(),
            "--skip-existing and --overwrite are mutually exclusive"
        );
    }

    #[test]
    fn collect_parses_repo() {
        let command = parse(&["collect", "--repo", "/work/folo"]);
        let Command::Collect(options) = command else {
            panic!("expected collect command");
        };
        assert_eq!(options.repo, Some(PathBuf::from("/work/folo")));
    }

    #[test]
    fn local_defaults_to_none() {
        let Command::Collect(options) = parse(&["collect"]) else {
            panic!("expected collect command");
        };
        assert_eq!(options.local, None);
    }

    #[test]
    fn local_with_value_selects_an_explicit_path() {
        let Command::Collect(options) = parse(&["collect", "--local=./store"]) else {
            panic!("expected collect command");
        };
        assert_eq!(
            options.local,
            Some(LocalStorageSelection::Path(PathBuf::from("./store")))
        );
    }

    #[test]
    fn bare_local_selects_the_environment_variable() {
        let Command::Analyze(options) = parse(&["analyze", "--local"]) else {
            panic!("expected analyze command");
        };
        assert_eq!(options.local, Some(LocalStorageSelection::FromEnv));
    }

    #[test]
    fn local_requires_equals_for_its_value() {
        // Space-separated form is rejected so a trailing positional can never be
        // mistaken for the `--local` path.
        let Command::Backfill(options) = parse(&["backfill", "--local", "abc", "def"]) else {
            panic!("expected backfill command");
        };
        assert_eq!(options.local, Some(LocalStorageSelection::FromEnv));
        assert_eq!(options.from, "abc");
        assert_eq!(options.to, "def");
    }

    #[test]
    fn cache_defaults_to_none() {
        let Command::Analyze(options) = parse(&["analyze"]) else {
            panic!("expected analyze command");
        };
        assert_eq!(options.cache, None);
    }

    #[test]
    fn cache_with_value_selects_an_explicit_path() {
        let Command::Analyze(options) = parse(&["analyze", "--cache=./mirror"]) else {
            panic!("expected analyze command");
        };
        assert_eq!(
            options.cache,
            Some(CacheSelection::Path(PathBuf::from("./mirror")))
        );
    }

    #[test]
    fn bare_cache_selects_the_environment_variable() {
        let Command::List(options) = parse(&["list", "discriminants", "--cache"]) else {
            panic!("expected list command");
        };
        assert_eq!(options.cache, Some(CacheSelection::FromEnv));
    }

    #[test]
    fn prune_parses_cache() {
        let Command::Prune(options) = parse(&["prune", "--clean", "--cache=./mirror"]) else {
            panic!("expected prune command");
        };
        assert_eq!(
            options.cache,
            Some(CacheSelection::Path(PathBuf::from("./mirror")))
        );
    }

    #[test]
    fn cache_requires_equals_for_its_value() {
        // Like `--local`, the space-separated form binds nothing so a following
        // positional (an analyze prefix) is never swallowed as the cache path.
        let Command::Analyze(options) = parse(&["analyze", "--cache", "all_the_time/read_cell"])
        else {
            panic!("expected analyze command");
        };
        assert_eq!(options.cache, Some(CacheSelection::FromEnv));
        assert_eq!(
            options.prefixes,
            vec![BenchmarkIdPrefix::new("all_the_time/read_cell").unwrap()]
        );
    }

    #[test]
    fn analyze_cache_conflicts_with_local() {
        // The read-through cache applies only to the cloud backend, so pairing
        // `--cache` with `--local` is a usage error rather than a silent ignore.
        let parsed = from_args(
            &["cargo-bench-history"],
            &["analyze", "--local=./store", "--cache=./mirror"],
        );
        assert!(
            parsed.is_err(),
            "--cache and --local are mutually exclusive"
        );
    }

    #[test]
    fn list_cache_conflicts_with_local() {
        assert!(
            from_args(
                &["cargo-bench-history"],
                &[
                    "list",
                    "discriminants",
                    "--local=./store",
                    "--cache=./mirror"
                ],
            )
            .is_err(),
            "list must reject --cache with --local"
        );
    }

    #[test]
    fn prune_cache_conflicts_with_local() {
        assert!(
            from_args(
                &["cargo-bench-history"],
                &["prune", "--clean", "--local=./store", "--cache=./mirror"],
            )
            .is_err(),
            "prune must reject --cache with --local"
        );
    }

    #[test]
    fn collect_parses_verbose_switch() {
        let Command::Collect(options) = parse(&["collect", "--verbose"]) else {
            panic!("expected collect command");
        };
        assert!(options.verbose);
    }

    #[test]
    fn collect_verbose_defaults_to_false() {
        let Command::Collect(options) = parse(&["collect"]) else {
            panic!("expected collect command");
        };
        assert!(!options.verbose);
    }

    #[test]
    fn install_maps_to_install_command() {
        let command = parse(&["install"]);
        assert_eq!(command, Command::Install(InstallOptions::default()));
    }

    #[test]
    fn install_captures_config_path() {
        let command = parse(&["install", "--config", "custom/bench.toml"]);
        let Command::Install(options) = command else {
            panic!("expected install command");
        };
        assert_eq!(
            options.config_path,
            Some(PathBuf::from("custom/bench.toml"))
        );
    }

    #[test]
    fn install_parses_verbose_switch() {
        let Command::Install(options) = parse(&["install", "--verbose"]) else {
            panic!("expected install command");
        };
        assert!(options.verbose);
    }

    #[test]
    fn install_verbose_defaults_to_false() {
        let Command::Install(options) = parse(&["install"]) else {
            panic!("expected install command");
        };
        assert!(!options.verbose);
    }

    #[test]
    fn machine_key_maps_to_machine_key_command() {
        let command = parse(&["machine-key"]);
        assert_eq!(command, Command::MachineKey(MachineKeyOptions::default()));
    }

    #[test]
    fn machine_key_parses_verbose_switch() {
        let Command::MachineKey(options) = parse(&["machine-key", "--verbose"]) else {
            panic!("expected machine-key command");
        };
        assert!(options.verbose);
    }

    #[test]
    fn machine_key_verbose_defaults_to_false() {
        let Command::MachineKey(options) = parse(&["machine-key"]) else {
            panic!("expected machine-key command");
        };
        assert!(!options.verbose);
    }

    #[test]
    fn analyze_parses_verbose_switch() {
        let Command::Analyze(options) = parse(&["analyze", "--verbose"]) else {
            panic!("expected analyze command");
        };
        assert!(options.verbose);
    }

    #[test]
    fn analyze_verbose_defaults_to_false() {
        let Command::Analyze(options) = parse(&["analyze"]) else {
            panic!("expected analyze command");
        };
        assert!(!options.verbose);
    }

    #[test]
    fn analyze_collects_topology_and_repeatable_discriminants() {
        let command = parse(&[
            "analyze",
            "--repo",
            "/work/folo",
            "--context",
            "feature",
            "--base",
            "master",
            "--since",
            "2024-06-01T00:00:00Z",
            "--no-dirty",
            "--engine",
            "callgrind",
            "--engine",
            "criterion",
            "--target-triple",
            "all",
            "--machine-key",
            "ci-pool",
        ]);
        let Command::Analyze(options) = command else {
            panic!("expected analyze command");
        };
        assert_eq!(options.repo, Some(PathBuf::from("/work/folo")));
        assert_eq!(options.context.as_deref(), Some("feature"));
        assert_eq!(options.base.as_deref(), Some("master"));
        assert_eq!(options.since.as_deref(), Some("2024-06-01T00:00:00Z"));
        assert!(options.no_dirty);
        assert_eq!(
            options.engine,
            vec!["callgrind".to_owned(), "criterion".to_owned()]
        );
        assert_eq!(options.target_triple, vec!["all".to_owned()]);
        assert_eq!(options.machine_key, vec!["ci-pool".to_owned()]);
    }

    #[test]
    fn analyze_discriminants_default_to_empty() {
        let Command::Analyze(options) = parse(&["analyze"]) else {
            panic!("expected analyze command");
        };
        assert!(options.engine.is_empty());
        assert!(options.target_triple.is_empty());
        assert!(options.machine_key.is_empty());
        assert!(options.since.is_none());
    }

    fn assert_until_rejected(args: &[&str]) {
        // `--context`, not `--until`, selects the timeline's end.
        let error = from_args(&["cargo-bench-history"], args).unwrap_err();
        assert!(error.status.is_err());
    }

    #[test]
    fn analyze_rejects_until() {
        assert_until_rejected(&["analyze", "--until", "2024-06-01"]);
    }

    #[test]
    fn list_rejects_until() {
        assert_until_rejected(&["list", "runs", "--until", "2024-06-01"]);
    }

    #[test]
    fn examine_rejects_until() {
        assert_until_rejected(&[
            "examine",
            "--benchmark",
            "b",
            "--metric",
            "m",
            "--until",
            "2024-06-01",
        ]);
    }

    #[test]
    fn prune_rejects_until() {
        assert_until_rejected(&["prune", "--dirty", "--until", "2024-06-01"]);
    }

    #[test]
    fn analyze_output_defaults_to_text_only() {
        let Command::Analyze(options) = parse(&["analyze"]) else {
            panic!("expected analyze command");
        };
        assert!(!options.no_text);
        assert!(options.markdown.is_none());
        assert!(options.json.is_none());
    }

    #[test]
    fn analyze_collects_output_toggles() {
        let Command::Analyze(options) = parse(&[
            "analyze",
            "--no-text",
            "--markdown",
            "out/report.md",
            "--json",
            "out/report.json",
        ]) else {
            panic!("expected analyze command");
        };
        assert!(options.no_text);
        assert_eq!(options.markdown, Some(PathBuf::from("out/report.md")));
        assert_eq!(options.json, Some(PathBuf::from("out/report.json")));
    }

    #[test]
    fn list_requires_a_subject() {
        let parsed = from_args(&["cargo-bench-history"], &["list"]);
        let early = parsed.unwrap_err();
        assert!(early.status.is_err(), "a missing subject is a parse error");
        for subject in ["runs", "discriminants", "blessings"] {
            assert!(
                early.output.contains(subject),
                "the error names the {subject} subject: {}",
                early.output
            );
        }
    }

    #[test]
    fn list_runs_collects_selection() {
        let command = parse(&[
            "list",
            "runs",
            "--repo",
            "/work/folo",
            "--context",
            "feature",
            "--base",
            "master",
            "--no-dirty",
            "--verbose",
        ]);
        let Command::List(options) = command else {
            panic!("expected list command");
        };
        assert_eq!(options.subject, ListSubject::Runs);
        assert_eq!(options.repo, Some(PathBuf::from("/work/folo")));
        assert_eq!(options.context.as_deref(), Some("feature"));
        assert_eq!(options.base.as_deref(), Some("master"));
        assert!(options.no_dirty);
        assert!(options.verbose);
    }

    #[test]
    fn list_runs_collects_discriminants_and_output() {
        let command = parse(&[
            "list",
            "runs",
            "--engine",
            "callgrind",
            "--target-triple",
            "x86_64-unknown-linux-gnu",
            "--machine-key",
            "ci-pool",
            "--no-text",
            "--markdown",
            "list.md",
            "--json",
            "list.json",
        ]);
        let Command::List(options) = command else {
            panic!("expected list command");
        };
        assert_eq!(options.subject, ListSubject::Runs);
        assert_eq!(options.engine, vec!["callgrind".to_owned()]);
        assert_eq!(
            options.target_triple,
            vec!["x86_64-unknown-linux-gnu".to_owned()]
        );
        assert_eq!(options.machine_key, vec!["ci-pool".to_owned()]);
        assert!(options.no_text);
        assert_eq!(options.markdown, Some(PathBuf::from("list.md")));
        assert_eq!(options.json, Some(PathBuf::from("list.json")));
    }

    #[test]
    fn list_discriminants_selects_the_subject() {
        let Command::List(options) = parse(&["list", "discriminants"]) else {
            panic!("expected list command");
        };
        assert_eq!(options.subject, ListSubject::Discriminants);
    }

    #[test]
    fn list_blessings_collects_all_switch() {
        let Command::List(options) = parse(&["list", "blessings", "--all"]) else {
            panic!("expected list command");
        };
        assert_eq!(options.subject, ListSubject::Blessings);
        assert!(options.all);
    }

    #[test]
    fn list_blessings_all_defaults_to_false() {
        let Command::List(options) = parse(&["list", "blessings"]) else {
            panic!("expected list command");
        };
        assert!(!options.all);
    }

    #[test]
    fn examine_collects_selection_and_scope() {
        let command = parse(&[
            "examine",
            "--benchmark",
            "nm/nm::observe/pull",
            "--metric",
            "instruction_count",
            "--repo",
            "/work/folo",
            "--context",
            "feature",
            "--base",
            "master",
            "--no-dirty",
            "--since",
            "2024-01-01",
            "--verbose",
        ]);
        let Command::Examine(options) = command else {
            panic!("expected examine command");
        };
        assert_eq!(options.benchmark, "nm/nm::observe/pull");
        assert_eq!(options.metric, "instruction_count");
        assert_eq!(options.repo, Some(PathBuf::from("/work/folo")));
        assert_eq!(options.context.as_deref(), Some("feature"));
        assert_eq!(options.base.as_deref(), Some("master"));
        assert!(options.no_dirty);
        assert_eq!(options.since.as_deref(), Some("2024-01-01"));
        assert!(options.verbose);
    }

    #[test]
    fn examine_collects_discriminants_and_output() {
        let command = parse(&[
            "examine",
            "--benchmark",
            "b",
            "--metric",
            "m",
            "--engine",
            "callgrind",
            "--target-triple",
            "x86_64-unknown-linux-gnu",
            "--machine-key",
            "ci-pool",
            "--no-text",
            "--markdown",
            "examine.md",
            "--json",
            "examine.json",
        ]);
        let Command::Examine(options) = command else {
            panic!("expected examine command");
        };
        assert_eq!(options.engine, vec!["callgrind".to_owned()]);
        assert_eq!(
            options.target_triple,
            vec!["x86_64-unknown-linux-gnu".to_owned()]
        );
        assert_eq!(options.machine_key, vec!["ci-pool".to_owned()]);
        assert!(options.no_text);
        assert_eq!(options.markdown, Some(PathBuf::from("examine.md")));
        assert_eq!(options.json, Some(PathBuf::from("examine.json")));
    }

    #[test]
    fn examine_requires_scope() {
        // With neither required scope flag, clap reports both as missing.
        let early = from_args(&["cargo-bench-history"], &["examine"]).unwrap_err();
        assert!(
            early.status.is_err(),
            "missing required flags are a parse error"
        );
        assert!(early.output.contains("--benchmark"), "{}", early.output);
        assert!(early.output.contains("--metric"), "{}", early.output);
    }

    #[test]
    fn examine_requires_metric() {
        // Supplying only one still fails, naming the other.
        let missing_metric = from_args(
            &["cargo-bench-history"],
            &["examine", "--benchmark", "nm/nm::observe/pull"],
        )
        .unwrap_err();
        assert!(missing_metric.status.is_err());
        assert!(
            missing_metric.output.contains("--metric"),
            "{}",
            missing_metric.output
        );
    }

    #[test]
    fn bless_collects_prefixes_discriminants_and_context() {
        let command = parse(&[
            "bless",
            "--engine",
            "callgrind",
            "--context",
            "abc123",
            "all_the_time/read_cell",
            "overhead/groups_",
        ]);
        let Command::Bless(options) = command else {
            panic!("expected bless command");
        };
        assert_eq!(
            options.prefixes,
            vec![
                BenchmarkIdPrefix::new("all_the_time/read_cell").unwrap(),
                BenchmarkIdPrefix::new("overhead/groups_").unwrap()
            ]
        );
        assert_eq!(options.engine, vec!["callgrind".to_owned()]);
        assert_eq!(options.context.as_deref(), Some("abc123"));
        assert!(!options.all);
    }

    #[test]
    fn bless_all_switch_needs_no_prefixes() {
        let Command::Bless(options) = parse(&["bless", "--all"]) else {
            panic!("expected bless command");
        };
        assert!(options.all);
        assert!(options.prefixes.is_empty());
    }

    #[test]
    fn bless_all_conflicts_with_prefixes() {
        let error =
            from_args(&["cargo-bench-history"], &["bless", "--all", "foo/bar"]).unwrap_err();
        assert_eq!(error.status, Err(()));
        assert!(
            error.output.contains("cannot be used with"),
            "{}",
            error.output
        );
    }

    #[test]
    fn bless_rejects_an_empty_prefix() {
        let error = from_args(&["cargo-bench-history"], &["bless", ""]).unwrap_err();
        assert_eq!(error.status, Err(()));
        assert!(
            error.output.contains("benchmark-id prefix"),
            "{}",
            error.output
        );
    }

    #[test]
    fn unbless_parses_discriminants() {
        let command = parse(&[
            "unbless",
            "--context",
            "abc123",
            "--target-triple",
            "x86_64-unknown-linux-gnu",
            "--machine-key",
            "ci-pool",
        ]);
        let Command::Unbless(options) = command else {
            panic!("expected unbless command");
        };
        assert_eq!(options.context.as_deref(), Some("abc123"));
        assert_eq!(
            options.target_triple,
            vec!["x86_64-unknown-linux-gnu".to_owned()]
        );
        assert_eq!(options.machine_key, vec!["ci-pool".to_owned()]);
    }

    #[test]
    fn prune_collects_commits_selection_and_dry_run() {
        let command = parse(&[
            "prune",
            "abc123",
            "def456",
            "--repo",
            "/work/folo",
            "--context",
            "feature",
            "--base",
            "master",
            "--since",
            "2024-01-01T00:00:00Z",
            "--dirty",
            "--dry-run",
            "--verbose",
        ]);
        let Command::Prune(options) = command else {
            panic!("expected prune command");
        };
        assert_eq!(options.repo, Some(PathBuf::from("/work/folo")));
        assert_eq!(options.context.as_deref(), Some("feature"));
        assert_eq!(options.base.as_deref(), Some("master"));
        assert_eq!(
            options.commit,
            vec!["abc123".to_owned(), "def456".to_owned()]
        );
        assert_eq!(options.since.as_deref(), Some("2024-01-01T00:00:00Z"));
        assert!(options.dirty);
        assert!(!options.clean);
        assert!(options.dry_run);
        assert!(options.verbose);
    }

    #[test]
    fn prune_collects_discriminants_and_output() {
        let command = parse(&[
            "prune",
            "--dirty",
            "--engine",
            "callgrind",
            "--target-triple",
            "x86_64-unknown-linux-gnu",
            "--machine-key",
            "ci-pool",
            "--no-text",
            "--json",
            "prune.json",
        ]);
        let Command::Prune(options) = command else {
            panic!("expected prune command");
        };
        assert_eq!(options.engine, vec!["callgrind".to_owned()]);
        assert_eq!(
            options.target_triple,
            vec!["x86_64-unknown-linux-gnu".to_owned()]
        );
        assert_eq!(options.machine_key, vec!["ci-pool".to_owned()]);
        assert!(options.no_text);
        assert_eq!(options.json, Some(PathBuf::from("prune.json")));
    }

    #[test]
    fn prune_all_expands_to_clean_and_dirty() {
        let command = parse(&[
            "prune",
            "--target-triple",
            "x86_64-unknown-linux-gnu",
            "--all",
        ]);
        let Command::Prune(options) = command else {
            panic!("expected prune command");
        };
        assert_eq!(
            options.target_triple,
            vec!["x86_64-unknown-linux-gnu".to_owned()]
        );
        assert!(options.commit.is_empty());
        assert!(options.clean, "--all enables clean removal");
        assert!(options.dirty, "--all enables dirty removal");
        assert!(!options.dry_run);
    }

    #[test]
    fn prune_include_blessings_combines_with_a_run_scope() {
        // `--include-blessings` is additive: it must be usable alongside a run scope
        // to prune runs and their blessings in one pass.
        let Command::Prune(options) = parse(&["prune", "--all", "--include-blessings"]) else {
            panic!("expected prune command");
        };
        assert!(options.clean, "--all enables clean removal");
        assert!(options.dirty, "--all enables dirty removal");
        assert!(options.include_blessings, "--include-blessings is set");
    }

    #[test]
    fn prune_include_blessings_alone_is_accepted() {
        let Command::Prune(options) = parse(&["prune", "--include-blessings"]) else {
            panic!("expected prune command");
        };
        assert!(!options.clean);
        assert!(!options.dirty);
        assert!(options.include_blessings);
    }

    #[test]
    fn prune_requires_a_scope() {
        let error = from_args(&["cargo-bench-history"], &["prune"]).unwrap_err();
        assert!(error.status.is_err());
    }

    #[test]
    fn prune_rejects_combining_clean_and_dirty() {
        // `--clean`, `--dirty`, and `--all` remain mutually exclusive alternatives;
        // `--all` is the way to remove both run kinds.
        let error =
            from_args(&["cargo-bench-history"], &["prune", "--clean", "--dirty"]).unwrap_err();
        assert!(
            error.output.contains("cannot be used with")
                || error.output.contains("conflict")
                || error.output.contains("cannot be used"),
            "combining --clean and --dirty should be rejected: {}",
            error.output
        );
    }

    #[test]
    fn backfill_collects_range_and_passthrough() {
        let command = parse(&[
            "backfill",
            "abc123",
            "def456",
            "--package",
            "nm",
            "--bench",
            "nm_observe",
            "--overwrite",
            "--ignore-errors",
            "--",
            "--noplot",
        ]);
        let Command::Backfill(options) = command else {
            panic!("expected backfill command");
        };
        assert_eq!(options.from, "abc123");
        assert_eq!(options.to, "def456");
        assert_eq!(options.packages, vec!["nm".to_owned()]);
        assert_eq!(options.benches, vec!["nm_observe".to_owned()]);
        assert!(options.overwrite);
        assert!(options.ignore_errors);
        assert_eq!(options.passthrough, vec!["--noplot".to_owned()]);
    }

    #[test]
    fn backfill_help_documents_range_positionals() {
        #[cfg(not(miri))]
        let early_exit = from_args(&["cargo-bench-history"], &["backfill", "--help"]).unwrap_err();
        #[cfg(miri)]
        let early_exit = {
            // This check concerns the range positionals, not rendering every flag's description.
            // Keep the real schema, but hide unrelated flags in the interpreted help fixture.
            // Native runs render the complete production help.
            let error = BackfillCommand::augment_args(ClapCommand::new("backfill"))
                .mut_args(|arg| {
                    let positional = arg.is_positional();
                    arg.hide(!positional)
                })
                .try_get_matches_from(["backfill", "--help"])
                .unwrap_err();
            EarlyExit::from_clap(&error)
        };
        assert!(early_exit.status.is_ok());
        assert!(early_exit.output.contains("FROM"));
        assert!(early_exit.output.contains("TO"));
    }

    #[test]
    fn backfill_requires_from_and_to() {
        let parsed = from_args(&["cargo-bench-history"], &["backfill", "abc123"]);
        assert!(parsed.is_err(), "a missing `to` must be rejected");
    }

    #[test]
    fn backfill_parses_verbose_switch() {
        let Command::Backfill(options) = parse(&["backfill", "abc123", "def456", "--verbose"])
        else {
            panic!("expected backfill command");
        };
        assert!(options.verbose);
    }

    #[test]
    fn backfill_verbose_defaults_to_false() {
        let Command::Backfill(options) = parse(&["backfill", "abc123", "def456"]) else {
            panic!("expected backfill command");
        };
        assert!(!options.verbose);
    }

    #[test]
    fn backfill_best_of_defaults_to_one() {
        let Command::Backfill(options) = parse(&["backfill", "abc123", "def456"]) else {
            panic!("expected backfill command");
        };
        assert_eq!(
            options.best_of.get(),
            1,
            "--best-of defaults to a single run"
        );
    }

    #[test]
    fn backfill_best_of_parses_a_value() {
        let Command::Backfill(options) = parse(&["backfill", "abc123", "def456", "--best-of", "3"])
        else {
            panic!("expected backfill command");
        };
        assert_eq!(options.best_of.get(), 3);
    }

    #[test]
    fn backfill_best_of_rejects_zero() {
        let parsed = from_args(
            &["cargo-bench-history"],
            &["backfill", "abc123", "def456", "--best-of", "0"],
        );
        assert!(parsed.is_err(), "--best-of 0 must be rejected");
    }

    #[test]
    fn unknown_subcommand_is_rejected() {
        from_args(&["cargo-bench-history"], &["frobnicate"]).unwrap_err();
    }

    #[test]
    fn collect_rejects_unknown_flag() {
        from_args(&["cargo-bench-history"], &["collect", "--frobnicate"]).unwrap_err();
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "renders the full command catalog; option schemas run individually under Miri"
    )]
    fn help_request_lists_subcommands() {
        let early_exit = Cli::from_args(&["cargo-bench-history"], &["--help"]).unwrap_err();
        assert!(
            early_exit.output.contains("collect"),
            "help should list subcommands: {}",
            early_exit.output
        );
        assert!(
            early_exit.output.contains("install"),
            "{}",
            early_exit.output
        );
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "renders the full command catalog; option schemas run individually under Miri"
    )]
    fn help_text_describes_each_command_in_alphabetical_order() {
        let help = Cli::help("cargo-bench-history");

        // Each command is accompanied by its description, not just its bare name.
        assert!(
            help.contains("Analyze stored history"),
            "help should describe `analyze`: {help}"
        );
        assert!(
            help.contains("Replay `collect` across a range"),
            "help should describe `backfill`: {help}"
        );

        // The commands appear in alphabetical order. Each marker is a distinct,
        // non-overlapping substring, so the offsets are strictly increasing
        // exactly when they are sorted.
        let order = ["analyze", "backfill", "collect", "install", "list", "prune"];
        let positions: Vec<usize> = order
            .iter()
            .map(|name| help.find(&format!("\n  {name} ")).unwrap())
            .collect();
        assert!(
            positions.is_sorted(),
            "commands should be listed alphabetically: {help}"
        );
    }

    #[test]
    fn subcommand_help_is_a_successful_early_exit() {
        let early = from_args(&["cargo-bench-history"], &["install", "--help"]).unwrap_err();
        assert!(early.status.is_ok());
        assert!(early.output.contains("--config"));
    }
}
