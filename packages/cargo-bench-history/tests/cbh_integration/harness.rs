pub(crate) use std::cell::RefCell;
pub(crate) use std::collections::HashMap;
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
pub(crate) use std::path::{Path, PathBuf};
use std::process::{Child, ChildStderr, ChildStdin, ChildStdout, Stdio};
use std::sync::LazyLock;
use std::sync::atomic::{AtomicU64, Ordering};

pub(crate) use cargo_bench_history::{
    AnalysisOutcome, AutoDiscriminants, BenchmarkId, BenchmarkResult, Cli, Command,
    EnvironmentInfo, GitInfo, Metric, MetricKind, Overrides, Run, RunContext, RunOutcome,
    SCHEMA_VERSION, ToolchainInfo, default_template, run, run_with_overrides,
};
use cbh_codec as codec;
use cbh_model::{DiscriminantSet, Engine, MachineKey, TargetTriple};
pub(crate) use jiff::Timestamp;
use nonempty::nonempty;
pub(crate) use ohno::AppError;
pub(crate) use serial_test::serial;
pub(crate) use testing::CwdGuard;
use tick::Clock;

/// The tool version recorded with each run. The integration test compiles within
/// the package, so its `CARGO_PKG_VERSION` matches the version `collect` records.
pub(crate) const TOOL_VERSION: &str = env!("CARGO_PKG_VERSION");

/// The target triple the harness pins the auto-detected discriminant value to, matching the
/// triple the seed helpers write their partitions under. Pinning it keeps the
/// suite host-independent: sets obey the target-triple filter (a `list`/`analyze`
/// with an auto triple never sees foreign-triple data), so a bare query must
/// auto-detect the seeded triple regardless of the host the test runs on.
pub(crate) const HARNESS_AUTO_TRIPLE: &str = "x86_64-unknown-linux-gnu";

/// The machine key the harness pins the auto-detected discriminant value to. Every engine is
/// machine-keyed, so a bare query must auto-detect a machine key the seeded sets
/// share; the callgrind/alloc seed helpers plant their objects under this key so a
/// default query matches them regardless of the host the test runs on. Tests that
/// exercise per-machine isolation seed additional, explicit machine keys and
/// select them with an explicit `--machine-key`.
pub(crate) const HARNESS_AUTO_MACHINE_KEY: &str = "harness-auto-machine";

/// The minimum number of points a change-point regime (the flat side and the
/// stepped side of a split) must hold before the history detector trusts the
/// split. Mirrors `cbh_detect::MIN_REGIME`; the
/// [`fixture_gate_sizes_match_the_detector_defaults`] guard test keeps the two in
/// lockstep. Fixtures build a flat regime and a stepped regime of this size so a
/// seeded step clears the detector's per-regime persistence gate.
pub(crate) const MIN_REGIME: usize = 5;

/// The minimum number of points a series must carry to be analysed at all — below
/// it the series is not judged and is not even counted in the false-discovery
/// family. Equal to two full regimes and mirrors
/// `cbh_detect::MIN_SERIES_POINTS` (also the value of
/// `DRIFT_MIN_POINTS` and the base-side commit-level floor branch mode demands).
/// Fixtures seed at least this many points so their series is judged rather than
/// silently skipped.
pub(crate) const MIN_SERIES_POINTS: usize = 2 * MIN_REGIME;

/// The committer date of the first commit the rising Callgrind fixture
/// ([`Workspace::seed_rising_callgrind_history`]) stamps; each later commit is one
/// calendar day on. Named so a test that must know where the fixture's timeline
/// starts or ends derives it (via [`sequential_dates`]) instead of restating a
/// date that moves whenever the fixture's length does.
pub(crate) const RISING_HISTORY_FIRST_DATE: &str = "2024-01-01";

/// Canonical faker `--callgrind` identity/metric fragments, kept in lockstep with
/// what the `collect` identity assertions expect. A fragment is everything after
/// the on-disk `GROUP` — `MODULE|FUNCTION[|ID[|PACKAGE_DIR]]=IR/BC/BI`; the
/// [`callgrind_arg`] helper prepends the group.
///
/// `CALLGRIND_SINGLE` is an unparametrized bench in package `fast_time`
/// (`Ir`/`Bc`/`Bi` = 36/4/2); `CALLGRIND_PARAMETRIZED` adds the `two_instants` id
/// (87/2/1); `CALLGRIND_SINGLE_ALT_PKG` keeps `CALLGRIND_SINGLE`'s identity but
/// reports a different `package_dir` (package `other_pkg`), to exercise
/// cross-package bench-name collisions.
pub(crate) const CALLGRIND_SINGLE: &str = "fast_time_timestamp_performance_cg::timestamp_capture::timestamp_capture_std_now|timestamp_capture_std_now||/mnt/c/Source/folo/packages/fast_time=36/4/2";
pub(crate) const CALLGRIND_PARAMETRIZED: &str = "fast_time_timestamp_performance_cg::timestamp_capture::timestamp_capture_instant_saturating_duration_since|timestamp_capture_instant_saturating_duration_since|two_instants|/mnt/c/Source/folo/packages/fast_time=87/2/1";
pub(crate) const CALLGRIND_SINGLE_ALT_PKG: &str = "fast_time_timestamp_performance_cg::timestamp_capture::timestamp_capture_std_now|timestamp_capture_std_now||/work/packages/other_pkg=36/4/2";

/// Builds a faker `--callgrind` argument value for on-disk `group` from one of the
/// `CALLGRIND_*` identity/metric fragments (prepends `GROUP|`).
pub(crate) fn callgrind_arg(group: &str, fragment: &str) -> String {
    format!("{group}|{fragment}")
}

/// A process-global base repository template: a `master`-branch repo carrying the
/// volatile-directory excludes and one empty `root` commit, built once via the real
/// git commands and copied into each fresh workspace by [`Workspace::init_repo`].
/// Copying the tiny pruned `.git` tree is far cheaper than re-spawning `git init` +
/// `git commit` per repository, where process startup — compounded by real-time
/// antivirus on Windows — dominates the integration suite. Built lazily on first use
/// and kept alive for the whole test binary (its `TempDir` is never dropped, so the
/// source `.git` outlives every copy).
static BASE_TEMPLATE: LazyLock<tempfile::TempDir> = LazyLock::new(build_base_template);

/// A process-global counter handing each `drive_json`/`drive_markdown` call a
/// unique report filename, so concurrent tests writing into their own workspaces
/// never collide on a shared name.
static OUTPUT_SEQ: AtomicU64 = AtomicU64::new(0);

/// Runs `git -C <root> <args>`, returning its captured output and asserting success.
///
/// Every invocation injects the committer identity plus throughput-oriented config:
/// `core.fsync=none`/`core.fsyncObjectFiles=false` skip the per-object disk flush (by
/// far the largest per-commit cost on Windows, where real-time antivirus compounds it)
/// and `gc.auto=0` keeps a background repack from firing mid-test. The identity is
/// supplied here rather than via separate `git config` subprocesses so a fresh
/// repository needs only `init`/`commit`, not extra process spawns. These settings are
/// safe for throwaway test repositories and unknown keys are ignored by older git, so
/// they are inert where unsupported. Shared by [`Workspace::git`] and the one-time
/// [`build_base_template`] so both speak to git identically.
fn run_git(root: &Path, args: &[&str]) -> std::process::Output {
    run_git_with_env(root, args, &[])
}

/// Like [`run_git`], but additionally sets the given environment variables on the
/// `git` invocation — used to pin `GIT_AUTHOR_DATE`/`GIT_COMMITTER_DATE` so a seed
/// commit's committer date lands where a window test expects it on the timeline.
fn run_git_with_env(root: &Path, args: &[&str], env: &[(&str, &str)]) -> std::process::Output {
    let root = root.to_string_lossy().into_owned();
    let mut full: Vec<&str> = vec![
        "-c",
        "user.email=test@example.invalid",
        "-c",
        "user.name=Bench History Test",
        "-c",
        "commit.gpgsign=false",
        "-c",
        "core.fsync=none",
        "-c",
        "core.fsyncObjectFiles=false",
        "-c",
        "gc.auto=0",
        "-C",
        root.as_str(),
    ];
    full.extend_from_slice(args);
    let mut command = std::process::Command::new("git");
    command.args(&full);
    for (key, value) in env {
        command.env(key, value);
    }
    let output = command.output().unwrap();
    assert!(
        output.status.success(),
        "git {:?} failed: {}",
        args,
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

/// Builds the shared base template (see [`BASE_TEMPLATE`]) in its own temporary
/// directory using the same real git helpers a live repo uses, then prunes the
/// per-init noise git writes that only inflates each copy: the `*.sample` hook scripts
/// and the placeholder `description`.
fn build_base_template() -> tempfile::TempDir {
    let template = tempfile::tempdir().unwrap();
    let root = template.path();
    run_git(root, &["init", "-b", "master"]);
    fs::write(
        root.join(".git").join("info").join("exclude"),
        "/.cargo/\n/store/\n/target/\n",
    )
    .unwrap();
    run_git(root, &["commit", "--allow-empty", "-m", "root"]);
    let git_dir = root.join(".git");
    if let Ok(entries) = fs::read_dir(git_dir.join("hooks")) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == "sample") {
                remove_best_effort(&path);
            }
        }
    }
    remove_best_effort(&git_dir.join("description"));
    template
}

/// Removes `path`, ignoring failure. Pruning the template's per-init noise is purely
/// an optimization: a file that cannot be removed (already absent on some git version,
/// say) only leaves the copy marginally larger and is never an error.
fn remove_best_effort(path: &Path) {
    match fs::remove_file(path) {
        Ok(()) | Err(_) => {}
    }
}

/// Recursively copies the directory tree at `src` into `dst`, creating `dst` and any
/// missing parents. Used to clone the base template's `.git` into a fresh workspace.
fn copy_dir_all(src: &Path, dst: &Path) {
    fs::create_dir_all(dst).unwrap();
    for entry in fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let from = entry.path();
        let to = dst.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir_all(&from, &to);
        } else {
            fs::copy(&from, &to).unwrap();
        }
    }
}

/// The first synthetic committer second handed to an undated commit. Undated
/// commits exist to express topology (branch tips, merge parents) rather than a
/// timeline, so any distinct, ordered dates suffice — but they must land inside
/// `analyze`/`list`'s default six-month look-back window (see [`analysis_now`],
/// anchored at 2024-06-01 with a 2023-12-01 cutoff), or the default window would
/// silently drop them. Anchoring at 2024-01-01 and advancing one day per commit
/// keeps an undated history comfortably inside that window with ample headroom.
///
/// [`analysis_now`]: super::harness::analysis_now
const UNDATED_EPOCH_SECONDS: i64 = 1_704_067_200;

/// One day in seconds, the spacing between successive undated commits.
const SECONDS_PER_DAY: i64 = 86_400;

/// Builds a workspace's commit history through a single long-lived
/// `git fast-import` process instead of one `git commit` subprocess per commit.
///
/// Process startup — amplified by real-time antivirus on Windows — dominates the
/// integration suite, and a seed-heavy history pays it once per commit. Streaming
/// the commits into one `fast-import` collapses an N-commit history into a single
/// spawn: each commit is one buffered write plus a `get-mark` round-trip that
/// reports the new commit ID, so [`Workspace::commit`] and friends stay synchronous and
/// their call sites read exactly as they would against real `git commit`.
///
/// All commits the harness creates this way carry the base template's empty tree,
/// so `fast-import` advancing a branch ref without touching the index or working
/// tree leaves `git status` clean — the property that makes `analyze` pick
/// `history` mode. Operations that genuinely change the tree or the checkout
/// (merge, branch switch, file commits) stay on real `git`; the workspace
/// [`finalize`](Self::finalize)s the open stream first so those refs and objects
/// are on disk before the next real `git` reads them.
struct GitGraph {
    /// The repository root, addressed explicitly so streaming never depends on
    /// the process current directory.
    root: PathBuf,
    /// The branch new commits land on, mirroring the working tree's checked-out
    /// branch. The workspace's `checkout` helpers keep it in step, so each stream
    /// roots its first commit at this branch's current tip.
    current_branch: String,
    /// The open `fast-import` stream, spawned lazily on the first commit and torn
    /// down by [`finalize`](Self::finalize) before any real `git` command.
    session: Option<Session>,
    /// The next `fast-import` mark, used transiently to read back each commit's
    /// commit ID via `get-mark`.
    next_mark: u32,
    /// The committer second handed to the next undated commit (see
    /// [`UNDATED_EPOCH_SECONDS`]).
    next_undated_second: i64,
}

/// An open `git fast-import` stream and the pipes used to drive it.
struct Session {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    stderr: ChildStderr,
    /// Whether a commit has already been emitted on this stream. The first commit
    /// roots the branch with an explicit `from`; later commits append to the tip
    /// `fast-import` is already tracking.
    has_commit: bool,
}

impl GitGraph {
    fn new(root: PathBuf, current_branch: &str) -> Self {
        Self {
            root,
            current_branch: current_branch.to_owned(),
            session: None,
            next_mark: 0,
            next_undated_second: UNDATED_EPOCH_SECONDS,
        }
    }

    /// Records the branch subsequent commits land on, after a real `git checkout`
    /// has moved the working tree (and created the ref, for a new branch).
    fn set_current_branch(&mut self, name: &str) {
        name.clone_into(&mut self.current_branch);
    }

    /// Streams an empty commit dated `second` (raw committer seconds, UTC) with
    /// message `message` onto the current branch, returning its full commit ID.
    fn commit(&mut self, message: &str, second: i64) -> String {
        let mark = self
            .next_mark
            .checked_add(1)
            .expect("mark counter overflow");
        self.next_mark = mark;
        let branch = self.current_branch.clone();
        let session = self.ensure_session();

        let mut stream: Vec<u8> = Vec::new();
        // `writeln!` emits a bare LF (never CRLF, on any platform), which the stream
        // requires: `fast-import`'s `data` directive is byte-counted, so a stray CR
        // would corrupt the following message.
        writeln!(stream, "commit refs/heads/{branch}").unwrap();
        writeln!(stream, "mark :{mark}").unwrap();
        writeln!(
            stream,
            "committer Bench History Test <test@example.invalid> {second} +0000"
        )
        .unwrap();
        writeln!(stream, "data {}", message.len()).unwrap();
        stream.extend_from_slice(message.as_bytes());
        stream.push(b'\n');
        if !session.has_commit {
            // Root this stream at the branch's current on-disk tip. The `^0`
            // peels the ref to its commit: `from refs/heads/<branch>` (without it)
            // is rejected as "creating a branch from itself" when the commit
            // targets that same branch. Later commits append to the tip
            // `fast-import` tracks, so `from` is implicit for them.
            writeln!(stream, "from refs/heads/{branch}^0").unwrap();
            session.has_commit = true;
        }
        stream.push(b'\n');
        writeln!(stream, "get-mark :{mark}").unwrap();

        session.stdin.write_all(&stream).unwrap();
        session.stdin.flush().unwrap();

        let mut line = String::new();
        session.stdout.read_line(&mut line).unwrap();
        let commit_id = line.trim().to_owned();
        assert_eq!(
            commit_id.len(),
            40,
            "git fast-import get-mark returned `{commit_id}`, not a full commit ID"
        );
        commit_id
    }

    /// Allocates the committer second for the next undated commit, advancing the
    /// synthetic clock one day so a date-free topology still has distinct, ordered
    /// timestamps.
    fn next_undated_second(&mut self) -> i64 {
        let second = self.next_undated_second;
        self.next_undated_second = second
            .checked_add(SECONDS_PER_DAY)
            .expect("undated commit clock overflow");
        second
    }

    /// Returns the open stream, spawning a fresh `fast-import` if none is active.
    fn ensure_session(&mut self) -> &mut Session {
        if self.session.is_none() {
            let mut child = std::process::Command::new("git")
                .arg("-C")
                .arg(&self.root)
                // Match `run_git`'s throughput config: skip the per-object disk
                // flush and keep a background repack from firing mid-stream.
                .args([
                    "-c",
                    "core.fsync=none",
                    "-c",
                    "core.fsyncObjectFiles=false",
                    "-c",
                    "gc.auto=0",
                    "fast-import",
                    "--quiet",
                    "--date-format=raw",
                ])
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap();
            let stdin = child.stdin.take().unwrap();
            let stdout = BufReader::new(child.stdout.take().unwrap());
            let stderr = child.stderr.take().unwrap();
            self.session = Some(Session {
                child,
                stdin,
                stdout,
                stderr,
                has_commit: false,
            });
        }
        self.session.as_mut().unwrap()
    }

    /// Closes any open stream so `fast-import` updates the branch refs and writes
    /// its objects, making them visible to the real `git` commands that follow.
    fn finalize(&mut self) {
        let Some(session) = self.session.take() else {
            return;
        };
        let Session {
            mut child,
            stdin,
            stdout,
            mut stderr,
            ..
        } = session;
        // Closing stdin signals end-of-stream; `fast-import` then finalizes the
        // pack, updates the refs, and exits. Dropping stdout lets it observe the
        // read end going away too.
        drop(stdin);
        drop(stdout);
        let mut errors = String::new();
        // Drain any diagnostics fast-import wrote; they are surfaced only when the
        // exit status below is non-zero. A read failure just leaves a marker.
        if let Err(read_error) = stderr.read_to_string(&mut errors) {
            errors = format!("<failed to read fast-import stderr: {read_error}>");
        }
        let status = child.wait();
        // Always wait above so the child releases its `.git` handles before the
        // workspace tempdir is removed; only assert success when we are not already
        // unwinding, so a failing test is not masked by a double panic.
        if !std::thread::panicking() {
            let status = status.expect("waiting on git fast-import");
            assert!(
                status.success(),
                "git fast-import exited with {status}: {errors}"
            );
        }
    }
}

/// A hermetic workspace for driving `collect` against the real adapters.
///
/// Writes a configuration and (optionally) fake engine output under a temporary
/// directory, then drives a command with that directory as the process current
/// directory, restoring the original directory before returning so assertions and
/// temp-dir cleanup happen outside it (required on Windows).
pub(crate) struct Workspace {
    dir: tempfile::TempDir,
    /// Maps a short commit label (for example `c1`) to the full commit ID of the empty
    /// commit created for it, so seeded storage keys and the live git topology
    /// agree on the commit-directory segment. Interior mutability lets the
    /// `&self` commit helpers populate it lazily as a test builds its topology.
    commits: RefCell<HashMap<String, String>>,
    /// Read-through cache of each commit ID's git committer timestamp, used to
    /// stamp a clean seed's provenance observation from the commit it sits on.
    /// [`commit_dated`](Self::commit_dated) populates it at creation (the common
    /// case, so no extra `git` spawn); undated commits are read from git on first
    /// use. Git remains the source of truth — this only avoids redundant spawns.
    committer_times: RefCell<HashMap<String, Timestamp>>,
    /// Arguments passed to the mock benchmark engine that `collect`/`backfill` invoke
    /// in place of `cargo bench`. They tell the mock which fixtures to emit (which
    /// `--callgrind` / `--criterion` cases, or an `--exit-code`), so each engine's
    /// output tree is produced by the single benchmark command `collect` invokes.
    bench: Vec<String>,
    /// Streams commit creation through one long-lived `git fast-import` rather than
    /// a `git commit` subprocess per commit (see [`GitGraph`]). The `&self` commit
    /// helpers drive it through interior mutability; real `git` operations
    /// [`finalize`](Self::flush_git) it first so its refs and objects are on disk.
    graph: RefCell<GitGraph>,
    /// Whether [`drive`](Self::drive) injects `--local=<root>/store` for
    /// storage-backed commands. Local-storage paths are a run-time `--local`
    /// selection rather than configuration, so the standard constructors enable
    /// this; [`without_local_storage`](Self::without_local_storage) disables it for
    /// tests that exercise the no-storage-configured error path, and azure tests use
    /// their own workspace type entirely.
    inject_local: bool,
    /// Whether [`drive`](Self::drive) uses real host auto-detection for the query
    /// discriminant values instead of the pinned [`HARNESS_AUTO_TRIPLE`] /
    /// [`HARNESS_AUTO_MACHINE_KEY`].
    /// The standard constructors pin the discriminant values so seeded-partition tests are
    /// host-independent; [`with_real_auto_discriminants`](Self::with_real_auto_discriminants)
    /// enables real detection for the `collect`→`analyze` round-trip tests, whose
    /// whole point is that a real `collect` (which stamps the host triple) and a
    /// bare `analyze` resolve the *same* host partition.
    real_auto_discriminants: bool,
}

impl Drop for Workspace {
    fn drop(&mut self) {
        // Close any open `fast-import` stream so the child releases its handles on
        // the `.git` directory before `TempDir` cleanup removes the repository.
        // `finalize` only asserts success when the thread is not already panicking,
        // so a failing test is reported as itself rather than a double panic here.
        self.flush_git();
    }
}

impl Workspace {
    /// Creates a workspace with `config` at the default `.cargo/bench_history.toml`.
    pub(crate) fn new(config: &str) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_path_buf();
        let workspace = Self {
            dir,
            commits: RefCell::new(HashMap::new()),
            committer_times: RefCell::new(HashMap::new()),
            bench: Vec::new(),
            graph: RefCell::new(GitGraph::new(root, "master")),
            inject_local: true,
            real_auto_discriminants: false,
        };
        let cargo_dir = workspace.root().join(".cargo");
        fs::create_dir_all(&cargo_dir).unwrap();
        fs::write(cargo_dir.join("bench_history.toml"), config).unwrap();
        workspace
    }

    /// Creates a workspace with `config` plus an initialized git repository (on a
    /// `master` branch with one empty root commit), as `analyze` requires a
    /// repository to resolve a series' timeline from git topology.
    pub(crate) fn repo(config: &str) -> Self {
        let workspace = Self::new(config);
        workspace.init_repo();
        workspace
    }

    /// Alias for [`repo`](Self::repo): the standard repository already keeps its
    /// working tree clean across a `collect` (the volatile `.cargo`/`store`/`target`
    /// directories are git-ignored). Retained for call sites that want to spell out
    /// that a clean tree — and therefore `history`-mode analysis — is intended.
    pub(crate) fn clean_repo(config: &str) -> Self {
        Self::repo(config)
    }

    /// Creates an empty workspace with no configuration file written.
    pub(crate) fn empty() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_path_buf();
        Self {
            dir,
            commits: RefCell::new(HashMap::new()),
            committer_times: RefCell::new(HashMap::new()),
            bench: Vec::new(),
            graph: RefCell::new(GitGraph::new(root, "master")),
            inject_local: false,
            real_auto_discriminants: false,
        }
    }

    /// Creates a workspace with `config` at a non-default `relative` path.
    pub(crate) fn with_config_at(relative: &str, config: &str) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_path_buf();
        let workspace = Self {
            dir,
            commits: RefCell::new(HashMap::new()),
            committer_times: RefCell::new(HashMap::new()),
            bench: Vec::new(),
            graph: RefCell::new(GitGraph::new(root, "master")),
            inject_local: true,
            real_auto_discriminants: false,
        };
        let path = workspace.root().join(relative);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, config).unwrap();
        workspace
    }

    /// Sets the arguments the mock benchmark engine receives, describing the
    /// fixtures it should emit (consuming builder used at construction). A run
    /// invokes the mock once with these arguments and harvests every engine's
    /// output tree it wrote.
    pub(crate) fn with_bench(mut self, args: &[&str]) -> Self {
        self.bench = args.iter().map(|arg| (*arg).to_owned()).collect();
        self
    }

    /// Disables the automatic `--local=<root>/store` injection (consuming builder),
    /// so a driven command resolves storage exactly as written. Used by tests that
    /// exercise the no-storage-configured error path.
    pub(crate) fn without_local_storage(mut self) -> Self {
        self.inject_local = false;
        self
    }

    /// Uses real host auto-detection for the discriminant filters instead of the pinned
    /// harness defaults (consuming builder). Reserved for `collect`→`analyze`
    /// round-trip tests: a real `collect` stamps the host triple, so a bare
    /// `analyze` must auto-detect that same host triple to find the run. Every
    /// other test seeds a fixed-triple partition and relies on the pin.
    pub(crate) fn with_real_auto_discriminants(mut self) -> Self {
        self.real_auto_discriminants = true;
        self
    }

    /// The auto-detected discriminant override a drive injects through `Overrides`.
    ///
    /// Pinned to [`HARNESS_AUTO_TRIPLE`]/[`HARNESS_AUTO_MACHINE_KEY`] by default so
    /// the suite is host-independent; `None` (real detection) when
    /// [`with_real_auto_discriminants`](Self::with_real_auto_discriminants) opted in.
    fn auto_discriminants_override(&self) -> Option<AutoDiscriminants> {
        if self.real_auto_discriminants {
            None
        } else {
            Some(AutoDiscriminants {
                triple: HARNESS_AUTO_TRIPLE.to_owned(),
                machine_key: HARNESS_AUTO_MACHINE_KEY.to_owned(),
            })
        }
    }

    /// Builds the effective CLI arguments for a drive, injecting `--verbose` for
    /// every command and `--local=<root>/store` for storage-backed commands.
    ///
    /// Local-storage paths are a run-time selection rather than configuration, so
    /// each test stores to its own workspace `store/` directory via `--local`. The
    /// `install` command takes no storage, and an explicit `--local` already in
    /// `args` is left untouched, as is every command when injection is disabled.
    /// `--verbose` is injected unconditionally (every subcommand accepts it) so the
    /// reporter is enabled and its `note_with` closures run under test.
    fn effective_args(&self, args: &[&str]) -> Vec<String> {
        let mut effective: Vec<String> = args.iter().map(|arg| (*arg).to_owned()).collect();

        // Enable verbose reporting for every driven command so the reporter's
        // `note_with`/`if_enabled` closures actually run under test (they are
        // skipped when the reporter is disabled), keeping their bodies covered.
        // Notes go to stderr, so this never perturbs a command's stdout or the
        // rendered reports the assertions read back. Every subcommand accepts
        // `--verbose`; a caller that already passed it is left untouched.
        let has_subcommand = !effective.is_empty();
        let already_verbose = effective.iter().any(|arg| arg == "--verbose");
        if has_subcommand && !already_verbose {
            effective.insert(1, "--verbose".to_owned());
        }

        let already_local = effective
            .iter()
            .any(|arg| arg == "--local" || arg.starts_with("--local="));
        let injects = self.inject_local
            && !already_local
            && effective
                .first()
                .is_some_and(|subcommand| subcommand != "install");
        if injects {
            let store = self.root().join("store");
            effective.insert(1, format!("--local={}", store.display()));
        }
        effective
    }

    pub(crate) fn root(&self) -> &Path {
        self.dir.path()
    }

    /// Closes any open [`GitGraph`] stream so its commits' refs and objects are on
    /// disk before a real `git` command (or a direct `.git` read) observes them.
    fn flush_git(&self) {
        self.graph.borrow_mut().finalize();
    }

    /// Runs `git -C <root> <args>` against this workspace, returning its captured
    /// output. Thin wrapper over [`run_git`]; the repository directory is addressed
    /// explicitly so commit creation never depends on the process current directory.
    pub(crate) fn git(&self, args: &[&str]) -> std::process::Output {
        self.flush_git();
        run_git(self.root(), args)
    }

    /// Runs a real `git` command that creates a commit, stamping its author and
    /// committer dates with the synthetic committer `second` (raw UTC seconds) so
    /// merges and file commits stay on the same monotonic, in-window timeline as the
    /// empty `fast-import` commits, rather than taking the current wall-clock time.
    fn git_at(&self, args: &[&str], second: i64) -> std::process::Output {
        self.flush_git();
        let when = format!("@{second} +0000");
        run_git_with_env(
            self.root(),
            args,
            &[
                ("GIT_AUTHOR_DATE", when.as_str()),
                ("GIT_COMMITTER_DATE", when.as_str()),
            ],
        )
    }

    /// Initializes a `master`-branch repository with one empty root commit so `HEAD`
    /// always resolves. Rather than re-spawning `git init` + `git commit` for every
    /// repository (process startup, amplified by real-time antivirus, dominates these
    /// tests on Windows), it copies the pre-built [`BASE_TEMPLATE`] `.git` tree, which
    /// already carries the volatile-directory excludes (`.cargo`, `store`, `target`)
    /// and the empty root commit. The excludes are untracked, so the working tree stays
    /// clean unless a test deliberately dirties it (via [`make_dirty`]); a clean tree on
    /// the base branch is what makes `analyze` pick `history` mode.
    ///
    /// [`make_dirty`]: Self::make_dirty
    pub(crate) fn init_repo(&self) {
        copy_dir_all(
            &BASE_TEMPLATE.path().join(".git"),
            &self.root().join(".git"),
        );
        // Guard against a malformed template (for example a loose ref the copy
        // dropped): resolving HEAD straight from the copied `.git` must yield a full
        // commit ID, so a broken copy fails loudly here instead of misbehaving in a later
        // command. The read is cheap and never spawns git for a healthy template.
        assert_eq!(
            self.head_commit_id().len(),
            40,
            "copied base template did not yield a resolvable HEAD"
        );
    }

    /// Reads `HEAD`'s commit ID straight from the `.git` directory, avoiding a
    /// `git rev-parse` subprocess (process startup dominates these tests on
    /// Windows). For the small, freshly-created repositories the harness builds the
    /// branch ref is always loose; a `git rev-parse` fallback covers the unexpected
    /// (packed refs, detached `HEAD`).
    pub(crate) fn head_commit_id(&self) -> String {
        self.flush_git();
        let git_dir = self.root().join(".git");
        if let Ok(head) = fs::read_to_string(git_dir.join("HEAD")) {
            let head = head.trim();
            if let Some(reference) = head.strip_prefix("ref: ") {
                // `refs/heads/<branch>` uses `/`, which the Windows path APIs accept.
                if let Ok(commit_id) = fs::read_to_string(git_dir.join(reference)) {
                    let commit_id = commit_id.trim();
                    if commit_id.len() == 40 {
                        return commit_id.to_owned();
                    }
                }
            } else if head.len() == 40 {
                return head.to_owned();
            }
        }
        let head = self.git(&["rev-parse", "HEAD"]);
        String::from_utf8(head.stdout).unwrap().trim().to_owned()
    }

    /// Lazily creates an empty commit labeled `label` on the current branch and
    /// returns its full commit ID, reusing the commit ID if the label was already created.
    ///
    /// Creation order is the first-parent topology order, so seeding in oldest-first
    /// order makes the storage keys line up with the live git history `analyze`
    /// reconstructs the series from.
    pub(crate) fn commit(&self, label: &str) -> String {
        if let Some(commit_id) = self.commits.borrow().get(label) {
            return commit_id.clone();
        }
        let second = self.graph.borrow_mut().next_undated_second();
        let commit_id = self.graph.borrow_mut().commit(label, second);
        // Cache the committer time from the synthetic second we just stamped, so an
        // interleaved seed reading this commit's provenance never has to spawn git
        // (which would finalize the stream mid-history). Git stays the source of
        // truth: this matches exactly what `fast-import` wrote.
        let observed = Timestamp::from_second(second).unwrap();
        self.committer_times
            .borrow_mut()
            .insert(commit_id.clone(), observed);
        self.commits
            .borrow_mut()
            .insert(label.to_owned(), commit_id.clone());
        commit_id
    }

    /// Lazily creates an empty commit labeled `label`, dated `date` (`YYYY-MM-DD`,
    /// at UTC midnight), on the current branch and returns its full commit ID, reusing the
    /// commit ID (without redating) if the label already exists.
    ///
    /// Pinning the author and committer date lets the cutoff tests exercise
    /// `--since`: `analyze`/`list` read each commit's committer timestamp
    /// from git topology to decide the cutoff before any object is fetched, so the
    /// date stamped here governs how a seeded object behaves under the cutoff.
    pub(crate) fn commit_dated(&self, date: &str, label: &str) -> String {
        if let Some(commit_id) = self.commits.borrow().get(label) {
            return commit_id.clone();
        }
        let when = format!("{date}T00:00:00+00:00");
        let observed: Timestamp = when.parse().unwrap();
        let commit_id = self.graph.borrow_mut().commit(label, observed.as_second());
        self.committer_times
            .borrow_mut()
            .insert(commit_id.clone(), observed);
        self.commits
            .borrow_mut()
            .insert(label.to_owned(), commit_id.clone());
        commit_id
    }

    /// Resolves a previously created commit `label` to its full commit ID, panicking if
    /// the label was never created. Seeds attach objects to commits the test owns,
    /// so a missing label means the test forgot to create its topology first (via
    /// [`commit`](Self::commit), [`commit_dated`](Self::commit_dated), or
    /// [`merge`](Self::merge)).
    fn commit_id(&self, label: &str) -> String {
        self.commits
            .borrow()
            .get(label)
            .cloned()
            .unwrap_or_else(|| {
                panic!(
                    "commit label `{label}` was not created; create it with \
                 commit()/commit_dated()/merge() before seeding"
                )
            })
    }

    /// Returns `commit_id`'s git committer timestamp, used to stamp a clean seed's
    /// provenance observation from the commit it sits on. Dated commits are served
    /// from the cache populated at creation; any other commit is read from git once
    /// (`git show -s --format=%cI`) and cached. Git is the source of truth here.
    fn committer_time(&self, commit_id: &str) -> Timestamp {
        if let Some(observed) = self.committer_times.borrow().get(commit_id) {
            return *observed;
        }
        let output = self.git(&["show", "-s", "--format=%cI", commit_id]);
        let text = String::from_utf8(output.stdout).unwrap();
        let observed: Timestamp = text.trim().parse().unwrap();
        self.committer_times
            .borrow_mut()
            .insert(commit_id.to_owned(), observed);
        observed
    }
    pub(crate) fn checkout_new_branch(&self, name: &str) {
        self.git(&["checkout", "-b", name]);
        self.graph.borrow_mut().set_current_branch(name);
    }

    /// Checks out an existing branch.
    pub(crate) fn checkout(&self, name: &str) {
        self.git(&["checkout", name]);
        self.graph.borrow_mut().set_current_branch(name);
    }

    /// Merges `branch` into the current branch with an always-materialized merge
    /// commit (`--no-ff`), labels the resulting merge commit `label`, and returns
    /// its full commit ID. The merge commit has the current branch's tip as its first
    /// parent and `branch`'s tip as its second, so it sits on the current branch's
    /// first-parent line while the merged-in commits stay off it — the topology the
    /// git-aware `analyze`/`backfill` selection must respect.
    ///
    /// The merge commit takes the next synthetic committer date so it stays on the
    /// same monotonic, in-window timeline as the empty `fast-import` commits around
    /// it; without that, a real `git merge` would stamp it with the current
    /// wall-clock time, a future-dated point the default analysis window would treat
    /// inconsistently with its neighbours.
    pub(crate) fn merge(&self, branch: &str, label: &str) -> String {
        let second = self.graph.borrow_mut().next_undated_second();
        self.git_at(
            &["merge", "--no-ff", "--no-edit", "-m", label, branch],
            second,
        );
        let commit_id = self.head();
        self.committer_times
            .borrow_mut()
            .insert(commit_id.clone(), Timestamp::from_second(second).unwrap());
        self.commits
            .borrow_mut()
            .insert(label.to_owned(), commit_id.clone());
        commit_id
    }

    /// Commits a tracked file with `contents` at `relative` on the current branch
    /// and returns the new commit's full ID. Unlike [`commit`](Self::commit), the
    /// commit changes the tree, so the file appears in any worktree checked out to
    /// it — used to stand in for a "broken" commit the faker reacts to. It
    /// takes the next synthetic committer date, keeping the timeline monotonic and
    /// in-window like the surrounding empty commits.
    pub(crate) fn commit_with_file(&self, message: &str, relative: &str, contents: &str) -> String {
        fs::write(self.root().join(relative), contents).unwrap();
        self.git(&["add", relative]);
        let second = self.graph.borrow_mut().next_undated_second();
        self.git_at(&["commit", "-m", message], second);
        let commit_id = self.head();
        self.committer_times
            .borrow_mut()
            .insert(commit_id.clone(), Timestamp::from_second(second).unwrap());
        commit_id
    }

    /// Removes a previously tracked file at `relative`, commits the removal on the
    /// current branch, and returns the new commit's full ID. As with
    /// [`commit_with_file`](Self::commit_with_file), the removal commit takes the
    /// next synthetic committer date.
    pub(crate) fn commit_removing_file(&self, message: &str, relative: &str) -> String {
        self.git(&["rm", relative]);
        let second = self.graph.borrow_mut().next_undated_second();
        self.git_at(&["commit", "-m", message], second);
        let commit_id = self.head();
        self.committer_times
            .borrow_mut()
            .insert(commit_id.clone(), Timestamp::from_second(second).unwrap());
        commit_id
    }

    /// The full commit ID of the current `HEAD`.
    pub(crate) fn head(&self) -> String {
        let output = self.git(&["rev-parse", "HEAD"]);
        String::from_utf8(output.stdout).unwrap().trim().to_owned()
    }

    /// The name of the currently checked-out branch.
    pub(crate) fn current_branch(&self) -> String {
        let output = self.git(&["rev-parse", "--abbrev-ref", "HEAD"]);
        String::from_utf8(output.stdout).unwrap().trim().to_owned()
    }

    /// Writes an untracked file at `relative`, leaving the working tree dirty (it
    /// is neither committed nor git-ignored).
    pub(crate) fn make_dirty(&self, relative: &str) {
        fs::write(self.root().join(relative), "uncommitted\n").unwrap();
    }

    /// Reads a file relative to the workspace root, if it exists.
    pub(crate) fn read(&self, relative: &str) -> Option<String> {
        fs::read_to_string(self.root().join(relative)).ok()
    }

    /// Drives a command with `args` against this workspace.
    pub(crate) async fn drive(&self, args: &[&str]) -> Result<RunOutcome, AppError> {
        self.flush_git();
        // Point the harvest at this workspace's own `target/` explicitly, so a
        // shared ambient `CARGO_TARGET_DIR` (as `cargo llvm-cov` sets during
        // coverage runs) cannot make the harvester pick up summaries written by
        // other tests sharing that directory. Passing the root through the API
        // keeps the test hermetic without mutating the process environment.
        let target_root = self.root().join("target");

        // Drive `collect`/`backfill` against the faker instead of `cargo bench`:
        // the program plus its fixture-describing arguments form the benchmark
        // command, which the single bench invocation runs to produce engine output.
        let mut bench_command = vec![cargo_bench_history_faker::binary_path().to_owned()];
        bench_command.extend(self.bench.iter().cloned());

        let effective = self.effective_args(args);
        let refs: Vec<&str> = effective.iter().map(String::as_str).collect();
        run_with_overrides(
            &command_from(&refs),
            Overrides {
                workspace_dir: Some(self.root().to_path_buf()),
                target_root: Some(target_root),
                bench_command: Some(bench_command),
                clock: Some(Clock::new_frozen_at(analysis_now())),
                storage_override: None,
                auto_discriminants: self.auto_discriminants_override(),
            },
        )
        .await
    }

    /// Like [`drive`], but leaves the target root unset so the harvester resolves
    /// it the default way, exercising the production `resolve_target_root` wiring
    /// (`collect`'s `target_root.unwrap_or_else(|| resolve_target_root_in(base))`).
    ///
    /// The ambient `CARGO_TARGET_DIR` is cleared for the duration of the run so the
    /// resolution deterministically falls back to `<workspace>/target` (a fresh
    /// tempdir tree). Without this, a developer's shared or per-worktree
    /// `CARGO_TARGET_DIR` would send the harvest into a directory already holding
    /// *other* engines' output, silently storing foreign result sets. `drive`
    /// stays hermetic by pinning `target_root` instead; this method cannot, since
    /// its whole purpose is the no-override path, so it clears the variable and
    /// restores it on drop. Callers must therefore be `#[serial]`.
    ///
    /// [`drive`]: Self::drive
    pub(crate) async fn drive_resolving_target_root(
        &self,
        args: &[&str],
    ) -> Result<RunOutcome, AppError> {
        self.flush_git();
        let mut bench_command = vec![cargo_bench_history_faker::binary_path().to_owned()];
        bench_command.extend(self.bench.iter().cloned());

        let effective = self.effective_args(args);
        let refs: Vec<&str> = effective.iter().map(String::as_str).collect();
        let _cleared = ClearedTargetDir::enter();
        run_with_overrides(
            &command_from(&refs),
            Overrides {
                workspace_dir: Some(self.root().to_path_buf()),
                target_root: None,
                bench_command: Some(bench_command),
                clock: Some(Clock::new_frozen_at(analysis_now())),
                storage_override: None,
                auto_discriminants: self.auto_discriminants_override(),
            },
        )
        .await
    }

    /// Drives a reporting command (`analyze`/`list`/`prune`) requesting the JSON
    /// report into a file and returns its contents. Appends
    /// `--no-text --json <unique path>`, so the JSON file is the only rendered
    /// output, and reads it back. Panics if the command fails.
    pub(crate) async fn drive_json(&self, args: &[&str]) -> String {
        let name = format!(
            "cbh-report-{}.json",
            OUTPUT_SEQ.fetch_add(1, Ordering::Relaxed)
        );
        let mut full: Vec<&str> = args.to_vec();
        full.extend_from_slice(&["--no-text", "--json", &name]);
        self.drive(&full).await.unwrap();
        self.read(&name)
            .expect("the JSON report was written to the requested path")
    }

    /// Drives a reporting command requesting the Markdown report into a file and
    /// returns its contents. Appends `--no-text --markdown <unique path>`, so the
    /// Markdown file is the only rendered output. Panics if the command fails.
    pub(crate) async fn drive_markdown(&self, args: &[&str]) -> String {
        let name = format!(
            "cbh-report-{}.md",
            OUTPUT_SEQ.fetch_add(1, Ordering::Relaxed)
        );
        let mut full: Vec<&str> = args.to_vec();
        full.extend_from_slice(&["--no-text", "--markdown", &name]);
        self.drive(&full).await.unwrap();
        self.read(&name)
            .expect("the Markdown report was written to the requested path")
    }

    /// Drives `analyze` requesting the condensed Markdown summary into a file and
    /// returns its contents. Appends `--no-text --markdown-summary <unique path>`,
    /// so the summary file is the only rendered output. Panics if the command fails.
    pub(crate) async fn drive_markdown_summary(&self, args: &[&str]) -> String {
        let name = format!(
            "cbh-summary-{}.md",
            OUTPUT_SEQ.fetch_add(1, Ordering::Relaxed)
        );
        let mut full: Vec<&str> = args.to_vec();
        full.extend_from_slice(&["--no-text", "--markdown-summary", &name]);
        self.drive(&full).await.unwrap();
        self.read(&name)
            .expect("the Markdown summary was written to the requested path")
    }

    /// All stored objects as `(object key, parsed result set)` pairs, sorted by key.
    pub(crate) fn stored_objects(&self) -> Vec<(String, Run)> {
        let store = self.root().join("store");
        let mut files = Vec::new();
        collect_json_files(&store, &mut files);

        let mut objects: Vec<(String, Run)> = files
            .into_iter()
            .map(|path| {
                let key = path
                    .strip_prefix(&store)
                    .unwrap()
                    .components()
                    .map(|component| component.as_os_str().to_string_lossy().into_owned())
                    .collect::<Vec<_>>()
                    .join("/");
                let raw = fs::read(&path).unwrap();
                let json = String::from_utf8(codec::decompress(&raw).unwrap()).unwrap();
                let set = Run::from_json(&json).unwrap();
                (key, set)
            })
            .collect();
        objects.sort_by(|left, right| left.0.cmp(&right.0));
        objects
    }

    /// Returns the single stored object, failing if there is not exactly one.
    pub(crate) fn single_object(&self) -> (String, Run) {
        let mut objects = self.stored_objects();
        assert_eq!(
            objects.len(),
            1,
            "expected exactly one stored object, found {}",
            objects.len()
        );
        objects.pop().unwrap()
    }

    /// Writes `set` to `key` (a `/`-separated object key) under the local store,
    /// mirroring the layout `collect` produces — including the gzip body encoding the
    /// storage layer applies, so the production read path inflates it correctly.
    pub(crate) fn seed(&self, key: &str, set: &Run) {
        let mut path = self.root().join("store");
        for part in key.split('/') {
            path.push(part);
        }
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, codec::compress(set.to_json().unwrap().as_bytes())).unwrap();
    }

    /// Writes arbitrary `json` to `key`, compressed exactly as [`seed`](Self::seed)
    /// does. Used to plant objects a current [`Run`] can no longer express — notably
    /// a run carrying a metric kind the tool has since dropped — so the read path is
    /// exercised against legacy on-disk data.
    pub(crate) fn seed_raw_json(&self, key: &str, json: &str) {
        let mut path = self.root().join("store");
        for part in key.split('/') {
            path.push(part);
        }
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, codec::compress(json.as_bytes())).unwrap();
    }

    /// Seeds a Callgrind result set for commit `label` that carries a valid `Ir`
    /// metric at `ir_value` plus a `conditional_branch_misses` metric — one of the
    /// build-layout-volatile kinds dropped from the tool. A current [`Run`] cannot
    /// represent that kind, so the object is planted as raw JSON, reproducing history
    /// written by an older tool.
    pub(crate) fn seed_callgrind_with_legacy_metric(&self, label: &str, ir_value: f64) {
        // The dropped metric's value is irrelevant to the read path under test (it is
        // discarded on parse), so any fixed figure standing in for real Callgrind
        // output serves.
        const LEGACY_METRIC_VALUE: f64 = 3.0;

        let commit_id = self.commit_id(label);
        let observed = self.committer_time(&commit_id);
        let key = seed_clean_key(
            Engine::Callgrind,
            "x86_64-unknown-linux-gnu",
            HARNESS_AUTO_MACHINE_KEY,
            &commit_id,
        );
        let mut object: serde_json::Value = serde_json::from_str(
            &ir_result_set(observed.as_second(), &commit_id, ir_value)
                .to_json()
                .unwrap(),
        )
        .unwrap();
        object["results"][0]["metrics"]
            .as_array_mut()
            .unwrap()
            .push(serde_json::json!({
                "kind": "conditional_branch_misses",
                "value": LEGACY_METRIC_VALUE,
            }));
        self.seed_raw_json(&key, &serde_json::to_string(&object).unwrap());
    }

    /// Seeds one Callgrind result set with an `Ir` value for the previously created
    /// commit `label`. The run's provenance observation is taken from that commit's
    /// own git committer date, so the test owns the topology the window is decided
    /// from.
    pub(crate) fn seed_callgrind(&self, label: &str, value: f64) {
        self.seed_callgrind_in(
            "x86_64-unknown-linux-gnu",
            HARNESS_AUTO_MACHINE_KEY,
            label,
            value,
        );
    }

    /// Seeds one clean Callgrind result set into the `triple`/`machine` partition
    /// for the previously created commit `label`, so a single commit can host
    /// objects in several comparable sets (as parallel CI pools produce).
    pub(crate) fn seed_callgrind_in(&self, triple: &str, machine: &str, label: &str, value: f64) {
        let commit_id = self.commit_id(label);
        let observed = self.committer_time(&commit_id);
        let key = seed_clean_key(Engine::Callgrind, triple, machine, &commit_id);
        self.seed(
            &key,
            &ir_result_set(observed.as_second(), &commit_id, value),
        );
    }

    /// Seeds one *dirty* (uncommitted-tree) Callgrind snapshot with an `Ir` value on
    /// the previously created commit `label`, keyed by `observed` (the snapshot's
    /// effective second, the `dirty-<unix>` key, given as `YYYY-MM-DD` at UTC
    /// midnight). A real dirty run executes after the commit it is based on, so the
    /// effective second is legitimately later than the commit's own date.
    pub(crate) fn seed_dirty_callgrind(&self, observed: &str, label: &str, value: f64) {
        let commit_id = self.commit_id(label);
        let effective: Timestamp = format!("{observed}T00:00:00Z").parse().unwrap();
        let key = seed_dirty_key(
            Engine::Callgrind,
            "x86_64-unknown-linux-gnu",
            HARNESS_AUTO_MACHINE_KEY,
            &commit_id,
            effective.as_second(),
        );
        self.seed(
            &key,
            &ir_result_set(effective.as_second(), &commit_id, value),
        );
    }

    /// Seeds one Callgrind result set carrying `metrics` for benchmark
    /// `nm::observe/pull` on the previously created commit `label`.
    pub(crate) fn seed_metrics(&self, label: &str, metrics: Vec<Metric>) {
        let commit_id = self.commit_id(label);
        let observed = self.committer_time(&commit_id);
        let key = seed_clean_key(
            Engine::Callgrind,
            "x86_64-unknown-linux-gnu",
            HARNESS_AUTO_MACHINE_KEY,
            &commit_id,
        );
        self.seed(
            &key,
            &result_set_with(observed.as_second(), &commit_id, metrics),
        );
    }

    /// Seeds a flat-then-stepped clean Callgrind history: [`MIN_REGIME`] commits at
    /// `baseline` then [`MIN_REGIME`] commits at `raised`, labeled `{prefix}1`..
    /// and dated one day apart from `first_date` (`YYYY-MM-DD`). The series holds
    /// [`MIN_SERIES_POINTS`] points — the minimum the change-point detector will
    /// analyse — with each regime long enough to clear the per-regime persistence
    /// gate, so a step between the regimes is a trustable sustained change.
    pub(crate) fn seed_stepped_callgrind(
        &self,
        first_date: &str,
        prefix: &str,
        baseline: f64,
        raised: f64,
    ) {
        for (index, date) in sequential_dates(first_date, MIN_SERIES_POINTS)
            .into_iter()
            .enumerate()
        {
            let label = format!(
                "{prefix}{}",
                index.checked_add(1).expect("commit index overflow")
            );
            self.commit_dated(&date, &label);
            let value = if index < MIN_REGIME { baseline } else { raised };
            self.seed_callgrind(&label, value);
        }
    }

    /// Seeds a flat clean Callgrind history followed by a clear, sustained upward
    /// step: [`MIN_REGIME`] commits at 100 then [`MIN_REGIME`] commits at 130,
    /// labeled `c1`.. and dated one day apart from [`RISING_HISTORY_FIRST_DATE`].
    /// The series holds [`MIN_SERIES_POINTS`] points — long enough that the
    /// change-point detector analyses it, with each regime clearing the per-regime
    /// persistence gate.
    pub(crate) fn seed_rising_callgrind_history(&self) {
        self.seed_stepped_callgrind(RISING_HISTORY_FIRST_DATE, "c", 100.0, 130.0);
    }

    /// Seeds a rising Callgrind history for `count` distinct benchmarks sharing one
    /// partition, so a single analysis pass yields `count` regression findings of
    /// distinct magnitudes. Each benchmark holds a flat baseline of 100 across the
    /// first [`MIN_REGIME`] commits, then steps to a benchmark-specific higher value
    /// across the last [`MIN_REGIME`] — a sustained regression the change-point
    /// detector flags. Benchmark `i` steps to `120 + i`, so magnitudes are distinct
    /// and strictly increasing, giving the global ranking a deterministic order.
    pub(crate) fn seed_many_rising_callgrind_history(&self, count: usize) {
        let baseline = vec![100.0; count];
        // `count` is a small test parameter; a u16 cast keeps the value exact and
        // avoids a lossy `usize as f64` conversion.
        let raised: Vec<f64> = (0..count)
            .map(|index| {
                120.0 + f64::from(u16::try_from(index).expect("benchmark count fits in u16"))
            })
            .collect();
        for (index, date) in sequential_dates("2024-01-01", MIN_SERIES_POINTS)
            .into_iter()
            .enumerate()
        {
            let label = format!("c{}", index.checked_add(1).expect("commit index overflow"));
            self.commit_dated(&date, &label);
            let values = if index < MIN_REGIME {
                &baseline
            } else {
                &raised
            };
            self.seed_many_callgrind(&label, values);
        }
    }

    /// Seeds one Callgrind result set carrying one `Ir` benchmark per entry in
    /// `values` for the previously created commit `label`, all in the shared
    /// `x86_64`/[`HARNESS_AUTO_MACHINE_KEY`] partition.
    pub(crate) fn seed_many_callgrind(&self, label: &str, values: &[f64]) {
        let commit_id = self.commit_id(label);
        let observed = self.committer_time(&commit_id);
        let key = seed_clean_key(
            Engine::Callgrind,
            "x86_64-unknown-linux-gnu",
            HARNESS_AUTO_MACHINE_KEY,
            &commit_id,
        );
        self.seed(
            &key,
            &many_benchmark_result_set(observed.as_second(), &commit_id, values),
        );
    }

    /// Seeds one Criterion `wall_time` result set on the previously created commit
    /// `label`, in the machine-key partition `machine`.
    pub(crate) fn seed_criterion(&self, label: &str, machine: &str, value: f64) {
        let commit_id = self.commit_id(label);
        let observed = self.committer_time(&commit_id);
        let key = seed_clean_key(
            Engine::Criterion,
            "x86_64-pc-windows-msvc",
            machine,
            &commit_id,
        );
        self.seed(
            &key,
            &criterion_result_set(observed.as_second(), &commit_id, value),
        );
    }

    /// Seeds one *dirty* (uncommitted-tree) Criterion `wall_time` snapshot on the
    /// previously created commit `label`, in the machine-key partition `machine`,
    /// keyed by `observed` (the snapshot's effective second, given as `YYYY-MM-DD`
    /// at UTC midnight) like a real dirty run.
    pub(crate) fn seed_dirty_criterion(
        &self,
        observed: &str,
        label: &str,
        machine: &str,
        value: f64,
    ) {
        let commit_id = self.commit_id(label);
        let effective: Timestamp = format!("{observed}T00:00:00Z").parse().unwrap();
        let key = seed_dirty_key(
            Engine::Criterion,
            "x86_64-pc-windows-msvc",
            machine,
            &commit_id,
            effective.as_second(),
        );
        self.seed(
            &key,
            &criterion_result_set(effective.as_second(), &commit_id, value),
        );
    }

    /// Seeds a flat Criterion `wall_time` history then a clear, sustained upward
    /// step: [`MIN_REGIME`] commits at 20 then [`MIN_REGIME`] commits at 30, labeled
    /// `d1`.. and dated one day apart from 2024-02-01. The series holds
    /// [`MIN_SERIES_POINTS`] points, long enough for the change-point detector to
    /// analyse it with each regime clearing the per-regime persistence gate.
    pub(crate) fn seed_rising_criterion_history(&self, machine: &str) {
        for (index, date) in sequential_dates("2024-02-01", MIN_SERIES_POINTS)
            .into_iter()
            .enumerate()
        {
            let label = format!("d{}", index.checked_add(1).expect("commit index overflow"));
            self.commit_dated(&date, &label);
            let value = if index < MIN_REGIME { 20.0 } else { 30.0 };
            self.seed_criterion(&label, machine, value);
        }
    }

    /// Seeds one Callgrind result set carrying two distinct benchmark identities,
    /// `alpha::bench/wide` and `beta::bench/narrow`, each with an `Ir` metric at the
    /// given value, on the previously created commit `label`.
    pub(crate) fn seed_two_benchmarks(&self, label: &str, alpha: f64, beta: f64) {
        let commit_id = self.commit_id(label);
        let observed = self.committer_time(&commit_id);
        let key = seed_clean_key(
            Engine::Callgrind,
            "x86_64-unknown-linux-gnu",
            HARNESS_AUTO_MACHINE_KEY,
            &commit_id,
        );
        self.seed(
            &key,
            &two_benchmark_result_set(observed.as_second(), &commit_id, alpha, beta),
        );
    }

    /// Seeds one clean `alloc_tracker` result set for `operation` on the previously
    /// created commit `label`, recording `bytes` mean bytes and `allocs` mean
    /// allocations per iteration in the [`HARNESS_AUTO_MACHINE_KEY`] partition.
    pub(crate) fn seed_alloc_tracker(&self, label: &str, operation: &str, bytes: f64, allocs: f64) {
        let commit_id = self.commit_id(label);
        let observed = self.committer_time(&commit_id);
        let key = seed_clean_key(
            Engine::AllocTracker,
            "x86_64-unknown-linux-gnu",
            HARNESS_AUTO_MACHINE_KEY,
            &commit_id,
        );
        self.seed(
            &key,
            &alloc_result_set(observed.as_second(), &commit_id, operation, bytes, allocs),
        );
    }

    /// Seeds one clean `all_the_time` result set for `operation` on the previously
    /// created commit `label`, recording `nanos` mean processor-time nanoseconds
    /// per iteration in the `machine`-keyed partition.
    pub(crate) fn seed_all_the_time(
        &self,
        label: &str,
        machine: &str,
        operation: &str,
        nanos: f64,
    ) {
        let commit_id = self.commit_id(label);
        let observed = self.committer_time(&commit_id);
        let key = seed_clean_key(
            Engine::AllTheTime,
            "x86_64-unknown-linux-gnu",
            machine,
            &commit_id,
        );
        self.seed(
            &key,
            &time_result_set(observed.as_second(), &commit_id, operation, nanos),
        );
    }

    /// Seeds one clean `all_the_time` result set for `operation`, like
    /// [`Self::seed_all_the_time`], but additionally recording a confidence
    /// interval of `nanos` ± `half_width` so the noise detector's
    /// CI-non-overlap gate has dispersion to compare.
    pub(crate) fn seed_all_the_time_with_interval(
        &self,
        label: &str,
        machine: &str,
        operation: &str,
        nanos: f64,
        half_width: f64,
    ) {
        let commit_id = self.commit_id(label);
        let observed = self.committer_time(&commit_id);
        let key = seed_clean_key(
            Engine::AllTheTime,
            "x86_64-unknown-linux-gnu",
            machine,
            &commit_id,
        );
        self.seed(
            &key,
            &time_result_set_with_dispersion(
                observed.as_second(),
                &commit_id,
                operation,
                nanos,
                half_width,
            ),
        );
    }
}

/// The project every seeded object is recorded under, matching the identity the
/// faker-backed `collect`/`import` runs resolve for the test repository.
const SEED_PROJECT: &str = "testproj";

/// Builds a clean-object storage key through the model's key builder — the exact
/// code path `run` writes under — so a seed helper cannot drift from the
/// production storage layout by hand-formatting a key string.
fn seed_clean_key(engine: Engine, triple: &str, machine: &str, commit: &str) -> String {
    DiscriminantSet::new(
        engine,
        &TargetTriple::from(triple),
        &MachineKey::from(machine),
    )
    .clean_key(SEED_PROJECT, commit)
}

/// Builds a dirty-snapshot storage key through the model's key builder, keyed by
/// `observation_unix` exactly as a real dirty run is.
fn seed_dirty_key(
    engine: Engine,
    triple: &str,
    machine: &str,
    commit: &str,
    observation_unix: i64,
) -> String {
    DiscriminantSet::new(
        engine,
        &TargetTriple::from(triple),
        &MachineKey::from(machine),
    )
    .dirty_key(SEED_PROJECT, commit, observation_unix)
}

/// Clears `CARGO_TARGET_DIR` from the process environment for its lifetime,
/// restoring the previous value on drop.
///
/// [`Workspace::drive_resolving_target_root`] uses this so the production
/// `resolve_target_root` deterministically falls back to `<workspace>/target`
/// rather than picking up whatever shared target directory the developer's
/// environment happens to point at.
struct ClearedTargetDir {
    previous: Option<std::ffi::OsString>,
}

impl ClearedTargetDir {
    fn enter() -> Self {
        let previous = std::env::var_os("CARGO_TARGET_DIR");
        // SAFETY: The one caller is `#[serial]`, and the suite runs under nextest
        // (a separate process per test), so no other thread reads or writes the
        // environment concurrently with this mutation.
        unsafe {
            std::env::remove_var("CARGO_TARGET_DIR");
        }
        Self { previous }
    }
}

impl Drop for ClearedTargetDir {
    fn drop(&mut self) {
        let Some(previous) = self.previous.take() else {
            return;
        };
        // SAFETY: The one caller is `#[serial]`, and the suite runs under nextest
        // (a separate process per test), so no other thread reads or writes the
        // environment concurrently with this mutation.
        unsafe {
            std::env::set_var("CARGO_TARGET_DIR", previous);
        }
    }
}

/// Produces `count` consecutive `YYYY-MM-DD` date strings starting at `start`
/// (`YYYY-MM-DD`), one calendar day apart. Fixtures use it to generate commit
/// chains long enough to clear the detectors' minimum-evidence gates without
/// hand-writing every date. Callers keep `start` and `count` inside `analyze`'s
/// default six-month look-back window (see [`analysis_now`]).
pub(crate) fn sequential_dates(start: &str, count: usize) -> Vec<String> {
    let mut date: jiff::civil::Date = start.parse().expect("start is a valid YYYY-MM-DD date");
    let mut dates = Vec::with_capacity(count);
    for _ in 0..count {
        dates.push(date.to_string());
        date = date
            .tomorrow()
            .expect("date stays within the supported range");
    }
    dates
}

/// A fixed clock anchor for `analyze`/`list`'s history-mode default `--since`
/// window. Seed data lands across 2024; anchoring "now" at 2024-06-01 keeps the
/// default six-month look-back (cutoff 2023-12-01) inclusive of that data, so the
/// default window does not silently drop seeded points as wall-clock time passes.
pub(crate) fn analysis_now() -> Timestamp {
    "2024-06-01T00:00:00Z".parse::<Timestamp>().unwrap()
}

/// Parses CLI arguments into the typed [`Command`], exactly as the binary does.
pub(crate) fn command_from(args: &[&str]) -> Command {
    Cli::from_args(&["cargo-bench-history"], args)
        .unwrap()
        .into_command()
}

/// Builds a [`GitInfo`] for a committed run: the full hash is `<commit>full` and
/// the branch is `main` (clean working tree).
pub(crate) fn git_info(commit: &str) -> GitInfo {
    GitInfo {
        commit: Some(format!("{commit}full")),
        branch: Some("main".to_owned()),
        dirty: false,
    }
}

/// Builds a Callgrind result set with two records — `alpha::bench/wide` and
/// `beta::bench/narrow` — each carrying a single `Ir` metric, stamped with the
/// effective second and `commit`.
pub(crate) fn two_benchmark_result_set(effective: i64, commit: &str, alpha: f64, beta: f64) -> Run {
    let time = Timestamp::from_second(effective).unwrap();
    let git = git_info(commit);
    let context = RunContext::new(
        time,
        git,
        EnvironmentInfo::default(),
        ToolchainInfo::default(),
        TOOL_VERSION.to_owned(),
    );
    let ir = |value: f64| vec![Metric::new(MetricKind::InstructionCount, value)];
    let records = vec![
        BenchmarkResult::new(
            BenchmarkId::new(nonempty![
                "alpha".to_owned(),
                "alpha::bench".to_owned(),
                "wide".to_owned(),
            ]),
            ir(alpha),
        ),
        BenchmarkResult::new(
            BenchmarkId::new(nonempty![
                "beta".to_owned(),
                "beta::bench".to_owned(),
                "narrow".to_owned(),
            ]),
            ir(beta),
        ),
    ];
    Run::new(context, records)
}

/// Builds a Callgrind result set carrying one `Ir` benchmark per entry in
/// `values`, each under a distinct, zero-padded benchmark id so a single
/// partition can host many independent series. Stamped with the effective second
/// and `commit`.
pub(crate) fn many_benchmark_result_set(effective: i64, commit: &str, values: &[f64]) -> Run {
    let time = Timestamp::from_second(effective).unwrap();
    let git = git_info(commit);
    let context = RunContext::new(
        time,
        git,
        EnvironmentInfo::default(),
        ToolchainInfo::default(),
        TOOL_VERSION.to_owned(),
    );
    let records = values
        .iter()
        .enumerate()
        .map(|(index, &value)| {
            BenchmarkResult::new(
                BenchmarkId::new(nonempty![
                    "many".to_owned(),
                    format!("many::bench_{index:03}"),
                    "wide".to_owned(),
                ]),
                vec![Metric::new(MetricKind::InstructionCount, value)],
            )
        })
        .collect();
    Run::new(context, records)
}

/// Builds a Criterion result set for benchmark `time/capture/std_instant`
/// carrying a single `wall_time` metric at `value` nanoseconds, stamped with the
/// effective second and `commit`.
pub(crate) fn criterion_result_set(effective: i64, commit: &str, value: f64) -> Run {
    let time = Timestamp::from_second(effective).unwrap();
    let git = git_info(commit);
    let context = RunContext::new(
        time,
        git,
        EnvironmentInfo::default(),
        ToolchainInfo::default(),
        TOOL_VERSION.to_owned(),
    );
    let record = BenchmarkResult::new(
        BenchmarkId::new(nonempty![
            "time/capture".to_owned(),
            "std_instant".to_owned()
        ]),
        vec![Metric::new(MetricKind::WallTime, value)],
    );
    Run::new(context, vec![record])
}

/// Builds an `alloc_tracker` result set for `operation` carrying an
/// `allocated_bytes` and an `allocation_count` metric, stamped with the effective
/// second and `commit`.
pub(crate) fn alloc_result_set(
    effective: i64,
    commit: &str,
    operation: &str,
    bytes: f64,
    allocs: f64,
) -> Run {
    let time = Timestamp::from_second(effective).unwrap();
    let git = git_info(commit);
    let context = RunContext::new(
        time,
        git,
        EnvironmentInfo::default(),
        ToolchainInfo::default(),
        TOOL_VERSION.to_owned(),
    );
    let record = BenchmarkResult::new(
        BenchmarkId::new(nonempty![operation.to_owned()]),
        vec![
            Metric::new(MetricKind::AllocatedBytes, bytes),
            Metric::new(MetricKind::AllocationCount, allocs),
        ],
    );
    Run::new(context, vec![record])
}

/// Builds an `all_the_time` result set for `operation` carrying a single
/// `processor_time` metric at `value` nanoseconds, stamped with the effective
/// second and `commit`.
pub(crate) fn time_result_set(effective: i64, commit: &str, operation: &str, value: f64) -> Run {
    let time = Timestamp::from_second(effective).unwrap();
    let git = git_info(commit);
    let context = RunContext::new(
        time,
        git,
        EnvironmentInfo::default(),
        ToolchainInfo::default(),
        TOOL_VERSION.to_owned(),
    );
    let record = BenchmarkResult::new(
        BenchmarkId::new(nonempty![operation.to_owned()]),
        vec![Metric::new(MetricKind::ProcessorTime, value)],
    );
    Run::new(context, vec![record])
}

/// Builds an `all_the_time` result set for `operation` carrying a single
/// `processor_time` metric at `value` nanoseconds, like [`time_result_set`],
/// but additionally recording a confidence interval of `value` ± `half_width`
/// (and a matching standard deviation) so the noise detector can apply its
/// CI-non-overlap gate to processor time.
pub(crate) fn time_result_set_with_dispersion(
    effective: i64,
    commit: &str,
    operation: &str,
    value: f64,
    half_width: f64,
) -> Run {
    let time = Timestamp::from_second(effective).unwrap();
    let git = git_info(commit);
    let context = RunContext::new(
        time,
        git,
        EnvironmentInfo::default(),
        ToolchainInfo::default(),
        TOOL_VERSION.to_owned(),
    );
    let record = BenchmarkResult::new(
        BenchmarkId::new(nonempty![operation.to_owned()]),
        vec![
            Metric::new(MetricKind::ProcessorTime, value).with_dispersion(
                Some(half_width),
                Some(value - half_width),
                Some(value + half_width),
            ),
        ],
    );
    Run::new(context, vec![record])
}

/// The number of series a rendered text report states it judged, read from the
/// coverage field every analysis report carries in its header.
///
/// Panics when the report carries no coverage field: a report with nothing in scope
/// cannot support any claim about the detectors.
pub(crate) fn judged_series(report: &str) -> usize {
    let (_, tail) = report
        .split_once("in-scope series judged: ")
        .unwrap_or_else(|| panic!("the report header states its series coverage: {report}"));
    let (judged, _) = tail
        .split_once(" of ")
        .unwrap_or_else(|| panic!("the coverage field reads `judged of in-scope`: {report}"));
    judged
        .parse()
        .unwrap_or_else(|_| panic!("the coverage field counts series: {report}"))
}

/// Asserts that an analysis actually reached a verdict on at least one series.
///
/// An assertion that nothing was flagged only says something about the detectors
/// when the data reached them. The same silence is also what a history too short to
/// judge, a ghost-filtered benchmark, or a detector switched off by a bad gate
/// produces — so a test asserting an absence states this first, and thereby cannot
/// pass vacuously.
pub(crate) fn assert_history_was_judged(report: &str) {
    assert!(
        judged_series(report) > 0,
        "the analysis judged nothing, so its silence proves nothing: {report}"
    );
}

pub(crate) fn ir_of(record: &BenchmarkResult) -> f64 {
    record
        .metrics
        .iter()
        .find(|metric| metric.kind == MetricKind::InstructionCount)
        .unwrap()
        .value
}

/// The metric of kind `kind` within a record.
pub(crate) fn metric_of(record: &BenchmarkResult, kind: MetricKind) -> &Metric {
    record
        .metrics
        .iter()
        .find(|metric| metric.kind == kind)
        .unwrap()
}

/// Escapes a string for embedding in a TOML basic (double-quoted) string, so a
/// project id with unusual characters survives as a literal.
pub(crate) fn toml_escape(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

/// A configuration with a project id but no storage section, used by `analyze`
/// tests that seed history directly and never launch an engine. Local storage is
/// supplied at run time via the harness's injected `--local`, so it is absent here.
pub(crate) fn storage_only_config() -> String {
    "[project]\n\
     id = \"testproj\"\n"
        .to_owned()
}

/// Builds a Callgrind result set for benchmark `nm::observe/pull` carrying the
/// given `metrics`, stamped with the effective second and `commit`.
pub(crate) fn result_set_with(effective: i64, commit: &str, metrics: Vec<Metric>) -> Run {
    let time = Timestamp::from_second(effective).unwrap();
    let git = git_info(commit);
    let context = RunContext::new(
        time,
        git,
        EnvironmentInfo::default(),
        ToolchainInfo::default(),
        TOOL_VERSION.to_owned(),
    );
    let record = BenchmarkResult::new(
        BenchmarkId::new(nonempty![
            "nm".to_owned(),
            "nm::observe".to_owned(),
            "pull".to_owned(),
        ]),
        metrics,
    );
    Run::new(context, vec![record])
}

/// Builds a Callgrind result set with a single `Ir` metric at `value`, stamped
/// with the given effective second and `commit`.
pub(crate) fn ir_result_set(effective: i64, commit: &str, value: f64) -> Run {
    result_set_with(
        effective,
        commit,
        vec![Metric::new(MetricKind::InstructionCount, value)],
    )
}

/// A configuration with an explicit project `id` (which may contain characters
/// that require sanitizing for the storage partition) and no storage section.
/// Local storage is supplied at run time via the harness's injected `--local`.
pub(crate) fn storage_only_config_with_id(id: &str) -> String {
    format!(
        "[project]\n\
         id = \"{}\"\n",
        toml_escape(id)
    )
}

pub(crate) fn collect_json_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_json_files(&path, out);
        } else if path.extension().is_some_and(|ext| ext == "json") {
            out.push(path);
        }
    }
}

/// Reusing a label returns the original commit without redating it, so several
/// comparable objects (parallel CI pools, or a dirty snapshot sitting on an
/// already-seeded commit) share one commit directory and committer date.
#[test]
#[cfg_attr(miri, ignore)]
fn commit_dated_reuses_the_existing_commit() {
    let workspace = Workspace::repo(&storage_only_config());
    let first = workspace.commit_dated("2024-01-01", "c1");
    let again = workspace.commit_dated("2024-01-05", "c1");
    assert_eq!(first, again, "the label reuses c1's commit");
}

/// The fixture sizing constants must track the detector's public defaults; if a
/// gate default moves, this guard fails so the fixtures are re-derived rather than
/// silently seeding too little (or needless) history.
#[test]
fn fixture_gate_sizes_match_the_detector_defaults() {
    assert_eq!(
        MIN_REGIME,
        cbh_detect::MIN_REGIME,
        "MIN_REGIME must match the detector's per-regime persistence gate"
    );
    assert_eq!(
        MIN_SERIES_POINTS,
        cbh_detect::MIN_SERIES_POINTS,
        "MIN_SERIES_POINTS must match the detector's minimum-series-length gate"
    );
}
