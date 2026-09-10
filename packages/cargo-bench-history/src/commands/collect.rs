//! The `collect` command: execute the configured engines and store the results.
//!
//! Orchestration is generic over a set of small async ports (process runner,
//! environment probe, benchmark-output source, storage) plus an injected
//! [`tick::Clock`], so the whole flow is exercised in-process with fakes. The
//! public [`execute`] wires the real adapters and is what the binary runs.

use std::num::NonZero;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use cbh_config::{
    CloudStorageConfig, Config, STORAGE_ENV_VAR, load_config, resolve_config_path,
    resolve_local_path, resolve_project_id, resolve_repo, storage_env,
};
use cbh_diag::{Reporter, ReporterExt, StderrReporter, count_noun};
use cbh_engines::{
    BenchOutputSource, FsBenchOutputSource, Harvest, RawOperationFile, injected_bench_env,
    parse_all_the_time_operation, parse_alloc_tracker_operation, parse_callgrind_summary,
    parse_criterion_case,
};
use cbh_git::{BenchRunner, TokioBenchRunner};
use cbh_probe::{
    EnvironmentProbe, HardwareProfile, RustcInfo, SystemProbe, describe_fingerprint_components,
    resolve_machine_key,
};
use cbh_storage::{Storage, StorageFacade, build_storage};
use jiff::Timestamp;
use ohno::AppError;
use tick::Clock;

use crate::errors::{
    BenchCommandFailedError, EngineFailedError, EngineTerminatedError, GitProbeFailedError,
    HarvestFailedError, InconsistentRunsError, InvalidCommandError, ParseOutputError,
    ToolchainProbeFailedError,
};
use crate::model::{
    BenchmarkResult, DiscriminantSet, Engine, EnvironmentInfo, GitInfo, MachineInfo, MachineKey,
    Run, RunContext, TargetTriple, ToolchainInfo, detect_environment, min_per_metric,
};
use crate::{
    CollectOptions, DuplicateResultError, LocalStorageSelection, RunOutcome, finish_with_flush,
};

/// The program and base arguments the production tool runs to benchmark the
/// workspace. The first-class scope flags (`--workspace`/`--package`/`--bench`)
/// and any `--` passthrough are appended to this.
const DEFAULT_BENCH_COMMAND: [&str; 2] = ["cargo", "bench"];

/// The label used for the single benchmark command in error messages.
const BENCH_COMMAND_LABEL: &str = "cargo bench";

/// The injected collaborators an orchestrated run operates against.
pub(crate) struct CollectDeps<'a, R, P, O, S> {
    /// Launches the benchmark command.
    pub(crate) runner: &'a R,
    /// Probes git and toolchain facts.
    pub(crate) probe: &'a P,
    /// Harvests engine output.
    pub(crate) output: &'a O,
    /// Persists result sets. `None` under `--no-store`, where benchmarks run but
    /// nothing is written; the store path is never reached in that case.
    pub(crate) storage: Option<&'a S>,
    /// Supplies wall-clock time.
    pub(crate) clock: &'a Clock,
    /// Resolves environment variables (for CI detection).
    pub(crate) env: &'a dyn Fn(&str) -> Option<String>,
    /// Resolved project identity for the storage partition.
    pub(crate) project_id: &'a str,
    /// Version of this tool, recorded with each run.
    pub(crate) tool_version: &'a str,
    /// Cargo target directory that engines write to and the harvest scans. It is
    /// injected into the benchmark command's environment as `CARGO_TARGET_DIR` so
    /// the output always lands where the harvest looks for it.
    pub(crate) target_root: &'a Path,
    /// The benchmark command (program plus base arguments) run once per `collect`.
    /// Production uses `cargo bench`; the scope flags and passthrough are appended
    /// to it. Tests inject a mock program in its place.
    pub(crate) bench_command: &'a [String],
    /// Sink for `--verbose` diagnostic notes describing each step of the run.
    pub(crate) reporter: &'a dyn Reporter,
}

/// The real `collect`: wire the production adapters and orchestrate.
///
/// `target_root` overrides the cargo target directory the harvest scans.
/// Production passes `None`, resolving the root from `CARGO_TARGET_DIR` (or the
/// `target/` default) as an absolute path. `bench_command` overrides the program
/// run to produce benchmark output; production passes `None`, defaulting to
/// `cargo bench`. Tests pass explicit values so the flow is hermetic without
/// mutating the process environment.
pub(crate) async fn execute(
    options: &CollectOptions,
    workspace_dir: &Path,
    target_root: Option<PathBuf>,
    bench_command: Option<Vec<String>>,
    storage_override: Option<StorageFacade>,
) -> Result<RunOutcome, AppError> {
    let reporter = StderrReporter::new(options.verbose);

    // `--repo` selects the repository the run operates on (where benches run, git
    // state is read, and output is harvested), relative to the ambient base; it
    // defaults to the base directory itself.
    let base = resolve_repo(workspace_dir, options.repo.as_deref());
    let base = base.as_path();

    let config_path = resolve_config_path(base, options.config_path.as_deref());
    reporter.note_with(|| format!("loading configuration from {}", config_path.display()));
    let config = load_config(&config_path, options.config_path.is_some()).await?;

    let project_id = resolve_project_id(&config, base);
    reporter.note_with(|| format!("project id: {project_id}"));

    // Under `--no-store` the run produces no stored objects, so storage selection
    // is skipped entirely: the command works with no `--local` and no configured
    // cloud backend, which would otherwise be an error.
    let storage = if let Some(backend) = storage_override {
        reporter.note_with(|| "storage backend: injected by test override".to_owned());
        Some(backend)
    } else if options.no_store {
        reporter.note_with(|| {
            "not resolving a storage backend because --no-store was given; \
             benchmarks will run but no results will be stored"
                .to_owned()
        });
        None
    } else {
        let local = resolve_local_path(options.local.as_ref(), storage_env().as_deref())?;
        reporter.note_with(|| {
            format!(
                "storage backend: {}",
                describe_storage(options.local.as_ref(), local.as_deref(), &config)
            )
        });
        Some(build_storage(local.as_deref(), &config, base, None)?)
    };

    let runner = TokioBenchRunner::in_dir(base);
    let probe = SystemProbe::in_dir(base);
    let target_root = target_root.unwrap_or_else(|| resolve_target_root_in(base));
    reporter.note_with(|| {
        format!(
            "cargo target directory (scanned for engine output): {}",
            target_root.display()
        )
    });
    let output = FsBenchOutputSource::new(target_root.clone());
    let clock = Clock::new_tokio();
    let env = |name: &str| std::env::var(name).ok();
    let bench_command = bench_command.unwrap_or_else(default_bench_command);

    let deps = CollectDeps {
        runner: &runner,
        probe: &probe,
        output: &output,
        storage: storage.as_ref(),
        clock: &clock,
        env: &env,
        project_id: &project_id,
        tool_version: env!("CARGO_PKG_VERSION"),
        target_root: &target_root,
        bench_command: &bench_command,
        reporter: &reporter,
    };

    let result = execute_collect(options, &deps).await;
    // Flush the cache-invalidation marker after the run: an `--overwrite` that
    // replaced a stored object armed it, and bumping the marker is what invalidates
    // *other* machines' read-through caches. An append-only run never arms it, so
    // this is a cheap no-op there. The flush runs even on a partial failure, since a
    // write that already reached the cloud must still invalidate caches.
    let flush = match storage.as_ref() {
        Some(storage) => {
            storage
                .flush_pending_invalidation(&project_id, &reporter)
                .await
        }
        None => Ok(()),
    };
    finish_with_flush(result, flush)
}

/// A short human-readable description of where results are stored, for the
/// verbose diagnostic trail.
///
/// `selection` is the raw `--local` choice and `resolved_local` is the path it
/// resolved to (if any), so the note can state both the chosen backend and why —
/// an explicit `--local` path, the environment-variable path behind a bare
/// `--local`, or the cloud backend configured when no `--local` was given.
pub(crate) fn describe_storage(
    selection: Option<&LocalStorageSelection>,
    resolved_local: Option<&Path>,
    config: &Config,
) -> String {
    match (selection, resolved_local) {
        (Some(LocalStorageSelection::Path(_)), Some(path)) => {
            format!("local filesystem at {} (from --local)", path.display())
        }
        (Some(LocalStorageSelection::FromEnv), Some(path)) => {
            format!(
                "local filesystem at {} (from {STORAGE_ENV_VAR})",
                path.display()
            )
        }
        _ => match &config.storage {
            Some(CloudStorageConfig::Azure(azure)) => format!(
                "Azure Blob (account {}, container {})",
                azure.account, azure.container
            ),
            None => "none configured".to_owned(),
        },
    }
}

/// The production benchmark command: `cargo bench`.
pub(crate) fn default_bench_command() -> Vec<String> {
    DEFAULT_BENCH_COMMAND
        .iter()
        .map(|part| (*part).to_owned())
        .collect()
}

/// Probe facts shared across every engine in a single run.
///
/// `collect` builds this straight from the host probe; `import` builds it the same
/// way and then applies its metadata overrides (target triple, commit, dirty)
/// before handing it to [`finalize_and_store`], which treats it as ground truth.
pub(crate) struct SharedContext {
    /// Git facts of the working directory.
    pub(crate) git: GitInfo,
    /// Active toolchain facts.
    pub(crate) rustc: RustcInfo,
    /// Detected execution environment.
    pub(crate) env: EnvironmentInfo,
    /// Effective target triple the run is keyed under and recorded against — the
    /// host triple `rustc` reports for `collect`, or an `import --target-triple`
    /// override. It feeds both the partition key and `ToolchainInfo.target_triple`.
    pub(crate) target_triple: TargetTriple,
    /// Host hardware profile: the factors fingerprinted into the machine key, plus
    /// the provenance recorded beside them.
    pub(crate) hardware: HardwareProfile,
}

/// Probes the host once for the facts every engine shares.
///
/// Both commands start here: `collect` stores the result as-is, `import` mutates
/// the key-affecting discriminants afterwards. It is the single place the raw host
/// context is assembled.
pub(crate) async fn probe_context<P>(
    probe: &P,
    env: &dyn Fn(&str) -> Option<String>,
) -> Result<SharedContext, AppError>
where
    P: EnvironmentProbe,
{
    let rustc = probe
        .toolchain()
        .await
        .map_err(ToolchainProbeFailedError::caused_by)?;
    Ok(SharedContext {
        git: probe.git().await.map_err(GitProbeFailedError::caused_by)?,
        target_triple: TargetTriple::from(rustc.host.clone().unwrap_or_default()),
        rustc,
        env: detect_environment(env),
        hardware: probe.hardware().await,
    })
}

/// The storage partition a run's results are keyed under, less the engine.
///
/// A stored object's discriminant set is `engine / target_triple / machine_key`.
/// The engine varies across the engines of a single run, while the other two are
/// fixed by the host — so they are derived once, here. This is the one place
/// that derivation lives, so `backfill`'s pre-check
/// and the store path share it rather than reimplementing it.
///
/// The target triple is whatever `rustc -vV` reports in the checkout being
/// probed, and a worktree governs its own toolchain — so `backfill`, which
/// derives the partition once from the newest checkout in its range, holds that
/// partition for the whole range only as long as every toolchain in the range
/// resolves to the same host triple. Pinning a bare channel does; a
/// fully-qualified pin carrying its own host triple (`1.75.0-x86_64-unknown-linux-musl`)
/// does not, and a range that crosses such a change scans one partition while
/// writing another.
///
/// The components are held exactly as probed;
/// [`discriminant_set`](Self::discriminant_set) is what sanitizes them into path
/// segments, so the effective-partition announcement still reports what the host
/// reported.
pub(crate) struct Partition {
    /// Effective target triple: the toolchain host, or an `import` override.
    pub(crate) target_triple: TargetTriple,
    /// Effective machine key: the auto-detected hardware fingerprint.
    pub(crate) machine_key: MachineKey,
}

impl Partition {
    /// The discriminant set this partition forms together with `engine`.
    pub(crate) fn discriminant_set(&self, engine: Engine) -> DiscriminantSet {
        DiscriminantSet::new(engine, &self.target_triple, &self.machine_key)
    }
}

/// Derives the storage partition from an already-probed host context.
pub(crate) fn partition_of(shared: &SharedContext) -> Partition {
    Partition {
        target_triple: shared.target_triple.clone(),
        machine_key: MachineKey::from(resolve_machine_key(&shared.hardware)),
    }
}

/// Probes the host and derives the storage partition a run there would write to.
///
/// The read-side counterpart of the store path: `backfill` calls this against the
/// worktree probe before any commit is benchmarked, so the commits it treats as
/// already recorded are exactly the ones present in the partition it would write.
pub(crate) async fn probe_partition<P>(
    probe: &P,
    env: &dyn Fn(&str) -> Option<String>,
) -> Result<Partition, AppError>
where
    P: EnvironmentProbe,
{
    let shared = probe_context(probe, env).await?;
    Ok(partition_of(&shared))
}

/// The injected collaborators the store back half operates against, shared by
/// `collect` and `import`.
///
/// A lean subset of [`CollectDeps`] with only what persisting a finalized run
/// needs — no runner, output source, or bench command — so `import` (which has no
/// benchmark command to run) can call [`finalize_and_store`] without inventing
/// stand-ins for collect-only ports.
pub(crate) struct FinalizeDeps<'a, S> {
    /// Persists result sets. `None` under `--no-store`, where the store path is
    /// never reached.
    pub(crate) storage: Option<&'a S>,
    /// Resolved project identity for the storage partition.
    pub(crate) project_id: &'a str,
    /// Version of this tool, recorded with each run.
    pub(crate) tool_version: &'a str,
    /// Sink for `--verbose` diagnostic notes.
    pub(crate) reporter: &'a dyn Reporter,
}

/// How a finalized run is placed in storage, independent of which command
/// produced it.
///
/// Decouples the store logic from `CollectOptions` so `collect` and `import` feed
/// it from their own option structs. The dirty/clean choice is *not* here: it
/// rides on [`SharedContext::git`]'s `dirty` flag (probed by `collect`, set from
/// `--dirty` by `import`), keeping the stored body and its key coherent by
/// construction.
pub(crate) struct StoreParams {
    /// Replace an existing object in place instead of failing on a collision.
    pub(crate) overwrite: bool,
    /// Treat an existing object as a success that writes nothing (append-only).
    pub(crate) skip_existing: bool,
    /// Skip storage entirely (`collect --no-store`). `import` never sets this.
    pub(crate) no_store: bool,
}

/// One engine's best-of reduction: the records to store and how many repetitions
/// of the benchmark suite their per-metric minima were taken over.
///
/// The two travel together because the count is only meaningful as a description
/// of how those records were produced, and it is recorded on the stored run as
/// measurement-protocol provenance.
struct Reduction {
    /// The records to store, each metric the minimum across the repetitions.
    records: Vec<BenchmarkResult>,
    /// How many repetitions the reduction actually consumed. Derived from the
    /// harvested runs rather than the requested `--best-of`, so the recorded value
    /// can never claim a repetition that did not happen.
    runs: usize,
}

/// The result of harvesting one engine's output.
struct EngineSummary {
    /// Whether a result set was stored.
    stored: bool,
    /// Number of benchmark cases harvested.
    count: usize,
    /// Human-readable per-engine summary, or `None` when the engine produced no
    /// output (so it is silently absent from the summary).
    label: Option<String>,
}

/// Aggregate outcome of running every selected engine in one run.
pub(crate) struct CollectSummary {
    /// Number of result sets stored.
    pub(crate) stored: usize,
    /// Number of benchmark cases harvested across all engines.
    pub(crate) harvested: usize,
    /// Per-engine human-readable labels.
    pub(crate) labels: Vec<String>,
}

/// Orchestrates a run against injected collaborators.
pub(crate) async fn execute_collect<R, P, O, S>(
    options: &CollectOptions,
    deps: &CollectDeps<'_, R, P, O, S>,
) -> Result<RunOutcome, AppError>
where
    R: BenchRunner,
    P: EnvironmentProbe,
    O: BenchOutputSource,
    S: Storage,
{
    let summary = run_engines(options, deps).await?;
    let message = build_message(
        options.no_store,
        summary.stored,
        summary.harvested,
        &summary.labels,
    );
    Ok(RunOutcome::Completed { message })
}

/// Runs the benchmark command `--best-of` times and harvests every engine's output.
///
/// This is the storage-aware core shared by the `collect` command and `backfill`:
/// the former wraps the summary in a human-readable message, the latter maps it
/// to a per-commit outcome. The benchmark command (`cargo bench` in production) is
/// run `options.best_of` times, each time with the union of every engine's
/// injected environment; each engine is identified by which output tree it
/// populated. An engine that produced no output (for example Callgrind off Linux,
/// where its benches compile to no-ops) simply contributes nothing.
///
/// With `--best-of N` the whole suite runs `N` times and each metric is reduced to
/// its minimum across the runs (see [`min_per_metric`]), so a transient slowdown
/// on one run is discarded rather than stored. Every run must measure the same set
/// of cases and metrics; a mismatch fails the collection ([`InconsistentRunsError`]).
/// The stored run takes its timeline position and dirty-snapshot key from the
/// first run's start, and the git/toolchain/hardware context is probed once after
/// the runs finish (it does not change between them). `N == 1` reproduces a plain
/// single run.
pub(crate) async fn run_engines<R, P, O, S>(
    options: &CollectOptions,
    deps: &CollectDeps<'_, R, P, O, S>,
) -> Result<CollectSummary, AppError>
where
    R: BenchRunner,
    P: EnvironmentProbe,
    O: BenchOutputSource,
    S: Storage,
{
    let argv = build_bench_argv(deps.bench_command, options)?;

    // The benchmark command runs with the union of every engine's injected
    // environment plus `CARGO_TARGET_DIR` pinned to the directory the harvest
    // scans, so engine output always lands where it is collected from — notably
    // when an ambient `CARGO_TARGET_DIR` (such as the one `cargo llvm-cov` sets)
    // differs from the root this run resolved.
    let mut env = injected_bench_env();
    env.push((
        "CARGO_TARGET_DIR".to_owned(),
        deps.target_root.to_string_lossy().into_owned(),
    ));

    deps.reporter
        .note_with(|| format!("running benchmark command: {}", argv.join(" ")));
    deps.reporter.note_with(|| {
        let rendered_env = env
            .iter()
            .map(|(name, value)| format!("{name}={value}"))
            .collect::<Vec<_>>()
            .join(", ");
        format!("injected environment: {rendered_env}")
    });

    let runs = options.best_of.get();
    if runs > 1 {
        deps.reporter.note_with(|| {
            format!(
                "best-of collection: running the suite {runs} times and keeping the minimum \
                 value per metric, so a transient slowdown on any single run is discarded"
            )
        });
    }

    // One bucket per engine (parallel to `Engine::ALL`), each collecting that
    // engine's harvested records from every run so they can be reduced together.
    let mut per_engine: Vec<Vec<Vec<BenchmarkResult>>> = Engine::ALL
        .iter()
        .map(|_| Vec::with_capacity(runs))
        .collect();
    let mut first_run_start: Option<SystemTime> = None;

    for run_number in 1..=runs {
        let run_start = deps.clock.system_time();
        if first_run_start.is_none() {
            first_run_start = Some(run_start);
        }
        if runs > 1 {
            deps.reporter.note_with(|| {
                format!(
                    "best-of run {run_number}/{runs}: invoking {}",
                    argv.join(" ")
                )
            });
        }

        let status = deps
            .runner
            .run_benches(&argv, &env)
            .await
            .map_err(|error| BenchCommandFailedError::caused_by(argv.join(" "), error))?;
        if !status.success {
            return Err(match status.code {
                Some(code) => EngineFailedError::new(BENCH_COMMAND_LABEL, code).into(),
                None => EngineTerminatedError::new(BENCH_COMMAND_LABEL).into(),
            });
        }
        deps.reporter.note_with(|| {
            format!(
                "benchmark command finished; harvesting output modified at or after {} \
             (older files are treated as stale leftovers)",
                timestamp_from(run_start)
            )
        });

        for (bucket, engine) in per_engine.iter_mut().zip(Engine::ALL) {
            let records =
                harvest_records(deps.output, deps.reporter, engine, Some(run_start)).await?;
            bucket.push(records);
        }
    }

    // At least one run always executes (`best_of` is non-zero), so a first start
    // was recorded. It stamps the stored run's observation time and dirty-snapshot
    // second; later runs share the same commit, so their starts do not matter.
    let run_start = first_run_start.expect("best-of runs the suite at least once");

    let shared = probe_context(deps.probe, deps.env).await?;

    let store = FinalizeDeps {
        storage: deps.storage,
        project_id: deps.project_id,
        tool_version: deps.tool_version,
        reporter: deps.reporter,
    };
    let params = StoreParams {
        overwrite: options.overwrite,
        skip_existing: options.skip_existing,
        no_store: options.no_store,
    };
    finalize_and_store(
        &store,
        &shared,
        &params,
        "collecting",
        "toolchain host",
        &per_engine,
        run_start,
    )
    .await
}

/// Reduces and stores every engine's harvested records against a resolved context.
///
/// The shared back half of the import pipeline (announce partition → per-metric
/// reduce → per-engine store). Given `per_engine` (one inner vector per run), the
/// resolved [`SharedContext`], and the [`StoreParams`] governing placement, it is
/// used verbatim by both `collect` and `import`; they differ only in how they
/// produce `per_engine` and `shared` (and in the `operation` verb naming the
/// partition announcement). `run_start` stamps each stored run's observation time
/// and any dirty-snapshot second.
pub(crate) async fn finalize_and_store<S>(
    store: &FinalizeDeps<'_, S>,
    shared: &SharedContext,
    params: &StoreParams,
    operation: &str,
    triple_note: &str,
    per_engine: &[Vec<Vec<BenchmarkResult>>],
    run_start: SystemTime,
) -> Result<CollectSummary, AppError>
where
    S: Storage,
{
    // The always-on effective-partition announcement: one line, printed regardless
    // of `--verbose`, naming the storage partition this run's results land in (the
    // target triple, always the toolchain host, and the auto-detected machine key
    // every engine uses). It mirrors the query commands' effective-selection
    // summary so the partition a run writes and an `analyze` reads is stated the
    // same way.
    let partition = partition_of(shared);
    store.reporter.announce(&partition_selection_summary(
        operation,
        shared.target_triple.as_str(),
        triple_note,
        partition.machine_key.as_str(),
    ));
    // Under --verbose, spell out the individual hardware factors behind the
    // auto-detected fingerprint so that if the machine key changes between runs
    // the responsible factor can be identified from the log.
    store.reporter.note_with(|| {
        format!(
            "hardware fingerprint components: {}",
            describe_fingerprint_components(&shared.hardware)
        )
    });

    let mut stored = 0_usize;
    let mut harvested = 0_usize;
    let mut labels = Vec::new();

    for (bucket, engine) in per_engine.iter().zip(Engine::ALL) {
        let runs = bucket.len();
        let combined = min_per_metric(bucket)
            .map_err(|error| InconsistentRunsError::caused_by(engine.to_string(), error))?;
        note_best_of_selections(store.reporter, engine, runs, &combined.selections);

        let summary = store_engine(
            store,
            shared,
            params,
            &partition,
            engine,
            Reduction {
                records: combined.results,
                runs,
            },
            run_start,
        )
        .await?;
        if summary.stored {
            stored = stored.saturating_add(1);
        }
        harvested = harvested.saturating_add(summary.count);
        if let Some(label) = summary.label {
            labels.push(label);
        }
    }

    Ok(CollectSummary {
        stored,
        harvested,
        labels,
    })
}

/// Builds the always-on, one-line summary of a run's effective storage partition:
/// the target triple its results are keyed under and the auto-detected machine key
/// every engine uses.
///
/// `operation` is the gerund naming the action ("collecting"/"importing").
/// `triple_note` qualifies the triple in parentheses ("toolchain host" for a
/// probed run, "from --target-triple" for an import override); an empty note omits
/// the parenthetical.
///
/// A pure formatter so the wording is unit-tested without a probe; the effective
/// values are resolved by the caller.
pub(crate) fn partition_selection_summary(
    operation: &str,
    target_triple: &str,
    triple_note: &str,
    machine_key: &str,
) -> String {
    let triple = if target_triple.is_empty() {
        "unknown"
    } else {
        target_triple
    };
    let triple_suffix = if triple_note.is_empty() {
        String::new()
    } else {
        format!(" ({triple_note})")
    };
    format!(
        "{operation}: target-triple={triple}{triple_suffix}; \
         machine-key={machine_key} (auto-detected)"
    )
}

/// Builds the write-only host-hardware provenance recorded on the runs this
/// command stores: the hardware facts the probe observed and the key the
/// fingerprint factors among them hash to.
///
/// It records the auto-detected fingerprint so that a later change in a partition
/// key can be traced back to the specific hardware factor that moved.
/// The per-processor speed histogram is recorded alongside as provenance even
/// though it does not take part in the key.
fn machine_info(hardware: &HardwareProfile) -> MachineInfo {
    MachineInfo {
        processors: hardware.processors,
        memory_regions: hardware.memory_regions,
        processor_models: hardware.processor_models.clone(),
        processor_speeds: hardware.processor_speeds.clone(),
        fingerprint: resolve_machine_key(hardware),
    }
}

/// Emits the per-metric best-of provenance: the samples seen and which run won.
///
/// Only meaningful when more than one run was taken, so it is a no-op for a plain
/// single run. The chosen sample is bracketed so the reasoning behind each stored
/// value can be reconstructed from the verbose log.
fn note_best_of_selections(
    reporter: &dyn Reporter,
    engine: Engine,
    runs: usize,
    selections: &[crate::model::Selection],
) {
    if runs <= 1 {
        return;
    }
    reporter.if_enabled(|notes| {
        for selection in selections {
            let samples = selection
                .samples
                .iter()
                .enumerate()
                .map(|(index, value)| {
                    if index == selection.chosen_run {
                        format!("[{value}]")
                    } else {
                        value.to_string()
                    }
                })
                .collect::<Vec<_>>()
                .join(", ");
            notes.note(&format!(
                "{engine} {}/{}: best-of samples {samples} (kept run {})",
                selection.id,
                selection.kind.as_str(),
                selection.chosen_run.saturating_add(1),
            ));
        }
    });
}

/// Builds the benchmark command line: the base command followed by the cargo
/// scope flags translated from the run options, then any `--` passthrough.
///
/// Scope follows cargo's own conventions: with no `--package` filters the whole
/// workspace is benched (`--workspace`), optionally minus any `--exclude`d
/// packages; otherwise each requested package is passed with `--package`. Any
/// `--bench` filters are appended next, then the cargo feature-selection flags
/// (`--features`/`--all-features`/`--no-default-features`). Passthrough
/// arguments are forwarded to the benchmark binary, so they follow a `--`
/// separator that splits them from cargo's own arguments (Criterion's `--noplot`
/// or Gungraun's `--output-format`, for example, are harness flags cargo would
/// otherwise reject). Non-overlapping `--package`/`--bench` runs at one commit
/// therefore exercise disjoint benchmark cases.
fn build_bench_argv(
    bench_command: &[String],
    options: &CollectOptions,
) -> Result<Vec<String>, AppError> {
    let Some((program, _)) = bench_command.split_first() else {
        return Err(InvalidCommandError::new(
            BENCH_COMMAND_LABEL,
            "the benchmark command is empty",
        )
        .into());
    };
    debug_assert!(!program.is_empty());

    let mut argv = bench_command.to_vec();
    if options.packages.is_empty() {
        argv.push("--workspace".to_owned());
        // `--exclude` is only valid alongside `--workspace`, so it lives in this
        // branch — the CLI already rejects combining it with `--package`.
        for exclude in &options.excludes {
            argv.push("--exclude".to_owned());
            argv.push(exclude.clone());
        }
    } else {
        for package in &options.packages {
            argv.push("--package".to_owned());
            argv.push(package.clone());
        }
    }
    for bench in &options.benches {
        argv.push("--bench".to_owned());
        argv.push(bench.clone());
    }
    for features in &options.features {
        argv.push("--features".to_owned());
        argv.push(features.clone());
    }
    if options.all_features {
        argv.push("--all-features".to_owned());
    }
    if options.no_default_features {
        argv.push("--no-default-features".to_owned());
    }
    if !options.passthrough.is_empty() {
        // The CLI strips the `--` that separates passthrough from the tool's own
        // arguments, so re-insert one here to forward these flags to the benchmark
        // binary rather than letting cargo try (and fail) to interpret them.
        argv.push("--".to_owned());
        argv.extend(options.passthrough.iter().cloned());
    }
    Ok(argv)
}

/// Harvests and parses one engine's output for a single run.
///
/// The front half of storing an engine's results, split out so the `--best-of`
/// loop can gather each run's records before they are reduced together, and so
/// `import` can harvest a curated tree the same way. Producing no fresh output
/// yields an empty vector (the steady state for an engine that does not apply
/// here). `since` gates by modification time (`Some`) or admits everything
/// (`None`, which `import` passes to take a curated tree wholesale).
pub(crate) async fn harvest_records<O>(
    output: &O,
    reporter: &dyn Reporter,
    engine: Engine,
    since: Option<SystemTime>,
) -> Result<Vec<BenchmarkResult>, AppError>
where
    O: BenchOutputSource,
{
    let harvest = output
        .collect(engine, since, reporter)
        .await
        .map_err(|error| HarvestFailedError::caused_by(engine.to_string(), error))?;
    parse_harvest(&harvest, reporter)
}

/// Stores one engine's reduced result set (unless suppressed).
///
/// The back half of collecting an engine: given the [`Reduction`] to persist
/// (already reduced across the `--best-of` runs) and the [`Partition`] the run
/// writes to, it builds the run context and storage key and writes the object.
/// `run_start` is the first run's start, which stamps the observation time and any
/// dirty-snapshot second.
async fn store_engine<S>(
    store: &FinalizeDeps<'_, S>,
    shared: &SharedContext,
    params: &StoreParams,
    partition: &Partition,
    engine: Engine,
    reduction: Reduction,
    run_start: SystemTime,
) -> Result<EngineSummary, AppError>
where
    S: Storage,
{
    let Reduction { records, runs } = reduction;
    let count = records.len();

    // An engine that produced no fresh output contributes nothing. Off Linux the
    // Callgrind tree is simply absent; a `--package`/`--bench` filter may also
    // match no cases for one engine. Storing an empty result set would inflate
    // `analyze`'s run count with a series-less object, so skip it silently — there
    // is no misconfiguration to report, since absence is the expected steady state
    // for an engine that does not apply here.
    if count == 0 {
        store.reporter.note_with(|| {
            format!("{engine}: no fresh benchmark cases harvested; nothing to store")
        });
        return Ok(EngineSummary {
            stored: false,
            count: 0,
            label: None,
        });
    }

    if params.no_store {
        store.reporter.note_with(|| {
            format!(
                "{engine}: harvested {}; not storing (--no-store)",
                count_noun(count, "case")
            )
        });
        return Ok(EngineSummary {
            stored: false,
            count,
            label: Some(format!("{engine}: {count} harvested (not stored)")),
        });
    }

    let observation = timestamp_from(run_start);
    let dirty = shared.git.dirty;
    let target_triple = &shared.target_triple;

    let context = RunContext::new(
        observation,
        shared.git.clone(),
        shared.env.clone(),
        ToolchainInfo {
            target_triple: target_triple.clone(),
            rustc_version: shared.rustc.version.clone(),
        },
        store.tool_version.to_owned(),
    );
    // Record the host hardware provenance on the runs this command stores
    // (write-only): the hardware facts and the key their fingerprint factors hash
    // to, so a later change in a machine key can be traced to the specific factor
    // that moved.
    let mut context = context;
    context.machine = Some(machine_info(&shared.hardware));
    // Record the measurement protocol alongside it: how many repetitions of the
    // suite the stored minima were taken over. `NonZero::new` yields `None` only for
    // an empty reduction, which cannot reach here — no repetitions means no records,
    // and the zero-case early return above already took that path.
    context.best_of = NonZero::new(runs);
    store.reporter.note_with(|| {
        format!(
            "{engine}: recording best-of={runs} on the stored run; each value is the minimum \
             of {}, and because that minimum falls as the count rises, a later reader needs \
             the count to tell a protocol change from a code change",
            count_noun(runs, "sample")
        )
    });
    let run = Run::new(context, records);

    // Every engine partitions its history by a machine key so only equivalent
    // machines share a series; the run's partition supplies the computed hardware
    // fingerprint for every engine.
    let machine_key = &partition.machine_key;
    let key = partition.discriminant_set(engine);
    // History is organized by commit, so the full commit ID names the directory
    // (`analyze` resolves which commits to read from git topology). A clean run is
    // keyed solely by its commit and so is deterministic; a dirty snapshot adds its
    // observation time so concurrent snapshots of the same commit coexist.
    let commit = shared.git.commit.as_deref().unwrap_or("unknown");
    let object_key = if dirty {
        key.dirty_key(store.project_id, commit, observation.as_second())
    } else {
        key.clean_key(store.project_id, commit)
    };

    store.reporter.note_with(|| {
        format!(
            "{engine}: {} at commit {commit} ({}), machine {machine_key} -> {object_key}",
            count_noun(count, "case"),
            if dirty { "dirty" } else { "clean" },
        )
    });

    // A freshly built run is composed of plain structs and finite counts, so
    // serialization cannot fail.
    let json = run
        .to_json()
        .expect("a freshly built run always serializes to JSON");
    // The early `--no-store` return above is the only path that leaves storage
    // unset, so reaching here guarantees a backend was built.
    let storage = store
        .storage
        .expect("storage is built whenever a run may store results");
    let outcome = store_result(
        storage,
        &object_key,
        json.as_bytes(),
        params.overwrite,
        params.skip_existing,
    )
    .await?;

    match outcome {
        StoreOutcome::Skipped => {
            store.reporter.note_with(|| {
                format!(
                    "{engine}: {object_key} already exists; left unchanged (--skip-existing), \
                 so nothing was written and the cache-invalidation marker was not armed"
                )
            });
            Ok(EngineSummary {
                stored: false,
                count,
                label: Some(format!(
                    "{engine}: {count} harvested ({object_key} already exists)"
                )),
            })
        }
        StoreOutcome::Stored => {
            store
                .reporter
                .note_with(|| format!("{engine}: stored {object_key}"));

            Ok(EngineSummary {
                stored: true,
                count,
                label: Some(format!("{engine}: {count} stored")),
            })
        }
    }
}

/// The disposition of a single result-set store.
#[derive(Debug)]
enum StoreOutcome {
    /// The result set was written (a new object, or a replacement under
    /// `--overwrite`).
    Stored,
    /// An object already existed at the key and `--skip-existing` left it
    /// untouched. No write happened, so the cloud cache-invalidation marker was
    /// not armed.
    Skipped,
}

/// Persists a serialized result set at `object_key`.
///
/// A normal run is write-once: if an object already exists at the key (a clean
/// re-run of the same commit, or a dirty snapshot sharing an effective second),
/// the collision surfaces as [`DuplicateResultError`] so the caller can refuse it.
/// `--overwrite` replaces any existing object in place instead; `--skip-existing`
/// instead treats the existing object as a success that writes nothing — the
/// append-only mode the CI collection uses so it never overwrites an object
/// (and so never invalidates the cloud read-through cache).
async fn store_result<S: Storage>(
    storage: &S,
    object_key: &str,
    bytes: &[u8],
    overwrite: bool,
    skip_existing: bool,
) -> Result<StoreOutcome, AppError> {
    if overwrite {
        storage.put_overwrite(object_key, bytes).await?;
        return Ok(StoreOutcome::Stored);
    }
    match storage.put(object_key, bytes).await {
        Ok(()) => Ok(StoreOutcome::Stored),
        Err(error) => {
            let Some(key) = error.already_existing_key().map(ToOwned::to_owned) else {
                return Err(error.into());
            };
            if skip_existing {
                Ok(StoreOutcome::Skipped)
            } else {
                Err(DuplicateResultError::caused_by(key, error).into())
            }
        }
    }
}

/// Parses harvested engine output into result records, naming the offending
/// source on a parse failure.
///
/// The in-workspace engines can emit an operation with no usable per-iteration
/// measurement (a zero-iteration run the workload could not complete); such an
/// operation has nothing to store, so it is dropped with an explanatory note
/// rather than carried into stored history as a non-finite value.
fn parse_harvest(
    harvest: &Harvest,
    reporter: &dyn Reporter,
) -> Result<Vec<BenchmarkResult>, AppError> {
    match harvest {
        Harvest::Callgrind(summaries) => {
            let mut records = Vec::with_capacity(summaries.len());
            for summary in summaries {
                let record = parse_callgrind_summary(&summary.content)
                    .map_err(|error| ParseOutputError::caused_by(&summary.path, error))?;
                records.push(record);
            }
            Ok(records)
        }
        Harvest::Criterion(cases) => {
            let mut records = Vec::with_capacity(cases.len());
            for case in cases {
                let record = parse_criterion_case(&case.benchmark, &case.estimates)
                    .map_err(|error| ParseOutputError::caused_by(&case.dir, error))?;
                records.push(record);
            }
            Ok(records)
        }
        Harvest::AllocTracker(files) => {
            let mut records = Vec::with_capacity(files.len());
            for file in files {
                let record = parse_alloc_tracker_operation(&file.content)
                    .map_err(|error| ParseOutputError::caused_by(&file.path, error))?;
                push_or_skip(&mut records, record, file, reporter);
            }
            Ok(records)
        }
        Harvest::AllTheTime(files) => {
            let mut records = Vec::with_capacity(files.len());
            for file in files {
                let record = parse_all_the_time_operation(&file.content)
                    .map_err(|error| ParseOutputError::caused_by(&file.path, error))?;
                push_or_skip(&mut records, record, file, reporter);
            }
            Ok(records)
        }
    }
}

/// Records a parsed operation, or notes and drops it when the engine reported no
/// usable per-iteration measurement (`None`).
fn push_or_skip(
    records: &mut Vec<BenchmarkResult>,
    record: Option<BenchmarkResult>,
    file: &RawOperationFile,
    reporter: &dyn Reporter,
) {
    match record {
        Some(record) => records.push(record),
        None => reporter.note_with(|| {
            format!(
                "{}: no usable per-iteration measurement (zero-iteration operation); skipping",
                file.path.display()
            )
        }),
    }
}

/// Builds the human-readable outcome message summarizing what a run stored.
pub(crate) fn build_message(
    no_store: bool,
    stored: usize,
    harvested: usize,
    labels: &[String],
) -> String {
    let mut message = if no_store {
        format!(
            "Harvested {}; nothing stored (--no-store).",
            count_noun(harvested, "benchmark case")
        )
    } else {
        format!(
            "Stored {} covering {}.",
            count_noun(stored, "result set"),
            count_noun(harvested, "benchmark case")
        )
    };
    if !labels.is_empty() {
        message.push_str(" [");
        message.push_str(&labels.join("; "));
        message.push(']');
    }
    message
}

/// Converts a [`SystemTime`] to a [`Timestamp`].
fn timestamp_from(time: SystemTime) -> Timestamp {
    Timestamp::try_from(time).expect("system time is within the supported timestamp range")
}

/// The cargo target directory, honoring `CARGO_TARGET_DIR`, as an absolute path,
/// resolving a relative target directory against `base` (the workspace directory)
/// rather than the process working directory.
fn resolve_target_root_in(base: &Path) -> PathBuf {
    target_root_from(std::env::var_os("CARGO_TARGET_DIR"), base)
}

/// Resolves the cargo target directory from an optional `CARGO_TARGET_DIR` value,
/// falling back to the conventional `target/` directory when it is unset, and
/// makes the result absolute by joining a relative path onto `base`.
///
/// Absolutizing matters because the value is injected back as `CARGO_TARGET_DIR`
/// for the benchmark command. Cargo runs each benchmark binary with its working
/// directory set to the owning package's directory, so a relative `target/` would
/// be resolved there by an engine such as Criterion — depositing output under the
/// package instead of the workspace root the harvest scans. An absolute root keeps
/// the write and the scan pointed at the same tree regardless of that cwd.
fn target_root_from(configured: Option<std::ffi::OsString>, base: &Path) -> PathBuf {
    let configured = configured.map_or_else(|| PathBuf::from("target"), PathBuf::from);
    if configured.is_absolute() {
        configured
    } else {
        base.join(configured)
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    #![allow(clippy::indexing_slicing, reason = "panic is fine in tests")]
    #![allow(
        clippy::float_cmp,
        reason = "best-of stores exact input values, so comparisons are exact"
    )]

    use std::collections::HashMap;
    use std::future::{Future, ready};
    use std::io;
    use std::num::NonZeroUsize;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use cbh_config::parse_config;
    use cbh_diag::RecordingReporter;
    use cbh_engines::testing::{
        ALL_THE_TIME_READ_CELL as ALL_THE_TIME_FIXTURE,
        ALLOC_TRACKER_ALLOCATE_VEC as ALLOC_TRACKER_FIXTURE, CALLGRIND_MINIMAL as SINGLE_FIXTURE,
        CRITERION_MINIMAL_BENCHMARK as CRITERION_BENCHMARK_FIXTURE,
        CRITERION_MINIMAL_ESTIMATES as CRITERION_ESTIMATES_FIXTURE,
    };
    use cbh_engines::{Harvest, RawCriterionCase, RawOperationFile, RawSummary};
    use cbh_git::{EngineStatus, parse_git_info};
    use cbh_storage::{MemoryStorage, StorageError, TestStorageError};
    use futures::executor::block_on;

    use super::*;
    use crate::model::{AggregateError, BenchmarkIdPrefix, BlessingRecord};

    /// A zero-iteration `alloc_tracker` operation the workload could not run: the
    /// producer emits null slopes (a NaN per-iteration rate), which the adapter
    /// drops as carrying no usable measurement.
    const ZERO_ITERATION_ALLOC_TRACKER_FIXTURE: &str = concat!(
        "{\"operation\":\"failed_op\",\"total_iterations\":0,",
        "\"total_bytes_allocated\":0,\"total_allocations_count\":0,\"span_count\":1,",
        "\"slope_bytes_per_iteration\":null,\"slope_allocations_per_iteration\":null}"
    );

    /// The frozen wall-clock instant used by orchestration tests (2023-11-14Z).
    const FROZEN_UNIX: u64 = 1_700_000_000;

    #[test]
    fn describe_storage_names_the_backend() {
        // An explicit `--local=<path>` describes a local backend and cites the flag.
        let selection = LocalStorageSelection::Path(PathBuf::from("/tmp/history"));
        let described = describe_storage(
            Some(&selection),
            Some(Path::new("/tmp/history")),
            &Config::default(),
        );
        assert!(described.contains("local filesystem"), "{described}");
        assert!(described.contains("history"), "{described}");
        assert!(described.contains("--local"), "{described}");

        // A bare `--local` cites the environment variable it resolved from.
        let described = describe_storage(
            Some(&LocalStorageSelection::FromEnv),
            Some(Path::new("/env/history")),
            &Config::default(),
        );
        assert!(described.contains("local filesystem"), "{described}");
        assert!(described.contains(STORAGE_ENV_VAR), "{described}");

        // With no `--local`, the configured cloud backend is described.
        let config = parse_config(
            "[storage.azure]\naccount = \"devstoreaccount1\"\ncontainer = \"bench\"\n",
        )
        .unwrap();
        let described = describe_storage(None, None, &config);
        assert!(described.contains("Azure Blob"), "{described}");
        assert!(described.contains("devstoreaccount1"), "{described}");
        assert!(described.contains("bench"), "{described}");

        // With neither a selection nor a configured backend, it reports none.
        let described = describe_storage(None, None, &Config::default());
        assert!(described.contains("none"), "{described}");
    }

    #[test]
    fn default_bench_command_is_cargo_bench() {
        assert_eq!(
            default_bench_command(),
            vec!["cargo".to_owned(), "bench".to_owned()]
        );
    }

    /// A storage double whose `put` always fails with an I/O error, used to prove a
    /// backend failure during a normal run propagates rather than being swallowed.
    #[derive(Debug)]
    struct FailingStorage;

    impl Storage for FailingStorage {
        fn put(&self, _key: &str, _bytes: &[u8]) -> impl Future<Output = Result<(), StorageError>> {
            ready(Err(TestStorageError::new().into()))
        }

        fn put_overwrite(
            &self,
            _key: &str,
            _bytes: &[u8],
        ) -> impl Future<Output = Result<(), StorageError>> + Send {
            ready(Err(TestStorageError::new().into()))
        }

        fn get(&self, _key: &str) -> impl Future<Output = Result<Vec<u8>, StorageError>> + Send {
            ready(Err(TestStorageError::new().into()))
        }

        fn list(&self, _prefix: &str) -> impl Future<Output = Result<Vec<String>, StorageError>> {
            ready(Ok(Vec::new()))
        }

        fn delete(&self, _key: &str) -> impl Future<Output = Result<(), StorageError>> {
            ready(Err(TestStorageError::new().into()))
        }
    }

    #[test]
    fn store_result_propagates_a_non_collision_storage_error() {
        // A backend I/O failure (anything other than AlreadyExists) surfaces as a
        // storage error, not as a Duplicate.
        let error = block_on(store_result(
            &FailingStorage,
            "v1/p/e/t/m/c/clean.json",
            b"{}",
            false,
            false,
        ))
        .unwrap_err();
        assert!(error.find_source::<StorageError>().is_some());
        assert!(error.find_source::<TestStorageError>().is_some());
        assert!(error.find_source::<DuplicateResultError>().is_none());
    }

    fn frozen_time() -> SystemTime {
        SystemTime::UNIX_EPOCH
            .checked_add(Duration::from_secs(FROZEN_UNIX))
            .unwrap()
    }

    #[test]
    fn target_root_falls_back_to_the_conventional_directory() {
        let base = if cfg!(windows) {
            Path::new(r"C:\work")
        } else {
            Path::new("/work")
        };
        let resolved = target_root_from(None, base);
        assert_eq!(resolved, base.join("target"));
        assert!(resolved.is_absolute());
    }

    #[test]
    fn target_root_resolves_a_relative_cargo_target_dir_against_the_base() {
        let base = if cfg!(windows) {
            Path::new(r"C:\work")
        } else {
            Path::new("/work")
        };
        let resolved = target_root_from(Some(std::ffi::OsString::from("out")), base);
        assert_eq!(resolved, base.join("out"));
        assert!(resolved.is_absolute());
    }

    #[test]
    fn target_root_honors_an_explicit_cargo_target_dir() {
        let absolute = if cfg!(windows) {
            r"C:\custom\out"
        } else {
            "/custom/out"
        };
        let configured = std::ffi::OsString::from(absolute);
        assert_eq!(
            target_root_from(Some(configured), Path::new("ignored-base")),
            PathBuf::from(absolute)
        );
    }

    #[test]
    #[cfg_attr(miri, ignore)] // Reads the process working directory, which Miri cannot access.
    fn resolve_target_root_matches_the_ambient_environment() {
        // Resolving never panics, yields an absolute path, and agrees with
        // `target_root_from` for whatever `CARGO_TARGET_DIR` happens to be set
        // (or unset) in this process.
        let base = std::env::current_dir().unwrap();
        let resolved = resolve_target_root_in(&base);
        assert_eq!(
            resolved,
            target_root_from(std::env::var_os("CARGO_TARGET_DIR"), &base)
        );
        assert!(resolved.is_absolute());
    }

    #[test]
    fn timestamp_from_preserves_the_instant() {
        let timestamp = timestamp_from(frozen_time());
        let expected = Timestamp::from_second(i64::try_from(FROZEN_UNIX).unwrap()).unwrap();
        assert_eq!(timestamp, expected);
    }

    #[test]
    fn partition_selection_summary_names_the_effective_partition() {
        // Auto-detected fingerprint: the machine key is marked auto-detected and the
        // triple is named as the toolchain host.
        let auto = partition_selection_summary(
            "collecting",
            "x86_64-pc-windows-msvc",
            "toolchain host",
            "abcd1234",
        );
        assert!(auto.starts_with("collecting: "), "{auto}");
        assert!(
            auto.contains("target-triple=x86_64-pc-windows-msvc (toolchain host)"),
            "{auto}"
        );
        assert!(
            auto.contains("machine-key=abcd1234 (auto-detected)"),
            "{auto}"
        );

        // A missing host triple degrades to a readable placeholder rather than blank.
        let unknown = partition_selection_summary("collecting", "", "toolchain host", "abcd1234");
        assert!(unknown.contains("target-triple=unknown"), "{unknown}");

        // `import` reuses the formatter with its own verb and triple note, and an
        // empty note drops the parenthetical entirely.
        let imported = partition_selection_summary(
            "importing",
            "wasm32-unknown-unknown",
            "from --target-triple",
            "abcd1234",
        );
        assert!(imported.starts_with("importing: "), "{imported}");
        assert!(
            imported.contains("target-triple=wasm32-unknown-unknown (from --target-triple)"),
            "{imported}"
        );
        let no_note = partition_selection_summary("importing", "wasm32-unknown-unknown", "", "k");
        assert!(
            no_note.contains("target-triple=wasm32-unknown-unknown;"),
            "{no_note}"
        );
    }

    #[test]
    fn build_message_brackets_labels_only_when_present() {
        let labels = vec!["callgrind: 1 stored".to_owned()];
        let with_labels = build_message(false, 1, 1, &labels);
        assert!(
            with_labels.contains("[callgrind: 1 stored]"),
            "{with_labels}"
        );

        let without_labels = build_message(false, 0, 0, &[]);
        assert!(!without_labels.contains('['), "{without_labels}");
    }

    #[test]
    fn build_message_reports_no_store() {
        let no_store = build_message(true, 0, 3, &[]);
        assert!(
            no_store.contains("nothing stored (--no-store)"),
            "{no_store}"
        );
    }

    #[test]
    fn build_message_pluralizes_counts_correctly() {
        let singular = build_message(false, 1, 1, &[]);
        assert!(
            singular.contains("Stored 1 result set covering 1 benchmark case."),
            "{singular}"
        );

        let plural = build_message(false, 2, 3, &[]);
        assert!(
            plural.contains("Stored 2 result sets covering 3 benchmark cases."),
            "{plural}"
        );
    }

    /// One engine invocation's injected environment, recorded by [`FakeRunner`].
    type RecordedEnv = Vec<(String, String)>;

    #[derive(Clone)]
    struct FakeRunner {
        status: EngineStatus,
        failure: Option<io::ErrorKind>,
        calls: Arc<Mutex<Vec<Vec<String>>>>,
        envs: Arc<Mutex<Vec<RecordedEnv>>>,
    }

    impl FakeRunner {
        fn succeeding() -> Self {
            Self {
                status: EngineStatus {
                    success: true,
                    code: Some(0),
                },
                failure: None,
                calls: Arc::default(),
                envs: Arc::default(),
            }
        }

        fn failing(code: i32) -> Self {
            Self {
                status: EngineStatus {
                    success: false,
                    code: Some(code),
                },
                failure: None,
                calls: Arc::default(),
                envs: Arc::default(),
            }
        }

        /// A run that died without reporting an exit code, as a process killed by a
        /// signal does.
        fn terminated() -> Self {
            Self {
                status: EngineStatus {
                    success: false,
                    code: None,
                },
                failure: None,
                calls: Arc::default(),
                envs: Arc::default(),
            }
        }

        fn io_failing() -> Self {
            Self {
                status: EngineStatus {
                    success: false,
                    code: None,
                },
                failure: Some(io::ErrorKind::Other),
                calls: Arc::default(),
                envs: Arc::default(),
            }
        }

        fn last_command(&self) -> Option<Vec<String>> {
            self.calls.lock().unwrap().last().cloned()
        }

        fn last_env(&self) -> Option<Vec<(String, String)>> {
            self.envs.lock().unwrap().last().cloned()
        }
    }

    impl BenchRunner for FakeRunner {
        fn run_benches(
            &self,
            argv: &[String],
            env: &[(String, String)],
        ) -> impl Future<Output = io::Result<EngineStatus>> {
            self.calls.lock().unwrap().push(argv.to_vec());
            self.envs.lock().unwrap().push(env.to_vec());
            ready(
                self.failure
                    .map_or(Ok(self.status), |kind| Err(io::Error::from(kind))),
            )
        }
    }

    #[derive(Clone)]
    struct FakeProbe {
        git: GitInfo,
        rustc: RustcInfo,
        hardware: HardwareProfile,
        git_failure: Option<io::ErrorKind>,
        toolchain_failure: Option<io::ErrorKind>,
    }

    impl FakeProbe {
        fn new() -> Self {
            Self::with_status("")
        }

        /// A probe whose git snapshot reports a dirty working tree.
        fn dirty() -> Self {
            Self::with_status(" M src/lib.rs")
        }

        fn with_status(status: &str) -> Self {
            Self {
                git: parse_git_info("deadbeefdeadbeefdeadbeefdeadbeefdeadbeef", "main", status),
                rustc: RustcInfo {
                    version: Some("1.91.0".to_owned()),
                    host: Some("x86_64-pc-windows-msvc".to_owned()),
                },
                hardware: HardwareProfile {
                    processors: 8,
                    memory_regions: 1,
                    processor_models: vec!["Test CPU 3000".to_owned()],
                    processor_speeds: vec![(3141, 8)],
                },
                git_failure: None,
                toolchain_failure: None,
            }
        }

        fn with_git_failure(mut self) -> Self {
            self.git_failure = Some(io::ErrorKind::Other);
            self
        }

        fn with_toolchain_failure(mut self) -> Self {
            self.toolchain_failure = Some(io::ErrorKind::Other);
            self
        }
    }

    impl EnvironmentProbe for FakeProbe {
        fn git(&self) -> impl Future<Output = io::Result<GitInfo>> {
            ready(
                self.git_failure
                    .map_or_else(|| Ok(self.git.clone()), |kind| Err(io::Error::from(kind))),
            )
        }

        fn toolchain(&self) -> impl Future<Output = io::Result<RustcInfo>> {
            ready(
                self.toolchain_failure
                    .map_or_else(|| Ok(self.rustc.clone()), |kind| Err(io::Error::from(kind))),
            )
        }

        fn hardware(&self) -> impl Future<Output = HardwareProfile> {
            ready(self.hardware.clone())
        }
    }

    /// The auto-detected machine key every engine partitions under for the
    /// [`FakeProbe`] hardware profile. Every engine is machine-keyed, so a probed
    /// run lands under this fingerprint.
    fn probe_machine_key() -> String {
        resolve_machine_key(&FakeProbe::new().hardware)
    }

    /// Establishes an existing object without rerunning collection as test setup.
    ///
    /// The literal key independently checks collection's identity mapping. The
    /// seed's tool version differs from collection's so replacement is observable.
    fn seed_callgrind_run(storage: &MemoryStorage, dirty: bool, now_unix: u64) -> String {
        let mut probe = FakeProbe::new();
        probe.git.dirty = dirty;
        let context = RunContext::new(
            Timestamp::from_second(i64::try_from(now_unix).unwrap()).unwrap(),
            probe.git,
            EnvironmentInfo::default(),
            ToolchainInfo {
                target_triple: TargetTriple::from("x86_64-pc-windows-msvc"),
                rustc_version: probe.rustc.version,
            },
            "0.0.0".to_owned(),
        );
        let record = parse_callgrind_summary(SINGLE_FIXTURE).unwrap();
        // The extra case must disappear on overwrite, so these tests distinguish
        // replacement from merging old records with the new harvest.
        let mut stale_record = record.clone();
        stale_record.id.segments.push("stale".to_owned());
        let run = Run::new(context, vec![record, stale_record]);
        let machine = probe_machine_key();
        let snapshot = if dirty {
            format!("dirty-{now_unix}")
        } else {
            "clean".to_owned()
        };
        let key = format!(
            "v1/folo/objects/callgrind/x86_64-pc-windows-msvc/{machine}/\
             deadbeefdeadbeefdeadbeefdeadbeefdeadbeef/{snapshot}.json"
        );
        block_on(storage.put(&key, run.to_json().unwrap().as_bytes())).unwrap();
        key
    }

    #[derive(Clone, Default)]
    struct FakeOutput {
        callgrind: Vec<RawSummary>,
        criterion: Vec<RawCriterionCase>,
        alloc: Vec<RawOperationFile>,
        time: Vec<RawOperationFile>,
        failure: Option<Engine>,
    }

    impl FakeOutput {
        fn with_callgrind_summary() -> Self {
            Self {
                callgrind: vec![RawSummary {
                    path: PathBuf::from("a/summary.json"),
                    content: SINGLE_FIXTURE.to_owned(),
                }],
                ..Self::default()
            }
        }

        fn with_two_callgrind_summaries() -> Self {
            Self {
                callgrind: vec![
                    RawSummary {
                        path: PathBuf::from("a/summary.json"),
                        content: SINGLE_FIXTURE.to_owned(),
                    },
                    RawSummary {
                        path: PathBuf::from("b/summary.json"),
                        // A distinct parameter preserves the multi-record scenario
                        // without parsing a second full producer schema canary.
                        content: SINGLE_FIXTURE.replace(r#""id":null"#, r#""id":"other""#),
                    },
                ],
                ..Self::default()
            }
        }

        fn with_malformed_summary() -> Self {
            Self {
                callgrind: vec![RawSummary {
                    path: PathBuf::from("a/summary.json"),
                    content: "{ not valid json".to_owned(),
                }],
                ..Self::default()
            }
        }

        fn with_criterion_case() -> Self {
            Self {
                criterion: vec![RawCriterionCase {
                    dir: PathBuf::from("criterion/grp/std/now/new"),
                    benchmark: CRITERION_BENCHMARK_FIXTURE.to_owned(),
                    estimates: CRITERION_ESTIMATES_FIXTURE.to_owned(),
                }],
                ..Self::default()
            }
        }

        fn with_callgrind_and_criterion() -> Self {
            let mut output = Self::with_callgrind_summary();
            output.criterion = Self::with_criterion_case().criterion;
            output
        }

        fn with_malformed_criterion_case() -> Self {
            Self {
                criterion: vec![RawCriterionCase {
                    dir: PathBuf::from("criterion/grp/std/now/new"),
                    benchmark: "{ not valid json".to_owned(),
                    estimates: CRITERION_ESTIMATES_FIXTURE.to_owned(),
                }],
                ..Self::default()
            }
        }

        fn with_alloc_tracker_operation() -> Self {
            Self {
                alloc: vec![RawOperationFile {
                    path: PathBuf::from("alloc_tracker/allocate_vec.json"),
                    content: ALLOC_TRACKER_FIXTURE.to_owned(),
                }],
                ..Self::default()
            }
        }

        /// A measured operation alongside a zero-iteration one, to prove the
        /// unmeasured operation is dropped while the measured one is still stored.
        fn with_measured_and_zero_iteration_alloc_tracker_operations() -> Self {
            Self {
                alloc: vec![
                    RawOperationFile {
                        path: PathBuf::from("alloc_tracker/allocate_vec.json"),
                        content: ALLOC_TRACKER_FIXTURE.to_owned(),
                    },
                    RawOperationFile {
                        path: PathBuf::from("alloc_tracker/failed_op.json"),
                        content: ZERO_ITERATION_ALLOC_TRACKER_FIXTURE.to_owned(),
                    },
                ],
                ..Self::default()
            }
        }

        fn with_all_the_time_operation() -> Self {
            Self {
                time: vec![RawOperationFile {
                    path: PathBuf::from("all_the_time/read_cell.json"),
                    content: ALL_THE_TIME_FIXTURE.to_owned(),
                }],
                ..Self::default()
            }
        }

        fn with_malformed_alloc_tracker_operation() -> Self {
            Self {
                alloc: vec![RawOperationFile {
                    path: PathBuf::from("alloc_tracker/allocate_vec.json"),
                    content: "{ not valid json".to_owned(),
                }],
                ..Self::default()
            }
        }

        fn with_malformed_all_the_time_operation() -> Self {
            Self {
                time: vec![RawOperationFile {
                    path: PathBuf::from("all_the_time/read_cell.json"),
                    content: "{ not valid json".to_owned(),
                }],
                ..Self::default()
            }
        }

        fn failing(engine: Engine) -> Self {
            Self {
                failure: Some(engine),
                ..Self::default()
            }
        }
    }

    impl BenchOutputSource for FakeOutput {
        fn collect(
            &self,
            engine: Engine,
            _since: Option<SystemTime>,
            _reporter: &dyn Reporter,
        ) -> impl Future<Output = io::Result<Harvest>> {
            ready(if self.failure == Some(engine) {
                Err(io::Error::other("injected harvest failure"))
            } else {
                Ok(match engine {
                    Engine::Callgrind => Harvest::Callgrind(self.callgrind.clone()),
                    Engine::Criterion => Harvest::Criterion(self.criterion.clone()),
                    Engine::AllocTracker => Harvest::AllocTracker(self.alloc.clone()),
                    Engine::AllTheTime => Harvest::AllTheTime(self.time.clone()),
                })
            })
        }
    }

    /// The benchmark program tests pretend to run. The [`FakeRunner`] records the
    /// argv and never executes anything, so the value only needs to be non-empty.
    fn mock_bench_command() -> Vec<String> {
        vec!["mock".to_owned()]
    }

    fn drive(
        options: &CollectOptions,
        runner: &FakeRunner,
        probe: &FakeProbe,
        output: &FakeOutput,
        storage: &MemoryStorage,
    ) -> Result<RunOutcome, AppError> {
        drive_at(FROZEN_UNIX, options, runner, probe, output, storage)
    }

    fn drive_at(
        now_unix: u64,
        options: &CollectOptions,
        runner: &FakeRunner,
        probe: &FakeProbe,
        output: &FakeOutput,
        storage: &MemoryStorage,
    ) -> Result<RunOutcome, AppError> {
        // Tests asserting verbose output inject a RecordingReporter explicitly.
        let reporter = RecordingReporter::quiet();
        drive_at_with(now_unix, options, runner, probe, output, storage, &reporter)
    }

    fn drive_at_with(
        now_unix: u64,
        options: &CollectOptions,
        runner: &FakeRunner,
        probe: &FakeProbe,
        output: &FakeOutput,
        storage: &MemoryStorage,
        reporter: &dyn Reporter,
    ) -> Result<RunOutcome, AppError> {
        let now = SystemTime::UNIX_EPOCH
            .checked_add(Duration::from_secs(now_unix))
            .unwrap();
        let clock = Clock::new_frozen_at(now);
        let env = |_name: &str| None::<String>;
        let bench_command = mock_bench_command();
        let deps = CollectDeps {
            runner,
            probe,
            output,
            storage: Some(storage),
            clock: &clock,
            env: &env,
            project_id: "folo",
            tool_version: "0.0.1",
            target_root: Path::new("target"),
            bench_command: &bench_command,
            reporter,
        };
        block_on(execute_collect(options, &deps))
    }

    #[test]
    fn verbose_collect_notes_the_command_and_stored_key() {
        let runner = FakeRunner::succeeding();
        let probe = FakeProbe::new();
        let output = FakeOutput::with_callgrind_summary();
        let storage = MemoryStorage::new();
        let reporter = RecordingReporter::new();

        drive_at_with(
            FROZEN_UNIX,
            &CollectOptions::default(),
            &runner,
            &probe,
            &output,
            &storage,
            &reporter,
        )
        .unwrap();

        assert!(
            reporter.contains("running benchmark command: mock"),
            "expected an argv note, got {:?}",
            reporter.notes()
        );
        assert!(
            reporter.contains("GUNGRAUN_SAVE_SUMMARY"),
            "expected the injected-env note, got {:?}",
            reporter.notes()
        );
        assert!(
            reporter.contains("stored v1/folo/objects/callgrind/"),
            "expected a stored-key note, got {:?}",
            reporter.notes()
        );
    }

    #[test]
    fn collect_announces_the_effective_storage_partition() {
        let runner = FakeRunner::succeeding();
        let probe = FakeProbe::new();
        let output = FakeOutput::with_callgrind_summary();
        let storage = MemoryStorage::new();
        let reporter = RecordingReporter::new();

        drive_at_with(
            FROZEN_UNIX,
            &CollectOptions::default(),
            &runner,
            &probe,
            &output,
            &storage,
            &reporter,
        )
        .unwrap();

        // The always-on announcement names the probed host triple and the
        // auto-detected machine key, so a collect run reports where its results
        // land even without `--verbose`.
        assert!(
            reporter.announced("target-triple=x86_64-pc-windows-msvc (toolchain host)"),
            "expected the effective-partition announcement, got {:?}",
            reporter.announcements()
        );
        assert!(
            reporter.announced("(auto-detected)"),
            "machine key should read as auto-detected, got {:?}",
            reporter.announcements()
        );
    }

    #[test]
    fn verbose_collect_notes_an_empty_harvest() {
        let runner = FakeRunner::succeeding();
        let probe = FakeProbe::new();
        let output = FakeOutput::default();
        let storage = MemoryStorage::new();
        let reporter = RecordingReporter::new();

        drive_at_with(
            FROZEN_UNIX,
            &CollectOptions::default(),
            &runner,
            &probe,
            &output,
            &storage,
            &reporter,
        )
        .unwrap();

        assert!(
            reporter.contains("no fresh benchmark cases harvested"),
            "expected an empty-harvest note, got {:?}",
            reporter.notes()
        );
        assert!(storage.keys().is_empty());
    }

    #[test]
    fn happy_path_stores_one_set_with_all_records() {
        let runner = FakeRunner::succeeding();
        let probe = FakeProbe::new();
        let output = FakeOutput::with_two_callgrind_summaries();
        let storage = MemoryStorage::new();

        let outcome = drive(
            &CollectOptions::default(),
            &runner,
            &probe,
            &output,
            &storage,
        )
        .unwrap();

        let RunOutcome::Completed { message } = outcome else {
            panic!("expected completion");
        };
        assert!(message.contains("Stored 1"), "{message}");

        let keys = storage.keys();
        let machine = probe_machine_key();
        assert_eq!(
            keys,
            vec![format!(
                "v1/folo/objects/callgrind/x86_64-pc-windows-msvc/{machine}/\
                 deadbeefdeadbeefdeadbeefdeadbeefdeadbeef/clean.json"
            )]
        );

        let bytes = block_on(storage.get(&keys[0])).unwrap();
        let set = Run::from_json(&String::from_utf8(bytes).unwrap()).unwrap();
        assert_eq!(set.schema_version, crate::SCHEMA_VERSION);
        assert_eq!(set.results.len(), 2);
        assert_eq!(
            set.context.toolchain.target_triple,
            "x86_64-pc-windows-msvc"
        );
    }

    #[test]
    fn probed_partition_is_the_partition_that_is_written_to() {
        // The backfill pre-check asks `probe_partition` which partition a run here
        // would occupy and then scans exactly that. If the two ever diverged, a
        // backfill would scan one partition and write to another, so every commit
        // would look unrecorded and be benchmarked again.
        let storage = MemoryStorage::new();

        drive(
            &CollectOptions::default(),
            &FakeRunner::succeeding(),
            &FakeProbe::new(),
            &FakeOutput::with_callgrind_summary(),
            &storage,
        )
        .unwrap();

        let env = |_name: &str| None::<String>;
        let partition = block_on(probe_partition(&FakeProbe::new(), &env)).unwrap();
        let prefix = partition
            .discriminant_set(Engine::Callgrind)
            .partition_prefix("folo");

        let keys = storage.keys();
        assert!(
            keys.iter().all(|key| key.starts_with(&prefix)),
            "scanned {prefix} but wrote {keys:?}"
        );
        assert!(!keys.is_empty(), "stored nothing");
    }

    #[test]
    fn clean_re_run_of_the_same_commit_is_refused_as_a_duplicate() {
        let storage = MemoryStorage::new();
        let key = seed_callgrind_run(&storage, false, FROZEN_UNIX);
        let original = block_on(storage.get(&key)).unwrap();

        let error = drive(
            &CollectOptions::default(),
            &FakeRunner::succeeding(),
            &FakeProbe::new(),
            &FakeOutput::with_callgrind_summary(),
            &storage,
        )
        .unwrap_err();

        let duplicate = error.find_source::<DuplicateResultError>().unwrap();
        assert!(duplicate.key().ends_with("/clean.json"));
        assert!(error.find_source::<StorageError>().is_some());
        // The second run left the single stored object untouched.
        assert_eq!(storage.keys().len(), 1);
        assert_eq!(block_on(storage.get(&key)).unwrap(), original);
    }

    #[test]
    fn skip_existing_treats_a_same_commit_re_run_as_a_no_op_success() {
        let storage = MemoryStorage::new();
        let key = seed_callgrind_run(&storage, false, FROZEN_UNIX);
        let original = block_on(storage.get(&key)).unwrap();

        // A re-run of the same commit under --skip-existing succeeds without a
        // duplicate error and leaves the stored object byte-for-byte unchanged
        // (so the cache-invalidation marker is never armed).
        let skip = CollectOptions {
            skip_existing: true,
            ..CollectOptions::default()
        };
        drive(
            &skip,
            &FakeRunner::succeeding(),
            &FakeProbe::new(),
            &FakeOutput::with_callgrind_summary(),
            &storage,
        )
        .unwrap();

        let keys = storage.keys();
        assert_eq!(keys.len(), 1, "no new object is written: {keys:?}");
        let after = block_on(storage.get(&keys[0])).unwrap();
        assert_eq!(after, original, "the existing object is left untouched");
    }

    #[test]
    fn overwrite_replaces_a_clean_result_in_place() {
        let storage = MemoryStorage::new();
        let key = seed_callgrind_run(&storage, false, FROZEN_UNIX);
        let original = block_on(storage.get(&key)).unwrap();

        let overwrite = CollectOptions {
            overwrite: true,
            ..CollectOptions::default()
        };
        drive(
            &overwrite,
            &FakeRunner::succeeding(),
            &FakeProbe::new(),
            &FakeOutput::with_callgrind_summary(),
            &storage,
        )
        .unwrap();

        let keys = storage.keys();
        assert_eq!(keys.len(), 1, "{keys:?}");
        assert!(keys[0].ends_with("/clean.json"), "{keys:?}");
        let bytes = block_on(storage.get(&keys[0])).unwrap();
        assert_ne!(bytes, original, "overwrite should replace the contents");
        let set = Run::from_json(&String::from_utf8(bytes).unwrap()).unwrap();
        assert_eq!(set.results.len(), 1);
    }

    #[test]
    fn overwriting_a_clean_run_keeps_blessing_sidecars() {
        let storage = MemoryStorage::new();
        _ = seed_callgrind_run(&storage, false, FROZEN_UNIX);
        let machine = probe_machine_key();
        let commit_dir = format!(
            "v1/folo/objects/callgrind/x86_64-pc-windows-msvc/{machine}/\
             deadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
        );
        let bless_key = format!("{commit_dir}/bless-100.json");
        let record = BlessingRecord::new(
            "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef".to_owned(),
            Timestamp::from_second(100).unwrap(),
            vec![BenchmarkIdPrefix::new("group").unwrap()],
            "0.0.1".to_owned(),
        );
        block_on(storage.put(&bless_key, record.to_json().unwrap().as_bytes())).unwrap();
        assert!(storage.keys().iter().any(|key| key == &bless_key));

        // Capturing a run never removes a blessing — even an overwrite that replaces
        // the clean run leaves the sidecar in place. A blessing is removed only by the
        // `unbless` command, so a pre-emptive blessing (applied before the data lands)
        // survives the capture that gives it something to baseline.
        let overwrite = CollectOptions {
            overwrite: true,
            ..CollectOptions::default()
        };
        drive(
            &overwrite,
            &FakeRunner::succeeding(),
            &FakeProbe::new(),
            &FakeOutput::with_callgrind_summary(),
            &storage,
        )
        .unwrap();

        let keys = storage.keys();
        assert!(
            keys.iter().any(|key| key == &bless_key),
            "the blessing survives the capture: {keys:?}"
        );
        assert!(
            keys.iter().any(|key| key.ends_with("/clean.json")),
            "the clean run survives: {keys:?}"
        );
    }

    #[test]
    fn dirty_run_is_keyed_by_observation_time() {
        let storage = MemoryStorage::new();
        drive(
            &CollectOptions::default(),
            &FakeRunner::succeeding(),
            &FakeProbe::dirty(),
            &FakeOutput::with_callgrind_summary(),
            &storage,
        )
        .unwrap();

        let keys = storage.keys();
        assert_eq!(keys.len(), 1, "{keys:?}");
        assert!(
            keys[0].ends_with(&format!(
                "/deadbeefdeadbeefdeadbeefdeadbeefdeadbeef/dirty-{FROZEN_UNIX}.json"
            )),
            "{keys:?}"
        );
    }

    #[test]
    fn overwriting_a_dirty_snapshot_keeps_blessing_sidecars() {
        let storage = MemoryStorage::new();
        // Capturing never removes a blessing, so a blessing sidecar on the commit
        // survives when a dirty snapshot of that commit is rewritten, just as it does
        // for a clean run. Blessings are removed only by the `unbless` command.
        let machine = probe_machine_key();
        let commit_dir = format!(
            "v1/folo/objects/callgrind/x86_64-pc-windows-msvc/{machine}/\
             deadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
        );
        let bless_key = format!("{commit_dir}/bless-100.json");
        let record = BlessingRecord::new(
            "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef".to_owned(),
            Timestamp::from_second(100).unwrap(),
            vec![BenchmarkIdPrefix::new("group").unwrap()],
            "0.0.1".to_owned(),
        );
        block_on(storage.put(&bless_key, record.to_json().unwrap().as_bytes())).unwrap();

        let overwrite = CollectOptions {
            overwrite: true,
            ..CollectOptions::default()
        };
        drive(
            &overwrite,
            &FakeRunner::succeeding(),
            &FakeProbe::dirty(),
            &FakeOutput::with_callgrind_summary(),
            &storage,
        )
        .unwrap();

        let keys = storage.keys();
        assert!(
            keys.iter().any(|key| key == &bless_key),
            "a dirty overwrite must leave blessings in place: {keys:?}"
        );
        assert!(
            keys.iter()
                .any(|key| key.ends_with(&format!("/dirty-{FROZEN_UNIX}.json"))),
            "the dirty snapshot is stored: {keys:?}"
        );
    }

    #[test]
    fn two_dirty_runs_at_different_times_coexist() {
        let storage = MemoryStorage::new();
        let original_key = seed_callgrind_run(&storage, true, FROZEN_UNIX);
        drive_at(
            FROZEN_UNIX + 1,
            &CollectOptions::default(),
            &FakeRunner::succeeding(),
            &FakeProbe::dirty(),
            &FakeOutput::with_callgrind_summary(),
            &storage,
        )
        .unwrap();

        let keys = storage.keys();
        assert_eq!(keys.len(), 2, "{keys:?}");
        assert!(keys.contains(&original_key), "{keys:?}");
    }

    #[test]
    fn dirty_runs_sharing_an_effective_second_collide() {
        // Two dirty snapshots taken at the same effective second map to the same
        // key, so the second is refused as a duplicate just like a clean re-run.
        let storage = MemoryStorage::new();
        let key = seed_callgrind_run(&storage, true, FROZEN_UNIX);
        let original = block_on(storage.get(&key)).unwrap();

        let error = drive_at(
            FROZEN_UNIX,
            &CollectOptions::default(),
            &FakeRunner::succeeding(),
            &FakeProbe::dirty(),
            &FakeOutput::with_callgrind_summary(),
            &storage,
        )
        .unwrap_err();

        assert!(error.find_source::<DuplicateResultError>().is_some());
        assert_eq!(storage.keys().len(), 1);
        assert_eq!(block_on(storage.get(&key)).unwrap(), original);
    }

    #[test]
    fn overwrite_replaces_a_dirty_snapshot_in_place() {
        let storage = MemoryStorage::new();
        let key = seed_callgrind_run(&storage, true, FROZEN_UNIX);
        let original = block_on(storage.get(&key)).unwrap();

        // The new run differs from the seed, proving it was not merely skipped.
        let overwrite = CollectOptions {
            overwrite: true,
            ..CollectOptions::default()
        };
        drive_at(
            FROZEN_UNIX,
            &overwrite,
            &FakeRunner::succeeding(),
            &FakeProbe::dirty(),
            &FakeOutput::with_callgrind_summary(),
            &storage,
        )
        .unwrap();
        assert_eq!(storage.keys().len(), 1);
        assert_ne!(block_on(storage.get(&key)).unwrap(), original);
        assert_eq!(only_stored_run(&storage).results.len(), 1);
    }

    #[test]
    fn no_store_harvests_without_writing() {
        let options = CollectOptions {
            no_store: true,
            ..CollectOptions::default()
        };
        let storage = MemoryStorage::new();

        let outcome = drive(
            &options,
            &FakeRunner::succeeding(),
            &FakeProbe::new(),
            &FakeOutput::with_callgrind_summary(),
            &storage,
        )
        .unwrap();

        let RunOutcome::Completed { message } = outcome else {
            panic!("expected completion");
        };
        assert!(message.contains("nothing stored"), "{message}");
        assert!(storage.keys().is_empty());
    }

    #[test]
    fn empty_harvest_stores_nothing_and_reports_it() {
        // A benchmark command that exits cleanly but produces no fresh output (for
        // example a `--bench` filter that matched nothing, or an engine that does
        // not apply on this OS) must not store an empty result set, which would
        // otherwise inflate `analyze`'s run count.
        let storage = MemoryStorage::new();
        let outcome = drive(
            &CollectOptions::default(),
            &FakeRunner::succeeding(),
            &FakeProbe::new(),
            &FakeOutput::default(),
            &storage,
        )
        .unwrap();

        let RunOutcome::Completed { message } = outcome else {
            panic!("expected completion");
        };
        assert!(message.contains("Stored 0 result sets"), "{message}");
        // An engine that produced nothing is absent from the summary rather than
        // flagged, because absence is the expected steady state off its OS.
        assert!(!message.contains('['), "{message}");
        assert!(storage.keys().is_empty(), "{:?}", storage.keys());
    }

    #[test]
    fn empty_harvest_for_one_engine_does_not_block_the_other() {
        // With only Criterion producing output, the empty Callgrind harvest is
        // skipped while Criterion is still stored.
        let storage = MemoryStorage::new();
        let outcome = drive(
            &CollectOptions::default(),
            &FakeRunner::succeeding(),
            &FakeProbe::new(),
            &FakeOutput::with_criterion_case(),
            &storage,
        )
        .unwrap();

        let RunOutcome::Completed { message } = outcome else {
            panic!("expected completion");
        };
        assert!(
            message.contains("Stored 1 result set covering"),
            "{message}"
        );

        let keys = storage.keys();
        assert_eq!(keys.len(), 1, "{keys:?}");
        assert!(keys[0].contains("/criterion/"), "{keys:?}");
    }

    #[test]
    fn non_zero_engine_exit_is_an_error() {
        let storage = MemoryStorage::new();
        let error = drive(
            &CollectOptions::default(),
            &FakeRunner::failing(101),
            &FakeProbe::new(),
            &FakeOutput::with_callgrind_summary(),
            &storage,
        )
        .unwrap_err();

        let failure = error.find_source::<EngineFailedError>().unwrap();
        assert_eq!(failure.engine(), "cargo bench");
        assert_eq!(failure.code(), 101);
        assert!(storage.keys().is_empty());
    }

    #[test]
    fn engine_termination_without_an_exit_code_is_an_error() {
        // A process killed by a signal reports failure with no exit code to name, so
        // it is a distinct failure from a non-zero exit rather than a missing code.
        let storage = MemoryStorage::new();
        let error = drive(
            &CollectOptions::default(),
            &FakeRunner::terminated(),
            &FakeProbe::new(),
            &FakeOutput::with_callgrind_summary(),
            &storage,
        )
        .unwrap_err();

        let terminated = error.find_source::<EngineTerminatedError>().unwrap();
        assert_eq!(terminated.engine(), "cargo bench");
        assert!(error.find_source::<EngineFailedError>().is_none());
        assert!(storage.keys().is_empty());
    }

    #[test]
    fn benchmark_command_io_failure_is_mapped_at_the_call_site() {
        let storage = MemoryStorage::new();
        let error = drive(
            &CollectOptions::default(),
            &FakeRunner::io_failing(),
            &FakeProbe::new(),
            &FakeOutput::default(),
            &storage,
        )
        .unwrap_err();

        assert!(error.find_source::<BenchCommandFailedError>().is_some());
        assert_eq!(
            error.find_source::<io::Error>().unwrap().kind(),
            io::ErrorKind::Other
        );
        assert!(storage.keys().is_empty());
    }

    #[test]
    fn toolchain_probe_failure_is_mapped_at_the_call_site() {
        let storage = MemoryStorage::new();
        let error = drive(
            &CollectOptions::default(),
            &FakeRunner::succeeding(),
            &FakeProbe::new().with_toolchain_failure(),
            &FakeOutput::default(),
            &storage,
        )
        .unwrap_err();

        assert!(error.find_source::<ToolchainProbeFailedError>().is_some());
        assert!(error.find_source::<GitProbeFailedError>().is_none());
        assert_eq!(
            error.find_source::<io::Error>().unwrap().kind(),
            io::ErrorKind::Other
        );
        assert!(storage.keys().is_empty());
    }

    #[test]
    fn git_probe_failure_is_mapped_at_the_call_site() {
        let storage = MemoryStorage::new();
        let error = drive(
            &CollectOptions::default(),
            &FakeRunner::succeeding(),
            &FakeProbe::new().with_git_failure(),
            &FakeOutput::default(),
            &storage,
        )
        .unwrap_err();

        assert!(error.find_source::<GitProbeFailedError>().is_some());
        assert!(error.find_source::<ToolchainProbeFailedError>().is_none());
        assert_eq!(
            error.find_source::<io::Error>().unwrap().kind(),
            io::ErrorKind::Other
        );
        assert!(storage.keys().is_empty());
    }

    #[test]
    fn harvest_failure_is_mapped_at_the_call_site() {
        let storage = MemoryStorage::new();
        let error = drive(
            &CollectOptions::default(),
            &FakeRunner::succeeding(),
            &FakeProbe::new(),
            &FakeOutput::failing(Engine::Criterion),
            &storage,
        )
        .unwrap_err();

        assert!(error.find_source::<HarvestFailedError>().is_some());
        assert_eq!(
            error.find_source::<io::Error>().unwrap().kind(),
            io::ErrorKind::Other
        );
        assert!(storage.keys().is_empty());
    }

    #[test]
    fn both_engines_run_stores_a_set_per_engine() {
        let storage = MemoryStorage::new();
        let outcome = drive(
            &CollectOptions::default(),
            &FakeRunner::succeeding(),
            &FakeProbe::new(),
            &FakeOutput::with_callgrind_and_criterion(),
            &storage,
        )
        .unwrap();

        let RunOutcome::Completed { message } = outcome else {
            panic!("expected completion");
        };
        assert!(message.contains("Stored 2"), "{message}");

        // Every engine partitions under the machine fingerprint of the probed
        // hardware. Both sets are stored under the same machine key.
        let machine = probe_machine_key();
        let keys = storage.keys();
        assert_eq!(keys.len(), 2, "{keys:?}");
        assert!(
            keys.iter()
                .any(|key| key.contains("/callgrind/") && key.contains(&format!("/{machine}/"))),
            "{keys:?}"
        );
        assert!(
            keys.iter()
                .any(|key| key.contains("/criterion/") && key.contains(&format!("/{machine}/"))),
            "{keys:?}"
        );
    }

    #[test]
    fn criterion_output_is_stored() {
        let storage = MemoryStorage::new();
        let outcome = drive(
            &CollectOptions::default(),
            &FakeRunner::succeeding(),
            &FakeProbe::new(),
            &FakeOutput::with_criterion_case(),
            &storage,
        )
        .unwrap();

        let RunOutcome::Completed { message } = outcome else {
            panic!("expected completion");
        };
        assert!(message.contains("Stored 1"), "{message}");

        // Only Criterion produced output, partitioned by the machine fingerprint.
        let keys = storage.keys();
        assert_eq!(keys.len(), 1, "{keys:?}");
        assert!(keys[0].contains("/criterion/"), "{keys:?}");

        let bytes = block_on(storage.get(&keys[0])).unwrap();
        let set = Run::from_json(&String::from_utf8(bytes).unwrap()).unwrap();
        assert_eq!(set.results.len(), 1);
        assert_eq!(set.results[0].metrics[0].kind, crate::MetricKind::WallTime);
    }

    #[test]
    fn malformed_criterion_case_is_a_parse_error() {
        let storage = MemoryStorage::new();
        let error = drive(
            &CollectOptions::default(),
            &FakeRunner::succeeding(),
            &FakeProbe::new(),
            &FakeOutput::with_malformed_criterion_case(),
            &storage,
        )
        .unwrap_err();

        let parse = error.find_source::<ParseOutputError>().unwrap();
        assert_eq!(parse.path(), Path::new("criterion/grp/std/now/new"));
        assert!(storage.keys().is_empty());
    }

    #[test]
    fn malformed_alloc_tracker_operation_is_a_parse_error() {
        let storage = MemoryStorage::new();
        let error = drive(
            &CollectOptions::default(),
            &FakeRunner::succeeding(),
            &FakeProbe::new(),
            &FakeOutput::with_malformed_alloc_tracker_operation(),
            &storage,
        )
        .unwrap_err();

        let parse = error.find_source::<ParseOutputError>().unwrap();
        assert_eq!(parse.path(), Path::new("alloc_tracker/allocate_vec.json"));
        assert!(storage.keys().is_empty());
    }

    #[test]
    fn malformed_all_the_time_operation_is_a_parse_error() {
        let storage = MemoryStorage::new();
        let error = drive(
            &CollectOptions::default(),
            &FakeRunner::succeeding(),
            &FakeProbe::new(),
            &FakeOutput::with_malformed_all_the_time_operation(),
            &storage,
        )
        .unwrap_err();

        let parse = error.find_source::<ParseOutputError>().unwrap();
        assert_eq!(parse.path(), Path::new("all_the_time/read_cell.json"));
        assert!(storage.keys().is_empty());
    }

    #[test]
    fn alloc_tracker_output_is_stored_under_the_machine_key() {
        let storage = MemoryStorage::new();
        let outcome = drive(
            &CollectOptions::default(),
            &FakeRunner::succeeding(),
            &FakeProbe::new(),
            &FakeOutput::with_alloc_tracker_operation(),
            &storage,
        )
        .unwrap();

        let RunOutcome::Completed { message } = outcome else {
            panic!("expected completion");
        };
        assert!(message.contains("Stored 1"), "{message}");

        // Allocation counts are machine-dependent (allocator behaviour varies by
        // build and platform), so the partition is the machine fingerprint.
        let machine = probe_machine_key();
        let keys = storage.keys();
        assert_eq!(keys.len(), 1, "{keys:?}");
        assert!(keys[0].contains("/alloc_tracker/"), "{keys:?}");
        assert!(keys[0].contains(&format!("/{machine}/")), "{keys:?}");

        let bytes = block_on(storage.get(&keys[0])).unwrap();
        let set = Run::from_json(&String::from_utf8(bytes).unwrap()).unwrap();
        assert_eq!(set.results.len(), 1);
        let kinds: Vec<crate::MetricKind> = set.results[0].metrics.iter().map(|m| m.kind).collect();
        assert_eq!(
            kinds,
            vec![
                crate::MetricKind::AllocatedBytes,
                crate::MetricKind::AllocationCount
            ]
        );
    }

    #[test]
    fn zero_iteration_operation_is_dropped_while_measured_ones_are_stored() {
        let storage = MemoryStorage::new();
        let reporter = RecordingReporter::new();
        let outcome = drive_at_with(
            FROZEN_UNIX,
            &CollectOptions::default(),
            &FakeRunner::succeeding(),
            &FakeProbe::new(),
            &FakeOutput::with_measured_and_zero_iteration_alloc_tracker_operations(),
            &storage,
            &reporter,
        )
        .unwrap();

        // The zero-iteration operation has no usable measurement, so only the
        // measured one is stored.
        let RunOutcome::Completed { message } = outcome else {
            panic!("expected completion");
        };
        assert!(message.contains("Stored 1"), "{message}");

        let keys = storage.keys();
        assert_eq!(keys.len(), 1, "{keys:?}");

        // The stored run must round-trip: a dropped null slope keeps every stored
        // metric value finite, whereas storing a NaN would serialize as JSON `null`
        // and fail to deserialize back here.
        let bytes = block_on(storage.get(&keys[0])).unwrap();
        let set = Run::from_json(&String::from_utf8(bytes).unwrap()).unwrap();
        assert_eq!(set.results.len(), 1);
        assert!(
            set.results[0].metrics.iter().all(|m| m.value.is_finite()),
            "every stored metric value must be finite: {:?}",
            set.results[0].metrics
        );

        // The skip is reported so the omission is explained in verbose output.
        assert!(
            reporter.contains("no usable per-iteration measurement"),
            "expected a skip note, got {:?}",
            reporter.notes()
        );
    }

    #[test]
    fn all_the_time_output_is_partitioned_by_machine_key() {
        let storage = MemoryStorage::new();
        let outcome = drive(
            &CollectOptions::default(),
            &FakeRunner::succeeding(),
            &FakeProbe::new(),
            &FakeOutput::with_all_the_time_operation(),
            &storage,
        )
        .unwrap();

        let RunOutcome::Completed { message } = outcome else {
            panic!("expected completion");
        };
        assert!(message.contains("Stored 1"), "{message}");

        // Processor time depends on the host, so it is partitioned by machine key.
        let machine = probe_machine_key();
        let keys = storage.keys();
        assert_eq!(keys.len(), 1, "{keys:?}");
        assert!(
            keys[0].contains(&format!("/all_the_time/x86_64-pc-windows-msvc/{machine}/")),
            "{keys:?}"
        );

        let bytes = block_on(storage.get(&keys[0])).unwrap();
        let set = Run::from_json(&String::from_utf8(bytes).unwrap()).unwrap();
        assert_eq!(set.results.len(), 1);
        assert_eq!(
            set.results[0].metrics[0].kind,
            crate::MetricKind::ProcessorTime
        );
    }

    #[test]
    fn passthrough_arguments_reach_the_runner_verbatim() {
        let runner = FakeRunner::succeeding();
        // The CLI strips the leading `--` separator, so at runtime the passthrough
        // vector holds only the forwarded flags.
        let options = CollectOptions {
            passthrough: vec!["--quiet".to_owned()],
            ..CollectOptions::default()
        };

        drive(
            &options,
            &runner,
            &FakeProbe::new(),
            &FakeOutput::default(),
            &MemoryStorage::new(),
        )
        .unwrap();

        // The benchmark command is the mock program, the default `--workspace`
        // scope, then a single re-inserted `--` separator followed by the
        // passthrough forwarded verbatim to the benchmark harness.
        assert_eq!(
            runner.last_command().unwrap(),
            ["mock", "--workspace", "--", "--quiet"]
        );
    }

    #[test]
    fn exclude_arguments_reach_the_runner_verbatim() {
        let runner = FakeRunner::succeeding();
        let options = CollectOptions {
            excludes: vec!["nm".to_owned(), "many_cpus".to_owned()],
            ..CollectOptions::default()
        };

        drive(
            &options,
            &runner,
            &FakeProbe::new(),
            &FakeOutput::default(),
            &MemoryStorage::new(),
        )
        .unwrap();

        // Excludes ride alongside the implicit `--workspace`, each forwarded to
        // cargo as an `--exclude` pair.
        assert_eq!(
            runner.last_command().unwrap(),
            [
                "mock",
                "--workspace",
                "--exclude",
                "nm",
                "--exclude",
                "many_cpus"
            ]
        );
    }

    #[test]
    fn feature_selection_reaches_the_runner_verbatim() {
        let runner = FakeRunner::succeeding();
        let options = CollectOptions {
            features: vec!["foo".to_owned()],
            all_features: true,
            ..CollectOptions::default()
        };

        drive(
            &options,
            &runner,
            &FakeProbe::new(),
            &FakeOutput::default(),
            &MemoryStorage::new(),
        )
        .unwrap();

        // Feature flags follow the implicit `--workspace` scope, forwarded to
        // cargo in declaration order.
        assert_eq!(
            runner.last_command().unwrap(),
            ["mock", "--workspace", "--features", "foo", "--all-features"]
        );
    }

    #[test]
    fn bench_environment_pins_the_target_directory() {
        let runner = FakeRunner::succeeding();

        drive(
            &CollectOptions::default(),
            &runner,
            &FakeProbe::new(),
            &FakeOutput::default(),
            &MemoryStorage::new(),
        )
        .unwrap();

        let env = runner.last_env().unwrap();
        // The benchmark command receives the combined engine environment (the
        // Callgrind summary flag) plus the resolved target directory, so output
        // lands exactly where the harvest scans.
        assert!(
            env.contains(&("GUNGRAUN_SAVE_SUMMARY".to_owned(), "pretty-json".to_owned())),
            "{env:?}"
        );
        assert!(
            env.contains(&("CARGO_TARGET_DIR".to_owned(), "target".to_owned())),
            "{env:?}"
        );
    }

    #[test]
    fn malformed_summary_is_a_parse_error() {
        let storage = MemoryStorage::new();
        let error = drive(
            &CollectOptions::default(),
            &FakeRunner::succeeding(),
            &FakeProbe::new(),
            &FakeOutput::with_malformed_summary(),
            &storage,
        )
        .unwrap_err();

        let parse = error.find_source::<ParseOutputError>().unwrap();
        assert_eq!(parse.path(), Path::new("a/summary.json"));
        assert!(storage.keys().is_empty());
    }

    #[test]
    fn build_bench_argv_defaults_to_the_whole_workspace() {
        let argv = build_bench_argv(&mock_bench_command(), &CollectOptions::default()).unwrap();
        assert_eq!(argv, ["mock", "--workspace"]);
    }

    #[test]
    fn build_bench_argv_translates_package_filters() {
        let options = CollectOptions {
            packages: vec!["nm".to_owned(), "many_cpus".to_owned()],
            ..CollectOptions::default()
        };
        let argv = build_bench_argv(&mock_bench_command(), &options).unwrap();
        // Packages omit `--workspace` and each becomes a `--package` pair.
        assert_eq!(argv, ["mock", "--package", "nm", "--package", "many_cpus"]);
    }

    #[test]
    fn build_bench_argv_translates_exclude_filters() {
        let options = CollectOptions {
            excludes: vec!["nm".to_owned(), "many_cpus".to_owned()],
            ..CollectOptions::default()
        };
        let argv = build_bench_argv(&mock_bench_command(), &options).unwrap();
        // Excludes ride alongside the implicit `--workspace`, each an
        // `--exclude` pair, so the whole workspace minus those packages is run.
        assert_eq!(
            argv,
            [
                "mock",
                "--workspace",
                "--exclude",
                "nm",
                "--exclude",
                "many_cpus"
            ]
        );
    }

    #[test]
    fn build_bench_argv_translates_feature_selection() {
        let options = CollectOptions {
            packages: vec!["nm".to_owned()],
            features: vec!["foo".to_owned(), "bar baz".to_owned()],
            no_default_features: true,
            ..CollectOptions::default()
        };
        let argv = build_bench_argv(&mock_bench_command(), &options).unwrap();
        // Feature flags follow the scope flags, each `--features` value forwarded
        // verbatim (so a space-separated set survives as one argument).
        assert_eq!(
            argv,
            [
                "mock",
                "--package",
                "nm",
                "--features",
                "foo",
                "--features",
                "bar baz",
                "--no-default-features"
            ]
        );
    }

    #[test]
    fn build_bench_argv_translates_all_features() {
        let options = CollectOptions {
            all_features: true,
            ..CollectOptions::default()
        };
        let argv = build_bench_argv(&mock_bench_command(), &options).unwrap();
        assert_eq!(argv, ["mock", "--workspace", "--all-features"]);
    }

    #[test]
    fn build_bench_argv_translates_bench_filters_and_passthrough_in_order() {
        let options = CollectOptions {
            packages: vec!["nm".to_owned()],
            benches: vec!["nm_observe".to_owned()],
            // The CLI strips the leading `--` separator, so passthrough arrives
            // here without it; `build_bench_argv` re-inserts one before the
            // forwarded flags so cargo forwards them to the benchmark binary.
            passthrough: vec!["--noplot".to_owned()],
            ..CollectOptions::default()
        };
        let argv = build_bench_argv(&mock_bench_command(), &options).unwrap();
        assert_eq!(
            argv,
            [
                "mock",
                "--package",
                "nm",
                "--bench",
                "nm_observe",
                "--",
                "--noplot"
            ]
        );
    }

    #[test]
    fn build_bench_argv_omits_the_separator_without_passthrough() {
        // No passthrough means no trailing `--`, so cargo is not handed a bare
        // separator with nothing after it.
        let options = CollectOptions {
            packages: vec!["nm".to_owned()],
            ..CollectOptions::default()
        };
        let argv = build_bench_argv(&mock_bench_command(), &options).unwrap();
        assert_eq!(argv, ["mock", "--package", "nm"]);
    }

    #[test]
    fn build_bench_argv_rejects_an_empty_command() {
        let error = build_bench_argv(&[], &CollectOptions::default()).unwrap_err();
        let invalid = error.find_source::<InvalidCommandError>().unwrap();
        assert_eq!(invalid.engine(), "cargo bench");
        assert_eq!(invalid.problem(), "the benchmark command is empty");
    }

    // --- best-of-N orchestration ---------------------------------------------

    /// Builds a `NonZeroUsize` for a test `--best-of` count.
    fn best_of(n: usize) -> NonZeroUsize {
        NonZeroUsize::new(n).expect("test best-of counts are non-zero")
    }

    /// A single-operation `all_the_time` harvest whose stored value is `slope`.
    ///
    /// The adapter maps `slope_processor_time_nanos` straight to the metric value,
    /// so authoring a slope lets a test pin exactly what value a run contributes.
    fn all_the_time_output(slope: f64) -> FakeOutput {
        FakeOutput {
            time: vec![RawOperationFile {
                path: PathBuf::from("all_the_time/read_cell.json"),
                content: format!(
                    "{{\"operation\":\"read_cell\",\"total_iterations\":4,\
                     \"total_processor_time_nanos\":80000000,\"span_count\":1,\
                     \"slope_processor_time_nanos\":{slope}}}"
                ),
            }],
            ..FakeOutput::default()
        }
    }

    /// An output source that hands out a different [`FakeOutput`] per invocation,
    /// so `--best-of` runs can be given distinct per-run measurements.
    ///
    /// Each engine advances its own cursor through `runs`, mirroring how the real
    /// harvest scans the freshly produced tree once per engine per run. Requesting
    /// more runs than were supplied is a test-author error and panics.
    struct SequencedOutput {
        runs: Vec<FakeOutput>,
        cursors: Arc<Mutex<HashMap<Engine, usize>>>,
    }

    impl SequencedOutput {
        fn new(runs: Vec<FakeOutput>) -> Self {
            Self {
                runs,
                cursors: Arc::default(),
            }
        }
    }

    impl BenchOutputSource for SequencedOutput {
        async fn collect(
            &self,
            engine: Engine,
            since: Option<SystemTime>,
            reporter: &dyn Reporter,
        ) -> io::Result<Harvest> {
            let index = {
                let mut cursors = self.cursors.lock().unwrap();
                let cursor = cursors.entry(engine).or_insert(0);
                let index = *cursor;
                *cursor = cursor.saturating_add(1);
                index
            };
            let output = self
                .runs
                .get(index)
                .expect("a sequenced test must supply one output per requested run");
            output.collect(engine, since, reporter).await
        }
    }

    /// A runner that succeeds until a chosen 1-based invocation, then reports a
    /// non-zero exit, to prove `--best-of` aborts fail-fast on any failing run.
    struct FailOnNthRunner {
        fail_on: usize,
        calls: Arc<Mutex<usize>>,
    }

    impl FailOnNthRunner {
        fn new(fail_on: usize) -> Self {
            Self {
                fail_on,
                calls: Arc::default(),
            }
        }

        fn call_count(&self) -> usize {
            *self.calls.lock().unwrap()
        }
    }

    impl BenchRunner for FailOnNthRunner {
        fn run_benches(
            &self,
            _argv: &[String],
            _env: &[(String, String)],
        ) -> impl Future<Output = io::Result<EngineStatus>> {
            let mut calls = self.calls.lock().unwrap();
            *calls = calls.saturating_add(1);
            let this_call = *calls;
            ready(Ok(EngineStatus {
                success: this_call != self.fail_on,
                code: (this_call == self.fail_on).then_some(101),
            }))
        }
    }

    /// Drives `execute_collect` over arbitrary runner and output doubles.
    ///
    /// The other `drive*` helpers pin the concrete [`FakeRunner`]/[`FakeOutput`];
    /// the best-of tests need a per-run runner or output, so this variant is
    /// generic over both ports while keeping the rest of the wiring fixed. The
    /// frozen clock hands every run the same start, which is fine because the fake
    /// output ignores the harvest cutoff.
    fn drive_best_of<R, O>(
        options: &CollectOptions,
        runner: &R,
        probe: &FakeProbe,
        output: &O,
        storage: &MemoryStorage,
        reporter: &dyn Reporter,
    ) -> Result<RunOutcome, AppError>
    where
        R: BenchRunner,
        O: BenchOutputSource,
    {
        let now = SystemTime::UNIX_EPOCH
            .checked_add(Duration::from_secs(FROZEN_UNIX))
            .unwrap();
        let clock = Clock::new_frozen_at(now);
        let env = |_name: &str| None::<String>;
        let bench_command = mock_bench_command();
        let deps = CollectDeps {
            runner,
            probe,
            output,
            storage: Some(storage),
            clock: &clock,
            env: &env,
            project_id: "folo",
            tool_version: "0.0.1",
            target_root: Path::new("target"),
            bench_command: &bench_command,
            reporter,
        };
        block_on(execute_collect(options, &deps))
    }

    /// Reads the single stored object back as a [`Run`], failing if there is not
    /// exactly one.
    fn only_stored_run(storage: &MemoryStorage) -> Run {
        let keys = storage.keys();
        assert_eq!(
            keys.len(),
            1,
            "expected exactly one stored object: {keys:?}"
        );
        let bytes = block_on(storage.get(&keys[0])).unwrap();
        Run::from_json(&String::from_utf8(bytes).unwrap()).unwrap()
    }

    #[test]
    fn best_of_runs_the_suite_n_times_and_stores_the_minimum_value() {
        // Three runs measure the same case at 30, 10 and 20 ns; the stored value is
        // the minimum (10), discarding the two slower, interference-perturbed runs.
        let runner = FakeRunner::succeeding();
        let probe = FakeProbe::new();
        let output = SequencedOutput::new(vec![
            all_the_time_output(30.0),
            all_the_time_output(10.0),
            all_the_time_output(20.0),
        ]);
        let storage = MemoryStorage::new();
        let reporter = RecordingReporter::quiet();
        let options = CollectOptions {
            best_of: best_of(3),
            ..CollectOptions::default()
        };

        drive_best_of(&options, &runner, &probe, &output, &storage, &reporter).unwrap();

        assert_eq!(
            runner.calls.lock().unwrap().len(),
            3,
            "the suite must run once per best-of count"
        );

        let run = only_stored_run(&storage);
        assert_eq!(run.results.len(), 1);
        assert_eq!(run.results[0].metrics.len(), 1);
        assert_eq!(run.results[0].metrics[0].value, 10.0);
    }

    #[test]
    fn best_of_selects_each_metric_independently() {
        // Two cases, each minimized on its own: read_cell wins on run 2 (5 < 30),
        // write_cell wins on run 1 (7 < 40), so a stored result blends metrics from
        // different physical runs.
        fn two_ops(read: f64, write: f64) -> FakeOutput {
            FakeOutput {
                time: vec![
                    RawOperationFile {
                        path: PathBuf::from("all_the_time/read_cell.json"),
                        content: format!(
                            "{{\"operation\":\"read_cell\",\
                             \"slope_processor_time_nanos\":{read}}}"
                        ),
                    },
                    RawOperationFile {
                        path: PathBuf::from("all_the_time/write_cell.json"),
                        content: format!(
                            "{{\"operation\":\"write_cell\",\
                             \"slope_processor_time_nanos\":{write}}}"
                        ),
                    },
                ],
                ..FakeOutput::default()
            }
        }

        let runner = FakeRunner::succeeding();
        let probe = FakeProbe::new();
        let output = SequencedOutput::new(vec![two_ops(30.0, 7.0), two_ops(5.0, 40.0)]);
        let storage = MemoryStorage::new();
        let reporter = RecordingReporter::quiet();
        let options = CollectOptions {
            best_of: best_of(2),
            ..CollectOptions::default()
        };

        drive_best_of(&options, &runner, &probe, &output, &storage, &reporter).unwrap();

        let run = only_stored_run(&storage);
        let mut values: Vec<(String, f64)> = run
            .results
            .iter()
            .map(|result| (result.id.to_string(), result.metrics[0].value))
            .collect();
        values.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(
            values,
            vec![
                ("read_cell".to_owned(), 5.0),
                ("write_cell".to_owned(), 7.0),
            ]
        );
    }

    #[test]
    fn best_of_reports_provenance_of_the_kept_sample() {
        // The verbose log must explain each choice: that a best-of pass is running,
        // each invocation, and — per metric — the samples seen and which run
        // supplied the kept minimum, with the winner bracketed.
        let runner = FakeRunner::succeeding();
        let probe = FakeProbe::new();
        let output =
            SequencedOutput::new(vec![all_the_time_output(30.0), all_the_time_output(10.0)]);
        let storage = MemoryStorage::new();
        let reporter = RecordingReporter::new();
        let options = CollectOptions {
            best_of: best_of(2),
            ..CollectOptions::default()
        };

        drive_best_of(&options, &runner, &probe, &output, &storage, &reporter).unwrap();

        assert!(
            reporter.contains("best-of collection: running the suite 2 times"),
            "expected an announcement of the best-of pass, got {:?}",
            reporter.notes()
        );
        assert!(
            reporter.contains("best-of run 1/2"),
            "expected a per-run note, got {:?}",
            reporter.notes()
        );
        assert!(
            reporter.contains("best-of samples 30, [10] (kept run 2)"),
            "expected a provenance note naming the kept sample, got {:?}",
            reporter.notes()
        );
    }

    #[test]
    fn a_single_run_emits_no_best_of_notes() {
        // With the default single run there is nothing to choose, so none of the
        // best-of verbose notes appear — the guards that suppress them at `N == 1`
        // must hold.
        let runner = FakeRunner::succeeding();
        let probe = FakeProbe::new();
        let output = all_the_time_output(20.0);
        let storage = MemoryStorage::new();
        let reporter = RecordingReporter::new();

        drive_best_of(
            &CollectOptions::default(),
            &runner,
            &probe,
            &output,
            &storage,
            &reporter,
        )
        .unwrap();

        assert!(
            !reporter.contains("best-of collection"),
            "a single run must not announce a best-of pass, got {:?}",
            reporter.notes()
        );
        assert!(
            !reporter.contains("best-of run"),
            "a single run must not emit per-run best-of notes, got {:?}",
            reporter.notes()
        );
        assert!(
            !reporter.contains("best-of samples"),
            "a single run has no samples to choose between, got {:?}",
            reporter.notes()
        );
    }

    #[test]
    fn best_of_fails_fast_when_any_run_fails() {
        // The second of three runs exits non-zero, so collection aborts there
        // (never reaching a third run) and stores nothing.
        let runner = FailOnNthRunner::new(2);
        let probe = FakeProbe::new();
        let output = SequencedOutput::new(vec![
            all_the_time_output(30.0),
            all_the_time_output(10.0),
            all_the_time_output(20.0),
        ]);
        let storage = MemoryStorage::new();
        let reporter = RecordingReporter::quiet();
        let options = CollectOptions {
            best_of: best_of(3),
            ..CollectOptions::default()
        };

        let error =
            drive_best_of(&options, &runner, &probe, &output, &storage, &reporter).unwrap_err();

        let failure = error.find_source::<EngineFailedError>().unwrap();
        assert_eq!(failure.engine(), "cargo bench");
        assert_eq!(failure.code(), 101);
        assert_eq!(
            runner.call_count(),
            2,
            "the run after the failure must not start"
        );
        assert!(storage.keys().is_empty());
    }

    #[test]
    fn best_of_rejects_a_case_missing_from_a_later_run() {
        // Run 1 measures read_cell but run 2 does not, so the runs did not exercise
        // the same work: a hard error that stores nothing.
        let runner = FakeRunner::succeeding();
        let probe = FakeProbe::new();
        let output = SequencedOutput::new(vec![all_the_time_output(10.0), FakeOutput::default()]);
        let storage = MemoryStorage::new();
        let reporter = RecordingReporter::quiet();
        let options = CollectOptions {
            best_of: best_of(2),
            ..CollectOptions::default()
        };

        let error =
            drive_best_of(&options, &runner, &probe, &output, &storage, &reporter).unwrap_err();

        let inconsistent = error.find_source::<InconsistentRunsError>().unwrap();
        assert_eq!(inconsistent.engine(), Engine::AllTheTime.to_string());
        assert!(error.find_source::<AggregateError>().is_some());
        assert!(storage.keys().is_empty());
    }

    #[test]
    fn best_of_with_no_store_runs_every_time_but_stores_nothing() {
        let runner = FakeRunner::succeeding();
        let probe = FakeProbe::new();
        let output =
            SequencedOutput::new(vec![all_the_time_output(30.0), all_the_time_output(10.0)]);
        let storage = MemoryStorage::new();
        let reporter = RecordingReporter::quiet();
        let options = CollectOptions {
            best_of: best_of(2),
            no_store: true,
            ..CollectOptions::default()
        };

        let outcome =
            drive_best_of(&options, &runner, &probe, &output, &storage, &reporter).unwrap();

        assert_eq!(runner.calls.lock().unwrap().len(), 2);
        assert!(storage.keys().is_empty());
        let RunOutcome::Completed { message } = outcome else {
            panic!("expected completion");
        };
        assert!(message.contains("nothing stored"), "{message}");
    }

    #[test]
    fn best_of_one_stores_the_single_run_unchanged() {
        // `--best-of 1` is the default single run: the lone sample is stored as-is,
        // with no minimization to perform.
        let runner = FakeRunner::succeeding();
        let probe = FakeProbe::new();
        let output = all_the_time_output(20.0);
        let storage = MemoryStorage::new();
        let reporter = RecordingReporter::quiet();
        let options = CollectOptions {
            best_of: best_of(1),
            ..CollectOptions::default()
        };

        drive_best_of(&options, &runner, &probe, &output, &storage, &reporter).unwrap();

        assert_eq!(runner.calls.lock().unwrap().len(), 1);
        let run = only_stored_run(&storage);
        assert_eq!(run.results[0].metrics[0].value, 20.0);
    }

    #[test]
    fn the_stored_run_records_a_single_repetition() {
        assert_stored_repetition_count(1);
    }

    #[test]
    fn the_stored_run_records_multiple_repetitions() {
        // More than one repetition distinguishes the measured protocol from
        // a hardcoded single-run default.
        assert_stored_repetition_count(2);
    }

    fn assert_stored_repetition_count(count: usize) {
        // The stored value is the minimum of the repetitions, and that minimum falls
        // as the count rises, so the count is part of the measurement protocol and
        // must travel with the run. Recorded from the repetitions that actually
        // produced the harvests, so it can never claim a run that did not happen.
        let runner = FakeRunner::succeeding();
        let probe = FakeProbe::new();
        let output = SequencedOutput::new(vec![all_the_time_output(10.0); count]);
        let storage = MemoryStorage::new();
        let reporter = RecordingReporter::quiet();
        let options = CollectOptions {
            best_of: best_of(count),
            ..CollectOptions::default()
        };

        drive_best_of(&options, &runner, &probe, &output, &storage, &reporter).unwrap();

        // This reads the persisted JSON, including the protocol recorded by collect.
        let run = only_stored_run(&storage);
        assert_eq!(run.context.best_of, NonZero::new(count));
    }

    #[test]
    fn the_recorded_repetition_count_is_explained_in_the_verbose_log() {
        // The verbose trail must state the count recorded with the run and why it is
        // recorded, so the stored measurement protocol is reconstructable from the
        // log rather than only from the object.
        let runner = FakeRunner::succeeding();
        let probe = FakeProbe::new();
        let output =
            SequencedOutput::new(vec![all_the_time_output(30.0), all_the_time_output(10.0)]);
        let storage = MemoryStorage::new();
        let reporter = RecordingReporter::new();
        let options = CollectOptions {
            best_of: best_of(2),
            ..CollectOptions::default()
        };

        drive_best_of(&options, &runner, &probe, &output, &storage, &reporter).unwrap();

        assert!(
            reporter.contains("recording best-of=2 on the stored run"),
            "expected a note naming the recorded count, got {:?}",
            reporter.notes()
        );
        assert!(
            reporter.contains("minimum of 2 samples"),
            "expected the note to explain what the count means, got {:?}",
            reporter.notes()
        );
    }
}
