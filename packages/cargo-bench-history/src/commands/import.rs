//! The `import` command: store pre-existing engine output without running a
//! benchmark.
//!
//! `import` is the `collect` pipeline minus the `cargo bench` run. It harvests a
//! curated `target/`-shaped tree wholesale (an ungated harvest — every file is
//! admitted, unlike `collect`, which gates by the benchmark run's start), probes
//! the real host context exactly as `collect` does, optionally applies a small set
//! of key-affecting metadata overrides, and stores each engine's result set
//! through the shared [`finalize_and_store`] back half.
//!
//! It is internal and hidden from `--help`, but general-purpose: it makes no
//! assumption that the imported output is synthetic, so feeding it real engine
//! output stores exactly what `collect` would. The overrides
//! (`--target-triple`/`--commit`/`--dirty`) touch only key-affecting metadata that
//! can legitimately be attributed independently; the rest of the run context stays
//! probed from the real host.

use std::path::{Path, PathBuf};

use cbh_config::{
    load_config, resolve_config_path, resolve_local_path, resolve_project_id, resolve_repo,
    storage_env,
};
use cbh_diag::{Reporter, ReporterExt, StderrReporter};
use cbh_engines::{BenchOutputSource, FsBenchOutputSource};
use cbh_git::{GitHistory, SystemGitHistory};
use cbh_probe::{EnvironmentProbe, SystemProbe};
use cbh_storage::{Storage, StorageFacade, build_storage};
use ohno::AppError;
use tick::Clock;

use super::collect::{
    FinalizeDeps, SharedContext, StoreParams, build_message, describe_storage, finalize_and_store,
    harvest_records, probe_context,
};
use crate::errors::{ImportError, ResolveRefFailedError};
use crate::model::{BenchmarkResult, Engine, TargetTriple};
use crate::{ImportOptions, RunOutcome, finish_with_flush};

/// The real `import`: wire the production adapters and orchestrate.
///
/// `storage_override` lets end-to-end tests drive the import against a test
/// backend instead of one resolved from configuration; production passes `None`.
pub(crate) async fn execute(
    options: &ImportOptions,
    workspace_dir: &Path,
    storage_override: Option<StorageFacade>,
) -> Result<RunOutcome, AppError> {
    let reporter = StderrReporter::new(options.verbose);

    // `--repo` selects the repository whose git state and project identity the
    // import records against, relative to the ambient base; it defaults to the
    // base directory itself. (The output tree comes from `--target-dir`, not the
    // repository, so the two need not coincide.)
    let base = resolve_repo(workspace_dir, options.repo.as_deref());
    let base = base.as_path();

    let config_path = resolve_config_path(base, options.config_path.as_deref());
    reporter.note_with(|| format!("loading configuration from {}", config_path.display()));
    let config = load_config(&config_path, options.config_path.is_some()).await?;

    let project_id = resolve_project_id(&config, base);
    reporter.note_with(|| format!("project id: {project_id}"));

    // `import` exists to store its harvest, so a backend is always resolved — there
    // is no `--no-store` mode as `collect` has.
    let storage = if let Some(backend) = storage_override {
        reporter.note_with(|| "storage backend: injected by test override".to_owned());
        backend
    } else {
        let local = resolve_local_path(options.local.as_ref(), storage_env().as_deref())?;
        reporter.note_with(|| {
            format!(
                "storage backend: {}",
                describe_storage(options.local.as_ref(), local.as_deref(), &config)
            )
        });
        build_storage(local.as_deref(), &config, base, None)?
    };

    let probe = SystemProbe::in_dir(base);
    let git_history = SystemGitHistory::new(base);

    // `--target-dir` is required and taken as given (a relative path resolves
    // against the repository base, matching `collect`'s target-root resolution).
    // The harvest is ungated, so the caller must name the tree it curated rather
    // than have the shared `target/` directory swept for stale leftovers.
    let target_root = resolve_target_root(&options.target_dir, base)?;
    reporter.note_with(|| {
        format!(
            "scanning for engine output (ungated, curated tree): {}",
            target_root.display()
        )
    });
    let output = FsBenchOutputSource::new(target_root);

    let clock = Clock::new_tokio();
    let env = |name: &str| std::env::var(name).ok();

    let store = FinalizeDeps {
        storage: Some(&storage),
        project_id: &project_id,
        tool_version: env!("CARGO_PKG_VERSION"),
        reporter: &reporter,
    };

    let result =
        orchestrate_import(options, &output, &probe, &git_history, &store, &clock, &env).await;
    // Flush the cache-invalidation marker after the import, even on a partial
    // failure: an `--overwrite` that already replaced a stored object must still
    // invalidate other machines' caches. An append-only import never arms it, so
    // this is a cheap no-op there.
    let flush = storage
        .flush_pending_invalidation(&project_id, &reporter)
        .await;
    finish_with_flush(result, flush)
}

/// Resolves the required `--target-dir` into an absolute scan root.
///
/// The harvest is ungated, so an empty `target_dir` would let `base.join("")`
/// sweep the repository root and pull stale, unrelated engine output into the
/// import; reject it up front instead. The CLI always supplies a value (clap
/// requires it), but a programmatic caller can leave it empty (it defaults to an
/// empty [`PathBuf`] in [`ImportOptions`]), so the guard lives here rather than
/// relying on argument parsing. A relative path resolves against the repository
/// base, matching `collect`'s target-root resolution; an absolute path is taken
/// as given.
fn resolve_target_root(target_dir: &Path, base: &Path) -> Result<PathBuf, AppError> {
    if target_dir.as_os_str().is_empty() {
        return Err(ImportError::new(
            "import requires a non-empty --target-dir naming the curated output tree",
        )
        .into());
    }
    Ok(if target_dir.is_absolute() {
        target_dir.to_path_buf()
    } else {
        base.join(target_dir)
    })
}

/// Orchestrates an import against injected collaborators.
///
/// Harvest every engine's curated output wholesale, probe the host context, apply
/// the metadata overrides, then reduce and store through the shared
/// [`finalize_and_store`] back half. Generic over the same small ports as
/// [`collect`](super::collect), so the flow is exercised hermetically.
async fn orchestrate_import<O, P, G, S>(
    options: &ImportOptions,
    output: &O,
    probe: &P,
    git: &G,
    store: &FinalizeDeps<'_, S>,
    clock: &Clock,
    env: &dyn Fn(&str) -> Option<String>,
) -> Result<RunOutcome, AppError>
where
    O: BenchOutputSource,
    P: EnvironmentProbe,
    G: GitHistory,
    S: Storage,
{
    // `--overwrite` and `--skip-existing` are mutually exclusive; clap rejects the
    // combination on the CLI, so enforce the same contract for programmatic callers
    // rather than silently letting overwrite win.
    if options.overwrite && options.skip_existing {
        return Err(
            ImportError::new("--overwrite and --skip-existing are mutually exclusive").into(),
        );
    }

    // One bucket per engine (parallel to `Engine::ALL`). Import has a single
    // "run" — the curated tree — so each bucket holds exactly one harvest, which
    // `finalize_and_store` reduces trivially (a one-run best-of is the identity).
    let mut per_engine: Vec<Vec<Vec<BenchmarkResult>>> = Vec::with_capacity(Engine::ALL.len());
    for engine in Engine::ALL {
        // `since = None` admits every file: the caller curated this tree, so there
        // are no stale leftovers to gate out by modification time.
        let records = harvest_records(output, store.reporter, engine, None).await?;
        per_engine.push(vec![records]);
    }

    // Import has no benchmark run to date it, so it stamps the run at import time.
    let run_start = clock.system_time();

    let shared = probe_context(probe, env).await?;
    let shared = apply_overrides(shared, options, git, store.reporter).await?;

    // Name the source of the effective triple in the always-on partition
    // announcement: an override says so, otherwise it is the probed toolchain host.
    let triple_note = if options.target_triple.is_some() {
        "from --target-triple"
    } else {
        "toolchain host"
    };
    let params = StoreParams {
        overwrite: options.overwrite,
        skip_existing: options.skip_existing,
        no_store: false,
    };
    let summary = finalize_and_store(
        store,
        &shared,
        &params,
        "importing",
        triple_note,
        &per_engine,
        run_start,
    )
    .await?;
    Ok(RunOutcome::Completed {
        message: build_message(false, summary.stored, summary.harvested, &summary.labels),
    })
}

/// Applies the import metadata overrides to the probed host context.
///
/// Only the key-affecting discriminants are touched; every other field stays as
/// probed. `--target-triple` sets the effective triple (which feeds both the
/// partition key and the recorded `ToolchainInfo.target_triple`). `--commit` is
/// resolved through git — a ref naming no real commit is a hard error, since the
/// import avoids a checkout but not the existence requirement — and the recorded
/// branch is cleared, because the imported data did not come from the current
/// checkout. Imports are clean by default regardless of the repository's current
/// state; `--dirty` routes through the shared git facts so the stored body and its
/// dirty-snapshot key stay coherent by construction.
async fn apply_overrides<G>(
    mut shared: SharedContext,
    options: &ImportOptions,
    git: &G,
    reporter: &dyn Reporter,
) -> Result<SharedContext, AppError>
where
    G: GitHistory,
{
    if let Some(triple) = options.target_triple.as_deref() {
        reporter.note_with(|| {
            format!("overriding target triple to {triple} (partition key and recorded toolchain)")
        });
        shared.target_triple = TargetTriple::from(triple);
    }

    if let Some(reference) = options.commit.as_deref() {
        let resolved = git
            .resolve(reference)
            .await
            .map_err(|error| ResolveRefFailedError::caused_by(reference, error))?
            .ok_or_else(|| {
                ImportError::new(format!(
                    "--commit {reference:?} does not resolve to any commit in this repository"
                ))
            })?;
        reporter.note_with(|| {
            format!(
                "overriding commit to {resolved} (resolved from {reference:?}); clearing branch"
            )
        });
        shared.git.commit = Some(resolved);
        shared.git.branch = None;
    }

    shared.git.dirty = options.dirty;
    if shared.git.dirty {
        reporter.note_with(|| {
            "recording a dirty snapshot (--dirty): keyed by the import-time second".to_owned()
        });
    }

    Ok(shared)
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    #![allow(clippy::indexing_slicing, reason = "panic is fine in tests")]
    #![allow(
        clippy::float_cmp,
        reason = "the faker value core stores exact metric values, so comparisons are exact"
    )]

    use std::future::{Future, ready};
    use std::io;
    use std::num::NonZero;
    use std::path::PathBuf;
    use std::time::{Duration, SystemTime};

    use cargo_bench_history_faker::callgrind_summary;
    use cbh_diag::RecordingReporter;
    use cbh_engines::testing::CALLGRIND_MINIMAL;
    use cbh_engines::{Harvest, RawOperationFile, RawSummary};
    use cbh_git::{FakeGitHistory, parse_git_info};
    use cbh_probe::{HardwareProfile, RustcInfo, resolve_machine_key};
    use cbh_storage::MemoryStorage;
    use futures::executor::block_on;

    use super::*;
    use crate::model::{GitInfo, MetricKind, Run};

    /// The frozen wall-clock instant import tests stamp runs at (2023-11-14Z).
    const FROZEN_UNIX: u64 = 1_700_000_000;

    /// The commit the probed working tree reports (40 hex characters).
    const PROBED_COMMIT: &str = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef";

    /// A minimal host probe with a fixed, deterministic identity, mirroring the
    /// `collect` test double: it lets the import flow run without touching the real
    /// machine, so the stored partition and body are exactly predictable.
    #[derive(Clone)]
    struct FakeProbe {
        git: GitInfo,
        rustc: RustcInfo,
        hardware: HardwareProfile,
    }

    impl FakeProbe {
        fn new() -> Self {
            Self {
                git: parse_git_info(PROBED_COMMIT, "main", ""),
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
            }
        }
    }

    impl EnvironmentProbe for FakeProbe {
        fn git(&self) -> impl Future<Output = io::Result<GitInfo>> {
            ready(Ok(self.git.clone()))
        }

        fn toolchain(&self) -> impl Future<Output = io::Result<RustcInfo>> {
            ready(Ok(self.rustc.clone()))
        }

        fn hardware(&self) -> impl Future<Output = HardwareProfile> {
            ready(self.hardware.clone())
        }
    }

    /// The auto-detected machine key every engine partitions under for the
    /// [`FakeProbe`] hardware profile.
    fn probe_machine_key() -> String {
        resolve_machine_key(&FakeProbe::new().hardware)
    }

    /// A curated-tree stand-in that hands back canned per-engine output, so a test
    /// drives the harvest without a real `target/` directory. `import` gates
    /// nothing, so the modification-time cutoff is ignored.
    #[derive(Clone, Default)]
    struct FakeOutput {
        callgrind: Vec<RawSummary>,
        time: Vec<RawOperationFile>,
    }

    impl BenchOutputSource for FakeOutput {
        fn collect(
            &self,
            engine: Engine,
            _since: Option<SystemTime>,
            _reporter: &dyn Reporter,
        ) -> impl Future<Output = io::Result<Harvest>> {
            ready(Ok(match engine {
                Engine::Callgrind => Harvest::Callgrind(self.callgrind.clone()),
                Engine::Criterion => Harvest::Criterion(Vec::new()),
                Engine::AllocTracker => Harvest::AllocTracker(Vec::new()),
                Engine::AllTheTime => Harvest::AllTheTime(self.time.clone()),
            }))
        }
    }

    /// A minimal harvest for import's identity and storage decisions. The
    /// producer-to-import round trip supplies real faker output explicitly.
    fn callgrind_output() -> FakeOutput {
        FakeOutput {
            callgrind: vec![RawSummary {
                path: PathBuf::from("a/summary.json"),
                content: CALLGRIND_MINIMAL.to_owned(),
            }],
            ..FakeOutput::default()
        }
    }

    /// A curated tree carrying both a Callgrind and an `all_the_time` engine's
    /// output, to prove every engine is machine-keyed.
    fn callgrind_and_time_output() -> FakeOutput {
        FakeOutput {
            time: vec![RawOperationFile {
                path: PathBuf::from("all_the_time/read_cell.json"),
                content: "{\"operation\":\"read_cell\",\"slope_processor_time_nanos\":12.0}"
                    .to_owned(),
            }],
            ..callgrind_output()
        }
    }

    /// Drives [`orchestrate_import`] against in-memory doubles and a frozen clock,
    /// so every test is hermetic (no filesystem, process, or real time) and thus
    /// Miri-safe.
    fn run_import(
        options: &ImportOptions,
        output: &FakeOutput,
        probe: &FakeProbe,
        git: &FakeGitHistory,
        storage: &MemoryStorage,
    ) -> Result<RunOutcome, AppError> {
        let now = SystemTime::UNIX_EPOCH
            .checked_add(Duration::from_secs(FROZEN_UNIX))
            .unwrap();
        let clock = Clock::new_frozen_at(now);
        let env = |_name: &str| None::<String>;
        let reporter = RecordingReporter::quiet();
        let store = FinalizeDeps {
            storage: Some(storage),
            project_id: "folo",
            tool_version: "0.0.1",
            reporter: &reporter,
        };
        block_on(orchestrate_import(
            options, output, probe, git, &store, &clock, &env,
        ))
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

    /// The value of a result's metric of the given kind, panicking if absent.
    fn metric(result: &BenchmarkResult, kind: MetricKind) -> f64 {
        result
            .metrics
            .iter()
            .find(|metric| metric.kind == kind)
            .unwrap_or_else(|| panic!("missing {kind:?} metric"))
            .value
    }

    #[test]
    fn import_stores_the_harvested_run_under_the_probed_identity() {
        let storage = MemoryStorage::new();
        // Preserve the producer-to-parser-to-storage contract without making
        // every orchestration scenario construct the producer's full document.
        let output = FakeOutput {
            callgrind: vec![RawSummary {
                path: PathBuf::from("a/summary.json"),
                content: callgrind_summary("m", "f", None, Some("/pkg"), 36, 4, 2),
            }],
            ..FakeOutput::default()
        };
        let outcome = run_import(
            &ImportOptions::default(),
            &output,
            &FakeProbe::new(),
            &FakeGitHistory::new(),
            &storage,
        )
        .unwrap();

        let RunOutcome::Completed { message } = outcome else {
            panic!("expected completion, got {outcome:?}");
        };
        assert!(message.contains("Stored 1"), "{message}");

        // Format canary: the exact storage key and decompressed body an importer
        // must produce for the single-case Callgrind harvest. Every engine is
        // machine-keyed, so the machine segment is the probed host fingerprint; the
        // commit and triple come from the probed host.
        let machine = probe_machine_key();
        let keys = storage.keys();
        assert_eq!(
            keys,
            vec![format!(
                "v1/folo/objects/callgrind/x86_64-pc-windows-msvc/{machine}/\
                 deadbeefdeadbeefdeadbeefdeadbeefdeadbeef/clean.json"
            )]
        );

        let run = only_stored_run(&storage);
        assert_eq!(run.schema_version, crate::SCHEMA_VERSION);
        assert_eq!(run.results.len(), 1);
        // The import stamps the run at the injected clock's instant, not real time.
        assert_eq!(
            run.context.observation.as_second(),
            i64::try_from(FROZEN_UNIX).unwrap()
        );
        assert_eq!(run.context.git.commit.as_deref(), Some(PROBED_COMMIT));
        assert_eq!(run.context.git.branch.as_deref(), Some("main"));
        assert!(!run.context.git.dirty);
        // An import reduces one curated harvest, so the recorded measurement
        // protocol is a single repetition.
        assert_eq!(run.context.best_of, NonZero::new(1));
        assert_eq!(
            run.context.toolchain.target_triple,
            "x86_64-pc-windows-msvc"
        );
        // The faker value core's metrics survive the harvest-parse-store round trip
        // intact.
        assert_eq!(metric(&run.results[0], MetricKind::InstructionCount), 36.0);
        assert_eq!(
            metric(&run.results[0], MetricKind::ConditionalBranches),
            4.0
        );
        assert_eq!(metric(&run.results[0], MetricKind::IndirectBranches), 2.0);
    }

    #[test]
    fn target_triple_override_repartitions_and_records_the_toolchain() {
        let storage = MemoryStorage::new();
        let options = ImportOptions {
            target_triple: Some("aarch64-apple-darwin".to_owned()),
            ..ImportOptions::default()
        };

        run_import(
            &options,
            &callgrind_output(),
            &FakeProbe::new(),
            &FakeGitHistory::new(),
            &storage,
        )
        .unwrap();

        let keys = storage.keys();
        assert_eq!(keys.len(), 1);
        assert!(
            keys[0].contains("/callgrind/aarch64-apple-darwin/"),
            "{}",
            keys[0]
        );
        // The override feeds the recorded toolchain too, so the stored body stays
        // coherent with its partition even though the host is x86_64 Windows.
        let run = only_stored_run(&storage);
        assert_eq!(run.context.toolchain.target_triple, "aarch64-apple-darwin");
    }

    #[test]
    fn commit_override_keys_the_resolved_commit_and_clears_branch() {
        const RESOLVED: &str = "feedfacefeedfacefeedfacefeedfacefeedface";
        let mut git = FakeGitHistory::new();
        git.commit(RESOLVED, None).branch("release-1.0", RESOLVED);
        let storage = MemoryStorage::new();
        let options = ImportOptions {
            commit: Some("release-1.0".to_owned()),
            ..ImportOptions::default()
        };

        run_import(
            &options,
            &callgrind_output(),
            &FakeProbe::new(),
            &git,
            &storage,
        )
        .unwrap();

        let run = only_stored_run(&storage);
        // The ref resolves to the underlying commit, which keys the run...
        assert_eq!(run.context.git.commit.as_deref(), Some(RESOLVED));
        assert!(
            storage.keys()[0].contains(RESOLVED),
            "{}",
            storage.keys()[0]
        );
        // ...and the branch is cleared, since the imported data did not come from
        // the current checkout.
        assert_eq!(run.context.git.branch, None);
    }

    #[test]
    fn unresolvable_commit_override_is_a_hard_error() {
        let storage = MemoryStorage::new();
        let options = ImportOptions {
            commit: Some("does-not-exist".to_owned()),
            ..ImportOptions::default()
        };

        let error = run_import(
            &options,
            &callgrind_output(),
            &FakeProbe::new(),
            &FakeGitHistory::new(),
            &storage,
        )
        .unwrap_err();

        assert!(error.find_source::<ImportError>().is_some());
        assert!(
            storage.keys().is_empty(),
            "an unresolved commit stores nothing"
        );
    }

    #[test]
    fn commit_resolution_failure_is_mapped_at_the_call_site() {
        let storage = MemoryStorage::new();
        let options = ImportOptions {
            commit: Some("release-1.0".to_owned()),
            ..ImportOptions::default()
        };
        let mut git = FakeGitHistory::new();
        git.fail_resolve();

        let error = run_import(
            &options,
            &callgrind_output(),
            &FakeProbe::new(),
            &git,
            &storage,
        )
        .unwrap_err();

        assert!(error.find_source::<ResolveRefFailedError>().is_some());
        assert!(error.find_source::<io::Error>().is_some());
        assert!(storage.keys().is_empty());
    }

    #[test]
    fn overwrite_and_skip_existing_together_is_a_hard_error() {
        let storage = MemoryStorage::new();
        let options = ImportOptions {
            overwrite: true,
            skip_existing: true,
            ..ImportOptions::default()
        };

        let error = run_import(
            &options,
            &callgrind_output(),
            &FakeProbe::new(),
            &FakeGitHistory::new(),
            &storage,
        )
        .unwrap_err();

        assert!(error.find_source::<ImportError>().is_some());
        assert!(
            storage.keys().is_empty(),
            "a rejected import stores nothing"
        );
    }

    #[test]
    fn overwrite_alone_still_imports() {
        assert_dedup_flag_imports(&ImportOptions {
            overwrite: true,
            ..ImportOptions::default()
        });
    }

    #[test]
    fn skip_existing_alone_still_imports() {
        assert_dedup_flag_imports(&ImportOptions {
            skip_existing: true,
            ..ImportOptions::default()
        });
    }

    fn assert_dedup_flag_imports(options: &ImportOptions) {
        // Only one of the mutually exclusive flags set is a valid import: the guard
        // must inspect both operands, so overwrite-only and skip-existing-only each
        // store the single harvested run against an empty backend rather than being
        // mistaken for the both-set combination.
        let storage = MemoryStorage::new();
        let outcome = run_import(
            options,
            &callgrind_output(),
            &FakeProbe::new(),
            &FakeGitHistory::new(),
            &storage,
        )
        .unwrap();
        let RunOutcome::Completed { message } = outcome else {
            panic!("expected completion, got {outcome:?}");
        };
        assert!(message.contains("Stored 1"), "{message}");
    }

    #[test]
    fn resolve_target_root_rejects_an_empty_target_dir() {
        let error = resolve_target_root(&PathBuf::new(), &PathBuf::from("/work")).unwrap_err();
        assert!(error.find_source::<ImportError>().is_some());
    }

    #[test]
    fn resolve_target_root_resolves_a_relative_dir_against_the_base() {
        let base = if cfg!(windows) {
            PathBuf::from(r"C:\work")
        } else {
            PathBuf::from("/work")
        };
        let resolved = resolve_target_root(&PathBuf::from("out"), &base).unwrap();
        assert_eq!(resolved, base.join("out"));
        assert!(resolved.is_absolute());
    }

    #[test]
    fn resolve_target_root_honors_an_absolute_dir() {
        let absolute = if cfg!(windows) {
            r"C:\custom\out"
        } else {
            "/custom/out"
        };
        let resolved =
            resolve_target_root(&PathBuf::from(absolute), &PathBuf::from("ignored-base")).unwrap();
        assert_eq!(resolved, PathBuf::from(absolute));
    }

    #[test]
    fn dirty_override_records_a_dirty_snapshot_keyed_by_the_import_second() {
        let storage = MemoryStorage::new();
        let options = ImportOptions {
            dirty: true,
            ..ImportOptions::default()
        };

        run_import(
            &options,
            &callgrind_output(),
            &FakeProbe::new(),
            &FakeGitHistory::new(),
            &storage,
        )
        .unwrap();

        let keys = storage.keys();
        assert_eq!(keys.len(), 1);
        // A dirty snapshot is keyed by the injected clock's second, not `clean`, so
        // concurrent snapshots of the same commit coexist.
        assert!(keys[0].ends_with("/dirty-1700000000.json"), "{}", keys[0]);
        let run = only_stored_run(&storage);
        assert!(run.context.git.dirty);
    }

    #[test]
    fn import_is_clean_by_default_even_when_the_repository_is_dirty() {
        let storage = MemoryStorage::new();
        let mut probe = FakeProbe::new();
        probe.git.dirty = true;

        run_import(
            &ImportOptions::default(),
            &callgrind_output(),
            &probe,
            &FakeGitHistory::new(),
            &storage,
        )
        .unwrap();

        let keys = storage.keys();
        assert_eq!(keys.len(), 1);
        assert!(keys[0].ends_with("/clean.json"), "{}", keys[0]);
        assert!(!only_stored_run(&storage).context.git.dirty);
    }

    #[test]
    fn import_partitions_every_engine_under_the_probed_machine_key() {
        let storage = MemoryStorage::new();

        run_import(
            &ImportOptions::default(),
            &callgrind_and_time_output(),
            &FakeProbe::new(),
            &FakeGitHistory::new(),
            &storage,
        )
        .unwrap();

        let keys = storage.keys();
        let machine = probe_machine_key();
        assert_eq!(keys.len(), 2, "{keys:?}");
        assert!(
            keys.iter()
                .any(|key| key.contains("/callgrind/") && key.contains(&format!("/{machine}/"))),
            "{keys:?}"
        );
        assert!(
            keys.iter()
                .any(|key| key.contains("/all_the_time/") && key.contains(&format!("/{machine}/"))),
            "{keys:?}"
        );
    }
}
