//! The `analyze` orchestration entry points: [`execute`] wires the real adapters
//! and [`analyze_with`] is the storage- and git-generic orchestrator that the sibling
//! modules (`selection`, `discriminants`, `load`, `dataset`, `history`, `window`) compose.
//! The parent module re-exports the surface the sibling query commands
//! (`list`, `prune`, `examine`, `bless`) share.

use std::io::IsTerminal;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use anyspawn::Spawner;
use cbh_command::AnalyzeOptions;
use cbh_config::{
    Config, cache_env, load_config, resolve_cache_path, resolve_config_path, resolve_local_path,
    resolve_project_id, resolve_repo, storage_env,
};
use cbh_detect::{
    AnalysisContext, AnalysisMode, BranchComparison, BranchEvaluationTrace, Detection,
    MAX_BRANCH_BASE_COMMITS, MIN_REGIME, MIN_SERIES_POINTS, Series, SeriesCensus, SeriesFilter,
    Testability, UnjudgedReason, apply_base_blessings, apply_blessings, find_changes_spawned,
    retain_present_at_context, short_commit, testability,
};
use cbh_diag::{Reporter, ReporterExt, StderrReporter, count_noun};
use cbh_git::{GitHistory, SystemGitHistory};
use cbh_model::DiscriminantSet;
use cbh_probe::{EnvironmentProbe, SystemProbe, resolve_machine_key};
use cbh_render::{
    Coverage, DEFAULT_SUMMARY_LIMIT, ReportInput, SetSummary, render, render_markdown_summary,
};
use cbh_storage::{Storage, StorageFacade, resolve_storage};
use jiff::Timestamp;
use tick::Clock;

use super::comparison_base::classify_comparison_base_lags;
use super::dataset::{empty_history_hint, select_dataset};
use super::discriminants::AutoDiscriminants;
use super::history::dirty_base_exception_warning;
use super::selection::Selection;
use crate::{AnalyzeError, RenderedReports, ReportRequest, ToolchainProbeFailedError};

/// The real `analyze`: load configuration, wire the configured storage and git
/// history, and orchestrate.
///
/// `clock_override` injects the [`tick::Clock`] the analysis anchors its "now" to:
/// `None` reads the runtime wall clock (`Clock::new_tokio`), while tests pass a
/// frozen clock (`Clock::new_frozen_at`) so the anchor is deterministic. That
/// single anchor drives both the history-mode default `--since` look-back and the
/// resolution of any relative `--since` duration, so the cutoff is
/// deterministic under a frozen clock.
///
/// Returns the rendered reports for the requested formats plus the regression
/// count; the shell writes the files and prints the text report.
// Thin real-adapter wiring: loads config from disk, builds the configured storage,
// and shells out via `SystemGitHistory`/`detect_auto_discriminants` before delegating
// every decision to the mutation-tested `analyze_with`. In-crate tests cannot drive
// these real adapters deterministically; the binary's integration tests cover this edge.
#[cfg_attr(test, mutants::skip)]
pub async fn execute(
    options: &AnalyzeOptions,
    workspace_dir: &Path,
    clock_override: Option<Clock>,
    storage_override: Option<StorageFacade>,
    auto_override: Option<AutoDiscriminants>,
) -> Result<(RenderedReports, usize), AnalyzeError> {
    // Per-object notes follow `--verbose`; stage timings are emitted under either
    // `--verbose` or the programmatic `timing` flag (the stress harness sets the
    // latter alone to see the load breakdown without the per-object flood).
    let reporter = StderrReporter::with_timing(options.verbose, options.stage_timings_enabled());

    let config_path = resolve_config_path(workspace_dir, options.config_path.as_deref());
    reporter.note_with(|| format!("loading configuration from {}", config_path.display()));
    let config = load_config(&config_path, options.config_path.is_some()).await?;

    let project_id = resolve_project_id(&config, workspace_dir);
    let local = resolve_local_path(options.local.as_ref(), storage_env().as_deref())?;
    let cache = resolve_cache_path(options.cache.as_ref(), cache_env().as_deref())?;
    let storage = resolve_storage(
        storage_override,
        local.as_deref(),
        &config,
        workspace_dir,
        cache.as_deref(),
        &reporter,
    )?;
    // Reconcile the read-through cache (if any) with the cloud before loading, so a
    // stale mirror is wiped rather than served.
    storage.synchronize_cache(&project_id, &reporter).await?;

    let git = SystemGitHistory::new(resolve_repo(workspace_dir, options.repo.as_deref()));
    let auto = resolve_auto_discriminants(auto_override).await?;

    let now = resolve_now(clock_override);
    let color = should_colorize(
        std::io::stdout().is_terminal(),
        std::env::var_os("NO_COLOR").is_some(),
    );
    // Distribute the compute-bound detection across the runtime's blocking pool, so
    // the analysis shares the ambient Tokio worker threads rather than spawning its
    // own short-lived ones.
    let spawner = Spawner::new_tokio();
    let outcome = analyze_with(
        &git,
        &storage,
        &project_id,
        &config,
        options,
        &auto,
        now,
        &reporter,
        color,
        &spawner,
    )
    .await;
    // Surface the cache hit/miss tally after the load, so a slow analyze can be
    // diagnosed as a cold or invalidated mirror regardless of the load's outcome.
    storage.report_cache_tally(&reporter);
    outcome
}

/// Reads the analysis anchor instant from a [`tick::Clock`], the single source of
/// wall-clock time for the whole analyze family (`analyze`/`list`/`examine`/`prune`/
/// `bless`).
///
/// Production passes `clock_override: None` and reads the runtime clock
/// (`Clock::new_tokio`); tests inject a frozen clock (`Clock::new_frozen_at`) so the
/// resolved window is deterministic. Sourcing the instant through the clock keeps
/// time injectable rather than minting it from a bare `Timestamp::now()`.
pub(crate) fn resolve_now(clock_override: Option<Clock>) -> Timestamp {
    clock_override
        .unwrap_or_else(Clock::new_tokio)
        .system_time_as::<Timestamp>()
}

/// Whether colored output should be emitted: only to an interactive terminal with
/// `NO_COLOR` unset.
fn should_colorize(is_terminal: bool, no_color: bool) -> bool {
    is_terminal && !no_color
}

/// Probes the current machine's auto-detect discriminant values for the query commands.
///
/// The host triple comes from `rustc -vV` (with a platform fallback) and the
/// machine key from the hardware fingerprint. There is no engine probe — a bare
/// query analyzes every engine. Tests drive the generic orchestrators directly
/// with deterministic [`AutoDiscriminants`] instead of calling this.
// Probes the host environment; the discriminant resolution it feeds is tested.
#[cfg_attr(test, mutants::skip)]
pub(crate) async fn detect_auto_discriminants() -> Result<AutoDiscriminants, AnalyzeError> {
    let probe = SystemProbe::default();
    detect_auto_discriminants_with(&probe).await
}

/// Resolves auto-detected discriminant values from an injected environment probe.
async fn detect_auto_discriminants_with<P: EnvironmentProbe>(
    probe: &P,
) -> Result<AutoDiscriminants, AnalyzeError> {
    let toolchain = probe
        .toolchain()
        .await
        .map_err(ToolchainProbeFailedError::caused_by)?;
    let hardware = probe.hardware().await;
    Ok(AutoDiscriminants {
        triple: toolchain.host.unwrap_or_default(),
        machine_key: resolve_machine_key(&hardware),
    })
}

/// Resolves the auto-detect discriminants for a query command, preferring an injected
/// override over probing the host.
///
/// Production passes `None` and probes via [`detect_auto_discriminants`]; the binary's
/// integration tests inject deterministic [`AutoDiscriminants`] through the `Overrides`
/// test hook so the suite is independent of the host it runs on.
// Trivial override-or-probe selection; the probe path is host-dependent.
#[cfg_attr(test, mutants::skip)]
pub(crate) async fn resolve_auto_discriminants(
    auto_override: Option<AutoDiscriminants>,
) -> Result<AutoDiscriminants, AnalyzeError> {
    match auto_override {
        Some(auto) => Ok(auto),
        None => detect_auto_discriminants().await,
    }
}

/// Storage- and git-generic `analyze`: apply discriminant filters to the stored
/// objects, resolve the git topology, select the comparable commits, build the
/// series, detect changes, and render a report for the requested format.
///
/// `color` enables ANSI styling and colored charts in the text report; callers
/// pass the terminal-detection result so piped output and tests stay plain.
#[expect(
    clippy::too_many_arguments,
    reason = "analyze orchestration wires several injected ports plus the rendering color flag"
)]
pub(crate) async fn analyze_with<G, S>(
    git: &G,
    storage: &S,
    project_id: &str,
    config: &Config,
    options: &AnalyzeOptions,
    auto: &AutoDiscriminants,
    now: Timestamp,
    reporter: &dyn Reporter,
    color: bool,
    spawner: &Spawner,
) -> Result<(RenderedReports, usize), AnalyzeError>
where
    G: GitHistory,
    S: Storage + Clone + 'static,
{
    let request = ReportRequest::resolve_analyze(
        options.no_text,
        options.markdown.as_deref(),
        options.json.as_deref(),
        options.markdown_summary.as_deref(),
    )?;
    let selection = Selection::from_analyze(options);
    let filter = SeriesFilter {
        prefixes: &options.prefixes,
    };
    let load_started = Instant::now();
    let dataset = select_dataset(
        git, storage, project_id, config, &selection, filter, true, auto, now, reporter, spawner,
    )
    .await?;
    reporter.timing(
        "select_dataset (full load: list + filter + topology + fetch/parse/fold + build)",
        load_started.elapsed(),
    );

    let mut series = dataset.series;

    // Ghost filtering: analyze only benchmarks that still exist at the context
    // commit. A benchmark that appears only for past commits — renamed, removed,
    // or replaced — is a "ghost"; re-flagging it is noise. Dropping ghosts
    // *before* detection also keeps them out of the false-discovery-rate
    // correction, so a removed benchmark cannot dilute the correction for real
    // ones. Presence is read from the raw points, independent of re-baselining.
    let (ghosts_excluded, ghost_series) = {
        let ghost_started = Instant::now();
        let before = series.len();
        let ghosts = retain_present_at_context(&mut series, &dataset.tip_commit);
        debug_assert_eq!(
            ghosts.len(),
            before.saturating_sub(series.len()),
            "the filter reports every series it drops"
        );
        for (set, id, kind) in &ghosts {
            reporter.note_with(|| {
                format!(
                    "excluding {} {} in {set}: not present at the context commit {}",
                    id.qualified(),
                    kind.as_str(),
                    short_commit(&dataset.tip_commit),
                )
            });
        }
        // The report's ghost tally speaks of benchmarks while the census speaks of
        // metric series. Both are read off the one list, so the two units cannot
        // disagree about what was dropped. The list is ordered by `(set, id, kind)`,
        // so a benchmark's series are adjacent and plain deduplication suffices.
        let mut benchmarks: Vec<_> = ghosts.iter().map(|(set, id, _)| (set, id)).collect();
        benchmarks.dedup();
        reporter.note_with(|| {
            format!(
                "ghost filter: excluded {} ghost series across {} not present at the \
                 context commit {}, leaving {} series",
                ghosts.len(),
                count_noun(benchmarks.len(), "benchmark"),
                short_commit(&dataset.tip_commit),
                series.len(),
            )
        });
        reporter.timing(
            "ghost filter (retain_present_at_context)",
            ghost_started.elapsed(),
        );
        (benchmarks.len(), ghosts.len())
    };

    // Apply blessings on the topology each mode analyzes. History re-baselines the
    // context series; branch mode truncates each base-ref comparison window.
    let rebaseline_started = Instant::now();
    let blessing_timing = match dataset.mode {
        AnalysisMode::History => {
            apply_blessings(&mut series, &dataset.blessings);
            "re-baseline blessed series (apply_blessings)"
        }
        AnalysisMode::Branch => {
            apply_base_blessings(&mut series, &dataset.blessings);
            "truncate blessed base evidence (apply_base_blessings)"
        }
    };
    reporter.timing(blessing_timing, rebaseline_started.elapsed());
    let context = AnalysisContext {
        mode: dataset.mode,
        merge_base_index: dataset.merge_base_index,
        base_ref_index: dataset.base_ref_index,
        tip_index: dataset.tip_index,
    };
    // Share the series across the detection's blocking tasks without copying; the
    // remaining per-set reporting reads them back through this same handle.
    let series: Arc<[Series]> = Arc::from(series);
    let detect_started = Instant::now();
    let Detection {
        findings,
        mut census,
        branch_comparisons,
        branch_trace,
    } = find_changes_spawned(Arc::clone(&series), context, spawner).await;
    // The ghost filter judged nothing either, and it ran before detection could see
    // those series, so its exclusions join the same account.
    census.record_unjudged(UnjudgedReason::Ghost, ghost_series);
    let detection_stage = match dataset.mode {
        AnalysisMode::History => "change detection (per-series detectors + FDR filter)",
        AnalysisMode::Branch => "change detection (current-regime excursions + report comparison)",
    };
    reporter.timing(detection_stage, detect_started.elapsed());
    note_branch_evaluation(reporter, &branch_trace);
    note_series_census(reporter, &series, &context, &census);
    let regressions = findings
        .iter()
        .filter(|finding| finding.is_regression())
        .count();
    let notable = !findings.is_empty();

    // Disclose when a branch finding's comparison base lags the base ref — the
    // recent base commits carry data only under a rotated machine key, or none at
    // all. Classified per set from the detector's actual comparison point, drawing on
    // the already loaded series first and only then a lazy sibling fetch.
    let lag_started = Instant::now();
    let comparison_base_lags = classify_comparison_base_lags(
        storage,
        &findings,
        &series,
        dataset.base_ref_index,
        &dataset.sibling_observations,
        reporter,
    )
    .await;
    reporter.timing(
        "comparison-base lag classification (classify_comparison_base_lags)",
        lag_started.elapsed(),
    );

    // Break the report down by comparable set so each partition reads on its own.
    let mut sets: Vec<DiscriminantSet> = series.iter().map(|one| one.set.clone()).collect();
    sets.sort();
    sets.dedup();
    let summaries: Vec<SetSummary<'_>> = sets
        .iter()
        .map(|set| SetSummary {
            set,
            runs: dataset.run_index.runs_in_set(set),
            series: series.iter().filter(|one| &one.set == set).count(),
            findings: findings
                .iter()
                .filter(|finding| &finding.set == set)
                .collect(),
            comparison_base_lags: comparison_base_lags.get(set).cloned().unwrap_or_default(),
            branch_comparison: branch_comparison_for_set(&branch_comparisons, set),
        })
        .collect();

    // When stored runs existed but none entered the analysis, the empty outcome is
    // otherwise indistinguishable from "no data". Explain the dominant reasons so
    // the user can act without resorting to `--verbose`.
    //
    // The ghost filter is a distinct empty case: runs *did* load and analyze, but
    // every benchmark was dropped as a ghost. `empty_history_hint` keys off an
    // empty load and stays silent here, so name the ghost case on its own.
    let hint = if ghosts_excluded > 0 && series.is_empty() {
        Some(all_ghosts_hint(&dataset.tip_commit))
    } else {
        empty_history_hint(
            dataset.run_index.is_empty(),
            dataset.candidate_count,
            &dataset.target_ref,
            dataset.tally,
            &dataset.discriminants,
        )
    };

    // Admitting a dirty snapshot on the base branch's tip is a courtesy for the
    // "evaluating the tool" / "accidentally working on the base branch" cases; warn
    // that such data is not persisted across commits.
    let warning = dataset
        .included_dirty_base_exception
        .then(dirty_base_exception_warning);

    let input = ReportInput {
        project: project_id,
        tip_commit: &dataset.tip_commit,
        tip_dirty: dataset.tip_dirty,
        mode: dataset.mode,
        notable,
        runs: dataset.run_index.total(),
        series: series.len(),
        commit_span: dataset.run_index.commit_span(),
        report_improvements: context.reports_improvements(),
        findings: &findings,
        sets: &summaries,
        hint: hint.as_deref(),
        warning: warning.as_deref(),
        ghosts_excluded,
        census,
    };
    let render_started = Instant::now();
    let rendered = request.render_analyze(
        |format| render(&input, format, color),
        || render_markdown_summary(&input, DEFAULT_SUMMARY_LIMIT),
    );
    reporter.timing("report render", render_started.elapsed());

    Ok((rendered, regressions))
}

/// Finds the historical branch comparison belonging to one comparable partition.
fn branch_comparison_for_set<'a>(
    comparisons: &'a [BranchComparison],
    set: &DiscriminantSet,
) -> Option<&'a BranchComparison> {
    comparisons.iter().find(|comparison| &comparison.set == set)
}

/// The empty-outcome hint for the all-ghosts case: runs loaded and analyzed, but
/// the ghost filter dropped every benchmark because none is present at the context
/// commit. Distinct from [`empty_history_hint`], which speaks only to an empty load.
fn all_ghosts_hint(tip_commit: &str) -> String {
    format!(
        "Runs were analyzed, but every benchmark was filtered as a ghost — none is \
         present at the context commit {}. This usually means the context commit has \
        no stored runs (collect at the context commit), or its benchmark set differs \
        from history.",
        short_commit(tip_commit)
    )
}

/// Explains, under `--verbose`, which series the detectors judged and what each of
/// the rest lacked.
///
/// The report discloses the tallies; this trail names the individual series behind
/// them and the gate that declined each, so the verdict can be reconstructed rather
/// than merely counted. Ghost-filtered series are already named one by one where they
/// are dropped, so only the summary repeats them here.
fn note_series_census<R: Reporter + ?Sized>(
    reporter: &R,
    series: &[Series],
    context: &AnalysisContext,
    census: &SeriesCensus,
) {
    reporter.if_enabled(|notes| {
        for one in series {
            let Testability::Unjudged(reason) = testability(one, context) else {
                continue;
            };
            let evidence = match context.mode {
                // A blessed history series is judged only on what came after the blessing,
                // so name that window rather than the misleading full length.
                AnalysisMode::History if one.active_start > 0 => format!(
                    "{}, of which {} since its blessing",
                    count_noun(one.points.len(), "point"),
                    one.points.len().saturating_sub(one.active_start),
                ),
                AnalysisMode::History => count_noun(one.points.len(), "point"),
                AnalysisMode::Branch => {
                    let retained = count_noun(one.base_window.len(), "base-branch commit");
                    let available = count_noun(
                        one.base_history_count.max(one.base_window.len()),
                        "base-branch commit",
                    );
                    format!(
                        "{retained} retained from {available} available before the cap and blessings"
                    )
                }
            };
            notes.note(&format!(
                "not judging {} {} in {}: {} — it carries {evidence}",
                one.id.qualified(),
                one.kind.as_str(),
                one.set,
                reason.describe(),
            ));
        }
        let rule = match context.mode {
            AnalysisMode::History => format!(
                "history mode judges a series only from {} in the analyzed window, \
                 since a change point needs {} on each side of it",
                count_noun(MIN_SERIES_POINTS, "point"),
                count_noun(MIN_REGIME, "point"),
            ),
            AnalysisMode::Branch => format!(
                "branch mode judges a series only with a measurement on the branch and \
                 {} in the {}-commit comparison window to judge it against",
                count_noun(MIN_SERIES_POINTS, "base-branch commit"),
                MAX_BRANCH_BASE_COMMITS,
            ),
        };
        // The trail states the same ratio the report's header and verdict do, read from
        // the one projection all of them share, so a verbose run cannot contradict the
        // report it explains.
        let coverage = Coverage::from_census(census);
        let breakdown = if coverage.unjudged() == 0 {
            "every series was judged".to_owned()
        } else {
            coverage
                .reasons()
                .map(|(reason, count)| format!("{count} {}", reason.describe()))
                .collect::<Vec<_>>()
                .join(", ")
        };
        notes.note(&format!(
            "series census: judged {} of {} in-scope series; {breakdown}; {rule}",
            coverage.judged(),
            coverage.in_scope(),
        ));
    });
}

/// Explains branch regime selection and report-wide historical comparison.
fn note_branch_evaluation<R: Reporter + ?Sized>(reporter: &R, trace: &BranchEvaluationTrace) {
    reporter.if_enabled(|notes| {
        for series in &trace.series {
            if let Some(reason) = series.unresolved {
                notes.note(&format!(
                    "branch evidence for {} {} in {} was withheld: {}; {} base commits were \
                     available, {} remained after the cap and blessings, split into {} selector \
                     and {} reference commits",
                    series.id.qualified(),
                    series.kind.as_str(),
                    series.set,
                    reason.describe(),
                    series.available_base_commits,
                    series.retained_base_commits,
                    series.selector_commits.len(),
                    series.reference_commits.len(),
                ));
                continue;
            }
            if let Some((minimum, maximum)) = series.current_range {
                notes.note(&format!(
                    "branch evidence for {} {} in {} used current-base range {minimum}..={maximum} \
                     from {} observations; selector commits {:?}, reference commits {:?}, \
                     current-regime start {:?}, branch relation {:?}",
                    series.id.qualified(),
                    series.kind.as_str(),
                    series.set,
                    series.reference_count,
                    series.selector_commits,
                    series.reference_commits,
                    series.current_regime_start,
                    series.branch_relation,
                ));
            }
        }
        for comparison in &trace.comparisons {
            notes.note(&format!(
                "historical comparison for {} scored {} existing base commits against the same \
                 rectangular family; {} tied or exceeded the branch score {} (base scores {:?})",
                comparison.set,
                comparison.base_scores.len(),
                comparison.at_least_as_much,
                comparison.branch_score,
                comparison.base_scores,
            ));
        }
    });
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    #![allow(clippy::indexing_slicing, reason = "panic is fine in tests")]

    use std::future::{Future, ready};
    use std::io;
    use std::path::PathBuf;

    use cbh_config::{Config, parse_config};
    use cbh_diag::RecordingReporter;
    use cbh_git::FakeGitHistory;
    use cbh_model::{
        BenchmarkId, BenchmarkIdPrefix, BenchmarkResult, BlessingRecord, Engine, EnvironmentInfo,
        GitInfo, Metric, MetricKind, Run, RunContext, ToolchainInfo, sanitize_segment,
    };
    use cbh_probe::{HardwareProfile, RustcInfo};
    use cbh_storage::{MemoryStorage, Storage};
    use futures::executor::block_on;
    use jiff::Timestamp;
    use nonempty::nonempty;
    use ohno::ErrorExt as _;

    use super::*;
    use crate::{
        BaseBranchUnavailableError, FirstParentWalkFailedError, InvalidBlessingError,
        InvalidResultSetError, InvalidStoredUtf8Error, MergeBaseUnavailableError,
        NoOutputSelectedError, UnknownEngineError, UnresolvedRefError,
    };

    fn ts(seconds: i64) -> Timestamp {
        Timestamp::from_second(seconds).unwrap()
    }

    /// A minimal configuration; `analyze_with` only reads `project.default_branch`.
    fn config() -> Config {
        Config::default()
    }

    struct FailingProbe;

    impl EnvironmentProbe for FailingProbe {
        fn git(&self) -> impl Future<Output = io::Result<GitInfo>> {
            ready(Ok(GitInfo::default()))
        }

        fn toolchain(&self) -> impl Future<Output = io::Result<RustcInfo>> {
            ready(Err(io::Error::other("injected toolchain failure")))
        }

        fn hardware(&self) -> impl Future<Output = HardwareProfile> {
            ready(HardwareProfile {
                processors: 1,
                memory_regions: 1,
                processor_models: Vec::new(),
                processor_speeds: Vec::new(),
            })
        }
    }

    #[test]
    fn auto_discriminant_toolchain_failure_is_mapped_at_the_call_site() {
        let error = block_on(detect_auto_discriminants_with(&FailingProbe)).unwrap_err();

        assert!(error.find_source::<ToolchainProbeFailedError>().is_some());
        assert!(error.find_source::<io::Error>().is_some());
    }

    #[test]
    fn branch_comparisons_are_matched_to_their_exact_set() {
        let left = DiscriminantSet {
            engine: Engine::Callgrind,
            target_triple: "x86_64-unknown-linux-gnu".into(),
            machine_key: "left".into(),
        };
        let right = DiscriminantSet {
            machine_key: "right".into(),
            ..left.clone()
        };
        let comparisons = [
            BranchComparison {
                set: left.clone(),
                evaluated_base_commits: 5,
                at_least_as_much: 1,
                series: 2,
            },
            BranchComparison {
                set: right.clone(),
                evaluated_base_commits: 7,
                at_least_as_much: 3,
                series: 4,
            },
        ];

        assert_eq!(
            branch_comparison_for_set(&comparisons, &right)
                .expect("the right partition has a comparison")
                .evaluated_base_commits,
            7
        );
        let missing = DiscriminantSet {
            machine_key: "missing".into(),
            ..left
        };
        assert!(branch_comparison_for_set(&comparisons, &missing).is_none());
    }

    /// Builds a stored result set carrying one record with one `Ir` metric.
    fn ir_set(effective: i64, commit: &str, value: f64) -> Run {
        let time = ts(effective);
        let context = RunContext::new(
            time,
            GitInfo {
                commit: Some(commit.to_owned()),
                branch: Some("main".to_owned()),
                dirty: false,
            },
            EnvironmentInfo::default(),
            ToolchainInfo::default(),
            "0.0.1".to_owned(),
        );
        let record = BenchmarkResult::new(
            BenchmarkId::new(nonempty![
                "nm".to_owned(),
                "nm::observe".to_owned(),
                "pull".to_owned(),
            ]),
            vec![Metric::new(MetricKind::InstructionCount, value)],
        );
        Run::new(context, vec![record])
    }

    /// The clean object key for `commit` in the callgrind/linux partition.
    fn clean_key(commit: &str) -> String {
        format!("v1/folo/objects/callgrind/x86_64-unknown-linux-gnu/m1/{commit}/clean.json")
    }

    /// The clean object key for `commit` in an arbitrary engine/triple/machine-key partition.
    fn clean_key_in(engine: &str, triple: &str, machine: &str, commit: &str) -> String {
        format!("v1/folo/objects/{engine}/{triple}/{machine}/{commit}/clean.json")
    }

    /// A stored result set whose single record carries two metrics (`Ir` and
    /// `ConditionalBranches`), so its partition reconstructs two distinct series.
    fn two_metric_set(effective: i64, commit: &str, ir: f64, branches: f64) -> Run {
        let time = ts(effective);
        let context = RunContext::new(
            time,
            GitInfo {
                commit: Some(commit.to_owned()),
                branch: Some("main".to_owned()),
                dirty: false,
            },
            EnvironmentInfo::default(),
            ToolchainInfo::default(),
            "0.0.1".to_owned(),
        );
        let record = BenchmarkResult::new(
            BenchmarkId::new(nonempty![
                "nm".to_owned(),
                "nm::observe".to_owned(),
                "pull".to_owned(),
            ]),
            vec![
                Metric::new(MetricKind::InstructionCount, ir),
                Metric::new(MetricKind::ConditionalBranches, branches),
            ],
        );
        Run::new(context, vec![record])
    }

    /// A dirty snapshot key for `commit` taken at `unix`.
    fn dirty_key(commit: &str, unix: i64) -> String {
        format!("v1/folo/objects/callgrind/x86_64-unknown-linux-gnu/m1/{commit}/dirty-{unix}.json")
    }

    /// Stores a value at `key` in `storage`, panicking on failure (test helper).
    fn store(storage: &MemoryStorage, key: &str, set: &Run) {
        let json = set.to_json().unwrap();
        block_on(storage.put(key, json.as_bytes())).unwrap();
    }

    /// Commits each regime of a seeded step holds: the production `min_regime`
    /// gate, the fewest points the change-point detector trusts on either side of
    /// the split it locates.
    const REGIME_COMMITS: usize = 5;

    /// Commits a history-mode fixture holds: two full regimes, which is the
    /// production `min_series_points` gate — the shortest series the history
    /// detectors evaluate at all.
    const HISTORY_COMMITS: usize = 2 * REGIME_COMMITS;

    /// Base-side commits a branch-mode fixture holds. Branch mode collapses each
    /// base commit's runs to that commit's level and needs `min_series_points` such
    /// levels before it will judge the context commit against them, so a branch
    /// fixture's base line is as long as a whole history fixture.
    const BASE_COMMITS: usize = HISTORY_COMMITS;

    /// Commits a selection-only fixture holds. Deliberately below
    /// [`HISTORY_COMMITS`]: the tests that use it assert on which runs the selection
    /// admits — topology, dirty handling, discriminant filters, `--since` — never on findings.
    const SELECTION_COMMITS: usize = 4;

    // The fixture sizes above are literals so the seeded shapes read plainly, but each one
    // exists to satisfy a production gate. Bind them to the gates here, so moving a gate
    // fails the build instead of silently making a fixture vacuous.
    const _: () = assert!(
        REGIME_COMMITS == MIN_REGIME,
        "a seeded step must hold a full regime on each side of its split"
    );
    const _: () = assert!(
        HISTORY_COMMITS == MIN_SERIES_POINTS,
        "a history fixture must be long enough for the detectors to judge it"
    );
    const _: () = assert!(
        BASE_COMMITS <= MAX_BRANCH_BASE_COMMITS,
        "a branch fixture's whole base line must fit the comparison window"
    );
    const _: () = assert!(
        SELECTION_COMMITS < MIN_SERIES_POINTS,
        "the selection fixture is deliberately too short to be judged"
    );

    /// The name of the `index`th commit on a master fixture's line.
    fn commit_name(index: usize) -> String {
        format!("c{index}")
    }

    /// Appends a linear chain of `commits` commits named `c0 … c{commits-1}` to
    /// `git`, returning the tip's name.
    ///
    /// Each `cN` carries committer time `ts(N)`, the same `effective`-second
    /// convention the seeders use, so the topology-decided `--since` cutoff can be
    /// exercised.
    fn append_master_chain(git: &mut FakeGitHistory, commits: usize) -> String {
        let mut parent: Option<String> = None;
        for index in 0..commits {
            let commit = commit_name(index);
            git.commit_at(
                &commit,
                parent.as_deref(),
                ts(i64::try_from(index).unwrap()),
            );
            parent = Some(commit);
        }
        parent.expect("a chain fixture always holds at least one commit")
    }

    /// A linear master history of `commits` commits, HEAD at the tip and `master`
    /// advertised as the default branch.
    fn master_chain(commits: usize) -> FakeGitHistory {
        let mut git = FakeGitHistory::new();
        let tip = append_master_chain(&mut git, commits);
        git.branch("master", &tip)
            .head("master")
            .mark_default("master");
        git
    }

    /// A master history of `base_commits` commits with a two-commit feature branch
    /// forked off `c{fork}`, HEAD on `feature`:
    ///
    /// ```text
    /// master:  c0 - … - c{fork} - … - c{base_commits-1}
    ///                        \
    /// feature:                f1 - f2   (HEAD)
    /// ```
    fn feature_chain(base_commits: usize, fork: usize) -> FakeGitHistory {
        let mut git = FakeGitHistory::new();
        let master_tip = append_master_chain(&mut git, base_commits);
        let forked_at = i64::try_from(base_commits).unwrap();
        git.commit_at("f1", Some(&commit_name(fork)), ts(forked_at))
            .commit_at("f2", Some("f1"), ts(forked_at.saturating_add(1)))
            .branch("master", &master_tip)
            .branch("feature", "f2")
            .head("feature")
            .mark_default("master");
        git
    }

    /// A feature branch forked off the tip of a `base_commits`-long master line, so
    /// every base commit is an ancestor of the feature tip.
    fn feature_off_tip(base_commits: usize) -> FakeGitHistory {
        feature_chain(base_commits, base_commits.saturating_sub(1))
    }

    /// A short linear master history `c0 - c1 - c2 - c3`, HEAD at the tip.
    ///
    /// Deliberately too short to be judged (see [`SELECTION_COMMITS`]): it serves
    /// the tests that assert on which runs the selection admits.
    fn linear_git() -> FakeGitHistory {
        master_chain(SELECTION_COMMITS)
    }

    /// A short master history with a feature branch off `c1`, HEAD on the feature
    /// branch. Like [`linear_git`], it serves the selection-only tests.
    fn feature_git() -> FakeGitHistory {
        feature_chain(SELECTION_COMMITS, 1)
    }

    /// A linear master history long enough for the history detectors to reach a
    /// verdict ([`HISTORY_COMMITS`] commits), HEAD at the tip.
    fn history_git() -> FakeGitHistory {
        master_chain(HISTORY_COMMITS)
    }

    /// A feature branch off the master tip, over a base line long enough for branch
    /// mode to judge the tip against ([`BASE_COMMITS`] commits).
    fn branch_git() -> FakeGitHistory {
        feature_off_tip(BASE_COMMITS)
    }

    /// A feature branch off a master tip that carries no base data.
    ///
    /// Master runs one commit past the [`BASE_COMMITS`] base line the seeders fill,
    /// and the merge-base is that unmeasured tip, so a surviving branch finding's
    /// comparison base lags the merge-base by exactly one commit.
    fn lagging_branch_git() -> FakeGitHistory {
        feature_off_tip(BASE_COMMITS.saturating_add(1))
    }

    /// A linear master history whose tip carries no clean run: master runs one
    /// commit past the [`BASE_COMMITS`] base line the seeders fill, so a fixture can
    /// place dirty snapshots on a tip that holds nothing else.
    fn unmeasured_tip_git() -> FakeGitHistory {
        master_chain(BASE_COMMITS.saturating_add(1))
    }

    /// The master commit just past the seeded base line — the tip of both
    /// [`lagging_branch_git`] and [`unmeasured_tip_git`].
    fn unmeasured_tip() -> String {
        commit_name(BASE_COMMITS)
    }

    /// The values of a sustained step: [`REGIME_COMMITS`] points at `before`
    /// followed by [`REGIME_COMMITS`] at `after` — the shortest series that can hold
    /// a change point, and exactly [`HISTORY_COMMITS`] points long.
    fn step_values(before: f64, after: f64) -> Vec<f64> {
        [before; REGIME_COMMITS]
            .into_iter()
            .chain([after; REGIME_COMMITS])
            .collect()
    }

    /// Stores one clean `Ir` run per value under the default partition: `values[N]`
    /// on commit `cN`, observed at `ts(N)`.
    fn seed_master(storage: &MemoryStorage, values: &[f64]) {
        for (index, &value) in values.iter().enumerate() {
            let commit = commit_name(index);
            let second = i64::try_from(index).unwrap();
            store(
                storage,
                &clean_key(&commit),
                &ir_set(second, &commit, value),
            );
        }
    }

    /// Seeds a clean linear sustained-step history under the default partition, so
    /// the change-point detector flags a single major regression at the split.
    fn seed_linear_step(storage: &MemoryStorage) {
        seed_master(storage, &step_values(100.0, 130.0));
    }

    /// Seeds a flat base line of `base_commits` clean runs (`c0 …`) plus a raised
    /// feature regime. Returns the number of runs stored.
    fn seed_raised_feature(storage: &MemoryStorage, base_commits: usize) -> usize {
        seed_feature_over(storage, &vec![100.0; base_commits])
    }

    /// Seeds `base` as the base line (`c0 …`) plus a raised feature regime: clean `f1`
    /// and `f2` runs and a dirty `f2` snapshot on top of them. Returns the number of runs
    /// stored.
    fn seed_feature_over(storage: &MemoryStorage, base: &[f64]) -> usize {
        seed_master(storage, base);
        let observed = i64::try_from(base.len()).unwrap();
        let dirty_at = observed.saturating_add(2);
        store(storage, &clean_key("f1"), &ir_set(observed, "f1", 130.0));
        store(
            storage,
            &clean_key("f2"),
            &ir_set(observed.saturating_add(1), "f2", 130.0),
        );
        store(
            storage,
            &dirty_key("f2", dirty_at),
            &ir_set(dirty_at, "f2", 130.0),
        );
        base.len().saturating_add(3)
    }

    /// The observation second the extra merge-base run in a lagging-base fixture
    /// carries: past every run [`seed_lagging_branch`] stores, so it is
    /// unambiguously the newest base observation.
    const SIBLING_OBSERVED: i64 = 100;

    /// Seeds the PR runner's (`m1`) runs for [`lagging_branch_git`]: the flat base
    /// line stops at `c{BASE_COMMITS-1}`, one commit short of the merge-base tip
    /// that `m1` never measured, so a surviving branch finding's comparison base
    /// lags by one commit.
    fn seed_lagging_branch(storage: &MemoryStorage) {
        seed_raised_feature(storage, BASE_COMMITS);
    }

    fn options() -> AnalyzeOptions {
        AnalyzeOptions::default()
    }

    /// A fixed clock anchor for the history-mode default `--since` window in unit
    /// tests. The seeded data sits at the Unix epoch (`ts(0..)`); anchoring here
    /// keeps the default six-month look-back well before it, so the default window
    /// never drops a seeded point.
    fn now_anchor() -> Timestamp {
        Timestamp::from_second(0).unwrap()
    }

    /// The auto-detected discriminant values the unit-test data is seeded under
    /// (`x86_64-unknown-linux-gnu`, `m1` machine).
    fn auto() -> AutoDiscriminants {
        AutoDiscriminants {
            triple: "x86_64-unknown-linux-gnu".to_owned(),
            machine_key: "m1".into(),
        }
    }

    /// An inline spawner that runs the detection's blocking tasks on the calling
    /// thread, so `analyze_with` needs no Tokio runtime under `block_on` or Miri.
    fn spawner() -> Spawner {
        cbh_detect::testing::synchronous_spawner()
    }

    /// Runs `analyze_with` requesting the JSON report, returning the JSON text, the
    /// regression count, and the recording reporter so a test can assert on the
    /// machine-readable report and the verbose trail together. The text report is
    /// suppressed, so the JSON is the only rendered output.
    fn analyze_json(
        git: &FakeGitHistory,
        storage: &MemoryStorage,
        project: &str,
        options: &AnalyzeOptions,
    ) -> (String, usize, RecordingReporter) {
        let mut options = options.clone();
        options.no_text = true;
        options.markdown = None;
        options.json = Some(PathBuf::from("report.json"));
        let (rendered, regressions, reporter) = analyze_reports(git, storage, project, &options);
        let report = rendered
            .json
            .expect("the JSON report was rendered for the requested path");
        (report, regressions, reporter)
    }

    /// Requests both report surfaces from one load and detection pass.
    ///
    /// Warning tests compare presentations of the same analysis, not independent executions.
    fn analyze_text_and_json(
        git: &FakeGitHistory,
        storage: &MemoryStorage,
    ) -> (String, String, usize) {
        let mut options = options();
        options.json = Some(PathBuf::from("report.json"));
        let (rendered, regressions, _) = analyze_reports(git, storage, "folo", &options);
        (
            rendered.text.expect("the text report was requested"),
            rendered.json.expect("the JSON report was requested"),
            regressions,
        )
    }

    /// Runs the in-memory pipeline once with the requested output formats.
    fn analyze_reports(
        git: &FakeGitHistory,
        storage: &MemoryStorage,
        project: &str,
        options: &AnalyzeOptions,
    ) -> (RenderedReports, usize, RecordingReporter) {
        let reporter = RecordingReporter::new();
        let (rendered, regressions) = block_on(analyze_with(
            git,
            storage,
            project,
            &config(),
            options,
            &auto(),
            now_anchor(),
            &reporter,
            false,
            &spawner(),
        ))
        .unwrap();
        (rendered, regressions, reporter)
    }

    /// Asserts that a rendered report reached the history detectors at all: exactly
    /// one series survived selection, the report itself states that it judged that
    /// series, and it carries at least [`HISTORY_COMMITS`] runs — the shortest series
    /// the detectors evaluate.
    ///
    /// A "nothing was flagged" assertion only says something about the gates when the
    /// data cleared that bar; without this check the same silence is also what an
    /// unanalyzed or ghost-filtered series produces.
    fn assert_history_was_judged(parsed: &serde_json::Value) {
        assert_eq!(parsed["series"], 1, "{parsed}");
        assert_eq!(
            parsed["census"]["judged"], 1,
            "the report must account for the series as judged: {parsed}"
        );
        assert_eq!(
            parsed["census"]["unjudged"], 0,
            "nothing may have been silently dropped: {parsed}"
        );
        let runs = parsed["runs"]
            .as_u64()
            .expect("the report tallies the runs it loaded");
        assert!(
            runs >= u64::try_from(HISTORY_COMMITS).unwrap(),
            "the analyzed series must be long enough to be judged: {parsed}"
        );
    }

    #[test]
    fn should_colorize_only_in_an_interactive_terminal_without_no_color() {
        assert!(should_colorize(true, false), "terminal, NO_COLOR unset");
        assert!(!should_colorize(false, false), "not a terminal");
        assert!(!should_colorize(true, true), "NO_COLOR set");
        assert!(!should_colorize(false, true), "neither");
    }

    #[test]
    fn discriminant_filter_skips_an_unrecognized_storage_key() {
        let storage = MemoryStorage::new();
        store(&storage, &clean_key("c3"), &ir_set(3, "c3", 100.0));
        // A `.json` object under the project's objects prefix whose key is not a
        // valid eight-segment storage key is noted and skipped, not parsed as data.
        block_on(storage.put("v1/folo/objects/bogus.json", b"{}")).unwrap();
        let reporter = RecordingReporter::new();
        block_on(analyze_with(
            &linear_git(),
            &storage,
            "folo",
            &config(),
            &options(),
            &auto(),
            now_anchor(),
            &reporter,
            false,
            &spawner(),
        ))
        .unwrap();
        assert!(
            reporter.contains("not a recognized"),
            "{:?}",
            reporter.notes()
        );
    }

    /// Runs `analyze_with` and unwraps the rendered text report and regression count.
    fn analyze(
        git: &FakeGitHistory,
        storage: &MemoryStorage,
        project: &str,
        options: &AnalyzeOptions,
    ) -> (String, usize) {
        let (rendered, regressions, _) = analyze_reports(git, storage, project, options);
        (rendered.text.unwrap_or_default(), regressions)
    }

    #[test]
    fn analyze_rejects_an_unresolved_head() {
        let storage = MemoryStorage::new();
        let git = FakeGitHistory::new(); // No commits: HEAD does not resolve.
        let error = block_on(analyze_with(
            &git,
            &storage,
            "folo",
            &config(),
            &options(),
            &auto(),
            now_anchor(),
            &RecordingReporter::new(),
            false,
            &spawner(),
        ))
        .unwrap_err();
        let found = error.find_source::<UnresolvedRefError>().unwrap();
        assert_eq!(found.reference, "HEAD");
    }

    #[test]
    fn analyze_propagates_a_typed_git_failure() {
        // The typed git failures raised deep in history resolution must survive
        // propagation out of the top-level entry point, not be flattened on the way.
        let storage = MemoryStorage::new();
        let mut git = linear_git();
        git.fail_first_parent();
        let error = block_on(analyze_with(
            &git,
            &storage,
            "folo",
            &config(),
            &options(),
            &auto(),
            now_anchor(),
            &RecordingReporter::new(),
            false,
            &spawner(),
        ))
        .unwrap_err();
        assert!(error.find_source::<FirstParentWalkFailedError>().is_some());
    }

    #[test]
    fn empty_history_reports_that_nothing_was_analyzed() {
        let storage = MemoryStorage::new();
        let git = linear_git();
        let (report, regressions) = analyze(&git, &storage, "folo", &options());
        assert_eq!(regressions, 0);
        // An analysis that reconstructed no series ruled nothing out, so it leads with
        // that rather than with an all-clear it did not earn.
        assert!(
            report.contains("Nothing was analyzed, so no change could be detected."),
            "{report}"
        );
        assert!(!report.contains("No notable changes detected."), "{report}");
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "detecting a stored regression requires decoding a full minimum-length history"
    )]
    fn official_view_detects_a_clean_regression_in_topology_order() {
        let storage = MemoryStorage::new();
        seed_linear_step(&storage);
        let git = history_git();
        let (report, regressions) = analyze(&git, &storage, "folo", &options());
        assert_eq!(regressions, 1);
        assert!(report.contains("regression"), "{report}");
        assert!(report.contains("nm/nm::observe/pull"), "{report}");
        assert!(report.contains("instruction_count"), "{report}");
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "the positive notable control loads a full history through detection and rendering"
    )]
    fn json_notable_flag_reflects_whether_findings_survived() {
        // The `notable` signal appears only in the JSON report (the text report
        // keys off the finding list directly), so assert it there.
        let storage = MemoryStorage::new();
        seed_linear_step(&storage);
        let (report, regressions, _) = analyze_json(&history_git(), &storage, "folo", &options());
        assert_eq!(regressions, 1);
        assert!(report.contains("\"notable\": true"), "{report}");

        let empty = MemoryStorage::new();
        let (report, _, _) = analyze_json(&linear_git(), &empty, "folo", &options());
        assert!(report.contains("\"notable\": false"), "{report}");
    }

    #[test]
    fn select_dataset_notes_blessing_sidecars_in_the_partition() {
        // A blessing sidecar shares the run partition prefix; the verbose trail
        // calls it out only when at least one is present.
        let storage = MemoryStorage::new();
        // One measured commit suffices to exercise sidecar discovery and loading.
        store(&storage, &clean_key("c3"), &ir_set(3, "c3", 100.0));
        let record = BlessingRecord::new(
            "c3".to_owned(),
            ts(3),
            vec![BenchmarkIdPrefix::new("nm").unwrap()],
            "0.0.1".to_owned(),
        );
        let bless_key =
            "v1/folo/objects/callgrind/x86_64-unknown-linux-gnu/m1/c3/bless-3.json".to_owned();
        block_on(storage.put(&bless_key, record.to_json().unwrap().as_bytes())).unwrap();

        let reporter = RecordingReporter::new();
        block_on(analyze_with(
            &linear_git(),
            &storage,
            "folo",
            &config(),
            &options(),
            &auto(),
            now_anchor(),
            &reporter,
            false,
            &spawner(),
        ))
        .unwrap();
        assert!(
            reporter.contains("are blessing sidecars"),
            "{:?}",
            reporter.notes()
        );

        // No sidecar → the note is absent.
        let clean = MemoryStorage::new();
        store(&clean, &clean_key("c3"), &ir_set(3, "c3", 100.0));
        let reporter = RecordingReporter::new();
        block_on(analyze_with(
            &linear_git(),
            &clean,
            "folo",
            &config(),
            &options(),
            &auto(),
            now_anchor(),
            &reporter,
            false,
            &spawner(),
        ))
        .unwrap();
        assert!(
            !reporter.contains("are blessing sidecars"),
            "{:?}",
            reporter.notes()
        );
    }

    #[test]
    fn history_mode_does_not_resolve_a_base_ref_comparison_window() {
        // History mode compares within the base branch's own first-parent line, so it has
        // no separate base-ref comparison window to resolve. The base-ref window load is
        // gated on branch mode *and* the caller opting in; in history mode neither the
        // base ref's ancestry walk nor its window note may appear.
        let storage = MemoryStorage::new();
        store(&storage, &clean_key("c3"), &ir_set(3, "c3", 100.0));
        let reporter = RecordingReporter::new();
        block_on(analyze_with(
            &linear_git(),
            &storage,
            "folo",
            &config(),
            &options(),
            &auto(),
            now_anchor(),
            &reporter,
            false,
            &spawner(),
        ))
        .unwrap();
        assert!(
            !reporter.timed("base ref's first-parent line"),
            "history mode must not walk the base ref's first-parent line: {:?}",
            reporter.notes()
        );
        assert!(
            !reporter.contains("for branch comparison windows"),
            "history mode must not resolve a base-ref comparison window: {:?}",
            reporter.notes()
        );
    }

    #[test]
    fn analyze_records_a_timing_for_each_pipeline_stage() {
        // Every stage drawn in docs/analyze.md emits a timing on the dedicated
        // timing channel, so a `--verbose` run can localize a mystery slowdown.
        // History mode is used because it also exercises the blessing-load stage.
        let storage = MemoryStorage::new();
        // A measured tip and its sidecar reach every stage without a detection-sized history.
        store(&storage, &clean_key("c3"), &ir_set(3, "c3", 100.0));
        let record = BlessingRecord::new(
            "c3".to_owned(),
            ts(3),
            vec![BenchmarkIdPrefix::new("nm").unwrap()],
            "0.0.1".to_owned(),
        );
        let bless_key =
            "v1/folo/objects/callgrind/x86_64-unknown-linux-gnu/m1/c3/bless-3.json".to_owned();
        block_on(storage.put(&bless_key, record.to_json().unwrap().as_bytes())).unwrap();

        let reporter = RecordingReporter::new();
        block_on(analyze_with(
            &linear_git(),
            &storage,
            "folo",
            &config(),
            &options(),
            &auto(),
            now_anchor(),
            &reporter,
            false,
            &spawner(),
        ))
        .unwrap();

        for stage in [
            // analyze_with stages.
            "select_dataset",
            "re-baseline",
            "change detection",
            "report render",
            // select_dataset sub-stages.
            "candidate listing",
            "storage.list",
            "git topology",
            "git.first_parent",
            "phase 1",
            "phase 2/3",
            "series build finalization",
            // History-mode-only blessing load.
            "blessing sidecar load",
        ] {
            assert!(reporter.timed(stage), "missing timing for {stage:?}");
        }

        // Timings are a distinct channel: they never leak into the per-object note
        // stream a `--verbose` run also prints.
        assert!(!reporter.contains("timing:"), "{:?}", reporter.notes());
    }

    /// Drives history-mode analyze expecting the blessing load to fail.
    fn analyze_blessing_error(storage: &MemoryStorage) -> AnalyzeError {
        block_on(analyze_with(
            &linear_git(),
            storage,
            "folo",
            &config(),
            &options(),
            &auto(),
            now_anchor(),
            &RecordingReporter::new(),
            false,
            &spawner(),
        ))
        .unwrap_err()
    }

    #[test]
    fn history_mode_rejects_a_non_utf8_blessing_on_the_analyzed_history() {
        let storage = MemoryStorage::new();
        store(&storage, &clean_key("c3"), &ir_set(3, "c3", 100.0));
        // c3 is on the analyzed history, so history mode loads its sidecar.
        let bless_key =
            "v1/folo/objects/callgrind/x86_64-unknown-linux-gnu/m1/c3/bless-3.json".to_owned();
        block_on(storage.put(&bless_key, &[0xff, 0xfe, 0x00])).unwrap();
        let error = analyze_blessing_error(&storage);
        let found = error.find_source::<InvalidStoredUtf8Error>().unwrap();
        assert_eq!(found.object_kind, "stored blessing");
        assert_eq!(found.key, bless_key);
        assert!(error.find_source::<std::string::FromUtf8Error>().is_some());
    }

    #[test]
    fn history_mode_rejects_an_invalid_blessing_on_the_analyzed_history() {
        let storage = MemoryStorage::new();
        store(&storage, &clean_key("c3"), &ir_set(3, "c3", 100.0));
        let bless_key =
            "v1/folo/objects/callgrind/x86_64-unknown-linux-gnu/m1/c3/bless-3.json".to_owned();
        block_on(storage.put(&bless_key, b"{ not a blessing record")).unwrap();
        let error = analyze_blessing_error(&storage);
        let found = error.find_source::<InvalidBlessingError>().unwrap();
        assert_eq!(found.object_kind, "stored blessing");
        assert_eq!(found.key, bless_key);
        assert_eq!(found.expected, "blessing record");
        assert!(error.find_source::<serde_json::Error>().is_some());
    }

    #[test]
    fn history_mode_skips_a_blessing_off_the_analyzed_history() {
        let storage = MemoryStorage::new();
        store(&storage, &clean_key("c3"), &ir_set(3, "c3", 100.0));
        // A blessing on a commit that is not on the analyzed history is noted and
        // skipped rather than applied.
        let record = BlessingRecord::new(
            "z9".to_owned(),
            ts(3),
            vec![BenchmarkIdPrefix::new("nm").unwrap()],
            "0.0.1".to_owned(),
        );
        let bless_key =
            "v1/folo/objects/callgrind/x86_64-unknown-linux-gnu/m1/z9/bless-3.json".to_owned();
        block_on(storage.put(&bless_key, record.to_json().unwrap().as_bytes())).unwrap();

        let reporter = RecordingReporter::new();
        block_on(analyze_with(
            &linear_git(),
            &storage,
            "folo",
            &config(),
            &options(),
            &auto(),
            now_anchor(),
            &reporter,
            false,
            &spawner(),
        ))
        .unwrap();
        assert!(
            reporter.contains("is not on HEAD's analyzed history"),
            "{:?}",
            reporter.notes()
        );
    }

    #[test]
    fn per_set_report_counts_runs_and_series_independently() {
        let storage = MemoryStorage::new();
        // Both sets run through the `linear_git` tip (`c3`) so neither benchmark is a
        // ghost there and the always-on tip filter keeps every series.
        //
        // Set A has fewer runs but more metric series than set B, so neither tally
        // can be substituted for the other or taken from the wrong partition.
        store(
            &storage,
            &clean_key("c3"),
            &two_metric_set(3, "c3", 100.0, 200.0),
        );
        // Set B carries one metric across distinct commits.
        for index in 2..4 {
            let commit = format!("c{index}");
            let second = i64::from(index);
            store(
                &storage,
                &clean_key_in("callgrind", "aarch64-apple-darwin", "m1", &commit),
                &ir_set(second, &commit, 100.0),
            );
        }

        let git = linear_git();
        // The two sets live under different triples, and every set obeys the
        // target-triple filter, so an auto-detected triple would report only its own.
        // Widen to `all` to exercise the per-set tallies across both partitions.
        let opts = AnalyzeOptions {
            target_triple: vec!["all".to_owned()],
            ..options()
        };
        let (report, _, _) = analyze_json(&git, &storage, "folo", &opts);

        let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
        let sets = parsed["sets"].as_array().unwrap();

        let set_a = sets
            .iter()
            .find(|set| set["target_triple"] == "x86_64-unknown-linux-gnu")
            .unwrap();
        assert_eq!(set_a["runs"], 1, "{report}");
        assert_eq!(set_a["series"], 2, "{report}");

        let set_b = sets
            .iter()
            .find(|set| set["target_triple"] == "aarch64-apple-darwin")
            .unwrap();
        assert_eq!(set_b["runs"], 2, "{report}");
        assert_eq!(set_b["series"], 1, "{report}");
    }

    /// A stored result set naming several benchmarks, each carrying one `Ir` metric,
    /// so one commit's object can present or omit specific benchmarks — the shape a
    /// ghost (a benchmark that disappears before the tip) needs.
    fn multi_bench(effective: i64, commit: &str, benches: &[(&str, f64)]) -> Run {
        let time = ts(effective);
        let context = RunContext::new(
            time,
            GitInfo {
                commit: Some(commit.to_owned()),
                branch: Some("main".to_owned()),
                dirty: false,
            },
            EnvironmentInfo::default(),
            ToolchainInfo::default(),
            "0.0.1".to_owned(),
        );
        let records = benches
            .iter()
            .map(|(name, value)| {
                BenchmarkResult::new(
                    BenchmarkId::new(nonempty![(*name).to_owned()]),
                    vec![Metric::new(MetricKind::InstructionCount, *value)],
                )
            })
            .collect::<Vec<_>>();
        Run::new(context, records)
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "a fully judged census requires loading the detector's minimum history"
    )]
    fn the_verbose_census_states_when_nothing_was_left_unjudged() {
        // The healthy case still explains itself: a reader of the trail sees that the
        // suite was judged in full, not that the breakdown was omitted.
        let storage = MemoryStorage::new();
        seed_linear_step(&storage);

        let (_, _, reporter) = analyze_json(&history_git(), &storage, "folo", &options());
        assert!(
            reporter
                .contains("series census: judged 1 of 1 in-scope series; every series was judged"),
            "{:?}",
            reporter.notes()
        );
    }

    #[test]
    fn branch_trace_reasons_match_the_complete_series_identity() {
        // The shortest searchable base leaves a repeated but incomplete trailing regime.
        let mut values = vec![100.0; 16];
        values.extend(std::iter::repeat_n(200.0, 4));
        values.push(220.0);
        let exact = cbh_detect::examples::with_base_window(
            cbh_detect::examples::series("exact", &values, MetricKind::InstructionCount, 0),
            19,
        );
        let context = cbh_detect::examples::branch_context(&exact, 19);
        assert_eq!(
            testability(&exact, &context),
            Testability::Unjudged(UnjudgedReason::CurrentBaseRegimeUnresolved)
        );
        let detection = cbh_detect::find_changes(std::slice::from_ref(&exact), &context);
        assert_eq!(detection.census.judged(), 0);
        assert_eq!(detection.census.unjudged(), 1);
        assert_eq!(
            detection.branch_trace.series[0].unresolved,
            Some(UnjudgedReason::CurrentBaseRegimeUnresolved)
        );

        let mut different_set = exact.clone();
        different_set.set.machine_key = "other".into();
        let mut different_id = exact.clone();
        different_id.id = BenchmarkId::new(nonempty!["different".to_owned()]);
        let mut different_kind = exact.clone();
        different_kind.kind = MetricKind::ConditionalBranches;
        let series = [exact, different_set, different_id, different_kind];
        let reporter = RecordingReporter::new();

        note_series_census(&reporter, &series, &context, &detection.census);

        let recorded_notes = reporter.notes();
        let notes: Vec<&str> = recorded_notes
            .iter()
            .map(String::as_str)
            .filter(|note| note.starts_with("not judging"))
            .collect();
        assert_eq!(notes.len(), 4, "{notes:?}");
        for expected in [
            "exact/case instruction_count in callgrind/t/m1",
            "exact/case instruction_count in callgrind/t/other",
            "different instruction_count in callgrind/t/m1",
            "exact/case conditional_branches in callgrind/t/m1",
        ] {
            assert!(
                notes.iter().any(|note| note.contains(expected)),
                "{notes:?}"
            );
        }
        assert!(
            notes
                .iter()
                .all(|note| note.contains("current base regime is unresolved"))
        );
        assert!(notes.iter().all(|note| {
            note.contains(
                "20 base-branch commits retained from 20 base-branch commits available before \
                 the cap and blessings",
            )
        }));
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "blessing must truncate a previously judgeable full stored history"
    )]
    fn a_blessed_series_with_too_little_evidence_left_names_its_active_window() {
        // A blessing re-baselines a series, so the points before it are no longer
        // evidence. Such a series is long yet unjudged, and the trail must state the
        // window that was actually short rather than the full length that was not.
        let storage = MemoryStorage::new();
        seed_linear_step(&storage);
        let blessed_at = commit_name(HISTORY_COMMITS - 3);
        let record = BlessingRecord::new(
            blessed_at.clone(),
            ts(i64::try_from(HISTORY_COMMITS).unwrap()),
            vec![BenchmarkIdPrefix::new("nm").unwrap()],
            "0.0.1".to_owned(),
        );
        let bless_key = format!(
            "v1/folo/objects/callgrind/x86_64-unknown-linux-gnu/m1/{blessed_at}/bless-3.json"
        );
        block_on(storage.put(&bless_key, record.to_json().unwrap().as_bytes())).unwrap();

        let (report, regressions, reporter) =
            analyze_json(&history_git(), &storage, "folo", &options());
        assert_eq!(regressions, 0, "the blessed step is re-baselined: {report}");
        assert_eq!(parse_census_judged(&report), 0, "{report}");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&report).unwrap()["census"]["reasons"][0]["reason"],
            "too_few_points_since_blessing",
            "{report}"
        );
        assert!(
            reporter.contains(
                "with too few points since being blessed — it carries 10 points, \
                 of which 3 since its blessing"
            ),
            "{:?}",
            reporter.notes()
        );
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "base-ref blessing needs a full stored comparison window before truncation"
    )]
    fn a_base_ref_blessing_off_the_feature_history_truncates_branch_evidence() {
        // The feature forked before the blessing, so the blessed commit exists only on
        // the base ref's first-parent line. Branch mode must still apply it because the
        // blessing governs the base evidence, not the feature's ancestry.
        let storage = MemoryStorage::new();
        seed_raised_feature(&storage, BASE_COMMITS);
        let blessed_at = commit_name(BASE_COMMITS - 2);
        let record = BlessingRecord::new(
            blessed_at.clone(),
            ts(i64::try_from(BASE_COMMITS).unwrap()),
            vec![BenchmarkIdPrefix::new("nm").unwrap()],
            "0.0.1".to_owned(),
        );
        let bless_key = format!(
            "v1/folo/objects/callgrind/x86_64-unknown-linux-gnu/m1/{blessed_at}/bless-3.json"
        );
        block_on(storage.put(&bless_key, record.to_json().unwrap().as_bytes())).unwrap();
        let git = feature_chain(BASE_COMMITS, REGIME_COMMITS - 1);

        let (report, regressions, reporter) = analyze_json(&git, &storage, "folo", &options());
        assert_eq!(
            regressions, 0,
            "pre-blessing base data must not be used: {report}"
        );
        assert_eq!(parse_census_judged(&report), 0, "{report}");
        let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
        assert_eq!(
            parsed["census"]["reasons"][0]["reason"], "too_few_base_commits_since_blessing",
            "{report}"
        );
        assert!(
            reporter.contains(
                "with too few base-ref commits remaining since being blessed; 10 base commits \
                 were available, 2 remained after the cap and blessings"
            ),
            "{:?}",
            reporter.notes()
        );
        assert!(
            reporter.contains(
                "it carries 2 base-branch commits retained from 10 base-branch commits available \
                 before the cap and blessings"
            ),
            "{:?}",
            reporter.notes()
        );
    }

    /// The `census.judged` tally of a rendered JSON report.
    fn parse_census_judged(report: &str) -> u64 {
        serde_json::from_str::<serde_json::Value>(report).unwrap()["census"]["judged"]
            .as_u64()
            .expect("every report carries a census")
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "the historical-comparison trail requires loading a full base window and branch"
    )]
    fn the_branch_trail_explains_the_regime_and_historical_comparison() {
        // The verbose trail carries the inputs and reasoning needed to reconstruct both
        // the per-series range and the report-wide comparison.
        let storage = MemoryStorage::new();
        seed_raised_feature(&storage, BASE_COMMITS);

        let (report, _, reporter) = analyze_json(&branch_git(), &storage, "folo", &options());
        assert!(
            reporter.contains(
                "used current-base range 100..=100 from 10 observations; selector commits"
            ),
            "{report}\n{:?}",
            reporter.notes()
        );
        assert!(
            reporter.contains("reference commits")
                && reporter.contains("current-regime start Some(")
                && !reporter.contains("boundary Some(")
                && reporter.contains("branch relation Above")
                && reporter.contains(
                    "historical comparison for callgrind/x86_64-unknown-linux-gnu/m1 scored"
                )
                && reporter.contains("against the same rectangular family"),
            "{report}\n{:?}",
            reporter.notes()
        );
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "mixed judged and excluded series require decoding a complete multi-benchmark history"
    )]
    fn the_census_accounts_for_every_series_and_explains_each_exclusion() {
        // Three series, one of each fate: `kept` runs the full history and is judged,
        // `ghost` stops before the tip, and `young` appears only at the end. Silence
        // over this suite covers one series in three, and the report must say so — on
        // both the machine-readable surface and the verbose trail.
        let storage = MemoryStorage::new();
        for index in 0..HISTORY_COMMITS {
            let commit = commit_name(index);
            let second = i64::try_from(index).unwrap();
            let mut benches = vec![("kept", 100.0)];
            if index < 3 {
                benches.push(("ghost", 100.0));
            }
            if index >= HISTORY_COMMITS - 2 {
                benches.push(("young", 100.0));
            }
            store(
                &storage,
                &clean_key(&commit),
                &multi_bench(second, &commit, &benches),
            );
        }

        let (report, regressions, reporter) =
            analyze_json(&history_git(), &storage, "folo", &options());
        assert_eq!(regressions, 0);
        let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
        let census = &parsed["census"];
        assert_eq!(census["total"], 3, "{report}");
        assert_eq!(census["judged"], 1, "{report}");
        assert_eq!(census["unjudged"], 2, "{report}");
        assert_eq!(census["reasons"][0]["reason"], "ghost", "{report}");
        assert_eq!(census["reasons"][0]["count"], 1, "{report}");
        assert_eq!(census["reasons"][1]["reason"], "too_few_points", "{report}");
        assert_eq!(census["reasons"][1]["count"], 1, "{report}");

        // The trail names the series that was declined, what it carried, and the rule
        // that declined it, so the verdict can be reconstructed rather than trusted.
        assert!(
            reporter.contains("not judging young instruction_count"),
            "{:?}",
            reporter.notes()
        );
        assert!(
            reporter.contains("with too few points in the analyzed window — it carries 2 points"),
            "{:?}",
            reporter.notes()
        );
        assert!(
            !reporter.contains("since its blessing"),
            "an unblessed series carries no blessing window to name: {:?}",
            reporter.notes()
        );
        assert!(
            reporter.contains("series census: judged 1 of 2 in-scope series"),
            "{:?}",
            reporter.notes()
        );
        assert!(
            reporter.contains("history mode judges a series only from 10 points"),
            "{:?}",
            reporter.notes()
        );
    }

    #[test]
    fn the_census_counts_ghost_series_while_the_tally_counts_ghost_benchmarks() {
        // One benchmark carrying two metrics disappears before the tip. It is a single
        // ghost benchmark but two unjudged series, and the two tallies must each count
        // in their own unit rather than borrow the other's.
        let storage = MemoryStorage::new();
        store(
            &storage,
            &clean_key("c0"),
            &two_metric_set(0, "c0", 100.0, 200.0),
        );

        let (report, _, reporter) = analyze_json(&linear_git(), &storage, "folo", &options());
        let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
        assert_eq!(parsed["ghosts_excluded"], 1, "one benchmark: {report}");
        assert_eq!(parsed["series"], 0, "none survived the filter: {report}");
        assert_eq!(parsed["census"]["total"], 2, "two series: {report}");
        assert_eq!(parsed["census"]["judged"], 0, "{report}");
        assert_eq!(
            parsed["census"]["reasons"][0]["reason"], "ghost",
            "{report}"
        );
        assert_eq!(parsed["census"]["reasons"][0]["count"], 2, "{report}");

        // The trail names each excluded series, so a reader reconciling it against the
        // census counts the same two things the census counted.
        assert!(
            reporter.contains("excluding nm/nm::observe/pull instruction_count in "),
            "{:?}",
            reporter.notes()
        );
        assert!(
            reporter.contains("excluding nm/nm::observe/pull conditional_branches in "),
            "{:?}",
            reporter.notes()
        );
        assert!(
            reporter.contains("ghost filter: excluded 2 ghost series across 1 benchmark"),
            "{:?}",
            reporter.notes()
        );
        assert!(
            reporter.contains("leaving 0 series"),
            "{:?}",
            reporter.notes()
        );
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "the measured-metric positive control needs a full two-metric base and branch history"
    )]
    fn a_branch_analysis_accounts_for_a_metric_the_branch_never_measured() {
        // The benchmark still runs on the branch, but it stopped reporting one of its
        // two metrics there. That metric's series is not silent — it is unjudged for a
        // reason of its own, which the report names rather than folding into the
        // shortfalls of the judged history.
        let storage = MemoryStorage::new();
        for index in 0..BASE_COMMITS {
            let commit = commit_name(index);
            let second = i64::try_from(index).unwrap();
            store(
                &storage,
                &clean_key(&commit),
                &two_metric_set(second, &commit, 100.0, 200.0),
            );
        }
        let observed = i64::try_from(BASE_COMMITS).unwrap();
        store(&storage, &clean_key("f1"), &ir_set(observed, "f1", 130.0));
        store(
            &storage,
            &clean_key("f2"),
            &ir_set(observed.saturating_add(1), "f2", 130.0),
        );

        let (report, regressions, reporter) =
            analyze_json(&branch_git(), &storage, "folo", &options());
        assert_eq!(regressions, 1, "the measured metric still moved: {report}");
        let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
        assert_eq!(parsed["census"]["total"], 2, "{report}");
        assert_eq!(parsed["census"]["judged"], 1, "{report}");
        assert_eq!(
            parsed["census"]["reasons"][0]["reason"], "not_measured_on_branch",
            "{report}"
        );
        assert_eq!(parsed["census"]["reasons"][0]["count"], 1, "{report}");
        assert!(
            reporter.contains("not judging nm/nm::observe/pull conditional_branches"),
            "{:?}",
            reporter.notes()
        );
        assert!(
            reporter.contains("branch mode judges a series only with a measurement on the branch"),
            "{:?}",
            reporter.notes()
        );
    }

    #[test]
    fn a_ghost_benchmark_is_excluded() {
        // `kept` is measured through the tip; `ghost` disappears after c2.
        // The tip is c3, so `ghost` is no longer part of the current suite there.
        let storage = MemoryStorage::new();
        store(
            &storage,
            &clean_key("c2"),
            &multi_bench(2, "c2", &[("kept", 100.0), ("ghost", 100.0)]),
        );
        store(
            &storage,
            &clean_key("c3"),
            &multi_bench(3, "c3", &[("kept", 100.0)]),
        );
        let git = linear_git();

        // The ghost is filtered out before detection, and the verbose trail names it
        // and the context commit it is absent from.
        let (report, _, reporter) = analyze_json(&git, &storage, "folo", &options());
        let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
        assert_eq!(parsed["ghosts_excluded"], 1, "{report}");
        assert_eq!(parsed["series"], 1, "only `kept` survives, {report}");
        assert!(
            reporter.contains("excluding ghost instruction_count in "),
            "{:?}",
            reporter.notes()
        );
        assert!(
            reporter.contains("not present at the context commit c3"),
            "{:?}",
            reporter.notes()
        );
    }

    #[test]
    fn an_all_ghosts_analysis_emits_the_dedicated_hint() {
        // Every benchmark disappears before the tip (data stops at c2, tip is c3), so
        // the filter empties the analysis. The empty outcome must read as an
        // all-ghosts case, distinct from a bare "no data".
        let storage = MemoryStorage::new();
        store(&storage, &clean_key("c2"), &ir_set(2, "c2", 100.0));
        let git = linear_git();

        let (report, _, _) = analyze_json(&git, &storage, "folo", &options());
        let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
        assert_eq!(parsed["ghosts_excluded"], 1, "{report}");
        assert_eq!(parsed["series"], 0, "{report}");
        assert_eq!(parsed["runs"], 1, "the run still loaded, {report}");
        let hint = parsed["hint"].as_str().unwrap_or_default();
        assert!(hint.contains("filtered as a ghost"), "{report}");
        assert!(
            hint.contains("context commit"),
            "the hint names the context commit: {report}"
        );
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "reversed observation times must reconstruct a full detectable stored regression"
    )]
    fn series_order_follows_topology_not_observation_time() {
        // Topology is a rising sustained step along `c0 …`, but the objects'
        // observation clock is reversed (c0 newest, the tip oldest). Ordering by
        // topology reconstructs the rising step and flags a regression; were the
        // provenance-only observation time ever allowed to order the series it would
        // reverse into a falling step (an improvement, no regression). So a single
        // detected regression proves topology won.
        let storage = MemoryStorage::new();
        for (index, value) in step_values(100.0, 130.0).into_iter().enumerate() {
            let commit = commit_name(index);
            // Reverse the clock: c0 has the newest observation time, the tip the oldest.
            let second = 100 - i64::try_from(index).unwrap();
            store(
                &storage,
                &clean_key(&commit),
                &ir_set(second, &commit, value),
            );
        }
        let git = history_git();
        let (_, regressions) = analyze(&git, &storage, "folo", &options());
        assert_eq!(regressions, 1, "the step must be read in topology order");
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "excluding dirty data must leave a fully judged clean stored history"
    )]
    fn official_view_excludes_dirty_runs() {
        // A dirty snapshot on the master tip must not enter the official timeline.
        // The clean line reaches the tip and is long enough to be judged, so the
        // silent verdict is evidence that the wild dirty value stayed out — not that
        // the series was too short (or ghost-filtered) to be looked at.
        let storage = MemoryStorage::new();
        seed_master(&storage, &[100.0; HISTORY_COMMITS]);
        // A wildly different dirty value on the tip: if admitted it would flag.
        let tip = commit_name(HISTORY_COMMITS.saturating_sub(1));
        store(&storage, &dirty_key(&tip, 500), &ir_set(500, &tip, 999.0));
        let git = history_git();

        let (report, regressions, _) = analyze_json(&git, &storage, "folo", &options());
        let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
        assert_eq!(
            parsed["runs"], HISTORY_COMMITS,
            "the dirty tip run is excluded"
        );
        assert_history_was_judged(&parsed);
        assert_eq!(regressions, 0);
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "dirty-run admission is verified by detection over a full stored base window"
    )]
    fn feature_view_admits_dirty_after_the_merge_base() {
        // feature branched at the master tip; the target side rises at f1 and a dirty
        // f2 snapshot sustains the new level. Branch mode judges the tip's latest
        // cohort, which is the dirty snapshot because it sorts after the clean f2 run
        // sharing its commit — so the finding is about the admitted dirty state, and
        // the run tally proves that state entered the analysis at all.
        let storage = MemoryStorage::new();
        let runs = seed_raised_feature(&storage, BASE_COMMITS);
        let git = branch_git();

        let (report, regressions, _) = analyze_json(&git, &storage, "folo", &options());
        let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
        assert_eq!(parsed["runs"], runs, "the dirty f2 snapshot is admitted");
        assert_eq!(
            regressions, 1,
            "the admitted dirty f2 carries the raised level"
        );
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "lag classification follows full stored base and branch detection plus a sibling load"
    )]
    fn a_lagging_comparison_base_with_a_sibling_run_warns_of_a_mismatch() {
        // The PR runner's key (m1) carries base data only up to one commit behind the
        // merge-base, while a sibling key (m2) holds the same benchmark and metric at
        // the merge-base itself. The finding's comparison base therefore lags by one
        // commit because of machine-key rotation, and every surface must disclose it.
        let storage = MemoryStorage::new();
        seed_lagging_branch(&storage);
        let merge_base = unmeasured_tip();
        store(
            &storage,
            &clean_key_in("callgrind", "x86_64-unknown-linux-gnu", "m2", &merge_base),
            &ir_set(SIBLING_OBSERVED, &merge_base, 100.0),
        );
        let git = lagging_branch_git();

        let (text, report, regressions) = analyze_text_and_json(&git, &storage);
        assert_eq!(regressions, 1);
        assert!(
            text.contains(
                "Warning: comparison base is 1 commit behind base (discriminant set mismatch)"
            ),
            "{text}"
        );

        let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
        let lags = &parsed["sets"][0]["comparison_base_lags"];
        assert_eq!(lags[0]["commits_behind"], 1, "{report}");
        assert_eq!(lags[0]["reason"], "discriminant_set_mismatch", "{report}");
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "the missing-data warning requires a detected excursion over a full stored base"
    )]
    fn a_lagging_comparison_base_without_a_sibling_warns_of_missing_data() {
        // Same one-commit lag, but no sibling key holds newer base data at all, so the
        // reason is ordinary missing base data rather than machine-key rotation.
        let storage = MemoryStorage::new();
        seed_lagging_branch(&storage);
        let git = lagging_branch_git();

        let (text, report, regressions) = analyze_text_and_json(&git, &storage);
        assert_eq!(regressions, 1);
        assert!(
            text.contains(
                "Warning: comparison base is 1 commit behind base \
                 (no base data at more recent commits)"
            ),
            "{text}"
        );

        let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
        let lags = &parsed["sets"][0]["comparison_base_lags"];
        assert_eq!(lags[0]["reason"], "no_recent_base_data", "{report}");
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "the no-warning positive control detects an excursion over a full stored base"
    )]
    fn a_comparison_base_reaching_the_merge_base_warns_of_nothing() {
        // When m1 also carries the merge-base tip, the comparison base reaches it and
        // no comparison-base warning is emitted on any surface. The finding still
        // survives, so the silence is about the lag classification rather than about
        // there being nothing to report.
        let storage = MemoryStorage::new();
        seed_lagging_branch(&storage);
        let merge_base = unmeasured_tip();
        store(
            &storage,
            &clean_key(&merge_base),
            &ir_set(SIBLING_OBSERVED, &merge_base, 100.0),
        );
        let git = lagging_branch_git();

        let (text, report, regressions) = analyze_text_and_json(&git, &storage);
        assert_eq!(regressions, 1, "{text}");
        assert!(!text.contains("comparison base is"), "{text}");

        let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
        assert!(
            parsed["sets"][0]["comparison_base_lags"].is_null(),
            "an unaffected set omits the field entirely: {report}"
        );
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "the absent-lag-warning control must still detect a full stored history regression"
    )]
    fn history_mode_never_warns_of_a_lagging_comparison_base() {
        // History mode has no single comparison base, so the warning never applies
        // even when older commits carry data under a different machine key. The step
        // is flagged, so the absent warning is not merely an absent finding.
        let storage = MemoryStorage::new();
        seed_linear_step(&storage);
        let git = history_git();

        let (text, regressions) = analyze(&git, &storage, "folo", &options());
        assert_eq!(regressions, 1, "{text}");
        assert!(!text.contains("comparison base is"), "{text}");
    }

    #[test]
    fn no_dirty_suppresses_the_target_side_dirty_run() {
        let storage = MemoryStorage::new();
        store(&storage, &clean_key("f1"), &ir_set(2, "f1", 100.0));
        store(&storage, &dirty_key("f2", 3), &ir_set(3, "f2", 130.0));
        let git = feature_git();

        let opts = AnalyzeOptions {
            no_dirty: true,
            ..options()
        };
        let (report, _, _) = analyze_json(&git, &storage, "folo", &opts);
        let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
        assert_eq!(parsed["runs"], 1, "--no-dirty drops the dirty snapshot");
    }

    #[test]
    fn dirty_run_on_a_base_side_commit_is_excluded() {
        // A dirty snapshot on c1 (at/before the merge-base) is base-side, so even
        // on the feature view it is clean-only and the dirty file is excluded.
        let storage = MemoryStorage::new();
        store(&storage, &clean_key("c1"), &ir_set(1, "c1", 100.0));
        store(&storage, &dirty_key("c1", 9), &ir_set(9, "c1", 999.0));
        store(&storage, &clean_key("f1"), &ir_set(2, "f1", 100.0));
        let git = feature_git();

        let (report, _, _) = analyze_json(&git, &storage, "folo", &options());
        let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
        assert_eq!(parsed["runs"], 2, "the base-side dirty c1 run is excluded");
    }

    #[test]
    fn all_dirty_on_base_yields_zero_runs_with_a_hint() {
        // The user-reported trap: on the default branch's tip every run is a
        // dirty snapshot (e.g. because the config file was never committed), so
        // all are excluded and the empty outcome must explain itself with a hint
        // and per-object verbose notes rather than looking like "no data".
        let storage = MemoryStorage::new();
        store(&storage, &dirty_key("c3", 100), &ir_set(100, "c3", 100.0));
        store(&storage, &dirty_key("c3", 200), &ir_set(200, "c3", 130.0));
        let git = linear_git();

        let (report, _, reporter) = analyze_json(&git, &storage, "folo", &options());

        let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
        assert_eq!(
            parsed["runs"], 0,
            "every dirty-on-base snapshot is excluded"
        );
        let hint = parsed["hint"].as_str().unwrap();
        assert!(
            hint.contains("Found 2 stored runs"),
            "the hint should count the stored runs: {hint}"
        );
        assert!(
            hint.contains("dirty"),
            "the hint should explain the dirty-on-base exclusion: {hint}"
        );

        assert!(
            reporter.contains("dirty snapshot on a base-side commit"),
            "verbose notes should explain each exclusion: {:?}",
            reporter.notes()
        );
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "dirty-cohort detection requires loading a full base window and repeated snapshots"
    )]
    fn dirty_tree_on_base_branch_admits_tip_dirty_runs_with_a_warning() {
        // On the base branch (official view) with a currently-dirty working tree,
        // the dirty snapshots on the tip are the user's in-flight work and ARE
        // admitted, with a warning that they are ephemeral. Admitting them puts the
        // analysis in branch mode, judging the tip's dirty cohort against the clean
        // base line below it.
        let storage = MemoryStorage::new();
        seed_master(&storage, &[100.0; BASE_COMMITS]);
        let tip = unmeasured_tip();
        for observed in [300, 400, 500] {
            store(
                &storage,
                &dirty_key(&tip, observed),
                &ir_set(observed, &tip, 130.0),
            );
        }
        let mut git = unmeasured_tip_git();
        git.mark_dirty();

        let (report, regressions, reporter) = analyze_json(&git, &storage, "folo", &options());

        let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
        assert_eq!(
            parsed["runs"],
            BASE_COMMITS.saturating_add(3),
            "all three dirty tip snapshots are admitted"
        );
        assert_eq!(regressions, 1, "the dirty tip snapshots complete the step");
        assert_eq!(
            parsed["tip_commit"], tip,
            "the report names the analyzed tip"
        );
        assert_eq!(
            parsed["tip_dirty"], true,
            "the currently-dirty working tree annotates the tip"
        );
        let warning = parsed["warning"].as_str().unwrap();
        assert!(
            warning.contains("dirty runs") && warning.contains("Switch to a new branch"),
            "{warning}"
        );
        assert!(
            reporter.contains("ephemeral"),
            "a verbose note should flag the ephemeral inclusion: {:?}",
            reporter.notes()
        );
    }

    #[test]
    fn clean_tree_on_base_branch_excludes_dirty_and_warns_nothing() {
        // The exception is gated on the working tree being dirty: with a clean
        // tree the base-tip dirty snapshot stays excluded and no warning fires.
        let storage = MemoryStorage::new();
        store(&storage, &clean_key("c0"), &ir_set(0, "c0", 100.0));
        store(&storage, &dirty_key("c3", 300), &ir_set(300, "c3", 999.0));
        let git = linear_git(); // Clean working tree (the default).

        let (report, _, _) = analyze_json(&git, &storage, "folo", &options());
        let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
        assert_eq!(parsed["runs"], 1, "the dirty tip run stays excluded");
        assert_eq!(
            parsed["tip_commit"], "c3",
            "the report names the analyzed tip"
        );
        assert_eq!(
            parsed["tip_dirty"], false,
            "a clean working tree leaves the tip unannotated"
        );
        assert!(
            parsed["warning"].is_null(),
            "no warning when the tree is clean"
        );
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "the history-mode positive control requires a full detectable stored clean step"
    )]
    fn dirty_working_tree_without_recorded_dirty_runs_stays_history_mode() {
        // The reported corner case: on the base branch with a currently-dirty
        // working tree but ONLY clean runs recorded (no dirty run on the tip), mode
        // auto-detection keys off the *admitted* runs — a dirty tree with no admitted
        // dirty run carries no branch evidence — and picks history mode, so the
        // long-range change-point detector still flags the sustained step. The old
        // behaviour keyed off `git.is_dirty()` alone and wrongly fell into branch
        // mode here.
        let storage = MemoryStorage::new();
        seed_linear_step(&storage);
        let mut git = history_git();
        git.mark_dirty();

        let (report, regressions, reporter) = analyze_json(&git, &storage, "folo", &options());

        let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
        assert_eq!(
            parsed["mode"], "history",
            "a dirty tree with only clean runs is still the official history view"
        );
        assert_eq!(
            regressions, 1,
            "history mode flags the sustained clean step"
        );
        assert!(
            parsed["warning"].is_null(),
            "no dirty runs are admitted, so nothing is ephemeral"
        );
        assert!(
            reporter.contains("no dirty run is")
                && reporter.contains("admitted only while the working tree is currently dirty"),
            "the verbose note should explain why history mode was chosen: {:?}",
            reporter.notes()
        );
    }

    #[test]
    fn no_dirty_overrides_the_dirty_tree_exception() {
        // `--no-dirty` skips the dirtiness probe and the exception, so even with a
        // dirty tree the base-tip dirty snapshot is excluded and no warning fires.
        let storage = MemoryStorage::new();
        store(&storage, &clean_key("c0"), &ir_set(0, "c0", 100.0));
        store(&storage, &dirty_key("c3", 300), &ir_set(300, "c3", 999.0));
        let mut git = linear_git();
        git.mark_dirty();

        let opts = AnalyzeOptions {
            no_dirty: true,
            ..options()
        };
        let (report, _, reporter) = analyze_json(&git, &storage, "folo", &opts);
        let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
        assert_eq!(parsed["runs"], 1, "--no-dirty drops the dirty tip snapshot");
        assert_eq!(
            parsed["tip_dirty"], false,
            "--no-dirty skips the dirtiness probe, so the tip is never annotated dirty"
        );
        assert!(parsed["warning"].is_null(), "no warning under --no-dirty");
        assert!(
            !reporter.contains("dirty snapshots on a base-side tip will be admitted"),
            "--no-dirty skips the dirtiness probe, so the dirty-tree exception never fires: {:?}",
            reporter.notes()
        );
    }

    #[test]
    fn only_the_tip_admits_dirty_under_the_exception() {
        // With a dirty tree the exception applies ONLY to the base-branch tip: a
        // dirty snapshot on an earlier base-side commit stays excluded while the
        // tip's dirty snapshot is admitted (and warned).
        let storage = MemoryStorage::new();
        store(&storage, &clean_key("c3"), &ir_set(3, "c3", 100.0));
        store(&storage, &dirty_key("c1", 150), &ir_set(150, "c1", 999.0));
        store(&storage, &dirty_key("c3", 300), &ir_set(300, "c3", 130.0));
        let mut git = linear_git();
        git.mark_dirty();

        let (report, _, reporter) = analyze_json(&git, &storage, "folo", &options());
        let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
        assert_eq!(
            parsed["runs"], 2,
            "only the tip's dirty run joins the clean run"
        );
        assert!(
            !parsed["warning"].is_null(),
            "the tip's admitted dirty run warns: {report}"
        );
        assert!(
            reporter.contains("dirty snapshot on a base-side commit"),
            "the earlier base-side dirty run is still excluded: {:?}",
            reporter.notes()
        );
    }

    #[test]
    fn commits_off_the_first_parent_chain_are_excluded() {
        // c2 is on master but not on feature's first-parent ancestry, so its run
        // never enters a feature-view analysis.
        let storage = MemoryStorage::new();
        store(&storage, &clean_key("c1"), &ir_set(1, "c1", 100.0));
        store(&storage, &clean_key("c2"), &ir_set(2, "c2", 999.0));
        store(&storage, &clean_key("f1"), &ir_set(4, "f1", 100.0));
        let git = feature_git();

        let (report, _, _) = analyze_json(&git, &storage, "folo", &options());
        let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
        assert_eq!(parsed["runs"], 2, "c2 is off the feature mainline");
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "explicit-context selection is verified by detecting the full stored master history"
    )]
    fn explicit_branch_selects_the_official_master_view() {
        // From a feature checkout, `--context master` analyzes master's own history:
        // a full-length clean line carrying a sustained step whose raised regime the
        // feature tip's own first-parent walk never reaches.
        let storage = MemoryStorage::new();
        seed_linear_step(&storage);
        let git = feature_chain(HISTORY_COMMITS, 1);

        let opts = AnalyzeOptions {
            context: Some("master".to_owned()),
            ..options()
        };
        let (report, regressions, _) = analyze_json(&git, &storage, "folo", &opts);
        let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
        assert_eq!(parsed["runs"], HISTORY_COMMITS, "master's whole line");
        assert_eq!(regressions, 1);
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "cohort ordering is verified through detection over a full base and dirty snapshots"
    )]
    fn within_a_commit_clean_precedes_dirty() {
        // On a target-side commit, a clean run and dirty snapshots both load. Branch
        // mode judges the tip's latest cohort — the contiguous suffix sharing the last
        // point's commit and dirty flag — so the flag can only come from the dirty
        // snapshots sorting after the clean f2 run at the baseline level. Were the
        // ordering reversed the cohort would be that clean run and nothing would flag.
        let storage = MemoryStorage::new();
        seed_master(&storage, &[100.0; BASE_COMMITS]);
        let observed = i64::try_from(BASE_COMMITS).unwrap();
        store(&storage, &clean_key("f1"), &ir_set(observed, "f1", 100.0));
        store(
            &storage,
            &clean_key("f2"),
            &ir_set(observed.saturating_add(1), "f2", 100.0),
        );
        for offset in 2..5_i64 {
            let second = observed.saturating_add(offset);
            store(
                &storage,
                &dirty_key("f2", second),
                &ir_set(second, "f2", 130.0),
            );
        }
        let git = branch_git();

        let (_, regressions, _) = analyze_json(&git, &storage, "folo", &options());
        assert_eq!(regressions, 1, "the dirty f2 values are the latest points");
    }

    #[test]
    fn target_triple_discriminant_selects_the_windows_set() {
        // Two sets differing only by triple; an explicit `--target-triple` reports
        // just the matching one, even though the auto-detected default is Linux.
        // Both are seeded at the `linear_git` tip (`c3`) so the tip filter keeps them.
        let storage = MemoryStorage::new();
        store(&storage, &clean_key("c3"), &ir_set(3, "c3", 100.0));
        store(
            &storage,
            "v1/folo/objects/callgrind/x86_64-pc-windows-msvc/m1/c3/clean.json",
            &ir_set(3, "c3", 100.0),
        );
        let git = linear_git();

        let opts = AnalyzeOptions {
            target_triple: vec!["x86_64-pc-windows-msvc".to_owned()],
            ..options()
        };
        let (report, _, _) = analyze_json(&git, &storage, "folo", &opts);
        let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
        assert_eq!(parsed["runs"], 1, "only the windows set is loaded");
        assert_eq!(parsed["sets"].as_array().unwrap().len(), 1, "{report}");
        assert_eq!(
            parsed["sets"][0]["target_triple"], "x86_64-pc-windows-msvc",
            "{report}"
        );
    }

    #[test]
    fn target_triple_discriminant_selects_one_set() {
        // Two sets differing only by triple; `--target-triple` reports just the one.
        // Both are seeded at the `linear_git` tip (`c3`) so the tip filter keeps them.
        let storage = MemoryStorage::new();
        store(&storage, &clean_key("c3"), &ir_set(3, "c3", 100.0));
        store(
            &storage,
            "v1/folo/objects/callgrind/x86_64-pc-windows-msvc/m1/c3/clean.json",
            &ir_set(3, "c3", 100.0),
        );
        let git = linear_git();

        let opts = AnalyzeOptions {
            target_triple: vec!["x86_64-unknown-linux-gnu".to_owned()],
            ..options()
        };
        let (report, _, _) = analyze_json(&git, &storage, "folo", &opts);
        let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
        assert_eq!(parsed["runs"], 1, "only the linux-gnu triple is loaded");
        assert_eq!(parsed["sets"].as_array().unwrap().len(), 1, "{report}");
        assert_eq!(
            parsed["sets"][0]["target_triple"], "x86_64-unknown-linux-gnu",
            "{report}"
        );
    }

    #[test]
    fn two_sets_produce_two_report_sections() {
        // Both sets are seeded at the sole commit so the tip filter keeps each and
        // every partition is reported. They differ only by triple, and
        // every set obeys the target-triple filter, so the query widens to
        // `--target-triple all` to search both partitions rather than just the host's.
        let storage = MemoryStorage::new();
        store(&storage, &clean_key("c0"), &ir_set(0, "c0", 100.0));
        store(
            &storage,
            "v1/folo/objects/callgrind/x86_64-pc-windows-msvc/m1/c0/clean.json",
            &ir_set(0, "c0", 100.0),
        );
        let git = master_chain(1);

        let opts = AnalyzeOptions {
            target_triple: vec!["all".to_owned()],
            ..options()
        };
        let (report, _, _) = analyze_json(&git, &storage, "folo", &opts);
        let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
        assert_eq!(parsed["sets"].as_array().unwrap().len(), 2, "{report}");
    }

    #[test]
    fn engine_discriminant_narrows_the_listing() {
        // Two sets in the same triple/machine-key partition differing only by engine,
        // so the engine discriminant alone selects one.
        let storage = MemoryStorage::new();
        store(&storage, &clean_key("c0"), &ir_set(0, "c0", 100.0));
        store(
            &storage,
            "v1/folo/objects/criterion/x86_64-unknown-linux-gnu/m1/c0/clean.json",
            &ir_set(0, "c0", 100.0),
        );
        let git = linear_git();

        let opts = AnalyzeOptions {
            engine: vec!["callgrind".to_owned()],
            ..options()
        };
        let (report, _, _) = analyze_json(&git, &storage, "folo", &opts);
        let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
        assert_eq!(parsed["runs"], 1, "only the callgrind object is loaded");
    }

    #[test]
    fn since_window_excludes_earlier_runs() {
        let storage = MemoryStorage::new();
        // c0,c1 at epoch seconds 0,1; c2,c3 at 2,3. `--since` epoch 2 keeps c2,c3.
        for (index, value) in [100.0, 100.0, 100.0, 130.0].into_iter().enumerate() {
            let commit = format!("c{index}");
            let second = i64::try_from(index).unwrap();
            store(
                &storage,
                &clean_key(&commit),
                &ir_set(second, &commit, value),
            );
        }
        let git = linear_git();

        let opts = AnalyzeOptions {
            since: Some("1970-01-01T00:00:02Z".to_owned()),
            ..options()
        };
        let (report, _, _) = analyze_json(&git, &storage, "folo", &opts);
        let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
        assert_eq!(parsed["runs"], 2, "only c2 and c3 are within the window");
    }

    #[test]
    fn analyze_without_a_resolvable_base_branch_is_an_error() {
        let storage = MemoryStorage::new();
        // HEAD resolves, but there is no advertised default branch and no --base /
        // config default, so the base branch cannot be determined and there is no
        // merge-base to split the timeline on. Rather than silently analyze the
        // incomplete topology as a base-branch (history) view, this is an error.
        let mut git = FakeGitHistory::new();
        let tip = append_master_chain(&mut git, 1);
        git.branch("master", &tip).head("master"); // No `.mark_default(...)`.
        let error = block_on(analyze_with(
            &git,
            &storage,
            "folo",
            &config(),
            &options(),
            &auto(),
            now_anchor(),
            &RecordingReporter::new(),
            false,
            &spawner(),
        ))
        .unwrap_err();
        let found = error.find_source::<BaseBranchUnavailableError>().unwrap();
        assert_eq!(found.target_ref, "HEAD");
    }

    #[test]
    fn analyze_without_a_common_ancestor_is_an_error() {
        let storage = MemoryStorage::new();
        // The base branch resolves, but it shares no history with the target — the
        // shallow-clone case, where the fetched depth stops short of the branch
        // point. `git merge-base` finds no common ancestor, so the timeline cannot
        // be split and this errors rather than guessing a base-branch view.
        let mut git = FakeGitHistory::new();
        let tip = append_master_chain(&mut git, 1);
        git
            // A disjoint base history with no common ancestor with the target.
            .commit("m0", None)
            .branch("master", "m0")
            .branch("feature", &tip)
            .head("feature")
            .mark_default("master");
        let error = block_on(analyze_with(
            &git,
            &storage,
            "folo",
            &config(),
            &options(),
            &auto(),
            now_anchor(),
            &RecordingReporter::new(),
            false,
            &spawner(),
        ))
        .unwrap_err();
        let found = error.find_source::<MergeBaseUnavailableError>().unwrap();
        assert_eq!(found.target_ref, "HEAD");
        assert_eq!(found.base_commit, "m0");
    }

    #[test]
    fn analyze_rejects_an_explicit_disjoint_base() {
        let storage = MemoryStorage::new();
        // The user deliberately chose `--base master`, which resolves but shares no
        // history with the target, so the requested topology cannot be resolved.
        let mut git = FakeGitHistory::new();
        let tip = append_master_chain(&mut git, 1);
        git.commit("m0", None)
            .branch("master", "m0")
            .branch("feature", &tip)
            .head("feature")
            .mark_default("master");
        let mut options = options();
        options.base = Some("master".to_owned());
        let error = block_on(analyze_with(
            &git,
            &storage,
            "folo",
            &config(),
            &options,
            &auto(),
            now_anchor(),
            &RecordingReporter::new(),
            false,
            &spawner(),
        ))
        .unwrap_err();
        let found = error.find_source::<MergeBaseUnavailableError>().unwrap();
        assert_eq!(found.target_ref, "HEAD");
        assert_eq!(found.base_commit, "m0");
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "the sanitized-project positive control detects a full stored history regression"
    )]
    fn history_is_found_for_a_project_id_that_requires_sanitizing() {
        // `collect` stores under the sanitized project segment, so `analyze` must list
        // under that same segment; listing under the raw id would miss the history.
        let storage = MemoryStorage::new();
        let raw_project = "my project/v2";
        let sanitized = sanitize_segment(raw_project);
        for (index, value) in step_values(100.0, 130.0).into_iter().enumerate() {
            let commit = commit_name(index);
            let second = i64::try_from(index).unwrap();
            let key = format!(
                "v1/{sanitized}/objects/callgrind/x86_64-unknown-linux-gnu/m1/{commit}/clean.json"
            );
            store(&storage, &key, &ir_set(second, &commit, value));
        }
        let git = history_git();

        let (report, regressions) = analyze(&git, &storage, raw_project, &options());
        assert_eq!(
            regressions, 1,
            "history stored under the sanitized key must be found"
        );
        assert!(report.contains("nm/nm::observe/pull"), "{report}");
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "the successful-result control requires loading and detecting a full stored regression"
    )]
    fn a_flagged_regression_still_yields_a_successful_analysis() {
        // The exit code no longer depends on findings: even a flagged regression
        // yields a successful (Ok) analysis (the signal lives in the report JSON).
        // The shell maps this into an always-successful `RunOutcome`.
        let storage = MemoryStorage::new();
        seed_linear_step(&storage);
        let git = history_git();

        let (_, regressions) = block_on(analyze_with(
            &git,
            &storage,
            "folo",
            &config(),
            &options(),
            &auto(),
            now_anchor(),
            &RecordingReporter::new(),
            false,
            &spawner(),
        ))
        .expect("a flagged regression must not fail the analysis");
        assert_eq!(regressions, 1, "the seeded step is a flagged regression");
    }

    #[test]
    fn json_format_is_rendered() {
        let storage = MemoryStorage::new();
        store(&storage, &clean_key("c0"), &ir_set(0, "c0", 10.0));
        let git = linear_git();

        let (report, _, _) = analyze_json(&git, &storage, "folo", &options());
        let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
        assert_eq!(parsed["project"], "folo");
        assert_eq!(parsed["runs"], 1);
    }

    #[test]
    fn non_json_objects_are_skipped() {
        let storage = MemoryStorage::new();
        store(&storage, &clean_key("c0"), &ir_set(0, "c0", 10.0));
        // A stray non-result object under the prefix must be ignored, not parsed.
        block_on(storage.put("v1/folo/objects/callgrind/README.txt", b"not json")).unwrap();
        let git = linear_git();

        let (report, regressions, _) = analyze_json(&git, &storage, "folo", &options());
        let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
        assert_eq!(parsed["runs"], 1, "only the real result object loaded");
        assert_eq!(regressions, 0);
    }

    #[test]
    fn malformed_stored_object_is_an_analyze_error() {
        let storage = MemoryStorage::new();
        block_on(storage.put(&clean_key("c0"), b"{ not valid")).unwrap();
        let git = linear_git();

        let error = block_on(analyze_with(
            &git,
            &storage,
            "folo",
            &config(),
            &options(),
            &auto(),
            now_anchor(),
            &RecordingReporter::new(),
            false,
            &spawner(),
        ))
        .unwrap_err();
        let found = error.find_source::<InvalidResultSetError>().unwrap();
        assert_eq!(found.key, clean_key("c0"));
        assert!(error.find_source::<serde_json::Error>().is_some());
    }

    #[test]
    fn invalid_utf8_object_is_an_analyze_error() {
        let storage = MemoryStorage::new();
        block_on(storage.put(&clean_key("c0"), &[0xff, 0xfe])).unwrap();
        let git = linear_git();

        let error = block_on(analyze_with(
            &git,
            &storage,
            "folo",
            &config(),
            &options(),
            &auto(),
            now_anchor(),
            &RecordingReporter::new(),
            false,
            &spawner(),
        ))
        .unwrap_err();
        let found = error.find_source::<InvalidStoredUtf8Error>().unwrap();
        assert_eq!(found.object_kind, "stored object");
        assert_eq!(found.key, clean_key("c0"));
        assert!(error.find_source::<std::str::Utf8Error>().is_some());
    }

    #[test]
    fn no_output_selected_is_rejected() {
        // Suppressing the text report without requesting any file output leaves
        // nothing to produce, which is a usage error rather than a silent no-op.
        let storage = MemoryStorage::new();
        let git = linear_git();
        let opts = AnalyzeOptions {
            no_text: true,
            ..options()
        };
        let error = block_on(analyze_with(
            &git,
            &storage,
            "folo",
            &config(),
            &opts,
            &auto(),
            now_anchor(),
            &RecordingReporter::new(),
            false,
            &spawner(),
        ))
        .unwrap_err();
        assert!(error.find_source::<NoOutputSelectedError>().is_some());
    }

    #[test]
    fn unknown_engine_is_rejected() {
        let storage = MemoryStorage::new();
        let git = linear_git();
        let opts = AnalyzeOptions {
            engine: vec!["dhat".to_owned()],
            ..options()
        };
        let error = block_on(analyze_with(
            &git,
            &storage,
            "folo",
            &config(),
            &opts,
            &auto(),
            now_anchor(),
            &RecordingReporter::new(),
            false,
            &spawner(),
        ))
        .unwrap_err();
        let found = error.find_source::<UnknownEngineError>().unwrap();
        assert_eq!(found.name, "dhat");
    }

    #[test]
    fn unresolvable_base_is_rejected() {
        let storage = MemoryStorage::new();
        let git = linear_git();
        let opts = AnalyzeOptions {
            base: Some("does-not-exist".to_owned()),
            ..options()
        };
        let error = block_on(analyze_with(
            &git,
            &storage,
            "folo",
            &config(),
            &opts,
            &auto(),
            now_anchor(),
            &RecordingReporter::new(),
            false,
            &spawner(),
        ))
        .unwrap_err();
        let found = error.find_source::<UnresolvedRefError>().unwrap();
        assert_eq!(found.reference, "does-not-exist");
    }

    #[test]
    fn configured_default_branch_is_used_as_the_base() {
        // The config names `master` as the default branch; analyzing the feature
        // branch must split at the master merge-base even without `--base`.
        let storage = MemoryStorage::new();
        store(&storage, &clean_key("c1"), &ir_set(1, "c1", 100.0));
        store(&storage, &dirty_key("c1", 9), &ir_set(9, "c1", 999.0));
        store(&storage, &clean_key("f1"), &ir_set(2, "f1", 100.0));
        // A git history that does NOT advertise a default branch, so resolution
        // must fall through to the configured `project.default_branch`.
        let mut git = FakeGitHistory::new();
        git.commit("c0", None)
            .commit("c1", Some("c0"))
            .commit("f1", Some("c1"))
            .branch("master", "c1")
            .branch("feature", "f1")
            .head("feature");
        let config = parse_config("[project]\ndefault_branch = \"master\"\n").unwrap();

        let opts = AnalyzeOptions {
            no_text: true,
            json: Some(PathBuf::from("report.json")),
            ..options()
        };
        let (rendered, _) = block_on(analyze_with(
            &git,
            &storage,
            "folo",
            &config,
            &opts,
            &auto(),
            now_anchor(),
            &RecordingReporter::new(),
            false,
            &spawner(),
        ))
        .unwrap();
        let report = rendered.json.expect("the JSON report was rendered");
        let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
        // c1's dirty run is base-side (excluded); c1 clean and f1 clean load.
        assert_eq!(
            parsed["runs"], 2,
            "base-side dirty c1 excluded via config base"
        );
    }

    #[test]
    fn resolve_now_reads_the_injected_clock() {
        // The analyze family sources its wall-clock anchor through an injectable
        // `tick::Clock`; a frozen clock must surface its own instant verbatim rather than
        // any default minted independently of the clock.
        let anchor = ts(1_700_000_000);
        assert_eq!(resolve_now(Some(Clock::new_frozen_at(anchor))), anchor);
    }
}
