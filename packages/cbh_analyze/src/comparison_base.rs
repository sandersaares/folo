//! Branch-mode comparison-base lag classification.
//!
//! In branch mode each surviving finding is compared against the newest base-ref
//! point of its own discriminant set. On rotating CI machine pools the newest base
//! commits may carry data only under a different machine key, so the context runner's
//! machine key has usable base data only several commits behind the base ref — the
//! comparison silently reaches back in history.
//!
//! This module measures that lag per finding from the detector's actual comparison
//! point ([`Finding::comparison_base_index`]) and classifies why it lags:
//!
//! - [`ComparisonBaseLagReason::DiscriminantSetMismatch`] — a newer base-ref run for
//!   the same benchmark and metric exists, but under a different machine key
//!   (machine-pool rotation);
//! - [`ComparisonBaseLagReason::NoRecentBaseData`] — no newer base-ref run for the
//!   affected series exists at all.
//!
//! Evidence is drawn from the already reconstructed series first and only then from a
//! lazy, deduplicated fetch of the retained base-ref clean-run objects that could fall
//! in a lagging finding's gap. Sets with no lagging finding never trigger a fetch.
//!
//! Comparison-base warnings are advisory: if the optional sibling evidence cannot be
//! fetched or parsed, the failure is noted and the affected findings degrade to the
//! generic reason rather than failing the analysis run.

use std::collections::HashMap;
use std::num::NonZero;

use cbh_detect::{Finding, RunPoints, Series};
use cbh_diag::{Reporter, ReporterExt, count_noun};
use cbh_model::{BenchmarkId, DiscriminantSet, MetricKind, StorageKey};
use cbh_render::{ComparisonBaseLag, ComparisonBaseLagReason};
use cbh_storage::Storage;

use super::dataset::SiblingObservation;
use super::load::load_objects_concurrently;
use crate::{AnalyzeError, InvalidResultSetError, InvalidStoredUtf8Error};

/// A surviving branch finding whose comparison base sits behind the base ref.
struct LaggingFinding<'a> {
    /// The finding's discriminant set (the affected set the warning attaches to).
    set: &'a DiscriminantSet,
    /// The benchmark identity the comparison covered.
    id: &'a BenchmarkId,
    /// The metric kind the comparison covered.
    kind: MetricKind,
    /// Topological index of the comparison base — the exclusive lower bound of the
    /// gap a newer sibling observation would have to fall into.
    comparison_base_index: usize,
    /// First-parent distance from the comparison base to the base ref.
    commits_behind: NonZero<usize>,
}

/// A fetched sibling clean run reduced to the identity evidence classification reads.
struct SiblingEvidence {
    /// The sibling's discriminant set (a machine key the selection did not cover).
    set: DiscriminantSet,
    /// First-parent topological index of the sibling's commit.
    topo_index: usize,
    /// The sibling's fold-relevant payload, for benchmark-and-metric presence.
    points: RunPoints,
}

/// Classifies the comparison-base lag of every surviving branch finding and groups
/// the deterministic, deduplicated warnings by discriminant set.
///
/// Returns an empty map in history mode (unknown base ref) and when no finding's
/// comparison base lags. Only fetches foreign sibling payloads that could classify a
/// still-unresolved lagging finding, reusing the shared bounded-concurrency loader.
///
/// Comparison-base warnings are advisory and never fail the analysis run. If the
/// optional sibling evidence cannot be fetched or parsed, the failure is noted and the
/// affected findings fall through to the generic reason rather than aborting — a
/// mismatch is only ever asserted from positive evidence we could actually read.
pub(crate) async fn classify_comparison_base_lags<S>(
    storage: &S,
    findings: &[Finding],
    series: &[Series],
    base_ref_index: Option<usize>,
    siblings: &[SiblingObservation],
    reporter: &dyn Reporter,
) -> HashMap<DiscriminantSet, Vec<ComparisonBaseLag>>
where
    S: Storage,
{
    let Some(base_ref) = base_ref_index else {
        return HashMap::new();
    };

    let lagging: Vec<LaggingFinding<'_>> = findings
        .iter()
        .filter_map(|finding| {
            let comparison_base_index = finding.comparison_base_index?;
            let commits_behind = base_ref
                .checked_sub(comparison_base_index)
                .and_then(NonZero::new)?;
            Some(LaggingFinding {
                set: &finding.set,
                id: &finding.id,
                kind: finding.kind,
                comparison_base_index,
                commits_behind,
            })
        })
        .collect();
    if lagging.is_empty() {
        return HashMap::new();
    }
    reporter.note_with(|| {
        format!(
            "classifying comparison-base lag for {}",
            count_noun(lagging.len(), "lagging finding")
        )
    });

    // Satisfy evidence from the already loaded series first: a newer base-ref clean
    // point for the same benchmark and metric under a sibling machine key proves a
    // discriminant-set mismatch without any fetch. This is the whole story under
    // cases where every relevant machine key survived the ghost filter.
    let mut unresolved: Vec<&LaggingFinding<'_>> = Vec::new();
    let mut reasons: HashMap<usize, ComparisonBaseLagReason> = HashMap::new();
    for (index, finding) in lagging.iter().enumerate() {
        if loaded_series_prove_mismatch(finding, series, base_ref) {
            reasons.insert(index, ComparisonBaseLagReason::DiscriminantSetMismatch);
        } else {
            unresolved.push(finding);
        }
    }

    // Only fetch the sibling objects that could still change a verdict: exact
    // clean-run keys sharing a still-unresolved finding's comparable axes and falling
    // inside its gap. Deduplicate so an object shared by several findings loads once.
    // A fetch or parse failure is advisory-only: note it and leave the affected
    // findings to fall through to the generic reason, never aborting the analysis.
    let evidence = if unresolved.is_empty() {
        Vec::new()
    } else {
        match load_sibling_evidence(storage, &unresolved, siblings, base_ref, reporter).await {
            Ok(evidence) => evidence,
            Err(error) => {
                reporter.note_with(move || {
                    format!("skipping comparison-base mismatch evidence: {error}")
                });
                Vec::new()
            }
        }
    };

    let mut lags: HashMap<&DiscriminantSet, Vec<ComparisonBaseLag>> = HashMap::new();
    for (index, finding) in lagging.iter().enumerate() {
        let reason = reasons.get(&index).copied().unwrap_or_else(|| {
            if evidence_proves_mismatch(finding, &evidence, base_ref) {
                ComparisonBaseLagReason::DiscriminantSetMismatch
            } else {
                ComparisonBaseLagReason::NoRecentBaseData
            }
        });
        lags.entry(finding.set)
            .or_default()
            .push(ComparisonBaseLag {
                commits_behind: finding.commits_behind,
                reason,
            });
    }

    // Partial runs can leave findings in one set with different gaps or reasons.
    // Deduplicate exact pairs and order deterministically (largest gap first, then
    // mismatch before generic) so a set's warnings render stably.
    let mut classified: HashMap<DiscriminantSet, Vec<ComparisonBaseLag>> = HashMap::new();
    for (set, mut set_lags) in lags {
        set_lags.sort_by(|left, right| {
            right
                .commits_behind
                .cmp(&left.commits_behind)
                .then_with(|| reason_rank(left.reason).cmp(&reason_rank(right.reason)))
        });
        set_lags.dedup();
        classified.insert(set.clone(), set_lags);
    }
    classified
}

/// Whether an already loaded sibling series proves a machine-key mismatch for
/// `finding`: a series sharing the finding's engine, target triple, benchmark, and
/// metric, but under a different machine key, with a clean point strictly newer than
/// the comparison base and no newer than the base ref.
fn loaded_series_prove_mismatch(
    finding: &LaggingFinding<'_>,
    series: &[Series],
    base_ref: usize,
) -> bool {
    series.iter().any(|candidate| {
        is_sibling_set(&candidate.set, finding.set)
            && &candidate.id == finding.id
            && candidate.kind == finding.kind
            && candidate.base_window.iter().any(|level| {
                level.topo_index > finding.comparison_base_index && level.topo_index <= base_ref
            })
    })
}

/// Whether a fetched sibling payload proves a machine-key mismatch for `finding`.
fn evidence_proves_mismatch(
    finding: &LaggingFinding<'_>,
    evidence: &[SiblingEvidence],
    base_ref: usize,
) -> bool {
    evidence.iter().any(|sibling| {
        is_sibling_set(&sibling.set, finding.set)
            && sibling.topo_index > finding.comparison_base_index
            && sibling.topo_index <= base_ref
            && sibling.points.results().iter().any(|result| {
                &result.id == finding.id
                    && result
                        .metrics
                        .iter()
                        .any(|metric| metric.kind == finding.kind)
            })
    })
}

/// Loads the sibling clean-run payloads that could still classify an unresolved
/// lagging finding, deduplicated so each object is fetched at most once.
async fn load_sibling_evidence<S>(
    storage: &S,
    unresolved: &[&LaggingFinding<'_>],
    siblings: &[SiblingObservation],
    base_ref: usize,
    reporter: &dyn Reporter,
) -> Result<Vec<SiblingEvidence>, AnalyzeError>
where
    S: Storage,
{
    let mut needed: Vec<(String, StorageKey)> = Vec::new();
    let mut topo_by_key: HashMap<String, usize> = HashMap::new();
    for sibling in siblings {
        let relevant = unresolved.iter().any(|finding| {
            is_sibling_set(&sibling.parsed.set, finding.set)
                && sibling.topo_index > finding.comparison_base_index
                && sibling.topo_index <= base_ref
        });
        if relevant && !topo_by_key.contains_key(&sibling.key) {
            topo_by_key.insert(sibling.key.clone(), sibling.topo_index);
            needed.push((sibling.key.clone(), sibling.parsed.clone()));
        }
    }
    if needed.is_empty() {
        return Ok(Vec::new());
    }
    reporter.note_with(|| {
        format!(
            "fetching {} to classify comparison-base lag",
            count_noun(needed.len(), "sibling clean-run object")
        )
    });

    let loaded = load_objects_concurrently(storage, needed, |key, bytes| {
        let text = str::from_utf8(&bytes)
            .map_err(|error| InvalidStoredUtf8Error::caused_by("stored object", key, error))?;
        RunPoints::from_json(text)
            .map_err(|error| InvalidResultSetError::caused_by(key, error).into())
    })
    .await?;

    Ok(loaded
        .into_iter()
        .map(|(key, parsed, points)| SiblingEvidence {
            set: parsed.set,
            topo_index: topo_by_key
                .get(&key)
                .copied()
                .expect("fetched key came from the sibling inventory"),
            points,
        })
        .collect())
}

/// Whether `candidate` is a sibling of `target`: same engine and target triple, but a
/// different machine key. The comparable axes must match for the counts to belong to
/// the same measurement; the machine key must differ for it to be a rotation sibling.
fn is_sibling_set(candidate: &DiscriminantSet, target: &DiscriminantSet) -> bool {
    candidate.engine == target.engine
        && candidate.target_triple == target.target_triple
        && candidate.machine_key != target.machine_key
}

/// Deterministic reason ordering for a set's warnings: mismatch before generic.
fn reason_rank(reason: ComparisonBaseLagReason) -> u8 {
    match reason {
        ComparisonBaseLagReason::DiscriminantSetMismatch => 0,
        ComparisonBaseLagReason::NoRecentBaseData => 1,
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    #![allow(clippy::indexing_slicing, reason = "panic is fine in tests")]

    use cbh_detect::{BaseLevel, Direction, FindingMethod, SeriesPoint};
    use cbh_diag::RecordingReporter;
    use cbh_model::{
        BenchmarkId, BenchmarkResult, Engine, EnvironmentInfo, GitInfo, Metric, Run, RunContext,
        ToolchainInfo, parse_key,
    };
    use cbh_storage::MemoryStorage;
    use futures::executor::block_on;
    use jiff::Timestamp;
    use nonempty::nonempty;
    use ohno::ErrorExt as _;

    use super::*;
    use crate::testing::run_points_json;

    /// The discriminant set for `machine`, sharing every other comparable axis.
    fn set(machine: &str) -> DiscriminantSet {
        DiscriminantSet {
            engine: Engine::Callgrind,
            target_triple: "x86_64-unknown-linux-gnu".into(),
            machine_key: machine.into(),
        }
    }

    /// The benchmark identity every fixture shares.
    fn bench_id() -> BenchmarkId {
        BenchmarkId::new(nonempty![
            "nm".to_owned(),
            "observe".to_owned(),
            "pull".to_owned(),
        ])
    }

    /// A branch finding stamped with `comparison_base_index`, in `machine`'s set.
    fn finding(machine: &str, comparison_base_index: Option<usize>) -> Finding {
        Finding {
            set: set(machine),
            id: bench_id(),
            kind: MetricKind::InstructionCount,
            method: FindingMethod::ChangePoint,
            direction: Direction::Regression,
            baseline: 100.0,
            latest: 130.0,
            delta: 30.0,
            relative_delta: 0.3,
            commit: Some("f2".to_owned()),
            window_start_commit: None,
            blessed_at: None,
            blessed_commit_time: None,
            series: Vec::new(),
            comparison_base_index,
            chart_base_ref: None,
            branch: None,
        }
    }

    /// A branch finding for a specific benchmark `id`, otherwise like [`finding`].
    fn finding_for(
        machine: &str,
        id: BenchmarkId,
        comparison_base_index: Option<usize>,
    ) -> Finding {
        let mut finding = finding(machine, comparison_base_index);
        finding.id = id;
        finding
    }

    /// A loaded series in `machine`'s set carrying one point at `topo_index` with the
    /// given cleanliness, for the given benchmark and metric.
    fn loaded_series(
        machine: &str,
        id: BenchmarkId,
        kind: MetricKind,
        topo_index: usize,
        dirty: bool,
    ) -> Series {
        Series {
            set: set(machine),
            id,
            kind,
            points: vec![SeriesPoint {
                topo_index,
                dirty,
                object_ordinal: 0,
                commit: None,
                value: 100.0,
                interval_low: None,
                interval_high: None,
            }],
            base_window: if dirty {
                Vec::new()
            } else {
                vec![BaseLevel {
                    topo_index,
                    commit: None,
                    value: 100.0,
                    interval: None,
                }]
            },
            base_history_count: usize::from(!dirty),
            active_start: 0,
            blessing: None,
        }
    }

    /// A stored clean run at `commit` carrying `id` with one `kind` metric.
    fn run_with(id: BenchmarkId, kind: MetricKind) -> Run {
        let context = RunContext::new(
            Timestamp::from_second(0).unwrap(),
            GitInfo {
                commit: Some("c".to_owned()),
                branch: Some("main".to_owned()),
                dirty: false,
            },
            EnvironmentInfo::default(),
            ToolchainInfo::default(),
            "0.0.1".to_owned(),
        );
        let record = BenchmarkResult::new(id, vec![Metric::new(kind, 100.0)]);
        Run::new(context, vec![record])
    }

    /// A sibling observation for `machine` at `commit`, positioned at `topo_index`.
    ///
    /// Nothing is stored under the key. A test either leaves it that way — so a fetch
    /// of it fails, which doubles as a probe that classification never fetched — or
    /// seeds it through [`store_sibling`] / [`store_sibling_bytes`].
    fn sibling_at(machine: &str, commit: &str, topo_index: usize) -> SiblingObservation {
        let key = format!(
            "v1/folo/objects/callgrind/x86_64-unknown-linux-gnu/{machine}/{commit}/clean.json"
        );
        let parsed = parse_key(&key).unwrap();
        SiblingObservation {
            key,
            parsed,
            topo_index,
        }
    }

    /// Stores `run` under `machine`'s clean key for `commit` and returns the matching
    /// sibling observation positioned at `topo_index`.
    fn store_sibling(
        storage: &MemoryStorage,
        machine: &str,
        commit: &str,
        topo_index: usize,
        run: &Run,
    ) -> SiblingObservation {
        store_sibling_bytes(
            storage,
            machine,
            commit,
            topo_index,
            run_points_json(run).as_bytes(),
        )
    }

    /// Stores `bytes` verbatim under `machine`'s clean key for `commit`, so a test can
    /// seed a payload the loader cannot decode.
    fn store_sibling_bytes(
        storage: &MemoryStorage,
        machine: &str,
        commit: &str,
        topo_index: usize,
        bytes: &[u8],
    ) -> SiblingObservation {
        let sibling = sibling_at(machine, commit, topo_index);
        block_on(storage.put(&sibling.key, bytes)).unwrap();
        sibling
    }

    /// Classifies with a recording reporter.
    fn classify(
        storage: &MemoryStorage,
        findings: &[Finding],
        series: &[Series],
        base_ref_index: Option<usize>,
        siblings: &[SiblingObservation],
    ) -> HashMap<DiscriminantSet, Vec<ComparisonBaseLag>> {
        classify_reported(storage, findings, series, base_ref_index, siblings).0
    }

    /// Classifies and returns the recording reporter so a test can inspect whether a
    /// sibling fetch was attempted or an evidence failure was noted.
    fn classify_reported(
        storage: &MemoryStorage,
        findings: &[Finding],
        series: &[Series],
        base_ref_index: Option<usize>,
        siblings: &[SiblingObservation],
    ) -> (
        HashMap<DiscriminantSet, Vec<ComparisonBaseLag>>,
        RecordingReporter,
    ) {
        let reporter = RecordingReporter::new();
        let lags = block_on(classify_comparison_base_lags(
            storage,
            findings,
            series,
            base_ref_index,
            siblings,
            &reporter,
        ));
        (lags, reporter)
    }

    #[test]
    fn history_mode_yields_no_lags() {
        // No base ref: history mode has no single comparison base, so nothing lags.
        let storage = MemoryStorage::new();
        let findings = [finding("m1", Some(2))];
        let lags = classify(&storage, &findings, &[], None, &[]);
        assert!(lags.is_empty(), "{lags:?}");
    }

    #[test]
    fn a_comparison_base_reaching_the_base_ref_is_not_a_lag() {
        // The comparison base is the base ref itself (index 3 == base ref 3).
        let storage = MemoryStorage::new();
        let findings = [finding("m1", Some(3))];
        let lags = classify(&storage, &findings, &[], Some(3), &[]);
        assert!(lags.is_empty(), "{lags:?}");
    }

    #[test]
    fn a_history_finding_without_a_comparison_base_is_not_a_lag() {
        let storage = MemoryStorage::new();
        let findings = [finding("m1", None)];
        let lags = classify(&storage, &findings, &[], Some(3), &[]);
        assert!(lags.is_empty(), "{lags:?}");
    }

    #[test]
    fn a_loaded_sibling_series_proves_a_mismatch_without_fetching() {
        // `--machine-key all`: the sibling machine's series is already resident, so a
        // newer clean point under it proves a mismatch. The lone sibling observation
        // is absent from storage — reaching for it would error, so a clean Ok proves
        // classification never fetched.
        let storage = MemoryStorage::new();
        let findings = [finding("m1", Some(2))];
        let series = [loaded_series(
            "m2",
            bench_id(),
            MetricKind::InstructionCount,
            3,
            false,
        )];
        let siblings = [sibling_at("m2", "c3", 3)];
        let lags = classify(&storage, &findings, &series, Some(3), &siblings);
        let set_lags = &lags[&set("m1")];
        assert_eq!(set_lags.len(), 1);
        assert_eq!(set_lags[0].commits_behind.get(), 1);
        assert_eq!(
            set_lags[0].reason,
            ComparisonBaseLagReason::DiscriminantSetMismatch
        );
    }

    #[test]
    fn a_dirty_loaded_sibling_point_does_not_prove_a_mismatch() {
        // Only clean runs can serve as a comparison base, so a dirty sibling point is
        // not evidence of a usable newer base-side run.
        let storage = MemoryStorage::new();
        let findings = [finding("m1", Some(2))];
        let series = [loaded_series(
            "m2",
            bench_id(),
            MetricKind::InstructionCount,
            3,
            true,
        )];
        let lags = classify(&storage, &findings, &series, Some(3), &[]);
        let set_lags = &lags[&set("m1")];
        assert_eq!(
            set_lags[0].reason,
            ComparisonBaseLagReason::NoRecentBaseData
        );
    }

    #[test]
    fn no_newer_base_data_anywhere_is_the_generic_reason() {
        let storage = MemoryStorage::new();
        let findings = [finding("m1", Some(2))];
        let lags = classify(&storage, &findings, &[], Some(3), &[]);
        let set_lags = &lags[&set("m1")];
        assert_eq!(set_lags.len(), 1);
        assert_eq!(set_lags[0].commits_behind.get(), 1);
        assert_eq!(
            set_lags[0].reason,
            ComparisonBaseLagReason::NoRecentBaseData
        );
    }

    #[test]
    fn a_fetched_sibling_with_the_benchmark_and_metric_is_a_mismatch() {
        let storage = MemoryStorage::new();
        let sibling = store_sibling(
            &storage,
            "m2",
            "c3",
            3,
            &run_with(bench_id(), MetricKind::InstructionCount),
        );
        let findings = [finding("m1", Some(2))];
        let lags = classify(&storage, &findings, &[], Some(3), &[sibling]);
        let set_lags = &lags[&set("m1")];
        assert_eq!(
            set_lags[0].reason,
            ComparisonBaseLagReason::DiscriminantSetMismatch
        );
    }

    #[test]
    fn a_fetched_sibling_missing_the_benchmark_is_generic() {
        // The newer sibling run exists but omits the affected benchmark, so it is not
        // evidence a comparable newer base-side measurement existed.
        let storage = MemoryStorage::new();
        let other = BenchmarkId::new(nonempty!["nm".to_owned(), "other".to_owned()]);
        let sibling = store_sibling(
            &storage,
            "m2",
            "c3",
            3,
            &run_with(other, MetricKind::InstructionCount),
        );
        let findings = [finding("m1", Some(2))];
        let lags = classify(&storage, &findings, &[], Some(3), &[sibling]);
        let set_lags = &lags[&set("m1")];
        assert_eq!(
            set_lags[0].reason,
            ComparisonBaseLagReason::NoRecentBaseData
        );
    }

    #[test]
    fn a_failed_sibling_fetch_degrades_to_the_generic_reason() {
        // The only mismatch evidence for this lagging finding would come from a
        // relevant sibling object that is missing from storage, so fetching it fails.
        // Advisory classification must not abort: it notes the failure and degrades to
        // the generic reason.
        let storage = MemoryStorage::new();
        let sibling = sibling_at("m2", "c3", 3);
        let findings = [finding("m1", Some(2))];
        let (lags, reporter) = classify_reported(&storage, &findings, &[], Some(3), &[sibling]);
        let set_lags = &lags[&set("m1")];
        assert_eq!(set_lags.len(), 1);
        assert_eq!(set_lags[0].commits_behind.get(), 1);
        assert_eq!(
            set_lags[0].reason,
            ComparisonBaseLagReason::NoRecentBaseData
        );
        assert!(reporter.contains("skipping comparison-base mismatch evidence"));
    }

    /// Loads the sibling evidence one lagging finding at index 2 would need, expecting
    /// the load to fail on `siblings`.
    fn sibling_evidence_error(
        storage: &MemoryStorage,
        siblings: &[SiblingObservation],
    ) -> AnalyzeError {
        let set = set("m1");
        let id = bench_id();
        let lagging = LaggingFinding {
            set: &set,
            id: &id,
            kind: MetricKind::InstructionCount,
            comparison_base_index: 2,
            commits_behind: NonZero::new(1).unwrap(),
        };
        // `SiblingEvidence` is not `Debug`, so `unwrap_err` is unavailable here.
        block_on(load_sibling_evidence(
            storage,
            &[&lagging],
            siblings,
            3,
            &RecordingReporter::quiet(),
        ))
        .err()
        .unwrap()
    }

    #[test]
    fn an_undecodable_sibling_object_retains_the_utf8_source() {
        // Evidence is fetched as raw bytes, so a payload that is not text at all fails
        // before any parsing is attempted.
        let storage = MemoryStorage::new();
        let sibling = store_sibling_bytes(&storage, "m2", "c3", 3, &[0xff, 0xfe, 0x00]);
        let error = sibling_evidence_error(&storage, &[sibling]);
        let found = error.find_source::<InvalidStoredUtf8Error>().unwrap();
        assert_eq!(found.object_kind, "stored object");
        assert!(found.key.ends_with("/c3/clean.json"));
        assert!(error.find_source::<std::str::Utf8Error>().is_some());
    }

    #[test]
    fn a_sibling_object_that_is_not_a_result_set_retains_the_json_source() {
        // Decodable text that does not describe a run is a distinct failure from
        // undecodable bytes, so it must not borrow the other's wording.
        let storage = MemoryStorage::new();
        let sibling = store_sibling_bytes(&storage, "m2", "c3", 3, b"{}");
        let error = sibling_evidence_error(&storage, &[sibling]);
        let found = error.find_source::<InvalidResultSetError>().unwrap();
        assert!(found.key.ends_with("/c3/clean.json"));
        assert!(error.find_source::<serde_json::Error>().is_some());
    }

    #[test]
    fn a_fetched_sibling_missing_the_metric_is_generic() {
        // The newer sibling run carries the benchmark but only a different metric.
        let storage = MemoryStorage::new();
        let sibling = store_sibling(
            &storage,
            "m2",
            "c3",
            3,
            &run_with(bench_id(), MetricKind::ConditionalBranches),
        );
        let findings = [finding("m1", Some(2))];
        let lags = classify(&storage, &findings, &[], Some(3), &[sibling]);
        let set_lags = &lags[&set("m1")];
        assert_eq!(
            set_lags[0].reason,
            ComparisonBaseLagReason::NoRecentBaseData
        );
    }

    #[test]
    fn a_sibling_at_or_before_the_comparison_base_is_ignored() {
        // A sibling at the comparison base (index 2, not strictly newer) is not
        // evidence of a *newer* base-side run and never triggers a fetch.
        let storage = MemoryStorage::new();
        let sibling = sibling_at("m2", "c2", 2);
        let findings = [finding("m1", Some(2))];
        let (lags, reporter) = classify_reported(&storage, &findings, &[], Some(3), &[sibling]);
        let set_lags = &lags[&set("m1")];
        assert_eq!(
            set_lags[0].reason,
            ComparisonBaseLagReason::NoRecentBaseData
        );
        assert!(!reporter.contains("fetching"), "no fetch was attempted");
    }

    #[test]
    fn a_sibling_newer_than_the_base_ref_is_not_fetched() {
        // Evidence must be no newer than the base ref. A sibling past the base ref
        // (index 5, base ref 3) is outside the finding's gap and never fetched.
        let storage = MemoryStorage::new();
        let sibling = sibling_at("m2", "c5", 5);
        let findings = [finding("m1", Some(2))];
        let (lags, reporter) = classify_reported(&storage, &findings, &[], Some(3), &[sibling]);
        let set_lags = &lags[&set("m1")];
        assert_eq!(
            set_lags[0].reason,
            ComparisonBaseLagReason::NoRecentBaseData
        );
        assert!(!reporter.contains("fetching"), "no fetch was attempted");
    }

    #[test]
    fn a_same_machine_key_observation_is_not_fetched() {
        // An observation under the finding's own machine key is the finding's own set,
        // not a rotation sibling, so even a newer clean point never triggers a fetch.
        let storage = MemoryStorage::new();
        let sibling = sibling_at("m1", "c3", 3);
        let findings = [finding("m1", Some(2))];
        let (lags, reporter) = classify_reported(&storage, &findings, &[], Some(3), &[sibling]);
        let set_lags = &lags[&set("m1")];
        assert_eq!(
            set_lags[0].reason,
            ComparisonBaseLagReason::NoRecentBaseData
        );
        assert!(!reporter.contains("fetching"), "no fetch was attempted");
    }

    #[test]
    fn identical_lags_in_one_set_deduplicate() {
        // Two findings in the same set with the same gap and reason collapse to one
        // warning line rather than repeating.
        let storage = MemoryStorage::new();
        let findings = [finding("m1", Some(2)), finding("m1", Some(2))];
        let lags = classify(&storage, &findings, &[], Some(3), &[]);
        let set_lags = &lags[&set("m1")];
        assert_eq!(set_lags.len(), 1);
    }

    #[test]
    fn distinct_lags_in_one_set_are_ordered_by_distance_descending() {
        // Partial runs can leave two findings in one set at different comparison
        // bases; both distinct lags are kept, largest gap first.
        let storage = MemoryStorage::new();
        let findings = [finding("m1", Some(2)), finding("m1", Some(1))];
        let lags = classify(&storage, &findings, &[], Some(3), &[]);
        let set_lags = &lags[&set("m1")];
        assert_eq!(set_lags.len(), 2);
        assert_eq!(set_lags[0].commits_behind.get(), 2);
        assert_eq!(set_lags[1].commits_behind.get(), 1);
    }

    #[test]
    fn a_loaded_sibling_point_at_the_comparison_base_does_not_prove_a_mismatch() {
        // Evidence must be strictly newer than the comparison base. A loaded sibling
        // point exactly at the comparison base (index 2) is the same commit, not a
        // newer base-side run, so the reason stays generic.
        let storage = MemoryStorage::new();
        let findings = [finding("m1", Some(2))];
        let series = [loaded_series(
            "m2",
            bench_id(),
            MetricKind::InstructionCount,
            2,
            false,
        )];
        let lags = classify(&storage, &findings, &series, Some(3), &[]);
        let set_lags = &lags[&set("m1")];
        assert_eq!(
            set_lags[0].reason,
            ComparisonBaseLagReason::NoRecentBaseData
        );
    }

    #[test]
    fn a_fetched_sibling_at_a_findings_comparison_base_does_not_prove_its_mismatch() {
        // One fetched object at index 3 is newer evidence for the finding whose gap it
        // falls in (comparison base 2), but sits exactly at the other finding's
        // comparison base (3). The strict lower bound is judged per finding, so the
        // shared object proves only the first finding's mismatch.
        let storage = MemoryStorage::new();
        let sibling = store_sibling(
            &storage,
            "m2",
            "c3",
            3,
            &run_with(bench_id(), MetricKind::InstructionCount),
        );
        let findings = [finding("m1", Some(2)), finding("m1", Some(3))];
        let lags = classify(&storage, &findings, &[], Some(4), &[sibling]);
        let set_lags = &lags[&set("m1")];
        assert_eq!(set_lags.len(), 2);
        assert_eq!(set_lags[0].commits_behind.get(), 2);
        assert_eq!(
            set_lags[0].reason,
            ComparisonBaseLagReason::DiscriminantSetMismatch
        );
        assert_eq!(set_lags[1].commits_behind.get(), 1);
        assert_eq!(
            set_lags[1].reason,
            ComparisonBaseLagReason::NoRecentBaseData
        );
    }

    #[test]
    fn a_set_is_a_sibling_only_when_the_machine_key_differs() {
        // Rotation evidence must share the comparable axes (engine and target triple)
        // and differ only in machine key. The same machine key is the finding's own
        // set, and a different engine or triple is not comparable.
        let target = set("m1");
        assert!(is_sibling_set(&set("m2"), &target), "different machine key");
        assert!(!is_sibling_set(&set("m1"), &target), "same machine key");
        let other_engine = DiscriminantSet {
            engine: Engine::Criterion,
            target_triple: "x86_64-unknown-linux-gnu".into(),
            machine_key: "m2".into(),
        };
        assert!(!is_sibling_set(&other_engine, &target), "different engine");
        let other_triple = DiscriminantSet {
            engine: Engine::Callgrind,
            target_triple: "aarch64-apple-darwin".into(),
            machine_key: "m2".into(),
        };
        assert!(!is_sibling_set(&other_triple, &target), "different triple");
    }

    #[test]
    fn equal_gaps_in_one_set_order_mismatch_before_generic() {
        // Two findings in one set at the same distance but different reasons keep a
        // deterministic order: the mismatch renders before the generic reason even when
        // the generic finding is discovered first.
        let storage = MemoryStorage::new();
        let other = BenchmarkId::new(nonempty!["nm".to_owned(), "other".to_owned()]);
        let generic = finding_for("m1", other, Some(2));
        let mismatch = finding("m1", Some(2));
        let series = [loaded_series(
            "m2",
            bench_id(),
            MetricKind::InstructionCount,
            3,
            false,
        )];
        let findings = [generic, mismatch];
        let lags = classify(&storage, &findings, &series, Some(3), &[]);
        let set_lags = &lags[&set("m1")];
        assert_eq!(set_lags.len(), 2);
        assert_eq!(set_lags[0].commits_behind.get(), 1);
        assert_eq!(set_lags[1].commits_behind.get(), 1);
        assert_eq!(
            set_lags[0].reason,
            ComparisonBaseLagReason::DiscriminantSetMismatch
        );
        assert_eq!(
            set_lags[1].reason,
            ComparisonBaseLagReason::NoRecentBaseData
        );
    }
}
