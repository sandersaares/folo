//! Branch-mode current-regime selection and historical report comparison.

use std::collections::{BTreeMap, BTreeSet, HashSet, VecDeque};
use std::num::NonZero;
use std::ops::Range;
use std::sync::Arc;

use anyspawn::Spawner;
use cbh_model::{DiscriminantSet, MetricKind};
use cbh_stats as stats;

use crate::detect::findings::{
    AnalysisContext, BranchComparison, BranchComparisonTrace, BranchEvaluationTrace,
    BranchExcursion, BranchRangeRelation, BranchSeriesTrace, Detection, Direction, Finding,
    FindingMethod, SeriesCensus, count_to_f64, materialize_chart, passes_significance,
    short_commit,
};
use crate::detect::gate_log::{Gate, GateLog, GateStage, StageLog};
use crate::detect::parallel::{balanced_chunk_sizes, worker_count};
use crate::detect::{BaseLevel, Series, SeriesPoint, Testability, UnjudgedReason, noise_gates};

/// Maximum number of pairwise and derived seeds retained for multi-way family search.
///
/// The historical comparison is explanatory context and never suppresses findings, so
/// a deterministic bounded search is preferable to an exact set-intersection closure
/// whose candidate count can grow combinatorially. The budget keeps the extra closure
/// work proportional to a production-sized benchmark suite while the complete
/// chronological-window and pairwise passes remain uncapped.
const MAX_BRANCH_MULTIWAY_SEEDS: usize = 1_024;

/// One value that can take a candidate or reference role.
#[derive(Clone, Copy)]
struct Observation {
    value: f64,
    interval: Option<(f64, f64)>,
}

/// A prepared branch series whose regime boundary no candidate can influence.
struct PreparedSeries {
    source_index: usize,
    current_start: usize,
    previous_range: Option<(f64, f64)>,
    current_regime_start: Option<String>,
    tip: Observation,
    tip_commit: Option<String>,
    stable_for_comparison: bool,
    actual: ExcursionEvaluation,
}

/// One series' preparation result and its production decision trace.
struct PreparedEntry {
    prepared: Option<PreparedSeries>,
    unjudged: Option<UnjudgedReason>,
    trace: BranchSeriesTrace,
}

/// A candidate measurement's relation to its references and gate outcomes.
#[derive(Clone)]
struct ExcursionEvaluation {
    relation: BranchRangeRelation,
    reference_count: usize,
    reference_min: Option<f64>,
    reference_max: Option<f64>,
    edge: Option<f64>,
    excess: Option<f64>,
    relative_excess: Option<f64>,
    relative_floor_passed: Option<bool>,
    absolute_floor_passed: Option<bool>,
    interval_disjoint_passed: Option<bool>,
    noise_band_passed: Option<bool>,
    survives: bool,
}

/// One rectangular historical family selected without reading excursion magnitude.
struct HistoricalFamily {
    member_indices: Vec<usize>,
    candidate_commits: Vec<usize>,
}

/// Regime selection performed only on selector-lane measurements.
struct RegimeSelection {
    current_start: usize,
    previous_range: Option<(f64, f64)>,
    boundary_commit: Option<String>,
    unresolved: bool,
}

/// The branch-mode verdict that both detection and testability consume.
struct BranchJudgment<'a> {
    tip_points: &'a [SeriesPoint],
    selection: RegimeSelection,
}

/// Runs branch analysis sequentially.
#[cfg(any(test, feature = "private-test-util"))]
pub(crate) fn find_changes(series: &[Series], context: &AnalysisContext) -> Detection {
    let entries: Vec<PreparedEntry> = series
        .iter()
        .enumerate()
        .map(|(index, one)| prepare_series(index, one, context, &mut GateLog::disabled()))
        .collect();
    finish(series, entries, context)
}

/// Runs branch preparation through the injected worker pool and finalizes the report.
pub(crate) async fn find_changes_spawned(
    series: Arc<[Series]>,
    context: AnalysisContext,
    spawner: &Spawner,
) -> Detection {
    let len = series.len();
    let workers = worker_count(len);
    let mut handles = Vec::with_capacity(workers);
    let mut start = 0_usize;
    for size in balanced_chunk_sizes(len, workers) {
        let end = start.saturating_add(size);
        handles.push(spawner.spawn_blocking({
            let series = Arc::clone(&series);
            move || prepare_range(&series, start..end, &context)
        }));
        start = end;
    }

    let mut entries = Vec::with_capacity(len);
    for handle in handles {
        entries.extend(handle.await);
    }
    finish(&series, entries, &context)
}

/// Shares the branch-mode verdict between the public testability check and the
/// detector's preparation path.
pub(crate) fn testability(series: &Series, context: &AnalysisContext) -> Testability {
    match branch_judgment(series, context) {
        Ok(_) => Testability::Judged,
        Err(reason) => Testability::Unjudged(reason),
    }
}

/// Evaluates one series with the same branch evaluator production uses.
#[cfg(any(test, feature = "private-test-util"))]
pub(crate) fn evaluate_with_log(
    series: &Series,
    context: &AnalysisContext,
) -> (Option<Finding>, GateLog) {
    let mut log = GateLog::recording();
    let entry = prepare_series(0, series, context, &mut log);
    let mut detection = finish(std::slice::from_ref(series), vec![entry], context);
    (detection.findings.pop(), log)
}

fn prepare_range(
    series: &[Series],
    range: Range<usize>,
    context: &AnalysisContext,
) -> Vec<PreparedEntry> {
    range
        .map(|index| {
            let one = series
                .get(index)
                .expect("the worker range is bounded by the series slice");
            prepare_series(index, one, context, &mut GateLog::disabled())
        })
        .collect()
}

fn prepare_series(
    source_index: usize,
    series: &Series,
    context: &AnalysisContext,
    log: &mut GateLog,
) -> PreparedEntry {
    let selector_commits: Vec<usize> = series
        .base_window
        .iter()
        .enumerate()
        .filter(|(index, _)| is_selector_position(*index))
        .map(|(_, level)| level.topo_index)
        .collect();
    let reference_commits: Vec<usize> = series
        .base_window
        .iter()
        .enumerate()
        .filter(|(index, _)| !is_selector_position(*index))
        .map(|(_, level)| level.topo_index)
        .collect();
    let mut trace = BranchSeriesTrace {
        set: series.set.clone(),
        id: series.id.clone(),
        kind: series.kind,
        available_base_commits: series.base_history_count.max(series.base_window.len()),
        retained_base_commits: series.base_window.len(),
        selector_commits,
        reference_commits,
        current_regime_start: None,
        unresolved: None,
        current_range: None,
        previous_range: None,
        reference_count: 0,
        branch_relation: BranchRangeRelation::Unavailable,
        relative_floor_passed: None,
        absolute_floor_passed: None,
        interval_disjoint_passed: None,
        noise_band_passed: None,
        included_in_historical_comparison: false,
    };
    let enough_base = series.base_window.len() >= noise_gates::MIN_SERIES_POINTS;
    log.stage(GateStage::Branch).numeric(
        Gate::MinBaseCommits,
        count_to_f64(series.base_window.len()),
        count_to_f64(noise_gates::MIN_SERIES_POINTS),
        enough_base,
    );
    let judgment = match branch_judgment(series, context) {
        Ok(judgment) => judgment,
        Err(reason) => return unjudged(trace, reason),
    };
    let current = series
        .base_window
        .get(judgment.selection.current_start..)
        .unwrap_or_default();
    let references: Vec<Observation> = current.iter().map(observation_of_level).collect();
    let tip = observation_of_points(judgment.tip_points);
    let actual = evaluate_excursion(series.kind, tip, &references, log);
    let current_range = range_of(current.iter().map(|level| level.value));
    let current_regime_start = judgment.selection.boundary_commit.or_else(|| {
        series
            .blessing
            .as_ref()
            .map(|blessing| blessing.commit.clone())
    });

    trace.current_regime_start = current.first().map(|level| level.topo_index);
    trace.current_range = current_range;
    trace.previous_range = judgment.selection.previous_range;
    trace.reference_count = actual.reference_count;
    trace.branch_relation = actual.relation;
    trace.relative_floor_passed = actual.relative_floor_passed;
    trace.absolute_floor_passed = actual.absolute_floor_passed;
    trace.interval_disjoint_passed = actual.interval_disjoint_passed;
    trace.noise_band_passed = actual.noise_band_passed;

    PreparedEntry {
        prepared: Some(PreparedSeries {
            source_index,
            current_start: judgment.selection.current_start,
            previous_range: judgment.selection.previous_range,
            current_regime_start,
            tip,
            tip_commit: judgment
                .tip_points
                .first()
                .and_then(|point| point.commit.as_deref().map(str::to_owned)),
            stable_for_comparison: regime_is_stable(current),
            actual,
        }),
        unjudged: None,
        trace,
    }
}

fn unjudged(mut trace: BranchSeriesTrace, reason: UnjudgedReason) -> PreparedEntry {
    trace.unresolved = Some(reason);
    PreparedEntry {
        prepared: None,
        unjudged: Some(reason),
        trace,
    }
}

fn branch_judgment<'a>(
    series: &'a Series,
    context: &AnalysisContext,
) -> Result<BranchJudgment<'a>, UnjudgedReason> {
    let Some(tip_points) = latest_context_points(&series.points, context.tip_index) else {
        return Err(UnjudgedReason::NotMeasuredOnBranch);
    };
    if series.base_window.len() < noise_gates::MIN_SERIES_POINTS {
        return Err(
            if series.blessing.is_some()
                && series.base_history_count >= noise_gates::MIN_SERIES_POINTS
            {
                UnjudgedReason::TooFewBaseCommitsSinceBlessing
            } else {
                UnjudgedReason::TooFewBaseCommits
            },
        );
    }
    let selection = select_regime(series);
    if selection.unresolved {
        return Err(UnjudgedReason::CurrentBaseRegimeUnresolved);
    }
    Ok(BranchJudgment {
        tip_points,
        selection,
    })
}

fn select_regime(series: &Series) -> RegimeSelection {
    if series.base_window.len() < noise_gates::MIN_BRANCH_REGIME_SELECTION_COMMITS {
        return RegimeSelection {
            current_start: 0,
            previous_range: None,
            boundary_commit: None,
            unresolved: false,
        };
    }
    let selector: Vec<&BaseLevel> = series
        .base_window
        .iter()
        .enumerate()
        .filter(|(index, _)| is_selector_position(*index))
        .map(|(_, level)| level)
        .collect();

    let search_alpha = regime_search_alpha(selector.len());
    // Check the calibration before recursive segmentation because a broader-than-budget chance
    // level can make every candidate eligible and drive prohibitively expensive searches.
    debug_assert!(
        search_alpha <= noise_gates::MAX_BRANCH_REGIME_CHANCE_LEVEL,
        "one regime search cannot consume more than the complete search budget",
    );
    let mut boundaries = Vec::new();
    collect_supported_boundaries(series.kind, &selector, 0, search_alpha, &mut boundaries);
    extend_current_regime(series.kind, &selector, search_alpha, &mut boundaries);

    // The newest supported regime is the comparison regime; every older one belongs to the
    // history the branch is no longer measured against. Ref: docs/DESIGN.md, "Branch mode".
    let current_selector_start = boundaries.last().copied().unwrap_or(0);
    let current_start = boundaries
        .last()
        .map_or(0, |&boundary| base_start_after(series, &selector, boundary));
    let previous_start = (!boundaries.is_empty()).then(|| {
        boundaries
            .len()
            .checked_sub(2)
            .and_then(|index| boundaries.get(index))
            .map_or(0, |&boundary| base_start_after(series, &selector, boundary))
    });
    let boundary_commit = boundaries.last().and_then(|_| {
        series
            .base_window
            .get(current_start)
            .and_then(|level| level.commit.as_deref())
            .map(str::to_owned)
    });

    let unresolved = recent_step_is_unresolved(
        series.kind,
        selector.get(current_selector_start..).unwrap_or_default(),
    );
    let previous_range = previous_start.and_then(|start| {
        // The reference observation immediately before `current_start` lies between the
        // selector observations that establish the boundary, so neither adjacent regime
        // can claim it as evidence.
        let end = current_start.checked_sub(1)?;
        range_of(
            series
                .base_window
                .get(start..end)?
                .iter()
                .map(|level| level.value),
        )
    });
    RegimeSelection {
        current_start,
        previous_range,
        boundary_commit,
        unresolved,
    }
}

/// Appends every supported regime boundary in `selector`, in ascending selector-lane order.
///
/// Each search locates the strongest split in its segment and then recurses into *both*
/// sides, whether or not that split turned out to be a boundary. The strongest split need
/// not be a real one, and a statistically negligible step must not discard the candidates
/// sitting on either side of it: descending regardless is what keeps a large earlier
/// boundary from being hidden behind a step that merely happens to sit nearer the middle.
/// Ref: docs/DESIGN.md, "Branch mode".
fn collect_supported_boundaries(
    kind: MetricKind,
    selector: &[&BaseLevel],
    offset: usize,
    search_alpha: f64,
    boundaries: &mut Vec<usize>,
) {
    let Some(split) = strongest_split(kind, selector, search_alpha) else {
        return;
    };
    // A split always leaves a full regime on each side, so both halves are strictly
    // shorter than the segment and the recursion terminates. Visiting the earlier half,
    // then this split, then the later half appends the boundaries in ascending order.
    let (before, after) = selector.split_at(split.index);
    collect_supported_boundaries(kind, before, offset, search_alpha, boundaries);
    if split.supported {
        boundaries.push(offset.saturating_add(split.index));
    }
    collect_supported_boundaries(
        kind,
        after,
        offset.saturating_add(split.index),
        search_alpha,
        boundaries,
    );
}

/// Advances the comparison regime past boundaries the segmentation could not certify.
///
/// A stretch that starts and ends at the same level — a base regression that was later
/// reverted, say — hides its exit. Measured against everything that came before it, the
/// exit's two sides have the same median and its step statistic is diluted by the matching
/// prefix, so segmentation certifies only the entry and leaves the stretch inside the
/// comparison range. Segmenting the suffix that begins at the newest certified boundary
/// measures the exit against the stretch it actually ends and searches around any stronger
/// unsupported split in that suffix. Ref: docs/DESIGN.md, "Branch mode".
fn extend_current_regime(
    kind: MetricKind,
    selector: &[&BaseLevel],
    search_alpha: f64,
    boundaries: &mut Vec<usize>,
) {
    let Some(mut start) = boundaries.last().copied() else {
        // Repeating the initial segmentation cannot discover anything new.
        return;
    };
    let max_rounds = selector
        .len()
        .checked_div(noise_gates::MIN_REGIME)
        .unwrap_or(0)
        .saturating_sub(1);
    for _ in 0..max_rounds {
        let suffix = selector.get(start..).unwrap_or_default();
        // Segmenting the suffix, rather than accepting only its strongest split, lets an
        // irrelevant long-running move conceal neither side of a shorter supported regime.
        collect_supported_boundaries(kind, suffix, start, search_alpha, boundaries);
        let Some(next_start) = boundaries.last().copied() else {
            return;
        };
        if next_start == start {
            return;
        }
        start = next_start;
    }
    unreachable!("each extension round advances by at least one complete regime");
}

/// The strongest split of one segment, and what the searcher concluded about it.
struct StrongestSplit {
    /// Where the segment divides, as an offset from its own start.
    index: usize,

    /// Whether the division is significant enough to survive the shared error budget *and*
    /// large enough to matter, making it a regime boundary rather than a mere search step.
    supported: bool,
}

/// Searches one segment for its strongest split, charging the search to the shared budget.
fn strongest_split(
    kind: MetricKind,
    selector: &[&BaseLevel],
    search_alpha: f64,
) -> Option<StrongestSplit> {
    let values: Vec<f64> = selector.iter().map(|level| level.value).collect();
    let calibration = stats::SelectionCalibration {
        permutation_order_budget: NonZero::new(noise_gates::MIN_CHANGE_PERMUTATION_ORDER)
            .expect("the configured permutation order is nonzero"),
        analytic_weight: noise_gates::CHANGE_ANALYTIC_WEIGHT,
        accept_analytic_below: search_alpha,
        reject_at_or_above: search_alpha,
    };
    // A segment too short to hold a full regime on each side yields no split at all, which
    // is what ends the recursion.
    let change =
        stats::selection_adjusted_change_point(&values, noise_gates::MIN_REGIME, calibration)?;
    Some(StrongestSplit {
        index: change.index,
        supported: passes_significance(change.adjusted_p, search_alpha)
            && supported_boundary(kind, &values, change.index, change.superiority),
    })
}
/// Maps a selector-lane boundary to the first base observation known to be after it.
///
/// A reference-lane commit can sit between the selector observations on either side of
/// the split. Its regime is ambiguous, so it cannot honestly define the current range;
/// start at the first selector observation known to be after the boundary instead.
fn base_start_after(series: &Series, selector: &[&BaseLevel], boundary: usize) -> usize {
    let Some(split_after) = selector.get(boundary) else {
        return 0;
    };
    series
        .base_window
        .partition_point(|level| level.topo_index < split_after.topo_index)
}

/// Allocates the declared boundary-error budget across every possible search.
///
/// Boundaries are located in two stages, and either stage can place one, so the budget
/// covers both. One segmentation can search once fewer than the number of minimum regimes
/// in its input. Each extension starts at least one minimum regime later than its
/// predecessor, so the maximum across the initial segmentation and every possible suffix
/// is the triangular sum of those per-segmentation bounds.
fn regime_search_alpha(selector_len: usize) -> f64 {
    let minimum_regimes = selector_len
        .checked_div(noise_gates::MIN_REGIME)
        .unwrap_or(0);
    let max_searches = minimum_regimes
        .saturating_mul(minimum_regimes.saturating_sub(1))
        .checked_div(2)
        .expect("the triangular-number divisor is nonzero")
        .max(1);
    noise_gates::MAX_BRANCH_REGIME_CHANCE_LEVEL / count_to_f64(max_searches)
}

fn supported_boundary(kind: MetricKind, values: &[f64], split: usize, superiority: f64) -> bool {
    let Some(after) = values.get(split..) else {
        return false;
    };
    let before = values
        .get(..split)
        .expect("the same split was bounded by the values slice");
    let (Some(before_level), Some(after_level)) = (stats::median(before), stats::median(after))
    else {
        return false;
    };
    let delta = after_level - before_level;
    relative_delta(delta, before_level).abs() >= noise_gates::BRANCH_PRACTICAL_RELATIVE
        && delta.abs() >= absolute_floor(kind)
        && oriented_superiority(superiority, delta) >= noise_gates::MIN_BASE_SPLIT_SEPARATION
}

fn recent_step_is_unresolved(kind: MetricKind, selector: &[&BaseLevel]) -> bool {
    for trailing in noise_gates::MIN_UNRESOLVED_BRANCH_REGIME_POINTS..noise_gates::MIN_REGIME {
        let split = selector.len().saturating_sub(trailing);
        let before_start = split.saturating_sub(noise_gates::MIN_REGIME);
        // An unresolved regime is a local discontinuity. Comparing the short tail with the
        // complete earlier window would mistake a smooth long-running trend for a recent step.
        let before_levels = selector
            .get(before_start..split)
            .expect("the saturating look-back ends at a bounded split");
        let after_levels = selector
            .get(split..)
            .expect("the trailing split was derived from the selector length");
        if before_levels.len() < noise_gates::MIN_REGIME {
            continue;
        }
        let before: Vec<f64> = before_levels.iter().map(|level| level.value).collect();
        let after: Vec<f64> = after_levels.iter().map(|level| level.value).collect();
        let before_level =
            stats::median(&before).expect("a complete preceding regime is non-empty");
        let after_level =
            stats::median(&after).expect("the configured trailing group is non-empty");
        let superiority = stats::mann_whitney_superiority(&before, &after)
            .expect("both emerging-regime samples are non-empty");
        let delta = after_level - before_level;
        if relative_delta(delta, before_level).abs() >= noise_gates::BRANCH_PRACTICAL_RELATIVE
            && delta.abs() >= absolute_floor(kind)
            && oriented_superiority(superiority, delta) >= noise_gates::MIN_BASE_SPLIT_SEPARATION
        {
            return true;
        }
    }
    false
}

fn regime_is_stable(levels: &[BaseLevel]) -> bool {
    let values: Vec<f64> = levels.iter().map(|level| level.value).collect();
    let trend = stats::mann_kendall(&values);
    let (slope, _) =
        stats::theil_sen_line(&values).expect("a judged current regime has multiple observations");
    let span = count_to_f64(values.len().saturating_sub(1));
    let movement = slope * span;
    let baseline = stats::median(&values).unwrap_or(0.0);
    !(trend.p_value < noise_gates::MAX_DRIFT_CHANCE_LEVEL
        && relative_delta(movement, baseline).abs() >= noise_gates::BRANCH_PRACTICAL_RELATIVE)
}

fn evaluate_excursion(
    kind: MetricKind,
    candidate: Observation,
    references: &[Observation],
    log: &mut GateLog,
) -> ExcursionEvaluation {
    let Some((minimum, maximum)) = range_of(references.iter().map(|one| one.value)) else {
        return unavailable_excursion();
    };
    let (relation, edge) = if candidate.value < minimum {
        (BranchRangeRelation::Below, minimum)
    } else if candidate.value > maximum {
        (BranchRangeRelation::Above, maximum)
    } else {
        log.stage(GateStage::Branch)
            .numeric(Gate::NonZeroDelta, 0.0, 0.0, false);
        return ExcursionEvaluation {
            relation: BranchRangeRelation::Inside,
            reference_count: references.len(),
            reference_min: Some(minimum),
            reference_max: Some(maximum),
            edge: None,
            excess: None,
            relative_excess: None,
            relative_floor_passed: None,
            absolute_floor_passed: None,
            interval_disjoint_passed: None,
            noise_band_passed: None,
            survives: false,
        };
    };
    let excess = candidate.value - edge;
    let relative_excess = relative_delta(excess, edge);
    let mut evaluation = ExcursionEvaluation {
        relation,
        reference_count: references.len(),
        reference_min: Some(minimum),
        reference_max: Some(maximum),
        edge: Some(edge),
        excess: Some(excess),
        relative_excess: Some(relative_excess),
        relative_floor_passed: None,
        absolute_floor_passed: None,
        interval_disjoint_passed: None,
        noise_band_passed: None,
        survives: false,
    };
    let mut stage = log.stage(GateStage::Branch);
    stage.numeric(Gate::NonZeroDelta, excess.abs(), 0.0, true);
    let relative_floor_passed = relative_excess.abs() >= noise_gates::BRANCH_PRACTICAL_RELATIVE;
    stage.numeric(
        Gate::RelativeFloor,
        relative_excess.abs(),
        noise_gates::BRANCH_PRACTICAL_RELATIVE,
        relative_floor_passed,
    );
    evaluation.relative_floor_passed = Some(relative_floor_passed);
    if !relative_floor_passed {
        return evaluation;
    }

    let floor = absolute_floor(kind);
    let absolute_floor_passed = excess.abs() >= floor;
    stage.numeric(
        Gate::AbsoluteFloor,
        excess.abs(),
        floor,
        absolute_floor_passed,
    );
    evaluation.absolute_floor_passed = Some(absolute_floor_passed);
    if !absolute_floor_passed {
        return evaluation;
    }

    let interval_disjoint_passed = intervals_allow(relation, candidate, references, &mut stage);
    evaluation.interval_disjoint_passed = interval_disjoint_passed;
    if interval_disjoint_passed == Some(false) {
        return evaluation;
    }

    let noise_band_passed = noise_band_allows(excess, candidate, references, &mut stage);
    evaluation.noise_band_passed = noise_band_passed;
    evaluation.survives = noise_band_passed.unwrap_or(true);
    evaluation
}

fn unavailable_excursion() -> ExcursionEvaluation {
    ExcursionEvaluation {
        relation: BranchRangeRelation::Unavailable,
        reference_count: 0,
        reference_min: None,
        reference_max: None,
        edge: None,
        excess: None,
        relative_excess: None,
        relative_floor_passed: None,
        absolute_floor_passed: None,
        interval_disjoint_passed: None,
        noise_band_passed: None,
        survives: false,
    }
}

fn intervals_allow(
    relation: BranchRangeRelation,
    candidate: Observation,
    references: &[Observation],
    log: &mut StageLog<'_>,
) -> Option<bool> {
    let candidate_interval = candidate.interval?;
    let reference_intervals: Vec<(f64, f64)> =
        references.iter().filter_map(|one| one.interval).collect();
    if reference_intervals.is_empty() {
        return None;
    }
    let passed = match relation {
        BranchRangeRelation::Above => {
            let highest = reference_intervals
                .iter()
                .map(|(_, high)| *high)
                .max_by(f64::total_cmp)
                .expect("non-empty reference intervals");
            candidate_interval.0 > highest
        }
        BranchRangeRelation::Below => {
            let lowest = reference_intervals
                .iter()
                .map(|(low, _)| *low)
                .min_by(f64::total_cmp)
                .expect("non-empty reference intervals");
            candidate_interval.1 < lowest
        }
        BranchRangeRelation::Inside | BranchRangeRelation::Unavailable => return None,
    };
    log.boolean(Gate::IntervalDisjoint, passed);
    Some(passed)
}

fn noise_band_allows(
    excess: f64,
    candidate: Observation,
    references: &[Observation],
    log: &mut StageLog<'_>,
) -> Option<bool> {
    let mut half_widths: Vec<f64> = references
        .iter()
        .filter_map(|one| one.interval.map(interval_half_width))
        .chain(candidate.interval.map(interval_half_width))
        .collect();
    let half_width = stats::median_in_place(&mut half_widths)?;
    let band = noise_gates::BRANCH_NOISE_MULTIPLE * half_width;
    let passed = excess.abs() > band;
    log.numeric(Gate::IntervalNoiseBand, excess.abs(), band, passed);
    Some(passed)
}

fn finish(
    series: &[Series],
    mut entries: Vec<PreparedEntry>,
    context: &AnalysisContext,
) -> Detection {
    let mut census = SeriesCensus::default();
    for entry in &entries {
        census.record(match entry.unjudged {
            Some(reason) => Testability::Unjudged(reason),
            None => Testability::Judged,
        });
    }

    let (branch_comparisons, comparison_traces) = historical_comparisons(series, &mut entries);
    let mut findings: Vec<Finding> = entries
        .iter()
        .filter_map(|entry| finding_of(series, entry, context))
        .collect();
    findings.sort_by(|left, right| {
        right
            .relative_delta
            .abs()
            .total_cmp(&left.relative_delta.abs())
            .then_with(|| left.set.cmp(&right.set))
            .then_with(|| left.id.cmp(&right.id))
            .then_with(|| left.kind.cmp(&right.kind))
    });
    let trace = BranchEvaluationTrace {
        series: entries.into_iter().map(|entry| entry.trace).collect(),
        comparisons: comparison_traces,
    };
    Detection {
        findings,
        census,
        branch_comparisons,
        branch_trace: trace,
    }
}

fn finding_of(
    series: &[Series],
    entry: &PreparedEntry,
    context: &AnalysisContext,
) -> Option<Finding> {
    let prepared = entry.prepared.as_ref()?;
    if !prepared.actual.survives {
        return None;
    }
    let source = series
        .get(prepared.source_index)
        .expect("the prepared source index came from this series slice");
    let edge = prepared.actual.edge?;
    let excess = prepared.actual.excess?;
    let relative_excess = prepared.actual.relative_excess?;
    let reference_min = prepared.actual.reference_min?;
    let reference_max = prepared.actual.reference_max?;
    let matches_previous_regime = prepared.previous_range.is_some_and(|(minimum, maximum)| {
        prepared.tip.value >= minimum && prepared.tip.value <= maximum
    });
    let included = entry.trace.included_in_historical_comparison;
    let mut finding = Finding {
        set: source.set.clone(),
        id: source.id.clone(),
        kind: source.kind,
        method: FindingMethod::BranchExcursion,
        direction: direction_of(excess),
        baseline: edge,
        latest: prepared.tip.value,
        delta: excess,
        relative_delta: relative_excess,
        commit: prepared.tip_commit.clone(),
        window_start_commit: None,
        blessed_at: source
            .blessing
            .as_ref()
            .map(|blessing| short_commit(&blessing.commit)),
        blessed_commit_time: source
            .blessing
            .as_ref()
            .and_then(|blessing| blessing.commit_time)
            .map(|time| time.to_string()),
        series: Vec::new(),
        comparison_base_index: source
            .base_window
            .get(prepared.current_start..)
            .and_then(|levels| levels.last())
            .map(|level| level.topo_index),
        chart_base_ref: None,
        branch: Some(BranchExcursion {
            reference_count: prepared.actual.reference_count,
            reference_min,
            reference_max,
            excess,
            relative_excess,
            current_regime_start: prepared.current_regime_start.clone(),
            matches_previous_regime,
            included_in_historical_comparison: included,
        }),
    };
    materialize_chart(source, &mut finding, context);
    Some(finding)
}

fn historical_comparisons(
    series: &[Series],
    entries: &mut [PreparedEntry],
) -> (Vec<BranchComparison>, Vec<BranchComparisonTrace>) {
    let mut by_set: BTreeMap<DiscriminantSet, Vec<usize>> = BTreeMap::new();
    for (index, entry) in entries.iter().enumerate() {
        let Some(prepared) = entry.prepared.as_ref() else {
            continue;
        };
        if prepared.stable_for_comparison {
            by_set
                .entry(entry.trace.set.clone())
                .or_default()
                .push(index);
        }
    }

    let mut comparisons = Vec::new();
    let mut traces = Vec::new();
    for (set, eligible) in by_set {
        let Some(family) = select_family(series, entries, &eligible) else {
            continue;
        };
        for &index in &family.member_indices {
            let entry = entries
                .get_mut(index)
                .expect("the family index came from the entries slice");
            entry.trace.included_in_historical_comparison = true;
        }

        let branch_score = score_candidate(series, entries, &family, None);
        let base_scores: Vec<f64> = family
            .candidate_commits
            .iter()
            .map(|&topo_index| score_candidate(series, entries, &family, Some(topo_index)))
            .collect();
        let at_least_as_much = base_scores
            .iter()
            .filter(|&&score| score >= branch_score)
            .count();
        comparisons.push(BranchComparison {
            set: set.clone(),
            evaluated_base_commits: base_scores.len(),
            at_least_as_much,
            series: family.member_indices.len(),
        });
        traces.push(BranchComparisonTrace {
            set,
            base_scores,
            branch_score,
            at_least_as_much,
        });
    }
    (comparisons, traces)
}

fn select_family(
    series: &[Series],
    entries: &[PreparedEntry],
    eligible: &[usize],
) -> Option<HistoricalFamily> {
    let candidate_sets: BTreeMap<usize, BTreeSet<usize>> = eligible
        .iter()
        .filter_map(|&index| {
            let prepared = entries.get(index)?.prepared.as_ref()?;
            let source = series.get(prepared.source_index)?;
            let candidates = source
                .base_window
                .iter()
                .enumerate()
                .skip(prepared.current_start)
                .filter(|(index, _)| !is_selector_position(*index))
                .map(|(_, level)| level.topo_index)
                .collect();
            Some((index, candidates))
        })
        .collect();

    let mut grouped = BTreeMap::<BTreeSet<usize>, Vec<usize>>::new();
    for (&index, candidates) in &candidate_sets {
        grouped.entry(candidates.clone()).or_default().push(index);
    }
    let groups: Vec<(BTreeSet<usize>, Vec<usize>)> = grouped.into_iter().collect();
    if groups.is_empty() {
        return None;
    }

    let bits_per_word =
        usize::try_from(u64::BITS).expect("the platform can represent a u64 bit count");
    let word_count = groups.len().div_ceil(bits_per_word);
    let mut commit_members = BTreeMap::<usize, Vec<u64>>::new();
    for (group_index, (candidates, _)) in groups.iter().enumerate() {
        for &commit in candidates {
            let words = commit_members
                .entry(commit)
                .or_insert_with(|| vec![0_u64; word_count]);
            let Some((word_index, bit_index)) = membership_position(group_index, bits_per_word)
            else {
                continue;
            };
            if let Some(word) = words.get_mut(word_index) {
                *word |= 1_u64 << bit_index;
            }
        }
    }

    let minimum = noise_gates::MIN_BRANCH_COMPARISON_COMMITS;
    let mut seen_members = HashSet::new();
    let mut best: Option<HistoricalFamily> = None;
    for (candidates, _) in &groups {
        let ordered: Vec<usize> = candidates.iter().copied().collect();
        for seed in ordered.windows(minimum) {
            consider_family_seed(
                seed,
                &commit_members,
                &groups,
                bits_per_word,
                &mut seen_members,
                &mut best,
            );
        }
    }

    let pair_seed_limit = MAX_BRANCH_MULTIWAY_SEEDS
        .checked_div(2)
        .expect("the multi-way seed budget has a nonzero divisor");
    let mut closure_seeds = VecDeque::new();
    let mut seen_closure_seeds = HashSet::new();
    for (left_index, (left, _)) in groups.iter().enumerate() {
        for (right, _) in groups.iter().skip(left_index.saturating_add(1)) {
            let intersection: Vec<usize> = left.intersection(right).copied().collect();
            if intersection.len() < minimum {
                continue;
            }
            consider_family_seed(
                &intersection,
                &commit_members,
                &groups,
                bits_per_word,
                &mut seen_members,
                &mut best,
            );
            enqueue_family_seed(
                intersection,
                pair_seed_limit,
                &mut seen_closure_seeds,
                &mut closure_seeds,
            );
        }
    }

    'closure: while let Some(seed) = closure_seeds.pop_front() {
        for (candidates, _) in &groups {
            if seen_closure_seeds.len() >= MAX_BRANCH_MULTIWAY_SEEDS {
                break 'closure;
            }
            let intersection: Vec<usize> = seed
                .iter()
                .copied()
                .filter(|commit| candidates.contains(commit))
                .collect();
            if intersection.len() < minimum {
                continue;
            }
            if enqueue_family_seed(
                intersection.clone(),
                MAX_BRANCH_MULTIWAY_SEEDS,
                &mut seen_closure_seeds,
                &mut closure_seeds,
            ) {
                consider_family_seed(
                    &intersection,
                    &commit_members,
                    &groups,
                    bits_per_word,
                    &mut seen_members,
                    &mut best,
                );
            }
        }
    }
    best.filter(|family| family.candidate_commits.len() >= minimum)
}

fn enqueue_family_seed(
    seed: Vec<usize>,
    limit: usize,
    seen: &mut HashSet<Vec<usize>>,
    queue: &mut VecDeque<Vec<usize>>,
) -> bool {
    if seen.len() >= limit {
        return false;
    }
    if !seen.insert(seed.clone()) {
        return false;
    }
    queue.push_back(seed);
    true
}

fn consider_family_seed(
    seed: &[usize],
    commit_members: &BTreeMap<usize, Vec<u64>>,
    groups: &[(BTreeSet<usize>, Vec<usize>)],
    bits_per_word: usize,
    seen_members: &mut HashSet<Vec<u64>>,
    best: &mut Option<HistoricalFamily>,
) {
    let Some(first) = seed.first().and_then(|commit| commit_members.get(commit)) else {
        return;
    };
    let mut member_words = first.clone();
    for commit in seed.iter().skip(1) {
        let Some(next) = commit_members.get(commit) else {
            return;
        };
        for (members, next_members) in member_words.iter_mut().zip(next) {
            *members &= next_members;
        }
    }

    let member_count: usize = groups
        .iter()
        .enumerate()
        .filter(|(index, _)| membership_contains(&member_words, *index, bits_per_word))
        .map(|(_, (_, members))| members.len())
        .sum();
    if member_count == 0
        || best
            .as_ref()
            .is_some_and(|current| member_count < current.member_indices.len())
        || !seen_members.insert(member_words.clone())
    {
        return;
    }

    let mut member_groups = groups
        .iter()
        .enumerate()
        .filter(|(index, _)| membership_contains(&member_words, *index, bits_per_word))
        .map(|(_, group)| group);
    let Some((first_candidates, first_members)) = member_groups.next() else {
        return;
    };
    let mut common = first_candidates.clone();
    let mut members = first_members.clone();
    for (candidates, group_members) in member_groups {
        common.retain(|candidate| candidates.contains(candidate));
        members.extend(group_members);
    }
    if common.len() < noise_gates::MIN_BRANCH_COMPARISON_COMMITS {
        return;
    }
    let candidate = HistoricalFamily {
        member_indices: members,
        candidate_commits: common.into_iter().collect(),
    };
    if family_is_better(&candidate, best.as_ref()) {
        *best = Some(candidate);
    }
}

fn membership_contains(words: &[u64], index: usize, bits_per_word: usize) -> bool {
    let Some((word_index, bit_index)) = membership_position(index, bits_per_word) else {
        return false;
    };
    words
        .get(word_index)
        .is_some_and(|word| word & (1_u64 << bit_index) != 0)
}

fn membership_position(index: usize, bits_per_word: usize) -> Option<(usize, usize)> {
    Some((
        index.checked_div(bits_per_word)?,
        index.checked_rem(bits_per_word)?,
    ))
}

fn family_is_better(candidate: &HistoricalFamily, current: Option<&HistoricalFamily>) -> bool {
    let Some(current) = current else {
        return true;
    };
    candidate.member_indices.len() > current.member_indices.len()
        || (candidate.member_indices.len() == current.member_indices.len()
            && (candidate.candidate_commits.len() > current.candidate_commits.len()
                || (candidate.candidate_commits.len() == current.candidate_commits.len()
                    && candidate.candidate_commits.last() > current.candidate_commits.last())))
}

fn score_candidate(
    series: &[Series],
    entries: &[PreparedEntry],
    family: &HistoricalFamily,
    held_base_commit: Option<usize>,
) -> f64 {
    family.member_indices.iter().fold(0.0, |score, &index| {
        let Some(entry) = entries.get(index) else {
            return score;
        };
        let Some(prepared) = entry.prepared.as_ref() else {
            return score;
        };
        let Some(source) = series.get(prepared.source_index) else {
            return score;
        };
        let current = source
            .base_window
            .get(prepared.current_start..)
            .unwrap_or_default();
        let candidate = held_base_commit.and_then(|topo_index| {
            current
                .iter()
                .find(|level| level.topo_index == topo_index)
                .map(observation_of_level)
        });
        let candidate = candidate.unwrap_or(prepared.tip);
        let mut references: Vec<Observation> = current
            .iter()
            .filter(|level| Some(level.topo_index) != held_base_commit)
            .map(observation_of_level)
            .collect();
        if held_base_commit.is_some() {
            references.push(prepared.tip);
        }
        let evaluation = evaluate_excursion(
            source.kind,
            candidate,
            &references,
            &mut GateLog::disabled(),
        );
        if !evaluation.survives {
            return score;
        }
        let edge = evaluation
            .edge
            .expect("a surviving excursion has an observed-range edge");
        let excess = evaluation
            .excess
            .expect("a surviving excursion has an excess beyond the edge");
        let scale = edge
            .abs()
            .max(absolute_floor(source.kind) / noise_gates::BRANCH_PRACTICAL_RELATIVE);
        score + excess.abs() / scale
    })
}

/// The points that share the branch tip's commit and lane preference.
fn latest_context_points(points: &[SeriesPoint], context_index: usize) -> Option<&[SeriesPoint]> {
    let start = points.partition_point(|point| point.topo_index < context_index);
    let end = points.partition_point(|point| point.topo_index <= context_index);
    let points = points.get(start..end).filter(|points| !points.is_empty())?;
    let dirty_start = points.partition_point(|point| !point.dirty);
    let dirty_points = points
        .get(dirty_start..)
        .filter(|dirty_points| !dirty_points.is_empty());
    Some(dirty_points.unwrap_or(points))
}

fn observation_of_level(level: &BaseLevel) -> Observation {
    Observation {
        value: level.value,
        interval: level.interval,
    }
}

/// Collapses one commit's selected context points to a single observation.
fn observation_of_points(points: &[SeriesPoint]) -> Observation {
    let mut values = Vec::with_capacity(points.len());
    let mut lows = Vec::new();
    let mut highs = Vec::new();
    for point in points {
        values.push(point.value);
        if let Some(low) = point.interval_low {
            lows.push(low);
        }
        if let Some(high) = point.interval_high {
            highs.push(high);
        }
    }
    Observation {
        value: stats::median_in_place(&mut values)
            .expect("the selected context slice is non-empty"),
        interval: stats::median_in_place(&mut lows).zip(stats::median_in_place(&mut highs)),
    }
}

fn is_selector_position(index: usize) -> bool {
    index.is_multiple_of(2)
}

fn range_of(values: impl Iterator<Item = f64>) -> Option<(f64, f64)> {
    let mut values = values;
    let first = values.next()?;
    Some(values.fold((first, first), |(minimum, maximum), value| {
        (minimum.min(value), maximum.max(value))
    }))
}

fn relative_delta(delta: f64, baseline: f64) -> f64 {
    if baseline.abs() <= f64::EPSILON {
        delta.signum()
    } else {
        delta / baseline
    }
}

fn direction_of(delta: f64) -> Direction {
    if delta > 0.0 {
        Direction::Regression
    } else {
        Direction::Improvement
    }
}

fn oriented_superiority(superiority: f64, delta: f64) -> f64 {
    if delta > 0.0 {
        superiority
    } else {
        1.0 - superiority
    }
}

fn absolute_floor(kind: MetricKind) -> f64 {
    match kind {
        MetricKind::InstructionCount
        | MetricKind::ConditionalBranches
        | MetricKind::IndirectBranches => noise_gates::PRACTICAL_ABSOLUTE_COUNT,
        MetricKind::WallTime | MetricKind::ProcessorTime => noise_gates::PRACTICAL_ABSOLUTE_TIME,
        MetricKind::AllocatedBytes | MetricKind::AllocationCount => {
            noise_gates::PRACTICAL_ABSOLUTE_ALLOC
        }
    }
}

fn interval_half_width((low, high): (f64, f64)) -> f64 {
    (high - low) / 2.0
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    #![allow(
        clippy::float_cmp,
        reason = "metric values are exact integer-derived counts"
    )]
    #![allow(
        clippy::indexing_slicing,
        reason = "fixture cardinality is asserted before indexing and panic is appropriate in tests"
    )]
    #![allow(
        clippy::integer_division,
        reason = "even fixture lengths produce exact selector-lane candidate counts"
    )]

    use std::sync::Arc;

    use cbh_model::{BenchmarkId, Engine};
    #[cfg(feature = "private-test-util")]
    use futures::executor::block_on;
    use nonempty::nonempty;

    use super::*;
    #[cfg(feature = "private-test-util")]
    use crate::testing::synchronous_spawner;

    /// Builds one comparable partition for branch-analysis fixtures.
    fn set(machine_key: &str) -> DiscriminantSet {
        DiscriminantSet {
            engine: Engine::Callgrind,
            target_triple: "x86_64-unknown-linux-gnu".into(),
            machine_key: machine_key.into(),
        }
    }

    /// Builds one instruction-count series with a measured branch tip.
    fn series(name: &str, machine_key: &str, base: &[f64], tip: f64) -> Series {
        let tip_index = base.len();
        Series {
            set: set(machine_key),
            id: BenchmarkId::new(nonempty!["bench".to_owned(), name.to_owned()]),
            kind: MetricKind::InstructionCount,
            points: vec![SeriesPoint {
                topo_index: tip_index,
                dirty: false,
                object_ordinal: 0,
                commit: Some(Arc::from("branch-tip")),
                value: tip,
                interval_low: None,
                interval_high: None,
            }],
            base_window: base
                .iter()
                .enumerate()
                .map(|(topo_index, &value)| BaseLevel {
                    topo_index,
                    commit: Some(Arc::from(format!("c{topo_index}"))),
                    value,
                    interval: None,
                })
                .collect(),
            base_history_count: base.len(),
            active_start: 0,
            blessing: None,
        }
    }

    /// Builds a branch-tip series with repeated measurements on the context commit.
    fn series_with_tip_points(
        name: &str,
        machine_key: &str,
        base: &[f64],
        tip_points: &[(bool, f64)],
    ) -> Series {
        let tip_index = base.len();
        Series {
            set: set(machine_key),
            id: BenchmarkId::new(nonempty!["bench".to_owned(), name.to_owned()]),
            kind: MetricKind::InstructionCount,
            points: tip_points
                .iter()
                .enumerate()
                .map(|(object_ordinal, &(dirty, value))| SeriesPoint {
                    topo_index: tip_index,
                    dirty,
                    object_ordinal: u32::try_from(object_ordinal)
                        .expect("test fixture has fewer than u32::MAX points"),
                    commit: Some(Arc::from("branch-tip")),
                    value,
                    interval_low: None,
                    interval_high: None,
                })
                .collect(),
            base_window: base
                .iter()
                .enumerate()
                .map(|(topo_index, &value)| BaseLevel {
                    topo_index,
                    commit: Some(Arc::from(format!("c{topo_index}"))),
                    value,
                    interval: None,
                })
                .collect(),
            base_history_count: base.len(),
            active_start: 0,
            blessing: None,
        }
    }

    /// Branch context for fixtures whose tip follows `base_commits`.
    fn context(base_commits: usize) -> AnalysisContext {
        AnalysisContext {
            mode: crate::detect::AnalysisMode::Branch,
            merge_base_index: base_commits.checked_sub(1),
            base_ref_index: base_commits.checked_sub(1),
            tip_index: base_commits,
        }
    }

    #[test]
    fn supported_history_lengths_report_strict_range_excursions() {
        // Miri covers both complete-window and selector/reference evaluation. The native
        // matrix additionally checks scaling up to the supported comparison-window cap.
        let lengths: &[usize] = if cfg!(miri) {
            &[
                noise_gates::MIN_SERIES_POINTS,
                noise_gates::MIN_BRANCH_REGIME_SELECTION_COMMITS,
            ]
        } else {
            &[10, 20, 40, 64, 128]
        };
        for &base_commits in lengths {
            let one = series("length", "m1", &vec![100.0; base_commits], 130.0);
            let detection = find_changes(std::slice::from_ref(&one), &context(base_commits));
            assert_eq!(
                detection.findings.len(),
                1,
                "{base_commits} base commits should support a range excursion"
            );
            let branch = detection.findings[0]
                .branch
                .as_ref()
                .expect("branch findings carry range evidence");
            assert_eq!(branch.reference_count, base_commits);
            assert_eq!(branch.reference_min, 100.0);
            assert_eq!(branch.reference_max, 100.0);
            let comparison = detection
                .branch_comparisons
                .first()
                .expect("every supported length has enough reference-lane candidates");
            assert_eq!(comparison.evaluated_base_commits, base_commits / 2);
            assert_eq!(comparison.at_least_as_much, 0);
        }
    }

    #[test]
    #[cfg(feature = "private-test-util")]
    fn spawned_branch_evaluation_matches_the_serial_result() {
        // Worker recombination needs judged series, not regime-selection searches.
        const BASE_COMMITS: usize = noise_gates::MIN_SERIES_POINTS;

        let batch: Arc<[Series]> = Arc::from([
            series("regression", "m1", &[100.0; BASE_COMMITS], 130.0),
            series("quiet", "m1", &[100.0; BASE_COMMITS], 100.0),
            series("improvement", "m1", &[200.0; BASE_COMMITS], 100.0),
        ]);
        let context = context(BASE_COMMITS);
        let serial = find_changes(&batch, &context);
        let spawned = block_on(find_changes_spawned(
            Arc::clone(&batch),
            context,
            &synchronous_spawner(),
        ));

        assert_eq!(spawned.findings.len(), serial.findings.len());
        assert_eq!(spawned.census, serial.census);
        assert_eq!(
            spawned
                .findings
                .iter()
                .map(|finding| (&finding.id, finding.direction))
                .collect::<Vec<_>>(),
            serial
                .findings
                .iter()
                .map(|finding| (&finding.id, finding.direction))
                .collect::<Vec<_>>()
        );
        assert_eq!(
            spawned.branch_trace.series.len(),
            serial.branch_trace.series.len()
        );
    }

    #[test]
    fn single_series_evaluation_returns_the_branch_finding() {
        let one = series("logged", "m1", &[100.0; 20], 130.0);
        let (finding, _) = evaluate_with_log(&one, &context(20));
        let finding = finding.expect("the clear branch excursion must be returned");

        assert_eq!(finding.method, FindingMethod::BranchExcursion);
        assert_eq!(finding.direction, Direction::Regression);
    }

    #[test]
    fn minimum_base_evidence_passes_the_recorded_gate() {
        let base = vec![100.0; noise_gates::MIN_SERIES_POINTS];
        let one = series("minimum-base", "m1", &base, 130.0);
        let (_, log) = evaluate_with_log(&one, &context(base.len()));
        let outcome = log
            .entries()
            .iter()
            .find(|outcome| outcome.gate == Gate::MinBaseCommits)
            .expect("branch evaluation records the base-evidence gate");

        assert!(outcome.passed);
    }

    #[test]
    fn repeated_context_commit_points_are_collapsed_before_evaluation() {
        let one = series_with_tip_points(
            "collapsed",
            "m1",
            &[100.0; 20],
            &[(true, 100.0), (true, 100.0), (true, 130.0)],
        );
        let (finding, _) = evaluate_with_log(&one, &context(20));

        assert!(
            finding.is_none(),
            "the tip commit should be judged from its per-commit median rather than the last point",
        );
    }

    #[test]
    fn repeated_clean_context_points_are_collapsed_before_evaluation() {
        let one = series_with_tip_points(
            "collapsed-clean",
            "m1",
            &[100.0; 20],
            &[(false, 100.0), (false, 100.0), (false, 130.0)],
        );
        let (finding, _) = evaluate_with_log(&one, &context(20));

        assert!(
            finding.is_none(),
            "clean context points must remain eligible when there is no dirty snapshot",
        );
    }

    #[test]
    fn context_intervals_are_collapsed_with_the_context_values() {
        let mut one = series_with_tip_points(
            "collapsed-intervals",
            "m1",
            &[100.0; 20],
            &[(false, 100.0), (false, 120.0)],
        );
        one.points[0].interval_low = Some(90.0);
        one.points[0].interval_high = Some(110.0);
        one.points[1].interval_low = Some(110.0);
        one.points[1].interval_high = Some(130.0);

        let observation = observation_of_points(&one.points);

        assert_eq!(observation.value, 110.0);
        assert_eq!(observation.interval, Some((100.0, 120.0)));
    }

    #[test]
    fn dirty_context_points_replace_clean_points_from_the_same_commit() {
        let one = series_with_tip_points(
            "dirty-preferred",
            "m1",
            &[100.0; 20],
            &[(false, 160.0), (true, 100.0)],
        );
        let (finding, _) = evaluate_with_log(&one, &context(20));

        assert!(
            finding.is_none(),
            "dirty snapshot points represent the analyzed workspace when both lanes exist",
        );
    }

    #[test]
    fn missing_context_commit_is_unjudged() {
        let mut one = series("missing-context", "m1", &[100.0; 20], 130.0);
        one.points.clear();

        let detection = find_changes(std::slice::from_ref(&one), &context(20));

        assert!(detection.findings.is_empty());
        assert_eq!(
            detection
                .census
                .reasons()
                .find(|&(reason, _)| reason == UnjudgedReason::NotMeasuredOnBranch),
            Some((UnjudgedReason::NotMeasuredOnBranch, 1))
        );
    }

    #[test]
    fn selector_and_reference_lanes_are_distinct_observation_sequences() {
        let one = series("lanes", "m1", &[100.0; 20], 130.0);
        let detection = find_changes(std::slice::from_ref(&one), &context(20));
        let trace = &detection.branch_trace.series[0];

        assert_eq!(
            trace.selector_commits,
            (0..20).step_by(2).collect::<Vec<_>>()
        );
        assert_eq!(
            trace.reference_commits,
            (1..20).step_by(2).collect::<Vec<_>>()
        );
    }

    #[test]
    fn latest_supported_regime_excludes_the_older_level() {
        // Each alternating lane keeps a minimum-sized regime on both sides of the step.
        let mut base = vec![200.0; 10];
        base.extend(std::iter::repeat_n(100.0, 10));
        let one = series("regimes", "m1", &base, 200.0);
        let detection = find_changes(std::slice::from_ref(&one), &context(base.len()));

        let finding = detection
            .findings
            .first()
            .expect("returning to the older regime is an excursion from the current one");
        let branch = finding
            .branch
            .as_ref()
            .expect("branch findings carry range evidence");
        assert_eq!(branch.reference_min, 100.0);
        assert_eq!(branch.reference_max, 100.0);
        assert_eq!(branch.current_regime_start.as_deref(), Some("c10"));
        assert!(branch.matches_previous_regime);
        assert_eq!(
            detection.branch_trace.series[0].current_regime_start,
            Some(10)
        );
    }

    #[test]
    fn an_excursion_beyond_both_current_and_previous_regimes_is_not_a_return() {
        // Minimum-sized selector regimes suffice to identify the preceding level.
        let mut base = vec![200.0; 10];
        base.extend(std::iter::repeat_n(100.0, 10));
        let one = series("new-excursion", "m1", &base, 230.0);
        let detection = find_changes(std::slice::from_ref(&one), &context(base.len()));
        let branch = detection.findings[0]
            .branch
            .as_ref()
            .expect("a branch excursion carries its range context");

        assert!(!branch.matches_previous_regime);
    }

    #[test]
    fn an_ambiguous_reference_observation_is_not_previous_regime_evidence() {
        // The reference immediately before the minimum-sized new regime is ambiguous.
        let mut base = vec![200.0; 10];
        base[9] = 150.0;
        base.extend(std::iter::repeat_n(100.0, 10));
        let one = series("ambiguous-reference", "m1", &base, 150.0);
        let detection = find_changes(std::slice::from_ref(&one), &context(base.len()));
        let branch = detection.findings[0]
            .branch
            .as_ref()
            .expect("a branch excursion carries its range context");

        assert_eq!(
            detection.branch_trace.series[0].previous_range,
            Some((200.0, 200.0))
        );
        assert!(!branch.matches_previous_regime);
    }

    #[test]
    fn regime_search_budget_is_shared_across_every_search() {
        // Forty selector observations divide into eight minimum regimes. The initial search
        // can perform seven tests; successive suffixes can perform six, five, and so on.
        assert_eq!(
            regime_search_alpha(40),
            noise_gates::MAX_BRANCH_REGIME_CHANCE_LEVEL / 28.0
        );
        // The shortest searchable lane permits exactly one statistical split search. A
        // shorter suffix returns before testing a boundary and consumes none of the budget.
        assert_eq!(
            regime_search_alpha(noise_gates::MIN_SERIES_POINTS),
            noise_gates::MAX_BRANCH_REGIME_CHANCE_LEVEL
        );
    }

    #[test]
    fn a_boundary_must_clear_every_support_gate() {
        let absolute_only = [10_000.0; 10]
            .into_iter()
            .chain([10_200.0; 10])
            .collect::<Vec<_>>();
        assert!(!supported_boundary(
            MetricKind::InstructionCount,
            &absolute_only,
            10,
            1.0
        ));

        let relative_only = [10.0; 10].into_iter().chain([11.0; 10]).collect::<Vec<_>>();
        assert!(!supported_boundary(
            MetricKind::InstructionCount,
            &relative_only,
            10,
            1.0
        ));

        let supported = [10_000.0; 10]
            .into_iter()
            .chain([11_000.0; 10])
            .collect::<Vec<_>>();
        assert!(!supported_boundary(
            MetricKind::InstructionCount,
            &supported,
            supported.len().saturating_add(1),
            1.0,
        ));
        for split in [0, supported.len()] {
            assert!(!supported_boundary(
                MetricKind::InstructionCount,
                &supported,
                split,
                1.0,
            ));
        }
        assert!(!supported_boundary(
            MetricKind::InstructionCount,
            &supported,
            10,
            0.5
        ));
        assert!(supported_boundary(
            MetricKind::InstructionCount,
            &supported,
            10,
            1.0
        ));
    }

    #[test]
    fn a_statistical_split_below_the_practical_floor_does_not_move_the_regime() {
        // A minimum-sized pair of selector regimes already establishes a statistical split.
        let mut base = vec![10_000.0; 10];
        base.extend(std::iter::repeat_n(10_200.0, 10));
        let one = series("impractical-split", "m1", &base, 11_000.0);
        let selection = select_regime(&one);

        assert_eq!(selection.current_start, 0);
        assert_eq!(selection.boundary_commit, None);
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "multiple supported boundaries require repeated selection-adjusted calibration"
    )]
    fn the_newest_of_several_supported_boundaries_becomes_the_comparison_regime() {
        // Three supported steps. The search finds the middle one first and reaches the
        // outer two by recursing into both of its sides, so the boundaries must still come
        // out in chronological order for the newest to win.
        let mut base = vec![10_000.0; 20];
        base.extend(std::iter::repeat_n(11_000.0, 20));
        base.extend(std::iter::repeat_n(30_000.0, 20));
        base.extend(std::iter::repeat_n(33_000.0, 20));
        let one = series("several-supported-splits", "m1", &base, 40_000.0);
        let selection = select_regime(&one);

        assert_eq!(selection.current_start, 60);
        assert_eq!(selection.boundary_commit.as_deref(), Some("c60"));
        assert_eq!(selection.previous_range, Some((30_000.0, 30_000.0)));
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "a weak central split hiding an earlier regime requires a large permutation orbit"
    )]
    fn an_unsupported_split_does_not_hide_an_earlier_supported_one() {
        // The 100 -> 100.5 step is statistically real but far below the practical floor,
        // and it sits closer to the middle than the large 90 -> 100 step, so the strongest
        // split lands on it. Searching only later suffixes would discard the supported
        // boundary behind it and leave the 90 observations inside the comparison range.
        let mut base = vec![90.0; 10];
        base.extend(std::iter::repeat_n(100.0, 30));
        base.extend(std::iter::repeat_n(100.5, 30));
        let one = series("earlier-supported-split", "m1", &base, 90.0);
        let selection = select_regime(&one);

        assert_eq!(selection.current_start, 10);
        assert_eq!(selection.boundary_commit.as_deref(), Some("c10"));
        assert!(!selection.unresolved);

        let detection = find_changes(std::slice::from_ref(&one), &context(base.len()));
        let finding = detection
            .findings
            .first()
            .expect("the tip sits below every observation in the current regime");
        assert_eq!(finding.direction, Direction::Improvement);
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "a weak central split hiding a later regime requires repeated permutation searches"
    )]
    fn an_unsupported_split_does_not_prevent_searching_for_a_later_supported_one() {
        let mut base = vec![100.0; 20];
        base.extend(std::iter::repeat_n(104.0, 20));
        base.extend(std::iter::repeat_n(130.0, 10));
        let one = series("later-supported-split", "m1", &base, 130.0);
        let selection = select_regime(&one);

        assert_eq!(selection.current_start, 40);
        assert_eq!(selection.boundary_commit.as_deref(), Some("c40"));
        assert!(!selection.unresolved);
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "the permutation-heavy 128-observation segmentation takes hours under Miri"
    )]
    fn a_reverted_base_regression_does_not_stay_inside_the_comparison_range() {
        // The base stepped up and then back down, so the strongest split of the whole lane
        // is the step back down - and measured against everything before it, both of its
        // sides have the same median. Segmentation can only certify the step up; the
        // forward extension has to certify the step back down against the raised stretch
        // it ends, or the raised observations stay inside the comparison range.
        let mut base = vec![100.0; 50];
        base.extend(std::iter::repeat_n(200.0, 10));
        base.extend(std::iter::repeat_n(100.0, 68));
        let one = series("reverted-base-regression", "m1", &base, 150.0);
        let selection = select_regime(&one);

        assert_eq!(selection.current_start, 60);
        assert_eq!(selection.boundary_commit.as_deref(), Some("c60"));
        assert_eq!(selection.previous_range, Some((200.0, 200.0)));
        assert!(!selection.unresolved);

        let detection = find_changes(std::slice::from_ref(&one), &context(base.len()));
        let finding = detection
            .findings
            .first()
            .expect("the tip sits above every observation in the current regime");
        assert_eq!(finding.direction, Direction::Regression);
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "the permutation-heavy 116-observation segmentation takes hours under Miri"
    )]
    fn an_unsupported_suffix_split_does_not_hide_a_reverted_base_regression() {
        // The long-running 100 -> 102 move is the strongest split after the 200 plateau
        // begins, but it is too small to define a regime. Segmenting around it must still
        // find the fully separated 200 -> 100 exit or the plateau remains in the range.
        let mut base = vec![100.0; 20];
        base.extend(std::iter::repeat_n(200.0, 10));
        base.extend(std::iter::repeat_n(100.0, 24));
        base.extend(std::iter::repeat_n(102.0, 62));
        let one = series("unsupported-suffix-split", "m1", &base, 150.0);
        let selection = select_regime(&one);

        assert_eq!(selection.current_start, 30);
        assert_eq!(selection.boundary_commit.as_deref(), Some("c30"));
        assert_eq!(selection.previous_range, Some((200.0, 200.0)));
        assert!(!selection.unresolved);

        let detection = find_changes(std::slice::from_ref(&one), &context(base.len()));
        let finding = detection
            .findings
            .first()
            .expect("the tip sits above every observation in the current regime");
        assert_eq!(finding.direction, Direction::Regression);
    }

    #[test]
    fn a_recent_step_too_short_to_establish_is_unjudged() {
        // The shortest searchable base retains two trailing selector observations.
        let mut base = vec![100.0; 16];
        base.extend(std::iter::repeat_n(200.0, 4));
        let one = series("unresolved", "m1", &base, 220.0);
        let detection = find_changes(std::slice::from_ref(&one), &context(base.len()));

        assert!(detection.findings.is_empty());
        assert_eq!(detection.census.judged(), 0);
        assert_eq!(
            detection.branch_trace.series[0].unresolved,
            Some(UnjudgedReason::CurrentBaseRegimeUnresolved)
        );
    }

    #[test]
    fn an_incomplete_preceding_window_cannot_imply_an_emerging_regime() {
        let one = series(
            "short-tail",
            "m1",
            &[100.0, 100.0, 200.0, 200.0, 200.0, 200.0],
            220.0,
        );
        let selector: Vec<&BaseLevel> = one.base_window.iter().collect();

        assert!(!recent_step_is_unresolved(
            MetricKind::InstructionCount,
            &selector
        ));
    }

    #[test]
    fn a_clean_short_tail_is_recognized_as_an_emerging_regime() {
        let one = series(
            "emerging",
            "m1",
            &[100.0, 100.0, 100.0, 100.0, 100.0, 200.0, 200.0],
            220.0,
        );
        let selector: Vec<&BaseLevel> = one.base_window.iter().collect();

        assert!(recent_step_is_unresolved(
            MetricKind::InstructionCount,
            &selector
        ));
    }

    #[test]
    fn a_clean_short_decline_is_recognized_as_an_emerging_regime() {
        let one = series(
            "emerging-decline",
            "m1",
            &[200.0, 200.0, 200.0, 200.0, 200.0, 100.0, 100.0],
            80.0,
        );
        let selector: Vec<&BaseLevel> = one.base_window.iter().collect();

        assert!(recent_step_is_unresolved(
            MetricKind::InstructionCount,
            &selector
        ));
    }

    #[test]
    fn an_overlapping_short_tail_is_not_an_emerging_regime() {
        let one = series(
            "overlapping-tail",
            "m1",
            &[100.0, 100.0, 100.0, 130.0, 130.0, 130.0, 130.0],
            140.0,
        );
        let selector: Vec<&BaseLevel> = one.base_window.iter().collect();

        assert!(!recent_step_is_unresolved(
            MetricKind::InstructionCount,
            &selector
        ));
    }

    #[test]
    fn a_smooth_base_drift_is_not_mistaken_for_an_unresolved_step() {
        // A shortest searchable base still spans a practically large, smooth drift.
        let base: Vec<f64> = (0_u32..20).map(|index| 100.0 + f64::from(index)).collect();
        let one = series("drift", "m1", &base, 140.0);
        let detection = find_changes(std::slice::from_ref(&one), &context(base.len()));

        assert_eq!(detection.census.judged(), 1);
        assert_eq!(detection.findings.len(), 1);
        assert_eq!(detection.branch_trace.series[0].unresolved, None);
    }

    #[test]
    fn stability_requires_a_significant_and_practically_large_drift() {
        let flat = series("flat-stability", "m1", &[100.0; 20], 130.0);
        assert!(regime_is_stable(&flat.base_window));

        let small_drift = series(
            "small-drift",
            "m1",
            &(0_u32..20)
                .map(|index| 100.0 + f64::from(index) / 10.0)
                .collect::<Vec<_>>(),
            130.0,
        );
        assert!(regime_is_stable(&small_drift.base_window));

        let large_drift = series(
            "large-drift",
            "m1",
            &(100_u32..120).map(f64::from).collect::<Vec<_>>(),
            150.0,
        );
        assert!(!regime_is_stable(&large_drift.base_window));
    }

    #[test]
    fn a_recent_blessing_with_too_little_retained_history_is_unjudged() {
        let base = vec![100.0; 9];
        let mut one = series("blessed", "m1", &base, 130.0);
        one.base_history_count = 40;
        one.blessing = Some(crate::detect::Blessing {
            commit: "blessed-commit".to_owned(),
            commit_time: None,
        });
        let detection = find_changes(std::slice::from_ref(&one), &context(base.len()));

        assert_eq!(detection.census.judged(), 0);
        assert_eq!(
            detection.branch_trace.series[0].unresolved,
            Some(UnjudgedReason::TooFewBaseCommitsSinceBlessing)
        );
        assert_eq!(detection.branch_trace.series[0].available_base_commits, 40);
        assert_eq!(detection.branch_trace.series[0].retained_base_commits, 9);
    }

    #[test]
    fn a_blessing_does_not_relabel_insufficient_total_history() {
        let base = vec![100.0; 9];
        let mut one = series("too-short", "m1", &base, 130.0);
        one.base_history_count = 9;
        one.blessing = Some(crate::detect::Blessing {
            commit: "blessed-commit".to_owned(),
            commit_time: None,
        });
        let detection = find_changes(std::slice::from_ref(&one), &context(base.len()));

        assert_eq!(detection.census.judged(), 0);
        assert_eq!(
            detection.branch_trace.series[0].unresolved,
            Some(UnjudgedReason::TooFewBaseCommits)
        );
    }

    #[test]
    fn an_observed_bimodal_value_is_not_an_excursion() {
        let base: Vec<f64> = (0_usize..20)
            .map(|index| {
                if index.is_multiple_of(2) {
                    100.0
                } else {
                    150.0
                }
            })
            .collect();
        let one = series("bimodal", "m1", &base, 150.0);
        let detection = find_changes(std::slice::from_ref(&one), &context(base.len()));

        assert!(detection.findings.is_empty());
        assert_eq!(
            detection.branch_trace.series[0].branch_relation,
            BranchRangeRelation::Inside
        );
        let comparison = &detection.branch_comparisons[0];
        assert_eq!(
            comparison.at_least_as_much,
            comparison.evaluated_base_commits
        );
    }

    #[test]
    fn equality_with_either_observed_range_edge_is_quiet() {
        let base = [100.0, 200.0]
            .into_iter()
            .cycle()
            .take(20)
            .collect::<Vec<_>>();
        for tip in [100.0, 200.0] {
            let one = series("edge", "m1", &base, tip);
            let detection = find_changes(std::slice::from_ref(&one), &context(base.len()));

            assert!(detection.findings.is_empty(), "tip {tip}");
            assert_eq!(
                detection.branch_trace.series[0].branch_relation,
                BranchRangeRelation::Inside
            );
        }
    }

    #[test]
    fn an_empty_reference_set_cannot_form_an_excursion() {
        let evaluation = evaluate_excursion(
            MetricKind::InstructionCount,
            Observation {
                value: 130.0,
                interval: None,
            },
            &[],
            &mut GateLog::disabled(),
        );

        assert_eq!(evaluation.relation, BranchRangeRelation::Unavailable);
        assert_eq!(evaluation.reference_count, 0);
        assert_eq!(evaluation.reference_min, None);
        assert_eq!(evaluation.reference_max, None);
        assert_eq!(evaluation.edge, None);
        assert_eq!(evaluation.excess, None);
        assert_eq!(evaluation.relative_excess, None);
        assert_eq!(evaluation.relative_floor_passed, None);
        assert_eq!(evaluation.absolute_floor_passed, None);
        assert_eq!(evaluation.interval_disjoint_passed, None);
        assert_eq!(evaluation.noise_band_passed, None);
        assert!(!evaluation.survives);
    }

    #[test]
    fn branch_gates_stop_at_the_first_decline() {
        let references = [Observation {
            value: 100.0,
            interval: None,
        }];
        let mut relative_log = GateLog::recording();
        let relative = evaluate_excursion(
            MetricKind::InstructionCount,
            Observation {
                // This is outside the range but below the relative floor.
                value: 104.0,
                interval: None,
            },
            &references,
            &mut relative_log,
        );
        assert_eq!(
            relative_log
                .entries()
                .iter()
                .map(|outcome| outcome.gate)
                .collect::<Vec<_>>(),
            vec![Gate::NonZeroDelta, Gate::RelativeFloor]
        );
        assert_eq!(relative.absolute_floor_passed, None);
        assert_eq!(relative.interval_disjoint_passed, None);
        assert_eq!(relative.noise_band_passed, None);

        let mut absolute_log = GateLog::recording();
        let absolute = evaluate_excursion(
            MetricKind::InstructionCount,
            Observation {
                // The small baseline clears the relative floor while the move remains
                // below the count metric's absolute floor.
                value: 14.0,
                interval: None,
            },
            &[Observation {
                value: 10.0,
                interval: None,
            }],
            &mut absolute_log,
        );
        assert_eq!(
            absolute_log
                .entries()
                .iter()
                .map(|outcome| outcome.gate)
                .collect::<Vec<_>>(),
            vec![Gate::NonZeroDelta, Gate::RelativeFloor, Gate::AbsoluteFloor]
        );
        assert_eq!(absolute.absolute_floor_passed, Some(false));
        assert_eq!(absolute.interval_disjoint_passed, None);
        assert_eq!(absolute.noise_band_passed, None);

        let mut interval_log = GateLog::recording();
        let interval = evaluate_excursion(
            MetricKind::InstructionCount,
            Observation {
                value: 130.0,
                interval: Some((100.0, 140.0)),
            },
            &[Observation {
                value: 100.0,
                interval: Some((90.0, 110.0)),
            }],
            &mut interval_log,
        );
        assert_eq!(
            interval_log
                .entries()
                .iter()
                .map(|outcome| outcome.gate)
                .collect::<Vec<_>>(),
            vec![
                Gate::NonZeroDelta,
                Gate::RelativeFloor,
                Gate::AbsoluteFloor,
                Gate::IntervalDisjoint
            ]
        );
        assert_eq!(interval.interval_disjoint_passed, Some(false));
        assert_eq!(interval.noise_band_passed, None);
    }

    #[test]
    fn interval_vetoes_require_strict_separation_on_both_sides() {
        let references = [
            Observation {
                value: 100.0,
                interval: Some((90.0, 110.0)),
            },
            Observation {
                value: 100.0,
                interval: Some((91.0, 109.0)),
            },
        ];
        let mut log = GateLog::disabled();
        assert_eq!(
            intervals_allow(
                BranchRangeRelation::Above,
                Observation {
                    value: 130.0,
                    interval: Some((111.0, 140.0)),
                },
                &references,
                &mut log.stage(GateStage::Branch),
            ),
            Some(true)
        );
        for low in [110.0, 109.0] {
            assert_eq!(
                intervals_allow(
                    BranchRangeRelation::Above,
                    Observation {
                        value: 130.0,
                        interval: Some((low, 140.0)),
                    },
                    &references,
                    &mut log.stage(GateStage::Branch),
                ),
                Some(false)
            );
        }
        assert_eq!(
            intervals_allow(
                BranchRangeRelation::Below,
                Observation {
                    value: 70.0,
                    interval: Some((60.0, 89.0)),
                },
                &references,
                &mut log.stage(GateStage::Branch),
            ),
            Some(true)
        );
        for high in [90.0, 91.0] {
            assert_eq!(
                intervals_allow(
                    BranchRangeRelation::Below,
                    Observation {
                        value: 70.0,
                        interval: Some((60.0, high)),
                    },
                    &references,
                    &mut log.stage(GateStage::Branch),
                ),
                Some(false)
            );
        }
        assert_eq!(
            intervals_allow(
                BranchRangeRelation::Above,
                Observation {
                    value: 130.0,
                    interval: None,
                },
                &references,
                &mut log.stage(GateStage::Branch),
            ),
            None
        );
        let without_reference_intervals = [Observation {
            value: 100.0,
            interval: None,
        }];
        assert_eq!(
            intervals_allow(
                BranchRangeRelation::Above,
                Observation {
                    value: 130.0,
                    interval: Some((111.0, 140.0)),
                },
                &without_reference_intervals,
                &mut log.stage(GateStage::Branch),
            ),
            None
        );
        for relation in [
            BranchRangeRelation::Inside,
            BranchRangeRelation::Unavailable,
        ] {
            assert_eq!(
                intervals_allow(
                    relation,
                    Observation {
                        value: 100.0,
                        interval: Some((90.0, 110.0)),
                    },
                    &references,
                    &mut log.stage(GateStage::Branch),
                ),
                None
            );
        }
    }

    #[test]
    fn interval_vetoes_ignore_missing_reference_intervals() {
        let references = [
            Observation {
                value: 100.0,
                interval: None,
            },
            Observation {
                value: 100.0,
                interval: Some((91.0, 109.0)),
            },
        ];
        let mut log = GateLog::disabled();
        assert_eq!(
            intervals_allow(
                BranchRangeRelation::Above,
                Observation {
                    value: 130.0,
                    interval: Some((110.0, 140.0)),
                },
                &references,
                &mut log.stage(GateStage::Branch),
            ),
            Some(true)
        );
        assert_eq!(
            intervals_allow(
                BranchRangeRelation::Below,
                Observation {
                    value: 70.0,
                    interval: Some((60.0, 90.0)),
                },
                &references,
                &mut log.stage(GateStage::Branch),
            ),
            Some(true)
        );
    }

    #[test]
    fn interval_noise_band_is_strict_and_uses_interval_half_width() {
        let references = [Observation {
            value: 100.0,
            interval: Some((95.0, 105.0)),
        }];
        let candidate = Observation {
            value: 130.0,
            interval: Some((125.0, 135.0)),
        };
        let band = noise_gates::BRANCH_NOISE_MULTIPLE * 5.0;
        let mut log = GateLog::disabled();

        assert_eq!(
            noise_band_allows(
                band + 1.0,
                candidate,
                &references,
                &mut log.stage(GateStage::Branch),
            ),
            Some(true)
        );
        for excess in [band, band - 1.0] {
            assert_eq!(
                noise_band_allows(
                    excess,
                    candidate,
                    &references,
                    &mut log.stage(GateStage::Branch),
                ),
                Some(false)
            );
        }
        assert_eq!(
            noise_band_allows(
                band + 1.0,
                Observation {
                    value: 130.0,
                    interval: None,
                },
                &[],
                &mut log.stage(GateStage::Branch),
            ),
            None
        );
    }

    #[test]
    fn historical_turns_include_the_real_branch_tip_as_reference() {
        let base = vec![100.0; 20];
        let one = series("symmetric", "m1", &base, 130.0);
        let detection = find_changes(std::slice::from_ref(&one), &context(base.len()));
        let trace = &detection.branch_trace.comparisons[0];

        assert_eq!(trace.branch_score, 0.3);
        assert!(
            trace.base_scores.iter().all(|&score| score == 0.0),
            "a held-out base value is inside the range formed by the other base values and tip"
        );
        assert_eq!(trace.at_least_as_much, 0);
    }

    #[test]
    fn zero_baselines_use_the_absolute_floor_for_score_scale() {
        let base = vec![0.0; 20];
        let one = series("zero", "m1", &base, 10.0);
        let detection = find_changes(std::slice::from_ref(&one), &context(base.len()));

        assert_eq!(detection.findings.len(), 1);
        assert_eq!(detection.branch_trace.comparisons[0].branch_score, 0.1);
    }

    #[test]
    fn comparisons_are_partitioned_by_discriminant_set() {
        let base = vec![100.0; 20];
        let series = [
            series("linux", "m1", &base, 130.0),
            series("mac", "m2", &base, 140.0),
        ];
        let detection = find_changes(&series, &context(base.len()));

        assert_eq!(detection.findings.len(), 2);
        assert_eq!(detection.branch_comparisons.len(), 2);
        assert!(
            detection
                .branch_comparisons
                .iter()
                .all(|comparison| comparison.series == 1)
        );
    }

    #[test]
    fn sparse_topology_still_balances_selector_and_reference_observations() {
        let base = vec![100.0; 20];
        let mut sparse = series("sparse", "m1", &base, 130.0);
        for (position, level) in sparse.base_window.iter_mut().enumerate() {
            level.topo_index = position.saturating_mul(2).saturating_add(1);
        }
        sparse.points[0].topo_index = 100;

        let detection = find_changes(std::slice::from_ref(&sparse), &context(100));
        let trace = &detection.branch_trace.series[0];

        assert_eq!(trace.selector_commits.len(), 10);
        assert_eq!(trace.reference_commits.len(), 10);
        assert_eq!(detection.census.judged(), 1);
        assert_eq!(detection.branch_comparisons[0].evaluated_base_commits, 10);
    }

    #[test]
    fn nonconsecutive_shared_candidates_form_one_larger_family() {
        let mut left = series("left", "m1", &[100.0; 14], 130.0);
        let mut right = series("right", "m1", &[100.0; 14], 130.0);
        replace_reference_commits(&mut left, &[11, 31, 51, 91, 111, 131, 171], 200);
        replace_reference_commits(&mut right, &[11, 51, 71, 91, 131, 151, 171], 200);

        let detection = find_changes(&[left, right], &context(200));
        let comparison = &detection.branch_comparisons[0];

        assert_eq!(comparison.series, 2);
        assert_eq!(comparison.evaluated_base_commits, 5);
    }

    #[test]
    fn family_members_must_contain_every_seed_commit() {
        let mut first = series("first", "m1", &[100.0; 10], 130.0);
        let mut second = series("second", "m1", &[100.0; 10], 130.0);
        replace_reference_commits(&mut first, &[1, 3, 5, 7, 9], 20);
        replace_reference_commits(&mut second, &[1, 3, 5, 7, 11], 20);
        let batch = [first, second];
        let entries: Vec<PreparedEntry> = batch
            .iter()
            .enumerate()
            .map(|(index, one)| prepare_series(index, one, &context(20), &mut GateLog::disabled()))
            .collect();
        let family = select_family(&batch, &entries, &[0, 1])
            .expect("each series independently provides a complete candidate family");

        assert_eq!(family.member_indices, vec![1]);
        assert_eq!(family.candidate_commits, vec![1, 3, 5, 7, 11]);
    }

    #[test]
    fn no_eligible_series_yields_no_historical_family() {
        assert!(select_family(&[], &[], &[]).is_none());
    }

    #[test]
    fn three_way_shared_candidates_form_a_larger_family() {
        let mut first = series("first", "m1", &[100.0; 20], 100.0);
        let mut second = series("second", "m1", &[100.0; 20], 100.0);
        let mut third = series("third", "m1", &[100.0; 20], 100.0);
        replace_reference_commits(&mut first, &[10, 15, 20, 30, 35, 40, 50], 200);
        replace_reference_commits(&mut second, &[10, 15, 20, 30, 40, 45, 50], 200);
        replace_reference_commits(&mut third, &[10, 20, 30, 35, 40, 45, 50], 200);

        let detection = find_changes(&[first, second, third], &context(200));
        let comparison = &detection.branch_comparisons[0];

        assert_eq!(comparison.series, 3);
        assert_eq!(comparison.evaluated_base_commits, 5);
    }

    #[test]
    fn three_way_shared_candidates_can_exceed_the_minimum_family() {
        let mut first = series("first-wide", "m1", &[100.0; 20], 100.0);
        let mut second = series("second-wide", "m1", &[100.0; 20], 100.0);
        let mut third = series("third-wide", "m1", &[100.0; 20], 100.0);
        replace_reference_commits(&mut first, &[10, 15, 20, 30, 35, 40, 50, 60], 200);
        replace_reference_commits(&mut second, &[10, 15, 20, 30, 40, 45, 50, 60], 200);
        replace_reference_commits(&mut third, &[10, 20, 30, 35, 40, 45, 50, 60], 200);

        let detection = find_changes(&[first, second, third], &context(200));
        let comparison = &detection.branch_comparisons[0];

        assert_eq!(comparison.series, 3);
        assert_eq!(comparison.evaluated_base_commits, 6);
    }

    #[test]
    fn family_ranking_uses_members_then_commits_then_recency() {
        let current = HistoricalFamily {
            member_indices: vec![0, 1],
            candidate_commits: vec![1, 2, 3, 4, 5],
        };
        assert!(family_is_better(
            &HistoricalFamily {
                member_indices: vec![0, 1, 2],
                candidate_commits: vec![1, 2, 3, 4],
            },
            Some(&current)
        ));
        assert!(!family_is_better(
            &HistoricalFamily {
                member_indices: vec![0],
                candidate_commits: vec![1, 2, 3, 4, 5, 6],
            },
            Some(&current)
        ));
        assert!(family_is_better(
            &HistoricalFamily {
                member_indices: vec![0, 1],
                candidate_commits: vec![1, 2, 3, 4, 5, 6],
            },
            Some(&current)
        ));
        assert!(!family_is_better(
            &HistoricalFamily {
                member_indices: vec![0, 1],
                candidate_commits: vec![1, 2, 3, 4],
            },
            Some(&current)
        ));
        assert!(family_is_better(
            &HistoricalFamily {
                member_indices: vec![0, 1],
                candidate_commits: vec![2, 3, 4, 5, 6],
            },
            Some(&current)
        ));
        assert!(!family_is_better(
            &HistoricalFamily {
                member_indices: vec![0, 1],
                candidate_commits: vec![0, 1, 2, 3, 4],
            },
            Some(&current)
        ));
        assert!(!family_is_better(&current, Some(&current)));
    }

    #[test]
    fn multi_way_seed_queue_deduplicates_and_obeys_its_budget() {
        let mut seen = HashSet::new();
        let mut queue = VecDeque::new();

        assert!(enqueue_family_seed(vec![1, 2, 3], 1, &mut seen, &mut queue));
        assert!(!enqueue_family_seed(
            vec![1, 2, 3],
            1,
            &mut seen,
            &mut queue
        ));
        assert!(!enqueue_family_seed(
            vec![4, 5, 6],
            1,
            &mut seen,
            &mut queue
        ));
        assert_eq!(queue.into_iter().collect::<Vec<_>>(), vec![vec![1, 2, 3]]);
    }

    #[test]
    fn held_out_scoring_uses_the_named_base_commit() {
        let mut base = vec![10_000.0; 20];
        base[19] = 13_000.0;
        let one = series("held", "m1", &base, 10_000.0);
        let entry = prepare_series(0, &one, &context(20), &mut GateLog::disabled());
        let family = HistoricalFamily {
            member_indices: vec![0],
            candidate_commits: vec![19],
        };

        assert_eq!(
            score_candidate(
                std::slice::from_ref(&one),
                std::slice::from_ref(&entry),
                &family,
                Some(19)
            ),
            0.3
        );
    }

    #[test]
    fn membership_bits_and_numeric_helpers_cover_their_boundaries() {
        assert_eq!(membership_position(65, 64), Some((1, 1)));
        assert_eq!(membership_position(0, 0), None);
        assert!(membership_contains(&[0, 2], 65, 64));
        assert!(!membership_contains(&[0, 2], 64, 64));
        assert!(!membership_contains(&[0, 2], 129, 64));

        assert_eq!(direction_of(1.0), Direction::Regression);
        assert_eq!(direction_of(0.0), Direction::Improvement);
        assert_eq!(direction_of(-1.0), Direction::Improvement);
        assert_eq!(oriented_superiority(0.8, 1.0), 0.8);
        assert!((oriented_superiority(0.8, 0.0) - 0.2).abs() < f64::EPSILON);
        assert!((oriented_superiority(0.8, -1.0) - 0.2).abs() < f64::EPSILON);
        assert_eq!(interval_half_width((90.0, 110.0)), 10.0);
    }

    /// Rebuilds a fixture so its reference-lane observations land on the requested topology.
    fn replace_reference_commits(series: &mut Series, references: &[usize], tip_index: usize) {
        series.base_window = references
            .iter()
            .flat_map(|&reference| {
                [
                    BaseLevel {
                        topo_index: reference.saturating_sub(1),
                        commit: Some(Arc::from(format!("c{}", reference.saturating_sub(1)))),
                        value: 100.0,
                        interval: None,
                    },
                    BaseLevel {
                        topo_index: reference,
                        commit: Some(Arc::from(format!("c{reference}"))),
                        value: 100.0,
                        interval: None,
                    },
                ]
            })
            .collect();
        series.base_history_count = series.base_window.len();
        series.points[0].topo_index = tip_index;
    }

    #[test]
    fn unstable_series_are_reported_outside_the_historical_family() {
        let stable = series("stable", "m1", &[100.0; 20], 130.0);
        let drifting_base: Vec<f64> = (100..120).map(f64::from).collect();
        let drifting = series("drifting", "m1", &drifting_base, 150.0);
        let detection = find_changes(&[stable, drifting], &context(20));

        assert_eq!(detection.findings.len(), 2);
        let comparison = &detection.branch_comparisons[0];
        assert_eq!(comparison.series, 1);
        assert_eq!(
            detection
                .findings
                .iter()
                .filter(|finding| {
                    finding
                        .branch
                        .as_ref()
                        .is_some_and(|branch| branch.included_in_historical_comparison)
                })
                .count(),
            1
        );
    }
}
