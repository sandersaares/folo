//! The finding algorithms for history and branch analysis.
//!
//! No benchmark engine is treated as noise-free. Callgrind instruction and event
//! counts jitter a few percent run to run, and `alloc_tracker`'s per-iteration
//! figures carry warmup and buffer-resize allocations amortized over a
//! Criterion-chosen iteration count; Criterion wall time and `all_the_time`
//! processor time jitter more visibly still.
//!
//! The jitter is easy to underestimate because a Callgrind run *repeated on one
//! unchanged machine* often reports the same count every time — its simulated
//! counter barely notices the machine conditions that move a timing. What is not
//! fixed is everything feeding it across the commits we compare: a different OS
//! or CPU-microcode patch level, a different compiler patch release, the
//! compiler's own run-to-run nondeterministic code-generation choices (inlining,
//! ordering, layout) even at the same version, and Criterion scheduling a
//! different iteration count when background load differs (which shifts how
//! warmup and buffer-resize costs are amortized). Any of these perturbs the
//! measured count without the code under test changing, so no metric can be
//! assumed reproducible commit to commit. Every series is therefore judged
//! noise-aware:
//!
//! * A Pettitt change-point *locates* a candidate split (its analytic p-value is
//!   too conservative on short series to gate significance); both regimes must
//!   hold at least `min_regime` points (persistence), and a series shorter than
//!   `min_series_points` is not judged at all.
//! * A Mann–Whitney rank test must then confirm the two regimes differ, the move
//!   must clear a practical-magnitude floor, and it must exceed the series' own
//!   between-commit residual scatter (the primary, series-intrinsic noise gate).
//!   The practical-magnitude floor is relative (a minimum percentage) *and*
//!   absolute (a minimum span of the metric's own units), so a move too small to
//!   act on — a few instructions of build layout, a fraction of a nanosecond —
//!   cannot read as a large-percentage regression on a small baseline.
//! * Where the engine reports a per-point confidence interval (Criterion,
//!   `all_the_time`, `alloc_tracker`) the two regimes' intervals must also be
//!   disjoint; if they overlap this veto *withholds* the finding, treating the
//!   move as measurement noise. The veto direction is one-way: it can only
//!   suppress a candidate the other gates would have reported — it can never
//!   promote a move into a finding.
//! * Surviving history candidates are screened to regressions, then pass a
//!   Benjamini–Hochberg false-discovery filter, taken over every series judged
//!   rather than only those that raised a candidate, so a batch of series does not
//!   manufacture spurious findings and the rate the filter controls is the rate among
//!   the findings actually reported.
//! * Branch mode reports values outside the observed current-base regime after
//!   practical and interval/noise vetoes. A symmetric report-wide comparison says how
//!   often eligible base commits produced at least as much aggregate out-of-range movement.
//!
//! A separate slow-[`Drift`](FindingMethod::Drift) finding is raised from a
//! Mann–Kendall trend test plus a Theil–Sen slope, gated by the same practical
//! floor and residual-scatter check, and is suppressed when a single step on the
//! same series already explains at least as much movement.
//!
//! Every gate above tests evidence, never cause. A level shift the surrounding
//! infrastructure produced — a toolchain upgrade, a move to a different runner — is
//! a real shift in the measured series, so it is reported as a finding to be blessed
//! rather than filtered out on the grounds of its origin. That is a statement about
//! what the detectors decline to look at, not a promise of detection: such a shift
//! still has to clear the same history-length, magnitude, scatter, significance and
//! false-discovery gates as any other history move, or the same range, practical, and
//! interval gates as any other branch move.
//!
//! Polarity: every metric is lower-is-better (instruction counts, branch counts,
//! allocations, wall and processor time), so a rise is a
//! [`Direction::Regression`] and a fall is a [`Direction::Improvement`].

use std::collections::BTreeMap;
use std::num::NonZero;
use std::ops::Range;
#[cfg(any(test, feature = "private-test-util"))]
use std::slice;
use std::sync::Arc;

use anyspawn::Spawner;
use cbh_model::{BenchmarkId, DiscriminantSet, MetricKind};
use cbh_stats as stats;
use serde::Serialize;

use crate::detect::gate_log::{Gate, GateLog, GateStage, StageLog};
use crate::detect::parallel::{balanced_chunk_sizes, worker_count};
use crate::detect::{Series, SeriesPoint, branch, noise_gates};

/// Chance level carrying no evidence against the null hypothesis.
const NO_EVIDENCE: f64 = 1.0;

/// Which analysis a [`find_changes_spawned`] pass performs.
///
/// The mode is auto-detected by the caller from git topology and the admitted data
/// set (a base ref whose context commit is its own merge-base with no dirty run
/// admitted on that commit is [`History`](AnalysisMode::History); commits — or an
/// admitted dirty run — on top of the base make it [`Branch`](AnalysisMode::Branch)).
/// The working tree affects the choice only indirectly, through the exception that
/// admits a dirty run at the context commit while the tree is dirty.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AnalysisMode {
    /// Long-range trend and change-point analysis over a base branch's history.
    History,
    /// Latest context-run comparison against the base ref's observed current regime,
    /// ignoring the intermediate commits the branch passed through.
    Branch,
}

impl AnalysisMode {
    /// The lowercase wire name of the mode.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::History => "history",
            Self::Branch => "branch",
        }
    }
}

/// The context a [`find_changes_spawned`] pass runs in.
///
/// Carries which analysis to perform and the topology anchors the mode needs.
#[derive(Clone, Copy, Debug)]
pub struct AnalysisContext {
    /// The analysis to perform.
    pub mode: AnalysisMode,
    /// First-parent topological index of the merge-base on the context line, when it
    /// lies there. Branch detection does not derive its base window from this split;
    /// it compares against [`Series::base_window`], which is loaded from the base ref.
    pub merge_base_index: Option<usize>,
    /// First-parent topological index of the base ref's resolved commit.
    ///
    /// Branch mode uses this base-ref coordinate to measure comparison-base lag and
    /// to place the context commit just after the base ref in comparison charts.
    pub base_ref_index: Option<usize>,
    /// First-parent topological index of the analyzed context commit (the resolved
    /// `--context`/HEAD). History-mode chart building uses it as the trailing-fill
    /// target so a series that stops short of the context renders the data-less commits
    /// after its last observation as a gap. Consulted only in [`AnalysisMode::History`].
    pub tip_index: usize,
}

impl AnalysisContext {
    /// Whether a finding of the given `direction` is reported in this mode.
    ///
    /// The two modes ask differently shaped questions, so they keep different
    /// directions (DESIGN, "Multiple-comparison discipline" and "Analysis modes").
    /// History mode is a drift watch over the base
    /// branch, where improvement over time is the expected background and only a
    /// worsening warrants attention, so it is one-directional. Branch mode judges one
    /// change against its base, where any movement is what the reader came for.
    fn keeps(&self, direction: Direction) -> bool {
        match self.mode {
            AnalysisMode::History => direction == Direction::Regression,
            AnalysisMode::Branch => true,
        }
    }

    /// Whether this analysis reports improvements at all. `false` in history mode's
    /// regressions-only drift watch, where an always-zero improvement tally is noise
    /// the report omits.
    #[must_use]
    pub fn reports_improvements(&self) -> bool {
        self.keeps(Direction::Improvement)
    }
}

/// Why a series was left unjudged.
///
/// A detection pass reaches a verdict only on series carrying enough evidence for
/// their mode's detector; every other series is unjudged for one of these reasons.
/// The set is exhaustive over the ways the analysis declines to test a series, so a
/// silent report can say how much of the suite its silence covers.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum UnjudgedReason {
    /// The benchmark carries no measurement at the analyzed context commit, so it is no
    /// longer part of the suite and was dropped before detection.
    Ghost,
    /// History mode: the series carries fewer than
    /// [`MIN_SERIES_POINTS`](noise_gates::MIN_SERIES_POINTS) points.
    TooFewPoints,
    /// History mode: a blessing re-baselined the series and fewer than
    /// [`MIN_SERIES_POINTS`](noise_gates::MIN_SERIES_POINTS) points have been
    /// measured since, so the evidence the blessing left standing is too thin to
    /// judge.
    TooFewPointsSinceBlessing,
    /// Branch mode: the context commit measured nothing for this series, so there is no
    /// context state to compare against the base.
    NotMeasuredOnBranch,
    /// Branch mode: the recent base window holds fewer than
    /// [`MIN_SERIES_POINTS`](noise_gates::MIN_SERIES_POINTS) base-ref commit
    /// levels, so there is not enough base evidence to judge the branch. A later
    /// regime split may compare against a shorter trailing regime, but only after
    /// this full-window evidence floor is met.
    TooFewBaseCommits,
    /// Branch mode: a blessing intentionally discarded older base-ref measurements,
    /// leaving too few commits to judge the branch.
    TooFewBaseCommitsSinceBlessing,
    /// Branch mode: recent base measurements support a practically important change
    /// but cannot yet establish where the current regime begins.
    CurrentBaseRegimeUnresolved,
}

impl UnjudgedReason {
    /// Every reason, in declaration order, so a test can cover the set exhaustively.
    ///
    /// Reachable from the documentation generator as well as from this crate's own tests,
    /// because the appendix lists every reason and a list nothing checks would fall
    /// silently out of step the first time the set changed.
    #[cfg(any(test, feature = "private-test-util"))]
    pub const ALL: [Self; 7] = [
        Self::Ghost,
        Self::TooFewPoints,
        Self::TooFewPointsSinceBlessing,
        Self::NotMeasuredOnBranch,
        Self::TooFewBaseCommits,
        Self::TooFewBaseCommitsSinceBlessing,
        Self::CurrentBaseRegimeUnresolved,
    ];

    /// The lowercase wire name of the reason.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ghost => "ghost",
            Self::TooFewPoints => "too_few_points",
            Self::TooFewPointsSinceBlessing => "too_few_points_since_blessing",
            Self::NotMeasuredOnBranch => "not_measured_on_branch",
            Self::TooFewBaseCommits => "too_few_base_commits",
            Self::TooFewBaseCommitsSinceBlessing => "too_few_base_commits_since_blessing",
            Self::CurrentBaseRegimeUnresolved => "current_base_regime_unresolved",
        }
    }

    /// A prose phrase describing the shortfall, worded to follow a count of series:
    /// `"9 series with too few points in the analyzed window"`.
    #[must_use]
    pub fn describe(self) -> &'static str {
        match self {
            Self::Ghost => "not measured at the analyzed context commit",
            Self::TooFewPoints => "with too few points in the analyzed window",
            Self::TooFewPointsSinceBlessing => "with too few points since being blessed",
            Self::NotMeasuredOnBranch => "not measured on the branch",
            Self::TooFewBaseCommits => "with too few base-ref commits to compare against",
            Self::TooFewBaseCommitsSinceBlessing => {
                "with too few base-ref commits remaining since being blessed"
            }
            Self::CurrentBaseRegimeUnresolved => "whose current base regime is unresolved",
        }
    }
}

/// Whether a series carries enough evidence for its mode's detector to reach a
/// verdict, and if not, what it lacks.
///
/// History mode's false-discovery family is exactly its
/// [`Judged`](Testability::Judged) series. An unjudged history series is not a tested
/// hypothesis, while every judged history series must be counted whether or not it raised
/// a candidate. Branch mode uses the same verdict for its coverage census but does not form
/// a false-discovery family.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Testability {
    /// The detector reached a verdict on the series.
    Judged,
    /// The series was not tested at all.
    Unjudged(UnjudgedReason),
}

impl Testability {
    /// Whether the detector reached a verdict.
    #[must_use]
    fn is_judged(self) -> bool {
        self == Self::Judged
    }
}

/// How many series an analysis judged, and why it left the rest unjudged.
///
/// This is what makes a report's silence readable: "nothing moved" says something
/// about the code only for the series that were judged, so the census travels with
/// the findings and every rendering discloses it. Each series is accounted for
/// exactly once, whether it was dropped before detection or declined by it, so
/// [`total`](Self::total) is the whole suite the analysis started from.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SeriesCensus {
    judged: usize,
    unjudged: BTreeMap<UnjudgedReason, usize>,
}

impl SeriesCensus {
    /// Accounts for one series with the verdict [`testability`] reached on it.
    pub fn record(&mut self, testability: Testability) {
        match testability {
            Testability::Judged => self.judged = self.judged.saturating_add(1),
            Testability::Unjudged(reason) => self.record_unjudged(reason, 1),
        }
    }

    /// Accounts for `series` series left unjudged for the same `reason`.
    ///
    /// Bulk form for the stages that drop series before detection sees them, which
    /// know only how many they dropped.
    pub fn record_unjudged(&mut self, reason: UnjudgedReason, series: usize) {
        if series == 0 {
            return;
        }
        let counted = self.unjudged.entry(reason).or_default();
        *counted = counted.saturating_add(series);
    }

    /// How many series the detectors reached a verdict on — the false-discovery
    /// family size.
    #[must_use]
    pub fn judged(&self) -> usize {
        self.judged
    }

    /// How many series went unjudged, for any reason.
    #[must_use]
    pub fn unjudged(&self) -> usize {
        self.unjudged
            .values()
            .fold(0_usize, |total, &series| total.saturating_add(series))
    }

    /// How many series the analysis accounted for in total.
    #[must_use]
    pub fn total(&self) -> usize {
        self.judged.saturating_add(self.unjudged())
    }

    /// The unjudged series broken down by reason, in [`UnjudgedReason`] order.
    ///
    /// That order runs with the pipeline: what the ghost filter dropped before
    /// detection, then the history-mode shortfalls, then the branch-mode ones.
    /// Reasons that account for no series are omitted.
    pub fn reasons(&self) -> impl Iterator<Item = (UnjudgedReason, usize)> + '_ {
        self.unjudged
            .iter()
            .map(|(&reason, &series)| (reason, series))
    }
}

/// What a detection pass found, and what it judged to find it.
///
/// The census travels with the findings because the two are only meaningful
/// together: an empty finding list means "nothing moved" only across the series the
/// census reports as judged.
#[derive(Clone, Debug, Default)]
pub struct Detection {
    /// The surviving findings, ranked most-notable first.
    pub findings: Vec<Finding>,
    /// What the pass judged, and why it left the rest unjudged.
    pub census: SeriesCensus,
    /// Per-discriminant-set historical comparisons produced by branch mode.
    ///
    /// Empty in history mode and for branch sets without enough rectangular,
    /// current-regime history.
    pub branch_comparisons: Vec<BranchComparison>,
    /// Branch-mode decision trace used by diagnostics and deterministic tests.
    ///
    /// Empty in history mode.
    pub branch_trace: BranchEvaluationTrace,
}

/// A report-wide historical comparison for one comparable discriminant set.
#[derive(Clone, Debug)]
pub struct BranchComparison {
    /// The comparable partition this comparison covers.
    pub set: DiscriminantSet,
    /// Base commits evaluated as if each were the newest result.
    pub evaluated_base_commits: usize,
    /// Base-commit reports with at least as much normalized out-of-range movement as
    /// the real branch report.
    pub at_least_as_much: usize,
    /// Series present in every candidate commit and included in the comparison.
    pub series: usize,
}

/// Production branch-analysis trace shared by diagnostics and deterministic tests.
#[derive(Clone, Debug, Default)]
pub struct BranchEvaluationTrace {
    /// Per-series preparation and excursion decisions.
    pub series: Vec<BranchSeriesTrace>,
    /// Per-set candidate scores, including the real branch score.
    pub comparisons: Vec<BranchComparisonTrace>,
}

/// One branch series' evidence selection and excursion decision.
#[derive(Clone, Debug)]
pub struct BranchSeriesTrace {
    /// The comparable partition the series belongs to.
    pub set: DiscriminantSet,
    /// The benchmark identity.
    pub id: BenchmarkId,
    /// The metric category.
    pub kind: MetricKind,
    /// Base levels available before the configured cap and blessings.
    pub available_base_commits: usize,
    /// Base levels retained after the cap and blessings.
    pub retained_base_commits: usize,
    /// Selector-lane first-parent coordinates.
    pub selector_commits: Vec<usize>,
    /// Reference-lane first-parent coordinates.
    pub reference_commits: Vec<usize>,
    /// Conservative first-parent coordinate where the current regime begins.
    pub current_regime_start: Option<usize>,
    /// Why the series was withheld after evidence selection, if applicable.
    pub unresolved: Option<UnjudgedReason>,
    /// Minimum and maximum of the selected current base regime.
    pub current_range: Option<(f64, f64)>,
    /// Minimum and maximum of the immediately preceding supported regime.
    pub previous_range: Option<(f64, f64)>,
    /// Number of observations in the real branch's reference range.
    pub reference_count: usize,
    /// Where the branch value lies relative to the reference range.
    pub branch_relation: BranchRangeRelation,
    /// Whether the practical relative-magnitude gate passed, when evaluated.
    pub relative_floor_passed: Option<bool>,
    /// Whether the practical absolute-magnitude gate passed, when evaluated.
    pub absolute_floor_passed: Option<bool>,
    /// Whether engine confidence intervals allowed the excursion, when available.
    pub interval_disjoint_passed: Option<bool>,
    /// Whether the excursion cleared the engine measurement-noise band, when available.
    pub noise_band_passed: Option<bool>,
    /// Whether the series belongs to its set's rectangular historical family.
    pub included_in_historical_comparison: bool,
}

/// Candidate scores behind one report-wide historical comparison.
#[derive(Clone, Debug)]
pub struct BranchComparisonTrace {
    /// The comparable partition this comparison covers.
    pub set: DiscriminantSet,
    /// Base-candidate scores in stable first-parent order.
    pub base_scores: Vec<f64>,
    /// Score of the real branch report.
    pub branch_score: f64,
    /// Base scores tied with or exceeding the real branch.
    pub at_least_as_much: usize,
}

/// Where a branch measurement lies relative to its selected base range.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BranchRangeRelation {
    /// No branch or reference measurement was available.
    Unavailable,
    /// The branch is below every reference observation.
    Below,
    /// The branch is within or on the reference range.
    Inside,
    /// The branch is above every reference observation.
    Above,
}

/// Which detector produced a finding.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FindingMethod {
    /// A sustained level shift located by the Pettitt change-point test.
    ChangePoint,
    /// A slow monotonic trend located by the Mann–Kendall / Theil–Sen pair.
    Drift,
    /// A branch measurement outside the observed current-base range.
    BranchExcursion,
}

/// The direction of a flagged change relative to the baseline.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    /// The latest value is worse than the baseline.
    Regression,
    /// The latest value is better than the baseline.
    Improvement,
}

/// One point of a finding's underlying series, retained for charting.
///
/// Carries, for charting and provenance, the commit it was measured against, the
/// value, and whether it came from a dirty (uncommitted-tree) snapshot.
#[derive(Clone, Debug)]
pub struct SeriesValue {
    /// Commit the point was measured against, if known.
    pub commit: Option<String>,
    /// The measured value.
    pub value: f64,
    /// Whether the point is a dirty (uncommitted-tree) snapshot.
    pub dirty: bool,
    /// First-parent topological index of the commit the point was measured against.
    /// Charting places each point in its own per-commit column and materializes gaps
    /// for the data-less commits between points; it is not part of the JSON contract.
    pub topo_index: usize,
}

/// One flagged change: what moved, by how much, and where.
#[derive(Clone, Debug)]
pub struct Finding {
    /// The comparable discriminant set the series belongs to.
    pub set: DiscriminantSet,
    /// The benchmark identity.
    pub id: BenchmarkId,
    /// The category of the metric that moved (governs unit and polarity).
    pub kind: MetricKind,
    /// Which detector produced this finding.
    pub method: FindingMethod,
    /// Whether the move is a regression or an improvement.
    pub direction: Direction,
    /// Reference value the latest measurement was compared against.
    ///
    /// In history mode this is the before-regime representative. In branch mode it is
    /// the nearest observed current-base range edge; there is no before regime.
    pub baseline: f64,
    /// Latest measured value.
    ///
    /// In history mode this is the after-regime representative. In branch mode it is
    /// the context-commit observation.
    pub latest: f64,
    /// Signed difference from [`baseline`](Self::baseline).
    ///
    /// In history mode this is `latest - baseline`. In branch mode it is the signed
    /// excess beyond the nearest range edge, matching [`BranchExcursion::excess`].
    pub delta: f64,
    /// Signed difference relative to [`baseline`](Self::baseline).
    ///
    /// In history mode this is `delta / baseline`. In branch mode it is the signed
    /// excess relative to the nearest range edge, matching
    /// [`BranchExcursion::relative_excess`].
    pub relative_delta: f64,
    /// Commit associated with the finding, if known. For a change point this is the
    /// detector's estimate of where the new level begins — somewhere near the true
    /// first commit of that level, which cannot always be identified. For a branch
    /// comparison it is the context commit; for a drift it is the newest commit the
    /// trend reached (paired with [`window_start_commit`](Self::window_start_commit)
    /// to name the accumulation range, since a drift belongs to the whole window
    /// rather than one commit).
    pub commit: Option<String>,
    /// The oldest commit of a drift's accumulation window, so the report can name the
    /// range the trend accrued over. `Some` only for a drift finding; `None` for a
    /// change point or branch comparison.
    pub window_start_commit: Option<String>,
    /// Abbreviated commit of the blessing that re-baselined this series, if any.
    pub blessed_at: Option<String>,
    /// Effective (committer) time of the blessed commit, RFC 3339, if blessed.
    pub blessed_commit_time: Option<String>,
    /// The full underlying series, oldest-first. Retained internally so the text and
    /// Markdown reports can draw a chart; it is not part of the machine-readable JSON
    /// contract.
    pub series: Vec<SeriesValue>,
    /// Base-ref first-parent index of the newest base datum in the comparison sample.
    ///
    /// Set only in branch mode (`None` in history mode, where there is no single
    /// comparison base). When branch mode discards stale levels before an accepted
    /// base-side step, this remains the newest point of the trailing regime, because
    /// lag classification answers how current the compared base state is. Internal
    /// cross-crate analysis metadata that lets the analysis measure how far the
    /// comparison base sits behind the base ref; it is not part of the JSON finding
    /// contract.
    pub comparison_base_index: Option<usize>,
    /// Trailing-fill target for the chart: the first-parent index the charted series
    /// extends to when its last observation stops short of it. `Some(tip_index)` in
    /// history mode, so the data-less commits between the last observation and the
    /// analyzed context commit render as a gap; `None` in branch mode, where the
    /// context commit is the always-present last column. Chart-only — like
    /// [`Finding::series`] it is not part of the JSON finding contract.
    pub chart_base_ref: Option<usize>,
    /// Branch-specific range and excess details.
    ///
    /// Present only for [`FindingMethod::BranchExcursion`].
    pub branch: Option<BranchExcursion>,
}

/// The observed range and excess behind a branch excursion finding.
#[derive(Clone, Debug)]
pub struct BranchExcursion {
    /// Base observations in the selected reference range.
    pub reference_count: usize,
    /// Smallest selected reference observation.
    pub reference_min: f64,
    /// Largest selected reference observation.
    pub reference_max: f64,
    /// Signed distance beyond the nearest range edge.
    pub excess: f64,
    /// Signed excess relative to the nearest range edge.
    pub relative_excess: f64,
    /// Commit opening the selected current regime, when a boundary was established.
    pub current_regime_start: Option<String>,
    /// Whether the branch value falls inside the immediately preceding supported regime.
    pub matches_previous_regime: bool,
    /// Whether the finding belongs to its set's report-wide historical comparison.
    pub included_in_historical_comparison: bool,
}

impl Finding {
    /// Whether this finding is a regression (as opposed to an improvement).
    #[must_use]
    pub fn is_regression(&self) -> bool {
        self.direction == Direction::Regression
    }
}

/// A finding before false-discovery filtering, carrying the p-value the
/// Benjamini–Hochberg pool needs and the fitted model parameters used to arbitrate
/// between the two detectors.
struct Candidate {
    /// The finding that will be emitted if it survives filtering.
    finding: Finding,
    /// Index of the source series in the analysed slice. The finding's charting
    /// points ([`Finding::series`]) are materialised from it only once the
    /// candidate survives filtering, so a dropped candidate never pays for them.
    source_index: usize,
    /// The p-value contributed to the false-discovery pool.
    bh_p: f64,
    /// The Pettitt split index, for a change-point candidate.
    split: Option<usize>,
    /// The Theil–Sen `(slope, intercept)`, for a drift candidate.
    line: Option<(f64, f64)>,
}

/// Casts a small count to `f64`. Series lengths are far below 2^53, so the
/// conversion is exact.
#[expect(
    clippy::cast_precision_loss,
    reason = "series lengths are far below 2^53, so the cast is exact"
)]
pub(crate) fn count_to_f64(count: usize) -> f64 {
    count as f64
}

/// One source point as a compact [`SeriesValue`] chart point, carrying its
/// `topo_index` so the renderer can place it in its own per-commit column.
fn series_value_of(point: &SeriesPoint) -> SeriesValue {
    SeriesValue {
        commit: owned_commit(point),
        value: point.value,
        dirty: point.dirty,
        topo_index: point.topo_index,
    }
}

/// Builds a surviving finding's compact chart series and its trailing-fill target,
/// dispatching on the analysis mode.
///
/// The series stays compact — one [`SeriesValue`] per real observation, never a
/// materialized gap — and every point carries its `topo_index`, so the renderer can
/// place each in its own per-commit column and draw the data-less commits between (and
/// after) observations as gaps. This is presentation-only: detection reads
/// `Series.points`, never this series.
///
/// History mode maps every source point 1:1 and returns `Some(context.tip_index)` as
/// the trailing-fill target, so a series that stops short of the analyzed context
/// commit renders the intervening commits as the "no newer data" gap. Branch mode
/// collapses the series (see [`branch_chart_series`]).
fn build_chart_series(
    source: &Series,
    finding: &Finding,
    context: &AnalysisContext,
) -> (Vec<SeriesValue>, Option<usize>) {
    match context.mode {
        AnalysisMode::History => (
            source.points.iter().map(series_value_of).collect(),
            Some(context.tip_index),
        ),
        AnalysisMode::Branch => branch_chart_series(source, finding, context),
    }
}

/// Materializes one surviving finding's presentation-only chart points.
pub(crate) fn materialize_chart(source: &Series, finding: &mut Finding, context: &AnalysisContext) {
    let (series, base_ref) = build_chart_series(source, finding, context);
    finding.series = series;
    finding.chart_base_ref = base_ref;
}

/// The branch-collapsed chart series and its (absent) trailing-fill target.
///
/// Branch mode judges the context commit alone, so the chart keeps the base ref's
/// comparison-window levels at their real base-ref `topo_index`, including any stale
/// pre-step base levels that detection deliberately ignored, drops every interior
/// context commit, and represents the context by a single point just after the base
/// ref. The chart is context rather than the comparison sample; when the detector
/// narrows to a trailing base regime, the older base points remain visible so the
/// base-side shift is understandable. The trailing-fill target is `None` — the
/// context is the always-present last column.
///
/// A real branch finding always carries a known base ref and comparison base; the
/// fallback to a plain whole-series chart is defensive (never a panic) for a finding
/// that somehow lacks either.
fn branch_chart_series(
    source: &Series,
    finding: &Finding,
    context: &AnalysisContext,
) -> (Vec<SeriesValue>, Option<usize>) {
    debug_assert!(
        context.base_ref_index.is_some() && finding.comparison_base_index.is_some(),
        "a branch finding always carries a known base ref and comparison base",
    );
    let Some(base_ref_index) = context.base_ref_index else {
        return (source.points.iter().map(series_value_of).collect(), None);
    };
    let mut series: Vec<SeriesValue> = source
        .base_window
        .iter()
        .map(|level| SeriesValue {
            commit: level.commit.as_deref().map(str::to_owned),
            value: level.value,
            dirty: false,
            topo_index: level.topo_index,
        })
        .collect();
    let latest = source
        .points
        .last()
        .filter(|point| point.topo_index == context.tip_index);
    series.push(SeriesValue {
        commit: finding.commit.clone(),
        value: finding.latest,
        dirty: latest.is_some_and(|point| point.dirty),
        topo_index: base_ref_index.saturating_add(1),
    });
    (series, None)
}

/// The commit of a point as an owned `String`, for the JSON output.
///
/// Points intern their commit as a shared `Arc<str>`; the public finding fields are
/// plain owned strings, so a surviving finding pays one allocation here rather than
/// every point carrying its own copy.
fn owned_commit(point: &SeriesPoint) -> Option<String> {
    point.commit.as_deref().map(str::to_owned)
}

/// The direction of a change, given the signed delta from the baseline.
///
/// Every metric is lower-is-better, so a positive delta is a regression and a
/// negative one an improvement. The caller only reaches this with a non-zero delta,
/// so the exact zero case never arises in practice; it is defined as an improvement
/// so the classification is total.
fn direction_of(delta: f64) -> Direction {
    if delta > 0.0 {
        Direction::Regression
    } else {
        Direction::Improvement
    }
}

/// The relative size of `delta` against `baseline`.
///
/// A move away from a (near-)zero baseline is proportionally unbounded; its sign
/// is returned as a full-magnitude move so it ranks as major.
pub(crate) fn relative_delta_of(delta: f64, baseline: f64) -> f64 {
    if baseline.abs() <= f64::EPSILON {
        delta.signum()
    } else {
        delta / baseline
    }
}

/// The absolute-magnitude floor that applies to `kind`, in the metric's own units.
///
/// Each metric has a magnitude below which a move is not worth reporting, whatever
/// percentage it works out to: a few instructions is build layout, a fraction of a
/// nanosecond is not worth acting on, and a fraction of an allocation cannot happen.
/// The floors differ because those units do.
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

/// Whether a move clears the absolute-magnitude floor for `series`.
///
/// `delta` must span at least [`absolute_floor`] of the metric's own units,
/// otherwise a move too small to mean anything would clear the relative floor and
/// read as a regression on a small baseline. The gate composes with the relative
/// floor by conjunction and can only *suppress*, never promote, a move.
fn clears_absolute_floor(series: &Series, delta: f64, log: &mut StageLog<'_>) -> bool {
    let floor = absolute_floor(series.kind);
    log.numeric(
        Gate::AbsoluteFloor,
        delta.abs(),
        floor,
        delta.abs() >= floor,
    )
}

/// The representative confidence interval of a regime: the median of its points'
/// lower and upper bounds, available only when the engine reports dispersion.
fn regime_interval(points: &[&SeriesPoint]) -> Option<(f64, f64)> {
    let mut lows: Vec<f64> = points
        .iter()
        .filter_map(|point| point.interval_low)
        .collect();
    let mut highs: Vec<f64> = points
        .iter()
        .filter_map(|point| point.interval_high)
        .collect();
    // `median_in_place` yields `None` for an empty side, so a regime missing either
    // bound short-circuits here without a separate emptiness guard.
    Some((
        stats::median_in_place(&mut lows)?,
        stats::median_in_place(&mut highs)?,
    ))
}

/// Whether two intervals are disjoint (the after regime sits wholly above or
/// wholly below the before regime).
fn intervals_disjoint(before: (f64, f64), after: (f64, f64)) -> bool {
    after.1 < before.0 || after.0 > before.1
}

/// Whether two regimes' confidence intervals stand apart, where the engine reports
/// dispersion.
///
/// A one-way veto: a regime pair whose intervals overlap is treated as one measurement
/// spread across two windows and the candidate is withheld. An engine that reports no
/// dispersion offers no evidence either way, so the veto abstains and the move is
/// trusted — which is why the gate is recorded only when both intervals exist.
fn regime_intervals_are_disjoint(
    before: &[&SeriesPoint],
    after: &[&SeriesPoint],
    log: &mut StageLog<'_>,
) -> bool {
    let (Some(before_ci), Some(after_ci)) = (regime_interval(before), regime_interval(after))
    else {
        return true;
    };
    log.boolean(
        Gate::IntervalDisjoint,
        intervals_disjoint(before_ci, after_ci),
    )
}

/// Whether a move exceeds `multiple` times the per-measurement noise floor, where the
/// engine reports dispersion.
///
/// The noise floor is the median confidence-interval half-width across `points` (see
/// [`median_half_width`]): the per-point dispersion a single measurement carries. A move
/// inside that band is indistinguishable from that dispersion however the rest of the
/// evidence reads, so this is a one-way veto like [`regime_intervals_are_disjoint`], and
/// it abstains — recording nothing — on an engine that reports no dispersion.
fn exceeds_noise_band(
    delta: f64,
    points: &[SeriesPoint],
    multiple: f64,
    log: &mut StageLog<'_>,
) -> bool {
    let Some(half_width) = median_half_width(points) else {
        return true;
    };
    let band = multiple * half_width;
    log.numeric(
        Gate::IntervalNoiseBand,
        delta.abs(),
        band,
        delta.abs() > band,
    )
}

/// The median confidence-interval half-width across `points`, when the engine
/// reports dispersion. Used as the per-measurement noise floor for noisy drift.
fn median_half_width(points: &[SeriesPoint]) -> Option<f64> {
    let mut halves: Vec<f64> = points.iter().filter_map(point_half_width).collect();
    median_half_widths(&mut halves)
}

fn median_half_widths(halves: &mut [f64]) -> Option<f64> {
    if halves.is_empty() {
        return None;
    }
    stats::median_in_place(halves)
}

fn point_half_width(point: &SeriesPoint) -> Option<f64> {
    match (point.interval_low, point.interval_high) {
        (Some(low), Some(high)) => Some(interval_half_width((low, high))),
        _ => None,
    }
}

fn interval_half_width((low, high): (f64, f64)) -> f64 {
    (high - low) / 2.0
}

/// The median absolute residual of the two-regime (step) model: each point's
/// distance from its own regime's median, split at `tau`.
fn step_model_residual(values: &[f64], tau: usize) -> Option<f64> {
    let before = values.get(..tau)?;
    let after = values.get(tau..)?;
    let before_median = stats::median(before)?;
    let after_median = stats::median(after)?;
    let mut residuals: Vec<f64> = before
        .iter()
        .map(|value| (value - before_median).abs())
        .chain(after.iter().map(|value| (value - after_median).abs()))
        .collect();
    stats::median_in_place(&mut residuals)
}

/// The median absolute residual of the linear (drift) model `intercept + slope·i`.
fn line_model_residual(values: &[f64], slope: f64, intercept: f64) -> Option<f64> {
    let mut residuals: Vec<f64> = values
        .iter()
        .enumerate()
        .map(|(index, value)| (value - (intercept + slope * count_to_f64(index))).abs())
        .collect();
    stats::median_in_place(&mut residuals)
}

/// Whether `delta` stands clear of a series' own between-commit scatter: it must
/// exceed [`RESIDUAL_NOISE_MULTIPLE`](noise_gates::RESIDUAL_NOISE_MULTIPLE) times the
/// model's median absolute residual. A clean series has a near-zero residual, so any
/// persistent move passes; a jittery one demands a move that stands out above its
/// wobble. A missing residual (an empty model) is treated as no evidence of noise, so
/// the move is trusted.
fn exceeds_residual_noise(delta: f64, residual: Option<f64>, log: &mut StageLog<'_>) -> bool {
    match residual {
        Some(residual) => {
            let band = noise_gates::RESIDUAL_NOISE_MULTIPLE * residual;
            log.numeric(Gate::ResidualNoise, delta.abs(), band, delta.abs() > band)
        }
        None => true,
    }
}

/// Whether the two regimes of a `delta`-signed level shift are *separated enough*
/// to be distinct populations, rather than two windows onto one noisy distribution.
///
/// A rank test's p-value proves only that the regimes *differ*, and it grows more
/// significant with sample size even for a heavily overlapping move — so a long but
/// stationary series that oscillates between two levels (noisy yet stable) passes
/// the significance gate. This gate adds the effect-size the significance test
/// lacks: the Mann–Whitney probability of superiority (the chance a random `after`
/// point exceeds a random `before` one), oriented in the move's direction, must
/// reach `floor`. A genuine step scores ~1; bimodal jitter scores near ½ and is
/// rejected. Missing statistics (`None`, from an empty sample) are treated as no
/// evidence of overlap, so the move is trusted.
///
/// The `floor` is the caller's, because what the separation buys differs by caller:
/// reporting a move is held to `min_regime_separation`, while accepting a base-window
/// regime boundary — which discards the levels before it — is held to the stricter
/// `min_base_split_separation`.
fn regimes_are_separated(
    superiority: Option<f64>,
    delta: f64,
    floor: f64,
    log: &mut StageLog<'_>,
) -> bool {
    match superiority {
        // `superiority` is P(after > before); a fall is judged by the complementary
        // P(before > after), so both directions are measured against the same floor.
        Some(superiority) => {
            let directional = if delta >= 0.0 {
                superiority
            } else {
                1.0 - superiority
            };
            log.numeric(
                Gate::RegimeSeparation,
                directional,
                floor,
                directional >= floor,
            )
        }
        None => true,
    }
}

/// Chooses between a change-point and a drift candidate for the same series.
///
/// When both detectors fire, the data is described as whichever model fits it
/// better — a sharp step leaves a flat residual under the two-regime model, while
/// a gradual ramp leaves a flat residual under the line — so we keep the candidate
/// with the smaller median absolute residual (ties favour the more specific
/// change-point). When only one fires, it is kept.
///
/// The second result is a drift fallback when the change-point fits at least as
/// well. Calibration may still reject that preferred change, in which case the
/// already-qualified drift is the result, matching arbitration over two fully
/// evaluated candidates without calibrating a change that loses on fit.
fn arbitrate(
    values: &[f64],
    change: Option<Candidate>,
    drift: Option<Candidate>,
) -> (Option<Candidate>, Option<Candidate>) {
    match (change, drift) {
        (Some(change), Some(drift)) => {
            let step_residual = change
                .split
                .and_then(|tau| step_model_residual(values, tau));
            let line_residual = drift
                .line
                .and_then(|(slope, intercept)| line_model_residual(values, slope, intercept));
            match (step_residual, line_residual) {
                (Some(step), Some(line)) if line < step => (Some(drift), None),
                _ => (Some(change), Some(drift)),
            }
        }
        (Some(change), None) => (Some(change), None),
        (None, drift) => (drift, None),
    }
}

/// Doubles a history detector's chance level to account for the tool running *both* the
/// change-point and drift detectors on every series and reporting whichever fits better
/// (decision D4).
///
/// Reporting only the better-fitting of two detectors is itself a selection: across a
/// corpus of series with no real change, "the better of two detectors looked striking"
/// happens about twice as often as a single fixed detector would. Doubling the reported
/// chance level cancels that inflation. The factor is a conservative ceiling ("at most
/// about twice as often"), so it holds however strongly the two detectors agree, and it is
/// applied to both before each detector's significance gate so a candidate cannot clear the
/// gate on an uncorrected value. Ref: `../../cargo-bench-history/docs/DESIGN.md`,
/// "Multiple-comparison discipline".
///
/// The result is clamped to `1.0`, since a chance level cannot exceed certainty. Branch
/// mode runs no arbitration, so the factor does not apply there.
fn across_both_detectors(chance_level: f64) -> f64 {
    (count_to_f64(noise_gates::HISTORY_DETECTOR_COUNT) * chance_level).min(1.0)
}

/// Bounded permutation-group order for one change-point test in this analysis family.
fn change_point_permutation_order_budget(family_size: usize) -> NonZero<usize> {
    NonZero::new(
        family_size
            .saturating_mul(noise_gates::PERMUTATION_ORDER_PER_JUDGED_SERIES)
            .clamp(
                noise_gates::MIN_CHANGE_PERMUTATION_ORDER,
                noise_gates::MAX_CHANGE_PERMUTATION_ORDER,
            ),
    )
    .expect("an evaluated series belongs to a nonempty judged family")
}

/// Rank-1 Benjamini-Hochberg boundary before the two-detector correction.
fn smallest_family_chance_level(family_size: usize) -> f64 {
    noise_gates::TARGET_FALSE_DISCOVERY_RATE
        / (count_to_f64(noise_gates::HISTORY_DETECTOR_COUNT) * count_to_f64(family_size))
}

/// Largest selection-adjusted chance level that can pass detector arbitration.
fn max_selection_adjusted_chance_level() -> f64 {
    noise_gates::MAX_CHANGE_CHANCE_LEVEL / count_to_f64(noise_gates::HISTORY_DETECTOR_COUNT)
}

pub(crate) fn passes_significance(chance_level: f64, limit: f64) -> bool {
    chance_level < limit
}

/// Locates a sustained level shift in `series`, returning a [`Candidate`] when the
/// noise-aware gates pass.
///
/// The Pettitt test *locates* the split (its analytic p-value is conservative for
/// short series, so it is not used as a significance gate); both regimes must hold
/// at least `min_regime` points (persistence). The move must then be confirmed by a
/// significant Mann–Whitney rank-sum difference between the regimes, clear the
/// practical-magnitude floor (relative, plus the metric's own absolute floor), stand
/// above the series' own between-commit residual
/// scatter, separate the two regimes as populations (the Mann–Whitney effect-size
/// gate that rejects a noisy-but-stable series whose levels interleave), and — when
/// the engine reports per-point confidence intervals — separate the two regimes'
/// intervals.
fn evaluate_change_point(series: &Series, values: &[f64], log: &mut GateLog) -> Option<Candidate> {
    let mut log = log.stage(GateStage::ChangePoint);
    let points = &series.points;
    let n = points.len();

    let located = stats::pettitt(values);
    if !log.boolean(Gate::SplitLocated, located.is_some()) {
        return None;
    }
    let change = located?;
    let tau = change.index;
    let before_len = tau;
    let after_len = n.checked_sub(tau)?;
    let shortest = before_len.min(after_len);
    if !log.numeric(
        Gate::MinRegime,
        count_to_f64(shortest),
        count_to_f64(noise_gates::MIN_REGIME),
        shortest >= noise_gates::MIN_REGIME,
    ) {
        return None;
    }

    let before = values.get(..tau)?;
    let after = values.get(tau..)?;
    let baseline = stats::median(before)?;
    let latest = stats::median(after)?;
    let delta = latest - baseline;
    if !log.numeric(Gate::NonZeroDelta, delta.abs(), 0.0, delta.abs() > 0.0) {
        return None;
    }
    let relative_delta = relative_delta_of(delta, baseline);
    if !log.numeric(
        Gate::RelativeFloor,
        relative_delta.abs(),
        noise_gates::PRACTICAL_RELATIVE,
        relative_delta.abs() >= noise_gates::PRACTICAL_RELATIVE,
    ) {
        return None;
    }
    if !clears_absolute_floor(series, delta, &mut log) {
        return None;
    }
    if !exceeds_residual_noise(delta, step_model_residual(values, tau), &mut log) {
        return None;
    }
    if !regimes_are_separated(
        stats::mann_whitney_superiority(before, after),
        delta,
        noise_gates::MIN_REGIME_SEPARATION,
        &mut log,
    ) {
        return None;
    }
    let before_points: Vec<&SeriesPoint> = points.iter().take(tau).collect();
    let after_points: Vec<&SeriesPoint> = points.iter().skip(tau).collect();
    if !regime_intervals_are_disjoint(&before_points, &after_points, &mut log) {
        return None;
    }

    let commit = points.get(tau).and_then(owned_commit);
    Some(Candidate {
        finding: Finding {
            set: series.set.clone(),
            id: series.id.clone(),
            kind: series.kind,
            method: FindingMethod::ChangePoint,
            direction: direction_of(delta),
            baseline,
            latest,
            delta,
            relative_delta,
            commit,
            window_start_commit: None,
            blessed_at: None,
            blessed_commit_time: None,
            series: Vec::new(),
            comparison_base_index: None,
            chart_base_ref: None,
            branch: None,
        },
        source_index: 0,
        bh_p: NO_EVIDENCE,
        split: Some(tau),
        line: None,
    })
}

/// Selection-adjusts a preferred change-point candidate after cheap gates and arbitration.
fn calibrate_change_point(
    values: &[f64],
    family_size: usize,
    mut candidate: Candidate,
    log: &mut GateLog,
) -> Option<Candidate> {
    let mut log = log.stage(GateStage::ChangePoint);
    let reject_at_or_above = max_selection_adjusted_chance_level();
    debug_assert!(
        across_both_detectors(reject_at_or_above)
            .total_cmp(&noise_gates::MAX_CHANGE_CHANCE_LEVEL)
            .is_eq(),
        "the early-rejection boundary must invert detector arbitration"
    );
    let calibration = stats::SelectionCalibration {
        permutation_order_budget: change_point_permutation_order_budget(family_size),
        analytic_weight: noise_gates::CHANGE_ANALYTIC_WEIGHT,
        accept_analytic_below: smallest_family_chance_level(family_size).min(reject_at_or_above),
        reject_at_or_above,
    };
    let selection =
        stats::selection_adjusted_change_point(values, noise_gates::MIN_REGIME, calibration)?;
    debug_assert_eq!(
        candidate.split,
        Some(selection.index),
        "the observed ordering must use the same Pettitt scorer as calibration"
    );
    let adjusted_p = log.adjustment(
        Gate::SelectionAdjustment,
        selection.tainted_p,
        selection.adjusted_p,
    );
    // Both history detectors run on every series and the better-fitting one is reported, a
    // second selection that inflates the false-alarm rate about twofold (decision D4).
    let effective_p = across_both_detectors(adjusted_p);
    if !log.numeric(
        Gate::Significance,
        effective_p,
        noise_gates::MAX_CHANGE_CHANCE_LEVEL,
        passes_significance(effective_p, noise_gates::MAX_CHANGE_CHANCE_LEVEL),
    ) {
        return None;
    }
    candidate.bh_p = effective_p;
    Some(candidate)
}

/// Locates a slow monotonic drift in `series`, returning a [`Candidate`] when the
/// trend is significant and practically meaningful.
///
/// The trend is established by the Mann–Kendall test and quantified by the
/// Theil–Sen line, so a single outlier cannot manufacture a drift. The total
/// movement must clear the practical-magnitude floor (relative, plus the metric's
/// own absolute floor) and stand above the series'
/// own residual scatter about the fitted line; where the engine reports confidence
/// intervals it must additionally exceed a multiple of the per-measurement noise floor
/// ([`DRIFT_NOISE_MULTIPLE`](noise_gates::DRIFT_NOISE_MULTIPLE) times the median
/// half-width), so jitter does not read as a trend.
fn evaluate_drift(series: &Series, values: &[f64], log: &mut GateLog) -> Option<Candidate> {
    let mut log = log.stage(GateStage::Drift);
    let points = &series.points;
    let n = points.len();
    if !log.numeric(
        Gate::MinSeriesPoints,
        count_to_f64(n),
        count_to_f64(noise_gates::DRIFT_MIN_POINTS),
        n >= noise_gates::DRIFT_MIN_POINTS,
    ) {
        return None;
    }

    let trend = stats::mann_kendall(values);
    // The drift detector fits one predetermined trend line and runs no split search, so its
    // p-value is not search-tainted and needs no selection adjustment. It still shares the
    // series with the change-point detector, and the better-fitting result is reported, so it
    // takes the same two-detector factor (decision D4).
    let effective_p = across_both_detectors(trend.p_value);
    if !log.numeric(
        Gate::Significance,
        effective_p,
        noise_gates::MAX_DRIFT_CHANCE_LEVEL,
        passes_significance(effective_p, noise_gates::MAX_DRIFT_CHANCE_LEVEL),
    ) {
        return None;
    }
    let (slope, intercept) = stats::theil_sen_line(values)?;
    let span = count_to_f64(n.checked_sub(1)?);
    let baseline = intercept;
    let latest = intercept + slope * span;
    let delta = latest - baseline;
    if !log.numeric(Gate::NonZeroDelta, delta.abs(), 0.0, delta != 0.0) {
        return None;
    }
    let relative_delta = relative_delta_of(delta, baseline);
    if !log.numeric(
        Gate::RelativeFloor,
        relative_delta.abs(),
        noise_gates::PRACTICAL_RELATIVE,
        relative_delta.abs() >= noise_gates::PRACTICAL_RELATIVE,
    ) {
        return None;
    }
    if !clears_absolute_floor(series, delta, &mut log) {
        return None;
    }
    if !exceeds_residual_noise(
        delta,
        line_model_residual(values, slope, intercept),
        &mut log,
    ) {
        return None;
    }
    // Where the engine reports dispersion, a trend must also clear the measurement
    // noise floor: the endpoints have to separate by more than the run-to-run
    // dispersion, or it is just jitter.
    if !exceeds_noise_band(delta, points, noise_gates::DRIFT_NOISE_MULTIPLE, &mut log) {
        return None;
    }

    let commit = points.last().and_then(owned_commit);
    let window_start_commit = points.first().and_then(owned_commit);
    Some(Candidate {
        finding: Finding {
            set: series.set.clone(),
            id: series.id.clone(),
            kind: series.kind,
            method: FindingMethod::Drift,
            direction: direction_of(delta),
            baseline,
            latest,
            delta,
            relative_delta,
            commit,
            window_start_commit,
            blessed_at: None,
            blessed_commit_time: None,
            series: Vec::new(),
            comparison_base_index: None,
            chart_base_ref: None,
            branch: None,
        },
        source_index: 0,
        bh_p: effective_p,
        split: None,
        line: Some((slope, intercept)),
    })
}

/// The post-blessing window of `series` as a standalone series for detection, capped to
/// the most recent [`MAX_SERIES_POINTS`](noise_gates::MAX_SERIES_POINTS) points.
///
/// History-mode detection runs on this view so a blessed (re-baselined) series is
/// only judged from the blessed commit onward; the full series is restored on the
/// finding afterwards for charting. An unblessed series (`active_start == 0`) starts
/// at its first point, but still loses its oldest points if it exceeds the cap.
///
/// The cap drops the oldest points beyond the supported length so both detectors
/// and runtime permutation calibration see the same bounded `n`. The tool is
/// built for series of dozens to a few hundred points, so the cap changes
/// nothing in ordinary use. Ref: `../../cargo-bench-history/docs/DESIGN.md`,
/// "Supported series length".
fn active_view(series: &Series) -> Series {
    let active = series.points.get(series.active_start..).unwrap_or_default();
    let keep_from = active.len().saturating_sub(noise_gates::MAX_SERIES_POINTS);
    let points = active
        .get(keep_from..)
        .map(<[SeriesPoint]>::to_vec)
        .unwrap_or_default();
    Series {
        set: series.set.clone(),
        id: series.id.clone(),
        kind: series.kind,
        points,
        base_window: series.base_window.clone(),
        base_history_count: series.base_history_count,
        active_start: 0,
        blessing: None,
    }
}

/// Records a history-mode finding's re-baseline provenance, so the report can name
/// the blessing.
///
/// The finding's charting points ([`Finding::series`]) are filled in later, when
/// the candidate survives filtering (see [`find_changes_spawned`]); a dropped candidate
/// never builds them.
fn stamp_history(finding: &mut Finding, series: &Series) {
    if let Some(blessing) = &series.blessing {
        finding.blessed_at = Some(short_commit(&blessing.commit));
        finding.blessed_commit_time = blessing.commit_time.map(|time| time.to_string());
    }
}

/// Abbreviates a commit ID for display (first 12 hex digits).
#[must_use]
pub fn short_commit(commit: &str) -> String {
    commit.get(..12).unwrap_or(commit).to_owned()
}

/// Serial reference for the spawner-distributed [`find_changes_spawned`]: detects
/// every series in one contiguous scan, then runs the shared finalize tail.
///
/// Exists only as test scaffolding — the independent oracle for
/// `find_changes_spawned_matches_the_serial_pass` (the spawned path chunks and
/// recombines; this one never chunks), a spawner-free convenience for the crate's
/// unit tests (the tests below and the `signal_validation` suite), and the
/// documentation generator's batch entry point. Production detection goes through
/// [`find_changes_spawned`].
#[cfg(any(test, feature = "private-test-util"))]
#[must_use]
pub fn find_changes(series: &[Series], context: &AnalysisContext) -> Detection {
    if context.mode == AnalysisMode::Branch {
        return branch::find_changes(series, context);
    }
    let census = census_of(series, context);
    let candidates = detect_all(series, context, census.judged());
    let findings = finalize_findings(candidates, &census, series, context);
    Detection {
        findings,
        census,
        branch_comparisons: Vec::new(),
        branch_trace: BranchEvaluationTrace::default(),
    }
}

/// Evaluates every series and returns the surviving findings, ranked
/// most-notable first, together with the census of what was judged to produce them
/// — the analysis's detection entry point.
///
/// The [`AnalysisContext`] selects the per-series detector: history mode locates a
/// change-point and a drift and keeps the better-fitting one; branch mode compares
/// the branch's latest state against the observed current-base regime. A series that
/// cannot be judged is accounted for in the returned [`SeriesCensus`].
///
/// History candidates pass a Benjamini–Hochberg false-discovery filter at
/// [`TARGET_FALSE_DISCOVERY_RATE`](noise_gates::TARGET_FALSE_DISCOVERY_RATE). Branch
/// excursions remain factual findings; their report-wide context comes from symmetric
/// historical base turns.
///
/// Per-series preparation is split into balanced contiguous chunks and run on blocking
/// tasks via `spawner`, then recombined in series order. History uses a serial
/// testability prepass to establish the false-discovery family size before parallel
/// detection. Branch mode prepares regimes and excursions in parallel, then constructs
/// and scores the rectangular historical family on the calling thread. A single
/// available CPU, as reported under Miri, yields one chunk and one task.
///
/// The series are taken as an `Arc<[Series]>` so each blocking task can share them
/// without copying. Production passes a Tokio-backed spawner; tests and Miri pass an
/// inline spawner that runs each task on the calling thread.
pub async fn find_changes_spawned(
    series: Arc<[Series]>,
    context: AnalysisContext,
    spawner: &Spawner,
) -> Detection {
    if context.mode == AnalysisMode::Branch {
        return branch::find_changes_spawned(series, context, spawner).await;
    }
    let census = census_of(&series, &context);
    let candidates = detect_all_spawned(&series, context, census.judged(), spawner).await;
    let findings = finalize_findings(candidates, &census, &series, &context);
    Detection {
        findings,
        census,
        branch_comparisons: Vec::new(),
        branch_trace: BranchEvaluationTrace::default(),
    }
}

/// Whether `series` carries enough evidence for its mode's detector to reach a
/// verdict, and if not, what it lacks.
///
/// This is the single definition of what "judged" means: detection consults it to
/// decide whether to evaluate a series at all, and the census counts its answers.
/// History mode's false-discovery family is exactly the history series it calls
/// [`Judged`](Testability::Judged). Branch mode reuses the branch detector's final verdict
/// for coverage accounting but applies no false-discovery correction, so unresolved
/// current-base regimes stay unjudged everywhere that matters.
#[must_use]
pub fn testability(series: &Series, context: &AnalysisContext) -> Testability {
    match context.mode {
        AnalysisMode::History => {
            // The detectors run on the post-blessing window (see `active_view`), so
            // that window's length — not the whole series' — is the evidence.
            let active_points = series.points.len().saturating_sub(series.active_start);
            if active_points >= noise_gates::MIN_SERIES_POINTS {
                Testability::Judged
            } else if series.active_start > 0 {
                Testability::Unjudged(UnjudgedReason::TooFewPointsSinceBlessing)
            } else {
                Testability::Unjudged(UnjudgedReason::TooFewPoints)
            }
        }

        AnalysisMode::Branch => branch::testability(series, context),
    }
}

/// Classifies the complete analysis family before per-series detection.
///
/// Change-point permutation precision depends on the number of judged
/// hypotheses that Benjamini-Hochberg will later filter. Testability is a cheap
/// metadata-only decision, so this prepass makes that family size available to
/// every parallel worker without moving statistical work out of the workers.
fn census_of(series: &[Series], context: &AnalysisContext) -> SeriesCensus {
    let mut census = SeriesCensus::default();
    for one in series {
        census.record(testability(one, context));
    }
    census
}

/// Applies the false-discovery filter, materialises the surviving findings' charting
/// points, and ranks them — the cross-series tail shared by the serial and
/// spawner-distributed detection passes.
///
/// `candidates` must be in series order (the order both detection paths produce) so
/// the Benjamini–Hochberg mask stays aligned, and `census` must be the account the
/// same pass produced, since its judged tally is the family the correction divides
/// by.
fn finalize_findings(
    candidates: Vec<Candidate>,
    census: &SeriesCensus,
    series: &[Series],
    context: &AnalysisContext,
) -> Vec<Finding> {
    // Screen to the directions this mode reports *before* the correction, so that every
    // hypothesis the correction rejects is a finding the report goes on to show.
    // Correcting over both directions and discarding one afterwards would attach the
    // false-discovery guarantee to a set larger than the reported one: discarding true
    // improvements shrinks the denominator the rate is defined over while leaving false
    // regressions in place, so the regressions actually shown would inherit no bound.
    // Screening first costs only power, and it is conservative: the detectors report
    // two-sided p-values from symmetric nulls, so for an unchanged series the chance of
    // raising a candidate in a direction named in advance is at most half the chance of
    // raising one either way. The p-values the correction sees therefore overstate the
    // risk of what it admits, and the bound holds with room to spare.
    // Ref: DESIGN.md, "Multiple-comparison discipline".
    let mut candidates = candidates;
    candidates.retain(|candidate| context.keeps(candidate.finding.direction));

    // Control the false-discovery rate across every series that was actually judged,
    // not merely those that raised a candidate. Feeding the filter only its own
    // survivors would make it a no-op: each has already cleared `change_alpha`, which
    // is below the loosest Benjamini–Hochberg threshold, so nothing could ever be
    // rejected. The family is the whole set of hypotheses tested, which is precisely
    // what the census counted as judged.
    let family_size = census.judged();
    let candidate_p: Vec<f64> = candidates.iter().map(|candidate| candidate.bh_p).collect();
    let keep = stats::benjamini_hochberg(
        &candidate_p,
        noise_gates::TARGET_FALSE_DISCOVERY_RATE,
        family_size,
    );
    let mut keep_iter = keep.into_iter();

    // `candidates` and `candidate_p` were built in the same order, so advancing
    // `keep_iter` for each candidate keeps the mask aligned. A surviving finding
    // materialises its charting points here — a dropped candidate never pays for them.
    let mut findings: Vec<Finding> = candidates
        .into_iter()
        .filter_map(|candidate| {
            if !keep_iter.next().unwrap_or(false) {
                return None;
            }
            let Candidate {
                mut finding,
                source_index,
                ..
            } = candidate;
            let source = series
                .get(source_index)
                .expect("the source index was assigned from this series slice");
            let (chart_series, chart_base_ref) = build_chart_series(source, &finding, context);
            finding.series = chart_series;
            finding.chart_base_ref = chart_base_ref;
            Some(finding)
        })
        .collect();

    findings.sort_by(|left, right| {
        right
            .relative_delta
            .abs()
            .total_cmp(&left.relative_delta.abs())
            .then_with(|| left.method.cmp(&right.method))
            .then_with(|| left.set.cmp(&right.set))
            .then_with(|| left.id.cmp(&right.id))
            .then_with(|| left.kind.cmp(&right.kind))
    });
    findings
}

/// Detects every series sequentially, returning the raised candidates in series
/// order — the order [`finalize_findings`] relies on — and the census of what was
/// judged.
#[cfg(any(test, feature = "private-test-util"))]
fn detect_all(series: &[Series], context: &AnalysisContext, family_size: usize) -> Vec<Candidate> {
    detect_range(
        series,
        0..series.len(),
        context,
        family_size,
        &mut GateLog::disabled(),
    )
}

/// Detects every series, distributed across workers: splits the series into one
/// balanced contiguous chunk per worker (the worker count is the available
/// parallelism capped at the series count), runs each chunk on its own blocking task
/// via `spawner`, and recombines the candidates in series order.
///
/// A single available CPU (which is what Miri reports) yields a single worker — one
/// chunk, one task covering every series — so the one-worker case is just the
/// degenerate partition rather than a separate serial branch. An empty slice yields no
/// workers and dispatches no task.
async fn detect_all_spawned(
    series: &Arc<[Series]>,
    context: AnalysisContext,
    family_size: usize,
    spawner: &Spawner,
) -> Vec<Candidate> {
    let len = series.len();
    let workers = worker_count(len);

    // Spawn every chunk before awaiting any, so the blocking tasks run concurrently;
    // each owns a shared `Arc` handle to the series and a `Copy` of the context.
    let mut handles = Vec::with_capacity(workers);
    let mut start: usize = 0;
    for size in balanced_chunk_sizes(len, workers) {
        let end = start.saturating_add(size);
        let chunk = Arc::clone(series);
        handles.push(spawner.spawn_blocking(move || {
            detect_range(
                &chunk,
                start..end,
                &context,
                family_size,
                &mut GateLog::disabled(),
            )
        }));
        start = end;
    }

    // Concatenate in spawn order, which is series order, so the candidate sequence is
    // identical to the serial pass.
    let mut candidates = Vec::new();
    for handle in handles {
        candidates.extend(handle.await);
    }
    candidates
}

/// Detects the series in `range`, returning the raised candidates in index order.
///
/// `log` observes the gates of every series in `range`, so a caller that wants a
/// readable log passes a range covering exactly one series (see [`evaluate_with_log`]);
/// every other caller passes a disabled log.
fn detect_range(
    series: &[Series],
    range: Range<usize>,
    context: &AnalysisContext,
    family_size: usize,
    log: &mut GateLog,
) -> Vec<Candidate> {
    let mut candidates = Vec::new();
    for index in range {
        let one = series
            .get(index)
            .expect("the range is within the series slice");
        let verdict = testability(one, context);
        if verdict.is_judged()
            && let Some(candidate) = detect_one(index, one, family_size, log)
        {
            candidates.push(candidate);
        }
    }
    candidates
}

/// Runs the mode-appropriate detector on the series at `index` and returns its
/// candidate finding, if one is raised.
///
/// This is pure and depends on no other series, which is what lets
/// [`find_changes_spawned`] evaluate the series across workers. Callers must have
/// established that the series can be judged (see [`testability`]). History mode
/// locates a change-point and a drift and keeps the better-fitting one; branch mode
/// delegates to its dedicated detector.
/// `index` is the series' position in the analysed slice, stamped onto the candidate so
/// the finalize tail can materialise its charting points only if it survives filtering.
fn detect_one(
    index: usize,
    one: &Series,
    family_size: usize,
    log: &mut GateLog,
) -> Option<Candidate> {
    let active = active_view(one);
    // The point values are projected once here and shared by every history detector,
    // rather than each rebuilding the same `Vec<f64>`.
    let values: Vec<f64> = active.points.iter().map(|point| point.value).collect();
    let change = evaluate_change_point(&active, &values, log);
    let drift = evaluate_drift(&active, &values, log);
    let (preferred, fallback) = arbitrate(&values, change, drift);
    let candidate = preferred
        .and_then(|candidate| {
            if candidate.finding.method == FindingMethod::ChangePoint {
                calibrate_change_point(&values, family_size, candidate, log).or(fallback)
            } else {
                Some(candidate)
            }
        })
        .map(|mut candidate| {
            stamp_history(&mut candidate.finding, one);
            candidate
        });
    candidate.map(|mut candidate| {
        candidate.source_index = index;
        candidate
    })
}

/// Evaluates one series exactly as an analysis pass would, returning both the finding it
/// yields and a [`GateLog`] of how the detectors reached that verdict.
///
/// This is the observable form of detection: the same mode-specific code path a real pass
/// runs, including history detector arbitration and false-discovery filtering or branch
/// current-regime range evaluation. Evaluating one series produces the same factual
/// finding and gate decisions as a whole-suite pass; report-wide history correction or
/// branch comparison context necessarily reflects the smaller family.
///
/// Exists for tests that must assert *why* a series was reported or was quiet, and for
/// the documentation figures, which read the log rather than restating the policy. It is
/// an inspection facility, not part of the analysis API, so it is available only to
/// in-workspace consumers under `private-test-util`; the recording itself is compiled
/// unconditionally, so what is observed here is what production runs.
#[cfg(any(test, feature = "private-test-util"))]
#[must_use]
pub fn evaluate_with_log(series: &Series, context: &AnalysisContext) -> (Option<Finding>, GateLog) {
    if context.mode == AnalysisMode::Branch {
        return branch::evaluate_with_log(series, context);
    }
    let mut log = GateLog::recording();
    let batch = slice::from_ref(series);
    let census = census_of(batch, context);
    let candidates = detect_range(batch, 0..batch.len(), context, census.judged(), &mut log);
    let findings = finalize_findings(candidates, &census, batch, context);
    (findings.into_iter().next(), log)
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    #![allow(
        clippy::float_cmp,
        reason = "metric values are exact integer-derived counts"
    )]
    #![allow(clippy::indexing_slicing, reason = "panic is fine in tests")]

    use std::slice;
    use std::sync::Arc;

    use cbh_model::{DiscriminantSet, Engine, MetricKind};
    use jiff::Timestamp;
    use nonempty::nonempty;

    use super::*;
    use crate::detect::gate_log::GateOutcome;
    use crate::detect::noise_gates::{
        CHANGE_ANALYTIC_WEIGHT, DRIFT_MIN_POINTS, DRIFT_NOISE_MULTIPLE, MAX_CHANGE_CHANCE_LEVEL,
        MAX_CHANGE_PERMUTATION_ORDER, MAX_DRIFT_CHANCE_LEVEL, MAX_SERIES_POINTS,
        MIN_BASE_SPLIT_SEPARATION, MIN_CHANGE_PERMUTATION_ORDER, MIN_REGIME, MIN_REGIME_SEPARATION,
        MIN_SERIES_POINTS, PERMUTATION_ORDER_PER_JUDGED_SERIES, PRACTICAL_ABSOLUTE_COUNT,
        PRACTICAL_RELATIVE, RESIDUAL_NOISE_MULTIPLE, TARGET_FALSE_DISCOVERY_RATE,
    };
    use crate::detect::recorded::STATIONARY_BIMODAL_NOISE;
    use crate::detect::{Blessing, SeriesPoint, examples};

    /// Builds a Callgrind-style series carrying `values` in topological order, with
    /// no dispersion (no confidence interval).
    fn series_of(values: &[f64]) -> Series {
        series_with(values, MetricKind::InstructionCount, &[])
    }

    /// Builds a series whose benchmark id carries a distinct `name`, so a batch of
    /// series stays individually identifiable in the findings.
    fn named_series(name: &str, values: &[f64]) -> Series {
        let mut series = series_of(values);
        series.id = BenchmarkId::new(nonempty![name.to_owned(), "case".to_owned()]);
        series
    }

    /// Builds a series tagged with `kind`. When `intervals` is non-empty it
    /// supplies a per-point confidence half-width, modelling a noisy engine; an
    /// empty `intervals` leaves the points without dispersion.
    fn series_with(values: &[f64], kind: MetricKind, intervals: &[f64]) -> Series {
        let points = values
            .iter()
            .enumerate()
            .map(|(index, &value)| {
                let half = intervals.get(index).copied();
                SeriesPoint {
                    topo_index: index,
                    dirty: false,
                    object_ordinal: u32::try_from(index).unwrap(),
                    commit: Some(Arc::from(format!("commit{index}"))),
                    value,
                    interval_low: half.map(|half| value - half),
                    interval_high: half.map(|half| value + half),
                }
            })
            .collect();
        Series {
            set: DiscriminantSet {
                engine: Engine::Callgrind,
                target_triple: "t".into(),
                machine_key: "m1".into(),
            },
            id: BenchmarkId::new(nonempty!["group".to_owned(), "case".to_owned()]),
            kind,
            points,
            base_window: Vec::new(),
            base_history_count: 0,
            active_start: 0,
            blessing: None,
        }
    }

    /// A wall-time (noisy) series with a uniform confidence half-width on each
    /// point.
    fn wall_series(values: &[f64], half_width: f64) -> Series {
        let intervals = vec![half_width; values.len()];
        series_with(values, MetricKind::WallTime, &intervals)
    }

    fn only(findings: Vec<Finding>) -> Finding {
        assert_eq!(findings.len(), 1, "expected exactly one finding");
        findings.into_iter().next().unwrap()
    }

    /// The point values of a series, projected as the history detectors receive
    /// them (the production path shares one such projection across detectors).
    fn values_of(series: &Series) -> Vec<f64> {
        series.points.iter().map(|point| point.value).collect()
    }

    /// The `(value, threshold)` the first recorded outcome for `gate` compared, or
    /// `None` when the gate never ran or has no number to report.
    fn gate_value(log: &GateLog, gate: Gate) -> Option<(f64, f64)> {
        let outcome = log.entries().iter().find(|entry| entry.gate == gate)?;
        Some((outcome.value?, outcome.threshold?))
    }

    /// Three consecutive [`MIN_REGIME`]-point regimes at the given levels: the
    /// shortest history that can hold a level moving twice.
    fn three_regimes(first: f64, second: f64, third: f64) -> Vec<f64> {
        let mut values = vec![first; MIN_REGIME];
        values.extend(std::iter::repeat_n(second, MIN_REGIME));
        values.extend(std::iter::repeat_n(third, MIN_REGIME));
        values
    }

    /// Two consecutive [`MIN_REGIME`]-point regimes: the shortest history that can
    /// hold a change point, and exactly [`MIN_SERIES_POINTS`] points long.
    fn step_values(before: f64, after: f64) -> Vec<f64> {
        let mut values = vec![before; MIN_REGIME];
        values.extend(std::iter::repeat_n(after, MIN_REGIME));
        values
    }

    /// A perfectly straight `count`-point ramp starting at `start` and climbing by
    /// `slope` per point, which Theil-Sen fits exactly.
    fn ramp(start: f64, slope: f64, count: usize) -> Vec<f64> {
        #[expect(
            clippy::cast_precision_loss,
            reason = "fixture lengths are far below the f64 integer limit"
        )]
        (0..count)
            .map(|index| slope.mul_add(index as f64, start))
            .collect()
    }

    /// A `count`-point staircase starting at `start` that gains one unit every second
    /// point, which Theil-Sen fits with a slope of one half.
    fn staircase(start: f64, count: usize) -> Vec<f64> {
        ramp(start, 1.0, count)
            .into_iter()
            .flat_map(|level| [level, level])
            .take(count)
            .collect()
    }

    /// Builds a minimal [`Candidate`] carrying only the fields [`arbitrate`]
    /// inspects (`method`, `split`, `line`); every other field is a placeholder.
    fn candidate(
        method: FindingMethod,
        split: Option<usize>,
        line: Option<(f64, f64)>,
    ) -> Candidate {
        Candidate {
            finding: Finding {
                set: DiscriminantSet {
                    engine: Engine::Callgrind,
                    target_triple: "t".into(),
                    machine_key: "m1".into(),
                },
                id: BenchmarkId::new(nonempty!["group".to_owned(), "case".to_owned()]),
                kind: MetricKind::InstructionCount,
                method,
                direction: Direction::Regression,
                baseline: 0.0,
                latest: 0.0,
                delta: 0.0,
                relative_delta: 0.0,
                commit: None,
                window_start_commit: None,
                blessed_at: None,
                blessed_commit_time: None,
                series: Vec::new(),
                comparison_base_index: None,
                chart_base_ref: None,
                branch: None,
            },
            source_index: 0,
            bh_p: 0.0,
            split,
            line,
        }
    }

    /// Runs both phases of history change-point evaluation for focused gate tests.
    fn evaluate_change_point_fully(
        series: &Series,
        values: &[f64],
        family_size: usize,
        log: &mut GateLog,
    ) -> Option<Candidate> {
        let candidate = evaluate_change_point(series, values, log)?;
        calibrate_change_point(values, family_size, candidate, log)
    }

    /// The largest `topo_index` across every point of `series`, the realistic context
    /// index for a history-mode [`AnalysisContext`] over this test fixture. Zero when
    /// there are no points.
    fn max_topo_index(series: &[Series]) -> usize {
        series
            .iter()
            .flat_map(|one| one.points.iter())
            .map(|point| point.topo_index)
            .max()
            .unwrap_or(0)
    }

    /// Runs the history-mode detector under the fixed detection policy.
    fn changes(series: &[Series]) -> Vec<Finding> {
        find_changes(series, &history_context(series)).findings
    }

    /// The history-mode [`AnalysisContext`] the [`changes`] helper runs under.
    fn history_context(series: &[Series]) -> AnalysisContext {
        AnalysisContext {
            mode: AnalysisMode::History,
            merge_base_index: None,
            base_ref_index: None,
            tip_index: max_topo_index(series),
        }
    }

    /// Asserts that every series in `batch` is long enough to be judged and that the
    /// history detectors nevertheless raise nothing. Silence is only evidence about a
    /// gate when the series reached the gates at all, so a negative assertion must
    /// never be satisfied by a fixture that was never testable.
    fn judged_but_silent(batch: &[Series]) {
        let detection = find_changes(batch, &history_context(batch));
        assert_eq!(
            detection.census.judged(),
            batch.len(),
            "every fixture series must be judged, or the silence proves nothing"
        );
        assert!(detection.findings.is_empty());
    }

    #[test]
    fn selection_adjustment_boundary_accounts_for_detector_arbitration() {
        // The change-point gate allows 0.05 after the approved two-detector correction,
        // so the selection-adjusted result must be below half of that before arbitration.
        assert_eq!(max_selection_adjusted_chance_level(), 0.025);
    }

    #[test]
    fn significance_limit_is_strict() {
        assert!(passes_significance(0.04, 0.05));
        assert!(!passes_significance(0.05, 0.05));
        assert!(!passes_significance(0.06, 0.05));
    }

    #[test]
    fn change_point_permutation_order_budget_is_capped() {
        assert_eq!(
            change_point_permutation_order_budget(1).get(),
            MIN_CHANGE_PERMUTATION_ORDER
        );
        assert_eq!(
            change_point_permutation_order_budget(500).get(),
            500 * PERMUTATION_ORDER_PER_JUDGED_SERIES
        );
        assert_eq!(
            change_point_permutation_order_budget(MAX_CHANGE_PERMUTATION_ORDER).get(),
            MAX_CHANGE_PERMUTATION_ORDER
        );
    }

    #[test]
    fn capped_exact_group_resolves_the_default_stress_family() {
        // The stress harness's default large family is the scale promised by the
        // exact-group documentation. Pin that cross-package scenario here so an
        // order budget or weight change cannot silently make rank one unresolvable.
        const DEFAULT_STRESS_FAMILY_SIZE: usize = 20_000;
        const DOCUMENTED_RESOLUTION_LIMIT: usize = 22_394;

        let permutation_weight = 1.0 - CHANGE_ANALYTIC_WEIGHT;
        let budget =
            NonZero::new(MAX_CHANGE_PERMUTATION_ORDER).expect("the production cap is nonzero");
        let group_order = stats::selection_fallback_group_order(MAX_SERIES_POINTS, budget);
        let weighted_floor = 1.0 / (count_to_f64(group_order.get()) * permutation_weight);
        assert!(weighted_floor < smallest_family_chance_level(DEFAULT_STRESS_FAMILY_SIZE));
        assert!(weighted_floor < smallest_family_chance_level(DOCUMENTED_RESOLUTION_LIMIT));
        assert!(
            weighted_floor
                >= smallest_family_chance_level(DOCUMENTED_RESOLUTION_LIMIT.saturating_add(1))
        );
    }

    #[test]
    fn change_point_method_sorts_before_drift() {
        assert!(FindingMethod::ChangePoint < FindingMethod::Drift);
    }

    /// The spawner-distributed [`find_changes_spawned`] must produce exactly the same
    /// findings as the serial [`find_changes`] oracle. On a multi-core host this
    /// exercises the chunked spawn-and-recombine path across several chunks. The
    /// synchronous spawner runs each chunk inline on the calling thread.
    #[cfg_attr(
        miri,
        ignore = "the full mixed batch repeats statistical detection and compares complete rendered findings"
    )]
    #[cfg(feature = "private-test-util")]
    #[test]
    fn find_changes_spawned_matches_the_serial_pass() {
        use crate::testing::synchronous_spawner;

        // A batch large enough to span several worker chunks, mixing series that raise
        // a finding with flat ones that do not, so the spawned path must detect across
        // chunks and preserve series order when recombining.
        let step_up = step_values(100.0, 130.0);
        let step_down = step_values(130.0, 100.0);
        let flat = [100.0; MIN_SERIES_POINTS];
        let shapes: [&[f64]; 3] = [&step_up, &step_down, &flat];
        let series: Vec<Series> = shapes
            .iter()
            .cycle()
            .take(24)
            .enumerate()
            .map(|(index, &values)| named_series(&format!("bench{index:02}"), values))
            .collect();

        let context = AnalysisContext {
            mode: AnalysisMode::History,
            merge_base_index: None,
            base_ref_index: None,
            tip_index: max_topo_index(&series),
        };

        let serial = find_changes(&series, &context);
        let spawned = futures::executor::block_on(find_changes_spawned(
            Arc::from(series.as_slice()),
            context,
            &synchronous_spawner(),
        ));

        // `Finding` is not `PartialEq`; its `Debug` projection is a faithful, total
        // rendering of every field, so equal debug output means equal findings. The
        // census is compared too: a chunked pass that lost a worker's account would
        // shrink the false-discovery family without changing any single finding.
        assert!(
            !serial.findings.is_empty(),
            "the fixture must raise some findings"
        );
        assert_eq!(serial.census.judged(), series.len());
        assert_eq!(format!("{serial:#?}"), format!("{spawned:#?}"));
    }

    #[test]
    fn direction_of_flags_a_rise_as_a_regression() {
        // Every metric is lower-is-better, so a positive delta is a regression and a
        // negative one an improvement.
        assert_eq!(direction_of(1.0), Direction::Regression);
        assert_eq!(direction_of(-1.0), Direction::Improvement);
    }

    #[test]
    fn direction_of_classifies_a_zero_delta_as_an_improvement() {
        // The classification is total: a zero delta (never reached in practice) is
        // defined as an improvement.
        assert_eq!(direction_of(0.0), Direction::Improvement);
    }

    #[test]
    fn regime_interval_takes_the_median_of_each_bound() {
        // Half-width 4 around [10,20,30] gives lows [6,16,26] and highs [14,24,34];
        // their medians are 16 and 24.
        let series = wall_series(&[10.0, 20.0, 30.0], 4.0);
        let refs: Vec<&SeriesPoint> = series.points.iter().collect();
        assert_eq!(regime_interval(&refs), Some((16.0, 24.0)));
    }

    #[test]
    fn regime_interval_without_dispersion_is_none() {
        let series = series_of(&[10.0, 20.0, 30.0]);
        let refs: Vec<&SeriesPoint> = series.points.iter().collect();
        assert_eq!(regime_interval(&refs), None);
    }

    #[test]
    fn intervals_disjoint_detects_separation_in_both_orders() {
        // The after regime sits wholly above the before regime.
        assert!(intervals_disjoint((10.0, 20.0), (30.0, 40.0)));
        // ...and wholly below it.
        assert!(intervals_disjoint((30.0, 40.0), (10.0, 20.0)));
        // Overlapping ranges are not disjoint.
        assert!(!intervals_disjoint((10.0, 20.0), (15.0, 25.0)));
        // Touching at a single boundary counts as overlapping, pinning the strict
        // `<`/`>` comparisons against `<=`/`>=` slips.
        assert!(!intervals_disjoint((10.0, 20.0), (20.0, 30.0)));
        assert!(!intervals_disjoint((20.0, 30.0), (10.0, 20.0)));
    }

    #[test]
    fn median_half_width_is_the_median_interval_half() {
        // A uniform half-width of 4 yields a median half-width of 4 (a `+`/`*` slip
        // in `(high - low) / 2` would instead give the point value or twice the
        // width).
        let series = wall_series(&[10.0, 20.0, 30.0], 4.0);
        assert_eq!(median_half_width(&series.points), Some(4.0));
    }

    #[test]
    fn median_half_width_without_dispersion_is_none() {
        let series = series_of(&[10.0, 20.0, 30.0]);
        assert_eq!(median_half_width(&series.points), None);
    }

    #[test]
    fn noise_band_is_strict_at_the_exact_floor() {
        // A move equal to the band is within noise, not above it: the gate requires a
        // strict excess, so a move exactly at `multiple * half_width` is suppressed.
        // The scenario fixes an exact boundary (half-width 2.0, multiple 3.0, band
        // 6.0, delta 6.0), so relaxing `>` to `>=` would wrongly clear the gate here.
        let series = wall_series(&[100.0, 100.0, 100.0], 2.0);
        let mut unobserved = GateLog::disabled();
        let mut log = unobserved.stage(GateStage::Drift);
        assert!(!exceeds_noise_band(6.0, &series.points, 3.0, &mut log));
    }

    #[test]
    fn step_model_residual_is_the_median_absolute_deviation_per_regime() {
        // before [1,7] -> median 4 -> residuals 3,3; after [40,40] -> median 40 ->
        // residuals 0,0; the median of [3,3,0,0] is 1.5.
        assert_eq!(step_model_residual(&[1.0, 7.0, 40.0, 40.0], 2), Some(1.5));
    }

    #[test]
    fn step_model_residual_out_of_range_tau_is_none() {
        assert_eq!(step_model_residual(&[1.0, 2.0], 5), None);
    }

    #[test]
    fn line_model_residual_measures_distance_from_the_fitted_line() {
        // The line 10 + 2*i predicts [10,12,14,16]; the values deviate by [0,1,0,2],
        // whose median absolute residual is 0.5.
        assert_eq!(
            line_model_residual(&[10.0, 13.0, 14.0, 18.0], 2.0, 10.0),
            Some(0.5)
        );
    }

    #[test]
    fn exceeds_residual_noise_requires_the_move_to_clear_the_scatter_band() {
        let mut unobserved = GateLog::disabled();
        let mut log = unobserved.stage(GateStage::ChangePoint);
        // A residual of 1.0 puts the band at 3x = 3.0. A move inside the band is
        // not clear of it, a move exactly at the band is still not (the comparison
        // is strict), a move above it is, and a missing residual trusts the move.
        assert!(!exceeds_residual_noise(1.0, Some(1.0), &mut log));
        assert!(!exceeds_residual_noise(3.0, Some(1.0), &mut log));
        assert!(exceeds_residual_noise(3.5, Some(1.0), &mut log));
        assert!(exceeds_residual_noise(0.0, None, &mut log));
    }

    #[test]
    fn arbitrate_breaks_a_residual_tie_in_favour_of_the_change_point() {
        // Both models fit a flat series perfectly (residual 0): the tie favours the
        // more specific change-point, so a `line < step` -> `line <= step` slip that
        // would pick the drift is caught.
        let values = [0.0, 0.0, 0.0, 0.0];
        let change = candidate(FindingMethod::ChangePoint, Some(2), None);
        let drift = candidate(FindingMethod::Drift, None, Some((0.0, 0.0)));
        let chosen = arbitrate(&values, Some(change), Some(drift))
            .0
            .expect("one candidate wins");
        assert_eq!(chosen.finding.method, FindingMethod::ChangePoint);
    }

    #[test]
    fn arbitrate_prefers_the_better_fitting_line() {
        // A pure ramp: the line fits with zero residual while the two-regime split
        // leaves a positive residual, so the drift candidate wins.
        let values = [0.0, 1.0, 2.0, 3.0];
        let change = candidate(FindingMethod::ChangePoint, Some(2), None);
        let drift = candidate(FindingMethod::Drift, None, Some((1.0, 0.0)));
        let chosen = arbitrate(&values, Some(change), Some(drift))
            .0
            .expect("one candidate wins");
        assert_eq!(chosen.finding.method, FindingMethod::Drift);
    }

    #[test]
    fn arbitrate_keeps_the_sole_candidate_that_fires() {
        let values = [0.0, 0.0, 5.0, 5.0];
        let change = candidate(FindingMethod::ChangePoint, Some(2), None);
        let only_change = arbitrate(&values, Some(change), None)
            .0
            .expect("the change is retained");
        assert_eq!(only_change.finding.method, FindingMethod::ChangePoint);

        let drift = candidate(FindingMethod::Drift, None, Some((1.0, 0.0)));
        let only_drift = arbitrate(&values, None, Some(drift))
            .0
            .expect("the drift is retained");
        assert_eq!(only_drift.finding.method, FindingMethod::Drift);

        assert!(arbitrate(&values, None, None).0.is_none());
    }

    #[test]
    fn change_point_accepts_a_minimal_before_regime() {
        // Pettitt splits at tau=MIN_REGIME, so the before regime holds exactly
        // `min_regime` points: a `<=`/`==` slip on the before-regime bound would
        // reject the step. One extra after point makes only the before bound minimal.
        // This targets candidate persistence, not the independent permutation calibration.
        let mut values = vec![100.0; MIN_REGIME];
        values.extend(std::iter::repeat_n(130.0, MIN_REGIME + 1));
        assert_minimal_regime_is_accepted(&values);
    }

    #[test]
    fn change_point_accepts_a_minimal_after_regime() {
        // The mirror image: Pettitt splits after the longer before regime, so the after regime
        // holds exactly `min_regime` points and a `<=` slip on the after-regime bound
        // would reject the step.
        let mut values = vec![100.0; MIN_REGIME + 1];
        values.extend(std::iter::repeat_n(130.0, MIN_REGIME));
        assert_minimal_regime_is_accepted(&values);
    }

    fn assert_minimal_regime_is_accepted(values: &[f64]) {
        let series = series_of(values);
        let finding = evaluate_change_point(&series, values, &mut GateLog::disabled())
            .expect("the shorter regime reaches the persistence floor")
            .finding;
        assert_eq!(finding.method, FindingMethod::ChangePoint);
        assert_eq!(finding.baseline, 100.0);
        assert_eq!(finding.latest, 130.0);

        // These lopsided minimal splits need expensive conditional calibration. Native
        // retains that end-to-end assertion; Miri checks the persistence boundary here,
        // with balanced full-detector steps and small kernel orbits covering calibration.
        if !cfg!(miri) {
            let finding = only(changes(&[series]));
            assert_eq!(finding.method, FindingMethod::ChangePoint);
            assert_eq!(finding.baseline, 100.0);
            assert_eq!(finding.latest, 130.0);
        }
    }

    #[test]
    fn change_point_rejects_a_single_point_regime() {
        // Pettitt splits at tau=1, leaving a one-point before regime (below
        // min_regime) against a full-size after regime. The size guard judges the
        // *shorter* regime, so a `.min()`->`.max()` slip would read the full after
        // regime and wrongly admit this lopsided split. The recorded chain names the
        // persistence gate as the one that rejects it: it runs before significance and
        // short-circuits, so the silence is unambiguously the size check's.
        let mut values = vec![100.0];
        values.extend(std::iter::repeat_n(130.0, MIN_REGIME));
        let series = series_of(&values);
        let mut log = GateLog::recording();
        assert!(evaluate_change_point_fully(&series, &values_of(&series), 1, &mut log).is_none());
        assert_eq!(
            log.declined_by_stage(GateStage::ChangePoint),
            Some(Gate::MinRegime),
        );
    }

    #[test]
    fn change_point_within_its_own_residual_scatter_is_suppressed() {
        // A rank-significant step whose regimes each wobble by 2 stands clear of that
        // scatter under the fixed residual multiple, so it is flagged: medians 102 ->
        // 132, a move of 30 against a residual band of 6.
        let clear = series_of(&[
            100.0, 104.0, 100.0, 104.0, 102.0, 130.0, 134.0, 130.0, 134.0, 132.0,
        ]);
        assert!(
            evaluate_change_point_fully(&clear, &values_of(&clear), 1, &mut GateLog::disabled())
                .is_some()
        );

        // The mirror: a cleanly separated step whose regimes wobble by 10 around
        // medians only 30 apart. The move (30) does not exceed its residual band
        // (RESIDUAL_NOISE_MULTIPLE x 10 = 30), so the residual gate suppresses it even
        // though the regimes never interleave. The recorded chain attributes the
        // silence to that gate, with every earlier gate (persistence, significance,
        // practical floors) passing.
        let buried = series_of(&[
            90.0, 90.0, 100.0, 110.0, 110.0, 120.0, 120.0, 130.0, 140.0, 140.0,
        ]);
        let mut log = GateLog::recording();
        assert!(evaluate_change_point_fully(&buried, &values_of(&buried), 1, &mut log).is_none());
        assert_eq!(
            log.declined_by_stage(GateStage::ChangePoint),
            Some(Gate::ResidualNoise),
        );
    }

    #[test]
    fn change_point_significance_is_measured_against_the_fixed_alpha() {
        // The rank-test gate compares the Mann–Whitney p-value against the fixed
        // MAX_CHANGE_CHANCE_LEVEL. A clean step reports, and its recorded p sits strictly below
        // that alpha — so the gate's comparison and its direction are pinned without
        // relying on a per-run alpha. (The `<`-vs-`<=` distinction at a p landing
        // exactly on alpha is not reachable: the rank statistic's p-values are discrete
        // and never equal 0.05, so the shipped policy never sees that boundary.)
        let series = series_of(&[
            100.0, 104.0, 100.0, 104.0, 102.0, 130.0, 134.0, 130.0, 134.0, 132.0,
        ]);
        let mut log = GateLog::recording();
        assert!(
            evaluate_change_point_fully(&series, &values_of(&series), 1, &mut log).is_some(),
            "the fixture must report under the fixed alpha"
        );
        let (p, alpha) = gate_value(&log, Gate::Significance).expect("the significance gate ran");
        assert_eq!(
            alpha, MAX_CHANGE_CHANCE_LEVEL,
            "the gate compares against the fixed alpha"
        );
        assert!(
            p < MAX_CHANGE_CHANCE_LEVEL,
            "the reported step's p sits below alpha: {p}"
        );
    }

    #[test]
    fn regimes_are_separated_rejects_interleaved_levels() {
        let floor = MIN_REGIME_SEPARATION;
        let mut unobserved = GateLog::disabled();
        let mut log = unobserved.stage(GateStage::ChangePoint);
        // A clean rise: every after-point exceeds every before-point (superiority 1).
        assert!(regimes_are_separated(
            stats::MannWhitneyU::new(&[10.0, 11.0, 12.0], &[20.0, 21.0, 22.0])
                .map(|ranked| ranked.superiority()),
            10.0,
            floor,
            &mut log,
        ));
        // A clean fall: judged by the complementary direction, still fully separated.
        assert!(regimes_are_separated(
            stats::MannWhitneyU::new(&[20.0, 21.0, 22.0], &[10.0, 11.0, 12.0])
                .map(|ranked| ranked.superiority()),
            -10.0,
            floor,
            &mut log,
        ));
        // Two levels that recur on both sides: only 0.75 of the after-vs-before pairs
        // move in the rise's direction, below the 0.85 floor, so it is not separated.
        assert!(!regimes_are_separated(
            stats::MannWhitneyU::new(&[10.0, 10.0, 10.0, 30.0], &[30.0, 30.0, 30.0, 10.0],)
                .map(|ranked| ranked.superiority()),
            20.0,
            floor,
            &mut log,
        ));
        // The falling mirror of that overlap: the same two levels recur on both sides,
        // so only 0.75 of the pairs move in the fall's (complementary) direction and it
        // is likewise rejected. Unlike the clean fall above — whose superiority of 0
        // leaves `1 − superiority` indistinguishable from other arithmetic — this pins
        // the fall branch at a fractional superiority (0.25), so the complementary
        // `1 − 0.25 = 0.75 < 0.85` is exercised as a genuine subtraction.
        assert!(!regimes_are_separated(
            stats::MannWhitneyU::new(&[30.0, 30.0, 30.0, 10.0], &[10.0, 10.0, 10.0, 30.0],)
                .map(|ranked| ranked.superiority()),
            -20.0,
            floor,
            &mut log,
        ));
        // No statistics at all (an empty regime): the gate has nothing to veto on, so
        // it trusts the move rather than suppressing it.
        assert!(regimes_are_separated(None, 10.0, floor, &mut log));
    }

    #[test]
    fn the_floor_a_caller_passes_is_the_one_the_separation_gate_applies() {
        // One overlapping split, judged against both floors: the reporting floor admits
        // it and the stricter base-split floor rejects it. This is what makes the two
        // floors distinct policies rather than one constant read from two places.
        let mut unobserved = GateLog::disabled();
        let mut log = unobserved.stage(GateStage::Branch);
        // Seven before-levels against five after-levels, with one before-level lying
        // inside the after regime but under only its topmost value: 34 of 35 crossing
        // pairs fall, a superiority of ~0.971.
        let after = [10.0, 11.0, 12.0, 13.0, 14.0];
        let before = [30.0, 31.0, 32.0, 33.0, 34.0, 35.0, 13.5];
        let superiority =
            stats::MannWhitneyU::new(&before, &after).map(|ranked| ranked.superiority());
        assert!(regimes_are_separated(
            superiority,
            -20.0,
            MIN_REGIME_SEPARATION,
            &mut log,
        ));
        assert!(regimes_are_separated(
            superiority,
            -20.0,
            MIN_BASE_SPLIT_SEPARATION,
            &mut log,
        ));

        // The same shape with that stray level one step lower, so it sits under two of
        // the after-levels: 33 of 35 pairs fall, ~0.943.
        let before = [30.0, 31.0, 32.0, 33.0, 34.0, 35.0, 12.5];
        let superiority =
            stats::MannWhitneyU::new(&before, &after).map(|ranked| ranked.superiority());
        assert!(regimes_are_separated(
            superiority,
            -20.0,
            MIN_REGIME_SEPARATION,
            &mut log,
        ));
        assert!(!regimes_are_separated(
            superiority,
            -20.0,
            MIN_BASE_SPLIT_SEPARATION,
            &mut log,
        ));
    }

    #[test]
    fn change_point_across_interleaved_regimes_is_suppressed() {
        // The real-world series that motivated the separation gate: a wall-time metric
        // that oscillates between ~13 and ~25-29 throughout its whole history, so no
        // commit marks a real level shift. Pettitt still aligns a split with each side's
        // dominant mode, but the populations remain heavily interleaved. The cheap
        // separation gate rejects that shape before permutation calibration starts.
        let values = STATIONARY_BIMODAL_NOISE.to_vec();
        let series = series_of(&values);
        let mut log = GateLog::recording();
        assert!(evaluate_change_point_fully(&series, &values, 1, &mut log).is_none());
        assert_eq!(
            log.declined_by_stage(GateStage::ChangePoint),
            Some(Gate::RegimeSeparation),
        );
    }

    #[test]
    fn sustained_step_is_flagged_as_a_change_point() {
        // A clean step from 100 to 130 with `min_regime` points each side: the
        // shortest history a change point can be found in.
        let series = series_of(&step_values(100.0, 130.0));
        let finding = only(changes(&[series]));
        assert_eq!(finding.method, FindingMethod::ChangePoint);
        assert_eq!(finding.direction, Direction::Regression);
        assert_eq!(finding.baseline, 100.0);
        assert_eq!(finding.latest, 130.0);
        assert_eq!(finding.delta, 30.0);
        assert!((finding.relative_delta - 0.30).abs() <= 1e-9);
        // The change is attributed to the first commit of the after regime.
        assert_eq!(
            finding.commit.as_deref(),
            Some(format!("commit{MIN_REGIME}").as_str())
        );
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "testing the production history cap requires a maximum-length statistical series"
    )]
    fn history_detection_uses_the_newest_capped_active_window_and_keeps_the_full_chart() {
        // A nonzero prefix proves re-baselining is applied before the cap. The following
        // old active regime is also discarded by the cap; if either prefix reached
        // detection, its much higher level would change the statistics and attribution.
        const INACTIVE_PREFIX: usize = 7;
        let half_cap = MAX_SERIES_POINTS
            .checked_div(2)
            .expect("the divisor is nonzero");
        let discarded_active = half_cap;
        let before = half_cap;
        let after = MAX_SERIES_POINTS.saturating_sub(before);

        let mut values = vec![999.0; INACTIVE_PREFIX];
        values.extend(std::iter::repeat_n(900.0, discarded_active));
        values.extend(std::iter::repeat_n(100.0, before));
        values.extend(std::iter::repeat_n(200.0, after));
        let mut series = series_of(&values);
        series.active_start = INACTIVE_PREFIX;

        let active = active_view(&series);
        assert_eq!(active.points.len(), MAX_SERIES_POINTS);
        assert_eq!(
            active
                .points
                .first()
                .map(|point| (point.topo_index, point.value)),
            Some((INACTIVE_PREFIX + discarded_active, 100.0))
        );
        assert_eq!(
            active
                .points
                .last()
                .map(|point| (point.topo_index, point.value)),
            Some((values.len() - 1, 200.0))
        );

        let finding = only(changes(slice::from_ref(&series)));
        assert_eq!(finding.baseline, 100.0);
        assert_eq!(finding.latest, 200.0);
        assert_eq!(
            finding.commit.as_deref(),
            Some(format!("commit{}", INACTIVE_PREFIX + discarded_active + before).as_str())
        );

        // Detection used the capped view, but charting restored the untouched source.
        assert_eq!(series.points.len(), values.len());
        assert_eq!(finding.series.len(), values.len());
        assert_eq!(
            finding
                .series
                .first()
                .map(|point| (point.topo_index, point.value)),
            Some((0, 999.0))
        );
        assert_eq!(
            finding
                .series
                .last()
                .map(|point| (point.topo_index, point.value)),
            Some((values.len() - 1, 200.0))
        );
    }

    #[test]
    fn step_below_the_practical_floor_is_suppressed() {
        // A sub-3% move is treated as measurement noise even when it looks clean, so
        // the practical-magnitude floor suppresses it: 1000 -> 1001 is a 0.1% move.
        let series = series_of(&step_values(1000.0, 1001.0));
        judged_but_silent(&[series]);
    }

    #[test]
    fn step_at_the_practical_floor_is_flagged() {
        // The practical floor is a strict `<` rejection, so a step whose relative move
        // EQUALS the 3% floor is still reported: 1000 -> 1030 is exactly a 3% move.
        let series = series_of(&step_values(1000.0, 1030.0));
        let finding = only(changes(&[series]));
        assert_eq!(finding.method, FindingMethod::ChangePoint);
        assert_eq!(finding.delta, 30.0);
        assert!((finding.relative_delta - 0.03).abs() <= 1e-9);
    }

    #[test]
    fn sub_practical_floor_improvement_is_also_suppressed() {
        // The floor applies in any direction: a sub-3% improvement is just as
        // meaningless as a sub-3% regression, so 1000 -> 999 (a 0.1% drop) raises
        // nothing.
        let series = series_of(&step_values(1000.0, 999.0));
        judged_but_silent(&[series]);
    }

    #[test]
    fn change_point_below_the_absolute_floor_is_suppressed() {
        // On a quantized metric a 4-count move clears the relative floor (4/60 ≈ 6.7%
        // ≥ 3%) and every other gate — significant separation, zero residual — yet is
        // suppressed because it falls short of the absolute floor of 5, where a
        // single-quantum wobble on a tiny count would otherwise read as a regression.
        let series = series_of(&step_values(60.0, 64.0));
        judged_but_silent(&[series]);
    }

    #[test]
    fn change_point_at_the_absolute_floor_is_flagged() {
        // The absolute floor is a `>=` gate, so a 5-count move exactly at the floor is
        // still reported (a `>`/`==` mutant would suppress or misgate it).
        let series = series_of(&step_values(60.0, 65.0));
        let finding = only(changes(&[series]));
        assert_eq!(finding.method, FindingMethod::ChangePoint);
        assert_eq!(finding.delta, 5.0);
    }

    #[test]
    fn change_point_below_the_absolute_floor_on_a_continuous_metric_is_suppressed() {
        // The absolute floor is universal: a continuous metric gets its own, much
        // smaller floor rather than an exemption. This sub-nanosecond wall-time move
        // clears the relative floor, separates cleanly and carries disjoint
        // intervals, yet 0.63 ns of movement is below the resolution any wall-clock
        // measurement can be trusted at, so it stays silent — and the recorded chain
        // attributes that silence to the absolute floor: every other gate passes and
        // AbsoluteFloor is the first to decline.
        let series = wall_series(&step_values(2.49, 3.12), 0.05);
        judged_but_silent(slice::from_ref(&series));
        let mut log = GateLog::recording();
        assert!(evaluate_change_point_fully(&series, &values_of(&series), 1, &mut log).is_none());
        assert_eq!(
            log.declined_by_stage(GateStage::ChangePoint),
            Some(Gate::AbsoluteFloor),
        );
    }

    #[test]
    fn branch_count_rise_is_a_regression() {
        // Branch-execution counts are lower-is-better, so a sustained rise is a
        // regression.
        let series = series_with(
            &step_values(70.0, 100.0),
            MetricKind::ConditionalBranches,
            &[],
        );
        let finding = only(changes(&[series]));
        assert!(finding.is_regression());
        assert_eq!(finding.delta, 30.0);
    }

    #[test]
    fn flat_series_never_flags() {
        let series = series_of(&[100.0; MIN_SERIES_POINTS]);
        judged_but_silent(&[series]);
    }

    #[test]
    fn many_independent_series_are_detected_in_a_stable_order() {
        // `find_changes` runs the per-series detection sequentially. The work is
        // embarrassingly parallel — no series depends on another — so this guards
        // the properties any detection pass must preserve: every independent
        // finding is produced exactly once (the `filter_map`/`collect` neither
        // drops nor duplicates a candidate), flat series stay silent, and the
        // ranking is deterministic across runs (the order-preserving collect plus
        // the final sort fix the output). `find_changes_spawned_matches_the_serial_pass`
        // pins the spawner-distributed pass to this same output.
        let mut series = Vec::new();
        let mut stepped_ids = Vec::new();
        // Distinct findings plus flat companions cover filtering and ranking under Miri.
        // Native runs additionally exercise a large independent family.
        let pairs = if cfg!(miri) { 2 } else { 32 };
        for raw in 0_i32..pairs {
            // A clean step of a distinct magnitude: flags as a regression with its own
            // `|relative_delta|`, so the final ranking is a total order.
            let name = format!("step{raw:03}");
            let raised = 130.0 + f64::from(raw);
            series.push(named_series(&name, &step_values(100.0, raised)));
            stepped_ids.push(BenchmarkId::new(nonempty![name, "case".to_owned()]).qualified());
            // A flat companion never flags, so it must be absent from the output.
            series.push(named_series(
                &format!("flat{raw:03}"),
                &[100.0; MIN_SERIES_POINTS],
            ));
        }

        let findings = changes(&series);

        // Magnitudes increase with each inserted step, so the expected descending
        // ranking is known without rerunning all of detection as its own oracle.
        assert_eq!(
            findings
                .iter()
                .map(|finding| finding.id.qualified())
                .collect::<Vec<_>>(),
            stepped_ids.iter().rev().cloned().collect::<Vec<_>>()
        );

        // Exactly the stepped series flag, each exactly once.
        let mut flagged: Vec<String> = findings
            .iter()
            .map(|finding| finding.id.qualified())
            .collect();
        flagged.sort();
        stepped_ids.sort();
        assert_eq!(flagged, stepped_ids);
    }

    #[test]
    fn a_lone_blip_does_not_flag_a_change_point() {
        // A single spike returns to baseline: the after regime is one point, which
        // fails the persistence requirement.
        let mut values = vec![100.0; MIN_SERIES_POINTS];
        values.push(175.0);
        judged_but_silent(&[series_of(&values)]);
    }

    /// A sustained excursion that has since returned to its opening level is silent, whatever
    /// its magnitude, because the analysis reports what is true of the current state and the
    /// current state matches the baseline.
    ///
    /// This is the property the analysis is narrowed to, so it is asserted directly rather
    /// than left to follow from the detectors' arithmetic: a future change to the
    /// change-point or arbitration path could start reporting such an excursion at its rise,
    /// and nothing else in the suite would notice.
    #[test]
    fn an_excursion_that_returned_to_its_opening_level_is_silent() {
        judged_but_silent(&[series_of(&three_regimes(100.0, 130.0, 100.0))]);
    }

    #[test]
    fn step_in_the_final_point_fails_persistence() {
        // The shift has one point too few after it, so it is rejected even though the
        // levels differ; `change_point_accepts_a_minimal_after_regime` pins the other
        // side of the same boundary.
        let mut values = vec![100.0; MIN_SERIES_POINTS];
        values.extend(std::iter::repeat_n(130.0, MIN_REGIME - 1));
        judged_but_silent(&[series_of(&values)]);
    }

    #[test]
    fn noisy_jitter_around_a_stable_mean_is_not_flagged() {
        // Pure measurement jitter with no real shift must stay silent.
        let series = wall_series(
            &[
                100.0, 103.0, 98.0, 101.0, 99.0, 102.0, 97.0, 100.0, 101.0, 99.0,
            ],
            5.0,
        );
        judged_but_silent(&[series]);
    }

    #[test]
    fn noisy_sustained_step_with_disjoint_intervals_is_flagged() {
        // Two well-separated regimes (≈100 then ≈130) with tight, non-overlapping
        // confidence intervals: the realistic "regression on a noisy series" path.
        let series = wall_series(
            &[
                98.0, 100.0, 102.0, 99.0, 101.0, 128.0, 130.0, 132.0, 129.0, 131.0,
            ],
            2.0,
        );
        let finding = only(changes(&[series]));
        assert_eq!(finding.method, FindingMethod::ChangePoint);
        assert_eq!(finding.direction, Direction::Regression);
        assert_eq!(finding.baseline, 100.0);
        assert_eq!(finding.latest, 130.0);
    }

    #[test]
    fn noisy_step_below_the_practical_floor_is_suppressed() {
        // A real but tiny (~1%) shift clears the statistical tests yet falls under
        // the 3% practical-magnitude floor, so it is not reported.
        let series = wall_series(
            &[
                1000.0, 1001.0, 999.0, 1000.0, 1001.0, 1010.0, 1011.0, 1009.0, 1010.0, 1011.0,
            ],
            1.0,
        );
        judged_but_silent(&[series]);
    }

    #[test]
    fn noisy_step_exactly_at_the_practical_floor_is_reported() {
        // The practical-magnitude floor is a strict `<` rejection, so a step whose
        // relative move EQUALS the fixed floor must still be reported — even on a noisy
        // metric that carries confidence intervals. A wall-time step of 100 -> 103 is
        // exactly PRACTICAL_RELATIVE (3%), and a `<=` slip would suppress this at-floor
        // regression.
        let series = wall_series(
            &[
                100.0, 100.0, 100.0, 100.0, 100.0, 103.0, 103.0, 103.0, 103.0, 103.0,
            ],
            0.5,
        );
        let candidate =
            evaluate_change_point_fully(&series, &values_of(&series), 1, &mut GateLog::disabled())
                .unwrap();
        assert_eq!(candidate.finding.baseline, 100.0);
        assert_eq!(candidate.finding.latest, 103.0);
        assert_eq!(candidate.finding.relative_delta, PRACTICAL_RELATIVE);
    }

    #[test]
    fn noisy_step_with_overlapping_intervals_is_suppressed() {
        // The point values separate cleanly, but each regime's confidence interval
        // is so wide that they overlap, so the change-point gate rejects it.
        let series = wall_series(
            &[
                98.0, 100.0, 102.0, 99.0, 101.0, 128.0, 130.0, 132.0, 129.0, 131.0,
            ],
            60.0,
        );
        judged_but_silent(&[series]);
    }

    #[test]
    fn monotonic_drift_is_flagged() {
        // A steady climb with no single dominant step surfaces as a drift finding.
        let series = series_of(&ramp(100.0, 4.0, MIN_SERIES_POINTS));
        let finding = only(changes(&[series]));
        assert_eq!(finding.method, FindingMethod::Drift);
        assert_eq!(finding.direction, Direction::Regression);
        assert!(finding.delta > 0.0);
        // baseline = fitted intercept (100), latest = intercept + slope*(n-1).
        assert_eq!(finding.baseline, 100.0);
        assert_eq!(finding.latest, 136.0);
    }

    #[test]
    fn a_sharp_step_is_reported_as_a_change_point_not_a_drift() {
        // A series that both trends and steps: the two-regime model fits the sharp
        // jump better than a line, so it is reported once, as a change-point.
        let mut values = ramp(100.0, 1.0, MIN_REGIME);
        values.extend(ramp(160.0, 1.0, MIN_REGIME));
        let findings = changes(&[series_of(&values)]);
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].method, FindingMethod::ChangePoint);
    }

    #[test]
    fn a_batch_of_pure_noise_produces_no_findings() {
        // Twelve independent noisy series, each only wobble within a wide
        // confidence band: every detector gate rejects them, so a batch of noise
        // manufactures no findings.
        let series: Vec<Series> = (0..12)
            .map(|seed: i32| {
                let bump = f64::from(seed.rem_euclid(3));
                wall_series(
                    &[
                        100.0 + bump,
                        99.0,
                        101.0,
                        100.0,
                        98.0 + bump,
                        102.0,
                        100.0,
                        101.0 - bump,
                        99.0 + bump,
                        100.0,
                    ],
                    6.0,
                )
            })
            .collect();
        judged_but_silent(&series);
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "the multi-series permutation fixture takes hours under Miri"
    )]
    fn a_strong_noisy_signal_survives_the_false_discovery_filter() {
        // One unmistakable step alongside many flat series. The false-discovery
        // correction divides by the number of *testable* series, so the single real
        // finding must carry a p-value below q/m to survive: six-point regimes with
        // distinct values inside each give it the margin that a bare `min_regime`
        // step would not have.
        let mut series = vec![wall_series(
            &[
                98.0, 100.0, 102.0, 99.0, 101.0, 100.0, 148.0, 150.0, 152.0, 149.0, 151.0, 150.0,
            ],
            2.0,
        )];
        for _ in 0..6 {
            series.push(wall_series(
                &[
                    100.0, 101.0, 99.0, 100.0, 101.0, 99.0, 100.0, 101.0, 99.0, 100.0,
                ],
                3.0,
            ));
        }
        let findings = changes(&series);
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].method, FindingMethod::ChangePoint);
        assert_eq!(findings[0].baseline, 100.0);
        assert_eq!(findings[0].latest, 150.0);
    }

    #[test]
    fn find_changes_ranks_larger_relative_move_first() {
        let larger = series_of(&step_values(100.0, 200.0));
        let smaller = series_of(&step_values(1000.0, 1050.0));
        let findings = changes(&[smaller, larger]);
        assert_eq!(findings.len(), 2);
        assert!(findings[0].relative_delta.abs() > findings[1].relative_delta.abs());
        assert_eq!(findings[0].latest, 200.0);
        assert_eq!(findings[1].latest, 1050.0);
    }

    #[test]
    fn find_changes_retains_distinct_identities_ordered_by_move() {
        let larger = series_with(
            &step_values(100.0, 200.0),
            MetricKind::InstructionCount,
            &[],
        );
        let mut smaller = series_with(
            &step_values(100.0, 150.0),
            MetricKind::InstructionCount,
            &[],
        );
        // Distinguish the identity so both findings are retained.
        smaller.id = BenchmarkId::new(nonempty!["other".to_owned(), "case".to_owned()]);
        let findings = changes(&[smaller, larger]);
        assert_eq!(findings.len(), 2);
        assert!(findings[0].relative_delta.abs() > findings[1].relative_delta.abs());
        assert_eq!(findings[0].latest, 200.0);
        assert_eq!(findings[1].latest, 150.0);
    }

    /// Builds a series of `kind` from explicit `(topo_index, value, dirty)` points, so
    /// branch splits can be modelled precisely. Points are taken in the given order
    /// (already topological) and carry no dispersion.
    fn placed_series_of_kind(points: &[(usize, f64, bool)], kind: MetricKind) -> Series {
        let points = points
            .iter()
            .map(|&(topo_index, value, dirty)| SeriesPoint {
                topo_index,
                dirty,
                object_ordinal: u32::try_from(topo_index).unwrap(),
                commit: Some(Arc::from(format!("commit{topo_index}"))),
                value,
                interval_low: None,
                interval_high: None,
            })
            .collect();
        Series {
            set: DiscriminantSet {
                engine: Engine::Callgrind,
                target_triple: "t".into(),
                machine_key: "m1".into(),
            },
            id: BenchmarkId::new(nonempty!["group".to_owned(), "case".to_owned()]),
            kind,
            points,
            base_window: Vec::new(),
            base_history_count: 0,
            active_start: 0,
            blessing: None,
        }
    }

    /// Builds a Callgrind-style (instruction count) series from explicit
    /// `(topo_index, value, dirty)` points.
    fn placed_series(points: &[(usize, f64, bool)]) -> Series {
        placed_series_of_kind(points, MetricKind::InstructionCount)
    }

    #[test]
    fn history_finding_has_no_comparison_base_index() {
        // History mode has no single comparison base, so the field stays `None`.
        let series = series_of(&step_values(100.0, 130.0));
        let finding = only(changes(slice::from_ref(&series)));
        assert_eq!(finding.comparison_base_index, None);
    }

    /// The `(topo_index, value)` pairs of a finding's compact chart series.
    fn chart_pairs(finding: &Finding) -> Vec<(usize, f64)> {
        finding
            .series
            .iter()
            .map(|point| (point.topo_index, point.value))
            .collect()
    }

    #[test]
    fn history_chart_series_maps_every_observation_and_targets_the_context() {
        // History mode keeps the series compact and 1:1 — every observation becomes one
        // chart point carrying its real topo index — and stamps the analyzed context
        // commit as the trailing-fill target so a lagging series can render its "no newer
        // data" gap.
        let values = step_values(100.0, 130.0);
        let series = series_of(&values);
        let finding = only(changes(slice::from_ref(&series)));
        assert_eq!(
            chart_pairs(&finding),
            values
                .iter()
                .copied()
                .enumerate()
                .collect::<Vec<(usize, f64)>>(),
        );
        // `changes` analyses up to the last observation.
        assert_eq!(finding.chart_base_ref, Some(values.len() - 1));
        // Detection is unaffected by carrying the topology through.
        assert_eq!(finding.direction, Direction::Regression);
        assert_eq!(finding.latest, 130.0);
    }

    #[test]
    fn history_chart_series_preserves_interior_topology_gaps() {
        // Data-less commits between observations survive as a jump in topo index, which
        // the renderer turns into gap columns: the two regimes are separated by a run of
        // commits carrying no data.
        let gap = MIN_SERIES_POINTS;
        let mut points: Vec<(usize, f64, bool)> =
            (0..MIN_REGIME).map(|index| (index, 100.0, false)).collect();
        points.extend((0..MIN_REGIME).map(|index| (gap + index, 130.0, false)));
        let series = placed_series(&points);
        let finding = only(changes(slice::from_ref(&series)));
        assert_eq!(finding.direction, Direction::Regression);
        assert_eq!(
            chart_pairs(&finding)
                .iter()
                .map(|&(topo, _)| topo)
                .collect::<Vec<_>>(),
            points.iter().map(|&(topo, _, _)| topo).collect::<Vec<_>>(),
            "the interior topo gap is preserved for the renderer to draw",
        );
    }

    #[test]
    fn history_chart_base_ref_is_the_analyzed_context_beyond_the_last_observation() {
        // When analysis reaches commits newer than the last observation, the
        // trailing-fill target is that context commit, so the chart shows the lag as a
        // trailing gap — the visual form of the "lagged history" warning.
        let series = series_of(&step_values(100.0, 130.0));
        let context = AnalysisContext {
            mode: AnalysisMode::History,
            merge_base_index: None,
            base_ref_index: None,
            tip_index: 20,
        };
        let finding = only(find_changes(slice::from_ref(&series), &context).findings);
        assert_eq!(finding.chart_base_ref, Some(20));
    }

    fn step_before_blessing() -> Series {
        // One extra pre-step point makes this analytically conclusive while retaining
        // a shortest judged post-blessing window. Calibration is not the property here.
        let mut values = vec![100.0; MIN_REGIME + 1];
        values.extend(std::iter::repeat_n(130.0, MIN_SERIES_POINTS));
        series_of(&values)
    }

    #[test]
    fn history_reports_the_step_before_it_is_blessed() {
        assert_eq!(only(changes(&[step_before_blessing()])).latest, 130.0);
    }

    #[test]
    fn history_does_not_reflag_a_blessed_step() {
        // Blessing the post-step level re-baselines the series: the active window
        // begins at the first elevated point, leaving a full-length but flat 130
        // regime to judge, which no longer moves.
        let mut blessed = step_before_blessing();
        blessed.active_start = MIN_REGIME + 1;
        blessed.blessing = Some(Blessing {
            commit: "abcdef0123456789".to_owned(),
            commit_time: Some(Timestamp::from_second(3).unwrap()),
        });
        judged_but_silent(&[blessed]);
    }

    #[test]
    fn history_stamps_blessing_provenance_on_a_finding() {
        // Pre-blessing history (100) is retained for charting but excluded from
        // detection; a real step *after* the blessed baseline (130 -> 160) still
        // flags, and the finding carries the blessing provenance and full series.
        let values = three_regimes(100.0, 130.0, 160.0);
        let mut series = series_of(&values);
        series.active_start = MIN_REGIME;
        series.blessing = Some(Blessing {
            commit: "abcdef0123456789cafe".to_owned(),
            commit_time: Some(Timestamp::from_second(3).unwrap()),
        });
        let finding = only(changes(&[series]));
        assert_eq!(finding.baseline, 130.0);
        assert_eq!(finding.latest, 160.0);
        // The full series, including the pre-blessing prefix, is restored for
        // charting...
        assert_eq!(finding.series.len(), values.len());
        // ...and the blessing provenance is recorded.
        assert_eq!(finding.blessed_at.as_deref(), Some("abcdef012345"));
        assert_eq!(
            finding.blessed_commit_time.as_deref(),
            Some("1970-01-01T00:00:03Z")
        );
    }

    #[test]
    fn drift_at_the_practical_floor_is_flagged_with_a_real_p_value() {
        // A steady climb whose relative drift is exactly PRACTICAL_RELATIVE (3%): the
        // floor gate must be a strict `<`, not a `<=`. Its p-value is above 0, so a
        // mutated `-p` / `p - 1` would clamp to 0. The nine-unit rise over a baseline
        // of 300 lands the relative move on the floor exactly.
        let series = series_of(&ramp(300.0, 1.0, MIN_SERIES_POINTS));
        let candidate =
            evaluate_drift(&series, &values_of(&series), &mut GateLog::disabled()).unwrap();
        assert_eq!(candidate.finding.method, FindingMethod::Drift);
        assert_eq!(candidate.finding.relative_delta, PRACTICAL_RELATIVE);
        assert!(candidate.bh_p > 0.0);
    }

    #[test]
    fn drift_significance_is_measured_against_the_fixed_alpha() {
        // The Mann–Kendall gate tests the trend p-value against MAX_DRIFT_CHANCE_LEVEL. A clean
        // ramp clears it decisively; the recorded gate pins both the threshold and that
        // the trend sits below it.
        let series = series_of(&ramp(100.0, 4.0, MIN_SERIES_POINTS));
        let mut log = GateLog::recording();
        assert!(
            evaluate_drift(&series, &values_of(&series), &mut log).is_some(),
            "the ramp must report drift"
        );
        let (p, threshold) =
            gate_value(&log, Gate::Significance).expect("the significance gate ran");
        assert_eq!(threshold, MAX_DRIFT_CHANCE_LEVEL);
        assert!(p < MAX_DRIFT_CHANCE_LEVEL, "{p}");
    }

    #[test]
    fn drift_below_the_absolute_floor_is_suppressed() {
        // An upward drift on a quantized metric that gains one count every second
        // commit, totalling only 4.5 counts across the fitted line. Its relative move
        // (4.5%) clears the relative floor and the trend is significant, but 4.5 counts
        // is under the five-count floor below which a count move is not worth acting on,
        // so nothing is reported and the recorded chain pins the absolute floor as the
        // reason.
        let series = series_of(&staircase(100.0, MIN_SERIES_POINTS));
        let mut log = GateLog::recording();
        assert!(evaluate_drift(&series, &values_of(&series), &mut log).is_none());
        assert_eq!(
            log.declined_by_stage(GateStage::Drift),
            Some(Gate::AbsoluteFloor)
        );
    }

    #[test]
    fn drift_at_the_absolute_floor_is_flagged() {
        // One more commit on the same staircase carries the fitted line to exactly 5
        // counts, which clears the absolute floor and is flagged, pinning the gate's
        // `>=` boundary.
        let series = series_of(&staircase(100.0, MIN_SERIES_POINTS + 1));
        let candidate =
            evaluate_drift(&series, &values_of(&series), &mut GateLog::disabled()).unwrap();
        assert_eq!(candidate.finding.method, FindingMethod::Drift);
        assert_eq!(candidate.finding.delta, 5.0);
    }

    #[test]
    fn drift_noise_band_is_the_half_width_times_the_fixed_multiple() {
        // A total movement of 36 against a confidence half-width of 20. The move sits
        // between one and two half-widths, so only the noise band decides. The band is
        // the half-width times DRIFT_NOISE_MULTIPLE (40), so the move is withheld, and
        // the recorded threshold pins the product (a `+` mutant would lower it to 22).
        let series = wall_series(&ramp(100.0, 4.0, MIN_SERIES_POINTS), 20.0);
        let mut log = GateLog::recording();
        assert!(evaluate_drift(&series, &values_of(&series), &mut log).is_none());
        assert_eq!(log.declined_by(), Some(Gate::IntervalNoiseBand));
        assert_eq!(
            gate_value(&log, Gate::IntervalNoiseBand),
            Some((36.0, 40.0))
        );
    }

    #[test]
    fn drift_within_its_own_residual_scatter_is_suppressed() {
        // A climbing zigzag whose net rise is real — Mann-Kendall significant even after
        // the two-detector factor, and clear of the relative and absolute floors — but
        // which swings a fixed four units either side of its Theil-Sen line. The
        // eleven-unit fitted move does not exceed RESIDUAL_NOISE_MULTIPLE times that
        // four-unit scatter, so the trend is buried in its own residual and refused by the
        // residual gate.
        let series = series_of(&[
            96.0, 105.0, 98.0, 107.0, 100.0, 109.0, 102.0, 111.0, 104.0, 113.0, 106.0, 115.0,
        ]);
        let mut log = GateLog::recording();
        assert!(evaluate_drift(&series, &values_of(&series), &mut log).is_none());
        assert_eq!(
            log.declined_by_stage(GateStage::Drift),
            Some(Gate::ResidualNoise)
        );
    }

    #[test]
    fn drift_needs_at_least_the_minimum_points() {
        // The length gate is `n < DRIFT_MIN_POINTS`: a series one point short is
        // rejected outright, while a series of exactly that length is still evaluated
        // (so a gate mutated to reject the longer series instead is caught).
        let short = series_of(&ramp(100.0, 4.0, DRIFT_MIN_POINTS - 1));
        assert!(evaluate_drift(&short, &values_of(&short), &mut GateLog::disabled()).is_none());
        let long = series_of(&ramp(100.0, 4.0, DRIFT_MIN_POINTS));
        assert!(evaluate_drift(&long, &values_of(&long), &mut GateLog::disabled()).is_some());
    }

    #[test]
    fn analysis_mode_wire_names() {
        assert_eq!(AnalysisMode::History.as_str(), "history");
        assert_eq!(AnalysisMode::Branch.as_str(), "branch");
    }

    #[test]
    fn history_reports_regressions_only() {
        let context = AnalysisContext {
            mode: AnalysisMode::History,
            merge_base_index: None,
            base_ref_index: None,
            tip_index: 0,
        };
        // A drift watch over the base branch is one-directional: improvement over time
        // is the expected background there, so only a worsening is a finding.
        assert!(context.keeps(Direction::Regression));
        assert!(!context.keeps(Direction::Improvement));
    }

    #[test]
    fn reports_improvements_reflects_the_mode() {
        let context = |mode| AnalysisContext {
            mode,
            merge_base_index: None,
            base_ref_index: None,
            tip_index: 0,
        };
        // History is the regressions-only drift watch; branch compares both
        // directions.
        assert!(!context(AnalysisMode::History).reports_improvements());
        assert!(context(AnalysisMode::Branch).reports_improvements());
    }

    /// The judged family the direction-order case is corrected against. Ten makes the
    /// first two Benjamini–Hochberg thresholds `TARGET_FALSE_DISCOVERY_RATE / 10` and `TARGET_FALSE_DISCOVERY_RATE / 5`, far enough
    /// apart to seat a p-value strictly between them.
    const DIRECTION_ORDER_FAMILY: usize = 10;

    /// The improvement's p-value, as a fraction of `TARGET_FALSE_DISCOVERY_RATE`. Well under the rank-1
    /// threshold (`TARGET_FALSE_DISCOVERY_RATE / 10`), so it is rejected under either order and always takes
    /// rank 1 away from the regression.
    const DIRECTION_ORDER_IMPROVEMENT_P: f64 = 0.01;

    /// The regression's p-value, as a fraction of `TARGET_FALSE_DISCOVERY_RATE`. Chosen to sit strictly
    /// between the rank-1 threshold (`TARGET_FALSE_DISCOVERY_RATE / 10`) and the rank-2 one (`TARGET_FALSE_DISCOVERY_RATE / 5`), so
    /// its survival depends entirely on which rank it lands at — which is exactly what
    /// the screening order decides.
    const DIRECTION_ORDER_REGRESSION_P: f64 = 0.15;

    /// Builds a candidate carrying `direction` and `bh_p`, drawn from `source`.
    ///
    /// `comparison_base_index` is the branch-mode chart anchor, left `None` for history
    /// mode. Everything the false-discovery filter does not read is left at a neutral
    /// value: the filter arbitrates on `bh_p` alone, and the surviving finding's
    /// charting points are materialised from `source` afterwards.
    fn direction_order_candidate(
        source: &Series,
        source_index: usize,
        direction: Direction,
        bh_p: f64,
        comparison_base_index: Option<usize>,
    ) -> Candidate {
        Candidate {
            finding: Finding {
                set: source.set.clone(),
                id: source.id.clone(),
                kind: source.kind,
                method: FindingMethod::ChangePoint,
                direction,
                baseline: 100.0,
                latest: 110.0,
                delta: 10.0,
                relative_delta: 0.1,
                commit: None,
                window_start_commit: None,
                blessed_at: None,
                blessed_commit_time: None,
                series: Vec::new(),
                comparison_base_index,
                chart_base_ref: None,
                branch: None,
            },
            source_index,
            bh_p,
            split: None,
            line: None,
        }
    }

    /// The mode's direction screen runs *before* the false-discovery correction, so a
    /// regression is corrected against the ranks its own direction occupies rather than
    /// borrowing an earlier one from an improvement the report would never show.
    ///
    /// The two candidates are sized so the orders disagree: correcting both directions
    /// seats the regression at rank 2, where it clears the looser threshold, while
    /// screening first leaves it alone at rank 1, where it does not. History mode must
    /// therefore report nothing, and branch mode — which reports both directions, so the
    /// screen is a no-op — must report both.
    #[test]
    fn history_screens_direction_before_the_correction() {
        let improvement_p = TARGET_FALSE_DISCOVERY_RATE * DIRECTION_ORDER_IMPROVEMENT_P;
        let regression_p = TARGET_FALSE_DISCOVERY_RATE * DIRECTION_ORDER_REGRESSION_P;

        // Bind the case to the thresholds it is built around, so a moved gate fails here
        // rather than silently leaving the two orders in agreement.
        let family = count_to_f64(DIRECTION_ORDER_FAMILY);
        assert!(improvement_p < TARGET_FALSE_DISCOVERY_RATE / family);
        assert!(regression_p > TARGET_FALSE_DISCOVERY_RATE / family);
        assert!(regression_p < TARGET_FALSE_DISCOVERY_RATE * 2.0 / family);

        let series = vec![
            named_series("improving", &[100.0, 70.0]),
            named_series("regressing", &[100.0, 110.0]),
        ];
        let mut census = SeriesCensus::default();
        for _ in 0..DIRECTION_ORDER_FAMILY {
            census.record(Testability::Judged);
        }

        let candidates = |comparison_base_index| {
            vec![
                direction_order_candidate(
                    &series[0],
                    0,
                    Direction::Improvement,
                    improvement_p,
                    comparison_base_index,
                ),
                direction_order_candidate(
                    &series[1],
                    1,
                    Direction::Regression,
                    regression_p,
                    comparison_base_index,
                ),
            ]
        };

        // History charts against the analyzed tip; branch charts against the base ref,
        // so each leg supplies the anchors its own charting path reads.
        let history_context = AnalysisContext {
            mode: AnalysisMode::History,
            merge_base_index: None,
            base_ref_index: None,
            tip_index: 1,
        };
        let branch_context = AnalysisContext {
            mode: AnalysisMode::Branch,
            merge_base_index: Some(1),
            base_ref_index: Some(1),
            tip_index: 1,
        };

        let history = finalize_findings(candidates(None), &census, &series, &history_context);
        assert!(
            history.is_empty(),
            "the regression alone sits at rank 1, where it does not clear the bar: {history:?}"
        );

        let branch = finalize_findings(candidates(Some(1)), &census, &series, &branch_context);
        assert_eq!(branch.len(), 2, "{branch:?}");
    }

    #[test]
    fn relative_delta_against_a_zero_baseline_is_a_full_magnitude_move() {
        // A move away from a (near-)zero baseline is proportionally unbounded, so its
        // sign is returned at full magnitude to rank as a major change.
        assert_eq!(relative_delta_of(5.0, 0.0), 1.0);
        assert_eq!(relative_delta_of(-5.0, 0.0), -1.0);
    }

    #[test]
    fn testability_agrees_with_detection_in_history_mode() {
        // Detection and family membership share one definition, so a series that is
        // not judged raises nothing and a judged one is evaluated on its merits. The
        // census reports the same verdict, so a report can never claim to have judged
        // a series the detectors declined.
        let mut short_values = vec![100.0; MIN_REGIME];
        short_values.extend(std::iter::repeat_n(130.0, MIN_REGIME - 1));
        let short = series_of(&short_values);
        let context = history_context(slice::from_ref(&short));
        assert_eq!(
            testability(&short, &context),
            Testability::Unjudged(UnjudgedReason::TooFewPoints)
        );
        let detection = find_changes(slice::from_ref(&short), &context);
        assert!(detection.findings.is_empty());
        assert_eq!(detection.census.judged(), 0);
        assert_eq!(
            detection.census.reasons().collect::<Vec<_>>(),
            vec![(UnjudgedReason::TooFewPoints, 1)]
        );

        let long = series_of(&step_values(100.0, 130.0));
        let context = history_context(slice::from_ref(&long));
        assert_eq!(testability(&long, &context), Testability::Judged);
        let detection = find_changes(slice::from_ref(&long), &context);
        assert_eq!(detection.findings.len(), 1);
        assert_eq!(detection.census.judged(), 1);
        assert_eq!(detection.census.unjudged(), 0);
    }

    #[test]
    fn branch_testability_requires_the_tip_and_the_minimum_base_window() {
        let values = vec![100.0; MIN_SERIES_POINTS.saturating_add(1)];
        let mut branch =
            examples::with_base_window(series_of(&values), MIN_SERIES_POINTS.saturating_sub(1));
        let context = examples::branch_context(&branch, MIN_SERIES_POINTS.saturating_sub(1));
        assert_eq!(testability(&branch, &context), Testability::Judged);

        branch.points.last_mut().unwrap().topo_index = context.tip_index.saturating_add(1);
        assert_eq!(
            testability(&branch, &context),
            Testability::Unjudged(UnjudgedReason::NotMeasuredOnBranch)
        );

        branch.points.last_mut().unwrap().topo_index = context.tip_index;
        branch.base_window.pop();
        assert_eq!(
            testability(&branch, &context),
            Testability::Unjudged(UnjudgedReason::TooFewBaseCommits)
        );
        branch.blessing = Some(Blessing {
            commit: "blessed".to_owned(),
            commit_time: None,
        });
        assert_eq!(
            testability(&branch, &context),
            Testability::Unjudged(UnjudgedReason::TooFewBaseCommitsSinceBlessing)
        );

        // The shortest searchable base leaves a repeated but incomplete trailing regime.
        let mut unresolved_values = vec![100.0; 16];
        unresolved_values.extend(std::iter::repeat_n(200.0, 4));
        unresolved_values.push(220.0);
        let unresolved_branch = examples::with_base_window(series_of(&unresolved_values), 19);
        let unresolved_context = examples::branch_context(&unresolved_branch, 19);
        assert_eq!(
            testability(&unresolved_branch, &unresolved_context),
            Testability::Unjudged(UnjudgedReason::CurrentBaseRegimeUnresolved)
        );
        let detection = find_changes(slice::from_ref(&unresolved_branch), &unresolved_context);
        assert_eq!(detection.census.judged(), 0);
        assert_eq!(detection.census.unjudged(), 1);
        assert_eq!(
            detection.branch_trace.series[0].unresolved,
            Some(UnjudgedReason::CurrentBaseRegimeUnresolved)
        );
    }

    #[test]
    fn branch_findings_materialize_the_base_window_and_tip_chart() {
        // Chart materialization needs a judged base window, not regime-selection evidence.
        let mut values = vec![100.0; MIN_SERIES_POINTS];
        values.push(130.0);
        let mut source = series_of(&values);
        source.points.last_mut().unwrap().dirty = true;
        let branch = examples::with_base_window(source, MIN_SERIES_POINTS - 1);
        let context = examples::branch_context(&branch, MIN_SERIES_POINTS - 1);
        let finding = only(find_changes(std::slice::from_ref(&branch), &context).findings);

        assert!(finding.is_regression());
        assert_eq!(finding.series.len(), MIN_SERIES_POINTS + 1);
        assert_eq!(finding.series.first().unwrap().value, 100.0);
        assert_eq!(finding.series.last().unwrap().value, 130.0);
        assert_eq!(finding.series.last().unwrap().topo_index, MIN_SERIES_POINTS);
        assert!(finding.series.last().unwrap().dirty);
        assert_eq!(finding.chart_base_ref, None);

        let mut improvement = finding;
        improvement.direction = Direction::Improvement;
        assert!(!improvement.is_regression());
    }

    #[test]
    fn a_blessing_that_leaves_too_little_evidence_is_accounted_for_separately() {
        // A blessing re-baselines the series, so only the points after it are evidence.
        // A recent blessing can therefore blind a long series, which is a different
        // (and fixable) situation from a series that is simply new — so the census
        // distinguishes the two rather than lumping both under "too few points".
        let mut series = series_of(&[100.0; MIN_SERIES_POINTS * 2]);
        series.active_start = MIN_SERIES_POINTS.saturating_add(1);
        series.blessing = Some(Blessing {
            commit: "c".repeat(40),
            commit_time: None,
        });
        let context = history_context(slice::from_ref(&series));
        assert_eq!(
            testability(&series, &context),
            Testability::Unjudged(UnjudgedReason::TooFewPointsSinceBlessing)
        );

        // The same series blessed at its very start keeps every point as evidence, so
        // it is judged: it is the truncation that blinds, not the blessing.
        let mut blessed_at_start = series;
        blessed_at_start.active_start = 0;
        let context = history_context(slice::from_ref(&blessed_at_start));
        assert_eq!(
            testability(&blessed_at_start, &context),
            Testability::Judged
        );
    }

    #[test]
    fn the_census_accounts_for_every_series_exactly_once() {
        // The census is only readable as coverage if it is total: whatever mix of
        // judged and unjudged series a pass sees, the tallies must add back up to the
        // series it was handed.
        let mut blessed = series_of(&[100.0; MIN_SERIES_POINTS]);
        blessed.active_start = 1;
        let batch = vec![
            named_series("judged", &step_values(100.0, 130.0)),
            named_series("silent", &[100.0; MIN_SERIES_POINTS]),
            named_series("short", &[100.0; MIN_SERIES_POINTS - 1]),
            named_series("shorter", &[100.0; 1]),
            blessed,
        ];
        let census = find_changes(&batch, &history_context(&batch)).census;
        assert_eq!(census.judged(), 2);
        assert_eq!(census.unjudged(), 3);
        assert_eq!(census.total(), batch.len());
        assert_eq!(
            census.reasons().collect::<Vec<_>>(),
            vec![
                (UnjudgedReason::TooFewPoints, 2),
                (UnjudgedReason::TooFewPointsSinceBlessing, 1),
            ],
            "the breakdown is ordered and sums to the unjudged total"
        );
    }

    #[test]
    fn a_census_ignores_an_empty_tally() {
        // Stages that record in bulk pass whatever count they dropped, so a zero
        // must leave no trace of a reason that accounts for nothing.
        let mut census = SeriesCensus::default();
        census.record(Testability::Judged);
        census.record_unjudged(UnjudgedReason::Ghost, 3);
        census.record_unjudged(UnjudgedReason::NotMeasuredOnBranch, 0);

        assert_eq!(census.judged(), 1);
        assert_eq!(census.total(), 4);
        assert_eq!(
            census.reasons().collect::<Vec<_>>(),
            vec![(UnjudgedReason::Ghost, 3)]
        );
    }

    #[test]
    fn a_verdict_calls_itself_judged_only_when_it_is_one() {
        // Detection and the census both branch on this, so an always-true answer
        // would run the detectors on series the census reports as never tested.
        assert!(Testability::Judged.is_judged());
        for reason in UnjudgedReason::ALL {
            assert!(
                !Testability::Unjudged(reason).is_judged(),
                "{reason:?} is not a verdict"
            );
        }
    }

    #[test]
    fn every_unjudged_reason_has_a_distinct_wire_name_and_phrase() {
        // Both renderings of a reason are contracts: the wire name is read by
        // automation and the phrase by a human, so neither may collide.
        let names: Vec<&str> = UnjudgedReason::ALL
            .iter()
            .map(|reason| reason.as_str())
            .collect();
        let phrases: Vec<&str> = UnjudgedReason::ALL
            .iter()
            .map(|reason| reason.describe())
            .collect();
        for (index, name) in names.iter().enumerate() {
            assert!(
                !names[index + 1..].contains(name),
                "duplicate wire name {name}"
            );
        }
        for (index, phrase) in phrases.iter().enumerate() {
            assert!(
                !phrases[index + 1..].contains(phrase),
                "duplicate phrase {phrase}"
            );
        }

        // The declaration order is the reporting order, and a `BTreeMap` census keyed
        // by the derived `Ord` must therefore iterate in the same order.
        let mut census = SeriesCensus::default();
        for reason in UnjudgedReason::ALL.iter().rev() {
            census.record_unjudged(*reason, 1);
        }
        assert_eq!(
            census
                .reasons()
                .map(|(reason, _)| reason)
                .collect::<Vec<_>>(),
            UnjudgedReason::ALL.to_vec()
        );
    }

    fn false_discovery_family(size: usize, companion_points: usize) -> Vec<Series> {
        // The correction divides by the number of hypotheses *tested*, which is the
        // number of testable series — including those that raised nothing. Feeding it
        // only its own survivors would make it a no-op, since every survivor has
        // already cleared `change_alpha`.
        //
        // The stepped series is real but modest, and its selection-adjusted
        // chance level falls on opposite sides of the Benjamini-Hochberg
        // decisions for the family sizes below. Every batch raises exactly the
        // one candidate; the only input that differs is whether the silent
        // companions join the family (and therefore its calibration budget and
        // BH denominator). The permutation component's 90% Bonferroni weight is
        // already reflected in that chance level. Flat companions are judged and
        // do count, while companions one point too short are not judged and do not.
        let stepped = named_series(
            "stepped",
            &[
                98.0, 100.0, 102.0, 99.0, 101.0, 128.0, 130.0, 132.0, 129.0, 131.0,
            ],
        );
        let mut family = vec![stepped];
        family
            .extend((1..size).map(|index| {
                named_series(&format!("flat{index}"), &vec![100.0; companion_points])
            }));
        family
    }

    #[test]
    fn false_discovery_family_reports_below_the_family_boundary() {
        // This family places the step just below the BH boundary.
        const FAMILY_THAT_REPORTS: usize = 6;
        let family = false_discovery_family(FAMILY_THAT_REPORTS, MIN_SERIES_POINTS);
        assert_eq!(
            only(changes(&family)).id.qualified(),
            "stepped/case",
            "the candidate clears the threshold this family size sets"
        );
    }

    #[test]
    fn false_discovery_family_includes_silent_testable_companions() {
        // One more judged companion moves the step across the BH boundary.
        const FAMILY_THAT_REJECTS: usize = 7;
        let family = false_discovery_family(FAMILY_THAT_REJECTS, MIN_SERIES_POINTS);
        assert!(
            changes(&family).is_empty(),
            "one more silent but testable companion tightens the threshold past it"
        );
    }

    #[test]
    fn false_discovery_family_excludes_unjudged_companions() {
        // This batch would reject if its short companions were incorrectly judged.
        const FAMILY_THAT_REJECTS: usize = 7;
        let family = false_discovery_family(FAMILY_THAT_REJECTS, MIN_SERIES_POINTS - 1);
        assert_eq!(
            only(changes(&family)).id.qualified(),
            "stepped/case",
            "companions that were never judged must not enlarge the family"
        );
    }

    /// One gate-observation scenario: a history whose evaluation a named gate ends.
    ///
    /// The catalogue holds one scenario per gate family, so the table doubles as an
    /// inventory of the shape of history each gate exists to decline.
    struct DeclinedCase {
        /// How the history reads, quoted back by a failing assertion.
        shape: &'static str,
        /// The series the detectors judge.
        series: Series,
        /// Which detector's chain the scenario makes its claim about. Every history
        /// detector runs on every series, so naming the chain is part of the claim.
        stage: GateStage,
        /// The gate expected to end that chain.
        gate: Gate,
        /// The `(value, threshold)` the gate compared, where both are worth pinning by
        /// hand. `None` where the gate carries no numbers, or where its numbers are a
        /// rank statistic no reader would recompute from the series by eye.
        compared: Option<(f64, f64)>,
    }

    /// The history-mode context a gate-observation scenario runs under.
    fn observed_context(series: &Series) -> AnalysisContext {
        history_context(slice::from_ref(series))
    }

    /// Builds only the gate scenario the test evaluates, not the whole catalogue.
    fn declined_case(gate: Gate) -> DeclinedCase {
        match gate {
            Gate::MinRegime => DeclinedCase {
                shape: "a step with one point too few after it",
                series: series_of(&{
                    let mut short_after = vec![100.0; MIN_SERIES_POINTS];
                    short_after.extend(std::iter::repeat_n(130.0, MIN_REGIME - 1));
                    short_after
                }),
                stage: GateStage::ChangePoint,
                gate: Gate::MinRegime,
                // The shorter regime holds `MIN_REGIME - 1` points against the floor.
                compared: Some((count_to_f64(MIN_REGIME - 1), count_to_f64(MIN_REGIME))),
            },
            Gate::NonZeroDelta => DeclinedCase {
                shape: "a flat history with one excursion at its midpoint",
                series: series_of(&{
                    let mut values = [100.0; MIN_SERIES_POINTS];
                    // The excursion splits the history into two regimes of equal length
                    // and identical level, which is the only way a located split can
                    // carry no move at all.
                    values[MIN_REGIME] = 200.0;
                    values
                }),
                stage: GateStage::ChangePoint,
                gate: Gate::NonZeroDelta,
                // Both regimes sit at the same level, so the move is exactly nothing.
                compared: Some((0.0, 0.0)),
            },
            Gate::RelativeFloor => DeclinedCase {
                shape: "a one percent step",
                series: series_of(&step_values(100.0, 101.0)),
                stage: GateStage::ChangePoint,
                gate: Gate::RelativeFloor,
                // 1 unit on a baseline of 100 against the 3% relative floor.
                compared: Some((0.01, PRACTICAL_RELATIVE)),
            },
            Gate::AbsoluteFloor => DeclinedCase {
                shape: "a four-count step",
                series: series_of(&step_values(60.0, 64.0)),
                stage: GateStage::ChangePoint,
                gate: Gate::AbsoluteFloor,
                // 4 instruction counts against the metric's five-count floor; the same
                // move clears the relative floor, so this is the gate that binds.
                compared: Some((4.0, PRACTICAL_ABSOLUTE_COUNT)),
            },
            Gate::ResidualNoise => DeclinedCase {
                shape: "a step no larger than its own residual scatter",
                // Two rank-separated regimes, so the rank test is decisive and the chain
                // reaches the residual gate. Each regime descends internally, which cancels
                // any Theil-Sen slope so the drift detector reads no trend and the series is
                // silent overall rather than reported as drift.
                series: series_of(&[
                    1000.0, 1000.0, 950.0, 900.0, 900.0, 1100.0, 1100.0, 1050.0, 1001.0, 1001.0,
                ]),
                stage: GateStage::ChangePoint,
                gate: Gate::ResidualNoise,
                // A 100-unit move against three times the 50-unit median absolute residual
                // the same two-regime model leaves behind.
                compared: Some((100.0, RESIDUAL_NOISE_MULTIPLE * 50.0)),
            },
            Gate::RegimeSeparation => DeclinedCase {
                shape: "two levels that both recur in each half of the history",
                series: series_of(&{
                    // Each half is complete, but its minority cannot form a regime.
                    const MINORITY_POINTS: usize = MIN_REGIME.div_euclid(2);
                    const MAJORITY_POINTS: usize = MIN_SERIES_POINTS - MINORITY_POINTS;

                    // The majority shifts while overlap limits superiority. Ordering
                    // each half high-then-low also prevents a surviving drift.
                    let mut values = Vec::new();
                    values.extend(std::iter::repeat_n(130.0, MINORITY_POINTS));
                    values.extend(std::iter::repeat_n(100.0, MAJORITY_POINTS));
                    values.extend(std::iter::repeat_n(130.0, MAJORITY_POINTS));
                    values.extend(std::iter::repeat_n(100.0, MINORITY_POINTS));
                    values
                }),
                stage: GateStage::ChangePoint,
                gate: Gate::RegimeSeparation,
                compared: None,
            },
            Gate::IntervalDisjoint => DeclinedCase {
                shape: "a clean step under confidence intervals that overlap",
                series: wall_series(
                    &[
                        98.0, 100.0, 102.0, 99.0, 101.0, 128.0, 130.0, 132.0, 129.0, 131.0,
                    ],
                    60.0,
                ),
                stage: GateStage::ChangePoint,
                gate: Gate::IntervalDisjoint,
                compared: None,
            },
            Gate::Significance => DeclinedCase {
                shape: "a flat history, judged as a trend",
                series: series_of(&[100.0; MIN_SERIES_POINTS]),
                stage: GateStage::Drift,
                gate: Gate::Significance,
                // No pair of points is ordered, so the rank test reports no trend at all.
                compared: Some((1.0, MAX_DRIFT_CHANCE_LEVEL)),
            },
            Gate::IntervalNoiseBand => DeclinedCase {
                shape: "a climb smaller than the measurement noise band",
                series: wall_series(&ramp(100.0, 4.0, MIN_SERIES_POINTS), 20.0),
                stage: GateStage::Drift,
                gate: Gate::IntervalNoiseBand,
                // A fitted movement of 4 units per point across ten points, against
                // twice the 20-unit half-width every point carries.
                compared: Some((36.0, DRIFT_NOISE_MULTIPLE * 20.0)),
            },
            _ => panic!("the requested gate has no declining scenario"),
        }
    }

    fn assert_declined_case(gate: Gate) {
        // The log's whole purpose is to name the gate that ended an evaluation, so every
        // gate family needs a history it is the one to decline — and the reason recorded
        // has to be the reason that actually applied, not merely a plausible one.
        let case = declined_case(gate);
        let context = observed_context(&case.series);
        let (finding, log) = evaluate_with_log(&case.series, &context);
        assert!(
            finding.is_none(),
            "{}: expected silence, got {finding:?}",
            case.shape
        );
        assert_eq!(
            log.declined_by_stage(case.stage),
            Some(case.gate),
            "{}: the {} chain declined for the wrong reason",
            case.shape,
            case.stage.label()
        );

        // A logged value a reader cannot reproduce from the series is worse than no log
        // at all, so each scenario pins the arithmetic its gate performed.
        if let Some((value, threshold)) = case.compared {
            let outcome = log
                .entries()
                .iter()
                .find(|entry| entry.stage == case.stage && entry.gate == case.gate)
                .unwrap_or_else(|| panic!("{}: the gate never ran", case.shape));
            assert!(!outcome.passed, "{}", case.shape);
            assert_eq!(outcome.value, Some(value), "{}", case.shape);
            assert_eq!(outcome.threshold, Some(threshold), "{}", case.shape);
        }
        // Gates short-circuit, so a log is a prefix of the chain rather than a survey of
        // it: everything before the declining gate passed and nothing after it ran. A
        // reader who does not know this would misread a missing gate as a passing one.
        let chain: Vec<&GateOutcome> = log
            .entries()
            .iter()
            .filter(|entry| entry.stage == case.stage)
            .collect();
        let (last, earlier) = chain.split_last().unwrap();
        assert_eq!(last.gate, case.gate, "{}", case.shape);
        assert!(!last.passed, "{}", case.shape);
        assert!(
            earlier.iter().all(|entry| entry.passed),
            "{}: a gate before the declining one is recorded as failing",
            case.shape
        );

        // Recording must neither change the verdict nor accumulate entries when disabled.
        // Reuse this scenario's observed verdict instead of evaluating it again per assertion.
        let mut disabled = GateLog::disabled();
        let batch = slice::from_ref(&case.series);
        let unobserved = detect_range(batch, 0..batch.len(), &context, 1, &mut disabled);
        assert!(unobserved.is_empty(), "{}", case.shape);
        assert!(disabled.entries().is_empty(), "{}", case.shape);
        assert_eq!(disabled.declined_by(), None, "{}", case.shape);
    }

    #[test]
    fn declined_minimum_regime_preserves_the_verdict_and_log() {
        assert_declined_case(Gate::MinRegime);
    }

    #[test]
    fn declined_zero_delta_preserves_the_verdict_and_log() {
        assert_declined_case(Gate::NonZeroDelta);
    }

    #[test]
    fn declined_relative_floor_preserves_the_verdict_and_log() {
        assert_declined_case(Gate::RelativeFloor);
    }

    #[test]
    fn declined_absolute_floor_preserves_the_verdict_and_log() {
        assert_declined_case(Gate::AbsoluteFloor);
    }

    #[test]
    fn declined_residual_noise_preserves_the_verdict_and_log() {
        assert_declined_case(Gate::ResidualNoise);
    }

    #[test]
    fn declined_regime_separation_preserves_the_verdict_and_log() {
        assert_declined_case(Gate::RegimeSeparation);
    }

    #[test]
    fn declined_interval_overlap_preserves_the_verdict_and_log() {
        assert_declined_case(Gate::IntervalDisjoint);
    }

    #[test]
    fn declined_drift_significance_preserves_the_verdict_and_log() {
        assert_declined_case(Gate::Significance);
    }

    #[test]
    fn declined_interval_noise_preserves_the_verdict_and_log() {
        assert_declined_case(Gate::IntervalNoiseBand);
    }

    #[test]
    fn a_reported_change_point_passes_every_gate_in_its_chain() {
        // The complement of the declining scenarios: when a finding is reported, its
        // chain must show the whole gauntlet cleared, in the order the engine applies
        // it. A series without dispersion gives the interval gate nothing to judge, so
        // it abstains rather than recording a verdict it did not reach.
        let series = series_of(&step_values(100.0, 130.0));
        let (finding, log) = evaluate_with_log(&series, &observed_context(&series));
        assert_eq!(finding.unwrap().method, FindingMethod::ChangePoint);
        assert_eq!(log.declined_by_stage(GateStage::ChangePoint), None);
        let chain: Vec<Gate> = log
            .entries()
            .iter()
            .filter(|entry| entry.stage == GateStage::ChangePoint)
            .map(|entry| entry.gate)
            .collect();
        assert_eq!(
            chain,
            vec![
                Gate::SplitLocated,
                Gate::MinRegime,
                Gate::NonZeroDelta,
                Gate::RelativeFloor,
                Gate::AbsoluteFloor,
                Gate::ResidualNoise,
                Gate::RegimeSeparation,
                Gate::SelectionAdjustment,
                Gate::Significance,
            ]
        );
        assert!(log.entries().iter().all(|entry| entry.passed));
    }

    /// The log observes detection without participating in the verdict.
    fn assert_recording_preserves_verdict(shape: &str, series: &Series) {
        let context = observed_context(series);
        let unobserved = find_changes(slice::from_ref(series), &context).findings;
        let (observed, _) = evaluate_with_log(series, &context);
        // `Finding` carries no equality contract, so compare the whole rendering
        // rather than a hand-picked subset of fields that could hide a difference.
        assert_eq!(
            format!("{:?}", unobserved.first()),
            format!("{observed:?}"),
            "{shape}"
        );
    }

    #[test]
    fn recording_the_gates_does_not_change_a_reported_step() {
        assert_recording_preserves_verdict("a clean step", &series_of(&step_values(100.0, 130.0)));
    }

    #[test]
    fn recording_the_gates_does_not_change_a_reported_drift() {
        assert_recording_preserves_verdict(
            "a steady climb",
            &series_of(&ramp(100.0, 4.0, MIN_SERIES_POINTS)),
        );
    }

    /// The named example series, paired with the verdict its documentation claims.
    ///
    /// The examples are the vocabulary the documentation figures are drawn from, so a
    /// documented verdict that no longer holds is a documentation defect this catches.
    fn example_verdicts() -> Vec<ExampleVerdict> {
        vec![
            ExampleVerdict {
                name: "clean_step",
                values: examples::clean_step(),
                reported: Some(FindingMethod::ChangePoint),
            },
            ExampleVerdict {
                name: "slow_ramp",
                values: examples::slow_ramp(),
                reported: Some(FindingMethod::Drift),
            },
            ExampleVerdict {
                name: "blip",
                values: examples::blip(),
                reported: None,
            },
            ExampleVerdict {
                name: "flat_noisy",
                values: examples::flat_noisy(),
                reported: None,
            },
        ]
    }

    /// One named example paired with what history mode is documented to make of it.
    struct ExampleVerdict {
        name: &'static str,
        values: Vec<f64>,
        reported: Option<FindingMethod>,
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "evaluates the complete scattered documentation examples; small detector shapes have separate Miri coverage"
    )]
    fn every_named_example_reaches_the_verdict_it_documents() {
        // The figures and the prose both quote these verdicts, so the series and their
        // documentation are only worth sharing while the engine still agrees with them.
        for case in example_verdicts() {
            let series = timing_series(case.name, &case.values);
            let context = examples::history_context(&series);
            let finding = evaluate_with_log(&series, &context).0;
            assert_eq!(
                finding.as_ref().map(|finding| finding.method),
                case.reported,
                "{} does not reach its documented verdict: {finding:?}",
                case.name
            );
            if case.reported.is_some() {
                assert_eq!(
                    finding.map(|finding| finding.direction),
                    Some(Direction::Regression),
                    "{}",
                    case.name
                );
            }
        }
    }

    /// An example series as a wall-time history whose engine reports no dispersion, so
    /// the only noise in it is the scatter the example itself carries.
    fn timing_series(name: &str, values: &[f64]) -> Series {
        examples::series(name, values, MetricKind::WallTime, 0)
    }

    #[test]
    fn the_shared_example_builder_lays_a_series_out_the_way_detection_expects() {
        // Every documentation figure builds its series through this one function, so its
        // layout is a contract: consecutive topological indices from the requested start,
        // one named commit per point, and no dispersion until a caller asks for it.
        let series = examples::series("demo", &[100.0, 110.0, 120.0], MetricKind::WallTime, 7);
        assert_eq!(series.kind, MetricKind::WallTime);
        assert_eq!(
            series.id.qualified(),
            BenchmarkId::new(nonempty!["demo".to_owned(), "case".to_owned()]).qualified()
        );
        assert_eq!(series.active_start, 0);
        assert!(series.blessing.is_none());
        assert_eq!(
            series
                .points
                .iter()
                .map(|point| point.topo_index)
                .collect::<Vec<_>>(),
            vec![7, 8, 9]
        );
        assert_eq!(series.points[0].object_ordinal, 7);
        assert_eq!(series.points[2].commit.as_deref(), Some("commit9"));
        assert!(series.points.iter().all(|point| !point.dirty
            && point.interval_low.is_none()
            && point.interval_high.is_none()));
        assert_eq!(examples::history_context(&series).tip_index, 9);
    }

    #[test]
    fn attaching_intervals_is_what_lets_an_example_reach_the_interval_gates() {
        // The interval vetoes only run where the engine reports dispersion, so an example
        // that has to demonstrate them must carry intervals — and the width the caller
        // picks is what decides the verdict, which is the whole illustration.
        let bare = examples::series("step", &step_values(100.0, 130.0), MetricKind::WallTime, 0);
        assert!(
            evaluate_with_log(&bare, &examples::history_context(&bare))
                .0
                .is_some()
        );

        let narrow = examples::with_intervals(bare.clone(), 2.0);
        assert_eq!(narrow.points[0].interval_low, Some(98.0));
        assert_eq!(narrow.points[0].interval_high, Some(102.0));
        assert!(
            evaluate_with_log(&narrow, &examples::history_context(&narrow))
                .0
                .is_some()
        );

        let wide = examples::with_intervals(bare, 60.0);
        let (finding, log) = evaluate_with_log(&wide, &examples::history_context(&wide));
        assert!(finding.is_none());
        assert_eq!(
            log.declined_by_stage(GateStage::ChangePoint),
            Some(Gate::IntervalDisjoint)
        );
    }

    #[test]
    fn the_shared_branch_context_judges_the_branch_side_against_the_base() {
        // Branch-mode examples need the same single-call setup history ones have; the
        // merge base is the only thing that distinguishes the two.
        let mut values = vec![100.0; MIN_SERIES_POINTS];
        values.push(130.0);
        let series = examples::with_base_window(
            examples::series("branch", &values, MetricKind::InstructionCount, 0),
            MIN_SERIES_POINTS - 1,
        );
        let context = examples::branch_context(&series, MIN_SERIES_POINTS - 1);
        let finding = only(find_changes(slice::from_ref(&series), &context).findings);
        assert_eq!(finding.direction, Direction::Regression);
        assert_eq!(finding.latest, 130.0);
    }
}
