//! Rendering analysis results into a human- or machine-readable report.
//!
//! Three formats are offered: a compact `text` summary for terminals, a
//! `markdown` document for pasting into pull requests, and a `json` document for
//! programmatic consumption.
//!
//! A report covers one or more *discriminant sets* (engine / triple / machine
//! partitions). The top level carries the project-wide totals and the globally
//! ranked findings, and a per-set breakdown follows so each comparable partition
//! reads as its own section.

use std::collections::HashSet;
use std::num::NonZero;
use std::sync::{Mutex, MutexGuard, PoisonError};

use cbh_detect::{
    AnalysisMode, BranchComparison, BranchExcursion, Direction, Finding, FindingMethod,
    SeriesCensus, short_commit,
};
use cbh_model::{BenchmarkId, DiscriminantSet, MetricKind};
use colored::Colorize;
use rasciigraph::{Config, plot};
use serde::Serialize;

use crate::{AnalysisOutcome, Coverage};

/// Height, in rows, of a finding chart.
const CHART_HEIGHT: u32 = 4;
/// Width, in columns, of a finding chart.
const CHART_WIDTH: u32 = 48;

/// Maximum number of values in a branch-mode chart.
///
/// Branch mode judges the context commit alone against a recent base-ref level, so the
/// context run is the one point that matters. Plotting the whole (often
/// months-long) series would resample it down to [`CHART_WIDTH`] columns, shrinking that
/// context to a single edge column where it reads as noise. The chart starts with the
/// comparison baseline and fills the remaining slots with the recent observed tail,
/// keeping both sides of the reported change visible. The cap stays below
/// [`CHART_WIDTH`] so every value maps to its own column without resampling.
const BRANCH_CHART_MAX_POINTS: usize = 30;

/// The number of findings a Markdown summary retains by default.
///
/// Enough to convey the most significant movers while keeping the rendered report
/// comfortably within a GitHub issue body's size limit even when an analysis flags
/// many changes.
pub const DEFAULT_SUMMARY_LIMIT: NonZero<usize> = NonZero::new(10).expect("10 is non-zero");

/// The selectable output format of an analysis report.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReportFormat {
    /// A compact, human-readable plain-text summary.
    Text,
    /// A machine-readable JSON document.
    Json,
    /// A Markdown summary mirroring the text report, with charts as fenced blocks.
    Markdown,
}

impl ReportFormat {
    /// Parses a format from its command-line name, if recognized.
    #[must_use]
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "text" => Some(Self::Text),
            "json" => Some(Self::Json),
            "markdown" | "md" => Some(Self::Markdown),
            _ => None,
        }
    }
}

/// One discriminant set's slice of the report.
#[derive(Clone, Debug)]
pub struct SetSummary<'a> {
    /// The comparable partition this slice covers.
    pub set: &'a DiscriminantSet,
    /// Number of stored runs loaded for this set.
    pub runs: usize,
    /// Number of distinct series compared in this set.
    pub series: usize,
    /// The set's findings, in the same global ranking as the top level.
    pub findings: Vec<&'a Finding>,
    /// How far this set's comparison base(s) sit behind the base ref, with the
    /// reason for each distinct lag. Branch mode only; empty when every finding's
    /// comparison base reaches the base ref (the usual whole-suite case) and in
    /// history mode. Partial runs can leave different findings comparing against
    /// different points, so this is a deduplicated, deterministically ordered list
    /// rather than a single value.
    pub comparison_base_lags: Vec<ComparisonBaseLag>,
    /// Branch-mode historical comparison for this set, when enough comparable
    /// current-regime history exists.
    pub branch_comparison: Option<&'a BranchComparison>,
}

/// How far a discriminant set's comparison base sits behind the base ref, and why.
///
/// In branch mode each finding is compared against the recent base-ref points of its
/// own discriminant set. On rotating CI machine pools the newest base-ref commits may carry
/// data only under a different machine key, so the context run's machine key has usable
/// base data only several commits behind the base ref — the comparison silently reaches back in
/// history. This records that lag for one set so the report can disclose it.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct ComparisonBaseLag {
    /// First-parent distance from the comparison base to the base ref. Always at
    /// least one: a comparison base that reaches the base ref is not a lag and is
    /// never recorded.
    pub commits_behind: NonZero<usize>,
    /// Why the comparison base lags.
    pub reason: ComparisonBaseLagReason,
}

/// Why a discriminant set's comparison base lags the base ref.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ComparisonBaseLagReason {
    /// A newer base-ref run for the same benchmark and metric exists, but under a
    /// different machine key — machine-pool rotation, not missing measurements. The
    /// comparison base could not use it because counts are not comparable across
    /// machine keys.
    DiscriminantSetMismatch,
    /// No base-ref run for the affected series exists at any more recent commit; the
    /// comparison base is simply the newest data available for this partition.
    NoRecentBaseData,
}

/// The inputs a report is rendered from.
#[derive(Clone, Debug)]
pub struct ReportInput<'a> {
    /// The project the history belongs to.
    pub project: &'a str,
    /// The commit the analysis was run against (the resolved `--context`, HEAD by
    /// default) — the context commit whose line of history the report describes, so a
    /// reader can identify exactly which state the findings pertain to.
    pub tip_commit: &'a str,
    /// Whether the working tree carried uncommitted changes when the analysis ran.
    /// When set, the context is annotated `+ uncommitted changes` because the analyzed
    /// checkout differs from the committed context. False for a clean tree (the CI
    /// collection case).
    pub tip_dirty: bool,
    /// The analysis mode the report was produced in.
    pub mode: AnalysisMode,
    /// Whether any finding survived — the at-a-glance signal a downstream
    /// automation reads to decide whether the report is worth surfacing.
    pub notable: bool,
    /// Total stored runs loaded across every set.
    pub runs: usize,
    /// Total distinct series compared across every set. Carried for the JSON report;
    /// the text and Markdown reports omit it as an uninformative tally.
    pub series: usize,
    /// The oldest and newest analyzed commit (full SHAs, first-parent order), so the
    /// header can state the span of history the analysis covered. `None` when no run
    /// entered the analysis.
    pub commit_span: Option<(&'a str, &'a str)>,
    /// Whether this analysis reports improvements. When `false` (history mode's
    /// regressions-only watch) every rendering omits the improvement tally rather
    /// than stating a zero the analysis never looked for.
    pub report_improvements: bool,
    /// Every set's findings, globally ranked most-notable first.
    pub findings: &'a [Finding],
    /// The per-set breakdown, one entry per set that contributed data.
    pub sets: &'a [SetSummary<'a>],
    /// A diagnostic hint shown when stored runs existed but none were analyzed,
    /// explaining why the outcome is empty. Absent in the normal case.
    pub hint: Option<&'a str>,
    /// A warning shown when the analysis admitted dirty runs on the base ref's context
    /// commit (the working-tree-dirty exception). Absent in the normal case.
    pub warning: Option<&'a str>,
    /// How many benchmarks were dropped as "ghosts" — present only for past commits,
    /// not at the context commit — before detection. Zero when nothing was dropped.
    /// Carried for the JSON report so a machine consumer sees that scoping happened;
    /// the text and Markdown reports surface it only through the verbose trail and the
    /// empty-outcome hint.
    pub ghosts_excluded: usize,
    /// What the analysis judged, and why it left the rest unjudged. Every rendering
    /// discloses it, because "nothing moved" is a statement about the judged series
    /// alone.
    pub census: SeriesCensus,
}

/// The JSON shape of the series census.
///
/// Self-contained: `total` is `judged` plus `unjudged`, `unjudged` is the sum of the
/// `reasons` counts, and `coverage` is the verdict-bearing state derived from
/// `in_scope` — so a consumer reads coverage without cross-referencing the rest of the
/// document or re-deriving the ghost arithmetic. It counts *series*, and its total
/// spans the whole suite the analysis started from — including the ghost-filtered
/// series the top-level `series` tally excludes.
#[derive(Serialize)]
struct JsonCensus {
    /// Every series the analysis accounted for.
    total: usize,
    /// Every series that could have been judged: `total` less the ghosts, which no
    /// analysis can judge. The denominator `coverage` is derived from.
    in_scope: usize,
    /// Series the detectors reached a verdict on. A silent report says nothing about
    /// the rest.
    judged: usize,
    /// Series that were not tested at all.
    unjudged: usize,
    /// How much of the in-scope suite was judged, as a stable `snake_case` state name
    /// (see [`CoverageState`]). The one field automation gates on: an empty findings
    /// list means "nothing moved" only at `full`.
    coverage: &'static str,
    /// The unjudged series broken down by reason, in reporting order. Empty when
    /// every series was judged.
    reasons: Vec<JsonUnjudged>,
}

/// The JSON shape of one census reason and the series it accounts for.
#[derive(Serialize)]
struct JsonUnjudged {
    /// The reason's stable `snake_case` wire name.
    reason: &'static str,
    /// How many series it accounts for. Always at least one — a reason accounting for
    /// nothing is omitted.
    count: usize,
}

impl JsonCensus {
    /// Projects the shared [`Coverage`] onto its JSON shape.
    fn from_coverage(coverage: &Coverage) -> Self {
        Self {
            total: coverage.total(),
            in_scope: coverage.in_scope(),
            judged: coverage.judged(),
            unjudged: coverage.unjudged(),
            coverage: coverage.state().as_str(),
            reasons: coverage
                .reasons()
                .map(|(reason, count)| JsonUnjudged {
                    reason: reason.as_str(),
                    count,
                })
                .collect(),
        }
    }
}

/// The JSON shape of a per-set slice.
///
/// Carries only the partition identity and its tallies — the cheap metadata that
/// mirrors the per-set header the text and Markdown reports print. The findings
/// themselves live once in the top-level [`JsonReport::findings`] list (each names
/// its own set), so the document never duplicates a finding per set.
#[derive(Serialize)]
struct JsonSet<'a> {
    /// Engine identifier.
    engine: &'a str,
    /// Resolved target triple.
    target_triple: &'a str,
    /// Machine key: the hardware fingerprint partition value the runs were stored under.
    machine_key: &'a str,
    /// Stored runs loaded for this set.
    runs: usize,
    /// Distinct series compared in this set.
    series: usize,
    /// Flagged regressions in this set.
    regressions: usize,
    /// Flagged improvements in this set. Absent in a mode that does not report
    /// improvements, so the document never states a tally the analysis did not look for.
    #[serde(skip_serializing_if = "Option::is_none")]
    improvements: Option<usize>,
    /// How far this set's comparison base(s) lag the base ref, with the reason for
    /// each distinct lag (branch mode only). Omitted when the comparison base reaches
    /// the base ref — the usual case — and in history mode.
    #[serde(skip_serializing_if = "<[ComparisonBaseLag]>::is_empty")]
    comparison_base_lags: &'a [ComparisonBaseLag],
    /// Report-wide branch comparison for this set.
    #[serde(skip_serializing_if = "Option::is_none")]
    branch_comparison: Option<JsonBranchComparison>,
}

/// Machine-readable historical comparison for one branch-mode set.
#[derive(Serialize)]
struct JsonBranchComparison {
    /// Existing base commits evaluated as the candidate.
    evaluated_base_commits: usize,
    /// Existing-base reports tied with or exceeding the branch report.
    at_least_as_much: usize,
    /// Series shared by every candidate turn.
    series: usize,
}

impl From<&BranchComparison> for JsonBranchComparison {
    fn from(comparison: &BranchComparison) -> Self {
        Self {
            evaluated_base_commits: comparison.evaluated_base_commits,
            at_least_as_much: comparison.at_least_as_much,
            series: comparison.series,
        }
    }
}

/// The JSON shape of one finding: the machine-readable form of a text-report
/// finding paragraph.
///
/// It carries exactly the data the text and Markdown reports show — the partition,
/// the benchmark identity, the metric, the detected move, and the provenance — with
/// no underlying series (the text chart is a presentation concern, not data a
/// consumer reconstructs) and full `f64` precision (the human reports round).
#[derive(Serialize)]
struct JsonFinding<'a> {
    /// The comparable discriminant set, inlined as `engine`/`target_triple`/
    /// `machine_key` so the flat list is self-describing.
    #[serde(flatten)]
    set: &'a DiscriminantSet,
    /// The benchmark identity, inlined as `segments`.
    #[serde(flatten)]
    id: &'a BenchmarkId,
    /// The metric kind that moved (its `snake_case` wire name).
    kind: &'static str,
    /// Which detector produced the finding.
    method: FindingMethod,
    /// Whether the move is a regression or an improvement.
    direction: Direction,
    /// Reference value the latest measurement was compared against.
    ///
    /// In history mode this is the before-regime representative. In branch mode it is
    /// the nearest observed current-base range edge; there is no before regime.
    baseline: f64,
    /// Latest measured value.
    ///
    /// In history mode this is the after-regime representative. In branch mode it is
    /// the context-commit observation.
    latest: f64,
    /// Signed difference relative to `baseline`.
    ///
    /// In history mode this is `(latest - baseline) / baseline`. In branch mode it is
    /// the signed excess relative to the nearest range edge, matching
    /// [`BranchExcursion::relative_excess`].
    relative_delta: f64,
    /// Commit associated with the finding, if known. For a change point this is the
    /// detector's estimate of where the new level begins, not a claim that that commit
    /// introduced it. For a drift this is the newest commit the trend reached;
    /// `window_start` names where it began.
    #[serde(skip_serializing_if = "Option::is_none")]
    commit: Option<&'a str>,
    /// The oldest commit of a drift's accumulation window, present only for a drift, so
    /// a consumer can see the range rather than reading `commit` as a single point.
    #[serde(skip_serializing_if = "Option::is_none")]
    window_start: Option<&'a str>,
    /// Abbreviated commit of the blessing that re-baselined the series, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    blessed_at: Option<&'a str>,
    /// Effective (committer) time of the blessed commit, RFC 3339, if blessed.
    #[serde(skip_serializing_if = "Option::is_none")]
    blessed_commit_time: Option<&'a str>,
    /// Branch-specific observed range and excess.
    #[serde(skip_serializing_if = "Option::is_none")]
    branch: Option<JsonBranchExcursion<'a>>,
}

/// Machine-readable range evidence for a branch excursion.
#[derive(Serialize)]
struct JsonBranchExcursion<'a> {
    /// Base observations in the selected range.
    reference_count: usize,
    /// Smallest selected base observation.
    reference_min: f64,
    /// Largest selected base observation.
    reference_max: f64,
    /// Signed distance beyond the nearest range edge.
    excess: f64,
    /// Signed excess relative to that edge.
    relative_excess: f64,
    /// Commit opening the current regime, when established.
    #[serde(skip_serializing_if = "Option::is_none")]
    current_regime_start: Option<&'a str>,
    /// Whether the branch falls inside the immediately preceding regime.
    matches_previous_regime: bool,
    /// Whether this series contributed to the report-wide historical comparison.
    included_in_historical_comparison: bool,
}

impl<'a> From<&'a BranchExcursion> for JsonBranchExcursion<'a> {
    fn from(excursion: &'a BranchExcursion) -> Self {
        Self {
            reference_count: excursion.reference_count,
            reference_min: excursion.reference_min,
            reference_max: excursion.reference_max,
            excess: excursion.excess,
            relative_excess: excursion.relative_excess,
            current_regime_start: excursion.current_regime_start.as_deref(),
            matches_previous_regime: excursion.matches_previous_regime,
            included_in_historical_comparison: excursion.included_in_historical_comparison,
        }
    }
}

impl<'a> JsonFinding<'a> {
    /// Projects a [`Finding`] onto its cheap, series-free JSON shape.
    fn from_finding(finding: &'a Finding) -> Self {
        Self {
            set: &finding.set,
            id: &finding.id,
            kind: finding.kind.as_str(),
            method: finding.method,
            direction: finding.direction,
            baseline: finding.baseline,
            latest: finding.latest,
            relative_delta: finding.relative_delta,
            commit: finding.commit.as_deref(),
            window_start: finding.window_start_commit.as_deref(),
            blessed_at: finding.blessed_at.as_deref(),
            blessed_commit_time: finding.blessed_commit_time.as_deref(),
            branch: finding.branch.as_ref().map(JsonBranchExcursion::from),
        }
    }
}

/// The JSON shape of a rendered report.
#[derive(Serialize)]
struct JsonReport<'a> {
    /// The project the history belongs to.
    project: &'a str,
    /// The commit the analysis was run against (resolved `--context`, HEAD by
    /// default): the full commit ID, so a consumer can link the report to a commit.
    tip_commit: &'a str,
    /// Whether the working tree carried uncommitted changes when the analysis ran.
    tip_dirty: bool,
    /// The analysis mode. Serializes to its stable lowercase wire name
    /// (`history`/`branch`).
    mode: AnalysisMode,
    /// The primary verdict of the successful analysis.
    outcome: &'static str,
    /// Whether any finding survived — the downstream automation signal.
    notable: bool,
    /// Total stored runs loaded.
    runs: usize,
    /// Total distinct series compared.
    series: usize,
    /// Number of flagged regressions.
    regressions: usize,
    /// Number of flagged improvements. Absent in a mode that does not report
    /// improvements, so the document never states a tally the analysis did not look for.
    #[serde(skip_serializing_if = "Option::is_none")]
    improvements: Option<usize>,
    /// Benchmarks dropped as ghosts (present only for past commits, not at the
    /// context commit) before detection. Zero when nothing was dropped.
    ghosts_excluded: usize,
    /// What the analysis judged, and why it left the rest unjudged — the coverage a
    /// `notable: false` verdict must be read against.
    census: JsonCensus,
    /// A diagnostic hint when stored runs existed but none were analyzed.
    #[serde(skip_serializing_if = "Option::is_none")]
    hint: Option<&'a str>,
    /// A warning when dirty base-ref context runs were admitted.
    #[serde(skip_serializing_if = "Option::is_none")]
    warning: Option<&'a str>,
    /// Every finding, globally ranked most-notable first; each names its own set.
    findings: Vec<JsonFinding<'a>>,
    /// The per-set breakdown (partition identity and tallies only).
    sets: Vec<JsonSet<'a>>,
}

/// Renders `input` in the requested `format`.
///
/// `color` enables ANSI styling of the text format — the direction-colored headline
/// percentage, the bold benchmark id, and the dimmed detail and blessing lines. The
/// caller decides it from the output terminal so tests and pipes stay plain; charts
/// are always uncolored, and `markdown` and `json` ignore it.
#[must_use]
pub fn render(input: &ReportInput<'_>, format: ReportFormat, color: bool) -> String {
    match format {
        ReportFormat::Text => render_text(input, color),
        ReportFormat::Markdown => render_markdown(input),
        ReportFormat::Json => render_json(input),
    }
}

/// Counts findings matching `direction`.
fn count_direction(findings: &[&Finding], direction: Direction) -> usize {
    findings
        .iter()
        .filter(|finding| finding.direction == direction)
        .count()
}

/// Counts top-level findings matching `direction`.
fn count_top(findings: &[Finding], direction: Direction) -> usize {
    findings
        .iter()
        .filter(|finding| finding.direction == direction)
        .count()
}

/// Joins report lines into the final string with a trailing newline.
fn finish(lines: &[String]) -> String {
    format!("{}\n", lines.join("\n"))
}

/// Appends the ephemeral-data warning (if any) as a trailing, blank-line-separated
/// block, so it reads at the very end of the report.
fn push_warning(lines: &mut Vec<String>, warning: Option<&str>) {
    if let Some(warning) = warning {
        lines.push(String::new());
        lines.push(warning.to_owned());
    }
}

/// Formats one comparison-base lag as its exact report warning line, with the
/// singular/plural agreement the text and Markdown reports share.
fn comparison_base_lag_warning(lag: &ComparisonBaseLag) -> String {
    let count = lag.commits_behind.get();
    let commits = if count == 1 { "commit" } else { "commits" };
    let reason = match lag.reason {
        ComparisonBaseLagReason::DiscriminantSetMismatch => "discriminant set mismatch",
        ComparisonBaseLagReason::NoRecentBaseData => "no base data at more recent commits",
    };
    format!("Warning: comparison base is {count} {commits} behind base ({reason})")
}

/// A one-line label for a set, naming its `engine / triple / machine` partition.
fn set_label(set: &DiscriminantSet) -> String {
    set.to_string()
}

/// The `analyze` discriminant-filter flags that select exactly this discriminant set, ready to
/// paste into a follow-up query. Naming every filter explicitly pins the one set, so a
/// reader who spots a finding can drill into that partition without having to guess
/// which engine / triple / machine it came from.
fn set_filter_flags(set: &DiscriminantSet) -> String {
    format!(
        "--engine {} --target-triple {} --machine-key {}",
        set.engine, set.target_triple, set.machine_key
    )
}

/// The commit label shared by the text and Markdown headers: the analyzed context
/// commit, annotated `+ uncommitted changes` when the working tree carried
/// uncommitted changes so a reader knows the analyzed checkout differed from the
/// committed context.
fn tip_label(commit: &str, dirty: bool) -> String {
    if dirty {
        format!("{commit} + uncommitted changes")
    } else {
        commit.to_owned()
    }
}

/// Serializes access to `colored`'s process-global override.
static COLOR_OVERRIDE_LOCK: Mutex<()> = Mutex::new(());

/// Forces `colored`'s process-global override to `value` until dropped, then restores
/// ambient auto-detection.
///
/// Holding [`COLOR_OVERRIDE_LOCK`] for the override's lifetime prevents concurrent
/// renders from changing the process-global state while output is being assembled.
struct ColorOverride {
    _lock: MutexGuard<'static, ()>,
}

impl ColorOverride {
    #[must_use]
    fn force(value: bool) -> Self {
        let lock = COLOR_OVERRIDE_LOCK
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        colored::control::set_override(value);
        Self { _lock: lock }
    }
}

impl Drop for ColorOverride {
    // Restoring `colored`'s process-global override is exercised by every render test
    // (each builds and drops a guard), but `colored` exposes no override-state getter
    // to assert the restoration directly. Skipped for mutation only; the restore itself
    // is covered behaviourally.
    #[cfg_attr(test, mutants::skip)]
    fn drop(&mut self) {
        colored::control::unset_override();
    }
}

/// Whether a quiet branch report must continue into its per-set context.
fn should_render_empty_branch_sets(input: &ReportInput<'_>) -> bool {
    matches!(
        (input.mode, input.findings.is_empty()),
        (AnalysisMode::Branch, true)
    )
}

fn render_text(input: &ReportInput<'_>, color: bool) -> String {
    // Force `colored` to honor this explicit decision rather than its own ambient
    // terminal auto-detection, so tests and pipes are deterministic regardless of how
    // the process is run. The guard restores auto-detection on return.
    let _color = ColorOverride::force(color);

    let coverage = Coverage::from_census(&input.census);
    let regressions = count_top(input.findings, Direction::Regression);

    let mut header = vec![format!(
        "runs: {}",
        runs_with_span(input.runs, input.commit_span)
    )];
    header.extend(judged_field(&coverage));
    header.push(format!("regressions: {regressions}"));
    if input.report_improvements {
        header.push(format!(
            "improvements: {}",
            count_top(input.findings, Direction::Improvement)
        ));
    }

    let mut lines = vec![
        format!(
            "Analyzed project {} ({} mode)",
            input.project,
            input.mode.as_str()
        ),
        format!("  commit: {}", tip_label(input.tip_commit, input.tip_dirty)),
        format!("  {}", header.join("  ")),
    ];

    let render_empty_branch_sets = should_render_empty_branch_sets(input);
    if input.findings.is_empty() {
        lines.push(coverage.verdict().to_owned());
        // Silence is a claim about the judged series only, so state how far it
        // reaches. Indented under the verdict it qualifies.
        for sentence in coverage.qualifications() {
            lines.push(format!("  {sentence}"));
        }
        if let Some(hint) = input.hint {
            lines.push(String::new());
            lines.push(hint.to_owned());
        }
        if !render_empty_branch_sets {
            push_warning(&mut lines, input.warning);
            return finish(&lines);
        }
    }

    // Both modes draw a per-finding chart; the scope differs. History walks the whole
    // series; branch charts the comparison baseline and recent tail so the context commit
    // it judges stays legible (see `ChartScope`).
    let scope = chart_scope(input.mode);
    for summary in input.sets {
        if summary.findings.is_empty()
            && summary.branch_comparison.is_none()
            && summary.comparison_base_lags.is_empty()
            && !render_empty_branch_sets
        {
            continue;
        }
        lines.push(String::new());
        lines.push(set_label(summary.set));
        lines.push(set_counts_line(summary, input.report_improvements));
        lines.push(format!("  filter: {}", set_filter_flags(summary.set)));
        for lag in &summary.comparison_base_lags {
            lines.push(format!("  {}", comparison_base_lag_warning(lag)));
        }
        if input.mode == AnalysisMode::Branch {
            lines.push(format!(
                "  {}",
                historical_comparison_text(
                    summary.branch_comparison,
                    has_findings_in_historical_comparison(summary),
                )
            ));
        }
        let included: Vec<&Finding> = summary
            .findings
            .iter()
            .copied()
            .filter(|finding| is_primary_finding(summary, finding))
            .collect();
        for finding in included {
            push_finding_block(&mut lines, finding, scope);
        }
        let additional: Vec<&Finding> = summary
            .findings
            .iter()
            .copied()
            .filter(|finding| !is_primary_finding(summary, finding))
            .collect();
        if !additional.is_empty() {
            lines.push(String::new());
            lines.push("  Additional excursions outside this comparison:".to_owned());
        }
        for finding in additional {
            push_finding_block(&mut lines, finding, scope);
        }
    }
    push_warning(&mut lines, input.warning);
    finish(&lines)
}

/// Appends one finding as a paragraph: the benchmark id on its own line as a
/// chapter title, then a direction-colored headline, a dimmed detail line, an
/// optional blessing note, and a chart of the metric over commits, scoped per
/// [`ChartScope`].
fn push_finding_block(lines: &mut Vec<String>, finding: &Finding, scope: ChartScope) {
    lines.push(String::new());

    // Lead with the benchmark id on its own line, like a chapter title: some ids are
    // long, so a dedicated line keeps the change headline that follows readable.
    lines.push(describe_id(&finding.id).bold().to_string());

    let headline_text = if let Some(branch) = &finding.branch {
        let relation = branch_relation(finding.kind, finding.direction);
        format!(
            "{} {} - {relation} {} current-base observations",
            format_value(finding.latest),
            finding.kind.as_str(),
            branch.reference_count,
        )
    } else {
        format!(
            "{} {}",
            format_percent(finding.relative_delta),
            finding.kind.as_str()
        )
    };
    let headline = match finding.direction {
        Direction::Regression => headline_text.red().bold(),
        Direction::Improvement => headline_text.green().bold(),
    };
    lines.push(format!("  {headline}"));

    lines.push(format!("    {}", detail_text(finding)).dimmed().to_string());

    // Name the blessing that re-baselined the series, so the reader knows the
    // history before it is intentionally excluded from detection.
    if let Some(blessing) = blessing_text(finding) {
        lines.push(format!("    {blessing}").dimmed().to_string());
    }
    if let Some(context) = previous_regime_text(finding) {
        lines.push(format!("    {context}").dimmed().to_string());
    }

    if let Some(chart) = scoped_chart(finding, scope) {
        lines.push(chart);
    }
}

/// The per-set summary line — the cheap tally the JSON `sets` block carries, shown
/// under each set header so the text and Markdown reports surface it too. The
/// improvement tally is omitted when the analysis does not report improvements.
fn set_counts_line(summary: &SetSummary<'_>, report_improvements: bool) -> String {
    let mut fields = vec![
        format!("runs: {}", summary.runs),
        format!(
            "regressions: {}",
            count_direction(&summary.findings, Direction::Regression)
        ),
    ];
    if report_improvements {
        fields.push(format!(
            "improvements: {}",
            count_direction(&summary.findings, Direction::Improvement)
        ));
    }
    format!("  {}", fields.join("  "))
}

/// The header field disclosing how much of the suite was judged, as
/// `in-scope series judged: 42 of 53`. `None` when nothing was in scope, where a `0 of
/// 0` ratio would be noise and the verdict — or the empty-outcome hint — speaks instead.
fn judged_field(coverage: &Coverage) -> Option<String> {
    (coverage.in_scope() > 0).then(|| {
        format!(
            "in-scope series judged: {} of {}",
            coverage.judged(),
            coverage.in_scope()
        )
    })
}

/// The Markdown bullet form of [`judged_field`].
fn judged_bullet(coverage: &Coverage) -> Option<String> {
    (coverage.in_scope() > 0).then(|| {
        format!(
            "- In-scope series judged: {} of {}",
            coverage.judged(),
            coverage.in_scope()
        )
    })
}

/// Formats the run tally with the analyzed commit span appended, so the report
/// header states both how many runs entered the analysis and the stretch of history
/// they cover. A single analyzed commit collapses the range to that one commit; with
/// no runs the count stands alone.
fn runs_with_span(runs: usize, span: Option<(&str, &str)>) -> String {
    match span {
        Some((first, last)) if first == last => format!("{runs} ({})", short_commit(first)),
        Some((first, last)) => {
            format!("{runs} ({} → {})", short_commit(first), short_commit(last))
        }
        None => runs.to_string(),
    }
}

/// The plain-text detail body shared by the text and Markdown reports: the
/// direction, detector, the `baseline → latest` move, and the commit associated with
/// it. A change point names a commit somewhere near the split; a drift belongs to its
/// whole window rather than one commit, so it names the range it accumulated over
/// (`from … to …`). Carries no styling and no leading indent; each format applies
/// its own.
fn detail_text(finding: &Finding) -> String {
    if let Some(branch) = &finding.branch {
        return format!(
            "current base range: {}-{} · branch excess: {} / {} · {}",
            format_value(branch.reference_min),
            format_value(branch.reference_max),
            format_signed_value(branch.excess),
            format_percent(branch.relative_excess),
            attribution_text(finding),
        );
    }
    format!(
        "{} via {} · {} → {} · {}",
        direction_label(finding.direction),
        method_label(finding.method),
        format_value(finding.baseline),
        format_value(finding.latest),
        attribution_text(finding),
    )
}

/// Reader-facing historical comparison without internal statistical terminology.
fn historical_comparison_text(comparison: Option<&BranchComparison>, has_findings: bool) -> String {
    let Some(comparison) = comparison else {
        return "There was not enough comparable base history for a report-wide comparison."
            .to_owned();
    };
    if !has_findings {
        return format!(
            "The branch produced no reportable out-of-range movement in this comparison ({} \
             series and {} comparable base commits).",
            comparison.series, comparison.evaluated_base_commits,
        );
    }
    if comparison.at_least_as_much == 0 {
        return format!(
            "None of {} comparable base commits showed as much out-of-range movement as this \
             branch ({} series compared).",
            comparison.evaluated_base_commits, comparison.series,
        );
    }
    format!(
        "{} of {} comparable base commits showed at least as much out-of-range movement as this \
         branch ({} series compared).",
        comparison.at_least_as_much, comparison.evaluated_base_commits, comparison.series,
    )
}

fn has_findings_in_historical_comparison(summary: &SetSummary<'_>) -> bool {
    summary
        .findings
        .iter()
        .any(|finding| is_in_historical_comparison(finding))
}

fn is_primary_finding(summary: &SetSummary<'_>, finding: &Finding) -> bool {
    summary.branch_comparison.is_none() || is_in_historical_comparison(finding)
}

fn is_in_historical_comparison(finding: &Finding) -> bool {
    finding
        .branch
        .as_ref()
        .is_some_and(|branch| branch.included_in_historical_comparison)
}

/// Context note when the branch returns to the regime immediately before the current one.
fn previous_regime_text(finding: &Finding) -> Option<String> {
    let branch = finding.branch.as_ref()?;
    if !branch.matches_previous_regime {
        return None;
    }
    let boundary = branch
        .current_regime_start
        .as_deref()
        .unwrap_or("the current boundary");
    Some(format!(
        "The branch value matches the regime preceding base commit {}.",
        short_commit(boundary)
    ))
}

/// The commit(s) a finding is associated with, for the detail body.
///
/// A change point names a commit somewhere near the detected split, because the split
/// search estimates a regime boundary and cannot always identify the first commit that
/// introduced the new level. A branch comparison names the context commit
/// (`@ <commit>`). A drift names the range it accumulated over
/// (`accumulated <oldest> → <newest>`), because no single commit is responsible.
/// A missing commit is attributed across the analyzed window, matching the book
/// fragments, rather than naming a placeholder.
fn attribution_text(finding: &Finding) -> String {
    match (finding.method, finding.commit.as_deref()) {
        (_, None) => "across the analyzed window".to_owned(),
        (FindingMethod::ChangePoint, Some(commit)) => format!("somewhere near {commit}"),
        (FindingMethod::Drift, Some(commit)) => match finding.window_start_commit.as_deref() {
            Some(start) => format!("accumulated {start} → {commit}"),
            None => format!("accumulated ending at {commit}"),
        },
        (FindingMethod::BranchExcursion, Some(commit)) => format!("@ {commit}"),
    }
}

/// The plain-text blessing note, when the series was re-baselined by a
/// blessing. Carries no styling and no leading indent.
fn blessing_text(finding: &Finding) -> Option<String> {
    let blessed_at = finding.blessed_at.as_deref()?;
    let date = finding
        .blessed_commit_time
        .as_deref()
        .map_or_else(String::new, |effective| format!(" ({effective})"));
    Some(format!("blessed at {blessed_at}{date}"))
}

/// How a finding's metric chart is scoped for the analysis mode it was produced in.
///
/// The two modes ask different questions of the same stored history, so they chart
/// different slices of a finding's series.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ChartScope {
    /// History mode: plot the whole series, so the long-range trend shows.
    FullHistory,
    /// Branch mode: plot the comparison baseline followed by the most recent points
    /// ending at the context commit. Branch mode judges the branch by that one commit, so
    /// the bounded chart keeps it legible instead of aliasing it into a single edge
    /// column (see [`BRANCH_CHART_MAX_POINTS`]).
    BranchComparison,
}

/// Chooses the [`ChartScope`] for an analysis `mode`.
///
/// Only [`AnalysisMode::Branch`] uses the bounded comparison; history charts the
/// full series, the information-preserving default.
fn chart_scope(mode: AnalysisMode) -> ChartScope {
    if mode == AnalysisMode::Branch {
        ChartScope::BranchComparison
    } else {
        ChartScope::FullHistory
    }
}

/// Builds the bounded, topology-accurate columns for a branch finding's comparison
/// chart.
///
/// The first column is the detector's actual comparison baseline. The rest are the
/// recent per-commit tail ending at the context commit: one column for each first-parent commit
/// from `start_topo` up to the context point's `topo_index`, carrying the mean of the
/// observations at that commit or a gap ([`f64::NAN`]) where the commit has none. The
/// context commit's column carries the finding's judged latest value and is never a gap, so
/// both sides of the reported change stay visible — and the gap between the newest base
/// point and the context commit (the comparison-base lag) is drawn as those empty interior
/// columns — however long the underlying history is. The window spans at most
/// [`BRANCH_CHART_MAX_POINTS`] columns.
fn branch_chart_values(finding: &Finding) -> Vec<f64> {
    let mut values = vec![finding.baseline];
    let Some(tip_topo) = finding.series.last().map(|point| point.topo_index) else {
        return values;
    };
    // One column per first-parent commit in the recent window ending at the context, so a
    // data-less commit between the newest base point and the context commit is a visible gap.
    let window = BRANCH_CHART_MAX_POINTS.saturating_sub(1);
    let start_topo = tip_topo.saturating_sub(window.saturating_sub(1));
    // The series is ordered by `topo_index`, so the window is a contiguous suffix. Fold
    // each in-window commit's observations into its own column in a single pass, rather
    // than re-scanning the whole (in branch mode still-full) base history once per column.
    let column_count = tip_topo.saturating_sub(start_topo).saturating_add(1);
    let mut bins = vec![(0.0_f64, 0_usize); column_count];
    let first = finding
        .series
        .partition_point(|point| point.topo_index < start_topo);
    for point in finding.series.get(first..).unwrap_or_default() {
        // Every point from `first` onward has `start_topo <= topo_index <= tip_topo`, so
        // the offset stays within `0..column_count`; `get_mut` guards it regardless.
        let col = point.topo_index.saturating_sub(start_topo);
        if let Some((sum, count)) = bins.get_mut(col) {
            *sum += point.value;
            *count = count.saturating_add(1);
        }
    }
    values.extend(bins.into_iter().map(|(sum, count)| {
        if count == 0 {
            f64::NAN
        } else {
            sum / count_as_f64(count)
        }
    }));
    values
}

/// Bins real `(topo_index, value)` observations into at most `max_width` chart columns.
///
/// A gap ([`f64::NAN`]) is materialized for every commit — or, once the span exceeds
/// `max_width`, every bin — that carries no observation.
///
/// `points` are ascending by `topo_index` (equal indices allowed, e.g. a commit's clean
/// and dirty snapshots) and hold only real observations. `base_ref` is the trailing-fill
/// target: when it sits past the last point the columns extend to it, so the data-less
/// commits after the last observation render as a gap (the "no newer data" tail); `None`,
/// or a value at or before the last point, adds no trailing gap. The leftmost column
/// always holds the first observation, so there is never a leading gap. `max_width` must
/// be at least 1; wide-chart callers pass [`CHART_WIDTH`].
///
/// While the span fits (`span + 1 <= max_width`) each commit maps to its own column, so
/// the topology is exact and a data-less commit is a single `NaN` column. Beyond that
/// the span is downsampled: every real point still lands in some column (placed by
/// integer index, never interpolated away), so an isolated observation is never dropped
/// and empty columns stay `NaN`. A column that catches several observations averages
/// them, which blurs a dense region and can attenuate an extreme that shares a bin — the
/// one detail binning gives up. Binning to `max_width` before the series reaches [`chart`]
/// is essential: `rasciigraph` interpolates to its width *before* computing the axis
/// min/max and its linear interpolation is NaN-poisoning, so a longer series would blend
/// an isolated observation surrounded by `NaN` into `NaN` and drop it (and its value)
/// entirely.
#[must_use]
pub fn topology_columns(
    points: &[(usize, f64)],
    base_ref: Option<usize>,
    max_width: usize,
) -> Vec<f64> {
    assert!(max_width >= 1, "a chart needs at least one column");
    let Some(&(first, _)) = points.first() else {
        return Vec::new();
    };
    let last = points.last().map_or(first, |&(topo, _)| topo);
    let end = base_ref.map_or(last, |target| target.max(last));
    let span = end.saturating_sub(first);
    let width = span.saturating_add(1).min(max_width);
    let mut bins = vec![(0.0_f64, 0_usize); width];
    for &(topo, value) in points {
        // `topo` lies in `[first, last] ⊆ [first, end]`, so `topo - first ∈ [0, span]` and
        // the column index stays within `0..width`; when `span == 0` every point maps to
        // the single column 0. The saturating operations only guard against overflow that
        // real histories never reach; the true product is at most `span * (width - 1)`.
        let offset = topo
            .saturating_sub(first)
            .saturating_mul(width.saturating_sub(1));
        let col = offset.checked_div(span).unwrap_or(0);
        if let Some((sum, count)) = bins.get_mut(col) {
            *sum += value;
            *count = count.saturating_add(1);
        }
    }
    bins.into_iter()
        .map(|(sum, count)| {
            if count == 0 {
                f64::NAN
            } else {
                sum / count_as_f64(count)
            }
        })
        .collect()
}

/// Renders a compact line chart of real `(topo_index, value)` observations.
///
/// The observations are binned into topology-accurate columns (see [`topology_columns`])
/// before plotting. `None` when fewer than two columns carry a real value.
///
/// Both wide-chart callers — a history finding's whole-series chart and `examine`'s
/// per-commit chart — go through here, so a sparse or lagging series always renders its
/// interior and trailing gaps.
#[must_use]
pub fn chart_series(points: &[(usize, f64)], base_ref: Option<usize>) -> Option<String> {
    chart(&topology_columns(points, base_ref, CHART_WIDTH as usize))
}

/// Casts a small bin count to `f64` for averaging. Bin counts are bounded by the
/// observation count, far below 2^53, so the conversion is exact.
#[expect(
    clippy::cast_precision_loss,
    reason = "bin counts are far below 2^53, so the cast is exact"
)]
fn count_as_f64(count: usize) -> f64 {
    count as f64
}

/// Renders a finding's metric chart for the given [`ChartScope`], or `None` when the
/// scoped series has too few points to plot.
///
/// [`ChartScope::FullHistory`] charts the whole series topology (one column per
/// first-parent commit from the first observation onward, with gaps for data-less
/// commits and a trailing gap up to the analysis context commit);
/// [`ChartScope::BranchComparison`] charts [`branch_chart_values`], so the comparison
/// baseline and context commit stay legible rather than becoming aliased edge columns.
fn scoped_chart(finding: &Finding, scope: ChartScope) -> Option<String> {
    match scope {
        ChartScope::FullHistory => {
            let points: Vec<(usize, f64)> = finding
                .series
                .iter()
                .map(|point| (point.topo_index, point.value))
                .collect();
            chart_series(&points, finding.chart_base_ref)
        }
        // Chart the comparison baseline and recent tail ending at the context commit. This is
        // business-critical: the context commit is the sole data point branch mode judges,
        // so it must remain visible and unaliased regardless of how much history
        // precedes it.
        ChartScope::BranchComparison => chart(&branch_chart_values(finding)),
    }
}

/// Renders a compact line chart of `values` over commits at the report's chart height.
///
/// A non-finite (`NaN`) value renders as a gap in the line. Returns `None` when fewer
/// than two values are finite (nothing to plot a line between).
///
/// The chart is drawn one column per supplied value, up to [`CHART_WIDTH`]; callers bin
/// to at most that many columns first (see [`topology_columns`]), so the chart spans at
/// most 48 columns. The width deliberately tracks the value count rather than always
/// stretching to 48: `rasciigraph` resamples every series to `config.width` with linear
/// interpolation *before* computing the axis extrema, and that interpolation is
/// NaN-poisoning — stretching a gapped series blends an isolated observation trapped
/// between two gaps into `NaN` and drops it (and its value) from both the line and the
/// axis. Matching the width to the value count makes that resample an identity, so every
/// real observation and both axis extrema survive and each gap stays exactly as wide as
/// the run of data-less commits it represents.
///
/// The line is always drawn uncolored: the report is most often read as Markdown, where
/// ANSI styling would only add noise, so the chart carries plain characters that render
/// anywhere.
#[must_use]
pub fn chart(values: &[f64]) -> Option<String> {
    if values.iter().filter(|value| value.is_finite()).count() < 2 {
        return None;
    }
    let width = u32::try_from(values.len())
        .unwrap_or(CHART_WIDTH)
        .min(CHART_WIDTH);
    let config = Config::default()
        .with_height(CHART_HEIGHT)
        .with_width(width);
    Some(
        plot(values.to_vec(), config)
            .trim_end_matches('\n')
            .to_owned(),
    )
}

fn render_markdown(input: &ReportInput<'_>) -> String {
    let coverage = Coverage::from_census(&input.census);
    let regressions = count_top(input.findings, Direction::Regression);

    let mut lines = vec![
        format!("# Benchmark history analysis: {}", input.project),
        String::new(),
        format!("- Commit: {}", tip_label(input.tip_commit, input.tip_dirty)),
        format!("- Mode: {}", input.mode.as_str()),
        format!(
            "- Runs analyzed: {}",
            runs_with_span(input.runs, input.commit_span)
        ),
    ];
    lines.extend(judged_bullet(&coverage));
    lines.push(format!("- Regressions: {regressions}"));
    if input.report_improvements {
        lines.push(format!(
            "- Improvements: {}",
            count_top(input.findings, Direction::Improvement)
        ));
    }

    let render_empty_branch_sets = should_render_empty_branch_sets(input);
    if input.findings.is_empty() {
        lines.push(String::new());
        lines.push(coverage.verdict().to_owned());
        for sentence in coverage.qualifications() {
            lines.push(String::new());
            lines.push(sentence);
        }
        if let Some(hint) = input.hint {
            lines.push(String::new());
            lines.push(hint.to_owned());
        }
        if !render_empty_branch_sets {
            push_warning(&mut lines, input.warning);
            return finish(&lines);
        }
    }

    // Both modes draw a per-finding chart, matching the text report; the scope differs
    // (history walks the whole series, branch charts the baseline and recent tail).
    let scope = chart_scope(input.mode);
    for summary in input.sets {
        if summary.findings.is_empty()
            && summary.branch_comparison.is_none()
            && summary.comparison_base_lags.is_empty()
            && !render_empty_branch_sets
        {
            continue;
        }
        lines.push(String::new());
        lines.push(format!("## {}", set_label(summary.set)));
        lines.push(String::new());
        lines.push(format!("- Runs: {}", summary.runs));
        lines.push(format!(
            "- Regressions: {}",
            count_direction(&summary.findings, Direction::Regression)
        ));
        if input.report_improvements {
            lines.push(format!(
                "- Improvements: {}",
                count_direction(&summary.findings, Direction::Improvement)
            ));
        }
        lines.push(format!("- Filter: `{}`", set_filter_flags(summary.set)));
        for lag in &summary.comparison_base_lags {
            lines.push(String::new());
            lines.push(format!("> {}", comparison_base_lag_warning(lag)));
        }
        if input.mode == AnalysisMode::Branch {
            lines.push(String::new());
            lines.push(format!(
                "**Historical comparison:** {}",
                historical_comparison_text(
                    summary.branch_comparison,
                    has_findings_in_historical_comparison(summary),
                )
            ));
        }
        let included: Vec<&Finding> = summary
            .findings
            .iter()
            .copied()
            .filter(|finding| is_primary_finding(summary, finding))
            .collect();
        for finding in included {
            push_finding_markdown(&mut lines, finding, "###", scope);
        }
        let additional: Vec<&Finding> = summary
            .findings
            .iter()
            .copied()
            .filter(|finding| !is_primary_finding(summary, finding))
            .collect();
        if !additional.is_empty() {
            lines.push(String::new());
            lines.push("### Additional excursions outside this comparison".to_owned());
        }
        for finding in additional {
            push_finding_markdown(&mut lines, finding, "####", scope);
        }
    }
    push_warning(&mut lines, input.warning);
    finish(&lines)
}

/// Renders a condensed Markdown report carrying only the `limit` most significant
/// findings, so a large analysis still fits within a downstream size limit (a GitHub
/// issue body caps at 65,536 characters).
///
/// The header repeats the full Markdown report's totals — computed from *every*
/// finding, not the retained subset — then a flat, globally-ranked list of the top
/// `limit` findings follows. The per-set breakdown the full Markdown report groups by
/// is deliberately dropped: a summary is a single ranked list, and that grouping is the
/// main length driver. When findings were dropped, a note states how many of the
/// total are shown so a reader knows the report is partial and to consult the full
/// report for the rest.
#[must_use]
pub fn render_markdown_summary(input: &ReportInput<'_>, limit: NonZero<usize>) -> String {
    let coverage = Coverage::from_census(&input.census);
    let regressions = count_top(input.findings, Direction::Regression);

    let mut lines = vec![
        format!("# Benchmark history analysis: {}", input.project),
        String::new(),
        format!("- Commit: {}", tip_label(input.tip_commit, input.tip_dirty)),
        format!("- Mode: {}", input.mode.as_str()),
        format!(
            "- Runs analyzed: {}",
            runs_with_span(input.runs, input.commit_span)
        ),
    ];
    lines.extend(judged_bullet(&coverage));
    lines.push(format!("- Regressions: {regressions}"));
    if input.report_improvements {
        lines.push(format!(
            "- Improvements: {}",
            count_top(input.findings, Direction::Improvement)
        ));
    }

    let render_empty_branch_sets = should_render_empty_branch_sets(input);
    if input.findings.is_empty() {
        lines.push(String::new());
        lines.push(coverage.verdict().to_owned());
        for sentence in coverage.qualifications() {
            lines.push(String::new());
            lines.push(sentence);
        }
        if let Some(hint) = input.hint {
            lines.push(String::new());
            lines.push(hint.to_owned());
        }
        if !render_empty_branch_sets {
            push_warning(&mut lines, input.warning);
            return finish(&lines);
        }
    }
    if input.findings.is_empty() {
        for summary in input.sets {
            lines.push(String::new());
            lines.push(format!("## {}", set_label(summary.set)));
            lines.push(String::new());
            for lag in &summary.comparison_base_lags {
                lines.push(format!("> {}", comparison_base_lag_warning(lag)));
            }
            lines.push(String::new());
            lines.push(format!(
                "**Historical comparison:** {}",
                historical_comparison_text(summary.branch_comparison, false)
            ));
            push_set_filter_footer(&mut lines, summary.set);
        }
        push_warning(&mut lines, input.warning);
        return finish(&lines);
    }

    // The findings are already globally ranked by descending magnitude, so the leading
    // `limit` are the top movers. When any were dropped, name the total so a reader
    // knows to reach for the full report (the total exceeds `limit`, which is at least
    // one, so the total is at least two and "findings" is unconditionally plural).
    if input.findings.len() > limit.get() {
        lines.push(String::new());
        lines.push(format!(
            "> Showing the top {limit} of {} findings by magnitude.",
            input.findings.len()
        ));
    }

    // Both modes draw a per-finding chart, matching the full reports; the scope differs
    // (history walks the whole series, branch charts the baseline and recent tail).
    //
    // Comparison-base warnings are per-set metadata, but the summary flattens the set
    // grouping, so surface each affected set's warnings once — immediately before that
    // set's first retained finding.
    let scope = chart_scope(input.mode);
    let mut contextualized_sets: HashSet<&DiscriminantSet> = HashSet::new();
    for finding in input.findings.iter().take(limit.get()) {
        let summary = input
            .sets
            .iter()
            .find(|summary| *summary.set == finding.set);
        if contextualized_sets.insert(&finding.set)
            && let Some(summary) = summary
        {
            for lag in &summary.comparison_base_lags {
                lines.push(String::new());
                lines.push(format!("> {}", comparison_base_lag_warning(lag)));
            }
            if input.mode == AnalysisMode::Branch {
                lines.push(String::new());
                lines.push(format!(
                    "> **Historical comparison for {}:** {}",
                    set_label(summary.set),
                    historical_comparison_text(
                        summary.branch_comparison,
                        has_findings_in_historical_comparison(summary),
                    )
                ));
            }
        }
        if summary.is_some_and(|summary| !is_primary_finding(summary, finding)) {
            lines.push(String::new());
            lines.push(
                "> **Outside the historical comparison:** This excursion did not meet the \
                 shared-history requirements of the compared family."
                    .to_owned(),
            );
        }
        push_finding_markdown(&mut lines, finding, "##", scope);
        push_set_filter_footer(&mut lines, &finding.set);
    }
    push_warning(&mut lines, input.warning);
    finish(&lines)
}

/// Appends the discriminant-set filter flags as a de-emphasized footer on a summary
/// finding, after its chart. The summary drops the per-set grouping to stay within a
/// downstream size cap, so without this a reader could not tell which partition a
/// finding came from — or, when the same benchmark moved in several sets, tell the
/// otherwise near-identical blocks apart. The flags trail the block rather than lead
/// it because they are reference material for a follow-up query, not the headline.
fn push_set_filter_footer(lines: &mut Vec<String>, set: &DiscriminantSet) {
    lines.push(String::new());
    lines.push(format!("_Filter:_ `{}`", set_filter_flags(set)));
}

/// Appends one finding as a Markdown block mirroring the text report: the benchmark
/// id as a `heading` (a chapter title), then a bold headline, the shared detail line,
/// an optional blessing note, and the metric chart in a fenced `text` block so it
/// survives Markdown rendering, scoped per [`ChartScope`].
/// `heading` carries the ATX prefix (`##`/`###`) so the block nests correctly —
/// top-level in the summary, one level under the set heading in the full report.
fn push_finding_markdown(
    lines: &mut Vec<String>,
    finding: &Finding,
    heading: &str,
    scope: ChartScope,
) {
    lines.push(String::new());

    // The benchmark id is a heading of its own, so a long id reads as a chapter title
    // rather than crowding the change headline that follows.
    lines.push(format!("{heading} `{}`", describe_id(&finding.id)));

    if let Some(branch) = &finding.branch {
        let relation = branch_relation(finding.kind, finding.direction);
        lines.push(format!(
            "**{}** `{}` - {relation} {} current-base observations",
            format_value(finding.latest),
            finding.kind.as_str(),
            branch.reference_count,
        ));
    } else {
        lines.push(format!(
            "**{}** `{}`",
            format_percent(finding.relative_delta),
            finding.kind.as_str(),
        ));
    }

    lines.push(String::new());
    lines.push(detail_text(finding));

    if let Some(blessing) = blessing_text(finding) {
        lines.push(String::new());
        lines.push(blessing);
    }
    if let Some(context) = previous_regime_text(finding) {
        lines.push(String::new());
        lines.push(context);
    }

    if let Some(chart) = scoped_chart(finding, scope) {
        lines.push(String::new());
        lines.push("```text".to_owned());
        lines.push(chart);
        lines.push("```".to_owned());
    }
}

fn branch_relation(kind: MetricKind, direction: Direction) -> &'static str {
    match (kind, direction) {
        (MetricKind::WallTime | MetricKind::ProcessorTime, Direction::Regression) => {
            "slower than all"
        }
        (MetricKind::WallTime | MetricKind::ProcessorTime, Direction::Improvement) => {
            "faster than all"
        }
        (_, Direction::Regression) => "higher than all",
        (_, Direction::Improvement) => "lower than all",
    }
}

// Pure serialization glue, fully exercised by `json_report_is_structured` and
// `report_renders_direction_labels` in regular CI. Skipped for mutation only: the
// empty-string mutant is caught fast by those lib tests locally, but tips the 60s
// cargo-mutants timeout on the slower Windows shards.
#[cfg_attr(test, mutants::skip)]
fn render_json(input: &ReportInput<'_>) -> String {
    let coverage = Coverage::from_census(&input.census);
    let outcome = AnalysisOutcome::from_analysis(input.notable, &coverage);
    let sets = input
        .sets
        .iter()
        .map(|summary| JsonSet {
            engine: summary.set.engine.as_str(),
            target_triple: summary.set.target_triple.as_str(),
            machine_key: summary.set.machine_key.as_str(),
            runs: summary.runs,
            series: summary.series,
            regressions: count_direction(&summary.findings, Direction::Regression),
            improvements: input
                .report_improvements
                .then(|| count_direction(&summary.findings, Direction::Improvement)),
            comparison_base_lags: &summary.comparison_base_lags,
            branch_comparison: summary.branch_comparison.map(JsonBranchComparison::from),
        })
        .collect();

    let report = JsonReport {
        project: input.project,
        tip_commit: input.tip_commit,
        tip_dirty: input.tip_dirty,
        mode: input.mode,
        outcome: outcome.as_str(),
        notable: input.notable,
        runs: input.runs,
        series: input.series,
        regressions: count_top(input.findings, Direction::Regression),
        improvements: input
            .report_improvements
            .then(|| count_top(input.findings, Direction::Improvement)),
        ghosts_excluded: input.ghosts_excluded,
        census: JsonCensus::from_coverage(&coverage),
        hint: input.hint,
        warning: input.warning,
        findings: input
            .findings
            .iter()
            .map(JsonFinding::from_finding)
            .collect(),
        sets,
    };
    // The report is built from plain structs whose only numbers are finite (or
    // serialized as `null` by serde_json), so serialization cannot fail.
    serde_json::to_string_pretty(&report).expect("report structures always serialize to JSON")
}

/// The lowercase label for a change direction.
fn direction_label(direction: Direction) -> &'static str {
    match direction {
        Direction::Regression => "regression",
        Direction::Improvement => "improvement",
    }
}

/// The lowercase label for the detector that produced a finding.
fn method_label(method: FindingMethod) -> &'static str {
    match method {
        FindingMethod::ChangePoint => "change point",
        FindingMethod::Drift => "drift",
        FindingMethod::BranchExcursion => "branch excursion",
    }
}

/// Renders a benchmark identity as `package/group/case/value`, omitting absent
/// parts. The package-qualified form keeps benchmarks with the same `module_path`
/// in different packages distinguishable in reports.
fn describe_id(id: &BenchmarkId) -> String {
    id.qualified()
}

/// Formats a measured value for human-readable display.
///
/// Integer-valued counts print whole. Other values keep four significant figures,
/// counting the integer-part digits toward the four, so the whole number before
/// the decimal point is never truncated: `0.20970324` becomes `0.2097`,
/// `96.7664` becomes `96.77`, `0.000001234` keeps all four (`0.000001234`), and a
/// large `1234567.89` drops its fraction entirely (`1234568`). Trailing zeros are
/// trimmed. The machine-readable JSON keeps full precision.
#[must_use]
pub fn format_value(value: f64) -> String {
    if value.fract().abs() <= f64::EPSILON {
        return format!("{value:.0}");
    }
    let decimals = significant_decimals(value.abs());
    let formatted = format!("{value:.decimals$}");
    trim_trailing_zeros(&formatted)
}

/// Formats a measured value with an explicit sign for human-readable display.
fn format_signed_value(value: f64) -> String {
    let sign = if value.is_sign_negative() { "-" } else { "+" };
    format!("{sign}{}", format_value(value.abs()))
}

/// The number of decimal places that yields four significant figures for
/// `magnitude` (a non-negative, non-integer value), while always keeping every
/// integer-part digit.
fn significant_decimals(magnitude: f64) -> usize {
    // Order of magnitude `e` with 10^e <= magnitude < 10^(e+1), found by a bounded
    // search over the exponent range that covers realistic measurement magnitudes.
    let mut exponent: i32 = -12;
    for candidate in (-12..=12).rev() {
        if magnitude >= 10_f64.powi(candidate) {
            exponent = candidate;
            break;
        }
    }
    // Four significant figures: the least significant digit sits three places below
    // the most significant one. Negative exponents (values below one) add decimal
    // places; large exponents need none.
    let places = 3_i32.saturating_sub(exponent).max(0);
    usize::try_from(places).unwrap_or(0)
}

/// Trims trailing zeros (and a dangling decimal point) from a formatted decimal.
fn trim_trailing_zeros(formatted: &str) -> String {
    if formatted.contains('.') {
        formatted
            .trim_end_matches('0')
            .trim_end_matches('.')
            .to_owned()
    } else {
        formatted.to_owned()
    }
}

/// Formats a relative delta as a signed percentage with two decimals.
fn format_percent(relative_delta: f64) -> String {
    format!("{:+.2}%", relative_delta * 100.0)
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    #![allow(clippy::indexing_slicing, reason = "panic is fine in tests")]

    use std::sync::TryLockError;
    use std::thread;

    use cbh_detect::{SeriesValue, Testability, UnjudgedReason};
    use cbh_model::{Engine, MetricKind};
    use nonempty::nonempty;

    use super::*;

    fn discriminant_set() -> DiscriminantSet {
        DiscriminantSet {
            engine: Engine::Callgrind,
            target_triple: "x86_64-unknown-linux-gnu".into(),
            machine_key: "m1".into(),
        }
    }

    fn regression() -> Finding {
        Finding {
            set: discriminant_set(),
            id: BenchmarkId::new(nonempty![
                "nm".to_owned(),
                "nm::observe".to_owned(),
                "pull".to_owned(),
            ]),
            kind: MetricKind::InstructionCount,
            method: FindingMethod::ChangePoint,
            direction: Direction::Regression,
            baseline: 100.0,
            latest: 130.0,
            delta: 30.0,
            relative_delta: 0.30,
            commit: Some("deadbee".to_owned()),
            window_start_commit: None,
            blessed_at: None,
            blessed_commit_time: None,
            series: Vec::new(),
            comparison_base_index: None,
            chart_base_ref: None,
            branch: None,
        }
    }

    fn branch_regression(included_in_historical_comparison: bool) -> Finding {
        let mut finding = regression();
        finding.method = FindingMethod::BranchExcursion;
        finding.baseline = 110.0;
        finding.latest = 130.0;
        finding.delta = 20.0;
        finding.relative_delta = 20.0 / 110.0;
        finding.branch = Some(BranchExcursion {
            reference_count: 20,
            reference_min: 99.0,
            reference_max: 110.0,
            excess: 20.0,
            relative_excess: 20.0 / 110.0,
            current_regime_start: Some("abcdef0123456789".to_owned()),
            matches_previous_regime: false,
            included_in_historical_comparison,
        });
        finding
    }

    /// A census in which every one of `series` series was judged — the healthy shape
    /// a rendering fixture carries unless it is exercising the unjudged case.
    fn judged_census(series: usize) -> SeriesCensus {
        let mut census = SeriesCensus::default();
        for _ in 0..series {
            census.record(Testability::Judged);
        }
        census
    }

    /// Wraps a findings slice into a single-set report over `set`.
    fn single_set_input<'a>(
        project: &'a str,
        set: &'a DiscriminantSet,
        findings: &'a [Finding],
        summaries: &'a mut Vec<SetSummary<'a>>,
    ) -> ReportInput<'a> {
        summaries.push(SetSummary {
            set,
            runs: findings.len().saturating_add(3),
            series: findings.len().max(1),
            findings: findings.iter().collect(),
            comparison_base_lags: Vec::new(),
            branch_comparison: None,
        });
        ReportInput {
            project,
            tip_commit: "1234567890abcdef1234",
            tip_dirty: false,
            mode: AnalysisMode::History,
            notable: !findings.is_empty(),
            runs: findings.len().saturating_add(3),
            series: findings.len().max(1),
            commit_span: None,
            report_improvements: true,
            findings,
            sets: summaries,
            hint: None,
            warning: None,
            ghosts_excluded: 0,
            census: judged_census(findings.len().max(1)),
        }
    }

    /// Wraps a findings slice into a report with no per-set breakdown, for the
    /// summary tests (which render a flat, globally-ranked list and never read
    /// `sets`). Regressions only, so the header carries a single tally.
    fn flat_input(findings: &[Finding]) -> ReportInput<'_> {
        ReportInput {
            project: "folo",
            tip_commit: "1234567890abcdef1234",
            tip_dirty: false,
            mode: AnalysisMode::History,
            notable: !findings.is_empty(),
            runs: findings.len().saturating_add(3),
            series: findings.len().max(1),
            commit_span: None,
            report_improvements: false,
            findings,
            sets: &[],
            hint: None,
            warning: None,
            ghosts_excluded: 0,
            census: judged_census(findings.len().max(1)),
        }
    }

    /// A regression finding named `name` (a single-segment id) with the given
    /// relative move, so a summary test can tell which findings the render kept by
    /// their distinctive ids and magnitudes.
    fn named_regression(name: &str, relative_delta: f64) -> Finding {
        Finding {
            id: BenchmarkId::new(nonempty![name.to_owned()]),
            relative_delta,
            ..regression()
        }
    }

    /// An improvement finding named `name` with the given (negative) relative move, so
    /// a summary test can exercise the optional improvements tally alongside
    /// regressions.
    fn named_improvement(name: &str, relative_delta: f64) -> Finding {
        Finding {
            id: BenchmarkId::new(nonempty![name.to_owned()]),
            direction: Direction::Improvement,
            relative_delta,
            ..regression()
        }
    }

    /// Five regressions in the descending-magnitude order the ranking produces, so a
    /// summary render can cap the leading few and drop the tail.
    fn ranked_five() -> Vec<Finding> {
        vec![
            named_regression("mover_a", 0.50),
            named_regression("mover_b", 0.40),
            named_regression("mover_c", 0.30),
            named_regression("dropped_d", 0.20),
            named_regression("dropped_e", 0.10),
        ]
    }

    #[test]
    fn markdown_summary_caps_to_the_limit_and_keeps_full_totals() {
        let findings = ranked_five();
        let input = flat_input(&findings);

        let report = render_markdown_summary(&input, NonZero::new(3).unwrap());

        // The header tally counts every finding, not the retained subset.
        assert!(report.contains("- Regressions: 5"), "{report}");
        // The truncation note names how many of the total are shown.
        assert!(
            report.contains("> Showing the top 3 of 5 findings by magnitude."),
            "{report}"
        );
        // The three largest movers are kept; the two smallest are dropped.
        assert!(report.contains("mover_a"), "{report}");
        assert!(report.contains("mover_b"), "{report}");
        assert!(report.contains("mover_c"), "{report}");
        assert!(!report.contains("dropped_d"), "{report}");
        assert!(!report.contains("dropped_e"), "{report}");
        // The per-set breakdown the full Markdown report groups by is dropped: a
        // summary is a single flat list, so no `## engine/triple/machine` heading.
        assert!(!report.contains("## callgrind"), "{report}");
    }

    #[test]
    fn markdown_summary_omits_the_note_when_within_the_limit() {
        let findings = ranked_five();
        let input = flat_input(&findings);

        // A limit at or above the finding count keeps every finding and shows no
        // truncation note.
        let report = render_markdown_summary(&input, NonZero::new(5).unwrap());

        assert!(!report.contains("Showing the top"), "{report}");
        assert!(report.contains("mover_a"), "{report}");
        assert!(report.contains("dropped_e"), "{report}");

        // A limit beyond the count behaves the same as an exact fit.
        let generous = render_markdown_summary(&input, NonZero::new(20).unwrap());
        assert_eq!(generous, report);
    }

    #[test]
    fn markdown_summary_with_no_findings_matches_the_empty_message() {
        let input = ReportInput {
            hint: Some("Found 2 stored runs ... dirty snapshots"),
            ..flat_input(&[])
        };

        let report = render_markdown_summary(&input, DEFAULT_SUMMARY_LIMIT);

        assert!(report.contains("No notable changes detected."), "{report}");
        assert!(report.contains("Found 2 stored runs"), "{report}");
        assert!(!report.contains("Showing the top"), "{report}");
    }

    #[test]
    fn markdown_summary_draws_a_fenced_chart_in_history_mode() {
        // `flat_input` fixes the mode to `history`, where a retained finding with a
        // series is rendered with its per-commit chart — so the summary reuses the full
        // report's chart-in-history behaviour rather than dropping it.
        let findings = vec![regression_with_series()];
        let input = flat_input(&findings);

        let report = render_markdown_summary(&input, DEFAULT_SUMMARY_LIMIT);

        // The chart sits inside a fenced `text` block (its axis marker proves a chart was
        // drawn) and carries no ANSI escapes.
        assert!(report.contains("```text"), "{report}");
        assert!(report.contains('┤') || report.contains('┼'), "{report}");
        assert!(!report.contains('\u{1b}'), "{report}");
        // The set-filter footer trails the fenced chart, not precedes it: it is
        // reference material for a follow-up query, not the headline.
        let chart_close = report.rfind("```").expect("fenced chart present");
        let footer_at = report.find("_Filter:_").expect("filter footer present");
        assert!(
            footer_at > chart_close,
            "the filter footer must follow the chart: {report}"
        );
    }

    #[test]
    fn markdown_summary_footers_each_finding_with_its_set_filter() {
        // The summary drops the per-set grouping, so each finding carries the discriminant-filter
        // flags that isolate its partition as a trailing footer — enough to query that
        // exact set without leading the block.
        let findings = vec![named_regression("mover_a", 0.50)];
        let input = flat_input(&findings);

        let report = render_markdown_summary(&input, DEFAULT_SUMMARY_LIMIT);

        let footer = "_Filter:_ `--engine callgrind --target-triple x86_64-unknown-linux-gnu --machine-key m1`";
        assert!(report.contains(footer), "{report}");
        // The footer trails the finding headline rather than leading it.
        let headline_at = report.find("mover_a").expect("headline present");
        let footer_at = report.find(footer).expect("footer present");
        assert!(
            footer_at > headline_at,
            "footer must follow the finding: {report}"
        );
    }

    #[test]
    fn markdown_summary_distinguishes_the_same_benchmark_across_sets() {
        // The same benchmark id regresses in two discriminant sets. Without the set
        // footer their flat summary blocks would be indistinguishable; with it, each
        // names the filter that isolates its own partition.
        let linux = named_regression("shared", 0.50);
        let windows = Finding {
            set: DiscriminantSet {
                engine: Engine::Criterion,
                target_triple: "x86_64-pc-windows-msvc".into(),
                machine_key: "m1".into(),
            },
            ..named_regression("shared", 0.40)
        };
        let findings = vec![linux, windows];
        let input = flat_input(&findings);

        let report = render_markdown_summary(&input, DEFAULT_SUMMARY_LIMIT);

        assert!(
            report.contains(
                "--engine callgrind --target-triple x86_64-unknown-linux-gnu --machine-key m1"
            ),
            "{report}"
        );
        assert!(
            report.contains(
                "--engine criterion --target-triple x86_64-pc-windows-msvc --machine-key m1"
            ),
            "{report}"
        );
    }

    #[test]
    fn markdown_summary_reports_improvements_only_when_enabled() {
        // Magnitudes descend so the list already reads as globally ranked; two
        // regressions and three improvements are interleaved.
        let findings = vec![
            named_regression("reg_a", 0.50),
            named_improvement("imp_b", -0.40),
            named_regression("reg_c", 0.30),
            named_improvement("imp_d", -0.20),
            named_improvement("imp_e", -0.10),
        ];

        // Disabled (the `flat_input` default): the header carries no improvements tally.
        let without = render_markdown_summary(&flat_input(&findings), NonZero::new(2).unwrap());
        assert!(!without.contains("Improvements:"), "{without}");

        // Enabled: the header carries an improvements tally counted from *every*
        // finding — like the regressions tally — even though the cap of two drops
        // `imp_d` and `imp_e` from the rendered list.
        let input = ReportInput {
            report_improvements: true,
            ..flat_input(&findings)
        };
        let with = render_markdown_summary(&input, NonZero::new(2).unwrap());
        assert!(with.contains("- Regressions: 2"), "{with}");
        assert!(with.contains("- Improvements: 3"), "{with}");
    }

    #[test]
    fn format_from_name_recognizes_known_formats() {
        assert_eq!(ReportFormat::from_name("text"), Some(ReportFormat::Text));
        assert_eq!(ReportFormat::from_name("json"), Some(ReportFormat::Json));
        assert_eq!(
            ReportFormat::from_name("markdown"),
            Some(ReportFormat::Markdown)
        );
        assert_eq!(ReportFormat::from_name("md"), Some(ReportFormat::Markdown));
        assert_eq!(ReportFormat::from_name("yaml"), None);
    }

    #[test]
    fn text_report_with_no_findings_is_explicit() {
        let input = ReportInput {
            project: "folo",
            tip_commit: "1234567890abcdef1234",
            tip_dirty: false,
            mode: AnalysisMode::History,
            notable: false,
            runs: 3,
            series: 1,
            commit_span: None,
            report_improvements: false,
            findings: &[],
            sets: &[],
            hint: None,
            warning: None,
            ghosts_excluded: 0,
            census: judged_census(1),
        };
        let report = render(&input, ReportFormat::Text, false);
        assert!(report.contains("Analyzed project folo"), "{report}");
        assert!(report.contains("regressions: 0"), "{report}");
        assert!(report.contains("No notable changes detected."), "{report}");
        // The verdict is qualified by how much of the suite it covers, so the silence
        // cannot be read as an all-clear over series that were never looked at.
        assert!(
            report.contains("in-scope series judged: 1 of 1"),
            "{report}"
        );
        assert!(
            report
                .contains("Judged 1 of 1 in-scope series; no reportable move survived the gates."),
            "{report}"
        );
        assert!(
            !report.contains("Not judged"),
            "a fully judged analysis stays quiet about exclusions: {report}"
        );
    }

    /// A census of `judged` judged series plus the given unjudged breakdown.
    fn census_of(judged: usize, unjudged: &[(UnjudgedReason, usize)]) -> SeriesCensus {
        let mut census = judged_census(judged);
        for &(reason, count) in unjudged {
            census.record_unjudged(reason, count);
        }
        census
    }

    #[test]
    fn silence_names_the_series_it_did_not_judge() {
        // Silence over a partly-judged suite must disclose the gap and its causes on
        // every human surface, or a repository can go blind without the report saying so.
        let census = census_of(
            4,
            &[
                (UnjudgedReason::Ghost, 2),
                (UnjudgedReason::TooFewPoints, 3),
            ],
        );
        let input = ReportInput {
            census,
            ..flat_input(&[])
        };

        let text = render(&input, ReportFormat::Text, false);
        assert!(text.contains("in-scope series judged: 4 of 7"), "{text}");
        assert!(
            text.contains("Judged 4 of 7 in-scope series; no reportable move survived the gates."),
            "{text}"
        );
        assert!(
            text.contains(
                "Not judged: 2 series not measured at the analyzed context commit; \
                 3 series with too few points in the analyzed window."
            ),
            "{text}"
        );

        let markdown = render(&input, ReportFormat::Markdown, false);
        assert!(
            markdown.contains("- In-scope series judged: 4 of 7"),
            "{markdown}"
        );
        assert!(markdown.contains("Not judged: 2 series"), "{markdown}");

        let summary = render_markdown_summary(&input, DEFAULT_SUMMARY_LIMIT);
        assert!(
            summary.contains("- In-scope series judged: 4 of 7"),
            "{summary}"
        );
        assert!(summary.contains("Not judged: 2 series"), "{summary}");
    }

    #[test]
    fn a_run_whose_only_shortfall_is_ghosts_reads_as_a_full_all_clear() {
        // The contradiction this guards against: an unqualified all-clear over a ratio
        // reading as partial coverage, so one silent report tells a reader who trusts the
        // headline and a reader who trusts the ratio opposite things. The ghosts are
        // still disclosed, in the breakdown that lists what went unjudged.
        let input = ReportInput {
            census: census_of(3, &[(UnjudgedReason::Ghost, 2)]),
            ..flat_input(&[])
        };

        for (surface, rendering) in [
            ("text", render(&input, ReportFormat::Text, false)),
            ("markdown", render(&input, ReportFormat::Markdown, false)),
            (
                "summary",
                render_markdown_summary(&input, DEFAULT_SUMMARY_LIMIT),
            ),
        ] {
            assert!(
                rendering.contains("No notable changes detected."),
                "{surface}: {rendering}"
            );
            assert!(
                rendering.contains(
                    "Judged 3 of 3 in-scope series; no reportable move survived the gates."
                ),
                "{surface}: {rendering}"
            );
            assert!(
                rendering
                    .contains("Not judged: 2 series not measured at the analyzed context commit."),
                "{surface}: {rendering}"
            );
        }
    }

    #[test]
    fn a_report_that_judged_nothing_says_so_plainly() {
        // The dangerous case: every series was dropped or too short, so "no notable
        // changes" is not evidence about the code at all. The report must say that
        // outright rather than let the reader infer an all-clear.
        let input = ReportInput {
            census: census_of(0, &[(UnjudgedReason::TooFewPoints, 7)]),
            ..flat_input(&[])
        };

        let text = render(&input, ReportFormat::Text, false);
        assert!(text.contains("in-scope series judged: 0 of 7"), "{text}");
        assert!(
            text.contains("this silence is not evidence that nothing moved"),
            "{text}"
        );
        assert!(
            !text.contains("no reportable move survived the gates"),
            "nothing was measured against the floor: {text}"
        );
        assert!(
            !text.contains("No notable changes detected"),
            "a run that judged nothing claims no all-clear: {text}"
        );

        let markdown = render(&input, ReportFormat::Markdown, false);
        assert!(
            markdown.contains("this silence is not evidence that nothing moved"),
            "{markdown}"
        );
        assert!(
            !markdown.contains("No notable changes detected"),
            "{markdown}"
        );
    }

    #[test]
    fn an_empty_analysis_leaves_the_coverage_field_to_the_hint() {
        // With no series at all there is no coverage ratio to report — a "0 of 0" ratio
        // is noise — and the verdict already states that nothing was analyzed, so the
        // lead line is not read as an all-clear over a suite that was never looked at.
        // The empty-outcome hint carries the explanation, and it carries it once.
        let input = ReportInput {
            census: SeriesCensus::default(),
            hint: Some("Found 2 stored runs ... dirty snapshots"),
            ..flat_input(&[])
        };

        let text = render(&input, ReportFormat::Text, false);
        assert!(!text.contains("series judged"), "{text}");
        assert!(!text.contains("Judged"), "{text}");
        assert!(
            text.contains("Nothing was analyzed, so no change could be detected."),
            "{text}"
        );
        assert!(
            !text.contains("this run tested nothing"),
            "the verdict and the hint already say it: {text}"
        );
        assert!(text.contains("Found 2 stored runs"), "{text}");

        let markdown = render(&input, ReportFormat::Markdown, false);
        assert!(!markdown.contains("Series judged"), "{markdown}");
        assert!(!markdown.contains("this run tested nothing"), "{markdown}");
        assert!(markdown.contains("Found 2 stored runs"), "{markdown}");
    }

    /// Every distinct coverage situation, with the phrase each human surface must
    /// carry and the phrase it must not. Shared by the text, Markdown and summary
    /// renderings so a coverage state cannot degrade on one surface while the others
    /// stay correct.
    fn silent_surface_cases() -> Vec<(&'static str, SeriesCensus, &'static str, &'static str)> {
        vec![
            (
                "absent census",
                SeriesCensus::default(),
                "Nothing was analyzed, so no change could be detected.",
                "No notable changes detected.",
            ),
            (
                "every series a ghost",
                census_of(0, &[(UnjudgedReason::Ghost, 4)]),
                "Nothing was in scope at the analyzed context commit, so nothing was judged.",
                "No notable changes detected",
            ),
            (
                "nothing judged",
                census_of(0, &[(UnjudgedReason::TooFewPoints, 2)]),
                "Nothing was judged, so no change could be detected either way.",
                "No notable changes detected",
            ),
            (
                "partial: too few points",
                census_of(2, &[(UnjudgedReason::TooFewPoints, 1)]),
                "with too few points in the analyzed window",
                "No notable changes detected.",
            ),
            (
                "partial: too few points since blessing",
                census_of(2, &[(UnjudgedReason::TooFewPointsSinceBlessing, 1)]),
                "with too few points since being blessed",
                "No notable changes detected.",
            ),
            (
                "partial: not measured on branch",
                census_of(2, &[(UnjudgedReason::NotMeasuredOnBranch, 1)]),
                "not measured on the branch",
                "No notable changes detected.",
            ),
            (
                "partial: too few base commits",
                census_of(2, &[(UnjudgedReason::TooFewBaseCommits, 1)]),
                "with too few base-ref commits to compare against",
                "No notable changes detected.",
            ),
            (
                "partial: too few base commits since blessing",
                census_of(2, &[(UnjudgedReason::TooFewBaseCommitsSinceBlessing, 1)]),
                "with too few base-ref commits remaining since being blessed",
                "No notable changes detected.",
            ),
            (
                "partial: unresolved current base regime",
                census_of(2, &[(UnjudgedReason::CurrentBaseRegimeUnresolved, 1)]),
                "whose current base regime is unresolved",
                "No notable changes detected.",
            ),
            (
                "mixed reasons",
                census_of(
                    2,
                    &[
                        (UnjudgedReason::Ghost, 1),
                        (UnjudgedReason::NotMeasuredOnBranch, 2),
                    ],
                ),
                "Not judged: 1 series not measured at the analyzed context commit; 2 series \
                 not measured on the branch.",
                "No notable changes detected.",
            ),
            (
                "full coverage",
                census_of(3, &[]),
                "No notable changes detected.",
                "Not judged",
            ),
            (
                "full coverage with ghosts only",
                census_of(3, &[(UnjudgedReason::Ghost, 2)]),
                "No notable changes detected.",
                "among the series that were judged",
            ),
        ]
    }

    #[test]
    fn every_silent_surface_reports_the_same_coverage() {
        for (name, census, expected, forbidden) in silent_surface_cases() {
            let input = ReportInput {
                census,
                ..flat_input(&[])
            };
            let renderings = [
                ("text", render(&input, ReportFormat::Text, false)),
                ("markdown", render(&input, ReportFormat::Markdown, false)),
                (
                    "summary",
                    render_markdown_summary(&input, DEFAULT_SUMMARY_LIMIT),
                ),
            ];
            for (surface, rendering) in renderings {
                assert!(
                    rendering.contains(expected),
                    "{name} on {surface} states its coverage: {rendering}"
                );
                assert!(
                    !rendering.contains(forbidden),
                    "{name} on {surface} overstates its coverage: {rendering}"
                );
            }
        }
    }

    #[test]
    fn a_multi_metric_ghost_counts_once_per_metric_series() {
        // The census counts metric series, so a single benchmark carrying two metrics
        // leaves two ghosts behind — the report must reconcile against that unit, not
        // against a benchmark tally.
        let input = ReportInput {
            census: census_of(1, &[(UnjudgedReason::Ghost, 2)]),
            ghosts_excluded: 1,
            ..flat_input(&[])
        };

        let text = render(&input, ReportFormat::Text, false);
        assert!(text.contains("in-scope series judged: 1 of 1"), "{text}");
        assert!(
            text.contains("Not judged: 2 series not measured at the analyzed context commit."),
            "{text}"
        );

        let parsed: serde_json::Value =
            serde_json::from_str(&render(&input, ReportFormat::Json, false)).unwrap();
        assert_eq!(parsed["census"]["total"], 3);
        assert_eq!(parsed["census"]["in_scope"], 1);
        assert_eq!(parsed["census"]["coverage"], "full");
        assert_eq!(
            parsed["ghosts_excluded"], 1,
            "the benchmark tally is a separate unit from the census's series"
        );
    }

    #[test]
    fn an_analysis_with_nothing_in_scope_states_no_ratio_on_any_surface() {
        // Every series was a ghost, so the ratio would read "0 of 0" — a figure that
        // reports no coverage while looking like a measurement. The verdict carries the
        // meaning instead, on text and Markdown alike.
        let input = ReportInput {
            census: census_of(0, &[(UnjudgedReason::Ghost, 3)]),
            ghosts_excluded: 3,
            ..flat_input(&[])
        };

        let text = render(&input, ReportFormat::Text, false);
        assert!(!text.contains("in-scope series judged"), "{text}");
        assert!(
            text.contains("Nothing was in scope at the analyzed context commit"),
            "{text}"
        );

        let markdown = render(&input, ReportFormat::Markdown, false);
        assert!(!markdown.contains("In-scope series judged"), "{markdown}");
        assert!(
            markdown.contains("Nothing was in scope at the analyzed context commit"),
            "{markdown}"
        );

        let parsed: serde_json::Value =
            serde_json::from_str(&render(&input, ReportFormat::Json, false)).unwrap();
        assert_eq!(parsed["census"]["in_scope"], 0);
        assert_eq!(parsed["census"]["coverage"], "nothing_in_scope");
    }

    #[test]
    fn json_census_carries_the_coverage_state_for_every_shape() {
        // Automation gates on `coverage`, so every distinct situation must reach the
        // JSON with its own state and an in-scope denominator that excludes ghosts.
        let cases = [
            (
                "absent census",
                SeriesCensus::default(),
                "no_series",
                "nothing_in_scope",
                0,
            ),
            (
                "every series a ghost",
                census_of(0, &[(UnjudgedReason::Ghost, 4)]),
                "nothing_in_scope",
                "nothing_in_scope",
                0,
            ),
            (
                "nothing judged",
                census_of(0, &[(UnjudgedReason::NotMeasuredOnBranch, 2)]),
                "nothing_judged",
                "insufficient_baseline",
                2,
            ),
            (
                "partial",
                census_of(2, &[(UnjudgedReason::TooFewBaseCommits, 1)]),
                "partial",
                "partial",
                3,
            ),
            ("full", census_of(2, &[]), "full", "clean", 2),
        ];

        for (name, census, state, outcome, in_scope) in cases {
            let input = ReportInput {
                census,
                ..flat_input(&[])
            };
            let json = render(&input, ReportFormat::Json, false);
            let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
            assert_eq!(parsed["census"]["coverage"], state, "{name}: {json}");
            assert_eq!(parsed["outcome"], outcome, "{name}: {json}");
            assert_eq!(parsed["census"]["in_scope"], in_scope, "{name}: {json}");
        }
    }

    #[test]
    fn json_outcome_prioritizes_findings_over_partial_coverage() {
        let findings = [regression()];
        let input = ReportInput {
            notable: true,
            findings: &findings,
            census: census_of(1, &[(UnjudgedReason::TooFewPoints, 1)]),
            ..flat_input(&findings)
        };
        let json = render(&input, ReportFormat::Json, false);
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed["outcome"], "findings", "{json}");
        assert_eq!(parsed["census"]["coverage"], "partial", "{json}");
    }

    #[test]
    fn json_census_accounts_for_every_series_by_reason() {
        // The census is an interface: automation reads it to tell a genuine all-clear
        // from a blind run, so its shape stays self-contained and its totals add up.
        let input = ReportInput {
            census: census_of(
                4,
                &[
                    (UnjudgedReason::Ghost, 2),
                    (UnjudgedReason::TooFewBaseCommits, 1),
                ],
            ),
            ..flat_input(&[])
        };

        let json = render(&input, ReportFormat::Json, false);
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        let census = &parsed["census"];
        assert_eq!(census["total"], 7, "{json}");
        assert_eq!(census["judged"], 4, "{json}");
        assert_eq!(census["unjudged"], 3, "{json}");
        assert_eq!(census["reasons"][0]["reason"], "ghost", "{json}");
        assert_eq!(census["reasons"][0]["count"], 2, "{json}");
        assert_eq!(
            census["reasons"][1]["reason"], "too_few_base_commits",
            "{json}"
        );
        assert_eq!(census["reasons"][1]["count"], 1, "{json}");
        assert!(census["reasons"][2].is_null(), "{json}");

        // A fully judged analysis still carries the block, with an empty breakdown, so
        // a consumer never has to distinguish "absent" from "nothing to report".
        let judged = ReportInput {
            census: judged_census(4),
            ..flat_input(&[])
        };
        let parsed: serde_json::Value =
            serde_json::from_str(&render(&judged, ReportFormat::Json, false)).unwrap();
        assert_eq!(parsed["census"]["judged"], 4);
        assert_eq!(parsed["census"]["unjudged"], 0);
        assert_eq!(
            parsed["census"]["reasons"],
            serde_json::Value::Array(Vec::new())
        );
    }

    #[test]
    fn text_report_renders_hint_when_present() {
        let input = ReportInput {
            project: "folo",
            tip_commit: "1234567890abcdef1234",
            tip_dirty: false,
            mode: AnalysisMode::History,
            notable: false,
            runs: 0,
            series: 0,
            commit_span: None,
            report_improvements: false,
            findings: &[],
            sets: &[],
            hint: Some("Found 2 stored runs ... dirty snapshots"),
            warning: None,
            ghosts_excluded: 0,
            census: judged_census(0),
        };
        let report = render(&input, ReportFormat::Text, false);
        assert!(
            report.contains("Nothing was analyzed, so no change could be detected."),
            "{report}"
        );
        assert!(report.contains("Found 2 stored runs"), "{report}");
    }

    #[test]
    fn text_report_lists_a_finding() {
        let set = discriminant_set();
        let findings = vec![regression()];
        let mut summaries = Vec::new();
        let input = single_set_input("folo", &set, &findings, &mut summaries);
        let report = render(&input, ReportFormat::Text, false);
        assert!(report.contains("regressions: 1"), "{report}");
        assert!(
            report.contains("callgrind/x86_64-unknown-linux-gnu/m1"),
            "the set heading drops the redundant `Set ` prefix: {report}"
        );
        // The set header names the discriminant-filter flags that reproduce exactly this partition,
        // so a reader who spots a finding knows how to query it directly.
        assert!(
            report.contains(
                "  filter: --engine callgrind --target-triple x86_64-unknown-linux-gnu --machine-key m1"
            ),
            "{report}"
        );
        assert!(report.contains("+30.00%"), "{report}");
        assert!(!report.contains("[major]"), "{report}");
        // The benchmark id leads on its own chapter-title line; the change headline
        // that follows carries the metric, no longer the id.
        assert!(report.contains("nm/nm::observe/pull"), "{report}");
        assert!(report.contains("+30.00% instruction_count"), "{report}");
        // The report no longer surfaces a confidence figure anywhere.
        assert!(
            report.contains("regression via change point · 100 → 130"),
            "{report}"
        );
        assert!(!report.contains("confidence"), "{report}");
    }

    #[test]
    fn text_report_shows_per_set_counts_distinct_from_totals() {
        // Give the set tallies that differ from the top-level aggregate so the per-set
        // counts line is identifiable on its own — a blanked-out line would no longer
        // match, unlike when the set and total counts coincide.
        let set = discriminant_set();
        let findings = vec![regression()];
        let summaries = vec![SetSummary {
            set: &set,
            runs: 7,
            series: 5,
            findings: findings.iter().collect(),
            comparison_base_lags: Vec::new(),
            branch_comparison: None,
        }];
        let input = ReportInput {
            project: "folo",
            tip_commit: "1234567890abcdef1234",
            tip_dirty: false,
            mode: AnalysisMode::History,
            notable: true,
            runs: 99,
            series: 88,
            commit_span: None,
            report_improvements: true,
            findings: &findings,
            sets: &summaries,
            hint: None,
            warning: None,
            ghosts_excluded: 0,
            census: judged_census(88),
        };
        let report = render(&input, ReportFormat::Text, false);
        // The per-set counts line carries the set's own tallies, distinct from the
        // top-level totals (`runs: 99`). The series count is no longer surfaced.
        assert!(
            report.contains("  runs: 7  regressions: 1  improvements: 0"),
            "{report}"
        );
        assert!(!report.contains("series:"), "{report}");
    }

    #[test]
    fn branch_reports_use_range_and_historical_comparison_language() {
        let set = discriminant_set();
        let findings = [branch_regression(true)];
        let comparison = BranchComparison {
            set: set.clone(),
            evaluated_base_commits: 10,
            at_least_as_much: 3,
            series: 1,
        };
        let summaries = vec![SetSummary {
            set: &set,
            runs: 21,
            series: 1,
            findings: vec![&findings[0]],
            comparison_base_lags: Vec::new(),
            branch_comparison: Some(&comparison),
        }];
        let input = ReportInput {
            project: "folo",
            tip_commit: "1234567890abcdef1234",
            tip_dirty: false,
            mode: AnalysisMode::Branch,
            notable: true,
            runs: 21,
            series: 1,
            commit_span: None,
            report_improvements: true,
            findings: &findings,
            sets: &summaries,
            hint: None,
            warning: None,
            ghosts_excluded: 0,
            census: judged_census(1),
        };

        let text = render(&input, ReportFormat::Text, false);
        assert!(
            text.contains(
                "3 of 10 comparable base commits showed at least as much out-of-range movement as \
                 this branch (1 series compared)."
            ),
            "{text}"
        );
        assert!(
            text.contains("130 instruction_count - higher than all 20 current-base observations"),
            "{text}"
        );
        assert!(
            text.contains("current base range: 99-110 · branch excess: +20"),
            "{text}"
        );
        assert!(!text.contains("change point"), "{text}");

        let json: serde_json::Value =
            serde_json::from_str(&render(&input, ReportFormat::Json, false)).unwrap();
        assert_eq!(json["findings"][0]["method"], "branch_excursion");
        assert_eq!(json["findings"][0]["branch"]["reference_min"], 99.0);
        assert_eq!(json["sets"][0]["branch_comparison"]["at_least_as_much"], 3);

        let summary = render_markdown_summary(&input, DEFAULT_SUMMARY_LIMIT);
        assert!(
            summary.contains(
                "3 of 10 comparable base commits showed at least as much out-of-range movement"
            ),
            "{summary}"
        );
        assert!(
            !summary.contains("Outside the historical comparison"),
            "{summary}"
        );

        let markdown = render(&input, ReportFormat::Markdown, false);
        assert!(
            markdown.contains("**Historical comparison:** 3 of 10 comparable base commits"),
            "{markdown}"
        );
        assert!(
            !markdown.contains("Additional excursions outside this comparison"),
            "{markdown}"
        );
    }

    #[test]
    fn branch_comparison_is_rendered_even_without_findings() {
        let set = discriminant_set();
        let comparison = BranchComparison {
            set: set.clone(),
            evaluated_base_commits: 10,
            at_least_as_much: 10,
            series: 1,
        };
        let summaries = vec![SetSummary {
            set: &set,
            runs: 21,
            series: 2,
            findings: Vec::new(),
            comparison_base_lags: Vec::new(),
            branch_comparison: Some(&comparison),
        }];
        let input = ReportInput {
            project: "folo",
            tip_commit: "1234567890abcdef1234",
            tip_dirty: false,
            mode: AnalysisMode::Branch,
            notable: false,
            runs: 21,
            series: 2,
            commit_span: None,
            report_improvements: true,
            findings: &[],
            sets: &summaries,
            hint: None,
            warning: None,
            ghosts_excluded: 0,
            census: census_of(1, &[(UnjudgedReason::TooFewBaseCommits, 1)]),
        };

        let text = render(&input, ReportFormat::Text, false);
        assert!(
            text.contains(
                "The branch produced no reportable out-of-range movement in this comparison \
                 (1 series and 10 comparable base commits)."
            ),
            "{text}"
        );
        assert!(
            text.contains("No notable changes detected among the series that were judged."),
            "{text}"
        );
        assert!(text.contains("Not judged: 1 series"), "{text}");

        let markdown = render(&input, ReportFormat::Markdown, false);
        assert!(
            markdown.contains("No notable changes detected among the series that were judged."),
            "{markdown}"
        );
        assert!(markdown.contains("Not judged: 1 series"), "{markdown}");
        assert!(
            markdown.contains(
                "The branch produced no reportable out-of-range movement in this comparison \
                 (1 series and 10 comparable base commits)."
            ),
            "{markdown}"
        );

        let summary = render_markdown_summary(&input, DEFAULT_SUMMARY_LIMIT);
        assert!(
            summary.contains(
                "The branch produced no reportable out-of-range movement in this comparison \
                 (1 series and 10 comparable base commits)."
            ),
            "{summary}"
        );
        assert!(
            summary.contains("No notable changes detected among the series that were judged."),
            "{summary}"
        );
        assert!(summary.contains("Not judged: 1 series"), "{summary}");
    }

    #[test]
    fn quiet_branch_report_discloses_when_no_historical_family_exists() {
        let set = discriminant_set();
        let summaries = vec![SetSummary {
            set: &set,
            runs: 21,
            series: 1,
            findings: Vec::new(),
            comparison_base_lags: Vec::new(),
            branch_comparison: None,
        }];
        let input = ReportInput {
            project: "folo",
            tip_commit: "1234567890abcdef1234",
            tip_dirty: false,
            mode: AnalysisMode::Branch,
            notable: false,
            runs: 21,
            series: 1,
            commit_span: None,
            report_improvements: true,
            findings: &[],
            sets: &summaries,
            hint: None,
            warning: None,
            ghosts_excluded: 0,
            census: judged_census(1),
        };
        let expected = "There was not enough comparable base history for a report-wide comparison.";

        let text = render(&input, ReportFormat::Text, false);
        assert!(text.contains(expected), "{text}");

        let markdown = render(&input, ReportFormat::Markdown, false);
        assert!(markdown.contains(expected), "{markdown}");

        let summary = render_markdown_summary(&input, DEFAULT_SUMMARY_LIMIT);
        assert!(summary.contains(expected), "{summary}");
    }

    #[test]
    fn branch_reports_with_only_lag_metadata_still_surface_the_context_limit() {
        let set = discriminant_set();
        let lag = ComparisonBaseLag {
            commits_behind: NonZero::new(2).expect("non-zero"),
            reason: ComparisonBaseLagReason::NoRecentBaseData,
        };
        let summaries = vec![SetSummary {
            set: &set,
            runs: 21,
            series: 1,
            findings: Vec::new(),
            comparison_base_lags: vec![lag],
            branch_comparison: None,
        }];
        let input = ReportInput {
            project: "folo",
            tip_commit: "1234567890abcdef1234",
            tip_dirty: false,
            mode: AnalysisMode::Branch,
            notable: false,
            runs: 21,
            series: 1,
            commit_span: None,
            report_improvements: true,
            findings: &[],
            sets: &summaries,
            hint: None,
            warning: None,
            ghosts_excluded: 0,
            census: census_of(1, &[(UnjudgedReason::TooFewBaseCommits, 1)]),
        };

        let expected_lag = comparison_base_lag_warning(&lag);

        let text = render(&input, ReportFormat::Text, false);
        assert!(text.contains(&expected_lag), "{text}");

        let markdown = render(&input, ReportFormat::Markdown, false);
        assert!(markdown.contains(&expected_lag), "{markdown}");

        let summary = render_markdown_summary(&input, DEFAULT_SUMMARY_LIMIT);
        assert!(summary.contains(&expected_lag), "{summary}");
    }

    #[test]
    fn branch_summary_marks_excursions_outside_the_historical_family() {
        let set = discriminant_set();
        let findings = [branch_regression(false)];
        let comparison = BranchComparison {
            set: set.clone(),
            evaluated_base_commits: 10,
            at_least_as_much: 0,
            series: 3,
        };
        let summaries = vec![SetSummary {
            set: &set,
            runs: 21,
            series: 4,
            findings: vec![&findings[0]],
            comparison_base_lags: Vec::new(),
            branch_comparison: Some(&comparison),
        }];
        let input = ReportInput {
            project: "folo",
            tip_commit: "1234567890abcdef1234",
            tip_dirty: false,
            mode: AnalysisMode::Branch,
            notable: true,
            runs: 21,
            series: 4,
            commit_span: None,
            report_improvements: true,
            findings: &findings,
            sets: &summaries,
            hint: None,
            warning: None,
            ghosts_excluded: 0,
            census: judged_census(4),
        };

        let summary = render_markdown_summary(&input, DEFAULT_SUMMARY_LIMIT);
        assert!(
            summary.contains("**Outside the historical comparison:**"),
            "{summary}"
        );
        assert!(
            summary.contains(
                "The branch produced no reportable out-of-range movement in this comparison"
            ),
            "{summary}"
        );
        assert!(
            !summary.contains("showed as much out-of-range movement as this branch"),
            "{summary}"
        );

        let text = render(&input, ReportFormat::Text, false);
        assert!(
            text.contains("Additional excursions outside this comparison:"),
            "{text}"
        );
        assert!(
            text.contains(
                "The branch produced no reportable out-of-range movement in this comparison"
            ),
            "{text}"
        );
        assert!(
            !text.contains("showed as much out-of-range movement as this branch"),
            "{text}"
        );
        let markdown = render(&input, ReportFormat::Markdown, false);
        assert!(
            markdown.contains("### Additional excursions outside this comparison"),
            "{markdown}"
        );
        assert!(
            markdown.contains(
                "The branch produced no reportable out-of-range movement in this comparison"
            ),
            "{markdown}"
        );
        assert!(
            !markdown.contains("showed as much out-of-range movement as this branch"),
            "{markdown}"
        );
    }

    #[test]
    fn an_excursion_is_not_marked_outside_when_no_comparison_exists() {
        let set = discriminant_set();
        let findings = [branch_regression(false)];
        let summaries = vec![SetSummary {
            set: &set,
            runs: 21,
            series: 1,
            findings: vec![&findings[0]],
            comparison_base_lags: Vec::new(),
            branch_comparison: None,
        }];
        let input = ReportInput {
            project: "folo",
            tip_commit: "1234567890abcdef1234",
            tip_dirty: false,
            mode: AnalysisMode::Branch,
            notable: true,
            runs: 21,
            series: 1,
            commit_span: None,
            report_improvements: true,
            findings: &findings,
            sets: &summaries,
            hint: None,
            warning: None,
            ghosts_excluded: 0,
            census: judged_census(1),
        };

        let text = render(&input, ReportFormat::Text, false);
        assert!(
            !text.contains("Additional excursions outside this comparison"),
            "{text}"
        );
        let markdown = render(&input, ReportFormat::Markdown, false);
        assert!(
            !markdown.contains("Additional excursions outside this comparison"),
            "{markdown}"
        );
        let summary = render_markdown_summary(&input, DEFAULT_SUMMARY_LIMIT);
        assert!(
            !summary.contains("Outside the historical comparison"),
            "{summary}"
        );
    }

    #[test]
    fn previous_regime_context_is_present_only_for_a_return_to_that_regime() {
        let mut finding = branch_regression(true);
        assert_eq!(previous_regime_text(&finding), None);

        let branch = finding
            .branch
            .as_mut()
            .expect("the fixture is a branch excursion");
        branch.matches_previous_regime = true;
        assert_eq!(
            previous_regime_text(&finding).as_deref(),
            Some("The branch value matches the regime preceding base commit abcdef012345.")
        );

        let mut findings = [finding];
        {
            let set = discriminant_set();
            let mut summaries = Vec::new();
            let mut input = single_set_input("folo", &set, &findings, &mut summaries);
            input.mode = AnalysisMode::Branch;

            let text = render(&input, ReportFormat::Text, false);
            assert!(
                text.contains(
                    "The branch value matches the regime preceding base commit abcdef012345."
                ),
                "{text}"
            );
            let markdown = render(&input, ReportFormat::Markdown, false);
            assert!(
                markdown.contains(
                    "The branch value matches the regime preceding base commit abcdef012345."
                ),
                "{markdown}"
            );
        }

        findings[0].branch = None;
        assert_eq!(previous_regime_text(&findings[0]), None);
    }

    #[test]
    fn report_renders_direction_labels() {
        let set = discriminant_set();
        let mut improvement = regression();
        improvement.direction = Direction::Improvement;
        improvement.delta = -5.0;
        improvement.relative_delta = -0.05;
        let findings = vec![regression(), improvement];
        let mut summaries = Vec::new();
        let input = single_set_input("folo", &set, &findings, &mut summaries);

        let text = render(&input, ReportFormat::Text, false);
        assert!(text.contains("regression via change point"), "{text}");
        assert!(text.contains("improvement via change point"), "{text}");

        let markdown = render(&input, ReportFormat::Markdown, false);
        assert!(
            markdown.contains("regression via change point"),
            "{markdown}"
        );
        assert!(
            markdown.contains("improvement via change point"),
            "{markdown}"
        );
        assert!(markdown.contains("**+30.00%**"), "{markdown}");
        assert!(markdown.contains("**-5.00%**"), "{markdown}");
        assert_eq!(
            method_label(FindingMethod::BranchExcursion),
            "branch excursion"
        );

        // The per-set JSON tallies count each direction independently: this set
        // holds one regression and one improvement.
        let json = render(&input, ReportFormat::Json, false);
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        let set_json = &parsed["sets"][0];
        assert_eq!(set_json["regressions"], 1, "{json}");
        assert_eq!(set_json["improvements"], 1, "{json}");
    }

    #[test]
    fn text_report_annotates_a_blessed_finding() {
        let set = discriminant_set();
        let mut blessed = regression();
        blessed.blessed_at = Some("c3".to_owned());
        blessed.blessed_commit_time = Some("2024-01-01T00:00:00Z".to_owned());
        let findings = vec![blessed];
        let mut summaries = Vec::new();
        let input = single_set_input("folo", &set, &findings, &mut summaries);
        let report = render(&input, ReportFormat::Text, false);
        assert!(
            report.contains("blessed at c3 (2024-01-01T00:00:00Z)"),
            "{report}"
        );
    }

    #[test]
    fn json_per_set_tally_counts_each_direction_independently() {
        // Two regressions and no improvements in one set: the per-set JSON tally
        // must report the real counts, not a constant.
        let set = discriminant_set();
        let mut second = regression();
        second.id = BenchmarkId::new(nonempty![
            "nm".to_owned(),
            "nm::other".to_owned(),
            "push".to_owned(),
        ]);
        let findings = vec![regression(), second];
        let mut summaries = Vec::new();
        let input = single_set_input("folo", &set, &findings, &mut summaries);
        let json = render(&input, ReportFormat::Json, false);
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        let set_json = &parsed["sets"][0];
        assert_eq!(set_json["regressions"], 2, "{json}");
        assert_eq!(set_json["improvements"], 0, "{json}");
    }

    #[test]
    fn markdown_report_renders_a_block_per_set() {
        let set = discriminant_set();
        let findings = vec![regression()];
        let mut summaries = Vec::new();
        let input = single_set_input("folo", &set, &findings, &mut summaries);
        let report = render(&input, ReportFormat::Markdown, false);
        assert!(
            report.contains("# Benchmark history analysis: folo"),
            "{report}"
        );
        assert!(
            report.contains("## callgrind/x86_64-unknown-linux-gnu/m1"),
            "the set heading drops the redundant `Set ` prefix: {report}"
        );
        // The per-set tally mirrors the JSON metadata and the text header.
        assert!(report.contains("- Regressions: 1"), "{report}");
        // The set header names the discriminant-filter flags that reproduce exactly this partition.
        assert!(
            report.contains(
                "- Filter: `--engine callgrind --target-triple x86_64-unknown-linux-gnu --machine-key m1`"
            ),
            "{report}"
        );
        // Findings render as heading + bold-headline blocks, not a table.
        assert!(!report.contains("| Change | Direction |"), "{report}");
        // The benchmark id is its own heading, nested one level under the set heading
        // (`##`), so it reads as a chapter title; the change headline follows.
        assert!(report.contains("### `nm/nm::observe/pull`"), "{report}");
        assert!(
            report.contains("**+30.00%** `instruction_count`"),
            "{report}"
        );
        // The old inline em-dash headline is gone.
        assert!(!report.contains("—"), "{report}");
        // The report no longer surfaces a confidence figure anywhere.
        assert!(
            report.contains("regression via change point · 100 → 130"),
            "{report}"
        );
        assert!(!report.contains("confidence"), "{report}");
    }

    #[test]
    fn markdown_report_annotates_a_blessed_finding() {
        let set = discriminant_set();
        let mut blessed = regression();
        blessed.blessed_at = Some("c3".to_owned());
        blessed.blessed_commit_time = Some("2024-01-01T00:00:00Z".to_owned());
        let findings = vec![blessed];
        let mut summaries = Vec::new();
        let input = single_set_input("folo", &set, &findings, &mut summaries);
        let report = render(&input, ReportFormat::Markdown, false);
        assert!(
            report.contains("blessed at c3 (2024-01-01T00:00:00Z)"),
            "{report}"
        );
    }

    #[test]
    fn markdown_history_mode_draws_a_fenced_chart() {
        let set = discriminant_set();
        let findings = vec![regression_with_series()];
        let mut summaries = Vec::new();
        let mut input = single_set_input("folo", &set, &findings, &mut summaries);
        input.mode = AnalysisMode::History;
        let report = render(&input, ReportFormat::Markdown, false);
        // The chart sits inside a fenced `text` block and carries no ANSI escapes.
        assert!(report.contains("```text"), "{report}");
        assert!(report.contains('┤') || report.contains('┼'), "{report}");
        assert!(!report.contains('\u{1b}'), "{report}");
    }

    #[test]
    fn markdown_report_with_no_findings() {
        let input = ReportInput {
            project: "folo",
            tip_commit: "1234567890abcdef1234",
            tip_dirty: false,
            mode: AnalysisMode::History,
            notable: false,
            runs: 0,
            series: 0,
            commit_span: None,
            report_improvements: false,
            findings: &[],
            sets: &[],
            hint: Some("Found 2 stored runs ... commit your working tree"),
            warning: None,
            ghosts_excluded: 0,
            census: judged_census(0),
        };
        let report = render(&input, ReportFormat::Markdown, false);
        assert!(
            report.contains("Nothing was analyzed, so no change could be detected."),
            "{report}"
        );
        assert!(report.contains("commit your working tree"), "{report}");
    }

    #[test]
    fn json_report_includes_hint_field_when_present() {
        let input = ReportInput {
            project: "folo",
            tip_commit: "1234567890abcdef1234",
            tip_dirty: false,
            mode: AnalysisMode::History,
            notable: false,
            runs: 0,
            series: 0,
            commit_span: None,
            report_improvements: false,
            findings: &[],
            sets: &[],
            hint: Some("dirty snapshots on base-ref commits"),
            warning: None,
            ghosts_excluded: 0,
            census: judged_census(0),
        };
        let report = render(&input, ReportFormat::Json, false);
        let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
        assert_eq!(
            parsed["hint"], "dirty snapshots on base-ref commits",
            "{report}"
        );
    }

    #[test]
    fn warning_renders_at_the_end_of_every_format() {
        let set = discriminant_set();
        let findings = vec![regression()];
        let mut summaries = Vec::new();
        let mut input = single_set_input("folo", &set, &findings, &mut summaries);
        input.warning = Some("Warning: analysis included dirty runs (ephemeral).");

        let text = render(&input, ReportFormat::Text, false);
        assert!(text.trim_end().ends_with("(ephemeral)."), "{text}");

        let markdown = render(&input, ReportFormat::Markdown, false);
        assert!(markdown.trim_end().ends_with("(ephemeral)."), "{markdown}");

        let json = render(&input, ReportFormat::Json, false);
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(
            parsed["warning"], "Warning: analysis included dirty runs (ephemeral).",
            "{json}"
        );
    }

    #[test]
    fn warning_renders_even_when_there_are_no_findings() {
        let input = ReportInput {
            project: "folo",
            tip_commit: "1234567890abcdef1234",
            tip_dirty: false,
            mode: AnalysisMode::History,
            notable: false,
            runs: 1,
            series: 1,
            commit_span: None,
            report_improvements: false,
            findings: &[],
            sets: &[],
            hint: None,
            warning: Some("Warning: dirty runs were included."),
            ghosts_excluded: 0,
            census: judged_census(1),
        };
        let text = render(&input, ReportFormat::Text, false);
        assert!(text.contains("No notable changes detected."), "{text}");
        assert!(text.trim_end().ends_with("included."), "{text}");

        let markdown = render(&input, ReportFormat::Markdown, false);
        assert!(markdown.trim_end().ends_with("included."), "{markdown}");
    }

    #[test]
    fn omitted_warning_is_absent_from_json() {
        let input = ReportInput {
            project: "folo",
            tip_commit: "1234567890abcdef1234",
            tip_dirty: false,
            mode: AnalysisMode::History,
            notable: false,
            runs: 0,
            series: 0,
            commit_span: None,
            report_improvements: false,
            findings: &[],
            sets: &[],
            hint: None,
            warning: None,
            ghosts_excluded: 0,
            census: judged_census(0),
        };
        let report = render(&input, ReportFormat::Json, false);
        let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
        assert!(parsed.get("warning").is_none(), "{report}");
    }

    #[test]
    fn json_report_is_structured() {
        let set = discriminant_set();
        let findings = vec![regression()];
        let mut summaries = Vec::new();
        let input = single_set_input("folo", &set, &findings, &mut summaries);
        let report = render(&input, ReportFormat::Json, false);
        let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
        assert_eq!(parsed["project"], "folo");
        assert_eq!(parsed["tip_commit"], "1234567890abcdef1234");
        assert_eq!(parsed["tip_dirty"], false);
        assert_eq!(parsed["regressions"], 1);
        assert_eq!(parsed["improvements"], 0);
        let finding = &parsed["findings"][0];
        // Flattened DiscriminantSet and BenchmarkId fields appear inline.
        assert_eq!(finding["engine"], "callgrind");
        assert_eq!(finding["segments"][0], "nm");
        assert_eq!(finding["segments"][1], "nm::observe");
        assert_eq!(finding["direction"], "regression");
        assert_eq!(finding["kind"], "instruction_count");
        // The bulky per-commit series is no longer carried: JSON mirrors the text
        // data, not the chart it draws from.
        assert!(finding.get("series").is_none(), "{report}");
        // The per-set breakdown carries the partition triple and tallies only — no
        // duplicated findings array.
        let set_json = &parsed["sets"][0];
        assert_eq!(set_json["engine"], "callgrind");
        assert_eq!(set_json["target_triple"], "x86_64-unknown-linux-gnu");
        assert_eq!(set_json["regressions"], 1);
        assert!(set_json.get("findings").is_none(), "{report}");
    }

    #[test]
    fn report_header_names_the_analyzed_context_commit() {
        let set = discriminant_set();
        let findings = vec![regression()];
        let mut summaries = Vec::new();
        let input = single_set_input("folo", &set, &findings, &mut summaries);

        // A clean context commit is named without annotation in both human formats.
        let text = render(&input, ReportFormat::Text, false);
        assert!(text.contains("commit: 1234567890abcdef1234"), "{text}");
        assert!(
            !text.contains("uncommitted changes"),
            "a clean context commit must not be annotated: {text}"
        );
        let markdown = render(&input, ReportFormat::Markdown, false);
        assert!(
            markdown.contains("- Commit: 1234567890abcdef1234"),
            "{markdown}"
        );
    }

    #[test]
    fn dirty_context_commit_is_annotated_with_uncommitted_changes() {
        let input = ReportInput {
            project: "folo",
            tip_commit: "1234567890abcdef1234",
            tip_dirty: true,
            mode: AnalysisMode::History,
            notable: false,
            runs: 1,
            series: 1,
            commit_span: None,
            report_improvements: true,
            findings: &[],
            sets: &[],
            hint: None,
            warning: None,
            ghosts_excluded: 0,
            census: judged_census(1),
        };

        let text = render(&input, ReportFormat::Text, false);
        assert!(
            text.contains("commit: 1234567890abcdef1234 + uncommitted changes"),
            "{text}"
        );
        let markdown = render(&input, ReportFormat::Markdown, false);
        assert!(
            markdown.contains("- Commit: 1234567890abcdef1234 + uncommitted changes"),
            "{markdown}"
        );
        let json = render(&input, ReportFormat::Json, false);
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["tip_commit"], "1234567890abcdef1234");
        assert_eq!(parsed["tip_dirty"], true);
    }

    #[test]
    fn format_value_drops_integer_fraction() {
        assert_eq!(format_value(36.0), "36");
        assert_eq!(format_value(12.5), "12.5");
    }

    #[test]
    fn format_value_keeps_four_significant_figures() {
        // Counting integer-part digits toward the four significant figures.
        assert_eq!(format_value(0.209_703_243_360_777_45), "0.2097");
        assert_eq!(format_value(96.766_413_608_934_1), "96.77");
        assert_eq!(format_value(507.428_215_753_575_5), "507.4");
        // Sub-decimal values keep all four significant digits past the zeros.
        assert_eq!(format_value(0.000_001_234), "0.000001234");
        // A large value drops its fraction entirely; the integer part is never cut.
        assert_eq!(format_value(1_234_567.89), "1234568");
        // Trailing zeros are trimmed.
        assert_eq!(format_value(0.25), "0.25");
    }

    #[test]
    fn signed_values_keep_the_human_readable_precision() {
        assert_eq!(format_signed_value(0.300_000_000_000_04), "+0.3");
        assert_eq!(format_signed_value(-0.300_000_000_000_04), "-0.3");
    }

    #[test]
    fn branch_relations_match_the_metric_kind() {
        for kind in [MetricKind::WallTime, MetricKind::ProcessorTime] {
            assert_eq!(
                branch_relation(kind, Direction::Regression),
                "slower than all"
            );
            assert_eq!(
                branch_relation(kind, Direction::Improvement),
                "faster than all"
            );
        }

        for kind in [
            MetricKind::InstructionCount,
            MetricKind::ConditionalBranches,
            MetricKind::IndirectBranches,
            MetricKind::AllocatedBytes,
            MetricKind::AllocationCount,
        ] {
            assert_eq!(
                branch_relation(kind, Direction::Regression),
                "higher than all"
            );
            assert_eq!(
                branch_relation(kind, Direction::Improvement),
                "lower than all"
            );
        }
    }

    /// Builds a regression finding whose series steps up over four commits, so a
    /// chart has enough points to draw.
    fn regression_with_series() -> Finding {
        let mut finding = regression();
        finding.series = vec![
            SeriesValue {
                commit: Some("c0".to_owned()),
                value: 100.0,
                dirty: false,
                topo_index: 0,
            },
            SeriesValue {
                commit: Some("c1".to_owned()),
                value: 100.0,
                dirty: false,
                topo_index: 1,
            },
            SeriesValue {
                commit: Some("c2".to_owned()),
                value: 130.0,
                dirty: false,
                topo_index: 2,
            },
            SeriesValue {
                commit: Some("c3".to_owned()),
                value: 130.0,
                dirty: false,
                topo_index: 3,
            },
        ];
        finding
    }

    /// A y-axis value that only ever appears on a chart's scale, never in a finding's
    /// prose (its baseline/latest are 100/130), so a report either charting or omitting
    /// it can be told apart by a plain substring search.
    const CHART_ONLY_MARKER: f64 = 1000.0;

    /// Builds a regression finding with a long, sparse topology: a lone ancient spike at
    /// `topo_index` 0, then a wide data-less gap, then a recent 30-commit cluster ending
    /// at the context commit (`topo_index` 199).
    ///
    /// The ancient spike ([`CHART_ONLY_MARKER`]) sits far outside the branch chart's
    /// bounded recent window and dwarfs the context value, so it dominates a
    /// whole-series chart's y-axis but is absent from a branch chart's — the
    /// discriminator the scope tests key off. Because it is isolated at
    /// `topo_index` 0 it keeps its own leftmost column (never averaged away), so
    /// the whole-series chart still shows it. The context run is the one point
    /// branch mode judges; it must always survive into the chart.
    fn regression_with_long_series() -> Finding {
        let context_topo = 199;
        let cluster_start = 170;
        let baseline = 100.0;
        let context_value = 130.0;
        let mut series = vec![SeriesValue {
            commit: Some("c0".to_owned()),
            value: CHART_ONLY_MARKER,
            dirty: false,
            topo_index: 0,
        }];
        series.extend((cluster_start..=context_topo).map(|topo| SeriesValue {
            commit: Some(format!("c{topo}")),
            value: if topo == context_topo {
                context_value
            } else {
                baseline
            },
            dirty: false,
            topo_index: topo,
        }));
        Finding {
            series,
            ..regression()
        }
    }

    #[test]
    fn history_mode_text_draws_a_chart() {
        let set = discriminant_set();
        let findings = vec![regression_with_series()];
        let mut summaries = Vec::new();
        let mut input = single_set_input("folo", &set, &findings, &mut summaries);
        input.mode = AnalysisMode::History;
        let text = render(&input, ReportFormat::Text, false);
        // The rasciigraph axis marker proves a chart was drawn under the finding.
        assert!(text.contains('┤') || text.contains('┼'), "{text}");
    }

    #[test]
    fn branch_mode_text_draws_a_chart() {
        let set = discriminant_set();
        let findings = vec![regression_with_series()];
        let mut summaries = Vec::new();
        let mut input = single_set_input("folo", &set, &findings, &mut summaries);
        input.mode = AnalysisMode::Branch;
        let text = render(&input, ReportFormat::Text, false);
        // Branch mode now charts too (it previously did not); the axis marker proves it.
        assert!(text.contains('┤') || text.contains('┼'), "{text}");
    }

    #[test]
    fn branch_mode_text_charts_the_bounded_comparison_including_the_context() {
        // BUSINESS-CRITICAL INVARIANT. Branch mode judges a feature branch by its context
        // commit alone, so the context run is the one data point the report exists to
        // convey. It must remain visible on the chart no matter how long the history is
        // — never aliased away or shrunk to an indistinct edge column by resampling a
        // months-long series down to the chart width. This test pins that: charting the
        // baseline and recent tail keeps the context value as the chart's maximum while
        // dropping ancient history. Do NOT weaken it to "a chart is drawn" — the point
        // is *which* values the chart shows.
        let set = discriminant_set();
        let findings = vec![regression_with_long_series()];
        let mut summaries = Vec::new();
        let mut input = single_set_input("folo", &set, &findings, &mut summaries);
        input.mode = AnalysisMode::Branch;
        let text = render(&input, ReportFormat::Text, false);

        // A chart is drawn.
        assert!(text.contains('┤') || text.contains('┼'), "{text}");
        // The context value is the tail's peak, so it labels the top of the y-axis: the last
        // commit analyzed is unmistakably plotted, not aliased into the baseline.
        assert!(
            text.contains("130 ┤") || text.contains("130 ┼"),
            "the context value must head the chart's y-axis: {text}"
        );
        // The ancient spike lies outside the bounded comparison, so it must not appear
        // on the chart scale — proof the whole series was not charted, and that ancient
        // outliers cannot squash the context value out of view.
        assert!(
            !text.contains("1000"),
            "branch mode must exclude ancient history from the chart: {text}"
        );
    }

    #[test]
    fn history_mode_text_charts_the_whole_series() {
        // The companion to the branch comparison test: history mode charts the *entire*
        // series, so the same ancient spike that a branch chart drops here heads the
        // y-axis. This keeps the two scopes distinct and stops a refactor from silently
        // collapsing branch's bounded view into history's whole-series view (or vice
        // versa).
        let set = discriminant_set();
        let findings = vec![regression_with_long_series()];
        let mut summaries = Vec::new();
        let mut input = single_set_input("folo", &set, &findings, &mut summaries);
        input.mode = AnalysisMode::History;
        let text = render(&input, ReportFormat::Text, false);

        assert!(text.contains('┤') || text.contains('┼'), "{text}");
        assert!(
            text.contains("1000"),
            "history mode charts the whole series, so the early spike heads the y-axis: {text}"
        );
    }

    #[test]
    fn branch_mode_markdown_draws_a_fenced_chart() {
        let set = discriminant_set();
        let findings = vec![regression_with_series()];
        let mut summaries = Vec::new();
        let mut input = single_set_input("folo", &set, &findings, &mut summaries);
        input.mode = AnalysisMode::Branch;
        let report = render(&input, ReportFormat::Markdown, false);
        // The branch chart sits inside the same fenced `text` block as a history chart.
        assert!(report.contains("```text"), "{report}");
        assert!(report.contains('┤') || report.contains('┼'), "{report}");
    }

    #[test]
    fn markdown_summary_charts_the_branch_comparison() {
        // The summary is the third renderer that must chart branch findings; and, like the
        // full reports, it must keep the context commit visible while windowing out
        // ancient history. See
        // `branch_mode_text_charts_the_bounded_comparison_including_the_context`.
        let findings = vec![regression_with_long_series()];
        let mut input = flat_input(&findings);
        input.mode = AnalysisMode::Branch;
        let report = render_markdown_summary(&input, DEFAULT_SUMMARY_LIMIT);

        assert!(report.contains("```text"), "{report}");
        assert!(
            report.contains("130 ┤") || report.contains("130 ┼"),
            "the context value must head the summary chart's y-axis: {report}"
        );
        assert!(
            !report.contains("1000"),
            "the branch summary must exclude ancient history: {report}"
        );
    }

    #[test]
    fn branch_chart_values_keep_the_baseline_and_context() {
        // BUSINESS-CRITICAL INVARIANT. A long-lived branch can itself contribute more
        // points than the chart cap. Even then, the bounded chart must retain both the
        // comparison baseline and the last point — the context commit branch mode
        // judges — without resampling either one away.
        let mut finding = regression_with_long_series();
        for point in finding
            .series
            .iter_mut()
            .rev()
            .take(BRANCH_CHART_MAX_POINTS)
        {
            point.value = finding.latest;
        }
        let values = branch_chart_values(&finding);
        assert_eq!(values.len(), BRANCH_CHART_MAX_POINTS, "the chart is capped");
        assert_eq!(
            values
                .first()
                .expect("the baseline is always present")
                .to_bits(),
            finding.baseline.to_bits(),
            "the detector's comparison baseline must remain visible"
        );
        assert_eq!(
            values
                .last()
                .expect("the observed tail is non-empty")
                .to_bits(),
            finding
                .series
                .last()
                .expect("the test series is non-empty")
                .value
                .to_bits(),
            "the context value must be the last charted point"
        );
        assert!(
            values
                .iter()
                .all(|value| value.to_bits() != CHART_ONLY_MARKER.to_bits()),
            "ancient observations must fall outside the bounded chart"
        );
    }

    /// A regression finding carrying an explicit compact chart series, for exercising the
    /// column-building helpers directly.
    fn finding_with_series(baseline: f64, latest: f64, points: &[(usize, f64)]) -> Finding {
        let mut finding = regression();
        finding.baseline = baseline;
        finding.latest = latest;
        finding.series = points
            .iter()
            .map(|&(topo, value)| SeriesValue {
                commit: Some(format!("c{topo}")),
                value,
                dirty: false,
                topo_index: topo,
            })
            .collect();
        finding
    }

    /// The count of gap (`NaN`) columns in a column slice.
    fn gap_count(columns: &[f64]) -> usize {
        columns.iter().filter(|value| value.is_nan()).count()
    }

    #[test]
    fn topology_columns_empty_series_yields_no_columns() {
        assert!(topology_columns(&[], None, CHART_WIDTH as usize).is_empty());
        assert!(topology_columns(&[], Some(9), CHART_WIDTH as usize).is_empty());
    }

    #[test]
    fn topology_columns_single_point_is_one_solid_column() {
        // One observation, no base ref: a single finite column, no leading or trailing
        // gap. (A lone column can't be charted, but the binning is still exact.)
        let columns = topology_columns(&[(5, 42.0)], None, CHART_WIDTH as usize);
        assert_eq!(columns.len(), 1);
        assert_eq!(columns[0].to_bits(), 42.0_f64.to_bits());
    }

    #[test]
    fn topology_columns_trims_the_leading_gap() {
        // The first observation is at topo 5, not 0: the leftmost column still holds it,
        // so the older empty commits never become leading gap columns.
        let columns = topology_columns(&[(5, 10.0), (7, 20.0)], None, CHART_WIDTH as usize);
        assert_eq!(columns.len(), 3, "topos 5..=7 span three columns");
        assert_eq!(columns[0].to_bits(), 10.0_f64.to_bits(), "no leading gap");
        assert!(
            columns[1].is_nan(),
            "the data-less topo 6 is the one interior gap"
        );
        assert_eq!(columns[2].to_bits(), 20.0_f64.to_bits());
        assert_eq!(gap_count(&columns), 1);
    }

    #[test]
    fn topology_columns_keeps_every_interior_gap() {
        // Five data-less commits between two observations become exactly five gap
        // columns — the topology is reproduced commit for commit.
        let columns = topology_columns(&[(0, 10.0), (6, 20.0)], None, CHART_WIDTH as usize);
        assert_eq!(columns.len(), 7);
        assert_eq!(columns[0].to_bits(), 10.0_f64.to_bits());
        assert_eq!(columns[6].to_bits(), 20.0_f64.to_bits());
        assert_eq!(gap_count(&columns), 5, "five data-less commits, five gaps");
        assert!(columns[1..6].iter().all(|value| value.is_nan()));
    }

    #[test]
    fn topology_columns_keeps_the_trailing_gap_up_to_the_base_ref() {
        // The base ref (the analyzed context) sits three commits past the last
        // observation, so those three data-less commits render as a trailing gap — the
        // visual form of the "no newer data" lag.
        let columns = topology_columns(&[(0, 10.0), (2, 20.0)], Some(5), CHART_WIDTH as usize);
        assert_eq!(columns.len(), 6, "topos 0..=5 span six columns");
        assert_eq!(columns[0].to_bits(), 10.0_f64.to_bits());
        assert_eq!(columns[2].to_bits(), 20.0_f64.to_bits());
        assert!(
            columns[3..6].iter().all(|value| value.is_nan()),
            "trailing lag gap"
        );
        assert!(
            columns[1].is_nan(),
            "the data-less topo 1 is an interior gap"
        );
        assert_eq!(
            gap_count(&columns),
            4,
            "one interior (topo 1) plus three trailing (topos 3..=5)"
        );
    }

    #[test]
    fn topology_columns_base_ref_at_or_before_last_adds_no_trailing_gap() {
        // A base ref that does not exceed the last observation leaves no trailing gap:
        // there is no newer commit to wait for.
        let at_last = topology_columns(&[(0, 10.0), (4, 20.0)], Some(4), CHART_WIDTH as usize);
        assert_eq!(at_last.len(), 5);
        assert_eq!(
            at_last[4].to_bits(),
            20.0_f64.to_bits(),
            "last column is the observation"
        );
        let before_last = topology_columns(&[(0, 10.0), (4, 20.0)], Some(2), CHART_WIDTH as usize);
        assert!(
            before_last
                .iter()
                .zip(&at_last)
                .all(|(left, right)| left.to_bits() == right.to_bits()),
            "a base ref before the last point is inert: {before_last:?} vs {at_last:?}"
        );
    }

    #[test]
    fn topology_columns_averages_observations_that_share_a_commit() {
        // A commit's clean and dirty snapshots share a topo index, so they land in one
        // column and average — the column is the commit's mean, not two columns.
        let columns = topology_columns(
            &[(0, 10.0), (0, 20.0), (2, 30.0)],
            None,
            CHART_WIDTH as usize,
        );
        assert_eq!(columns.len(), 3);
        assert_eq!(
            columns[0].to_bits(),
            15.0_f64.to_bits(),
            "the shared commit's mean"
        );
        assert!(columns[1].is_nan());
        assert_eq!(columns[2].to_bits(), 30.0_f64.to_bits());
    }

    #[test]
    fn topology_columns_downsamples_without_losing_isolated_observations_or_extrema() {
        // REGRESSION GUARD. `rasciigraph::plot` interpolates to its width *before*
        // computing the axis min/max, and that interpolation is NaN-poisoning: handed a
        // topology span wider than the chart, an isolated observation trapped between
        // gaps blends into NaN and vanishes — taking its value out of the axis extrema.
        // Binning to CHART_WIDTH first (here a 100-commit span into 48 columns) must place
        // each isolated observation in its own column so neither those observations nor the
        // axis extrema they define can be lost.
        let points = [(0, 1000.0), (49, 1.0), (99, 500.0)];
        let columns = topology_columns(&points, None, CHART_WIDTH as usize);
        assert_eq!(
            columns.len(),
            CHART_WIDTH as usize,
            "the span is capped at the width"
        );
        assert_eq!(
            columns[0].to_bits(),
            1000.0_f64.to_bits(),
            "the max survives, in column 0"
        );
        assert_eq!(
            columns[23].to_bits(),
            1.0_f64.to_bits(),
            "the isolated min survives"
        );
        assert_eq!(
            columns[47].to_bits(),
            500.0_f64.to_bits(),
            "the last observation survives"
        );
        assert_eq!(
            columns.iter().filter(|value| value.is_finite()).count(),
            3,
            "each of the three observations keeps its own column — none dropped or merged"
        );
        // And the extrema survive all the way into the rendered axis.
        let chart = chart_series(&points, None).expect("three finite columns plot");
        assert!(chart.contains("1000"), "the max heads the y-axis: {chart}");
        assert!(chart.contains("┼") || chart.contains("┤"), "{chart}");
    }

    #[test]
    fn chart_series_of_a_lone_observation_with_a_lag_draws_nothing() {
        // One real observation plus a trailing lag is a single finite value among NaNs —
        // too few to plot a line, so no chart.
        assert!(chart_series(&[(0, 5.0)], Some(10)).is_none());
    }

    #[test]
    fn chart_gates_on_the_finite_count_not_the_length() {
        // The gate counts *finite* values: a lone real value padded with gap columns must
        // not plot (pins the `< 2` finite test against a length-based slip).
        assert!(
            chart(&[1.0, f64::NAN]).is_none(),
            "one finite value is too few"
        );
        assert!(chart(&[f64::NAN, f64::NAN]).is_none(), "no finite values");
        assert!(
            chart(&[1.0, f64::NAN, 2.0]).is_some(),
            "two finite values plot"
        );
    }

    #[test]
    fn chart_renders_a_gap_without_disturbing_the_axis() {
        // A NaN column is a gap, not a data point: it must not poison the axis extrema,
        // and an isolated interior extreme surrounded by gaps must still head the axis.
        let gapped = chart(&[10.0, f64::NAN, 30.0]).expect("two finite values plot");
        assert!(
            gapped.contains("30"),
            "the max still labels the axis top: {gapped}"
        );
        assert!(
            gapped.contains("10"),
            "the min still labels the axis bottom: {gapped}"
        );
        let spike =
            chart(&[10.0, f64::NAN, 1000.0, f64::NAN, 20.0]).expect("three finite values plot");
        assert!(
            spike.contains("1000"),
            "an isolated interior spike survives, un-poisoned: {spike}"
        );
    }

    #[test]
    fn branch_chart_values_open_with_baseline_close_with_latest_and_gap_the_lag() {
        // The compact branch series is a single base observation at topo 0 and the
        // context commit at topo 3; topos 1 and 2 are the commits the branch's base is
        // behind by. The chart must open with the comparison baseline, close with the
        // context run's judged latest, and render those two lagging commits as exactly
        // two gap columns.
        let finding = finding_with_series(100.0, 130.0, &[(0, 100.0), (3, 130.0)]);
        let values = branch_chart_values(&finding);
        assert_eq!(values.len(), 5, "baseline column + topos 0..=3");
        assert_eq!(
            values[0].to_bits(),
            100.0_f64.to_bits(),
            "opens with the baseline"
        );
        assert_eq!(
            values[1].to_bits(),
            100.0_f64.to_bits(),
            "the base observation at topo 0"
        );
        assert!(values[2].is_nan(), "the first lagging commit is a gap");
        assert!(values[3].is_nan(), "the second lagging commit is a gap");
        assert_eq!(
            values[4].to_bits(),
            130.0_f64.to_bits(),
            "closes with the context run's latest"
        );
        assert_eq!(gap_count(&values), 2, "exactly commits_behind gap columns");
    }

    #[test]
    fn branch_chart_values_average_a_commits_shared_observations() {
        // A base commit contributes both a clean and a dirty snapshot at the same topo
        // index, so the branch chart's column for that commit is their mean, not their sum
        // or product. topo 0 holds 100 and 120 (mean 110); the context commit at topo
        // 2 is the judged latest, with topo 1 the single lagging commit.
        let finding = finding_with_series(100.0, 130.0, &[(0, 100.0), (0, 120.0), (2, 130.0)]);
        let values = branch_chart_values(&finding);
        assert_eq!(values.len(), 4, "baseline column + topos 0..=2");
        assert_eq!(
            values[1].to_bits(),
            110.0_f64.to_bits(),
            "the shared commit's column is the mean of its two observations"
        );
        assert!(values[2].is_nan(), "the lagging commit is a gap");
        assert_eq!(
            values[3].to_bits(),
            130.0_f64.to_bits(),
            "closes with the context run's latest"
        );
    }

    #[test]
    fn branch_chart_values_of_an_empty_series_is_just_the_baseline() {
        // A finding with no charted observations still yields its baseline column (and so
        // never plots, having one finite value).
        let finding = finding_with_series(100.0, 130.0, &[]);
        let values = branch_chart_values(&finding);
        assert_eq!(values.len(), 1);
        assert_eq!(values[0].to_bits(), 100.0_f64.to_bits());
    }

    #[test]
    fn describe_id_joins_present_parts() {
        let id = BenchmarkId::new(nonempty![
            "pkg".to_owned(),
            "group".to_owned(),
            "case".to_owned(),
            "value".to_owned(),
        ]);
        assert_eq!(describe_id(&id), "pkg/group/case/value");
        let bare = BenchmarkId::new(nonempty!["group".to_owned()]);
        assert_eq!(describe_id(&bare), "group");
    }

    #[test]
    fn chart_needs_at_least_two_points() {
        // A single point cannot be plotted; two is the minimum (a `< 2` -> `<= 2`
        // slip would reject the two-point case).
        assert!(chart(&[1.0]).is_none());
        assert!(chart(&[1.0, 2.0]).is_some());
    }

    #[test]
    fn color_override_holds_the_global_lock_across_threads() {
        let _color = ColorOverride::force(false);

        let lock_was_held = thread::spawn(|| {
            matches!(
                COLOR_OVERRIDE_LOCK.try_lock(),
                Err(TryLockError::WouldBlock)
            )
        })
        .join()
        .unwrap();

        assert!(lock_was_held);
    }

    #[test]
    fn chart_plots_uncolored_without_ansi() {
        // Charts are always uncolored, whatever `colored`'s process-global override
        // says. Force it on to prove a drawn chart never embeds an ANSI escape.
        let _color = ColorOverride::force(true);
        let chart = chart(&[1.0, 2.0, 3.0]).expect("two-plus points plot");
        // The rasciigraph axis marker proves a chart was drawn; no escape byte proves
        // the line stayed uncolored.
        assert!(chart.contains('┤') || chart.contains('┼'), "{chart}");
        assert!(!chart.contains('\u{1b}'), "no ANSI escape: {chart:?}");
    }

    #[test]
    fn significant_decimals_handles_a_value_below_the_search_floor() {
        // A magnitude smaller than 10^-12 never satisfies the exponent search, so
        // the exponent stays at its -12 floor, yielding 15 decimal places. This
        // pins the sign of the `-12` initializer.
        assert_eq!(significant_decimals(1e-13), 15);
    }

    fn drift() -> Finding {
        Finding {
            method: FindingMethod::Drift,
            commit: Some("deadbee".to_owned()),
            window_start_commit: Some("f00dcafe".to_owned()),
            ..regression()
        }
    }

    #[test]
    fn attribution_text_names_a_nearby_commit_or_a_drift_range() {
        // A change point names an estimated location, not the exact first commit.
        assert_eq!(attribution_text(&regression()), "somewhere near deadbee");
        // A drift names the whole range it accumulated over, not one commit.
        assert_eq!(attribution_text(&drift()), "accumulated f00dcafe → deadbee");
        let mut drift_without_start = drift();
        drift_without_start.window_start_commit = None;
        assert_eq!(
            attribution_text(&drift_without_start),
            "accumulated ending at deadbee"
        );
        // A branch comparison names the context commit itself.
        assert_eq!(attribution_text(&branch_regression(true)), "@ deadbee");
        // A series point may lack commit metadata; do not invent a placeholder name.
        let mut unattributed = regression();
        unattributed.commit = None;
        assert_eq!(
            attribution_text(&unattributed),
            "across the analyzed window"
        );
        unattributed.method = FindingMethod::Drift;
        assert_eq!(
            attribution_text(&unattributed),
            "across the analyzed window"
        );
        unattributed.method = FindingMethod::BranchExcursion;
        assert_eq!(
            attribution_text(&unattributed),
            "across the analyzed window"
        );
    }

    fn darwin_set() -> DiscriminantSet {
        DiscriminantSet {
            engine: Engine::Callgrind,
            target_triple: "aarch64-apple-darwin".into(),
            machine_key: "m2".into(),
        }
    }

    #[test]
    fn text_report_labels_a_drift_finding_and_skips_an_empty_set() {
        let set_a = discriminant_set();
        let set_b = darwin_set();
        let findings = [drift()];
        let summaries = vec![
            SetSummary {
                set: &set_a,
                runs: 6,
                series: 1,
                findings: vec![&findings[0]],
                comparison_base_lags: Vec::new(),
                branch_comparison: None,
            },
            SetSummary {
                set: &set_b,
                runs: 4,
                series: 1,
                findings: Vec::new(),
                comparison_base_lags: Vec::new(),
                branch_comparison: None,
            },
        ];
        let input = ReportInput {
            project: "folo",
            tip_commit: "1234567890abcdef1234",
            tip_dirty: false,
            mode: AnalysisMode::History,
            notable: true,
            runs: 10,
            series: 2,
            commit_span: None,
            report_improvements: false,
            findings: &findings,
            sets: &summaries,
            hint: None,
            warning: None,
            ghosts_excluded: 0,
            census: judged_census(2),
        };
        let report = render(&input, ReportFormat::Text, false);
        assert!(report.contains("drift"), "{report}");
        assert!(report.contains("x86_64-unknown-linux-gnu"), "{report}");
        assert!(
            !report.contains("aarch64-apple-darwin"),
            "the empty set is skipped: {report}"
        );
    }

    #[test]
    fn markdown_report_labels_a_drift_finding_and_skips_an_empty_set() {
        let set_a = discriminant_set();
        let set_b = darwin_set();
        let findings = [drift()];
        let summaries = vec![
            SetSummary {
                set: &set_a,
                runs: 6,
                series: 1,
                findings: vec![&findings[0]],
                comparison_base_lags: Vec::new(),
                branch_comparison: None,
            },
            SetSummary {
                set: &set_b,
                runs: 4,
                series: 1,
                findings: Vec::new(),
                comparison_base_lags: Vec::new(),
                branch_comparison: None,
            },
        ];
        let input = ReportInput {
            project: "folo",
            tip_commit: "1234567890abcdef1234",
            tip_dirty: false,
            mode: AnalysisMode::History,
            notable: true,
            runs: 10,
            series: 2,
            commit_span: None,
            report_improvements: false,
            findings: &findings,
            sets: &summaries,
            hint: None,
            warning: None,
            ghosts_excluded: 0,
            census: judged_census(2),
        };
        let report = render(&input, ReportFormat::Markdown, false);
        assert!(report.contains("drift"), "{report}");
        assert!(report.contains("x86_64-unknown-linux-gnu"), "{report}");
        assert!(
            !report.contains("aarch64-apple-darwin"),
            "the empty set is skipped: {report}"
        );
    }

    /// Builds a comparison-base lag from a plain count and reason.
    fn lag(commits_behind: usize, reason: ComparisonBaseLagReason) -> ComparisonBaseLag {
        ComparisonBaseLag {
            commits_behind: NonZero::new(commits_behind).expect("test count is non-zero"),
            reason,
        }
    }

    /// A single-set branch-mode report whose set carries `lags`, for the
    /// comparison-base warning-surface tests.
    fn input_with_lags<'a>(
        set: &'a DiscriminantSet,
        findings: &'a [Finding],
        lags: Vec<ComparisonBaseLag>,
        summaries: &'a mut Vec<SetSummary<'a>>,
    ) -> ReportInput<'a> {
        summaries.push(SetSummary {
            set,
            runs: findings.len().saturating_add(3),
            series: findings.len().max(1),
            findings: findings.iter().collect(),
            comparison_base_lags: lags,
            branch_comparison: None,
        });
        ReportInput {
            project: "folo",
            tip_commit: "1234567890abcdef1234",
            tip_dirty: false,
            mode: AnalysisMode::Branch,
            notable: !findings.is_empty(),
            runs: findings.len().saturating_add(3),
            series: findings.len().max(1),
            commit_span: None,
            report_improvements: false,
            findings,
            sets: summaries,
            hint: None,
            warning: None,
            ghosts_excluded: 0,
            census: judged_census(findings.len().max(1)),
        }
    }

    #[test]
    fn every_format_reports_a_discriminant_set_mismatch_warning() {
        let set = discriminant_set();
        let findings = vec![regression()];
        let mut summaries = Vec::new();
        let input = input_with_lags(
            &set,
            &findings,
            vec![lag(5, ComparisonBaseLagReason::DiscriminantSetMismatch)],
            &mut summaries,
        );
        let expected =
            "Warning: comparison base is 5 commits behind base (discriminant set mismatch)";
        for report in [
            render(&input, ReportFormat::Text, false),
            render(&input, ReportFormat::Markdown, false),
            render_markdown_summary(&input, DEFAULT_SUMMARY_LIMIT),
        ] {
            assert!(report.contains(expected), "{report}");
        }
    }

    #[test]
    fn missing_base_data_warning_uses_a_singular_count() {
        let set = discriminant_set();
        let findings = vec![regression()];
        let mut summaries = Vec::new();
        let input = input_with_lags(
            &set,
            &findings,
            vec![lag(1, ComparisonBaseLagReason::NoRecentBaseData)],
            &mut summaries,
        );
        assert!(
            render(&input, ReportFormat::Text, false).contains(
                "Warning: comparison base is 1 commit behind base \
                 (no base data at more recent commits)"
            ),
            "singular count and generic reason"
        );
    }

    #[test]
    fn json_report_carries_comparison_base_lags_in_order() {
        let set = discriminant_set();
        let findings = vec![regression()];
        let mut summaries = Vec::new();
        let input = input_with_lags(
            &set,
            &findings,
            vec![
                lag(5, ComparisonBaseLagReason::DiscriminantSetMismatch),
                lag(2, ComparisonBaseLagReason::NoRecentBaseData),
            ],
            &mut summaries,
        );
        let json = render(&input, ReportFormat::Json, false);
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        let lags = &parsed["sets"][0]["comparison_base_lags"];
        assert_eq!(lags[0]["commits_behind"], 5);
        assert_eq!(lags[0]["reason"], "discriminant_set_mismatch");
        assert_eq!(lags[1]["commits_behind"], 2);
        assert_eq!(lags[1]["reason"], "no_recent_base_data");
    }

    #[test]
    fn json_report_omits_comparison_base_lags_when_absent() {
        let set = discriminant_set();
        let findings = vec![regression()];
        let mut summaries = Vec::new();
        let input = single_set_input("folo", &set, &findings, &mut summaries);
        let json = render(&input, ReportFormat::Json, false);
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(
            parsed["sets"][0].get("comparison_base_lags").is_none(),
            "unaffected sets carry no comparison_base_lags: {json}"
        );
    }

    #[test]
    fn summary_emits_a_set_warning_once_before_its_findings() {
        // Two findings from the same set: the set's warning must surface exactly once,
        // even though the summary flattens the per-set grouping.
        let set = discriminant_set();
        let findings = vec![
            named_regression("alpha", 0.50),
            named_regression("beta", 0.40),
        ];
        let mut summaries = Vec::new();
        let input = input_with_lags(
            &set,
            &findings,
            vec![lag(3, ComparisonBaseLagReason::DiscriminantSetMismatch)],
            &mut summaries,
        );
        let summary = render_markdown_summary(&input, DEFAULT_SUMMARY_LIMIT);
        let warning =
            "Warning: comparison base is 3 commits behind base (discriminant set mismatch)";
        assert_eq!(summary.matches(warning).count(), 1, "{summary}");
    }

    #[test]
    fn header_shows_the_analyzed_commit_span() {
        let set = discriminant_set();
        let findings = vec![regression()];
        let mut summaries = Vec::new();
        let mut input = single_set_input("folo", &set, &findings, &mut summaries);
        input.runs = 12;
        input.commit_span = Some(("a1b2c3d4e5f60000", "f6e5d4c3b2a10000"));

        // Both ends abbreviate to 12 hex digits, joined by an arrow, so the reader can
        // see the stretch of history the analysis covered.
        let text = render(&input, ReportFormat::Text, false);
        assert!(
            text.contains("runs: 12 (a1b2c3d4e5f6 → f6e5d4c3b2a1)"),
            "{text}"
        );
        let markdown = render(&input, ReportFormat::Markdown, false);
        assert!(
            markdown.contains("- Runs analyzed: 12 (a1b2c3d4e5f6 → f6e5d4c3b2a1)"),
            "{markdown}"
        );
    }

    #[test]
    fn header_collapses_a_single_commit_span() {
        let set = discriminant_set();
        let findings = vec![regression()];
        let mut summaries = Vec::new();
        let mut input = single_set_input("folo", &set, &findings, &mut summaries);
        input.runs = 1;
        input.commit_span = Some(("abcdef0123456789", "abcdef0123456789"));

        // One analyzed commit reads as a single anchor, not `x → x`.
        let text = render(&input, ReportFormat::Text, false);
        assert!(text.contains("runs: 1 (abcdef012345)"), "{text}");
    }

    #[test]
    fn improvement_tally_is_omitted_when_improvements_are_not_reported() {
        let set = discriminant_set();
        let findings = vec![regression()];
        let mut summaries = Vec::new();
        let mut input = single_set_input("folo", &set, &findings, &mut summaries);
        input.report_improvements = false;

        // Neither the top-level header nor the per-set breakdown counts improvements
        // when the analysis reports none (an always-zero tally).
        let text = render(&input, ReportFormat::Text, false);
        assert!(text.contains("regressions: 1"), "{text}");
        assert!(!text.contains("improvements:"), "{text}");

        let markdown = render(&input, ReportFormat::Markdown, false);
        assert!(markdown.contains("- Regressions: 1"), "{markdown}");
        assert!(!markdown.contains("- Improvements:"), "{markdown}");
    }

    #[test]
    fn improvement_tally_is_shown_when_improvements_are_reported() {
        let set = discriminant_set();
        let findings = vec![regression()];
        let mut summaries = Vec::new();
        let mut input = single_set_input("folo", &set, &findings, &mut summaries);
        input.report_improvements = true;

        let text = render(&input, ReportFormat::Text, false);
        assert!(text.contains("improvements: 0"), "{text}");
        let markdown = render(&input, ReportFormat::Markdown, false);
        assert!(markdown.contains("- Improvements: 0"), "{markdown}");
    }

    #[test]
    fn the_bare_series_tally_stays_out_of_text_and_markdown_but_coverage_does_not() {
        let set = discriminant_set();
        let findings = vec![regression()];
        let mut summaries = Vec::new();
        let input = single_set_input("folo", &set, &findings, &mut summaries);

        // A bare "how many series" tally tells a reader nothing, but the share of them
        // that was judged qualifies every verdict in the report, so it is stated.
        let text = render(&input, ReportFormat::Text, false);
        assert!(!text.contains("series: "), "{text}");
        assert!(text.contains("in-scope series judged: 1 of 1"), "{text}");
        let markdown = render(&input, ReportFormat::Markdown, false);
        assert!(!markdown.contains("- Series: "), "{markdown}");
        assert!(
            markdown.contains("- In-scope series judged: 1 of 1"),
            "{markdown}"
        );

        // JSON keeps the series count for machine consumers (e.g. the stress harness).
        let json = render(&input, ReportFormat::Json, false);
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(parsed["series"].is_number(), "{json}");
        assert!(parsed["sets"][0]["series"].is_number(), "{json}");
    }

    #[test]
    fn markdown_wraps_the_metric_name_in_inline_code() {
        let set = discriminant_set();
        let findings = vec![regression()];
        let mut summaries = Vec::new();
        let input = single_set_input("folo", &set, &findings, &mut summaries);

        // The metric name is a keyword-like identifier, so Markdown renders it as inline
        // code (backticks) rather than bare prose. The text report carries no such markup.
        let markdown = render(&input, ReportFormat::Markdown, false);
        assert!(markdown.contains("`instruction_count`"), "{markdown}");

        let text = render(&input, ReportFormat::Text, false);
        assert!(text.contains("instruction_count"), "{text}");
        assert!(!text.contains("`instruction_count`"), "{text}");
    }
}
