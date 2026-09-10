//! The `examine` command: pivot one `(benchmark, metric)` series into its raw
//! per-commit data points.
//!
//! `examine` is a drill-down sibling of `list runs`: both are read-only previews
//! over `analyze`'s exact data-set selection (see
//! `AnalyzeOptions` and `ExamineOptions`) resolved via
//! the shared [`select_dataset`](super::select_dataset) path, so a selection
//! parameter added to `analyze` applies here unchanged. Where `list` counts the
//! selected runs, `examine` names one `(benchmark, metric)` series and lists every
//! commit it spans, in git first-parent order, pairing each value with the short
//! commit id and the start of the commit's title so a maintainer can correlate a
//! value's move with the commit that caused it. A commit carrying no selected
//! observation is listed as `n/a`, so the listing is a complete range of commits
//! rather than a sparse set of measurements.
//!
//! It runs no detection, re-baselining, or blessing: it shows every selected
//! observation with the value and dirty flag exactly as recorded (a commit's clean
//! run and its dirty snapshots each contribute a flagged row, clean before dirty).
//! The text and Markdown
//! renderings lead each set with the whole-series line chart history-mode `analyze`
//! draws for a finding (reusing its renderer), which plots that set's real
//! observations only. Like `analyze` it needs a resolvable repository — first-parent
//! topology to order and name the commits and each commit's title to label them —
//! and repeats the pivot once per matching discriminant set.

use std::collections::HashMap;
use std::path::Path;

use anyspawn::Spawner;
use cbh_command::ExamineOptions;
use cbh_config::{
    Config, cache_env, load_config, resolve_cache_path, resolve_config_path, resolve_local_path,
    resolve_project_id, resolve_repo, storage_env,
};
use cbh_detect::SeriesPoint;
use cbh_diag::{Reporter, ReporterExt, StderrReporter, count_noun};
use cbh_git::{GitHistory, SystemGitHistory};
use cbh_model::{BenchmarkIdPrefix, DiscriminantSet, MetricKind};
use cbh_storage::{Storage, StorageFacade, resolve_storage};
use jiff::Timestamp;
use serde::Serialize;
use tick::Clock;

use super::{
    AutoDiscriminants, ReportFormat, Selection, Series, SeriesFilter, chart_series,
    dirty_base_exception_warning, empty_history_hint, format_value, resolve_auto_discriminants,
    resolve_now, select_dataset,
};
use crate::{
    AnalyzeError, EmptyBenchmarkError, RenderedReports, ReportRequest, UnknownMetricError,
};

/// How many leading characters of a commit title the text and Markdown tables
/// keep. The truncation is a readability convenience of those renderings; the JSON
/// form carries the full title.
const TITLE_LIMIT: usize = 50;

/// What the text and Markdown tables show in place of a value for a commit that
/// carries no data point in this pivot.
const NO_DATA: &str = "n/a";

/// The real `examine`: load configuration, wire the configured storage and git
/// history, and orchestrate.
///
/// `clock_override` injects the [`tick::Clock`] the shared selection anchors its
/// "now" to (see [`analyze`](super::analyze)); production passes `None` for the
/// runtime wall clock.
// Thin real-adapter wiring: loads config from disk, builds the configured storage,
// and shells out via `SystemGitHistory`/`detect_auto_discriminants` before delegating every
// decision to the mutation-tested `examine_with`. In-crate tests cannot drive these
// real adapters deterministically; the binary's integration tests cover this edge.
#[cfg_attr(test, mutants::skip)]
pub async fn execute(
    options: &ExamineOptions,
    workspace_dir: &Path,
    clock_override: Option<Clock>,
    storage_override: Option<StorageFacade>,
    auto_override: Option<AutoDiscriminants>,
) -> Result<RenderedReports, AnalyzeError> {
    let reporter = StderrReporter::new(options.verbose);

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
    storage.synchronize_cache(&project_id, &reporter).await?;

    let git = SystemGitHistory::new(resolve_repo(workspace_dir, options.repo.as_deref()));
    let auto = resolve_auto_discriminants(auto_override).await?;

    let now = resolve_now(clock_override);
    // The object-load work shares the ambient Tokio worker threads (mirrors
    // `analyze::execute`).
    let spawner = Spawner::new_tokio();
    let outcome = examine_with(
        &git,
        &storage,
        &project_id,
        &config,
        options,
        &auto,
        now,
        &reporter,
        &spawner,
    )
    .await;
    storage.report_cache_tally(&reporter);
    outcome
}

/// Storage- and git-generic `examine`: resolve the same data set `analyze` would,
/// narrow it to one `(benchmark, metric)` series per discriminant set, and render
/// the ordered data points.
#[expect(
    clippy::too_many_arguments,
    reason = "mirrors the analyze selection pipeline, which threads the same injected ports"
)]
pub(crate) async fn examine_with<G, S>(
    git: &G,
    storage: &S,
    project_id: &str,
    config: &Config,
    options: &ExamineOptions,
    auto: &AutoDiscriminants,
    now: Timestamp,
    reporter: &dyn Reporter,
    spawner: &Spawner,
) -> Result<RenderedReports, AnalyzeError>
where
    G: GitHistory,
    S: Storage + Clone + 'static,
{
    let request = ReportRequest::resolve(
        options.no_text,
        options.markdown.as_deref(),
        options.json.as_deref(),
    )?;

    // Reject an unknown metric name up front, before any load — the one command
    // that names a metric validates it against the known set.
    let metric_kind = parse_metric(&options.metric)?;

    // The benchmark identity scopes the series load coarsely (a prefix of the
    // qualified id); the exact `id == benchmark` narrowing happens after series
    // reconstruction. An unmatched id is not an error — it yields an empty pivot.
    let prefix = BenchmarkIdPrefix::new(options.benchmark.clone())
        .map_err(EmptyBenchmarkError::caused_by)?;
    let prefixes = [prefix];
    let filter = SeriesFilter {
        prefixes: &prefixes,
    };

    let selection = Selection::from_examine(options);
    let dataset = select_dataset(
        git, storage, project_id, config, &selection, filter, false, auto, now, reporter, spawner,
    )
    .await?;

    let pivot = build_pivot(
        project_id,
        &options.benchmark,
        metric_kind,
        &dataset.series,
        &dataset.commit_subjects,
        &dataset.ordered_commits,
        dataset.tip_index,
    );
    report_listed_range(
        &pivot,
        &dataset.ordered_commits,
        dataset.run_index.total(),
        reporter,
    );

    // When the pivot is empty, explain why: either no run entered the selection at
    // all (the same self-explaining hint `analyze` gives), or runs entered but none
    // carried this `(benchmark, metric)` pair (a data-dependent id/name mismatch).
    let hint = if !pivot.sets.is_empty() {
        None
    } else if dataset.run_index.is_empty() {
        empty_history_hint(
            true,
            dataset.candidate_count,
            &dataset.target_ref,
            dataset.tally,
            &dataset.discriminants,
        )
    } else {
        Some(unmatched_series_hint(
            &options.benchmark,
            metric_kind,
            dataset.run_index.total(),
        ))
    };

    // The same ephemeral-data warning `analyze`/`list` show when a dirty
    // base-branch-tip run was admitted because the working tree is dirty.
    let warning = dataset
        .included_dirty_base_exception
        .then(dirty_base_exception_warning);

    Ok(request.render(|format| render_pivot(&pivot, format, hint.as_deref(), warning.as_deref())))
}

/// Parses the `--metric` value into a [`MetricKind`], rejecting an unknown name
/// with the list of valid names.
fn parse_metric(name: &str) -> Result<MetricKind, AnalyzeError> {
    MetricKind::from_name(name).ok_or_else(|| {
        let valid = MetricKind::ALL
            .iter()
            .map(|kind| kind.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        UnknownMetricError::new(name, valid).into()
    })
}

/// One recorded observation of the examined series.
#[derive(Clone, Debug)]
struct Observation {
    /// The measured value.
    value: f64,
    /// Whether the observation came from a dirty (uncommitted-tree) snapshot.
    dirty: bool,
}

/// One row of the pivot: a commit in the listed range, and the observation it
/// carries — or none, when no selected run recorded this series against it.
#[derive(Clone, Debug)]
struct DataPoint {
    /// The commit the row names (full commit ID, or a label in tests).
    commit: String,
    /// The commit's full title (subject), empty when topology reported none.
    title: String,
    /// First-parent topological index of the commit (oldest = 0). It places the
    /// row's observation in its own per-commit chart column; the tables and JSON
    /// never render it.
    topo_index: usize,
    /// The measurement recorded here, or `None` for a commit with no data point in
    /// this pivot — which the tables show as [`NO_DATA`].
    observation: Option<Observation>,
}

/// One discriminant set's slice of the pivot.
#[derive(Clone, Debug)]
struct SetPivot {
    /// The comparable partition these points share.
    set: DiscriminantSet,
    /// One row per commit in the pivot's range, oldest first by git topology. A
    /// commit carrying several observations contributes one row per observation
    /// (clean before dirty); a commit carrying none contributes a single data-less
    /// row.
    points: Vec<DataPoint>,
}

/// The fully resolved pivot of one `(benchmark, metric)` series, ready to render.
#[derive(Clone, Debug)]
struct Pivot {
    /// The project the data belongs to.
    project: String,
    /// The qualified benchmark identity that was examined.
    benchmark: String,
    /// The metric's stable name.
    metric: &'static str,
    /// The metric's unit, for display.
    unit: &'static str,
    /// The per-set slices, one entry per discriminant set carrying the series.
    sets: Vec<SetPivot>,
    /// The inclusive first-parent index range every set lists, for the verbose
    /// note. `None` when no set carries the series.
    range: Option<(usize, usize)>,
    /// Trailing-fill target for each set's chart: the analyzed tip's first-parent
    /// index, so a series that stops short of the tip renders the data-less commits
    /// after its last observation as a gap. Chart-only.
    base_ref: Option<usize>,
}

/// Narrows the reconstructed series to the one `(benchmark, metric)` pair, once per
/// discriminant set, and lists every commit in the pivot's range against it.
///
/// `tip_index` is the analyzed tip's first-parent index: it both closes the listed
/// range and serves as the chart's trailing-fill target, so a series that stops short
/// of the tip renders a trailing gap in both (see [`chart_of_points`]).
/// `ordered_commits` names the commit at each first-parent index, so a commit with no
/// observation can still be listed.
fn build_pivot(
    project_id: &str,
    benchmark: &str,
    metric_kind: MetricKind,
    series: &[Series],
    commit_subjects: &HashMap<String, String>,
    ordered_commits: &[String],
    tip_index: usize,
) -> Pivot {
    let mut matching: Vec<&Series> = series
        .iter()
        .filter(|one| one.kind == metric_kind && one.id.qualified() == benchmark)
        .collect();
    // Sort the sets deterministically so the same data set always renders in the
    // same order regardless of series-build order.
    matching.sort_by(|left, right| left.set.cmp(&right.set));

    let observations: Vec<&[SeriesPoint]> =
        matching.iter().map(|one| one.points.as_slice()).collect();
    let range = pivot_range(&observations, tip_index);
    let sets = matching
        .iter()
        .map(|one| SetPivot {
            set: one.set.clone(),
            points: range.map_or_else(Vec::new, |range| {
                points_in_range(&one.points, range, ordered_commits, commit_subjects)
            }),
        })
        .collect();

    Pivot {
        project: project_id.to_owned(),
        benchmark: benchmark.to_owned(),
        metric: metric_kind.as_str(),
        unit: metric_kind.as_unit(),
        sets,
        range,
        base_ref: Some(tip_index),
    }
}

/// The inclusive first-parent index range every set lists: from the earliest
/// observation of *any* matching set to the analyzed tip.
///
/// The start is a union across the sets rather than each set's own first
/// observation, so every set lists exactly the same commits and their tables can be
/// read side by side. `None` when no set carries an observation — there is then
/// nothing to anchor a range to, and nothing to list.
///
/// The start is the minimum topological index rather than the leading point's, so
/// the range covers every observation even if a set ever arrives unsorted.
///
/// The tip closes the range unconditionally: it is the last commit of the
/// first-parent ancestry the topological indices were assigned from, so no
/// observation can sit past it.
fn pivot_range(observations: &[&[SeriesPoint]], tip_index: usize) -> Option<(usize, usize)> {
    let start = observations
        .iter()
        .flat_map(|points| points.iter())
        .map(|point| point.topo_index)
        .min()?;
    Some((start, tip_index))
}

/// Lists every commit in `range` against one set's `observations`, mirroring how the
/// chart materializes a data-less commit as a gap.
///
/// A commit with observations contributes one row per observation, in the order the
/// series
/// already holds them (clean before dirty, then object ordinal); a commit with none
/// contributes a single data-less row. `observations` is ordered by ascending
/// topological index, so one forward cursor walks it alongside the range.
fn points_in_range(
    observations: &[SeriesPoint],
    range: (usize, usize),
    ordered_commits: &[String],
    commit_subjects: &HashMap<String, String>,
) -> Vec<DataPoint> {
    let (start, end) = range;
    // Every commit in the range contributes at least one row, and a commit with
    // several observations contributes one each.
    let mut points = Vec::with_capacity(
        end.saturating_sub(start)
            .saturating_add(1)
            .saturating_add(observations.len()),
    );
    let mut cursor = observations
        .iter()
        .skip_while(|point| point.topo_index < start)
        .peekable();
    for topo_index in start..=end {
        let commit = ordered_commits
            .get(topo_index)
            .expect("the range is bounded by the same first-parent ancestry it indexes");
        let title = commit_subjects.get(commit).cloned().unwrap_or_default();
        let mut listed = false;
        while let Some(point) = cursor.next_if(|point| point.topo_index == topo_index) {
            points.push(DataPoint {
                commit: commit.clone(),
                title: title.clone(),
                topo_index,
                observation: Some(Observation {
                    value: point.value,
                    dirty: point.dirty,
                }),
            });
            listed = true;
        }
        if !listed {
            points.push(DataPoint {
                commit: commit.clone(),
                title,
                topo_index,
                observation: None,
            });
        }
    }
    points
}

/// Explains, under `--verbose`, which commits the listing spans and why it opens
/// and closes where it does — so a reader can tell an `n/a` row (a commit inside the
/// range with no data) from a commit the range deliberately never reached.
fn report_listed_range(
    pivot: &Pivot,
    ordered_commits: &[String],
    entered: usize,
    reporter: &dyn Reporter,
) {
    reporter.note_with(|| {
        let Some((start, end)) = pivot.range else {
            if entered == 0 {
                return "commit listing spans no commits: no run entered the selection at all, \
                        so there is no observation to anchor a range to"
                    .to_owned();
            }
            return format!(
                "commit listing spans no commits: {} entered the selection, but none recorded \
                 benchmark {} with metric {}, so there is no observation to anchor a range to",
                count_noun(entered, "run"),
                pivot.benchmark,
                pivot.metric
            );
        };
        let name = |index: usize| {
            ordered_commits
                .get(index)
                .map_or("<unknown>", |commit| short_commit_id(commit))
        };
        format!(
            "commit listing spans {} from {} to {}: the range opens at the earliest observation \
             across the {} carrying this series (first-parent index {start}) and closes at the \
             analyzed tip (index {end}), so every set lists the same commits and their tables \
             line up; a commit inside the range with no selected observation is listed as \
             {NO_DATA} rather than omitted, and `--since` is what narrows the range",
            count_noun(end.saturating_sub(start).saturating_add(1), "commit"),
            name(start),
            name(end),
            count_noun(pivot.sets.len(), "discriminant set"),
        )
    });
}

/// The hint shown when runs entered the selection but none carried the examined
/// `(benchmark, metric)` pair — a data-dependent id or name mismatch.
fn unmatched_series_hint(benchmark: &str, metric: MetricKind, entered: usize) -> String {
    format!(
        "{} entered the analysis, but none recorded benchmark {benchmark:?} with metric {:?}. \
         Check the benchmark id and metric name — copy them verbatim from an `analyze` finding \
         (`list runs` shows the benchmark ids present in the data set).",
        count_noun(entered, "run"),
        metric.as_str(),
    )
}

/// The first [`TITLE_LIMIT`] characters of a commit title, for the text and
/// Markdown tables.
fn truncate_title(title: &str) -> String {
    title.chars().take(TITLE_LIMIT).collect()
}

/// The short commit id: the first 12 characters of the commit ID (mirrors the
/// abbreviation `list`, `bless`, and `backfill` use).
fn short_commit_id(commit_id: &str) -> &str {
    commit_id.get(..12).unwrap_or(commit_id)
}

/// The value cell of a row: the formatted measurement, or [`NO_DATA`] for a commit
/// carrying no data point.
fn cell_value(point: &DataPoint) -> String {
    point.observation.as_ref().map_or_else(
        || NO_DATA.to_owned(),
        |observation| format_value(observation.value),
    )
}

/// Whether a row's observation came from a dirty snapshot. A data-less row is not
/// dirty: it has no run whose cleanliness could be described.
fn is_dirty(point: &DataPoint) -> bool {
    point
        .observation
        .as_ref()
        .is_some_and(|observation| observation.dirty)
}

/// The line chart of a set's observations, drawn before its data points (the same
/// chart history-mode `analyze` renders for a finding). Each observation sits in its
/// own per-commit column, with the data-less commits between observations — and,
/// when `base_ref` extends past the last observation, the trailing commits up to the
/// analyzed tip — rendered as gaps. `None` when there are too few points to plot.
///
/// The chart plots the real observations only, so it trims its own leading gap: a
/// set whose first observation is later than the listed range's start draws a chart
/// that begins after its table does.
fn chart_of_points(points: &[DataPoint], base_ref: Option<usize>) -> Option<String> {
    let pairs: Vec<(usize, f64)> = observed_pairs(points);
    chart_series(&pairs, base_ref)
}

/// The `(topological index, value)` pairs of the rows that carry an observation —
/// the chart's input, which ignores the data-less rows the tables list.
fn observed_pairs(points: &[DataPoint]) -> Vec<(usize, f64)> {
    points
        .iter()
        .filter_map(|point| {
            point
                .observation
                .as_ref()
                .map(|observation| (point.topo_index, observation.value))
        })
        .collect()
}

/// Renders the pivot in the requested format, appending the diagnostic hint and
/// ephemeral-data warning (if any).
fn render_pivot(
    pivot: &Pivot,
    format: ReportFormat,
    hint: Option<&str>,
    warning: Option<&str>,
) -> String {
    match format {
        ReportFormat::Text => render_pivot_text(pivot, hint, warning),
        ReportFormat::Markdown => render_pivot_markdown(pivot, hint, warning),
        ReportFormat::Json => render_pivot_json(pivot, hint, warning),
    }
}

fn render_pivot_text(pivot: &Pivot, hint: Option<&str>, warning: Option<&str>) -> String {
    let mut lines = vec![format!(
        "Data points for {} metric {} ({}) in project {}",
        pivot.benchmark, pivot.metric, pivot.unit, pivot.project
    )];
    if pivot.sets.is_empty() {
        lines.push(String::new());
        lines.push("No data point matches the selection.".to_owned());
    } else {
        for set in &pivot.sets {
            lines.push(String::new());
            lines.push(set.set.to_string());
            // Lead the set with the same small line chart `analyze` draws, so a
            // maintainer sees the shape of the series before reading the points it
            // pivots.
            if let Some(chart) = chart_of_points(&set.points, pivot.base_ref) {
                lines.push(chart);
                lines.push(String::new());
            }
            // Align the commit and value columns so a maintainer can read the values
            // straight down and spot where one jumps.
            let commit_width = set
                .points
                .iter()
                .map(|point| short_commit_id(&point.commit).len())
                .max()
                .unwrap_or(0);
            let values: Vec<String> = set.points.iter().map(cell_value).collect();
            let value_width = values.iter().map(String::len).max().unwrap_or(0);
            for (point, value) in set.points.iter().zip(&values) {
                let title = truncate_title(&point.title);
                let marker = if is_dirty(point) { "  (dirty)" } else { "" };
                lines.push(format!(
                    "  {commit:<commit_width$}  {value:>value_width$}  {title}{marker}",
                    commit = short_commit_id(&point.commit),
                ));
            }
        }
    }
    append_hint_and_warning(&mut lines, hint, warning);
    format!("{}\n", lines.join("\n"))
}

fn render_pivot_markdown(pivot: &Pivot, hint: Option<&str>, warning: Option<&str>) -> String {
    let mut lines = vec![
        format!("# Data points for {} in {}", pivot.benchmark, pivot.project),
        String::new(),
        format!("**Metric:** {} ({})", pivot.metric, pivot.unit),
    ];
    if pivot.sets.is_empty() {
        lines.push(String::new());
        lines.push("No data point matches the selection.".to_owned());
    } else {
        for set in &pivot.sets {
            lines.push(String::new());
            lines.push(format!("## {}", set.set));
            // The same series chart the text pivot draws, fenced as a `text`
            // block so it survives Markdown rendering (mirrors `analyze`).
            if let Some(chart) = chart_of_points(&set.points, pivot.base_ref) {
                lines.push(String::new());
                lines.push("```text".to_owned());
                lines.push(chart);
                lines.push("```".to_owned());
            }
            lines.push(String::new());
            lines.push("| Commit | Value | Kind | Title |".to_owned());
            lines.push("| --- | --- | --- | --- |".to_owned());
            for point in &set.points {
                let kind = match &point.observation {
                    Some(observation) if observation.dirty => "dirty",
                    Some(_) => "clean",
                    None => NO_DATA,
                };
                lines.push(format!(
                    "| {} | {} | {} | {} |",
                    short_commit_id(&point.commit),
                    cell_value(point),
                    kind,
                    escape_cell(&truncate_title(&point.title)),
                ));
            }
        }
    }
    append_hint_and_warning(&mut lines, hint, warning);
    format!("{}\n", lines.join("\n"))
}

fn render_pivot_json(pivot: &Pivot, hint: Option<&str>, warning: Option<&str>) -> String {
    #[derive(Serialize)]
    struct JsonPoint<'a> {
        commit: &'a str,
        /// `null` for a commit that carries no data point in this pivot.
        value: Option<f64>,
        /// Absent for a commit that carries no data point: there is no run whose
        /// cleanliness the flag could describe.
        #[serde(skip_serializing_if = "Option::is_none")]
        dirty: Option<bool>,
        title: &'a str,
    }
    #[derive(Serialize)]
    struct JsonSet<'a> {
        engine: &'a str,
        target_triple: &'a str,
        machine_key: &'a str,
        points: Vec<JsonPoint<'a>>,
    }
    #[derive(Serialize)]
    struct JsonPivot<'a> {
        project: &'a str,
        benchmark: &'a str,
        metric: &'a str,
        unit: &'a str,
        sets: Vec<JsonSet<'a>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        hint: Option<&'a str>,
        #[serde(skip_serializing_if = "Option::is_none")]
        warning: Option<&'a str>,
    }

    let sets: Vec<JsonSet<'_>> = pivot
        .sets
        .iter()
        .map(|set| JsonSet {
            engine: set.set.engine.as_str(),
            target_triple: set.set.target_triple.as_str(),
            machine_key: set.set.machine_key.as_str(),
            points: set
                .points
                .iter()
                .map(|point| JsonPoint {
                    commit: &point.commit,
                    value: point.observation.as_ref().map(|one| one.value),
                    dirty: point.observation.as_ref().map(|one| one.dirty),
                    title: &point.title,
                })
                .collect(),
        })
        .collect();

    let document = JsonPivot {
        project: &pivot.project,
        benchmark: &pivot.benchmark,
        metric: pivot.metric,
        unit: pivot.unit,
        sets,
        hint,
        warning,
    };
    serde_json::to_string_pretty(&document).expect("pivot structures always serialize to JSON")
}

/// Escapes a Markdown table cell so a commit title's pipe characters do not break
/// the table's column layout.
fn escape_cell(value: &str) -> String {
    value.replace('|', "\\|")
}

/// Appends the hint and warning (if any) as trailing, blank-line-separated blocks
/// so they read at the very end of a text or Markdown pivot.
fn append_hint_and_warning(lines: &mut Vec<String>, hint: Option<&str>, warning: Option<&str>) {
    if let Some(hint) = hint {
        lines.push(String::new());
        lines.push(hint.to_owned());
    }
    if let Some(warning) = warning {
        lines.push(String::new());
        lines.push(warning.to_owned());
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    #![allow(clippy::indexing_slicing, reason = "panic is fine in tests")]
    use std::path::PathBuf;

    use cbh_config::Config;
    use cbh_diag::RecordingReporter;
    use cbh_git::FakeGitHistory;
    use cbh_model::{
        BenchmarkId, BenchmarkResult, EnvironmentInfo, GitInfo, Metric, MetricKind, Run,
        RunContext, ToolchainInfo,
    };
    use cbh_storage::MemoryStorage;
    use futures::executor::block_on;
    use jiff::Timestamp;
    use nonempty::nonempty;
    use ohno::ErrorExt as _;

    use super::*;
    use crate::testing::store_run as store;
    use crate::{EmptyBenchmarkError, UnknownMetricError, UnresolvedRefError};

    fn config() -> Config {
        Config::default()
    }

    /// The auto-detected discriminant values the tests seed their default partition under.
    fn auto() -> AutoDiscriminants {
        AutoDiscriminants {
            triple: "x86_64-unknown-linux-gnu".to_owned(),
            machine_key: "m1".into(),
        }
    }

    fn options() -> ExamineOptions {
        ExamineOptions {
            benchmark: "nm/nm::observe/pull".to_owned(),
            metric: "instruction_count".to_owned(),
            ..ExamineOptions::default()
        }
    }

    /// An inline spawner that runs the load tasks on the calling thread, so the
    /// tests need no Tokio runtime under `block_on` or Miri.
    fn spawner() -> Spawner {
        cbh_detect::testing::synchronous_spawner()
    }

    /// A run with one benchmark carrying a single instruction-count metric of the
    /// given value on the given commit.
    fn single_metric_run(effective: i64, commit: &str, value: f64) -> Run {
        let time = Timestamp::from_second(effective).unwrap();
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

    /// A run whose one benchmark carries two metrics, so the partition reconstructs
    /// two distinct series (only one of which any single `--metric` selects).
    fn two_metric_run(effective: i64, commit: &str, ir: f64, branches: f64) -> Run {
        let time = Timestamp::from_second(effective).unwrap();
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

    fn clean_key(commit: &str) -> String {
        format!("v1/folo/objects/callgrind/x86_64-unknown-linux-gnu/m1/{commit}/clean.json")
    }

    fn dirty_key(commit: &str, unix: i64) -> String {
        format!("v1/folo/objects/callgrind/x86_64-unknown-linux-gnu/m1/{commit}/dirty-{unix}.json")
    }

    /// A linear history `c0 <- c1 <- c2 <- c3` with commit titles, on the default
    /// branch `master`.
    fn linear_git() -> FakeGitHistory {
        let mut git = FakeGitHistory::new();
        git.commit("c0", None)
            .commit("c1", Some("c0"))
            .commit("c2", Some("c1"))
            .commit("c3", Some("c2"))
            .subject("c0", "Add the pull benchmark")
            .subject("c1", "Optimize the hot loop")
            .subject(
                "c2",
                "Refactor the observer to shave allocations off the record path",
            )
            .branch("master", "c3")
            .head("master")
            .mark_default("master");
        git
    }

    /// Drives `examine_with` and unwraps the rendered text message.
    fn examine(storage: &MemoryStorage, git: &FakeGitHistory, options: &ExamineOptions) -> String {
        examine_reports(storage, git, options)
            .text
            .expect("examine renders the text report by default")
    }

    /// Drives the query once for every requested presentation of the same pivot.
    fn examine_reports(
        storage: &MemoryStorage,
        git: &FakeGitHistory,
        options: &ExamineOptions,
    ) -> RenderedReports {
        block_on(examine_with(
            git,
            storage,
            "folo",
            &config(),
            options,
            &auto(),
            Timestamp::from_second(0).unwrap(),
            &RecordingReporter::quiet(),
            &spawner(),
        ))
        .unwrap()
    }

    /// Drives `examine_with` requesting the JSON report and returns the JSON text
    /// (the text report is suppressed).
    fn examine_json(
        storage: &MemoryStorage,
        git: &FakeGitHistory,
        options: &ExamineOptions,
    ) -> String {
        let mut options = options.clone();
        options.no_text = true;
        options.markdown = None;
        options.json = Some(PathBuf::from("report.json"));
        let rendered = block_on(examine_with(
            git,
            storage,
            "folo",
            &config(),
            &options,
            &auto(),
            Timestamp::from_second(0).unwrap(),
            &RecordingReporter::quiet(),
            &spawner(),
        ))
        .unwrap();
        rendered
            .json
            .expect("the JSON report was rendered for the requested path")
    }

    /// Drives `examine_with` requesting the Markdown report and returns the Markdown
    /// text.
    fn examine_markdown(
        storage: &MemoryStorage,
        git: &FakeGitHistory,
        options: &ExamineOptions,
    ) -> String {
        let mut options = options.clone();
        options.no_text = true;
        options.json = None;
        options.markdown = Some(PathBuf::from("report.md"));
        let rendered = block_on(examine_with(
            git,
            storage,
            "folo",
            &config(),
            &options,
            &auto(),
            Timestamp::from_second(0).unwrap(),
            &RecordingReporter::quiet(),
            &spawner(),
        ))
        .unwrap();
        rendered
            .markdown
            .expect("the Markdown report was rendered for the requested path")
    }

    /// Drives `examine_with` and returns the diagnostic notes the run emitted, for
    /// the tests that assert on `--verbose` reasoning rather than on the report.
    fn examine_notes(
        storage: &MemoryStorage,
        git: &FakeGitHistory,
        options: &ExamineOptions,
    ) -> Vec<String> {
        let reporter = RecordingReporter::new();
        block_on(examine_with(
            git,
            storage,
            "folo",
            &config(),
            options,
            &auto(),
            Timestamp::from_second(0).unwrap(),
            &reporter,
            &spawner(),
        ))
        .unwrap();
        reporter.notes()
    }

    #[test]
    fn pivots_one_series_into_ordered_points() {
        let storage = MemoryStorage::new();
        store(
            &storage,
            &clean_key("c0"),
            &single_metric_run(0, "c0", 100.0),
        );
        store(
            &storage,
            &clean_key("c1"),
            &single_metric_run(1, "c1", 130.0),
        );
        store(
            &storage,
            &clean_key("c2"),
            &single_metric_run(2, "c2", 128.0),
        );
        let git = linear_git();

        let report = examine_json(&storage, &git, &options());
        let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();

        assert_eq!(parsed["benchmark"], "nm/nm::observe/pull");
        assert_eq!(parsed["metric"], "instruction_count");
        assert_eq!(parsed["unit"], "count");
        let sets = parsed["sets"].as_array().unwrap();
        assert_eq!(sets.len(), 1);
        let points = sets[0]["points"].as_array().unwrap();
        assert_eq!(points.len(), 4, "every commit c0..=c3 is listed: {report}");
        // Oldest first by topology.
        assert_eq!(points[0]["commit"], "c0");
        assert_eq!(points[0]["value"], 100.0);
        assert_eq!(points[0]["dirty"], false);
        assert_eq!(points[0]["title"], "Add the pull benchmark");
        assert_eq!(points[2]["commit"], "c2");
        assert_eq!(points[2]["value"], 128.0);
        // Data stops before the tip, which is listed without a value.
        assert_eq!(points[3]["commit"], "c3");
        assert!(points[3]["value"].is_null(), "{report}");
        assert!(points[3].get("dirty").is_none(), "{report}");
    }

    #[test]
    fn json_keeps_full_precision_and_full_title() {
        let storage = MemoryStorage::new();
        store(
            &storage,
            &clean_key("c2"),
            &single_metric_run(2, "c2", 128.499_999),
        );
        let git = linear_git();

        let report = examine_json(&storage, &git, &options());
        let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
        let point = &parsed["sets"][0]["points"][0];
        assert_eq!(point["value"], 128.499_999);
        // The full, untruncated title (54 chars) survives in JSON.
        assert_eq!(
            point["title"],
            "Refactor the observer to shave allocations off the record path"
        );
    }

    #[test]
    fn text_truncates_the_title_to_fifty_characters() {
        let storage = MemoryStorage::new();
        store(
            &storage,
            &clean_key("c2"),
            &single_metric_run(2, "c2", 128.0),
        );
        let git = linear_git();

        let report = examine(&storage, &git, &options());
        assert!(
            report.contains("Data points for nm/nm::observe/pull"),
            "{report}"
        );
        // The 62-char subject is cut to its first 50 characters ("...off the"),
        // dropping the trailing " record path".
        assert!(
            report.contains("Refactor the observer to shave allocations off the"),
            "{report}"
        );
        assert!(
            !report.contains("record path"),
            "the title is truncated to 50 characters: {report}"
        );
    }

    #[test]
    fn flags_dirty_snapshots_after_the_clean_run() {
        // On the base branch (linear history, tip == merge-base) with a dirty
        // working tree, the tip's clean run and its dirty snapshot both show — the
        // dirty snapshot admitted via the base-tip dirty exception, ordered after
        // the clean run and flagged.
        let storage = MemoryStorage::new();
        store(
            &storage,
            &clean_key("c3"),
            &single_metric_run(3, "c3", 100.0),
        );
        store(
            &storage,
            &dirty_key("c3", 400),
            &single_metric_run(4, "c3", 118.0),
        );
        let mut git = linear_git();
        git.mark_dirty();

        let options = ExamineOptions {
            json: Some(PathBuf::from("report.json")),
            markdown: Some(PathBuf::from("report.md")),
            ..options()
        };
        let rendered = examine_reports(&storage, &git, &options);
        let report = rendered.json.unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
        let points = parsed["sets"][0]["points"].as_array().unwrap();
        assert_eq!(points.len(), 2, "clean and dirty both shown: {report}");
        assert_eq!(points[0]["dirty"], false, "clean first");
        assert_eq!(points[0]["value"], 100.0);
        assert_eq!(points[1]["dirty"], true, "dirty second");
        assert_eq!(points[1]["value"], 118.0);
        // The dirty tip snapshot is ephemeral, so the pivot carries the warning.
        assert!(
            parsed["warning"].as_str().is_some(),
            "ephemeral-data warning present: {report}"
        );

        let text = rendered.text.unwrap();
        assert!(text.contains("(dirty)"), "the dirty row is flagged: {text}");
        assert_eq!(
            text.matches("(dirty)").count(),
            1,
            "only the dirty row is flagged, not the clean one: {text}"
        );

        let markdown = rendered.markdown.unwrap();
        assert!(
            markdown.contains("| c3 | 100 | clean |"),
            "the clean run's kind: {markdown}"
        );
        assert!(
            markdown.contains("| c3 | 118 | dirty |"),
            "the dirty snapshot's kind: {markdown}"
        );
    }

    #[test]
    fn selects_only_the_named_metric_of_a_multi_metric_benchmark() {
        let storage = MemoryStorage::new();
        store(
            &storage,
            &clean_key("c3"),
            &two_metric_run(3, "c3", 100.0, 250.0),
        );
        let git = linear_git();

        let report = examine_json(&storage, &git, &options());
        let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
        let points = parsed["sets"][0]["points"].as_array().unwrap();
        assert_eq!(
            points.len(),
            1,
            "only the measured tip is needed to compare metric selection: {report}"
        );
        // The instruction-count value, not the conditional-branches one.
        assert_eq!(points[0]["value"], 100.0);

        let branches = ExamineOptions {
            metric: "conditional_branches".to_owned(),
            ..options()
        };
        let report = examine_json(&storage, &git, &branches);
        let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
        assert_eq!(parsed["sets"][0]["points"][0]["value"], 250.0);
    }

    #[test]
    fn markdown_renders_a_per_set_table() {
        let storage = MemoryStorage::new();
        store(
            &storage,
            &clean_key("c0"),
            &single_metric_run(0, "c0", 100.0),
        );
        let git = linear_git();

        let report = examine_markdown(&storage, &git, &options());
        assert!(
            report.contains("# Data points for nm/nm::observe/pull in folo"),
            "{report}"
        );
        assert!(
            report.contains("**Metric:** instruction_count (count)"),
            "{report}"
        );
        assert!(
            report.contains("| Commit | Value | Kind | Title |"),
            "{report}"
        );
        assert!(report.contains("| c0 | 100 | clean |"), "{report}");
    }

    #[test]
    fn text_draws_a_chart_before_the_points() {
        let storage = MemoryStorage::new();
        store(
            &storage,
            &clean_key("c0"),
            &single_metric_run(0, "c0", 100.0),
        );
        store(
            &storage,
            &clean_key("c1"),
            &single_metric_run(1, "c1", 130.0),
        );
        store(
            &storage,
            &clean_key("c2"),
            &single_metric_run(2, "c2", 128.0),
        );
        let git = linear_git();

        let report = examine(&storage, &git, &options());
        // The rasciigraph axis marker proves the chart was drawn.
        let axis = report
            .find('┤')
            .or_else(|| report.find('┼'))
            .expect("a chart is drawn");
        // It leads the set: the chart precedes the first point row (the c0 title).
        let first_point = report
            .find("Add the pull benchmark")
            .expect("the first point row is present");
        assert!(
            axis < first_point,
            "the chart precedes the points: {report}"
        );
        // The chart carries no color (no ANSI escapes).
        assert!(!report.contains('\u{1b}'), "no ANSI escape: {report:?}");
    }

    #[test]
    fn markdown_fences_the_chart_before_the_table() {
        let storage = MemoryStorage::new();
        store(
            &storage,
            &clean_key("c0"),
            &single_metric_run(0, "c0", 100.0),
        );
        store(
            &storage,
            &clean_key("c1"),
            &single_metric_run(1, "c1", 130.0),
        );
        let git = linear_git();

        let report = examine_markdown(&storage, &git, &options());
        let fence = report.find("```text").expect("the chart is fenced");
        assert!(
            report.contains('┤') || report.contains('┼'),
            "a chart is drawn: {report}"
        );
        // The fenced chart precedes the per-set table.
        let table = report
            .find("| Commit | Value | Kind | Title |")
            .expect("the table header is present");
        assert!(fence < table, "the chart precedes the table: {report}");
    }

    #[test]
    fn a_single_point_set_draws_no_chart() {
        // A lone observation has too few points to plot, so no chart is drawn.
        let storage = MemoryStorage::new();
        store(
            &storage,
            &clean_key("c0"),
            &single_metric_run(0, "c0", 100.0),
        );
        let git = linear_git();

        let report = examine(&storage, &git, &options());
        assert!(
            !report.contains('┤') && !report.contains('┼'),
            "no chart for a single point: {report}"
        );
    }

    /// A `DataPoint` carrying an observation at the given topology and value, with
    /// the other fields fixed — enough to drive [`chart_of_points`] and
    /// [`cbh_render::topology_columns`].
    fn data_point(topo: usize, value: f64) -> DataPoint {
        DataPoint {
            commit: format!("c{topo}"),
            title: String::new(),
            topo_index: topo,
            observation: Some(Observation {
                value,
                dirty: false,
            }),
        }
    }

    /// A `DataPoint` for a commit that carries no observation — the `n/a` row.
    fn gap_point(topo: usize) -> DataPoint {
        DataPoint {
            commit: format!("c{topo}"),
            title: String::new(),
            topo_index: topo,
            observation: None,
        }
    }

    /// A series observation at the given topology, carrying the fields the pivot
    /// reads and neutral values for the rest.
    fn observation(topo: usize, value: f64) -> SeriesPoint {
        SeriesPoint {
            topo_index: topo,
            dirty: false,
            object_ordinal: 0,
            commit: None,
            value,
            interval_low: None,
            interval_high: None,
        }
    }

    /// The ordered first-parent ancestry the pivot names its commits from.
    fn ordered_commits(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| (*name).to_owned()).collect()
    }

    #[test]
    fn chart_of_points_materializes_a_data_less_interior_commit_as_one_gap() {
        // c0, c1, then c3 have data; the interior c2 (topo 2) does not. The tip is
        // c3 (topo 3), so there is no trailing gap — exactly one interior gap column.
        let gapped = [
            data_point(0, 100.0),
            data_point(1, 130.0),
            gap_point(2),
            data_point(3, 128.0),
        ];
        let pairs = observed_pairs(&gapped);
        // The span (3) is below the chart width, so the column count is exact and
        // independent of the 48-wide cap: one column per commit c0..=c3.
        let columns = cbh_render::topology_columns(&pairs, Some(3), 48);
        assert_eq!(columns.len(), 4, "one column per commit c0..=c3");
        assert_eq!(
            columns.iter().filter(|value| value.is_nan()).count(),
            1,
            "exactly the data-less c2 is a gap"
        );
        assert!(columns[2].is_nan(), "the gap sits at c2's column");
        assert!(
            columns[0].is_finite() && columns[1].is_finite() && columns[3].is_finite(),
            "the three real observations survive"
        );

        // The rendered chart differs from the same three values placed contiguously,
        // proving topology (not observation order) drives the column layout.
        let contiguous = [
            data_point(0, 100.0),
            data_point(1, 130.0),
            data_point(2, 128.0),
        ];
        let with_gap = chart_of_points(&gapped, Some(3)).expect("the gapped series draws");
        let without_gap =
            chart_of_points(&contiguous, Some(2)).expect("the contiguous series draws");
        assert_ne!(
            with_gap, without_gap,
            "the interior gap changes the chart raster"
        );
    }

    #[test]
    fn chart_of_points_materializes_the_no_newer_data_tail_as_a_trailing_gap() {
        // Data stops at c2 while the analyzed tip is c3 (topo 3): the last commit has
        // no observation, so the chart shows a single trailing gap column.
        let points = [
            data_point(0, 100.0),
            data_point(1, 130.0),
            data_point(2, 128.0),
            gap_point(3),
        ];
        let pairs = observed_pairs(&points);
        let columns = cbh_render::topology_columns(&pairs, Some(3), 48);
        assert_eq!(columns.len(), 4, "the tip commit c3 gets its own column");
        assert_eq!(
            columns.iter().filter(|value| value.is_nan()).count(),
            1,
            "exactly the analyzed tip is a trailing gap"
        );
        assert!(
            columns[3].is_nan(),
            "the trailing gap sits at the tip column"
        );
        assert!(
            chart_of_points(&points, Some(3)).is_some(),
            "the tail draws"
        );
    }

    #[test]
    fn a_late_starting_sets_chart_still_trims_its_leading_gap() {
        // A set whose table opens with `n/a` rows charts only from its own first
        // observation: the table lists the shared commit range, while the chart shows
        // the shape of that set's series. A deliberate divergence.
        let points = [
            gap_point(0),
            gap_point(1),
            data_point(2, 128.0),
            data_point(3, 126.0),
        ];
        let pairs = observed_pairs(&points);
        assert_eq!(
            pairs,
            [(2, 128.0), (3, 126.0)],
            "the leading gaps never reach the chart"
        );
        let columns = cbh_render::topology_columns(&pairs, Some(3), 48);
        assert_eq!(
            columns.len(),
            2,
            "the chart opens at the set's first observation, not the range's start"
        );
        assert!(columns.iter().all(|value| value.is_finite()));
    }

    #[test]
    fn lists_a_data_less_interior_commit_as_n_a() {
        // Store c1 and c3 — skipping the interior c2 — on the linear history whose
        // tip is c3. The chart materializes c2 as a gap, and so does the listing: c2
        // gets a row of its own, without a value but still naming its commit.
        let storage = MemoryStorage::new();
        store(
            &storage,
            &clean_key("c1"),
            &single_metric_run(1, "c1", 130.0),
        );
        store(
            &storage,
            &clean_key("c3"),
            &single_metric_run(3, "c3", 128.0),
        );
        let git = linear_git();

        let options = ExamineOptions {
            json: Some(PathBuf::from("report.json")),
            ..options()
        };
        let rendered = examine_reports(&storage, &git, &options);
        let report = rendered.json.unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
        let points = parsed["sets"][0]["points"].as_array().unwrap();
        assert_eq!(points.len(), 3, "every commit c1..=c3 is listed: {report}");
        let commits: Vec<&str> = points
            .iter()
            .map(|point| point["commit"].as_str().unwrap())
            .collect();
        assert_eq!(commits, ["c1", "c2", "c3"], "{report}");
        // The gap entry carries no value and no cleanliness flag...
        assert!(points[1]["value"].is_null(), "{report}");
        assert!(points[1].get("dirty").is_none(), "{report}");
        // ...but still names what its commit changed.
        assert_eq!(
            points[1]["title"], "Refactor the observer to shave allocations off the record path",
            "{report}"
        );
        // An observation entry carries both, and neither shape leaks the topology.
        assert_eq!(points[0]["value"], 130.0, "{report}");
        assert_eq!(points[0]["dirty"], false, "{report}");
        assert!(
            points[0].get("topo_index").is_none(),
            "the JSON point carries no topology index: {report}"
        );

        let text = rendered.text.unwrap();
        assert!(
            text.contains('┤') || text.contains('┼'),
            "the chart is drawn across the gap: {text}"
        );
        assert!(
            text.contains(NO_DATA),
            "the data-less c2 reads {NO_DATA}: {text}"
        );
        assert!(
            text.contains("Refactor the observer to shave allocations off the"),
            "the data-less row still carries its commit title: {text}"
        );
    }

    #[test]
    fn lists_the_trailing_commits_up_to_the_tip_as_n_a() {
        // Data stops at c1 while the analyzed tip is c3: the listing runs to the tip,
        // so c2 and c3 are both listed without a value.
        let storage = MemoryStorage::new();
        store(
            &storage,
            &clean_key("c0"),
            &single_metric_run(0, "c0", 100.0),
        );
        store(
            &storage,
            &clean_key("c1"),
            &single_metric_run(1, "c1", 130.0),
        );
        let git = linear_git();

        let report = examine_json(&storage, &git, &options());
        let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
        let points = parsed["sets"][0]["points"].as_array().unwrap();
        assert_eq!(points.len(), 4, "the listing runs to the tip: {report}");
        assert_eq!(points[1]["value"], 130.0, "{report}");
        assert!(points[2]["value"].is_null(), "{report}");
        assert!(points[3]["value"].is_null(), "{report}");
    }

    #[test]
    fn the_listing_opens_at_the_earliest_observation_of_any_set() {
        // Callgrind first records at c0, Criterion only at c2. The range is the union
        // of the two, so both sets list c0..=c3 and Criterion's earlier commits read
        // `n/a` — keeping the two tables aligned row for row.
        let storage = MemoryStorage::new();
        store(
            &storage,
            &clean_key("c0"),
            &single_metric_run(0, "c0", 100.0),
        );
        store(
            &storage,
            "v1/folo/objects/criterion/x86_64-unknown-linux-gnu/m1/c2/clean.json",
            &single_metric_run(2, "c2", 200.0),
        );
        let git = linear_git();

        let opts = ExamineOptions {
            engine: vec!["all".to_owned()],
            ..options()
        };
        let report = examine_json(&storage, &git, &opts);
        let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
        let sets = parsed["sets"].as_array().unwrap();
        assert_eq!(sets.len(), 2, "{report}");
        for set in sets {
            let commits: Vec<&str> = set["points"]
                .as_array()
                .unwrap()
                .iter()
                .map(|point| point["commit"].as_str().unwrap())
                .collect();
            assert_eq!(
                commits,
                ["c0", "c1", "c2", "c3"],
                "every set lists the same commits: {report}"
            );
        }
        let criterion = sets
            .iter()
            .find(|set| set["engine"] == "criterion")
            .expect("the criterion set is present");
        let points = criterion["points"].as_array().unwrap();
        assert!(
            points[0]["value"].is_null() && points[1]["value"].is_null(),
            "criterion's listing opens before its own first observation: {report}"
        );
        assert_eq!(points[2]["value"], 200.0, "{report}");
    }

    fn dirty_tip_fixture() -> (MemoryStorage, FakeGitHistory) {
        // c2 has a clean run and the tip c3 only a dirty snapshot, admitted by the
        // base-tip dirty exception. `--no-dirty` drops that snapshot, and the tip
        // becomes an `n/a` row rather than vanishing from the listing.
        let storage = MemoryStorage::new();
        store(
            &storage,
            &clean_key("c2"),
            &single_metric_run(2, "c2", 100.0),
        );
        store(
            &storage,
            &dirty_key("c3", 400),
            &single_metric_run(4, "c3", 118.0),
        );
        let mut git = linear_git();
        git.mark_dirty();
        (storage, git)
    }

    #[test]
    fn a_dirty_only_tip_is_listed_with_its_value() {
        let (storage, git) = dirty_tip_fixture();
        let admitted = examine_json(&storage, &git, &options());
        let parsed: serde_json::Value = serde_json::from_str(&admitted).unwrap();
        let points = parsed["sets"][0]["points"].as_array().unwrap();
        assert_eq!(
            points[1]["value"], 118.0,
            "the snapshot is listed: {admitted}"
        );
        assert_eq!(points[1]["dirty"], true, "{admitted}");
    }

    #[test]
    fn no_dirty_leaves_the_commit_as_an_n_a_row() {
        let (storage, git) = dirty_tip_fixture();
        let opts = ExamineOptions {
            no_dirty: true,
            ..options()
        };
        let report = examine_json(&storage, &git, &opts);
        let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
        let points = parsed["sets"][0]["points"].as_array().unwrap();
        assert_eq!(points.len(), 2, "the tip is still listed: {report}");
        assert_eq!(points[1]["commit"], "c3", "{report}");
        assert!(
            points[1]["value"].is_null(),
            "its only run was excluded: {report}"
        );
    }

    #[test]
    fn markdown_renders_a_data_less_commit_as_n_a() {
        let storage = MemoryStorage::new();
        store(
            &storage,
            &clean_key("c0"),
            &single_metric_run(0, "c0", 100.0),
        );
        let git = linear_git();

        let report = examine_markdown(&storage, &git, &options());
        assert!(report.contains("| c0 | 100 | clean |"), "{report}");
        assert!(report.contains("| c1 | n/a | n/a |"), "{report}");
    }

    #[test]
    fn the_text_value_column_stays_aligned_with_an_n_a_row() {
        // The `n/a` cell takes part in the value-column width, so the values still
        // read straight down.
        let storage = MemoryStorage::new();
        store(
            &storage,
            &clean_key("c0"),
            &single_metric_run(0, "c0", 123_456.0),
        );
        let git = linear_git();

        let report = examine(&storage, &git, &options());
        let value = format_value(123_456.0);
        let cell_end = |needle: &str| {
            let line = report
                .lines()
                .find(|line| line.contains(needle))
                .unwrap_or_else(|| panic!("the {needle} row is present: {report}"));
            line.find(needle)
                .expect("the needle was just located on this line")
                + needle.len()
        };
        assert_eq!(
            cell_end(&value),
            cell_end(NO_DATA),
            "both value cells end in the same column: {report}"
        );
    }

    #[test]
    fn pivot_range_is_none_when_no_set_carries_an_observation() {
        let empty: [&[SeriesPoint]; 0] = [];
        assert_eq!(pivot_range(&empty, 3), None);
        let no_points: [&[SeriesPoint]; 1] = [&[]];
        assert_eq!(pivot_range(&no_points, 3), None);
    }

    #[test]
    fn pivot_range_opens_at_the_first_observation_and_closes_at_the_tip() {
        let points = [observation(1, 100.0), observation(2, 130.0)];
        let sets: [&[SeriesPoint]; 1] = [&points];
        assert_eq!(pivot_range(&sets, 5), Some((1, 5)));
    }

    #[test]
    fn pivot_range_unions_the_earliest_observation_across_sets() {
        // Whichever order the sets arrive in, the range opens at the earliest
        // observation of any of them.
        let early = [observation(1, 100.0)];
        let late = [observation(4, 200.0)];
        let forwards: [&[SeriesPoint]; 2] = [&early, &late];
        let backwards: [&[SeriesPoint]; 2] = [&late, &early];
        assert_eq!(pivot_range(&forwards, 5), Some((1, 5)));
        assert_eq!(pivot_range(&backwards, 5), Some((1, 5)));
    }

    #[test]
    fn pivot_range_of_a_tip_only_set_is_the_tip_alone() {
        let points = [observation(3, 100.0)];
        let sets: [&[SeriesPoint]; 1] = [&points];
        assert_eq!(pivot_range(&sets, 3), Some((3, 3)));
    }

    #[test]
    fn points_in_range_lists_every_commit_and_every_observation() {
        // c0 carries a clean run and a dirty snapshot, c2 a clean run, and c1 and c3
        // nothing: five rows across four commits.
        let observations = [
            observation(0, 100.0),
            SeriesPoint {
                dirty: true,
                ..observation(0, 110.0)
            },
            observation(2, 130.0),
        ];
        let commits = ordered_commits(&["c0", "c1", "c2", "c3"]);
        let mut subjects = HashMap::new();
        subjects.insert("c1".to_owned(), "Optimize the hot loop".to_owned());

        let points = points_in_range(&observations, (0, 3), &commits, &subjects);
        let listed: Vec<(&str, Option<f64>)> = points
            .iter()
            .map(|point| {
                (
                    point.commit.as_str(),
                    point.observation.as_ref().map(|one| one.value),
                )
            })
            .collect();
        assert_eq!(
            listed,
            [
                ("c0", Some(100.0)),
                ("c0", Some(110.0)),
                ("c1", None),
                ("c2", Some(130.0)),
                ("c3", None),
            ]
        );
        assert!(is_dirty(&points[1]), "the snapshot keeps its dirty flag");
        assert_eq!(
            points[2].title, "Optimize the hot loop",
            "a data-less row still carries its commit title"
        );
        assert!(
            points[4].title.is_empty(),
            "a commit whose subject topology did not report lists an empty title"
        );
    }

    #[test]
    fn points_in_range_lists_only_the_commits_inside_the_range() {
        // Observations outside the range are not listed, and the range's own bounds
        // decide which commits appear.
        let observations = [
            observation(0, 100.0),
            observation(1, 130.0),
            observation(3, 128.0),
        ];
        let commits = ordered_commits(&["c0", "c1", "c2", "c3"]);
        let points = points_in_range(&observations, (1, 2), &commits, &HashMap::new());
        let listed: Vec<(&str, Option<f64>)> = points
            .iter()
            .map(|point| {
                (
                    point.commit.as_str(),
                    point.observation.as_ref().map(|one| one.value),
                )
            })
            .collect();
        assert_eq!(listed, [("c1", Some(130.0)), ("c2", None)]);
    }

    #[test]
    fn the_verbose_note_explains_the_listed_range() {
        let storage = MemoryStorage::new();
        store(
            &storage,
            &clean_key("c1"),
            &single_metric_run(1, "c1", 130.0),
        );
        let git = linear_git();

        let notes = examine_notes(&storage, &git, &options());
        let note = notes
            .iter()
            .find(|note| note.starts_with("commit listing spans"))
            .unwrap_or_else(|| panic!("the range note is emitted: {notes:?}"));
        assert!(note.contains("3 commits"), "the commit count: {note}");
        assert!(note.contains("from c1 to c3"), "both endpoints: {note}");
        assert!(
            note.contains("earliest observation"),
            "why the range opens there: {note}"
        );
        assert!(
            note.contains("analyzed tip"),
            "why the range closes there: {note}"
        );
        assert!(
            note.contains("1 discriminant set"),
            "how many sets the union spans: {note}"
        );
        assert!(
            note.contains(NO_DATA),
            "what a data-less commit reads as: {note}"
        );
        assert!(
            note.contains("`--since`"),
            "which lever narrows the range: {note}"
        );
    }

    #[test]
    fn the_verbose_note_explains_an_unmatched_series() {
        let storage = MemoryStorage::new();
        store(
            &storage,
            &clean_key("c0"),
            &single_metric_run(0, "c0", 100.0),
        );
        let git = linear_git();

        let opts = ExamineOptions {
            benchmark: "nm/nm::observe/nonexistent".to_owned(),
            ..options()
        };
        let notes = examine_notes(&storage, &git, &opts);
        let note = notes
            .iter()
            .find(|note| note.starts_with("commit listing spans"))
            .unwrap_or_else(|| panic!("the range note is emitted: {notes:?}"));
        assert!(note.contains("no commits"), "nothing is listed: {note}");
        assert!(
            note.contains("1 run entered the selection"),
            "runs did enter the selection: {note}"
        );
        assert!(
            note.contains("nonexistent"),
            "which series found no observation: {note}"
        );
    }

    #[test]
    fn the_verbose_note_explains_an_empty_selection() {
        // Nothing was ever recorded, so the range is anchorless for a different
        // reason than an unmatched series: no run entered the selection at all.
        let storage = MemoryStorage::new();
        let git = linear_git();

        let notes = examine_notes(&storage, &git, &options());
        let note = notes
            .iter()
            .find(|note| note.starts_with("commit listing spans"))
            .unwrap_or_else(|| panic!("the range note is emitted: {notes:?}"));
        assert!(note.contains("no commits"), "nothing is listed: {note}");
        assert!(
            note.contains("no run entered the selection"),
            "the selection itself is empty: {note}"
        );
        assert!(
            !note.contains("but none recorded"),
            "an empty selection is not an unmatched series: {note}"
        );
    }

    #[test]
    fn a_contiguous_history_charts_without_a_gap() {
        // Every commit c2..=c3 has data and c3 is the tip, so the densified chart
        // holds one finite column per commit with no `NaN`.
        let storage = MemoryStorage::new();
        store(
            &storage,
            &clean_key("c2"),
            &single_metric_run(2, "c2", 128.0),
        );
        store(
            &storage,
            &clean_key("c3"),
            &single_metric_run(3, "c3", 126.0),
        );
        let git = linear_git();

        let report = examine_json(&storage, &git, &options());
        let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
        let points = parsed["sets"][0]["points"].as_array().unwrap();
        assert_eq!(
            points.len(),
            2,
            "every commit is a real observation: {report}"
        );

        // The densified columns the chart draws hold no gap.
        let pairs = [(2_usize, 128.0), (3, 126.0)];
        let columns = cbh_render::topology_columns(&pairs, Some(3), 48);
        assert_eq!(columns.len(), 2);
        assert!(
            columns.iter().all(|value| value.is_finite()),
            "a contiguous history has no gap columns"
        );
    }

    #[test]
    fn unknown_metric_is_rejected_up_front() {
        let storage = MemoryStorage::new();
        store(
            &storage,
            &clean_key("c0"),
            &single_metric_run(0, "c0", 100.0),
        );
        let git = linear_git();

        let opts = ExamineOptions {
            metric: "not_a_metric".to_owned(),
            ..options()
        };
        let error = block_on(examine_with(
            &git,
            &storage,
            "folo",
            &config(),
            &opts,
            &auto(),
            Timestamp::from_second(0).unwrap(),
            &RecordingReporter::quiet(),
            &spawner(),
        ))
        .unwrap_err();
        let found = error.find_source::<UnknownMetricError>().unwrap();
        assert_eq!(found.name, "not_a_metric");
    }

    #[test]
    fn empty_benchmark_preserves_the_typed_source() {
        let storage = MemoryStorage::new();
        let git = linear_git();
        let opts = ExamineOptions {
            benchmark: String::new(),
            ..options()
        };

        let error = block_on(examine_with(
            &git,
            &storage,
            "folo",
            &config(),
            &opts,
            &auto(),
            Timestamp::from_second(0).unwrap(),
            &RecordingReporter::quiet(),
            &spawner(),
        ))
        .unwrap_err();

        assert!(error.find_source::<EmptyBenchmarkError>().is_some());
        assert!(
            error
                .find_source::<cbh_model::EmptyBenchmarkIdPrefix>()
                .is_some()
        );
    }

    #[test]
    fn unmatched_benchmark_yields_empty_pivot_with_hint() {
        let storage = MemoryStorage::new();
        store(
            &storage,
            &clean_key("c0"),
            &single_metric_run(0, "c0", 100.0),
        );
        let git = linear_git();

        let opts = ExamineOptions {
            benchmark: "nm/nm::observe/nonexistent".to_owned(),
            ..options()
        };
        let report = examine(&storage, &git, &opts);
        assert!(
            report.contains("No data point matches the selection."),
            "{report}"
        );
        // Runs entered, but none carried the pair: the id/name-mismatch hint.
        assert!(report.contains("entered the analysis"), "{report}");
        assert!(report.contains("nonexistent"), "{report}");
    }

    #[test]
    fn rejects_an_unresolved_head() {
        let storage = MemoryStorage::new();
        store(
            &storage,
            &clean_key("c0"),
            &single_metric_run(0, "c0", 100.0),
        );
        let git = FakeGitHistory::new(); // No commits: HEAD does not resolve.
        let error = block_on(examine_with(
            &git,
            &storage,
            "folo",
            &config(),
            &options(),
            &auto(),
            Timestamp::from_second(0).unwrap(),
            &RecordingReporter::quiet(),
            &spawner(),
        ))
        .unwrap_err();
        let found = error.find_source::<UnresolvedRefError>().unwrap();
        assert_eq!(found.reference, "HEAD");
    }

    #[test]
    fn pivots_each_matching_discriminant_set() {
        // The same benchmark and metric recorded under two engines.
        let storage = MemoryStorage::new();
        store(
            &storage,
            &clean_key("c0"),
            &single_metric_run(0, "c0", 100.0),
        );
        store(
            &storage,
            "v1/folo/objects/criterion/x86_64-unknown-linux-gnu/m1/c0/clean.json",
            &single_metric_run(0, "c0", 200.0),
        );
        let git = linear_git();

        let opts = ExamineOptions {
            engine: vec!["all".to_owned()],
            ..options()
        };
        let report = examine_json(&storage, &git, &opts);
        let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
        let sets = parsed["sets"].as_array().unwrap();
        assert_eq!(sets.len(), 2, "one pivot per discriminant set: {report}");
    }

    #[test]
    fn parse_metric_rejects_an_unknown_name() {
        let error = parse_metric("bogus").unwrap_err();
        let found = error.find_source::<UnknownMetricError>().unwrap();
        let expected = MetricKind::ALL
            .iter()
            .map(|kind| kind.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        assert_eq!(found.name, "bogus");
        assert_eq!(found.valid, expected);
    }

    #[test]
    fn escape_cell_backslash_escapes_pipes() {
        // A commit title containing a pipe would otherwise open extra Markdown
        // table columns, so each pipe must be backslash-escaped.
        assert_eq!(escape_cell("feat: parse a|b"), "feat: parse a\\|b");
        // Text with no pipe passes through byte-for-byte.
        assert_eq!(escape_cell("plain title"), "plain title");
    }
}
