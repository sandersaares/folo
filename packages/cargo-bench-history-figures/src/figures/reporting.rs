//! Report excerpts and tables for the Reporting chapter.
//!
//! Every excerpt here is a **real rendering**. The chapter shows what a report looks like
//! and explains how to read it, and an excerpt somebody typed is exactly the kind of thing
//! that goes stale silently: the renderer changes a field, the book keeps showing the old
//! one, and a reader learns to look for something the tool no longer prints. Each asset is
//! therefore built by running the real detectors over the shared example series, assembling
//! the same [`ReportInput`] the analysis pipeline assembles, and handing it to
//! [`cbh_render::render`]. What the book shows is what the tool prints.
//!
//! The prose around each excerpt is the part that cannot be rendered, so it names the parts
//! of the output rather than restating their contents, and every value it quotes is read
//! back out of the rendering.

use std::fmt::Write as _;
use std::num::NonZero;

use cbh_detect::{
    AnalysisContext, AnalysisMode, Detection, Finding, Series, SeriesCensus, UnjudgedReason,
    evaluate_with_log, examples, find_changes,
};
use cbh_model::{DiscriminantSet, Engine, MachineKey, MetricKind, TargetTriple};
use cbh_render::{
    ComparisonBaseLag, ComparisonBaseLagReason, Coverage, DEFAULT_SUMMARY_LIMIT, ReportFormat,
    ReportInput, SetSummary, render,
};

use crate::assets::Asset;
use crate::styles::plot::{Mark, Observation, Plot};
use crate::theme;

/// Every asset the Reporting chapter embeds.
#[must_use]
pub fn assets() -> Vec<Asset> {
    vec![
        Asset::new("reporting-finding-annotated.md", finding_annotated()),
        Asset::new("reporting-formats.md", formats()),
        Asset::new("reporting-text.md", text()),
        Asset::new("reporting-json.md", json()),
        Asset::new("reporting-census.md", census()),
        Asset::new("reporting-lag.svg", lag_figure()),
        Asset::new("reporting-lag.md", lag()),
    ]
}

// ---------------------------------------------------------------------------------
// The worked analyses every excerpt is rendered from
// ---------------------------------------------------------------------------------

/// The project the worked reports describe.
const PROJECT: &str = "textproc";

/// The analyzed context commit.
///
/// A full hexadecimal object name, because that is what the pipeline hands the renderer;
/// the report abbreviates it itself, and an excerpt built from a pre-abbreviated commit
/// would not show that it does.
const TIP_COMMIT: &str = "9f2c4a1d3b5e708aab12cd34ef5678901234567a";

/// The oldest analyzed commit, which the header states alongside the tip so the reader
/// sees the stretch of history the run covered.
const FIRST_COMMIT: &str = "4d17b0c93ea25f6187de0b4c2a99f13355e8760b";

/// How many stored runs the worked analyses loaded.
///
/// A plausible several months of a suite collected per commit on one machine. Nothing in
/// the report is derived from it, so its only job is to be a number a reader recognizes as
/// a real collection rather than a toy.
const RUNS_LOADED: usize = 128;

/// How many stationary series pad the worked suite so the false-discovery family is
/// larger than the two findings.
const QUIET_SERIES: usize = 3;

/// How many series are too short to judge.
///
/// The census carries more than one reason on purpose: a breakdown with a single entry
/// reads as a special case rather than as the account of the whole suite that it is.
const SHORT_SERIES: usize = 3;

/// How many series the ghost filter dropped: benchmarks history remembers that the
/// analyzed commit no longer measures.
///
/// Ghosts are dropped before detection, so they are attached to the census after the
/// batch pass rather than being fed to it.
const GHOST_SERIES: usize = 2;

/// The discriminant set the worked analyses are attributed to.
fn worked_set() -> DiscriminantSet {
    DiscriminantSet::new(
        Engine::Criterion,
        &TargetTriple::from("x86_64-unknown-linux-gnu"),
        &MachineKey::from("a1b2c3d4"),
    )
}

/// Attaches the ghost account after detection.
///
/// Ghosts are dropped before the detectors run, so a batch pass cannot observe them.
/// Recording them on the returned census is the same hand-off the pipeline performs.
fn attach_ghosts(mut census: SeriesCensus) -> SeriesCensus {
    census.record_unjudged(UnjudgedReason::Ghost, GHOST_SERIES);
    census
}

/// A history-mode context whose tip reaches every series in `suite`.
fn suite_context(suite: &[Series]) -> AnalysisContext {
    let tip_index = suite
        .iter()
        .flat_map(|series| series.points.iter().map(|point| point.topo_index))
        .max()
        .unwrap_or(0);
    AnalysisContext {
        mode: AnalysisMode::History,
        merge_base_index: None,
        base_ref_index: None,
        tip_index,
    }
}

/// One series of the worked suite, attributed to the chapter's discriminant set.
fn suite_series(name: &str, values: &[f64]) -> Series {
    let mut series = examples::series(name, values, MetricKind::WallTime, 0);
    series.set = worked_set();
    series
}

/// The series a batch pass actually judges: two findings, some quiet neighbours, and
/// some too-short series. Ghosts are not in this list.
fn worked_suite() -> Vec<Series> {
    let mut suite = vec![
        suite_series("http_parse", &examples::clean_step()),
        suite_series("index_build", &examples::slow_ramp()),
    ];
    for index in 0..QUIET_SERIES {
        suite.push(suite_series(
            &format!("steady_{index}"),
            &examples::flat_noisy(),
        ));
    }
    for index in 0..SHORT_SERIES {
        suite.push(suite_series(&format!("short_{index}"), &[100.0, 101.0]));
    }
    suite
}

/// The silent excerpt's suite: quiet and short series only, so the report has a census
/// and no findings.
fn silent_suite() -> Vec<Series> {
    let mut suite = Vec::new();
    for index in 0..QUIET_SERIES {
        suite.push(suite_series(
            &format!("steady_{index}"),
            &examples::flat_noisy(),
        ));
    }
    for index in 0..SHORT_SERIES {
        suite.push(suite_series(&format!("short_{index}"), &[100.0, 101.0]));
    }
    suite
}

/// One batch detection pass over `suite`, with ghosts accounted for after the pass.
fn detect_suite(suite: &[Series]) -> Detection {
    let Detection {
        findings, census, ..
    } = find_changes(suite, &suite_context(suite));
    Detection {
        findings,
        census: attach_ghosts(census),
        ..Detection::default()
    }
}

/// The census the worked analyses account for their suite with.
fn worked_census() -> SeriesCensus {
    detect_suite(&worked_suite()).census
}

/// A worked analysis, held as the owned data a report is rendered from.
///
/// [`ReportInput`] borrows everything it renders, so the parts have to outlive it. Owning
/// them here is what lets every excerpt in the chapter be a real rendering: one analysis
/// feeds the text, Markdown and JSON forms, so none of them can drift from another or from
/// the renderer.
struct Analysis {
    /// The mode the analysis ran in.
    mode: AnalysisMode,

    /// The partition every finding belongs to.
    set: DiscriminantSet,

    /// The findings, ranked as the chapter describes.
    findings: Vec<Finding>,

    /// How far the set's comparison base sits behind the merge base. Branch mode only.
    lags: Vec<ComparisonBaseLag>,

    /// What the analysis judged, and why it left the rest unjudged.
    census: SeriesCensus,
}

impl Analysis {
    /// The report this analysis renders to, in `format`.
    fn render(&self, format: ReportFormat) -> String {
        let sets = vec![SetSummary {
            set: &self.set,
            runs: RUNS_LOADED,
            series: self.coverage().in_scope(),
            findings: self.findings.iter().collect(),
            comparison_base_lags: self.lags.clone(),
            branch_comparison: None,
        }];
        let input = ReportInput {
            project: PROJECT,
            tip_commit: TIP_COMMIT,
            tip_dirty: false,
            mode: self.mode,
            notable: !self.findings.is_empty(),
            runs: RUNS_LOADED,
            series: self.coverage().in_scope(),
            commit_span: Some((FIRST_COMMIT, TIP_COMMIT)),
            report_improvements: false,
            findings: &self.findings,
            sets: &sets,
            hint: None,
            warning: None,
            ghosts_excluded: GHOST_SERIES,
            census: self.census.clone(),
        };
        // Colour is emitted as ANSI escape sequences, which a book page shows as
        // mojibake rather than as colour.
        render(&input, format, false)
    }

    /// What the analysis judged, in the form the report reads it in.
    fn coverage(&self) -> Coverage {
        Coverage::from_census(&self.census)
    }
}

/// Runs the real branch-mode detector over `values`, forking the branch from its base
/// where the example's level changes.
///
/// Branch mode judges the context run against a window of recent base-ref commits, so the fork has
/// to sit between the two regimes for the example to be a branch that moved rather than a branch
/// indistinguishable from its base.
fn judge_branch(name: &str, values: &[f64], kind: MetricKind) -> Finding {
    let mut series = examples::series(name, values, kind, 0);
    series.set = worked_set();
    let merge_base = values
        .len()
        .checked_div(2)
        .and_then(|half| half.checked_sub(1))
        .expect("the example series holds more than one point");
    let mut series = examples::with_base_window(series, merge_base);
    series.set = worked_set();
    let context = examples::branch_context(&series, merge_base);
    let (finding, _) = evaluate_with_log(&series, &context);
    finding.expect(
        "the stepped example is a branch that moved away from its base, which is the case \
         branch mode reports",
    )
}

/// The worked analysis the chapter's report excerpts are rendered from: one batch
/// detection pass, so the findings and the census are the same account.
fn worked_analysis() -> Analysis {
    let Detection {
        findings, census, ..
    } = detect_suite(&worked_suite());
    Analysis {
        mode: AnalysisMode::History,
        set: worked_set(),
        findings,
        lags: Vec::new(),
        census,
    }
}

/// The same kind of pass with nothing to report: the case the coverage line exists for.
fn silent_analysis() -> Analysis {
    let Detection {
        findings, census, ..
    } = detect_suite(&silent_suite());
    Analysis {
        mode: AnalysisMode::History,
        set: worked_set(),
        findings,
        lags: Vec::new(),
        census,
    }
}

/// A branch analysis whose comparison base lags the merge base for `reason`.
fn lagging_analysis(lag: ComparisonBaseLag) -> Analysis {
    Analysis {
        mode: AnalysisMode::Branch,
        set: worked_set(),
        findings: vec![judge_branch(
            "http_parse",
            &examples::clean_step(),
            MetricKind::WallTime,
        )],
        lags: vec![lag],
        census: worked_census(),
    }
}

// ---------------------------------------------------------------------------------
// The excerpts
// ---------------------------------------------------------------------------------

/// `content` wrapped in a fenced block tagged `language`.
fn fenced(language: &str, content: &str) -> String {
    format!("```{language}\n{}\n```\n", content.trim_end())
}

/// The lines of the report block describing the finding identified by `id`.
///
/// A finding is rendered as a paragraph: the identity on its own line, then the headline,
/// the detail, and the chart, up to the blank line that separates it from the next
/// finding. Reading the block back out of the report is what keeps the annotation beside
/// it describing the output the tool actually produces.
fn finding_block(report: &str, id: &str) -> Vec<String> {
    report
        .lines()
        .skip_while(|line| *line != id)
        .take_while(|line| !line.trim().is_empty())
        .map(str::to_owned)
        .collect()
}

/// One finding with every part of it named.
fn finding_annotated() -> String {
    let analysis = worked_analysis();
    let report = analysis.render(ReportFormat::Text);
    let finding = analysis
        .findings
        .first()
        .expect("the worked analysis reports");
    let block = finding_block(&report, &finding.id.qualified());

    let mut lines = block.iter().map(|line| line.trim());
    let identity = lines.next().unwrap_or_default();
    let headline = lines.next().unwrap_or_default();
    let detail = lines.next().unwrap_or_default();

    let mut markdown = String::from("A finding, as the text report prints it:\n\n");
    markdown.push_str(&fenced("text", &block.join("\n")));
    markdown.push_str("\n| Part | What it carries |\n|---|---|\n");
    writeln!(
        markdown,
        "| `{identity}` | The benchmark identity: every benchmark-ID segment joined by `/`. \
         This slash-rendered identity is the string accepted by \
         [`examine`](../commands/examine.md) and matched by the benchmark-ID prefix supplied \
         to [`bless`](../commands/bless.md). |"
    )
    .expect("writing to a String never fails");
    writeln!(
        markdown,
        "| `{headline}` | The headline: the move as a percentage of the baseline, and the \
         metric kind that moved. Findings are ranked by the magnitude of that percentage. |"
    )
    .expect("writing to a String never fails");
    writeln!(
        markdown,
        "| `{detail}` | The detail: direction, the detector that produced the finding, \
         the baseline and latest representative values, and a commit somewhere near \
         the change. |"
    )
    .expect("writing to a String never fails");
    markdown.push_str(
        "| the lines below it | The chart: the series drawn against topology, one column \
         per commit. |\n",
    );
    markdown
}

/// Every output the tool can produce, and what each is for.
fn formats() -> String {
    let summary_limit: NonZero<usize> = DEFAULT_SUMMARY_LIMIT;

    let mut markdown = String::from(
        "| Output | How to request it | What it is for | What it carries |\n\
         |---|---|---|---|\n",
    );
    markdown.push_str(
        "| Text | the default; `--no-text` suppresses it | Reading a report in a terminal \
         | every finding; omits the per-reason census when findings exist |\n",
    );
    markdown.push_str(
        "| Markdown | `--markdown <path>` | Pasting into a pull request or an issue | \
         every finding; omits the per-reason census when findings exist |\n",
    );
    markdown.push_str(
        "| JSON | `--json <path>` | Automation | the complete census always, and every \
         finding; omits the per-commit chart series, which is presentation rather than \
         data |\n",
    );
    writeln!(
        markdown,
        "| Condensed summary | `--markdown-summary <path>` (`analyze` only) | A \
         size-limited destination, such as a pull request comment or a rolling issue body \
         | at most the {} findings of greatest magnitude, flattened so the \
         per-set grouping is dropped — lossy by design |",
        summary_limit.get(),
    )
    .expect("writing to a String never fails");

    markdown.push_str(
        "\nJSON is the complete machine-readable result: it always carries every finding and \
         the full per-reason census, including the ghost count. It is not an observation \
         archive — the per-commit series behind the charts is obtained with `examine`. Text \
         and Markdown carry every finding but drop the per-reason census when there are \
         findings to show, so only JSON always reveals the ghost count. The condensed \
         summary is lossy by design, so it is the one output that must not be automated \
         against: a check reading it cannot distinguish findings that were capped away from \
         findings that were never made.\n",
    );
    markdown
}

/// The worked analysis as the text report prints it.
fn text() -> String {
    let mut markdown =
        String::from("**Text** — the default output, printed to standard output.\n\n");
    markdown.push_str(&fenced(
        "text",
        &worked_analysis().render(ReportFormat::Text),
    ));
    markdown
}

/// The same analysis as the JSON report writes it.
fn json() -> String {
    let mut markdown = String::from("**JSON** — the same analysis, written by `--json`.\n\n");
    markdown.push_str(&fenced(
        "json",
        &worked_analysis().render(ReportFormat::Json),
    ));
    markdown
}

/// The coverage account as a report states it, and how to read it.
fn census() -> String {
    let analysis = silent_analysis();
    let report = analysis.render(ReportFormat::Text);
    let coverage = analysis.coverage();

    let mut markdown =
        String::from("A report with nothing to say still says how far that silence reaches:\n\n");
    markdown.push_str(&fenced("text", &report));

    markdown.push_str("\n| Part | How to read it |\n|---|---|\n");
    writeln!(
        markdown,
        "| `in-scope series judged: {} of {}` | The denominator of every claim the report \
         makes. It counts series the analysis could have judged, which is every series it \
         accounted for except the ghosts. |",
        coverage.judged(),
        coverage.in_scope(),
    )
    .expect("writing to a String never fails");
    writeln!(
        markdown,
        "| `{}` | The verdict. It is a statement about the judged series alone, and its \
         wording changes with how much of the suite that is. |",
        coverage.verdict(),
    )
    .expect("writing to a String never fails");
    // The qualifications come in reading order — what the silence covers, then what it
    // does not — and the second sentence is present only where something went unjudged.
    let qualifications = coverage.qualifications();
    let breakdown = if coverage.unjudged() > 0 {
        qualifications.len().checked_sub(1)
    } else {
        None
    };
    for (index, sentence) in qualifications.iter().enumerate() {
        let meaning = if Some(index) == breakdown {
            "What the verdict does not cover, named reason by reason. The judged count and \
             these reasons account for every series between them."
        } else {
            "How far the verdict reaches: the share of the in-scope suite it is a statement \
             about."
        };
        writeln!(markdown, "| `{sentence}` | {meaning} |")
            .expect("writing to a String never fails");
    }

    markdown.push_str(
        "\nThe judged ratio heads every report that had anything in scope. The per-reason \
         breakdown is printed by the text and Markdown reports only where the report has no \
         findings, as here; the JSON report always carries it, under `census.reasons`.\n",
    );
    markdown
}

/// How far behind the two lag examples' comparison bases sit.
///
/// Different distances, and one of them a single commit, so the pair shows both that the
/// warning states a distance and that it agrees in number with it.
const LAG_DISTANCES: [(usize, ComparisonBaseLagReason); 2] = [
    (7, ComparisonBaseLagReason::DiscriminantSetMismatch),
    (1, ComparisonBaseLagReason::NoRecentBaseData),
];

/// The base-ref measurements available for the context run's machine key.
///
/// Light variation keeps the figure reading as real measured history while staying close
/// enough to one level that the context run is the visible move.
const LAG_FIGURE_BASE_VALUES: [f64; 8] = [99.8, 100.3, 99.9, 100.2, 99.7, 100.1, 100.4, 100.0];

/// How many base-ref commits have no data for the context run's machine key.
const LAG_FIGURE_GAP_COMMITS: usize = 4;

/// The newest base commit that carries same-key data.
fn lag_figure_base_position() -> usize {
    LAG_FIGURE_BASE_VALUES.len().saturating_sub(1)
}

/// The context commit position in the lag illustration.
fn lag_figure_tip_position() -> usize {
    LAG_FIGURE_BASE_VALUES
        .len()
        .saturating_add(LAG_FIGURE_GAP_COMMITS)
}

/// The level measured at the context commit.
const LAG_FIGURE_TIP_LEVEL: f64 = 130.0;

/// The topology shape behind a comparison-base lag warning.
fn lag_figure() -> String {
    let comparison_base = lag_figure_base_position();
    let tip = lag_figure_tip_position();
    let base_observations = LAG_FIGURE_BASE_VALUES
        .into_iter()
        .enumerate()
        .map(|(index, value)| {
            let observation = Observation::new(index, value);
            if index == comparison_base {
                observation.marked(Mark::Focus)
            } else {
                observation
            }
        });
    let tip_observation = Observation::new(tip, LAG_FIGURE_TIP_LEVEL).marked(Mark::Regression);

    Plot::new(
        "same machine key: the comparison reaches back to older base data",
        tip.saturating_add(1),
    )
    .value_label("ns")
    .scattered()
    .observations(base_observations.chain(std::iter::once(tip_observation)))
    .band(
        comparison_base.saturating_add(1),
        tip.saturating_sub(1),
        "no same-key base data",
        theme::MUTED,
    )
    .split(comparison_base, "comparison base")
    .split(tip, "context commit")
    .render()
}

/// What each comparison-base lag reason means for a branch finding.
fn lag() -> String {
    let mut markdown =
        String::from("| Reason | The line the report prints | What it means |\n|---|---|---|\n");

    for (commits_behind, reason) in LAG_DISTANCES {
        let lag = ComparisonBaseLag {
            commits_behind: NonZero::new(commits_behind)
                .expect("a comparison base that reaches the merge base is not a lag"),
            reason,
        };
        let report = lagging_analysis(lag).render(ReportFormat::Text);
        let line = warning_line(&report).expect(
            "a branch report carrying a comparison-base lag prints it under the set it \
             belongs to",
        );

        writeln!(
            markdown,
            "| {} | `{line}` | {} |",
            // The reason's own name, read back out of the line the renderer produced
            // rather than transcribed beside it.
            named_reason(&line).expect("the warning names its reason in a trailing note"),
            describe_reason(reason),
        )
        .expect("writing to a String never fails");
    }

    markdown.push_str(
        "\nThe lag is advisory. It never changes which findings are reported and never \
         affects the exit code; what it changes is how much weight a marginal branch \
         finding deserves, because a comparison against a base state several commits old is \
         exactly that.\n",
    );
    markdown
}

/// The comparison-base lag warning a report printed.
fn warning_line(report: &str) -> Option<String> {
    report
        .lines()
        .map(str::trim)
        .find(|line| line.starts_with("Warning: comparison base"))
        .map(str::to_owned)
}

/// The reason named in parentheses at the end of a lag warning.
fn named_reason(line: &str) -> Option<&str> {
    let (_, tail) = line.rsplit_once('(')?;
    tail.strip_suffix(')')
}

/// What a lag reason tells the reader about their comparison.
fn describe_reason(reason: ComparisonBaseLagReason) -> &'static str {
    match reason {
        ComparisonBaseLagReason::DiscriminantSetMismatch => {
            "Newer base data exists, but it was measured under a different machine key. \
             Measurements are never compared across machine keys, so the comparison reached back \
             to the newest base commit this partition covers. A rotating CI pool is the \
             usual cause."
        }
        ComparisonBaseLagReason::NoRecentBaseData => {
            "No base-side run for the series exists at any more recent commit. The \
             comparison base is simply the newest base data there is, and collection on the \
             base branch is what would move it forward."
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg_attr(
        miri,
        ignore = "plotters SVG generation is host graphics, not memory-safety-relevant, and exceeds the Miri CI budget"
    )]
    fn every_documented_asset_is_produced() {
        let paths: Vec<String> = assets().into_iter().map(|asset| asset.path).collect();

        for expected in [
            "reporting-finding-annotated.md",
            "reporting-formats.md",
            "reporting-text.md",
            "reporting-json.md",
            "reporting-census.md",
            "reporting-lag.svg",
            "reporting-lag.md",
        ] {
            assert!(
                paths.iter().any(|path| path == expected),
                "{expected} missing"
            );
        }
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "plotters SVG generation is host graphics, not memory-safety-relevant, and exceeds the Miri CI budget"
    )]
    fn the_lag_figure_names_the_comparison_base_context_and_gap() {
        let svg = lag_figure();

        assert!(svg.contains("comparison base"));
        assert!(svg.contains("context commit"));
        assert!(svg.contains("no same-key base data"));
    }

    /// Findings and census come from one pass. A glued census would let the two drift.
    #[test]
    #[cfg_attr(
        miri,
        ignore = "runs both complete worked suites through statistical detection to verify the book's report and census"
    )]
    fn the_worked_report_is_one_batch_detection_pass() {
        let analysis = worked_analysis();
        let expected_judged = 2_usize.saturating_add(QUIET_SERIES);

        assert_eq!(analysis.findings.len(), 2);
        assert_eq!(analysis.census.judged(), expected_judged);
        assert!(
            analysis
                .census
                .reasons()
                .any(|(reason, count)| reason == UnjudgedReason::Ghost && count == GHOST_SERIES)
        );
        assert!(silent_analysis().findings.is_empty());
    }

    /// The chapter's claim is that these are the same analysis in two forms. Rendering
    /// them from one [`Analysis`] is what makes that true; this holds the two renderings
    /// to naming the same findings.
    #[test]
    #[cfg_attr(
        miri,
        ignore = "detects the full worked statistical suite and renders both text and JSON excerpts"
    )]
    fn the_text_and_json_excerpts_describe_the_same_findings() {
        let analysis = worked_analysis();
        let text = analysis.render(ReportFormat::Text);
        let json = analysis.render(ReportFormat::Json);

        assert!(!analysis.findings.is_empty(), "the excerpt must report");
        for finding in &analysis.findings {
            let id = finding.id.qualified();
            assert!(text.contains(&id), "the text report omits {id}");
            // JSON carries the identity as its ordered segments rather than as the
            // qualified string the human reports print, so it is looked for that way.
            for segment in id.split('/') {
                assert!(json.contains(segment), "the JSON report omits {segment}");
            }
        }
    }

    /// The annotation names the parts of a finding block by position, so a renderer that
    /// reorders or drops one would leave the table labelling the wrong lines.
    #[test]
    #[cfg_attr(
        miri,
        ignore = "detects and renders the complete worked statistical suite; small finding-block extraction is tested separately"
    )]
    fn the_annotated_finding_block_carries_an_identity_a_headline_a_detail_and_a_chart() {
        let analysis = worked_analysis();
        let report = analysis.render(ReportFormat::Text);
        let finding = analysis.findings.first().expect("the analysis reports");
        let identity = finding.id.qualified();

        let block = finding_block(&report, &identity);

        assert_eq!(block.first().map(String::as_str), Some(identity.as_str()));
        assert!(
            block
                .get(1)
                .is_some_and(|line| line.contains(finding.kind.as_str())),
            "the headline names the metric kind that moved: {block:?}"
        );
        assert!(
            block.get(2).is_some_and(|line| line.contains('→')),
            "the detail states the baseline → latest move: {block:?}"
        );
        assert!(
            block
                .iter()
                .any(|line| line.contains('┤') || line.contains('┼')),
            "the block ends in a chart: {block:?}"
        );
    }

    /// The excerpt exists to be a real rendering; a fence around hand-written prose would
    /// look identical in the book.
    #[test]
    #[cfg_attr(
        miri,
        ignore = "repeats full worked-suite detection and report rendering to verify the generated excerpt"
    )]
    fn the_report_excerpts_are_renderings_of_the_worked_analysis() {
        let rendered = worked_analysis().render(ReportFormat::Text);
        let excerpt = text();

        assert!(excerpt.contains(rendered.trim_end()), "{excerpt}");
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "repeats full quiet-suite detection and report rendering to verify the census excerpt"
    )]
    fn the_census_excerpt_quotes_the_coverage_the_report_states() {
        let analysis = silent_analysis();
        let report = analysis.render(ReportFormat::Text);
        let coverage = analysis.coverage();
        let excerpt = census();

        let judged = format!(
            "in-scope series judged: {} of {}",
            coverage.judged(),
            coverage.in_scope()
        );
        assert!(report.contains(&judged), "{report}");
        assert!(excerpt.contains(&judged), "{excerpt}");
        assert!(excerpt.contains(coverage.verdict()), "{excerpt}");
        for sentence in coverage.qualifications() {
            assert!(excerpt.contains(&sentence), "{excerpt}");
        }
    }

    /// A census that judged everything would render no breakdown at all, leaving the
    /// chapter's excerpt showing none of what it is about.
    #[test]
    #[cfg_attr(
        miri,
        ignore = "derives the worked census by detecting every full statistical example in the report suite"
    )]
    fn the_worked_census_leaves_series_unjudged_for_more_than_one_reason() {
        let coverage = Coverage::from_census(&worked_census());

        assert!(coverage.judged() > 0);
        assert!(coverage.reasons().count() > 1);
    }

    /// The two qualification sentences say different things, so labelling both with one
    /// explanation would describe neither.
    #[test]
    #[cfg_attr(
        miri,
        ignore = "repeats quiet-suite statistical detection to verify the full census qualification excerpt"
    )]
    fn each_qualification_is_explained_as_the_sentence_it_is() {
        let excerpt = census();
        let coverage = silent_analysis().coverage();
        let qualifications = coverage.qualifications();

        assert!(qualifications.len() > 1, "{qualifications:?}");
        let explanations: Vec<&str> = excerpt
            .lines()
            .filter_map(|line| {
                qualifications
                    .iter()
                    .any(|sentence| line.contains(sentence.as_str()))
                    .then(|| line.rsplit('|').nth(1))
                    .flatten()
            })
            .collect();

        assert_eq!(explanations.len(), qualifications.len(), "{excerpt}");
        assert_ne!(
            explanations.first(),
            explanations.last(),
            "both sentences carry the same explanation: {excerpt}"
        );
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "repeatedly detects the complete worked suite and renders branch reports for every lag reason"
    )]
    fn every_lag_reason_is_shown_with_the_line_the_report_prints_for_it() {
        let excerpt = lag();

        for (commits_behind, reason) in LAG_DISTANCES {
            let lag = ComparisonBaseLag {
                commits_behind: NonZero::new(commits_behind).expect("a lag is non-zero"),
                reason,
            };
            let report = lagging_analysis(lag).render(ReportFormat::Text);
            let line = warning_line(&report).expect("the report warns about the lag");
            let named = named_reason(&line).expect("the warning names its reason");

            assert!(excerpt.contains(&line), "{excerpt}");
            assert!(
                !named.is_empty(),
                "the reason column would be blank: {line}"
            );
            assert!(excerpt.contains(&format!("| {named} |")), "{excerpt}");
        }
    }

    /// The distance the warning states must be the distance it was given, or the excerpt
    /// teaches the reader to read a number the tool did not mean.
    #[test]
    #[cfg_attr(
        miri,
        ignore = "detects the complete worked suite for each branch-lag report; small warning extraction is tested separately"
    )]
    fn each_lag_warning_states_the_distance_it_was_given() {
        for (commits_behind, reason) in LAG_DISTANCES {
            let lag = ComparisonBaseLag {
                commits_behind: NonZero::new(commits_behind).expect("a lag is non-zero"),
                reason,
            };
            let report = lagging_analysis(lag).render(ReportFormat::Text);
            let line = warning_line(&report).expect("the report warns about the lag");

            assert!(line.contains(&commits_behind.to_string()), "{line}");
        }
    }

    #[test]
    fn a_lag_warning_is_extracted_without_neighboring_report_lines() {
        let warning = "Warning: comparison base is behind (no recent base data)";
        let report = format!("report heading\n\n  {warning}\n\nfinding detail");

        assert_eq!(warning_line(&report).as_deref(), Some(warning));
        assert_eq!(named_reason(warning), Some("no recent base data"));
        assert_eq!(warning_line("a report without a lag warning"), None);
        assert_eq!(named_reason("a line without a parenthesized reason"), None);
    }

    #[test]
    fn a_finding_block_ends_before_the_next_report_paragraph() {
        let report = "heading\n\nbench\n  headline\n  detail\n\nother bench\n  other headline\n";

        assert_eq!(
            finding_block(report, "bench"),
            ["bench", "  headline", "  detail"]
        );
        assert!(finding_block(report, "missing").is_empty());
    }

    #[test]
    fn the_formats_table_states_the_summary_cap_the_tool_applies() {
        let table = formats();

        assert!(
            table.contains(&DEFAULT_SUMMARY_LIMIT.get().to_string()),
            "{table}"
        );
    }

    #[test]
    fn the_formats_table_distinguishes_census_completeness() {
        let table = formats();

        assert!(
            table.contains("omits the per-reason census when findings exist"),
            "{table}"
        );
        assert!(table.contains("the complete census always"), "{table}");
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "plotters SVG generation is host graphics, not memory-safety-relevant, and exceeds the Miri CI budget"
    )]
    fn rendering_is_reproducible() {
        assert_eq!(assets(), assets());
    }
}
