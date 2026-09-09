//! Figures and worked examples for the Multiplicity and coverage chapter.
//!
//! The chapter makes three arithmetic claims — that the rank threshold tightens with the
//! size of the family, that filtering direction before correction is stricter than
//! correcting both directions first, and that a lone perfect step at the detection minimum
//! cannot clear the rank-1 bar of a large family — and one accounting claim, that every
//! series an analysis did not judge is named. Each figure here is the arithmetic itself: the
//! step-up decisions come from the real [`benjamini_hochberg`](cbh_stats::benjamini_hochberg)
//! procedure and the coverage account from a real [`SeriesCensus`], so a change in either
//! rewrites the chapter rather than leaving its numbers behind.

use std::fmt::Write as _;

use cbh_detect::{
    HISTORY_DETECTOR_COUNT, MIN_REGIME, MIN_SERIES_POINTS, SeriesCensus,
    TARGET_FALSE_DISCOVERY_RATE, Testability, UnjudgedReason,
};
use cbh_render::{Coverage, CoverageState};
use plotters::style::RGBColor;

use crate::assets::Asset;
use crate::styles::multiplicity::{Candidate, Census, Staircase};
use crate::theme;

/// Every asset the Multiplicity and coverage chapter embeds.
#[must_use]
pub fn assets() -> Vec<Asset> {
    let mut assets = staircase();
    assets.extend(families());
    assets.push(Asset::new(
        "coverage-short-series.md",
        short_series_lone_table(),
    ));
    assets.push(Asset::new(
        "coverage-short-series-reach.md",
        short_series_reach_table(),
    ));
    assets.push(Asset::new(
        "coverage-direction-order.md",
        direction_order_table(),
    ));
    assets.push(census_bar());
    assets.push(Asset::new("coverage-reasons.md", reasons_table()));
    assets.push(Asset::new("coverage-states.md", states_table()));
    assets
}

/// One candidate's place in the step-up procedure, as both the figure and the table read
/// it.
///
/// The procedure answers in a keep-or-drop mask over the candidates it was given, which
/// leaves each candidate's rank and associated threshold implicit. Recovering them once
/// here is what lets the figure and the table beside it state the same numbers by
/// construction rather than by two agreeing derivations.
#[derive(Clone, Debug)]
struct Judged {
    /// How the appendix names the series.
    label: String,

    /// Its place in the sorted list, counting from one.
    rank: usize,

    /// How often chance alone would produce a pattern at least this strong.
    chance_level: f64,

    /// The threshold associated with this rank.
    threshold: f64,

    /// Whether the procedure kept it.
    kept: bool,
}

impl Judged {
    /// Whether the appendix calls this candidate reported or dropped.
    fn outcome(&self) -> &'static str {
        if self.kept { "kept" } else { "dropped" }
    }
}

/// Runs the real step-up procedure over `candidates` against a family of `family_size`,
/// and recovers each candidate's rank and associated threshold.
///
/// `candidates` are labelled chance levels in any order; the result is sorted, which is
/// the order the procedure itself works in.
fn judge(candidates: &[(&str, f64)], family_size: usize) -> Vec<Judged> {
    let mut sorted: Vec<(&str, f64)> = candidates.to_vec();
    sorted.sort_by(|left, right| {
        left.1
            .partial_cmp(&right.1)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let chance_levels: Vec<f64> = sorted.iter().map(|&(_, chance)| chance).collect();
    let kept = cbh_stats::benjamini_hochberg(&chance_levels, fdr_q(), family_size);

    sorted
        .iter()
        .enumerate()
        .map(|(index, &(label, chance_level))| {
            let rank = index.saturating_add(1);
            Judged {
                label: label.to_owned(),
                rank,
                chance_level,
                threshold: threshold_at(rank, family_size),
                kept: kept.get(index).copied().unwrap_or_default(),
            }
        })
        .collect()
}

/// The target expected false-discovery proportion for reported findings.
fn fdr_q() -> f64 {
    TARGET_FALSE_DISCOVERY_RATE
}

/// The threshold associated with `rank` in a family of `family_size`.
///
/// The keep-or-drop decision comes from the real implementation. This recovers the
/// per-rank line that implementation uses to find the largest passing rank; that rank
/// keeps the preceding prefix.
fn threshold_at(rank: usize, family_size: usize) -> f64 {
    crate::coord::of(rank) / crate::coord::of(family_size.max(1)) * fdr_q()
}

/// The staircase's worked family: a run where several series raised a candidate and the
/// procedure kept most of them.
///
/// Chosen so the figure shows the one thing a per-candidate threshold reading would get
/// wrong — a candidate that exceeds its own rank's threshold and is kept anyway, because
/// the largest passing rank keeps the prefix above it. Any set without that case would
/// leave a reader believing the procedure is a row-by-row comparison.
const STAIRCASE_CANDIDATES: [(&str, f64); 5] = [
    ("parse_headers", 0.002),
    ("tokenize", 0.011),
    ("index_build", 0.031),
    ("flush", 0.033),
    ("compress", 0.045),
];

/// The step-up staircase, and the same decisions as a table.
fn staircase() -> Vec<Asset> {
    let family = family_size_of_run();
    let judged = judge(&STAIRCASE_CANDIDATES, family);
    let figure = staircase_figure(
        format!(
            "{} candidates against rank thresholds in a family of {family}",
            judged.len()
        ),
        &judged,
    );

    let mut table =
        String::from("| Rank | Series | Chance level | Threshold at rank | Outcome |\n");
    table.push_str("|---:|---|---:|---:|---|\n");
    for candidate in &judged {
        writeln!(
            table,
            "| {} | `{}` | {} | {} | {} |",
            candidate.rank,
            candidate.label,
            chance(candidate.chance_level),
            chance(candidate.threshold),
            candidate.outcome()
        )
        .expect("writing to a String never fails");
    }
    writeln!(
        table,
        "\nThe family is the {family} series this run judged. The threshold at rank *k* \
         is *k* / {family} of the target expected false-discovery proportion, {}, and \
         the largest passing rank keeps the preceding prefix.",
        percent(fdr_q())
    )
    .expect("writing to a String never fails");

    vec![
        Asset::new("coverage-staircase.svg", figure),
        Asset::new("coverage-staircase.md", table),
    ]
}

/// A staircase captioned `caption` over `judged`.
fn staircase_figure(caption: impl Into<String>, judged: &[Judged]) -> String {
    judged
        .iter()
        .fold(Staircase::new(caption), |staircase, candidate| {
            staircase.candidate(Candidate {
                label: candidate.label.clone(),
                chance_level: candidate.chance_level,
                threshold: candidate.threshold,
                kept: candidate.kept,
            })
        })
        .render()
}

/// The candidates the family-size example judges in both families.
///
/// One decisive, one marginal and one weak. The marginal one is the figure's subject: it
/// is the candidate whose fate the size of the family decides, and the other two are there
/// to give it a rank.
const FAMILY_SIZE_CANDIDATES: [(&str, f64); 3] =
    [("checksum", 0.0004), ("tokenize", 0.006), ("flush", 0.02)];

/// The series whose fate the size of the family decides.
const MARGINAL_SERIES: &str = "tokenize";

/// The large family the same candidate is judged against.
///
/// A family at the scale the chapter contrasts a small one with. Chance alone is likely
/// to produce low chance levels somewhere in a family this large, so the marginal
/// candidate's threshold moves by more than a rounding.
const LARGE_FAMILY: usize = 2000;

/// The same marginal candidate judged in a small family and a large one.
fn families() -> Vec<Asset> {
    let small = family_size_case(family_size_of_run());
    let large = family_size_case(LARGE_FAMILY);

    // The two staircases are one asset because the chapter includes them at one point and
    // the comparison only works side by side. A single newline between them keeps the pair
    // inside one HTML block, which is what stops the book's Markdown parser from splitting
    // them.
    let figure = format!("{}\n{}", small.figure, large.figure);

    let mut table =
        String::from("| Family size | Rank | Chance level | Threshold at rank | Outcome |\n");
    table.push_str("|---:|---:|---:|---:|---|\n");
    for case in [&small, &large] {
        let candidate = case.marginal();
        writeln!(
            table,
            "| {} | {} | {} | {} | {} |",
            case.family,
            candidate.rank,
            chance(candidate.chance_level),
            chance(candidate.threshold),
            candidate.outcome()
        )
        .expect("writing to a String never fails");
    }
    writeln!(
        table,
        "\nThe same series, the same measurements, the same chance level. A larger \
         family is more likely to produce chance candidates somewhere in the set, so this \
         rank threshold is {} as strict.",
        times(small.marginal().threshold / large.marginal().threshold)
    )
    .expect("writing to a String never fails");

    vec![
        Asset::new("coverage-family-size.svg", figure),
        Asset::new("coverage-family-size.md", table),
    ]
}

/// One family's judgement of the same candidates.
///
/// Pairs the rendered staircase with the decisions behind it so the figure and the table
/// beside it cannot state different numbers.
#[derive(Clone, Debug)]
struct FamilyCase {
    /// How many series the family holds.
    family: usize,

    /// What the procedure decided about each candidate.
    judged: Vec<Judged>,

    /// The rendered staircase.
    figure: String,
}

impl FamilyCase {
    /// What became of the candidate the example is about.
    fn marginal(&self) -> &Judged {
        self.judged
            .iter()
            .find(|candidate| candidate.label == MARGINAL_SERIES)
            .expect("the marginal candidate is one of the candidates every family judges")
    }
}

/// Judges the family-size candidates against a family of `family`.
fn family_size_case(family: usize) -> FamilyCase {
    let judged = judge(&FAMILY_SIZE_CANDIDATES, family);
    let figure = staircase_figure(format!("in a family of {family} judged series"), &judged);
    FamilyCase {
        family,
        judged,
        figure,
    }
}

/// A perfect midpoint step of a given length, and the largest judged family in which
/// that lone step still reports.
///
/// The chance level is the two-sided Mann-Whitney floor for complete separation of a
/// balanced split, multiplied by [`HISTORY_DETECTOR_COUNT`] so both history detectors
/// are accounted for — the same doubling the detectors apply before the group-wide
/// correction. The family size is what the real Benjamini-Hochberg procedure keeps
/// at rank 1.
struct LoneStep {
    /// How many points the series holds.
    points: usize,

    /// Chance level the group-wide correction sees for this lone step.
    chance_level: f64,

    /// Largest judged family in which rank 1 still keeps this chance level.
    largest_family: usize,
}

/// The worked example: a series at the detection minimum that is the only candidate.
fn short_series_lone_table() -> String {
    let step = lone_step(MIN_SERIES_POINTS);
    let kept_family = step.largest_family;
    let dropped_family = kept_family.saturating_add(1);
    let kept = judge(&[("step", step.chance_level)], kept_family);
    let dropped = judge(&[("step", step.chance_level)], dropped_family);

    let mut table = String::from("| Judged family | Rank-1 bar | Outcome |\n");
    table.push_str("|---:|---:|---|\n");
    for (family, judged) in [(kept_family, &kept), (dropped_family, &dropped)] {
        let candidate = judged
            .first()
            .expect("a lone candidate always has a rank-1 decision");
        writeln!(
            table,
            "| {family} | {} | {} |",
            chance(candidate.threshold),
            candidate.outcome()
        )
        .expect("writing to a String never fails");
    }
    writeln!(
        table,
        "\nA series of {} points whose later half sits entirely above its earlier \
         half has chance level {}. Rank 1 in a family of {kept_family} still admits \
         it; rank 1 in a family of {dropped_family} does not.",
        step.points,
        chance(step.chance_level)
    )
    .expect("writing to a String never fails");

    table
}

/// How family reach grows as the series gains balanced pairs of points.
fn short_series_reach_table() -> String {
    let lengths = lone_step_lengths();
    let mut table = String::from(
        "| Points | Chance level of a perfect midpoint step | \
         Largest family that still reports that lone step |\n",
    );
    table.push_str("|---:|---:|---:|\n");
    for points in &lengths {
        let step = lone_step(*points);
        writeln!(
            table,
            "| {} | {} | {} |",
            step.points,
            chance(step.chance_level),
            step.largest_family
        )
        .expect("writing to a String never fails");
    }
    let last = lengths
        .last()
        .copied()
        .expect("the reach table always includes the detection minimum");
    writeln!(
        table,
        "\nThe family of {LARGE_FAMILY} used in the staircase above first admits a \
         lone perfect midpoint step at {last} points."
    )
    .expect("writing to a String never fails");

    table
}

/// Even lengths from the detection minimum through the first that still reports
/// alone in [`LARGE_FAMILY`].
fn lone_step_lengths() -> Vec<usize> {
    let mut lengths = Vec::new();
    let mut points = MIN_SERIES_POINTS;
    loop {
        lengths.push(points);
        if lone_step(points).largest_family >= LARGE_FAMILY {
            return lengths;
        }
        // One more observation on each side of the split: the next balanced length.
        let next = points.saturating_add(2);
        assert!(
            next > points,
            "balanced lengths must grow, or the reach table cannot terminate"
        );
        points = next;
    }
}

/// A perfect midpoint step of `points` observations.
fn lone_step(points: usize) -> LoneStep {
    let chance_level = best_lone_chance_level(points);
    LoneStep {
        points,
        chance_level,
        largest_family: largest_family_keeping_lone(chance_level),
    }
}

/// Chance level of complete separation at the most balanced split, after both
/// history detectors are accounted for.
fn best_lone_chance_level(points: usize) -> f64 {
    let left = balanced_split(points);
    assert!(
        left >= MIN_REGIME && points.saturating_sub(left) >= MIN_REGIME,
        "a lone-step example must still be a pair of full regimes"
    );
    let assignments = combinations(points, left);
    let two_sided = 2.0 / binomial_as_f64(assignments);
    two_sided * crate::coord::of(HISTORY_DETECTOR_COUNT)
}

/// Most balanced split `points` can offer.
///
/// That split is also the complete-separation ranking with the most assignments,
/// and therefore the most surprising Mann-Whitney floor.
fn balanced_split(points: usize) -> usize {
    points
        .checked_div(2)
        .expect("a judged series has at least two points")
}

/// Number of ways to choose `k` of `n` distinct ranks.
fn combinations(n: usize, k: usize) -> u128 {
    let k = k.min(n.saturating_sub(k));
    let mut count: u128 = 1;
    for i in 1..=k {
        let numerator = u128::try_from(n.saturating_sub(k).saturating_add(i))
            .expect("split sizes used here fit in u128");
        let i = u128::try_from(i).expect("the loop index fits in u128");
        let product = count
            .checked_mul(numerator)
            .expect("binomial counts for the series lengths this appendix uses fit in u128");
        // Multiplying in this order keeps every partial product divisible by i.
        assert_eq!(
            product.checked_rem(i),
            Some(0),
            "binomial partial products are integers"
        );
        count = product
            .checked_div(i)
            .expect("the loop index is at least 1");
    }
    count
}

/// `count` as an `f64`, for chance-level arithmetic over binomial assignments.
fn binomial_as_f64(count: u128) -> f64 {
    let count = usize::try_from(count)
        .expect("binomial counts for the series lengths this appendix uses fit in usize");
    crate::coord::of(count)
}

/// Whether rank 1 of `family` keeps a lone candidate at `chance_level`.
fn keeps_lone(chance_level: f64, family: usize) -> bool {
    cbh_stats::benjamini_hochberg(&[chance_level], fdr_q(), family)
        .first()
        .copied()
        .unwrap_or(false)
}

/// Search bound for [`largest_family_keeping_lone`].
///
/// Well above any family a short-series Mann-Whitney floor can survive at rank 1.
/// Crossing it means the length is no longer illustrating a short-series floor.
const LONE_STEP_FAMILY_SEARCH_BOUND: usize = 100_000;

/// Largest family in which a lone candidate at `chance_level` is still kept.
fn largest_family_keeping_lone(chance_level: f64) -> usize {
    assert!(
        chance_level > 0.0,
        "a zero chance level would report in every family"
    );
    let mut family = 1;
    while keeps_lone(chance_level, family) {
        family = family.saturating_add(1);
        assert!(
            family < LONE_STEP_FAMILY_SEARCH_BOUND,
            "a chance level that still reports alone at this family size is not a \
             short-series floor"
        );
    }
    family.saturating_sub(1)
}

/// How many series the chapter's worked example says were judged when both directions were
/// on the table.
///
/// The direction-order example is stated over its own family rather than the census's,
/// because the point it makes is about rank rather than about scale: two candidates in a
/// family this size land on thresholds a reader can check by hand.
const DIRECTION_FAMILY: usize = 10;

/// The improvement's chance level.
///
/// Decisive enough that the alternate order keeps it, isolating the comparison to the
/// regression's rank instead of making both candidates marginal.
const IMPROVEMENT_CHANCE: f64 = 0.001;

/// The regression's chance level.
///
/// Between the thresholds for the first and second rank in this family, which is exactly the
/// range where the table can demonstrate the cost of filtering first.
const REGRESSION_CHANCE: f64 = 0.015;

/// How the appendix names the improvement.
const IMPROVEMENT_SERIES: &str = "checksum";

/// How the appendix names the regression.
const REGRESSION_SERIES: &str = "tokenize";

/// The worked example comparing direction filtering before and after correction.
fn direction_order_table() -> String {
    let corrected_first = judge(
        &[
            (IMPROVEMENT_SERIES, IMPROVEMENT_CHANCE),
            (REGRESSION_SERIES, REGRESSION_CHANCE),
        ],
        DIRECTION_FAMILY,
    );
    // Filtering first removes the improvement from the corrected list without changing the
    // judged family, so the whole difference is the rank the regression lands on.
    let filtered_first = judge(&[(REGRESSION_SERIES, REGRESSION_CHANCE)], DIRECTION_FAMILY);

    let mut table =
        String::from("| Order | Candidate | Rank | Chance level | Threshold at rank | Outcome |\n");
    table.push_str("|---|---|---:|---:|---:|---|\n");
    for (order, judged) in [
        ("correct, then filter", &corrected_first),
        ("filter, then correct", &filtered_first),
    ] {
        for candidate in judged {
            let direction = if candidate.label == IMPROVEMENT_SERIES {
                "improvement"
            } else {
                "regression"
            };
            writeln!(
                table,
                "| {order} | `{}` ({direction}) | {} | {} | {} | {} |",
                candidate.label,
                candidate.rank,
                chance(candidate.chance_level),
                chance(candidate.threshold),
                candidate.outcome()
            )
            .expect("writing to a String never fails");
        }
    }

    let corrected = regression_outcome(&corrected_first);
    let filtered = regression_outcome(&filtered_first);
    writeln!(
        table,
        "\nBoth orders divide by the same {DIRECTION_FAMILY} judged series. The tool \
         filters, then corrects: the regression is at rank {}, where it is {}. Correcting \
         both directions first would leave it at rank {}, where it is {}, before the \
         display would hide the improvement.",
        filtered.rank,
        filtered.outcome(),
        corrected.rank,
        corrected.outcome()
    )
    .expect("writing to a String never fails");

    table
}

/// What became of the regression under one of the two orders.
fn regression_outcome(judged: &[Judged]) -> &Judged {
    judged
        .iter()
        .find(|candidate| candidate.label == REGRESSION_SERIES)
        .expect("the regression is judged under both orders; that is the whole comparison")
}

/// The census the coverage figure accounts for.
///
/// The shape of a pull-request run against a store that has grown past it: most of the
/// suite untouched by the branch and therefore ghosted, a handful of series too young to
/// judge, and a majority of what remains judged. A run where everything was judged would
/// make the figure a single bar and teach a reader nothing about how to read a partial
/// one.
fn worked_census() -> SeriesCensus {
    let mut census = SeriesCensus::default();
    for _ in 0..12 {
        census.record(Testability::Judged);
    }
    census.record_unjudged(UnjudgedReason::Ghost, 5);
    census.record_unjudged(UnjudgedReason::TooFewPoints, 3);
    census.record_unjudged(UnjudgedReason::TooFewPointsSinceBlessing, 1);
    census
}

/// How many series the worked run judged — the family every correction on this page
/// divides by.
///
/// Read from the census rather than stated beside it, which is the chapter's own point:
/// the number the correction divides by and the number the report claims to have covered
/// are the same number, so they cannot drift apart.
fn family_size_of_run() -> usize {
    Coverage::from_census(&worked_census()).judged()
}

/// The census bar: what the run judged, and the reasons it did not judge the rest.
fn census_bar() -> Asset {
    let coverage = Coverage::from_census(&worked_census());
    let caption = format!(
        "{} of {} in-scope series judged ({})",
        coverage.judged(),
        coverage.in_scope(),
        coverage.state().as_str()
    );

    let bar = coverage.reasons().fold(
        Census::new(caption).slice("judged", coverage.judged(), theme::IMPROVEMENT),
        |census, (reason, count)| census.slice(reason_label(reason), count, reason_color(reason)),
    );

    Asset::new("coverage-census.svg", bar.render())
}

/// How the census bar names `reason`.
fn reason_label(reason: UnjudgedReason) -> &'static str {
    match reason {
        UnjudgedReason::Ghost => "ghosts",
        UnjudgedReason::TooFewPoints => "too few points",
        UnjudgedReason::TooFewPointsSinceBlessing => "too few points since blessing",
        UnjudgedReason::NotMeasuredOnBranch => "not measured on the branch",
        UnjudgedReason::TooFewBaseCommits => "too few base-ref commits to compare against",
        UnjudgedReason::TooFewBaseCommitsSinceBlessing => "too few base-ref commits since blessing",
        UnjudgedReason::CurrentBaseRegimeUnresolved => "current base regime unresolved",
    }
}

/// The colour the census bar draws `reason` in.
///
/// A run analyses history or a branch, never both, so the two modes' shortfalls never
/// share a bar and can share a colour. That is what keeps the palette down to the shades
/// the theme distinguishes reliably.
fn reason_color(reason: UnjudgedReason) -> RGBColor {
    match reason {
        // Ghosts sit outside the denominator the coverage state is judged against, so they
        // are drawn in the shade the theme reserves for what is present but not counted.
        UnjudgedReason::Ghost => theme::MUTED,
        UnjudgedReason::TooFewPoints | UnjudgedReason::TooFewBaseCommits => theme::HIGHLIGHT,
        UnjudgedReason::TooFewPointsSinceBlessing
        | UnjudgedReason::TooFewBaseCommitsSinceBlessing
        | UnjudgedReason::NotMeasuredOnBranch
        | UnjudgedReason::CurrentBaseRegimeUnresolved => theme::ALTERNATE,
    }
}

/// What a reader should do about a series left unjudged for `reason`.
fn reason_remedy(reason: UnjudgedReason) -> &'static str {
    match reason {
        UnjudgedReason::Ghost => {
            "Nothing, if the benchmark was removed or its package was not built. Otherwise \
             check that it still runs at the analyzed context commit."
        }
        UnjudgedReason::TooFewPoints => {
            "Wait. The series is judged once enough commits have been measured."
        }
        UnjudgedReason::TooFewPointsSinceBlessing => {
            "Wait. A blessing discards the evidence before it, so the count restarts."
        }
        UnjudgedReason::NotMeasuredOnBranch => {
            "Run the benchmark on the branch, or accept that this one is out of scope for \
             the comparison."
        }
        UnjudgedReason::TooFewBaseCommits => {
            "Measure more of the base ref. A comparison needs a base window to compare \
             against."
        }
        UnjudgedReason::TooFewBaseCommitsSinceBlessing => {
            "Wait for more base-ref measurements after the blessing. Earlier evidence was \
             intentionally excluded."
        }
        UnjudgedReason::CurrentBaseRegimeUnresolved => {
            "Inspect the recent base history. It appears to have moved, but the new level has \
             too little support for an honest comparison."
        }
    }
}

/// Every reason a series goes unjudged, what it means, and what to do about it.
fn reasons_table() -> String {
    let mut table = String::from("| Reason | What it means | What to do |\n|---|---|---|\n");
    for reason in UnjudgedReason::ALL {
        writeln!(
            table,
            "| `{}` | A series {}. | {} |",
            reason.as_str(),
            reason.describe(),
            reason_remedy(reason)
        )
        .expect("writing to a String never fails");
    }
    table
}

/// A census that lands in `state`, so the table quotes a verdict the renderer really
/// produces for it.
///
/// The counts are incidental — what each case has to get right is the relationship between
/// judged, in-scope and total that decides the state.
fn census_in(state: CoverageState) -> SeriesCensus {
    let mut census = SeriesCensus::default();
    match state {
        CoverageState::NoSeries => {}
        CoverageState::NothingInScope => census.record_unjudged(UnjudgedReason::Ghost, 4),
        CoverageState::NothingJudged => census.record_unjudged(UnjudgedReason::TooFewPoints, 4),
        CoverageState::Partial => return worked_census(),
        CoverageState::Full => {
            for _ in 0..4 {
                census.record(Testability::Judged);
            }
        }
    }
    census
}

/// The coverage states, each with the verdict it produces and how far that verdict
/// reaches.
fn states_table() -> String {
    let mut table = String::from("| State | Verdict on a silent run | What the silence covers |\n");
    table.push_str("|---|---|---|\n");
    for state in CoverageState::ALL {
        let coverage = Coverage::from_census(&census_in(state));
        writeln!(
            table,
            "| `{}` | {} | {} |",
            state.as_str(),
            coverage.verdict(),
            state.reach()
        )
        .expect("writing to a String never fails");
    }
    table
}

/// Below this a chance level is written in exponent form; rounded to a fixed number of
/// places it would read as zero, claiming an impossibility.
const CHANCE_DECIMAL_FLOOR: f64 = 0.0001;

/// `p` as a chance level.
fn chance(p: f64) -> String {
    if p >= CHANCE_DECIMAL_FLOOR {
        let text = format!("{p:.4}");
        // The fixed precision always emits a decimal point, so trimming zeros from the end
        // cannot reach a whole number's own digits.
        text.trim_end_matches('0').trim_end_matches('.').to_owned()
    } else {
        format!("{p:.1e}")
    }
}

/// `fraction` as a percentage.
fn percent(fraction: f64) -> String {
    format!("{:.1}%", fraction * 100.0)
}

/// `multiple` as a plain-language multiple.
fn times(multiple: f64) -> String {
    let text = format!("{multiple:.1}");
    format!("{}×", text.trim_end_matches('0').trim_end_matches('.'))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every asset the chapter includes, by the name it includes it under.
    const EMBEDDED: [&str; 10] = [
        "coverage-staircase.svg",
        "coverage-staircase.md",
        "coverage-family-size.svg",
        "coverage-family-size.md",
        "coverage-short-series.md",
        "coverage-short-series-reach.md",
        "coverage-direction-order.md",
        "coverage-census.svg",
        "coverage-reasons.md",
        "coverage-states.md",
    ];

    /// Selects a fragment from its own producer without generating unrelated chapter assets.
    fn content(assets: Vec<Asset>, path: &str) -> String {
        assets
            .into_iter()
            .find(|asset| asset.path == path)
            .unwrap_or_else(|| panic!("{path} is not produced"))
            .content
    }

    /// Judges the marginal candidate without rendering the companion staircase.
    fn marginal_candidate(family: usize) -> Judged {
        judge(&FAMILY_SIZE_CANDIDATES, family)
            .into_iter()
            .find(|candidate| candidate.label == MARGINAL_SERIES)
            .unwrap()
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "plotters SVG generation is host graphics, not memory-safety-relevant, and exceeds the Miri CI budget"
    )]
    fn every_documented_asset_is_produced() {
        let paths: Vec<String> = assets().into_iter().map(|asset| asset.path).collect();

        for expected in EMBEDDED {
            assert!(
                paths.iter().any(|path| path == expected),
                "{expected} missing"
            );
        }
    }

    /// The staircase's lesson is the step-up rescue, which only exists if one candidate
    /// exceeds its own rank's threshold and is kept anyway.
    #[test]
    fn the_staircase_keeps_a_candidate_that_exceeds_its_own_threshold() {
        let judged = judge(&STAIRCASE_CANDIDATES, family_size_of_run());

        let rescued = judged
            .iter()
            .filter(|candidate| candidate.kept && candidate.chance_level > candidate.threshold)
            .count();

        assert!(
            rescued > 0,
            "no candidate is rescued by the step-up: {judged:?}"
        );
    }

    #[test]
    fn the_staircase_drops_the_weakest_candidate() {
        let judged = judge(&STAIRCASE_CANDIDATES, family_size_of_run());

        let last = judged
            .last()
            .expect("the example judges several candidates");

        assert!(!last.kept, "the weakest candidate must be dropped");
        assert!(judged.iter().any(|candidate| candidate.kept));
    }

    /// The table beside the figure is the same decisions in words, so every candidate the
    /// procedure judged has to appear in it.
    #[test]
    #[cfg_attr(
        miri,
        ignore = "renders the complete worked staircase alongside its table; small step-up decisions are tested separately"
    )]
    fn the_staircase_table_states_every_rank_and_threshold() {
        let judged = judge(&STAIRCASE_CANDIDATES, family_size_of_run());
        let table = content(staircase(), "coverage-staircase.md");

        assert!(table.contains(&percent(TARGET_FALSE_DISCOVERY_RATE)));

        for candidate in &judged {
            assert!(
                table.contains(&format!("| {} | `{}` |", candidate.rank, candidate.label)),
                "{} is missing from:\n{table}",
                candidate.label
            );
            assert!(
                table.contains(&chance(candidate.threshold)),
                "the threshold for rank {} is missing from:\n{table}",
                candidate.rank
            );
        }
    }

    /// The chapter's own claim: the number the correction divides by is the number the
    /// report says it judged.
    #[test]
    fn the_family_is_the_number_of_series_the_run_judged() {
        let coverage = Coverage::from_census(&worked_census());

        assert_eq!(family_size_of_run(), coverage.judged());
        assert!(family_size_of_run() >= STAIRCASE_CANDIDATES.len());
    }

    /// The threshold is the step-up procedure's own line, so it must be the one the real
    /// implementation uses: every candidate at or below its rank's threshold is kept.
    #[test]
    fn every_candidate_under_its_threshold_is_kept() {
        let judged = judge(&STAIRCASE_CANDIDATES, family_size_of_run());

        for candidate in &judged {
            if candidate.chance_level <= candidate.threshold {
                assert!(
                    candidate.kept,
                    "rank {} is at or below its threshold but was dropped",
                    candidate.rank
                );
            }
        }
    }

    /// The figure's whole subject is the threshold moving with the size of the family.
    #[test]
    fn the_marginal_candidate_survives_the_small_family_and_not_the_large_one() {
        let small = marginal_candidate(family_size_of_run());
        let large = marginal_candidate(LARGE_FAMILY);

        assert!(small.kept, "the small family must keep it");
        assert!(!large.kept, "the large family must drop it");
        assert!(
            large.threshold < small.threshold,
            "the threshold must tighten as the family grows"
        );
    }

    #[test]
    fn the_same_candidate_is_judged_in_both_families() {
        let small = marginal_candidate(family_size_of_run());
        let large = marginal_candidate(LARGE_FAMILY);

        assert!(
            (small.chance_level - large.chance_level).abs() < f64::EPSILON,
            "the comparison only works if the candidate itself is unchanged"
        );
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "renders both complete family-size staircases; their small candidate comparisons are tested separately"
    )]
    fn the_family_size_table_states_both_families() {
        let table = content(families(), "coverage-family-size.md");

        assert!(table.contains(&format!("| {} |", family_size_of_run())));
        assert!(table.contains(&format!("| {LARGE_FAMILY} |")));
    }

    /// The detection minimum is judged, but a lone perfect step there cannot survive
    /// the chapter's large family.
    #[test]
    fn a_lone_step_at_the_detection_minimum_reports_only_in_a_small_family() {
        let step = lone_step(MIN_SERIES_POINTS);

        assert!(step.largest_family > 0);
        assert!(step.largest_family < LARGE_FAMILY);
        assert!(keeps_lone(step.chance_level, step.largest_family));
        assert!(!keeps_lone(
            step.chance_level,
            step.largest_family.saturating_add(1)
        ));
    }

    #[test]
    fn the_short_series_table_states_the_keep_and_drop_families() {
        let step = lone_step(MIN_SERIES_POINTS);
        let table = short_series_lone_table();
        let dropped = step.largest_family.saturating_add(1);

        assert!(table.contains(&format!("| {} |", step.largest_family)));
        assert!(table.contains(&format!("| {dropped} |")));
        assert!(table.contains(&chance(step.chance_level)));
        assert!(table.contains("kept"));
        assert!(table.contains("dropped"));
    }

    /// The chance level the group-wide correction sees is the Mann-Whitney floor
    /// after both detectors.
    #[test]
    fn the_lone_step_accounts_for_both_history_detectors() {
        let step = lone_step(MIN_SERIES_POINTS);
        let two_sided = 2.0
            / binomial_as_f64(combinations(
                MIN_SERIES_POINTS,
                balanced_split(MIN_SERIES_POINTS),
            ));
        let expected = two_sided * crate::coord::of(HISTORY_DETECTOR_COUNT);

        assert!((step.chance_level - expected).abs() < f64::EPSILON);
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "searches thousands of family sizes with repeated Benjamini-Hochberg evaluations; the detection-minimum case is tested separately"
    )]
    fn a_lone_step_eventually_reaches_the_large_family() {
        let lengths = lone_step_lengths();
        let first = *lengths
            .first()
            .expect("the reach table includes the detection minimum");
        let last = *lengths
            .last()
            .expect("the reach table includes the detection minimum");

        assert_eq!(first, MIN_SERIES_POINTS);
        assert!(lone_step(last).largest_family >= LARGE_FAMILY);
        if let Some(&previous) = lengths.get(lengths.len().saturating_sub(2)) {
            assert!(lone_step(previous).largest_family < LARGE_FAMILY);
        }
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "generates and verifies the complete reach table by repeatedly searching thousands of family sizes"
    )]
    fn the_reach_table_states_every_length() {
        let table = short_series_reach_table();

        for points in lone_step_lengths() {
            let step = lone_step(points);
            assert!(
                table.contains(&format!("| {points} |")),
                "{points} is missing from:\n{table}"
            );
            assert!(table.contains(&chance(step.chance_level)));
            assert!(table.contains(&format!("| {} |", step.largest_family)));
        }
        assert!(table.contains(&LARGE_FAMILY.to_string()));
    }

    /// The chapter states that filtering first is stricter, so the arithmetic has to show
    /// the regression moving to a stricter rank.
    #[test]
    fn filtering_first_is_stricter_for_the_regression() {
        let corrected_first = judge(
            &[
                (IMPROVEMENT_SERIES, IMPROVEMENT_CHANCE),
                (REGRESSION_SERIES, REGRESSION_CHANCE),
            ],
            DIRECTION_FAMILY,
        );
        let filtered_first = judge(&[(REGRESSION_SERIES, REGRESSION_CHANCE)], DIRECTION_FAMILY);

        let corrected = regression_outcome(&corrected_first);
        let filtered = regression_outcome(&filtered_first);

        assert!(
            corrected.kept,
            "correcting both directions first would keep the regression"
        );
        assert!(
            !filtered.kept,
            "filtering first must suppress the regression"
        );
        assert_eq!(corrected.rank, 2);
        assert_eq!(filtered.rank, 1);
        assert!(
            filtered.threshold < corrected.threshold,
            "the earlier rank must carry the stricter threshold"
        );
    }

    /// The improvement is decisive under the alternate order, not a second marginal case.
    #[test]
    fn the_alternate_order_keeps_the_decisive_improvement() {
        let judged = judge(
            &[
                (IMPROVEMENT_SERIES, IMPROVEMENT_CHANCE),
                (REGRESSION_SERIES, REGRESSION_CHANCE),
            ],
            DIRECTION_FAMILY,
        );

        let improvement = judged
            .iter()
            .find(|candidate| candidate.label == IMPROVEMENT_SERIES)
            .expect("the improvement is judged");

        assert!(improvement.kept);
        assert_eq!(improvement.rank, 1);
    }

    #[test]
    fn the_direction_order_table_states_both_orders() {
        let table = direction_order_table();

        assert!(table.contains("correct, then filter"));
        assert!(table.contains("filter, then correct"));
        assert!(table.contains("The tool filters, then corrects"));
        assert!(table.contains("Correcting both directions first would leave it"));
        assert!(table.contains(&chance(IMPROVEMENT_CHANCE)));
        assert!(table.contains(&chance(REGRESSION_CHANCE)));
    }

    /// The threshold is only worth drawing if the tool would actually apply it, which means
    /// the share it derives from is the policy one.
    #[test]
    fn every_threshold_derives_from_the_policy_false_discovery_share() {
        assert!((fdr_q() - TARGET_FALSE_DISCOVERY_RATE).abs() < f64::EPSILON);
        assert!(
            (threshold_at(1, 1) - TARGET_FALSE_DISCOVERY_RATE).abs() < f64::EPSILON,
            "the last rank's threshold in a family of one is the share itself"
        );
    }

    /// The census figure is an account, so every series it draws must be one the census
    /// accounted for.
    #[test]
    fn the_census_bar_accounts_for_every_series() {
        let coverage = Coverage::from_census(&worked_census());

        let drawn = coverage.judged().saturating_add(
            coverage
                .reasons()
                .fold(0_usize, |total, (_, count)| total.saturating_add(count)),
        );

        assert_eq!(drawn, coverage.total());
        assert_eq!(coverage.state(), CoverageState::Partial);
    }

    #[test]
    fn every_reason_is_listed_with_a_meaning_and_a_remedy() {
        let table = reasons_table();

        for reason in UnjudgedReason::ALL {
            assert!(
                table.contains(&format!("| `{}` |", reason.as_str())),
                "{} is missing",
                reason.as_str()
            );
            assert!(table.contains(reason.describe()));
            assert!(table.contains(reason_remedy(reason)));
        }
    }

    /// Every declared reason must be one the detector can really produce, or the table
    /// documents a state that cannot happen.
    #[test]
    fn every_declared_reason_is_one_a_census_can_record() {
        for reason in UnjudgedReason::ALL {
            let mut census = SeriesCensus::default();
            census.record(Testability::Unjudged(reason));

            let recorded: Vec<UnjudgedReason> = Coverage::from_census(&census)
                .reasons()
                .map(|(reason, _)| reason)
                .collect();

            assert_eq!(recorded, vec![reason]);
        }
    }

    #[test]
    fn every_state_is_listed_with_the_verdict_the_renderer_produces() {
        let table = states_table();

        for state in CoverageState::ALL {
            let coverage = Coverage::from_census(&census_in(state));
            assert_eq!(
                coverage.state(),
                state,
                "the {} example lands in a different state",
                state.as_str()
            );
            assert!(
                table.contains(&format!("| `{}` |", state.as_str())),
                "{} is missing",
                state.as_str()
            );
            assert!(table.contains(coverage.verdict()));
            assert!(table.contains(state.reach()));
        }
    }

    /// Only one state judges the whole in-scope suite, which is what makes it the only
    /// silent state with no coverage qualification.
    #[test]
    fn only_the_full_state_has_no_coverage_qualification() {
        let complete: Vec<CoverageState> = CoverageState::ALL
            .into_iter()
            .filter(|&state| {
                let coverage = Coverage::from_census(&census_in(state));
                coverage.in_scope() > 0 && coverage.judged() == coverage.in_scope()
            })
            .collect();

        assert_eq!(complete, vec![CoverageState::Full]);
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
