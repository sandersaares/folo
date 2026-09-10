//! Figures and worked examples for the Noise gates chapter.
//!
//! Every figure here is a rendering of a real [`GateLog`]: the module runs the detector
//! over an example series and draws the decisions it recorded gate by gate. No threshold
//! is written down — each one is read from the policy constants or from the outcome
//! a gate logged — so a change in the gating policy rewrites the chapter rather than
//! leaving its numbers behind, and the freshness check turns that into a failing test.

use std::fmt::Write as _;
use std::iter;

use cbh_detect::{
    BRANCH_NOISE_MULTIPLE, BRANCH_PRACTICAL_RELATIVE, DRIFT_MIN_POINTS, DRIFT_NOISE_MULTIPLE,
    Finding, Gate, GateLog, GateOutcome, GateStage, MAX_CHANGE_CHANCE_LEVEL,
    MAX_DRIFT_CHANCE_LEVEL, MIN_REGIME, MIN_REGIME_SEPARATION, MIN_SERIES_POINTS,
    PRACTICAL_RELATIVE, RESIDUAL_NOISE_MULTIPLE, Series, evaluate_with_log, examples,
};
use cbh_model::MetricKind;

use crate::assets::Asset;
use crate::styles::gates::{Agreement, Residuals};
use crate::styles::operation::Operation;
use crate::styles::plot::{Mark, Observation, Plot};
use crate::theme;

/// Every asset the Noise gates chapter embeds.
#[must_use]
pub fn assets() -> Vec<Asset> {
    let mut assets = vec![Asset::new("gates-order.md", order_table())];
    assets.extend(ladders());
    assets.extend(scale());
    assets.push(Asset::new("gates-floors.md", floors_table()));
    assets.push(residual_strip());
    assets.extend(agreement_grids());
    assets.push(interval_overlap());
    assets
}

/// The metric every gate example is measured in.
///
/// A timing metric is what the chapter's prose is written around, and the one whose
/// absolute floor a reader can weigh against a value on the page without conversion.
const EXAMPLE_KIND: MetricKind = MetricKind::WallTime;

/// The detectors whose gates the chapter documents, in pipeline order.
const STAGES: [GateStage; 3] = [GateStage::ChangePoint, GateStage::Drift, GateStage::Branch];

/// The change-point detector's gates, in the order it applies them.
///
/// Declared here rather than derived from a log because a log only records the gates its
/// candidate reached: the order a reader needs is the whole sequence, including the part
/// no single example gets to. A test compares each declared order against what the
/// detector records, so a reordering in the code fails here instead of quietly
/// contradicting the chapter.
const CHANGE_POINT_GATES: [Gate; 10] = [
    Gate::SplitLocated,
    Gate::MinRegime,
    Gate::NonZeroDelta,
    Gate::RelativeFloor,
    Gate::AbsoluteFloor,
    Gate::ResidualNoise,
    Gate::RegimeSeparation,
    Gate::IntervalDisjoint,
    Gate::SelectionAdjustment,
    Gate::Significance,
];

/// The drift detector's gates, in the order it applies them.
const DRIFT_GATES: [Gate; 7] = [
    Gate::MinSeriesPoints,
    Gate::Significance,
    Gate::NonZeroDelta,
    Gate::RelativeFloor,
    Gate::AbsoluteFloor,
    Gate::ResidualNoise,
    Gate::IntervalNoiseBand,
];

/// The branch comparison's gates, in the order it applies them.
const BRANCH_GATES: [Gate; 6] = [
    Gate::MinBaseCommits,
    Gate::NonZeroDelta,
    Gate::RelativeFloor,
    Gate::AbsoluteFloor,
    Gate::IntervalDisjoint,
    Gate::IntervalNoiseBand,
];

/// The gates `stage` applies, in the order it applies them.
fn stage_gates(stage: GateStage) -> &'static [Gate] {
    match stage {
        GateStage::ChangePoint => &CHANGE_POINT_GATES,
        GateStage::Drift => &DRIFT_GATES,
        GateStage::Branch => &BRANCH_GATES,
    }
}

/// What the detector behind `stage` is looking for.
fn stage_subject(stage: GateStage) -> &'static str {
    match stage {
        GateStage::ChangePoint => "a level that moved and stayed moved",
        GateStage::Drift => "a level that is moving steadily",
        GateStage::Branch => "a context commit against the base ref",
    }
}

/// Runs the real history-mode detector over `series` and returns what it decided, with
/// the gate decisions it recorded on the way.
fn judge(series: &Series) -> (Option<Finding>, GateLog) {
    evaluate_with_log(series, &examples::history_context(series))
}

/// The outcome `stage` recorded for `gate`, where it reached the gate at all.
fn outcome_of(log: &GateLog, stage: GateStage, gate: Gate) -> Option<GateOutcome> {
    log.entries()
        .iter()
        .find(|outcome| outcome.stage == stage && outcome.gate == gate)
        .copied()
}

/// The gates `stage` applies below `gate`, which a veto at `gate` cuts off.
fn gates_below(stage: GateStage, gate: Gate) -> Vec<Gate> {
    stage_gates(stage)
        .iter()
        .copied()
        .skip_while(|&candidate| candidate != gate)
        .skip(1)
        .collect()
}

/// What `stage` demands of `gate`, as the chapter states it.
///
/// Every threshold is read from the same policy constants the detector uses. Several gates
/// are one policy asked by more than one detector, and some of them are asked against a
/// different figure depending on who is asking — which is why the stage is a parameter
/// rather than the gate alone deciding.
fn demanded(gate: Gate, stage: GateStage) -> String {
    match gate {
        Gate::MinSeriesPoints => points(DRIFT_MIN_POINTS),
        Gate::MinBaseCommits => commits(MIN_SERIES_POINTS),
        Gate::SplitLocated => "a split must be found".to_owned(),
        Gate::MinRegime => match stage {
            // Branch mode counts retained base-ref commit levels, not the context sample
            // and not the shorter side of a split. Ref:
            // packages/cargo-bench-history/docs/DESIGN.md, "Judging a context commit",
            // section 8.2, noise-aware gating.
            GateStage::Branch => commits(MIN_REGIME),
            GateStage::ChangePoint | GateStage::Drift => points(MIN_REGIME),
        },
        Gate::NonZeroDelta => "above zero".to_owned(),
        Gate::SelectionAdjustment => "corrects the level; never declines".to_owned(),
        Gate::RelativeFloor => match stage {
            GateStage::Branch => percent(BRANCH_PRACTICAL_RELATIVE),
            _ => percent(PRACTICAL_RELATIVE),
        },
        Gate::AbsoluteFloor => "the metric's own floor, below".to_owned(),
        Gate::ResidualNoise => format!("{}× the typical residual", number(RESIDUAL_NOISE_MULTIPLE)),
        Gate::Significance => match stage {
            GateStage::Drift => format!("p < {}", chance(MAX_DRIFT_CHANCE_LEVEL)),
            _ => format!("p < {}", chance(MAX_CHANGE_CHANCE_LEVEL)),
        },
        Gate::RegimeSeparation => share(MIN_REGIME_SEPARATION),
        Gate::IntervalDisjoint => "the two intervals must not overlap".to_owned(),
        Gate::IntervalNoiseBand => match stage {
            GateStage::Branch => {
                format!("{}× the reported half-width", number(BRANCH_NOISE_MULTIPLE))
            }
            _ => format!("{}× the reported half-width", number(DRIFT_NOISE_MULTIPLE)),
        },
    }
}

/// What a gate weighs, as the chapter describes it.
///
/// The match is exhaustive so a gate added to the detector cannot reach the book
/// undescribed: the compiler asks for its line here before the table can be rendered.
fn compares(gate: Gate, stage: GateStage) -> &'static str {
    match gate {
        Gate::MinSeriesPoints => "How many points the analyzed window holds.",
        Gate::MinBaseCommits => "How many base-ref commit levels the comparison window holds.",
        Gate::SplitLocated => "Whether a candidate split exists at all.",
        Gate::MinRegime => match stage {
            GateStage::ChangePoint | GateStage::Drift => {
                "How many points the shorter side of the split holds."
            }
            GateStage::Branch => "How many retained base-ref commit levels the comparison holds.",
        },
        Gate::NonZeroDelta => match stage {
            GateStage::ChangePoint => "Whether the two regime levels differ at all.",
            GateStage::Drift => "Whether the fitted line moved across the window.",
            GateStage::Branch => {
                "Whether the context run lies strictly outside the observed current-base range."
            }
        },
        Gate::SelectionAdjustment => {
            "The change-point's rank-test chance level, before the split-search correction."
        }
        Gate::RelativeFloor => "The move as a fraction of the baseline.",
        Gate::AbsoluteFloor => "The move in the metric's own units.",
        Gate::ResidualNoise => "The move against the series' own typical residual.",
        Gate::Significance => match stage {
            GateStage::ChangePoint => {
                "The chance level of the rank test comparing the two regimes."
            }
            GateStage::Drift => "The chance level of the trend test across the window.",
            GateStage::Branch => "Branch mode does not apply a per-series significance test.",
        },
        Gate::RegimeSeparation => "The share of before-and-after pairs that agree the level moved.",
        Gate::IntervalDisjoint => match stage {
            GateStage::Branch => {
                "The current-base observations' and context run's reported confidence intervals."
            }
            GateStage::ChangePoint | GateStage::Drift => {
                "The two regimes' reported confidence intervals."
            }
        },
        Gate::IntervalNoiseBand => "The move against the engine's own reported imprecision.",
    }
}

/// The gates each detector applies, in the order it applies them.
fn order_table() -> String {
    let mut markdown = String::from(
        "Each detector applies its own gates in its own order, and a candidate stops at \
         the first gate that declines it. A gate several detectors share is one policy \
         asked at a different point in each sequence.\n",
    );

    for stage in STAGES {
        writeln!(
            markdown,
            "\n**`{}`** — {}\n",
            stage.label(),
            stage_subject(stage)
        )
        .expect("writing to a String never fails");
        markdown.push_str("| Gate | What it compares | Threshold |\n|---|---|---|\n");
        for &gate in stage_gates(stage) {
            writeln!(
                markdown,
                "| `{}` | {} | {} |",
                gate.label(),
                compares(gate, stage),
                demanded(gate, stage)
            )
            .expect("writing to a String never fails");
        }
    }

    markdown
}

/// How a gate's recorded value and threshold read on the page.
///
/// The shape belongs to the gate rather than to the outcome: a gate comparing magnitudes
/// always does, whichever detector asked, and a boolean-shaped gate never carries a number
/// to show. Deriving the rendering from the gate is what keeps a gate with nothing to
/// measure reading as "held" rather than as a fabricated zero.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Reading {
    /// A move in the metric's own units, against a floor in the same units.
    Magnitude,

    /// A move in the metric's own units, against nothing but zero.
    NonZero,

    /// A fraction of the baseline.
    Fraction,

    /// A chance level against its alpha.
    Chance,

    /// A chance level corrected into another chance level: a mapping, not a bar.
    Correction,

    /// A probability of superiority against its floor.
    Share,

    /// A count of points, against a minimum.
    Points,

    /// Nothing to measure: the gate either held or it did not.
    Boolean,
}

/// What a gate did with the candidate in a rendered gate-ladder row.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Verdict {
    /// The candidate cleared the gate and moved on to the next one.
    Passed,

    /// The gate declined the candidate; nothing below it ran.
    Declined,

    /// The gate never ran because an earlier gate declined the candidate.
    NotReached,
}

impl Verdict {
    /// The word the table uses for this outcome.
    fn label(self) -> &'static str {
        match self {
            Self::Passed => "pass",
            Self::Declined => "**declined**",
            Self::NotReached => "not run",
        }
    }
}

/// One gate's row in a gate-ladder table.
#[derive(Clone, Debug)]
struct Rung {
    /// The gate's identifier, as the appendix refers to it.
    gate: String,

    /// What the gate computed from the candidate.
    value: String,

    /// What the gate required.
    threshold: String,

    /// What the gate did.
    verdict: Verdict,
}

/// How `gate` reads on the page.
fn reading(gate: Gate) -> Reading {
    match gate {
        Gate::MinSeriesPoints | Gate::MinBaseCommits | Gate::MinRegime => Reading::Points,
        Gate::SplitLocated | Gate::IntervalDisjoint => Reading::Boolean,
        Gate::RelativeFloor => Reading::Fraction,
        Gate::NonZeroDelta => Reading::NonZero,
        Gate::AbsoluteFloor | Gate::ResidualNoise | Gate::IntervalNoiseBand => Reading::Magnitude,
        Gate::Significance => Reading::Chance,
        Gate::SelectionAdjustment => Reading::Correction,
        Gate::RegimeSeparation => Reading::Share,
    }
}

/// What a gate's outcome computed, and what it demanded, rendered for the page.
fn reading_of(outcome: &GateOutcome, kind: MetricKind) -> (String, String) {
    let value = outcome.value;
    let threshold = outcome.threshold;
    match reading(outcome.gate) {
        Reading::Magnitude => (magnitude(kind, value), magnitude(kind, threshold)),
        Reading::NonZero => (magnitude(kind, value), "above zero".to_owned()),
        Reading::Fraction => (rendered(value, percent), rendered(threshold, percent)),
        Reading::Chance => (
            format!("p = {}", rendered(value, chance)),
            format!("p < {}", rendered(threshold, chance)),
        ),
        Reading::Correction => (
            format!(
                "p = {} → p = {}",
                rendered(value, chance),
                rendered(threshold, chance)
            ),
            "corrected for the search".to_owned(),
        ),
        Reading::Share => (rendered(value, share), rendered(threshold, share)),
        Reading::Points => (
            format!("{} points", rendered(value, number)),
            format!("{} points", rendered(threshold, number)),
        ),
        Reading::Boolean => (
            if outcome.passed {
                "held".to_owned()
            } else {
                "did not hold".to_owned()
            },
            "must hold".to_owned(),
        ),
    }
}

/// The rungs `stage` recorded in `log`, followed by the gates its veto cut off.
fn rungs(log: &GateLog, stage: GateStage, kind: MetricKind) -> Vec<Rung> {
    let mut rungs: Vec<Rung> = log
        .entries()
        .iter()
        .filter(|outcome| outcome.stage == stage)
        .map(|outcome| {
            let (value, threshold) = reading_of(outcome, kind);
            Rung {
                gate: outcome.gate.label().to_owned(),
                value,
                threshold,
                verdict: if outcome.passed {
                    Verdict::Passed
                } else {
                    Verdict::Declined
                },
            }
        })
        .collect();

    // Only the gates below the veto are rendered as not run. A gate the detector skipped
    // for want of an input — an interval gate on an engine that reports none — was not cut
    // off by anything, and drawing it here would claim a short-circuit that never happened.
    let Some(declining) = log.declined_by_stage(stage) else {
        return rungs;
    };
    rungs.extend(gates_below(stage, declining).into_iter().map(|gate| Rung {
        gate: gate.label().to_owned(),
        value: "not run".to_owned(),
        threshold: demanded(gate, stage),
        verdict: Verdict::NotReached,
    }));
    rungs
}

/// A gate ladder rendered as a Markdown table.
fn ladder_table(rungs: &[Rung]) -> String {
    let mut markdown = String::from("| Gate | Demand | Computed | Verdict |\n|---|---|---|---|\n");
    for rung in rungs {
        writeln!(
            markdown,
            "| `{}` | {} | {} | {} |",
            rung.gate,
            rung.threshold,
            rung.value,
            rung.verdict.label()
        )
        .expect("writing to a String never fails");
    }
    markdown
}

/// The confidence-interval half-width an engine measuring `values` would report.
///
/// Derived from the scatter the examples are drawn with rather than chosen, so the gates
/// that read dispersion are shown against a precision consistent with the data on the
/// page: an engine reporting an interval far wider than its own between-commit scatter
/// would be a broken engine rather than a stricter gate.
fn reported_half_width(values: &[f64]) -> f64 {
    cbh_stats::median(values).unwrap_or_default() * examples::TIMING_NOISE_CV
}

/// The candidate that clears every gate: the chapter's clean step, measured by an engine
/// whose reported precision matches the scatter the series carries.
fn passing_candidate() -> Series {
    let values = examples::clean_step();
    let series = examples::series("tokenize", &values, EXAMPLE_KIND, 0);
    examples::with_intervals(series, reported_half_width(&values))
}

/// The scatter the declined candidate's series carries, as a coefficient of variation.
///
/// Found by sweeping the real detector: wide enough that several times the series' own
/// typical residual covers the step, narrow enough that the rank test still calls the two
/// regimes different. Both are needed for the ladder to be worth drawing, since the
/// candidate has to reach the residual gate on merit before that gate can decline it.
const DECLINED_SCATTER_CV: f64 = 0.08;

/// The level the declined candidate's series sits at before the step.
const DECLINED_BASELINE: f64 = 100.0;

/// The level it steps to.
///
/// Several times the relative floor above the baseline, so no magnitude gate can be the
/// one that declines the candidate.
const DECLINED_ELEVATED: f64 = 115.0;

/// The candidate the residual gate declines: a real step that the series' own scatter
/// explains.
fn declined_candidate() -> Series {
    // Twice the persistence floor on each side, so regime length is never the binding
    // constraint and the ladder reaches the gate the figure is about.
    let regime = MIN_REGIME.saturating_mul(2);
    let levels: Vec<f64> = iter::repeat_n(DECLINED_BASELINE, regime)
        .chain(iter::repeat_n(DECLINED_ELEVATED, regime))
        .collect();
    let values = examples::scattered(
        &levels,
        DECLINED_SCATTER_CV,
        examples::seed_of("noisy_step"),
    );
    let series = examples::series("parse_headers", &values, EXAMPLE_KIND, 0);
    // The interval gates sit below the gate that declines this candidate, so the ladder
    // table marks them as not run — which is only honest if the series carries intervals for
    // them to have read.
    examples::with_intervals(series, reported_half_width(&values))
}

/// The two ladders: a candidate that survives everything, and one that does not.
fn ladders() -> Vec<Asset> {
    let passing = passing_candidate();
    let (_, passing_log) = judge(&passing);
    let passing_rungs = rungs(&passing_log, GateStage::ChangePoint, passing.kind);

    let declined = declined_candidate();
    let (_, declined_log) = judge(&declined);
    let declined_rungs = rungs(&declined_log, GateStage::ChangePoint, declined.kind);

    vec![
        Asset::new("gates-ladder-pass.md", ladder_table(&passing_rungs)),
        Asset::new(
            "gates-ladder-declined.md",
            declined_fragment(&declined_log, declined.kind, &declined_rungs),
        ),
    ]
}

/// The prose fragment naming the gate that declined the candidate.
///
/// The gate is read from the log rather than from what the example was built to
/// demonstrate, so a candidate that starts falling somewhere else renames the sentence
/// instead of leaving the chapter asserting a decision the detector no longer makes.
fn declined_fragment(log: &GateLog, kind: MetricKind, rungs: &[Rung]) -> String {
    let Some(gate) = log.declined_by_stage(GateStage::ChangePoint) else {
        return "> **Reported**, unexpectedly. This example is documented as being \
                declined, so this fragment means the two have diverged.\n"
            .to_owned();
    };

    let detail = outcome_of(log, GateStage::ChangePoint, gate).map_or_else(
        || "The gate log records the decision without a number to compare.".to_owned(),
        |outcome| {
            let (value, threshold) = reading_of(&outcome, kind);
            format!("The detector computed {value}, against a demand of {threshold}.")
        },
    );
    format!(
        "**Declined** by `{}`. {detail} The gates below it never ran.\n\n{}",
        gate.label(),
        ladder_table(rungs)
    )
}

/// The level the small-magnitude example sits at.
///
/// The benchmark the chapter's prose describes: one running at a few nanoseconds an
/// iteration, where a percentage stops meaning anything.
const SMALL_BASELINE: f64 = 3.0;

/// The level it moves to.
const SMALL_ELEVATED: f64 = 3.2;

/// How much larger the second example's benchmark is.
///
/// Three orders of magnitude, which is the distance between a benchmark measured in
/// nanoseconds and one measured in microseconds — far enough that the same proportional
/// move lands well clear of a floor the small one cannot reach.
const LARGE_MAGNITUDE: f64 = 1000.0;

/// The same proportional move at two magnitudes, against the floor that separates them.
fn scale() -> Vec<Asset> {
    let regime = MIN_REGIME.saturating_mul(2);
    let small: Vec<f64> = iter::repeat_n(SMALL_BASELINE, regime)
        .chain(iter::repeat_n(SMALL_ELEVATED, regime))
        .collect();
    let large: Vec<f64> = small.iter().map(|value| value * LARGE_MAGNITUDE).collect();

    let small_series = examples::series("checksum_byte", &small, EXAMPLE_KIND, 0);
    let large_series = examples::series("checksum_page", &large, EXAMPLE_KIND, 0);
    let (small_finding, small_log) = judge(&small_series);
    let (large_finding, _) = judge(&large_series);

    let floor = outcome_of(&small_log, GateStage::ChangePoint, Gate::AbsoluteFloor)
        .and_then(|outcome| outcome.threshold)
        .expect(
            "the small-magnitude example clears the relative floor, so the absolute floor \
             is the next gate it reaches; a missing outcome means it is now being declined \
             earlier",
        );
    let relative = outcome_of(&small_log, GateStage::ChangePoint, Gate::RelativeFloor)
        .and_then(|outcome| outcome.value)
        .expect(
            "the same evaluation reaches the relative floor before the absolute one, so \
             its outcome is recorded whenever the absolute floor's is",
        );

    let title = format!("the same {} move at two magnitudes", percent(relative));
    let figure = Operation::new(
        title,
        scale_pane(&small, floor, small_finding.is_some()),
        scale_pane(&large, floor, large_finding.is_some()),
    );

    vec![Asset::new("gates-absolute-floor.svg", figure.render())]
}

/// One magnitude's pane: the step, and the dead zone the absolute floor draws around the
/// level it started from.
fn scale_pane(values: &[f64], floor: f64, reported: bool) -> Plot {
    let split = values.len().saturating_div(2);
    let baseline = values.first().copied().unwrap_or_default();
    let latest = values.last().copied().unwrap_or_default();
    let caption = if reported {
        format!("at {}: reported", quantity(EXAMPLE_KIND, baseline))
    } else {
        format!(
            "at {}: the move fits inside the floor",
            quantity(EXAMPLE_KIND, baseline)
        )
    };

    // The elevated points carry the removed mark in the pane where the floor declined the
    // move: the gate under illustration is what took this candidate out of the report, and
    // the band it fits inside is the reason.
    let mark = if reported {
        Mark::Regression
    } else {
        Mark::Removed
    };
    Plot::new(caption, values.len())
        .value_label(EXAMPLE_KIND.as_unit())
        .observations(values.iter().enumerate().map(|(index, &value)| {
            let observation = Observation::new(index, value);
            if index >= split {
                observation.marked(mark)
            } else {
                observation
            }
        }))
        .value_band(
            0,
            values.len().saturating_sub(1),
            (baseline - floor, baseline + floor),
            format!("moves smaller than {}", quantity(EXAMPLE_KIND, floor)),
            theme::MUTED,
        )
        .rule(latest, "the new level", theme::REGRESSION)
}

/// The step every metric's absolute floor is quoted against.
///
/// A move large enough in relative terms to reach the absolute-floor gate on every metric,
/// and small enough in absolute terms that no metric's floor is trivially cleared. The
/// values themselves are incidental: what the table needs is for each metric's floor to be
/// quoted by a gate rather than transcribed from the policy constants.
fn floor_probe() -> Vec<f64> {
    let regime = MIN_REGIME.saturating_mul(2);
    iter::repeat_n(10.0_f64, regime)
        .chain(iter::repeat_n(10.5_f64, regime))
        .collect()
}

/// The absolute floor the detector applied to `kind`, read back from a real evaluation.
fn observed_absolute_floor(kind: MetricKind) -> f64 {
    let series = examples::series("floor_probe", &floor_probe(), kind, 0);
    let (_, log) = judge(&series);
    outcome_of(&log, GateStage::ChangePoint, Gate::AbsoluteFloor)
        .and_then(|outcome| outcome.threshold)
        .expect(
            "the probe series steps well past the relative floor, so every metric reaches \
             the absolute floor gate; a missing outcome means the gate order changed",
        )
}

/// Why `kind` has the absolute floor it has, condensed from the policy's own reasoning.
fn floor_reason(kind: MetricKind) -> &'static str {
    match kind {
        MetricKind::WallTime | MetricKind::ProcessorTime => {
            "A timing engine fits a slope across a run's iterations and resolves far below \
             a clock tick, so this is a judgement about what is worth acting on rather \
             than a resolution limit."
        }
        MetricKind::InstructionCount
        | MetricKind::ConditionalBranches
        | MetricKind::IndirectBranches => {
            "Code layout can shift these counts by a few units between builds of identical \
             source, so a handful of them says nothing about what the code costs."
        }
        MetricKind::AllocatedBytes | MetricKind::AllocationCount => {
            "A fraction of a byte or of an allocation cannot happen; the floor rejects only \
             the sub-unit moves that amortizing across a run's iterations manufactures."
        }
    }
}

/// The per-metric absolute floors, each read from the gate that applied it.
fn floors_table() -> String {
    let mut markdown = String::from("| Metric | Absolute floor | Why |\n|---|---|---|\n");
    for kind in MetricKind::ALL {
        writeln!(
            markdown,
            "| `{}` | {} | {} |",
            kind.as_str(),
            quantity(kind, observed_absolute_floor(kind)),
            floor_reason(kind)
        )
        .expect("writing to a String never fails");
    }
    markdown
}

/// Each point's signed distance from its own regime's median — the two-regime model the
/// change-point detector fits.
///
/// Signed rather than absolute because the figure is about spread around the model, where
/// a fold at zero would hide half the shape. The gate itself compares the *median absolute*
/// distance, which is the band drawn beside these.
fn step_residuals(values: &[f64], split: usize) -> Vec<f64> {
    let (Some(before), Some(after)) = (values.get(..split), values.get(split..)) else {
        return Vec::new();
    };
    let (Some(before_level), Some(after_level)) =
        (cbh_stats::median(before), cbh_stats::median(after))
    else {
        return Vec::new();
    };
    before
        .iter()
        .map(|value| value - before_level)
        .chain(after.iter().map(|value| value - after_level))
        .collect()
}

/// Where the split search placed the boundary in `values`.
fn split_of(values: &[f64]) -> usize {
    cbh_stats::pettitt(values).map_or(0, |change| change.index)
}

/// The residuals about the fitted model, the band the gate treats as ordinary, and the
/// move being judged against it.
fn residual_strip() -> Asset {
    let series = declined_candidate();
    let values: Vec<f64> = series.points.iter().map(|point| point.value).collect();
    let (_, log) = judge(&series);
    let outcome = outcome_of(&log, GateStage::ChangePoint, Gate::ResidualNoise).expect(
        "the declined example is built to reach the residual gate, so its outcome is \
         recorded; a missing one means the candidate is now falling earlier",
    );

    let strip = Residuals::new(
        "a step against the series' typical residual and the gate threshold it must clear",
        step_residuals(&values, split_of(&values)),
        outcome.threshold.unwrap_or_default(),
    )
    .move_size(outcome.value.unwrap_or_default());

    Asset::new("gates-residual.svg", strip.render())
}

/// The two agreement grids: a step whose sides separate, and a series that revisits both
/// levels.
fn agreement_grids() -> Vec<Asset> {
    vec![
        Asset::new(
            "gates-agreement-separated.svg",
            grid("a step", &examples::clean_step()),
        ),
        Asset::new(
            "gates-agreement-oscillating.svg",
            grid(
                "a series that revisits both levels",
                &examples::overlapping_regimes(),
            ),
        ),
    ]
}

/// A grid for `values`, captioned with the share it draws and the floor that share is
/// judged against.
///
/// The grid is a field of squares, which shows a reader whether the pairings agree but not
/// how close the series came to the gate's demand. Putting both numbers in the caption is
/// what turns the picture into evidence, and reading them from the data and the
/// policy is what stops the caption from outliving either.
fn grid(subject: &str, values: &[f64]) -> String {
    let floor = MIN_REGIME_SEPARATION;
    let grid = agreement("", values);
    let caption = format!(
        "{subject}: {} of pairings agree, against a floor of {}",
        share(grid.share()),
        share(floor)
    );
    agreement(&caption, values).render()
}

/// The grid of before-and-after pairings either side of `values`' own split.
fn agreement(caption: &str, values: &[f64]) -> Agreement {
    let split = split_of(values);
    let before = values.get(..split).unwrap_or_default().to_vec();
    let after = values.get(split..).unwrap_or_default().to_vec();
    Agreement::new(caption, before, after)
}

/// How much of the move the overlapping example's engine reports as its own imprecision.
///
/// Above one half, since intervals of exactly that half-width either side of two levels
/// that far apart merely touch. This leaves a visible overlap rather than a marginal one.
const OVERLAPPING_INTERVAL_SHARE: f64 = 0.75;

/// Two regimes whose reported confidence intervals overlap.
fn interval_overlap() -> Asset {
    let values = examples::clean_step();
    let split = split_of(&values);
    let before = values.get(..split).unwrap_or_default();
    let after = values.get(split..).unwrap_or_default();
    let before_level = cbh_stats::median(before).unwrap_or_default();
    let after_level = cbh_stats::median(after).unwrap_or_default();
    let half_width = (after_level - before_level).abs() * OVERLAPPING_INTERVAL_SHARE;

    let series = examples::with_intervals(
        examples::series("tokenize", &values, EXAMPLE_KIND, 0),
        half_width,
    );
    let (_, log) = judge(&series);
    let declined = log
        .declined_by_stage(GateStage::ChangePoint)
        .map_or_else(|| "nothing".to_owned(), |gate| gate.label().to_owned());

    // Both regimes' intervals are drawn across the whole figure rather than over their own
    // stretch of history: the gate compares the two ranges, and ranges side by side do not
    // show the reader whether they overlap.
    let span = values.len();
    let plot = Plot::new("two levels the engine cannot separate", span)
        .value_label(EXAMPLE_KIND.as_unit())
        .observations(values.iter().enumerate().map(|(index, &value)| {
            Observation::new(index, value).interval(value - half_width, value + half_width)
        }))
        .split(split, "change point")
        .value_band(
            0,
            span.saturating_sub(1),
            (before_level - half_width, before_level + half_width),
            "the earlier level's interval",
            theme::HIGHLIGHT,
        )
        .value_band(
            0,
            span.saturating_sub(1),
            (after_level - half_width, after_level + half_width),
            "the later level's interval",
            theme::REGRESSION,
        )
        .note(
            span.saturating_div(2),
            f64::midpoint(before_level, after_level),
            format!("declined by {declined}"),
            theme::MUTED,
        );

    Asset::new("gates-interval-overlap.svg", plot.render())
}

/// `values` in the metric's own units.
fn magnitude(kind: MetricKind, value: Option<f64>) -> String {
    value.map_or_else(|| "not recorded".to_owned(), |value| quantity(kind, value))
}

/// `rendered` applied to a recorded number, or a note that the gate recorded none.
fn rendered(value: Option<f64>, render: fn(f64) -> String) -> String {
    value.map_or_else(|| "not recorded".to_owned(), render)
}

/// The noun `kind` counts, in the singular and the plural.
///
/// [`MetricKind::as_unit`] answers "count" for the counted metrics, which reads as
/// "5 count" in a sentence the appendix has to print. These name the thing being counted
/// instead.
fn unit_nouns(kind: MetricKind) -> (&'static str, &'static str) {
    match kind {
        MetricKind::WallTime | MetricKind::ProcessorTime => ("ns", "ns"),
        MetricKind::InstructionCount => ("instruction", "instructions"),
        MetricKind::ConditionalBranches => ("conditional branch", "conditional branches"),
        MetricKind::IndirectBranches => ("indirect branch", "indirect branches"),
        MetricKind::AllocatedBytes => ("byte", "bytes"),
        MetricKind::AllocationCount => ("allocation", "allocations"),
    }
}

/// `value` in the metric's own units.
fn quantity(kind: MetricKind, value: f64) -> String {
    let (singular, plural) = unit_nouns(kind);
    let noun = if (value - 1.0).abs() < f64::EPSILON {
        singular
    } else {
        plural
    };
    format!("{} {noun}", number(value))
}

/// `value` as a short decimal.
///
/// Two places: enough to separate the figures the gates compare, few enough that a table
/// of them stays readable. A quantity needing more resolution than that is a chance level,
/// which [`chance`] renders instead.
fn number(value: f64) -> String {
    let text = format!("{value:.2}");
    // The fixed precision always emits a decimal point, so trimming zeros from the end
    // cannot reach an integer's own digits.
    text.trim_end_matches('0').trim_end_matches('.').to_owned()
}

/// `fraction` as a percentage of the level it is measured against.
fn percent(fraction: f64) -> String {
    format!("{:.1}%", fraction * 100.0)
}

/// `value` as a share of one.
///
/// Kept at a fixed width rather than trimmed, because a share is read against another
/// share and a bare `1` beside `0.85` invites reading the two as different quantities.
fn share(value: f64) -> String {
    format!("{value:.2}")
}

/// Below this a chance level is written in exponent form; rounded to a fixed number of
/// places it would lose the resolution that separates a decisive result from a marginal
/// one.
const CHANCE_DECIMAL_FLOOR: f64 = 0.001;

/// `p` as a chance level.
fn chance(p: f64) -> String {
    if p >= CHANCE_DECIMAL_FLOOR {
        let text = format!("{p:.4}");
        text.trim_end_matches('0').trim_end_matches('.').to_owned()
    } else {
        format!("{p:.1e}")
    }
}

/// `count` rendered as a plural-correct number of points.
fn points(count: usize) -> String {
    if count == 1 {
        "1 point".to_owned()
    } else {
        format!("{count} points")
    }
}

/// `count` rendered as a plural-correct number of commit levels.
fn commits(count: usize) -> String {
    if count == 1 {
        "1 commit level".to_owned()
    } else {
        format!("{count} commit levels")
    }
}

#[cfg(test)]
mod tests {
    use cbh_detect::{
        MIN_SERIES_POINTS, PRACTICAL_ABSOLUTE_ALLOC, PRACTICAL_ABSOLUTE_COUNT,
        PRACTICAL_ABSOLUTE_TIME,
    };

    use super::*;

    /// The gates chapter names every gate in a hand-written table grouping them by the
    /// question each asks. That table is the appendix's one list of gate identifiers that is
    /// not itself generated, so this holds it to the enum: a gate added or removed without
    /// the chapter following would otherwise leave the prose quietly incomplete.
    #[test]
    fn the_documented_stages_between_them_cover_every_gate() {
        let mut documented: Vec<Gate> = STAGES
            .into_iter()
            .flat_map(|stage| stage_gates(stage).iter().copied())
            .collect();
        documented.sort_by_key(|gate| gate.label());
        documented.dedup();

        let mut all = Gate::ALL.to_vec();
        all.sort_by_key(|gate| gate.label());

        assert_eq!(
            documented.len(),
            all.len(),
            "the chapter documents {} gates but the detectors define {}",
            documented.len(),
            all.len()
        );
        assert_eq!(documented, all);
    }

    /// The gates `stage` records for a candidate, in the order it recorded them.
    fn recorded_gates(log: &GateLog, stage: GateStage) -> Vec<Gate> {
        log.entries()
            .iter()
            .filter(|outcome| outcome.stage == stage)
            .map(|outcome| outcome.gate)
            .collect()
    }

    /// Every asset the chapter includes, by the name it includes it under.
    const EMBEDDED: [&str; 9] = [
        "gates-order.md",
        "gates-ladder-pass.md",
        "gates-ladder-declined.md",
        "gates-absolute-floor.svg",
        "gates-floors.md",
        "gates-residual.svg",
        "gates-agreement-separated.svg",
        "gates-agreement-oscillating.svg",
        "gates-interval-overlap.svg",
    ];

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

    #[test]
    fn the_declined_ladder_marks_gates_below_the_decline_as_not_run() {
        let series = declined_candidate();
        let (finding, log) = judge(&series);
        let declining = log
            .declined_by_stage(GateStage::ChangePoint)
            .expect("the declined example must not report");

        let rungs = rungs(&log, GateStage::ChangePoint, series.kind);
        let declined: Vec<&Rung> = rungs
            .iter()
            .filter(|rung| rung.verdict == Verdict::Declined)
            .collect();

        assert!(finding.is_none());
        assert_eq!(declined.len(), 1);
        assert_eq!(
            declined.first().map(|rung| rung.gate.as_str()),
            Some(declining.label())
        );
        assert!(
            rungs
                .iter()
                .skip_while(|rung| rung.verdict != Verdict::Declined)
                .skip(1)
                .all(|rung| rung.verdict == Verdict::NotReached),
            "every rung below the decline must be marked not run: {rungs:?}"
        );
    }

    /// The fragment is the chapter's only prose statement of why the candidate fell, so it
    /// has to name the gate the detector recorded rather than the one the example was
    /// built around.
    #[test]
    fn the_declined_fragment_names_the_gate_from_the_log() {
        let series = declined_candidate();
        let (_, log) = judge(&series);
        let declining = log
            .declined_by_stage(GateStage::ChangePoint)
            .expect("the example must not report");

        let rungs = rungs(&log, GateStage::ChangePoint, series.kind);
        let fragment = declined_fragment(&log, series.kind, &rungs);

        assert!(fragment.contains(declining.label()), "{fragment}");
        assert!(fragment.contains("Declined"));
        assert!(fragment.contains("| Gate | Demand | Computed | Verdict |"));
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "evaluates the full scattered passing example through both history detectors and their exact rank tests"
    )]
    fn the_passing_ladder_table_shows_every_gate_passing() {
        let series = passing_candidate();
        let (finding, log) = judge(&series);

        let rungs = rungs(&log, GateStage::ChangePoint, series.kind);
        let table = ladder_table(&rungs);

        assert!(finding.is_some(), "the passing example must report");
        assert_eq!(log.declined_by_stage(GateStage::ChangePoint), None);
        assert!(!rungs.is_empty());
        assert!(
            rungs.iter().all(|rung| rung.verdict == Verdict::Passed),
            "every rung must read as passed: {rungs:?}"
        );
        for rung in rungs {
            assert!(
                table.contains(&format!(
                    "| `{}` | {} | {} | pass |",
                    rung.gate, rung.threshold, rung.value
                )),
                "{table}"
            );
        }
    }

    /// The passing candidate is the chapter's complete-shape ladder, so it has to reach
    /// the end of its detector's sequence rather than merely avoid being declined.
    #[test]
    #[cfg_attr(
        miri,
        ignore = "evaluates the full scattered passing example through both history detectors and their exact rank tests"
    )]
    fn the_passing_ladder_covers_the_whole_change_point_sequence() {
        let series = passing_candidate();
        let (_, log) = judge(&series);

        assert_eq!(
            recorded_gates(&log, GateStage::ChangePoint),
            CHANGE_POINT_GATES.to_vec()
        );
    }

    /// The declared order is what the chapter publishes, so it must be the complete
    /// sequence the detectors actually apply. A prefix would leave optional trailing
    /// gates unchecked against the table.
    #[test]
    #[cfg_attr(
        miri,
        ignore = "evaluates complete change-point, drift and branch fixtures to reach every detector gate"
    )]
    fn every_declared_order_matches_what_the_detector_records() {
        let cases: [(GateStage, GateLog); 3] = [
            (GateStage::ChangePoint, judge(&passing_candidate()).1),
            (GateStage::Drift, judge(&drift_candidate()).1),
            (GateStage::Branch, judge_branch().1),
        ];

        for (stage, log) in cases {
            let recorded = recorded_gates(&log, stage);
            assert_eq!(
                recorded,
                stage_gates(stage),
                "{} applies its gates in a different order than the chapter declares",
                stage.label()
            );
        }
    }

    /// A drift candidate, which reaches every gate the trend detector applies.
    fn drift_candidate() -> Series {
        let values = examples::slow_ramp();
        let series = examples::series("index_build", &values, EXAMPLE_KIND, 0);
        examples::with_intervals(series, reported_half_width(&values))
    }

    /// A branch candidate, which reaches every gate the comparison applies.
    fn judge_branch() -> (Option<Finding>, GateLog) {
        let base = MIN_SERIES_POINTS;
        let levels: Vec<f64> = iter::repeat_n(DECLINED_BASELINE, base)
            .chain(iter::repeat_n(
                examples::clean_step().last().copied().unwrap_or_default(),
                3,
            ))
            .collect();
        let values = examples::scattered(
            &levels,
            examples::TIMING_NOISE_CV,
            examples::seed_of("branch"),
        );
        let series = examples::with_base_window(
            examples::with_intervals(
                examples::series("branch_tip", &values, EXAMPLE_KIND, 0),
                reported_half_width(&values),
            ),
            base.saturating_sub(1),
        );
        let context = examples::branch_context(&series, base.saturating_sub(1));
        let log = evaluate_with_log(&series, &context);
        (log.0, log.1)
    }

    /// The lockstep guard: every threshold the order table states is the policy one,
    /// so a tuned policy rewrites the chapter instead of leaving it behind.
    #[test]
    fn every_threshold_in_the_order_table_is_the_policy_one() {
        let table = order_table();

        for expected in [
            points(DRIFT_MIN_POINTS),
            points(MIN_REGIME),
            commits(MIN_SERIES_POINTS),
            percent(PRACTICAL_RELATIVE),
            percent(BRANCH_PRACTICAL_RELATIVE),
            format!("p < {}", chance(MAX_CHANGE_CHANCE_LEVEL)),
            format!("p < {}", chance(MAX_DRIFT_CHANCE_LEVEL)),
            format!("{}× the typical residual", number(RESIDUAL_NOISE_MULTIPLE)),
            format!("{}× the reported half-width", number(DRIFT_NOISE_MULTIPLE)),
            format!("{}× the reported half-width", number(BRANCH_NOISE_MULTIPLE)),
            share(MIN_REGIME_SEPARATION),
        ] {
            assert!(
                table.contains(&expected),
                "'{expected}' is missing from:\n{table}"
            );
        }
    }

    #[test]
    fn every_gate_appears_in_the_order_table_under_the_stage_that_applies_it() {
        let table = order_table();
        // Parse once rather than scanning the whole table for every gate. Retaining
        // section boundaries also verifies that each gate belongs to the right stage.
        let actual: Vec<_> = table
            .split("\n**`")
            .skip(1)
            .map(|section| {
                let (stage, rows) = section.split_once("`**").unwrap();
                let gates: Vec<_> = rows
                    .lines()
                    .filter_map(|row| row.strip_prefix("| `"))
                    .map(|row| row.split_once("` |").unwrap().0)
                    .collect();
                (stage, gates)
            })
            .collect();
        let expected: Vec<_> = STAGES
            .into_iter()
            .map(|stage| {
                (
                    stage.label(),
                    stage_gates(stage)
                        .iter()
                        .map(|gate| gate.label())
                        .collect::<Vec<_>>(),
                )
            })
            .collect();
        assert_eq!(actual, expected);
    }

    /// The floors table is the other half of the lockstep guard: the numbers it prints are
    /// the numbers the gates compared against, per metric.
    #[test]
    #[cfg_attr(
        miri,
        ignore = "runs the full history detector separately for every metric to derive and verify the floor table"
    )]
    fn every_absolute_floor_is_the_policy_one_for_its_metric() {
        let table = floors_table();

        for kind in MetricKind::ALL {
            let expected = match kind {
                MetricKind::InstructionCount
                | MetricKind::ConditionalBranches
                | MetricKind::IndirectBranches => PRACTICAL_ABSOLUTE_COUNT,
                MetricKind::WallTime | MetricKind::ProcessorTime => PRACTICAL_ABSOLUTE_TIME,
                MetricKind::AllocatedBytes | MetricKind::AllocationCount => {
                    PRACTICAL_ABSOLUTE_ALLOC
                }
            };

            assert!(
                (observed_absolute_floor(kind) - expected).abs() < f64::EPSILON,
                "{} is judged against a floor the policy does not set",
                kind.as_str()
            );
            assert!(
                table.contains(&quantity(kind, expected)),
                "{}'s floor is missing from:\n{table}",
                kind.as_str()
            );
        }
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "derives the floor table by evaluating the full history detector for every metric"
    )]
    fn every_metric_is_listed_with_a_reason() {
        let table = floors_table();

        for kind in MetricKind::ALL {
            assert!(
                table.contains(&format!("| `{}` |", kind.as_str())),
                "{} is missing",
                kind.as_str()
            );
            assert!(table.contains(floor_reason(kind)));
        }
    }

    /// The two grids exist to show the gate's floor doing its work, which they only do if
    /// they land either side of it.
    #[test]
    #[cfg_attr(
        miri,
        ignore = "locates splits with exact permutation tests on both full agreement-grid examples; small pair classifications are tested separately"
    )]
    fn the_agreement_grids_straddle_the_policy_separation_floor() {
        let floor = MIN_REGIME_SEPARATION;

        let separated = agreement("separated", &examples::clean_step()).share();
        let oscillating = agreement("oscillating", &examples::overlapping_regimes()).share();

        assert!(
            separated >= floor,
            "the step must clear the separation floor, got {separated}"
        );
        assert!(
            oscillating < floor,
            "the oscillating series must fall short of it, got {oscillating}"
        );
    }

    /// The grid's caption is where the two numbers the gate compared appear, so they have
    /// to be the numbers it really compared.
    #[test]
    #[cfg_attr(
        miri,
        ignore = "renders both complete agreement grids and repeats their exact permutation split searches"
    )]
    fn each_agreement_grid_states_its_own_share_and_the_policy_floor() {
        let floor = MIN_REGIME_SEPARATION;
        let assets = agreement_grids();

        for (path, values) in [
            ("gates-agreement-separated.svg", examples::clean_step()),
            (
                "gates-agreement-oscillating.svg",
                examples::overlapping_regimes(),
            ),
        ] {
            let svg = &assets
                .iter()
                .find(|asset| asset.path == path)
                .unwrap()
                .content;
            let drawn = agreement("", &values).share();

            assert!(svg.contains(&share(drawn)), "{path} omits its own share");
            assert!(svg.contains(&share(floor)), "{path} omits the floor");
        }
    }

    /// The grid is only evidence if it is drawing the same quantity the gate judged.
    #[test]
    #[cfg_attr(
        miri,
        ignore = "repeats an exact permutation split search on the full oscillating agreement-grid example"
    )]
    fn the_oscillating_grid_draws_the_share_the_detector_computed() {
        let values = examples::overlapping_regimes();
        let series = examples::series("bimodal", &values, EXAMPLE_KIND, 0);
        let (_, log) = judge(&series);
        let judged = outcome_of(&log, GateStage::ChangePoint, Gate::RegimeSeparation)
            .and_then(|outcome| outcome.value)
            .expect("the oscillating example is declined by the separation gate");

        let drawn = agreement("oscillating", &values).share();

        assert!(
            (drawn - judged).abs() < f64::EPSILON,
            "the grid draws {drawn} where the gate judged {judged}"
        );
    }

    #[test]
    fn the_oscillating_series_is_declined_by_the_separation_gate() {
        let values = examples::overlapping_regimes();
        let series = examples::series("bimodal", &values, EXAMPLE_KIND, 0);

        let (_, log) = judge(&series);

        assert_eq!(
            log.declined_by_stage(GateStage::ChangePoint),
            Some(Gate::RegimeSeparation)
        );
    }

    /// The figure claims an engine that cannot separate the two levels, so the gate has to
    /// agree.
    #[test]
    fn the_overlapping_intervals_are_declined_by_the_interval_gate() {
        let values = examples::clean_step();
        let split = split_of(&values);
        let before = cbh_stats::median(values.get(..split).expect("the split lies inside"))
            .expect("the earlier regime holds points");
        let after = cbh_stats::median(values.get(split..).expect("the split lies inside"))
            .expect("the later regime holds points");
        let series = examples::with_intervals(
            examples::series("tokenize", &values, EXAMPLE_KIND, 0),
            (after - before).abs() * OVERLAPPING_INTERVAL_SHARE,
        );

        let (_, log) = judge(&series);

        assert_eq!(
            log.declined_by_stage(GateStage::ChangePoint),
            Some(Gate::IntervalDisjoint)
        );
    }

    /// The scale figure's whole claim is that the same proportional move is declined at one
    /// magnitude and reported at the other.
    #[test]
    #[cfg_attr(
        miri,
        ignore = "evaluates both full scale-figure histories, including the exact rank test for the reported step"
    )]
    fn the_small_move_is_declined_by_the_absolute_floor_and_the_large_one_reported() {
        let regime = MIN_REGIME.saturating_mul(2);
        let small: Vec<f64> = iter::repeat_n(SMALL_BASELINE, regime)
            .chain(iter::repeat_n(SMALL_ELEVATED, regime))
            .collect();
        let large: Vec<f64> = small.iter().map(|value| value * LARGE_MAGNITUDE).collect();

        let (small_finding, small_log) =
            judge(&examples::series("checksum_byte", &small, EXAMPLE_KIND, 0));
        let (large_finding, _) = judge(&examples::series("checksum_page", &large, EXAMPLE_KIND, 0));

        assert!(small_finding.is_none());
        assert_eq!(
            small_log.declined_by_stage(GateStage::ChangePoint),
            Some(Gate::AbsoluteFloor)
        );
        assert!(large_finding.is_some(), "the larger benchmark must report");
    }

    /// The strip's lesson is that the move sits inside the band, which is a property of the
    /// data rather than of the drawing.
    #[test]
    fn the_residual_strip_draws_a_move_inside_the_band() {
        let series = declined_candidate();
        let (_, log) = judge(&series);
        let outcome =
            outcome_of(&log, GateStage::ChangePoint, Gate::ResidualNoise).expect("recorded");

        let (Some(move_size), Some(band)) = (outcome.value, outcome.threshold) else {
            panic!("the residual gate compares two numbers");
        };

        assert!(move_size < band, "{move_size} is not inside {band}");
    }

    /// A gate with nothing to measure must not acquire a number on the way to the page.
    #[test]
    fn a_boolean_gate_reads_as_held_rather_than_as_a_number() {
        // Rendering a boolean outcome does not require discovering a split first.
        let outcome = GateOutcome {
            stage: GateStage::ChangePoint,
            gate: Gate::SplitLocated,
            value: None,
            threshold: None,
            passed: true,
        };

        let (value, threshold) = reading_of(&outcome, EXAMPLE_KIND);

        assert_eq!(value, "held");
        assert_eq!(threshold, "must hold");
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
