//! Figures and tables for the Selection and Reconstruction chapters.
//!
//! Both stages are reductions, and neither leaves a trace in a finished report: selection
//! narrows a store to the objects an analysis may read, and reconstruction folds those
//! objects into series while removing more data again. A reader who cannot see what was
//! taken out has no way to tell a quiet report from an empty one, so every figure here
//! draws the operation acting on data rather than its result alone.
//!
//! The selection assets share one worked store. The funnel figure and the accounting
//! table beside it are both derived from it, so the picture cannot show one store while
//! the arithmetic describes another, and the counts in the prose are the counts the
//! filters produced.

use std::fmt::Write as _;

use cbh_analyze::auto_mode;
use cbh_detect::select_commits;
use cbh_model::MetricKind;

use crate::assets::Asset;
use crate::styles::occupancy::{Cell, Occupancy};
use crate::styles::operation::Operation;
use crate::styles::plot::{Mark, Observation, Plot};
use crate::theme;

/// Every asset the Selection and Reconstruction chapters embed.
#[must_use]
pub fn assets() -> Vec<Asset> {
    let mut assets = vec![
        Asset::new("selection-funnel.svg", funnel_figure()),
        Asset::new("selection-funnel.md", funnel_table()),
        Asset::new("selection-mode-table.md", mode_table()),
    ];
    assets.extend(
        operations()
            .into_iter()
            .map(|(path, panes)| Asset::new(path, panes.render())),
    );
    assets
}

/// Every before/after figure the two chapters embed.
///
/// Kept as panes rather than as rendered assets so a test can assert that each operation
/// left the data it acted on visibly changed. A two-pane figure whose panes are identical
/// teaches nothing, and the failure is invisible in the rendered markup.
fn operations() -> Vec<(&'static str, Panes)> {
    vec![
        ("selection-dirty.svg", dirty_admission()),
        ("reconstruction-fold.svg", fold()),
        ("reconstruction-gap.svg", gap()),
        ("reconstruction-ghost.svg", ghost()),
        ("reconstruction-blessing.svg", blessing()),
    ]
}

/// A two-pane figure held as its parts.
///
/// [`Operation`] renders its panes into one SVG and keeps nothing, which leaves no way to
/// ask whether the operation changed anything. Carrying the panes to the point of
/// rendering keeps that question answerable.
struct Panes {
    /// What the operation does, shown above both panes.
    title: &'static str,

    /// The state going in.
    before: Plot,

    /// The state coming out.
    after: Plot,
}

impl Panes {
    /// Renders the pair as one figure.
    fn render(&self) -> String {
        Operation::new(self.title, self.before.clone(), self.after.clone()).render()
    }

    /// Whether the operation left the data it acted on visibly changed.
    ///
    /// Compared on the drawn geometry rather than on the whole rendering: the two panes
    /// carry different captions by construction, so a comparison including the text would
    /// hold for a pair drawing exactly the same thing.
    #[cfg(test)]
    fn changes_the_data(&self) -> bool {
        geometry(&self.before.render()) != geometry(&self.after.render())
    }
}

/// The rendered figure with its text elements removed.
#[cfg(test)]
fn geometry(svg: &str) -> String {
    let mut remaining = svg;
    let mut kept = String::new();
    while let Some((before, after)) = remaining.split_once("<text") {
        kept.push_str(before);
        remaining = after.split_once("</text>").map_or("", |(_, tail)| tail);
    }
    kept.push_str(remaining);
    kept
}

// ---------------------------------------------------------------------------------
// Shared worked data
// ---------------------------------------------------------------------------------

/// The level the worked example series sit at.
///
/// A round number, so a reader converting a value to a percentage of baseline while
/// reading a figure can do it without arithmetic.
const LEVEL: f64 = 100.0;

/// The measurement scatter every worked series carries, as a fraction of its own level,
/// cycled along the commit line.
///
/// A fixed short cycle rather than a random draw: the figures must render identically on
/// every platform and every run, because the freshness check compares the rendered bytes
/// against the copies the book includes. Expressed relatively so a series drawn at any
/// level carries the same visible scatter.
const SCATTER: [f64; 5] = [0.0, 0.014, -0.009, 0.006, -0.012];

/// `levels` carrying the scatter a real measurement of them would.
fn scattered(levels: &[f64]) -> Vec<f64> {
    levels
        .iter()
        .zip(SCATTER.iter().copied().cycle())
        .map(|(level, offset)| level * (1.0 + offset))
        .collect()
}

/// A settled series of `span` measurements of one level.
fn flat(level: f64, span: usize) -> Vec<f64> {
    scattered(&vec![level; span])
}

/// `count` rendered as a plural-correct number of runs.
fn runs(count: usize) -> String {
    if count == 1 {
        "1 run".to_owned()
    } else {
        format!("{count} runs")
    }
}

/// `count` rendered as a plural-correct number of dirty runs.
fn dirty_runs(count: usize) -> String {
    if count == 1 {
        "1 dirty run".to_owned()
    } else {
        format!("{count} dirty runs")
    }
}

// ---------------------------------------------------------------------------------
// Selection: the funnel
// ---------------------------------------------------------------------------------

/// How many commits of first-parent history the worked selection example covers.
///
/// Long enough to hold a `--since` cutoff, a merge base and a stretch of branch work with
/// room between them, and short enough that a reader can count the columns of the grid.
const SPAN: usize = 24;

/// The position of the merge base on that line: at or before it is base-side, after it is
/// target-side.
const MERGE_BASE: usize = 19;

/// The oldest commit position inside the worked `--since` window.
const SINCE_CUTOFF: usize = 6;

/// The commits at which the worked store's main partition holds nothing, a collection
/// outage of the kind every real store has.
const OUTAGE: [usize; 2] = [11, 12];

/// The commits at which that partition's run came from a dirty working tree, one on each
/// side of the merge base — which is what makes the partition worth drawing at all.
const DIRTY_RUNS: [usize; 2] = [16, 22];

/// How many commits apart the worked store's second partition was measured.
///
/// A benchmark suite too slow to run per commit is the usual reason a partition is sparse,
/// and a sparse partition is what makes the `--since` stage's count differ between rows.
const NIGHTLY_CADENCE: usize = 3;

/// The commit at which that second partition's run came from a dirty working tree.
const NIGHTLY_DIRTY: usize = 23;

/// The oldest commit the worked store's third partition reaches, the machine having been
/// added to the pool part-way through the analyzed history.
const FOREIGN_START: usize = 4;

/// How many of the main partition's runs sit at commits that are not on the analyzed
/// first-parent line — measured on branches that were later merged in.
const OFF_LINE_RUNS: usize = 3;

/// What one commit holds for one discriminant set.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Holding {
    /// No run was stored.
    Nothing,

    /// A run measured from a clean working tree.
    Clean,

    /// A run measured from a working tree carrying uncommitted changes.
    Dirty,
}

/// The stage of the funnel that removed one object.
///
/// Selection is a sequence of filters, and an object removed by the first of them is never
/// offered to the rest. Naming the stage rather than counting removals per filter is what
/// lets the same classification drive both the grid and the accounting table, and keeps an
/// object from being counted twice.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RemovedBy {
    /// The query's discriminant filters do not name the object's discriminant set.
    Discriminants,

    /// The object was recorded at a commit that is not on the analyzed first-parent line.
    OffHistory,

    /// The object is a dirty run at or before the merge base.
    DirtyAdmission,

    /// The object is older than the `--since` cutoff.
    Since,
}

impl RemovedBy {
    /// The funnel stages in the order the loader applies them.
    ///
    /// Ref: `packages/cbh_analyze/src/dataset.rs`, whose first pass tests discriminant
    /// filters, then first-parent membership, then dirty admission, then the window.
    const IN_ORDER: [Self; 4] = [
        Self::Discriminants,
        Self::OffHistory,
        Self::DirtyAdmission,
        Self::Since,
    ];

    /// How the chapter names the stage.
    fn name(self) -> &'static str {
        match self {
            Self::Discriminants => "Discriminant filter",
            Self::OffHistory => "On the analyzed history",
            Self::DirtyAdmission => "Dirty admission",
            Self::Since => "`--since`",
        }
    }
}

/// One discriminant set in the worked store, and what it holds along the analyzed line.
struct Partition {
    /// How the appendix names the partition, abbreviating `engine / target triple /
    /// machine key` to what fits a row label.
    label: &'static str,

    /// Whether the query's discriminant filters name this partition.
    matches_discriminants: bool,

    /// What the store holds at each commit position on the analyzed line, oldest first.
    holdings: Vec<Holding>,

    /// How many runs this partition holds at commits off that line. They have no column
    /// in the figure, because having no place on the analyzed line is exactly why the
    /// on-history stage removes them.
    off_line: usize,
}

impl Partition {
    /// Every object this partition holds, on the analyzed line and off it.
    fn objects(&self) -> usize {
        self.on_line().count().saturating_add(self.off_line)
    }

    /// What the partition holds at each commit that holds anything.
    fn on_line(&self) -> impl Iterator<Item = (usize, Holding)> + '_ {
        self.holdings
            .iter()
            .copied()
            .enumerate()
            .filter(|&(_, holding)| holding != Holding::Nothing)
    }

    /// Which stage removes the object at `position`, or `None` where it survives to be
    /// fetched.
    ///
    /// Ordered as the loader orders its filters, so an object is attributed to the first
    /// stage that takes it out and never to a later one that would also have.
    fn removed_by(&self, position: usize, holding: Holding) -> Option<RemovedBy> {
        if !self.matches_discriminants {
            return Some(RemovedBy::Discriminants);
        }
        if holding == Holding::Dirty && position <= MERGE_BASE {
            return Some(RemovedBy::DirtyAdmission);
        }
        if position < SINCE_CUTOFF {
            return Some(RemovedBy::Since);
        }
        None
    }

    /// How many of this partition's objects `stage` removes.
    fn removals(&self, stage: RemovedBy) -> usize {
        if !self.matches_discriminants {
            return if stage == RemovedBy::Discriminants {
                self.objects()
            } else {
                0
            };
        }
        if stage == RemovedBy::OffHistory {
            return self.off_line;
        }
        self.on_line()
            .filter(|&(position, holding)| self.removed_by(position, holding) == Some(stage))
            .count()
    }

    /// How many of this partition's objects survive every stage.
    fn survivors(&self) -> usize {
        self.on_line()
            .filter(|&(position, holding)| self.removed_by(position, holding).is_none())
            .count()
    }

    /// The partition's row of the occupancy grid.
    fn cells(&self) -> Vec<Cell> {
        self.holdings
            .iter()
            .copied()
            .enumerate()
            .map(|(position, holding)| match holding {
                Holding::Nothing => Cell::Absent,
                Holding::Clean if self.removed_by(position, holding).is_none() => Cell::Clean,
                Holding::Dirty if self.removed_by(position, holding).is_none() => Cell::Dirty,
                Holding::Clean | Holding::Dirty => Cell::Excluded,
            })
            .collect()
    }
}

/// The worked store the selection assets are all derived from.
///
/// Three partitions, chosen so that each stage of the funnel has something to remove: a
/// dense per-commit partition carrying dirty runs on both sides of the merge base, a
/// sparse one the discriminant filters also name, and one measured on another machine that the
/// filters do not.
fn store() -> Vec<Partition> {
    let dense = (0..SPAN)
        .map(|position| {
            if OUTAGE.contains(&position) {
                Holding::Nothing
            } else if DIRTY_RUNS.contains(&position) {
                Holding::Dirty
            } else {
                Holding::Clean
            }
        })
        .collect();

    let nightly_commits: Vec<usize> = (0..SPAN).step_by(NIGHTLY_CADENCE).collect();
    let nightly = (0..SPAN)
        .map(|position| {
            if position == NIGHTLY_DIRTY {
                Holding::Dirty
            } else if nightly_commits.contains(&position) {
                Holding::Clean
            } else {
                Holding::Nothing
            }
        })
        .collect();

    let foreign = (0..SPAN)
        .map(|position| {
            if position < FOREIGN_START {
                Holding::Nothing
            } else {
                Holding::Clean
            }
        })
        .collect();

    vec![
        Partition {
            label: "criterion / linux-x64 / a1b2",
            matches_discriminants: true,
            holdings: dense,
            off_line: OFF_LINE_RUNS,
        },
        Partition {
            label: "callgrind / linux-x64 / a1b2",
            matches_discriminants: true,
            holdings: nightly,
            off_line: 0,
        },
        Partition {
            label: "criterion / macos-arm64 / 7c6d",
            matches_discriminants: false,
            holdings: foreign,
            off_line: 0,
        },
    ]
}

/// Every run object the worked store holds.
fn candidates(store: &[Partition]) -> usize {
    store.iter().map(Partition::objects).sum()
}

/// How many objects `stage` removes from the worked store.
fn removals(store: &[Partition], stage: RemovedBy) -> usize {
    store
        .iter()
        .map(|partition| partition.removals(stage))
        .sum()
}

/// Every run object the worked store hands on to be fetched and parsed.
fn survivors(store: &[Partition]) -> usize {
    store
        .iter()
        .filter(|partition| partition.matches_discriminants)
        .map(Partition::survivors)
        .sum()
}

/// The store with the objects the query kept set apart from the objects it excluded.
///
/// Every exclusion is drawn the same way; which stage made it is the accounting table's
/// job, and a grid that shaded each exclusion stage differently would be a legend to
/// decode rather than a picture to read.
fn funnel_figure() -> String {
    store()
        .iter()
        .fold(
            Occupancy::new("one store, one query: what survived and what did not"),
            |grid, partition| grid.row(partition.label, partition.cells()),
        )
        .render()
}

/// The same accounting as a table, with a running count of what is still eligible.
fn funnel_table() -> String {
    let store = store();
    let candidates = candidates(&store);

    let mut markdown = String::from(
        "The funnel below traces one worked query — a host-local `analyze` — against this store. \
         Two of its inputs drive the removals that would otherwise look arbitrary: the \
         **discriminant filters** resolve to this host's own partitions, so a foreign partition \
         measured on another machine is dropped; and **`--since`** resolves to a cutoff date part \
         way through the history, dropping everything committed before it. Each row is one \
         selection stage and what it removed.\n\n",
    );
    markdown.push_str(
        "| Stage | What it removes from this store | Objects removed | Still eligible |\n\
         |---|---|--:|--:|\n",
    );
    writeln!(
        markdown,
        "| Every run object in this store | | | {candidates} |"
    )
    .expect("writing to a String never fails");

    let mut eligible = candidates;
    for stage in RemovedBy::IN_ORDER {
        let removed = removals(&store, stage);
        eligible = eligible.saturating_sub(removed);
        writeln!(
            markdown,
            "| {} | {} | {removed} | {eligible} |",
            stage.name(),
            describe_removal(&store, stage),
        )
        .expect("writing to a String never fails");
    }
    writeln!(
        markdown,
        "| Fetched and parsed | | | {} |",
        survivors(&store)
    )
    .expect("writing to a String never fails");

    writeln!(
        markdown,
        "\n{} removed and {} account for all {candidates} run objects this store held. Only \
         those survivors are fetched and parsed; every other run was decided on its \
         storage key and the commit's place in the topology alone.",
        runs(candidates.saturating_sub(survivors(&store))),
        runs(survivors(&store)),
    )
    .expect("writing to a String never fails");
    markdown.push_str(
        "\nThe store in this worked example holds only **runs** — stored benchmark measurements \
         (see [Shape of the data](shape.md#what-a-stored-run-holds)). A **blessing**, a recorded \
         acceptance of a change (see [Reconstruction](reconstruction.md#blessings)), is a \
         separate object kind stored alongside them: it is set apart during discriminant \
         selection and follows its own path, so it never enters the run-only topology, \
         dirty-admission, and window stages counted here.\n",
    );
    markdown.push_str(
        "\nThe grid draws the analyzed first-parent line, so the runs the on-history stage \
         removed have no column in it — having no place on that line is exactly why they \
         were removed. Partitions are labeled by engine, target triple, and machine key, \
         with the triples shortened to fit.\n",
    );
    markdown
}

/// What `stage` took out of the worked store, in the reader's terms.
fn describe_removal(store: &[Partition], stage: RemovedBy) -> String {
    match stage {
        RemovedBy::Discriminants => {
            let excluded: Vec<&str> = store
                .iter()
                .filter(|partition| !partition.matches_discriminants)
                .map(|partition| partition.label)
                .collect();
            format!(
                "every object of `{}`, a partition the query's discriminant filters do not name",
                excluded.join("`, `"),
            )
        }
        RemovedBy::OffHistory => {
            "runs recorded at commits that are not on the context's first-parent line".to_owned()
        }
        RemovedBy::DirtyAdmission => format!(
            "{}, recorded at or before the merge base (commit {MERGE_BASE})",
            dirty_runs(removals(store, stage)),
        ),
        RemovedBy::Since => {
            format!("everything still eligible that is older than commit {SINCE_CUTOFF}")
        }
    }
}

// ---------------------------------------------------------------------------------
// Selection: mode and dirty admission
// ---------------------------------------------------------------------------------

/// The mode truth table: every combination of the two signals, and what it selects.
///
/// Calls the analysis crate's own `auto_mode`, so the table is derived from the rule rather
/// than transcribed beside it and cannot drift from it.
fn mode_table() -> String {
    let mut markdown = String::from(
        "| Context commit equals the merge base | Dirty run admitted on the context commit | \
         Mode |\n|---|---|---|\n",
    );
    for context_is_merge_base in [true, false] {
        for dirty_run_on_context in [false, true] {
            writeln!(
                markdown,
                "| {} | {} | `{}` |",
                yes_no(context_is_merge_base),
                yes_no(dirty_run_on_context),
                auto_mode(context_is_merge_base, dirty_run_on_context).as_str(),
            )
            .expect("writing to a String never fails");
        }
    }
    markdown
}

/// A signal rendered as the table reads it.
fn yes_no(signal: bool) -> &'static str {
    if signal { "yes" } else { "no" }
}

/// How far above the settled level a dirty run in the admission figure sits.
///
/// Somebody's uncommitted experiment is a different thing from the committed history
/// around it, and a dirty run drawn on the same level as its neighbours would leave the
/// figure showing two marks in search of a difference.
const DIRTY_EXCURSION: f64 = 1.15;

/// Where the admission figure's dirty runs sit: one ordinary base-side commit, and
/// the analyzed context commit. Official view treats every commit as base-side, so
/// only the context commit can take the dirty-tree exception.
const ADMISSION_DIRTY: [usize; 2] = [12, SPAN - 1];

/// How far above the excluded dirty run its label is placed.
///
/// The label belongs to the point but must clear the strike-through mark, so it is lifted
/// slightly without moving into the chart title.
const DIRTY_EXCLUDED_NOTE_OFFSET: f64 = 1.03;

/// Every stored run in the admission figure, and whether it came from a dirty tree.
///
/// Ordered as reconstruction orders points — by commit position, then clean before dirty
/// — because a dirty run is an extra object at a commit that already carries its
/// committed measurement, not a replacement for one. Ordering the panes' observations
/// that way is also what keeps the connecting line running forward along the history
/// rather than doubling back to reach a run appended out of place.
fn admission_runs() -> Vec<(Observation, bool)> {
    let level = flat(LEVEL, SPAN);
    let mut runs = Vec::new();
    for (position, value) in level.into_iter().enumerate() {
        runs.push((Observation::new(position, value), false));
        if ADMISSION_DIRTY.contains(&position) {
            runs.push((Observation::new(position, value * DIRTY_EXCURSION), true));
        }
    }
    runs
}

/// Official-view admission: the merge base is the context commit, so only a dirty
/// run at that commit is admitted, and only because the working tree is currently
/// dirty.
fn dirty_admission() -> Panes {
    let runs = admission_runs();
    let base_side_dirty_position = ADMISSION_DIRTY[0];
    let base_side_dirty_value = runs
        .iter()
        .find_map(|&(observation, dirty)| {
            (dirty && observation.position == base_side_dirty_position).then_some(observation.value)
        })
        .expect("the admission figure has one base-side dirty run");
    let ancestry: Vec<String> = (0..SPAN).map(|position| format!("{position}")).collect();
    let context_commit = ancestry
        .last()
        .expect("the admission figure covers more than one commit")
        .clone();
    // Official view: merge base is the context commit. The working tree is dirty, so
    // that commit's dirty observation is the in-flight exception [`select_commits`]
    // already encodes.
    let selected = select_commits(&ancestry, Some(context_commit.as_str()), true, true);

    let before = Plot::new("every stored run", SPAN)
        .value_label("ns")
        .base_color(theme::ALTERNATE)
        .observations(runs.iter().map(|&(observation, dirty)| {
            if dirty {
                observation.marked(Mark::Focus)
            } else {
                observation
            }
        }))
        .split(SPAN.saturating_sub(1), "analyzed context commit");

    let after = Plot::new("admitted for this analysis", SPAN)
        .value_label("ns")
        .base_color(theme::ALTERNATE)
        .observations(runs.iter().map(|&(observation, dirty)| {
            if !dirty {
                return observation;
            }
            let admits = selected
                .get(observation.position)
                .is_some_and(|commit| commit.dirty.admits_dirty());
            if admits {
                observation.marked(Mark::Focus)
            } else {
                observation.marked(Mark::Removed)
            }
        }))
        .split(SPAN.saturating_sub(1), "analyzed context commit")
        .note(
            base_side_dirty_position,
            base_side_dirty_value * DIRTY_EXCLUDED_NOTE_OFFSET,
            "dirty, base-side → excluded",
            theme::REGRESSION,
        );

    Panes {
        title: "a dirty run at the analyzed context commit is admitted while the tree is \
                dirty; earlier base-side dirty runs are not",
        before,
        after,
    }
}

// ---------------------------------------------------------------------------------
// Reconstruction
// ---------------------------------------------------------------------------------

/// How many commits of history the reconstruction figures cover.
const RECONSTRUCTION_SPAN: usize = 16;

/// The analyzed commit in those figures: the context commit whose presence or
/// absence decides what survives.
const RECONSTRUCTION_TIP: usize = 15;

/// The levels the three series of one Callgrind benchmark sit at in the fold figure.
///
/// The three share one value axis, so they are placed within one order of magnitude of
/// each other and far enough apart to read as three series. A realistic
/// instruction-to-branch ratio would push two of them onto the axis and leave the figure
/// showing one series and a smear.
const FOLD_LEVELS: [f64; 3] = [1240.0, 812.0, 496.0];

/// The metric kinds one Callgrind benchmark reports, each with its worked series.
fn fold_series() -> Vec<(MetricKind, Vec<f64>)> {
    [
        MetricKind::InstructionCount,
        MetricKind::ConditionalBranches,
        MetricKind::IndirectBranches,
    ]
    .into_iter()
    .zip(FOLD_LEVELS)
    .map(|(kind, level)| (kind, flat(level, RECONSTRUCTION_SPAN)))
    .collect()
}

/// One run's several metrics becoming points in as many distinct series.
fn fold() -> Panes {
    let series = fold_series();

    let contributed: Vec<Observation> = series
        .iter()
        .filter_map(|(_, values)| values.last().copied())
        .map(|value| Observation::new(RECONSTRUCTION_TIP, value).marked(Mark::Focus))
        .collect();

    let before = Plot::new("one stored run at the analyzed commit", RECONSTRUCTION_SPAN)
        .value_label("count")
        .scattered()
        .band(
            RECONSTRUCTION_TIP,
            RECONSTRUCTION_TIP,
            "one run, three metrics",
            theme::HIGHLIGHT,
        )
        .observations(contributed.iter().copied());

    // Drawn as points rather than as connected series: the pane carries three series on
    // one axis, and a plot draws a single line through all of its observations, so
    // connecting them would join the end of one series to the start of the next. The
    // levels and their labels are what identify the three, which is what the pane is
    // about.
    let mut after = Plot::new("three series, one per metric kind", RECONSTRUCTION_SPAN)
        .value_label("count")
        .scattered()
        .base_color(theme::ALTERNATE);
    for (kind, values) in &series {
        let last = values.len().saturating_sub(1);
        after = after
            .observations(values.iter().copied().enumerate().map(|(position, value)| {
                let observation = Observation::new(position, value);
                if position == last {
                    observation.marked(Mark::Focus)
                } else {
                    observation
                }
            }))
            .note(
                0,
                values.first().copied().unwrap_or_default() * FOLD_NOTE_OFFSET,
                kind.as_str(),
                theme::ALTERNATE,
            );
    }

    Panes {
        title: "one run's metrics become points in as many series",
        before,
        after,
    }
}

/// How far above its own series a fold-figure label is pinned.
///
/// Far enough that the text clears the points, close enough that it still reads as
/// belonging to that series rather than to the one above.
const FOLD_NOTE_OFFSET: f64 = 1.06;

/// How many observations the gap figure carries.
const GAP_OBSERVATIONS: usize = 10;

/// How many commits the gap figure spans.
const GAP_SPAN: usize = 20;

/// Where those observations actually sit in the topology: two clusters and a lone pair,
/// which is the shape a suite benchmarked on some commits and not others produces.
const GAP_POSITIONS: [usize; GAP_OBSERVATIONS] = [0, 1, 2, 3, 4, 11, 12, 13, 18, 19];

/// How much the series rises per commit. A steady climb means the history holds a straight line
/// a reader can follow through the gaps, while the detector — which sees only the sequence —
/// meets it as uneven steps wherever a gap hid the commits in between.
const GAP_SLOPE: f64 = 3.0;

/// The series as the history holds it (against topology, gaps to scale), then as the detector
/// receives it (a gapless sequence).
fn gap() -> Panes {
    // Value rises linearly with the commit *position*, so the true series is a straight line and
    // a reader can infer it from the topology pane even though most commits hold nothing.
    let values: Vec<f64> = GAP_POSITIONS
        .iter()
        .map(|&position| LEVEL + GAP_SLOPE * crate::coord::of(position))
        .collect();

    // History first: each observation at the commit it was measured at, gaps drawn to scale.
    let before = Plot::new("what the history contains: one column per commit", GAP_SPAN)
        .value_label("ns")
        .observations(
            GAP_POSITIONS
                .iter()
                .copied()
                .zip(values.iter().copied())
                .map(|(position, value)| Observation::new(position, value)),
        );

    // Then the detector's input: the same values as a bare sequence, with no notion of a commit
    // it holds no observation for. The straight climb now reads as uneven steps, because the gaps
    // it cannot see are exactly where the biggest jumps happened.
    let after = Plot::new(
        "what the detector receives: a gapless sequence",
        GAP_OBSERVATIONS,
    )
    .value_label("ns")
    .values(&values);

    Panes {
        title: "the same series as the history holds it, then as the detector receives it",
        before,
        after,
    }
}

/// The last commit at which the ghost figure's benchmark was measured, leaving the
/// analyzed context commit without an observation.
const GHOST_LAST_MEASURED: usize = 11;

/// A benchmark absent at the analyzed commit, losing every one of its series.
///
/// Drawn as points for the same reason as the fold figure: three series share one axis,
/// and one plot draws one line.
fn ghost() -> Panes {
    let series = fold_series();
    let measured = GHOST_LAST_MEASURED.saturating_add(1);

    let mut before = Plot::new(
        "a benchmark that stopped being measured",
        RECONSTRUCTION_SPAN,
    )
    .value_label("count")
    .scattered()
    .base_color(theme::ALTERNATE)
    .band(
        RECONSTRUCTION_TIP,
        RECONSTRUCTION_TIP,
        "analyzed commit",
        theme::HIGHLIGHT,
    );
    let mut after = Plot::new("every one of its series is dropped", RECONSTRUCTION_SPAN)
        .value_label("count")
        .scattered()
        .base_color(theme::ALTERNATE)
        .band(
            RECONSTRUCTION_TIP,
            RECONSTRUCTION_TIP,
            "analyzed commit",
            theme::HIGHLIGHT,
        );

    for (_, values) in &series {
        let points: Vec<Observation> = values
            .iter()
            .copied()
            .take(measured)
            .enumerate()
            .map(|(position, value)| Observation::new(position, value))
            .collect();
        before = before.observations(points.iter().copied());
        after = after.observations(points.iter().map(|point| point.marked(Mark::Removed)));
    }

    Panes {
        title: "no observation at the analyzed commit drops the whole benchmark",
        before,
        after,
    }
}

/// How many commits the blessing figure spans.
const BLESSING_SPAN: usize = 24;

/// The commit the blessing re-baselines the series at.
const BLESSED_AT: usize = 8;

/// The level the blessed series settled at after the accepted change.
const BLESSED_LEVEL: f64 = 132.0;

/// A blessing narrowing what detection judges, without narrowing what is drawn.
fn blessing() -> Panes {
    let levels: Vec<f64> = (0..BLESSING_SPAN)
        .map(|position| {
            if position < BLESSED_AT {
                LEVEL
            } else {
                BLESSED_LEVEL
            }
        })
        .collect();
    let values = scattered(&levels);

    let before = Plot::new("the series as reconstructed", BLESSING_SPAN)
        .value_label("ns")
        .values(&values);

    // The points before the blessing keep their ordinary role rather than being struck
    // out: a blessing removes them from what is judged, not from what is stored, and the
    // chart is still drawn from the whole series.
    let after = Plot::new("what detection judges", BLESSING_SPAN)
        .value_label("ns")
        .values(&values)
        .band(
            0,
            BLESSED_AT.saturating_sub(1),
            "no longer judged",
            theme::MUTED,
        )
        .split(BLESSED_AT, "blessing");

    Panes {
        title: "a blessing re-baselines detection; the chart still shows the whole series",
        before,
        after,
    }
}

#[cfg(test)]
mod tests {
    use cbh_detect::AnalysisMode;

    use super::*;

    #[test]
    #[cfg_attr(
        miri,
        ignore = "plotters SVG generation is host graphics, not memory-safety-relevant, and exceeds the Miri CI budget"
    )]
    fn every_documented_asset_is_produced() {
        let paths: Vec<String> = assets().into_iter().map(|asset| asset.path).collect();

        for expected in [
            "selection-funnel.svg",
            "selection-funnel.md",
            "selection-mode-table.md",
            "selection-dirty.svg",
            "reconstruction-fold.svg",
            "reconstruction-gap.svg",
            "reconstruction-ghost.svg",
            "reconstruction-blessing.svg",
        ] {
            assert!(
                paths.iter().any(|path| path == expected),
                "{expected} missing"
            );
        }
    }

    /// The funnel's whole claim is that it accounts for every candidate. A stage that
    /// double-counts an object, or one that quietly drops it, would leave the table's
    /// arithmetic wrong while every individual number still looked plausible.
    #[test]
    fn the_funnel_accounts_for_every_candidate() {
        let store = store();

        let removed: usize = RemovedBy::IN_ORDER
            .into_iter()
            .map(|stage| removals(&store, stage))
            .sum();

        assert_eq!(
            removed.saturating_add(survivors(&store)),
            candidates(&store),
            "exclusions plus survivors must equal the candidates the store held"
        );
    }

    #[test]
    fn every_stage_of_the_funnel_removes_something() {
        let store = store();

        for stage in RemovedBy::IN_ORDER {
            assert!(
                removals(&store, stage) > 0,
                "{stage:?} removes nothing, so the figure does not demonstrate it"
            );
        }
    }

    /// The grid and the table are two renderings of one accounting, embedded next to each
    /// other. Held to each other rather than each to the model, so a figure that stops
    /// agreeing with the sentence beside it fails here.
    #[test]
    fn the_funnel_hands_on_the_objects_the_grid_shows_as_kept() {
        let store = store();

        let kept = store
            .iter()
            .flat_map(Partition::cells)
            .filter(|cell| matches!(*cell, Cell::Clean | Cell::Dirty))
            .count();

        let stated = funnel_table()
            .lines()
            .find_map(|line| line.strip_prefix("| Fetched and parsed | | | "))
            .and_then(|tail| tail.trim_end_matches(" |").parse::<usize>().ok())
            .expect("the table closes with what selection hands on");

        assert_eq!(kept, stated, "the grid and the table must agree");
    }

    /// Official view admits a dirty run only at the context commit, and only while the
    /// tree is dirty. An absolute "never at or before the merge base" would hide that
    /// exception.
    #[test]
    fn the_admission_figure_admits_only_the_dirty_context_exception() {
        let ancestry: Vec<String> = (0..SPAN).map(|position| format!("{position}")).collect();
        let context_commit = ancestry
            .last()
            .expect("the admission figure covers more than one commit")
            .clone();
        let selected = select_commits(&ancestry, Some(context_commit.as_str()), true, true);

        let mid = ADMISSION_DIRTY[0];
        let context_position = ADMISSION_DIRTY[1];
        assert!(
            !selected
                .get(mid)
                .is_some_and(|commit| commit.dirty.admits_dirty()),
            "an ordinary base-side dirty run must stay excluded"
        );
        assert!(
            selected
                .get(context_position)
                .is_some_and(|commit| commit.dirty.is_base_exception()),
            "the context dirty run is admitted only as the working-tree exception"
        );
        assert!(!dirty_admission().title.contains("never at or before"));
    }

    /// A run appended out of position would be joined to its neighbours by a line running
    /// backwards along the history, which reads as a trajectory nothing followed.
    #[test]
    fn the_admission_figure_lays_its_runs_out_along_the_history() {
        let positions: Vec<usize> = admission_runs()
            .iter()
            .map(|&(observation, _)| observation.position)
            .collect();

        assert!(
            positions.array_windows::<2>().all(|[from, to]| from <= to),
            "{positions:?}"
        );
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "plotters SVG generation is host graphics, not memory-safety-relevant, and exceeds the Miri CI budget"
    )]
    fn the_admission_figure_labels_the_excluded_base_side_dirty_run() {
        let svg = dirty_admission().render();

        assert!(svg.contains("dirty, base-side → excluded"));
    }

    /// The table is the chapter's statement of the rule, so it is held to the rule rather
    /// than to a transcription of it.
    #[test]
    fn the_mode_table_matches_the_rule_for_every_combination() {
        let table = mode_table();

        for context_is_merge_base in [true, false] {
            for dirty_run_on_context in [false, true] {
                let expected = auto_mode(context_is_merge_base, dirty_run_on_context);
                let row = format!(
                    "| {} | {} | `{}` |",
                    yes_no(context_is_merge_base),
                    yes_no(dirty_run_on_context),
                    expected.as_str(),
                );

                assert!(table.contains(&row), "missing row: {row}");
            }
        }
    }

    #[test]
    fn history_is_the_only_combination_without_a_branch_signal() {
        assert_eq!(auto_mode(true, false), AnalysisMode::History);
        assert_eq!(auto_mode(true, true), AnalysisMode::Branch);
        assert_eq!(auto_mode(false, false), AnalysisMode::Branch);
        assert_eq!(auto_mode(false, true), AnalysisMode::Branch);
    }

    /// A before/after figure whose panes are identical shows an operation that did
    /// nothing, and the rendered markup gives no sign of it.
    #[test]
    #[cfg_attr(
        miri,
        ignore = "renders every complete before-and-after figure to compare SVG geometry; small plot geometry is tested separately"
    )]
    fn every_operation_figure_changes_the_data_it_acts_on() {
        for (path, panes) in operations() {
            assert!(panes.changes_the_data(), "{path} draws the same data twice");
        }
    }

    #[test]
    fn geometry_ignores_captions_but_keeps_drawing_elements() {
        let before = "<svg><path d=\"M0 0\"/><text>before</text><circle r=\"1\"/></svg>";
        let renamed = "<svg><path d=\"M0 0\"/><text>after</text><circle r=\"1\"/></svg>";
        let changed = "<svg><path d=\"M0 0\"/><text>before</text><circle r=\"2\"/></svg>";

        assert_eq!(geometry(before), geometry(renamed));
        assert_ne!(geometry(before), geometry(changed));
        assert_eq!(
            geometry(before),
            "<svg><path d=\"M0 0\"/><circle r=\"1\"/></svg>"
        );
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
