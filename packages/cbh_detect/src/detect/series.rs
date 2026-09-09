//! The series engine: reconstruct per-benchmark, per-metric time series from the
//! stored runs so the finding algorithms can reason over them.
//!
//! A series is identified by the comparable [`DiscriminantSet`] its runs share
//! (parsed from the storage key) together with the [`BenchmarkId`] and metric
//! kind. Its points are ordered by *git topology* — the first-parent position of
//! their commit, supplied as a `commit -> index` map — so the timeline reflects
//! the history the runs were measured against rather than when they were ingested.
//! Within a single commit, clean runs precede dirty snapshots, and any remaining
//! tie breaks by the object's storage-key order (its ordinal) for determinism.
//!
//! Points are kept deliberately small: tens of millions can be resident at once on
//! a large history, so the storage key is reduced to a 4-byte ordinal and the
//! per-point commit string is interned to a shared `Arc<str>` rather than cloned.

use std::cmp::Ordering;
use std::collections::hash_map::Entry as MapEntry;
use std::collections::{HashMap, HashSet};
use std::hash::BuildHasher;
use std::sync::Arc;

use cbh_model::{
    BenchmarkId, BenchmarkIdPrefix, BlessingRecord, DiscriminantSet, MetricKind, Run, StorageKey,
};
use cbh_stats as stats;
use foldhash::fast::RandomState;
use foldhash::{HashMap as FoldHashMap, HashMapExt};
use hashbrown::HashTable;
use hashbrown::hash_table::Entry;
use jiff::Timestamp;

use crate::detect::run_points::RunPoints;

/// A single observation in a series.
///
/// Kept compact because a large history materializes tens of millions of these at
/// once: the provenance storage key is reduced to [`object_ordinal`] (the key's
/// rank in sorted storage-key order, the final tie-break) and the commit is an
/// interned [`Arc<str>`] shared across every point measured against the same
/// commit.
///
/// [`object_ordinal`]: SeriesPoint::object_ordinal
#[derive(Clone, Debug)]
pub struct SeriesPoint {
    /// First-parent topological position of the run's commit (oldest = 0).
    pub topo_index: usize,
    /// Whether the observation came from a dirty (uncommitted-tree) snapshot.
    pub dirty: bool,
    /// The object's rank in sorted storage-key order (the final tie-break),
    /// standing in for the full storage key to keep the point small.
    pub object_ordinal: u32,
    /// Commit the run was measured against — the storage key's commit directory
    /// segment (a full commit ID, or `unknown`). Interned so all points on one commit
    /// share a single allocation.
    pub commit: Option<Arc<str>>,
    /// The measured value.
    pub value: f64,
    /// Lower confidence-interval bound, when the engine reports one (Criterion).
    pub interval_low: Option<f64>,
    /// Upper confidence-interval bound, when the engine reports one (Criterion).
    pub interval_high: Option<f64>,
}

/// A blessing that applies to a series: the commit it was issued at, that commit's
/// topological position, and the accepted level's provenance.
///
/// In history analysis a series with a matching blessing is *re-baselined* to the
/// blessed commit — the detector treats the blessed level as the new baseline and
/// only sees points from that commit onward, while the pre-blessing points are
/// retained for charting. See the *Re-baselining* analysis
/// section of `DESIGN.md`.
#[derive(Clone, Debug)]
pub struct Blessing {
    /// Full commit ID the blessing was issued at (the report anchor).
    pub commit: String,
    /// Committer date of the blessed commit (resolved from git topology), for the
    /// report anchor. `None` when topology did not report a date for the commit.
    pub commit_time: Option<Timestamp>,
}

/// A blessing positioned in history, collected per discriminant set and consumed
/// by [`apply_blessings`].
///
/// The tuple pairs the topological index of the commit the blessing was issued at,
/// that commit's committer date (resolved from git topology, `None` when topology
/// did not report one), and the blessing record itself.
pub type BlessingPlacement = (usize, Option<Timestamp>, BlessingRecord);

/// One clean base-ref commit level used by branch-mode comparison.
///
/// Branch mode builds its baseline from the base ref's first-parent history rather
/// than the analyzed context line. Each value is already collapsed to one
/// per-commit level, but it keeps the commit coordinate and any representative
/// engine confidence interval so the branch detector can apply the same
/// interval-overlap veto history mode applies to raw regimes.
#[derive(Clone, Debug, PartialEq)]
pub struct BaseLevel {
    /// First-parent topological index of the base-ref commit.
    pub topo_index: usize,
    /// Commit the base level was measured against, when known.
    pub commit: Option<Arc<str>>,
    /// Median value of this series' clean runs at the commit.
    pub value: f64,
    /// Representative confidence interval for the commit's runs, when available.
    pub interval: Option<(f64, f64)>,
}

/// A per-`(set, benchmark, metric kind)` time series ordered by git topology.
#[derive(Clone, Debug)]
pub struct Series {
    /// The comparable discriminant set all points share.
    pub set: DiscriminantSet,
    /// The benchmark identity all points share.
    pub id: BenchmarkId,
    /// The metric kind all points share (governs unit and comparison semantics).
    pub kind: MetricKind,
    /// Observations ordered by `(topo_index, dirty, object_ordinal)`.
    pub points: Vec<SeriesPoint>,
    /// Branch mode's base-ref comparison levels, oldest first.
    ///
    /// These levels are reconstructed from the base ref's own first-parent history,
    /// not from the context ref's ancestry. Each entry is one clean base commit's
    /// median level for this series. It retains each commit's coordinate and
    /// representative interval so branch-mode lag reporting and interval vetoes use
    /// the same base state as the observed current-regime range. Regime selection may
    /// limit analysis to a supported recent suffix without deleting observations inside it.
    pub base_window: Vec<BaseLevel>,
    /// Base-ref levels available before the branch window cap and blessings.
    ///
    /// Branch diagnostics disclose both the available history and the bounded history
    /// the detector actually used.
    pub base_history_count: usize,
    /// Index into `points` where the active (post-blessing) window begins; `0`
    /// when the series is unblessed (every point is active). History-mode
    /// detection considers only `points[active_start..]`; the full `points` are
    /// retained for reporting.
    pub active_start: usize,
    /// The blessing that re-baselined this series, if any (the report anchor).
    pub blessing: Option<Blessing>,
}

/// One stored object selected for analysis, ready to be folded into series.
#[derive(Clone, Debug)]
pub struct LoadedObject {
    /// The parsed storage key (discriminant set, commit, and cleanliness).
    pub key: StorageKey,
    /// The full storage object key (provenance and final tie-break).
    pub object_key: String,
    /// The decoded run.
    pub result: Run,
}

/// Filters applied while building series from stored runs.
#[derive(Clone, Copy, Debug, Default)]
pub struct SeriesFilter<'a> {
    /// Keep only series whose benchmark identity's qualified id starts with one of
    /// these prefixes. Empty keeps every series.
    pub prefixes: &'a [BenchmarkIdPrefix],
}

/// Whether `prefixes` accepts `id` (an empty prefix list accepts every id).
///
/// The match is a raw `starts_with` against the benchmark's qualified identity,
/// mirroring blessing-prefix matching so the same prefix selects the same family
/// of benchmarks in `bless` and `analyze`.
fn prefixes_accept(prefixes: &[BenchmarkIdPrefix], id: &BenchmarkId) -> bool {
    if prefixes.is_empty() {
        return true;
    }
    let qualified = id.qualified();
    prefixes
        .iter()
        .any(|prefix| qualified.starts_with(prefix.as_str()))
}

/// Reconstructs every series from the selected `objects`.
///
/// Each metric of each result becomes one point in the series for its
/// `(discriminant set, benchmark id, metric kind)`. A result carries at most one
/// metric of each kind, so the kind alone keys the series unambiguously. The
/// `order` map gives each commit its first-parent topological index; an object
/// whose commit is not in `order` is outside the analyzed selection and is
/// skipped. Points are sorted by `(topo_index, dirty, object_ordinal)` so a clean
/// run precedes a dirty snapshot on the same commit and the timeline follows git
/// history rather than storage-listing order; the ordinal is each object's rank in
/// sorted storage-key order, so the final tie-break matches a sort by storage key.
///
/// This is a convenience wrapper over [`SeriesBuilder`]: it assigns each object its
/// storage-key ordinal up front, then folds every object in. Callers that fetch
/// objects incrementally (and want to drop each parsed run immediately, never
/// holding the whole data set in memory) should drive a [`SeriesBuilder`] directly.
#[must_use]
pub fn build_series<S: BuildHasher>(
    objects: &[LoadedObject],
    order: &HashMap<String, usize, S>,
    filter: &SeriesFilter<'_>,
) -> Vec<Series> {
    // Assign each in-selection object an ordinal equal to its rank in sorted
    // storage-key order, so ordering points by `object_ordinal` is identical to
    // ordering them by the full storage key.
    let mut keys: Vec<&str> = objects
        .iter()
        .filter(|object| order.contains_key(&object.key.commit))
        .map(|object| object.object_key.as_str())
        .collect();
    keys.sort_unstable();
    keys.dedup();
    let ordinals: HashMap<&str, u32> = keys
        .iter()
        .enumerate()
        .map(|(index, key)| (*key, ordinal_of(index)))
        .collect();

    let mut builder = SeriesBuilder::new(*filter);
    for object in objects {
        let Some(&topo_index) = order.get(&object.key.commit) else {
            continue;
        };
        let ordinal = ordinals
            .get(object.object_key.as_str())
            .copied()
            .unwrap_or(u32::MAX);
        builder.push(
            &object.key.set,
            topo_index,
            object.key.is_dirty(),
            ordinal,
            &object.key.commit,
            &RunPoints::from(&object.result),
        );
    }
    builder.finish()
}

/// Narrows a storage-key rank to the [`SeriesPoint::object_ordinal`] width.
///
/// Ordinals are a pure tie-break, so the (practically impossible) overflow past
/// `u32::MAX` distinct objects merely lets the last ordinals collide — points then
/// keep their stable insertion order, never a panic.
#[expect(
    clippy::cast_possible_truncation,
    reason = "saturating: ordinals only tie-break, and >4 billion objects never occur"
)]
fn ordinal_of(rank: usize) -> u32 {
    rank.min(u32::MAX as usize) as u32
}

/// Folds stored runs into per-`(set, benchmark, metric kind)` series.
///
/// Each pushed run contributes one point per metric to the series for its
/// identity; [`finish`](SeriesBuilder::finish) sorts every series by
/// `(topo_index, dirty, object_ordinal)`. Unlike [`build_series`], the builder
/// retains nothing of a run beyond the extracted points, so a streaming loader can
/// fold each fetched run and drop it immediately, keeping only the (compact) points
/// resident rather than the whole parsed data set.
///
/// The builder owns its prefix filter (an `Arc<[BenchmarkIdPrefix]>`) and holds no
/// borrows, so it is `Send + 'static`: a spawned worker can fold a disjoint subset
/// of the objects into its own builder and hand it back, and [`merge`] folds the
/// per-worker builders into one before a single [`finish`](SeriesBuilder::finish).
///
/// The commit of each run is interned, so all points measured against one commit
/// share a single [`Arc<str>`] instead of cloning the string per point.
///
/// [`merge`]: SeriesBuilder::merge
#[derive(Debug)]
pub struct SeriesBuilder {
    prefixes: Arc<[BenchmarkIdPrefix]>,
    groups: GroupTree,
    commits: HashMap<Box<str>, Arc<str>>,
    /// Hashes benchmark ids for the [`HashTable`] id level. One fixed instance so
    /// the lookup-time hash and the growth-time rehash agree on the hash of a key.
    hasher: RandomState,
}

/// Series points for one benchmark id, keyed by metric kind. The metric kind is
/// `Copy`, so [`FoldHashMap::entry`] resolves it in a single probe with no clone.
type KindGroups = FoldHashMap<MetricKind, Vec<SeriesPoint>>;

/// Every benchmark id within one discriminant set, paired with its per-kind series.
///
/// A [`HashTable`] (not a [`HashMap`]) so the `entry(hash, eq, hasher)` API resolves
/// each id in a single probe and clones the [`BenchmarkId`] only on a true cache
/// miss — the steady state of the fold is one stored object per benchmark per
/// commit, so the same id is looked up once per commit and must stay clone-free on
/// that hot path. `HashMap`'s `entry` would force a clone per lookup, and the
/// `get_mut`-first early return that would avoid it fails the NLL borrow check.
type IdGroups = HashTable<(BenchmarkId, KindGroups)>;

/// Series points grouped by discriminant set, then benchmark id, then metric kind.
///
/// The nesting clones each key level only when it is first encountered rather than
/// once per point: the set is resolved once per run, the benchmark id once per
/// record, and the `Copy` metric kind needs no allocation at all — so each distinct
/// series costs one key clone, not one clone per metric point folded into it.
type GroupTree = FoldHashMap<DiscriminantSet, IdGroups>;

impl SeriesBuilder {
    /// Starts an empty builder that keeps only series matching `filter`.
    #[must_use]
    pub fn new(filter: SeriesFilter<'_>) -> Self {
        Self::with_prefixes(Arc::from(filter.prefixes))
    }

    /// Starts an empty builder that keeps only series whose benchmark id starts
    /// with one of `prefixes` (an empty list keeps every series).
    ///
    /// Takes the prefixes as an owned `Arc<[BenchmarkIdPrefix]>` so the builder
    /// borrows nothing and stays `Send + 'static`, letting a spawned worker own one
    /// and a caller share the prefix list across workers with a cheap clone.
    #[must_use]
    pub fn with_prefixes(prefixes: Arc<[BenchmarkIdPrefix]>) -> Self {
        Self {
            prefixes,
            groups: FoldHashMap::new(),
            commits: HashMap::new(),
            hasher: RandomState::default(),
        }
    }

    /// Folds one stored run into the accumulating series.
    ///
    /// `topo_index` is the run commit's first-parent position, `object_ordinal` its
    /// rank in sorted storage-key order (the final point tie-break), and `commit`
    /// the storage key's commit directory segment (a full commit ID, or `unknown`); the
    /// caller supplies all three because they come from the storage key and git
    /// topology, not the run payload. `run` is the fold-relevant projection of the
    /// run (see [`RunPoints`]); only the matching metrics are retained — it can be
    /// dropped as soon as this returns.
    pub fn push(
        &mut self,
        set: &DiscriminantSet,
        topo_index: usize,
        dirty: bool,
        object_ordinal: u32,
        commit: &str,
        run: &RunPoints,
    ) {
        let commit = Some(self.intern(commit));

        // The discriminant set is constant for the whole run, so resolve its
        // subtree once and clone the set key only when the set is first seen — not
        // per record or per metric. Borrowing the prefix slice and the hasher out
        // first keeps the `self.groups` borrow below from entangling with the other
        // fields.
        let prefixes: &[BenchmarkIdPrefix] = &self.prefixes;
        let hasher = &self.hasher;
        if !self.groups.contains_key(set) {
            self.groups.insert(set.clone(), HashTable::new());
        }
        let id_groups = self
            .groups
            .get_mut(set)
            .expect("the set's subtree was just inserted when absent");

        for record in run.results() {
            if !prefixes_accept(prefixes, &record.id) {
                continue;
            }
            // Resolve the benchmark id's subtree in a single probe, cloning the id
            // only on a true cache miss. The same id recurs across every run that
            // measured it, so this keeps id clones at one per distinct series. The
            // `eq` closure confirms the match, so a hash collision never merges two
            // distinct benchmarks.
            let hash = hasher.hash_one(&record.id);
            let kind_groups = match id_groups.entry(
                hash,
                |(existing, _)| existing == &record.id,
                |(existing, _)| hasher.hash_one(existing),
            ) {
                Entry::Occupied(occupied) => &mut occupied.into_mut().1,
                Entry::Vacant(vacant) => {
                    &mut vacant
                        .insert((record.id.clone(), FoldHashMap::new()))
                        .into_mut()
                        .1
                }
            };
            for metric in &record.metrics {
                let point = SeriesPoint {
                    topo_index,
                    dirty,
                    object_ordinal,
                    commit: commit.clone(),
                    value: metric.value,
                    interval_low: metric.interval_low,
                    interval_high: metric.interval_high,
                };
                kind_groups.entry(metric.kind).or_default().push(point);
            }
        }
    }

    /// Folds another builder's accumulated series into this one.
    ///
    /// Each `(set, id, kind)` series from `other` is merged into `self`,
    /// concatenating the points of any series both hold. This is the recombination
    /// step of the parallel fold: each worker folds a disjoint subset of the objects
    /// into its own builder, and the main thread merges the per-worker builders
    /// before a single [`finish`](Self::finish) sorts the combined series. Because
    /// the partitions are disjoint, the union of their points is exactly the set a
    /// single-threaded fold would have produced.
    ///
    /// The points keep the interned commit [`Arc<str>`] they were folded with in
    /// `other` (so a commit seen by several workers ends up with one `Arc` per
    /// worker rather than one overall — a negligible cost). The benchmark ids are
    /// re-hashed into `self`'s table with `self`'s hasher, so the two builders'
    /// independent hash seeds need not agree.
    pub fn merge(&mut self, other: Self) {
        let hasher = &self.hasher;
        for (set, other_ids) in other.groups {
            let id_groups = self.groups.entry(set).or_default();
            for (id, other_kinds) in other_ids {
                // Re-probe the id in this builder's table, cloning the id only when
                // the series is first seen here — mirrors `push`'s single-probe
                // insert so a hash collision never merges two distinct benchmarks.
                let hash = hasher.hash_one(&id);
                let kind_groups = match id_groups.entry(
                    hash,
                    |(existing, _)| existing == &id,
                    |(existing, _)| hasher.hash_one(existing),
                ) {
                    Entry::Occupied(occupied) => &mut occupied.into_mut().1,
                    Entry::Vacant(vacant) => {
                        &mut vacant.insert((id, FoldHashMap::new())).into_mut().1
                    }
                };
                for (kind, points) in other_kinds {
                    // Move the whole point vector on first sight of the kind and
                    // append (a single buffer copy) only when both builders already
                    // hold points for it, never reallocating per point.
                    match kind_groups.entry(kind) {
                        MapEntry::Occupied(mut occupied) => {
                            let mut points = points;
                            occupied.get_mut().append(&mut points);
                        }
                        MapEntry::Vacant(vacant) => {
                            vacant.insert(points);
                        }
                    }
                }
            }
        }
    }

    /// Returns the interned `Arc<str>` for `commit`, allocating once per distinct
    /// commit so the millions of points on a commit share one allocation.
    fn intern(&mut self, commit: &str) -> Arc<str> {
        if let Some(existing) = self.commits.get(commit) {
            return Arc::clone(existing);
        }
        let interned: Arc<str> = Arc::from(commit);
        self.commits
            .insert(Box::from(commit), Arc::clone(&interned));
        interned
    }

    /// Finalizes every accumulated series, each sorted into topological order.
    #[must_use]
    pub fn finish(self) -> Vec<Series> {
        // Flatten the nested grouping into one series per `(set, id, kind)`, with
        // points still in insertion order.
        let mut series: Vec<Series> = Vec::new();
        for (set, id_groups) in self.groups {
            for (id, kind_groups) in id_groups {
                for (kind, points) in kind_groups {
                    series.push(Series {
                        set: set.clone(),
                        id: id.clone(),
                        kind,
                        points,
                        base_window: Vec::new(),
                        base_history_count: 0,
                        active_start: 0,
                        blessing: None,
                    });
                }
            }
        }

        // The nested maps iterate in an unspecified order, so restore the
        // deterministic `(set, id, kind)` ordering callers rely on. The key is
        // unique per series, so the unstable sort has no ties to reorder.
        series.sort_unstable_by(|left, right| {
            left.set
                .cmp(&right.set)
                .then_with(|| left.id.cmp(&right.id))
                .then_with(|| left.kind.cmp(&right.kind))
        });

        // Each series' point sort is independent, so sort each series' points in
        // place. `sort_unstable_by` orders in place without the scratch buffer a
        // stable sort allocates. The key is a total order in practice — a series
        // holds at most one point per object, so `object_ordinal` is unique within
        // it — making the unstable sort deterministic.
        for series in &mut series {
            series.points.sort_unstable_by(|left, right| {
                left.topo_index
                    .cmp(&right.topo_index)
                    .then_with(|| left.dirty.cmp(&right.dirty))
                    .then_with(|| left.object_ordinal.cmp(&right.object_ordinal))
            });
        }

        series
    }
}

/// Attaches base-ref comparison windows to matching context series.
///
/// `base_series` is reconstructed from clean runs on the base ref's own
/// first-parent history, while `series` is reconstructed from the context ref's
/// first-parent history. Both slices are in `(set, id, kind)` order, so matching is a
/// linear merge. Each attached window is the newest `compare_window` per-commit
/// levels of the base series, with repeated runs for one commit collapsed to their
/// median just like branch detection's in-series commit-level logic.
pub fn attach_base_windows(series: &mut [Series], base_series: &[Series], compare_window: usize) {
    let mut base = base_series.iter().peekable();
    for one in series {
        while base
            .peek()
            .is_some_and(|candidate| series_cmp(candidate, one).is_lt())
        {
            _ = base.next();
        }
        let Some(candidate) = base.peek() else {
            break;
        };
        if series_cmp(candidate, one).is_eq() {
            let levels = commit_levels(&candidate.points);
            one.base_history_count = levels.len();
            let start = levels.len().saturating_sub(compare_window);
            one.base_window = levels.get(start..).unwrap_or_default().to_vec();
        }
    }
}

fn series_cmp(left: &Series, right: &Series) -> Ordering {
    left.set
        .cmp(&right.set)
        .then_with(|| left.id.cmp(&right.id))
        .then_with(|| left.kind.cmp(&right.kind))
}

fn commit_levels(points: &[SeriesPoint]) -> Vec<BaseLevel> {
    let mut levels = Vec::new();
    let mut values: Vec<f64> = Vec::new();
    let mut lows: Vec<f64> = Vec::new();
    let mut highs: Vec<f64> = Vec::new();
    let mut current: Option<(usize, bool, Option<Arc<str>>)> = None;
    for point in points {
        let key = (point.topo_index, point.dirty);
        if current
            .as_ref()
            .is_none_or(|(topo_index, dirty, _)| (*topo_index, *dirty) != key)
        {
            if let Some((topo_index, _, commit)) = current {
                push_base_level(
                    &mut levels,
                    topo_index,
                    commit,
                    &mut values,
                    &mut lows,
                    &mut highs,
                );
            }
            values.clear();
            lows.clear();
            highs.clear();
            current = Some((point.topo_index, point.dirty, point.commit.clone()));
        }
        values.push(point.value);
        if let Some(low) = point.interval_low {
            lows.push(low);
        }
        if let Some(high) = point.interval_high {
            highs.push(high);
        }
    }
    if let Some((topo_index, _, commit)) = current {
        push_base_level(
            &mut levels,
            topo_index,
            commit,
            &mut values,
            &mut lows,
            &mut highs,
        );
    }
    levels
}

fn push_base_level(
    levels: &mut Vec<BaseLevel>,
    topo_index: usize,
    commit: Option<Arc<str>>,
    values: &mut [f64],
    lows: &mut [f64],
    highs: &mut [f64],
) {
    let Some(value) = stats::median_in_place(values) else {
        return;
    };
    let interval = stats::median_in_place(lows).zip(stats::median_in_place(highs));
    levels.push(BaseLevel {
        topo_index,
        commit,
        value,
        interval,
    });
}

/// Applies each series' latest matching blessing to its branch base window.
///
/// Branch mode positions blessings on the base ref's first-parent history. A matching
/// blessing is a hard evidence boundary: levels before its commit position are removed,
/// and the retained baseline begins with the first measured level at or after that
/// position. The branch context points are not re-indexed because they live on a
/// different first-parent line.
pub fn apply_base_blessings<S: BuildHasher>(
    series: &mut [Series],
    blessings: &HashMap<DiscriminantSet, Vec<BlessingPlacement>, S>,
) {
    for one in series.iter_mut() {
        let Some(set_blessings) = blessings.get(&one.set) else {
            continue;
        };
        let latest = set_blessings
            .iter()
            .filter(|(_, _, record)| record.matches(&one.id))
            .max_by_key(|(topo_index, _, _)| *topo_index);
        let Some((topo_index, commit_time, record)) = latest else {
            continue;
        };
        let keep_from = one
            .base_window
            .partition_point(|level| level.topo_index < *topo_index);
        one.base_window.drain(..keep_from);
        one.blessing = Some(Blessing {
            commit: record.commit.clone(),
            commit_time: *commit_time,
        });
    }
}

/// Re-baselines each series to its latest matching blessing (history mode).
///
/// For every series, the most recent blessing (by topological position) whose
/// prefixes accept the series' benchmark id selects the re-baseline commit. The
/// series' `active_start` is set to the first point at or after that commit, so the
/// detector only sees the post-blessing window while the full series is retained
/// for charting. A series with no matching blessing is left untouched
/// (`active_start = 0`). Branch mode passes an empty map and so is
/// unaffected.
///
/// Each blessing carries the committer date of its commit (resolved from git
/// topology, `None` when topology did not report one), which becomes the report
/// anchor for the re-baselined series.
pub fn apply_blessings<S: BuildHasher>(
    series: &mut [Series],
    blessings: &HashMap<DiscriminantSet, Vec<BlessingPlacement>, S>,
) {
    for one in series.iter_mut() {
        let Some(set_blessings) = blessings.get(&one.set) else {
            continue;
        };
        let latest = set_blessings
            .iter()
            .filter(|(_, _, record)| record.matches(&one.id))
            .max_by_key(|(topo_index, _, _)| *topo_index);
        let Some((topo_index, commit_time, record)) = latest else {
            continue;
        };
        // The active window starts at the first point on or after the blessed
        // commit. `points` is already sorted by `topo_index`, so a partition point
        // locates the boundary.
        one.active_start = one
            .points
            .partition_point(|point| point.topo_index < *topo_index);
        one.blessing = Some(Blessing {
            commit: record.commit.clone(),
            commit_time: *commit_time,
        });
    }
}

/// Drops every series whose benchmark is not present at `context_commit`, keeping
/// only the benchmarks the context commit actually measured.
///
/// Presence is evaluated per `(discriminant set, benchmark id)`: a benchmark is
/// kept when at least one of its points was measured against `context_commit` — a
/// clean run or a dirty snapshot on that commit — after which *all* of its
/// metric-kind series are retained. A benchmark with points only on earlier commits
/// is a "ghost" that no longer exists in the current suite; all of its series are
/// removed so the detectors never re-flag a benchmark that is gone.
///
/// The check reads each series' raw points, so it is independent of any blessing
/// re-baselining; call it *before* [`apply_blessings`]. When `context_commit` was
/// itself never measured, every benchmark is a ghost and the whole list is emptied
/// — the caller distinguishes that empty outcome for the user.
///
/// Returns one entry per removed metric series, ordered by `(set, id, kind)`. That
/// is the unit the series census counts, so a caller's diagnostics and its census
/// reconcile against each other; deduplicating on `(set, id)` recovers the ghost
/// benchmarks behind them.
#[must_use]
pub fn retain_present_at_context(
    series: &mut Vec<Series>,
    context_commit: &str,
) -> Vec<(DiscriminantSet, BenchmarkId, MetricKind)> {
    // The benchmarks the context commit measured: a single series carrying a point
    // on that commit marks its whole benchmark present, across every metric kind.
    // Keyed by `(set, id)` so a benchmark present in one discriminant set does not
    // rescue a same-named ghost in another — sets are analyzed independently.
    let mut present: HashSet<(DiscriminantSet, BenchmarkId)> = HashSet::new();
    for one in series.iter() {
        let measured_here = one
            .points
            .iter()
            .any(|point| point.commit.as_deref() == Some(context_commit));
        if measured_here {
            present.insert((one.set.clone(), one.id.clone()));
        }
    }

    // Retain the present benchmarks and record every dropped series, so the report is
    // in the same unit the census counts rather than a coarser benchmark identity.
    let mut removed: Vec<(DiscriminantSet, BenchmarkId, MetricKind)> = Vec::new();
    series.retain(|one| {
        // Clone the identity key once per series and reuse it for both the presence
        // test and the removal record, avoiding a second clone per ghost.
        let key = (one.set.clone(), one.id.clone());
        if present.contains(&key) {
            true
        } else {
            let (set, id) = key;
            removed.push((set, id, one.kind));
            false
        }
    });

    // A stable, deterministic order for the diagnostics the caller emits. The triple
    // is unique per series, so the unstable sort has no ties to reorder.
    removed.sort_unstable();
    removed
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    #![allow(
        clippy::float_cmp,
        reason = "metric values are exact integer-derived counts"
    )]
    #![allow(clippy::indexing_slicing, reason = "panic is fine in tests")]

    use cbh_model::{
        BenchmarkResult, EnvironmentInfo, GitInfo, Metric, RunContext, ToolchainInfo, parse_key,
    };
    use nonempty::NonEmpty;

    use super::*;

    fn ts(seconds: i64) -> Timestamp {
        Timestamp::from_second(seconds).unwrap()
    }

    /// Builds a stored run with one result carrying one instruction-count metric.
    fn run(observation: Timestamp, commit: &str, value: f64) -> Run {
        run_for_package(observation, commit, value, None)
    }

    /// Builds a stored run whose single result is scoped to `package`, keeping
    /// every other identity segment fixed.
    fn run_for_package(
        observation: Timestamp,
        commit: &str,
        value: f64,
        package: Option<&str>,
    ) -> Run {
        let context = RunContext::new(
            observation,
            GitInfo {
                commit: Some(format!("{commit}full")),
                branch: Some("main".to_owned()),
                dirty: false,
            },
            EnvironmentInfo::default(),
            ToolchainInfo::default(),
            "0.0.1".to_owned(),
        );
        let mut segments = Vec::new();
        if let Some(package) = package {
            segments.push(package.to_owned());
        }
        segments.push("group".to_owned());
        segments.push("case".to_owned());
        let record = BenchmarkResult::new(
            BenchmarkId::new(NonEmpty::from_vec(segments).unwrap()),
            vec![Metric::new(MetricKind::InstructionCount, value)],
        );
        Run::new(context, vec![record])
    }

    /// A clean object at `commit` carrying the given instruction-count value.
    fn clean_object(commit: &str, observation: i64, value: f64) -> LoadedObject {
        let object_key =
            format!("v1/proj/objects/callgrind/x86_64-unknown-linux-gnu/m1/{commit}/clean.json");
        LoadedObject {
            key: parse_key(&object_key).unwrap(),
            object_key,
            result: run(ts(observation), commit, value),
        }
    }

    /// A dirty snapshot at `commit` taken at `unix`, carrying the given value.
    fn dirty_object(commit: &str, unix: i64, value: f64) -> LoadedObject {
        let object_key = format!(
            "v1/proj/objects/callgrind/x86_64-unknown-linux-gnu/m1/{commit}/dirty-{unix}.json"
        );
        LoadedObject {
            key: parse_key(&object_key).unwrap(),
            object_key,
            result: run(ts(unix), commit, value),
        }
    }

    fn order(commits: &[&str]) -> HashMap<String, usize> {
        commits
            .iter()
            .enumerate()
            .map(|(index, commit)| ((*commit).to_owned(), index))
            .collect()
    }

    /// One folded object: its discriminant set, the commit's topological index, the
    /// dirty flag, the storage-key ordinal, the storage-key commit, and the lean run
    /// projection to push.
    type FoldInput = (DiscriminantSet, usize, bool, u32, String, RunPoints);

    /// Builds the fold inputs for a small data set spanning two benchmark ids
    /// (packages) across three commits, including a clean/dirty pair on one commit,
    /// so a merge has to combine like series whose points come from both partitions.
    fn merge_inputs() -> Vec<FoldInput> {
        let rows = [
            ("pkga", "c0", 0_usize, false, 0_u32, 10.0),
            ("pkgb", "c0", 0, false, 1, 20.0),
            ("pkga", "c1", 1, false, 2, 11.0),
            ("pkga", "c1", 1, true, 3, 12.0),
            ("pkgb", "c2", 2, false, 4, 21.0),
        ];
        rows.into_iter()
            .map(|(package, commit, topo_index, dirty, ordinal, value)| {
                let run = run_for_package(ts(1), commit, value, Some(package));
                let object_key = format!(
                    "v1/proj/objects/callgrind/x86_64-unknown-linux-gnu/m1/{commit}/clean.json"
                );
                let key = parse_key(&object_key).unwrap();
                (
                    key.set,
                    topo_index,
                    dirty,
                    ordinal,
                    key.commit,
                    RunPoints::from(&run),
                )
            })
            .collect()
    }

    fn fold_all(builder: &mut SeriesBuilder, inputs: &[FoldInput]) {
        for (set, topo_index, dirty, ordinal, commit, run) in inputs {
            builder.push(set, *topo_index, *dirty, *ordinal, commit, run);
        }
    }

    /// A comparable projection of one finished series: identity plus each point's
    /// sort key and value, so two series sets can be checked for byte-for-byte
    /// agreement.
    type SeriesSummary = (
        DiscriminantSet,
        BenchmarkId,
        MetricKind,
        Vec<(usize, bool, u32, f64)>,
    );

    fn series_summary(series: &[Series]) -> Vec<SeriesSummary> {
        series
            .iter()
            .map(|one| {
                let points = one
                    .points
                    .iter()
                    .map(|point| {
                        (
                            point.topo_index,
                            point.dirty,
                            point.object_ordinal,
                            point.value,
                        )
                    })
                    .collect();
                (one.set.clone(), one.id.clone(), one.kind, points)
            })
            .collect()
    }

    #[test]
    fn merge_combines_disjoint_folds_into_one() {
        // Folding the whole data set into one builder must equal folding two disjoint
        // halves into separate builders and merging them: the parallel fold partitions
        // the objects across workers, so the union of the per-worker series has to
        // reproduce the single-threaded result exactly.
        let inputs = merge_inputs();
        let prefixes: Arc<[BenchmarkIdPrefix]> = Arc::from(Vec::new());

        let mut reference = SeriesBuilder::with_prefixes(Arc::clone(&prefixes));
        fold_all(&mut reference, &inputs);
        let reference = reference.finish();

        let (left, right) = inputs.split_at(2);
        let mut first = SeriesBuilder::with_prefixes(Arc::clone(&prefixes));
        fold_all(&mut first, left);
        let mut second = SeriesBuilder::with_prefixes(prefixes);
        fold_all(&mut second, right);
        first.merge(second);
        let merged = first.finish();

        // The data really spans more than one series, so the id-level merge is exercised.
        assert!(reference.len() >= 2);
        assert_eq!(series_summary(&reference), series_summary(&merged));
    }

    #[test]
    fn merge_into_empty_builder_yields_the_other() {
        // Merging a folded builder into a fresh one (the degenerate single-worker
        // case, where the main thread merges one partial) reproduces a direct fold.
        let inputs = merge_inputs();
        let prefixes: Arc<[BenchmarkIdPrefix]> = Arc::from(Vec::new());

        let mut reference = SeriesBuilder::with_prefixes(Arc::clone(&prefixes));
        fold_all(&mut reference, &inputs);
        let reference = reference.finish();

        let mut only = SeriesBuilder::with_prefixes(Arc::clone(&prefixes));
        fold_all(&mut only, &inputs);
        let mut empty = SeriesBuilder::with_prefixes(prefixes);
        empty.merge(only);
        let merged = empty.finish();

        assert_eq!(series_summary(&reference), series_summary(&merged));
    }

    #[test]
    fn build_series_orders_points_by_topology() {
        // The objects are supplied out of topological order; build_series must sort
        // them by their commit's first-parent index so the values come out in
        // commit order regardless of insertion order.
        let objects = vec![
            clean_object("c2", 100, 30.0),
            clean_object("c0", 300, 10.0),
            clean_object("c1", 200, 20.0),
        ];
        let series = build_series(
            &objects,
            &order(&["c0", "c1", "c2"]),
            &SeriesFilter::default(),
        );
        assert_eq!(series.len(), 1);
        let values: Vec<f64> = series[0].points.iter().map(|point| point.value).collect();
        assert_eq!(values, vec![10.0, 20.0, 30.0]);
    }

    #[test]
    fn build_series_orders_clean_before_dirty_within_a_commit() {
        // One commit with a clean run plus two dirty snapshots; clean comes first,
        // then the dirty snapshots ordered by their storage key (`dirty-<unix>`).
        let objects = vec![
            dirty_object("c0", 300, 33.0),
            dirty_object("c0", 200, 22.0),
            clean_object("c0", 100, 11.0),
        ];
        let series = build_series(&objects, &order(&["c0"]), &SeriesFilter::default());
        let values: Vec<f64> = series[0].points.iter().map(|point| point.value).collect();
        assert_eq!(values, vec![11.0, 22.0, 33.0]);
        let dirty: Vec<bool> = series[0].points.iter().map(|point| point.dirty).collect();
        assert_eq!(dirty, vec![false, true, true]);
    }

    #[test]
    fn build_series_skips_commits_outside_the_selection() {
        // `c9` is not in the order map (outside the analyzed selection), so its
        // object contributes nothing.
        let objects = vec![clean_object("c0", 100, 10.0), clean_object("c9", 200, 99.0)];
        let series = build_series(&objects, &order(&["c0"]), &SeriesFilter::default());
        assert_eq!(series.len(), 1);
        assert_eq!(series[0].points.len(), 1);
        assert_eq!(series[0].points[0].value, 10.0);
    }

    #[test]
    fn build_series_separates_sets() {
        // A second run in a different triple is a different (incomparable) series.
        let other_key =
            "v1/proj/objects/callgrind/aarch64-unknown-linux-gnu/m1/c0/clean.json".to_owned();
        let other = LoadedObject {
            key: parse_key(&other_key).unwrap(),
            object_key: other_key,
            result: run(ts(200), "c0", 20.0),
        };
        let objects = vec![clean_object("c0", 100, 10.0), other];
        let series = build_series(&objects, &order(&["c0"]), &SeriesFilter::default());
        assert_eq!(series.len(), 2, "different triples are different series");
    }

    #[test]
    fn build_series_separates_packages() {
        // Two results share group/case/kind and set but belong to different
        // packages, so they must form two series rather than silently merging.
        let foo_key =
            "v1/proj/objects/callgrind/x86_64-unknown-linux-gnu/m1/c0/clean.json".to_owned();
        let bar_key =
            "v1/proj/objects/callgrind/x86_64-unknown-linux-gnu/m1/c1/clean.json".to_owned();
        let objects = vec![
            LoadedObject {
                key: parse_key(&foo_key).unwrap(),
                object_key: foo_key,
                result: run_for_package(ts(100), "c0", 10.0, Some("foo")),
            },
            LoadedObject {
                key: parse_key(&bar_key).unwrap(),
                object_key: bar_key,
                result: run_for_package(ts(200), "c1", 20.0, Some("bar")),
            },
        ];
        let series = build_series(&objects, &order(&["c0", "c1"]), &SeriesFilter::default());
        assert_eq!(series.len(), 2, "different packages are different series");
    }

    #[test]
    fn build_series_applies_prefix_filter() {
        // Two benchmarks in different packages; a prefix selects only one family.
        let foo_key =
            "v1/proj/objects/callgrind/x86_64-unknown-linux-gnu/m1/c0/clean.json".to_owned();
        let bar_key =
            "v1/proj/objects/callgrind/x86_64-unknown-linux-gnu/m1/c1/clean.json".to_owned();
        let objects = vec![
            LoadedObject {
                key: parse_key(&foo_key).unwrap(),
                object_key: foo_key,
                result: run_for_package(ts(100), "c0", 10.0, Some("foo")),
            },
            LoadedObject {
                key: parse_key(&bar_key).unwrap(),
                object_key: bar_key,
                result: run_for_package(ts(200), "c1", 20.0, Some("bar")),
            },
        ];
        let prefixes = vec![BenchmarkIdPrefix::new("foo/").unwrap()];
        let filter = SeriesFilter {
            prefixes: &prefixes,
        };
        let series = build_series(&objects, &order(&["c0", "c1"]), &filter);
        assert_eq!(series.len(), 1, "only the foo-prefixed benchmark is kept");
        assert_eq!(series[0].id.qualified(), "foo/group/case");
    }

    fn blessing(prefixes: &[&str], commit: &str, issued: i64) -> BlessingRecord {
        BlessingRecord::new(
            commit.to_owned(),
            ts(issued),
            prefixes
                .iter()
                .map(|prefix| BenchmarkIdPrefix::new(*prefix).unwrap())
                .collect(),
            "0.0.1".to_owned(),
        )
    }

    /// A four-commit `c0..c3` series, ready for blessing tests.
    fn four_commit_series() -> Vec<Series> {
        let objects = vec![
            clean_object("c0", 100, 10.0),
            clean_object("c1", 200, 20.0),
            clean_object("c2", 300, 30.0),
            clean_object("c3", 400, 40.0),
        ];
        build_series(
            &objects,
            &order(&["c0", "c1", "c2", "c3"]),
            &SeriesFilter::default(),
        )
    }

    #[test]
    fn apply_blessings_rebaselines_to_the_matching_blessing() {
        let mut series = four_commit_series();
        let set = series[0].set.clone();
        let mut map = HashMap::new();
        // Blessed at the c2 commit (topological index 2), committer date ts(300).
        map.insert(
            set,
            vec![(2_usize, Some(ts(300)), blessing(&["group"], "c2full", 301))],
        );

        apply_blessings(&mut series, &map);

        // The active window begins at the first point on or after c2, so the c0/c1
        // points are excluded from detection while retained for charts.
        assert_eq!(series[0].active_start, 2);
        let recorded = series[0].blessing.as_ref().unwrap();
        assert_eq!(recorded.commit, "c2full");
        assert_eq!(recorded.commit_time, Some(ts(300)));
    }

    #[test]
    fn apply_base_blessings_removes_only_pre_blessing_levels() {
        let mut series = four_commit_series();
        series[0].base_window = series[0]
            .points
            .iter()
            .map(|point| BaseLevel {
                topo_index: point.topo_index,
                commit: point.commit.clone(),
                value: point.value,
                interval: point.interval_low.zip(point.interval_high),
            })
            .collect();
        series[0].base_history_count = series[0].base_window.len();
        let set = series[0].set.clone();
        let mut map = HashMap::new();
        map.insert(
            set,
            vec![(2_usize, Some(ts(300)), blessing(&["group"], "c2full", 301))],
        );

        apply_base_blessings(&mut series, &map);

        assert_eq!(
            series[0]
                .base_window
                .iter()
                .map(|level| level.topo_index)
                .collect::<Vec<_>>(),
            vec![2, 3],
            "the blessed commit remains and all earlier evidence is excluded"
        );
        assert_eq!(series[0].base_history_count, 4);
        assert_eq!(
            series[0].points.len(),
            4,
            "branch-side topology is unchanged"
        );
        assert_eq!(series[0].blessing.as_ref().unwrap().commit, "c2full");
    }

    #[test]
    fn apply_blessings_ignores_a_non_matching_blessing() {
        let mut series = four_commit_series();
        let set = series[0].set.clone();
        let mut map = HashMap::new();
        // The series' benchmark id is `group/case`; this prefix matches nothing.
        map.insert(
            set,
            vec![(2_usize, Some(ts(300)), blessing(&["other"], "c2full", 301))],
        );

        apply_blessings(&mut series, &map);

        assert_eq!(series[0].active_start, 0, "no re-baseline");
        assert!(series[0].blessing.is_none(), "no blessing recorded");
    }

    #[test]
    fn apply_blessings_picks_the_latest_matching_blessing() {
        let mut series = four_commit_series();
        let set = series[0].set.clone();
        let mut map = HashMap::new();
        // Two matching blessings; the later one (c3, index 3) wins.
        map.insert(
            set,
            vec![
                (1_usize, Some(ts(200)), blessing(&["group"], "c1full", 201)),
                (3_usize, Some(ts(400)), blessing(&["group"], "c3full", 401)),
            ],
        );

        apply_blessings(&mut series, &map);

        assert_eq!(series[0].active_start, 3);
        assert_eq!(series[0].blessing.as_ref().unwrap().commit, "c3full");
    }

    #[test]
    fn apply_blessings_leaves_a_series_whose_set_has_no_blessings() {
        // The blessings map is keyed by discriminant set; a series whose set is
        // absent from the map must be left fully active rather than re-baselined.
        let mut series = four_commit_series();
        // A blessing recorded only for a *different* set (a different target triple),
        // so the lookup for this series' set misses.
        let other_set =
            parse_key("v1/proj/objects/callgrind/aarch64-unknown-linux-gnu/m1/c0/clean.json")
                .unwrap()
                .set;
        let mut map = HashMap::new();
        map.insert(
            other_set,
            vec![(2_usize, Some(ts(300)), blessing(&["group"], "c2full", 301))],
        );

        apply_blessings(&mut series, &map);

        assert_eq!(
            series[0].active_start, 0,
            "an unrelated set leaves the series active from the start"
        );
        assert!(series[0].blessing.is_none(), "no blessing recorded");
    }

    /// A clean/dirty object at `commit` under `triple` whose run carries one
    /// `InstructionCount` result per `(package, value)`, so several benchmarks share
    /// one stored run exactly as a real `clean.json` holds a whole suite.
    fn multi_object(
        triple: &str,
        commit: &str,
        filename: &str,
        observation: i64,
        benches: &[(&str, f64)],
    ) -> LoadedObject {
        let object_key = format!("v1/proj/objects/callgrind/{triple}/m1/{commit}/{filename}");
        let context = RunContext::new(
            ts(observation),
            GitInfo {
                commit: Some(format!("{commit}full")),
                branch: Some("main".to_owned()),
                dirty: filename != "clean.json",
            },
            EnvironmentInfo::default(),
            ToolchainInfo::default(),
            "0.0.1".to_owned(),
        );
        let records = benches
            .iter()
            .map(|(package, value)| {
                BenchmarkResult::new(
                    BenchmarkId::new(
                        NonEmpty::from_vec(vec![
                            (*package).to_owned(),
                            "group".to_owned(),
                            "case".to_owned(),
                        ])
                        .unwrap(),
                    ),
                    vec![Metric::new(MetricKind::InstructionCount, *value)],
                )
            })
            .collect();
        LoadedObject {
            key: parse_key(&object_key).unwrap(),
            object_key,
            result: Run::new(context, records),
        }
    }

    /// A clean object under the default triple.
    fn clean_multi(commit: &str, observation: i64, benches: &[(&str, f64)]) -> LoadedObject {
        multi_object(
            "x86_64-unknown-linux-gnu",
            commit,
            "clean.json",
            observation,
            benches,
        )
    }

    #[test]
    fn retain_present_at_context_drops_a_benchmark_absent_at_the_context() {
        // `pkgb` was measured at c0 but not at the context commit c1, so it is a
        // ghost: every one of its series is removed, and `pkga` (present at c1)
        // stays. The dropped identity is reported for diagnostics.
        let objects = vec![
            clean_multi("c0", 100, &[("pkga", 10.0), ("pkgb", 20.0)]),
            clean_multi("c1", 200, &[("pkga", 11.0)]),
        ];
        let mut series = build_series(&objects, &order(&["c0", "c1"]), &SeriesFilter::default());
        assert_eq!(
            series.len(),
            2,
            "both benchmarks build a series before filtering"
        );

        let removed = retain_present_at_context(&mut series, "c1");

        assert_eq!(series.len(), 1);
        assert_eq!(series[0].id.qualified(), "pkga/group/case");
        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0].1.qualified(), "pkgb/group/case");
    }

    #[test]
    fn retain_present_at_context_keeps_every_metric_kind_of_a_present_benchmark() {
        // The benchmark carried two metric kinds historically but only
        // InstructionCount at the context commit. Presence is benchmark-level, so
        // the ConditionalBranches series — which has no point at c1 — is retained
        // because its benchmark is present, and nothing is reported as a ghost.
        let objects = vec![
            clean_metrics(
                "c0",
                100,
                "pkga",
                &[
                    (MetricKind::InstructionCount, 10.0),
                    (MetricKind::ConditionalBranches, 100.0),
                ],
            ),
            clean_metrics("c1", 200, "pkga", &[(MetricKind::InstructionCount, 11.0)]),
        ];
        let mut series = build_series(&objects, &order(&["c0", "c1"]), &SeriesFilter::default());
        assert_eq!(series.len(), 2, "one series per metric kind");

        let removed = retain_present_at_context(&mut series, "c1");

        assert_eq!(series.len(), 2, "both metric-kind series survive");
        assert!(
            removed.is_empty(),
            "the benchmark is present, so nothing is a ghost"
        );
    }

    /// A clean object whose single `package` result carries several metric kinds.
    fn clean_metrics(
        commit: &str,
        observation: i64,
        package: &str,
        metrics: &[(MetricKind, f64)],
    ) -> LoadedObject {
        let object_key =
            format!("v1/proj/objects/callgrind/x86_64-unknown-linux-gnu/m1/{commit}/clean.json");
        let context = RunContext::new(
            ts(observation),
            GitInfo {
                commit: Some(format!("{commit}full")),
                branch: Some("main".to_owned()),
                dirty: false,
            },
            EnvironmentInfo::default(),
            ToolchainInfo::default(),
            "0.0.1".to_owned(),
        );
        let record = BenchmarkResult::new(
            BenchmarkId::new(
                NonEmpty::from_vec(vec![
                    package.to_owned(),
                    "group".to_owned(),
                    "case".to_owned(),
                ])
                .unwrap(),
            ),
            metrics
                .iter()
                .map(|(kind, value)| Metric::new(*kind, *value))
                .collect::<Vec<_>>(),
        );
        LoadedObject {
            key: parse_key(&object_key).unwrap(),
            object_key,
            result: Run::new(context, vec![record]),
        }
    }

    #[test]
    fn retain_present_at_context_reports_a_ghost_once_per_metric_series() {
        // `pkgb` carries two metric kinds and disappears before the context commit.
        // The account is in metric series, so both of its series are named rather
        // than the benchmark once — a caller that reported per benchmark could not
        // reconcile its diagnostics against the series census.
        let objects = vec![
            clean_metrics(
                "c0",
                100,
                "pkgb",
                &[
                    (MetricKind::InstructionCount, 10.0),
                    (MetricKind::ConditionalBranches, 100.0),
                ],
            ),
            clean_metrics("c1", 200, "pkga", &[(MetricKind::InstructionCount, 11.0)]),
        ];
        let mut series = build_series(&objects, &order(&["c0", "c1"]), &SeriesFilter::default());

        let removed = retain_present_at_context(&mut series, "c1");

        assert_eq!(series.len(), 1, "only the present benchmark survives");
        assert_eq!(removed.len(), 2);
        assert_eq!(removed[0].1.qualified(), "pkgb/group/case");
        assert_eq!(removed[1].1.qualified(), "pkgb/group/case");
        assert_ne!(
            removed[0].2, removed[1].2,
            "each of the ghost's metric series is named"
        );
    }

    #[test]
    fn retain_present_at_context_empties_when_the_context_has_no_runs() {
        // c2 is an analyzed commit that carries no runs. Every benchmark is a ghost,
        // so the list empties — the caller turns this into the "collect at the
        // context commit" hint.
        let objects = vec![
            clean_multi("c0", 100, &[("pkga", 10.0)]),
            clean_multi("c1", 200, &[("pkga", 11.0)]),
        ];
        let mut series = build_series(
            &objects,
            &order(&["c0", "c1", "c2"]),
            &SeriesFilter::default(),
        );

        let removed = retain_present_at_context(&mut series, "c2");

        assert!(series.is_empty(), "no benchmark is present at c2");
        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0].1.qualified(), "pkga/group/case");
    }

    #[test]
    fn retain_present_at_context_is_evaluated_per_discriminant_set() {
        // The same benchmark id exists under two triples. It is present at the
        // context c1 in the linux set but only at c0 in the arm set, so it is kept
        // in one set and dropped in the other — presence never leaks across sets.
        let objects = vec![
            multi_object(
                "x86_64-unknown-linux-gnu",
                "c0",
                "clean.json",
                100,
                &[("pkga", 10.0)],
            ),
            multi_object(
                "x86_64-unknown-linux-gnu",
                "c1",
                "clean.json",
                200,
                &[("pkga", 11.0)],
            ),
            multi_object(
                "aarch64-unknown-linux-gnu",
                "c0",
                "clean.json",
                100,
                &[("pkga", 20.0)],
            ),
        ];
        let mut series = build_series(&objects, &order(&["c0", "c1"]), &SeriesFilter::default());
        assert_eq!(series.len(), 2, "one series per set");

        let removed = retain_present_at_context(&mut series, "c1");

        assert_eq!(
            series.len(),
            1,
            "the arm-set ghost is dropped, the linux one kept"
        );
        assert_eq!(
            series[0].points.len(),
            2,
            "the surviving series is the linux one (c0 + c1)"
        );
        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0].1.qualified(), "pkga/group/case");
        assert_ne!(
            removed[0].0, series[0].set,
            "the dropped ghost is in the other discriminant set"
        );
    }

    #[test]
    fn retain_present_at_context_admits_a_dirty_snapshot_at_the_context() {
        // `pkga` exists at the context commit only as a dirty snapshot (the
        // uncommitted-work case), which still counts as present; `pkgb`, seen only
        // at c0, is the ghost.
        let objects = vec![
            clean_multi("c0", 100, &[("pkga", 10.0), ("pkgb", 20.0)]),
            multi_object(
                "x86_64-unknown-linux-gnu",
                "c1",
                "dirty-200.json",
                200,
                &[("pkga", 11.0)],
            ),
        ];
        let mut series = build_series(&objects, &order(&["c0", "c1"]), &SeriesFilter::default());

        let removed = retain_present_at_context(&mut series, "c1");

        assert_eq!(series.len(), 1);
        assert_eq!(series[0].id.qualified(), "pkga/group/case");
        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0].1.qualified(), "pkgb/group/case");
    }

    #[test]
    fn push_and_merge_grow_the_id_table_across_resizes() {
        // Miri only needs the initial small-table resizes, which are asserted below;
        // native runs additionally exercise larger tables.
        const IDS_PER_BUILDER: u32 = if cfg!(miri) { 8 } else { 64 };

        // Folding many distinct benchmark ids into one discriminant set forces the
        // per-set id table to grow and rehash its existing entries; merging a second
        // builder full of further distinct ids grows it again. A wrong hash in either
        // rehash closure would silently drop or conflate series, so check that every
        // distinct id survives as its own series across both resize paths.
        let prefixes: Arc<[BenchmarkIdPrefix]> = Arc::from(Vec::new());
        let key = parse_key("v1/proj/objects/callgrind/x86_64-unknown-linux-gnu/m1/c0/clean.json")
            .unwrap();

        let mut first = SeriesBuilder::with_prefixes(Arc::clone(&prefixes));
        let mut initial_capacity = 0;
        for index in 0..IDS_PER_BUILDER {
            let package = format!("pkg{index:03}");
            let run = run_for_package(ts(1), "c0", f64::from(index), Some(&package));
            first.push(
                &key.set,
                0,
                false,
                index,
                &key.commit,
                &RunPoints::from(&run),
            );
            if index == 0 {
                initial_capacity = first.groups.get(&key.set).unwrap().capacity();
            }
        }
        let capacity_before_merge = first.groups.get(&key.set).unwrap().capacity();
        assert!(capacity_before_merge > initial_capacity);

        let mut second = SeriesBuilder::with_prefixes(prefixes);
        for index in IDS_PER_BUILDER..(IDS_PER_BUILDER * 2) {
            let package = format!("pkg{index:03}");
            let run = run_for_package(ts(1), "c0", f64::from(index), Some(&package));
            second.push(
                &key.set,
                0,
                false,
                index,
                &key.commit,
                &RunPoints::from(&run),
            );
        }

        first.merge(second);
        assert!(first.groups.get(&key.set).unwrap().capacity() > capacity_before_merge);
        let series = first.finish();

        assert_eq!(
            series.len(),
            usize::try_from(IDS_PER_BUILDER * 2).unwrap(),
            "every distinct id is its own series, none lost to a botched rehash"
        );
    }

    #[test]
    fn finish_orders_two_metric_kinds_of_one_benchmark_by_kind() {
        // One benchmark measured with two metric kinds yields two series that share
        // set and id and differ only by kind. finish must fall through to the kind
        // tie-break (the innermost series comparator) and order them deterministically.
        let context = RunContext::new(
            ts(1),
            GitInfo {
                commit: Some("c0full".to_owned()),
                branch: Some("main".to_owned()),
                dirty: false,
            },
            EnvironmentInfo::default(),
            ToolchainInfo::default(),
            "0.0.1".to_owned(),
        );
        let record = BenchmarkResult::new(
            BenchmarkId::new(
                NonEmpty::from_vec(vec!["group".to_owned(), "case".to_owned()]).unwrap(),
            ),
            vec![
                Metric::new(MetricKind::InstructionCount, 100.0),
                Metric::new(MetricKind::WallTime, 5.0),
            ],
        );
        let run = Run::new(context, vec![record]);
        let key = parse_key("v1/proj/objects/callgrind/x86_64-unknown-linux-gnu/m1/c0/clean.json")
            .unwrap();

        let mut builder = SeriesBuilder::new(SeriesFilter::default());
        builder.push(&key.set, 0, false, 0, &key.commit, &RunPoints::from(&run));
        let series = builder.finish();

        assert_eq!(series.len(), 2, "two metric kinds of one id are two series");
        assert_eq!(series[0].id, series[1].id, "same benchmark id");
        assert_eq!(series[0].set, series[1].set, "same discriminant set");
        assert!(
            series[0].kind < series[1].kind,
            "series sharing set and id are ordered by the kind tie-break"
        );
    }
}
