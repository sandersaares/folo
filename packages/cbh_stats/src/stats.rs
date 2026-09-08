//! Pure statistical primitives for the analysis detectors.
//!
//! Everything here is deterministic, allocation-light, and free of I/O or the
//! wall clock, so it runs under Miri and is unit-tested with named,
//! value-asserting cases on hand-computable inputs. The detectors in the
//! `cbh_detect` crate compose these primitives; keeping the math isolated
//! keeps both halves easy to reason about.

use std::cmp::Ordering;

use crate::{NO_EVIDENCE, clamp_p_value, two_sided_p_from_z};

/// Casts a small count to `f64`. Series lengths are far below 2^53, so the
/// conversion is exact.
#[expect(
    clippy::cast_precision_loss,
    reason = "series lengths are far below 2^53, so the cast is exact"
)]
fn count_to_f64(count: usize) -> f64 {
    count as f64
}

/// Whether two finite values are bit-for-bit equal (tie detection for ranks).
fn same(left: f64, right: f64) -> bool {
    left.total_cmp(&right) == Ordering::Equal
}

/// The number of unordered pairs `(i, j)` with `i < j` drawn from `count`
/// elements, i.e. `count·(count−1)/2`.
///
/// Sizes the pairwise-slope buffer in [`theil_sen_line`] up front so it does not
/// reallocate while filling. The product `count·(count−1)` is always even, so
/// halving it is exact. Realistic series lengths keep the product far below
/// `usize::MAX`; the checked multiply returns 0 if it ever would overflow, so the
/// buffer just grows on demand rather than requesting an absurd capacity.
//
// Mutation-skipped: the result only sizes a capacity hint, never the computed line,
// so no behavioral test can distinguish one return value from another.
#[cfg_attr(test, mutants::skip)]
fn pair_count(count: usize) -> usize {
    count
        .checked_mul(count.saturating_sub(1))
        .and_then(|product| product.checked_div(2))
        .unwrap_or(0)
}

/// The median of `values`, or `None` if empty.
///
/// Uses [`f64::total_cmp`] so `NaN` cannot corrupt the ordering, and computes the
/// midpoint index with checked integer arithmetic to satisfy the workspace lints.
///
/// This copies `values` into a scratch buffer first; a caller that already owns a
/// buffer it no longer needs in input order should call [`median_in_place`] to
/// avoid the copy.
#[must_use]
pub fn median(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    median_in_place(&mut sorted)
}

/// The median of `values`, sorting them in place rather than copying.
///
/// The allocation-free core of [`median`]: it sorts `values` with
/// [`f64::total_cmp`] (so `NaN` cannot corrupt the ordering) and reads the
/// midpoint, leaving the slice sorted. Returns `None` for an empty slice.
///
/// The sort is **unstable** ([`sort_unstable_by`](slice::sort_unstable_by)): it
/// runs entirely in place with no scratch allocation, whereas the stable sort
/// allocates a temporary buffer of up to `values.len()` elements. The median
/// only depends on the sorted *values*, and two elements that compare
/// [`Ordering::Equal`] under [`f64::total_cmp`] are bit-identical, so reordering
/// ties cannot change the result.
pub fn median_in_place(values: &mut [f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    values.sort_unstable_by(f64::total_cmp);
    let len = values.len();
    let mid = len.checked_div(2)?;
    if len.checked_rem(2) == Some(1) {
        values.get(mid).copied()
    } else {
        let lower = mid.checked_sub(1)?;
        let low = *values.get(lower)?;
        let high = *values.get(mid)?;
        Some(f64::midpoint(low, high))
    }
}

/// The arithmetic mean of `values`, or `None` if empty.
#[must_use]
pub fn mean(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let total: f64 = values.iter().sum();
    Some(total / count_to_f64(values.len()))
}

/// The sample standard deviation of `values`, or `None` for fewer than two
/// points.
///
/// Bessel-corrected: the squared deviations are divided by `n − 1`, so this
/// estimates the scatter of the population the values were drawn from rather
/// than the scatter of the values themselves. Identical values legitimately
/// give `0.0`.
///
/// The deviations are formed against the mean rather than accumulated as
/// `E[x²] − E[x]²`, so values clustered within a few parts per million of a
/// large mean — benchmark timings, for one — keep their significant digits
/// instead of losing them to cancellation between two huge, nearly equal sums.
#[must_use]
pub fn sample_std_dev(values: &[f64]) -> Option<f64> {
    let n = values.len();
    if n < 2 {
        return None;
    }
    let mean = mean(values)?;
    let sum_of_squares: f64 = values
        .iter()
        .map(|&value| {
            let deviation = value - mean;
            deviation * deviation
        })
        .sum();
    Some((sum_of_squares / count_to_f64(n.saturating_sub(1))).sqrt())
}

/// Average (fractional) ranks of `values`, 1-based, with ties sharing the mean
/// of the ranks they span.
#[cfg(test)]
fn average_ranks(values: &[f64]) -> Vec<f64> {
    scaled_average_ranks(values)
        .into_iter()
        .map(|rank| count_to_f64(rank) / 2.0)
        .collect()
}

/// Doubled average ranks of `values`, in original order.
///
/// Average ranks are always whole or half integers. Doubling them keeps Pettitt
/// prefix sums and exact Mann-Whitney subset sums integral, so the observed
/// ordering and permutation calibration share one lossless representation.
pub(crate) fn scaled_average_ranks(values: &[f64]) -> Vec<usize> {
    let mut indexed: Vec<(usize, f64)> = values.iter().copied().enumerate().collect();
    // Unstable sort: ties are resolved explicitly below by spanning every element
    // of equal value, so the relative order within a tie run is irrelevant and the
    // in-place sort avoids the stable sort's scratch allocation.
    indexed.sort_unstable_by(|left, right| left.1.total_cmp(&right.1));

    let mut ranks = vec![0_usize; values.len()];
    let mut start = 0_usize;
    for group in indexed.chunk_by(|left, right| same(left.1, right.1)) {
        let end = start.saturating_add(group.len());
        // The 1-based ranks spanned by the tie run are `start+1 ..= end`; their
        // doubled mean is their first rank plus their last rank.
        let scaled_average = start.saturating_add(1).saturating_add(end);
        for &(original_index, _) in group {
            if let Some(slot) = ranks.get_mut(original_index) {
                *slot = scaled_average;
            }
        }
        start = end;
    }
    ranks
}

/// Sizes of the tie groups in `values` (groups of one are omitted as they do not
/// affect any tie correction).
fn tie_group_sizes(values: &[f64]) -> Vec<usize> {
    let mut sorted = values.to_vec();
    sorted.sort_unstable_by(f64::total_cmp);
    sorted
        .chunk_by(|left, right| same(*left, *right))
        .map(<[f64]>::len)
        .filter(|&size| size > 1)
        .collect()
}

/// A located level shift in a series.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ChangePoint {
    /// Index where the *after* regime begins (`points[index..]`), in `1..n`.
    pub index: usize,
    /// The Pettitt `K` statistic (larger means a more pronounced split).
    pub k_statistic: f64,
    /// The approximate two-sided significance of the split.
    pub p_value: f64,
}

/// Locates the single most likely level shift with the **Pettitt** nonparametric
/// change-point test.
///
/// Returns `None` for fewer than two points. The statistic is the rank form
/// `U_t = 2·R_t − t·(n+1)` (with `R_t` the sum of the first `t` average ranks);
/// `K = max_t |U_t|`, the change index is `argmax`, and the significance is
/// `p ≈ 2·exp(−6K²/(n³+n²))`, clamped into the reportable p-value range. The
/// first maximizing `t` wins, so a perfectly flat series reports a degenerate
/// split at index 1 that the caller rejects on a zero median difference.
#[must_use]
pub fn pettitt(values: &[f64]) -> Option<ChangePoint> {
    let n = values.len();
    let ranks = scaled_average_ranks(values);
    let (best_index, _, k) = pettitt_rank_location(&ranks)?;
    let n_f = count_to_f64(n);

    let denominator = n_f * n_f * n_f + n_f * n_f;
    let p_value = clamp_p_value(2.0 * (-6.0 * k * k / denominator).exp());
    Some(ChangePoint {
        index: best_index,
        k_statistic: k,
        p_value,
    })
}

/// Locates Pettitt's first maximum from doubled average ranks.
///
/// Returns `(split_index, scaled_rank_sum_left, k_statistic)`. The rank sum is
/// retained because the selection-adjustment scorer feeds the same chosen split
/// directly into Mann-Whitney without ranking again.
pub(crate) fn pettitt_rank_location(ranks: &[usize]) -> Option<(usize, usize, f64)> {
    let n = ranks.len();
    if n < 2 {
        return None;
    }
    let n_f = count_to_f64(n);
    let mut prefix_rank_sum = 0_usize;
    let mut best_index = 1_usize;
    let mut best_rank_sum = 0_usize;
    let mut best_abs = -1.0_f64;
    let last = n.checked_sub(1)?;
    for (position, &rank) in ranks.iter().enumerate() {
        prefix_rank_sum = prefix_rank_sum.saturating_add(rank);
        let split = position.saturating_add(1);
        if split > last {
            break;
        }
        // `prefix_rank_sum` is twice the ordinary rank sum, so this is exactly
        // Pettitt's `2*R_t - t*(n+1)` statistic.
        let u = count_to_f64(prefix_rank_sum) - count_to_f64(split) * (n_f + 1.0);
        let abs = u.abs();
        if abs > best_abs {
            best_abs = abs;
            best_index = split;
            best_rank_sum = prefix_rank_sum;
        }
    }
    Some((best_index, best_rank_sum, best_abs))
}

/// The Mann–Whitney U statistics of two samples, ranked jointly once.
///
/// Both the significance p-value and the probability-of-superiority effect size
/// derive from the same joint ranking, so a caller that needs both — as the
/// change-point and branch-comparison gates do — pays for the sort and allocation
/// a single time via [`MannWhitneyU::new`] instead of ranking the data twice.
/// Read [`two_sided_p_value`] for significance and [`superiority`] for effect
/// size.
///
/// [`two_sided_p_value`]: MannWhitneyU::two_sided_p_value
/// [`superiority`]: MannWhitneyU::superiority
#[derive(Clone, Copy, Debug)]
pub struct MannWhitneyU {
    /// The size of the `left` sample, as `f64`.
    n1: f64,
    /// The size of the `right` sample, as `f64`.
    n2: f64,
    /// U for `right`: the `(left, right)` pairs with `right > left`, ties as one
    /// half. Every cross-sample pair contributes one unit split between the two
    /// statistics, so this complements the left statistic to `n1·n2`.
    u2: f64,
    /// The two-sided significance, computed once at construction: the exact
    /// permutation tail whenever the split's central subset count fits f64 exactly
    /// (see [`exact_mw_feasible`]), the normal approximation otherwise.
    two_sided_p: f64,
}

/// Joint ranks and U statistic shared by Mann–Whitney outputs.
///
/// Ranking is the common expensive step for significance and superiority. This
/// internal result lets callers that need only the effect size avoid exact-tail
/// enumeration while [`MannWhitneyU`] reuses the same values for both outputs.
struct RankedMannWhitney {
    n1: usize,
    n2: usize,
    rank_sum_left: f64,
    u2: f64,
    scaled_ranks: Vec<usize>,
}

/// The largest integer through which f64 represents every integer exactly. The
/// exact permutation tail counts rank subsets as f64, so it is trustworthy only
/// while those counts stay at or below this. Ref:
/// `packages/cargo-bench-history/docs/DESIGN.md`, "Exact significance where feasible".
const EXACT_COUNT_LIMIT: u128 = 1 << 53;

/// Whether the exact two-sided Mann–Whitney tail is representable in f64 for a
/// split of these two sample sizes.
///
/// The tail enumerates the size-`min(n1, n2)` subsets of the joint ranking, of
/// which there are `C(n1+n2, min(n1, n2))`; f64 counts them exactly only below
/// [`EXACT_COUNT_LIMIT`], so the exact path is taken precisely when that central
/// count fits. The decision is per split, not per series: a long series still
/// earns the exact tail at a lopsided split, where one side has few points and the
/// count is small — and that is exactly where the tie- and imbalance-driven
/// deep-tail error of the normal approximation is worst, understating a repeated-
/// value split's p-value by orders of magnitude. Only near-balanced splits, whose
/// count overflows, keep the approximation, and there the smallest honest p-value
/// already sits below the reporting clamp, so the approximation cannot report a
/// dishonestly small verdict. Ref:
/// `packages/cargo-bench-history/docs/DESIGN.md`, "Exact significance where
/// feasible".
// Mutating this guard can route large samples into combinatorial exact enumeration.
// Detecting that noncompletion would require the real-time timeout tests forbid; the
// feasibility boundary itself is covered directly.
#[cfg_attr(test, mutants::skip)]
pub(crate) fn exact_mw_feasible(n1: usize, n2: usize) -> bool {
    let n = n1.saturating_add(n2);
    let k = n1.min(n2);
    // `C(n, k)` accumulated as the exact integer sequence `C(n-k+i, i)`, aborting the moment it
    // reaches the f64 ceiling. Each partial value is an integer, so the running division is exact.
    let mut count: u128 = 1;
    for i in 1..=k {
        #[expect(
            clippy::arithmetic_side_effects,
            clippy::integer_division,
            reason = "n >= k, so `n - k` cannot underflow; the running value is the integer \
                      binomial C(n-k+i, i), so dividing by i is exact; and count stays below \
                      2^53 until the early return, keeping the product far inside u128"
        )]
        {
            count = count * (n - k + i) as u128 / i as u128;
        }
        if count >= EXACT_COUNT_LIMIT {
            return false;
        }
    }
    true
}

impl MannWhitneyU {
    /// Ranks `left` and `right` jointly and captures their U statistics.
    ///
    /// Returns `None` when either sample is empty, since a rank comparison needs
    /// points on both sides.
    #[must_use]
    pub fn new(left: &[f64], right: &[f64]) -> Option<Self> {
        let ranked = rank_mann_whitney(left, right)?;
        let n1_f = count_to_f64(ranked.n1);
        let n2_f = count_to_f64(ranked.n2);

        // A split whose smaller side is few enough points earns the exact
        // permutation tail; a near-balanced long split keeps the normal
        // approximation. Ref: [`exact_mw_feasible`].
        let two_sided_p = if exact_mw_feasible(ranked.n1, ranked.n2) {
            exact_two_sided_p(&ranked.scaled_ranks, ranked.n1)
        } else {
            normal_mann_whitney_p(
                ranked.n1,
                ranked.n2,
                ranked.rank_sum_left,
                mann_whitney_tie_term(&ranked.scaled_ranks),
            )
        };

        Some(Self {
            n1: n1_f,
            n2: n2_f,
            u2: ranked.u2,
            two_sided_p,
        })
    }

    /// The two-sided p-value that the samples are drawn from the same distribution.
    ///
    /// Exact — the permutation tail over all `C(n1+n2, min(n1, n2))` rank splits,
    /// doubled for two sidedness — whenever that count fits f64 exactly; the tie-
    /// and continuity-corrected normal approximation otherwise. Returns `1.0` (no
    /// evidence of a difference) when every observation ties.
    #[must_use]
    pub fn two_sided_p_value(&self) -> f64 {
        self.two_sided_p
    }

    /// The **probability of superiority** — the common-language effect size.
    ///
    /// This is the chance that a value drawn at random from `right` exceeds one
    /// drawn at random from `left`, counting ties as one half. It is
    /// `U_right / (n_left · n_right)`, ranging from `0` (every `right` value below
    /// every `left` value) through `0.5` (the two samples fully interleave) to `1`
    /// (every `right` value above every `left` value).
    ///
    /// This is the *effect-size* companion to [`two_sided_p_value`]: the p-value
    /// says whether the two samples differ, and this says *how far apart* they are.
    /// Crucially it does **not** drift toward an extreme as the samples grow — two
    /// heavily overlapping populations keep a superiority near `0.5` however many
    /// points are sampled, whereas their difference becomes ever more
    /// "statistically significant". A separation gate therefore needs this, not the
    /// p-value, to tell a genuine level shift from a long but jittery series that
    /// merely oscillates between two levels.
    ///
    /// [`two_sided_p_value`]: MannWhitneyU::two_sided_p_value
    #[must_use]
    pub fn superiority(&self) -> f64 {
        self.u2 / (self.n1 * self.n2)
    }
}

/// Computes only the Mann–Whitney probability-of-superiority effect size.
///
/// Unlike [`MannWhitneyU::new`], this does not enumerate or approximate a
/// significance tail. Use it when a caller needs regime separation but another
/// procedure owns significance.
#[must_use]
pub fn mann_whitney_superiority(left: &[f64], right: &[f64]) -> Option<f64> {
    rank_mann_whitney(left, right).map(|ranked| ranked.superiority())
}

impl RankedMannWhitney {
    fn superiority(&self) -> f64 {
        self.u2 / (count_to_f64(self.n1) * count_to_f64(self.n2))
    }
}

fn rank_mann_whitney(left: &[f64], right: &[f64]) -> Option<RankedMannWhitney> {
    let n1 = left.len();
    let n2 = right.len();
    if n1 == 0 || n2 == 0 {
        return None;
    }
    let mut combined = Vec::with_capacity(n1.saturating_add(n2));
    combined.extend_from_slice(left);
    combined.extend_from_slice(right);
    let scaled_ranks = scaled_average_ranks(&combined);
    let rank_sum_left = count_to_f64(scaled_ranks.iter().take(n1).copied().sum::<usize>()) / 2.0;
    let n1_f = count_to_f64(n1);
    let n2_f = count_to_f64(n2);

    // `u1 = R_left − n1·(n1+1)/2` counts the `(left, right)` pairs with `left >
    // right` (ties as one half); the complementary `u2` counts `right > left`.
    let u1 = rank_sum_left - n1_f * (n1_f + 1.0) / 2.0;
    let u2 = n1_f * n2_f - u1;
    Some(RankedMannWhitney {
        n1,
        n2,
        rank_sum_left,
        u2,
        scaled_ranks,
    })
}

/// The exact two-sided Mann–Whitney p-value from doubled joint average ranks.
///
/// `scaled_ranks` holds twice the average ranks of the combined sample in
/// `left ++ right` order, so its first `n1` entries are the left ranks. Under the
/// null either group is a uniformly random subset of the joint ranks, so the
/// p-value is the tail of that group's rank sum over all such subsets, doubled
/// for two sidedness and capped at `1`. The enumeration runs over the *smaller* side: its subset count
/// `C(n1+n2, min(n1, n2))` is the one [`exact_mw_feasible`] bounds below
/// [`EXACT_COUNT_LIMIT`], so every intermediate subset count stays exact in f64 —
/// which a lopsided split would break if the larger side were enumerated, its
/// half-size subsets overflowing well before the reported answer. The two-sided
/// tail is symmetric in the two sides, so the smaller side yields the same p.
/// Every observation tying leaves one attainable sum, which
/// returns `1.0` — the same no-evidence answer the approximation gives for zero
/// variance.
fn exact_two_sided_p(scaled_ranks: &[usize], n1: usize) -> f64 {
    let n2 = scaled_ranks.len().saturating_sub(n1);
    // Enumerate the smaller side, whose subset count is the one held exact.
    let subset_size = n1.min(n2);
    let observed: usize = if n1 <= n2 {
        scaled_ranks
            .iter()
            .take(n1)
            .copied()
            .fold(0, usize::saturating_add)
    } else {
        scaled_ranks
            .iter()
            .skip(n1)
            .copied()
            .fold(0, usize::saturating_add)
    };
    exact_rank_sum_p_values(scaled_ranks, subset_size)
        .get(subset_size)
        .and_then(|row| row.get(observed))
        .copied()
        .unwrap_or(NO_EVIDENCE)
}

/// Exact two-sided Mann-Whitney p-values indexed by subset size and doubled rank sum.
///
/// Rows through `max_subset_size` are built together because the subset-sum
/// recurrence for a larger size necessarily builds every smaller size. Runtime
/// permutation calibration can therefore cache all exact split sizes after the
/// first permutation that needs one.
pub(crate) fn exact_rank_sum_p_values(
    scaled_ranks: &[usize],
    max_subset_size: usize,
) -> Vec<Vec<f64>> {
    // A size-`subset_size` subset reaches at most the sum of the `subset_size`
    // largest ranks, so the count grid needs no wider a sum axis than that — a tight
    // bound for a lopsided split, where the enumerated side is small.
    let mut sorted = scaled_ranks.to_vec();
    sorted.sort_unstable();
    let max_subset_sum: usize = sorted
        .iter()
        .rev()
        .take(max_subset_size)
        .copied()
        .fold(0, usize::saturating_add);

    // `subsets[taken][sum]` counts the size-`taken` subsets whose scaled rank sum
    // is `sum`. Folding the ranks in one at a time while walking `taken` downward
    // spends each rank at most once per subset (the 0/1-knapsack order).
    let width = max_subset_sum.saturating_add(1);
    let mut subsets: Vec<Vec<f64>> = vec![vec![0.0_f64; width]; max_subset_size.saturating_add(1)];
    if let Some(first) = subsets.first_mut().and_then(|row| row.first_mut()) {
        *first = 1.0;
    }
    // Each row is nonzero only between the smallest and largest sums reached so far. Tracking that
    // active range avoids scanning the full final sum axis for every rank, which matters most for
    // the exact lopsided splits of a capped 1,000-point series.
    let mut active_sums: Vec<Option<(usize, usize)>> =
        vec![None; max_subset_size.saturating_add(1)];
    if let Some(first) = active_sums.first_mut() {
        *first = Some((0, 0));
    }
    for rank in sorted {
        for taken in (0..max_subset_size).rev() {
            let Some((first_sum, last_sum)) = active_sums.get(taken).copied().flatten() else {
                continue;
            };
            let previous_target_range = active_sums.get(taken.saturating_add(1)).copied().flatten();
            let (lower, upper) = subsets.split_at_mut(taken.saturating_add(1));
            if let (Some(source), Some(target)) = (lower.get(taken), upper.first_mut()) {
                for sum in first_sum..=last_sum {
                    let count = source.get(sum).copied().unwrap_or(0.0);
                    if count == 0.0 {
                        continue;
                    }
                    if let Some(slot) = target.get_mut(sum.saturating_add(rank)) {
                        *slot += count;
                    }
                }
                let next_range = (
                    first_sum.saturating_add(rank),
                    last_sum.saturating_add(rank),
                );
                if let Some(target_range) = active_sums.get_mut(taken.saturating_add(1)) {
                    *target_range = Some(previous_target_range.map_or(next_range, |previous| {
                        (previous.0.min(next_range.0), previous.1.max(next_range.1))
                    }));
                }
            }
        }
    }

    subsets
        .into_iter()
        .map(|counts| exact_tail_p_values(&counts))
        .collect()
}

/// Doubled minority-tail p-values for every attainable sum in one exact distribution.
fn exact_tail_p_values(counts: &[f64]) -> Vec<f64> {
    let total: f64 = counts.iter().sum();
    if total <= 0.0 {
        return vec![NO_EVIDENCE; counts.len()];
    }
    let mut cumulative = 0.0_f64;
    counts
        .iter()
        .map(|&count| {
            cumulative += count;
            let lower = cumulative;
            let upper = total - cumulative + count;
            clamp_p_value((2.0 * lower.min(upper) / total).min(1.0))
        })
        .collect()
}

/// The Mann-Whitney tie-correction term from doubled average ranks.
///
/// The value depends only on the rank multiset, so permutation calibration
/// computes it once and reuses it for every permuted ordering.
pub(crate) fn mann_whitney_tie_term(scaled_ranks: &[usize]) -> f64 {
    let mut sorted = scaled_ranks.to_vec();
    sorted.sort_unstable();
    sorted
        .chunk_by(|left, right| left == right)
        .map(|group| {
            let size = count_to_f64(group.len());
            size * size * size - size
        })
        .sum()
}

/// The tie- and continuity-corrected normal Mann-Whitney p-value from one rank sum.
///
/// Returns `1.0` when every observation ties and the corrected variance is zero.
pub(crate) fn normal_mann_whitney_p(
    n1: usize,
    n2: usize,
    rank_sum_left: f64,
    tie_term: f64,
) -> f64 {
    let n1_f = count_to_f64(n1);
    let n2_f = count_to_f64(n2);
    let n_f = n1_f + n2_f;
    let u1 = rank_sum_left - n1_f * (n1_f + 1.0) / 2.0;
    let u2 = n1_f * n2_f - u1;
    let u = u1.min(u2);
    let mean_u = n1_f * n2_f / 2.0;
    let variance = (n1_f * n2_f / 12.0) * ((n_f + 1.0) - tie_term / (n_f * (n_f - 1.0)));
    if variance <= 0.0 {
        return NO_EVIDENCE;
    }

    // Continuity-corrected z; `u` is the smaller statistic so `mean_u - u >= 0`.
    let z = ((mean_u - u) - 0.5).max(0.0) / variance.sqrt();
    two_sided_p_from_z(z)
}

/// The two-sided p-value of the **Mann–Whitney U** test that `left` and `right`
/// are drawn from the same distribution.
///
/// A convenience over [`MannWhitneyU`] for callers that need only the significance
/// and not the effect size; returns `1.0` (no evidence of a difference) when
/// either sample is empty. See [`MannWhitneyU::two_sided_p_value`] for the
/// approximation used.
#[must_use]
pub fn mann_whitney_u_pvalue(left: &[f64], right: &[f64]) -> f64 {
    MannWhitneyU::new(left, right).map_or(1.0, |mann_whitney| mann_whitney.two_sided_p_value())
}

/// The outcome of a Mann–Kendall trend test.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct MannKendall {
    /// The `S` statistic (positive for an upward trend, negative for downward).
    pub s: f64,
    /// The two-sided significance of the trend.
    pub p_value: f64,
}

/// The **Mann–Kendall** test for a monotonic trend in `values` (time order is the
/// slice order), tie-corrected with a continuity correction on `Z`.
///
/// Returns `S = 0` and `p = 1.0` for fewer than three points or zero variance.
#[must_use]
pub fn mann_kendall(values: &[f64]) -> MannKendall {
    let n = values.len();
    if n < 3 {
        return MannKendall {
            s: 0.0,
            p_value: NO_EVIDENCE,
        };
    }
    let mut s = 0.0_f64;
    for (i, &earlier) in values.iter().enumerate() {
        for &later in values.iter().skip(i.saturating_add(1)) {
            s += match later.total_cmp(&earlier) {
                Ordering::Greater => 1.0,
                Ordering::Less => -1.0,
                Ordering::Equal => 0.0,
            };
        }
    }

    let n_f = count_to_f64(n);
    let tie_term: f64 = tie_group_sizes(values)
        .into_iter()
        .map(|size| {
            let t = count_to_f64(size);
            t * (t - 1.0) * (2.0 * t + 5.0)
        })
        .sum();
    let variance = (n_f * (n_f - 1.0) * (2.0 * n_f + 5.0) - tie_term) / 18.0;
    if variance <= 0.0 {
        return MannKendall {
            s,
            p_value: NO_EVIDENCE,
        };
    }

    let z = if s > 0.0 {
        (s - 1.0) / variance.sqrt()
    } else if s < 0.0 {
        (s + 1.0) / variance.sqrt()
    } else {
        0.0
    };
    MannKendall {
        s,
        p_value: two_sided_p_from_z(z),
    }
}

/// The **Theil–Sen** robust line `(slope, intercept)` fitted to `values` against
/// their integer positions, or `None` for fewer than two points.
///
/// The slope is the median of all pairwise slopes; the intercept is the median of
/// `value_i − slope·i`, so the fitted endpoints are `intercept` and
/// `intercept + slope·(n−1)`.
#[must_use]
pub fn theil_sen_line(values: &[f64]) -> Option<(f64, f64)> {
    let n = values.len();
    if n < 2 {
        return None;
    }
    let mut slopes = Vec::with_capacity(pair_count(n));
    for (i, &earlier) in values.iter().enumerate() {
        let i_f = count_to_f64(i);
        for (j, &later) in values.iter().enumerate().skip(i.saturating_add(1)) {
            let span = count_to_f64(j) - i_f;
            slopes.push((later - earlier) / span);
        }
    }
    let slope = median_in_place(&mut slopes)?;

    let mut intercepts: Vec<f64> = values
        .iter()
        .enumerate()
        .map(|(i, &value)| value - slope * count_to_f64(i))
        .collect();
    let intercept = median_in_place(&mut intercepts)?;
    Some((slope, intercept))
}

/// Applies the **Benjamini–Hochberg** procedure to `p_values` at false-discovery
/// rate `q`, returning a keep-mask (in the input order) of the rejected (kept)
/// hypotheses.
///
/// Finds the largest rank `k` whose ordered p-value satisfies `p_(k) ≤ (k/m)·q`
/// and rejects every hypothesis of rank `≤ k` (the step-up property: an
/// intermediate rank that fails its own threshold is still rejected when a later
/// rank passes). Ranks are assigned within `p_values`, so rank 1 is the smallest
/// p-value handed over.
///
/// `q` is the false-discovery rate targeted across the whole family of
/// hypotheses tested, so `family_size` — the `m` of the threshold — must be the
/// number of hypotheses examined, counting every one that produced no candidate
/// finding. Only the caller knows that number: `p_values` carries the
/// hypotheses whose statistic is worth ranking, which is a subset whenever the
/// caller screens its candidates first. Passing the length of that subset
/// controls nothing when the screen is stricter than `q`, because the loosest
/// threshold `(m/m)·q = q` is then cleared by every p-value handed in and the
/// whole subset is rejected unconditionally.
///
/// # Panics
///
/// Panics if `family_size` is smaller than `p_values.len()`. Every p-value is
/// the verdict on a hypothesis the caller examined, so the family examined
/// always contains at least the hypotheses that produced p-values; a smaller
/// one is a miscounted census rather than a stricter correction, and correcting
/// it here would silently restore the unconditional pass described above.
#[must_use]
pub fn benjamini_hochberg(p_values: &[f64], q: f64, family_size: usize) -> Vec<bool> {
    let tested = p_values.len();
    assert!(
        family_size >= tested,
        "every p-value is the verdict on an examined hypothesis, so the examined \
         family contains at least the hypotheses that produced these p-values"
    );
    let m_f = count_to_f64(family_size);

    let mut ordered: Vec<(usize, f64)> = p_values.iter().copied().enumerate().collect();
    // Unstable sort: equal p-values are interchangeable for the step-up cutoff (a
    // tie can never fall across the `(k/m)·q` boundary, since a smaller rank passing
    // implies the equal larger rank passes too), so reordering ties cannot change
    // the keep-set, and the in-place sort skips the stable sort's allocation.
    ordered.sort_unstable_by(|left, right| left.1.total_cmp(&right.1));

    // The largest 1-based rank whose ordered p-value clears `(k/m)·q`.
    let mut max_rank = 0_usize;
    for (position, &(_, p)) in ordered.iter().enumerate() {
        let rank = position.saturating_add(1);
        let threshold = count_to_f64(rank) / m_f * q;
        if p <= threshold {
            max_rank = rank;
        }
    }

    let mut keep = vec![false; tested];
    for &(original_index, _) in ordered.iter().take(max_rank) {
        if let Some(slot) = keep.get_mut(original_index) {
            *slot = true;
        }
    }
    keep
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    #![allow(
        clippy::float_cmp,
        reason = "primitive outputs are compared against hand-computed exact values"
    )]
    #![allow(clippy::indexing_slicing, reason = "panic is fine in tests")]

    use super::*;
    use crate::test_util::close;

    /// The number of hypotheses a keep-mask rejects (i.e. reports as findings).
    fn keep_count(keep: &[bool]) -> usize {
        keep.iter().filter(|&&kept| kept).count()
    }

    #[test]
    fn mean_of_hand_computed_samples() {
        assert_eq!(mean(&[1.0, 2.0, 3.0]), Some(2.0));
        assert_eq!(mean(&[2.5]), Some(2.5));
        assert_eq!(mean(&[-4.0, 4.0]), Some(0.0));
        // The mean of an even count need not be one of the values.
        assert_eq!(mean(&[1.0, 2.0]), Some(1.5));
        assert_eq!(mean(&[]), None);
    }

    #[test]
    fn sample_std_dev_of_hand_computed_samples() {
        // Deviations −2, 0, 2 about a mean of 4: 8/(3−1) = 4, so the scatter is 2.
        assert_eq!(sample_std_dev(&[2.0, 4.0, 6.0]), Some(2.0));
        // Deviations ±3 about a mean of 10: 18/(2−1) = 18.
        assert_eq!(sample_std_dev(&[7.0, 13.0]), Some(18.0_f64.sqrt()));
    }

    #[test]
    fn sample_std_dev_corrects_for_the_sample_being_a_sample() {
        // Deviations ±1 about a mean of 2: the squared deviations sum to 4, and
        // dividing by 4 − 1 rather than 4 is the whole difference between
        // estimating the population behind these values and merely describing
        // them. The population formula would report exactly 1.0.
        assert_eq!(
            sample_std_dev(&[1.0, 1.0, 3.0, 3.0]),
            Some((4.0_f64 / 3.0).sqrt())
        );
    }

    #[test]
    fn sample_std_dev_is_zero_for_identical_values() {
        // A degenerate sample has genuinely no scatter, which the caller must be
        // able to see rather than have papered over.
        assert_eq!(sample_std_dev(&[4.0, 4.0, 4.0]), Some(0.0));
    }

    #[test]
    fn sample_std_dev_needs_two_points() {
        assert_eq!(sample_std_dev(&[]), None);
        assert_eq!(sample_std_dev(&[42.0]), None);
        // Two points are the smallest sample that has any scatter to estimate.
        assert_eq!(sample_std_dev(&[41.0, 43.0]), Some(2.0_f64.sqrt()));
    }

    #[test]
    fn sample_std_dev_keeps_its_precision_on_a_large_offset() {
        // Values a billion wide apart by ones: the deviations about the mean are
        // exactly −1, 0 and 1, so the scatter is exactly 1. Accumulating
        // `E[x²] − E[x]²` instead would subtract two numbers near 1e18 that agree
        // to sixteen digits and keep only noise.
        assert_eq!(sample_std_dev(&[1e9, 1e9 + 1.0, 1e9 + 2.0]), Some(1.0));

        // The same sample shifted to where `f64` spacing is coarser still holds,
        // because the deviations are formed before anything is squared.
        assert_eq!(sample_std_dev(&[1e15, 1e15 + 1.0, 1e15 + 2.0]), Some(1.0));
    }

    #[test]
    fn median_of_odd_and_even_counts() {
        assert_eq!(median(&[3.0, 1.0, 2.0]), Some(2.0));
        assert_eq!(median(&[1.0, 2.0, 3.0, 4.0]), Some(2.5));
        assert_eq!(median(&[]), None);
    }

    #[test]
    fn average_ranks_share_ranks_across_ties() {
        // Three 1s span ranks 1..3 → 2.0; three 5s span 4..6 → 5.0.
        assert_eq!(
            average_ranks(&[1.0, 1.0, 1.0, 5.0, 5.0, 5.0]),
            vec![2.0, 2.0, 2.0, 5.0, 5.0, 5.0]
        );
        // Distinct values keep position-independent ranks.
        assert_eq!(average_ranks(&[30.0, 10.0, 20.0]), vec![3.0, 1.0, 2.0]);
    }

    #[test]
    fn tie_group_sizes_omits_singletons() {
        assert_eq!(tie_group_sizes(&[1.0, 1.0, 2.0, 3.0, 3.0, 3.0]), vec![2, 3]);
        assert_eq!(tie_group_sizes(&[1.0, 2.0, 3.0]), Vec::<usize>::new());
    }

    #[test]
    fn pettitt_locates_a_clean_step() {
        // A clean step from 1 to 5 after three points: K = 9 at index 3.
        let change = pettitt(&[1.0, 1.0, 1.0, 5.0, 5.0, 5.0]).unwrap();
        assert_eq!(change.index, 3);
        assert_eq!(change.k_statistic, 9.0);
        // p ≈ 2·exp(−6·81/252) ≈ 0.291 — deliberately not tiny, which is why a
        // clean step must be confirmed by a second test (Mann–Whitney), never by
        // Pettitt's p-value alone.
        close(change.p_value, 0.291, 1e-3);
    }

    #[test]
    fn pettitt_step_is_at_the_boundary_for_an_asymmetric_split() {
        // Step after the second point: before [10,10], after [40,40,40,40].
        let change = pettitt(&[10.0, 10.0, 40.0, 40.0, 40.0, 40.0]).unwrap();
        assert_eq!(change.index, 2);
    }

    #[test]
    fn pettitt_flat_series_degenerates_at_index_one() {
        let change = pettitt(&[5.0, 5.0, 5.0, 5.0]).unwrap();
        assert_eq!(change.index, 1);
        assert_eq!(change.k_statistic, 0.0);
        assert_eq!(change.p_value, 1.0);
    }

    #[test]
    fn pettitt_needs_two_points() {
        assert_eq!(pettitt(&[]), None);
        assert_eq!(pettitt(&[1.0]), None);
    }

    #[test]
    fn pettitt_handles_the_two_point_minimum() {
        // Two ascending points are the smallest series Pettitt accepts: the only
        // split is at index 1 with U_1 = 2·1 − 1·3 = −1, so K = 1 (and p clamps to
        // 1.0). This pins both the `n < 2` lower bound and the `best_abs` seed.
        let change = pettitt(&[1.0, 2.0]).unwrap();
        assert_eq!(change.index, 1);
        assert_eq!(change.k_statistic, 1.0);
        assert_eq!(change.p_value, 1.0);
    }

    #[test]
    fn mann_whitney_separates_disjoint_samples() {
        // Fully separated samples of five: only the one split that puts all five
        // lows on the left reaches this rank sum, so the exact two-sided p is
        // 2 / C(10, 5) = 2/252 = 1/126. The normal approximation reports ≈0.0122
        // here, over 50% larger — the deep-tail pessimism the exact tail removes.
        let p = mann_whitney_u_pvalue(&[1.0, 2.0, 3.0, 4.0, 5.0], &[11.0, 12.0, 13.0, 14.0, 15.0]);
        close(p, 1.0 / 126.0, 1e-12);
    }

    #[test]
    fn mann_whitney_identical_samples_are_indistinguishable() {
        // Equal samples → variance zero → p = 1.0.
        let p = mann_whitney_u_pvalue(&[5.0, 5.0, 5.0], &[5.0, 5.0, 5.0]);
        assert_eq!(p, 1.0);
    }

    #[test]
    fn mann_whitney_empty_sample_is_one() {
        assert_eq!(mann_whitney_u_pvalue(&[], &[1.0, 2.0]), 1.0);
        assert_eq!(mann_whitney_u_pvalue(&[1.0, 2.0], &[]), 1.0);
    }

    #[test]
    fn mann_whitney_exact_feasibility_tracks_the_binomial_count_limit() {
        // The production cap's 6-vs-994 split has C(1000, 6) subsets below the exact
        // integer limit, while adding one point to the smaller side crosses it.
        assert!(exact_mw_feasible(6, 994));
        assert!(!exact_mw_feasible(7, 993));
        assert!(exact_mw_feasible(994, 6));

        // A balanced 29-vs-29 split is the first equal split whose central binomial
        // coefficient exceeds the exact integer limit.
        assert!(exact_mw_feasible(28, 28));
        assert!(!exact_mw_feasible(29, 29));
    }

    #[test]
    fn mann_whitney_tie_term_sums_cubic_group_contributions() {
        // Tie groups of three, two, and one contribute (27 - 3) + (8 - 2) + 0.
        assert_eq!(mann_whitney_tie_term(&[2, 8, 2, 10, 8, 2]), 30.0);
    }

    #[test]
    fn normal_mann_whitney_matches_a_hand_computed_tied_case() {
        // n1=8, n2=12, left rank sum 60, and tie term 30 give U=24,
        // variance=3180/19, and continuity-corrected z=1.8164820170425562. The
        // complementary rank sum 108 makes the other U statistic the smaller one.
        let expected = 0.069_296_464_029_822_27;
        close(normal_mann_whitney_p(8, 12, 60.0, 30.0), expected, 1e-12);
        close(normal_mann_whitney_p(8, 12, 108.0, 30.0), expected, 1e-12);
    }

    #[test]
    fn mann_whitney_with_ties_uses_the_smaller_u_statistic() {
        // `right` sits below `left`, and the repeated 4s/2s make the average ranks
        // half-integral, so the exact tail runs over the fractional ranks. Two of
        // the 70 size-four splits reach a rank sum at or below the observed one, so
        // the exact two-sided p is 2 · 2 / C(8, 4) = 4/70 = 2/35.
        let p = mann_whitney_u_pvalue(&[3.0, 4.0, 4.0, 5.0], &[1.0, 2.0, 2.0, 3.0]);
        close(p, 2.0 / 35.0, 1e-12);
    }

    #[test]
    fn mann_whitney_superiority_is_one_when_right_dominates() {
        // Every `right` value exceeds every `left` value: all 3×3 pairs favour
        // `right`, so the probability of superiority is exactly 1.
        let s = mann_whitney_superiority(&[1.0, 2.0, 3.0], &[4.0, 5.0, 6.0])
            .expect("both samples are nonempty");
        assert_eq!(s, 1.0);
    }

    #[test]
    fn mann_whitney_superiority_is_zero_when_right_is_dominated() {
        // The mirror image: no `right` value exceeds any `left` value, so the
        // probability of superiority is exactly 0.
        let s = MannWhitneyU::new(&[4.0, 5.0, 6.0], &[1.0, 2.0, 3.0])
            .unwrap()
            .superiority();
        assert_eq!(s, 0.0);
    }

    #[test]
    fn mann_whitney_superiority_counts_ties_as_one_half() {
        // Identical samples: every pair is a tie, each counting one half, so the
        // probability of superiority is exactly 0.5 — the "indistinguishable" value.
        let s = MannWhitneyU::new(&[5.0, 5.0], &[5.0, 5.0])
            .unwrap()
            .superiority();
        assert_eq!(s, 0.5);
    }

    #[test]
    fn mann_whitney_superiority_measures_partial_overlap() {
        // `right` = {2, 4} against `left` = {1, 3}: pairs (2>1), (4>1), (4>3) favour
        // right and (2<3) favours left, so 3 of 4 pairs favour right → 0.75. This is
        // the interleaving case a stationary-but-noisy series produces.
        let s =
            mann_whitney_superiority(&[1.0, 3.0], &[2.0, 4.0]).expect("both samples are nonempty");
        assert_eq!(s, 0.75);
    }

    #[test]
    fn mann_whitney_superiority_does_not_drift_with_sample_size() {
        // The effect size is invariant to how many times each level is sampled: two
        // fully interleaved two-level populations keep a superiority of 0.5 whether
        // sampled sparsely or repeatedly, even though the *p-value* would grow
        // significant. This is exactly why a separation gate needs the effect size,
        // not the p-value.
        // Doubling the repeated two-level sample proves size invariance under Miri;
        // the wider native sample retains coverage of the larger exact rank table.
        const LARGE_SAMPLE_SIZE: usize = if cfg!(miri) { 4 } else { 20 };
        let small = MannWhitneyU::new(&[10.0, 20.0], &[10.0, 20.0])
            .unwrap()
            .superiority();
        let large_left: Vec<f64> = [10.0, 20.0]
            .iter()
            .copied()
            .cycle()
            .take(LARGE_SAMPLE_SIZE)
            .collect();
        let large_right = large_left.clone();
        let large = MannWhitneyU::new(&large_left, &large_right)
            .unwrap()
            .superiority();
        assert_eq!(small, 0.5);
        assert_eq!(large, 0.5);
    }

    #[test]
    fn mann_whitney_empty_sample_is_none() {
        // A rank comparison needs points on both sides; an empty sample yields no
        // statistics at all, so neither the p-value nor the effect size exists.
        assert!(MannWhitneyU::new(&[], &[1.0, 2.0]).is_none());
        assert!(MannWhitneyU::new(&[1.0, 2.0], &[]).is_none());
        assert!(mann_whitney_superiority(&[], &[1.0, 2.0]).is_none());
        assert!(mann_whitney_superiority(&[1.0, 2.0], &[]).is_none());
    }

    /// Every size-`k` combination of the indices `0..n`, for the brute-force tail.
    fn index_combinations(n: usize, k: usize) -> Vec<Vec<usize>> {
        fn extend(
            start: usize,
            n: usize,
            k: usize,
            current: &mut Vec<usize>,
            out: &mut Vec<Vec<usize>>,
        ) {
            if current.len() == k {
                out.push(current.clone());
                return;
            }
            for index in start..n {
                current.push(index);
                extend(index.saturating_add(1), n, k, current, out);
                current.pop();
            }
        }
        let mut out = Vec::new();
        extend(0, n, k, &mut Vec::new(), &mut out);
        out
    }

    /// An independent exact two-sided p — the same doubled minority tail, but read
    /// off an explicit enumeration of every size-`n1` rank split rather than the
    /// production subset-sum recurrence, so the two paths cross-check.
    fn brute_two_sided_p(left: &[f64], right: &[f64]) -> f64 {
        let n1 = left.len();
        let mut combined = left.to_vec();
        combined.extend_from_slice(right);
        let ranks = average_ranks(&combined);
        let observed: f64 = ranks.iter().take(n1).sum();
        let mut total = 0.0_f64;
        let mut lower = 0.0_f64;
        let mut upper = 0.0_f64;
        for combo in index_combinations(combined.len(), n1) {
            let sum: f64 = combo.iter().map(|&index| ranks[index]).sum();
            total += 1.0;
            if sum <= observed + 1e-9 {
                lower += 1.0;
            }
            if sum >= observed - 1e-9 {
                upper += 1.0;
            }
        }
        (2.0 * lower.min(upper) / total).min(1.0)
    }

    #[test]
    fn mann_whitney_exact_matches_brute_force_with_ties() {
        // Hand-authored samples with heavy ties pit the subset-sum tail against the
        // independent enumeration, and check the p-value is symmetric in its two
        // arguments (the null does not privilege a side).
        // Distinct values on the left against tied values on the right exercise the
        // same rank handling in a smaller Miri orbit; native keeps the wider enumeration.
        let mixed_ties: (&[f64], &[f64]) = if cfg!(miri) {
            (&[1.0, 2.0, 3.0], &[2.0, 2.0, 4.0])
        } else {
            (
                &[1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
                &[3.0, 3.0, 3.0, 7.0, 8.0, 9.0],
            )
        };
        let cases: &[(&[f64], &[f64])] = &[
            (&[1.0, 2.0, 3.0], &[4.0, 5.0, 6.0]),
            (&[1.0, 1.0, 2.0, 3.0], &[2.0, 3.0, 3.0, 4.0]),
            (&[5.0, 5.0, 5.0], &[1.0, 2.0, 5.0]),
            (&[1.0, 2.0, 2.0, 3.0, 3.0], &[2.0, 3.0, 3.0, 4.0, 5.0]),
            (&[10.0, 10.0, 10.0, 10.0], &[10.0, 10.0, 10.0, 20.0]),
            mixed_ties,
        ];
        for &(left, right) in cases {
            let actual = mann_whitney_u_pvalue(left, right);
            close(actual, brute_two_sided_p(left, right), 1e-12);
            close(actual, mann_whitney_u_pvalue(right, left), 1e-12);
        }
    }

    #[test]
    fn mann_whitney_exact_complete_separation_matches_the_closed_form() {
        // With every left point below every right point only the one extreme split
        // reaches the observed rank sum, so the exact two-sided p is 2 / C(2r, r):
        // C(6,3)=20, C(10,5)=252, C(16,8)=12870.
        close(
            mann_whitney_u_pvalue(&[1.0, 2.0, 3.0], &[4.0, 5.0, 6.0]),
            2.0 / 20.0,
            1e-12,
        );
        close(
            mann_whitney_u_pvalue(&[1.0, 2.0, 3.0, 4.0, 5.0], &[6.0, 7.0, 8.0, 9.0, 10.0]),
            2.0 / 252.0,
            1e-12,
        );
        let left: Vec<f64> = (1..=8).map(f64::from).collect();
        let right: Vec<f64> = (9..=16).map(f64::from).collect();
        close(mann_whitney_u_pvalue(&left, &right), 2.0 / 12870.0, 1e-12);
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "the wide exact split builds a large subset-sum table; small closed-form cases cover Miri"
    )]
    fn mann_whitney_exact_wide_separation_matches_the_closed_form() {
        // A balanced N = 52 split enumerates 26 per side; C(52, 26) stays inside
        // f64's exact-integer range, so the closed form still holds.
        let left: Vec<f64> = (1..=26).map(f64::from).collect();
        let right: Vec<f64> = (27..=52).map(f64::from).collect();
        close(
            mann_whitney_u_pvalue(&left, &right),
            2.0 / 495_918_532_948_104.0,
            1e-20,
        );
    }

    #[test]
    fn mann_whitney_switches_to_the_normal_approximation_for_a_balanced_wide_split() {
        // A balanced N = 58 split enumerates 29 per side; C(58, 29) overflows f64's
        // exact-integer range, so the normal approximation runs. A clean separation
        // there still yields a vanishingly small p, confirming the else branch is
        // reached and stays on the significant side.
        let left: Vec<f64> = (1..=29).map(f64::from).collect();
        let right: Vec<f64> = (30..=58).map(f64::from).collect();
        let p = mann_whitney_u_pvalue(&left, &right);
        assert!(
            p > 0.0 && p < 1e-6,
            "normal-approx clean-separation p = {p}"
        );
    }

    #[test]
    fn mann_whitney_uses_the_exact_tail_for_a_lopsided_wide_split() {
        // The total is past the balanced feasibility limit, yet the smaller side's
        // subset count fits f64 exactly and the exact tail runs. On complete
        // separation it must report the discrete `2 / C(n, k)`, not the normal
        // approximation's tie-shrunken value, which understates it by millions and
        // would let the split masquerade as astronomically significant.
        const TOTAL_SIZE: usize = 57;
        // Miri keeps a nontrivial minority subset while avoiding the wider exact table.
        const MINORITY_SIZE: usize = if cfg!(miri) { 2 } else { 5 };
        let lows = vec![1.0_f64; MINORITY_SIZE];
        let highs = vec![2.0_f64; TOTAL_SIZE - MINORITY_SIZE];
        // C(57, 2) and C(57, 5), independently calculated for the respective fixtures.
        let subset_count = if cfg!(miri) { 1_596.0 } else { 4_187_106.0 };
        let exact = 2.0 / subset_count;
        close(mann_whitney_u_pvalue(&lows, &highs), exact, 1e-18);
    }

    #[test]
    fn mann_whitney_exact_p_shrinks_as_separation_grows() {
        // The doubled minority tail is monotone in separation: pulling the two
        // samples fully apart cannot make the split look less surprising.
        let overlapping =
            mann_whitney_u_pvalue(&[1.0, 2.0, 3.0, 4.0, 5.0], &[3.0, 4.0, 5.0, 6.0, 7.0]);
        let separated =
            mann_whitney_u_pvalue(&[1.0, 2.0, 3.0, 4.0, 5.0], &[6.0, 7.0, 8.0, 9.0, 10.0]);
        assert!(
            separated < overlapping,
            "separated {separated} < overlapping {overlapping}"
        );
    }

    #[test]
    fn mann_kendall_detects_a_monotonic_increase() {
        // Strictly increasing six points: S = 15, z ≈ 2.630, p ≈ 0.0085.
        let result = mann_kendall(&[1.0, 2.0, 3.0, 4.0, 5.0, 6.0]);
        assert_eq!(result.s, 15.0);
        close(result.p_value, 0.0085, 1e-3);
    }

    #[test]
    fn mann_kendall_is_sign_symmetric() {
        let up = mann_kendall(&[1.0, 2.0, 3.0, 4.0, 5.0, 6.0]);
        let down = mann_kendall(&[6.0, 5.0, 4.0, 3.0, 2.0, 1.0]);
        assert_eq!(down.s, -15.0);
        close(up.p_value, down.p_value, 1e-9);
    }

    #[test]
    fn mann_kendall_flat_series_has_no_trend() {
        let result = mann_kendall(&[5.0, 5.0, 5.0, 5.0]);
        assert_eq!(result.s, 0.0);
        assert_eq!(result.p_value, 1.0);
    }

    #[test]
    fn mann_kendall_needs_three_points() {
        assert_eq!(
            mann_kendall(&[1.0, 2.0]),
            MannKendall {
                s: 0.0,
                p_value: 1.0
            }
        );
    }

    #[test]
    fn mann_kendall_scores_the_three_point_minimum() {
        // Three ascending points are the smallest series the trend test scores:
        // all three pairs rise, so S = 3 (a flipped `n < 3` bound would short out
        // to S = 0).
        let result = mann_kendall(&[1.0, 2.0, 3.0]);
        assert_eq!(result.s, 3.0);
        close(result.p_value, 0.296_269_871_484_286_46, 1e-9);
    }

    #[test]
    fn mann_kendall_zero_trend_with_variance_is_insignificant() {
        // A non-monotonic series with no ties has S = 0 but positive variance, so
        // it reaches the sign branch with z = 0 → p ≈ 1.0. A `>`/`<` boundary slip
        // on the sign test would feed z = ∓1/σ and collapse the p-value.
        let result = mann_kendall(&[2.0, 4.0, 1.0, 3.0]);
        assert_eq!(result.s, 0.0);
        close(result.p_value, 1.0, 1e-6);
    }

    #[test]
    fn mann_kendall_tie_correction_shrinks_variance() {
        // Repeated endpoints (two 1s, two 3s) engage the tie correction; the exact
        // tie-corrected p pins the `t·(t−1)·(2t+5)` term.
        let result = mann_kendall(&[1.0, 1.0, 2.0, 3.0, 3.0]);
        assert_eq!(result.s, 8.0);
        close(result.p_value, 0.067_577_263_055_870_57, 1e-9);
    }

    #[test]
    fn mann_kendall_tie_correction_grows_with_group_size() {
        // A tie group of three, where `t·(t−1)·(2t+5)` first becomes
        // distinguishable from its neighbouring forms: pairs of tied values
        // cannot tell `t·(t−1)` from `t/(t−1)`, nor `2t+5` from `2+t+5`.
        let result = mann_kendall(&[1.0, 1.0, 1.0, 2.0, 3.0]);
        assert_eq!(result.s, 7.0);
        close(result.p_value, 0.096_092_329_455_673_28, 1e-9);
    }

    #[test]
    fn theil_sen_fits_a_line() {
        // y = x: slope 1, intercept 1 (positions 0..4, values 1..5).
        assert_eq!(theil_sen_line(&[1.0, 2.0, 3.0, 4.0, 5.0]), Some((1.0, 1.0)));
        // Decreasing by two each step.
        assert_eq!(
            theil_sen_line(&[10.0, 8.0, 6.0, 4.0, 2.0]),
            Some((-2.0, 10.0))
        );
    }

    #[test]
    fn theil_sen_resists_a_single_outlier() {
        // One wild point cannot drag the median-of-slopes off the true unit slope.
        let (slope, _intercept) = theil_sen_line(&[1.0, 2.0, 3.0, 999.0, 5.0]).unwrap();
        assert_eq!(slope, 1.0);
    }

    #[test]
    fn theil_sen_needs_two_points() {
        assert_eq!(theil_sen_line(&[1.0]), None);
    }

    #[test]
    fn theil_sen_fits_the_two_point_minimum() {
        // Two points are the smallest line the fit accepts: slope (5−2)/1 = 3,
        // intercept 2. This pins the `n < 2` lower bound (a slipped `==`/`<=`
        // boundary would reject this valid two-point series).
        assert_eq!(theil_sen_line(&[2.0, 5.0]), Some((3.0, 2.0)));
    }

    #[test]
    fn benjamini_hochberg_keeps_the_significant_prefix() {
        // sorted [0.01,0.02,0.5], q=0.1: ranks 1,2 clear k/m·q; rank 3 fails.
        assert_eq!(
            benjamini_hochberg(&[0.01, 0.02, 0.5], 0.1, 3),
            vec![true, true, false]
        );
    }

    #[test]
    fn benjamini_hochberg_step_up_rejects_through_a_failing_rank() {
        // sorted [0.001,0.03,0.031,0.049], q=0.05: rank 2 (0.03 > 0.025) fails its
        // own threshold, but rank 4 (0.049 ≤ 0.05) passes, so ALL are rejected.
        assert_eq!(
            benjamini_hochberg(&[0.001, 0.03, 0.031, 0.049], 0.05, 4),
            vec![true, true, true, true]
        );
    }

    #[test]
    fn benjamini_hochberg_step_up_reaches_back_across_a_larger_family() {
        // Three p-values out of a family of ten, q=0.1: thresholds are 0.01·k.
        // Rank 2 (0.025 > 0.02) fails, yet rank 3 (0.0299 ≤ 0.03) passes, so the
        // step-up still reaches back over the failing rank.
        assert_eq!(
            benjamini_hochberg(&[0.002, 0.025, 0.0299], 0.1, 10),
            vec![true, true, true]
        );
    }

    #[test]
    fn benjamini_hochberg_rejects_none_when_nothing_clears() {
        assert_eq!(benjamini_hochberg(&[0.2], 0.1, 1), vec![false]);
    }

    #[test]
    fn benjamini_hochberg_preserves_input_order() {
        // The single significant value is in the middle of the input.
        assert_eq!(
            benjamini_hochberg(&[0.9, 0.001, 0.8], 0.1, 3),
            vec![false, true, false]
        );
    }

    #[test]
    fn benjamini_hochberg_handles_an_empty_family() {
        assert_eq!(benjamini_hochberg(&[], 0.1, 0), Vec::<bool>::new());
        assert_eq!(benjamini_hochberg(&[], 0.1, 64), Vec::<bool>::new());
    }

    #[test]
    fn benjamini_hochberg_matches_the_published_reference_family() {
        // Benjamini & Hochberg (1995), table 1: at q=0.05 over all 15 hypotheses
        // the procedure rejects the four smallest p-values (rank 4 clears
        // 4/15·0.05 = 0.0133 with 0.0095; rank 5 misses 0.0167 with 0.0201).
        let p_values = [
            0.0001, 0.0004, 0.0019, 0.0095, 0.0201, 0.0278, 0.0298, 0.0344, 0.0459, 0.3240, 0.4262,
            0.5719, 0.6528, 0.7590, 1.000,
        ];
        let mut expected = vec![false; p_values.len()];
        for slot in expected.iter_mut().take(4) {
            *slot = true;
        }
        assert_eq!(benjamini_hochberg(&p_values, 0.05, 15), expected);
    }

    #[test]
    fn benjamini_hochberg_judges_candidates_against_the_whole_family() {
        // Twenty candidates that all cleared a 0.05 screen, drawn from a family of
        // 1280 hypotheses. Against the true family the thresholds are k·0.1/1280 =
        // k·7.8125e-5, which even rank 20 (1.5625e-3) misses by more than an order
        // of magnitude, so none survive the correction.
        let candidates = [
            0.0010, 0.0035, 0.0060, 0.0085, 0.0110, 0.0135, 0.0160, 0.0185, 0.0210, 0.0235, 0.0260,
            0.0285, 0.0310, 0.0335, 0.0360, 0.0385, 0.0410, 0.0435, 0.0460, 0.0485,
        ];
        assert_eq!(keep_count(&benjamini_hochberg(&candidates, 0.1, 1280)), 0);

        // The same candidates judged against themselves alone: every one is kept,
        // because each already cleared a bar below the loosest threshold q.
        assert_eq!(
            keep_count(&benjamini_hochberg(&candidates, 0.1, candidates.len())),
            candidates.len()
        );
    }

    #[test]
    fn benjamini_hochberg_keeps_the_strongest_candidates_of_a_large_family() {
        // Two overwhelming candidates among eighteen merely-significant ones, out
        // of a family of 1280 at q=0.1. Rank 1 clears 7.8125e-5 with 1e-5 and rank
        // 2 clears 1.5625e-4 with 5e-5; rank 3 misses 2.34375e-4 with 0.001, and
        // every later rank falls further behind, so exactly the two strongest
        // survive.
        let mut candidates = vec![1e-5, 5e-5];
        candidates.extend((0..18).map(|index| 0.001 + 0.002 * f64::from(index)));
        let keep = benjamini_hochberg(&candidates, 0.1, 1280);

        assert_eq!(keep_count(&keep), 2);
        assert_eq!(&keep[..2], &[true, true]);
    }

    #[test]
    fn benjamini_hochberg_keeps_everything_when_the_family_is_only_its_own_survivors() {
        // The degenerate case the `family_size` parameter exists to prevent: every
        // p-value has already cleared a screen at `alpha`, and `alpha ≤ q`, so the
        // loosest threshold `(m/m)·q = q` passes and the procedure rejects nothing
        // however the p-values are arranged.
        const ALPHA: f64 = 0.05;
        const Q: f64 = 0.1;

        for size in 1_usize..=12 {
            // A spread of p-values filling (0, ALPHA], plus their reverse, so the
            // conclusion cannot depend on the input happening to arrive sorted.
            let ascending: Vec<f64> = (1..=size)
                .map(|index| ALPHA * count_to_f64(index) / count_to_f64(size))
                .collect();
            let descending: Vec<f64> = ascending.iter().rev().copied().collect();

            for family in [&ascending, &descending] {
                assert_eq!(
                    keep_count(&benjamini_hochberg(family, Q, family.len())),
                    size,
                    "family of {size} p-values below {ALPHA} at q={Q}"
                );
            }
        }
    }

    #[test]
    fn benjamini_hochberg_never_keeps_more_as_the_family_grows() {
        let candidates = [0.0001, 0.002, 0.008, 0.011, 0.03, 0.047];
        let mut previous = candidates.len();

        for family_size in candidates.len()..=64 {
            let kept = keep_count(&benjamini_hochberg(&candidates, 0.1, family_size));
            assert!(
                kept <= previous,
                "family size {family_size} kept {kept}, up from {previous}"
            );
            previous = kept;
        }

        // The sweep is only meaningful if it actually descends over its range.
        assert!(previous < candidates.len());
    }

    #[test]
    #[should_panic]
    fn benjamini_hochberg_rejects_a_family_smaller_than_the_p_values() {
        // A family that does not contain every hypothesis these p-values came
        // from is a broken census, and normalising it away would restore the
        // candidates-only correction that `family_size` exists to prevent. The
        // accepted boundary — a family of exactly these p-values — is pinned by
        // `benjamini_hochberg_keeps_the_significant_prefix`.
        _ = benjamini_hochberg(&[0.01, 0.02, 0.5], 0.1, 2);
    }

    #[test]
    fn benjamini_hochberg_keeps_a_p_value_exactly_at_its_threshold() {
        // 1/4·0.5 = 0.125 is exact in binary, so this pins the boundary as
        // inclusive without any rounding slack: one candidate out of four
        // hypotheses sits precisely on its threshold.
        assert_eq!(benjamini_hochberg(&[0.125], 0.5, 4), vec![true]);
    }

    #[test]
    fn two_sided_p_is_symmetric_and_bounded() {
        close(two_sided_p_from_z(1.96), 0.05, 1e-3);
        close(two_sided_p_from_z(0.0), NO_EVIDENCE, 1e-6);
        close(two_sided_p_from_z(-1.96), 0.05, 1e-3);
    }
}
