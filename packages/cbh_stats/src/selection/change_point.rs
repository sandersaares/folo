//! Change-point selection and significance adjustment.

use std::num::NonZero;

use crate::selection::permutation::{PermutationOrbit, group_order};
use crate::{
    NO_EVIDENCE, exact_mw_feasible, exact_rank_sum_p_values, mann_whitney_tie_term,
    normal_mann_whitney_p, pettitt_rank_location, scaled_average_ranks,
};

/// Largest integer through which binary64 represents every integer exactly.
const MAX_EXACT_F64_INTEGER: u128 = 1 << f64::MANTISSA_DIGITS;

/// Relative slack for comparisons between independently evaluated normal p-values.
///
/// This allows one final-bit rounding difference per arithmetic step in the normal-score
/// path. Expanding the rejection region by this amount can only make the analytic bound
/// more conservative.
const APPROXIMATE_P_COMPARISON_RELATIVE_TOLERANCE: f64 = 64.0 * f64::EPSILON;

/// Controls bounded exact-group calibration of one selected split.
///
/// The analytic component can certify an especially clear change immediately.
/// Otherwise `permutation_order_budget` bounds an exact conditional orbit.
/// `analytic_weight` allocates part of the combined p-value to the analytic component;
/// the remainder belongs to permutation. The acceptance and rejection boundaries let
/// the scorer omit work that cannot affect its caller's decision.
#[derive(Clone, Copy, Debug)]
pub struct SelectionCalibration {
    /// Maximum order of the exact conditional orbit.
    pub permutation_order_budget: NonZero<usize>,
    /// Bonferroni weight allocated to the analytic component.
    pub analytic_weight: f64,
    /// Combined p-value below which the analytic component needs no permutation.
    pub accept_analytic_below: f64,
    /// Combined p-value at or above which the caller rejects the candidate.
    pub reject_at_or_above: f64,
}

/// A located change point with its selected and selection-adjusted statistics.
///
/// This is the complete statistical handoff needed by the detector: the split
/// locates the regimes, `tainted_p` explains the adjustment in diagnostics,
/// `adjusted_p` enters the significance and family filters, and `superiority`
/// drives the practical regime-separation gate.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SelectionAdjustedChangePoint {
    /// Index where the after regime begins.
    pub index: usize,
    /// Mann-Whitney p-value at the split selected from the observed ordering.
    ///
    /// This is not an honest standalone p-value because the split was selected
    /// by searching the same data.
    pub tainted_p: f64,
    /// Combined analytic and conditional-permutation p-value for split selection.
    pub adjusted_p: f64,
    /// Probability that an after-regime value exceeds a before-regime value,
    /// counting ties as one half.
    pub superiority: f64,
}

/// Scores one rank ordering with the production change-point procedure.
///
/// Rank-multiset properties are cached once, including the tie correction and,
/// on first use, exact rank-sum tails for every feasible split size.
struct SplitScorer {
    min_regime: usize,
    total_scaled_rank_sum: usize,
    tie_term: f64,
    max_exact_size: usize,
    sorted_ranks: Vec<usize>,
    exact_p_by_size: Option<Vec<Vec<f64>>>,
}

/// The score and effect size at one ordering's selected reportable split.
#[derive(Clone, Copy, Debug)]
struct SelectedScore {
    index: usize,
    p: f64,
    superiority: f64,
}

impl SplitScorer {
    fn new(sorted_ranks: Vec<usize>, min_regime: usize) -> Self {
        let n = sorted_ranks.len();
        let max_split_size = n.checked_div(2).expect("the divisor is nonzero");
        let max_exact_size = (min_regime..=max_split_size)
            .take_while(|&size| exact_mw_feasible(size, n.saturating_sub(size)))
            .last()
            .unwrap_or(0);
        let total_scaled_rank_sum = sorted_ranks.iter().copied().sum();
        let tie_term = mann_whitney_tie_term(&sorted_ranks);
        Self {
            min_regime,
            total_scaled_rank_sum,
            tie_term,
            max_exact_size,
            sorted_ranks,
            exact_p_by_size: None,
        }
    }

    fn score(&mut self, ranks: &[usize]) -> Option<SelectedScore> {
        let n = ranks.len();
        let (index, scaled_rank_sum_left, _) = pettitt_rank_location(ranks)?;
        let right_len = n.checked_sub(index)?;
        let smaller_side = index.min(right_len);
        if smaller_side < self.min_regime {
            return None;
        }

        let rank_sum_left = count_to_f64(scaled_rank_sum_left) / 2.0;
        let left_len_f = count_to_f64(index);
        let right_len_f = count_to_f64(right_len);
        let u1 = rank_sum_left - left_len_f * (left_len_f + 1.0) / 2.0;
        let u2 = left_len_f * right_len_f - u1;
        let superiority = u2 / (left_len_f * right_len_f);

        let observed_sum = if index <= right_len {
            scaled_rank_sum_left
        } else {
            self.total_scaled_rank_sum
                .saturating_sub(scaled_rank_sum_left)
        };
        let p = self.p_for_subset(smaller_side, observed_sum);

        Some(SelectedScore {
            index,
            p,
            superiority,
        })
    }

    /// Scores the rank sum of a predetermined subset of `smaller_side` values.
    fn p_for_subset(&mut self, smaller_side: usize, observed_sum: usize) -> f64 {
        if smaller_side <= self.max_exact_size {
            let exact = self.exact_p_by_size.get_or_insert_with(|| {
                exact_rank_sum_p_values(&self.sorted_ranks, self.max_exact_size)
            });
            exact
                .get(smaller_side)
                .and_then(|row| row.get(observed_sum))
                .copied()
                .unwrap_or(NO_EVIDENCE)
        } else {
            normal_mann_whitney_p(
                smaller_side,
                self.sorted_ranks.len().saturating_sub(smaller_side),
                count_to_f64(observed_sum) / 2.0,
                self.tie_term,
            )
        }
    }
}

/// Locates and selection-adjusts a change point by exact permutation.
///
/// The analytic component applies a union bound over every admissible split. Exact
/// Mann-Whitney splits contribute their valid fixed-split p-value; approximate splits
/// use the approximation only to locate a rank-sum rejection region, then bound that
/// region with a finite-population Chernoff inequality. It can certify a clear change
/// without sampling.
///
/// Otherwise calibration enumerates every distinct ordering of the observed rank
/// multiset when that fits `permutation_order_budget`, or every element of a
/// deterministic subgroup within that budget. The observed ordering is included. A
/// permuted ordering whose selected split violates `min_regime` contributes the
/// no-evidence score and remains in the denominator. Ties count as equally extreme.
///
/// The two valid components are combined by weighted Bonferroni and the result is
/// clamped to be no smaller than the selected Mann-Whitney score.
///
/// Returns `None` when Pettitt cannot locate a split or the selected split does
/// not leave `min_regime` values on each side.
///
/// # Panics
///
/// Panics if `min_regime` is zero, a calibration boundary or weight is invalid,
/// or the exact permutation-orbit order cannot be represented as an `f64` denominator.
#[must_use]
pub fn selection_adjusted_change_point(
    values: &[f64],
    min_regime: usize,
    calibration: SelectionCalibration,
) -> Option<SelectionAdjustedChangePoint> {
    assert!(min_regime > 0, "a regime must contain at least one value");
    assert!(
        calibration.analytic_weight > 0.0 && calibration.analytic_weight < 1.0,
        "the analytic weight must be in (0, 1)"
    );
    assert!(
        calibration.accept_analytic_below > 0.0 && calibration.accept_analytic_below <= 1.0,
        "the analytic acceptance boundary must be in (0, 1]"
    );
    assert!(
        calibration.reject_at_or_above > 0.0 && calibration.reject_at_or_above <= 1.0,
        "the rejection boundary must be in (0, 1]"
    );
    let observed_ranks = scaled_average_ranks(values);
    let mut sorted_ranks = observed_ranks.clone();
    sorted_ranks.sort_unstable();
    let mut scorer = SplitScorer::new(sorted_ranks.clone(), min_regime);
    let observed = scorer.score(&observed_ranks)?;

    // Adjustment cannot make the selected split more significant, so an observed
    // score already outside the next gate needs no calibration.
    if observed.p >= calibration.reject_at_or_above {
        return Some(result(observed, NO_EVIDENCE));
    }
    if single_exact_split_needs_no_adjustment(observed_ranks.len(), min_regime) {
        return Some(result(observed, observed.p));
    }

    let analytic = analytic_selection_p(&mut scorer, observed.p);
    let weighted_analytic = (analytic / calibration.analytic_weight).min(NO_EVIDENCE);
    if analytic_is_decisive(weighted_analytic, calibration.accept_analytic_below) {
        return Some(result(observed, weighted_analytic.max(observed.p)));
    }

    let permutation_weight = 1.0 - calibration.analytic_weight;
    let mut orbit = PermutationOrbit::new(
        sorted_ranks,
        observed_ranks.len(),
        calibration.permutation_order_budget.get(),
    );
    let order = orbit.order();
    assert!(
        order as u128 <= MAX_EXACT_F64_INTEGER,
        "the exact permutation-orbit denominator must be exactly representable as f64"
    );
    let mut permuted = observed_ranks.clone();
    let mut extreme = 0_usize;
    for element in 0..order {
        orbit.apply(&observed_ranks, &mut permuted);
        let permuted_p = scorer.score(&permuted).map_or(NO_EVIDENCE, |score| score.p);
        if permuted_p <= observed.p {
            extreme = extreme.saturating_add(1);
        }
        let partial_permutation = weighted_permutation_p(extreme, order, permutation_weight);
        if let Some(adjusted_p) = decisive_partial_orbit_p(
            weighted_analytic,
            partial_permutation,
            calibration.reject_at_or_above,
        ) {
            return Some(result(observed, adjusted_p.max(observed.p)));
        }
        if element.saturating_add(1) < order {
            assert!(
                orbit.advance(),
                "the exact permutation enumerator must produce its declared order"
            );
        }
    }

    Some(result(
        observed,
        combined_p(
            weighted_analytic,
            weighted_permutation_p(extreme, order, permutation_weight),
        )
        .max(observed.p),
    ))
}

/// Exact fallback-subgroup order used for a series and configured upper bound.
#[must_use]
pub fn selection_fallback_group_order(
    series_len: usize,
    order_budget: NonZero<usize>,
) -> NonZero<usize> {
    NonZero::new(group_order(series_len, order_budget.get()))
        .expect("the identity gives every permutation group nonzero order")
}

fn analytic_is_decisive(weighted_p: f64, acceptance_boundary: f64) -> bool {
    weighted_p < acceptance_boundary
}

fn single_exact_split_needs_no_adjustment(series_len: usize, min_regime: usize) -> bool {
    min_regime.checked_mul(2) == Some(series_len) && exact_mw_feasible(min_regime, min_regime)
}

fn decisive_partial_orbit_p(
    weighted_analytic: f64,
    partial_permutation: f64,
    rejection_boundary: f64,
) -> Option<f64> {
    if partial_permutation >= weighted_analytic {
        Some(weighted_analytic)
    } else if partial_permutation >= rejection_boundary {
        Some(NO_EVIDENCE)
    } else {
        None
    }
}

fn weighted_permutation_p(extreme: usize, order: usize, permutation_weight: f64) -> f64 {
    count_to_f64(extreme) / count_to_f64(order) / permutation_weight
}

/// Conservative conditional p-value for selecting the strongest admissible split.
fn analytic_selection_p(scorer: &mut SplitScorer, observed_p: f64) -> f64 {
    let n = scorer.sorted_ranks.len();
    let max_split_size = n.checked_div(2).expect("the divisor is nonzero");
    if scorer.min_regime > max_split_size {
        return NO_EVIDENCE;
    }

    let mut bound = 0.0_f64;
    let mut smallest_sum = 0_usize;
    let mut largest_sum = 0_usize;
    for smaller_side in 1..=max_split_size {
        let lower_index = smaller_side.saturating_sub(1);
        let upper_index = n.saturating_sub(smaller_side);
        let smallest_rank = scorer
            .sorted_ranks
            .get(lower_index)
            .copied()
            .expect("the smaller-side range stays inside the rank population");
        let largest_rank = scorer
            .sorted_ranks
            .get(upper_index)
            .copied()
            .expect("the smaller-side range stays inside the rank population");
        smallest_sum = smallest_sum.saturating_add(smallest_rank);
        largest_sum = largest_sum.saturating_add(largest_rank);
        if smaller_side < scorer.min_regime {
            continue;
        }

        let contribution = if smaller_side <= scorer.max_exact_size {
            observed_p
        } else {
            approximate_split_tail_bound(
                scorer,
                smaller_side,
                smallest_sum,
                largest_sum,
                observed_p,
            )
        };
        // Every smaller-side size occurs at a prefix and suffix split, except the
        // balanced split of an even-length series.
        let multiplicity = if smaller_side.saturating_mul(2) == n {
            1.0
        } else {
            2.0
        };
        bound += multiplicity * contribution;
        if bound >= NO_EVIDENCE {
            return NO_EVIDENCE;
        }
    }
    bound.min(NO_EVIDENCE)
}

/// Bounds one approximate fixed-split tail without trusting its normal p-value.
fn approximate_split_tail_bound(
    scorer: &mut SplitScorer,
    smaller_side: usize,
    smallest_sum: usize,
    largest_sum: usize,
    observed_p: f64,
) -> f64 {
    let expected_sum = smaller_side.saturating_mul(scorer.sorted_ranks.len().saturating_add(1));

    let lower =
        closest_lower_rejection_sum(scorer, smaller_side, smallest_sum, expected_sum, observed_p)
            .map_or(0.0, |sum| {
                rank_sum_tail_bound(&scorer.sorted_ranks, smaller_side, sum, false)
            });
    let upper =
        closest_upper_rejection_sum(scorer, smaller_side, expected_sum, largest_sum, observed_p)
            .map_or(0.0, |sum| {
                rank_sum_tail_bound(&scorer.sorted_ranks, smaller_side, sum, true)
            });
    (lower + upper).min(NO_EVIDENCE)
}

/// Largest lower-tail sum whose production approximation reaches `observed_p`.
fn closest_lower_rejection_sum(
    scorer: &mut SplitScorer,
    smaller_side: usize,
    minimum: usize,
    expected: usize,
    observed_p: f64,
) -> Option<usize> {
    if !reaches_approximate_rejection(scorer.p_for_subset(smaller_side, minimum), observed_p) {
        return None;
    }
    let mut low = minimum;
    let mut high = expected;
    while low != high {
        let middle = low.saturating_add(high.saturating_sub(low).div_ceil(2));
        if reaches_approximate_rejection(scorer.p_for_subset(smaller_side, middle), observed_p) {
            low = middle;
        } else {
            high = middle.saturating_sub(1);
        }
    }
    Some(low)
}

/// Smallest upper-tail sum whose production approximation reaches `observed_p`.
fn closest_upper_rejection_sum(
    scorer: &mut SplitScorer,
    smaller_side: usize,
    expected: usize,
    maximum: usize,
    observed_p: f64,
) -> Option<usize> {
    if !reaches_approximate_rejection(scorer.p_for_subset(smaller_side, maximum), observed_p) {
        return None;
    }
    let mut low = expected;
    let mut high = maximum;
    while low != high {
        let middle = low.midpoint(high);
        if reaches_approximate_rejection(scorer.p_for_subset(smaller_side, middle), observed_p) {
            high = middle;
        } else {
            low = middle.saturating_add(1);
        }
    }
    Some(low)
}

fn reaches_approximate_rejection(candidate_p: f64, observed_p: f64) -> bool {
    candidate_p <= observed_p.mul_add(APPROXIMATE_P_COMPARISON_RELATIVE_TOLERANCE, observed_p)
}

/// Chernoff bound for one rank-sum tail under sampling without replacement.
fn rank_sum_tail_bound(
    sorted_ranks: &[usize],
    sample_size: usize,
    threshold_sum: usize,
    upper: bool,
) -> f64 {
    let Some((&minimum, &maximum)) = sorted_ranks.first().zip(sorted_ranks.last()) else {
        return NO_EVIDENCE;
    };
    let range = maximum.saturating_sub(minimum);
    if range == 0 {
        return NO_EVIDENCE;
    }

    let sample_size_f = count_to_f64(sample_size);
    let range_f = count_to_f64(range);
    let minimum_f = count_to_f64(minimum);
    let n = sorted_ranks.len();
    let population_mean = count_to_f64(n.saturating_add(1));
    let normalized_mean = (population_mean - minimum_f) / range_f;
    let threshold_mean = count_to_f64(threshold_sum) / sample_size_f;
    let normalized_threshold = ((threshold_mean - minimum_f) / range_f).clamp(0.0, 1.0);
    if (upper && normalized_threshold <= normalized_mean)
        || (!upper && normalized_threshold >= normalized_mean)
    {
        return NO_EVIDENCE;
    }
    let deviation = (normalized_threshold - normalized_mean).abs();
    let chernoff = (-sample_size_f * binary_kl(normalized_threshold, normalized_mean)).exp();
    chernoff.min(serfling_tail_bound(
        sorted_ranks.len(),
        sample_size,
        deviation,
    ))
}

/// Bernoulli Kullback-Leibler divergence used by the bounded-rank Chernoff tail.
fn binary_kl(observed: f64, expected: f64) -> f64 {
    let lower = if observed == 0.0 {
        0.0
    } else {
        observed * (observed / expected).ln()
    };
    let upper = if observed >= 1.0 {
        0.0
    } else {
        (1.0 - observed) * ((1.0 - observed) / (1.0 - expected)).ln()
    };
    lower + upper
}

/// Serfling's finite-population correction for one bounded sample-mean tail.
fn serfling_tail_bound(population_size: usize, sample_size: usize, deviation: f64) -> f64 {
    let population_size = count_to_f64(population_size);
    let sample_size = count_to_f64(sample_size);
    let correction = 1.0 - (sample_size - 1.0) / population_size;
    (-2.0 * sample_size * deviation * deviation / correction).exp()
}

fn combined_p(left: f64, right: f64) -> f64 {
    left.min(right).min(NO_EVIDENCE)
}

fn result(score: SelectedScore, adjusted_p: f64) -> SelectionAdjustedChangePoint {
    SelectionAdjustedChangePoint {
        index: score.index,
        tainted_p: score.p,
        adjusted_p,
        superiority: score.superiority,
    }
}

/// Casts a bounded count to `f64`.
#[expect(
    clippy::cast_precision_loss,
    reason = "series lengths are bounded and permutation denominators are validated at or below 2^53"
)]
fn count_to_f64(count: usize) -> f64 {
    count as f64
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    #![allow(
        clippy::float_cmp,
        reason = "the shared scorer must reproduce the production primitives bit-for-bit"
    )]
    #![allow(clippy::indexing_slicing, reason = "panic is acceptable in tests")]

    use std::f64::consts::LN_2;

    use super::*;
    use crate::test_util::close;
    use crate::{MannWhitneyU, pettitt};

    fn calibration(permutation_order_budget: NonZero<usize>) -> SelectionCalibration {
        SelectionCalibration {
            permutation_order_budget,
            analytic_weight: 0.1,
            accept_analytic_below: f64::MIN_POSITIVE,
            reject_at_or_above: 0.025,
        }
    }

    #[test]
    fn selected_score_matches_production_primitives_with_ties() {
        let exact = vec![1.0, 1.0, 2.0, 1.0, 5.0, 5.0, 5.0, 2.0, 5.0, 5.0];
        let normal = [vec![1.0; 50], vec![5.0; 50]].concat();
        for (values, min_regime) in [(exact, 2), (normal, 5)] {
            let ranks = scaled_average_ranks(&values);
            let mut sorted = ranks.clone();
            sorted.sort_unstable();
            let mut scorer = SplitScorer::new(sorted, min_regime);
            let selected = scorer.score(&ranks).expect("the split is reportable");
            let located = pettitt(&values).expect("Pettitt locates a split");
            assert_eq!(selected.index, located.index);

            let before = &values[..selected.index];
            let after = &values[selected.index..];
            let mann_whitney = MannWhitneyU::new(before, after).expect("both regimes are nonempty");
            assert_eq!(selected.p, mann_whitney.two_sided_p_value());
            assert_eq!(selected.superiority, mann_whitney.superiority());
        }
    }

    #[test]
    fn scorer_accepts_a_split_at_the_minimum_regime_boundary() {
        let values = [vec![1.0; 5], vec![2.0; 5]].concat();
        let ranks = scaled_average_ranks(&values);
        let mut sorted = ranks.clone();
        sorted.sort_unstable();
        let score = SplitScorer::new(sorted, 5)
            .score(&ranks)
            .expect("both regimes meet the minimum exactly");
        assert_eq!(score.index, 5);
    }

    #[test]
    fn calibration_is_reproducible() {
        let values = [10.0, 11.0, 10.0, 12.0, 11.0, 30.0, 31.0, 30.0, 32.0, 31.0];
        let budget = NonZero::new(2_000).expect("the test budget is nonzero");
        let first = selection_adjusted_change_point(&values, 5, calibration(budget));
        let second = selection_adjusted_change_point(&values, 5, calibration(budget));
        assert_eq!(first, second);
    }

    #[test]
    #[should_panic(expected = "analytic weight")]
    fn zero_analytic_weight_is_rejected() {
        let mut calibration = calibration(NonZero::new(10).expect("the test budget is nonzero"));
        calibration.analytic_weight = 0.0;
        _ = selection_adjusted_change_point(&[1.0, 2.0], 1, calibration);
    }

    #[test]
    #[should_panic(expected = "analytic acceptance boundary")]
    fn zero_analytic_acceptance_boundary_is_rejected() {
        let mut calibration = calibration(NonZero::new(10).expect("the test budget is nonzero"));
        calibration.accept_analytic_below = 0.0;
        _ = selection_adjusted_change_point(&[1.0, 2.0], 1, calibration);
    }

    #[test]
    fn clean_tied_step_survives_selection_adjustment() {
        let mut values = vec![10.0; 10];
        values.extend(std::iter::repeat_n(20.0, 10));
        let adjusted = selection_adjusted_change_point(
            &values,
            5,
            calibration(NonZero::new(2_000).expect("the test budget is nonzero")),
        )
        .expect("the clean middle split is reportable");
        assert_eq!(adjusted.index, 10);
        assert!(adjusted.adjusted_p < 0.025, "{adjusted:?}");
        assert!(adjusted.adjusted_p >= adjusted.tainted_p);
        assert_eq!(adjusted.superiority, 1.0);
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "the production short-history orbit is too large for interpretation"
    )]
    fn distinct_noisy_step_has_family_scale_resolution() {
        let values = [
            98.0, 100.0, 102.0, 99.0, 101.0, 100.0, 148.0, 150.0, 152.0, 149.0, 151.0, 150.0,
        ];
        let adjusted = selection_adjusted_change_point(
            &values,
            5,
            SelectionCalibration {
                permutation_order_budget: NonZero::new(259_200)
                    .expect("the production minimum is nonzero"),
                analytic_weight: 0.1,
                accept_analytic_below: 0.1 / 14.0,
                reject_at_or_above: 0.025,
            },
        )
        .expect("the selected split is reportable");
        assert!(adjusted.adjusted_p < 0.1 / 14.0, "{adjusted:?}");
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "the supported-length analytic scan tests stress-family resolution over every split"
    )]
    fn analytic_certificate_clears_stress_scale_boundary_without_permutations() {
        // These constants mirror the detector policy at the stress harness's default
        // family size. The exact subgroup can resolve rank one, while this fixture
        // protects the analytic path that avoids enumerating the complete group.
        const FAMILY_SIZE: usize = 20_000;
        const DETECTOR_COUNT: usize = 2;
        const TARGET_FALSE_DISCOVERY_RATE: f64 = 0.10;
        const PERMUTATION_CAP: usize = 500_000;
        const ANALYTIC_WEIGHT: f64 = 0.1;
        const REGIME_POINTS: usize = 500;
        const MIN_REGIME: usize = 5;
        const BEFORE_LEVEL: f64 = 1.0;
        const AFTER_LEVEL: f64 = 2.0;

        let values = [
            vec![BEFORE_LEVEL; REGIME_POINTS],
            vec![AFTER_LEVEL; REGIME_POINTS],
        ]
        .concat();
        let ranks = scaled_average_ranks(&values);
        let mut sorted = ranks.clone();
        sorted.sort_unstable();
        let mut scorer = SplitScorer::new(sorted, MIN_REGIME);
        let observed = scorer.score(&ranks).expect("the clean split is reportable");
        let weighted_analytic = analytic_selection_p(&mut scorer, observed.p) / ANALYTIC_WEIGHT;
        let rank_one_boundary =
            TARGET_FALSE_DISCOVERY_RATE / count_to_f64(DETECTOR_COUNT.saturating_mul(FAMILY_SIZE));
        let budget = NonZero::new(PERMUTATION_CAP).expect("the production cap is nonzero");
        let order = selection_fallback_group_order(values.len(), budget);
        let weighted_permutation_floor =
            NO_EVIDENCE / (count_to_f64(order.get()) * (1.0 - ANALYTIC_WEIGHT));

        assert!(weighted_permutation_floor < rank_one_boundary);
        assert!(weighted_analytic < rank_one_boundary, "{weighted_analytic}");
    }

    #[test]
    fn analytic_calibration_without_an_admissible_split_returns_no_evidence() {
        let values = [1.0; 10];
        let ranks = scaled_average_ranks(&values);
        let mut scorer = SplitScorer::new(ranks, 6);
        assert_eq!(analytic_selection_p(&mut scorer, 0.01), NO_EVIDENCE);
    }

    #[test]
    fn exact_group_counts_the_complete_orbit() {
        let budget = NonZero::new(10).expect("the test budget is nonzero");
        let calibration = SelectionCalibration {
            permutation_order_budget: budget,
            analytic_weight: 1e-10,
            accept_analytic_below: f64::MIN_POSITIVE,
            reject_at_or_above: NO_EVIDENCE,
        };
        let adjusted =
            selection_adjusted_change_point(&[vec![1.0; 6], vec![2.0; 6]].concat(), 5, calibration)
                .expect("the clean split is reportable");
        close(adjusted.adjusted_p, 0.125_000_000_012_5, 1e-15);
    }

    #[test]
    fn partial_orbit_stopping_returns_only_a_proven_component() {
        assert_eq!(decisive_partial_orbit_p(0.02, 0.01, 0.025), None);
        assert_eq!(decisive_partial_orbit_p(0.02, 0.02, 0.025), Some(0.02));
        assert_eq!(
            decisive_partial_orbit_p(0.5, 0.025, 0.025),
            Some(NO_EVIDENCE)
        );
    }

    #[test]
    fn permutation_component_uses_orbit_order_and_weight() {
        close(weighted_permutation_p(3, 10, 0.8), 0.375, 1e-15);
    }

    #[test]
    fn analytic_acceptance_returns_the_weighted_certificate() {
        // The first balanced split beyond exact feasibility exercises the normal-score
        // certificate without building exact tables or scanning a production-length history.
        const REGIME_POINTS: usize = 29;
        const ANALYTIC_WEIGHT: f64 = 0.1;
        let values = [vec![1.0; REGIME_POINTS], vec![2.0; REGIME_POINTS]].concat();
        let calibration = SelectionCalibration {
            permutation_order_budget: NonZero::new(1).unwrap(),
            analytic_weight: ANALYTIC_WEIGHT,
            accept_analytic_below: NO_EVIDENCE,
            reject_at_or_above: NO_EVIDENCE,
        };
        assert!(!exact_mw_feasible(REGIME_POINTS, REGIME_POINTS));
        let adjusted =
            selection_adjusted_change_point(&values, REGIME_POINTS, calibration).unwrap();

        // At complete separation the normalized deviation is one half. Serfling's
        // exponent is -2 * 29 * (1/2)^2 / (1 - 28/58) = -841/30, tighter than
        // the Chernoff exponent -29 * ln(2). Both tails receive the analytic weight.
        let expected = 2.0 * (-841.0_f64 / 30.0).exp() / ANALYTIC_WEIGHT;
        close(adjusted.adjusted_p, expected, 1e-22);
        assert!(adjusted.adjusted_p > adjusted.tainted_p);
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "the supported-length certificate scans every approximate split and its rank tails"
    )]
    fn analytic_acceptance_returns_the_weighted_certificate_at_supported_length() {
        let budget = NonZero::new(10).expect("the test budget is nonzero");
        let calibration = SelectionCalibration {
            permutation_order_budget: budget,
            analytic_weight: 0.1,
            accept_analytic_below: NO_EVIDENCE,
            reject_at_or_above: NO_EVIDENCE,
        };
        let adjusted = selection_adjusted_change_point(
            &[vec![1.0; 500], vec![2.0; 500]].concat(),
            5,
            calibration,
        )
        .expect("the clean split is reportable");
        close(adjusted.adjusted_p, 1.668_848_293_376_855_4e-10, 1e-22);
    }

    #[test]
    fn analytic_acceptance_boundary_is_strict() {
        assert!(analytic_is_decisive(0.01, 0.02));
        assert!(!analytic_is_decisive(0.02, 0.02));
        assert!(!analytic_is_decisive(0.03, 0.02));
    }

    #[test]
    fn sole_exact_admissible_split_needs_no_search_penalty() {
        let values = [
            98.0, 100.0, 102.0, 99.0, 101.0, 128.0, 130.0, 132.0, 129.0, 131.0,
        ];
        let adjusted = selection_adjusted_change_point(
            &values,
            5,
            calibration(NonZero::new(1).expect("the identity budget is nonzero")),
        )
        .expect("the sole admissible split is selected");
        assert_eq!(adjusted.adjusted_p, adjusted.tainted_p);
    }

    #[test]
    fn analytic_union_counts_prefix_and_suffix_splits() {
        let sorted_ranks: Vec<usize> = (1..=12).map(|rank| rank * 2).collect();
        let mut scorer = SplitScorer::new(sorted_ranks.clone(), 5);
        close(analytic_selection_p(&mut scorer, 0.01), 0.03, 1e-15);

        let mut balanced_only = SplitScorer::new(sorted_ranks, 6);
        close(analytic_selection_p(&mut balanced_only, 0.01), 0.01, 1e-15);
    }

    #[test]
    fn approximate_rejection_searches_find_the_nearest_tail_sums() {
        const SAMPLE_SIZE: usize = 5;
        const MINIMUM_SUM: usize = 30;
        const EXPECTED_SUM: usize = 105;
        const MAXIMUM_SUM: usize = 190;

        let sorted_ranks: Vec<usize> = (1..=20).map(|rank| rank * 2).collect();
        let mut scorer = SplitScorer::new(sorted_ranks, SAMPLE_SIZE);
        scorer.max_exact_size = 0;
        assert_eq!(
            closest_lower_rejection_sum(&mut scorer, SAMPLE_SIZE, MINIMUM_SUM, EXPECTED_SUM, 0.05,),
            Some(59)
        );
        assert_eq!(
            closest_upper_rejection_sum(&mut scorer, SAMPLE_SIZE, EXPECTED_SUM, MAXIMUM_SUM, 0.05,),
            Some(151)
        );

        let minimum_p = scorer.p_for_subset(SAMPLE_SIZE, MINIMUM_SUM);
        let maximum_p = scorer.p_for_subset(SAMPLE_SIZE, MAXIMUM_SUM);
        assert_eq!(
            closest_lower_rejection_sum(
                &mut scorer,
                SAMPLE_SIZE,
                MINIMUM_SUM,
                EXPECTED_SUM,
                minimum_p,
            ),
            Some(MINIMUM_SUM)
        );
        assert_eq!(
            closest_upper_rejection_sum(
                &mut scorer,
                SAMPLE_SIZE,
                EXPECTED_SUM,
                MAXIMUM_SUM,
                maximum_p,
            ),
            Some(MAXIMUM_SUM)
        );
        assert_eq!(
            closest_lower_rejection_sum(
                &mut scorer,
                SAMPLE_SIZE,
                MINIMUM_SUM,
                EXPECTED_SUM,
                minimum_p / 2.0,
            ),
            None
        );
        assert_eq!(
            closest_upper_rejection_sum(
                &mut scorer,
                SAMPLE_SIZE,
                EXPECTED_SUM,
                MAXIMUM_SUM,
                maximum_p / 2.0,
            ),
            None
        );
    }

    #[test]
    fn rank_sum_tail_bound_matches_hand_computed_finite_population_case() {
        let sorted_ranks: Vec<usize> = (1..=20).map(|rank| rank * 2).collect();
        close(
            rank_sum_tail_bound(&sorted_ranks, 5, 59, false),
            0.480_615_848_080_104_8,
            1e-15,
        );
        close(
            rank_sum_tail_bound(&sorted_ranks, 5, 151, true),
            0.480_615_848_080_104_8,
            1e-15,
        );
        assert_eq!(
            rank_sum_tail_bound(&sorted_ranks, 5, 100, true),
            NO_EVIDENCE
        );
        assert_eq!(
            rank_sum_tail_bound(&sorted_ranks, 5, 110, false),
            NO_EVIDENCE
        );
    }

    #[test]
    fn binary_kl_matches_hand_computed_cases_and_boundaries() {
        close(binary_kl(0.25, 0.5), 0.130_812_035_941_136_97, 1e-15);
        close(binary_kl(0.2, 0.4), 0.091_516_221_849_435_78, 1e-15);
        close(binary_kl(0.0, 0.5), LN_2, 1e-15);
        close(binary_kl(1.0, 0.5), LN_2, 1e-15);
    }

    #[test]
    fn serfling_bound_matches_a_hand_computed_finite_population_case() {
        close(
            serfling_tail_bound(20, 5, 0.2),
            0.606_530_659_712_633_4,
            1e-15,
        );
    }

    #[test]
    fn reportable_tied_step_is_not_rejected_by_early_stopping() {
        // This is the smallest balanced tied step whose two fully separated orders
        // clear the detector boundary after permutation weighting. Allowing a shorter
        // regime keeps multiple splits admissible, so calibration cannot bypass the orbit.
        const REGIME_POINTS: usize = 5;
        let values = [vec![10.0; REGIME_POINTS], vec![20.0; REGIME_POINTS]].concat();
        let adjusted = selection_adjusted_change_point(
            &values,
            REGIME_POINTS - 1,
            calibration(NonZero::new(2_000).expect("the test budget is nonzero")),
        )
        .expect("the clean middle split is reportable");
        assert!(adjusted.adjusted_p < 0.025, "{adjusted:?}");
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "exhaustive tied-null calibration compares every ordering against every other ordering"
    )]
    fn tied_null_distribution_is_conservative() {
        // Six repeated low values and six repeated high values have 924 distinct
        // temporal orderings. Enumerating all of them exercises the tie pattern
        // that a length-only, tie-free calibration cannot represent.
        let mut scores = Vec::new();
        let sorted_ranks = scaled_average_ranks(&[vec![1.0; 6], vec![2.0; 6]].concat());
        let low_rank = sorted_ranks
            .first()
            .copied()
            .expect("the ranks are nonempty");
        let high_rank = sorted_ranks
            .last()
            .copied()
            .expect("the ranks are nonempty");
        let mut scorer = SplitScorer::new(sorted_ranks, 5);
        let mut unreportable = 0_usize;
        for mask in 0_u16..(1_u16 << 12) {
            if mask.count_ones() != 6 {
                continue;
            }
            let ranks: Vec<usize> = (0..12)
                .map(|index| {
                    if mask & (1 << index) == 0 {
                        low_rank
                    } else {
                        high_rank
                    }
                })
                .collect();
            match scorer.score(&ranks) {
                Some(score) => scores.push(score.p),
                None => {
                    unreportable = unreportable.saturating_add(1);
                    scores.push(NO_EVIDENCE);
                }
            }
        }
        assert!(
            unreportable > 0,
            "the denominator must include rejected splits"
        );

        let total = count_to_f64(scores.len());
        let adjusted: Vec<f64> = scores
            .iter()
            .map(|&observed| {
                let at_least_as_extreme = scores.iter().filter(|&&score| score <= observed).count();
                (count_to_f64(at_least_as_extreme) / total).max(observed)
            })
            .collect();
        let mut levels = adjusted.clone();
        levels.sort_unstable_by(f64::total_cmp);
        levels.dedup_by(|left, right| left.total_cmp(right).is_eq());
        for level in levels {
            let reported = adjusted.iter().filter(|&&value| value <= level).count();
            assert!(
                count_to_f64(reported) <= level * total + f64::EPSILON,
                "P(adjusted <= {level}) exceeded {level} for a tied null"
            );
        }
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "exhaustive tied-null calibration repeats normal-score tail searches for every ordering"
    )]
    fn analytic_normal_score_bound_is_conservative_with_ties() {
        // Force the approximate scorer over every ordering of a tied population. This
        // mechanically checks the finite-population tail inversion and union bound,
        // rather than relying on the normal score itself to be calibrated.
        let sorted_ranks = scaled_average_ranks(&[vec![1.0; 6], vec![2.0; 6]].concat());
        let low_rank = sorted_ranks
            .first()
            .copied()
            .expect("the ranks are nonempty");
        let high_rank = sorted_ranks
            .last()
            .copied()
            .expect("the ranks are nonempty");
        let mut adjusted = Vec::new();
        for mask in 0_u16..(1_u16 << 12) {
            if mask.count_ones() != 6 {
                continue;
            }
            let ranks: Vec<usize> = (0..12)
                .map(|index| {
                    if mask & (1 << index) == 0 {
                        low_rank
                    } else {
                        high_rank
                    }
                })
                .collect();
            let mut scorer = SplitScorer::new(sorted_ranks.clone(), 5);
            scorer.max_exact_size = 0;
            let Some(score) = scorer.score(&ranks) else {
                adjusted.push(NO_EVIDENCE);
                continue;
            };
            adjusted.push(analytic_selection_p(&mut scorer, score.p).max(score.p));
        }
        assert!(
            adjusted.iter().any(|&p| p < NO_EVIDENCE),
            "the fixture must exercise a nontrivial analytic certificate"
        );

        let total = count_to_f64(adjusted.len());
        let mut levels = adjusted.clone();
        levels.sort_unstable_by(f64::total_cmp);
        levels.dedup_by(|left, right| left.total_cmp(right).is_eq());
        for level in levels {
            let reported = adjusted.iter().filter(|&&value| value <= level).count();
            assert!(
                count_to_f64(reported) <= level * total + f64::EPSILON,
                "P(analytic <= {level}) exceeded {level} for a tied normal-score null"
            );
        }
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "exhaustive tied-null calibration rebuilds exact rank tables and a group orbit for every ordering"
    )]
    fn exact_group_combination_is_conservative_with_ties() {
        // Enumerating every observed ordering mechanically checks the weighted
        // analytic/exact-group combination under one tied conditional null.
        let budget = NonZero::new(10).expect("the test budget is nonzero");
        let calibration = SelectionCalibration {
            permutation_order_budget: budget,
            analytic_weight: 0.1,
            accept_analytic_below: f64::MIN_POSITIVE,
            reject_at_or_above: NO_EVIDENCE,
        };
        let mut adjusted = Vec::new();
        for mask in 0_u16..(1_u16 << 12) {
            if mask.count_ones() != 6 {
                continue;
            }
            let values: Vec<f64> = (0..12)
                .map(|index| if mask & (1 << index) == 0 { 1.0 } else { 2.0 })
                .collect();
            adjusted.push(
                selection_adjusted_change_point(&values, 5, calibration)
                    .map_or(NO_EVIDENCE, |selection| selection.adjusted_p),
            );
        }

        let total = count_to_f64(adjusted.len());
        let mut levels = adjusted.clone();
        levels.sort_unstable_by(f64::total_cmp);
        levels.dedup_by(|left, right| left.total_cmp(right).is_eq());
        for level in levels {
            let reported = adjusted.iter().filter(|&&value| value <= level).count();
            assert!(
                count_to_f64(reported) <= level * total + f64::EPSILON,
                "P(combined <= {level}) exceeded {level} for a tied null"
            );
        }
    }

    #[test]
    fn unreportable_selected_split_has_no_adjusted_result() {
        let values = [1.0; 6];
        assert_eq!(
            selection_adjusted_change_point(
                &values,
                3,
                calibration(NonZero::new(100).expect("the test budget is nonzero")),
            ),
            None
        );
    }
}
