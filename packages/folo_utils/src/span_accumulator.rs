//! Warmup-robust per-iteration slope and confidence interval over measured spans.
//!
//! A benchmark run records one span per Criterion sample, each covering some
//! number of iterations and a measured total (processor-time nanoseconds,
//! allocated bytes, allocation count — the quantity is opaque here). No benchmark
//! metric is truly deterministic: a per-iteration figure carries warmup calls,
//! buffer resizing and run-to-run jitter, and Criterion picks the iteration count
//! dynamically, so a naive pooled mean is biased by whichever low-iteration
//! warmup spans happened to run.
//!
//! [`SpanAccumulator`] folds each span into O(1) running sufficient statistics as
//! it is measured — it never retains the spans — and derives two figures the
//! tracker crates share:
//!
//! * a through-origin OLS **slope** as the per-iteration point estimate
//!   (`slope = Σ(nᵢ·tᵢ) / Σ(nᵢ²)`), which weights each span by its iteration
//!   count and so down-weights low-iteration warmup spans, and
//! * a closed-form heteroscedasticity-robust **confidence interval** of that
//!   slope. The nᵢ²-weighting of the residuals reproduces the warmup-robustness a
//!   percentile bootstrap of the same slope would give, without retaining the
//!   spans or resampling.
//!
//! Only these two figures are produced: they are the sole outputs downstream
//! noise-aware analysis consumes. The interval is a deterministic function of the
//! folded statistics, so identical measurements always yield identical bounds.
//!
//! A span may also carry a *level* — a quantity each iteration reaches on its own
//! rather than contributes to, such as a high-water mark. Folding one with
//! [`SpanAccumulator::add_level`] applies the same warmup-robust weighting, yielding
//! the levels averaged rather than a rate.
//!
//! Folding a span is a handful of additions and one multiply, allocation-free, so
//! it is cheap enough to run inside a measured span even when a benchmark
//! (against best practice) records one span per iteration.

#![expect(
    clippy::cast_precision_loss,
    reason = "iteration and quantity counts are cast to f64 for the regression; \
              per-sample counts stay well below 2^53, and the pathological \
              per-iteration case is defused rather than required to stay exact"
)]

/// Standard-normal 0.975 quantile: the two-sided 95% confidence multiplier.
const Z_95: f64 = 1.959_963_984_540_054;

/// Streaming estimator of the warmup-robust per-iteration slope and its
/// confidence interval.
///
/// Each measured span is folded in with [`add`](Self::add); the accumulator keeps
/// only O(1) running moments, never the spans themselves. [`slope`](Self::slope)
/// and [`interval`](Self::interval) read those moments in constant time.
///
/// The moments are held as `f64`. The highest-order term is `Σ nᵢ⁴`, which
/// overflows `u64` once nᵢ approaches 10⁷ (a large per-sample iteration count),
/// so integer accumulation is not an option; `f64` carries the magnitude
/// (≪ `f64::MAX`) at the cost of precision that the interval's defusing (see
/// [`interval`](Self::interval)) absorbs.
#[derive(Clone, Copy, Debug, Default)]
pub struct SpanAccumulator {
    /// Number of spans folded in.
    span_count: u64,

    /// Number of spans that covered a non-zero iteration count.
    ///
    /// A zero-iteration span contributes nothing to any moment below, so it carries no
    /// information about dispersion. Counting those separately keeps
    /// [`interval`](Self::interval) from treating them as evidence while
    /// [`span_count`](Self::span_count) still reports every span that was recorded.
    informative_span_count: u64,

    /// `Σ nᵢ²` — the slope denominator (and the regression's information).
    s_nn: f64,

    /// `Σ nᵢ·tᵢ` — the slope numerator.
    s_nt: f64,

    /// `Σ nᵢ²·tᵢ²` — first term of the robust residual sum of squares.
    s_nntt: f64,

    /// `Σ nᵢ³·tᵢ` — cross term of the robust residual sum of squares.
    s_nnnt: f64,

    /// `Σ nᵢ⁴` — quadratic term of the robust residual sum of squares.
    s_nnnn: f64,
}

impl SpanAccumulator {
    /// Creates an empty accumulator.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Folds one span covering `iterations` iterations with the given whole-span
    /// `total` (in the caller's opaque unit) into the running statistics.
    ///
    /// For the common `iterations == 1` case every power of nᵢ is one, so this
    /// reduces to a few additions and a single multiply.
    pub fn add(&mut self, iterations: u64, total: u64) {
        self.add_moments(iterations as f64, total as f64);
    }

    /// Folds one span covering `iterations` iterations whose measured quantity is a
    /// per-iteration `level` rather than a whole-span total.
    ///
    /// A level is a quantity each iteration reaches on its own instead of contributing
    /// to — a high-water mark, for example — so unlike a total it does not grow with the
    /// span's length. Scaling it by the iteration count makes it behave like a total, and
    /// the slope divides that scaling back out, so the estimate is an average of the spans'
    /// levels in which each span is weighted by the square of its own iteration count — the
    /// same warmup-robust weighting [`add`](Self::add) gives totals.
    ///
    /// The scaling happens in `f64`, so an arbitrarily large level and iteration count
    /// combine without overflow.
    pub fn add_level(&mut self, iterations: u64, level: u64) {
        let n = iterations as f64;
        self.add_moments(n, level as f64 * n);
    }

    /// Folds one span given the iteration count and total it regresses on.
    fn add_moments(&mut self, n: f64, t: f64) {
        self.span_count = self
            .span_count
            .checked_add(1)
            .expect("span count overflows u64 - this indicates an unrealistic scenario");

        if n != 0.0 {
            self.informative_span_count = self
                .informative_span_count
                .checked_add(1)
                .expect("span count overflows u64 - this indicates an unrealistic scenario");
        }

        let n2 = n * n;

        self.s_nn += n2;
        self.s_nt += n * t;
        self.s_nntt += n2 * t * t;
        self.s_nnnt += n2 * n * t;
        self.s_nnnn += n2 * n2;
    }

    /// Merges another accumulator's statistics into this one.
    ///
    /// Used to combine per-thread or per-session accumulators; the moments are
    /// additive, so a merge is exactly the accumulator that would have resulted
    /// from folding both span populations into one.
    pub fn merge(&mut self, other: &Self) {
        self.span_count = self
            .span_count
            .checked_add(other.span_count)
            .expect("span count overflows u64 - this indicates an unrealistic scenario");
        self.informative_span_count = self
            .informative_span_count
            .checked_add(other.informative_span_count)
            .expect("span count overflows u64 - this indicates an unrealistic scenario");
        self.s_nn += other.s_nn;
        self.s_nt += other.s_nt;
        self.s_nntt += other.s_nntt;
        self.s_nnnt += other.s_nnnt;
        self.s_nnnn += other.s_nnnn;
    }

    /// Number of spans folded in (distinct from the total iteration count).
    #[must_use]
    pub fn span_count(&self) -> u64 {
        self.span_count
    }

    /// The through-origin OLS slope `Σ(nᵢ·tᵢ) / Σ(nᵢ²)`: the per-iteration point
    /// estimate.
    ///
    /// Returns `None` when no spans were folded in. When every span recorded zero
    /// iterations the denominator vanishes and the per-iteration rate is
    /// undefined, so it is reported as `NaN`.
    #[must_use]
    pub fn slope(&self) -> Option<f64> {
        if self.span_count == 0 {
            return None;
        }
        if self.s_nn == 0.0 {
            return Some(f64::NAN);
        }
        Some(self.s_nt / self.s_nn)
    }

    /// The 95% heteroscedasticity-robust (HC0) confidence interval of the slope,
    /// or `None` when it cannot be estimated.
    ///
    /// The interval is `β̂ ± 1.96·SE`, where `SE² = Σ(nᵢ²·eᵢ²) / (Σnᵢ²)²` and the
    /// residuals `eᵢ = tᵢ − β̂·nᵢ`. Expanded over the folded moments the residual
    /// sum of squares is `S_nntt − 2·β̂·S_nnnt + β̂²·S_nnnn`. The lower bound is
    /// clamped at zero because the measured quantity is non-negative.
    ///
    /// Defusing of pathological inputs (per the design's "report no CI rather than
    /// a wrong one" policy):
    /// * fewer than two spans that covered a non-zero iteration count → `None`. A
    ///   zero-iteration span moves no moment, so counting it as evidence would let a
    ///   single real observation present itself as a zero-width interval;
    /// * a residual sum of squares that floating-point cancellation drives
    ///   slightly negative (only possible for near-deterministic data whose true
    ///   interval is ≈0) → treated as zero, collapsing the interval onto the
    ///   point estimate;
    /// * a non-finite slope or standard error → `None`.
    #[must_use]
    pub fn interval(&self) -> Option<(f64, f64)> {
        if self.informative_span_count < 2 || self.s_nn == 0.0 {
            return None;
        }

        let slope = self.s_nt / self.s_nn;
        let residual_sum_squares =
            (self.s_nntt - 2.0 * slope * self.s_nnnt + slope * slope * self.s_nnnn).max(0.0);
        let standard_error = residual_sum_squares.sqrt() / self.s_nn;

        if !slope.is_finite() || !standard_error.is_finite() {
            return None;
        }

        let half_width = Z_95 * standard_error;
        Some(((slope - half_width).max(0.0), slope + half_width))
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    #![allow(
        clippy::float_cmp,
        reason = "slope and count assertions are exact integer-derived values"
    )]

    use super::*;

    fn accumulate(spans: &[(u64, u64)]) -> SpanAccumulator {
        let mut accumulator = SpanAccumulator::new();
        for &(iterations, total) in spans {
            accumulator.add(iterations, total);
        }
        accumulator
    }

    fn assert_close(actual: f64, expected: f64) {
        assert!(
            (actual - expected).abs() < 1e-9,
            "expected {expected}, got {actual}"
        );
    }

    fn accumulate_levels(spans: &[(u64, u64)]) -> SpanAccumulator {
        let mut accumulator = SpanAccumulator::new();
        for &(iterations, level) in spans {
            accumulator.add_level(iterations, level);
        }
        accumulator
    }

    #[test]
    fn level_slope_recovers_an_unvarying_level() {
        // A level does not grow with the span, so however many iterations each span
        // covered, the estimate is the level itself.
        let accumulator = accumulate_levels(&[(2, 64), (8, 64), (1000, 64)]);
        assert_eq!(accumulator.slope(), Some(64.0));
    }

    #[test]
    fn level_slope_is_the_squared_iteration_weighted_mean() {
        // Squaring the iteration counts gives the longer span the greater say, so the
        // result sits nearer its level than an unweighted mean of the two would.
        let accumulator = accumulate_levels(&[(1, 1000), (3, 100)]);
        assert_eq!(accumulator.slope(), Some(190.0));
    }

    #[test]
    fn level_scaling_does_not_overflow_integer_range() {
        // The scale-up happens in f64, so a level and an iteration count whose product
        // exceeds u64 still yield the level back.
        const LEVEL: u64 = 3_000_000_000_000;
        const ITERATIONS: u64 = 9_000_000_000;

        // What is being protected is overflow resistance, not bit-exact arithmetic: the
        // level survives a multiply and a divide in f64. Deliberately far looser than the
        // rounding those two operations can introduce, and far tighter than any plausible
        // loss of the level itself, so it fails only if the scaling genuinely breaks.
        const TOLERANCE: f64 = 1e-9;

        let accumulator = accumulate_levels(&[(ITERATIONS, LEVEL)]);
        let slope = accumulator.slope().unwrap();

        assert!((slope - LEVEL as f64).abs() < LEVEL as f64 * TOLERANCE);
    }

    #[test]
    fn zero_iteration_spans_do_not_count_as_dispersion_evidence() {
        // A zero-iteration span moves no moment, so it carries no information about
        // spread. Counting it would let the one real observation below present itself
        // as a zero-width interval, claiming certainty from a single sample.
        let totals = accumulate(&[(0, 100), (1, 64)]);
        assert_eq!(totals.span_count(), 2);
        assert_eq!(totals.slope(), Some(64.0));
        assert_eq!(totals.interval(), None);

        // The level path reaches the same accumulator through `add_level`, so it must
        // defuse identically.
        let levels = accumulate_levels(&[(0, 100), (1, 64)]);
        assert_eq!(levels.span_count(), 2);
        assert_eq!(levels.slope(), Some(64.0));
        assert_eq!(levels.interval(), None);

        // Two informative spans alongside a zero-iteration one still yield an interval.
        let enough = accumulate_levels(&[(0, 100), (1, 64), (1, 64)]);
        assert_eq!(enough.interval(), Some((64.0, 64.0)));
    }

    #[test]
    fn merging_preserves_dispersion_evidence_counts() {
        // The informative count must survive a merge, or two accumulators each holding
        // one real span would fail to produce an interval after being combined.
        let mut left = accumulate_levels(&[(0, 100), (2, 64)]);
        let right = accumulate_levels(&[(0, 100), (2, 64)]);
        left.merge(&right);

        assert_eq!(left.span_count(), 4);
        assert_eq!(left.interval(), Some((64.0, 64.0)));
    }

    #[test]
    fn empty_accumulator_has_no_slope_or_interval() {
        let accumulator = SpanAccumulator::new();
        assert_eq!(accumulator.span_count(), 0);
        assert_eq!(accumulator.slope(), None);
        assert_eq!(accumulator.interval(), None);
    }

    #[test]
    fn span_count_tracks_the_number_of_spans() {
        let accumulator = accumulate(&[(1, 10), (1, 20), (1, 30), (1, 40)]);
        assert_eq!(accumulator.span_count(), 4);
    }

    #[test]
    fn constant_iterations_make_slope_equal_the_mean() {
        // Every span runs one iteration, so the slope degenerates to the plain
        // per-iteration mean: (10 + 20 + 30) / 3 = 20.
        let accumulator = accumulate(&[(1, 10), (1, 20), (1, 30)]);
        assert_eq!(accumulator.slope(), Some(20.0));
    }

    #[test]
    fn slope_weights_spans_by_iteration_count() {
        // Two spans on a perfectly linear series (5 per iter): the slope recovers
        // exactly 5 regardless of the differing iteration counts.
        let accumulator = accumulate(&[(2, 10), (8, 40)]);
        assert_eq!(accumulator.slope(), Some(5.0));
    }

    #[test]
    fn low_iteration_outlier_barely_moves_the_slope() {
        // A noisy single-iteration warmup span (1000) alongside many
        // high-iteration spans at 5 per iter is down-weighted by iters², so the
        // slope stays close to 5 rather than being dragged toward 1000.
        let accumulator = accumulate(&[(1, 1000), (1000, 5000), (1000, 5000)]);
        let slope = accumulator.slope().unwrap();
        assert!(
            slope < 6.0,
            "slope should resist the warmup outlier: {slope}"
        );
    }

    #[test]
    fn zero_iteration_spans_have_a_nan_slope_and_no_interval() {
        // With every span at zero iterations the weighted denominator Σ(nᵢ²) is
        // zero, so the per-iteration rate is 0/0 = undefined (NaN) and no
        // interval can be formed.
        let accumulator = accumulate(&[(0, 1000), (0, 2000)]);
        assert!(accumulator.slope().unwrap().is_nan());
        assert_eq!(accumulator.interval(), None);
    }

    #[test]
    fn zero_iteration_span_does_not_poison_a_finite_slope() {
        // A zero-iteration span (e.g. a workload that failed to run) contributes
        // nothing to either moment, so the slope is still recovered from the
        // remaining nonzero spans rather than collapsing to NaN.
        let accumulator = accumulate(&[(0, 1000), (2, 10), (2, 10)]);
        assert_eq!(accumulator.slope(), Some(5.0));
    }

    #[test]
    fn single_span_has_a_slope_but_no_interval() {
        // One span pins the slope but carries no dispersion information, so the
        // interval is withheld rather than fabricated as a zero-width point.
        let accumulator = accumulate(&[(4, 80)]);
        assert_eq!(accumulator.slope(), Some(20.0));
        assert_eq!(accumulator.interval(), None);
    }

    #[test]
    fn interval_matches_the_closed_form_robust_standard_error() {
        // Per-iteration values 10, 20, 30 (single iterations): slope 20, residual
        // sum of squares 200, SE = sqrt(200) / 3, half-width = 1.96·SE.
        let accumulator = accumulate(&[(1, 10), (1, 20), (1, 30)]);
        let (low, high) = accumulator.interval().unwrap();
        let expected_half = Z_95 * (200.0_f64.sqrt() / 3.0);
        assert_close(low, 20.0 - expected_half);
        assert_close(high, 20.0 + expected_half);
    }

    #[test]
    fn interval_brackets_the_point_estimate() {
        let accumulator = accumulate(&[(1, 18), (1, 20), (1, 22), (1, 19), (1, 21), (1, 20)]);
        let slope = accumulator.slope().unwrap();
        let (low, high) = accumulator.interval().unwrap();
        assert!(
            low <= slope && slope <= high,
            "point {slope} must lie within [{low}, {high}]"
        );
    }

    #[test]
    fn wider_spread_yields_a_wider_interval() {
        let tight = accumulate(&[(1, 19), (1, 20), (1, 21), (1, 20), (1, 19), (1, 21)]);
        let wide = accumulate(&[(1, 5), (1, 20), (1, 35), (1, 8), (1, 32), (1, 20)]);

        let (tight_low, tight_high) = tight.interval().unwrap();
        let (wide_low, wide_high) = wide.interval().unwrap();
        assert!(
            wide_high - wide_low > tight_high - tight_low,
            "wide interval should exceed tight interval"
        );
    }

    #[test]
    fn identical_spans_collapse_the_interval_onto_the_point() {
        // Two identical spans have zero residual dispersion, so the interval
        // collapses onto the slope even though two spans clear the `< 2` guard.
        let accumulator = accumulate(&[(2, 80), (2, 80)]);
        assert_eq!(accumulator.slope(), Some(40.0));
        assert_eq!(accumulator.interval(), Some((40.0, 40.0)));
    }

    #[test]
    fn interval_is_deterministic_across_computations() {
        let accumulator = accumulate(&[(1, 18), (1, 20), (1, 22), (1, 19), (1, 21)]);
        assert_eq!(accumulator.interval(), accumulator.interval());
    }

    #[test]
    fn lower_bound_is_clamped_at_zero() {
        // A near-zero slope with wide dispersion would push the analytic lower
        // bound negative; a measured quantity is non-negative, so it clamps to 0.
        let accumulator = accumulate(&[(1, 0), (1, 0), (1, 100), (1, 0), (1, 0)]);
        let (low, high) = accumulator.interval().unwrap();
        assert_eq!(low, 0.0);
        assert!(high > 0.0);
    }

    #[test]
    fn multi_iteration_interval_uses_the_fourth_moment() {
        // Non-collinear spans with iterations > 1 (per-iteration 5 vs 3) make the
        // Σnᵢ⁴ term of the robust variance load-bearing. Single-iteration spans
        // leave nᵢ⁴ = 1 for every span, so they cannot tell a correct fourth
        // moment apart from a degenerate one. slope = 68 / 20 = 3.4, residual sum
        // of squares 81.92, SE = sqrt(81.92) / 20.
        let accumulator = accumulate(&[(2, 10), (4, 12)]);
        assert_close(accumulator.slope().unwrap(), 3.4);
        let (low, high) = accumulator.interval().unwrap();
        let expected_half = Z_95 * (81.92_f64.sqrt() / 20.0);
        assert_close(low, 3.4 - expected_half);
        assert_close(high, 3.4 + expected_half);
    }

    #[test]
    fn a_non_finite_standard_error_withholds_the_interval() {
        // Realistic span inputs cannot overflow the folded moments, but the
        // public contract still defuses a non-finite standard error to `None`
        // rather than emitting a bogus interval. Drive the residual sum of
        // squares to infinity (via `s_nntt`) while the slope stays finite, and
        // confirm the interval is withheld — exercising the finiteness guard's
        // standard-error branch, which a finite slope alone would leave live.
        let mut accumulator = accumulate(&[(2, 10), (4, 12)]);
        assert!(accumulator.slope().unwrap().is_finite());
        accumulator.s_nntt = f64::INFINITY;
        assert_eq!(accumulator.interval(), None);
    }

    #[test]
    fn merge_is_equivalent_to_folding_both_populations() {
        let combined = accumulate(&[(2, 20), (4, 40), (1, 9), (3, 33)]);

        let mut left = accumulate(&[(2, 20), (4, 40)]);
        let right = accumulate(&[(1, 9), (3, 33)]);
        left.merge(&right);

        assert_eq!(left.span_count(), combined.span_count());
        assert_close(left.slope().unwrap(), combined.slope().unwrap());
        let (left_low, left_high) = left.interval().unwrap();
        let (combined_low, combined_high) = combined.interval().unwrap();
        assert_close(left_low, combined_low);
        assert_close(left_high, combined_high);
    }

    #[test]
    fn merging_an_empty_accumulator_changes_nothing() {
        let mut accumulator = accumulate(&[(1, 10), (1, 30)]);
        let before = accumulator.interval();
        accumulator.merge(&SpanAccumulator::new());
        assert_eq!(accumulator.span_count(), 2);
        assert_eq!(accumulator.interval(), before);
    }

    #[test]
    fn large_iteration_counts_stay_finite() {
        // Per-sample recording with a large iteration count exercises the
        // high-order moments (nᵢ⁴ ≈ 10²⁸) that would overflow u64; in f64 they
        // stay finite and the interval remains well-formed.
        let accumulator = accumulate(&[
            (10_000_000, 250_000_000),
            (10_000_000, 250_000_100),
            (10_000_000, 249_999_900),
        ]);
        let slope = accumulator.slope().unwrap();
        assert_close(slope, 25.0);
        let (low, high) = accumulator.interval().unwrap();
        assert!(low.is_finite() && high.is_finite() && low <= slope && slope <= high);
    }
}
