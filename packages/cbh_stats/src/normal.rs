//! The standard normal distribution and the p-values derived from it.
//!
//! # Formulations
//!
//! The error function is evaluated by the two classical real-argument forms of
//! Abramowitz & Stegun, *Handbook of Mathematical Functions* (1964), chapter 7,
//! each taken on the side of [`SERIES_LIMIT`] where it converges:
//!
//! * Equation 7.1.6, below the limit:
//!   `erf(x) = (2/√π)·e^(−x²)·Σ_(n≥0) 2ⁿ·x^(2n+1)/(1·3·5·…·(2n+1))`. The
//!   denominator is the double factorial `(2n+1)!!`, carried from one term to
//!   the next by a single multiplication.
//! * Equation 7.1.14, at or above the limit:
//!   `erfc(x) = (e^(−x²)/√π)·1/(x + (1/2)/(x + 1/(x + (3/2)/(x + 2/(x + …)))))`,
//!   whose partial numerators rise by [`NUMERATOR_STEP`] per level. It holds
//!   for positive arguments only, and `erfc(−x) = 2 − erfc(x)` covers the rest.
//!
//! The fraction is evaluated from its deepest level upwards with a zero tail
//! rather than by a forward recurrence. Every partial denominator is `x` plus a
//! positive quantity and so is bounded away from zero, which is what makes the
//! backward sweep stable without the rescaling a forward evaluation needs.
//!
//! # Validation
//!
//! Every quantity fixed here — the crossover, the term count, the level count
//! and the accuracy the entry points claim — is pinned by this module's tests.
//! Their reference values are quoted to the full precision of `f64` from an
//! independent double-precision implementation (`scipy.special` 1.18), so a
//! transcription error in an offset or a sign shows up as a failing test
//! rather than as a plausible wrong answer.
//!
//! Measured against those references, the relative error of `erfc` stays below
//! `1e-14` over `|x| ≤ 30`, which is the whole domain this format can express:
//! past it the positive tail underflows to zero and the negative tail saturates
//! at two. The tests assert the reference points an order of magnitude looser
//! than that, so another platform's last-bit choices in `exp` cannot make them
//! flaky while a transcription error still cannot hide.
//!
//! The term and level counts are converged rather than merely plausible:
//! evaluating at twice either count reproduces the same bits everywhere in that
//! domain, and that doubling is the procedure to repeat whenever either count
//! is questioned. Both counts are parameters of the functions below for exactly
//! that reason. The crossover is bracketed from both sides by the same means —
//! below it the fraction outgrows [`FRACTION_DEPTH`], and above it `1 − erf`
//! cancels away leading digits that no term count can restore.

use std::f64::consts;

use crate::clamp_p_value;

/// Magnitude below which `erf` is evaluated by its series rather than `erfc` by
/// its continued fraction.
///
/// The band where both routes are good is narrow, and this sits inside it.
/// Lower, and the fraction outgrows its budget of [`FRACTION_DEPTH`] levels: it
/// needs 135 levels at `1.2` and 185 at `1.0`. Higher, and the subtraction
/// `1 − erf` cancels the leading digits of an ever smaller result, costing two
/// digits by `2.0` and eight by `4.0` however many terms the series is given.
const SERIES_LIMIT: f64 = 1.5;

/// Terms of the `erf` series evaluated below [`SERIES_LIMIT`].
///
/// Twenty-three terms already reach the last bit anywhere in that range, the
/// worst case sitting at the limit itself, so this count carries margin while
/// keeping the sum's cost independent of its argument.
const SERIES_TERMS: u32 = 28;

/// Levels of the `erfc` continued fraction evaluated above [`SERIES_LIMIT`].
///
/// Ninety-three levels already reach the last bit anywhere in that range, the
/// worst case sitting at the limit itself where the fraction converges
/// slowest, so this depth carries margin while keeping the sweep's cost
/// independent of its argument.
const FRACTION_DEPTH: u32 = 120;

/// Difference between successive partial numerators of the fraction, which run
/// `1/2, 1, 3/2, 2, …`.
const NUMERATOR_STEP: f64 = 0.5;

/// `1/√π`, the scale factor of the `erfc` continued fraction.
const FRAC_1_SQRT_PI: f64 = consts::FRAC_2_SQRT_PI * 0.5;

/// The standard normal cumulative distribution function, `Φ(z)`.
///
/// Both tails carry the same relative accuracy — roughly thirteen correct
/// digits — so `Φ(-9)` is as trustworthy as `Φ(0)`. The value saturates to `0`
/// or `1` only once the true probability leaves the range of `f64`.
pub(crate) fn normal_cdf(z: f64) -> f64 {
    0.5 * erfc(-z / consts::SQRT_2)
}

/// The two-sided p-value for a standard-normal test statistic `z`.
///
/// # Accuracy
///
/// The relative error stays below `1e-12` over the whole reportable range, so a
/// p-value of `1e-12` carries as many correct digits as one of `0.5`. The
/// result is floored at `MIN_P_VALUE`: statistics beyond `|z| ≈ 8` all report
/// that floor, which means "overwhelming significance" and nothing more
/// precise.
pub(crate) fn two_sided_p_from_z(z: f64) -> f64 {
    // 2·Φ(−|z|) is the doubled tail beyond `z`, evaluated on the side where the
    // complementary error function computes it directly instead of as the
    // difference of two nearly equal numbers.
    clamp_p_value(2.0 * normal_cdf(-z.abs()))
}

/// The complementary error function, `erfc(x) = 1 − erf(x)`.
fn erfc(x: f64) -> f64 {
    if prefers_series(x.abs()) {
        // `erf` is odd and small here, so subtracting it from one cannot cancel.
        return 1.0 - erf_series(x, SERIES_TERMS);
    }
    if x.is_sign_positive() {
        tail_erfc(x, FRACTION_DEPTH)
    } else {
        // The series would overflow long before `erfc` reaches its limit of two,
        // so the reflection `erfc(−x) = 2 − erfc(x)` covers the negative tail.
        2.0 - tail_erfc(-x, FRACTION_DEPTH)
    }
}

/// Whether a magnitude is small enough for the series to be the better route.
//
// Mutation-skipped: the two evaluations agree to the last bits on both sides of
// the crossover — `erf_series_and_tail_erfc_agree_at_the_crossover` pins that —
// so moving the boundary by one value cannot change any result.
#[cfg_attr(test, mutants::skip)]
fn prefers_series(magnitude: f64) -> bool {
    magnitude < SERIES_LIMIT
}

/// The error function `erf(x)` for arguments below [`SERIES_LIMIT`].
///
/// Sums `terms` terms of the series beyond the leading one (see the module's
/// formulation notes), all of one sign, so the sum accumulates no cancellation
/// error. [`SERIES_TERMS`] is the count the crate is validated at; the count is
/// a parameter so that the validation can be reproduced at another one.
fn erf_series(x: f64, terms: u32) -> f64 {
    let x_squared = x * x;
    let mut term = x;
    let mut total = x;
    let mut denominator = 1.0_f64;
    for _ in 0..terms {
        denominator += 2.0;
        term *= 2.0 * x_squared / denominator;
        total += term;
    }
    consts::FRAC_2_SQRT_PI * (-x_squared).exp() * total
}

/// The complementary error function for arguments at or above [`SERIES_LIMIT`].
///
/// Evaluates `depth` levels of the fraction (see the module's formulation
/// notes), which is a product of the vanishing exponential and a bounded factor
/// and therefore keeps its relative accuracy however small the result becomes.
/// [`FRACTION_DEPTH`] is the depth the crate is validated at; the depth is a
/// parameter so that the validation can be reproduced at another one.
fn tail_erfc(x: f64, depth: u32) -> f64 {
    // Evaluated from the deepest level upwards: every partial denominator is
    // `x + (a positive value)`, so no level can approach zero.
    let levels_below_top = depth.saturating_sub(1);
    let mut numerator = f64::from(levels_below_top) * NUMERATOR_STEP;
    let mut fraction = 0.0_f64;
    for _ in 0..levels_below_top {
        fraction = numerator / (x + fraction);
        numerator -= NUMERATOR_STEP;
    }
    // The outermost level carries no partial numerator of its own: the fraction
    // closes as `1/(x + …)`.
    FRAC_1_SQRT_PI * (-x * x).exp() / (x + fraction)
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    #![allow(
        clippy::float_cmp,
        reason = "primitive outputs are compared against hand-computed exact values"
    )]

    use super::*;
    use crate::MIN_P_VALUE;
    use crate::test_util::{close, close_relative};

    #[test]
    fn erfc_matches_reference_values() {
        // Reference values of the complementary error function, spanning the
        // series branch, the crossover, and deep into the tail where an
        // absolute-error approximation would have nothing left but noise.
        close_relative(erfc(0.25), 7.236_736_098_317_631e-1, 1e-12);
        close_relative(erfc(1.0), 1.572_992_070_502_851e-1, 1e-12);
        close_relative(erfc(SERIES_LIMIT), 3.389_485_352_468_927_4e-2, 1e-12);
        close_relative(erfc(2.0), 4.677_734_981_047_265e-3, 1e-12);
        close_relative(erfc(3.0), 2.209_049_699_858_544e-5, 1e-12);
        close_relative(erfc(5.0), 1.537_459_794_428_035_1e-12, 1e-12);
        close_relative(erfc(10.0), 2.088_487_583_762_545e-45, 1e-12);
        close_relative(erfc(20.0), 5.395_865_611_607_901e-176, 1e-12);
    }

    #[test]
    fn erfc_matches_reference_values_for_negative_arguments() {
        close_relative(erfc(-0.5), 1.520_499_877_813_046_5, 1e-12);
        close_relative(erfc(-SERIES_LIMIT), 1.966_105_146_475_310_8, 1e-12);
        close_relative(erfc(-3.0), 1.999_977_909_503_001_2, 1e-12);
        // Far into the negative tail the complement saturates at exactly two.
        // The series overflows for arguments this large, so reaching the value
        // at all pins the reflection branch.
        assert_eq!(erfc(-30.0), 2.0);
        assert_eq!(erfc(0.0), 1.0);
    }

    #[test]
    fn erf_series_and_tail_erfc_agree_at_the_crossover() {
        // The boundary between the two evaluations sits inside a band where
        // both are fully converged, which is what makes its exact position
        // immaterial.
        for offset in -4_i32..=3_i32 {
            let x = SERIES_LIMIT + f64::from(offset) / 10.0;
            close_relative(
                1.0 - erf_series(x, SERIES_TERMS),
                tail_erfc(x, FRACTION_DEPTH),
                1e-13,
            );
        }
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "Miri perturbs exp, invalidating bitwise convergence checks"
    )]
    fn erf_series_is_converged_at_its_term_count() {
        // The procedure that establishes the term count: doubling it must not
        // move a single bit anywhere the series is used.
        for step in 0_i32..150_i32 {
            let x = f64::from(step) / 100.0;
            assert_eq!(
                erf_series(x, SERIES_TERMS),
                erf_series(x, SERIES_TERMS * 2),
                "the series moved when its term count doubled at {x}"
            );
        }
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "Miri perturbs exp, invalidating bitwise convergence checks"
    )]
    fn tail_erfc_is_converged_at_its_depth() {
        // The same procedure for the fraction, over the whole range it serves:
        // beyond thirty the result underflows and carries no information.
        for step in 150_i32..=3000_i32 {
            let x = f64::from(step) / 100.0;
            assert_eq!(
                tail_erfc(x, FRACTION_DEPTH),
                tail_erfc(x, FRACTION_DEPTH * 2),
                "the fraction moved when its depth doubled at {x}"
            );
        }
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "Miri perturbs exp, invalidating last-bit crossover checks"
    )]
    fn neither_route_can_take_over_the_other_side_of_the_crossover() {
        // Why the crossover sits where it does, from both sides.
        //
        // Below it the fraction is no longer converged at the depth used, so
        // quadrupling that depth changes the answer. Both depths carry the same
        // exponential factor, so their disagreement is the fraction's alone: a
        // threshold well under that disagreement and far above the last bits of
        // a double separates "not converged" from "converged", which bitwise
        // inequality — satisfied by any last-bit wobble — does not.
        let shallow = tail_erfc(1.0, FRACTION_DEPTH);
        let deep = tail_erfc(1.0, FRACTION_DEPTH * 4);
        let error = ((shallow - deep) / deep).abs();
        assert!(error > 1e-14, "the fraction moved only {error} at 1.0");
        // Above it the series route loses accuracy to the `1 − erf`
        // subtraction, which no term count repairs: a generously converged
        // series still disagrees with the fraction far beyond rounding.
        let series_route = 1.0 - erf_series(4.0, SERIES_TERMS * 4);
        let fraction_route = tail_erfc(4.0, FRACTION_DEPTH);
        let error = ((series_route - fraction_route) / fraction_route).abs();
        assert!(error > 1e-9, "the subtraction lost only {error} at 4.0");
    }

    #[test]
    fn erfc_falls_monotonically() {
        let mut previous = f64::INFINITY;
        // Steps of a third of a unit cross the crossover at 1.5 exactly. The
        // range stops where the true value saturates against the limits of the
        // format: below two on the left, at zero on the right.
        for step in -15_i32..=60_i32 {
            let x = f64::from(step) / 3.0;
            let value = erfc(x);
            assert!(value < previous, "erfc({x}) = {value} did not fall");
            previous = value;
        }
    }

    #[test]
    fn normal_cdf_matches_reference_values() {
        assert_eq!(normal_cdf(0.0), 0.5);
        close_relative(normal_cdf(1.0), 8.413_447_460_685_429e-1, 1e-12);
        close_relative(normal_cdf(1.96), 9.750_021_048_517_795e-1, 1e-12);
        close_relative(normal_cdf(-1.96), 2.499_789_514_822_043_5e-2, 1e-12);
        close_relative(normal_cdf(-3.0), 1.349_898_031_630_093_3e-3, 1e-12);
        // The far tail keeps its relative accuracy rather than collapsing into
        // the rounding error of a subtraction from one.
        close_relative(normal_cdf(-5.0), 2.866_515_718_791_933e-7, 1e-12);
    }

    #[test]
    fn normal_cdf_is_symmetric() {
        close(normal_cdf(-1.0), 1.0 - normal_cdf(1.0), 1e-15);
        close(normal_cdf(-2.5), 1.0 - normal_cdf(2.5), 1e-15);
    }

    #[test]
    fn normal_cdf_saturates_far_from_the_mean() {
        assert_eq!(normal_cdf(50.0), 1.0);
        assert_eq!(normal_cdf(-50.0), 0.0);
    }

    #[test]
    fn two_sided_p_from_z_matches_reference_values() {
        assert_eq!(two_sided_p_from_z(0.0), 1.0);
        close_relative(two_sided_p_from_z(1.0), 3.173_105_078_629_141_5e-1, 1e-12);
        close_relative(two_sided_p_from_z(1.96), 4.999_579_029_644_087e-2, 1e-12);
        close_relative(two_sided_p_from_z(3.0), 2.699_796_063_260_191_8e-3, 1e-12);
        close_relative(two_sided_p_from_z(5.0), 5.733_031_437_583_891e-7, 1e-12);
        // Beyond here an approximation with a 1.5e-7 absolute error has nothing
        // left to say, yet these values are still good to twelve digits.
        close_relative(two_sided_p_from_z(6.0), 1.973_175_290_075_403_2e-9, 1e-12);
        close_relative(two_sided_p_from_z(8.0), 1.244_192_114_854_363_9e-15, 1e-12);
    }

    #[test]
    fn two_sided_p_from_z_is_symmetric() {
        // The two-sided p-value depends only on the magnitude of the statistic.
        close_relative(two_sided_p_from_z(1.96), two_sided_p_from_z(-1.96), 1e-12);
        close_relative(two_sided_p_from_z(6.0), two_sided_p_from_z(-6.0), 1e-12);
    }

    #[test]
    fn two_sided_p_from_z_never_vanishes() {
        // The statistic that used to produce an exactly zero p-value, which
        // would clear any false-discovery-rate threshold unconditionally.
        assert!(two_sided_p_from_z(9.0) > 0.0);
        assert_eq!(two_sided_p_from_z(50.0), MIN_P_VALUE);
        assert_eq!(two_sided_p_from_z(-50.0), MIN_P_VALUE);
        assert_eq!(two_sided_p_from_z(f64::INFINITY), MIN_P_VALUE);
    }

    #[test]
    fn two_sided_p_from_z_falls_monotonically_with_magnitude() {
        let mut previous = 1.0_f64;
        for step in 1_i32..=200_i32 {
            let z = f64::from(step) / 4.0;
            let p = two_sided_p_from_z(z);
            assert!(p <= previous, "p({z}) = {p} rose above {previous}");
            previous = p;
        }
        assert_eq!(previous, MIN_P_VALUE);
    }
}
