//! Selection-adjusted change-point significance.
//!
//! A change-point detector searches the whole history and reports the split
//! that looks strongest. The Mann-Whitney p-value at that chosen split is
//! therefore tainted by the search: even unchanged histories occasionally have
//! one unusually convincing split.
//!
//! [`selection_adjusted_change_point`] combines a conservative analytic bound over
//! every eligible split with a complete finite permutation-group orbit of the series'
//! actual values. Every permuted ordering retains the same values and ties, runs the
//! same Pettitt first-maximum split selection, applies the same minimum-regime rule,
//! and scores the accepted split with the same exact-or-normal Mann-Whitney
//! implementation. Both components therefore account for split selection without
//! trusting the tainted winning score.

mod change_point;
mod permutation;

pub use change_point::*;
