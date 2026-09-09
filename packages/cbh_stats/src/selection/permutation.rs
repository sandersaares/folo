//! Deterministic exact permutation subgroups for runtime calibration.

/// Smallest coordinate that can contribute a nontrivial symmetric group.
const MIN_FACTOR: usize = 2;
/// Canonical `SplitMix64` increment used to conjugate the exact group reproducibly.
const SCATTER_INCREMENT: u64 = 0x9e37_79b9_7f4a_7c15;
/// Canonical `SplitMix64` first mixer used to conjugate the exact group reproducibly.
const SCATTER_MIXER_1: u64 = 0xbf58_476d_1ce4_e5b9;
/// Canonical `SplitMix64` second mixer used to conjugate the exact group reproducibly.
const SCATTER_MIXER_2: u64 = 0x94d0_49bb_1331_11eb;
/// Positions moved by the balanced short-history subgroup.
const BALANCED_GROUP_POINTS: usize = 12;
/// Coordinate size used by the balanced short-history subgroup.
const BALANCED_GROUP_FACTOR: usize = 6;
/// Order of the direct product of one alternating and one symmetric coordinate.
const BALANCED_GROUP_ORDER: usize = 259_200;

/// Complete conditional orbit used to calibrate one observed ordering.
///
/// When every distinct ordering of the tied rank multiset fits the budget, the full
/// orbit gives maximum resolution. Otherwise a complete finite subgroup keeps work
/// bounded without replacing exact randomization by a sample.
pub(crate) enum PermutationOrbit {
    Distinct { current: Vec<usize>, order: usize },
    Subgroup(PermutationGroup),
}

/// Exact subgroup used to randomize selected positions across the history.
///
/// Each factor contributes a symmetric group over one mixed-radix coordinate.
/// Their direct product acts on evenly distributed series positions, so every
/// group element is enumerated exactly once and the identity is included.
pub(crate) struct PermutationGroup {
    coordinates: Vec<Vec<usize>>,
    positions: Vec<usize>,
    grid_size: usize,
    order: usize,
    disjoint: Option<DisjointGroup>,
}

/// All-position subgroup for short histories where a Cartesian grid is too small.
///
/// The action is `A6 × S6` over disjoint, scattered position sets. It supplies a
/// large exact orbit while moving every position of the shortest crowded fixtures.
struct DisjointGroup {
    coordinates: Vec<Coordinate>,
    positions: Vec<usize>,
}

/// One independently enumerated coordinate of a disjoint permutation group.
struct Coordinate {
    values: Vec<usize>,
    alternating_only: bool,
}

/// Factorial-product decomposition that maximizes exact group order.
///
/// `factors` are mixed-radix coordinate sizes. Their product is the number of
/// positions the group moves, while the product of their factorials is its order.
#[derive(Clone, Debug, Eq, PartialEq)]
struct Factorization {
    factors: Vec<usize>,
    grid_size: usize,
    order: usize,
}

impl PermutationOrbit {
    pub(crate) fn new(sorted: Vec<usize>, series_len: usize, order_budget: usize) -> Self {
        if let Some(order) = distinct_permutation_count(&sorted, order_budget) {
            Self::Distinct {
                current: sorted,
                order,
            }
        } else {
            Self::Subgroup(PermutationGroup::new(series_len, order_budget))
        }
    }

    pub(crate) fn order(&self) -> usize {
        match self {
            Self::Distinct { order, .. } => *order,
            Self::Subgroup(group) => group.order(),
        }
    }

    pub(crate) fn apply(&self, source: &[usize], target: &mut [usize]) {
        match self {
            Self::Distinct { current, .. } => target.copy_from_slice(current),
            Self::Subgroup(group) => group.apply(source, target),
        }
    }

    pub(crate) fn advance(&mut self) -> bool {
        match self {
            Self::Distinct { current, .. } => next_permutation(current),
            Self::Subgroup(group) => group.advance(),
        }
    }
}

impl PermutationGroup {
    pub(crate) fn new(series_len: usize, order_budget: usize) -> Self {
        let factorization = best_factorization(series_len, order_budget);
        if uses_balanced_group(series_len, order_budget, &factorization) {
            assert!(
                series_len >= BALANCED_GROUP_POINTS
                    && order_budget >= BALANCED_GROUP_ORDER
                    && factorization.grid_size < BALANCED_GROUP_POINTS,
                "the balanced group requires enough positions and orbit budget"
            );
            return Self {
                coordinates: Vec::new(),
                positions: Vec::new(),
                grid_size: 0,
                order: BALANCED_GROUP_ORDER,
                disjoint: Some(DisjointGroup::new(series_len)),
            };
        }
        let positions = scattered_positions(series_len, factorization.grid_size);
        let coordinates = factorization
            .factors
            .iter()
            .map(|&factor| (0..factor).collect())
            .collect();
        Self {
            coordinates,
            positions,
            grid_size: factorization.grid_size,
            order: factorization.order,
            disjoint: None,
        }
    }

    pub(crate) fn order(&self) -> usize {
        self.order
    }

    /// Applies the current group element to `source`.
    pub(crate) fn apply(&self, source: &[usize], target: &mut [usize]) {
        if let Some(group) = &self.disjoint {
            group.apply(source, target);
            return;
        }
        assert_eq!(
            source.len(),
            target.len(),
            "a permutation preserves the series length"
        );
        target.copy_from_slice(source);
        for source_bucket in 0..self.grid_size {
            let target_bucket = self.map_bucket(source_bucket);
            let source_index = self
                .positions
                .get(source_bucket)
                .copied()
                .expect("every source bucket has a distributed position");
            let target_index = self
                .positions
                .get(target_bucket)
                .copied()
                .expect("every target bucket has a distributed position");
            let value = source
                .get(source_index)
                .copied()
                .expect("distributed positions lie inside the source");
            let target = target
                .get_mut(target_index)
                .expect("distributed positions lie inside the target");
            *target = value;
        }
    }

    /// Advances to the next group element, returning `false` after the last.
    pub(crate) fn advance(&mut self) -> bool {
        if let Some(group) = &mut self.disjoint {
            return group.advance();
        }
        for coordinate in &mut self.coordinates {
            if next_permutation(coordinate) {
                return true;
            }
        }
        false
    }

    fn map_bucket(&self, bucket: usize) -> usize {
        let mut remainder = bucket;
        let mut mapped = 0_usize;
        let mut stride = 1_usize;
        for coordinate in &self.coordinates {
            let factor = coordinate.len();
            let digit = remainder
                .checked_rem(factor)
                .expect("every coordinate factor is nonzero");
            remainder = remainder
                .checked_div(factor)
                .expect("every coordinate factor is nonzero");
            let mapped_digit = coordinate
                .get(digit)
                .copied()
                .expect("a coordinate permutation covers every digit");
            mapped = mapped.saturating_add(mapped_digit.saturating_mul(stride));
            stride = stride.saturating_mul(factor);
        }
        debug_assert_eq!(remainder, 0, "the bucket fits the mixed-radix grid");
        mapped
    }
}

impl DisjointGroup {
    fn new(series_len: usize) -> Self {
        Self {
            coordinates: vec![
                Coordinate::new(BALANCED_GROUP_FACTOR, true),
                Coordinate::new(BALANCED_GROUP_FACTOR, false),
            ],
            positions: scattered_positions(series_len, BALANCED_GROUP_POINTS),
        }
    }

    fn apply(&self, source: &[usize], target: &mut [usize]) {
        assert_eq!(
            source.len(),
            target.len(),
            "a permutation preserves the series length"
        );
        target.copy_from_slice(source);
        let mut offset = 0_usize;
        for coordinate in &self.coordinates {
            for source_digit in 0..coordinate.values.len() {
                let target_digit = coordinate
                    .values
                    .get(source_digit)
                    .copied()
                    .expect("a coordinate permutation covers every digit");
                let source_bucket = offset.saturating_add(source_digit);
                let target_bucket = offset.saturating_add(target_digit);
                let source_index = self
                    .positions
                    .get(source_bucket)
                    .copied()
                    .expect("every source bucket has a scattered position");
                let target_index = self
                    .positions
                    .get(target_bucket)
                    .copied()
                    .expect("every target bucket has a scattered position");
                let value = source
                    .get(source_index)
                    .copied()
                    .expect("scattered positions lie inside the source");
                let target = target
                    .get_mut(target_index)
                    .expect("scattered positions lie inside the target");
                *target = value;
            }
            offset = offset.saturating_add(coordinate.values.len());
        }
    }

    fn advance(&mut self) -> bool {
        for coordinate in &mut self.coordinates {
            if coordinate.advance() {
                return true;
            }
        }
        false
    }
}

impl Coordinate {
    fn new(factor: usize, alternating_only: bool) -> Self {
        Self {
            values: (0..factor).collect(),
            alternating_only,
        }
    }

    fn advance(&mut self) -> bool {
        loop {
            if !next_permutation(&mut self.values) {
                return false;
            }
            if !self.alternating_only || is_even_permutation(&self.values) {
                return true;
            }
        }
    }
}

impl Factorization {
    fn identity() -> Self {
        Self {
            factors: Vec::new(),
            grid_size: 1,
            order: 1,
        }
    }
}

pub(crate) fn group_order(series_len: usize, order_budget: usize) -> usize {
    let factorization = best_factorization(series_len, order_budget);
    if uses_balanced_group(series_len, order_budget, &factorization) {
        BALANCED_GROUP_ORDER
    } else {
        factorization.order
    }
}

fn uses_balanced_group(
    series_len: usize,
    order_budget: usize,
    factorization: &Factorization,
) -> bool {
    series_len >= BALANCED_GROUP_POINTS
        && order_budget >= BALANCED_GROUP_ORDER
        && factorization.grid_size < BALANCED_GROUP_POINTS
}

fn distinct_permutation_count(sorted: &[usize], limit: usize) -> Option<usize> {
    let mut order = 1_usize;
    let mut seen = 0_usize;
    let mut start = 0_usize;
    while start < sorted.len() {
        let value = sorted
            .get(start)
            .copied()
            .expect("the run starts inside the sorted ranks");
        let run_len = sorted
            .get(start..)
            .expect("the run starts inside the sorted ranks")
            .iter()
            .take_while(|&&rank| rank == value)
            .count();
        assert_ne!(run_len, 0, "a rank run always contains its first value");
        let remaining_limit = limit
            .checked_div(order)
            .expect("the accumulated orbit order is nonzero");
        let choices = bounded_binomial(seen.saturating_add(run_len), run_len, remaining_limit)?;
        order = order
            .checked_mul(choices)
            .filter(|&candidate| candidate <= limit)?;
        seen = seen.saturating_add(run_len);
        start = start.saturating_add(run_len);
    }
    Some(order)
}

fn bounded_binomial(n: usize, k: usize, limit: usize) -> Option<usize> {
    let k = k.min(n.saturating_sub(k));
    let mut value = 1_u128;
    let limit = u128::try_from(limit).expect("usize fits u128");
    for step in 1..=k {
        let numerator = n.saturating_sub(k).saturating_add(step);
        value = value
            .saturating_mul(u128::try_from(numerator).expect("usize fits u128"))
            .checked_div(u128::try_from(step).expect("usize fits u128"))
            .expect("a binomial recurrence step is nonzero");
        if value > limit {
            return None;
        }
    }
    usize::try_from(value).ok()
}

fn best_factorization(series_len: usize, order_budget: usize) -> Factorization {
    let options = factorial_options(series_len, order_budget);
    let mut best = Factorization::identity();
    let mut current = Factorization::identity();
    if let Some(max_index) = options.len().checked_sub(1) {
        search_factorizations(
            &options,
            max_index,
            series_len,
            order_budget,
            &mut current,
            &mut best,
        );
    }
    best
}

fn factorial_options(series_len: usize, order_budget: usize) -> Vec<(usize, usize)> {
    let mut options = Vec::new();
    let mut factorial = 1_usize;
    for factor in MIN_FACTOR..=series_len {
        let Some(next) = factorial.checked_mul(factor) else {
            break;
        };
        if next > order_budget {
            break;
        }
        factorial = next;
        options.push((factor, factorial));
    }
    options
}

fn search_factorizations(
    options: &[(usize, usize)],
    max_index: usize,
    series_len: usize,
    order_budget: usize,
    current: &mut Factorization,
    best: &mut Factorization,
) {
    for index in (0..=max_index).rev() {
        let (factor, factorial) = options
            .get(index)
            .copied()
            .expect("the search index comes from the options");
        let Some(grid_size) = current.grid_size.checked_mul(factor) else {
            continue;
        };
        let Some(order) = current.order.checked_mul(factorial) else {
            continue;
        };
        if grid_size > series_len || order > order_budget {
            continue;
        }

        current.factors.push(factor);
        let previous_grid_size = current.grid_size;
        let previous_order = current.order;
        current.grid_size = grid_size;
        current.order = order;
        assert!(
            grid_size <= series_len && order <= order_budget,
            "accepted factors stay within the position and orbit budgets"
        );
        if is_better_factorization(current, best) {
            best.clone_from(current);
        }
        search_factorizations(options, index, series_len, order_budget, current, best);
        current.grid_size = previous_grid_size;
        current.order = previous_order;
        _ = current.factors.pop();
    }
}

fn is_better_factorization(candidate: &Factorization, incumbent: &Factorization) -> bool {
    (candidate.order, candidate.grid_size) > (incumbent.order, incumbent.grid_size)
}

fn spread_index(bucket: usize, series_len: usize, grid_size: usize) -> usize {
    let bucket = u128::try_from(bucket).expect("usize fits u128");
    let series_len = u128::try_from(series_len).expect("usize fits u128");
    let grid_size = u128::try_from(grid_size).expect("usize fits u128");
    usize::try_from(
        bucket
            .saturating_mul(series_len)
            .checked_div(grid_size)
            .expect("the grid size is nonzero"),
    )
    .expect("the distributed index is below the series length")
}

fn scattered_positions(series_len: usize, grid_size: usize) -> Vec<usize> {
    let mut buckets: Vec<usize> = (0..grid_size).collect();
    // The SplitMix64 finalizer is a deterministic bijection. It conjugates the
    // complete group to avoid temporal alignment; it never samples group elements.
    buckets.sort_unstable_by_key(|&bucket| scatter_key(bucket));
    let mut positions = vec![0; grid_size];
    for (temporal_bucket, group_bucket) in buckets.into_iter().enumerate() {
        let position = positions
            .get_mut(group_bucket)
            .expect("every scattered bucket belongs to the group grid");
        *position = spread_index(temporal_bucket, series_len, grid_size);
    }
    positions
}

fn scatter_key(index: usize) -> u64 {
    let mut value = u64::try_from(index)
        .expect("supported platforms represent usize in no more than 64 bits")
        .wrapping_add(SCATTER_INCREMENT);
    value = (value ^ (value >> 30)).wrapping_mul(SCATTER_MIXER_1);
    value = (value ^ (value >> 27)).wrapping_mul(SCATTER_MIXER_2);
    value ^ (value >> 31)
}

fn next_permutation(values: &mut [usize]) -> bool {
    let Some(pivot) = values
        .array_windows::<2>()
        .rposition(|[left, right]| left < right)
    else {
        values.reverse();
        return false;
    };
    let pivot_value = values
        .get(pivot)
        .copied()
        .expect("the pivot comes from this slice");
    let successor = values
        .iter()
        .enumerate()
        .skip(pivot.saturating_add(1))
        .rfind(|(_, value)| **value > pivot_value)
        .map(|(index, _)| index)
        .expect("a lexicographic pivot has a larger suffix value");
    values.swap(pivot, successor);
    values
        .get_mut(pivot.saturating_add(1)..)
        .expect("the suffix starts inside the slice")
        .reverse();
    true
}

fn is_even_permutation(values: &[usize]) -> bool {
    let inversions = values
        .iter()
        .enumerate()
        .map(|(index, value)| {
            values
                .get(index.saturating_add(1)..)
                .expect("the suffix starts within the permutation")
                .iter()
                .filter(|candidate| candidate.cmp(&value).is_lt())
                .count()
        })
        .sum::<usize>();
    inversions.is_multiple_of(2)
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::collections::{HashMap, HashSet};

    use super::*;

    #[test]
    fn factorization_maximizes_order_within_length_and_budget() {
        assert_eq!(
            best_factorization(1_000, 500_000),
            Factorization {
                factors: vec![4, 4, 4, 3, 3],
                grid_size: 576,
                order: 497_664,
            }
        );
        assert_eq!(
            best_factorization(10, 600),
            Factorization {
                factors: vec![5, 2],
                grid_size: 10,
                order: 240,
            }
        );
        assert_eq!(
            best_factorization(12, 24),
            Factorization {
                factors: vec![3, 2, 2],
                grid_size: 12,
                order: 24,
            }
        );
        let tied = Factorization {
            factors: vec![4],
            grid_size: 4,
            order: 24,
        };
        assert!(!is_better_factorization(&tied, &tied));
    }

    #[test]
    fn group_enumerates_every_element_once_and_starts_at_identity() {
        let source: Vec<usize> = (0..6).collect();
        let mut target = source.clone();
        let mut group = PermutationGroup::new(source.len(), 24);
        let mut seen = HashSet::new();
        for element in 0..group.order() {
            group.apply(&source, &mut target);
            assert!(seen.insert(target.clone()));
            if element.saturating_add(1) < group.order() {
                assert!(group.advance());
            }
        }
        assert_eq!(seen.len(), 24);
        assert!(!group.advance());
    }

    #[test]
    fn complete_tied_orbit_is_used_when_it_fits_the_budget() {
        // A small tied multiset covers uniqueness and completion without storing a large orbit.
        let sorted = vec![1, 1, 1, 2, 2, 2];
        let mut orbit = PermutationOrbit::new(sorted.clone(), sorted.len(), 20);
        assert_eq!(orbit.order(), 20);

        let mut ordering = sorted.clone();
        let mut seen = HashSet::new();
        for element in 0..orbit.order() {
            orbit.apply(&sorted, &mut ordering);
            assert!(seen.insert(ordering.clone()));
            if element.saturating_add(1) < orbit.order() {
                assert!(orbit.advance());
            }
        }
        assert_eq!(seen.len(), 20);
        assert!(!orbit.advance());
    }

    #[test]
    fn oversized_distinct_orbit_uses_the_bounded_subgroup() {
        let sorted: Vec<usize> = (0..10).collect();
        let orbit = PermutationOrbit::new(sorted.clone(), sorted.len(), 600);
        assert!(matches!(orbit, PermutationOrbit::Subgroup(_)));
        assert_eq!(orbit.order(), 240);
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "enumerating the production short-history group is too slow under interpretation"
    )]
    fn short_history_uses_the_balanced_all_position_group() {
        let source: Vec<usize> = (0..BALANCED_GROUP_POINTS).collect();
        let mut target = source.clone();
        let mut group = PermutationGroup::new(BALANCED_GROUP_POINTS, BALANCED_GROUP_ORDER);
        assert_eq!(group.order(), BALANCED_GROUP_ORDER);
        assert!(group.disjoint.is_some());
        group.apply(&source, &mut target);
        assert_eq!(target, source);

        let tied_source = [vec![0; 6], vec![1; 6]].concat();
        let mut multiplicities = HashMap::<Vec<usize>, usize>::new();
        for element in 0..group.order() {
            group.apply(&tied_source, &mut target);
            let multiplicity = multiplicities.entry(target.clone()).or_default();
            *multiplicity = multiplicity.saturating_add(1);
            if element.saturating_add(1) < group.order() {
                assert!(group.advance());
            }
        }
        // Each scattered block receives three of each value, giving
        // C(6, 3) squared distinct orderings with uniform stabilizer multiplicity.
        assert_eq!(multiplicities.len(), 400);
        let expected_multiplicity = group
            .order()
            .checked_div(multiplicities.len())
            .expect("the tied orbit is nonempty");
        assert!(
            multiplicities
                .values()
                .all(|&multiplicity| multiplicity == expected_multiplicity)
        );
        assert!(!group.advance());
    }

    #[test]
    fn balanced_override_yields_to_the_broader_long_history_group() {
        assert_eq!(group_order(16, 500_000), BALANCED_GROUP_ORDER);
        assert_eq!(group_order(1_000, 500_000), 497_664);
        let complete_order = (1..=BALANCED_GROUP_POINTS).product();
        assert_eq!(
            group_order(BALANCED_GROUP_POINTS, complete_order),
            complete_order
        );
    }

    #[test]
    fn alternating_coordinate_enumerates_only_even_permutations() {
        let mut coordinate = Coordinate::new(4, true);
        let mut seen = HashSet::new();
        seen.insert(coordinate.values.clone());
        while coordinate.advance() {
            assert!(is_even_permutation(&coordinate.values));
            assert!(seen.insert(coordinate.values.clone()));
        }
        assert_eq!(seen.len(), 12);
    }

    #[test]
    fn bounded_binomial_includes_its_limit() {
        assert_eq!(bounded_binomial(6, 3, 20), Some(20));
        assert_eq!(bounded_binomial(6, 3, 19), None);
    }

    #[test]
    fn scatter_key_matches_the_splitmix64_reference() {
        assert_eq!(scatter_key(0), 0xe220_a839_7b1d_cdaf);
    }

    #[test]
    fn group_scatters_its_moved_positions_across_the_series() {
        let group = PermutationGroup::new(10, 24);
        assert_eq!(group.positions, vec![7, 2, 5, 0]);
    }

    #[test]
    fn group_scatters_product_coordinates_across_time() {
        let group = PermutationGroup::new(12, 2_000);
        assert_eq!(group.positions, vec![11, 6, 7, 1, 5, 3, 10, 4, 8, 9, 0, 2]);
    }

    #[test]
    fn budget_below_the_smallest_group_uses_only_identity() {
        let source = [1, 2, 3];
        let mut target = [0; 3];
        let mut group = PermutationGroup::new(source.len(), 1);
        assert_eq!(group.order(), 1);
        group.apply(&source, &mut target);
        assert_eq!(target, source);
        assert!(!group.advance());
    }
}
