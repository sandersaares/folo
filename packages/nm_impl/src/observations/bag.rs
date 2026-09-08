use std::cell::Cell;
use std::hint::cold_path;
use std::iter;
use std::sync::atomic::{self, AtomicI64, AtomicU64};

use crate::Magnitude;

/// Computes the running-total and running-sum increments for a batch observation.
///
/// Returns the amount to add to the occurrence count and the amount to add to the magnitude
/// sum (the batch count scaled by `magnitude`). Both observation-bag variants share this so
/// the conversion policy lives in exactly one place.
///
/// Observation never panics on mathematical overflow (see the `nm` design documentation,
/// "Numeric and panic policies"). The `as` conversions and wrapping multiplication implement
/// that policy: an out-of-range batch is truncated and an overflowing product wraps, yielding
/// an unspecified but non-panicking metric value.
#[inline]
fn observation_increments(magnitude: Magnitude, count: usize) -> (u64, i64) {
    let count_u64 = count as u64;
    #[expect(
        clippy::cast_possible_wrap,
        reason = "Wrapping is the documented nm numeric policy for out-of-range batches."
    )]
    let count_i64 = count as i64;

    let sum_increment = magnitude.wrapping_mul(count_i64);
    (count_u64, sum_increment)
}

/// Selects the histogram bucket that records an observation of `magnitude`.
///
/// Bucket boundaries are upper-bound-inclusive and stored in ascending order, so the first
/// boundary greater than or equal to `magnitude` owns the observation. Returns `None` when
/// `magnitude` exceeds every configured boundary, which the implicit final range absorbs.
///
/// The scan is deliberately linear rather than a binary search. Real histograms almost always
/// have ten or fewer buckets, and for those sizes a branch-predicted linear scan is the
/// cheapest option; a binary search only starts to win past roughly sixteen buckets, an
/// extreme case not worth optimizing for. A manual SIMD ("count less-than") variant was
/// benchmarked and lost across all measured scenarios, because branch prediction handles the
/// sorted lookup well and the SIMD setup (broadcast, compare, mask, popcount) exceeds the cost
/// of a well-predicted scalar loop:
///
/// ```text
/// Scenario                  SIMD     Scalar
/// small_5_hit_first         6.1 ns   1.2 ns
/// small_5_hit_last          5.9 ns   3.3 ns
/// large_32_hit_first       17.3 ns   1.3 ns
/// large_32_hit_last        17.6 ns   9.2 ns
/// large_32_miss            17.4 ns  10.6 ns
/// ```
#[inline]
fn select_bucket(bucket_magnitudes: &[Magnitude], magnitude: Magnitude) -> Option<usize> {
    bucket_magnitudes
        .iter()
        .position(|&bucket_magnitude| magnitude <= bucket_magnitude)
}

/// Records the observations of an event.
///
/// This variant is intended for single-threaded use, though may be shared on that
/// thread via `Rc` or similar mechanisms as it uses interior mutability.
#[derive(Debug)]
pub(crate) struct ObservationBag {
    count: Cell<u64>,
    sum: Cell<i64>,

    bucket_counts: Box<[Cell<u64>]>,
    bucket_magnitudes: &'static [Magnitude],

    /// Bitmap indicating which buckets have been modified since the last `copy_from`.
    ///
    /// Bit `i` (for `i < DIRTY_BUCKETS_OVERFLOW_INDEX`) is set when bucket at index `i`
    /// has been incremented by a non-zero observation. The highest bit
    /// (`DIRTY_BUCKETS_OVERFLOW_INDEX`) is a catch-all that is set when any bucket at
    /// that index or higher is modified. `copy_from` consumes (reads and clears) this
    /// bitmap to skip stores for buckets that have not changed since the previous push.
    ///
    /// Observations with `count == 0` short-circuit before reaching the bucket-update
    /// path, so the dirty bit is only set when the corresponding bucket count actually
    /// changes.
    dirty_buckets: Cell<u64>,
}

/// Records the observations of an event in a thread-safe manner.
///
/// While this variant is intended to be written to from a single thread, the data within
/// may be read from other threads for the purpose of generating metrics reports.
///
/// As reading is lock-free, logically torn reads (of different fields) are entirely possible.
/// Do not assume internal consistency between reading different fields.
#[derive(Debug)]
pub(crate) struct ObservationBagSync {
    count: AtomicU64,
    sum: AtomicI64,

    bucket_counts: Box<[AtomicU64]>,
    bucket_magnitudes: &'static [Magnitude],
}

/// Abstraction over the different types of observation bags.
pub(crate) trait Observations {
    /// Record `count` observations of the given `magnitude`.
    fn insert(&self, magnitude: Magnitude, count: usize);

    /// The bucket magnitudes used by this bag to generate histograms.
    ///
    /// Buckets with different magnitudes are incompatible, so this is used to verify
    /// that two ostensibly similar bags can be merged or compared.
    fn bucket_magnitudes(&self) -> &'static [Magnitude];
}

impl ObservationBag {
    pub(crate) fn new(bucket_magnitudes: &'static [Magnitude]) -> Self {
        let bag = Self {
            count: Cell::new(0),
            sum: Cell::new(0),
            bucket_counts: iter::repeat_with(|| Cell::new(0))
                .take(bucket_magnitudes.len())
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            bucket_magnitudes,
            dirty_buckets: Cell::new(0),
        };

        // The unchecked bucket indexing on the hot path relies on this length invariant.
        // Both fields derive their length from `bucket_magnitudes` above, so the assertion
        // documents the invariant rather than guarding a way it could be violated.
        debug_assert_eq!(
            bag.bucket_counts.len(),
            bag.bucket_magnitudes.len(),
            "we derive count length from magnitudes length, so they must match",
        );

        bag
    }

    /// Returns the current count of observations recorded in this bag.
    ///
    /// The count is incremented monotonically by every observation (by the batch
    /// size, which is non-zero for any data-changing observation). `MetricsPusher`
    /// uses this as a dirty indicator to skip pushing pairs that have not received
    /// new observations since the last push.
    pub(crate) fn count(&self) -> u64 {
        self.count.get()
    }

    /// Reads the dirty-bucket bitmap and clears it.
    ///
    /// Used by `ObservationBagSync::copy_from` to determine which buckets need to be
    /// copied to the global bag without iterating over buckets that have not been
    /// modified since the previous copy. See `dirty_buckets` for the bit encoding.
    pub(crate) fn take_dirty_buckets(&self) -> u64 {
        let bits = self.dirty_buckets.get();
        self.dirty_buckets.set(0);
        bits
    }

    /// Takes a snapshot of the current state for unit-test inspection.
    #[cfg(test)]
    #[cfg_attr(coverage_nightly, coverage(off))]
    pub(crate) fn snapshot(&self) -> ObservationBagSnapshot {
        ObservationBagSnapshot {
            count: self.count.get(),
            sum: self.sum.get(),
            bucket_counts: self
                .bucket_counts
                .iter()
                .map(Cell::get)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            bucket_magnitudes: self.bucket_magnitudes,
        }
    }
}

/// Maximum bucket index that gets its own bit in the per-bag dirty bitmap. Bucket
/// indices at or above this value are coalesced into the highest bit, which acts as
/// a catch-all that causes `copy_from` to scan all buckets at or above the threshold.
/// Histograms of this size are outside the expected workload, so scanning their tail
/// does not justify a larger dirty-state representation on every event.
pub(crate) const DIRTY_BUCKETS_OVERFLOW_INDEX: usize = 63;

/// Relaxed ordering minimizes synchronization on the observation hot path. Reports already
/// permit fields from different instants, so they do not require ordering between independent
/// counters.
const SYNC_BAG_ACCESS_ORDERING: atomic::Ordering = atomic::Ordering::Relaxed;

impl ObservationBagSync {
    pub(crate) fn new(bucket_magnitudes: &'static [Magnitude]) -> Self {
        let bag = Self {
            count: AtomicU64::new(0),
            sum: AtomicI64::new(0),
            bucket_counts: iter::repeat_with(|| AtomicU64::new(0))
                .take(bucket_magnitudes.len())
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            bucket_magnitudes,
        };

        // The unchecked bucket indexing on the hot path relies on this length invariant.
        // Both fields derive their length from `bucket_magnitudes` above, so the assertion
        // documents the invariant rather than guarding a way it could be violated.
        debug_assert_eq!(
            bag.bucket_counts.len(),
            bag.bucket_magnitudes.len(),
            "we derive count length from magnitudes length, so they must match",
        );

        bag
    }

    /// Returns whether `other` records the same histogram shape as `self`.
    ///
    /// Two bags are compatible when their bucket magnitudes are identical, which also
    /// guarantees equal bucket-count lengths because each bag derives its bucket count from
    /// its magnitudes at construction. Only compatible bags may be merged; incompatible bags
    /// describe different histograms.
    pub(crate) fn is_compatible_with(&self, other: &Self) -> bool {
        self.bucket_magnitudes == other.bucket_magnitudes
    }

    /// Merges another observation bag into this one, combining their data sets.
    ///
    /// Typically used when archiving data from unregistered threads into a single archive
    /// bag. Arithmetic wraps rather than panics on overflow, per the `nm` numeric policy (see
    /// the `nm` design documentation, "Numeric and panic policies").
    ///
    /// # Panics
    ///
    /// Panics if the bags have incompatible bucket magnitudes.
    pub(crate) fn merge_from(&self, other: &Self) {
        // Validate compatibility before mutating any field. This keeps the merge
        // transactional: a rejected merge leaves `self` untouched and, because the check
        // precedes every store, an incompatible-merge panic can never occur partway through
        // while a caller holds a lock. Comparing magnitudes (not just lengths) detects
        // same-length histograms whose boundaries differ.
        assert!(self.is_compatible_with(other));

        self.count.fetch_add(
            other.count.load(SYNC_BAG_ACCESS_ORDERING),
            SYNC_BAG_ACCESS_ORDERING,
        );
        self.sum.fetch_add(
            other.sum.load(SYNC_BAG_ACCESS_ORDERING),
            SYNC_BAG_ACCESS_ORDERING,
        );

        // Compatible bags have equal-length bucket slices (both derived from the shared
        // magnitudes), so zipping visits every bucket pair with no bounds risk.
        for (target, source) in self.bucket_counts.iter().zip(&*other.bucket_counts) {
            target.fetch_add(
                source.load(SYNC_BAG_ACCESS_ORDERING),
                SYNC_BAG_ACCESS_ORDERING,
            );
        }
    }

    /// Replaces the data in the bag with the data from the local observation bag.
    ///
    /// Only buckets that have been modified in `data` since the previous `copy_from` are
    /// stored; the rest are left as they were. Reads (and clears) `data`'s dirty bitmap as
    /// part of the copy.
    ///
    /// # Panics
    ///
    /// Panics if the bags have incompatible bucket magnitudes.
    pub(crate) fn copy_from(&self, data: &ObservationBag) {
        // Validate compatibility before mutating any field so the copy is transactional and
        // cannot panic partway through with a caller's lock held. Comparing magnitudes (not
        // just lengths) rejects same-length histograms whose boundaries differ.
        assert_eq!(self.bucket_magnitudes, data.bucket_magnitudes);

        self.count.store(data.count.get(), SYNC_BAG_ACCESS_ORDERING);
        self.sum.store(data.sum.get(), SYNC_BAG_ACCESS_ORDERING);

        let dirty = data.take_dirty_buckets();
        let mut dirty = self.drain_overflow_buckets(data, dirty);

        // Iterate the remaining set bits one at a time.
        while dirty != 0 {
            let i = dirty.trailing_zeros() as usize;

            let remaining_before = dirty;
            dirty = clear_lowest_set_bit(dirty);
            // Each iteration clears exactly the lowest set bit. Asserting a strictly
            // decreasing population count guarantees forward progress and turns any
            // regression that fails to clear a bit into a test failure rather than a hang,
            // which is why `clear_lowest_set_bit` needs no mutation-testing exclusion.
            debug_assert!(
                dirty.count_ones() < remaining_before.count_ones(),
                "each iteration must clear one dirty bit to guarantee termination"
            );

            // SAFETY: Bit `i` (with `i < DIRTY_BUCKETS_OVERFLOW_INDEX`) is only set by
            // `ObservationBag::insert` when a bucket at exactly index `i` was modified, which
            // requires `i < data.bucket_counts.len()`.
            let source = unsafe { data.bucket_counts.get_unchecked(i) };
            // SAFETY: The magnitudes are equal (asserted above) and each bag's bucket-count
            // length equals its magnitudes length, so `i` (in bounds for `data.bucket_counts`
            // as argued above) is also in bounds for `self.bucket_counts`.
            let target = unsafe { self.bucket_counts.get_unchecked(i) };
            target.store(source.get(), SYNC_BAG_ACCESS_ORDERING);
        }
    }

    /// Takes a point-in-time snapshot of this bag.
    ///
    /// Fields are loaded independently and can therefore describe different instants when
    /// observations are recorded concurrently.
    pub(crate) fn snapshot(&self) -> ObservationBagSnapshot {
        ObservationBagSnapshot {
            count: self.count.load(SYNC_BAG_ACCESS_ORDERING),
            sum: self.sum.load(SYNC_BAG_ACCESS_ORDERING),
            bucket_counts: self
                .bucket_counts
                .iter()
                .map(|count| count.load(SYNC_BAG_ACCESS_ORDERING))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            bucket_magnitudes: self.bucket_magnitudes,
        }
    }

    /// Copies buckets at indices `>= DIRTY_BUCKETS_OVERFLOW_INDEX` from `data` into `self`
    /// when the overflow bit is set in `dirty`. Returns `dirty` with the overflow bit cleared
    /// so the caller can iterate the remaining per-bucket bits.
    ///
    /// Mutation testing on this helper is suppressed because the mutations the tool generates
    /// here (`&` -> `|`, `&` -> `^`, `&=` -> `|=` on the overflow mask handling) all degrade
    /// to over-iteration of bucket stores. The redundant stores copy `source` buckets that
    /// already match the destination, leaving observable behavior unchanged in any state
    /// reachable through the public API. Catching them would require reaching into private
    /// fields to construct a state where `source.bucket_counts` and `self.bucket_counts`
    /// disagree in buckets that were not marked dirty, a condition no normal caller can
    /// produce. The function body is small and trivially reviewable.
    #[cfg_attr(test, mutants::skip)]
    fn drain_overflow_buckets(&self, data: &ObservationBag, dirty: u64) -> u64 {
        // The caller asserts magnitude compatibility, which implies equal bucket-count
        // lengths, so the two slices share the index range scanned below.
        debug_assert_eq!(self.bucket_counts.len(), data.bucket_counts.len());

        let overflow_mask = 1_u64 << DIRTY_BUCKETS_OVERFLOW_INDEX;
        if dirty & overflow_mask == 0 {
            return dirty;
        }

        for i in DIRTY_BUCKETS_OVERFLOW_INDEX..data.bucket_counts.len() {
            // SAFETY: The loop bound is `data.bucket_counts.len()`, so `i` is in bounds for
            // `data.bucket_counts`.
            let source = unsafe { data.bucket_counts.get_unchecked(i) };
            // SAFETY: The caller asserts magnitude compatibility, which implies
            // `self.bucket_counts.len() == data.bucket_counts.len()`, so `i` is also in
            // bounds for `self.bucket_counts`.
            let target = unsafe { self.bucket_counts.get_unchecked(i) };
            target.store(source.get(), SYNC_BAG_ACCESS_ORDERING);
        }

        dirty & !overflow_mask
    }
}

/// Clears the lowest set bit of `value` (Brian Kernighan's bit-clear trick).
///
/// `copy_from` relies on this to iterate the dirty-bucket bitmap one bit per step. That loop
/// asserts a strictly decreasing population count after each call, so a regression that fails
/// to clear a bit surfaces as a test failure rather than an infinite loop.
const fn clear_lowest_set_bit(value: u64) -> u64 {
    value & value.wrapping_sub(1)
}

impl Observations for ObservationBag {
    #[inline]
    fn insert(&self, magnitude: Magnitude, count: usize) {
        // No-op observations would not change any field anyway, but exiting early also
        // ensures the dirty-bucket bitmap is not polluted with bits whose buckets did
        // not actually change. That would force the next `copy_from` to perform stores
        // for buckets that hold the same value as before.
        if count == 0 {
            cold_path();
            return;
        }

        let (count_u64, sum_increment) = observation_increments(magnitude, count);

        self.count.set(self.count.get().wrapping_add(count_u64));
        self.sum.set(self.sum.get().wrapping_add(sum_increment));

        // Counter events (no buckets) are a common API shape, not a rare case,
        // so we exit here without a `cold_path` hint. Only the overflow branch
        // below is marked cold.
        if self.bucket_magnitudes.is_empty() {
            return;
        }

        let Some(bucket_index) = select_bucket(self.bucket_magnitudes, magnitude) else {
            // Magnitudes rarely exceed the largest bucket in a well-configured histogram.
            cold_path();
            return;
        };

        // Indexed without bounds checks because collecting observations is a very hot path.
        //
        // SAFETY: `select_bucket` produces `bucket_index` by scanning
        // `self.bucket_magnitudes`, so `bucket_index < self.bucket_magnitudes.len()`.
        // `bucket_counts` is constructed with exactly `bucket_magnitudes.len()` elements, so
        // `self.bucket_counts.len() == self.bucket_magnitudes.len()` and therefore
        // `bucket_index < self.bucket_counts.len()`. The access is in bounds.
        let bucket_count = unsafe { self.bucket_counts.get_unchecked(bucket_index) };
        bucket_count.set(bucket_count.get().wrapping_add(count_u64));

        // Mark this bucket as dirty so that the next `copy_from` knows to copy it.
        // Bucket indices at or above the overflow threshold share the highest bit.
        let dirty_bit = bucket_index.min(DIRTY_BUCKETS_OVERFLOW_INDEX);
        self.dirty_buckets
            .set(self.dirty_buckets.get() | (1_u64 << dirty_bit));
    }

    #[cfg_attr(test, mutants::skip)] // Would violate counts.len() == magnitudes.len() invariant.
    fn bucket_magnitudes(&self) -> &'static [Magnitude] {
        self.bucket_magnitudes
    }
}

impl Observations for ObservationBagSync {
    #[inline]
    fn insert(&self, magnitude: Magnitude, count: usize) {
        // No-op observations do not change any field, so exit before touching the atomics.
        // This mirrors the single-threaded bag and keeps the batch-of-zero case cheap.
        if count == 0 {
            cold_path();
            return;
        }

        let (count_u64, sum_increment) = observation_increments(magnitude, count);

        self.count.fetch_add(count_u64, SYNC_BAG_ACCESS_ORDERING);
        self.sum.fetch_add(sum_increment, SYNC_BAG_ACCESS_ORDERING);

        // Counter events (no buckets) are a common API shape, not a rare case,
        // so we exit here without a `cold_path` hint. Only the overflow branch
        // below is marked cold.
        if self.bucket_magnitudes.is_empty() {
            return;
        }

        let Some(bucket_index) = select_bucket(self.bucket_magnitudes, magnitude) else {
            // Magnitudes rarely exceed the largest bucket in a well-configured histogram.
            cold_path();
            return;
        };

        // Indexed without bounds checks because collecting observations is a very hot path.
        //
        // SAFETY: `select_bucket` produces `bucket_index` by scanning
        // `self.bucket_magnitudes`, so `bucket_index < self.bucket_magnitudes.len()`.
        // `bucket_counts` is constructed with exactly `bucket_magnitudes.len()` elements, so
        // `self.bucket_counts.len() == self.bucket_magnitudes.len()` and therefore
        // `bucket_index < self.bucket_counts.len()`. The access is in bounds.
        unsafe { self.bucket_counts.get_unchecked(bucket_index) }
            .fetch_add(count_u64, SYNC_BAG_ACCESS_ORDERING);
    }

    #[cfg_attr(test, mutants::skip)] // Would violate counts.len() == magnitudes.len() invariant.
    fn bucket_magnitudes(&self) -> &'static [Magnitude] {
        self.bucket_magnitudes
    }
}

/// A point in time snapshot of a single event's observations.
///
/// May represent the observations of a single thread or a merged set of observations
/// from multiple threads, depending on how it is obtained.
#[derive(Debug)]
pub(crate) struct ObservationBagSnapshot {
    pub(crate) count: u64,
    pub(crate) sum: Magnitude,

    /// Ascending order, not including the final `Magnitude::MAX` bucket.
    pub(crate) bucket_magnitudes: &'static [Magnitude],

    /// Not including the final `Magnitude::MAX` bucket.
    pub(crate) bucket_counts: Box<[u64]>,
}

impl ObservationBagSnapshot {
    /// Merges a synchronized observation bag directly into this snapshot.
    ///
    /// This avoids allocating a temporary snapshot for every additional thread that registered
    /// the same event. Arithmetic wraps rather than panics on overflow, per the `nm` numeric
    /// policy (see the `nm` design documentation, "Numeric and panic policies").
    ///
    /// # Panics
    ///
    /// Panics if the bag has incompatible bucket magnitudes.
    pub(crate) fn merge_from_observations(&mut self, other: &ObservationBagSync) {
        assert_eq!(self.bucket_magnitudes, other.bucket_magnitudes);

        self.count = self
            .count
            .wrapping_add(other.count.load(SYNC_BAG_ACCESS_ORDERING));
        self.sum = self
            .sum
            .wrapping_add(other.sum.load(SYNC_BAG_ACCESS_ORDERING));

        for (target, source) in self.bucket_counts.iter_mut().zip(&*other.bucket_counts) {
            *target = target.wrapping_add(source.load(SYNC_BAG_ACCESS_ORDERING));
        }
    }

    /// Merges another snapshot into this one, combining their data sets.
    ///
    /// Typically used to combine the data from multiple threads for reporting. Arithmetic
    /// wraps rather than panics on overflow, per the `nm` numeric policy (see the `nm` design
    /// documentation, "Numeric and panic policies").
    ///
    /// # Panics
    ///
    /// Panics if the snapshots have incompatible bucket magnitudes.
    #[cfg(test)]
    #[cfg_attr(coverage_nightly, coverage(off))]
    pub(crate) fn merge_from(&mut self, other: &Self) {
        // Validate compatibility before mutating any field so the merge is transactional.
        // Comparing magnitudes (not just lengths) rejects same-length histograms whose
        // boundaries differ.
        assert_eq!(self.bucket_magnitudes, other.bucket_magnitudes);

        self.count = self.count.wrapping_add(other.count);
        self.sum = self.sum.wrapping_add(other.sum);

        // Compatible snapshots have equal-length bucket slices (both derived from the shared
        // magnitudes), so zipping visits every bucket pair with no bounds risk.
        for (target, &source) in self.bucket_counts.iter_mut().zip(&*other.bucket_counts) {
            *target = target.wrapping_add(source);
        }
    }
}
