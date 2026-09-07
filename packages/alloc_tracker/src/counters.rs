//! Per-thread allocation counters and the process-wide registry over them.
//!
//! A tracked allocator event updates only the counters belonging to the thread that
//! caused it, so the hot path never contends. A registry retains every thread's
//! counters for the process lifetime, which lets a process-scoped span sum them.
//!
//! Two windows are deliberately left untracked, both to keep the allocator from
//! re-entering itself: allocations made while a thread's own counters are being
//! created, and deallocations on a thread that has no counters yet.

use std::cell::{Cell, OnceCell};
use std::marker::PhantomData;
use std::sync::atomic::{self, AtomicI64, AtomicU64};
use std::sync::{LazyLock, Mutex};

use crate::ERR_POISONED_LOCK;

/// One thread's allocation counters, retained for the process lifetime.
///
/// Only the owning thread writes these, through [`ThreadCounters`]; every other thread
/// sees them as read-only through the registry. That single-writer discipline is what
/// lets the writes be plain relaxed stores rather than read-modify-write instructions.
/// Readers on other threads may therefore observe a slightly stale value, which the
/// process-wide summation already tolerates.
///
/// All updates wrap on overflow. A counter that wrapped is meaningless either way, and
/// the alternatives are worse in an allocator hook: checked arithmetic would panic
/// inside a `GlobalAlloc` method, and saturating arithmetic would pin the counter
/// permanently and distort every later span delta. Wrapping keeps subsequent deltas —
/// which is all a span reads — correct as long as the span itself does not span a wrap.
#[derive(Debug)]
struct PerThreadCounters {
    bytes: AtomicU64,
    count: AtomicU64,
    outstanding: AtomicI64,
    watermark: AtomicI64,
}

impl PerThreadCounters {
    #[inline]
    const fn new() -> Self {
        Self {
            bytes: AtomicU64::new(0),
            count: AtomicU64::new(0),
            outstanding: AtomicI64::new(0),
            watermark: AtomicI64::new(0),
        }
    }

    #[inline]
    fn bytes(&self) -> u64 {
        self.bytes.load(atomic::Ordering::Relaxed)
    }

    #[inline]
    fn count(&self) -> u64 {
        self.count.load(atomic::Ordering::Relaxed)
    }

    /// Bytes allocated on this thread and not yet freed on this thread.
    ///
    /// May be negative. A thread can free blocks it never allocated: blocks handed to it by
    /// another thread, and blocks allocated before its counters existed or during the
    /// initialization window that deliberately skips tracking. The counter therefore records
    /// this thread's allocator traffic, not the memory it owns.
    fn outstanding(&self) -> i64 {
        self.outstanding.load(atomic::Ordering::Relaxed)
    }

    /// The high-water mark of [`outstanding`](Self::outstanding).
    ///
    /// Spans own this value: each resets it on entry and restores it on exit, so it means
    /// "the highest level reached since the innermost live span started" rather than an
    /// all-time maximum.
    fn watermark(&self) -> i64 {
        self.watermark.load(atomic::Ordering::Relaxed)
    }
}

/// The calling thread's counters, and the only way to write any counters.
///
/// [`PerThreadCounters`] is written with relaxed load/store pairs instead of atomic
/// read-modify-write instructions, which is sound only while one thread writes a given
/// counter block. This handle carries that restriction in the type system rather than in
/// prose: it can only be obtained from the calling thread's own thread-local slot, and it
/// is neither `Send` nor `Sync`, so no second writer can come into existence.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ThreadCounters {
    counters: &'static PerThreadCounters,

    _single_threaded: PhantomData<*const ()>,
}

impl ThreadCounters {
    fn new(counters: &'static PerThreadCounters) -> Self {
        Self {
            counters,
            _single_threaded: PhantomData,
        }
    }

    #[inline]
    fn register_allocation(self, bytes: u64) {
        self.add_to_totals(bytes);
        self.raise_outstanding(as_delta(bytes));
    }

    fn register_deallocation(self, bytes: u64) {
        self.shift_outstanding(as_delta(bytes).wrapping_neg());
    }

    /// Records a reallocation of a block that was `old_bytes` long and is now `new_bytes` long.
    ///
    /// The cumulative total counts the full new size, matching how the allocator reports the
    /// request. Only the difference is outstanding, because the old block is released as part
    /// of the same operation.
    fn register_reallocation(self, old_bytes: u64, new_bytes: u64) {
        self.add_to_totals(new_bytes);
        self.raise_outstanding(as_delta(new_bytes).wrapping_sub(as_delta(old_bytes)));
    }

    /// Adds one request of `bytes` to the cumulative totals.
    fn add_to_totals(self, bytes: u64) {
        let total = self.counters.bytes().wrapping_add(bytes);
        self.counters.bytes.store(total, atomic::Ordering::Relaxed);

        let count = self.counters.count().wrapping_add(1);
        self.counters.count.store(count, atomic::Ordering::Relaxed);
    }

    /// Applies `delta` to the outstanding total and lifts the high-water mark if the result
    /// exceeds it.
    fn raise_outstanding(self, delta: i64) {
        let outstanding = self.shift_outstanding(delta);

        // Written unconditionally so the path stays branchless. Whether a watermark update
        // is needed depends on the workload's allocation pattern, which this code cannot
        // predict; the measured cost of the unconditional form is what the paired
        // `alloc_tracker_tracking_overhead` benchmarks report.
        let watermark = self.counters.watermark();
        self.counters
            .watermark
            .store(outstanding.max(watermark), atomic::Ordering::Relaxed);
    }

    /// Applies `delta` to the outstanding total, returning the new value.
    fn shift_outstanding(self, delta: i64) -> i64 {
        let outstanding = self.counters.outstanding().wrapping_add(delta);
        self.counters
            .outstanding
            .store(outstanding, atomic::Ordering::Relaxed);
        outstanding
    }

    pub(crate) fn bytes(self) -> u64 {
        self.counters.bytes()
    }

    pub(crate) fn count(self) -> u64 {
        self.counters.count()
    }

    pub(crate) fn outstanding(self) -> i64 {
        self.counters.outstanding()
    }

    /// The high-water mark of [`outstanding`](Self::outstanding), owned by the innermost
    /// live span.
    pub(crate) fn watermark(self) -> i64 {
        self.counters.watermark()
    }

    /// Overwrites the high-water mark, for a span establishing or restoring its baseline.
    pub(crate) fn set_watermark(self, value: i64) {
        self.counters
            .watermark
            .store(value, atomic::Ordering::Relaxed);
    }
}

/// Converts an allocation size to the signed representation the outstanding counter uses.
fn as_delta(bytes: u64) -> i64 {
    i64::try_from(bytes)
        .expect("a single allocation cannot exceed isize::MAX bytes, which `Layout` guarantees")
}

/// Every thread's counters, including those of threads that have since exited.
///
/// Nothing is ever removed, which is what lets a `&'static` reference to any registered
/// block remain valid for the process lifetime.
static REGISTRY: LazyLock<Mutex<Vec<&'static PerThreadCounters>>> =
    LazyLock::new(|| Mutex::new(Vec::new()));

thread_local! {
    // The counters are leaked rather than reference-counted so that this slot holds a plain
    // shared reference. An owning handle here would run its destructor during thread
    // teardown, while the global allocator is still live, and could therefore re-enter
    // allocation tracking at a point where the thread's state is already being dismantled.
    // A shared reference has no destructor, so TLS teardown does nothing at all.
    static TLS_COUNTERS: OnceCell<&'static PerThreadCounters> = const { OnceCell::new() };

    // Creating the counters allocates and locks the registry, which would recurse into
    // tracking. This flag suppresses tracking for that window.
    static TLS_INIT_GUARD: Cell<bool> = const { Cell::new(false) };
}

#[inline]
pub(crate) fn get_or_init_thread_counters() -> ThreadCounters {
    ThreadCounters::new(TLS_COUNTERS.with(|cell| {
        if let Some(counters) = cell.get() {
            return *counters;
        }

        TLS_INIT_GUARD.set(true);

        // Leaked deliberately: the registry retains every thread's counters for the process
        // lifetime so that process-scoped spans can sum threads that have already exited.
        // One block per thread that ever allocates or opens a thread span is the whole cost,
        // since `ThreadSpan::new` reads its entry level through this same path.
        let counters: &'static PerThreadCounters = Box::leak(Box::new(PerThreadCounters::new()));
        REGISTRY.lock().expect(ERR_POISONED_LOCK).push(counters);

        // Publishing the reference is the last step before the guard is cleared, and nothing
        // between the two statements allocates. That is what lets the tracking path treat a
        // published reference as proof that it is outside the initialization window.
        _ = cell.set(counters);

        TLS_INIT_GUARD.set(false);

        counters
    }))
}

/// This thread's counters, if they already exist.
///
/// The deallocation path uses this instead of [`get_or_init_thread_counters`] because
/// creating the counters allocates and locks, which a free must not do.
fn existing_thread_counters() -> Option<ThreadCounters> {
    TLS_COUNTERS.with(|cell| cell.get().map(|counters| ThreadCounters::new(counters)))
}

/// Whether this thread's counters have been created yet.
///
/// Exists so tests can prove the deallocation path never initializes thread-local state,
/// which is what keeps a free from re-entering the allocator.
#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))] // Test scaffolding, not shipped behavior.
pub(crate) fn thread_has_counters() -> bool {
    existing_thread_counters().is_some()
}

/// Aggregate totals across all registered threads.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct AllocationTotals {
    pub(crate) bytes: u64,
    pub(crate) count: u64,
}

impl AllocationTotals {
    #[inline]
    const fn zero() -> Self {
        Self { bytes: 0, count: 0 }
    }
}

/// Sum all registered counters (process-wide view at a point in time).
#[inline]
pub(crate) fn allocation_totals() -> AllocationTotals {
    let registry = REGISTRY.lock().expect(ERR_POISONED_LOCK);

    let mut totals = AllocationTotals::zero();
    for counters in registry.iter() {
        totals.bytes = totals.bytes.wrapping_add(counters.bytes());
        totals.count = totals.count.wrapping_add(counters.count());
    }
    totals
}

/// This thread's counters, creating them if this is the thread's first tracked event.
///
/// Returns `None` while the counters are themselves being created, because that work
/// allocates and must not recurse into tracking.
fn thread_counters_for_tracking() -> Option<ThreadCounters> {
    if let Some(counters) = existing_thread_counters() {
        // Initialization publishes the counter reference as its last step, so the reference
        // being present already proves we are not inside the initialization window. Checking
        // the reentrancy guard as well would cost a second thread-local lookup on the hot path.
        return Some(counters);
    }

    if TLS_INIT_GUARD.get() {
        return None;
    }

    Some(get_or_init_thread_counters())
}

/// Records a successful allocation of `size` bytes on the calling thread.
///
/// Called only after the wrapped allocator returned a non-null pointer, so the counters
/// never describe a request that failed.
pub(crate) fn track_allocation(size: usize) {
    let size: u64 = size.try_into().expect("usize always fits into u64");

    if let Some(counters) = thread_counters_for_tracking() {
        counters.register_allocation(size);
    }
}

/// Records a successful resize from `old_size` to `new_size` on the calling thread.
///
/// Called only after the wrapped allocator returned a non-null pointer. The two sizes affect
/// the counters differently: the cumulative total gains the whole new size, while only the
/// difference is outstanding, because the old block is released by the same call.
pub(crate) fn track_reallocation(old_size: usize, new_size: usize) {
    let old_size: u64 = old_size.try_into().expect("usize always fits into u64");
    let new_size: u64 = new_size.try_into().expect("usize always fits into u64");

    if let Some(counters) = thread_counters_for_tracking() {
        counters.register_reallocation(old_size, new_size);
    }
}

/// Updates tracking counters for a released block of the given size.
///
/// Unlike the allocation paths, this never creates the thread's counters. Doing so allocates
/// and locks the registry, which would re-enter the allocator from inside a free and could
/// consume the one-shot `panic_on_next_alloc` flag. A thread whose first tracked event is a
/// free therefore records nothing, which the signed outstanding counter tolerates.
pub(crate) fn track_deallocation(size: usize) {
    let size: u64 = size.try_into().expect("usize always fits into u64");

    if let Some(counters) = existing_thread_counters() {
        counters.register_deallocation(size);
    }
}

/// Records an allocation on this thread without going through the global allocator.
///
/// Unit tests do not install the tracking allocator, so this drives the same writer the
/// allocator hooks use, producing counter movement equivalent to `count` allocator requests
/// totalling `bytes`.
#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))] // Test scaffolding, not shipped behavior.
pub(crate) fn register_fake_allocation(bytes: u64, count: u64) {
    let counters = get_or_init_thread_counters();

    // The whole byte total rides on the first request so the outstanding total moves exactly
    // once; the rest are empty requests that only advance the allocation count.
    for index in 0..count {
        counters.register_allocation(if index == 0 { bytes } else { 0 });
    }
}

/// Records a deallocation on this thread without going through the global allocator.
///
/// Lets a span test balance the synthetic outstanding state that
/// [`register_fake_allocation`] created, so watermark scenarios can be built without
/// installing the tracking allocator.
#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))] // Test scaffolding, not shipped behavior.
pub(crate) fn register_fake_deallocation(bytes: u64) {
    get_or_init_thread_counters().register_deallocation(bytes);
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::sync::{Arc, Barrier};
    use std::{iter, panic, thread};

    use testing::with_watchdog;

    use super::*;

    static_assertions::assert_impl_all!(PerThreadCounters: Send, Sync);
    static_assertions::assert_not_impl_any!(ThreadCounters: Send, Sync);

    /// Creates counters that no other thread can reach, for exercising the writer directly.
    ///
    /// A macro rather than a function because each expansion needs its own static: a static
    /// inside a function body would be shared by every test that calls it.
    macro_rules! detached_counters {
        () => {{
            static COUNTERS: PerThreadCounters = PerThreadCounters::new();

            ThreadCounters::new(&COUNTERS)
        }};
    }

    #[test]
    fn outstanding_follows_allocations_and_deallocations() {
        let counters = detached_counters!();

        assert_eq!(counters.outstanding(), 0);

        counters.register_allocation(100);
        counters.register_allocation(50);
        assert_eq!(counters.outstanding(), 150);

        counters.register_deallocation(100);
        assert_eq!(counters.outstanding(), 50);

        // Cumulative totals are unaffected by frees.
        assert_eq!(counters.bytes(), 150);
        assert_eq!(counters.count(), 2);
    }

    #[test]
    fn outstanding_goes_negative_when_freeing_untracked_memory() {
        let counters = detached_counters!();

        // A thread can free a block that another thread allocated, or one allocated before
        // its counters existed.
        counters.register_deallocation(100);

        assert_eq!(counters.outstanding(), -100);
        assert_eq!(counters.watermark(), 0);
    }

    #[test]
    fn watermark_holds_the_high_water_mark() {
        let counters = detached_counters!();

        counters.register_allocation(100);
        counters.register_allocation(50);
        assert_eq!(counters.watermark(), 150);

        counters.register_deallocation(150);
        counters.register_allocation(20);

        assert_eq!(counters.outstanding(), 20);
        assert_eq!(counters.watermark(), 150);
    }

    #[test]
    fn reallocation_adjusts_outstanding_by_size_difference() {
        const INITIAL: u64 = 100;
        const GROWN: u64 = 300;
        const SHRUNK: u64 = 80;

        let counters = detached_counters!();

        counters.register_allocation(INITIAL);
        counters.register_reallocation(INITIAL, GROWN);

        assert_eq!(counters.outstanding(), i64::try_from(GROWN).unwrap());
        assert_eq!(counters.watermark(), i64::try_from(GROWN).unwrap());

        counters.register_reallocation(GROWN, SHRUNK);

        assert_eq!(counters.outstanding(), i64::try_from(SHRUNK).unwrap());
        assert_eq!(counters.watermark(), i64::try_from(GROWN).unwrap());

        // The cumulative total counts each request at its full requested size.
        assert_eq!(counters.bytes(), INITIAL + GROWN + SHRUNK);
        assert_eq!(counters.count(), 3);
    }

    #[test]
    fn set_watermark_overwrites_the_high_water_mark() {
        let counters = detached_counters!();

        counters.register_allocation(100);
        counters.set_watermark(40);

        assert_eq!(counters.watermark(), 40);
    }

    #[test]
    fn concurrent_threads_register_and_totals_reflect_all() {
        const THREADS: u64 = 4;
        const BYTES_PER_THREAD: u64 = 100;
        const COUNT_PER_THREAD: u64 = 10;

        // Record the baseline to account for allocations from other tests,
        // since the global REGISTRY is shared across all tests.
        let baseline = allocation_totals();

        let handles: Vec<_> = iter::repeat_with(|| {
            thread::spawn(move || {
                register_fake_allocation(BYTES_PER_THREAD, COUNT_PER_THREAD);
            })
        })
        .take(usize::try_from(THREADS).unwrap())
        .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        let final_totals = allocation_totals();

        // The delta must be at least what we added. It may be higher due to
        // real allocations from the test infrastructure (thread spawning, etc.).
        let bytes_delta = final_totals.bytes.wrapping_sub(baseline.bytes);
        let count_delta = final_totals.count.wrapping_sub(baseline.count);

        assert!(bytes_delta >= THREADS * BYTES_PER_THREAD);
        assert!(count_delta >= THREADS * COUNT_PER_THREAD);
    }

    #[test]
    fn concurrent_register_and_read_totals() {
        const WRITER_THREADS: u64 = 4;
        const ALLOCS_PER_WRITER: u64 = 10;
        const BYTES_PER_ALLOC: u64 = 50;

        /// How many snapshots the reader takes while the writers are running. Enough to
        /// overlap the writes without making the test slow.
        const READS: usize = 20;

        // Writers drive the production single-writer path while a reader sums the registry.
        // The barrier makes the phases overlap: every writer has registered its counters
        // before the reader starts, and the reader is running while the writes land.
        with_watchdog(|| {
            let baseline = allocation_totals();

            let ready = Arc::new(Barrier::new(usize::try_from(WRITER_THREADS).unwrap() + 1));

            let writers: Vec<_> = iter::repeat_with(|| {
                let ready = Arc::clone(&ready);
                thread::spawn(move || {
                    // Register this thread's counters, then wait for everyone. Arriving at
                    // the barrier is kept off the panicking path so a failure here cannot
                    // block the reader forever, which no watchdog would break under
                    // mutation testing. Ref: docs/testing.md, "Tests must not hang".
                    let registered = panic::catch_unwind(|| {
                        register_fake_allocation(BYTES_PER_ALLOC, 1);
                    });

                    ready.wait();

                    registered.unwrap_or_else(|payload| panic::resume_unwind(payload));

                    for _ in 1..ALLOCS_PER_WRITER {
                        register_fake_allocation(BYTES_PER_ALLOC, 1);
                    }
                })
            })
            .take(usize::try_from(WRITER_THREADS).unwrap())
            .collect();

            ready.wait();

            // The reader runs concurrently with the writers, so what it observes is
            // asserted rather than discarded: totals only ever grow, so every snapshot
            // must sit at or above the previous one and at or above the baseline. A
            // registry read that tore across a concurrent write, skipped a block, or
            // double-counted one would break that ordering.
            let mut previous = baseline;
            for _ in 0..READS {
                let totals = allocation_totals();
                assert!(totals.bytes >= previous.bytes && totals.count >= previous.count);
                previous = totals;
            }

            for handle in writers {
                handle.join().unwrap();
            }

            let final_totals = allocation_totals();
            let bytes_delta = final_totals.bytes.wrapping_sub(baseline.bytes);
            assert!(bytes_delta >= WRITER_THREADS * ALLOCS_PER_WRITER * BYTES_PER_ALLOC);
            assert!(final_totals.bytes >= previous.bytes);
        });
    }
}
