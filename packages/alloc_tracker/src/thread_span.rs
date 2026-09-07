//! Thread-local allocation tracking span.

use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

use crate::counters::{ThreadCounters, get_or_init_thread_counters};
use crate::{ERR_POISONED_LOCK, Operation, OperationMetrics, SpanMeasurement};

/// A measurement of this thread's allocations over the span's lifetime.
///
/// Returned by [`Operation::measure_thread`](crate::Operation::measure_thread). It
/// captures the thread's allocation counters at creation and records the delta when
/// it is dropped, so the measured work should live inside the span's scope.
///
/// Before the span is dropped the caller must state how many iterations the
/// measured work covers by calling [`iterations`](Self::iterations). Dropping a
/// span without an iteration count **panics**, because a measurement with no
/// iteration count is a programming error. If the thread is already unwinding
/// from a panic when the span drops, it records nothing and does not panic again,
/// leaving the original panic to propagate.
///
/// Spans may nest, and nesting is inclusive: an enclosing span also measures the activity
/// its inner spans record. Overlapping thread spans created on one thread must be dropped in
/// reverse order of creation. Holding each span in a scoped binding naturally produces that
/// order.
///
/// # Examples
///
/// The canonical benchmark pattern feeds Criterion's chosen iteration count
/// straight into [`iterations`](Self::iterations) from within `iter_custom`:
///
/// ```no_run
/// use std::hint::black_box;
/// use std::time::Instant;
///
/// use alloc_tracker::{Allocator, Session};
/// use criterion::Criterion;
///
/// #[global_allocator]
/// static ALLOCATOR: Allocator<std::alloc::System> = Allocator::system();
///
/// fn bench(c: &mut Criterion) {
///     let session = Session::new();
///     let operation = session.operation("allocate_buffer");
///     c.bench_function("allocate_buffer", |b| {
///         b.iter_custom(|iters| {
///             let start = Instant::now();
///             let _span = operation.measure_thread().iterations(iters);
///
///             for _ in 0..iters {
///                 black_box(vec![1_u8; 64]);
///             }
///
///             start.elapsed()
///         });
///     });
/// }
/// ```
///
/// When the count is only known after the work has run, set it afterwards and let
/// the span record as it drops:
///
/// ```
/// use alloc_tracker::{Allocator, Session};
///
/// #[global_allocator]
/// static ALLOCATOR: Allocator<std::alloc::System> = Allocator::system();
///
/// # fn main() {
/// let session = Session::new();
/// # let session = session.no_stdout().no_file();
/// let operation = session.operation("drain_queue");
///
/// let span = operation.measure_thread();
/// let mut processed = 0_u64;
/// for item in 0..5 {
///     let _data = vec![item; 8]; // allocate while draining
///     processed += 1;
/// }
/// drop(span.iterations(processed));
/// # }
/// ```
#[derive(Debug)]
#[must_use = "a span must be held across the measured work and given a count with `.iterations(n)`; it records when dropped and panics if the count is missing"]
pub struct ThreadSpan {
    metrics: Arc<Mutex<OperationMetrics>>,
    start_bytes: u64,
    start_count: u64,
    start_outstanding: i64,
    enclosing_watermark: i64,
    iterations: Option<u64>,

    _single_threaded: PhantomData<*const ()>,
}

impl ThreadSpan {
    pub(crate) fn new(operation: &Operation) -> Self {
        let counters = get_or_init_thread_counters();
        let start_outstanding = counters.outstanding();

        // Rebase the watermark onto this span's entry level so that it goes on to measure
        // only what this span itself holds. The displaced value is handed back on drop,
        // which is what lets spans nest.
        let enclosing_watermark = counters.watermark();
        counters.set_watermark(start_outstanding);

        Self {
            metrics: operation.metrics(),
            start_bytes: counters.bytes(),
            start_count: counters.count(),
            start_outstanding,
            enclosing_watermark,
            iterations: None,
            _single_threaded: PhantomData,
        }
    }

    /// Sets how many iterations the measured work covers.
    ///
    /// This must be called before the span is dropped. Pass the number of times the
    /// measured region repeats the work, or `1` for a single unit of work.
    ///
    /// Passing `0` — for example when a benchmark could not execute its workload —
    /// is permitted; the operation then reports a `NaN` per-iteration figure to
    /// signal that no valid measurement was produced.
    pub fn iterations(mut self, iterations: u64) -> Self {
        self.iterations = Some(iterations);
        self
    }
}

impl Drop for ThreadSpan {
    fn drop(&mut self) {
        let counters = get_or_init_thread_counters();

        // The watermark must go back to the enclosing span before any early exit below. A
        // span that is abandoned without recording would otherwise leave its own rebased
        // watermark in place and silently suppress the enclosing span's peak.
        let peak_bytes =
            restore_watermark(counters, self.start_outstanding, self.enclosing_watermark);

        // A panic while the span is held records nothing; panicking again here would
        // abort the process.
        if std::thread::panicking() {
            return;
        }

        let iterations = self.iterations.expect(
            "the span was dropped without an iteration count; call `.iterations(1)` \
             if the measured region is a single iteration",
        );
        let (bytes_delta, count_delta) =
            thread_deltas(counters, self.start_bytes, self.start_count);
        let mut data = self.metrics.lock().expect(ERR_POISONED_LOCK);
        data.add_span(SpanMeasurement {
            iterations,
            bytes: bytes_delta,
            count: count_delta,
            peak_outstanding_bytes: Some(peak_bytes),
        });
    }
}

/// Hands the allocation watermark back to the enclosing span and reports how far above its
/// own entry level the closing span reached.
///
/// The enclosing span's watermark is the higher of the value it had on entry and the level
/// the inner span reached, because memory the inner span held was equally outstanding from
/// the enclosing span's point of view.
fn restore_watermark(
    counters: ThreadCounters,
    start_outstanding: i64,
    enclosing_watermark: i64,
) -> u64 {
    let span_watermark = counters.watermark();
    counters.set_watermark(enclosing_watermark.max(span_watermark));

    // The watermark was rebased to the entry level and only ever rises, so the difference is
    // already non-negative; the clamp states that invariant rather than correcting for it.
    span_watermark
        .saturating_sub(start_outstanding)
        .max(0)
        .cast_unsigned()
}

/// Computes the thread's allocation deltas since a span's start counters.
///
/// The whole-span deltas are returned undivided; per-iteration figures are derived
/// later by the shared span accumulator, which weights each span by its iteration
/// count.
fn thread_deltas(counters: ThreadCounters, start_bytes: u64, start_count: u64) -> (u64, u64) {
    let bytes_delta = counters
        .bytes()
        .checked_sub(start_bytes)
        .expect("thread bytes allocated could not possibly decrease");
    let count_delta = counters
        .count()
        .checked_sub(start_count)
        .expect("thread allocations count could not possibly decrease");

    (bytes_delta, count_delta)
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::panic::{self, AssertUnwindSafe, RefUnwindSafe, UnwindSafe};
    use std::sync::Barrier;
    use std::thread;

    use testing::{assert_panics, with_watchdog};

    use super::*;
    use crate::Session;
    use crate::counters::{register_fake_allocation, register_fake_deallocation};

    /// Representative allocation size for scenarios where the exact number carries no
    /// meaning beyond being distinguishable from the others in the same test.
    const BLOCK: u64 = 100;

    /// A byte count as the report exposes it.
    #[expect(
        clippy::cast_precision_loss,
        reason = "the byte counts these tests use are small integers that f64 represents exactly"
    )]
    fn as_reported(bytes: u64) -> f64 {
        bytes as f64
    }

    /// The peak an operation reports when equally weighted spans reached each of `levels`.
    fn mean_peak(levels: &[u64]) -> f64 {
        let count = u64::try_from(levels.len()).unwrap();

        levels.iter().copied().map(as_reported).sum::<f64>() / as_reported(count)
    }

    // Static assertions for thread safety.
    // The span should NOT be Send or Sync due to PhantomData<*const ()>.
    static_assertions::assert_not_impl_all!(ThreadSpan: Send);
    static_assertions::assert_not_impl_all!(ThreadSpan: Sync);

    // Static assertions for unwind safety.
    static_assertions::assert_impl_all!(ThreadSpan: UnwindSafe, RefUnwindSafe);

    #[test]
    fn peak_is_the_high_water_mark_not_the_total() {
        const ROUNDS: u64 = 3;

        let session = Session::new().no_stdout().no_file();
        let operation = session.operation("test");

        {
            let _span = operation.measure_thread().iterations(1);

            // Allocations that never coexist: each round frees before the next allocates.
            for _ in 0..ROUNDS {
                register_fake_allocation(BLOCK, 1);
                register_fake_deallocation(BLOCK);
            }
        }

        assert_eq!(operation.total_bytes_allocated(), BLOCK * ROUNDS);
        assert_eq!(operation.peak_outstanding_bytes(), Some(as_reported(BLOCK)));
    }

    #[test]
    fn peak_ignores_memory_outstanding_before_the_span() {
        const PRE_EXISTING: u64 = 1000;
        const SPAN_HELD: u64 = 50;

        let session = Session::new().no_stdout().no_file();
        let operation = session.operation("test");

        // Memory the caller already holds is not the span's doing.
        register_fake_allocation(PRE_EXISTING, 1);

        {
            let _span = operation.measure_thread().iterations(1);
            register_fake_allocation(SPAN_HELD, 1);
        }

        register_fake_deallocation(PRE_EXISTING + SPAN_HELD);

        assert_eq!(
            operation.peak_outstanding_bytes(),
            Some(as_reported(SPAN_HELD))
        );
    }

    #[test]
    fn peak_underreports_when_the_span_frees_first() {
        // A documented limitation of measuring against the entry level: the span holds
        // memory of its own but never pushes the outstanding total above where it started,
        // so it reports nothing.
        const PRE_EXISTING: u64 = 1000;
        const FREED_FIRST: u64 = 800;
        // Below what the span freed, so the outstanding total never regains the entry level.
        const SPAN_HELD: u64 = 500;

        let session = Session::new().no_stdout().no_file();
        let operation = session.operation("test");

        register_fake_allocation(PRE_EXISTING, 1);

        {
            let _span = operation.measure_thread().iterations(1);
            register_fake_deallocation(FREED_FIRST);
            register_fake_allocation(SPAN_HELD, 1);
        }

        register_fake_deallocation(PRE_EXISTING - FREED_FIRST + SPAN_HELD);

        assert_eq!(operation.peak_outstanding_bytes(), Some(0.0));
    }

    #[test]
    fn nested_span_peak_is_visible_to_the_enclosing_span() {
        // The inner span dominates, so the enclosing span's own level only adds to it.
        const OUTER_HELD: u64 = 10;
        const INNER_HELD: u64 = 200;

        let session = Session::new().no_stdout().no_file();
        let outer = session.operation("outer");
        let inner = session.operation("inner");

        {
            let _outer_span = outer.measure_thread().iterations(1);
            register_fake_allocation(OUTER_HELD, 1);

            {
                let _inner_span = inner.measure_thread().iterations(1);
                register_fake_allocation(INNER_HELD, 1);
                register_fake_deallocation(INNER_HELD);
            }

            register_fake_deallocation(OUTER_HELD);
        }

        // The inner span sees only what it held itself, while the outer span sees everything
        // that was outstanding at once while the inner span ran.
        assert_eq!(
            inner.peak_outstanding_bytes(),
            Some(as_reported(INNER_HELD))
        );
        assert_eq!(
            outer.peak_outstanding_bytes(),
            Some(as_reported(OUTER_HELD + INNER_HELD))
        );
    }

    #[test]
    fn enclosing_peak_survives_a_smaller_nested_span() {
        // The mirror image of the case above: the enclosing span already reached its high
        // point before the inner span opened, so restoration must keep the enclosing value.
        const OUTER_HELD: u64 = 900;
        const INNER_HELD: u64 = 5;

        let session = Session::new().no_stdout().no_file();
        let outer = session.operation("outer");
        let inner = session.operation("inner");

        {
            let _outer_span = outer.measure_thread().iterations(1);
            register_fake_allocation(OUTER_HELD, 1);
            register_fake_deallocation(OUTER_HELD);

            {
                let _inner_span = inner.measure_thread().iterations(1);
                register_fake_allocation(INNER_HELD, 1);
                register_fake_deallocation(INNER_HELD);
            }
        }

        assert_eq!(
            inner.peak_outstanding_bytes(),
            Some(as_reported(INNER_HELD))
        );
        assert_eq!(
            outer.peak_outstanding_bytes(),
            Some(as_reported(OUTER_HELD))
        );
    }

    #[test]
    fn abandoned_nested_span_does_not_suppress_the_enclosing_peak() {
        // The inner span is dropped during a panic and records nothing, but it must still
        // hand the watermark back or the outer span would lose its measurement.
        const INNER_HELD: u64 = 400;

        let session = Session::new().no_stdout().no_file();
        let outer = session.operation("outer");
        let inner = session.operation("inner");

        {
            let _outer_span = outer.measure_thread().iterations(1);

            assert_panics(|| {
                let _inner_span = inner.measure_thread().iterations(1);
                register_fake_allocation(INNER_HELD, 1);
                panic!("boom");
            });

            register_fake_deallocation(INNER_HELD);
        }

        assert_eq!(inner.peak_outstanding_bytes(), None);
        assert_eq!(
            outer.peak_outstanding_bytes(),
            Some(as_reported(INNER_HELD))
        );
    }

    #[test]
    fn nested_span_without_iterations_still_restores_the_enclosing_peak() {
        // The missing iteration count is a programmer error that panics from `drop`, which
        // is the third way a span can close. The watermark hand-off happens before that
        // check, so the enclosing span keeps its measurement either way.
        const INNER_HELD: u64 = 400;

        let session = Session::new().no_stdout().no_file();
        let outer = session.operation("outer");
        let inner = session.operation("inner");

        {
            let _outer_span = outer.measure_thread().iterations(1);

            assert_panics(|| {
                let _inner_span = inner.measure_thread();
                register_fake_allocation(INNER_HELD, 1);
            });

            register_fake_deallocation(INNER_HELD);
        }

        assert_eq!(inner.peak_outstanding_bytes(), None);
        assert_eq!(
            outer.peak_outstanding_bytes(),
            Some(as_reported(INNER_HELD))
        );
    }

    #[test]
    fn sequential_spans_each_contribute_their_peak() {
        // Equal iteration counts, so the peaks carry equal weight and the result is their
        // arithmetic mean.
        const LEVELS: [u64; 3] = [100, 700, 400];

        let session = Session::new().no_stdout().no_file();
        let operation = session.operation("test");

        for level in LEVELS {
            let _span = operation.measure_thread().iterations(1);
            register_fake_allocation(level, 1);
            register_fake_deallocation(level);
        }

        assert_eq!(operation.peak_outstanding_bytes(), Some(mean_peak(&LEVELS)));
    }

    #[test]
    fn concurrent_thread_spans_average_their_peaks() {
        // Each worker has its own counters and watermark, but both guards record into one
        // shared `OperationMetrics`. Overlapping the spans exercises that interaction: the
        // result must be the average of the levels, not their sum, and no worker may
        // observe another's outstanding memory.
        const LEVELS: [u64; 2] = [300, 900];

        with_watchdog(|| {
            let session = Session::new().no_stdout().no_file();
            let operation = session.operation("test");

            // Both workers must be inside their spans at the same time for the test to say
            // anything about concurrent contributions.
            let both_inside = Barrier::new(LEVELS.len());

            thread::scope(|scope| {
                for level in LEVELS {
                    let operation = &operation;
                    let both_inside = &both_inside;

                    scope.spawn(move || {
                        // Reaching the rendezvous is separated from the measured work so a
                        // panic in that work cannot strand the other worker. Watchdogs are
                        // disabled under mutation testing, so nothing would break the
                        // deadlock. Ref: docs/testing.md, "Tests must not hang".
                        let opened = panic::catch_unwind(AssertUnwindSafe(|| {
                            let span = operation.measure_thread().iterations(1);
                            register_fake_allocation(level, 1);
                            span
                        }));

                        both_inside.wait();

                        let span = opened.unwrap_or_else(|payload| panic::resume_unwind(payload));
                        register_fake_deallocation(level);
                        drop(span);
                    });
                }
            });

            assert_eq!(operation.peak_outstanding_bytes(), Some(mean_peak(&LEVELS)));
        });
    }

    #[test]
    fn iterations_zero_is_accepted() {
        // A workload that could not run reports zero iterations; this records
        // rather than panicking, so the harness survives a failed benchmark.
        let session = Session::new().no_stdout().no_file();
        let operation = session.operation("test");

        drop(operation.measure_thread().iterations(0));

        assert_eq!(operation.total_iterations(), 0);
    }

    #[test]
    fn records_span_via_post_hoc_iterations() {
        const ITERATIONS: u64 = 5;

        let session = Session::new().no_stdout().no_file();
        let operation = session.operation("test");

        drop(operation.measure_thread().iterations(ITERATIONS));

        assert_eq!(operation.total_iterations(), ITERATIONS);
    }

    #[test]
    fn records_span_via_iterations_guard() {
        const ITERATIONS: u64 = 3;

        let session = Session::new().no_stdout().no_file();
        let operation = session.operation("test");

        {
            let _span = operation.measure_thread().iterations(ITERATIONS);
        }

        assert_eq!(operation.total_iterations(), ITERATIONS);
    }

    #[test]
    #[should_panic]
    fn dropping_span_without_iterations_panics() {
        let session = Session::new().no_stdout().no_file();
        let operation = session.operation("test");

        drop(operation.measure_thread());
    }

    #[test]
    fn panic_while_held_records_nothing() {
        let session = Session::new().no_stdout().no_file();
        let operation = session.operation("test");

        assert_panics(|| {
            let _span = operation.measure_thread().iterations(1);
            panic!("boom");
        });

        assert_eq!(operation.total_iterations(), 0);
    }
}
