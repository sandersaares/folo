//! Process-wide allocation tracking span.

use std::cell::Cell;
use std::marker::PhantomData;
use std::panic::RefUnwindSafe;
use std::sync::{Arc, Mutex};

use crate::counters::{AllocationTotals, allocation_totals};
use crate::{ERR_POISONED_LOCK, Operation, OperationMetrics, SpanMeasurement};

/// A measurement of process-wide allocations over the span's lifetime.
///
/// Returned by [`Operation::measure_process`](crate::Operation::measure_process).
/// It captures the process's allocation counters at creation and records the delta
/// when it is dropped, so the measured work should live inside the span's scope.
///
/// Before the span is dropped the caller must state how many iterations the
/// measured work covers by calling [`iterations`](Self::iterations). Dropping a
/// span without an iteration count **panics**, because a measurement with no
/// iteration count is a programming error. If the thread is already unwinding
/// from a panic when the span drops, it records nothing and does not panic again,
/// leaving the original panic to propagate.
///
/// Reach for this when the threads doing the work cannot be instrumented themselves, or
/// when several of them collaborate on every iteration so that no single thread's span
/// covers a whole one. When each worker can be instrumented and counts iterations of its
/// own, give each one its own [`ThreadSpan`](crate::ThreadSpan) naming the same operation
/// instead. The trade-offs a process span accepts are listed under
/// [`Operation::measure_process`](crate::Operation::measure_process).
///
/// # Examples
///
/// Work that a library fans out over its own threads:
///
/// ```
/// use std::hint::black_box;
/// use std::thread;
///
/// use alloc_tracker::{Allocator, Session};
///
/// #[global_allocator]
/// static ALLOCATOR: Allocator<std::alloc::System> = Allocator::system();
///
/// // Stands in for a library that owns its worker threads. You cannot give them thread
/// // spans because you never see them, which is what leaves process scope as the option.
/// fn fan_out(items: usize) {
///     thread::scope(|scope| {
///         for item in 0..items {
///             scope.spawn(move || {
///                 black_box(vec![item as u8; 1024]);
///             });
///         }
///     });
/// }
///
/// # fn main() {
/// let session = Session::new();
/// # let session = session.no_stdout().no_file();
/// let operation = session.operation("fan_out");
///
/// const ITEMS: usize = 4;
/// let span = operation.measure_process();
///
/// fan_out(ITEMS);
///
/// drop(span.iterations(1));
/// # }
/// ```
#[derive(Debug)]
#[must_use = "a span must be held across the measured work and given a count with `.iterations(n)`; it records when dropped and panics if the count is missing"]
pub struct ProcessSpan {
    metrics: Arc<Mutex<OperationMetrics>>,
    start_bytes: u64,
    start_count: u64,
    iterations: Option<u64>,
    // Cell<()> is natively Send + !Sync, which opts the type out of Sync without requiring
    // an unsafe impl Send. Using PhantomData<*mut ()> + unsafe impl Send would be simpler
    // but triggers a rustc bug (rust-lang/rust#110338) in async generator Send inference.
    // We use the Cell<()> pattern here for consistency with the rest of the workspace.
    _not_sync: PhantomData<Cell<()>>,
}

// The Cell<()> marker is zero-sized with no actual mutable state, so there is nothing to
// observe in an inconsistent state during unwind.
impl RefUnwindSafe for ProcessSpan {}

impl ProcessSpan {
    pub(crate) fn new(operation: &Operation) -> Self {
        let AllocationTotals {
            bytes: start_bytes,
            count: start_count,
        } = allocation_totals();

        Self {
            metrics: operation.metrics(),
            start_bytes,
            start_count,
            iterations: None,
            _not_sync: PhantomData,
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

impl Drop for ProcessSpan {
    fn drop(&mut self) {
        // A panic while the span is held records nothing; panicking again here would
        // abort the process.
        if std::thread::panicking() {
            return;
        }

        let iterations = self.iterations.expect(
            "the span was dropped without an iteration count; call `.iterations(1)` \
             if the measured region is a single iteration",
        );
        let (bytes_delta, count_delta) = process_deltas(self.start_bytes, self.start_count);
        let mut data = self.metrics.lock().expect(ERR_POISONED_LOCK);
        data.add_span(SpanMeasurement {
            iterations,
            bytes: bytes_delta,
            count: count_delta,
            // A process-scoped span spans every thread, and the watermark is per-thread, so
            // there is no single high-water mark for it to read. Reporting nothing keeps the
            // operation honest rather than substituting the calling thread's figure.
            // Ref: docs/design.md, "Peak outstanding bytes".
            peak_outstanding_bytes: None,
        });
    }
}

/// Computes the process-wide allocation deltas since a span's start counters.
///
/// The whole-span deltas are returned undivided; per-iteration figures are derived
/// later by the shared span accumulator, which weights each span by its iteration
/// count.
fn process_deltas(start_bytes: u64, start_count: u64) -> (u64, u64) {
    let AllocationTotals {
        bytes: current_bytes,
        count: current_count,
    } = allocation_totals();

    let bytes_delta = current_bytes
        .checked_sub(start_bytes)
        .expect("total bytes allocated could not possibly decrease");
    let count_delta = current_count
        .checked_sub(start_count)
        .expect("total allocations count could not possibly decrease");

    (bytes_delta, count_delta)
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::panic::{RefUnwindSafe, UnwindSafe};

    use super::*;
    use crate::Session;

    // Static assertions for thread safety.
    // The span is Send but !Sync due to PhantomData<Cell<()>>.
    static_assertions::assert_impl_all!(ProcessSpan: Send, UnwindSafe, RefUnwindSafe);
    static_assertions::assert_not_impl_any!(ProcessSpan: Sync);

    #[test]
    fn iterations_zero_is_accepted() {
        // A workload that could not run reports zero iterations; this records
        // rather than panicking, so the harness survives a failed benchmark.
        let session = Session::new().no_stdout().no_file();
        let operation = session.operation("test");

        drop(operation.measure_process().iterations(0));

        assert_eq!(operation.total_iterations(), 0);
    }

    #[test]
    fn records_span_via_post_hoc_iterations() {
        let session = Session::new().no_stdout().no_file();
        let operation = session.operation("test");

        drop(operation.measure_process().iterations(5));

        assert_eq!(operation.total_iterations(), 5);
    }

    #[test]
    fn records_span_via_iterations_guard() {
        let session = Session::new().no_stdout().no_file();
        let operation = session.operation("test");

        {
            let _span = operation.measure_process().iterations(3);
        }

        assert_eq!(operation.total_iterations(), 3);
    }

    #[test]
    #[should_panic]
    fn dropping_span_without_iterations_panics() {
        let session = Session::new().no_stdout().no_file();
        let operation = session.operation("test");

        drop(operation.measure_process());
    }

    #[test]
    fn process_span_reports_no_peak() {
        let session = Session::new().no_stdout().no_file();
        let operation = session.operation("test");

        drop(operation.measure_process().iterations(1));

        assert_eq!(operation.peak_outstanding_bytes(), None);
    }

    #[test]
    fn a_process_span_suppresses_the_peak_of_a_mixed_operation() {
        // Mixing scopes within one operation makes the peak unknowable rather than merely
        // understated, so the thread span's figure is withheld too.
        let session = Session::new().no_stdout().no_file();
        let operation = session.operation("test");

        drop(operation.measure_thread().iterations(1));
        assert_eq!(operation.peak_outstanding_bytes(), Some(0.0));

        drop(operation.measure_process().iterations(1));

        assert_eq!(operation.peak_outstanding_bytes(), None);
    }

    #[test]
    fn panic_while_held_records_nothing() {
        let session = Session::new().no_stdout().no_file();
        let operation = session.operation("test");

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _span = operation.measure_process().iterations(1);
            panic!("boom");
        }));

        assert!(result.is_err());
        assert_eq!(operation.total_iterations(), 0);
    }
}
