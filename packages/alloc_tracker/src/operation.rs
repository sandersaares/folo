//! Per-iteration allocation tracking.

use std::fmt;
use std::sync::{Arc, Mutex};

use crate::report::format_count;
use crate::{ERR_POISONED_LOCK, OperationMetrics, ProcessSpan, ThreadSpan};

/// Aggregates allocation data from repeated measurements of a single operation.
///
/// Multiple operations with the same name can be created concurrently.
///
/// # Examples
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
///     let string_allocations = session.operation("string_allocations");
///     c.bench_function("string_allocations", |b| {
///         b.iter_custom(|iters| {
///             let start = Instant::now();
///             let _span = string_allocations.measure_thread().iterations(iters);
///
///             for _ in 0..iters {
///                 black_box(String::from("Hello, world!"));
///             }
///
///             start.elapsed()
///         });
///     });
/// }
/// ```
#[derive(Debug)]
pub struct Operation {
    metrics: Arc<Mutex<OperationMetrics>>,
}

impl Operation {
    #[must_use]
    pub(crate) fn new(_name: String, operation_data: Arc<Mutex<OperationMetrics>>) -> Self {
        Self {
            metrics: operation_data,
        }
    }

    /// Returns a clone of the operation metrics for use by spans.
    #[must_use]
    pub(crate) fn metrics(&self) -> Arc<Mutex<OperationMetrics>> {
        Arc::clone(&self.metrics)
    }

    /// Begins measuring allocations made by the current thread only.
    ///
    /// Use this for single-threaded operations or when you want to track
    /// per-thread allocation usage.
    ///
    /// You must call [`iterations(n)`](ThreadSpan::iterations) on the returned span
    /// to define how many iterations the measured work covers. This is mandatory.
    /// You may pass `0` to indicate that no work was performed (e.g. on failure).
    ///
    /// The returned span records its measurement when it is dropped.
    ///
    /// # Examples
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
    ///     let operation = session.operation("thread_work");
    ///     c.bench_function("thread_work", |b| {
    ///         b.iter_custom(|iters| {
    ///             let start = Instant::now();
    ///             let _span = operation.measure_thread().iterations(iters);
    ///
    ///             for _ in 0..iters {
    ///                 black_box(vec![1, 2, 3, 4, 5]);
    ///             }
    ///
    ///             start.elapsed()
    ///         });
    ///     });
    /// }
    /// ```
    pub fn measure_thread(&self) -> ThreadSpan {
        ThreadSpan::new(self)
    }

    /// Begins measuring allocations made by every thread in the process.
    ///
    /// Use this for one caller-owned measurement enclosing work whose threads cannot be
    /// instrumented, or that several threads collaborate on iteration by iteration. When
    /// the threads can be instrumented and each processes iterations of its own, prefer
    /// giving every worker its own [`measure_thread`](Self::measure_thread) span naming
    /// this same operation: spans aggregate per operation regardless of which thread
    /// produced them, and thread scope avoids everything this scope gives up.
    ///
    /// A process span accepts the following in exchange for its reach:
    ///
    /// * Allocations made by unrelated threads during the span are attributed to this
    ///   operation.
    /// * Capture is more expensive than thread scope.
    /// * The totals are approximate: they do not represent one instantaneous view of the
    ///   process.
    /// * No peak can be observed, and one such span withholds the peak from the whole
    ///   operation, including from thread spans that did measure one.
    ///
    /// You must call [`iterations(n)`](ProcessSpan::iterations) on the returned span
    /// to define how many iterations the measured work covers. This is mandatory.
    /// You may pass `0` to indicate that no work was performed (e.g. on failure).
    ///
    /// The returned span records its measurement when it is dropped.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::hint::black_box;
    /// use std::thread;
    /// use std::time::Instant;
    ///
    /// use alloc_tracker::{Allocator, Session};
    /// use criterion::Criterion;
    ///
    /// #[global_allocator]
    /// static ALLOCATOR: Allocator<std::alloc::System> = Allocator::system();
    ///
    /// // Stands in for a library that spreads work over a worker pool it owns. The pool is
    /// // sized independently of the workload, so the thread count stays bounded however
    /// // many iterations Criterion asks for.
    /// fn fan_out(items: u64) {
    ///     const WORKERS: u64 = 4;
    ///
    ///     thread::scope(|scope| {
    ///         for worker in 0..WORKERS {
    ///             // Striding the item indices distributes any remainder without running
    ///             // more bodies than the caller reports as iterations.
    ///             scope.spawn(move || {
    ///                 let mut item = worker;
    ///                 while item < items {
    ///                     black_box(vec![1, 2, 3, 4, 5]);
    ///                     item = item.saturating_add(WORKERS);
    ///                 }
    ///             });
    ///         }
    ///     });
    /// }
    ///
    /// fn bench(c: &mut Criterion) {
    ///     let session = Session::new();
    ///     let operation = session.operation("process_work");
    ///     c.bench_function("process_work", |b| {
    ///         b.iter_custom(|iters| {
    ///             let start = Instant::now();
    ///             let _span = operation.measure_process().iterations(iters);
    ///
    ///             // The work lands on threads this benchmark cannot instrument, which is
    ///             // what process scope exists for.
    ///             fan_out(iters);
    ///
    ///             start.elapsed()
    ///         });
    ///     });
    /// }
    /// ```
    pub fn measure_process(&self) -> ProcessSpan {
        ProcessSpan::new(self)
    }

    /// Calculates the mean bytes allocated per iteration.
    ///
    /// Returns 0 if no iterations have been recorded.
    #[must_use]
    #[cfg(test)]
    #[cfg_attr(coverage_nightly, coverage(off))] // Test scaffolding, not shipped behavior.
    pub(crate) fn mean(&self) -> u64 {
        let data = self.metrics.lock().expect(ERR_POISONED_LOCK);
        data.mean_bytes()
    }

    /// Returns the total number of iterations recorded.
    #[must_use]
    #[cfg(test)]
    #[cfg_attr(coverage_nightly, coverage(off))] // Test scaffolding, not shipped behavior.
    pub(crate) fn total_iterations(&self) -> u64 {
        let data = self.metrics.lock().expect(ERR_POISONED_LOCK);
        data.total_iterations()
    }

    /// Returns the total bytes allocated across all iterations.
    #[must_use]
    #[cfg(test)]
    #[cfg_attr(coverage_nightly, coverage(off))] // Test scaffolding, not shipped behavior.
    pub(crate) fn total_bytes_allocated(&self) -> u64 {
        let data = self.metrics.lock().expect(ERR_POISONED_LOCK);
        data.total_bytes_allocated()
    }

    /// Returns the per-iteration peak bytes held allocated across all recorded spans.
    #[must_use]
    #[cfg(test)]
    #[cfg_attr(coverage_nightly, coverage(off))] // Test scaffolding, not shipped behavior.
    pub(crate) fn peak_outstanding_bytes(&self) -> Option<f64> {
        let data = self.metrics.lock().expect(ERR_POISONED_LOCK);
        data.peak_outstanding_bytes()
    }
}

impl fmt::Display for Operation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The summary shows only the per-iteration slopes, not their intervals.
        let metrics = self.metrics.lock().expect(ERR_POISONED_LOCK);
        match (metrics.bytes_slope(), metrics.allocations_slope()) {
            (Some(bytes), Some(allocations)) => write!(
                f,
                "{} bytes/iter, {} allocations/iter",
                format_count(bytes),
                format_count(allocations),
            ),
            _ => write!(f, "no measurements"),
        }
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::panic::{RefUnwindSafe, UnwindSafe};

    use super::*;
    use crate::Session;
    use crate::counters::register_fake_allocation;

    fn create_test_operation() -> Operation {
        let session = Session::new().no_stdout().no_file();
        session.operation("test")
    }

    #[test]
    fn operation_new() {
        let operation = create_test_operation();
        assert_eq!(operation.mean(), 0);
        assert_eq!(operation.total_iterations(), 0);
        assert_eq!(operation.total_bytes_allocated(), 0);
    }

    #[test]
    fn operation_add_single() {
        let operation = create_test_operation();

        // Directly test the metrics
        {
            let mut metrics = operation.metrics.lock().unwrap();
            metrics.add_iterations(100, 1, 1);
        }

        assert_eq!(operation.mean(), 100);
        assert_eq!(operation.total_iterations(), 1);
        assert_eq!(operation.total_bytes_allocated(), 100);
    }

    #[test]
    fn operation_add_multiple() {
        let operation = create_test_operation();

        // Directly test the metrics
        {
            let mut metrics = operation.metrics.lock().unwrap();
            metrics.add_iterations(100, 1, 1); // 100 bytes, 1 allocation, 1 iteration
            metrics.add_iterations(200, 2, 1); // 200 bytes, 2 allocations, 1 iteration  
            metrics.add_iterations(300, 3, 1); // 300 bytes, 3 allocations, 1 iteration
        }

        assert_eq!(operation.mean(), 200); // (100 + 200 + 300) / (1 + 1 + 1) = 600 / 3 = 200
        assert_eq!(operation.total_iterations(), 3);
        assert_eq!(operation.total_bytes_allocated(), 600);
    }

    #[test]
    fn operation_add_zero() {
        let operation = create_test_operation();

        // Directly test the metrics
        {
            let mut metrics = operation.metrics.lock().unwrap();
            metrics.add_iterations(0, 0, 1);
            metrics.add_iterations(0, 0, 1);
        }

        assert_eq!(operation.mean(), 0);
        assert_eq!(operation.total_iterations(), 2);
        assert_eq!(operation.total_bytes_allocated(), 0);
    }

    #[test]
    fn operation_span_drop() {
        let operation = create_test_operation();

        {
            let _span = operation.measure_thread().iterations(1);
            // Simulate allocation
            register_fake_allocation(75, 1);
        }

        assert_eq!(operation.mean(), 75);
        assert_eq!(operation.total_iterations(), 1);
        assert_eq!(operation.total_bytes_allocated(), 75);
    }

    #[test]
    fn operation_multiple_spans() {
        let operation = create_test_operation();

        {
            let _span = operation.measure_thread().iterations(1);
            register_fake_allocation(100, 1);
        }

        {
            let _span = operation.measure_thread().iterations(1);
            register_fake_allocation(200, 1);
        }

        assert_eq!(operation.mean(), 150); // (100 + 200) / 2
        assert_eq!(operation.total_iterations(), 2);
        assert_eq!(operation.total_bytes_allocated(), 300);
    }

    #[test]
    fn operation_thread_span_drop() {
        let operation = create_test_operation();

        {
            let _span = operation.measure_thread().iterations(1);
            register_fake_allocation(50, 1);
        }

        assert_eq!(operation.mean(), 50);
        assert_eq!(operation.total_iterations(), 1);
        assert_eq!(operation.total_bytes_allocated(), 50);
    }

    #[test]
    fn operation_mixed_spans() {
        let operation = create_test_operation();

        {
            let _span = operation.measure_thread().iterations(1);
            register_fake_allocation(100, 1);
        }

        {
            let _span = operation.measure_thread().iterations(1);
            register_fake_allocation(200, 1);
        }

        assert_eq!(operation.mean(), 150); // (100 + 200) / 2
        assert_eq!(operation.total_iterations(), 2);
        assert_eq!(operation.total_bytes_allocated(), 300);
    }

    #[test]
    fn operation_thread_span_no_allocation() {
        let operation = create_test_operation();

        {
            let _span = operation.measure_thread().iterations(1);
            // No allocation
        }

        assert_eq!(operation.mean(), 0);
        assert_eq!(operation.total_iterations(), 1);
        assert_eq!(operation.total_bytes_allocated(), 0);
    }

    #[test]
    fn operation_batch_iterations() {
        let operation = create_test_operation();

        {
            let _span = operation.measure_thread().iterations(10);
            // Simulate a 1000 byte allocation that should be divided by 10 iterations
            register_fake_allocation(1000, 10);
        }

        assert_eq!(operation.total_iterations(), 10);
        assert_eq!(operation.total_bytes_allocated(), 1000);
        assert_eq!(operation.mean(), 100); // 1000 / 10
    }

    #[test]
    fn operation_drop_merges_data() {
        let session = Session::new().no_stdout().no_file();

        // Create and use operation
        {
            let operation = session.operation("test");

            // Directly test the metrics
            {
                let mut metrics = operation.metrics.lock().unwrap();
                metrics.add_iterations(100, 2, 5);
            }
            // operation is dropped here, merging data into session
        }

        // Check that session contains the data
        let report = session.to_report();
        assert!(!report.is_empty());

        // Verify the session shows the data was merged. A single span of 100
        // bytes/iter over 5 iterations yields a slope of 100; the two
        // allocations/iter behave the same way. The stdout table shows the slope
        // only (no interval).
        let report = session.to_report();
        let (_, op) = report.operations().next().expect("one operation");
        let stats = op.statistics().expect("operation has spans");
        let session_display = format!("{session}");
        println!("Actual session display: '{session_display}'");
        assert!(
            session_display.contains(&format_count(stats.bytes.slope)),
            "table should show the byte slope: got {session_display}"
        );
        assert!(
            session_display.contains(&format_count(stats.allocations.slope)),
            "table should show the allocation slope: got {session_display}"
        );
    }

    #[test]
    fn multiple_operations_concurrent() {
        let session = Session::new().no_stdout().no_file();

        let op1 = session.operation("test");
        let op2 = session.operation("test");

        // Directly manipulate the metrics
        {
            let mut metrics = op1.metrics.lock().unwrap();
            metrics.add_iterations(100, 1, 2); // 200 bytes, 2 allocations, 2 iterations
            metrics.add_iterations(200, 2, 3); // 600 bytes, 6 allocations, 3 iterations
        }

        // Both operations share the same data immediately since they have the same name
        // Total: 200 + 600 = 800 bytes, 2 + 3 = 5 iterations, mean = 800 / 5 = 160 bytes
        assert_eq!(op1.mean(), 160);
        assert_eq!(op2.mean(), 160);

        // Drop operations
        drop(op1);
        drop(op2);

        // Session should show merged results. The iterations²-weighted
        // through-origin slope over spans (200 bytes / 2 iters) and (600 bytes /
        // 3 iters) is (2·200 + 3·600) / (2² + 3²) = 2200 / 13 ≈ 169.23 bytes/iter,
        // which differs from the pooled mean of 160.
        let session_display = format!("{session}");
        assert!(session_display.contains("169.23"), "got {session_display}");
    }

    static_assertions::assert_impl_all!(Operation: Send, Sync);
    static_assertions::assert_impl_all!(
        Operation: UnwindSafe, RefUnwindSafe
    );

    #[test]
    fn operation_display_shows_robust_per_iteration_estimate() {
        let operation = create_test_operation();

        // Add some data to have a non-zero slope.
        // add_iterations(bytes_delta, count_delta, iterations) means bytes_delta * iterations total bytes.
        {
            let mut metrics = operation.metrics.lock().unwrap();
            metrics.add_iterations(250, 5, 2); // Single span → slope of 250 bytes/iter.
        }

        let display_output = operation.to_string();
        assert!(
            display_output.contains("250 bytes/iter"),
            "got {display_output}"
        );
        assert!(
            display_output.contains("5 allocations/iter"),
            "got {display_output}"
        );
    }

    #[test]
    fn operation_display_reports_no_measurements_when_empty() {
        // An operation with no recorded spans has no dispersion statistics, so its
        // Display takes the "no measurements" leg.
        let operation = create_test_operation();
        assert_eq!(operation.to_string(), "no measurements");
    }
}
