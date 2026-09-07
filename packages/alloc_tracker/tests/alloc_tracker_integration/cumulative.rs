//! Cumulative byte and allocation-count reporting for single-threaded work.

use std::hint::black_box;

use alloc_tracker::Session;

use crate::report_total_bytes;

#[test]
#[cfg_attr(miri, ignore)] // Test uses the real platform which cannot be executed under Miri.
fn single_thread_allocations() {
    const BYTES_PER_ITERATION: usize = 100;
    const TEST_ITERATIONS: usize = 5;

    let session = Session::new().no_stdout().no_file();

    // Test process span in single-threaded context
    {
        let process_op = session.operation("process_single_thread");
        for i in 1..=TEST_ITERATIONS {
            let _span = process_op.measure_process().iterations(1);
            let _data = vec![0_u8; i * BYTES_PER_ITERATION];
            black_box(&_data);
        }
    }
    let process_total = report_total_bytes(&session, "process_single_thread");

    // Test thread span in single-threaded context
    {
        let thread_op = session.operation("thread_single_thread");
        for i in 1..=TEST_ITERATIONS {
            let _span = thread_op.measure_thread().iterations(1);
            let _data = vec![0_u8; i * BYTES_PER_ITERATION];
            black_box(&_data);
        }
    }
    let thread_total = report_total_bytes(&session, "thread_single_thread");

    // Both should have allocated some memory
    assert!(process_total > 0);
    assert!(thread_total > 0);

    assert!(process_total >= thread_total);
}

#[test]
#[cfg_attr(miri, ignore)] // Test uses the real platform which cannot be executed under Miri.
fn report_slope_with_known_allocations() {
    const NUM_ITERATIONS: u64 = 5;

    /// Each iteration boxes this many `u64` values, which is the allocation count the slope
    /// must recover and the lower bound on the bytes the slope must report.
    const BOXES_PER_ITERATION: u64 = 2;
    const BYTES_PER_ITERATION: u64 = BOXES_PER_ITERATION * size_of::<u64>() as u64;

    let session = Session::new().no_stdout().no_file();

    {
        let operation = session.operation("known_allocation");
        for _ in 0..NUM_ITERATIONS {
            let _span = operation.measure_thread().iterations(1);
            // Distinct heap allocations per iteration - a predictable allocation count,
            // independent of any allocator size overhead.
            let first = Box::new(42_u64);
            let second = Box::new(7_u64);
            black_box((first, second)); // Ensure the allocations are not optimized away.
        }
    } // Operation is dropped here, merging data to session

    let report = session.to_report();
    let operations: Vec<_> = report.operations().collect();
    assert_eq!(operations.len(), 1);

    let (_name, op) = operations.first().unwrap();
    assert_eq!(op.total_iterations(), NUM_ITERATIONS);

    // Allocators may add overhead, so the boxed payload is a lower bound rather than the
    // exact figure.
    let total_bytes = op.total_bytes_allocated();
    assert!(total_bytes >= NUM_ITERATIONS * BYTES_PER_ITERATION);

    let bytes_per_iteration = op
        .bytes()
        .expect("operation with recorded spans has an estimable per-iteration slope");

    // The spans are identical single-iteration allocations, so the slope
    // collapses onto total / iterations.
    #[expect(
        clippy::cast_precision_loss,
        reason = "byte counts in this test are far below f64's exact-integer range"
    )]
    let expected = total_bytes as f64 / NUM_ITERATIONS as f64;
    assert!((bytes_per_iteration - expected).abs() < 1.0);

    #[expect(
        clippy::cast_precision_loss,
        reason = "byte counts in this test are far below f64's exact-integer range"
    )]
    let minimum_bytes_per_iteration = BYTES_PER_ITERATION as f64;
    assert!(bytes_per_iteration >= minimum_bytes_per_iteration);

    let allocations_per_iteration = op
        .allocations()
        .expect("operation with recorded spans has an estimable per-iteration slope");

    #[expect(
        clippy::cast_precision_loss,
        reason = "the allocation count in this test is far below f64's exact-integer range"
    )]
    let expected_allocations = BOXES_PER_ITERATION as f64;
    assert!((allocations_per_iteration - expected_allocations).abs() < f64::EPSILON);
}
