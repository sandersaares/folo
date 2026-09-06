//! How thread and process scope differ once work is spread across threads.

use std::hint::black_box;
use std::thread;

use alloc_tracker::Session;

use crate::report_total_bytes;

#[test]
#[cfg_attr(miri, ignore)] // Test uses the real platform which cannot be executed under Miri.
fn multithreaded_allocations_show_span_differences() {
    const NUM_WORKER_THREADS: u32 = 4;
    const ALLOCATIONS_PER_THREAD: u32 = 50;
    const MAIN_THREAD_ALLOCATIONS: u32 = 10;
    const TEST_ITERATIONS: usize = 3;

    let session = Session::new().no_stdout().no_file();

    // Helper function to spawn worker threads that allocate memory
    let spawn_workers = || {
        let handles: Vec<_> = (0..NUM_WORKER_THREADS)
            .map(|thread_id| {
                thread::spawn(move || {
                    for i in 0..ALLOCATIONS_PER_THREAD {
                        let size = ((thread_id + 1) * 100 + i) as usize;
                        let data = vec![42_u8; size];
                        black_box(data);
                    }
                })
            })
            .collect();

        // Do some allocations on the main thread
        for i in 0..MAIN_THREAD_ALLOCATIONS {
            #[expect(
                clippy::cast_possible_truncation,
                reason = "small test values will not truncate"
            )]
            let data = vec![i as u8; 100];
            black_box(data);
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }
    };

    // Test process span with multithreaded work (should capture all threads)
    {
        let process_op = session.operation("process_multithreaded");
        for _ in 0..TEST_ITERATIONS {
            let _span = process_op.measure_process().iterations(1);
            spawn_workers();
        }
    }
    let process_total = report_total_bytes(&session, "process_multithreaded");

    // Test thread span with multithreaded work (should only capture main thread)
    {
        let thread_op = session.operation("thread_multithreaded");
        for _ in 0..TEST_ITERATIONS {
            let _span = thread_op.measure_thread().iterations(1);
            spawn_workers();
        }
    }
    let thread_total = report_total_bytes(&session, "thread_multithreaded");

    // Both should have allocated some memory
    assert!(process_total > 0);
    assert!(thread_total > 0);

    // Process span should capture significantly more than thread span
    assert!(process_total > thread_total * 2);
}

#[test]
#[cfg_attr(miri, ignore)] // Test uses the real platform which cannot be executed under Miri.
fn mixed_span_types_in_multithreaded_context() {
    const ITERATIONS: usize = 3;

    let session = Session::new().no_stdout().no_file();
    let mixed_op = session.operation("mixed_multithreaded");

    for iteration in 1..=ITERATIONS {
        // Alternate between process and thread spans
        if iteration % 2 == 0 {
            let _span = mixed_op.measure_process().iterations(1);
            // Spawn a thread that allocates memory
            let handle = thread::spawn(|| {
                let data = vec![0_u8; 500];
                black_box(data);
            });
            // Also allocate on main thread
            let data = vec![0_u8; 100];
            black_box(data);
            handle.join().unwrap();
        } else {
            let _span = mixed_op.measure_thread().iterations(1);
            // Spawn a thread that allocates memory (will not be captured by thread span)
            let handle = thread::spawn(|| {
                let data = vec![0_u8; 500];
                black_box(data);
            });
            // Only main thread allocation should be captured
            let data = vec![0_u8; 100];
            black_box(data);
            handle.join().unwrap();
        }
    }

    let total = report_total_bytes(&session, "mixed_multithreaded");
    assert!(total > 0);
}

#[test]
#[cfg_attr(miri, ignore)] // Test uses the real platform which cannot be executed under Miri.
fn process_report_includes_allocations_from_multiple_threads() {
    const THREAD_A_ALLOCS: usize = 40;
    const THREAD_B_ALLOCS: usize = 25;
    const SIZE_A: usize = 128; // bytes per allocation in thread A
    const SIZE_B: usize = 256; // bytes per allocation in thread B

    let session = Session::new().no_stdout().no_file();
    {
        let op = session.operation("two_thread_process");
        let _span = op.measure_process().iterations(1);

        let handle_a = thread::spawn(|| {
            let mut total = 0_usize;
            for _ in 0..THREAD_A_ALLOCS {
                let v = vec![0_u8; SIZE_A];
                total += v.len();
                black_box(&v);
            }
            total
        });

        let handle_b = thread::spawn(|| {
            let mut total = 0_usize;
            for _ in 0..THREAD_B_ALLOCS {
                let v = vec![1_u8; SIZE_B];
                black_box(&v);
                total += v.len();
            }
            total
        });

        // Also allocate on the main thread so we can distinguish process span > sum of one thread.
        let main_alloc = vec![2_u8; 64];
        black_box(&main_alloc);

        let a_bytes = handle_a.join().unwrap();
        let b_bytes = handle_b.join().unwrap();

        // Basic sanity: ensure we actually performed the expected sizes.
        assert_eq!(a_bytes, THREAD_A_ALLOCS * SIZE_A);
        assert_eq!(b_bytes, THREAD_B_ALLOCS * SIZE_B);
    }

    let report = session.to_report();
    let operations: Vec<_> = report.operations().collect();
    assert_eq!(operations.len(), 1);
    let (_name, op) = operations.first().unwrap();
    let total = op.total_bytes_allocated();

    // Expect at least the sum of the two thread totals (plus main thread allocation overhead)
    let min_expected = (THREAD_A_ALLOCS * SIZE_A + THREAD_B_ALLOCS * SIZE_B) as u64;
    assert!(total >= min_expected);

    // Ensure neither thread's contribution is trivially missing: the total should exceed
    // each individual component.
    assert!(total >= (THREAD_A_ALLOCS * SIZE_A) as u64);
    assert!(total >= (THREAD_B_ALLOCS * SIZE_B) as u64);
}
