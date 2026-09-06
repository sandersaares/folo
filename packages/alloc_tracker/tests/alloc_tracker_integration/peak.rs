//! Peak outstanding bytes measured against real allocations.

use std::hint::black_box;

use alloc_tracker::Session;

use crate::{report_peak, report_total_bytes};

/// The buffer size every scenario here allocates.
///
/// Large enough that incidental allocations by the test harness cannot approach it, so the
/// bounds below hold regardless of what else runs on this thread.
const BUFFER_SIZE: usize = 64 * 1024;

/// How much of a second buffer a reported peak may include before the test concludes that
/// more than the intended number of buffers was live.
///
/// The scenarios allocate nothing else of comparable size, so any fraction would do; a whole
/// extra buffer is simply the least arbitrary choice.
const HEADROOM: usize = BUFFER_SIZE;

/// A byte count as the report exposes it.
#[expect(
    clippy::cast_precision_loss,
    reason = "these are compile-time buffer sizes, far below f64's exact-integer range"
)]
fn as_reported(bytes: usize) -> f64 {
    bytes as f64
}

#[test]
#[cfg_attr(miri, ignore)] // Test uses the real platform which cannot be executed under Miri.
fn peak_measures_one_iteration_regardless_of_batch_size() {
    const SHORT_BATCH: usize = 20;

    /// The long batch is this many times longer than the short one. The cumulative totals
    /// must reflect that factor and the peak must not.
    const BATCH_RATIO: usize = 10;
    const LONG_BATCH: usize = SHORT_BATCH * BATCH_RATIO;

    fn allocate_and_release(session: &Session, name: &str, iterations: usize) {
        let op = session.operation(name);
        let _span = op.measure_thread().iterations(iterations as u64);
        for _ in 0..iterations {
            let data = vec![0_u8; BUFFER_SIZE];
            black_box(&data);
        }
    }

    let session = Session::new().no_stdout().no_file();
    allocate_and_release(&session, "short_batch", SHORT_BATCH);
    allocate_and_release(&session, "long_batch", LONG_BATCH);

    let short_peak = report_peak(&session, "short_batch").unwrap();
    let long_peak = report_peak(&session, "long_batch").unwrap();

    // Every buffer is released before the next is taken, so only one is ever live — and the
    // reported peak says so whichever batch size the harness happened to choose.
    for peak in [short_peak, long_peak] {
        assert!(peak >= as_reported(BUFFER_SIZE));
        assert!(peak < as_reported(BUFFER_SIZE + HEADROOM));
    }

    // Meanwhile the cumulative total does scale with the batch size, which is what makes the
    // peak worth reporting separately. Requiring half the ratio leaves ample room for the
    // test's own incidental allocations.
    let short_total = report_total_bytes(&session, "short_batch");
    let long_total = report_total_bytes(&session, "long_batch");
    assert!(long_total * 2 > short_total * BATCH_RATIO as u64);
}

#[test]
#[cfg_attr(miri, ignore)] // Test uses the real platform which cannot be executed under Miri.
fn a_warmup_span_does_not_dominate_the_peak() {
    const STEADY_BATCH: usize = 1000;

    /// The warmup span allocates this many times the steady-state buffer, far enough above it
    /// that the estimate would be visibly wrong if the warmup batch carried anything close to
    /// equal weight.
    const WARMUP_RATIO: usize = 16;
    const WARMUP_BUFFER_SIZE: usize = BUFFER_SIZE * WARMUP_RATIO;

    let session = Session::new().no_stdout().no_file();
    {
        let op = session.operation("warmup_then_steady");

        {
            let _span = op.measure_thread().iterations(1);
            let data = vec![0_u8; WARMUP_BUFFER_SIZE];
            black_box(&data);
        }

        {
            let _span = op.measure_thread().iterations(STEADY_BATCH as u64);
            for _ in 0..STEADY_BATCH {
                let data = vec![0_u8; BUFFER_SIZE];
                black_box(&data);
            }
        }
    }

    let peak = report_peak(&session, "warmup_then_steady").unwrap();

    assert!(peak >= as_reported(BUFFER_SIZE));
    assert!(peak < as_reported(BUFFER_SIZE + HEADROOM));
}

#[test]
#[cfg_attr(miri, ignore)] // Test uses the real platform which cannot be executed under Miri.
fn peak_covers_buffers_held_simultaneously() {
    const BUFFER_COUNT: usize = 8;

    let session = Session::new().no_stdout().no_file();
    {
        let op = session.operation("held_simultaneously");
        let _span = op.measure_thread().iterations(1);

        // A fixed-size array keeps the handles on the stack, so the only heap allocations the
        // span sees are the buffers whose simultaneous lifetime it is measuring.
        let held: [Vec<u8>; BUFFER_COUNT] = std::array::from_fn(|_| vec![0_u8; BUFFER_SIZE]);
        black_box(&held);
    }

    let peak = report_peak(&session, "held_simultaneously").unwrap();

    assert!(peak >= as_reported(BUFFER_SIZE * BUFFER_COUNT));
}

#[test]
#[cfg_attr(miri, ignore)] // Test uses the real platform which cannot be executed under Miri.
fn process_span_reports_no_peak() {
    let session = Session::new().no_stdout().no_file();
    {
        let op = session.operation("process_scope");
        let _span = op.measure_process().iterations(1);
        let data = vec![0_u8; BUFFER_SIZE];
        black_box(&data);
    }

    assert_eq!(report_peak(&session, "process_scope"), None);
}
