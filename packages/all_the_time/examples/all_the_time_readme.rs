//! Example that demonstrates the `iter_custom` pattern shown in the README.md file.
//!
//! This shows the recommended `iter_custom` pattern for tracking processor time,
//! feeding the Criterion-chosen iteration count into each recorded span.
//!
//! Criterion runs each workload once in test mode, without warm-up or statistical
//! analysis. Pass `-- --bench` to `cargo run` to opt into actual benchmarking.

use std::hint::black_box;
use std::time::Instant;

use all_the_time::Session;
use criterion::Criterion;

fn main() {
    let session = Session::new();
    let operation = session.operation("my_operation");

    let mut criterion = Criterion::default().without_plots().configure_from_args();
    criterion.bench_function("my_operation", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            let _span = operation.measure_thread().iterations(iters);
            for _ in 0..iters {
                black_box(42_u64.wrapping_mul(2));
            }
            start.elapsed()
        });
    });

    // When `session` is dropped it prints a human-readable summary to stdout and
    // writes machine-readable JSON files (one per operation) into the Cargo
    // target directory: target/all_the_time/<operation>.json
}
