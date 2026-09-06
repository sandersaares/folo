//! Example that demonstrates the `iter_custom` pattern shown in the README.md file.
//!
//! This shows the recommended `iter_custom` pattern for tracking memory
//! allocations, plus how to debug unexpected allocations with the
//! `panic_on_next_alloc` feature.
//!
//! Criterion is configured with a small measurement budget so the example runs
//! quickly and deterministically as a smoke test; a real benchmark would use
//! `Criterion::default()`.
//!
//! To run the `panic_on_next_alloc` functionality, enable the feature:
//! ```bash
//! cargo run --example alloc_tracker_readme --features panic_on_next_alloc
//! ```

use std::hint::black_box;
use std::time::{Duration, Instant};

#[cfg(not(feature = "panic_on_next_alloc"))]
use alloc_tracker::{Allocator, Session};
#[cfg(feature = "panic_on_next_alloc")]
use alloc_tracker::{Allocator, Session, panic_on_next_alloc};
use criterion::Criterion;

#[global_allocator]
static ALLOCATOR: Allocator<std::alloc::System> = Allocator::system();

#[expect(clippy::useless_vec, reason = "example needs to show allocation")]
fn main() {
    let session = Session::new();
    let operation = session.operation("my_operation");

    let mut criterion = Criterion::default()
        .warm_up_time(Duration::from_millis(100))
        .measurement_time(Duration::from_millis(500))
        .sample_size(10)
        .without_plots();
    criterion.bench_function("my_operation", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            let _span = operation.measure_thread().iterations(iters);
            for _ in 0..iters {
                black_box(vec![1, 2, 3, 4, 5]); // This allocates memory
            }
            start.elapsed()
        });
    });

    // When `session` is dropped it prints a human-readable summary to stdout and
    // writes machine-readable JSON files (one per operation) into the Cargo
    // target directory: target/alloc_tracker/<operation>.json

    // Debugging unexpected allocations (only with feature enabled)
    #[cfg(feature = "panic_on_next_alloc")]
    {
        // Enable panic on next allocation.
        panic_on_next_alloc(true);

        // Any allocation attempt will now panic with a descriptive message
        // let _vec = vec![1, 2, 3]; // This would panic and auto-reset the flag!

        // Disable to allow allocations again.
        panic_on_next_alloc(false);
        let _vec = vec![1, 2, 3]; // This is safe
    }

    #[cfg(not(feature = "panic_on_next_alloc"))]
    {
        println!("To test panic_on_next_alloc functionality, run with:");
        println!("cargo run --example alloc_tracker_readme --features panic_on_next_alloc");
        let _vec = vec![1, 2, 3]; // Regular allocation without panic functionality
    }
}
