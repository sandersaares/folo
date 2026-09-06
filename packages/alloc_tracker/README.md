Memory allocation tracking utilities for benchmarks and performance analysis.

This package provides utilities to track memory allocations during code execution,
enabling analysis of allocation patterns in benchmarks and performance tests. It reports
bytes allocated per iteration, allocations per iteration, and the peak amount of memory
one iteration holds at a single moment.

## Basic usage

The typical pattern is to drive measurement from Criterion's `iter_custom` function,
feeding its chosen iteration count into `iterations` so each recorded span covers a
whole sample rather than a single iteration:

```rust
use std::hint::black_box;
use std::time::Instant;

use alloc_tracker::{Allocator, Session};
use criterion::Criterion;

#[global_allocator]
static ALLOCATOR: Allocator<std::alloc::System> = Allocator::system();

fn bench(c: &mut Criterion) {
    let session = Session::new();

    let operation = session.operation("my_operation");
    c.bench_function("my_operation", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            let _span = operation.measure_thread().iterations(iters);

            for _ in 0..iters {
                black_box(vec![1, 2, 3, 4, 5]);
            }

            start.elapsed()
        });
    });

    // When `session` is dropped it prints a human-readable summary to stdout and
    // writes machine-readable JSON files (one per operation) into the Cargo
    // target directory: target/alloc_tracker/<operation>.json.
}
```

You do not need to specify the iteration count up front, as long as it is provided
before the span is dropped. This allows you to measure work whose extent is not
known at the start.

`measure_thread` observes only the calling thread and is the right choice whenever the
measured work stays on that thread. It also covers work spread across threads that you can
instrument, provided each worker counts iterations of its own: an operation combines its
spans as repeated samples of one per-iteration cost rather than adding them up. Use
`measure_process` when the work is performed by threads you cannot instrument, or when
several threads collaborate on every iteration, accepting that it also picks up
allocations from unrelated threads, that its totals are approximate, and that it withholds
peak memory from the whole operation.

## See also

More details in the [package documentation](https://docs.rs/alloc_tracker/).

This is part of the [Folo project](https://github.com/folo-rs/folo) that provides mechanisms for
high-performance hardware-aware programming in Rust.
