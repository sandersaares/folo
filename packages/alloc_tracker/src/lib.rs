#![cfg_attr(coverage_nightly, feature(coverage_attribute))]
#![cfg_attr(docsrs, feature(doc_cfg))]

//! Memory allocation tracking utilities for benchmarks and performance analysis.
//!
//! This package provides utilities to track memory allocations during code execution,
//! enabling analysis of allocation patterns in benchmarks and performance tests.
//! The tracker reports the number of bytes allocated, the count of allocations and the
//! peak amount of memory held at any single moment.
//!
//! The core functionality includes:
//! - [`Allocator`] - A Rust memory allocator wrapper that enables allocation tracking
//! - [`Session`] - Configures allocation tracking and provides access to tracking data
//! - [`Report`] - Thread-safe memory allocation statistics that can be merged and processed independently
//! - [`ProcessSpan`] - Tracks process-wide memory allocation changes over a time period
//! - [`ThreadSpan`] - Tracks thread-local memory allocation changes over a time period
//! - [`Operation`] - Measures per-iteration memory allocation of a repeated operation
//!
//! Additionally, when the `panic_on_next_alloc` feature is enabled:
#![cfg_attr(
    feature = "panic_on_next_alloc",
    doc = "- [`panic_on_next_alloc`] - Function to enable panic-on-next-allocation for debugging"
)]
#![cfg_attr(
    not(feature = "panic_on_next_alloc"),
    doc = "- `panic_on_next_alloc` - Function to enable panic-on-next-allocation for debugging"
)]
//!
//! This package is not meant for use in production, serving only as a development tool.
//!
//! # Features
#![cfg_attr(
    feature = "panic_on_next_alloc",
    doc = "- `panic_on_next_alloc`: Enables the [`panic_on_next_alloc`] function for debugging"
)]
#![cfg_attr(
    not(feature = "panic_on_next_alloc"),
    doc = "- `panic_on_next_alloc`: Enables the `panic_on_next_alloc` function for debugging"
)]
//!   unexpected allocations. This feature adds some overhead to allocations, so it is optional.
//!
//! # Benchmarking
//!
//! The typical pattern is to drive measurement from Criterion's [`iter_custom()`]
//! function, feeding its chosen iteration count into
//! [`iterations()`](ThreadSpan::iterations) so each recorded span covers a whole
//! sample rather than a single iteration:
//!
//! ```no_run
//! use std::hint::black_box;
//! use std::time::Instant;
//!
//! use alloc_tracker::{Allocator, Session};
//! use criterion::Criterion;
//!
//! #[global_allocator]
//! static ALLOCATOR: Allocator<std::alloc::System> = Allocator::system();
//!
//! fn bench(c: &mut Criterion) {
//!     let session = Session::new();
//!
//!     let operation = session.operation("my_operation");
//!     c.bench_function("my_operation", |b| {
//!         b.iter_custom(|iters| {
//!             let start = Instant::now();
//!             let _span = operation.measure_thread().iterations(iters);
//!
//!             for _ in 0..iters {
//!                 black_box(vec![1, 2, 3, 4, 5]);
//!             }
//!
//!             start.elapsed()
//!         });
//!     });
//!
//!     // When `session` is dropped, the recorded statistics are printed to
//!     // stdout and written to the Cargo target directory as JSON.
//! }
//! ```
//!
//! You **must** call [`iterations()`](ThreadSpan::iterations) on the span before
//! it is dropped. Failure to do so will result in a panic.
//!
//! # Measurement scope
//!
//! [`Operation::measure_thread`] observes only the calling thread's allocator
//! activity and is the right choice whenever the measured work stays on that
//! thread, which covers most benchmarks.
//!
//! Work spread across threads is still best measured this way when those threads can
//! be instrumented and each processes iterations of its own: every worker opens a span
//! naming the same operation, counting the iterations it completed itself. An operation
//! combines its spans as repeated samples of one per-iteration cost rather than adding
//! them up, so threads that collaborate on every iteration need
//! [`Operation::measure_process`] instead.
//!
//! [`Operation::measure_process`] observes every thread in the process. Reach for it
//! when the measured work is performed by threads that cannot be instrumented,
//! accepting that it costs more to capture, that its totals are approximate, that it
//! attributes concurrent allocation by unrelated threads to the operation, and that it
//! withholds peak memory from the whole operation.
//!
//! # Metrics
//!
//! Every metric is a per-iteration figure, estimated across an operation's spans
//! with each span weighted by the square of its iteration count. Benchmark harnesses
//! run short warmup batches before settling into long steady-state ones, and the
//! weighting makes those short batches contribute almost nothing.
//!
//! Bytes per iteration and allocations per iteration describe the cost of performing
//! the operation once.
//!
//! Peak bytes is the memory one iteration holds at its high-water moment, measured
//! relative to the memory already outstanding when a span began. It answers how much
//! memory has to exist at once, which the cumulative byte count cannot: an operation
//! that takes and releases a buffer a thousand times and one that holds a thousand
//! buffers allocate the same total.
//!
//! The estimate assumes every iteration within a measured batch reaches the same peak.
//! An operation that instead accumulates memory across the iterations of a batch
//! violates that assumption. Nothing detects the violation, so a figure is still
//! reported, but it grows with the iteration counts the harness chose and is not
//! comparable between runs.
//!
//! Peak bytes requires that every span of the operation could measure it, so an
//! operation containing even one [`ProcessSpan`] reports no peak rather than one that
//! describes only part of the work. Spans that measured a peak but covered no
//! iterations leave the rate undefined, which also leaves nothing to report.
//!
//! Span watermarks are averaged, not summed, so an operation measured concurrently on
//! several threads reports what a typical one of them held rather than the total held
//! at once. It counts memory requested through the allocator as seen at the boundaries
//! of allocator calls, so memory an allocator transiently holds inside a call does not
//! appear in it.
//!
//! Because the measurement is relative to the level at span entry, an operation that
//! releases memory it did not allocate creates headroom that masks its own later
//! allocations. Peak bytes does not describe operations whose main effect is freeing
//! pre-existing memory. Deallocation is attributed to the thread that performs it, so
//! a [`ThreadSpan`] covering work that allocates on one thread and frees on another
//! does not describe how much memory is live in the process.
//!
//! # Human-readable summary
//!
//! When a [`Session`] is dropped it prints a table to stdout, one row per operation:
//!
//! ```text
//! Allocation statistics:
//!
//! | Operation       | Bytes/iter | Allocations/iter | Peak bytes |
//! |-----------------|------------|------------------|------------|
//! | allocate_buffer |       1024 |                3 |       1024 |
//! | build_map       |         64 |                1 |        n/a |
//! ```
//!
//! # Machine-readable output
//!
//! Dropping a [`Session`] also writes JSON files (one per operation) into the
//! Cargo target directory at `target/alloc_tracker/<operation>.json`, with
//! operation names sanitized to be filesystem-safe.
//!
//! Both outputs are produced automatically, so a typical benchmark only needs to
//! create a session and record work.
//!
//! # Measuring a variable amount of work
//!
//! You do not need to specify the iteration count up front, as long as it is
//! provided before the span is dropped.
//!
//! This allows you to measure work whose extent is not known at the start.
//!
//! ```
//! use std::hint::black_box;
//!
//! use alloc_tracker::{Allocator, Session};
//!
//! #[global_allocator]
//! static ALLOCATOR: Allocator<std::alloc::System> = Allocator::system();
//!
//! fn main() {
//!     let session = Session::new();
//! #   let session = session.no_stdout().no_file();
//!     let operation = session.operation("drain_queue");
//!
//!     let span = operation.measure_thread();
//!
//!     let mut processed = 0_u64;
//!
//!     while let Some(item) = get_next_item() {
//!         black_box(item.process());
//!         processed += 1;
//!     }
//!
//!     drop(span.iterations(processed));
//! }
//! # fn get_next_item() -> Option<Item> {
//! #     use std::sync::atomic::{AtomicU32, Ordering};
//! #     static REMAINING: AtomicU32 = AtomicU32::new(3);
//! #     (REMAINING.fetch_sub(1, Ordering::Relaxed) > 0).then_some(Item)
//! # }
//! # struct Item;
//! # impl Item { fn process(&self) -> Vec<u8> { vec![0_u8; 16] } }
//! ```
//!
//! # Overhead
//!
//! Capturing a single measurement by calling `measure_xyz()` incurs a small
//! overhead (on the order of tens of nanoseconds on an arbitrary sample machine),
//! and the tracking logic slightly perturbs allocator activity.
//!
//! It is crucial that you measure multiple iterations in the same sample to
//! amortize this overhead. This is the purpose of the [`iter_custom()`] pattern
//! described above.
//!
//! Operating without batching, by measuring individual iterations, is only viable
//! for macrobenchmarks for which a single iteration is a large unit of work (e.g.
//! an HTTP request).
//!
//! # Session management
//!
//! Multiple [`Session`] instances can be used concurrently as they track memory allocation
//! independently. Each session maintains its own set of operations and statistics.
//!
//! While [`Session`] itself is single-threaded, reports from sessions can be converted to
//! thread-safe [`Report`] instances and sent to other threads for processing:
//!
//! ```
//! use std::thread;
//!
//! use alloc_tracker::{Allocator, Session};
//!
//! #[global_allocator]
//! static ALLOCATOR: Allocator<std::alloc::System> = Allocator::system();
//!
//! # fn main() {
//! let session = Session::new();
//! # let session = session.no_stdout().no_file();
//! {
//!     let operation = session.operation("work");
//!     let _span = operation.measure_thread().iterations(1);
//!     let _data = vec![1, 2, 3]; // Some allocation work
//! }
//!
//! let report = session.to_report();
//!
//! // Reports are `Send`, so they can be moved to another thread for processing.
//! let total_bytes: u64 = thread::spawn(move || {
//!     report
//!         .operations()
//!         .map(|(_, op)| op.total_bytes_allocated())
//!         .sum()
//! })
//! .join()
//! .unwrap();
//!
//! println!("Captured {total_bytes} bytes across all operations");
//! # }
//! ```
//!
//! # Miri compatibility
//!
//! Miri replaces the global allocator with its own logic, so you cannot execute code that uses
//! this package under Miri.
//!
//! [`iter_custom()`]: https://docs.rs/criterion/latest/criterion/struct.Bencher.html#method.iter_custom

mod allocator;
mod constants;
mod counters;
mod operation;
mod operation_metrics;
mod process_span;
mod report;
mod session;
mod span_measurement;
mod target_output;
mod thread_span;

pub use allocator::*;
pub(crate) use constants::*;
pub use operation::*;
pub(crate) use operation_metrics::*;
pub use process_span::*;
pub use report::*;
pub use session::*;
pub(crate) use span_measurement::*;
pub use thread_span::*;
