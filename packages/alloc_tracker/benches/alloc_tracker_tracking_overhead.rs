//! Benchmarks measuring the compute overhead of `alloc_tracker` logic itself.
//!
//! The `overhead` subgroup benchmarks empty spans — spans that do no work but still pay for
//! span creation and destruction. The `allocator` subgroup benchmarks the [`GlobalAlloc`]
//! paths themselves, running each operation through the tracking wrapper and through the
//! bare system allocator so the difference between the pair is the tracking cost.
//!
//! # Scenario contract with the Callgrind benchmarks
//!
//! `alloc_tracker_tracking_overhead_cg.rs` measures these same scenarios as instruction
//! counts. A figure from one file only says anything about the same-named figure in the
//! other if both describe the same workload, so this file is the authority on what a
//! scenario means and the Callgrind file follows it:
//!
//! * Every Callgrind scenario has a counterpart here under the same name, as the pairing
//!   rule in `docs/callgrind-benchmarks.md` requires.
//! * Block sizes are the ones the constants below name. The Callgrind file repeats them
//!   because the two benchmarks are separate binaries, so a change here must be made there
//!   as well.
//! * A `tracked_*` scenario measures the steady state, with this thread's counters already
//!   created and registered. This file primes them once before the group; the Callgrind
//!   file isolates every benchmark in its own process and so primes them per scenario.
//! * A scenario measures only the operation its name states. Gungraun evaluates a setup
//!   expression outside the collected region, so the scenarios that operate on an existing
//!   block obtain it through `iter_batched` here to keep it out of the timed region too.

#![allow(
    missing_docs,
    reason = "No need for API documentation in benchmark code"
)]

use std::alloc::{GlobalAlloc, Layout, handle_alloc_error};
use std::hint::black_box;

use alloc_tracker::{Allocator, Session};
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};

#[global_allocator]
static ALLOCATOR: Allocator<std::alloc::System> = Allocator::system();

/// Representative of an ordinary short-lived object, comfortably inside the size
/// classes that system allocators serve from a thread-local fast path. This is the
/// case where the tracking wrapper's fixed cost is largest relative to the work it
/// wraps.
const SMALL_SIZE: usize = 64;

/// Past the size classes that system allocators serve from their fast path, so the
/// wrapper's fixed cost is measured against a materially more expensive underlying
/// operation.
const LARGE_SIZE: usize = 64 * 1024;

/// Growth target for the reallocation cases. Doubling a small block is the cheapest
/// realistic reallocation, which again maximizes the wrapper's relative share.
const REALLOC_GROWN_SIZE: usize = SMALL_SIZE * 2;

criterion_group!(benches, span_overhead, allocator_overhead);
criterion_main!(benches);

fn span_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("alloc_tracker_tracking_overhead/overhead");

    // What an `iter` closure costs before any span is involved, so the span figures below
    // can be read against it.
    group.bench_function("baseline_empty", |b| {
        b.iter(|| {
            black_box(());
        });
    });

    {
        // This bench measures tracking overhead itself, so the session suppresses
        // its own stdout and file output on drop.
        let alloc_session = Session::new().no_stdout().no_file();

        let process_op =
            alloc_session.operation("alloc_tracker_tracking_overhead/overhead/process_span_empty");
        group.bench_function("process_span_empty", |b| {
            b.iter(|| {
                let _span = process_op.measure_process().iterations(1);
                black_box(());
            });
        });

        let thread_op =
            alloc_session.operation("alloc_tracker_tracking_overhead/overhead/thread_span_empty");
        group.bench_function("thread_span_empty", |b| {
            b.iter(|| {
                let _span = thread_op.measure_thread().iterations(1);
                black_box(());
            });
        });
    }

    group.finish();
}

fn allocator_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("alloc_tracker_tracking_overhead/allocator");

    // Validating a layout is input preparation rather than allocator work, so every
    // layout a scenario needs is built once here instead of inside a timed closure.
    let small = layout(SMALL_SIZE);
    let large = layout(LARGE_SIZE);
    let grown = layout(REALLOC_GROWN_SIZE);

    // The first tracked allocation on a thread initializes that thread's counters and
    // registers them globally. Pay that one-time cost here so it does not land in the
    // measurements below.
    alloc_dealloc(&ALLOCATOR, small);

    group.bench_function("untracked_alloc_dealloc_small", |b| {
        b.iter(|| alloc_dealloc(&std::alloc::System, black_box(small)));
    });
    group.bench_function("tracked_alloc_dealloc_small", |b| {
        b.iter(|| alloc_dealloc(&ALLOCATOR, black_box(small)));
    });

    group.bench_function("untracked_alloc_dealloc_large", |b| {
        b.iter(|| alloc_dealloc(&std::alloc::System, black_box(large)));
    });
    group.bench_function("tracked_alloc_dealloc_large", |b| {
        b.iter(|| alloc_dealloc(&ALLOCATOR, black_box(large)));
    });

    group.bench_function("untracked_dealloc_small", |b| {
        b.iter_batched(
            || allocate(&std::alloc::System, small),
            |block| dealloc(&std::alloc::System, block),
            BatchSize::SmallInput,
        );
    });
    group.bench_function("tracked_dealloc_small", |b| {
        b.iter_batched(
            || allocate(&ALLOCATOR, small),
            |block| dealloc(&ALLOCATOR, block),
            BatchSize::SmallInput,
        );
    });

    group.bench_function("untracked_realloc_grow", |b| {
        b.iter_batched(
            || allocate(&std::alloc::System, small),
            |block| realloc_grow(&std::alloc::System, block, grown),
            BatchSize::SmallInput,
        );
    });
    group.bench_function("tracked_realloc_grow", |b| {
        b.iter_batched(
            || allocate(&ALLOCATOR, small),
            |block| realloc_grow(&ALLOCATOR, block, grown),
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

/// Layout of a `size`-byte block at an alignment an ordinary Rust value would ask for.
fn layout(size: usize) -> Layout {
    Layout::from_size_align(size, align_of::<u64>()).expect(
        "a type's alignment is a non-zero power of two and the benchmark sizes are orders of \
         magnitude below the layout size limit",
    )
}

/// Allocates one block, pairing it with the layout it must later be released under.
fn allocate<A: GlobalAlloc>(allocator: &A, layout: Layout) -> (*mut u8, Layout) {
    // SAFETY: `layout` has non-zero size, which is what `GlobalAlloc::alloc` requires.
    let ptr = unsafe { allocator.alloc(layout) };

    if ptr.is_null() {
        handle_alloc_error(layout);
    }

    (ptr, layout)
}

fn alloc_dealloc<A: GlobalAlloc>(allocator: &A, layout: Layout) {
    let block = allocate(allocator, layout);

    black_box(block.0);

    dealloc(allocator, block);
}

fn dealloc<A: GlobalAlloc>(allocator: &A, block: (*mut u8, Layout)) {
    let (ptr, layout) = block;

    // SAFETY: `ptr` was returned by this same allocator's `alloc` for this same `layout`
    // and has not been freed.
    unsafe {
        allocator.dealloc(ptr, layout);
    }
}

/// Owns a block until dropped.
///
/// A scenario that must not measure the release hands the block back to Criterion, which
/// drops routine outputs only after stopping the timer. That keeps the deallocation out of
/// the measured region without leaking across the benchmark's many iterations.
struct Block<'a, A: GlobalAlloc> {
    allocator: &'a A,
    ptr: *mut u8,
    layout: Layout,
}

impl<A: GlobalAlloc> Drop for Block<'_, A> {
    fn drop(&mut self) {
        // SAFETY: `ptr` was returned by `allocator` for `layout`, this type owns it, and
        // `Drop` runs exactly once, so it has not already been freed.
        unsafe {
            self.allocator.dealloc(self.ptr, self.layout);
        }
    }
}

/// Grows a block, returning it so that only the reallocation lands in the measured region.
fn realloc_grow<A: GlobalAlloc>(
    allocator: &A,
    block: (*mut u8, Layout),
    grown_layout: Layout,
) -> Block<'_, A> {
    let (ptr, layout) = block;

    // SAFETY: `ptr` was returned by `alloc` for `layout`, and `grown_layout` was built at
    // the same alignment with a size small enough that rounding it up cannot overflow
    // `isize`.
    let grown = unsafe { allocator.realloc(ptr, layout, grown_layout.size()) };

    if grown.is_null() {
        // The original block is still live, but a benchmark that cannot obtain memory has
        // nothing left to measure, and `handle_alloc_error` does not return.
        handle_alloc_error(grown_layout);
    }

    Block {
        allocator,
        ptr: grown,
        layout: grown_layout,
    }
}
