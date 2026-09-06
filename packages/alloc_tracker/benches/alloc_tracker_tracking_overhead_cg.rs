//! Callgrind benchmarks for the `alloc_tracker` tracking machinery itself.
//!
//! Paired with `alloc_tracker_tracking_overhead.rs`, which measures the same subgroups on
//! real hardware and states the scenario contract both files satisfy. Read it before
//! changing what a scenario here measures.
//!
//! # Scope and caveats
//!
//! Callgrind counts the user-space allocator's instructions but does not model the latency
//! of an allocation, so these numbers are **not** a measure of what allocation costs. They
//! measure the instruction count of the tracking wrapper that sits in front of the system
//! allocator, which is precisely the quantity this package promises to keep small.
//!
//! Only additive event counts may be compared by subtraction: executed instructions,
//! executed branches and global bus events. Within those, a `tracked_*` figure read against
//! its `untracked_*` sibling bounds the wrapper's added work, and the cost of allocation
//! alone is the `alloc_dealloc` pair minus the matching `dealloc` case. Cache misses, the
//! cache model's estimated cycles and branch mispredictions depend on each scenario's own
//! address and predictor history, so they do not decompose that way.
//!
//! The `allocator` subgroup isolates deallocation and reallocation by allocating in a setup
//! function, which Gungraun evaluates outside the measured region.

#![allow(
    missing_docs,
    reason = "No need for API documentation in benchmark code"
)]
#![cfg_attr(
    target_os = "linux",
    expect(
        clippy::exit,
        clippy::missing_docs_in_private_items,
        unused_qualifications,
        reason = "These lints originate in Gungraun macro expansion and cannot be addressed in \
          this benchmark."
    )
)]

use alloc_tracker::Allocator;

#[global_allocator]
static ALLOCATOR: Allocator<std::alloc::System> = Allocator::system();

#[cfg(not(target_os = "linux"))]
fn main() {
    // Valgrind is Linux-only. On other platforms this bench target compiles
    // to a no-op so `cargo build --all-targets` still works.
}

#[cfg(target_os = "linux")]
use gungraun::{Callgrind, CallgrindMetrics, LibraryBenchmarkConfig, main};
#[cfg(target_os = "linux")]
pub use linux::*;

// `--collect-bus=yes` makes Callgrind emit the global bus event (`Ge`), which counts every
// lock-prefixed instruction in the collected call graph. A non-zero `Ge` is not by itself a
// counter regression: the allocator cases include whatever the system allocator does, and
// the span cases deliberately perform `Arc` ownership operations and lock the operation's
// metrics, with process spans additionally locking the counter registry. In the allocator
// pairs, only the excess over the matching untracked case is a candidate for wrapper work,
// and where it comes from still has to be attributed. The span cases have no untracked
// sibling, so their absolute total is read against that intentional baseline.
#[cfg(target_os = "linux")]
main!(
    config = LibraryBenchmarkConfig::default().tool(
        Callgrind::default()
            .args(["--branch-sim=yes", "--collect-bus=yes"])
            .format([CallgrindMetrics::Default, CallgrindMetrics::BranchSim]),
    ),
    library_benchmark_groups = [allocator, overhead]
);

#[cfg(target_os = "linux")]
mod linux {
    use std::alloc::{GlobalAlloc, Layout, handle_alloc_error};
    use std::hint::black_box;
    use std::sync::LazyLock;

    use alloc_tracker::{Operation, Session};
    use gungraun::prelude::*;

    use crate::ALLOCATOR;

    /// Representative of an ordinary short-lived object, comfortably inside the size
    /// classes that system allocators serve from a thread-local fast path.
    const SMALL_SIZE: usize = 64;

    /// Past the size classes that system allocators serve from their fast path.
    const LARGE_SIZE: usize = 64 * 1024;

    /// Growth target for the reallocation cases.
    const REALLOC_GROWN_SIZE: usize = SMALL_SIZE * 2;

    /// Layout of a `size`-byte block at an alignment an ordinary Rust value would ask for.
    fn layout(size: usize) -> Layout {
        Layout::from_size_align(size, align_of::<u64>()).expect(
            "a type's alignment is a non-zero power of two and the benchmark sizes are \
             orders of magnitude below the layout size limit",
        )
    }

    /// Allocates one block through `allocator`, for a benchmark that measures only what
    /// happens to an already-allocated block.
    ///
    /// Called from a Gungraun setup expression, so the allocation stays outside the
    /// collected region.
    fn allocate<A: GlobalAlloc>(allocator: &A, layout: Layout) -> (*mut u8, Layout) {
        // SAFETY: `layout` has non-zero size, which is what `GlobalAlloc::alloc` requires.
        let ptr = unsafe { allocator.alloc(layout) };

        if ptr.is_null() {
            handle_alloc_error(layout);
        }

        (ptr, layout)
    }

    /// Warms the underlying allocator and hands back the layout the measured body uses.
    ///
    /// Gungraun isolates every benchmark in its own process and collects a single
    /// invocation, so an untouched allocator would charge first-touch costs to the measured
    /// body. The tracked variants receive the same warmup from `primed`, which is what lets
    /// the difference between a pair be read as tracking work.
    fn warmed(size: usize) -> Layout {
        let layout = layout(size);
        alloc_dealloc(&std::alloc::System, layout);
        layout
    }

    /// As [`warmed`], and additionally brings this thread's tracked-allocation state to the
    /// steady state the `tracked_*` scenarios measure.
    ///
    /// Going through `ALLOCATOR` creates and registers this thread's counters, so the
    /// measured body does not also pay for their one-time initialization.
    fn primed(size: usize) -> Layout {
        let layout = layout(size);
        alloc_dealloc(&ALLOCATOR, layout);
        layout
    }

    /// Allocates one block and pairs it with the layout a reallocation will grow it into.
    fn growable<A: GlobalAlloc>(allocator: &A) -> ((*mut u8, Layout), Layout) {
        (
            allocate(allocator, layout(SMALL_SIZE)),
            layout(REALLOC_GROWN_SIZE),
        )
    }

    fn alloc_dealloc<A: GlobalAlloc>(allocator: &A, layout: Layout) {
        let (ptr, layout) = allocate(allocator, layout);

        black_box(ptr);

        // SAFETY: `ptr` was returned by `alloc` for this same `layout` and has not been freed.
        unsafe {
            allocator.dealloc(ptr, layout);
        }
    }

    /// Grows a block and leaves it allocated, so the collected region covers only the
    /// reallocation the scenario is named for.
    ///
    /// Gungraun runs each benchmark in its own process and collects a single invocation, so
    /// the block is reclaimed at process exit. Releasing it here would put a deallocation
    /// inside the measured region, and the tracked-minus-untracked difference would then
    /// include deallocation tracking rather than isolating the reallocation.
    fn realloc_grow<A: GlobalAlloc>(allocator: &A, block: (*mut u8, Layout), grown_layout: Layout) {
        let (ptr, layout) = block;

        // SAFETY: `ptr` was returned by `alloc` for `layout`, and `grown_layout` was built
        // at the same alignment with a size small enough that rounding it up cannot
        // overflow `isize`.
        let grown = unsafe { allocator.realloc(ptr, layout, grown_layout.size()) };

        if grown.is_null() {
            // The original block is still live, but a benchmark that cannot obtain memory
            // has nothing left to measure, and `handle_alloc_error` does not return.
            handle_alloc_error(grown_layout);
        }

        black_box(grown);
    }

    fn dealloc<A: GlobalAlloc>(allocator: &A, block: (*mut u8, Layout)) {
        let (ptr, layout) = block;

        // SAFETY: `ptr` was returned by the matching allocator's `alloc` for this same
        // `layout` in the setup function, and has not been freed.
        unsafe {
            allocator.dealloc(ptr, layout);
        }
    }

    #[library_benchmark]
    #[bench::run(warmed(SMALL_SIZE))]
    fn allocator_untracked_alloc_dealloc_small(layout: Layout) {
        alloc_dealloc(&std::alloc::System, black_box(layout));
    }

    #[library_benchmark]
    #[bench::run(primed(SMALL_SIZE))]
    fn allocator_tracked_alloc_dealloc_small(layout: Layout) {
        alloc_dealloc(&ALLOCATOR, black_box(layout));
    }

    #[library_benchmark]
    #[bench::run(warmed(LARGE_SIZE))]
    fn allocator_untracked_alloc_dealloc_large(layout: Layout) {
        alloc_dealloc(&std::alloc::System, black_box(layout));
    }

    #[library_benchmark]
    #[bench::run(primed(LARGE_SIZE))]
    fn allocator_tracked_alloc_dealloc_large(layout: Layout) {
        alloc_dealloc(&ALLOCATOR, black_box(layout));
    }

    #[library_benchmark]
    #[bench::run(allocate(&std::alloc::System, layout(SMALL_SIZE)))]
    fn allocator_untracked_dealloc_small(block: (*mut u8, Layout)) {
        dealloc(&std::alloc::System, black_box(block));
    }

    #[library_benchmark]
    #[bench::run(allocate(&ALLOCATOR, layout(SMALL_SIZE)))]
    fn allocator_tracked_dealloc_small(block: (*mut u8, Layout)) {
        dealloc(&ALLOCATOR, black_box(block));
    }

    #[library_benchmark]
    #[bench::run(growable(&std::alloc::System))]
    fn allocator_untracked_realloc_grow(prepared: ((*mut u8, Layout), Layout)) {
        let (block, grown_layout) = black_box(prepared);
        realloc_grow(&std::alloc::System, block, grown_layout);
    }

    #[library_benchmark]
    #[bench::run(growable(&ALLOCATOR))]
    fn allocator_tracked_realloc_grow(prepared: ((*mut u8, Layout), Layout)) {
        let (block, grown_layout) = black_box(prepared);
        realloc_grow(&ALLOCATOR, block, grown_layout);
    }

    library_benchmark_group!(
        name = allocator,
        benchmarks = [
            allocator_untracked_alloc_dealloc_small,
            allocator_tracked_alloc_dealloc_small,
            allocator_untracked_alloc_dealloc_large,
            allocator_tracked_alloc_dealloc_large,
            allocator_untracked_dealloc_small,
            allocator_tracked_dealloc_small,
            allocator_untracked_realloc_grow,
            allocator_tracked_realloc_grow
        ]
    );

    /// The operation handles the span-overhead scenarios measure against.
    ///
    /// Each handle owns its metrics outright, so the session that created them is not kept
    /// alive. The operation names are workload labels rather than Criterion identifiers,
    /// which `docs/naming.md` permits for a session that is itself under measurement.
    struct Fixture {
        thread_op: Operation,
        process_op: Operation,
    }

    static FIXTURE: LazyLock<Fixture> = LazyLock::new(|| {
        let session = Session::new().no_stdout().no_file();

        Fixture {
            thread_op: session.operation("thread_span_empty"),
            process_op: session.operation("process_span_empty"),
        }
    });

    /// Forces the fixture into existence so its one-time construction and the first-touch
    /// initialization of this thread's allocation counters stay outside the measured
    /// region.
    fn fixture() -> &'static Fixture {
        let fixture = &*FIXTURE;
        drop(fixture.thread_op.measure_thread().iterations(1));

        // A process span's first submission also flips the operation's peak state to
        // unavailable, which never happens again. Criterion amortizes that across its
        // iterations; Gungraun collects one invocation, so it has to be primed away here.
        drop(fixture.process_op.measure_process().iterations(1));

        fixture
    }

    #[library_benchmark]
    #[bench::run(fixture())]
    fn overhead_thread_span_empty(fixture: &'static Fixture) {
        let _span = fixture.thread_op.measure_thread().iterations(1);
    }

    #[library_benchmark]
    #[bench::run(fixture())]
    fn overhead_process_span_empty(fixture: &'static Fixture) {
        let _span = fixture.process_op.measure_process().iterations(1);
    }

    library_benchmark_group!(
        name = overhead,
        benchmarks = [overhead_thread_span_empty, overhead_process_span_empty]
    );
}
