# Folo

Mechanisms for high-performance hardware-aware programming in Rust.

# What it gives you

![](doc/hardware.png)

To take advantage of hardware-awareness we must first gain that awareness.
[`many_cpus`][many_cpus] informs us about the nature of the system's processors and
[the arrangement of them in relation to main memory][numa], giving us control over what
specific logic runs on which specific processors. No longer do we simply `thread::spawn()`
blindly - now we spawn specific threads for specific processors or groups of processors.

[`many_cpus_benchmarking`][many_cpus_b] provides a benchmark harness to explore the effects of
how work and data are spread between processors, when applied to different algorithms. This
allows you to judge when it matters and by how much.

[`vicinal`][vicinal] allows you to schedule synchronous tasks on the same processor, providing
an easy way to preserve data locality for work that must be detached from the primary application
threads (e.g. because it is blocking code not suitable for an async application thread).

![](doc/region_cached.png)

[The ability to target our workloads to specific processors and specific memory regions unlocks new optimization opportunities.][structural_changes]
[`region_local`][region_local] and [`region_cached`][region_cached]
provide something like a layer of caching between the processor and main memory, ensuring that
even data sets that do not fit into processor caches still experience high data locality.

![](doc/linked.png)

Designing code for hardware-efficiency often benefits from a thread-isolated mindset, treating
each thread as its own universe. [`linked`][linked] provides valuable concepts, metaphors and
mechanisms to enable objects to present a unique face to each thread, acting as separate objects
on each thread while being connected through internal logic and only permitting opt-in transfers
across thread boundaries. These are the building blocks used to implement `region_local` and
`region_cached`.

Measuring effects of hardware-aware programming sometimes requires benchmarks to be multi-threaded,
which is not something you get out of the box with benchmark frameworks like [Criterion][criterion].
[`par_bench`][par_bench] extends Criterion with a simple harness for multithreaded benchmarking,
running your benchmark logic on a specific processor set obtained from `many_cpus`. It takes care
of all the dirty business involved in coordinating the threads and eliminating any test harness
overhead from the data.

```
Processor time statistics:

| Operation                   | Mean |
|-----------------------------|------|
| futures_oneshot_channel_mt  | 92ns |
| futures_oneshot_channel_st  | 76ns |
| local_once_event_managed    | 38ns |
| pooled_local_once_event_ptr | 27ns |
| pooled_local_once_event_rc  | 31ns |
| pooled_local_once_event_ref | 24ns |
```

When evaluating complex application logic, it can be important to take a holistic view - it does
not only matter how fast the benchmark logic runs but also how much energy (processor time) it
uses. Perhaps the code also runs logic on background threads or perhaps the code just blocks
some threads for a while on syscalls, costing wall clock time without costing processor time.
These are all factors that we must account for in complex scenarios. [`all_the_time`][all_the_time]
allows us to track the processor time spent by the process, in addition to the wall clock time.
It integrates well into Criterion and is natively supported by `par_bench`.

```
Allocation statistics:

| Operation                   | Bytes/iter | Allocations/iter | Peak bytes |
|-----------------------------|------------|------------------|------------|
| futures_oneshot_channel     |        128 |                1 |        128 |
| local_once_event_managed    |         48 |                1 |         48 |
| pooled_local_once_event_ptr |          0 |                0 |          0 |
| pooled_local_once_event_rc  |          0 |                0 |          0 |
| pooled_local_once_event_ref |          0 |                0 |          0 |
```

Memory allocation is the root of all evil. The simplest and most effective way to make a typical
application faster is to eliminate memory allocations from it - this can often multiply performance
several times. Before we can eliminate, we need to measure. [`alloc_tracker`][alloc_tracker] gives
us the ability to measure exactly how much heap memory is allocated by a particular piece of code.
It integrates well into Criterion and is natively supported by `par_bench`.

Once we have knowledge of how much memory we are allocating, we can start making a difference. The
simplest way is to change the algorithms so no memory allocations are necessary but sometimes that
is impractical. Nevertheless, the global Rust memory allocator (whichever one might be used) is a
general-purpose mechanism and it pays a price in performance for that generality. If we are
allocating a large number of objects of specific sizes, we can benefit from special-purpose
allocators that keep the memory around for reuse, so the next allocation is simple and fast.

While allocator APIs are still an unstable Rust feature, object pools provide stable alternatives.
[`plurality`][plurality] offers `Pool<T>` for one concrete type and `MultiPool` for heterogeneous
types, retaining storage for per-object slot reuse. [`infinity_pool`][infinity_pool] receives
maintenance only and remains available where its raw manual-lifetime handles or macro-generated
trait-object casting are required. Special-purpose pools can surpass the efficiency of the global
memory allocator under many conditions. Your mileage may vary - measure 100 times, cut 10 times.

A surprising source of memory allocations in high-performance code can be signaling. We are used
to thinking of oneshot channels as cheap and efficient things and while this is true, they are
still built upon shared memory allocated from the heap. Every signaling channel you create is a
heap allocation and they can add up fast! [`events_once`][events_once] provides you with pooled
signaling channels that reuse memory allocations, as well as providing single-threaded and
unsafe-code-managed events for lower overhead in specialized scenarios.

```
bagels_cooked_weight_grams: 2300; sum 744000; mean 323
value <=    0 [    0 ]: 
value <=  100 [    0 ]: 
value <=  200 [ 1300 ]: ∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎
value <=  300 [    0 ]: 
value <=  400 [    0 ]: 
value <=  500 [    0 ]: 
value <=  600 [ 1000 ]: ∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎∎
value <=  700 [    0 ]: 
value <=  800 [    0 ]: 
value <=  900 [    0 ]: 
value <= 1000 [    0 ]: 
value <= +inf [    0 ]: 
```

It is easy to think that performance and efficiency has been achieved once the benchmarks look good. 
Yet time makes fools of us all! Real-world data often shows surprising behaviors - where we thought
we would spawn 500 tasks, a surprise implementation detail from the HTTP stack may end up spawning
500 million! Benchmarks and belief is not enough. [`nm`][nm] provides a very high performance
and minimal metrics framework suitable for taking millions of measurements per second. Only with
the real data can we be assured that we achieve real performance. [`nm_otel`][nm_otel] bridges
nm metrics to OpenTelemetry for export to any compatible backend.

Many attempts to instrument high-performance logic are self-defeating because few people expect
that time itself is slow. Measuring the time, that is! `Instant::now()` is a remarkably slow
operation - never use this in high-performance code, as merely capturing the timestamp can massively
degrade performance. Accurate timing information can only ever be measured for large batches of
iterations so that one measurement span covers 100+ milliseconds of work. For situations where you
can sacrifice precision but still want satisfactory performance, [`fast_time`][fast_time] provides
a clock that is much cheaper to query. It will not give you precise numbers but it is safe to
query tens of thousands of times per second.

A single benchmark run is only a snapshot. The numbers that matter emerge over time: a hot
path that slowly regresses across dozens of commits, or a "harmless" refactor that doubles the
allocation count. [`cargo-bench-history`][cargo_bench_history] maintains a long-lived history of
your benchmark results - from [Criterion][criterion], [`all_the_time`][all_the_time],
[`alloc_tracker`][alloc_tracker] and [Callgrind][gungraun] - and analyzes it to detect
regressions and improvements across commits, so a
performance change cannot slip by unnoticed between one release and the next.


# Extras

Auxiliary packages developed and published by this project:

* [`awaiter_set`][awaiter_set] - zero-allocation awaiter tracking for async synchronization primitives.
* [`cargo-detect-package`][cargo_detect_package] - cargo subcommand to detect which package is used based on a provided path and to run another subcommand on that package.
* [`cargo-freeze-deps`][cargo_freeze_deps] - cargo subcommand that freezes floating dependency versions in a `Cargo.toml` to their resolved literal values.
* [`cpulist`][cpulist] - utilities for parsing and emitting Linux cpulist strings, used by `many_cpus`.
* [`events`][events] - async manual-reset and auto-reset events for multi-use signaling.
* [`future_deque`][future_deque] - pool-backed deque collections for managing groups of futures with precise control over polling order and result retrieval.
* `new_zealand` - [utilities for working with non-zero integers][nonzero].

Packages present in the repo but not relevant to a general audience:

* `benchmarks` - random pile of benchmarks to explore relevant scenarios and guide Folo development.
* `cargo-bench-history-faker` - unsupported synthetic benchmark-output generator that validates `cargo-bench-history` end to end (in this repo and sibling repos); published as a convenience binary but has no stable API or CLI.
* `cargo-bench-history-stress` - on-demand stress harness that seeds a synthetic benchmark history and times `cargo-bench-history` analysis modes over it; not published.
* `folo_ffi` - utilities for working with FFI logic; exists for internal use in Folo packages; no stable API surface.
* `folo_utils` - utilities for internal use in Folo packages; exists for internal use in Folo packages; no stable API surface.
* `testing` - private helpers for testing and examples in Folo packages.
* `ui_tests` - compile-time UI tests for workspace packages; not published.
* Various `_impl` packages (and the `linked_macros`/`cbh_*` families) that exist only to separate public and private API surface for implementation purposes; do not reference them directly.

[all_the_time]: packages/all_the_time/README.md
[alloc_tracker]: packages/alloc_tracker/README.md
[awaiter_set]: packages/awaiter_set/README.md
[cargo_bench_history]: packages/cargo-bench-history/README.md
[cargo_detect_package]: packages/cargo-detect-package/README.md
[cargo_freeze_deps]: packages/cargo-freeze-deps/README.md
[cpulist]: packages/cpulist/README.md
[criterion]: https://bheisler.github.io/criterion.rs/book/criterion_rs.html
[events]: packages/events/README.md
[events_once]: packages/events_once/README.md
[fast_time]: packages/fast_time/README.md
[future_deque]: packages/future_deque/README.md
[gungraun]: https://crates.io/crates/gungraun
[infinity_pool]: packages/infinity_pool/README.md
[linked]: packages/linked/README.md
[many_cpus]: packages/many_cpus/README.md
[many_cpus_b]: packages/many_cpus_benchmarking/README.md
[nm]: packages/nm/README.md
[nm_otel]: packages/nm_otel/README.md
[nonzero]: https://github.com/rust-lang/rfcs/pull/3786
[numa]: https://www.kernel.org/doc/html/v4.18/vm/numa.html
[par_bench]: packages/par_bench/README.md
[plurality]: https://crates.io/crates/plurality
[region_cached]: packages/region_cached/README.md
[region_local]: packages/region_local/README.md
[structural_changes]: https://sander.saares.eu/2025/03/31/structural-changes-for-48-throughput-in-a-rust-web-service/
[vicinal]: packages/vicinal/README.md

# Development environment setup

See [DEVELOPMENT.md](DEVELOPMENT.md).

# Quality assurance

This project aims for high quality standards:

✅ **Comprehensive testing** - All packages tested with extensive unit tests, integration tests, and doctests  
✅ **Miri validation** - All packages pass strict Rust memory safety validation via Miri  
✅ **Mutation testing** - Code quality verified through comprehensive mutation testing with `cargo-mutants`  
✅ **High test coverage** - Test coverage measured and maintained via `cargo-llvm-cov`  
✅ **Wall-clock benchmarks** - Hot paths covered by [Criterion][criterion] benchmarks (often multi-threaded via `par_bench`) to detect real-world performance changes  
✅ **Callgrind benchmarks** - Selected hot paths also covered by Valgrind/Callgrind-based one-shot benchmarks for deterministic, run-to-run-stable instruction counts and simulated cache behavior. See [docs/callgrind-benchmarks.md](docs/callgrind-benchmarks.md)  
✅ **Zero warnings policy** - All code must compile without any compiler or Clippy warnings  
✅ **Extensive Clippy rules** - 100+ custom Clippy lint rules enforced across the workspace  
✅ **Cross-platform validation** - All code tested on both Windows and Linux platforms  
✅ **Automated CI/CD** - Continuous integration runs full validation suite on every commit  
✅ **API documentation** - Complete API documentation with inline examples for all public APIs  
✅ **Dependency auditing** - Regular security audits of all dependencies via `cargo-audit`  
✅ **Semver compliance** - API changes validated for semantic versioning compliance via `cargo-semver-checks`