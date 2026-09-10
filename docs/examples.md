# Examples

This chapter covers the conventions for the two kinds of examples we ship:
inline (doctest) examples in API documentation, and stand-alone example binaries
under `examples/`.

## Examples show good practices

Examples are production code. Use proper patterns and practices — examples are
not tests, where looser rules can be allowed.

This includes following the same Clippy and style conventions as ordinary
production code. The only documented exception is `examples/package_name_readme.rs`
files (see the API documentation chapter), which may relax Clippy rules to stay
short and simple.

## Keep execution fast

Stand-alone examples should complete within single-digit seconds, excluding
compilation. Demonstrate the operation with a small, bounded workload instead of
using the example as a performance-measurement run.

Criterion examples should call `configure_from_args()` so their default invocation
uses Criterion's single-iteration test mode. Actual benchmarking is an explicit
opt-in via `-- --bench`, not part of example smoke execution. Keep the measured
closure representative in either mode.

The [example runner](build-and-tooling.md#example-execution) handles shared Cargo
output-directory discovery outside the per-example runtime budget.

## Inline examples separate scenarios into separate code blocks

If you create inline (doctest) examples that showcase multiple
scenarios/variations, separate each variant into its own code block with a short
individual description instead of showing examples of multiple scenarios in one
code block.

## Stand-alone examples separate scenarios into functions

If you create stand-alone example files that showcase multiple
scenarios/variations, separate each variant into its own function instead of
having everything in a giant `main()` function.

## Avoid the `pin!` macro in tests and examples

This macro is special-purpose and not intended for general use. Instead, use
`Box::pin(value)` to pin a value in examples.

If there is some reason `Box::pin(value)` would not work, you can use
`std::pin::pin!(value)` as a last resort but leave a comment to justify why this
is the case.

Benchmarks are an exception: on the measured path, use `std::pin::pin!` to avoid
allocator overhead distorting the measurement. See the benchmarks chapter for
details.

## Hide async entrypoint

When an inline example uses `.await`, the async entrypoint/wrapper should not
appear in the rendered API documentation. See the API documentation chapter for
the canonical pattern.
