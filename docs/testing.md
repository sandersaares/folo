# Testing

This chapter covers everything about writing and maintaining tests in this
workspace: panic and error assertions, synchronization, watchdogs, Miri
compatibility, mutation testing, coverage exclusions, and the policy on flaky
tests.

The `unwrap()` / `expect()` rule (both test and production sides) lives in
[`docs/error-handling.md`](error-handling.md).

## Testing for panics and errors

It is good to create tests that verify expected panics/errors are returned.
However, never check for a specific error/panic message — these are not part of
the public API and create fragile tests. Just verify that an error of an expected
type occurs or any panic occurs.

For tests that must verify a panic but also continue running afterwards (e.g. to
check that a data structure is still in a valid state), use
`testing::assert_panics(|| ...)` instead of hand-rolling
`catch_unwind(AssertUnwindSafe(...))`. When a canary substring check is warranted,
use `testing::assert_panics_with(|| ..., |message| assert!(message.contains("canary")))`.

### Do not check for specific panic or error messages

Tests that use `#[should_panic]` or use `Display` output of error types must not
check for specific panic or error messages — these messages are not an API
contract and may change at any time.

Exceptions where checking for a specific message is acceptable:

* **Canary substrings**: checking that a panic message contains a stable keyword
  (e.g. `#[should_panic(expected = "overflow")]`) when the keyword is inherent to
  the scenario and unlikely to be removed by refactoring.
* **Pass-through verification**: checking the exact panic message when the test
  verifies that a panic is forwarded without tampering (e.g. through `catch_unwind`
  + `resume_unwind`).

This only pertains to error messages — non-error outputs of `Display` should
still be tested.

## Delays are forbidden in test code

There should never be any delay/sleep on the successful path in tests — every
test must be near-instantaneous and time-based synchronization is forbidden.

To be clear, any form of "sleep" or "delay" that uses a real-time clock is
forbidden in test code. If the type can be made to use a `tick::Clock` then a
clock using a simulated time source may be used in test code (via
`tick::ClockControl`).

You may use events/signals for synchronization (e.g. `Barrier` or `events_once`
events or message channels), as long as there are no delays or wait-loops in the
test code itself.

## Tests must not hang

When there is a danger that a test may hang (e.g. it contains a `.wait()`,
`.recv()` or similar call), you must use `testing::with_watchdog(|| { ... })` to
wrap the test body with a timeout. Do not implement custom watchdog timers —
always use the existing watchdog from the `testing` package.

Use `testing::with_watchdog_phases()` when a test has several identifiable
blocking stages. Its initial label describes the work before the first report;
call `WatchdogPhaseReporter::report()` immediately before each later operation
that may block so a timeout identifies the latest stage. Keep
`testing::with_watchdog()` for tests where one timeout label is sufficient.

Watchdogs are automatically disabled during mutation testing (`MUTATION_TESTING=1`
environment variable). A mutation that causes a test to hang should be fixed by
either redesigning the test to catch the mutation without blocking (e.g. adding
`debug_assert!` checks) or by skipping the mutation if it is impractical to catch.

### Threshold-based mutation protections are an anti-pattern

When a mutation could make a test hang (for example, a mutation that turns an
iterator's `next()` into an infinite source), it is tempting to add a `.take(N)`
"safety bound" or a similar magic constant that caps consumption so the test
cannot hang. Do not do this. Such thresholds are fragile in the same way
time-based delays are fragile: they are easy to miss when tests evolve, and there
is no principled way to verify that the chosen value is the "right" threshold for
every future test scenario. The anti-pattern applies even when the threshold is
numeric rather than time-based.

Legitimate alternatives, in order of preference:

1. **Meaningful-iteration assertion.** Rewrite the test so it asserts on the
   actual produced values rather than just consuming them. Tools that naturally
   bound consumption while verifying values are ideal — for example,
   `Iterator::eq(expected_array)` calls `self.next()` at most `expected.len() + 1`
   times, returns `false` on the first value mismatch, and detects both wrong
   values and wrong length. A correct implementation matches the expected array;
   a mutation that yields wrong values or runs forever short-circuits on the
   first comparison. The bound is implicit in the scenario data (the expected
   length), not a hand-tuned safety margin.

2. **Skip the mutation** with `#[cfg_attr(test, mutants::skip)]` and a comment
   explaining why catching it is impractical (see "Mutation testing coverage and
   skipping mutations" below for the criteria).

## Flaky test discoveries are recorded as issues

If you stumble across a flaky test while working on something unrelated (for
example a CI failure that is not caused by your change, or a doctest that violates
the "no delays" rule above), file a GitHub issue so the discovery is not lost.
Use `gh issue create` with a clear title and a body that includes:

* The path and line range of the offending test.
* The failure mode (which assertion / message / scenario triggers it).
* A link to the run or PR where you noticed it.
* A suggested fix if one is obvious.

We do this for any flake — not just timing-related ones (e.g. order-sensitive,
environment-sensitive, machine-load-sensitive). Recording these accidental
discoveries lets us batch the cleanup later instead of losing the lead. Do not
silently fix the flake as part of an unrelated change — the issue lets us track
and prioritise it independently.

## Miri and platform compatibility

Tests that talk to the real operating system generally fail to execute under Miri.
This is fine and expected. However, a Miri test run must still succeed with a
clean result! If there are tests that cannot be executed under Miri, they should
be excluded via `#[cfg_attr(miri, ignore))]` (plus a comment justifying why it is
correct to exclude them).

Naturally, if it is possible to redesign a test so it does not rely on the
operating system, that is even better. However, this is not always possible.

Miri is too slow when running tests with large data sets (anything with 100s or
1000s of items). Exclude such tests from running under Miri.

Doctests are not executed under Miri. There is no need to make doctests
Miri-compatible.

## Testing atomic operations and custom synchronization

Any code that uses atomic operations, custom wakers, or other synchronization
primitives must include tests that exercise real multithreaded scenarios (e.g.
signaling completion from another thread via `events_once::Event`). These tests
should be Miri-compatible (no OS-specific calls) and verified via
`just package=<name> miri-harder`, which runs Miri with many random seeds
(`-Zmiri-many-seeds=..64`). The combination of true multithreaded tests and
miri-harder is highly effective at detecting data races, incorrect memory
orderings, and other concurrency bugs that are nearly impossible to catch with
single-threaded tests alone.

### parking_lot is forbidden — use std::sync primitives

Do not use `parking_lot::Mutex`, `parking_lot::RwLock`, or other primitives from
the `parking_lot` crate. They rely on OS-specific syscalls that Miri cannot model,
so introducing them breaks our Miri-based correctness validation. Use
`std::sync::Mutex`, `std::sync::RwLock`, and other std-provided synchronization
primitives instead — these are Miri-compatible.

This rule applies even when `parking_lot` would offer a measurable performance
benefit: Miri coverage is more valuable than the fast-path savings.

## Mutation testing coverage and skipping mutations

We expect all mutations to either be unviable or to be caught. Uncaught mutations
and mutants that time out are anomalies that must be corrected.

It is acceptable to skip mutations if they are impractical to test. Some
justifiable reasons are:

* Detecting the mutation requires real timing logic to be used. We intentionally
  do not permit any timing-dependent code in our test cases. However, if
  circumstances allow, we can use the `tick` crate with its simulated clock to
  create timing-dependent logic that can be tested without real time passing.
* Detecting the mutation requires detecting that a thing is not happening (e.g.
  detecting that an object is never dropped or that some code never executes).
  This can sometimes be impossible without relying on real-time timeouts (which
  would violate the above expectation).
* The mutation is in a defensive branch for defense in depth and can never be
  reached due to higher layers of the API preventing the situation from arising.
* The mutation is in trivial forwarder code (e.g. a facade that chooses between a
  real and mock implementation).

To skip a mutation, use the `#[cfg_attr(test, mutants::skip)]` style and leave a
comment to justify why we are skipping it.

Before skipping a mutation, consider how to catch it. Beyond simply improving test
coverage, the following techniques may help:

* Adding `debug_assert!()` statements to strategic places, verifying that logical
  invariants still hold.

## Test presence of traits via static assertions

Use `static_assertions::assert_impl_all!` and
`static_assertions::assert_not_impl_any!` to check for the presence or absence of
traits where the API documentation makes claims about them, for example in terms
of being thread-mobile (`Send + !Sync`), thread-safe (`Send + Sync`) or
single-threaded (neither).

If generic type parameters are involved, these assertions should use some
arbitrarily selected typical types that may be encountered in user code.

## UI tests

UI tests all go in a workspace-scoped `ui_tests` package due to technical
limitations. Follow inline documentation in this package to understand more.

Package dependencies of UI tests must be excluded from `udeps` scanner logic via
`Cargo.toml` in `ui_tests`. See existing examples in this file.

## Test coverage

In some circumstances, we may need to mark parts of code as intentionally not
covered by tests. For example:

* Tests themselves — the tooling measures test code as well as real code. We only
  care about real code, so all unit test modules and mock/fake test utilities
  (anything `#[cfg(test)]`) need to be excluded. Integration tests in `tests/`
  are automatically excluded, though — no need to worry about those.
* Defensive branches that can never be reached due to defense in depth layering.
* Code that is only ever executed in a const context, as const context is not
  covered in coverage measurements.
* When code has no API contract to test (e.g. `fmt::Debug` implementations which
  may contractually write anything).
* Facade types whose only purpose is to redirect calls to either a real or mock
  implementation — not worth testing.

To exclude code from coverage measurement, mark it with
`#[cfg_attr(coverage_nightly, coverage(off))]`. This also requires
`#![cfg_attr(coverage_nightly, feature(coverage_attribute))]` on the crate level.

Note that `coverage(off)` applies to entire functions, not to individual branches.
Defensive branches inside a function (e.g. `debug_assert!(false)` for structurally
unreachable states) cannot be excluded from coverage measurement. Accept these as
known gaps rather than trying to restructure the code to work around them.

When excluding code for any other reason than "it is test code", leave a comment
to explain why.
