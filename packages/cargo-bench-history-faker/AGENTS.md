# Agent notes for cargo-bench-history-faker

## What this crate is

A test-support stand-in benchmark engine for `cargo-bench-history`. It is a
`publish = true` **lib+bin** crate but **unsupported**: the crate root is
`#![cfg_attr(docsrs, doc(hidden))]` and neither the library API nor the binary CLI carries a semver
contract. It is published only so sibling repositories can run the binary (via
`cargo binstall`) without vendoring it.

## Layout

* `src/writers.rs` — the public library. Each engine has a **pure value core**
  (builds the on-disk document without touching the filesystem, so it is usable
  from unit tests and Miri) plus a thin `write_*` filesystem wrapper. The bin and
  in-workspace fidelity tests both call these, so there is one source of truth for
  the output shape.
* `src/main.rs` — a thin CLI over `writers`. It only parses arguments and calls
  library functions; keep logic out of it.
* `src/locate.rs` — the `binary_path()` locator used by `cargo-bench-history`'s
  tests to spawn the real binary. Gated behind `#[cfg(any(test, feature =
  "private-test-util"))]`, so it never ships as public API. In-workspace
  dev-dependencies enable `private-test-util`; the crate's own tests reach it via
  the `test` arm.

## Invariants

* **The faker invents nothing — every emitted value comes from the CLI.** Each
  writer records exactly the identity and metrics its argument specifies; there are
  no hidden presets or defaults kept in lockstep with a committed fixture. A test
  that needs a specific identity spells it out in the argument (the
  `cbh_integration` harness owns the canonical `CALLGRIND_*` fragments its own
  assertions depend on). Emit only the fields the `cargo-bench-history` parser
  actually reads, so the output stays a faithful, self-contained stand-in for real
  engine output — and feeding it real data would work identically.
* **The CLI flag surface is a contract with `cargo-bench-history`'s tests.** Many
  call sites pass `--callgrind`/`--criterion`/`--alloc-tracker`/`--all-the-time`/
  `--exit-code`/`--fail-if-exists`/`--chdir`. Renaming or removing a flag breaks
  them; grep the whole repo before changing one.
* **The binary must also run as a cargo subcommand** (`cargo bench-history-faker`),
  which passes the subcommand name back as the first argument. `main` drops that
  leading token so the argument grammar is identical whether run directly or via
  cargo.
* **Version independently from the `cargo-bench-history` implementation family.**
  Do not add a synthetic exact dependency merely to group this test-support package
  with the CLI. The repository's release behavior is defined in
  [`docs/release-versioning.md`](../../docs/release-versioning.md).

## Building and testing

`cargo test -p cargo-bench-history-faker` builds and runs both the `writers` unit
tests and the `locate` tests (the `test` cfg activates the gated locator). No
feature flag is needed for the crate's own tests.
