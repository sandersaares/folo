# External type exposure

This chapter covers the external-types check: a validation gate that fails when a
library's public API exposes an *external* type that is not on that crate's
allow-list. It is driven by
[`cargo-check-external-types`](https://github.com/awslabs/cargo-check-external-types)
and run by `just check-external-types`.

## What problem it solves

An **external type** is any type in a crate's public API that is neither a
standard-library type (`std`, `core`, `alloc`) nor defined by the crate itself.
Exposing one in a public signature couples the crate's API to that other crate:
a caller must now name the foreign type, and a semver-breaking change in the
foreign crate becomes a semver-breaking change in ours.

The gate exists to **catch accidents**, not to forbid external types. A leaked
dependency type (an error type surfacing through `?`, a builder returning a
foreign struct, a `pub use` that re-exports more than intended) is easy to add
without noticing and hard to remove later without breaking callers. The check
fails the build when a crate exposes an external type its allow-list does not
cover, so a *new* external exposure is a deliberate, reviewed act rather than an
oversight.

This is an **additive allow policy, not an exact two-way baseline**. Adding an
uncovered external type fails; removing an exposure does not — it merely leaves a
now-unreferenced allow-list entry, which the tool reports as a harmless warning.
Unused entries cannot be rejected automatically because a legitimate
platform-specific entry is unreferenced on every other platform (see *Platform
coverage* below). Prune stale entries by hand when you remove an exposure.

Exposing an external type remains perfectly legitimate — the check only asks that
it be **acknowledged**.

## How a crate declares its allowed types

Each crate lists the external types it intentionally exposes in its `Cargo.toml`:

```toml
[package.metadata.cargo_check_external_types]
allowed_external_types = [
    "some_crate::module::SomeType",
    "another_crate::*",
]
```

Entries are `wildmatch` globs, where `*` matches any run of characters (including
path separators). A type that matches **no** entry fails the check; a type that
matches **two or more** entries is also an error, so keep globs from overlapping.

When you intentionally add an external type to a public API, add it here in the
same change. When you remove one, delete its entry so the allow-list does not rot.

### Granularity: glob the internal, list the external

The allow-lists follow one convention so the gate stays sensitive to accidents
while not nagging about intentional internal layering:

* A crate's **own** implementation or macro partition is allowed with a
  crate-level glob (`many_cpus_impl::*`, `nm_impl::*`, `cbh_*::*`,
  `linked_macros::*`). Re-exporting any type from your own partition is by-design
  layering (see [impl-crate-split.md](impl-crate-split.md)); a glob keeps a routine
  internal refactor from tripping the gate.
* Every **other** external type — a third-party ecosystem type, or a public type
  from a *different* first-party crate — is listed individually. Listing the exact
  types means that exposing a *new* type from an already-referenced crate still
  trips the check, which is the accident we want to catch.

### Canonical paths

A type is reported by the crate that **defines** it, not the crate you name to
reach it. Because a shell crate re-exports its partition's types, a first-party
type is reported by its partition path: `many_cpus`'s `SystemHardware` appears
everywhere as `many_cpus_impl::system_hardware::SystemHardware`. Allow-list
entries must use that canonical path.

Canonical paths are not a complete inventory of the types reachable through
each exported item. The checker
[does not recursively inspect external re-export internals](https://github.com/awslabs/cargo-check-external-types/blob/705a0941997ebeb3bfb1a7f14070ba342f79d879/src/visitor.rs#L202-L250),
so naming an external type does not necessarily name the other crates used by
that type's public methods. The release process therefore retains
[conservative breaking-change propagation](release-versioning.md#conservative-breaking-change-propagation)
through private implementation packages rather than treating a dependent's
allow-list as sufficient evidence to skip them.

## The dedicated nightly toolchain

`cargo-check-external-types` reads nightly rustdoc's unstable JSON output, which
pins an exact schema version. It therefore works only on the single nightly it was
built against — **not** the workspace's general `RUST_NIGHTLY`. That nightly is
pinned separately as `RUST_NIGHTLY_EXTERNAL_TYPES` in `constants.env` and must
equal the channel in the pinned tool build's `rust-toolchain.toml`. Bump the two
together; bumping either alone fails the check with a rustdoc schema mismatch.

The tool itself is pinned in `constants.env` as well
(`CARGO_CHECK_EXTERNAL_TYPES_GIT` / `CARGO_CHECK_EXTERNAL_TYPES_REV`) and installed
by `just install-tools`. See the comments in `constants.env` for why it is pinned
to a git commit rather than a crates.io release, and the condition for switching
back.

## Running the check

```
just check-external-types                        # every checkable crate
just package="many_cpus nm" check-external-types # only the named crates
```

The check runs per crate and lets the tool's `--skip-unsupported` flag pass over
crates it cannot document (proc-macro and binary-only). It is part of
`validate-local` and runs in CI as a delta-scoped job.

Warnings about types inside `#[doc(hidden)]` items are expected and do not fail
the check: rustdoc records those items in a way the tool cannot inspect.

Published crates whose entire surface is hidden on docs.rs must use
`#![cfg_attr(docsrs, doc(hidden))]`, not an unconditional `#![doc(hidden)]`.
The check deliberately omits the `docsrs` cfg so rustdoc JSON retains the public
surface for inspection. `[lib] doc = false` may still suppress ordinary direct
documentation builds for implementation crates.

## Platform coverage

A public API can differ by platform through cfg-gated items, so the check runs on
both a Linux and a Windows target — a Windows-only exposure (for example
`testing`'s `deranged::RangedU8`, gated on `cfg(windows)`) is only observed on
Windows, and a Unix-only one only on Linux. An allow-list entry that applies to
one platform is simply unreferenced on the other, which is why unused entries are
tolerated rather than rejected.

Two targets suffice for this workspace, not by coincidence but because of how
platform variation is written here: platform-specific dependencies are gated on
`cfg(windows)` or `cfg(unix)` (which Linux represents for macOS), the
platform-specific implementations sit behind a platform abstraction layer whose
public facade is identical on every target (see [pal.md](pal.md)), and no public
item is gated on `target_arch`. If that ever stops holding — a dependency or
public type gated specifically on `cfg(target_os = "macos")`, or an
architecture-gated public item — extend the check's CI matrix to cover that
target, because a Linux or Windows run cannot see it.
