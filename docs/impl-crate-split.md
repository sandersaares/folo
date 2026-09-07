# The `_impl` crate split pattern

This document describes a structural convention we use to expose internal API
surface to benchmarks and tests **outside the crate boundary** without leaking
it to public consumers. The pattern is: factor the implementation of a public
crate `foo` into a companion crate `foo_impl`, and make `foo` a thin re-export
shell over `foo_impl`.

## Motivation

Rust's `pub`/`pub(crate)` visibility model has one major friction point for
crates that ship benchmarks or integration tests as separate compilation units:

- A benchmark in `packages/foo/benches/foo_internals.rs` is its own compilation
  unit outside the `foo` crate. It can only call into `pub` items.
- An integration test in `packages/foo/tests/` is also outside the crate.
- An external bench/test in a *different* package (e.g. `foo_otel` reaching
  into `foo`'s internals to drive a hot path directly) cannot reach
  `pub(crate)` items either.

The `_impl` crate split solves this by moving the implementation behind a
crate boundary that benches and tests can cross without making the same items
visible to end users.

## The split

For a published crate `foo`, the pattern is:

| Crate      | Role                                                                  |
| ---------- | --------------------------------------------------------------------- |
| `foo`      | Thin shell. All user-facing documentation lives here. Re-exports the public-API subset of items from `foo_impl`. |
| `foo_impl` | Hosts the entire implementation. `#![cfg_attr(docsrs, doc(hidden))]` crate root. Published only so `foo` can depend on it. Marked `[lib] doc = false`. |

The visibility model becomes:

- Items that are `pub` in `foo_impl` are reachable by anything that depends on
  `foo_impl` (such as `foo` itself, plus any in-workspace dev-dependency).
- The `foo` crate re-exports a *narrow, explicit list* of items via
  `pub use foo_impl::{...}` (no wildcards). Anything not on that list is not
  part of `foo`'s public API even though it is `pub` in `foo_impl`.
- The implementation uses plain `pub` for items benches/tests need to reach
  outside the crate boundary, without `#[cfg]` gates and without a Cargo
  feature to expose internal API for testing. Items that genuinely only need
  to be visible within the impl crate stay `pub(crate)` as usual.
- The narrow exception is the **internal-only test/bench API surface** that
  some crates expose behind a `private-test-util` Cargo feature: see
  "Internal-only test/bench helpers (`private-test-util`)" below.

`foo_impl` is published and reachable on crates.io, but documented as
off-limits via:

- `description = "Implementation crate for foo - do not reference directly"`
  in `Cargo.toml` (avoid backticks in the description — Cargo's description
  text does not render Markdown).
- `#![cfg_attr(docsrs, doc(hidden))]` at the crate root.
- `[lib] doc = false` in `Cargo.toml` (so `cargo doc` does not render it).
- A `README.md` that says "do not depend on this directly".
- A pointer back to `foo` in the crate-level rustdoc.

Functional Cargo features (those that exist for the crate's normal operation,
e.g. enabling an optional integration or alternative implementation) are
declared on `foo` and forwarded 1:1 to `foo_impl`. The shell crate is the
canonical advertised surface for every functional feature; `foo_impl`'s
feature table mirrors `foo`'s exactly.

## Versioning: `foo` and `foo_impl` are released in lockstep

`foo` and `foo_impl` are logically the same package. We do not maintain a
SemVer boundary between them, and we do not want downstream consumers to ever
end up with a mismatched pair (e.g. `foo 1.5.0` paired with `foo_impl 1.4.7`).
Both packages always carry the **same** version number, and that number bumps
together on every release. Two mechanisms enforce this:

1. **`[workspace.dependencies]` exact-pins `foo_impl`** using the `=X.Y.Z`
   constraint form:

   ```toml
   # workspace Cargo.toml
   [workspace.dependencies]
   foo = { version = "1.5.0", path = "packages/foo", default-features = false }
   foo_impl = { version = "=1.5.0", path = "packages/foo_impl", default-features = false }
   ```

   `foo/Cargo.toml` inherits the dependency in the usual workspace way:

   ```toml
   # packages/foo/Cargo.toml
   [dependencies]
   foo_impl = { workspace = true }
   ```

   The `=` constraint propagates through workspace inheritance, so downstream
   consumers of `foo` are locked to exactly the matching `foo_impl` version.

2. **`[workspace.metadata.release-plan.groups]` puts both packages in the same
   group**, which makes `cargo-release-plan` expand an increment across them:

   ```toml
   # Cargo.toml (workspace)
   [workspace.metadata.release-plan.groups]
   foo = ["foo", "foo_impl"]
   ```

   The `=` pin in `[workspace.dependencies]` is the Cargo-level complement:
   even if a future change dropped the group, the published `foo` would still
   refuse to install with a different `foo_impl` version. See
   [release-versioning.md](release-versioning.md).

When a pull request increments the pair, the `increment-versions` skill rewrites
the `=X.Y.Z` constraint with the new version. The pair is initialized at the
same version on the day of the split (typically the current version of `foo`),
and they bump together from there.

## When to apply this pattern

Apply this split when at least one of these holds:

- The crate has (or wants) benchmarks or external tests that need access to
  internal types, constructors, or invariants that should not be part of the
  public API.
- The crate currently uses a Cargo feature on its public API purely to
  expose internal items for in-workspace consumers (the historical
  workaround for the same problem).
- Another crate in the workspace needs to construct internal data structures
  for testing or benchmarking purposes.

Do **not** apply this split when:

- The "internal" API is actually a legitimate testing API for external users
  (e.g. `many_cpus::SystemHardware::fake()` is a documented testing aid that
  downstream crates use to write tests against `many_cpus`). Such APIs belong
  in the public crate, fully documented.
- The crate has no benchmarks or external tests at all.
- All internal items only need to be reached by the crate's own unit tests
  (`#[cfg(test)] mod tests` inside the same file already sees `pub(crate)`
  items).

## Private-use packages need no separate shell

The `foo`/`foo_impl` split exists to keep internal surface out of a *published,
public* API boundary while still letting out-of-crate benches and tests reach
it. A package that has no such boundary — a **private-use** package consumed
only by its siblings in this workspace and never offered as a stable surface
for external users — has nothing to protect, so there is nothing to split off.
**By default, treat such a package as an impl crate directly**, rather than
creating a companion `_impl` crate for it.

The common shape is a `*-core` package: the data-model and pure-logic internals
extracted out of an application, CLI, or other shell so the fast, I/O-free part
can be unit- and mutation-tested cheaply. The `-core` package *is* the impl
crate, and its sibling shell (the binary or facade that wraps it) is the
consumer. What drops away is the part of the pattern that exists to **wrap and
hide a companion crate**: the consumer is the application itself, not a thin
re-export library, so there is no narrow re-export list and no re-export smoke
test, and the `-core` package need not mark itself `#![cfg_attr(docsrs, doc(hidden))]` /
`[lib] doc = false` / "do not depend on this directly" (it has no public-shell
twin to defer documentation to). A *published* private-use impl crate may still
opt into those markings — to discourage external dependents on crates.io — as the
`cbh_*` family does; nothing *forces* it to. What carries over is:

- The internal-helper convention: a `private-test-util` Cargo feature (the
  `private-*` prefix in general) that gates
  `#[cfg(any(test, feature = "private-test-util"))]` helpers for in-workspace
  tests and benches, exactly as described in "Internal-only test/bench helpers"
  below. The sibling shell takes a regular dependency on the `-core` package
  *without* the feature, so end-user builds never compile those items; the
  shell's own tests activate it through a separate dev-dependency.
- Plain `pub` on internal items that out-of-crate benches/tests must reach,
  without a public feature gate, since the package is not a surface anyone is
  expected to depend on directly.
- Lockstep versioning, **if** the `-core` package is published and the shell
  pins it: the group in `[workspace.metadata.release-plan.groups]` plus the
  `=X.Y.Z` pin in `[workspace.dependencies]` keep the pair from ever drifting
  apart, exactly as for a `foo`/`foo_impl` pair (see "Versioning" above).

See the `cbh_*` crates under "Canonical examples" for a worked instance.

## Where each kind of artifact lives

The audience of the artifact is the deciding factor, not what API surface it
happens to need today.

| Artifact                          | Home crate | Rationale |
| --------------------------------- | ---------- | --------- |
| `src/**` (implementation)         | `foo_impl` | The whole point of the split. |
| Unit tests (`#[cfg(test)] mod tests` inside `src/**`) | `foo_impl` | They follow the code they test. |
| `benches/**`                      | `foo_impl` | Benches are a maintainer tool; whoever runs them already knows the impl crate exists. Hosting them in `foo_impl` means a future bench can reach for `private-test-util` items or any other `foo_impl`-internal `pub` item without having to be relocated first. |
| `tests/**` (integration tests)    | `foo_impl` | Same reasoning as benches: they are for maintainers, and proximity to `foo_impl` internals is occasionally needed. |
| `examples/**` (user-facing)       | `foo`      | Examples are a form of end-user documentation. They must compile against the same public API a user gets from `cargo add foo`. Keeping them in the public crate is what enforces that they cannot accidentally reach for internals. |
| Maintainer-only demo/dev binaries | `foo_impl/examples/` (optional) | If you do want a runnable internal demo (e.g. a load-generator, a profiling harness, a "wire up the internals to see what they do" app), put it under `foo_impl/examples/`. Treat it explicitly as "examples and dev apps for maintainers", a different category from end-user examples. |
| Re-export smoke test              | `foo`      | The one and only `tests/` file in `foo`. It exists specifically to assert that the explicit re-export list in `foo/src/lib.rs` keeps reaching every advertised item. See step 9 below. |

Two principles fall out of the table:

1. **The split is along an audience boundary, not a language-feature boundary.**
   `foo` is for users; `foo_impl` is for maintainers. Examples are user
   documentation, so they stay with the users. Benches and integration tests
   are maintainer tooling, so they stay with the maintainers.
2. **Benches and integration tests living in `foo_impl` should still prefer
   the public API.** Reach for `private-test-util` items or other
   `foo_impl`-internal items only when there is a specific reason —
   exercising a contract that the public API cannot express, or measuring a
   hot path that the public API gates behind a slower entry point. When a
   bench uses only `use foo::Event;` it is still measuring what the user
   pays; the impl crate is just a convenient host.

## Cargo features on the impl crate

Two distinct kinds of Cargo feature may live on `foo_impl`, and they are named
differently so they do not collide.

| Feature kind          | Naming             | Forwarded by `foo`? | Audience |
| --------------------- | ------------------ | ------------------- | -------- |
| Public functional or testing feature | `test-util`, `metrics`, `tokio`, etc. (whatever the public-API name is) | Yes, 1:1. `foo` declares `feature-name = ["foo_impl/feature-name"]`. | End users of `foo`. |
| Internal-only helpers | `private-test-util` (`private-*` prefix in general) | **No.** Never forwarded by `foo`. | In-workspace dev-dependencies only. |

The `private-*` prefix mirrors the workspace's existing `__private` /
`__private_*` "logically private but mechanically public" naming convention.

A package may have either kind, both, or neither. The rule is just that the
internal-only feature must be distinguishable by name from any public-API
feature so the shell crate can forward the right things without accidentally
exposing the wrong ones.

## Internal-only test/bench helpers (`private-test-util`)

Some items only make sense in a test or benchmark context — for example,
constructors that build a data structure from pre-assembled parts without
going through the normal entry path, accessors that expose internal state
for assertion, or methods that bypass an outer layer to drive a hot path
directly. Two competing concerns apply:

1. They must be reachable from external benches/tests in other workspace
   crates, which requires them to be `pub` in `foo_impl`.
2. They are pure dead weight in production builds and should not bloat the
   binary that real users ship.

The convention is to put these items directly on the type they relate to,
gated behind a `private-test-util` Cargo feature on `foo_impl`:

```rust
// in foo_impl/src/reports.rs

impl Report {
    /// Constructs a `Report` from pre-assembled parts without touching the
    /// global event registry.
    ///
    /// Intended for in-workspace tests and benchmarks.
    #[cfg(any(test, feature = "private-test-util"))]
    #[doc(hidden)]
    #[must_use]
    pub fn fake(events: Vec<EventMetrics>) -> Self {
        Self {
            events: events.into_boxed_slice(),
        }
    }
}
```

```toml
# foo_impl/Cargo.toml
[features]
# Internal-only API surface for in-workspace tests and benchmarks. Never
# forwarded by the `foo` shell crate, so end-user builds never see these
# items.
private-test-util = []
```

The `private-test-util` feature lives on `foo_impl` and is never declared on
`foo`. The `foo` shell crate's `Cargo.toml` declares
`foo_impl = { workspace = true }` without forwarding any private feature, so
end users running `cargo add foo` never get these items compiled into their
build. In-workspace consumers (`foo_otel` etc.) activate the feature on
their own `foo_impl` dev-dependency:

```toml
[dev-dependencies]
foo_impl = { path = "../foo_impl", features = ["private-test-util"] }
```

Cargo's feature unification then makes the gated items available across the
workspace's test/bench build graph. The
`cfg(any(test, feature = "private-test-util"))` form means `foo_impl`'s own
unit tests automatically see the items without anyone needing to opt the
workspace into the feature.

If `foo_impl`'s own benches use the feature-gated items, add
`required-features = ["private-test-util"]` to those bench entries so Cargo
skips them when the feature is off:

```toml
[[bench]]
name = "foo_internals"
harness = false
required-features = ["private-test-util"]
```

Call sites become natural and symmetric with the rest of the public API:

```rust
use foo::{EventMetrics, Histogram, Report};

let report = Report::fake(vec![
    EventMetrics::fake("event_a", 10, 100, None),
    EventMetrics::fake("event_b", 20, 200, Some(Histogram::fake(...))),
]);
```

Call sites name the types through `foo` (the public crate), not through
`foo_impl`. Gated items declared as inherent methods on a type are reachable
via any path that names the type — and `foo`'s re-exports expose the type,
even though `foo` does not advertise the gated method itself. The `foo_impl`
dev-dependency exists only to activate the `private-test-util` feature; no
`use foo_impl::*;` import is required in test code.

## Public testing APIs that the shell crate exposes (`test-util`)

A separate, larger pattern applies when a crate has a *documented* testing
aid intended for end users — for example, `many_cpus::fake::FakeHardware`,
which downstream crates use to write tests against `many_cpus`. This is a
public-API feature, gated by `test-util` (or whatever the public name is),
and it must be reachable when users enable it:

```toml
# foo/Cargo.toml
[features]
test-util = ["foo_impl/test-util"]   # forward 1:1

[dependencies]
foo_impl = { workspace = true }
```

```toml
# foo_impl/Cargo.toml
[features]
test-util         = []   # forwarded from `foo`
private-test-util = []   # internal only
```

A crate may have *both* features simultaneously. They are independent: the
public `test-util` items are advertised by `foo`'s documentation and become
part of `foo`'s SemVer surface, while `private-test-util` items remain
unreachable from `foo` and stay in the impl crate's internal scope.

## How to do the split

1. Create `packages/foo_impl/` with a `Cargo.toml` that mirrors `foo`'s former
   dependencies, plus `[lib] doc = false` and
   `description = "Implementation crate for foo - do not reference directly"`.
   Set the version equal to `foo`'s current version.

2. Add `foo_impl` to `[workspace.dependencies]` with the `=X.Y.Z` exact-version
   pin pointing at the version chosen in step 1:

   ```toml
   foo_impl = { version = "=X.Y.Z", path = "packages/foo_impl", default-features = false }
   ```

3. Move every source file under `packages/foo/src/`, plus every `benches/`
   and `tests/` file, into the matching folder under `packages/foo_impl/`.
   Leave `packages/foo/examples/` where it is — examples stay with the
   public crate (see the placement table above). Use `git mv` so history
   follows.

4. Replace `packages/foo/src/lib.rs` with a thin shell:
   - Preserve the entire crate-level `//!` documentation (this is what users
     will read on docs.rs).
   - Add an explicit `pub use foo_impl::{TypeA, TypeB, ...};` listing every
     item that should be part of the public API. **Do not** use a wildcard
     re-export here — the explicit list is the contract.

5. Replace `packages/foo/Cargo.toml` with a thin manifest that inherits
   `foo_impl` from the workspace:

   ```toml
   [dependencies]
   foo_impl = { workspace = true }
   ```

   Forward any functional features of `foo` to `foo_impl` 1:1 in `[features]`.

6. Add both packages to `[workspace.metadata.release-plan.groups]` in the root
   `Cargo.toml`:

   ```toml
   [workspace.metadata.release-plan.groups]
   foo = ["foo", "foo_impl"]
   ```

   This makes `cargo-release-plan` expand an increment across both packages, and
   the `increment-versions` skill rewrites the `=X.Y.Z` pin in
   `[workspace.dependencies]` so it always points at the matching `foo_impl`.

7. Inside `foo_impl/src/lib.rs`:
   - Put `#![cfg_attr(docsrs, doc(hidden))]` at the top.
   - Add a brief crate-level doc comment that points to `foo`.
   - Declare modules and re-export items just as the original `foo` lib.rs
     did.

8. Convert any internal-only API surface that already existed in `foo`
   (typically gated behind a workspace-internal `test-util` feature) so it
   lives in `foo_impl` instead. Add a `private-test-util = []` feature to
   `foo_impl/Cargo.toml`. Keep the
   `cfg(any(test, feature = "private-test-util"))` gate and the
   `#[doc(hidden)]` attribute on each item. See "Internal-only test/bench
   helpers (`private-test-util`)" above for the full rationale. If
   `foo_impl`'s own benches consume these items, set
   `required-features = ["private-test-util"]` on the affected `[[bench]]`
   entries.

   If `foo` *also* has a public-API testing feature (e.g. `many_cpus`'s
   `test-util` exposing `fake::FakeHardware` for downstream user tests),
   declare it on `foo_impl` under its own name and forward it 1:1 from `foo`
   — see "Public testing APIs that the shell crate exposes (`test-util`)"
   above.

9. Update in-workspace consumers (dev-dependencies that previously activated
   an internal feature on `foo` to reach these items) to instead declare
   `foo_impl = { path = "../foo_impl", features = ["private-test-util"] }`
   as a dev-dependency. Call sites continue to name types through `foo`
   (e.g. `use foo::Report; Report::fake(...)`); no `use foo_impl::*;` import
   is needed because gated inherent methods are reachable via any path that
   names the type, and `foo` re-exports the type.

10. Add `foo` itself as a dev-dependency of `foo_impl` so doctests written
    from the user's perspective (`use foo::Event;`) compile in
    `cargo test --doc -p foo_impl`. This is an allowed dev-dependency-only
    cycle.

11. Add a small re-export smoke test in `packages/foo/tests/foo_reexports.rs`
    that pulls every re-exported item by name and exercises just enough of it
    to confirm the contract. This catches regressions if a re-export is
    accidentally dropped from the list in step 4.

## Distinction from the `__private` module convention

This workspace already has a convention for exposing items to **proc macros**
within the same crate: place them under `#[doc(hidden)] pub mod __private { ... }`
or prefix methods with `__private_`. That convention solves a different
problem — macro-expanded code needs to reach items in the crate that defined
the macro — and the items live in the **same** crate, not a separate one.

The `_impl` crate split is a complementary mechanism that crosses the crate
boundary. The two patterns coexist:

- Use `__private` / `__private_` for items that need to be visible to a
  proc-macro in the same crate.
- Use the `_impl` split for items that need to be visible to **external**
  benchmarks, tests, or other in-workspace crates.

## Canonical examples

The full `foo`/`foo_impl` split has been applied to three crate pairs in this
workspace. A fourth example shows the simplified, shell-less form private-use
packages take.

### `nm` / `nm_impl`

The original worked example. Concrete files to study:

- `packages/nm/Cargo.toml` — thin shell manifest with
  `nm_impl = { workspace = true }`.
- `packages/nm/src/lib.rs` — preserved crate docs + explicit re-export list.
- `packages/nm/tests/nm_reexports.rs` — re-export smoke test.
- `packages/nm_impl/Cargo.toml` — impl manifest with `[lib] doc = false`, the
  `private-test-util` feature for internal helpers, and the `nm` dev-dep for
  the doctest cycle.
- `packages/nm_impl/src/lib.rs` — `#![cfg_attr(docsrs, doc(hidden))]` root.
- `packages/nm_impl/src/reports.rs` —
  `#[cfg(any(test, feature = "private-test-util"))] #[doc(hidden)] pub fn fake(...)`
  constructors on `Report`, `EventMetrics`, and `Histogram`.
- `packages/nm_impl/README.md` — "do not depend on this directly" notice.
- `Cargo.toml` (workspace) — `nm = ["nm", "nm_impl"]` in
  `[workspace.metadata.release-plan.groups]`.

### `nm_otel` / `nm_otel_impl`

The second worked example. Concrete files to study:

- `packages/nm_otel/Cargo.toml` — thin shell manifest with
  `nm_otel_impl = { workspace = true }`.
- `packages/nm_otel/src/lib.rs` — preserved user-facing crate docs +
  explicit `pub use nm_otel_impl::{Publisher, PublisherBuilder};`.
- `packages/nm_otel/tests/nm_otel_reexports.rs` — re-export smoke test.
- `packages/nm_otel_impl/Cargo.toml` — impl manifest with `[lib] doc = false`,
  the `private-test-util` feature gating one-iteration publisher drivers and
  histogram delta test state, the `nm_otel` dev-dep for the doctest cycle, and
  the `nm_impl` dev-dep with
  `features = ["private-test-util"]` so the impl-hosted benches can build
  input reports via `Report::fake`. Both `[[bench]]` entries declare
  `required-features = ["private-test-util"]`.
- `packages/nm_otel_impl/src/lib.rs` — `#![cfg_attr(docsrs, doc(hidden))]` root that re-exports
  the public-API subset for the shell crate plus feature-gated `EventState` for
  the alloc-tracking integration test.
- `packages/nm_otel_impl/README.md` — "do not depend on this directly" notice.
- `Cargo.toml` (workspace) — `nm_otel = ["nm_otel", "nm_otel_impl"]` in
  `[workspace.metadata.release-plan.groups]`.

### `many_cpus` / `many_cpus_impl`

The third worked example, and the first split that combines a forwarded
public `test-util` feature with internal benches that reach into the PAL.
Concrete files to study:

- `packages/many_cpus/Cargo.toml` — thin shell manifest with
  `many_cpus_impl = { workspace = true }` and
  `test-util = ["many_cpus_impl/test-util"]` forwarding the public testing
  aid to the impl crate.
- `packages/many_cpus/src/lib.rs` — preserved user-facing crate docs +
  explicit re-export list, plus
  `#[cfg(any(test, feature = "test-util"))] pub use many_cpus_impl::fake;`
  to forward the public testing module behind the same feature gate users
  enable on the shell.
- `packages/many_cpus/tests/many_cpus_reexports.rs` — re-export smoke test
  that covers both the unconditional public surface and the
  `test-util`-gated `fake` module.
- `packages/many_cpus_impl/Cargo.toml` — impl manifest with `[lib] doc = false`
  and the public-API `test-util` feature; no `private-test-util` feature is
  declared because the bench-only helpers do not need feature-gating (see
  next bullet).
- `packages/many_cpus_impl/src/pal/windows/platform.rs` —
  `pub fn current_thread_processors_for_bench`,
  `pub fn get_all_processors_for_bench`, and
  `pub fn affinity_mask_to_processor_ids_for_bench` wrappers on
  `BuildTargetPlatform`. These are plain `pub fn` rather than feature-gated,
  because the entire impl crate is `#![cfg_attr(docsrs, doc(hidden))]` and "do not depend on
  directly", so end users never see them; the `_for_bench` name suffix
  signals the intent at the call site.
- `packages/many_cpus_impl/benches/many_cpus_pal_windows.rs` — consumes those
  helpers via `use many_cpus_impl::pal::BUILD_TARGET_PLATFORM;`, bypassing
  the high-level public API to measure the PAL primitives directly.
- `packages/many_cpus_impl/src/lib.rs` — `#![cfg_attr(docsrs, doc(hidden))]` root that
  re-exports the public-API subset plus `pub mod pal;` (no `#[doc(hidden)]`
  needed on the module — the crate root already hides everything from
  docs.rs).
- `packages/many_cpus_impl/README.md` — "do not depend on this directly"
  notice.
- `Cargo.toml` (workspace) — `many_cpus = ["many_cpus", "many_cpus_impl"]` in
  `[workspace.metadata.release-plan.groups]`.

### `cbh_*` (private-use impl crates, no shell)

The `cargo-bench-history` CLI is split into a family of small `cbh_*` packages —
the pure, I/O-free `cbh_model`, `cbh_codec`, `cbh_stats`, `cbh_detect`, and
`cbh_render`, plus the extracted IO adapters and orchestration (`cbh_git`,
`cbh_storage`, `cbh_probe`, `cbh_engines`, …). Each is a private-use
package treated as an impl crate directly, with no companion `_impl` shell (see
"Private-use packages need no separate shell"): the CLI and its sibling impl
crates consume them directly, so there is nothing to wrap or hide behind a thin
re-export twin.

Unlike the minimal private-use form, these crates *are* `#![cfg_attr(docsrs, doc(hidden))]` /
`[lib] doc = false` / "do not depend on this directly". They are published to
crates.io so they can be released in lockstep with the CLI, and the doc-hidden
marking discourages external dependents even though there is no re-export shell
deferring their documentation. Concrete files to study:

- `packages/cbh_detect/Cargo.toml` — declares
  `private-test-util = ["dep:thread_aware"]` and `[lib] doc = false`.
- `packages/cbh_detect/src/lib.rs` — `#![cfg_attr(docsrs, doc(hidden))]` root that re-exports
  every type flat from the crate root.
- `packages/cbh_detect/src/testing.rs` — the `synchronous_spawner` helper,
  gated `#[cfg(feature = "private-test-util")]` (feature-only rather than the
  `any(test, …)` form used elsewhere, because the helper pulls in the optional
  `thread_aware` dependency that only the feature enables; the crate's own test
  that uses it is likewise feature-gated), that in-workspace tests inject in
  place of the production Tokio spawner.
- `packages/cargo-bench-history/Cargo.toml` — the consuming CLI (a binary, not a
  thin re-export). Its regular `[dependencies]` omit every `private-test-util`
  feature, so the test-only surface never reaches production builds; separate
  `[dev-dependencies]` entries with `features = ["private-test-util"]` (on
  `cbh_git`, `cbh_storage`, `cbh_diag`) activate it for the shell's own
  tests.
- `Cargo.toml` (workspace) — the lockstep machinery applies to every crate in
  the family: each `cbh_*` package is pinned with an exact `=` version in
  `[workspace.dependencies]` and shares the `cargo-bench-history` group in
  `[workspace.metadata.release-plan.groups]` with the CLI, so the published set
  can never drift to mismatched versions.
