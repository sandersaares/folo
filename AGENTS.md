# Workspace conventions

This file is the table of contents for the workspace-wide conventions of this
repository. The actual conventions live in topical chapter files under `docs/`
— this file's job is to tell you which chapter to open for any given task.

## How to use this file

1. Identify what kind of work you are about to do (write a test? add an `unsafe`
   block? tweak a benchmark? refactor a module?).
2. Find the matching entry in the chapter list below and open the linked file.

## Meta-principles

A few principles apply to *every* change and are short enough to live here
directly:

* **Zero warnings allowed.** Fix all warnings that validation generates. Never
  silence a warning without a `reason` field justifying it.
* **Keep the house in order.** Do not only focus on the immediate task at hand
  but also consider how it affects the codebase around it. If the change you
  are working on affords more simplicity, better organization or greater reuse,
  take action to perform the refactoring needed to achieve that. If the change
  you are working on makes some logic or tests obsolete, clean up.
* **Adjust patterns, fix entire classes of problems.** If you are asked to do a
  thing to one instance of a problem in a file, check for other instances. You
  must solve the entire class of problems at once, not expect each instance to
  be pointed out to you in instructions.
* **Treat automation as maintained code.** Prefer nonpublished Rust utilities for
  automation logic; use PowerShell when Rust execution is impractical in the
  invoking environment. Every script must explain its purpose, callers and
  workflow role in inline comments. Explain non-obvious decisions and link
  design-bearing workflow steps to the relevant documentation chapter. Follow
  [docs/build-and-tooling.md](docs/build-and-tooling.md#automation-language-and-boundaries).
* **Do not execute `just gh-release`** — it performs real crates.io publishes and is
  a CI-only entry point (driven by the release workflow); never run it manually.
* **Make version decisions reviewable in the PR.** Run `increment-versions` without a
  separate approval gate; human review of the complete PR is the approval step.
  Keep a **Version/release plan** section current with every expanded-plan package/group,
  previous and proposed versions, change levels, and substantive reasons, including
  dependent/group movements; explicitly state when there are no released-content or
  version changes. Keep `[Copilot speaking]` first in agent-authored PR bodies.
  Follow [docs/git-workflow.md](docs/git-workflow.md#versionrelease-plan-section) and
  [docs/release-versioning.md](docs/release-versioning.md#on-a-pull-request).
* **Check for a package-local `AGENTS.md`** before doing nontrivial work in a
  specific crate (e.g. `packages/events_once/AGENTS.md`). Package-local
  guidance refines and sometimes overrides the workspace-wide rules.
* **`AGENTS.md` files are agent instructions, not design or implementation
  docs.** A package-local `AGENTS.md` holds only actionable guidance for working
  in that package — conventions to follow, gotchas, invariants to preserve, how
  to run and test. It must **not** duplicate design or implementation
  documentation or keep a decision/change history. User-visible behavior and its
  tenets belong in the owning package's `docs/design.md`; internal architecture,
  ownership boundaries, and implementation tenets belong in the package's
  `docs/implementation.md`. If a fact is trivia that does not change how an agent
  works, leave it out. Point at the appropriate document instead of repeating it.
  The private `cbh_*` crates are implementation partitions of
  `cargo-bench-history`: their user-visible behavioral contracts belong in the
  parent application's design and API documentation, while partition-local
  documentation covers only implementation ownership and boundaries and links to
  the parent.

## Chapters

### [docs/build-and-tooling.md](docs/build-and-tooling.md)

Day-to-day mechanics: `just` commands, building, testing, validation, the
Windows + WSL multiplatform workflow, Rust automation utilities, justified
PowerShell boundaries, script documentation and rules for `*.just` recipes.

**Open this when**: running any build/test/lint/docs command; validating a
change; writing automation logic, a script or a `[script]` block in a justfile;
searching the workspace; running a command on some "other" operating system
from the one you are on.

### [docs/cargo-delta.md](docs/cargo-delta.md)

The `cargo-delta` workflow for validating only the packages affected by a
feature branch's diff (instead of the whole workspace). Only used in GitHub
workflows, though technically it also works on local PC (but we typically
already know what our changes affect, so do not need it on local PC).

**Open this when**: configuring or debugging `delta.toml` or the delta-build
GitHub workflow integration.

### [docs/code-style.md](docs/code-style.md)

General Rust style conventions: 100-column lines, imports, naming, comments,
language, whitespace, lint suppressions, closure-clone scoping, variable
shadowing, named constants, wildcard re-exports, YAML formatting, `println!`
rules.

**Open this when**: writing any Rust code; reviewing a diff for style;
resolving a Clippy lint where multiple valid forms exist; naming a constant,
builder method, or variable.

### [docs/design.md](docs/design.md)

Which packages own design documentation, where it lives (package-level
`docs/design.md` and optional component files), what it should contain
(user-visible behavior and design tenets), and what to keep out of it
(implementation detail, listings/catalogues, changelogs, decision logs).

**Open this when**: writing or maintaining design documentation; changing
user-visible behavior or its design tenets.

### [docs/implementation.md](docs/implementation.md)

The implementation-guide requirement for every package, including private
implementation crates; how owning packages link their component guides into one
architecture; and what belongs in implementation documentation rather than
behavioral design documentation.

**Open this when**: writing or maintaining implementation documentation; changing
internal architecture, package ownership boundaries, or implementation tenets.

### [docs/file-organization.md](docs/file-organization.md)

How files, modules, and visibility are structured: small files, flat public
APIs, `lib.rs`/`mod.rs` discipline, file contents flow, `pub(crate)`, and the
`__private` macro-visibility pattern.

**Open this when**: creating, moving, or splitting files/modules/packages;
choosing a visibility modifier; exposing internals to a published macro.

### [docs/dependencies.md](docs/dependencies.md)

How dependencies are declared in `Cargo.toml`: alphabetical ordering, the
no-default-features policy, and the rule that intra-workspace dev-dependencies
must be `path = "../foo"` references.

**Open this when**: adding, removing, or reordering a dependency in a
`Cargo.toml`; taking a dev-dependency on another workspace crate; deciding
whether to enable default features.

### [docs/packaging.md](docs/packaging.md)

What a published crate archive contains: the `include` allow-list every
publishable package declares, the compile-time inputs that justify shipping a
file outside `src/`, and the inputs Cargo adds regardless.

**Open this when**: adding a publishable package; changing what a package ships;
embedding a file into the crate at compile time; wondering whether a README,
diagram, or design document belongs in the archive.

### [docs/feature-flags.md](docs/feature-flags.md)

Conditional compilation: gating test-only code with `#[cfg(test)]`, gating
optional functionality behind Cargo features, and enabling feature-gated code
(and its dependencies) in `test` builds via `#[cfg(any(test, feature = "..."))]`.

**Open this when**: marking code `#[cfg(test)]`; adding or consuming a Cargo
feature; deciding how a feature gate should behave under `cargo test`.

### [docs/api-documentation.md](docs/api-documentation.md)

How to write `///` and `//!` comments, README examples, Mermaid diagrams,
feature-gated documentation, and the `#[cfg_attr(..., doc = "...")]` pattern
for intra-doc links to feature-gated items.

**Open this when**: writing or editing API documentation; updating a
`README.md`; adding a diagram; rustdoc fails with `unresolved link` because a
linked item exists only under a non-default feature; the
`docs-default-features` validation step flags a broken link.

### [docs/external-types.md](docs/external-types.md)

The external-types check: a validation gate (`just check-external-types`, backed
by `cargo-check-external-types` on a dedicated pinned nightly) that fails when a
library's public API exposes an external type absent from that crate's
`[package.metadata.cargo_check_external_types]` allow-list. Covers the
catch-accidents intent, the glob-the-internal/list-the-external convention,
canonical partition paths, and the separate `RUST_NIGHTLY_EXTERNAL_TYPES` pin.

**Open this when**: a PR fails the external-types check; intentionally exposing an
external or cross-crate first-party type in a public API; adding a dependency
whose types appear in public signatures; adding a new library crate; bumping the
tool or its nightly pin.

### [docs/examples.md](docs/examples.md)

Conventions for inline doctest examples and stand-alone `examples/` binaries:
separating scenarios into distinct blocks or functions, the
`Box::pin` vs. `pin!` rule, and the production-code-quality expectation.

**Open this when**: writing or editing an example (inline or stand-alone);
structuring a multi-scenario example.

### [docs/testing.md](docs/testing.md)

The full testing playbook: panic/error assertions, the ban on real-time delays
and on `parking_lot`, watchdogs, the threshold-based-mutation anti-pattern,
Miri compatibility, multithreaded synchronization tests, `static_assertions`
for trait contracts, mutation-testing skip criteria, UI tests, and coverage
exclusion. Follow its Miri workload budget: shrink tests that take more than
10 seconds, preserve representative interpreter coverage, and justify native-only
scale tests rather than raising the runner deadline.

**Open this when**: writing or modifying a test (unit, integration, doctest);
hitting a hang, slow Miri test, Miri failure, or uncaught mutation; asserting that
a panic or error occurs; marking code as not-relevant-for-coverage; discovering a
flaky test.

### [docs/benchmarks.md](docs/benchmarks.md)

Criterion benchmark design (single-threaded by default, elementary operations,
`black_box`) plus the `Box::pin` → `pin!` exception on the measured path, and
a pointer to the Callgrind chapter. Cross-links to `naming.md`.

**Open this when**: adding or modifying any file in `packages/<pkg>/benches/`;
deciding how to pin a future inside an `iter` closure.

### [docs/callgrind-benchmarks.md](docs/callgrind-benchmarks.md)

Deep reference for Callgrind / Gungraun instruction-count benchmarks: which
operations to cover, scenario selection, the bench file template, Cargo.toml
setup, Gungraun syntax gotchas, the Criterion-pairing convention, and how to
interpret results.

**Open this when**: adding a `*_cg.rs` benchmark file or deciding whether a hot
path warrants Callgrind coverage.

### [docs/naming.md](docs/naming.md)

Naming conventions for benchmark files, Criterion groups, and Callgrind
identifiers — the rules that keep wall-clock and instruction-count benchmarks
in lockstep and prevent name collisions in `target/.../deps/`.

**Open this when**: naming a new benchmark file, group, or function; pairing a
Callgrind file with its Criterion counterpart.

### [docs/error-handling.md](docs/error-handling.md)

The `unwrap()` / `expect()` rules in both test and production code, checked
arithmetic, `Drop`-time invariant checks with `thread::panicking()` guards,
the `NonZero<usize>` preference for non-zero numeric parameters, and how `ohno`
error boundaries use private condition leaves and public aggregates for semantic
library operations, foreign errors for mechanism-only boundaries, and
application-level `AppError` (supported decision queries, constructor visibility,
`#[display]` scoping, and which derives survive).

**Open this when**: choosing between `unwrap`, `expect`, `?`, or explicit
error handling; auditing arithmetic that could overflow; writing a `Drop` impl
that validates invariants; designing a numeric parameter that must be
non-zero; adding or reshaping any error type.

### [docs/unsafe-code.md](docs/unsafe-code.md)

Safety-comment conventions for `unsafe` blocks: the validity + aliasing
requirements when creating references from raw pointers, where safety comments
go, and the one-unsafe-call-per-block rule.

**Open this when**: writing or auditing an `unsafe` block; constructing a
reference from a raw pointer; writing a safety comment.

### [docs/unwind-safety.md](docs/unwind-safety.md)

The `UnwindSafe`/`RefUnwindSafe` contracts: public API types must be unwind-safe
(verified with `static_assertions`), the exception for types guarding user data,
the `PhantomData<Cell<()>>` `!Sync` marker pattern and its ref-unwind-safety
side-effect, manual `Send`/`Sync` impls and the `'static`-bound rustc bug, and
`ohno::error`-generated error types.

**Open this when**: adding or auditing an `impl UnwindSafe`, `impl RefUnwindSafe`,
`impl Send`, or `impl Sync`; deciding whether a type may cross a `catch_unwind()`
boundary; picking the right `PhantomData<...>` marker; a `static_assertions`
unwind-safety assertion fails.

### [docs/performance.md](docs/performance.md)

Workspace-wide performance principles: when to add `#[inline]`, the bias
toward surgical interventions over architectural rewrites, preserving
defensive runtime checks, staying idiomatic Rust, deprioritizing
first-insert/teardown optimizations, the no-allocation-on-the-hot-path
reminder, and the rule on justifying deviations from standard ecosystem
patterns.

**Open this when**: considering an `#[inline]` annotation; proposing or
reviewing a performance optimization PR or issue; tempted to reach for a
hand-rolled construct instead of an ecosystem default.

### [docs/pal.md](docs/pal.md)

The platform abstraction layer (PAL) pattern: trait-based abstraction, Windows
+ Linux + mock implementations, and the facade pass-through layer used by
packages like `many_cpus`.

**Open this when**: working in a package that has a `pal/`, `facade/`, or
`mocks.rs` layout; adding, changing, or removing a PAL abstraction.

### [docs/callback-safety.md](docs/callback-safety.md)

The reentrancy / callback-safety rules: no callbacks under borrows of shared
state, no `mem::take`/`swap`/`replace` of shared state when a callback can
re-enter, and the `# Reentrancy` documentation requirement on public async
primitives.

**Open this when**: writing or modifying code that invokes user-supplied
callbacks (`Waker::wake`, `Drop` impls, observer closures, stored `FnOnce`
values); adding a new async primitive that needs a reentrancy contract.

### [docs/impl-crate-split.md](docs/impl-crate-split.md)

The `_impl` crate split pattern for exposing internal surface to in-workspace
benches and tests: when to apply it, the `private-test-util` Cargo feature
naming rule, doctest-cycle dev-dependencies, lockstep versioning, how a
private-use `*-core` package acts as an impl crate with no separate shell, and
how the split differs from the in-crate `__private` macro-visibility convention.

**Open this when**: deciding whether a crate needs to be split into a shell +
`_impl` pair (or whether a private-use `-core` package can just be the impl
crate directly); configuring features on an `_impl` or `-core` crate; setting up
dev-dependencies for tests or benches that need internal surface.

### [docs/git-workflow.md](docs/git-workflow.md)

Conventions for PRs: version/release-plan presentation, using `--body-file` with
`gh pr create`, and replying to and resolving review comment threads.

**Open this when**: creating a pull request; addressing review comments.

### [docs/scheduled-validation.md](docs/scheduled-validation.md)

Scheduled checks and personal App remediation, including setup, recovery, and
the ready-PR lifecycle.

**Open this when**: working on scheduled checks, personal App remediation,
ready-PR lifecycle, or setup and recovery for that workflow.

### [docs/release-versioning.md](docs/release-versioning.md)

How crate version numbers are decided and enforced: a pull request that changes
released content increments the affected packages, `validate-versions` gates
merge, and the `increment-versions` skill applies the plan. Merge publishes.

**Open this when**: preparing a pull request that touches a published package;
deciding a version increment; debugging `validate-versions`, `cargo-release-plan`,
or the `increment-versions` skill.

### [docs/release-automation.md](docs/release-automation.md)

How crates.io publishing and `cargo-binstall` prebuilt binaries are automated from
CI on merge to `main`: the `release.yml` flow (publish via Trusted Publishing →
reconcile which binary-crate releases are missing per-target archives → matrix-build
+ upload checksummed archives → alert on failure), the asset-naming contract that
keeps the archive filenames and each crate's `[package.metadata.binstall]` block in
lockstep, the `release-plz.toml` configuration, and the
rate-limit/idempotency/self-healing-retry/new-crate-bootstrap handling.

**Open this when**: implementing or debugging automated releases, the
crates.io/OIDC publish flow, the prebuilt-binary matrix, or a `cargo binstall`
resolution failure; adding a new published binary crate.

### [docs/standalone-binaries.md](docs/standalone-binaries.md)

How standalone binaries (CLI tools, subcommands) are expected to behave: the
rule that verbose (`--verbose`) logging must be *explanatory* — stating the
inputs and reasoning behind each decision so the logic can be reconstructed from
the logs, never just announcing the conclusion.

**Open this when**: adding or editing verbose/diagnostic logging in a CLI tool or
other standalone binary; deciding what a `--verbose` note should say.

### [docs/feedback.md](docs/feedback.md)

An index of the *kinds* of review feedback that recur in this workspace (naming and
terminology consistency, justified `Option` fields, minimal public API surface,
`use`-over-long-paths, `expect` discipline, desired-state comments, realistic
READMEs, unit-vs-integration test placement, CLI conflict-is-an-error). Its job is
to let a reviewer or self-reviewing author anticipate and pre-empt these objections.

**Open this when**: reviewing a diff (your own or someone else's); about to request
review; deciding whether a naming, data-model, API-surface, or CLI choice will draw
the objections we keep making.
