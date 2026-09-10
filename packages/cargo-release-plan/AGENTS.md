# Agent notes for cargo-release-plan

## Git is a subprocess

Do not add `git2`, `gix`, or any other Git library. All Git access is
`std::process::Command` spawning `git`; the rationale and its trade-off are in
[docs/implementation.md](docs/implementation.md), "Subprocess boundaries".
Classification may also spawn `cargo metadata --no-deps`; verify-packaging may
spawn `cargo package --list`. Keep full resolution in explicit preparation and
preview, using `cargo update --offline --workspace`, never in report, check, or
application. Do not contact crates.io and do not compile as part of classification.

## Integration tests must be hermetic Git

Tests that create repositories must inject identity and throughput config on
every `git` invocation rather than relying on the user or host config:

* `user.email` / `user.name` pinned
* `commit.gpgsign=false`
* `gc.auto=0`

Use the helper in `tests/integration/fixture.rs`. Do not add real-time delays.

The integration suite is one test binary, `tests/integration/`, split into a
topic module per area of behavior over the shared `harness`. Add a new case to
the module that matches its subject rather than growing a single file.

## Modules own subjects, not categories

Put a new type, constant, or helper in the module that owns its subject, and
re-export it from `lib.rs` if it is public. Do not add a shared module for
"types", "constants", or "utilities"; there is deliberately none to add to.

A subject-owned module keeps an item next to the code that gives it meaning, so
a reader who has found the behavior has also found everything that defines it,
and a change to that behavior touches one module. A category module inverts
this: it gathers unrelated items that share only a syntactic kind, so it accretes
dependencies on every subject and every subject depends back on it.

## Miri

Tests that spawn `git` or `cargo`, or that touch the real filesystem beyond
in-memory data, must be `#[cfg_attr(miri, ignore)]` with a reason. Pure unit
tests (packaging rules, group verdicts, plan expansion, inherited-value
comparison, anchor resolution over a synthetic timeline) must keep running
under Miri.

## Version groups come from exact dependencies

Every Git-tracked workspace member participates in version grouping, including
`publish = false` helpers. A valid exact local dependency
`=major.minor.patch` creates an undirected edge; connected components with more
than one member are groups. Preserve raw manifest syntax through validation
because Cargo metadata normalizes away distinctions the contract rejects. Keep
release classification publishable-only and keep the broader Cargo-visible
member set for dependent requirement rewrites. See `docs/design.md`, "Version
groups", and `docs/implementation.md`, "Workspace snapshots".

## Release-process ownership

`docs/release-versioning.md`, `docs/git-workflow.md`, `RELEASING.md`, Standard
validation, and `just gh-release` own the surrounding release process. Do not
change them from this package.
