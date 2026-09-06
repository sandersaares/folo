# Dependency management

This chapter covers how dependencies are declared and managed in the `Cargo.toml`
manifests across the workspace.

## Ordering

Dependencies are sorted alphabetically by name of the package.

## Default features

Do not define default features in `Cargo.toml` unless there is a specific,
justified reason to do so. Features should be opt-in, not opt-out.

## Dev-dependencies within workspace packages must be path dependencies

Do not use `version = "1.2.3"` or `workspace = true` when adding a package from
the same workspace as a dev-dependency. Within the same workspace, dev-dependencies
must always be `path = "../foo"` style path-references.

## Lockfile

The committed `Cargo.lock` should track the latest compatible versions of all
dependencies. Refreshing it with `cargo generate-lockfile` is encouraged, and the
incidental transitive-dependency updates this pulls in are welcome — keep them
rather than reverting to a narrower set of changes.

## Intra-workspace requirements name the declared version

Every dependency on another package in this workspace declares the exact version
that package currently declares — `version = "1.2.3"` when it is at `1.2.3`, not
`version = "1.0.0"` merely because that requirement would still admit `1.2.3`.

This keeps the released manifest describing the combination the workspace
actually built and tested. A wider requirement lets a consumer resolve a pairing
that never ran here, and it makes release reasoning depend on requirement
arithmetic rather than on the declared versions alone.

`cargo release-plan check` fails on a requirement that has drifted, and
`cargo release-plan apply` rewrites requirements to follow every version it
moves, so the invariant is maintained rather than remembered. The consequence is
that incrementing a package also increments its in-workspace dependents: the
rewrite changes their published manifests, which is a released-content change.

Path-only dev-dependencies are exempt because Cargo strips them when packaging,
so they never reach a published manifest.

## Version groups and exact-pin cross-references

Some packages are logically one package split into multiple crates for
cargotechnical reasons (see [impl-crate-split.md](impl-crate-split.md)). Examples:
`linked` + `linked_macros` + `linked_macros_impl`, `many_cpus` + `many_cpus_impl`,
`nm` + `nm_impl`, `nm_otel` + `nm_otel_impl`, and `cargo-bench-history` +
its `cbh_*` implementation crates.

Such crates must always be released at the same version. Enforce this in two
places, and keep both in sync:

* **`release-plz.toml`** — give every crate in the set the same `version_group`
  so release-plz bumps them together.
* **The workspace `Cargo.toml`** — the public crate is referenced by a plain
  requirement, but every *internal* crate it depends on (the `_impl` / `_core` /
  macro crates) is referenced with an **exact `=` pin**
  (`version = "=1.2.3"`). The exact pin means a downstream consumer of the public
  crate can never resolve a mismatched version of its internal companion.

Both forms name the version the target declares, as every intra-workspace
requirement does; the `=` is what additionally forbids a consumer from resolving
a later compatible release of the internal crate.

For example, `many_cpus` is referenced as `version = "2.4.9"` while
`many_cpus_impl` is referenced as `version = "=2.4.9"`; likewise each `cbh_*`
implementation crate is referenced with an exact `=` pin because
`cargo-bench-history` depends on it.

## Public dependencies

A dependency is **public** when this crate's own public API exposes types from
it, which is what a `_impl` crate re-export does. The
`[package.metadata.cargo_check_external_types] allowed_external_types` list is
the declaration of record for that: it names every type outside the crate that
its public API may expose, and `just check-external-types` fails when the API
exposes one the list omits. See [external-types.md](external-types.md).

That list therefore decides more than the external-types check. A public
dependency's compatibility is part of this crate's contract, so when a public
dependency releases a semver-incompatible version this crate must release one
too — a consumer holding the older dependency can no longer hand its types
across. `cargo release-plan check` enforces this, so keeping the allow-list
accurate keeps the release decision accurate.

