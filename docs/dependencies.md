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

Every versioned dependency in a publishable package on another workspace package
names the version that package currently declares — `version = "1.2.3"` when it
is at `1.2.3`, not `version = "1.0.0"` merely because that requirement would
still admit `1.2.3`. Exact requirements that declare a version group follow the
same numerical rule even when the declaring package is not publishable.

This constrains the version *number*, not the kind of requirement. Both kinds
appear, and which one to use is decided by the next chapter:

| Reference | Requirement | Admits |
| --------- | ----------- | ------ |
| Declares a shared version group | `version = "=1.2.3"` | that version only |
| Does not declare shared versioning | `version = "1.2.3"` | `1.2.3` and later compatible releases |

So the rule here is that the number is always the current one; the `=` is a
separate decision about whether later compatible releases may be resolved.

This keeps the released manifest describing the combination the workspace
actually built and tested. A wider *number* lets a consumer resolve a pairing
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

An exact requirement between workspace members declares that they share a version.
`cargo-release-plan` treats the exact-reference graph as undirected and derives a
version group from each connected component with multiple members. A chain of exact
references is sufficient: not every dependency between packages in the resulting
group must be exact. A compatible requirement may therefore connect packages that
also belong to one group through another path.

Group discovery covers tracked workspace members whether or not they are
publishable. It considers normal, build, and development dependencies, including
optional and inactive target-specific declarations. Aliases and inherited workspace
dependencies resolve to the actual member package. An unused
`[workspace.dependencies]` entry and a versionless path dependency create no edge.

The accepted exact form is a single `=major.minor.patch` comparator, with insignificant
surrounding and operator whitespace permitted. Partial, prerelease, build, and compound
exact requirements between workspace members are manifest errors. A valid exact pin
must name its target's current declared version; `cargo release-plan check` diagnoses a
stale pin and `apply` rewrites it.

Use exact requirements only for dependencies that are intended to declare shared
versioning. References into a group from an unrelated package ordinarily remain
compatible. Widening an exact pin changes grouping intent and must not be used merely
to make an unexpected group disappear.

For example, `many_cpus` exact-pins `many_cpus_impl`, so they form one group. The
unrelated crates that depend on `many_cpus` use compatible requirements. The
`cargo-bench-history` implementation packages form a larger connected component
through their exact dependency references.

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

Note that the list names each type's **defining crate**, not the dependency this
crate reaches it through. Where `foo` re-exports a type from `bar` that
re-exports it from `baz`, every crate along the chain lists `baz::Something`,
including the one that only ever names `foo` in its dependency table.

That indirection is why the release decision follows re-export declarations
rather than matching the list against the dependency table directly: a crate's
allow-list frequently names a crate it has no edge to. It also makes the
propagation self-consistent — because each crate in the chain must list
`baz::Something` to pass the external-types check, a breaking release of `baz`
reaches every one of them rather than stopping at the first.
