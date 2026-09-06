# Determining a change level

Use this guide for every publishable package in the release-plan report. The decision concerns
the complete released change since the package's version anchor, including changes already
pending release.

## Evidence to inspect

Read the package entry in `report.json`, its referenced diff when present, the package's
`Cargo.toml`, the workspace `Cargo.toml`, and the decisions for its dependencies. Direct
`[package]` and dependency-table edits appear in the package's released-content diff alongside
any other changed published files. Values inherited through `.workspace = true` instead
appear in the report's `changed` array with `source: "inherited"`, and locked dependency
identities appear with `source: "lockfile"`; neither has a package diff.

Evaluate every `source: "inherited"` entry for every package. A workspace-level edit affects
each package that inherits the edited field, so a package can require an increment on an
inherited value alone, without appearing in any original file diff. Read the package's own
`Cargo.toml` to establish which fields it inherits rather than assuming a workspace-wide
convention.

Evaluate every `source: "lockfile"` entry the same way. A package that publishes an executable
releases its resolved dependency closure, so a locked dependency change is a released-content
change even though it produces no package diff. Judge the consumer impact of the moved
dependency; it establishes at least `patch`.

A package that is already `pending-release` is judged by these same criteria. Its existing
version movement is retained, but it does not replace analysis of the accumulated changes: an
increment that no longer covers them is raised.

`cargo-semver-checks` detects part of the Rust API surface. Its per-package summary establishes
the floor described in the skill's decision stage. No summary means the tool could not
determine a required version increment; it does not establish that the package is compatible.

## Breaking

Choose `breaking` when existing consumers may need to change or may observe an incompatible
contract. Examine public API removal or reshaping, stricter input requirements, changed output
or persisted formats, incompatible command-line behavior, and changed semantic guarantees.

For a public package backed by implementation packages, judge the behavior and API exposed
through the public package. Internal handoff changes matter only when they alter that exposed
contract.

Feature gating participates in this judgement. Placing an existing API behind a Cargo feature is
breaking even when that feature is enabled by default, because a consumer building with default
features disabled loses the item. Removing a feature is breaking when it withdraws functionality
or public items a consumer could reach. Adding a new opt-in feature is not breaking; it is a
compatible capability.

A package also takes `breaking` when one of its **public dependencies** does, whatever its own
diff shows. The report marks such a dependency `"public": true`, meaning this package's public
API exposes types from it. An incompatible release of that dependency changes the identity of
those types for consumers: code holding the older dependency can no longer hand its values
across, so the exposure itself becomes incompatible even when the dependency's breaking change
touched nothing this package re-exports. Read the flag rather than judging the exposure from
source; it is derived from the `allowed_external_types` allow-list that `check-external-types`
verifies, and `just validate-versions` rejects a tree where a public dependency breaks alone.

The flag resolves through version groups, so a package exposing an implementation crate's types
carries the flag on the public crate it actually depends on. That is the package whose version
moves with the implementation crate's.

Only a `breaking` dependency propagates this way. A dependency that adds API compatibly leaves
the exposure intact, so it establishes no more than the `patch` its requirement rewrite already
does.

## Nonbreaking

Choose `nonbreaking` for a meaningful compatible capability: a new API, supported input,
command, output option, or documented behavior that existing consumers can ignore.

Do not choose this level merely because implementation volume is large. The level describes
the consumer-visible change.

## Patch

Choose `patch` for compatible corrections, performance improvements, documentation changes
included in the published package, or internal changes that alter released content without
adding a meaningful consumer-facing capability.

A direct `[package]` metadata change or an inherited `[workspace.package]` change establishes
at least `patch` for every affected package. This rule applies to every package metadata field,
including `rust-version` (the minimum supported Rust version). Combine this minimum with the
package's other evidence and choose the highest applicable change level.

Treat dependency and feature-table changes separately: analyze the consumer impact of the
resulting dependency or feature behavior rather than assuming every `Cargo.toml` edit is
metadata-only.

A package also takes at least `patch` when applying the plan will rewrite an intra-workspace
requirement inside its own published manifest, which happens whenever it declares a version
requirement on a workspace package another decision moves. Every such requirement names the
version its target declares, so moving the target rewrites the requirement even though it would
still have admitted the new version. Such a package has no released-content change of its own
yet, so the report shows nothing for it; the rewrite arrives with the increment that moves the
dependency. Deciding it in the same pass keeps that package from being left behind at an
already-published version with a changed manifest.

## No increment

Choose no increment only when the complete evidence set supports it: every entry in the package's
`changed` array, its released-content diff when it has one, the changed inherited workspace
fields, the locked dependency changes, and the decisions recorded for its dependencies. Signal
this by omitting the package from `decisions.json`.

Membership of a group the report marks `"consistent": false` does not change this. A group whose
members disagree on a version is realigned mechanically when the plan is generated, normally onto
the highest version any member already declares, and by a patch increment of the whole group when
that alignment would rewrite a dependency inside a member that otherwise kept a published
version. Either way the realignment is chosen for you. Judge each member on its own released
changes and choose no increment when it has none.

A package that the report gives no anchor has never been released, so it has no version to
increment. It follows the first-publication path in
[`RELEASING.md`](../../../RELEASING.md#first-publish-of-a-new-crate) instead of taking a change
level.
