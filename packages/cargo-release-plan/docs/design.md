# cargo-release-plan - Design

## Purpose

`cargo-release-plan` protects a release process where merging to a release branch
publishes every changed package. It ensures that changed published content always
carries a version the registry has not seen, while giving maintainers enough
evidence to choose an appropriate increment.

A package has **unreleased changes** when the content it would publish differs
from its last release. Raising its version does not release that content; only the
release process does. The version instead decides whether the next release is
safe:

* **Pending release** means the package has unreleased changes and its version is
  already greater than the last released version.
* **Needs an increment** means the package has unreleased changes but still
  declares the last released version.
* **Unchanged** means its published content still matches the last release.

An **increment level** describes the compatibility significance of a change:
`patch` for compatible corrections, `minor` for compatible additions, and `major`
for breaking changes. An exact target version can be chosen instead when these
levels do not express the intended release.

## Design tenets

### The release branch is the source of truth

Release state comes from the history of the branch that actually publishes, not
from a pull request target or the registry. This keeps stacked pull requests and
local work trees meaningful without network access.

### One baseline, one anchor per package

Every package is assessed against the same **release baseline**: the current tip
of the release branch. Each package has its own **anchor** within that baseline's
history, identifying the commit that last introduced its declared release
version.

### Published artifacts decide relevance

The question is not whether files in a package directory changed. It is whether
the content Cargo would publish changed. Package rules, inherited manifest
values, executable bits, manifest-named resources, package boundaries, and
installable binary lockfile closures therefore participate where they affect
the consumer's build. Physical inclusion of a lockfile alone does not make its
contents release-relevant.

### Evidence and judgement stay separate

The tool determines whether an increment is required and records the evidence.
It does not infer API compatibility. A maintainer or automation with knowledge of
the package's promises chooses semantic increment levels. Resolution preview
completes their mechanical release effects and exposes the resulting evidence
before application.

### Consumer contracts

A published package does not necessarily offer an API for consumers to use. An
implementation partition exists to serve the public package in front of it, and
some packages are published only because Cargo requires a dependency to be
published. Both still have `pub` items, so nothing in the code distinguishes
them from a package meant for direct use: it is a promise the publisher makes,
and the package declares it.

```toml
[package.metadata.release-plan]
private-api = true
```

A package is public unless it declares itself private, and a package with no
library target presents no contract either way. That direction is chosen for its
failure mode rather than its frequency: a package wrongly treated as public
produces a finding a maintainer can act on, while one wrongly treated as private
produces nothing at all. A malformed declaration is an error for the same reason.

This bears on API-compatibility assessment, which is a consumer of the report
rather than part of it. Assessing an implementation partition directly would
measure a surface no consumer can reach, and demand version increases of the
public package for changes its consumers cannot observe. Assessing the public
package instead loses nothing, because a re-exported item appears in the public
package's own documented API.

### Public dependencies

A dependency is **public** when the dependent's own public API exposes types
from it, whether by re-exporting them or by naming them in a signature. The
distinction matters because a public dependency's compatibility is part of the
dependent's own contract.

Which dependencies are public is read from the dependent's
`allowed_external_types` allow-list rather than inferred from source. That
allow-list names every type outside the crate that its public API may expose,
and `check-external-types` fails the build when the API exposes one the list
omits, so in a passing workspace the list is a superset of what is genuinely
exposed. Reading a declaration the repository already verifies keeps this
offline and avoids a second, weaker inference of the public API.

The allow-list names the crate that *defines* a type, which is not always the
dependency that supplies it: a package usually reaches an implementation crate's
types re-exported through the public crate in front of it. The re-exporting
crate closes that gap, because it must declare the crate it re-exports in its
own allow-list. Following those declarations transitively attributes a named
crate to the direct dependency that actually supplies it. Only a normal
dependency qualifies, since a build or development dependency cannot supply
types to a library's public API.

Two consequences follow, and the tool enforces both:

* An intra-workspace requirement names the exact version its target declares.
  A requirement that merely admits the target's version lets a consumer resolve
  a combination the workspace never built. A path-only development dependency
  escapes packaging and is not assessed by this release rule.
* A package whose public dependency releases a semver-incompatible version
  must release one as well. Such a release changes the identity of the exposed
  types, so a consumer holding the older dependency can no longer hand its
  types to the dependent. This follows from the version move alone, however
  unrelated the dependency's breaking change was to the items actually exposed.

Only the second is a release decision. A requirement whose form is wrong is
corrected by editing the requirement, not by incrementing anything.

### The release decision is offline and reproducible

The normal assessment path uses only repository history, the work tree, and
`cargo metadata --no-deps`. It never contacts a registry, resolves the full
dependency graph, or compiles packages. The same inputs therefore produce the
same release decision without network or build-cache state.

### Rejected plans do not edit manifests

Plan targets, version direction, and group expansion are validated before any
manifest is written. Files are then edited structurally so comments and layout
survive.

### Resolution precedes application

Dependency resolution is explicit preparation, not part of classification or
application. The intended offline workspace refresh precedes semantic assessment.
Prospective version and requirement changes are resolved before application as well:
resolving unchanged manifests alone cannot predict their effects.

The proposal settles version groups, requirement propagation, and binary
dependency-closure effects internally. It retains adequate existing increments
instead of repeatedly increasing a package at each resolution pass. The captured
state includes resolved file contents and the inputs they depend on. Application
uses that state without a late dependency refresh or unlisted version targets.
Changed inputs require fresh preparation and assessment.

## Commands

### Produce evidence for versioning decisions with `report`

`report --out-dir <dir>` produces the evidence used to choose versions. It
writes a machine-readable package report and readable patches for packages whose
files changed. Dependency and dependent relationships are included so a
compatibility decision can account for changes that propagate through the
workspace.

Only relationships preserved in the published manifest are relevant. Normal and
build dependencies participate, as do development dependencies with a version
requirement. Cargo removes a path-only development dependency when packaging, so
it does not propagate a release decision.

### Protect a release with `check`

`check` is intended for a merge gate. It fails while any package needs an
increment, a version group disagrees with itself, an intra-workspace
requirement does not name the version its target declares, an exact
intra-workspace requirement is malformed, or a package that exposes a public
dependency stays compatible while that dependency releases a breaking change.
It points the maintainer to the `increment-versions` skill that prepares a plan.

`--format github` additionally emits GitHub Actions error annotations. These are
structured log records that attach each failure to the affected package
manifest, so the workflow summary and pull-request file view make the problem
visible without reading the raw log.

`check --verify-packaging` audits the tool's artifact model against
`cargo package --list`. It warns when Cargo and the tool select different paths
but does not alter the release verdict. The probe allows dirty trees, so
untracked inputs may legitimately appear only on Cargo's side. It also performs
dependency resolution and Cargo's package preparation work, which the normal
offline assessment deliberately avoids. A mismatch on a clean tree is evidence
that the artifact model needs correction.

### Planning stages

A plan exists in two stages, and they carry different guarantees about the
packages a document names.

A **proposed plan** is what a planner writes. Its entries may name a version
group, or a single member of one, and leave resolution to reach the rest, so what
it names is a starting point rather than the full set it moves.

An **expanded plan** names every package whose
version the plan sets and records the version each will carry. Both halves
matter: the first makes the documented set complete with respect to the release
decision, and the second makes it stable, since an increment level would be
resolved again against whatever the manifests say when the document is applied.
Resolving an expanded plan must therefore reproduce it exactly.

Applying a plan also rewrites the requirements that dependents declare on the
packages it moves. A dependent whose existing pending increment is sufficient
need not receive another one. A dependent that would otherwise keep an
already-published version needs its own release decision before application.

The expanded plan is applied unchanged, so the documented package/version set
and applied document are the same artifact. Review and approval policy belong
to the caller, not the tool.

### Expand version choices with `expand`

`expand --plan <plan.json> --out <expanded.json>` resolves a proposed plan's
version groups and increment levels into one explicit entry per package. A
proposed plan may omit version-group members that `apply` will update; `expand`
writes the explicit package/version set without resolving dependencies.

That set is the packages whose versions move. Applying it also rewrites
requirements inside their dependents, which the document does not name because
the plan gives them no version.

An expanded plan records its stage, which binds it to the package set it names:
applying it after a version group gained a member fails rather than quietly
editing an unlisted package. Recovering from that means refreshing the planning
inputs and expanding the proposal again to document the wider set. A proposed
plan keeps the opposite behavior, since naming a group and letting resolution
reach its members is how such a plan is written.

Structural expansion alone is not a complete resolved artifact. A release
proposal must also account for the actual lockfile effects of those versions.

### Prepare evidence and preview resolution

Preparation performs the workflow's intended offline workspace resolution before
collecting released-content evidence. It does not request blanket third-party
upgrades. The report and compatibility assessment used for semantic decisions
describe that prepared state.

Preview applies candidate versions and requirement rewrites in a disposable
workspace and resolves there under the same offline policy. It classifies the
prospective tree against the fixed release baseline and expands release effects
until versions and resolution agree. Transitive binary lockfile effects and
re-selection among already-locked dependency versions therefore appear before
application, not as a request for a second versioning pass.

Automatically required releases are visible in the final proposal and its
evidence. They establish minimum release requirements, not a claim of semantic
compatibility: the caller assesses newly exposed dependency changes and raises
levels when the package's contract requires it, then previews again before
applying the stable proposal.

### Carry out a decision with `apply`

`apply --plan <plan.json>` applies an expanded plan using the resolved
state captured by preview. It validates the input
snapshot and target set before installing the captured manifest and lockfile
contents. It does not run dependency resolution. An already-applied resolved
plan is an idempotent no-op; a partially changed or stale input is not treated as
the captured state. `--dry-run` reports what would change without writing.

Proposed plans support a separate low-level manifest-only application. That path
does not resolve or install lockfiles and is not the complete release workflow.
The guided release workflow accepts only the resolved expanded artifact.

### Between report and apply

Choosing an increment level requires comparing a change with the package's
contract. The report supplies the changed files, inherited values, locked
dependencies, and workspace relationships needed for that judgement. It does
not compile code, compare API surfaces, or infer compatibility from a textual
diff.

After a person or an agent records the choices in a plan, preview accounts for
the mechanical consequences. It expands version groups, derives new versions,
rewrites requirements that must follow, and resolves the lockfile before the
complete result is applied. Post-application verification confirms that result;
it is not a routine source of additional lockfile-only release decisions.

All commands use the workspace selected by `--manifest-path`. `report` and
`check` accept `--base` to name the shared release baseline.

## The release baseline

The release baseline is the tip of the branch releases are made from. Passing it
explicitly is most reliable because the caller knows the project's release
process. It is not necessarily the branch a pull request targets: a stacked pull
request may target an unreleased parent branch.

Without `--base`, the tool uses the default branch recorded for the `origin`
remote and falls back to `origin/main` when the remote records none. These are
conveniences for interactive use, not knowledge of the project's release policy.

The baseline is shared, while anchors differ by package:

```text
release baseline history

A ---- B ---- C ---- D ---- E   <- baseline tip
       ^           ^
       |           +-- package-beta anchor (version 2.1.0)
       +-------------- package-alpha anchor (version 1.4.0)

work tree
  package-alpha: compare B -> work tree
  package-beta:  compare D -> work tree
```

Running on the release branch itself reports packages awaiting its next publish.
Running from a dirty work tree is also supported, which lets `check` find a
missing increment before the edits are committed.

## Anchors

An anchor is the newest commit on the baseline's first-parent history where the
package's parsed version changed. Reformatting the version declaration does not
move it. The commit that first adds a package counts as a version change.

First-parent history makes a merged pull request one release event. If a version
was edited on a topic branch, its anchor is the merge commit where that version
first reached the release branch, not the topic commit where it was typed:

```text
          E ---- F
         /        \
A ---- B ---------- M ---- D   <- baseline first-parent history
                       ^
                       version first released here; M is the anchor
```

A shallow history that hides a required version change cannot support a release
claim, so the command fails rather than treating the package as unchanged.

### Packages the baseline does not publish

A package absent from the baseline, or present there with `publish = false`, has
no release on that baseline to compare. It is treated as preparing its first
release and is pending release at any declared version.

A package name that was published, removed, and later restored is also treated
as new. Guessing which old incarnation it continues would make clone depth a
correctness input. Whoever restores the name must reconcile it with versions
already present in the registry.

### Version monotonicity

Versions move forward relative to the selected release line. A version below the
anchor's version is an error because that release line has already published the
higher version.

Publishing a patch for an older series remains possible by using a separate
release branch based on that series. For example, `1.3.1` can follow `1.3.0` on a
maintenance branch even when another release branch has already reached `1.4.0`;
the maintenance branch supplies its own baseline and anchors.

## Released content

Released content is the git-tracked content Cargo would place in the package
artifact:

* Git decides which files exist and how clean filters and line endings identify
  their content. Untracked files are advisory only.
* The package's `include` and `exclude` rules select paths beneath the package.
  `Cargo.lock` at the package root is excluded from this file comparison.
* A nested `Cargo.toml` ends the enclosing package, whether or not the nested
  package is a workspace member.
* The package directory is resolved independently at the anchor and in the work
  tree, so moving a package does not break its identity.
* The executable bit is content because Cargo preserves it in the artifact. Git's
  configured work-tree model decides whether an unstaged mode change is visible;
  the index remains the fallback where file modes are not supported.

A path selected at either end participates. Deleted files, files dropped from an
`include` list, and formatting-only edits to a packaged manifest therefore remain
visible.

### Where Cargo adds content

Cargo includes several inputs outside ordinary package rules:

* A declared `readme` or `license-file` is included even when rules exclude it,
  including a resource inherited from `[workspace.package]` or located outside
  the package directory.
* Without a `readme` declaration, Cargo detects a default README in the package
  directory. `readme = false` opts out.
* A package-root `target` directory is never included.
* A symbolic link in released content stops the assessment. Cargo publishes the
  target bytes while Git stores the target path, so Git history alone cannot
  compare the artifact correctly.

### Relevant lockfile closures

Cargo includes a generated lockfile in every package artifact. The lockfile does
not constrain consumers of a library-only package: those consumers resolve the
library in their own dependency graph. Its dependency changes are therefore not
released content for this purpose.

An installable binary target makes its package's recorded dependency resolution
release-relevant, including when that package also contains a library. Examples,
benchmarks, tests, and build scripts do not qualify, even when they are executable
or physically included in an archive.

The package-specific closure is compared rather than the workspace lockfile's
bytes, so unrelated dependency movement does not affect every binary
package. Entries are identified by name, version, and source. The root package
is selected by its name and declared version, and excluded from its own closure
so incrementing it does not create another change.

The closure covers installation dependencies, including normal and build
dependencies across target platforms. Development-only dependency edges of
workspace members do not participate, either at the binary root or through a
transitive workspace dependency.

Dependency identity includes its source, so a same-named development dependency
from another source does not enter an installation closure. Workspace patches,
registry configuration, and Cargo-supported legacy dependency tables participate.
If source identity cannot be reconstructed without guessing, assessment stops
instead of reporting the dependency as unchanged or irrelevant.

Target shape is resolved independently at the anchor and in the work tree. An
endpoint with an installable binary target requires a workspace lockfile that
resolves the package at the version declared there. An endpoint without one
contributes an empty closure and requires no lockfile. This makes adding the
first binary compare an empty anchor closure with the current
resolution, while removing the last one compares the historical resolution with
an empty work-tree closure. If a required closure cannot be reconstructed, the
assessment stops rather than treating unknown released content as unchanged. A
new package has no anchor artifact to compare and is classified as new without a
historical closure.

### Inherited workspace values

Values a package inherits from `[workspace.package]` or
`[workspace.dependencies]` are part of its published manifest. A changed
inherited value therefore affects each package that uses it.

Cargo omits a versionless dev dependency from the published manifest. Changes to
such a dependency's inherited workspace entry do not affect released content
while the entry remains versionless; adding or removing its version does.

`[workspace.lints]` is not published behavior and does not participate.

### Path case

Member paths and default README names follow the case behavior of the workspace
volume rather than an operating-system assumption. Git-tracked spellings remain
distinct in reports so a case-only rename stays visible.

## Package status

| Status            | Meaning                                                      |
| ----------------- | ------------------------------------------------------------ |
| `pending-release` | Version is above the anchor and the next release publishes it |
| `needs-increment` | Released content changed without a version increase           |
| `unchanged`       | Released content and version still match the anchor            |

Of these statuses, only `needs-increment` fails `check`; the manifest-level
requirement and public-dependency rules fail it independently of status.
`publish = false` packages are excluded.

## Version groups

Every Git-tracked Cargo workspace member is a **version target**, including a
member that cannot be published. An exact dependency declaration between two
version targets states that their versions move together. Version groups are
the connected components formed by those declarations, in either dependency
direction, and contain at least two members. A group's key is its
lexicographically smallest member.

All normal, build, and development declarations participate, including optional
and target-specific declarations. An inherited declaration uses the effective
workspace dependency. A dependency alias follows the package identity it names,
and a local path must resolve to that workspace member. Registry dependencies,
outside or excluded paths, versionless paths, and unused workspace dependency
entries do not form groups.

The accepted exact form is one `=major.minor.patch` comparator, with
insignificant whitespace allowed. A partial exact version, a prerelease or build
suffix, or a compound requirement containing an exact comparator is a manifest
error. A well-formed exact requirement whose version is stale still forms its
group: `check` reports the stale requirement and `apply` can repair it.

If one publishable member needs an increment, the plan expands to every version
target in its group. A plan may also target a non-publishable member directly,
and helper-only groups can be aligned without publishing anything. The target
starts from the highest declared member version, including non-publishable and
new members, and applies the highest chosen increment level. Entries that expand
to the same group must all use increment levels or all use one matching exact
version.

`expand` exposes that resolution as a document so a caller can present and apply
the complete package/version set rather than leave group members implicit.

An inconsistent group is a check failure in its own right, independent of any
content change. A plan entry naming any member resolves it, and expansion is
plan-driven, so a group no entry names is left alone. An entry that carries an
increment level raises the group's highest declared version. An entry that
carries that highest version as an exact target instead moves lagging members up
to it and leaves the leading member unchanged. The lagging members then become
pending release because their declared versions advanced.

Members absent from the baseline are exempt from the consistency check, which
lets a new package join a group before its first release. This exemption does
not remove the member from alignment or from the version base. A member that
exists on the baseline with publication disabled is not absent.

The obsolete `[workspace.metadata.release-plan.groups]` key is rejected. Group
membership is declared only by exact workspace dependency requirements.

## Report artifacts

`report.json` is the complete machine-readable assessment. Its `packages` array
records every publishable package, its status and anchor, the reasons it changed,
and its dependencies and dependents. Its `non_publishable_packages` array
records each remaining version target's name, declared version, and group.
Group records cover the union of both arrays and report complete consistency.

Per-package patch files are a readable supplement for file changes. They cover
every package whose released files differ from its anchor, including one whose
version has already moved, because judging whether a pending increment still
covers the accumulated changes needs the same evidence. Changes that are not
file differences — inherited workspace values and locked dependency identities —
are reported only as change entries. They use zero-context
unified diffs, report binary changes without rendering binary bytes, and
preserve addition, deletion, and mode information. Expensive line-level
comparisons fall back to a whole-file replacement; this changes only the
presentation, never the release verdict.

Internal ownership is documented in the [implementation guide](implementation.md).
