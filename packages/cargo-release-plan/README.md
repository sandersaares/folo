# cargo-release-plan

A Cargo subcommand that classifies every publishable workspace package against its
version **anchor**, reports changes to **released content**, and prepares a
complete increment plan with its resolved dependency effects.

A package has unreleased changes when its released content differs between its
version anchor and the work tree. Such a package is pending release once its
declared version has been raised past the anchor, and needs an increment until
then. The anchor is the most recent commit on the release baseline's first-parent
line in which the package's parsed `version` changed.

## Usage

Install with [`cargo binstall cargo-release-plan`](https://github.com/cargo-bins/cargo-binstall)
to fetch a prebuilt binary on supported targets (transparently building from source
elsewhere), or `cargo install cargo-release-plan` to always build from source. Then:

```text
cargo release-plan report --out-dir <dir> [--base <rev>] [--manifest-path <path>] [--verbose]
cargo release-plan check [--base <rev>] [--manifest-path <path>] [--format text|github] [--verify-packaging] [--verbose]
cargo release-plan prepare --output <dir> [--base <rev>] [--manifest-path <path>] [--verbose]
cargo release-plan analysis-order --report <file-or-dir> [--verbose]
cargo release-plan semver-targets --report <file-or-dir> [--verbose]
cargo release-plan propose --report <file-or-dir> --decisions <decisions.json>
    --out <plan.json> [--verbose]
cargo release-plan preview --prepared <prepared.json> --plan <plan.json> --output <dir>
    [--manifest-path <path>] [--verbose]
cargo release-plan verify-preview --plan <plan.json> --manifest-path <prospective-manifest>
    [--verbose]
cargo release-plan expand --plan <plan.json> --out <expanded.json>
    [--manifest-path <path>] [--verbose]
cargo release-plan inspect-plan --plan <expanded.json> [--require-resolved]
    [--manifest-path <path>] [--verbose]
cargo release-plan apply --plan <plan.json> [--dry-run] [--manifest-path <path>] [--verbose]
```

`--base` names the **release baseline**: the tip of the branch releases are made
from, which is not necessarily the branch a pull request targets. CI should pass
it explicitly. Without it, the baseline is the default branch the `origin`
remote advertises, falling back to `origin/main`. `--manifest-path` defaults to
`Cargo.toml` in the current directory.

### `report`

Writes `<dir>/report.json` plus a `<dir>/diffs/<package>.patch` for every package
whose released files differ from its anchor, whether or not its version has
already moved. The JSON names each package's status, anchor, changed paths,
inherited workspace fields, intra-workspace dependencies, and version groups,
and is the complete verdict.

Each `.patch` is a zero-context unified diff in the shape `diff -U0` produces,
so it can be piped into standard tooling. Only `source: "package"` changes are
file differences: inherited workspace values and locked dependency identities
are not, so a package whose `changed` entries are all `inherited` or `lockfile`
has no patch. Enumerate `status`
in `report.json` rather than the `diffs/` directory to find every package that
needs an increment.

### `check`

Exits non-zero when any publishable package needs an increment, any version
group declares inconsistent versions, any intra-workspace requirement does not
name the version its target declares, an in-workspace exact requirement is
malformed, or any package that exposes a public dependency stays compatible
while that dependency releases a breaking change.
Failure text describes the
self-contained recovery workflow: run `report`, prepare a plan, and run `apply`.
It additionally reserves the `increment-versions` agent-skill name for the
automated workflow supplied by the release-versioning stack's separate skill
layer.

`--format github` also emits GitHub Actions workflow annotations.

`--verify-packaging` cross-checks this tool's released-content rules against
`cargo package --list`. Divergences are printed as warnings and do not fail the
check. The probe allows dirty trees, so an untracked input can legitimately
appear only in Cargo's list. It also resolves the dependency graph and performs
Cargo's package-preparation work, so gating on it would give up the normal
offline, no-resolve path. A divergence on a clean tree is evidence that the
rules need fixing.

### `prepare`

Prepares the workflow's intended offline workspace dependency resolution before
writing release evidence. Review changes from this prepared report before
choosing semantic increment levels. Preparation does not request blanket
third-party upgrades.

Preparation can modify the workspace lockfile. It writes `prepared.json`,
`report.json`, and per-package `diffs/` beneath the output directory. Read-only
`report` and `check` never perform this refresh.

### Artifact-only planning

`analysis-order`, `semver-targets`, and `propose` consume a report file, or a
directory containing `report.json`. They do not inspect the current workspace,
invoke Cargo or Git, or contact a registry. Relative paths resolve from the
current directory. These commands require the supported report schema.
`analysis-order` and `semver-targets` print JSON to stdout; `propose` writes its
JSON plan to `--out` and prints a human-readable summary. `--verbose` writes
explanatory decisions to stderr without changing those outputs.

`analysis-order` prints dependency-first batches as a JSON array:

```json
[
  { "order": 1, "packages": ["implementation"], "cyclic": false },
  { "order": 2, "packages": ["api"], "cyclic": false }
]
```

Every publishable package appears once, including unchanged packages.
Dependencies outside a batch precede it. All ready batches in a dependency wave
are emitted before proceeding to the next wave, with ordinal package-name ordering.
A cyclic batch contains mutually dependent packages; version-group membership
alone does not make a cycle. Non-publishable helpers are not assessment targets.

`semver-targets` prints an ordinally sorted JSON array of package names. Changed
released content selects the consumer contracts in that package's version group,
or the package itself when ungrouped. Private APIs and packages without a library
contract are not compared directly. An empty selection is `[]`. The command
selects targets only; the caller runs its compatibility tooling.

`propose` consumes caller-decided change levels:

```json
{
  "schema_version": 1,
  "changes": [
    { "name": "api", "level": "breaking" }
  ]
}
```

The decision format has its own schema revision. Levels are `breaking`,
`nonbreaking`, and `patch`; omit packages requiring no semantic increment.
The tool does not choose these levels. It retains sufficient pending increments,
resolves version-group alignment, and propagates required releases through
workspace dependencies. The proposed plan written to `--out` uses the ordinary
plan schema and must pass through `preview` before complete release application.
The report cannot predict new lockfile effects of a proposal; preview supplies
that evidence for further assessment.

### `expand`

Resolves a plan's version groups and increment levels into one explicit entry
per package, written to `--out`.

An input plan may omit version-group members that `apply` will update. `expand`
writes the explicit package/version set for review, naming every tracked member
whose version the plan sets, including non-publishable helpers and group members
the input plan did not mention, at the version each will carry. Applying it also
rewrites requirements inside those packages' dependents, which take no version
from the plan and so are not named.

Every entry carries an explicit `version`. This is structural expansion only:
it does not predict Cargo resolution effects and is not sufficient as a
complete resolved artifact.

Re-expand after changing the input plan. Editing an expanded plan by hand risks
giving one group's members different versions, which both `expand` and `apply`
reject.

### `preview`

Resolves proposed version and dependency-requirement changes in a disposable
workspace against the prepared inputs. The proposal expands until version
groups, requirement propagation, and installable binary lockfile effects are
covered. Existing adequate increments are retained rather than raised again
on each iteration.

The output directory contains an expanded `plan.json` with captured resolved
files and input identity, plus the prospective `report.json` and `diffs/`.
The final prospective checkout remains in `workspace/`. Its manifest path is
recorded in `resolved.evidence_manifest_path` so compatibility tools can build
the same source, manifest versions, and lockfile that the report describes.
Use its complete target set and additional dependency evidence to assess release
impact; a mechanically required release does not establish that a change is
semantically compatible. Adjust semantic levels and preview again when needed.
Apply the captured artifact unchanged. Review and approval policy belong to the
caller, not the tool.

### `verify-preview`

Checks the retained prospective workspace against its captured resolved plan
without dependency resolution or compilation. Run it after external compatibility
analysis, using the recorded evidence manifest path, to detect source, manifest,
configuration, or lockfile changes made during that analysis.

This evidence path does not become the application destination: `apply` remains
bound to the original post-preparation workspace inputs.

### `inspect-plan`

Validates an expanded plan against the selected workspace's tracked members and
prints JSON facts for external tooling:

```json
{
  "publication_targets": ["api"],
  "evidence_manifest_path": null
}
```

`publication_targets` contains the ordinally sorted publishable packages named by
the expansion, excluding alignment-only helpers. `evidence_manifest_path` is the
captured preview's compatibility manifest, or `null` for structural expansion.
The command does not contact a registry or change workspace files.

`--require-resolved` requires a captured preview valid for application. Captured
plans undergo the same read-only input and artifact checks as `apply --dry-run`.
Their retained compatibility workspace is also verified before its manifest path
is returned.
Use this inspection before external publication checks; use `verify-preview`
to validate the retained compatibility workspace.

### `apply`

The tool does not infer semantic compatibility. Deciding whether a change is
breaking, additive, or neither is a semantic judgement, and nothing here
compiles code or compares API surfaces. Prepared evidence supplies what that
judgement needs; a caller records a level per package in a plan; prospective
preview completes the mechanical consequences before application.

For a resolved expanded plan, the command:

* sets each listed package's `version`
* rewrites intra-workspace dependency requirements that must follow, including
  `=` pins
* installs the captured resolved lockfile so `--locked` builds keep working

Comments and layout are preserved in the prepared edits. Input and artifact
validation precede writes. Changed original inputs require fresh preparation;
apply does not silently widen the target set or resolve new lockfile effects.
Applying the same artifact to its fully applied state is a no-op. Writes can
still fail on I/O errors, so inspect a partial failure before taking recovery
action. `--dry-run` reports what would change and writes nothing.

Applying a proposed plan is a low-level manifest-only operation: it resolves
version groups and edits manifests, but does not prepare or install a lockfile.
It is not the complete release-planning workflow. Use `preview` and apply its resolved
expanded artifact when preparing a complete release.

The proposed-plan schema is:

```json
{
  "schema_version": 4,
  "increments": [
    { "name": "nm", "level": "patch" },
    { "name": "events", "version": "0.7.14" }
  ]
}
```

`name` is the name of any Git-tracked workspace member. If it belongs to a
version group, the entry reaches the complete group. `level` is `major`, `minor`,
or `patch`. An explicit `version` is used as-is for that target and its group,
and is rejected when it is lower than a version the target already declares.
An explicit group version must be a plain `major.minor.patch` triplet.
Each increment must supply exactly one of `level` or `version`. Entries that
expand to the same target must use the same choice: levels combine by taking the
highest, while explicit versions must match.

An optional top-level `expanded` records which planning stage a document belongs to. A **proposed
plan** leaves it absent: its entries may name a version group and let resolution reach the
members, and may carry an increment level resolved when the plan is applied, so what it names is
a starting point rather than the full set it moves. An **expanded plan**
sets it, names every package whose version the plan sets, and gives each an explicit `version`.
Both are
required of it: an entry left at a level would be resolved against the manifests as they stand
when it is applied, so the same document could apply a version other than the recorded one.
Resolving an expanded plan must reproduce exactly the set it names; reaching any other package
means the workspace's derived version groups changed after the document was written, and is rejected
rather than applied. Requirement rewrites inside those packages' dependents are not part of that
set, because the plan gives a dependent no version of its own. The resolved
artifact additionally captures the resolved files and original inputs;
the expanded stamp alone is not a substitute for that evidence.

`preview` supplies the required `resolved` object in the expanded
artifact. Treat it and `prepared.json` as opaque tool-owned evidence: keep the
files intact rather than synthesizing or editing them. Expanded `apply` requires
this resolved artifact and does not accept a structural expansion alone.
Unsupported schema versions require regenerating evidence with `prepare` and
the resolved artifact with `preview`; they do not select a compatibility mode.

### Plan and report schema

`report.json` uses the same schema revision. Top-level fields are
`schema_version`, `head`, `packages`, `non_publishable_packages`, and `groups`.
`packages` contains release assessments for publishable members. Each package object includes
`name`, `declared_version`, `status` (`pending-release` / `needs-increment` /
`unchanged`), `changed`, `stat`, `dependencies`, `dependents`, and
`consumer_contract`, plus omitted
when empty: `group`, `anchor`, `diff_path`, `untracked`. A change is one of
`{"path","change","source":"package"}`, `{"field","source":"inherited"}`, or
`{"dependency","change","source":"lockfile"}`.
A dependency is `{"name","req","exact_pin","public"}`. `public` marks a
dependency that supplies types the dependent's own public API exposes, derived
from the dependent's `allowed_external_types` allow-list and followed
transitively through re-exports, so a package exposing an implementation crate
marks the public crate it actually depends on. Only normal dependencies qualify.
`consumer_contract` is true when the package has a library target and has not
declared `[package.metadata.release-plan] private-api = true`, which is how a
package states that its library serves another package rather than consumers.
`diff_path` is relative to the report directory. Plan and report formats
advance this revision together: an incompatible field, enum, or path-layout
change increments it.

`non_publishable_packages` contains every remaining tracked version target.
Each entry has `name`, `declared_version`, and an optional `group`; it has no
release status, anchor, changes, or consumer contract. A group's sorted
`members` refer to the union of both package arrays, `version` is the highest
declared member version, and the group key is its lexicographically smallest
member.

## Classification

| Status            | Condition                                                |
| ----------------- | -------------------------------------------------------- |
| `pending-release` | version increased since the anchor                       |
| `needs-increment` | version unchanged, released content changed since anchor |
| `unchanged`       | version unchanged, released content unchanged            |

`check` fails on `needs-increment` alone as a release status. A `pending-release` package still holds
unreleased changes; merging is what releases them. Packages with
`publish = false` are excluded from release assessment but remain version
targets. Untracked files are advisory only. Versions only move forwards: a
declared version below the anchor's version is an error rather than a status.

Cargo includes a generated lockfile in every package archive, but a
library-only consumer resolves the library in its own dependency graph instead
of using that file. A package with an installable binary target also releases its
resolved dependency closure because `cargo install --locked` uses the archive's
lockfile. A workspace lockfile change that moves such a package's dependencies
is therefore a released-content change; the same change against a library is
not. A mixed library/binary package qualifies, but examples, benchmarks, tests,
and build scripts do not. The binary closure includes normal and build
dependencies, not development-only workspace dependency edges.

Source-aware matching distinguishes aliased dependencies with the same name and
version from different sources and accounts for workspace patches. Missing
registry mappings or source spellings whose equivalence cannot be reconstructed
are operational errors. Unequal URLs requiring complex normalization, such as
percent-encoded or internationalized spellings, are not treated as equivalent
by approximation.

A packaged file's executable bit is released content too, since Cargo carries
the mode Git records into the archive. Making a packaged file executable is
therefore a change even when its bytes are untouched. The mode is read from the
index, so a checkout on a platform without executable permissions classifies the
same way.

An exact local dependency requirement forms an undirected edge between two
Git-tracked workspace members. Connected components containing more than one
member are version groups. All dependency kinds, optional declarations,
workspace inheritance, and target-specific tables participate. Aliases follow
their declared package identity; registry dependencies, outside-workspace paths,
versionless paths, and unused workspace dependency entries do not participate.

The accepted exact syntax is one `=major.minor.patch` comparator, allowing
insignificant whitespace. Partial versions, prerelease or build suffixes, and
compound requirements containing an exact comparator are rejected for
in-workspace edges. A valid but stale exact pin still forms its group and is
reported by `check` until `apply` repairs it.

Every group member shares a declared version; if any publishable member needs an
increment, all members receive the resulting version. A plan may also target a
non-publishable member or an entirely non-publishable group for alignment.
Members absent from the baseline are exempt from consistency but remain in
alignment and the highest-version base. The obsolete
`[workspace.metadata.release-plan.groups]` key is rejected in the current
workspace.

## Offline operation

Classification shells out only to `git` and `cargo metadata --no-deps`. It does
not contact crates.io, resolve a dependency graph, or compile. `check
--verify-packaging` may spawn `cargo package --list`. Explicit preparation and
prospective preview use `cargo update --offline --workspace`. Application
installs the captured resolved files without running dependency resolution.
`verify-preview` also performs no dependency resolution or compilation.
