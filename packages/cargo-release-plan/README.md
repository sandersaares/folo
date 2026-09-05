# cargo-release-plan

A Cargo subcommand that classifies every publishable workspace package against its
version **anchor**, reports changes to **released content**, and applies an
approved increment plan (group expansion, `=`-pin rewrites, lockfile refresh).

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
cargo release-plan expand --plan <plan.json> --out <expanded.json>
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

Exits non-zero when any publishable package needs an increment or any version
group declares inconsistent versions. Failure text describes the
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

### `expand`

Resolves a plan's version groups and increment levels into one explicit entry
per package, written to `--out`.

An input plan may omit version-group members that `apply` will update. `expand`
writes the complete explicit package/version set for review, naming every
package the plan reaches, including group members the input plan did not mention,
at the version each will carry.

The output is itself a plan, so the expanded document is the one passed to
`apply` after review. Every entry carries an explicit `version`.

Re-expand after changing the input plan. Editing an expanded plan by hand risks
giving one group's members different versions, which both `expand` and `apply`
reject.

### `apply`

The tool does not choose increment levels. Deciding whether a change is
breaking, additive, or neither is a semantic judgement, and nothing here
compiles code or compares API surfaces. `report` supplies what that judgement
needs; a caller records a level per package in a plan; `apply` then owns the
mechanical part, including deriving the resulting version numbers.

Reads an approved plan and:

* sets each listed package's `version`
* expands version groups so every member receives the new version
* rewrites intra-workspace dependency requirements that must follow, including
  `=` pins
* refreshes the workspace lockfile so `--locked` builds keep working

Manifests are edited structurally, so comments and layout are preserved. Every
reason a plan can be rejected is found before anything is written: an unknown
target, a version that would move backwards, or an unreadable manifest is
reported while every manifest is still untouched. Writes themselves are
sequential, so an accepted plan that then fails on an I/O error can leave earlier
manifests updated; revert the work tree with `git` in that case. `--dry-run`
reports the manifests that would change and writes nothing.

The plan schema is:

```json
{
  "schema_version": 1,
  "increments": [
    { "name": "nm", "level": "patch" },
    { "name": "events", "version": "0.7.14" }
  ]
}
```

`name` is a package name or a version-group name. `level` is `major`, `minor`,
or `patch`. An explicit `version` is used as-is for that target (and its group),
and is rejected when it is lower than a version the target already declares.
Each increment must supply exactly one of `level` or `version`. Entries that
expand to the same target must use the same choice: levels combine by taking the
highest, while explicit versions must match.

An optional top-level `expanded` records which planning stage a document belongs to. A **proposed
plan** leaves it absent: its entries may name a version group and let resolution reach the
members, and may carry an increment level resolved when the plan is applied, so what it names is
a starting point rather than the full set it moves. An **expanded plan**, written by `expand`,
sets it, names every package the plan reaches, and gives each an explicit `version`. Both are
required of it: an entry left at a level would be resolved against the manifests as they stand
when it is applied, so the same document could apply a version other than the reviewed one.
Resolving an expanded plan must reproduce exactly the set it names; reaching any other package
means the workspace's version groups changed after the document was written, and is rejected
rather than applied.

### Plan and report schema

`report.json` uses the same schema revision. Top-level fields are
`schema_version`, `head`, `packages`, and `groups`. Each package object includes
`name`, `declared_version`, `status` (`pending-release` / `needs-increment` /
`unchanged`), `changed`, `stat`, `dependencies`, and `dependents`, plus omitted
when empty: `group`, `anchor`, `diff_path`, `untracked`. A change is one of
`{"path","change","source":"package"}`, `{"field","source":"inherited"}`, or
`{"dependency","change","source":"lockfile"}`.
`diff_path` is relative to the report directory. Plan and report formats
advance this revision together: an incompatible field, enum, or path-layout
change increments it.

## Classification

| Status            | Condition                                                |
| ----------------- | -------------------------------------------------------- |
| `pending-release` | version increased since the anchor                       |
| `needs-increment` | version unchanged, released content changed since anchor |
| `unchanged`       | version unchanged, released content unchanged            |

`check` fails on `needs-increment` alone. A `pending-release` package still holds
unreleased changes; merging is what releases them. Packages with
`publish = false` are ignored. Untracked files are advisory only. Versions only move forwards: a
declared version below the anchor's version is an error rather than a status.

Cargo includes a generated lockfile in every package archive, but a
library-only consumer resolves the library in its own dependency graph instead
of using that file. A package that publishes an executable also releases its
resolved dependency closure because `cargo install --locked` uses the archive's
lockfile. A workspace lockfile change that moves such a package's dependencies
is therefore a released-content change; the same change against a library is
not.

A packaged file's executable bit is released content too, since Cargo carries
the mode Git records into the archive. Making a packaged file executable is
therefore a change even when its bytes are untouched. The mode is read from the
index, so a checkout on a platform without executable permissions classifies the
same way.

Version groups are declared in the workspace root as
`[workspace.metadata.release-plan.groups]`. Members share a declared version; if
any member needs an increment, all members increment. Members the baseline does
not carry are exempt from the consistency rule.

## Offline operation

Classification shells out only to `git` and `cargo metadata --no-deps`. It does
not contact crates.io, resolve a dependency graph, or compile. `check
--verify-packaging` may spawn `cargo package --list`. `apply` may spawn
`cargo update --offline` to refresh the workspace lockfile.
