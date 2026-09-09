# cargo-release-plan implementation

User-visible behavior belongs in the package [design](design.md). This guide
describes the internal boundaries that keep that behavior consistent.

## Architecture

The binary is intentionally thin. `main` parses Cargo's injected subcommand
argument, then delegates to the library `run()` entry used by integration tests.
The selected command drives command-specific paths through shared components:

```text
Cli -> RunInput -> run()
                    |
                    +-> classify -> check diagnostics
                    |           \-> report JSON + patches
                    |
                    \-> load workspace -> normalize plan
                                       |-> expanded-plan JSON
                                       \-> compute edits -> write manifests
                                                         -> refresh lockfile
```

Modules own subjects rather than syntactic categories. `metadata` and `manifest`
build the work-tree model, `git` owns repository facts, `anchor` resolves release
history, `classify` combines those inputs, `groups` and `plan` expand release
decisions, and `apply`, `check`, `expand`, and `report` own their command outputs.

## Subprocess boundaries

All repository access goes through `GitRepo`, which spawns the installed `git`.
The tool therefore follows the repository formats, filters, configuration, and
extensions the maintainer's Git understands instead of maintaining a second Git
implementation.

The subprocess boundary also covers `cargo metadata --no-deps`,
`cargo package --list`, and the offline lockfile refresh. Classification never
runs a build or a full dependency resolution.

Git paths remain repository-relative and `/`-separated. Operating-system paths
from Cargo are converted once when they enter the manifest model. NUL-delimited
Git output is used for file names and decoded strictly as UTF-8; substituting an
invalid byte could collapse two different paths into one. Other command output
is decoded lossily because replacement there affects only diagnostics.

Every subprocess receives a fixed locale and disabled Cargo color. One routine
Git failure must be interpreted in process: a path absent from a revision. A
translated diagnostic would otherwise turn ordinary package creation or deletion
into an error.

## Workspace snapshots

`cargo metadata --no-deps` supplies candidate current members and normalized
dependency relationships. Git-tracked manifests constrain that candidate set.
The current model keeps both every tracked version target and the publishable
`WorkPackage` projection used for classification. An untracked or ignored
manifest found through a member glob can become neither a version target nor a
release assessment. Each tracked current member manifest is loaded and parsed
once per work-tree snapshot; its parsed document and derived package facts are
shared by version-target construction, exact-dependency discovery, and the
publishable projection. Historical workspaces cannot use Cargo without checking
out each commit, so `SnapshotCache` reconstructs them from tracked manifests.

The reconstruction starts from the root package and declared member patterns,
then follows in-workspace path dependencies to a fixed point while honoring
`exclude`. Explicit members may use `[package] workspace` from elsewhere in the
same Git repository; their workspace-relative parent components are retained
while Git access remains repository-relative. A non-virtual root is always a
member. Parsed manifests are cached per commit because anchor resolution and
content comparison revisit the same snapshots across packages.

Each snapshot resolves the package fields that may inherit from
`[workspace.package]` and the path dependencies inherited through
`[workspace.dependencies]`. Typed TOML values are retained for comparisons so
formatting changes do not masquerade as inherited-value changes. Dependency
table kinds are retained so versionless dev dependencies omitted by Cargo do not
create false inherited-value changes.

## Classification

Classification combines one current work-tree model with package-specific
historical snapshots:

```text
baseline first-parent commits
        |
        +-> package timeline -> anchor snapshot
                                      |
work-tree metadata -------------------+-> released-content comparison
                                      +-> inherited-value comparison
workspace lockfiles ------------------+-> binary/example closure comparison
```

### Anchor and change set

Only first-parent commits that can affect a parsed manifest are reconstructed,
plus the newest and oldest commits needed to distinguish a true root from a
shallow boundary. `anchor` walks the resulting package timeline until it finds a
version change, package creation, or insufficient history.

A package not published by the baseline bypasses the walk and becomes new.
Within a walk, an unpublished manifest is skipped rather than treated as absent:
withdrawal releases nothing, but an earlier published version can still be the
anchor.

After resolving the anchor, each side independently supplies the package
directory, packaging rules, resources, nested-package boundaries, and default
README. This keeps moves and changes to packaging structure comparable.

### Content identity and file modes

File equality uses Git object ids. `git hash-object` applies the same clean
filters and line-ending conversion used when committing, so work-tree bytes are
not compared directly with already-filtered historical blobs. The cleaned
work-tree blobs are written into Git's object database and read back by object id
when a patch needs their bytes. This keeps verdict and presentation on the exact
same filter result without changing refs, the index, or work-tree files;
unreachable blobs remain subject to ordinary Git garbage collection.

Historical file modes come from `git ls-tree`. Work-tree modes start from the
index and overlay `git diff-files --raw`: on a checkout with `core.fileMode`
enabled, an unstaged executable-bit change is observed; when it is disabled, the
index remains the stable fallback. Indexed symlink modes are retained as well
because `core.symlinks = false` can materialize a link as an ordinary work-tree
file; released symlinks are rejected before content hashing.

File bytes are loaded only after object identity differs, because mode-only
changes render directly and bytes are needed for presentation rather than the
verdict. Symbolic links are rejected before hashing or reading their targets.

### Package boundaries and resources

Tracked nested manifests define package boundaries independently of workspace
membership. The current side narrows that set to paths still present on disk, so
a deleted nested manifest no longer excludes its former subtree.

Manifest-named resources are resolved separately because they may live outside
the package directory and bypass `include` and `exclude`. Resources outside the
package are flattened to the archive root, matching Cargo's layout. Their
tracked state is queried explicitly so an untracked external README cannot affect
a verdict.

Path case is probed once at the workspace root and reused for member matching,
declared-resource resolution, and default README detection. Git's recorded
resource spelling is retained for blob and mode lookups. An inconclusive probe
chooses case-sensitive matching, which does not widen the selected content.

### Patch rendering

`diff` implements a bounded Myers line comparison. Its working set follows the
edit distance rather than total file size, and it falls back to a whole-file
replacement after the budget is exhausted. The fallback remains a correct patch
and cannot change the verdict, which was already established from object ids and
modes.

The renderer carries a file's content and mode together so an absent side cannot
accidentally receive a mode. Binary files receive presence and mode headers but
no textual hunk.

### Lockfile closures

`lockfile` parses only package identities and dependency references. Each
package with a binary or example target starts a breadth-first walk from the
source-less entry matching both its name and declared version. Name alone is
insufficient because another path dependency can share it; missing source alone
is insufficient because that dependency can also be source-less.

Dependency references are matched by every component Cargo writes: name, then
version and source when present. Parsing indexes these components and resolves
each textual edge to exactly one entry index once, so closure walks do not
search the package list. An unresolved or ambiguous edge makes the lockfile
incomplete and stops classification. A visited set terminates cycles. The root
is excluded from the result even if a dependency cycle reaches it.

The parsed work-tree lockfile is shared across all lockfile-bearing packages.
Historical lockfiles are shared by packages with the same anchor commit, so a
workspace-sized endpoint is parsed once rather than once per package.

Both endpoint target shapes come from explicit manifest target declarations and
Cargo's automatic target layouts, while respecting the manifest's `autobins` and
`autoexamples` controls. Work-tree automatic discovery considers only tracked
paths that remain present, so an untracked or ignored source file cannot turn a
library artifact into a lockfile-bearing artifact.

Each endpoint that has a binary or example target must have a lockfile resolving
the package at its corresponding declared version. An endpoint without either
target contributes an empty closure and does not require a lockfile. Missing or
incomplete required lockfile data stops classification because regenerating
historical resolution would violate the offline, no-full-resolution boundary. A
package absent from the baseline returns as new before lockfile comparison
because it has no historical artifact.

## Check and report

`check` and `report` consume the same `Classification`; neither recomputes release
rules. `check` renders failing package and group verdicts in text and optionally
as escaped GitHub workflow commands. Its packaging probe compares Cargo's list
with the exact work-tree selection produced by classification.

`check`'s verdict is read back from the rendered diagnostics rather than
recomputed from the classification, because every gating rule already appends a
line. A rule added to the rendering therefore cannot be reported without also
failing the check, which a second condition kept in step by hand would allow.

Some rules are properties of the manifests rather than of the released-content
comparison. An intra-workspace requirement must name the version its target
declares, which is checked against the normalized requirement `cargo metadata`
reports, so a bare requirement arrives as a caret one and both spellings that
name the version are accepted. A package whose public API exposes another
package must move incompatibly whenever that package does, compared against each
package's own anchor so an increment that landed in an earlier pull request
still counts.

Version-group discovery reads effective declarations from every tracked member
before the publishable projection is built. It resolves local path identity,
dependency aliases, workspace inheritance, dependency kind, and target-specific
tables without a full Cargo resolution. Exact requirements are validated from
the raw effective TOML because Cargo normalization loses suffix and compound
syntax. Validated declarations retain their source and target identities for
stale-version diagnostics. The resulting undirected edges are reduced to
deterministic connected components independently of release classification.

`apply` preserves each requirement's exact-or-compatible spelling while
rewriting the version it names, so applying a plan maintains both forms rather
than having to re-derive them. A hand-written requirement of the wrong form is
therefore caught by `check` rather than silently corrected, which is the right
split: it is a manifest edit, not a release decision.

Which dependencies are public is read in `metadata` from each package's
`allowed_external_types` allow-list, whose leading path segments name crates.
Matching follows `wildmatch`, the pattern language cargo-check-external-types
itself uses, against library target names taken from the target rather than
derived from the package name, so a `[lib] name` override cannot silently break
it.

An allow-list names the crate defining a type, while the release decision needs
the direct dependency supplying it. The two are bridged by growing each
package's exposed set to a fixed point: a dependency edge is public when what
that dependency exposes intersects what this package names, and its exposed set
then joins this package's own. The sets only grow and are bounded by the
workspace, so this settles; the bound is asserted rather than assumed.

`report` serializes the full package and group assessment, then writes patches
only where file differences exist. It removes any earlier `report.json` marker
before replacing the patch tree and writes the new marker through a same-directory
staging file after every patch succeeds. A failed rerun therefore cannot present
stale JSON and a partial patch set as one complete assessment.

## Plan resolution and application

`plan` owns both planning stages and the resolution shared between them. It first
resolves package and group entries into one target version per tracked version
target. Levels combine by taking the highest and matching explicit versions
coalesce. Mixed decision kinds, conflicting explicit versions, regressions, and
non-plain group targets fail before writes.

A plan's stage decides what resolution guarantees. A proposed plan may reach
packages it does not name, which is how a decision about one group member moves
the group. An expanded plan must resolve to exactly the set it names and must
already carry a version for each, because that document is what a caller
reviewed; reaching another package means the exact dependency graph changed after it
was written, and a surviving increment level would be re-resolved against the
manifests of the day. Both are rejected. The stage is matched on rather than
tested as a condition, so a new code path has to state which rule it wants.

`expand` and `apply` share that resolution and both read the same Git-tracked
version-target set. The resolved versions branch to expanded-plan output for
`expand`, and to manifest edits, writes, and lockfile processing for `apply`. The
resolved versions are not themselves the expanded plan: `apply` resolves a
proposed plan to the same shape without any expanded plan existing.

`apply` accepts plan targets and validates groups against the Git-tracked
version-target set. It parses and rewrites every affected
manifest in memory before writing any of them. All Cargo-visible members remain
rewrite candidates, including non-publishable, untracked, and ignored members,
because they may carry exact pins to a package being incremented. A dependency
requirement is changed only when:

* the entry has a path,
* that path resolves to the named workspace member,
* the resolved versions include the member, and
* the existing requirement does not already name the new version.

The last criterion is the same predicate `check` validates the requirement
convention with, kept in one place so the two cannot drift: `apply` must rewrite
exactly what `check` would reject, and leave exactly what it would accept.
Leaving an already-correct requirement byte for byte keeps an alignment, which
resolves the leading member to the version it already declares, from editing
that member's dependents under an unchanged version.

Paths are normalized lexically first and canonicalized only for link or
case-variant spellings, keeping the ordinary path free of filesystem calls.

After manifest writes, `cargo update --offline --workspace` refreshes an existing
workspace lockfile. A workspace-wide update avoids ambiguous bare package names
when the lockfile also contains a registry package of the same name. Empty plans
and workspaces without a lockfile skip the refresh.

## Diagnostics

All repository-controlled names pass through one quoting helper modeled after
Git's `core.quotePath` output. Quotes, backslashes, and control characters are
escaped so a path cannot forge another terminal line or GitHub workflow command.
GitHub command properties receive their additional delimiter escaping.

Subprocess stderr remains intact because Git and Cargo already quote their own
paths, and escaping the entire diagnostic would destroy its multiline structure.

Operational conditions are private `ohno::error` leaves carried through
`ohno::AppError`, preserving command, parse, and filesystem causes.

## Performance boundaries

Process startup and Git/Cargo work dominate end-to-end latency and are not useful
benchmark targets: they measure the host, repository, and caches more than this
tool. Benchmarks instead isolate deterministic in-process work whose cost can
scale with workspace size:

* bounded patch rendering, across low and high edit distances; and
* lockfile parsing and repeated dependency-closure walking, across small and
  large graphs.

Criterion tracks wall-clock behavior without subprocess or filesystem noise.
Callgrind is not used because both measured paths allocate variable-sized output
or parse state, and its fixed allocator model would omit a material part of their
cost. The benchmark-only surface is gated behind `private-test-util` and does not
participate in normal builds.
