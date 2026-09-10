# Release target verification implementation

`scripts/release/ReleasePublication.psm1` invokes a controller-built executable
against a separate candidate worktree before creating missing tags:

```text
release-target-check --manifest-path <candidate Cargo.toml> --commit <full immutable candidate SHA> --release-line <full immutable main-tip SHA> --package <name>@<version> [--package ...] [--verbose]
```

Relative manifest paths resolve against the process working directory. Commit
arguments are full lowercase Git object IDs, not revision expressions. Repeated
package names and repeated single-value options are errors.

For new-tag verification, orchestration pins freshly fetched main and supplies
that same SHA through `--commit` and `--release-line`. Existing tags do not pass
through this gate: orchestration preserves them and freezes their peeled commit
identity for binary builds.

## Ownership and evidence

Git subprocesses establish exact HEAD identity and first-parent membership.
Cleanliness checks reject staged edits, tracked edits, untracked files and index
flags that conceal worktree edits. Ordinary ignored build output is not evidence.
Cargo workspace and member manifests, and any present workspace lockfile, must be
tracked inside the candidate repository. This prevents ignored or external
manifests and lockfiles from silently supplying package identity. The release
checker decides when binary installation requires a lockfile; library-only
workspaces do not gain an additional lockfile requirement.

Identity comes from `cargo metadata --locked --offline --no-deps`. No dependency
resolution, lockfile repair or packaging verification is requested. HEAD and
cleanliness are checked before and after metadata and again after the release
checker, including failing invocations. These checks detect subprocess-induced
changes; the caller retains exclusive ownership of the worktree throughout
verification and its subsequent use.

## Verification boundary tests

Metadata decoding, resource ownership and package identity are tested independently
of process execution. This lets malformed or inconsistent metadata exercise the
same validation used by the executable without requiring a broken Cargo process.
Checker-result handling likewise has an observable diagnostic callback; production
sends those diagnostics to stderr, while unit tests verify verdicts, warning
forwarding and unexpected result rejection.

Real Git fixtures cover clean and concealed index state, corrupt or unavailable
history, and ownership failures. Native temporary directories avoid cross-filesystem
overhead when the checkout is mounted into another operating system. A Unix symlink
loop provides a deterministic filesystem lookup failure without racing a deletion
or relying on runner privilege. End-to-end tests continue to execute real Cargo
and the real release checker. These boundary tests supplement that path rather
than replacing it.

## Release policy reuse

The existing `cargo-release-plan` library owns package filtering, inherited values,
version anchors, version groups and locked binary installation closures. This
utility invokes its `Check` operation with the **candidate commit itself** as
`base`, not the later release-line tip. Since the worktree is that clean commit,
there are no pending branch edits or external-baseline increments to accept.
Each package is compared against the anchor of its version on its own first-parent
history. The version anchor is unchanged even when a tag uses a later equivalent
snapshot. The CLI also permits an older first-parent candidate; using its own
baseline avoids comparing it against later package versions. This capability does
not impose current release policy on authoritative historical tags.

GitHub authorization, fetching main, immutable target selection, tag preservation,
and publication ordering remain in the
[workflow implementation](../../../.github/workflows/implementation.md).
