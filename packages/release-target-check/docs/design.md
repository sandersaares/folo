# Release target verification

This nonpublished utility supplies the read-only source-verification boundary
before the repository's release scripts create missing package tags. The
user-visible release contract belongs to
[release-equivalent snapshots](../../../.github/workflows/design.md#release-equivalent-snapshots),
not to this implementation package.

The caller supplies a freshly fetched main-tip commit as both the candidate and
release line, together with the frozen package versions it intends to tag.
Verification accepts only a clean checkout of that candidate on the supplied
release line's first-parent history, with matching publishable packages and passing
release invariants.

Existing tags remain authoritative. Their preservation and subsequent use belong
to orchestration; they are not retroactively gated by this utility's current
release-policy checks. See
[publication and recovery](../../../.github/workflows/design.md#publication-and-recovery).

The utility does not establish remote branch provenance: the orchestration caller
owns fetching and pinning main, exclusive use of the candidate worktree, and every
subsequent GitHub write or build. It does not move tags, publish, repair manifests,
refresh lockfiles, or infer a release baseline from the current branch.
