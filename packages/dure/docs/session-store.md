# Session store

How live sessions are recorded, found, and reaped. Part of the `dure`
[implementation guide](implementation.md).

## What the store is

The store holds the state of live sessions for one user on one machine: what
exists, who owns each one, and how to reach it. Everything above it works in
those terms — claim an id, publish a session, read one, list them, remove one
that still belongs to a named owner — and nothing above it needs to know that
the real implementation is a directory of files.

Two invariants belong to the store rather than to any one implementation:

* A session it hands back is the session the caller asked for. A record whose
  own id disagrees with the key it was found under is not surfaced.
* Removal is compare-and-delete. Ids are reused, so a delete removes only the
  owner state the call inspected and never a replacement that has since taken
  the same id.

## Claimed and published

A session id passes through two states, and the store keeps them in one place so
they cannot disagree.

A **claim** is what a supervisor takes before it has everything a session record
needs. It names the process that made it, which is what lets a claim left behind
by a supervisor that died mid-initialization be reaped instead of occupying the
id forever. A claim is reported as absent to everything except that reaping.

A **published session** is the record clients and commands act on.

## The filesystem implementation

Per-session files under the PAL store root — by default the per-user
`LocalAppData` known folder, subdirectory `dure` — record id, supervisor pid,
supervisor process creation time, pipe name, launch directory, command, and
session start time. One file holds either a claim or a published session, and
either way it names the process it belongs to.

Liveness and termination open the pid once and verify the process creation time
on that handle, then confirm that it is still running. A missing, exited, or
mismatched process makes the record stale; failure to inspect the process is an
error and does not delete the record. A connect or pipe failure is not evidence
of process death.

Id allocation is filesystem-coordinated so two concurrent `run` invocations
cannot take the same id.

Every delete is conditional on the file still naming the process the caller
means to remove; the store offers no unconditional delete to reach for by
mistake. Deciding and deleting are one step rather than two: the record is
opened once, and both the ownership check and the removal go through that
handle. Windows removes the file the handle addresses, so a name that has come
to mean a different session in the meantime is never the thing removed — the
delete lands on the file that was inspected, which by then is nobody's. Losing
that race to another deleter is reported as success, since the outcome asked for
has happened; a file this process genuinely may not delete is still an error,
because the two are distinguished rather than conflated.

The Win32 record-file mechanics live under the filesystem implementation rather
than beside it, because they are meaningful only there.

## Isolation

Session isolation is a property of the per-user store root, not of anything the
records themselves carry: they are trusted by `list`, `resume`, and `kill`, so a
store another user could write would be a store another user could redirect an
attach through. The released tool therefore has no way to be pointed at a
different root; the override exists only in test builds.
