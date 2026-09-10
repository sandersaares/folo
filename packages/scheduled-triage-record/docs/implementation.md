# Local triage records

## Responsibility

This nonpublished utility validates data supplied by the Local App triage role.
Its behavioral owner is the
[scheduled workflow design](../../../.github/workflows/design.md), with operational
contracts in [Local failure triage](../../../docs/scheduled-triage.md). AI performs
diagnosis and causal comparison; this utility does not classify symptoms or assign
problem identities from hashes.

The JSON protocol accepts one request on standard input and returns one complete
response. Invalid input emits a diagnostic and unsuccessful exit rather than
partial success output. Network, native App operations and durable file transactions
belong to the PowerShell boundary.

## Completion basis

Analysis binds an immutable primary reporter revision. A separate completion basis
contains the exact attempt's complete API job/step inventory, its digest, and
supporting revisions restored from the same committed reporter index. The source,
controller and run-attempt identities must agree. API execution-status conflicts
are blockers; missing original diagnostics are not overwritten by newer data.

Dispositions cover the complete verified job/step set and every unsuccessful result
and gap in the primary and supporting revisions. Supporting-revision dispositions
explain collection recovery and diagnostic differences. Citations address the
primary evidence, the API evidence, or an explicitly identified supporting record.
Reading support does not acknowledge that supporting revision or another attempt.

Problem snapshots retain their contributing completion bases so a crash between
problem publication and final triage publication does not lose diagnostic context.
Original reporter evidence remains referenced by its exact run/attempt/digest.

## Identity and publication

Numeric repository ID and issue number identify a canonical problem. Local proposal
keys connect an analysis's jobs before GitHub assigns an issue number; publication
operation IDs identify writes, not causes.

Occurrence transitions retain all evidence and complete required scopes. A supported
recurrence requires prior resolution, newer execution and source applicability.
Historical evidence attaches to its existing occurrence without reopening a newer
one. Identical contributions are idempotent, including when the AI reconsiders a
refreshed index after partial multi-problem publication.
Already attached diagnostics do not establish a new recurrence: a new recurrence
disposition still validates resolution, ordering and source applicability.
Replaying a known publication operation preserves its completed transition.

Required-scope identity includes the operation as well as package, check, platform
and typed replay. Optional attribution does not guarantee that every execution
qualifier has a dedicated field, so changing only operation text conservatively
advances required scope. Citations remain evidence references rather than execution
requirements.

The shared [run-record library](../../scheduled-run-record/docs/implementation.md)
owns canonical JSON, byte framing, lossless pagination and committed page indexes.
Role-specific envelopes stay distinct, while serialization and recovery primitives
are reused. Only the publication adapter can confirm external writes and advance a
triage root. Analysis validation alone is not publication, resolution or repair
admission.

## Validation workloads

Native protocol tests exercise complete producer JSON, repeated canonical hashing
and publication round trips. These workloads exceed the interpreter test budget.
Small typed tests retain Miri coverage for scope/citation validation, complete-index
and occurrence matching, and execution ordering. Production behavior and runner
deadlines are identical; only the full protocol workloads are native-only.
