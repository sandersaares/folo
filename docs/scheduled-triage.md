# Local failure triage

## Responsibility

The Local App's selected AI model diagnoses hosted run evidence and compares causes
with the complete scheduled problem index. The deterministic helpers collect data,
validate its provenance and enforce durable publication; they do not decide whether
two symptoms share a cause. Triage can inspect source but cannot edit it, create a
repair PR, confirm a repair, merge, or activate an automation.

The owning [scheduled validation contract](scheduled-validation.md) distinguishes
run analysis from problem resolution. A run is triaged only after every reportable
evidence revision has complete analysis and reconciled problem publication.
Completing analysis does not authorize a new repair: `reserve-attempt` continues
to reject new admission with `ai-triage-unavailable`.

## Independent configuration

`scripts/scheduled/triage-policy.json` controls only triage. Repository and account
identity come from the shared reviewed policy. Repair configuration remains in
`scripts/scheduled/policy.json`; registering triage does not reset a repair profile,
claim, dispatch or budget.

The triage default is unenrolled observe mode with no model selection. Installing
the role creates only a disabled native entry, after the operator selects its
actual Local environment and personally funded model. Model/effort, timezone,
account, enrollment and consent are installation decisions, not portable defaults.
An active role requires a matching approved profile.

The saved App prompt and the executable skill are distinct inputs. Registration's
`prompt_digest` identifies the actual approved native prompt using
`Get-ScheduledTriagePromptDigest`. Each poll and accepting continuation supplies
concrete `profile_observation` facts (`automation_id` and the digest of the prompt
just read through supported native metadata). The helper binds those facts to its
scan or dispatch token and native session. A new dispatch cannot reuse the previous
dispatch's proof. Missing or changed observations block admission and appear as
role-specific health drift; they do not initialize or update registration.

Controller identity includes the executable skill, shared write/validation modules,
native helper sources and applicable repository normalization contracts. Batched
Git hashing reads current working files with their declared text normalization.
CRLF/LF checkout differences do not create drift, while dirty script/skill edits
and normalization-contract changes remain visible. Hosted health compares that
identity and the Local scan's owned prompt observation with the approved profile;
it does not query or invent App metadata.
Published health uses a fingerprint of the owning scan binding, not the local
authorization token itself.
Native and published observations retain strict scalar representations: schema
versions are supported integers, identifiers are nonempty strings, and digests are
canonical strings. Malformed present observations remain invalid even when a sibling
observation is absent; absence does not establish a successful profile observation.

The default three-hour schedule has a distinct offset from repair. This separates
the entries, not their correctness: publication is the handoff, never timer order.
One analysis start claims exactly one run/attempt/evidence digest. Multiple problems
or jobs in that revision do not consume additional starts. The daily start allowance
permits one new revision per scheduled poll; empty scans consume no analysis starts.
Only one analysis can remain registered at a time.

Continuations are separately charged before delivery and bounded both per day and
per analysis. The daily allowance permits a retained analysis to make progress at
each poll; the lifetime allowance bounds repeated diagnosis/recovery across polls.
These are admission limits, not token or spending caps. Backlog and oldest pending
evidence remain visible when a budget or blocked analysis delays other work.
The scan-health grace accommodates a missed poll and scheduling delay.

## Evidence and reasoning

Scan all open and closed run-level issues and refresh retained issue IDs. Restore
the reporter-owned pages against the committed index before deciding which exact
revisions remain unprocessed. Closed state, a label or a green rerun cannot erase
an unprocessed failure. Incomplete retrieval remains a blocker.

Completion also requires the exact claimed attempt's fully paginated API job/step
inventory. A partial original collection may use fuller committed reporter
revisions of that same attempt as supporting evidence. The primary digest remains
immutable. Persist the supporting digests and complete API snapshot/digest in the
analysis and problem detail so recovery can reconstruct the completion basis.
Controller/source mismatches and conflicting execution status remain blocked.
Every original gap and supporting diagnostic requires explicit disposition; there
is no keyword-based gap classifier or last-write-wins evidence merge.

Supporting evidence is read, not acknowledged. Each reportable revision still needs
its own complete triage entry, and a different attempt cannot supply the claimed
attempt's missing execution proof.

Every unsuccessful job and relevant failed step needs an evidence-backed
disposition. A job can contain independent problems, while a shared cause can span
jobs, runs, packages and checks. Setup failures explain blocked work; they are not
bugs in code that did not execute. Missing decisive diagnostics and ambiguous
matches remain unresolved. Mutation timeouts are not caught mutations.

The AI reads summaries from the complete open/resolved problem index and reads
plausible candidates fully. It records causal match or separation reasoning.
Digests identify evidence revisions, never semantic problems. A canonical problem
is identified by numeric repository ID and issue number.

## Ownership and recovery

Triage has a separately registered native session and durable claim under the
executor's existing short local transaction boundary. A fresh polling session
resumes that exact session after verifying native ownership and activity. Elapsed
age or idle status alone does not establish abandonment. Unknown native outcomes
retain ownership and require reconciliation.

Retained checkpoints, comparison baselines, publication plans and health intents
carry integrity digests. Restoring state checks those digests before transport.
Typed checkpoint validation and tool preparation run outside the short lock; the
validated owner and checkpoint digest are rechecked under the lock before use.
Observed creation IDs do not rewrite immutable intents.

Cached snapshots are payloads owned by a scan, an accepted analysis's working view,
or its checkpoint. The helper establishes durable pins before returning a usable
snapshot. Replacing a working view does not release its checkpoint, and a poll
cannot release another session's uncheckpointed working view. Atomic installation,
pin handoff and cleanup are serialized with state changes. Cleanup removes only
provably unowned payloads and abandoned temporary writes, never active analysis
data merely because time passed.
Replacing an expired scan runs cleanup after committing its new owner, without
depending on that poll subsequently saving or releasing a snapshot.

Persist publication intent before external writes. Stable operation identities,
known GitHub IDs and complete paginated lookup recover uncertain outcomes.
Append-only detail pages precede their bounded root/index checkpoint. A single
owned triage root comment on each run issue identifies its committed analysis.
Problem bodies contain their separately owned canonical record and detail links.
Preserve human text, reporter records and repair-session records.
Problem state changes also retain the prepared issue state as a preimage; an
intervening external transition requires reconsideration before sending an update.

An analysis completes only after all required problem updates are confirmed.
Partial publication, stale source/index, changed generation or ambiguous ownership
keeps it pending or blocked. A new attempt arriving during analysis is a new input,
not covered by completion of the claimed revision.

Retirement requires reconciled publication, fresh restoration of its committed
remote proof and verified native quiescence. It removes the large local analysis
payloads while retaining exact revision/native identities, start/continuation
accounting and completion references. This small identity/accounting history is
durable rather than an evictable cache. The full completed analysis remains in its
committed GitHub documents. Missing remote proof is an explicit recovery problem,
not permission to recreate the analysis or reset its budgets.

Repeated active problems receive evidence in place. Recurrence requires causal
agreement, prior applicable resolution, attempt ordering and source applicability.
Executing pre-fix source again is not proof of recurrence after a fix. Late old
evidence cannot reopen or resolve a newer occurrence. Closed state without
established resolution requires human review.

Triage never silently retargets or consolidates an owned repair. Scope-invalidating
diagnosis requires reconciliation with that ownership. Complete analysis may still
record a human repair decision without claiming that the problem is repair-ready.

## Readiness and health

Triage and repair publish independent observations on the existing rolling
coverage/health issue. Report last successful scan, backlog and oldest pending
evidence, registered work, blockers and profile drift. Setup is not a scan.
An unenrolled inactive role is disabled/not expected; its absence neither fails
hosted operation nor conceals missing coverage or failed reporting.
Setup restores stage-specific, integrity-checked role records before deciding that
a native entry is absent. Native automation identifiers remain opaque strings;
they are not GitHub issue numbers.

Deterministic tests establish serialization, discovery and transition behavior.
Actual model diagnosis quality, personal billing, unattended consent, native
session reuse and machine/restart behavior require a separately authorized
installation exercise. Executable feature readiness does not establish those
installation facts or authorize activation.

## Helper interface

The native skill invokes `Invoke-ScheduledTriageRequest` in
`scripts/scheduled/LocalTriage.psm1` with an absolute JSON request artifact.
The envelope has `action`, the existing `executor_id`, and `data`.
No request initializes enrollment or permits a repair action.

| Action | Data and result |
|---|---|
| `scan` | Empty data observes without caching. A current poll supplies `scan_token` and its native `session_id`; an accepted analysis supplies worker identity instead, without taking the sender's scan. An owned read returns a durably pinned content-addressed `snapshot_id` along with complete-read status, pending revisions and backlog/oldest evidence. No source is inspected and no analysis start is charged. |
| `state` | `action` naming an allowed polling/analysis lifecycle transition below and `fields` containing its arguments. `triage-acquire-scan` and `triage-accept-dispatch` include the current concrete `profile_observation`, or explicit null when native observation is unavailable. Returns the persisted state. |
| `recovery` | Empty data. Reads retained analysis and pending operation visibility even when partial publication prevents a clean inbox. Returns native ownership and a stable continuation evidence key; performs no writes. |
| `evidence` | Worker identity, `snapshot_id`, optional zero-based text `offset`. Streams JSON containing the exact primary evidence and its completion `basis`; continue until `next_offset` is null. |
| `index` | Worker identity, `snapshot_id`, nonnegative `offset`. Returns summaries and `next_offset`; read every batch, including an empty index. |
| `problem` | Worker identity, `snapshot_id`, canonical `issue_number`, optional text `offset`. Streams the complete semantic record/discussion. A full-read receipt is recorded only after every page is delivered in order. |
| `checkpoint` | Worker identity, `snapshot_id`, structured `analysis`. Validates and persists the analysis with the snapshot's complete index, primary evidence and completion basis. |
| `prepare-problem` | Worker identity and a proposal `problem_key`. Returns `prepared`, `native-create-issue` with the exact supported native payload, or `reanalysis-required` after index changes. |
| `native-issue-result` | Worker identity, `operation_key`, observed `issue_number`. Persists the known ID before readback; it does not itself confirm publication. |
| `publish-problem` | Worker identity and `problem_key`. Reconciles its fixed detail/root plan and returns canonical evidence linkage. |
| `finish` | Worker identity. Publishes validated progress/completion, rechecks the full current failed-revision set and reconciles run presentation. |
| `health` | Current role `scan_token`. Reconciles that role's observation on the existing rolling health issue. |

Worker identity is `analysis_id`, actual `session_id`, `claim_token` and accepted
`dispatch_token`. A fresh polling session does not impersonate the registered
worker when it resumes that worker through native messaging.

The analysis object has `schema_version`, `analysis_id`, increasing `checkpoint`,
the exact `revision`, `status`, `reason`, `index_digest`, `considered_issues`,
`jobs`, `results`, `gaps`, `problems`, `support_dispositions`, and optional `workflow`.
Status is `in-progress`, `blocked` or `complete`. A non-complete entry explains why.

A proved-empty exact-attempt inventory requires a `workflow` disposition grounded
in its API conclusion. It accounts for cancellation or failure before jobs existed;
it does not invent checker/source defects. Unavailable pages, inconsistent totals
or unexplained workflow outcomes remain blocked. The same disposition shape is
used as for jobs, with a citation to `/api_evidence/workflow_conclusion`.

Each job has `job_id`, a `disposition`, and step entries with `number` and
`disposition`. Results identify their zero-based `index`; gaps identify their
original `gap` string. A result/gap's optional `source_digest` selects a supporting
revision; absence denotes the primary. Supporting dispositions are keyed by their
exact digest.

A disposition has `kind`, `explanation`, `citations` and `problem_keys`. Kinds are
`actionable`, `duplicate`, `infrastructure`, `blocked`, `cancelled` and `unresolved`.
Only explained blocked/cancelled consequences can complete; unresolved analysis
cannot. Citations are JSON pointers into the primary evidence, `/api_evidence`,
or `/supporting_revisions/<index>/evidence`. They must reference retained data.

A problem proposal has a local `key`, `diagnosis` and `matching`. Diagnosis includes
title, summary, cause, category, repair disposition/reason, citations and the complete
scope array. Categories are `code`, `infrastructure`, `operator`, `nondeterminism`
and `unknown`; repair dispositions are `actionable`, `needs-human`,
`operator-recovery` and `unresolved`. A scope identifies its operation, optional
package/check/platform/replay and citations. Absence is explicit for failures that
precede package/check/source selection. Preserve execution qualifiers in the scope;
operation changes participate in required-scope revision even when the optional
attribution fields are unchanged. Citation-only changes do not change required scope.

Matching is `new` with separation reasoning and closest full-read candidates,
`existing` with canonical issue number, expected generation/scope revision, target
generation, record/full-read digests, relation and reasoning, or `ambiguous` with
candidate comparisons. Relations are `repeat`, `recurrence` and `historical`.
A candidate comparison includes issue number, full-read digest and reasoning.
Ambiguous matches cannot complete or create competing canonical problems.

The role's state entry allows `triage-read` and explicit lifecycle transitions.
Scans use `triage-acquire-scan`,
`triage-record-scan` and `triage-release-scan`; new ownership uses `triage-claim`.
Resume uses `triage-reconcile-dispatch` when accepted work is proven quiescent,
then `triage-reserve-continuation`, `triage-begin-dispatch` and
`triage-accept-dispatch`. `triage-block` retains a specific blocker even before a
typed checkpoint exists. `triage-complete-dispatch` records turn completion;
`triage-retire` separately requires complete publication and verified native
quiescence; the request entry point verifies current committed proof before
compacting the retired record. Claims inherit their scan's snapshot pin. Accepted
worker scans update only that worker's working-view pin; checkpoints retain their
own pin. Publication and cache helpers own their operation/document/pin transitions.
Internal read receipts, typed checkpoint acceptance and publication confirmation
are available only through their validating high-level actions, not raw role state
requests. Operator and internal transitions are rejected before shared-state or
GitHub access.

`triage-register-profile`, `triage-set-mode` and release of repair-scope holds use
the existing `scheduled-local` / `Invoke-ScheduledLocalAction` operator surface
with explicit approval outside the triage role. An approval field records that
decision; it is not a credential the role can supply to authorize itself.
These entry-point rules do not provide OS isolation from the user's trusted process.

Profile observations are bound when a scan is acquired or a dispatch accepted;
an initial claim transfers its same-session scan observation to the new dispatch.
Registration changes neither create a successful observation nor refresh health.
