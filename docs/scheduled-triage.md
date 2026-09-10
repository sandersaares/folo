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

Persist publication intent before external writes. Stable operation identities,
known GitHub IDs and complete paginated lookup recover uncertain outcomes.
Append-only detail pages precede their bounded root/index checkpoint. A single
owned triage root comment on each run issue identifies its committed analysis.
Problem bodies contain their separately owned canonical record and detail links.
Preserve human text, reporter records and repair-session records.

An analysis completes only after all required problem updates are confirmed.
Partial publication, stale source/index, changed generation or ambiguous ownership
keeps it pending or blocked. A new attempt arriving during analysis is a new input,
not covered by completion of the claimed revision.

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
| `scan` | Empty data. Returns complete-read status, pending revision descriptors, backlog/oldest evidence and a content-addressed snapshot ID when the role is registered. No source is inspected and no analysis start is charged. |
| `state` | `action` naming a `triage-*` transition and `fields` containing its arguments. Returns the persisted state. |
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
`jobs`, `results`, `gaps`, `problems`, and `support_dispositions`.
Status is `in-progress`, `blocked` or `complete`. A non-complete entry explains why.

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
precede package/check/source selection.

Matching is `new` with separation reasoning and closest full-read candidates,
`existing` with canonical issue number, expected generation/scope revision, target
generation, record/full-read digests, relation and reasoning, or `ambiguous` with
candidate comparisons. Relations are `repeat`, `recurrence` and `historical`.
A candidate comparison includes issue number, full-read digest and reasoning.
Ambiguous matches cannot complete or create competing canonical problems.

State transitions are role-specific. Scans use `triage-acquire-scan`,
`triage-record-scan` and `triage-release-scan`; new ownership uses `triage-claim`.
Resume uses `triage-reconcile-dispatch` when accepted work is proven quiescent,
then `triage-reserve-continuation`, `triage-begin-dispatch` and
`triage-accept-dispatch`. `triage-complete-dispatch` records turn completion;
`triage-retire` separately requires complete publication and verified native
quiescence. Publication helpers own their operation/document transitions.
`triage-register-profile`, `triage-set-mode` and release of repair-scope holds
require explicit operator approval.
