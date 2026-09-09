# Scheduled validation and local remediation

## Purpose and responsibility

GitHub Actions runs deterministic deep validation at an immutable `main` SHA and
files a run-level **Scheduled validation failed** issue with execution evidence.
That issue requests analysis; it is not itself a diagnosed problem or a repair task.
Evidence remains available while the local executor is unavailable.

Hosted reporting ends at durable run-level intake. It does not diagnose shared
root causes, assign semantic problem fingerprints, create `scheduled-finding`
issues or invoke an AI session. The Local helper boundary rejects new repair
admissions, including when policy mode and allowlists would otherwise permit them.
Raw execution evidence is not source-edit authorization.

AI triage and repair are separate downstream responsibilities. The consumer
contract below describes the required handoff; it does not provide an executable
triage automation or a way to bypass the new-admission restriction.
Existing registered repairs retain their state and can be reconciled and continued
in their **repair session**: a visible issue-linked Local App session with its own
worktree, branch and AI agent. Final approval and merge remain human actions.

```text
Actions: fixed source -> complete job inventory + available checker evidence
                                                    |
                     "Scheduled validation failed" issue + durable evidence pages
                                                    |
                            end of hosted intake; no automatic repair admission

Existing registered repairs -> retained Local session -> human review/merge
                                                    |
                                         hosted scope confirmation
```

This is repository-level orchestration, not a per-PR timer, cloud coding agent,
hidden process or organization-billed inference workflow. A paused session
keeps its ownership. Idle status or elapsed time never establishes that its work is
safe to replace.

The workflow [design](../.github/workflows/design.md) and
[implementation guide](../.github/workflows/implementation.md) describe hosted
execution, reporting and gate ownership. [Testing](testing.md) remains authoritative
for mutation quality and reviewed skip criteria. The shared versioned contracts and
reviewed defaults live under `scripts/scheduled/`; policy is not inferred from App UI
state.

## Problems and triage

These are the requirements for a downstream AI triage consumer. Hosted intake
supplies the run records and evidence pages; it does not create triage or problem
records. Neither a manually supplied record nor a label enables new Local repairs.

A **problem** is a failure that can be diagnosed and addressed independently:
for example, a Miri defect, a missed mutation, or a dependency-download failure.
Triage tracks each problem in a GitHub issue labelled `scheduled-finding`.
The run-level issue labelled `scheduled-run-failure` instead requests analysis of
a failed workflow; it can link to several problem issues.

Repeated evidence of the same unresolved problem updates its existing issue.
Unrelated failures in the same run remain separate. A confirmed recurrence reopens
the problem issue; delayed evidence from before resolution does not. Records carry
an occurrence number (`generation`) so confirmation for an earlier repair cannot
resolve a later recurrence.

Each active problem has at most one repair session and one open repair PR.
Duplicates cannot acquire competing sessions, and reopening an issue does not
bypass retained ownership of earlier work. Capacity limits may delay independent
problems, but do not combine their issues.

### Run-level intake and AI triage

The hosted completion reporter publishes the run's identity, attempts, source,
available manifest, complete API job/step inventory, parsed results and diagnostic
references. It inventories jobs before parsing checker artifacts, so planning,
checkout, setup and download failures remain visible even without a result file. Its
run-level issue is deduplicated by repository, workflow and run identity. Repeated
reporting of identical attempt evidence is a no-op; a rerun appends evidence and makes any
new unprocessed failure eligible for triage. Consecutive failing runs can have
separate intake issues without creating duplicate problem issues.

The triage automation scans all unprocessed run/attempt evidence, not just newly
created issues. AI analysis accounts for every unsuccessful job and relevant failed
step, including planning, environment setup, downloads, checker execution and
artifact handling. It reads the evidence and relevant source as needed to identify
causes and compare them with existing issues. A job boundary does not define a
problem. Programmatic parsers supply evidence and candidate
matches; they do not replace this semantic analysis.

Triage extracts every independently supported problem rather than selecting the
first error or creating one catch-all "workflow is red" issue. A job can contribute
several problems; a problem can affect several jobs or runs. A failed prerequisite
is reported as its own problem with the dependent scope marked blocked, not as a
code defect in each package that never ran. Missing or uninterpretable evidence
produces an explicit triage-required or evidence-collection problem; it is not an
empty successful result. Intentional skips and cancellation causes are accounted
for without inventing defects in unexecuted code.

All semantic triage uses personally funded Local App AI, never hosted inference.
If analysis cannot establish a cause or a safe match, retain the narrowest supported
symptom and expose the uncertainty. Incomplete analysis is not repair-ready.
Further diagnosis can refine the classification or establish a shared cause; it
must preserve issue history and reconcile existing repair ownership before
consolidating issues.

Only problems marked actionable by completed triage are eligible for repair.
Infrastructure recovery, permission decisions and unresolved classification have
their own dispositions; they are not permission to fabricate a code patch.
New evidence appended during a repair does not create another session. A changed
diagnosis that invalidates the repair scope blocks or informs the existing repair session
instead of silently replacing its task.

A grouped problem retains the affected packages, checks, platforms and replay
conditions as a set. Repair admission validates the entire required scope against
policy, not just a representative failure. The repair gate and resolution
criteria cover that set. A passing replay of one symptom cannot resolve other
linked failures without evidence that the confirmation covers their shared cause.

### Marking a run triaged

The GitHub Actions run has already finished when triage begins. Triage updates
GitHub issues; it does not control whether that Actions run can finish.
The following structured JSON records are stored in GitHub, not just in a local
file or the AI's conversation:

| Record | Writer and location | Contents |
|---|---|---|
| Run record | Hosted reporter: compact identity/checkpoint in the run-level `scheduled-run-failure` issue body; complete revisions reconstructed from its evidence comments. | The body shows counts and an evidence link without an unbounded history list. The publication checkpoint binds all complete revision identities and their comment references. |
| Run evidence | Hosted reporter, in deterministically paginated comments on the same run-level issue. | Source/check identity, available manifest/results, every job and step, bounded diagnostic excerpts and original log/artifact references, and explicit evidence gaps. Page metadata binds the run, attempt, digest and page order. |
| Triage record | Local triage automation, in one dedicated automation-owned comment on that same run-level issue. | Analysis status for each exact run ID, attempt number and evidence digest, plus each failed job's problem-issue links or explanation of a blocked/cancelled consequence. |
| Problem record | Local triage automation, in the body of each canonical `scheduled-finding` issue. | Diagnosis, supporting evidence, affected scope and whether automated repair may proceed or needs human action. |

Each writer updates its own record without replacing human discussion or another
writer's data. The triage comment starts with `[Copilot speaking]`; updates reuse
that comment rather than posting another status comment each poll.

Hosted publication first establishes the run issue, then reconciles all evidence
pages, and finally updates the run index with the confirmed page references.
An interrupted publication leaves recoverable identity, not a claim that missing
pages were persisted. Identical run/attempt/digest/page identities are reused after
lost responses. Body/comment limits cause pagination, not dropped jobs or results.
Captured logs are bounded and explicitly marked when truncated; the original
Actions log remains linked. Expired or inaccessible evidence is recorded as a gap.
The body checkpoint is compared with reconstructed comments before publication;
deleted committed pages cannot silently disappear from the indexed history.

Before marking an analysis complete, the triager must finish creating any new
problem issues and updating matched problem issues with the evidence, diagnosis
and repair disposition from this analysis. The resulting issue numbers are saved
in the triage record. If a GitHub create/update result is uncertain, the triager
reconciles that operation before recording completion. Creating no new issues is
valid when existing problem issues already account for every failure.

| Analysis status in the triage record | Meaning and next action |
|---|---|
| `in-progress` | Analysis or problem-issue updates are unfinished. The run-level issue stays open without `scheduled-triaged`; local state retains the triage session and progress so a later poll can resume that session. |
| `blocked` | Analysis cannot continue, for example because required logs are unavailable or GitHub access failed. Record the reason in the triage comment and health output. Keep the run-level issue open and unlabelled; resume only after the condition is resolved. |
| `complete` | Every failed job has been explained and all required problem-issue changes are confirmed. This entry covers only its recorded run, attempt and evidence digest. |

A problem may be fully analyzed but require a human decision before repair, such
as approval for a design change. The problem issue records that reason and remains
open; automation must not start or continue its repair until the operator releases
the restriction. This does not prevent marking the run's *analysis* complete once
all failures are explained and their problem issues are up to date.

The run-level issue receives `scheduled-triaged` and is closed when every failed
attempt/evidence revision in its run record has a matching `complete` entry in its
triage record. Closing that issue does not resolve its linked problems.

To find unfinished analysis, a triage consumer must paginate the configured repository's
issues API with `state=all` and `labels=scheduled-run-failure`, with no creation-date
filter. It also fetches run-level issue IDs retained in local triage claims, so a
removed label cannot abandon owned work. For each selected issue, it reads the run
record from the body, validate/reassemble its referenced evidence pages, and read
the triage record from its separately owned comment. It paginates comments as needed
and compares attempt numbers and evidence digests.
This query includes open and closed *run-level issues*, not every issue in the
repository and not the `scheduled-finding` problem-discovery query described below.
Missing or incomplete triage entries require analysis regardless of the issue's
label or closed state. API errors or malformed records are reported as blockers,
not interpreted as an empty queue.

| Incoming evidence | Run issue and triage result |
|---|---|
| Reporter retries the same attempt with identical evidence. | Preserve the triage record, label and closed state. |
| A rerun of the same run fails with a new attempt number. | Reopen the same run issue and remove `scheduled-triaged`. Analyze the new attempt, retaining earlier triage entries. |
| More evidence changes an already reported failed attempt's digest. | Reopen and remove the label; the previous complete triage entry does not cover the changed evidence. |
| A rerun passes. | Record the pass without erasing unprocessed failure evidence or claiming that earlier failures were triaged. |

The reporter clears stale presentation when it appends unprocessed failure
evidence. The triager rereads the reporter's run record before marking triage complete and
reconciles the label/state with that record. If evidence arrives concurrently,
the next poll still detects the unmatched revision even if a label update races.
An old analysis can never acknowledge the new attempt merely by closing the issue.

### Finding existing problems

Problem identity is the GitHub repository's numeric ID plus the canonical problem
issue number. It is assigned by issue creation and does not change when diagnosis
or wording improves. A log fingerprint does not assign semantic identity.

At each triage scan, the helper paginates GitHub's repository issues API with
`state=all` and `labels=scheduled-finding`. It validates the owned problem records
and builds a local index containing issue number, title, open/resolved state,
failure category, affected packages/checks, diagnostic summary, suspected or
established cause, prior alternative symptoms and evidence links. It also refreshes
registered issue IDs so removing a label cannot silently forget owned work.
Incomplete pagination, inaccessible records or malformed ownership blocks new
problem publication rather than treating the index as empty.

The AI receives compact summaries of that complete index, in bounded batches when
necessary. It reads the full records, discussion and diagnostic evidence for
plausible matches. Text/scope lookup can prioritize candidates, but exact digest,
package or check matches are not mandatory filters: a shared cause can affect
different tests, packages or check families.

For each problem in the failed run, the AI records either a match to an existing
issue with its reasoning, or a proposed new issue with the closest candidates and
why they differ. Only evidence of a common cause justifies combining different
symptoms. A definite repeat updates the active issue; a confirmed post-resolution
recurrence reopens it. Uncertain matches remain linked for further diagnosis and
are not admitted as competing repairs. Immediately before creating an issue, the
triager refreshes the index and reconciles its persisted publication intent so a
retried API call cannot create a duplicate.

Deterministic SHA-256 digests identify exact evidence revisions, not problems.
The helper hashes a versioned canonical JSON record of the run/attempt, immutable
source/check identity, job/step results, diagnostic-content digests and explicit
evidence gaps. Object keys are sorted and job/step collections use their stable
IDs; raw logs remain available for interpretation. Triage annotations, issue
state, fetch timestamps and human discussion do not enter that digest. AI
deduplication can match different digests, while a changed evidence digest always
requires its own completed analysis.

For Miri, replay scope and problem identity are different concepts. Several
unrelated failures can require the same target-level replay. A target, shard or
seed range alone must not collapse distinguishable failures, and interleaved output
must not be used to invent test/seed attribution.

| Evidence | Treatment |
|---|---|
| The same Miri defect fails in consecutive runs. | Append evidence to the existing problem issue, even while its repair is active. |
| Several tests in `foo` fail from an established shared Miri defect. | Track one problem with all affected tests and evidence; keep unrelated defects separate. |
| Different Miri diagnostics occur in the same Cargo target. | Track distinct problems unless a shared cause is established. |
| Mutation shard 3 cannot download dependency `bar`. | Track an execution problem identifying the download operation and failure reason; mark mutation coverage blocked, not caught or missed. |
| Other shards fail from that same download problem. | Add their evidence to the same issue; shard numbers are evidence, not independent problems. |
| One run reports A and B; the next reports B and C. | Update B and report C if unmatched. A remains governed by its own resolution evidence, not the workflow's red/green state. |

An unrelated later green job, an absent symptom or the appearance of a
different problem does not establish resolution. Resolution requires applicable
evidence for the current occurrence of that problem. Infrastructure recovery or an explained
no-change disposition need not manufacture a source patch, but must satisfy their
explicit resolution policy. Broad workflow health and complete-coverage status
remain separate from each problem's lifecycle.

## Validation and release contract

Ordinary tests, compilation, Clippy, docs, feature/dependency, external-type,
version/SemVer and integration checks remain on the normal validation path.
PR/push validation is shallow. Scheduled workflows own mutation testing, ordinary
and many-seed Miri, and careful checking through recurring complete manifests.
Locally, `just validate-local` always runs shallow validation;
`just package="foo bar" validate-deep-local` always runs deep validation.
Neither recipe reads scheduling policy or changes meaning with activation.
Managed repair PRs additionally run the relevant deep checks needed to prove their fix.

Hosted execution, issue reporting, Local triage and repair admission are independently
authorized. Hosted checks and reporting can run with both Local roles disabled.
Safe installation defaults disable hosted execution/reporting, leave both Local
roles disabled and repair allowlists unconfigured. Disabled scheduled execution
means no automatic recurring deep coverage; it does not move deep checks into PR
validation. Installation or profile reconciliation does not activate anything. The
[operating policy](../.github/workflows/implementation.md#operating-policy)
defines the exact configuration mapping and readiness requirements.

The [workflow event graph](../.github/workflows/implementation.md#workflow-events-and-orchestration)
identifies each scheduled, completion-triggered, push-triggered and reusable workflow,
and distinguishes GitHub scheduling from the Local App polls.

A complete expected package/platform/check/shard/seed manifest establishes success.
Missing, cancelled, blocked, unknown or expired evidence is not success. A mutation
timeout is not a caught mutation. An unexplained intermittent failure is not resolved
by an unrelated green run. Full compatible success receipts can suppress repeated
deep work on unchanged source and check contracts, subject to the policy's maximum
age and invalidations. A skip does not renew the receipt's age, replace a failed
execution, suppress local intake or stand in for full scheduled coverage.

Miri replay follows the structured scope supplied by validated evidence: Cargo
target kind/name, test filter and matching mode when present, flags, seed or seed
range, and shard. Target-level and seed-range failures are valid findings even when
the failing test or seed is unknown. Shared output from parallel seeds and
post-suite leak diagnostics does not establish narrower attribution. Narrow only
from independently known input scope, preserve the recorded replay for repair
verification, and do not classify missing test/seed attribution alone as missing
evidence.

`release.yml` continues publishing on merge. Scheduled deep checking accepts delayed
detection of potentially already-published defects. Operating it requires explicit
acceptance of this tradeoff and verified detection/reporting capabilities.

The managed repair gate validates the actual published head or combined queue
candidate. `required-checks` remains the sole ruleset-required check and includes
`scheduled-repair-gate`. Missing managed identity or stale relevant evidence must
not turn a repair into an ordinary PR. Neither a personal account name nor a label
alone identifies a managed PR.

## Installation and operating profile

Recreate or reconcile the setup using this checked-in entry point:

```text
Apply .github\prompts\setup-scheduled-remediation.prompt.md for this repository
using my selected personal GitHub Copilot account and a Local environment.
Reconcile existing setup; do not run the automation or reset repair state.
```

The [setup prompt](../.github/prompts/setup-scheduled-remediation.prompt.md) preserves
the canonical project, actual Local host, existing repair entry and operator choices.
It does not install or activate an AI triage role, and it cannot enable new repair
admissions. The
[intake skill](../.github/skills/scheduled-intake/SKILL.md) and
[repair skill](../.github/skills/scheduled-repair/SKILL.md) own repair admission and
repair-session continuation, not failed-run triage.
No repository configuration file is needed merely to install these automations.
Do not put cron in `.github/github-app.yml`, use `auto_issue_session` as a poller or
attach heavyweight `session.create` scripts to empty polling sessions.

The existing repair entry retains its reviewed cron, model, mode, budget and
installed identity. Its cron is `17 */3 * * *`; it can inspect retained work but
cannot admit a new repair. A downstream triage role requires its own supported
implementation and operator authorization, not merely another timer.

Native custom cron uses `interval: manual` plus `cron_expression`. Verify timezone
and next-run preview for any installed entry in the App. Real `host_id`, project
and automation identifiers are installation data, not portable constants.
New entries are disabled and observe-only.
Reapplication never enables a paused entry, runs a poll, changes billing identity,
resets a counter or discards a session. Renamed managed entries are matched by
verified marker and repository identity; a cached ID or matching name is not proof
of ownership. Markers distinguish owned entries from accidental duplicates. Ambiguous duplicates and unavailable native metadata require operator
reconciliation through supported App controls.

Repair allowlists are empty by default. Enrollment, approved scope, publication
safeguards and mode settings do not substitute for implemented AI triage and
evidence-bound admission. New reservations remain rejected. Policy constrains
retained repair sessions and continuations.
Continuations have daily and lifetime-per-attempt limits; consumed reservations
remain charged even if delivery fails. These are **admission limits, not hard token
or spending caps**. The repair coordinator can use a
cheaper orchestration model and select a separate repair model. Empty polls in
either automation are not free.

## Authentication, credentials and availability

The operator chooses the account and enrolled machine/profile. No account name,
numeric user ID or entitlement identifier is part of the design. The selected
account must meet these requirements:

* Authenticate through supported OAuth/App sign-in and satisfy repository access/SSO requirements.
* Read Actions jobs, logs and artifacts; read and create/update issues, labels and authorized comments.
* Push managed branches, create/update PRs, read checks/reviews and request authorized diagnostic workflows, without a protection bypass or merge permission requirement.
* Have personally funded Copilot access to the selected models and enough allowance for the configured schedules and admitted work.
* Support the approved unattended native tool operations; unresolved consent prompts block the relevant action.

Enrollment records the actual login and numeric user ID so later calls can detect
an unintended account change. A matching GitHub login alone does not prove inference
billing attribution. The operator verifies that attribution in the pilot,
configures personal limits/notifications and approves tool consent.
Never use PATs, export credentials to state, switch accounts to escape a quota,
introduce hosted AI billing permissions or invent unsupported per-run billing caps.

A worktree isolates files and branches, not the home directory, processes, network
or credentials. App repository scripts can expose `GH_TOKEN` and credentials for
other signed-in accounts. Read-only helper code and a scoped prompt do not create
a hard publisher sandbox. Review unattended setup/build code and permissions; use
a proven dedicated machine/profile or supported sandbox if stronger isolation is
required.

Before any same-repository installation-test or repair PR, including a draft, approve and land
the managed-repair production benchmark exclusion and Azure integration safeguards.
PR creation can trigger ordinary workflows regardless of draft status. A recorded
policy prerequisite is not itself proof that those protections are installed.

The enrolled machine and App must be available. Sleep, shutdown, lock-screen,
restart, missed-tick, overlapping-run and native session-reuse behavior require a
pilot on the actual installation. Backlog persistence prevents loss during downtime;
the next poll does not compensate by starting work for every missed timer tick.
GitHub schedules can also be delayed, dropped or disabled for repository inactivity.
Cross-monitoring cannot alert while both GitHub and the local executor are unavailable;
accept manual oversight or configure an approved external observer.

## Durable ownership and native calls

Resolve local state to the absolute path:

```text
%LOCALAPPDATA%\Folo\ScheduledRemediation\<repository-id>\state.json
```

`repository-id` is GitHub's numeric repository ID, the `id` returned by
`gh api repos/{owner}/{repo} --jq .id`. It is not the repository name, GraphQL
`node_id`, App project ID or local executor ID. Setup obtains and verifies it for
the selected repository; the directory name remains stable across repository renames.

The directory is outside worktrees and per-session scratch databases. State stores
only non-secret execution identity, profiles, triage checkpoints/publication intents,
repair attempts, dispatch accounting and health. One enrolled machine owns it. Local file locks do not coordinate multiple
machines; GitHub comments are an audit mirror, not a distributed atomic claim.

Each transaction briefly opens `transaction.lock` exclusively, validates the
entire state, and atomically replaces `state.json` after flushing a same-directory
temporary file. Lock contention is explicit. There is no wait loop or expectation
that a handle survives a helper process.

The **repair coordinator** is the scheduled repair-automation session that scans
issues and starts or resumes repair sessions. Its **coordinator token** is an
opaque random value returned by `acquire-coordinator` and stored with that session's
identity in local state. Every later coordinator mutation must supply the current
token. This prevents overlapping polls or an old resumed poll from changing a
newer poll's reservations after the file lock has been released. It is not a GitHub
credential. Expiry permits another scan, never takeover of a repair session.
Each admitted repair turn also receives a dispatch token bound to its actual
session and attempt; that session accepts it before changing source.

### Triage ownership and issue publication

One active triage session per repository serializes semantic deduplication and
problem-issue publication. Claim a source run/attempt plus its evidence revision
before analysis; register the actual native session and persist analysis checkpoints.
The scheduled triage session can own a new analysis. Later polls resume a retained
analysis in its registered session only after verifying its ownership and native
state; they do not start competing analysis or infer abandonment from elapsed time.
Repair has separate claims and limits, so a triage backlog and an existing repair
can progress independently without sharing source ownership.

Before GitHub writes, persist the proposed existing-issue matches, new-problem
identities and publication intents. Revalidate issue generations and record
ownership under a short state transaction. Reconcile a lost issue-create/update
response using its stable operation identity before retrying; uncertainty blocks
publication rather than creating another issue. Commit the run's triaged evidence
revision only after every disposition and resulting issue write is accounted for.
Newer evidence cannot be acknowledged by an older analysis checkpoint.

The reporter owns run records, coverage and authoritative confirmation. The triage
automation owns triage records on run-level issues and problem records on problem
issues, including causal grouping and run-to-problem links. Repair
records own repair-session/PR state. Each writer preserves the others' records.
A shared personal account name does not substitute for validated role, record,
source-run and issue identity.

### Repair admission

New repair reservation is unavailable at this boundary. The inbox reports the
missing AI triage capability and `reserve-attempt` rejects the operation even if
policy is set to repair mode with broad allowlists. It preserves registered work,
consumed budgets and existing sessions; it does not reset state or fabricate a
completed triage record.

The downstream admission requirement is:

The repair backlog is the complete set of open `scheduled-finding` issues whose
problem record permits source repair for the current occurrence and identifies
the required scope. The problem record must link to reporter-supplied evidence
and a matching complete analysis entry in the run-level issue's triage record.
Raw `scheduled-run-failure` issues, incomplete analysis and operator/infrastructure
recovery dispositions are not repair tasks. Holds, scope limits, budgets and existing
session/PR ownership are then applied to decide which backlog item may start.
Registered repairs are reconciled separately even if their issue closes or a label
is removed; these changes cannot free a still-owned session.

For already-reserved attempts, the ownership protocol remains:

1. Acquire the coordinator token and reconcile registered attempts.
2. Reconcile recorded attempts against native sessions and GitHub PRs before selecting new work.
3. Keep the recorded attempt and its consumed admission; never replace it with a new reservation.
4. Persist `opening-session` before native `open_issue_session` with a model-selected, strictly inert interactive bootstrap.
5. Verify the returned session's issue/repository and ownership, then register its actual session and generated branch/head before permitting any side effect.
6. Persist `dispatching`, then enqueue the actual autopilot repair instruction after the bootstrap. The repair session accepts its token and records native branch adoption before editing.
7. Persist publication branch/head identity, mirror the repair-session record, then create the initial PR with the repair marker already in its body.
8. Register the returned PR number and retain the same native session for every continuation.

Lost responses are reconciled, not blindly retried. `opening-session`, `dispatching`
and `publishing` are deliberately persistent uncertainty states. Read native session
associations/history and GitHub branch/PR records to determine what happened. If the
available API cannot establish the outcome, block and preserve ownership. A human-owned
session needs a handoff; never create competing work or silently adopt it.

Native `rename_branch` applies the App's configured user prefix. Use the intended
`scheduled-repair-<attempt-slug>` name and verify the actual returned name against
`policy.managed_branch_prefix`. Do not bypass app-managed naming with raw Git branch
renames or assume a slash namespace is supported.

### Repair helper interface

The `just` wrappers call the typed PowerShell interfaces. This boundary operates
before a prepared Rust environment is available, so empty inbox scans and native
App transactions do not require a utility build. Scripts cannot invoke native App
tools; the skill performs those calls between transactions. Structured execution
configuration uses the trusted Rust utility described under
[evidence decoding](../.github/workflows/implementation.md#evidence-decoding).

```powershell
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
Import-Module .\scripts\scheduled\LocalState.psm1 -Force
Invoke-ScheduledLocalRequest -RequestPath "{{REQUEST_PATH}}"
```

| Placeholder | Meaning |
|---|---|
| `REQUEST_PATH` | Absolute non-secret JSON request file under the current session's artifact directory. |

The request has `policy_path` (absolute), `executor_id`, `login`, `action` and `data`.
The production wrapper obtains current time itself; the underlying action function
accepts injected time for tests. Successful output is the persisted state snapshot.
An exception means the requested transition did not complete; it is not permission
to retry the following native action.

```json
{
  "policy_path": "C:\\approved-worktree\\scripts\\scheduled\\policy.json",
  "executor_id": "enrolled-executor",
  "login": "selected-user",
  "action": "acquire-coordinator",
  "data": { "owner_session_id": "actual-native-coordinator-session" }
}
```

The example identities and path are placeholders, not enrollment defaults.

| Action | Required `data` beyond identity |
|---|---|
| `initialize` | `operator_approved: true`; allowed only for genuine first enrollment, never missing state in an existing directory. |
| `read` | Empty object; no write or heartbeat. |
| `register-profile` | `operator_approved: true`, `profile` with actual automation/project/host/executor/login, cron/timezone/enabled, policy/prompt digests and coordinator/repair models. Identical registration is a no-op. |
| `set-mode` | `operator_approved: true`, `mode` (`observe`, `paused`, `repair`). |
| `acquire-coordinator` | `owner_session_id`; returns current `coordinator.token`. |
| `release-coordinator` | `coordinator_token`. |
| `record-scan` | `coordinator_token`, `successful`, `backlog_count`, `oldest_eligible_at`, `blocked_conditions`. Failed scans do not advance successful-scan health. |
| `reserve-attempt` | Unavailable: rejects new reservations until evidence-bound AI triage admission is supported. Policy/profile changes do not bypass this boundary. |
| `begin-session-open` | `coordinator_token`, `attempt_id`. |
| `register-session` | `coordinator_token`, `attempt_id`, `session_id`, `issue_number`, `ownership_verified`, `branch`, `head_sha`. |
| `register-branch` | Worker identity plus `expected_branch`, actual managed `branch`, unchanged `head_sha`; follows registration/acceptance and precedes source edits. |
| `begin-dispatch` | `coordinator_token`, `attempt_id`; allowed only once for each reserved dispatch. |
| `accept-dispatch` | `attempt_id`, `session_id`, `dispatch_token`; same-token acceptance is idempotent. |
| `prepare-publication` | Worker identity plus `expected_head`, `head_sha`, `branch`, `check_contract_digest`, nonempty causal `explanation` bounded by `repair.max_explanation_characters`; applies to initial creation and subsequent published heads. |
| `register-pr` | Worker identity plus `pr_number`, `head_sha`, `branch`; never accepts a replacement PR. |
| `record-version-plan` | Worker identity plus `version_evidence` containing current `head_sha`, `base_sha`, canonical `plan_digest`, `current`, `description_current`. |
| `complete-dispatch` | Worker identity plus `phase` (`pr-open`, `awaiting-review`, `blocked`), `reason`, `handled_evidence` fingerprints, optional `proposed_responses` (`kind`, `id`, `fingerprint`, `body`, `status`). Preserve pending human proposals until approved/posted. |
| `reserve-continuation` | `coordinator_token`, `attempt_id`, new `evidence_key`, `expected_head`, `session_id`, `native_idle_verified`; consumes budget before delivery and issues a new dispatch token. |
| `record-pr-disposition` | `coordinator_token`, `attempt_id`, `pr_number`, `head_sha`, `disposition` (`merged`, `closed-unmerged`, `confirmed`), `hosted_confirmation`, `native_idle_verified`; dispatch must already be completed before releasing ownership. |
| `block` | `coordinator_token`, `attempt_id`, explicit `reason`; keeps the slot. |

The action table's "Worker identity" means the agent in the registered repair
session, identified by `attempt_id`, `session_id`, `dispatch_token`. The serialized
`worker` record is that session's ownership record, not another background process. Assertions such
as `ownership_verified` or `hosted_confirmation` must come from the corresponding
native/API evidence, never from an agent's claim of success.
Check allowlists and priority use the triaged problem's evidence-bound check families;
exact catalog IDs and replay conditions remain intact in evidence, descriptors and
repair claims, including every required scope in a grouped problem.

The version evidence also carries the immutable `pre_version_sha`, canonical
`decisions`, `expanded_plan` and `expanded_plan_digest` used by
`ScheduledVersion.psm1` to regenerate the exact expected Cargo edits. Record that
source checkpoint before applying versions, with every Cargo manifest and lockfile
byte-identical to the pinned current trusted release baseline. An agent-selected
checkpoint containing pending increments is not a valid starting point, even if
the resulting expansion is empty.

For continuation, preserve the source repair and restore only the repair session's proven
mechanical Cargo edits to the current baseline before recording a new source
checkpoint and regenerating the full plan. Preserve human-owned, unrelated or
uncertain Cargo differences and block for reconciliation; do not reset them or
change checkpoint/baseline selection to evade the requirement. Test-only empty
expansions still require baseline-identical Cargo inputs.

Persist this evidence after publication
intent and before the initial ownership mirror/PR event; update the description flag
only after the complete version section is confirmed in the published body.

`Get-ScheduledWorkerRecord` projects the separate executor-owned issue comment.
`Get-ScheduledRepairRecord` projects the initial/update PR-body identity only after
publication intent is persisted and revalidates the explanation against its explicit
`Policy` argument. Both include numeric repository identity. Serialize
through shared `Write-ScheduledRecord`; do not handcraft markers. Every authored
comment and PR body begins `[Copilot speaking]`. Update the single owned repair-session
comment instead of appending a new ownership record every time.

The repair marker also carries the persisted `explanation`: the diagnosed failure
and why the specific change fixes it, grounded in evidence rather than a generic
success statement. This bounded causal summary is present from the initial PR
event and is refreshed when the repair changes. Unexplained nondeterminism remains
blocked/needs-human; a green merged repair without a causal explanation is not
eligible for automatic problem resolution.

The profile's repair model is passed through the supported native `kickoff.model`
field during the inert bootstrap, not an invented `save_workflow` field. The
bootstrap performs no commands, file/branch changes or publication. The actual repair
instruction is enqueued only after durable registration. Prove model selection,
ordering and lost-response recovery on the installed App; keep an existing owned
session's configured model.

## PR readiness and bounded continuation

Read the actual PR, `main` comparison, current-head check runs, every top-level
comment, every review summary, and every inline thread/comment. `LocalGitHub.psm1`
paginates REST collections and both nested GraphQL connections, rejects cursor
non-progress, and rechecks head/base after collection. Incomplete permissions or
API errors block readiness. The coordinator also verifies the same native session
is quiescent and still owns the registered branch.

`LocalLifecycle.psm1` combines that snapshot with the registered attempt and
canonical version evidence. New failed/skipped/cancelled relevant CI, missing base
synchronization, changed version decisions, and new/edited review input produce
stable evidence fingerprints. Repeated unchanged waiting-for-CI or already handled
input does not start another agent turn. Budgets remain charged across restarts;
a quota block is not an invitation to increase them.

The existing session receives an ordinary visible/native continuation, never a
replacement session or background watcher. Safe merges from `main` are preferred;
no force-push or overwriting human edits. Real design ambiguity, unknown publication
outcomes, permissions, quota and unexpected head changes are explicit blockers.

Use `increment-versions` and its canonical report/plan/expand/apply tooling whenever
released content or base changes require a fresh decision. There is no separate
version approval gate for these repairs. Every PR description starts with
`[Copilot speaking]`, explains motivation and substantive behavior, and includes a
clearly identified **Version/release plan** section naming every reached package
and group, previous -> proposed versions, change levels and reasons, including
dependent/group movements. Explicitly state when there are no released-content or
version changes. Refresh the full plan on source/base/decision changes; do not
replace it with a summary or include validation-log/changed-file listings.
Combine the canonical expansion with the report's pending-release entries so
sufficient existing increments remain visible on subsequent runs.
Those entries complete the PR's release-impact presentation after canonical
validation; they cannot authorize arbitrary Cargo edits in `pre_version_sha`.

Apply obvious valid fixes from all reviewers; design changes require human approval.
Automatically reply and resolve only agent-authored threads. All human-authored
comments, including the current user's own human comments, require response approval.
Persist proposed responses and present them to the user without submitting them.
Distinguish code/check readiness from pending human-response approval. If a
required unresolved discussion actually blocks readiness, surface that blocker
rather than marking it handled as a substitute for permission.

Readiness requires current required CI and relevant deep evidence, synchronized
base where needed, current canonical version plan and description, and handled
allowed review input. A draft is not the endpoint. Closed-unmerged PRs stop the
attempt; merged PRs enter `verifying-main` until matching hosted confirmation
establishes resolution. PR creation or the repair agent's report never closes the problem issue.

## Health, recovery and rollback

Health separates last successful scan, current failure, pause, budget block,
scheduling/profile drift, unsupported native capability, missing evidence and
GitHub schedule inactivity. Profile registration is not a successful inbox scan.
Publish role-specific triage and repair health on the designated rolling health
surface separately from reporter-owned state; preserve existing discussion and
avoid repeated warning comments. Show each queue's oldest eligible item, completed
evidence checkpoint and active native session. A successful repair scan cannot
mask failed or stale triage, and a triaged intake is not a resolved problem. Unchanged
success skips do not prove recent complete deep execution.

The surface is the single reporter-owned open issue carrying both
`scheduled-health` and `scheduled-coverage` labels.
The local executor's separate health record identifies triage and repair components
by role as well as repository name, numeric ID and executor ID. An absent/duplicate issue or
ambiguous comment ownership blocks registration rather than creating a replacement.
That issue's reporter-owned `coverage` record exposes
`last_plan.planned_at` separately from `receipt.completed_at`. Successful skips may
advance planning history but never renew full-success age.
Hosted planning freshness uses `coverage.expected_plan_gap_hours`, not the local
polling cadence. Deliberately disabled hosted operation is reported separately from
an enabled scheduler that has stopped producing plans. Disabled operation does not
establish recent coverage.

| Condition | Recovery |
|---|---|
| Missing/corrupt executor state | Stop admissions. Reconstruct identity, claims, start/continuation history and checkpoints from backups, repair-session records, PRs and native session history. If prior budget consumption cannot be established, remain blocked; do not initialize an empty replacement. |
| Unknown create/send/publish result | Reconcile actual native/GitHub association and token before completing the recorded transition. If not provable, preserve the session and ask the operator. |
| Paused, permission-blocked or idle repair session | Keep the slot and worktree. Resolve the specific condition in the same session; no age-based takeover or counter reset. |
| Automation deleted/renamed or project recreated | Reapply setup, matching verified repository/marker identity; create a disabled entry only when absence is established. Preserve claims and ledger. |
| Machine/account transfer | Pause both original role automations and their sessions, account for unpublished work and issue-write intents, deliberately transfer enrollment, and prove capabilities on the replacement. Never copy credentials or run two executors. |
| Auth, quota or unsupported native API | Request normal sign-in, personal budget action or supported manual App action; no fallback account, PAT or private-database edits. |
| Hosted schedule disabled/stale | Report the condition, restore scheduling deliberately and request an authoritative fresh run. Do not generate artificial source commits. |
| Reporter run failed/cancelled, including reporting queue overflow | Recover the existing `scheduled-report` run as described below; a newer successful report does not account for its missing report. |
| Scheduled detection/reporting unreliable | Retain the health failure and pause affected repair admissions. Restore scheduled operation deliberately; request explicit deep validation if needed. Do not silently change PR validation or local recipes. |

Use native App controls for session cleanup. Do not delete existing sessions or
worktrees as automated recovery. To pause, disable admissions for the selected role or its project
automation and preserve active ownership; scheduled detection/reporting remains
independent. There are no individual PR timers to remove.

### Recovering failed or cancelled reporting

The reporter's `queue: max` concurrency queue is bounded; it is not a durable
backlog. Queue overflow or cancellation can leave an existing `scheduled-report`
run unprocessed. Health retains failed/cancelled report visibility even after newer
reports succeed, because those newer reports do not establish that the missing
source-run evidence was consumed.

Use the reporting run identified by health or GitHub Actions history. Verify that
it belongs to this repository's `scheduled-report.yml` workflow and inspect its
failure or cancellation before retrying. Resolve any reported permission, evidence
availability or reporter defect first. Once the reporting queue has capacity, use
GitHub Actions **Re-run all jobs** on that existing reporting run, or:

```text
gh run rerun <REPORT_RUN_ID> --repo folo-rs/folo
```

`REPORT_RUN_ID` is the existing reporter run's ID, not its originating validation
run's ID. Rerun the reporting workflow itself, preserving its original source-run
association. Do not rerun heavy validation checks, dispatch a fresh validation
workflow or create a source commit merely to recover reporting.

The reporter persists a per-run `publication-<repository-id>-<workflow-id>-<run-id>.json`
journal in its uploaded report artifact before external writes. A rerun on a fresh
runner restores that journal from the previous attempt's validated artifact before
publication. Pending issue/page writes are reconciled against GitHub rather than
blindly repeated. Missing recovery artifacts or an unresolved write with no visible
result block the rerun and require operator reconciliation; deleting the journal
is not a retry mechanism.
An incomplete report can have partial side effects: `writes_authorized` records
whether writes were permitted, while `applied` becomes true only when reconciliation
finishes. The journal and actual GitHub state determine recovery, not `applied: false`.

Confirm that the new attempt completes successfully and that its expected
run-intake/coverage updates and reporting health are reconciled. Until then, retain
the unresolved reporting condition. If required evidence is expired or unavailable,
keep missing evidence explicit and escalate for an operator decision; do not
manufacture success or clear the failure because another report is green.

## Validation and installation readiness

`just test-scripts` includes deterministic Pester coverage for local state, API
pagination, intake, PR continuation and setup reconciliation. `just validate-scripts`
uses the existing analyzer. Tests inject time, contend a short real file lock without
sleeping, and simulate lost responses instead of hanging an agent.

Scheduled execution and reporting use the nonpublished `scheduled-mutation-config`
Rust utility to decode mutation configuration. It supports exact comparison with
trusted controller configuration and establishing an unmutated baseline for shards with no selected
mutants. The [evidence-decoding contract](../.github/workflows/implementation.md#evidence-decoding)
defines its on-demand, controller-only build and reuse. Hosted reporting prepares
the pinned stable toolchain without installing the deep-check tool suite. Verify
utility/toolchain availability in each actual execution/reporting environment,
including WSL when used; availability on
the Windows host alone is not sufficient. Missing tooling is an explicit
prerequisite blocker, not permission to install it automatically or skip the
baseline. Empty Local inbox scans do not require this build.

Before either role's activation, prove personal billing, expected GitHub permissions,
the actual Local host and both scheduling previews, native issue association/reuse, branch
naming, unattended consent, crash/restart behavior and publication protections.
Exercise no-work, old backlog, duplicate/human sessions, single-repair capacity,
lost-response recovery, unavailable WSL, quota, paused state, an existing PR and
closed-unmerged disposition. Reapply setup unchanged and after deletion/rename,
policy refresh, project-ID change and missing metadata. No code test can establish
these installation facts or authorize a real installation-test PR.

Evaluate semantic triage separately on representative controlled failure bundles
using the selected model. Include unrelated problems in one job, shared causes
across runs, setup-only failures, incomplete evidence, uncertain matches and
already-reported problems that need no new issues. Deterministic tests establish
queue and record mechanics, not the quality or completeness of AI diagnosis.

### Maintaining decoder dependency identity

The reviewed `packages/scheduled-mutation-config/dependency-contract.json` binds
the decoder's effective dependency requirements and reachable registry graph to
checker compatibility. Shared features reachable through that graph participate;
unrelated workspace release versions do not. Dependency drift is a prerequisite
failure, not authorization for the reporter to rewrite the contract.

For an intentional dependency change, use a trusted maintenance checkout with the
approved pinned Rust toolchain already prepared. From that checkout's repository
root, regenerate the snapshot:

```powershell
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
Import-Module .\scripts\build\CargoExecutable.psm1 -Force
Import-Module .\scripts\scheduled\ScheduledExecution.psm1 -Force
$pin = Get-ScheduledToolchain -Kind mutants
$messages = @(cargo "+$pin" build --locked --package scheduled-mutation-config `
    --bin scheduled-mutation-config --message-format=json)
$decoder = Resolve-CargoExecutable -CargoMessage $messages -TargetName scheduled-mutation-config
cargo "+$pin" metadata --locked --format-version=1 |
    & $decoder --dependency-contract |
    Set-Content .\packages\scheduled-mutation-config\dependency-contract.json -Encoding utf8
```

This example has no placeholders. Stop on any error; do not accept partial output.
Review the generated dependency changes with the intended dependency edit and
validate the helper and scheduled execution tests before committing the snapshot.
Do not run this command against a repair candidate or during routine reporting to
accept an unexpected graph.
