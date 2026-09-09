# Scheduled validation and local remediation

## Purpose and responsibility

GitHub Actions runs deterministic deep validation at an immutable `main` SHA and
files a run-level **Scheduled validation failed** issue with execution evidence.
That issue requests analysis; it is not itself a diagnosed problem or a repair task.
Evidence remains available while the local executor is unavailable.

Two project-level **Local GitHub Copilot App automations** use the selected
personal Copilot entitlement. The **triage automation** analyzes failed runs with
AI and creates or updates deduplicated problem issues. It does not change source or
create repair PRs. The **repair automation** scans those triaged problems and
continues registered repair PRs toward readiness. Its fresh coordinator never edits
source; an owned native issue session performs the repair and remains the execution
context for CI, base, version-plan and review work. Final approval and merge remain
human actions.

```text
Actions: fixed source -> job evidence -> "Scheduled validation failed" intake
                                                    |
Local App triage: poll -> AI analysis of every failed job -> deduplicate
                                                    |
                                    actionable problem issues
                                                    |
Local App repair: poll -> fenced claim -> native issue/PR session
                                                    |
                         reproduce -> repair -> PR -> current-head readiness
                                                    |
                                    human review/merge -> hosted confirmation
```

This is repository-level orchestration, not a per-PR timer, cloud coding agent,
hidden worker process or organization-billed inference workflow. A paused session
keeps its ownership. Idle status or elapsed time never establishes that its work is
safe to replace.

The workflow [design](../.github/workflows/design.md) and
[implementation guide](../.github/workflows/implementation.md) describe hosted
execution, reporting and gate ownership. [Testing](testing.md) remains authoritative
for mutation quality and reviewed skip criteria. The shared versioned contracts and
reviewed defaults live under `scripts/scheduled/`; policy is not inferred from App UI
state.

## Problems, incidents and triage

An **observation** is evidence of a failure from a particular run, job or step.
A **problem** is a distinct actionable failure supported by observations: for
example, a Miri defect, a missed mutation, or a dependency-download failure.
A **finding** is the normalized report of such a problem. The **finding issue**
is its canonical tracking item, not a ticket for the workflow run.

An **incident** is one unresolved occurrence of a problem. Its identity combines
the stable problem identity with a **generation** that changes only when the
problem recurs after confirmed resolution. Repeated observations across runs
update the same incident and issue. A recurrence reuses the issue with a new
generation; delayed evidence from an older occurrence does not reopen it.

Each incident has at most one active local repair and one open repair PR.
Duplicate observations or aliases of the same problem cannot acquire separate
workers. A new generation cannot bypass retained ownership from an older attempt.
Distinct incidents have independent lifecycles, subject to repository-wide
admission limits; a one-worker limit does not combine their issues.

### Run-level intake and AI triage

The hosted completion reporter publishes the run's identity, attempts, source,
manifest, job/step inventory, structured results and diagnostic references. Its
run-level issue is deduplicated by repository, workflow and run identity. Repeated
reporting of an attempt is a no-op; a rerun appends attempt evidence and makes any
new unprocessed failure eligible for triage. Consecutive failing runs can have
separate intake issues without creating duplicate problem issues.

The triage automation scans all unprocessed run/attempt evidence, not just newly
created issues. AI analysis accounts for every unsuccessful job and relevant failed
step, including planning, environment setup, downloads, checker execution and
artifact handling. It reads the evidence and relevant source as needed to identify
causes and compare them with existing issues. Failed jobs are evidence containers,
not incident boundaries. Programmatic parsers supply observations and candidate
matches; they do not replace this semantic analysis.

Triage extracts every independently supported problem rather than selecting the
first error or creating one catch-all "workflow is red" issue. A job can contribute
several problems; a problem can affect several jobs or runs. A failed prerequisite
is reported as its own problem with the dependent scope marked blocked, not as a
code defect in each package that never ran. Missing or uninterpretable evidence
produces an explicit triage-required or evidence-collection problem; it is not an
empty successful result. Intentional skips and cancellation causes are accounted
for without inventing defects in unexecuted code.

For each extracted problem, compare its evidence, diagnosis and established aliases
against the triage-owned problem index, including resolved issues. Update a matching
incident, reopen a confirmed recurrence with a new generation, or create an issue
only when no existing problem matches. Serialize this reconciliation so duplicate
triage retries and concurrent runs cannot create duplicate issues. Preserve
the contributing observations and their run/job/step references on the canonical
record even when they are grouped.

All semantic triage uses personally funded Local App AI, never hosted inference.
If analysis cannot establish a cause or a safe match, retain the narrowest supported
symptom and expose the uncertainty. Incomplete analysis is not repair-ready.
Further diagnosis can refine the classification or establish a shared cause; it
must preserve issue history and reconcile existing repair ownership before
consolidating issues.

The triager records a disposition for every failed job: linked problem issues,
an explained duplicate or blocked/cancelled consequence, or an explicit unresolved
analysis requirement. Mark the run-level issue **triaged** only when all such
analysis is complete and issue writes are reconciled. An unfinished job analysis
keeps the evidence revision pending; a fully analyzed problem can instead be linked
with an explicit operator hold. This may create no new
problem issues when everything is already reported. Closing a triaged intake
issue means analysis is complete, not that its linked problems are fixed. A new
unprocessed attempt makes that run eligible again without erasing prior analysis.

Only problems marked actionable by completed triage are eligible for repair.
Infrastructure recovery, permission decisions and unresolved classification have
their own dispositions; they are not permission to fabricate a code patch.
New observations appended during a repair do not create another worker. A changed
diagnosis that invalidates the repair scope blocks or informs the existing worker
instead of silently replacing its task.

A grouped problem retains the affected packages, checks, platforms and replay
conditions as a set. Repair admission validates the entire required scope against
policy, not just a representative observation. The repair gate and resolution
criteria cover that set. A passing replay of one symptom cannot resolve other
linked failures without evidence that the confirmation covers their shared cause.

### Problem identity and grouping

A stable fingerprint identifies a supported failure, not the fact that a workflow,
check family, package or Cargo target is failing. Identity includes the failure
category and affected operation/entity, with distinguishing diagnostics and
reproduction conditions where those are material. Run IDs, timestamps, source
SHAs, incidental log paths and shifting line numbers are observation metadata.
Normalize known volatile details without discarding distinctions between problems.

Equal normalized symptoms supply candidate matches. The AI triager evaluates
whether the evidence describes the same problem. Different symptoms belong
to one problem only when evidence establishes a shared cause; record that relation
as an alias rather than assuming that every failure in a package is related.
Keep uncertain matches separate and linked for diagnosis. Root-cause discovery is
not a prerequisite for reporting a failure, and a fingerprint is not proof of a
common root cause.

For Miri, replay scope and problem identity are different concepts. Several
unrelated failures can require the same target-level replay. A target, shard or
seed range alone must not collapse distinguishable failures, and interleaved output
must not be used to invent test/seed attribution.

| Observations | Incident treatment |
|---|---|
| The same normalized Miri defect fails in consecutive runs. | Append evidence to the existing incident, even while its repair is active. |
| Several tests in `foo` fail from an established shared Miri defect. | Track one problem with all affected tests and observations; keep unrelated defects separate. |
| Different Miri diagnostics occur in the same Cargo target. | Track distinct problems unless a shared cause is established. |
| Mutation shard 3 cannot download dependency `bar`. | Track an execution problem identifying the download operation and failure reason; mark mutation coverage blocked, not caught or missed. |
| Other shards fail from that same download problem. | Add their observations to the same issue; shard numbers are evidence, not independent problems. |
| One run reports A and B; the next reports B and C. | Update B and report C if unmatched. A remains governed by its own resolution evidence, not the workflow's red/green state. |

An unrelated later green job, an absent observation or the appearance of a
different problem does not resolve an incident. Resolution requires applicable
evidence for that problem and generation. Infrastructure recovery or an explained
no-change disposition need not manufacture a source patch, but must satisfy their
explicit resolution policy. Broad workflow health and complete-coverage status
remain separate from each incident's lifecycle.

## Validation and release contract

Ordinary tests, compilation, Clippy, docs, feature/dependency, external-type,
version/SemVer and integration checks remain on the normal validation path.
**Scheduled enforcement** runs mutation testing, ordinary and many-seed Miri, and
careful checking through recurring complete manifests. **Ordinary-validation
fallback** retains the ordinary PR/push deep jobs and routine local Miri/mutation
calls with their existing scopes. Retained platform coverage is not silently
reduced. Explicit `just package="foo bar" validate-deep` remains available in either
mode.

Hosted execution, issue reporting, Local triage and repair admission are independently
authorized. Hosted checks and reporting can run with both Local roles disabled and
ordinary-validation fallback selected. Safe installation defaults disable hosted
execution/reporting, leave both Local roles disabled and repair allowlists unconfigured,
and select ordinary-validation fallback; installation or profile reconciliation
does not change those settings. The
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

`release.yml` continues publishing on merge. Scheduled enforcement accepts delayed
detection of potentially already-published defects. Selecting it requires explicit
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

The [setup prompt](../.github/prompts/setup-scheduled-remediation.prompt.md) owns
reconciliation of the canonical project, actual Local host and the separately
identified triage and repair entries. It preserves operator choices and existing
triage and repair ownership. Triage has its own AI instructions and run queue; the
[intake skill](../.github/skills/scheduled-intake/SKILL.md) and
[repair skill](../.github/skills/scheduled-repair/SKILL.md) own repair admission and
worker continuation, not failed-run triage.
No repository configuration file is needed merely to install these automations.
Do not put cron in `.github/github-app.yml`, use `auto_issue_session` as a poller or
attach heavyweight `session.create` scripts to empty polling sessions.

Each automation has a separately reviewed cron, model, mode, budget and installed
identity. Both poll every three hours; repair retains cron `17 */3 * * *`.
An offset for triage is a scheduling preference, not a dependency on its completing
before repair starts. A problem becomes eligible when triage commits its result,
and the next repair poll can consume it. The handoff can therefore add another
polling interval, plus analysis, queue and machine-availability delay.

Native custom cron uses `interval: manual` plus `cron_expression`. Verify timezone
and next-run preview for both entries in the installed App. Real `host_id`, project
and automation identifiers are installation data, not portable constants.
New entries are disabled and observe-only.
Reapplication never enables a paused entry, runs a poll, changes billing identity,
resets a counter or discards a session. Renamed managed entries are matched by
verified marker and repository identity; a cached ID or matching name is not proof
of ownership. Role-specific markers distinguish the intended triage and repair
entries from accidental duplicates. Ambiguous duplicates and unavailable native metadata require operator
reconciliation through supported App controls.

Repair allowlists are empty by default. Enrollment, approved scope, publication
safeguards, verified native capabilities and explicit mode authorization are required
before admissions. Policy constrains starts, active workers and continuations.
Continuations have daily and lifetime-per-attempt limits; consumed reservations
remain charged even if delivery fails. Triage separately limits analyses and
continuations so repeated run failures cannot consume an unbounded personal budget.
These are **admission limits, not hard token or spending caps**. The triage model
must support substantive log/source analysis. The repair coordinator can use a
cheaper orchestration model and select a separate worker model. Empty polls in
either automation are not free.

## Authentication, credentials and availability

Normal OAuth/App authentication authorizes GitHub operations. The expected GitHub
login does not prove that inference uses a personally funded Copilot entitlement.
The selected `sandersaares` entitlement is personally funded and the approved Local
executor is the operator's current Windows profile; setup preserves these choices.
The operator must verify personal billing selection and actual pilot attribution,
configure personal usage limits/notifications, and approve native tool consent.
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

The directory is outside worktrees and per-session scratch databases. State stores
only non-secret execution identity, profiles, triage checkpoints/publication intents,
repair attempts, dispatch accounting and health. One enrolled machine owns it. Local file locks do not coordinate multiple
machines; GitHub comments are an audit mirror, not a distributed atomic claim.

Each transaction briefly opens `transaction.lock` exclusively, validates the
entire state, and atomically replaces `state.json` after flushing a same-directory
temporary file. Lock contention is explicit. There is no wait loop or expectation
that a handle survives a helper process. A persistent coordinator token fences
multi-call scans. Expiring it allows another coordinator scan, never reclaiming a
worker. Each dispatch has a separate token checked by the actual native worker.

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

Hosted records own run evidence, coverage and authoritative confirmation. Triage
records own problem identity, causal grouping and run-to-problem links. Repair
records own worker/session/PR state. Each writer preserves the others' records.
A shared personal account name does not substitute for validated role, record,
source-run and issue identity.

### Repair admission

The repair admission protocol is:

1. Reserve a repair coordinator token; scan and validate the complete triaged problem backlog, excluding raw run-intake issues.
2. Reconcile recorded attempts against native sessions and GitHub PRs before selecting new work.
3. Reserve one attempt and charge its admission under the lock.
4. Persist `opening-session` before native `open_issue_session` with a model-selected, strictly inert interactive bootstrap.
5. Verify the returned session's issue/repository and ownership, then register its actual session and generated branch/head before permitting any side effect.
6. Persist `dispatching`, then enqueue the actual autopilot worker instruction after the bootstrap. The worker accepts its token and records native branch adoption before editing.
7. Persist publication branch/head identity, mirror the worker record, then create the initial PR with the repair marker already in its body.
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
| `reserve-attempt` | `coordinator_token` and a validated problem descriptor binding issue, finding identity, generation, completed triage revision, hosted evidence and the entire required repair scope. |
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

Worker identity means `attempt_id`, `session_id`, `dispatch_token`. Assertions such
as `ownership_verified` or `hosted_confirmation` must come from the corresponding
native/API evidence, never from an agent's claim of success.
Check allowlists and priority use the triaged problem's evidence-bound check families;
exact catalog IDs and replay conditions remain intact in evidence, descriptors and
worker claims, including every required scope in a grouped problem.

The version evidence also carries the immutable `pre_version_sha`, canonical
`decisions`, `expanded_plan` and `expanded_plan_digest` used by
`ScheduledVersion.psm1` to regenerate the exact expected Cargo edits. Record that
source checkpoint before applying versions, with every Cargo manifest and lockfile
byte-identical to the pinned current trusted release baseline. A worker-selected
checkpoint containing pending increments is not a valid starting point, even if
the resulting expansion is empty.

For continuation, preserve the source repair and restore only the worker's proven
mechanical Cargo edits to the current baseline before recording a new source
checkpoint and regenerating the full plan. Preserve human-owned, unrelated or
uncertain Cargo differences and block for reconciliation; do not reset them or
change checkpoint/baseline selection to evade the requirement. Test-only empty
expansions still require baseline-identical Cargo inputs.

Persist this evidence after publication
intent and before the initial worker mirror/PR event; update the description flag
only after the complete version section is confirmed in the published body.

`Get-ScheduledWorkerRecord` projects the separate executor-owned issue comment.
`Get-ScheduledRepairRecord` projects the initial/update PR-body identity only after
publication intent is persisted and revalidates the explanation against its explicit
`Policy` argument. Both include numeric repository identity. Serialize
through shared `Write-ScheduledRecord`; do not handcraft markers. Every authored
comment and PR body begins `[Copilot speaking]`. Update the single owned worker
comment instead of appending a new ownership record every time.

The repair marker also carries the persisted `explanation`: the diagnosed failure
and why the specific change fixes it, grounded in evidence rather than a generic
success statement. This bounded causal summary is present from the initial PR
event and is refreshed when the repair changes. Unexplained nondeterminism remains
blocked/needs-human; a green merged repair without a causal explanation is not
eligible for automatic incident closure.

The profile's repair model is passed through the supported native `kickoff.model`
field during the inert bootstrap, not an invented `save_workflow` field. The
bootstrap performs no commands, file/branch changes or publication. The real worker
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
replacement worker or background watcher. Safe merges from `main` are preferred;
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
establishes resolution. PR creation or a worker's report never closes the finding.

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
| Missing/corrupt executor state | Stop admissions. Reconstruct complete identity, claims, start/continuation history and checkpoints from known backups, worker records, PRs and native session history. If prior budget consumption cannot be established, remain blocked; do not initialize an empty replacement. |
| Unknown create/send/publish result | Reconcile actual native/GitHub association and token before completing the recorded transition. If not provable, preserve the session and ask the operator. |
| Paused, permission-blocked or idle worker | Keep the slot and worktree. Resolve the specific condition in the same session; no age-based takeover or counter reset. |
| Automation deleted/renamed or project recreated | Reapply setup, matching verified repository/marker identity; create a disabled entry only when absence is established. Preserve claims and ledger. |
| Machine/account transfer | Pause both original role automations and their sessions, account for unpublished work and issue-write intents, deliberately transfer enrollment, and prove capabilities on the replacement. Never copy credentials or run two executors. |
| Auth, quota or unsupported native API | Request normal sign-in, personal budget action or supported manual App action; no fallback account, PAT or private-database edits. |
| Hosted schedule disabled/stale | Report the condition, restore scheduling deliberately and request an authoritative fresh run. Do not generate artificial source commits. |
| Reporter run failed/cancelled, including reporting queue overflow | Recover the existing `scheduled-report` run as described below; a newer successful report does not account for its missing report. |
| Detection/reporting unreliable in scheduled enforcement | Select ordinary-validation fallback before disabling hosted enforcement; never leave both enforcement paths disabled. |

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

Confirm that the new attempt completes successfully and that its expected
run-intake/coverage updates and reporting health are reconciled. Until then, retain
the unresolved reporting condition. If required evidence is expired or unavailable,
keep missing evidence explicit and escalate for an operator decision; do not
manufacture success or clear the failure because another report is green.

## Validation and installation readiness

`just test-scripts` includes deterministic Pester coverage for local state, API
pagination, intake, PR continuation and setup reconciliation. `just validate-scripts`
uses the existing analyzer. Tests inject time, contend a short real file lock without
sleeping, and simulate lost responses instead of hanging a worker.

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
Exercise no-work, old backlog, duplicate/human sessions, one-worker capacity,
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
