# Scheduled validation and local remediation

## Purpose and responsibility

GitHub Actions runs deterministic deep validation at an immutable `main` SHA and
maintains finding issues. A **finding** is a normalized failure signature; its
**generation** distinguishes a recurrence from an already resolved occurrence.
Issues remain available while the local executor is unavailable.

One project-level **Local GitHub Copilot App automation**, using the selected
personal Copilot entitlement, scans every unresolved eligible incident and continues
registered repair PRs toward readiness. A fresh coordinator session performs each
scan. It never edits source. An owned native issue session diagnoses and repairs the
finding, creates a PR, and remains the execution context for subsequent CI, base,
version-plan and review work. Final approval and merge remain human actions.

```text
Actions: fixed source -> complete evidence -> reporter-owned finding issue
                                                    |
Local App: repository poll -> fenced claim -> native issue/PR session
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

## Validation and release contract

Ordinary tests, compilation, Clippy, docs, feature/dependency, external-type,
version/SemVer and integration checks remain on the normal validation path.
Scheduled enforcement owns mutation testing, ordinary and many-seed Miri, and
careful checking when the corresponding cutover is approved. Retained platform
coverage is not silently reduced.

A complete expected package/platform/check/shard/seed manifest establishes success.
Missing, cancelled, blocked, unknown or expired evidence is not success. A mutation
timeout is not a caught mutation. An unexplained intermittent failure is not resolved
by an unrelated green run. Full compatible success receipts can suppress repeated
deep work on unchanged source and check contracts, subject to the policy's maximum
age and invalidations. A skip does not renew the receipt's age, replace a failed
execution, suppress local intake or stand in for full scheduled coverage.

`release.yml` continues publishing on merge. Scheduled enforcement accepts delayed
detection of potentially already-published defects. Do not enable cutover without
accepting this tradeoff and proving the replacement detection/reporting path.

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

The [setup prompt](../.github/prompts/setup-scheduled-remediation.prompt.md) discovers
the canonical project and actual Local host, verifies the managed entry, and
preserves operator choices. The [intake skill](../.github/skills/scheduled-intake/SKILL.md)
is the saved automation's entry point; the
[repair skill](../.github/skills/scheduled-repair/SKILL.md) defines its worker contract.
No repository configuration file is needed merely to install this automation.
Do not put cron in `.github/github-app.yml`, use `auto_issue_session` as a poller or
attach heavyweight `session.create` scripts to empty polling sessions.

Policy proposes cron `17 */3 * * *`, represented by the native automation API as
`interval: manual` plus `cron_expression`. Verify timezone and next-run preview in
the installed App. Real `host_id`, project and automation identifiers are
installation data, not portable constants. New entries are disabled and observe-only.
Reapplication never enables a paused entry, runs a poll, changes billing identity,
resets a counter or discards a session. Renamed managed entries are matched by
verified marker and repository identity; a cached ID or matching name is not proof
of ownership. Ambiguous duplicates and unavailable native metadata require operator
reconciliation through supported App controls.

Initial repair allowlists are empty. Enrollment, approved scope, publication
safeguards, native capability pilots and explicit mode activation are required
before admissions. Policy constrains starts, active workers and continuations.
Continuations have daily and lifetime-per-attempt limits; consumed reservations
remain charged even if delivery fails. These are **admission limits, not hard token
or spending caps**. A cheap coordinator model limits empty-poll overhead; it does
not make empty polls free. A repair model is an independently selected profile item.

## Authentication, credentials and availability

Normal OAuth/App authentication authorizes GitHub operations. The expected GitHub
login does not prove that inference uses a personally funded Copilot entitlement.
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

Before any same-repository canary or repair PR, including a draft, approve and land
the managed-repair production benchmark exclusion and Azure integration safeguards.
PR creation can trigger ordinary workflows regardless of draft status. A staged
policy flag is not itself proof that those protections are deployed.

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
only non-secret execution identity, profiles, attempts, dispatch accounting and
health. One enrolled machine owns it. Local file locks do not coordinate multiple
machines; GitHub comments are an audit mirror, not a distributed atomic claim.

Each transaction briefly opens `transaction.lock` exclusively, validates the
entire state, and atomically replaces `state.json` after flushing a same-directory
temporary file. Lock contention is explicit. There is no wait loop or expectation
that a handle survives a helper process. A persistent coordinator token fences
multi-call scans. Expiring it allows another coordinator scan, never reclaiming a
worker. Each dispatch has a separate token checked by the actual native worker.

The admission protocol is:

1. Reserve a coordinator token; scan and validate the complete open backlog.
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

### Local helper interface

The existing `just` wrappers call the typed PowerShell interfaces. Scripts cannot
invoke native App tools; the skill performs those calls between transactions.

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
| `reserve-attempt` | `coordinator_token` and validated descriptor's `issue_number`, `finding_id`, `generation`, `check_contract_digest`, `package`, exact catalog `check_id`, family `check_kind`, `evidence_key`. |
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
Check allowlists and priority use the reporter's `check_kind` family; the exact
catalog `check_id` remains intact in evidence, descriptors and worker claims.

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
Publish the executor's record to the designated rolling health surface separately
from reporter-owned state; preserve existing discussion and avoid repeated warning
comments. Show the oldest eligible issue and active native session. Unchanged
success skips do not prove recent complete deep execution.

The surface is the single reporter-owned open issue carrying both
`scheduled-health` and `scheduled-coverage` labels.
The local executor owns one separate worker-login comment, with a `health` marker
matching repository name, numeric ID and executor ID. An absent/duplicate issue or
ambiguous comment ownership blocks registration rather than creating a replacement.
That issue's reporter-owned `coverage` record exposes
`last_plan.planned_at` separately from `receipt.completed_at`. Successful skips may
advance planning history but never renew full-success age.
Hosted planning freshness uses `coverage.expected_plan_gap_hours`, not the local
polling cadence. Deliberately disabled staged hosted execution is reported as
staged, separately from an enabled scheduler that has stopped producing plans.

| Condition | Recovery |
|---|---|
| Missing/corrupt executor state | Stop admissions. Reconstruct complete identity, claims, start/continuation history and checkpoints from known backups, worker records, PRs and native session history. If prior budget consumption cannot be established, remain blocked; do not initialize an empty replacement. |
| Unknown create/send/publish result | Reconcile actual native/GitHub association and token before completing the recorded transition. If not provable, preserve the session and ask the operator. |
| Paused, permission-blocked or idle worker | Keep the slot and worktree. Resolve the specific condition in the same session; no age-based takeover or counter reset. |
| Automation deleted/renamed or project recreated | Reapply setup, matching verified repository/marker identity; create a disabled entry only when absence is established. Preserve claims and ledger. |
| Machine/account transfer | Pause original automation and worker activity, account for unpublished work, deliberately transfer enrollment, and prove capabilities on the replacement. Never copy credentials or run two executors. |
| Auth, quota or unsupported native API | Request normal sign-in, personal budget action or supported manual App action; no fallback account, PAT or private-database edits. |
| Hosted schedule disabled/stale | Report the condition, restore scheduling deliberately and request an authoritative fresh run. Do not generate artificial source commits. |
| Detection/reporting unreliable after cutover | Restore affected ordinary merge gates through the shared executor until repaired; never leave both enforcement paths disabled. |

Use native App controls for session cleanup. Do not delete existing sessions or
worktrees as automated recovery. To pause, disable admissions or the project
automation and preserve active ownership; scheduled detection/reporting remains
independent. There are no individual PR timers to remove.

## Validation and rollout prerequisites

`just test-scripts` includes deterministic Pester coverage for local state, API
pagination, intake, PR continuation and setup reconciliation. `just validate-scripts`
uses the existing analyzer. Tests inject time, contend a short real file lock without
sleeping, and simulate lost responses instead of hanging a worker.

Before repair activation, prove personal billing, expected GitHub permissions, the
actual Local host and scheduling preview, native issue association/reuse, branch
naming, unattended consent, crash/restart behavior and publication protections.
Exercise no-work, old backlog, duplicate/human sessions, one-worker capacity,
lost-response recovery, unavailable WSL, quota, paused state, an existing PR and
closed-unmerged disposition. Reapply setup unchanged and after deletion/rename,
policy refresh, project-ID change and missing metadata. No code test can establish
these installation facts or authorize a real canary.
