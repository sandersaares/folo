# Reconcile the Local scheduled remediation installation

Apply this procedure to the current `folo-rs/folo` project using the operator's
selected personal GitHub Copilot account and a **Local** App environment. This is
reproducible setup, not a request to run a repair. Never enable or run an automation,
reset repair state, change billing identity or accept repository configuration
implicitly. Preserve existing worker sessions, worktrees, claims and counters.

## Stage 1: Read desired state and establish prerequisites

Read `docs\scheduled-validation.md`, `scripts\scheduled\policy.json`, and
`.github\skills\scheduled-intake\SKILL.md`. Repository policy supplies desired cadence,
name, marker, limits and approved scope. An installed App entry is only its applied
copy. Review local prerequisite instructions; do not execute heavyweight setup or
install missing tools without operator approval.

Verify the expected GitHub API user and canonical numeric repository identity:

```powershell
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
gh api user --jq .login
gh api repos/folo-rs/folo --jq '{id: .id, full_name: .full_name, default_branch: .default_branch}'
```

These commands have no placeholders. Compare their output with policy and stop on
identity mismatch or nonzero exit. Never print tokens/environment credentials or
use PATs. Have the operator verify personal Copilot entitlement and billing
selection in the App; CLI login alone does not establish inference billing.
The approved selected `sandersaares` entitlement is personally funded; preserve that
decision while still proving actual pilot usage attribution and permissions.
Record remaining quota/consent/sandbox/publication prerequisites without changing
account settings. No same-repository PR canary before Azure/benchmark safeguards.

## Stage 2: Discover the canonical project and Local host

Use native `list_projects` and select the existing local project by exact canonical
repository identity. Reuse its actual ID. If absent or ambiguous, have the operator
connect/select the intended project through supported App UI; do not clone into a
guessed directory or invent a project identifier.

Discover the real Local environment through native metadata or the App environment
picker. The approved executor is the operator's current Windows profile; do not
provision a dedicated profile or choose another machine without a new decision.
`host_id` is required for direct native automation creation; never supply
the project ID, machine name or the word `local` as a guessed host ID. If host
discovery is unavailable, use the native creation confirmation dialog only after
absence of an existing managed automation has been established, or ask the
operator to select the host manually. Do not silently choose cloud execution.

Resolve executor enrollment to the absolute application-data path via
`Get-ScheduledStateRoot`. Missing state is not first installation merely because
this App automation is absent. Before genuine first enrollment, reconcile the
complete reporter queue's worker records, managed PRs and native issue sessions.
A directory with missing/corrupt state requires recovery, never empty initialization.
A different machine/account requires explicit ownership transfer with the old
executor paused and unpublished work accounted for.

## Stage 3: Discover and verify the managed automation

Use native `list_workflows` without an enabled-only filter. Inspect all entries
that can belong to the canonical repository, including renamed entries and entries
associated with an earlier project ID. Match the verified managed prompt marker,
repository, registered ID and canonical name; the name/ID are discovery hints,
not sufficient ownership proof. A deleted entry can have a stale cached ID.

Native schemas vary in exposed read metadata. `list_workflows` may omit the prompt,
host or project association needed to prove ownership. If so, inspect the entry in
the supported Automation UI or have the operator supply/confirm the missing
metadata. **Do not set metadata-complete to true based on guesses**, infer absence,
create another entry or read private App databases. Stop when duplicates or
ownership remain ambiguous; never delete arbitrary automations.

## Stage 4: Compute the idempotent reconciliation decision

The supported native payload exposes `projectId`, `hostId`, `cronExpression`,
`reasoningEffort` and `workspaceType` alongside the full prompt. Normalize it with
`ConvertTo-ScheduledNativeWorkflow`, joining project IDs to canonical repositories
established from `list_projects`. An observed native `hostId` of `local` is valid;
the prohibition is on guessing that value, not on preserving native evidence.

```powershell
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
Import-Module .\scripts\scheduled\LocalSetup.psm1 -Force
$native = Get-Content -LiteralPath "{{NATIVE_WORKFLOW_PATH}}" -Raw | ConvertFrom-Json -AsHashtable
$projects = Get-Content -LiteralPath "{{PROJECT_MAP_PATH}}" -Raw | ConvertFrom-Json -AsHashtable
ConvertTo-ScheduledNativeWorkflow -Workflow $native -ProjectRepository $projects |
    ConvertTo-Json -Depth 40
```

| Placeholder | Meaning |
|---|---|
| `NATIVE_WORKFLOW_PATH` | Absolute artifact containing one actual candidate `list_workflows` entry, retaining its native field names. |
| `PROJECT_MAP_PATH` | Absolute JSON object mapping actual project IDs to canonical repository names verified through native project discovery. |

Use each result as a normalized workflow below. A missing field or unknown
association is an explicit metadata blocker; do not replace it with an empty list.
Do not hardcode an installation's observed automation/project ID into repository
defaults. Preserve the existing disabled managed entry rather than creating another.

Prepare a non-secret JSON artifact containing the normalized native data:
`desired`, `workflows`, `metadata_complete`, `registered_profile`. Only use fields
actually observed or explicitly selected by the operator.

| Object | Fields |
|---|---|
| `desired` | Canonical `repository`, actual `project_id`, `host_id`, enrolled `executor_id`, selected `login`, policy `name`, `marker`, `cadence_cron`, desired `prompt`, operator-selected `coordinator_model` and optional supported `coordinator_effort`. |
| Each `workflows` entry | Actual `id`, canonical `repository` association, `project_id`, `host_id`, `name`, `prompt`, `enabled`, `interval`, `cron_expression`, `model`, `reasoning_effort`, `mode`, `workspace_type`. |
| `registered_profile` | Existing local profile, or null only after verified first-enrollment/recovery discovery. |
| `metadata_complete` | True only when the native facts needed for matching and comparison are available. |

The desired saved prompt is short and contains the exact policy marker:

```text
folo-scheduled-remediation:v1
Run the repository's scheduled-intake skill for folo-rs/folo in Local mode.
Read reviewed policy and use the deterministic inbox helper. Scan the whole
unresolved backlog and reconcile registered repair PRs through readiness in
their existing native App sessions, using bounded new-evidence continuations.
Respect observe/paused mode, enrollment, scope, budgets, profile drift and claims.
Never edit source in this coordinator, start cloud work, create per-PR timers,
merge, publish releases, change accounts/billing, or discard existing work.
```

Use the marker from reviewed policy if it changes; do not maintain an independent
unversioned copy. The saved prompt points to the skill rather than duplicating its
protocol.

```powershell
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
Import-Module .\scripts\scheduled\LocalSetup.psm1 -Force
$inputData = Get-Content -LiteralPath "{{SETUP_INPUT_PATH}}" -Raw | ConvertFrom-Json -AsHashtable
Get-ScheduledSetupDecision -Desired $inputData.desired -Workflows $inputData.workflows `
    -MetadataComplete $inputData.metadata_complete -RegisteredProfile $inputData.registered_profile |
    ConvertTo-Json -Depth 40
```

| Placeholder | Meaning |
|---|---|
| `SETUP_INPUT_PATH` | Absolute non-secret artifact containing the observed native setup snapshot. |
| `REQUEST_PATH` | Absolute local-state request artifact following the docs chapter's schema. |

Interpret the result, never treating an exception as an empty installation:

1. `unchanged`: make no native write, launch no session and leave state unchanged.
2. `create-disabled`: show the proposed single Local entry for operator confirmation.
3. `update`: reconcile that exact verified ID, preserving enabled/paused status.
4. `blocked`: surface the reason and, where available, the proposed differences.

A changed prompt, cron, project or mode needs review before applying. Preserve an
existing operator model unless the operator explicitly approves a profile change.
If that approval is supplied, repeat the same helper command with
`-ApproveProfileChange`; use its output rather than inventing a different patch.
If changing the coordinator model is explicitly requested, include `-UpdateModel`
in both preview and approved invocations. Approval of a cron/prompt refresh alone
does not reset the model.

## Stage 5: Apply through supported native tools

For a new entry, native `save_workflow` receives the helper's exact
`project_id`, actual `host_id`, name, prompt, `enabled: false`, `mode: autopilot`,
selected model, `interval: manual` and `cron_expression`. If direct creation is
unavailable, open the creation confirmation dialog with those known values and
have the operator select/confirm the Local host. Never guess native fields or
introduce unsupported AI-credit/billing caps.

For one existing managed entry, call `save_workflow` with its `workflow_id` and
only approved differences. Do not set `enabled: true`, overwrite a renamed entry's
name, remove an operator model, reset native sessions or assume updating requires
recreation. If the tool requires another confirmation, obtain it normally.

For ambiguous duplicates, missing permission or unavailable exact native
capability, stop with an actionable manual step. No API database workaround.
Do not call `run_workflow`, create a session automation, invoke intake, create a
canary PR or start a repair as part of setup.

## Stage 6: Register the observed profile without changing work

Re-read the actual saved entry. Confirm project, Local environment, selected account,
cron, timezone and next-run preview with native UI. Do not assume timezone semantics.
Record the installed non-secret profile: automation/project/host/executor/login,
cadence/timezone/enabled, canonical policy and saved-prompt digests, and selected
coordinator/repair model. Use shared `Get-ScheduledDigest` rather than a different
canonicalization. Re-register only when that profile changes.
Include the explicitly selected `coordinator_effort` and `repair_effort` when
supported; omit native effort overrides rather than inventing a value for a model
without that capability.

Proposed model defaults are `gpt-5.4-mini` with medium effort for coordination and
`gpt-6-astra` with high effort for repair, subject to the operator's selection.
Preserve existing choices. Native idle `open_issue_session` plus subsequent
`send_session_message` alone exposes no repair-model override. Use the supported
`open_issue_session.kickoff` model/effort fields with the strictly inert interactive
bootstrap specified in `scheduled-intake`. Register the actual returned session
before enqueuing the real autopilot instruction. The bootstrap performs no
commands, edits, branch changes or publication. Verify this ordering/model behavior
in the pilot; retain the configured model of an existing owned session. Do not
supply a fictitious `repair_model` field to `save_workflow`.

```powershell
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
Import-Module .\scripts\scheduled\LocalState.psm1 -Force
Invoke-ScheduledLocalRequest -RequestPath "{{REQUEST_PATH}}"
```

Use an operator-approved `initialize` request only for established first enrollment,
then `register-profile`. Existing enrollment uses only `register-profile`; unchanged
registration is a no-op. Do not change mode to repair. An explicit pause stays paused
and all attempt/start/continuation history stays intact. Do not call `record-scan`:
setup is not successful intake.

Publish the observed profile to executor-owned rolling health metadata using shared
health record serialization: locate the single reporter-owned open issue labelled
`scheduled-health`, then create/update the single worker-login-owned comment whose
`health` record matches repository name, numeric repository ID and executor ID.
Never create a replacement issue, overwrite reporter-owned evidence or human
discussion, or append duplicate executor records. Missing/duplicate issues,
ambiguous comment ownership or a lost write outcome require reconciliation. If
permissions prevent publication, report that specific blocker and
retain the successfully registered local profile without pretending remote health
was updated.

## Stage 7: Report installation outcome and remaining manual gates

Report whether setup was unchanged, created-disabled, updated or blocked, with
the actual automation/project/Local host, observed schedule/timezone and mode.
Report missing personal entitlement attribution, native consent, local tooling,
issue-session reuse/branch naming, sleep/restart pilot, scope/enrollment or
publication safeguards as applicable.

Confirm that no run was triggered, repair was not enabled, account settings were
not changed and existing state/session ownership was preserved. List only genuine
remaining operator actions. Stop; no canary, background watcher or follow-up timer.
