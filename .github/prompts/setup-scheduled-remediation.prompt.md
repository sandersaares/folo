# Reconcile disabled Local scheduled roles

Apply this procedure only when the operator explicitly requests installation/setup.
Reconcile the existing repair entry and a separate triage entry using the selected
personal Copilot account and an actual **Local** App environment. Create only
disabled entries. Do not run either automation, enroll implicitly, enable schedules,
change accounts/billing, start a pilot, dispatch checks, or reset any existing work.

Hosted execution and reporting remain independently enabled by reviewed policy.
Manual Selected/Full deep checks do not require Local installation or repair
allowlists. Executable triage is separate from the still-unavailable new-repair
handoff: `reserve-attempt` continues to reject `ai-triage-unavailable`.

## Stage 1: Read desired state and operator choices

Read `docs\scheduled-validation.md`, `docs\scheduled-triage.md`,
`scripts\scheduled\policy.json`, `scripts\scheduled\triage-policy.json`, and the
`scheduled-intake`, `scheduled-triage` and `scheduled-repair` skills.

Repair retains its marker, schedule, scopes, budgets, native sessions and selected
models. Triage has its own marker, cadence, mode, profile and budget. The triage
model/effort are intentionally unconfigured; ask the operator to choose supported
native settings rather than selecting a paid model or inheriting a default silently.
Preserve an existing role's model unless an explicit model change is requested.

Verify the selected GitHub identity without printing credentials:

```powershell
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
gh api user --jq '{login: .login, id: .id}'
gh api repos/folo-rs/folo --jq '{id: .id, full_name: .full_name, default_branch: .default_branch}'
```

These commands contain no placeholders. Compare the actual results with reviewed
policy and stop on mismatch. GitHub login does not prove personal inference
billing, model quality, consent or machine availability. Record those remaining
operator exercises without claiming they succeeded. Do not install tooling or
build/run checks during setup.

## Stage 2: Discover native entries and durable ownership

Use native `list_projects` to identify the canonical repository project and
`list_workflows` without an enabled-only filter. Obtain actual Local host, project,
prompt, model/effort, mode, enabled state, workspace type, timezone and next-run
preview through supported native metadata/UI. A host ID of `local` is valid only
when actually observed. Never substitute a project ID, machine name or guessed ID.

Inspect renamed entries, entries associated with an earlier project ID and cached
registered IDs. Match verified repository identity and the role marker; a name or
cached ID alone is not ownership proof. If native metadata is incomplete, use
supported App UI/operator confirmation or stop. Do not inspect private App storage.

Resolve existing executor state with `Get-ScheduledStateRoot`. Missing/corrupt
state in an existing enrollment requires recovery, not empty initialization.
Reconcile retained GitHub/native ownership before any genuinely new enrollment.
Keep enrollment a separate explicit operator action. Disabled entry installation
does not require fabricating an executor ID or creating `state.json`.

Use the separate setup journal to retain an uncertain native create even when
executor enrollment does not yet exist:

```powershell
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
Import-Module .\scripts\scheduled\LocalSetupState.psm1 -Force
$path = Get-ScheduledSetupJournalPath -RepositoryId {{REPOSITORY_ID}}
Invoke-ScheduledSetupJournal -Path $path -RepositoryId {{REPOSITORY_ID}} -Action read |
    ConvertTo-Json -Depth 100
```

| Placeholder | Source |
|---|---|
| `REPOSITORY_ID` | Numeric repository ID verified through GitHub, not an App project ID or repository name. |

Preserve an existing `creating` entry. An empty native lookup does not authorize
another create while its previous outcome remains uncertain.

## Stage 3: Compute independent reconciliation decisions

Normalize actual native metadata with `ConvertTo-ScheduledNativeWorkflow`.
Its camelCase fields are native read metadata, not fields to invent on a write.
Join project IDs to repositories only from verified native/registered association.

Prepare a JSON artifact containing `role`, `desired`, `workflows`,
`metadata_complete`, `registered_profile` and the actual `setup_journal`.

| Object | Required content |
|---|---|
| `desired` | Verified repository name and numeric `repository_id`, project/Local host, existing executor/login when enrolled, reviewed name/marker/cron and prompt. Repair uses its existing `coordinator_model` and optional supported `coordinator_effort`; triage uses operator-selected `model` and optional `reasoning_effort`. |
| `workflows` | Normalized actual native entries, including all candidate roles and renamed entries. |
| `registered_profile` | That role's existing profile, not the other role's; null only when absence/recovery is established. |
| `metadata_complete` | True only when all facts needed for matching and comparison are available. |
| `setup_journal` | The helper's persisted read result, including any unknown native create. |

The saved prompts contain only their own reviewed marker and point to the
corresponding version-controlled skill:

```text
TRIAGE_MARKER
Run the repository's scheduled-triage skill for folo-rs/folo in Local mode.
Use the selected App model for complete evidence-backed diagnosis and causal
comparison. Reconcile durable ownership and publication; never edit source,
create repair PRs, authorize new repairs, activate automation or change billing.
```

```text
REPAIR_MARKER
Run the repository's scheduled-intake skill for folo-rs/folo in Local mode.
Reconcile and continue already registered repairs in their existing native
sessions within policy and budgets. New repair admission remains unavailable
with ai-triage-unavailable; completed triage does not lift that boundary.
Do not create or enable automation, start new repairs or change accounts/billing.
```

Replace `TRIAGE_MARKER` and `REPAIR_MARKER` with their respective reviewed policy
values. Do not put both markers in one prompt or duplicate the skill protocol.

```powershell
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
Import-Module .\scripts\scheduled\LocalSetup.psm1 -Force
$inputData = Get-Content -LiteralPath "{{SETUP_INPUT_PATH}}" -Raw | ConvertFrom-Json -AsHashtable
Get-ScheduledRoleSetupDecision -Role $inputData.role -Desired $inputData.desired `
    -Workflows $inputData.workflows -MetadataComplete $inputData.metadata_complete `
    -RegisteredProfile $inputData.registered_profile -SetupJournal $inputData.setup_journal |
    ConvertTo-Json -Depth 100
```

| Placeholder | Source |
|---|---|
| `SETUP_INPUT_PATH` | Absolute non-secret artifact containing verified native facts and desired settings for one role. |

`unchanged` means no native write. `blocked` preserves work and reports the reason.
Review proposed differences before applying an `update`; rerun the same comparison
with `-ApproveProfileChange` after approval. Include `-UpdateModel` only for an
explicitly requested model/effort change. A paused role stays paused, a renamed entry
keeps its name, and an unexpectedly enabled entry requires operator reconciliation.

## Stage 4: Apply only approved disabled native changes

Before native `save_workflow` creates a missing entry, persist its intent:

```powershell
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
Import-Module .\scripts\scheduled\LocalSetupState.psm1 -Force
$inputData = Get-Content -LiteralPath "{{SETUP_INPUT_PATH}}" -Raw | ConvertFrom-Json -AsHashtable
$path = Get-ScheduledSetupJournalPath -RepositoryId {{REPOSITORY_ID}}
Invoke-ScheduledSetupJournal -Path $path -RepositoryId {{REPOSITORY_ID}} `
    -Role $inputData.role -Action begin-create `
    -Data @{ operator_approved = $true; desired = $inputData.desired } |
    ConvertTo-Json -Depth 100
```

Placeholders are the same verified input path and numeric repository ID above.
An exception stops creation. A later unknown create outcome is reconciled by
native lookup and the existing marker/identity; never repeat creation blindly.

Use native `save_workflow` with only the helper's supported changes. Custom cron is
`interval: manual` plus `cron_expression`. Creation has `enabled: false` and the
actual host/project; updates target only the verified `workflow_id`. Do not supply
invented billing, spending-cap, `repair_model` or workspace fields. The repair
session model belongs to its supported kickoff field, not `save_workflow`.

For an approved update with an explicit null `reasoning_effort` change, clear the
saved override with `clear_reasoning_effort: true` and omit `reasoning_effort` from
the native call. An absent change key preserves the existing effort selection.

Re-read the entry and verify identity, role marker and selected settings. Confirm
the actual ID through `Invoke-ScheduledSetupJournal` with `-Action confirm` and
`data` containing `operator_approved`, `ownership_verified` and `automation_id`.
If confirmation is unavailable, retain the creation fence and report the blocker.
Do not delete arbitrary entries, run an automation or create a per-PR timer.

## Stage 5: Register observations without resetting work

For an already verified enrollment, register only that role's observed profile.
Repair uses the existing `register-profile`; triage uses `triage-register-profile`.
These are operator setup operations through the existing `scheduled-local` /
`Invoke-ScheduledLocalAction` surface, not the AI-facing `scheduled-triage` entry.
The approval field records an actual operator decision; it is not a credential
that the triage role may supply to grant itself authority.
Both preserve claims, counters and owned sessions. Do not call `record-scan`:
setup is not a successful queue scan.

The triage profile contains actual automation/project/host/executor/login/numeric
user identity, enabled state, model/effort, cron/timezone and policy/controller/
prompt digests. Use the shared digest helpers. Do not store credentials or inferred
billing fields. If enrollment is intentionally absent, report profile registration
as deferred instead of initializing it implicitly.

Compute triage's registered `prompt_digest` with
`Get-ScheduledTriagePromptDigest` over the actual approved saved App prompt, not
over the skill file. `Get-ScheduledTriageControllerDigest` independently includes
the executable skill and normalized current controller content. Later executions
must supply fresh native prompt/identity facts bound to their scan or accepted
dispatch; installing or registering the profile does not supply those observations.

Report each role's unchanged/created-disabled/updated/blocked outcome separately.
Identify remaining operator enrollment, model/billing/consent, timezone,
machine/restart and semantic-quality exercises. Do not call those facts proved by
helper tests, configuration, or successful native metadata writes.

No activation or live exercise is part of this setup. New repair admission remains
a separate implementation boundary even after triage installation succeeds.
