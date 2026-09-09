#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
# Protects operator setup from silently transferring or duplicating the native App automation:
# an executor/login/host mismatch against an already-registered profile must block rather than
# quietly reconcile, and incomplete native metadata must never be read as "not yet installed".
BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'LocalSetup.psm1') -Force
}
Describe 'Native setup reconciliation' {
    BeforeEach {
        $desired = @{
            repository = 'folo-rs/folo'; project_id = 'project'; host_id = 'real-local'
            executor_id = 'machine'; login = 'operator'; marker = 'folo-scheduled-remediation:v1'
            name = 'Folo scheduled finding intake'; cadence_cron = '17 */3 * * *'
            prompt = 'folo-scheduled-remediation:v1 Run scheduled-intake'; coordinator_model = 'cheap'
        }
        $script:live = @{
            id = 'saved'; repository = $desired.repository; project_id = 'project'; host_id = 'real-local'
            name = $desired.name; prompt = $desired.prompt; enabled = $false; interval = 'manual'
            cron_expression = $desired.cadence_cron; model = 'operator-choice'; mode = 'autopilot'
            workspace_type = 'worktree'
        }
        $script:registered = @{
            automation_id = 'saved'; executor_id = 'machine'; login = 'operator'; host_id = 'real-local'
        }
    }
    It 'proposes disabled creation but never creates or enables anything itself' {
        $result = Get-ScheduledSetupDecision -Desired $desired -Workflows @() -MetadataComplete $true
        $result.action | Should -Be create-disabled
        $result.changes.enabled | Should -BeFalse
        $result.changes.host_id | Should -Be real-local
        $result.changes.interval | Should -Be manual
    }
    It 'is idempotent and preserves a renamed entry, pause and operator model' {
        $live.name = 'My renamed intake'
        $result = Get-ScheduledSetupDecision -Desired $desired -Workflows @($live) `
            -RegisteredProfile $registered -MetadataComplete $true
        $result.action | Should -Be unchanged
        $result.workflow_id | Should -Be saved
        $result.changes.Count | Should -Be 0
    }
    It 'recreates a deleted automation without mutating enrollment' {
        $result = Get-ScheduledSetupDecision -Desired $desired -Workflows @() `
            -RegisteredProfile $registered -MetadataComplete $true
        $result.action | Should -Be create-disabled
        $registered.automation_id | Should -Be saved
    }
    It 'requires approval of policy refresh and preserves an existing pause' {
        $desired.cadence_cron = '17 */6 * * *'
        $result = Get-ScheduledSetupDecision -Desired $desired -Workflows @($live) -MetadataComplete $true
        $result.action | Should -Be blocked
        $result.changes.cron_expression | Should -Be $desired.cadence_cron
        $result = Get-ScheduledSetupDecision -Desired $desired -Workflows @($live) `
            -MetadataComplete $true -ApproveProfileChange
        $result.action | Should -Be update
        $result.changes.ContainsKey('enabled') | Should -BeFalse
        $result.changes.ContainsKey('model') | Should -BeFalse
    }
    It 'previews an explicit model change before approving that same change set' {
        $preview = Get-ScheduledSetupDecision -Desired $desired -Workflows @($live) `
            -MetadataComplete $true -UpdateModel
        $preview.action | Should -Be blocked
        $preview.changes.model | Should -Be cheap
        $approved = Get-ScheduledSetupDecision -Desired $desired -Workflows @($live) `
            -MetadataComplete $true -UpdateModel -ApproveProfileChange
        $approved.action | Should -Be update
        $approved.changes.model | Should -Be $preview.changes.model
    }
    It 'finds the managed marker when project IDs change' {
        $desired.project_id = 'recreated-project'
        $result = Get-ScheduledSetupDecision -Desired $desired -Workflows @($live) `
            -MetadataComplete $true -ApproveProfileChange
        $result.workflow_id | Should -Be saved
        $result.changes.project_id | Should -Be recreated-project
    }
    It 'blocks ambiguous duplicates including renamed entries' {
        $duplicate = $live.Clone()
        $duplicate.id = 'duplicate'; $duplicate.name = 'renamed'
        (Get-ScheduledSetupDecision -Desired $desired -Workflows @($live, $duplicate) `
            -MetadataComplete $true).reason | Should -Be ambiguous-managed-automations
    }
    It 'never treats unknown native metadata as no automation' {
        (Get-ScheduledSetupDecision -Desired $desired -Workflows @() `
            -MetadataComplete $false).reason | Should -Be native-metadata-unavailable
    }
    It 'does not adopt a human automation by matching only its name or cached ID' {
        $live.prompt = 'Run my own task'
        (Get-ScheduledSetupDecision -Desired $desired -Workflows @($live) `
            -RegisteredProfile $registered -MetadataComplete $true).reason |
            Should -Be unverified-automation-ownership
    }
    It 'does not invent a host or transfer an executor' {
        $desired.host_id = ''
        (Get-ScheduledSetupDecision -Desired $desired -Workflows @() `
            -MetadataComplete $true).reason | Should -Be select-real-local-host
        $desired.host_id = 'different-host'
        (Get-ScheduledSetupDecision -Desired $desired -Workflows @() `
            -RegisteredProfile $registered -MetadataComplete $true).reason |
            Should -Be executor-transfer-requires-reconciliation
    }
    It 'normalizes the observed native camelCase schema and reuses its disabled Local entry' {
        $native = @'
{
  "id": "saved",
  "name": "Folo scheduled finding intake",
  "projectId": "project",
  "hostId": "local",
  "interval": "manual",
  "cronExpression": "17 */3 * * *",
  "enabled": false,
  "model": "gpt-5.4-mini",
  "reasoningEffort": "medium",
  "mode": "autopilot",
  "workspaceType": "worktree",
  "prompt": "folo-scheduled-remediation:v1 Run scheduled-intake. Human responses require approval."
}
'@ | ConvertFrom-Json -AsHashtable
        $normalized = ConvertTo-ScheduledNativeWorkflow -Workflow $native `
            -ProjectRepository @{ project = 'folo-rs/folo' }
        $normalized.host_id | Should -Be local
        $normalized.reasoning_effort | Should -Be medium
        $normalized.workspace_type | Should -Be worktree
        $normalized.enabled | Should -BeFalse
        $desired.host_id = 'local'
        $desired.prompt = $native.prompt
        $desired.coordinator_model = 'gpt-5.4-mini'
        $desired.coordinator_effort = 'medium'
        $result = Get-ScheduledSetupDecision -Desired $desired -Workflows @($normalized) -MetadataComplete $true
        $result.action | Should -Be unchanged
        $result.workflow_id | Should -Be saved
        $result.changes.Count | Should -Be 0
        $native.Contains('host_id') | Should -BeFalse
    }
    It 'does not manufacture missing native fields or unknown project associations' {
        $native = @{ id = 'saved'; name = 'name'; projectId = 'unknown'; hostId = 'local'
            interval = 'manual'; cronExpression = '17 */3 * * *'; enabled = $false
            model = 'gpt-5.4-mini'; reasoningEffort = 'medium'; mode = 'autopilot'
            workspaceType = 'worktree'; prompt = 'folo-scheduled-remediation:v1' }
        { ConvertTo-ScheduledNativeWorkflow -Workflow $native `
            -ProjectRepository @{ project = 'folo-rs/folo' } } | Should -Throw
        $native.projectId = 'project'; $native.Remove('prompt')
        { ConvertTo-ScheduledNativeWorkflow -Workflow $native `
            -ProjectRepository @{ project = 'folo-rs/folo' } } | Should -Throw
    }
    It 'requires the observed execution workspace to be a worktree' {
        $live.workspace_type = 'branch'
        (Get-ScheduledSetupDecision -Desired $desired -Workflows @($live) `
            -MetadataComplete $true).reason | Should -Be local-worktree-required
    }
}
