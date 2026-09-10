#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
# Keeps role installation decisions independent and disabled without selecting a paid model,
# guessing native identities, or mutating existing enrollment/analysis/repair state.
BeforeAll { Import-Module (Join-Path $PSScriptRoot 'LocalSetup.psm1') -Force }

Describe 'Independent disabled role setup' {
    BeforeEach {
        $script:desired = @{
            repository = 'owner/repository'; project_id = 'project'; host_id = 'observed-local'
            executor_id = 'executor'; login = 'operator'; name = 'Folo scheduled failure triage'
            marker = 'folo-scheduled-triage:v1'; cadence_cron = '7 */3 * * *'
            prompt = 'folo-scheduled-triage:v1 Run scheduled-triage'; model = $null
        }
        $script:repair = @{
            id = 'repair'; repository = 'owner/repository'; project_id = 'project'; host_id = 'observed-local'
            name = 'Folo scheduled finding intake'; prompt = 'folo-scheduled-remediation:v1 Run scheduled-intake'
            enabled = $false; interval = 'manual'; cron_expression = '17 */3 * * *'
            model = 'existing-choice'; reasoning_effort = 'medium'; mode = 'autopilot'; workspace_type = 'worktree'
        }
    }
    It 'requires an operator-selected triage model and leaves the existing repair entry untouched' {
        $before = $repair | ConvertTo-Json -Compress
        (Get-ScheduledRoleSetupDecision -Role triage -Desired $desired -Workflows @($repair) `
            -MetadataComplete $true).reason | Should -Be select-triage-model
        ($repair | ConvertTo-Json -Compress) | Should -BeExactly $before
    }
    It 'proposes only a separate disabled entry using supported native fields' {
        $desired.model = 'operator-choice'
        $decision = Get-ScheduledRoleSetupDecision -Role triage -Desired $desired -Workflows @($repair) -MetadataComplete $true
        $decision.action | Should -Be create-disabled
        $decision.changes.enabled | Should -BeFalse
        $decision.changes.host_id | Should -Be observed-local
        $decision.changes.model | Should -Be operator-choice
        $decision.changes.ContainsKey('repair_model') | Should -BeFalse
        $decision.changes.ContainsKey('billing') | Should -BeFalse
    }
    It 'preserves a renamed disabled role and its selected model on repeated setup' {
        $triage = $repair.Clone()
        $triage.id = 'triage'; $triage.name = 'Renamed triage'
        $triage.prompt = $desired.prompt; $triage.cron_expression = $desired.cadence_cron
        $decision = Get-ScheduledRoleSetupDecision -Role triage -Desired $desired `
            -Workflows @($repair, $triage) -MetadataComplete $true
        $decision.action | Should -Be unchanged
        $decision.workflow_id | Should -Be triage
        $decision.changes.Count | Should -Be 0
    }
    It 'does not infer a missing entry from incomplete native metadata' {
        (Get-ScheduledRoleSetupDecision -Role triage -Desired $desired -Workflows @() `
            -MetadataComplete $false).reason | Should -Be native-metadata-unavailable
    }
    It 'rejects ambiguous cross-role markers rather than adopting or duplicating an entry' {
        $repair.prompt += " $($desired.marker)"
        (Get-ScheduledRoleSetupDecision -Role triage -Desired $desired -Workflows @($repair) `
            -MetadataComplete $true).reason | Should -Be ambiguous-role-markers
    }
}
