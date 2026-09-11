#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
# Keeps role installation decisions independent and disabled without selecting a paid model,
# guessing native identities, or mutating existing enrollment/analysis/repair state.
BeforeAll { Import-Module (Join-Path $PSScriptRoot 'LocalSetup.psm1') -Force }

Describe 'Independent disabled role setup' {
    BeforeEach {
        $script:desired = @{
            repository = 'owner/repository'; repository_id = 123; project_id = 'project'; host_id = 'observed-local'
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
    It 'clears an explicit effort only when model-default effort is selected for an approved update' {
        $triage = $repair.Clone(); $triage.id = 'triage'; $triage.prompt = $desired.prompt
        $triage.cron_expression = $desired.cadence_cron
        $desired.model = $triage.model; $desired.reasoning_effort = $null
        $unchanged = Get-ScheduledRoleSetupDecision -Role triage -Desired $desired `
            -Workflows @($triage) -MetadataComplete $true
        $unchanged.action | Should -Be unchanged
        $review = Get-ScheduledRoleSetupDecision -Role triage -Desired $desired `
            -Workflows @($triage) -MetadataComplete $true -UpdateModel
        $review.reason | Should -Be review-profile-differences
        $approved = Get-ScheduledRoleSetupDecision -Role triage -Desired $desired `
            -Workflows @($triage) -MetadataComplete $true -UpdateModel -ApproveProfileChange
        $approved.action | Should -Be update
        $approved.changes.ContainsKey('reasoning_effort') | Should -BeTrue
        $approved.changes.reasoning_effort | Should -BeNullOrEmpty
        $triage.reasoning_effort = $null
        (Get-ScheduledRoleSetupDecision -Role triage -Desired $desired -Workflows @($triage) `
            -MetadataComplete $true -UpdateModel -ApproveProfileChange).action | Should -Be unchanged
        $desired.Remove('reasoning_effort'); $triage.reasoning_effort = 'medium'
        (Get-ScheduledRoleSetupDecision -Role triage -Desired $desired -Workflows @($triage) `
            -MetadataComplete $true -UpdateModel -ApproveProfileChange).action | Should -Be unchanged
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
    It 'refuses a mismatched desired marker and requires pausing before changing an active role' {
        $desired.marker = 'folo-scheduled-remediation:v1'
        { Get-ScheduledRoleSetupDecision -Role triage -Desired $desired -Workflows @() -MetadataComplete $true } | Should -Throw
        $desired.marker = 'folo-scheduled-triage:v1'; $desired.model = 'chosen'; $desired.reasoning_effort = 'high'
        $triage = $repair.Clone(); $triage.id = 'triage'; $triage.prompt = $desired.prompt; $triage.enabled = $true
        (Get-ScheduledRoleSetupDecision -Role triage -Desired $desired -Workflows @($triage) -MetadataComplete $true).reason |
            Should -Be pause-role-before-setup
    }
    It 'reconciles the existing repair role independently without selecting a new model' {
        $desired.marker = 'folo-scheduled-remediation:v1'
        $desired.prompt = $repair.prompt; $desired.cadence_cron = $repair.cron_expression
        $result = Get-ScheduledRoleSetupDecision -Role repair -Desired $desired -Workflows @($repair) -MetadataComplete $true
        $result.action | Should -Be unchanged
        $result.workflow_id | Should -Be repair
        $result.role | Should -Be repair
    }
}
