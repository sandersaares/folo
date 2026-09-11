Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
Import-Module (Join-Path $PSScriptRoot 'LocalSetupState.psm1')

# Reconciles the desired native App automation profile (host, executor, repository) against the
# App's own registered automations, for the one-time/occasional operator setup flow (not the
# scheduled skills' hot path). Kept free of native App calls so the reconciliation decision itself
# is unit-testable; the caller performs the actual create/update against the native automation
# once this module says which action is safe. See
# ../../docs/scheduled-validation.md#installation-and-operating-profile.
function Get-ScheduledSetupDecision {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][System.Collections.IDictionary] $Desired,
        [Parameter(Mandatory)][AllowEmptyCollection()][object[]] $Workflows,
        [Parameter(Mandatory)][bool] $MetadataComplete,
        [AllowNull()][System.Collections.IDictionary] $RegisteredProfile,
        [switch] $ApproveProfileChange,
        [switch] $UpdateModel
    )
    $result = @{ action = 'blocked'; reason = $null; workflow_id = $null; changes = @{} }
    # Unknown metadata is a manual capability gap, not evidence the automation is absent - treat
    # it as blocked rather than inferring "safe to create" from an incomplete native App read.
    if (-not $MetadataComplete) { $result.reason = 'native-metadata-unavailable'; return $result }
    if ([string]::IsNullOrWhiteSpace($Desired.host_id)) {
        $result.reason = 'select-real-local-host'; return $result
    }
    if ($null -ne $RegisteredProfile -and
        ($RegisteredProfile.executor_id -cne $Desired.executor_id -or
            $RegisteredProfile.login -cne $Desired.login -or
            $RegisteredProfile.host_id -cne $Desired.host_id)) {
        $result.reason = 'executor-transfer-requires-reconciliation'; return $result
    }
    $candidates = @($Workflows | Where-Object {
        $_.repository -ceq $Desired.repository -and (
            $_.prompt.Contains($Desired.marker) -or
            ($null -ne $RegisteredProfile -and $_.id -ceq $RegisteredProfile.automation_id) -or
            ($_.project_id -ceq $Desired.project_id -and $_.name -ceq $Desired.name))
    })
    if ($candidates.Count -gt 1) { $result.reason = 'ambiguous-managed-automations'; return $result }
    if ($candidates.Count -eq 0) {
        $result.action = 'create-disabled'
        $result.changes = @{
            name = $Desired.name; project_id = $Desired.project_id; host_id = $Desired.host_id
            interval = 'manual'; cron_expression = $Desired.cadence_cron; prompt = $Desired.prompt
            enabled = $false; mode = 'autopilot'; model = $Desired.coordinator_model
        }
        if ($Desired.Contains('coordinator_effort')) {
            $result.changes.reasoning_effort = $Desired.coordinator_effort
        }
        return $result
    }
    $live = $candidates[0]
    $result.workflow_id = $live.id
    if (-not $live.prompt.Contains($Desired.marker)) {
        $result.reason = 'unverified-automation-ownership'; return $result
    }
    if ($live.host_id -cne $Desired.host_id) {
        $result.reason = 'live-host-differs'; return $result
    }
    if ($live.workspace_type -cne 'worktree') {
        $result.reason = 'local-worktree-required'; return $result
    }
    # A renamed managed entry is still the same automation. Preserve the name and pause,
    # along with an operator's model choice unless a profile change is explicitly approved.
    $expected = @{
        project_id = $Desired.project_id; interval = 'manual'; cron_expression = $Desired.cadence_cron
        prompt = $Desired.prompt; mode = 'autopilot'
    }
    if ($UpdateModel) {
        $expected.model = $Desired.coordinator_model
        if ($Desired.Contains('coordinator_effort')) {
            $expected.reasoning_effort = $Desired.coordinator_effort
        }
    }
    foreach ($entry in $expected.GetEnumerator()) {
        if ($live[$entry.Key] -cne $entry.Value) { $result.changes[$entry.Key] = $entry.Value }
    }
    if ($result.changes.Count -eq 0) { $result.action = 'unchanged'; return $result }
    if (-not $ApproveProfileChange) {
        $result.reason = 'review-profile-differences'; return $result
    }
    $result.action = 'update'
    return $result
}

function ConvertTo-ScheduledNativeWorkflow {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][System.Collections.IDictionary] $Workflow,
        [Parameter(Mandatory)][System.Collections.IDictionary] $ProjectRepository
    )
    # list_workflows uses camelCase, whereas the controller contracts use snake_case.
    # Repository identity comes from separately verified project metadata, not prompt text.
    foreach ($field in @('id', 'name', 'projectId', 'hostId', 'interval', 'cronExpression',
        'enabled', 'model', 'reasoningEffort', 'mode', 'workspaceType', 'prompt')) {
        if (-not $Workflow.Contains($field)) { throw "Native automation field '$field' is unavailable." }
    }
    if ([string]::IsNullOrWhiteSpace($Workflow.projectId) -or
        -not $ProjectRepository.Contains($Workflow.projectId) -or
        [string]::IsNullOrWhiteSpace($ProjectRepository[$Workflow.projectId])) {
        throw 'Native project-to-repository association has not been verified.'
    }
    if ($Workflow.enabled -isnot [bool]) { throw 'Native enabled status must be a boolean.' }
    return @{
        id = $Workflow.id; name = $Workflow.name; repository = $ProjectRepository[$Workflow.projectId]
        project_id = $Workflow.projectId; host_id = $Workflow.hostId
        interval = $Workflow.interval; cron_expression = $Workflow.cronExpression
        enabled = $Workflow.enabled; model = $Workflow.model; reasoning_effort = $Workflow.reasoningEffort
        mode = $Workflow.mode; workspace_type = $Workflow.workspaceType; prompt = $Workflow.prompt
    }
}

function Get-ScheduledRoleSetupDecision {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][ValidateSet('triage', 'repair')][string] $Role,
        [Parameter(Mandatory)][hashtable] $Desired,
        [Parameter(Mandatory)][AllowEmptyCollection()][object[]] $Workflows,
        [Parameter(Mandatory)][bool] $MetadataComplete,
        [AllowNull()][hashtable] $RegisteredProfile,
        [AllowNull()][hashtable] $SetupJournal,
        [switch] $ApproveProfileChange,
        [switch] $UpdateModel
    )
    # This is an installation decision, not admission or a live automation update. Distinct
    # markers prevent either role from adopting the other's existing entry.
    # Ref: ../../docs/scheduled-triage.md#independent-configuration.
    $markers = @{ triage = 'folo-scheduled-triage:v1'; repair = 'folo-scheduled-remediation:v1' }
    if ($null -ne $SetupJournal) {
        Assert-ScheduledSetupJournal $SetupJournal $SetupJournal.repository_id
    }
    if ($Desired.marker -cne $markers[$Role]) {
        throw 'Desired automation marker does not identify its role.'
    }
    $otherRole = if ($Role -ceq 'triage') { 'repair' } else { 'triage' }
    if ($MetadataComplete) {
        foreach ($workflow in $Workflows) {
            if ($workflow.repository -ceq $Desired.repository -and
                $workflow.prompt.Contains($Desired.marker) -and $workflow.prompt.Contains($markers[$otherRole])) {
                return @{ action = 'blocked'; reason = 'ambiguous-role-markers'; workflow_id = $workflow.id; changes = @{} }
            }
        }
    }
    $comparison = $Desired.Clone()
    if ($Role -ceq 'triage') {
        $comparison.coordinator_model = $Desired['model']
        if ($Desired.ContainsKey('reasoning_effort')) {
            $comparison.coordinator_effort = $Desired.reasoning_effort
        }
    }
    $decision = Get-ScheduledSetupDecision -Desired $comparison -Workflows $Workflows `
        -MetadataComplete $MetadataComplete -RegisteredProfile $RegisteredProfile `
        -ApproveProfileChange:$ApproveProfileChange -UpdateModel:$UpdateModel
    if ($decision.action -ceq 'create-disabled' -and $null -ne $SetupJournal -and
        $SetupJournal.roles.ContainsKey($Role) -and $SetupJournal.roles[$Role].stage -ceq 'creating') {
        return @{ action = 'blocked'; reason = 'unknown-native-create'; workflow_id = $null; changes = @{} }
    }
    if ($Role -ceq 'triage' -and ($decision.action -ceq 'create-disabled' -or $UpdateModel) -and
        [string]::IsNullOrWhiteSpace($comparison.coordinator_model)) {
        return @{ action = 'blocked'; reason = 'select-triage-model'; workflow_id = $decision.workflow_id; changes = @{} }
    }
    if ($null -ne $decision.workflow_id -and
        @($Workflows | Where-Object { $_.id -ceq $decision.workflow_id -and $_.enabled }).Count -gt 0) {
        return @{ action = 'blocked'; reason = 'pause-role-before-setup'; workflow_id = $decision.workflow_id; changes = @{} }
    }
    $decision.role = $Role
    return $decision
}

Export-ModuleMember -Function Get-ScheduledSetupDecision, ConvertTo-ScheduledNativeWorkflow,
Get-ScheduledRoleSetupDecision
