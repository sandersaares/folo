Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true

# Pure native-tool reconciliation: unknown metadata is a manual capability gap, not absence.
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

Export-ModuleMember -Function Get-ScheduledSetupDecision
