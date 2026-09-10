#requires -Version 7
# Read-only projection of independent Local role observations for hosted health. A repair
# heartbeat cannot stand in for triage, and inactive unenrolled roles are not expected.
# Ref: ../../docs/scheduled-triage.md#readiness-and-health.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
Import-Module (Join-Path $PSScriptRoot 'ScheduledContracts.psm1')
Import-Module (Join-Path $PSScriptRoot 'LocalTriagePolicy.psm1')
Import-Module (Join-Path $PSScriptRoot 'LocalTriageProfile.psm1')

function Get-ScheduledRoleScan {
    [CmdletBinding()]
    param($Policy, $TriagePolicy, [object[]] $Comments, [ValidateSet('repair', 'triage')][string] $Role)
    $executor = if ($Role -ceq 'triage') { $TriagePolicy.enrolled_machine_id } else { $Policy.local.enrolled_machine_id }
    $records = @(
        foreach ($comment in $Comments) {
            if ($comment.user.login -cne $Policy.worker_login -or
                -not ([string]$comment.body).Contains('<!-- scheduled-health:')) { continue }
            $record = Read-ScheduledRecord $comment.body health
            $recordRole = if ($record.ContainsKey('role')) { $record.role } else { 'repair' }
            if ($recordRole -ceq $Role) { $record }
        }
    )
    if ($records.Count -gt 1) { throw [FormatException]::new("Ambiguous $Role health ownership.") }
    if ($records.Count -eq 0) { return $null }
    $record = $records[0]
    if ($record.repository_id -ne $Policy.repository_id -or $record.repository -cne $Policy.repository -or
        [string]::IsNullOrWhiteSpace($executor) -or $record.executor_id -cne $executor) {
        throw [FormatException]::new("Foreign $Role health identity.")
    }
    if (-not $record.ContainsKey('last_successful_scan')) {
        if ($Role -ceq 'repair') { return $record }
        throw [FormatException]::new('Triage health has no successful-scan checkpoint.')
    }
    $blockers = $record['blocked_conditions']
    # An absent or malformed inventory is unavailable health, not evidence of no blockers.
    if ($blockers -isnot [System.Collections.IList] -or @($blockers | Where-Object {
        $_ -isnot [string] -or [string]::IsNullOrWhiteSpace($_)
    }).Count -gt 0) {
        throw [FormatException]::new('Role health has no complete blocked-condition inventory.')
    }
    $conditions = @($blockers)
    if ($Role -ceq 'triage') {
        foreach ($field in @('profile', 'profile_scan', 'profile_observation')) {
            if ($null -ne $record[$field] -and $record[$field] -isnot [hashtable]) {
                throw [FormatException]::new("Triage health $field must be an object or absent.")
            }
        }
        $installed = $record['profile']
        $scan = $record['profile_scan']
        $observed = Test-ScheduledTriageHealthObservation $installed $record['profile_observation'] $scan
        if ($null -eq $installed -or
            $installed['policy_digest'] -cne (Get-ScheduledTriagePolicyDigest $Policy $TriagePolicy) -or
            $installed['controller_digest'] -cne (Get-ScheduledTriageControllerDigest) -or
            $installed['cadence_cron'] -cne $TriagePolicy.cadence_cron -or -not $observed -or
            ($TriagePolicy.mode -ceq 'triage' -and (
                $installed.model -cne $TriagePolicy.model -or
                $installed.reasoning_effort -cne $TriagePolicy.reasoning_effort -or -not $installed.enabled))) {
            $conditions += 'triage-profile-drift'
        }
    }
    return @{
        completed_at = $record.last_successful_scan
        outcome = if ($conditions.Count -gt 0) { 'failed' } else { 'passed' }
        details = $record; blocked_conditions = $conditions
    }
}

Export-ModuleMember -Function Get-ScheduledRoleScan
