#requires -Version 7
# Journals independent role health writes inside the shared local transaction. A current
# enrolled role scan may publish observations even when admission is paused; it cannot
# overwrite the other role's heartbeat or create a replacement health issue.
# Ref: ../../docs/scheduled-triage.md#readiness-and-health.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
Import-Module (Join-Path $PSScriptRoot 'ScheduledContracts.psm1')
Import-Module (Join-Path $PSScriptRoot 'LocalTriagePolicy.psm1')

function Invoke-ScheduledHealthStateChange {
    param($State, $Policy, [string] $Action, $Data, [DateTimeOffset] $Now)
    if ($Data['role'] -cnotin @('repair', 'triage')) { throw 'Unknown health role.' }
    $role = $Data.role
    if ($role -ceq 'triage') {
        $triagePolicy = Get-ScheduledTriagePolicy
        if (-not $State.Contains('triage') -or $triagePolicy.enrolled_machine_id -cne $State.executor_id) {
            throw 'Triage health is not enrolled.'
        }
        $scan = $State.triage.scan
        $installed = $State.triage.profile
    } else {
        if ($Policy.local.enrolled_machine_id -cne $State.executor_id) { throw 'Repair health is not enrolled.' }
        $scan = $State.coordinator
        $installed = $State.profile
    }
    if ($null -eq $installed -or $installed.login -cne $State.login -or
        $null -eq $scan -or $scan.token -cne $Data['scan_token'] -or [DateTimeOffset]$scan.expires_at -le $Now) {
        throw 'Health publication requires the current enrolled role scan.'
    }
    if ($Action -ceq 'health-authorize') { return }
    if (-not $State.Contains('health_publications')) { $State.health_publications = @{} }
    switch -CaseSensitive ($Action) {
        'health-prepare' {
            if ($State.health_publications.ContainsKey($role) -and
                $State.health_publications[$role].stage -cne 'complete') {
                throw 'Reconcile the previous health write before preparing another.'
            }
            if ($Data.intent.issue_number -le 0 -or
                -not ([string]$Data.intent.body).StartsWith('[Copilot speaking]')) {
                throw 'Health intent must identify the existing rolling surface.'
            }
            $State.health_publications[$role] = @{
                id = [guid]::NewGuid().ToString(); stage = 'prepared'; intent = $Data.intent
            }
        }
        'health-begin' {
            $operation = $State.health_publications[$role]
            if ($operation.stage -cne 'prepared') { throw 'Health write outcome must be reconciled.' }
            $operation.stage = 'sending'
        }
        'health-observe' {
            $operation = $State.health_publications[$role]
            if ($operation.stage -cne 'sending' -or $Data['comment_id'] -le 0 -or
                ($null -ne $operation.intent.comment_id -and $operation.intent.comment_id -ne $Data.comment_id)) {
                throw 'Observed health comment differs from the retained intent.'
            }
            $operation.intent.comment_id = $Data.comment_id
        }
        'health-confirm' {
            $operation = $State.health_publications[$role]
            if ($operation.id -cne $Data['operation_id'] -or $Data['comment_id'] -le 0 -or
                $Data['record_digest'] -cne (Get-ScheduledDigest $operation.intent.record)) {
                throw 'Health readback does not match its durable intent.'
            }
            $operation.intent.comment_id = $Data.comment_id
            $operation.stage = 'complete'
        }
        default { throw 'Unsupported health transition.' }
    }
}

Export-ModuleMember -Function Invoke-ScheduledHealthStateChange
