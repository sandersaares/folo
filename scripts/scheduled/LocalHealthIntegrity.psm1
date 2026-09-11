#requires -Version 7
# Validates restored role-health journals before LocalState permits any health transport.
# Immutable intent bytes stay separate from the observed comment ID and readback receipt.
# Ref: ../../docs/scheduled-triage.md#ownership-and-recovery.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
Import-Module (Join-Path $PSScriptRoot 'ScheduledContracts.psm1')

function Assert-HealthField {
    param([System.Collections.IDictionary] $Value, [string[]] $Names)
    if ($null -eq $Value) { throw [FormatException]::new('A health journal object is missing.') }
    foreach ($name in $Names) {
        if (-not $Value.Contains($name)) { throw [FormatException]::new("Health journal field is missing: $name") }
    }
}

function Assert-ScheduledHealthJournal {
    param([System.Collections.IDictionary] $State)
    if (-not $State.Contains('health_publications')) { return }
    if ($State.health_publications -isnot [System.Collections.IDictionary]) {
        throw [FormatException]::new('Role health journals must be keyed by their role.')
    }
    foreach ($entry in $State.health_publications.GetEnumerator()) {
        $operation = $entry.Value
        Assert-HealthField $operation @('id', 'stage', 'kind', 'intent', 'intent_digest', 'comment_id', 'receipt')
        $intent = $operation.intent
        Assert-HealthField $intent @('issue_number', 'comment_id', 'body', 'record', 'preimage')
        if ($entry.Key -cnotin @('repair', 'triage') -or [string]::IsNullOrWhiteSpace($operation.id) -or
            $operation.stage -cnotin @('prepared', 'sending', 'complete') -or
            [string]$intent.issue_number -cnotmatch '^[1-9][0-9]*$' -or
            $operation.intent_digest -cne (Get-ScheduledDigest $intent) -or
            -not ([string]$intent.body).StartsWith('[Copilot speaking]')) {
            throw [FormatException]::new('Restored health intent identity, stage or digest is invalid.')
        }
        $kind = if ($null -eq $intent.comment_id) { 'create-comment' } else { 'update-comment' }
        if ($operation.kind -cne $kind -or
            ($operation.stage -ceq 'prepared' -and $operation.comment_id -ne $intent.comment_id) -or
            ($null -ne $intent.comment_id -and (
                [string]$intent.comment_id -cnotmatch '^[1-9][0-9]*$' -or $operation.comment_id -ne $intent.comment_id)) -or
            ($null -ne $operation.comment_id -and [string]$operation.comment_id -cnotmatch '^[1-9][0-9]*$')) {
            throw [FormatException]::new('Restored health publication target differs from its intent.')
        }
        Assert-HealthField $intent.record @('schema_version', 'role', 'repository', 'repository_id', 'executor_id')
        $embedded = Read-ScheduledRecord $intent.body health
        if ($intent.record.schema_version -ne 1 -or $intent.record.role -cne $entry.Key -or
            $intent.record.repository -cne $State.repository -or $intent.record.repository_id -ne $State.repository_id -or
            $intent.record.executor_id -cne $State.executor_id -or
            (Get-ScheduledDigest $embedded) -cne (Get-ScheduledDigest $intent.record)) {
            throw [FormatException]::new('Restored health body does not identify its enrolled role record.')
        }
        if ($operation.stage -ceq 'complete') {
            Assert-HealthField $operation.receipt @('operation_id', 'comment_id', 'record_digest')
            if ($null -eq $operation.comment_id -or $operation.receipt.operation_id -cne $operation.id -or
                $operation.receipt.comment_id -ne $operation.comment_id -or
                $operation.receipt.record_digest -cne (Get-ScheduledDigest $intent.record)) {
                throw [FormatException]::new('Restored health receipt does not confirm its intent.')
            }
        } elseif ($null -ne $operation.receipt) {
            throw [FormatException]::new('An unfinished health publication cannot have a completion receipt.')
        }
    }
}

Export-ModuleMember -Function Assert-ScheduledHealthJournal
