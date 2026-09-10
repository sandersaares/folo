#requires -Version 7
# Runs the typed checkpoint contract outside LocalState's short filesystem transaction.
# Callers bind this result to the checkpoint digest rechecked under that transaction.
# Ref: ../../docs/scheduled-triage.md#ownership-and-recovery.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
Import-Module (Join-Path $PSScriptRoot 'ScheduledContracts.psm1')
Import-Module (Join-Path $PSScriptRoot 'ScheduledRecordTool.psm1')

function Get-ScheduledTriageCheckpointValidation {
    param([Parameter(Mandatory)][hashtable] $Checkpoint)
    foreach ($name in @('analysis', 'index', 'evidence', 'basis')) {
        if (-not $Checkpoint.ContainsKey($name)) { throw [FormatException]::new("Checkpoint field is missing: $name") }
    }
    $inputDigest = Get-ScheduledDigest $Checkpoint
    $snapshot = $Checkpoint | ConvertTo-Json -Depth 100 | ConvertFrom-Json -AsHashtable
    $snapshot.analysis = Invoke-ScheduledRecordTool -Package scheduled-triage-record -Request @{
        op = 'validate_analysis'; analysis = $snapshot.analysis
        evidence = $snapshot.evidence; index = $snapshot.index; basis = $snapshot.basis
    }
    return @{ input_digest = $inputDigest; checkpoint = $snapshot; digest = Get-ScheduledDigest $snapshot }
}

function Assert-ScheduledTriageCheckpointContent {
    param([Parameter(Mandatory)][hashtable] $Checkpoint, [Parameter(Mandatory)][string] $Digest)
    if ((Get-ScheduledDigest $Checkpoint) -cne $Digest) {
        throw [FormatException]::new('Retained checkpoint differs from its validated content.')
    }
    $validated = Get-ScheduledTriageCheckpointValidation $Checkpoint
    if ($validated.digest -cne $Digest) {
        throw [FormatException]::new('Retained checkpoint is not a canonical typed validation result.')
    }
}

Export-ModuleMember -Function Get-ScheduledTriageCheckpointValidation, Assert-ScheduledTriageCheckpointContent
