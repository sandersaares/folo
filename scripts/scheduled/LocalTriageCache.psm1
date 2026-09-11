#requires -Version 7
# Owns triage snapshot payload files. Serialization precedes the short state transaction;
# atomic installation and deletion run under that transaction against durable ownership pins.
# Ref: ../../docs/scheduled-triage.md#ownership-and-recovery.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
Import-Module (Join-Path $PSScriptRoot 'ScheduledContracts.psm1')

function Get-ScheduledTriageCacheProjection {
    param($Triage)
    $scan = if ($null -ne $Triage.scan) {
        @{ token = $Triage.scan.token; session_id = $Triage.scan.session_id; snapshot_id = $Triage.scan.snapshot_id }
    } else { $null }
    $active = if ($null -ne $Triage.active_analysis_id) {
        $analysis = $Triage.analyses[$Triage.active_analysis_id]
        @{
            id = $analysis.id; session_id = $analysis.session_id; claim_token = $analysis.claim_token
            working_snapshot_id = $analysis.working_snapshot_id
            checkpoint_snapshot_id = if ($null -ne $analysis.checkpoint -and $analysis.checkpoint.ContainsKey('snapshot_id')) {
                $analysis.checkpoint.snapshot_id
            } else { $null }
        }
    } else { $null }
    return @{ scan = $scan; analysis = $active }
}

function Get-ScheduledTriageSnapshotPath {
    param([string] $StateRoot, [ValidatePattern('^[0-9a-f]{64}$')][string] $Id)
    return Join-Path $StateRoot "triage-cache\$Id.json"
}

function Read-ScheduledTriageSnapshot {
    param([string] $StateRoot, [string] $Id)
    $snapshot = Get-Content -LiteralPath (Get-ScheduledTriageSnapshotPath $StateRoot $Id) -Raw |
        ConvertFrom-Json -AsHashtable
    if ((Get-ScheduledDigest $snapshot) -cne $Id) { throw 'Cached triage observation changed; repeat the complete scan.' }
    return $snapshot
}

function Get-ScheduledTriageClaimValidation {
    param([string] $StateRoot, $Scan, $Data)
    if ($null -eq $Scan -or $null -eq $Scan.snapshot_id -or
        $Scan.token -cne $Data['scan_token'] -or $Scan.session_id -cne $Data['session_id']) {
        throw [FormatException]::new('Claim admission needs the owning scan and its pinned snapshot.')
    }
    $snapshot = Read-ScheduledTriageSnapshot $StateRoot $Scan.snapshot_id
    $fields = @('repository_id', 'workflow_id', 'run_id', 'run_attempt', 'digest', 'issue_number')
    if ($Data['revision'] -isnot [hashtable] -or $Data.revision.PSBase.Count -ne $fields.Count) {
        throw [FormatException]::new('Claim revision must contain exactly its identity fields.')
    }
    $revisionDigest = Get-ScheduledDigest $Data.revision
    $pending = @($snapshot.pending | Where-Object {
        $identity = @{}
        foreach ($field in $fields) { $identity[$field] = $_[$field] }
        (Get-ScheduledDigest $identity) -ceq $revisionDigest
    })
    if ($snapshot.successful_scan -ne $true -or $pending.Count -ne 1) {
        throw [FormatException]::new('Claim revision is not an exact pending member of the pinned scan.')
    }
    # The transaction rechecks these immutable inputs after the potentially large cache read.
    return @{ snapshot_id = $Scan.snapshot_id; revision_digest = $revisionDigest }
}

function Write-ScheduledTriageSnapshotFile {
    param([string] $StateRoot, [hashtable] $Snapshot, [ValidateSet('scan', 'analysis')][string] $OwnerKind,
        [ValidatePattern('^[0-9a-f-]{36}$')][string] $OwnerToken)
    $id = Get-ScheduledDigest $Snapshot
    $directory = Split-Path -Parent (Get-ScheduledTriageSnapshotPath $StateRoot $id)
    $null = New-Item -ItemType Directory -Path $directory -Force
    $path = Join-Path $directory "$id.$OwnerKind.$OwnerToken.$([guid]::NewGuid().ToString('N')).tmp"
    $bytes = [Text.Encoding]::UTF8.GetBytes(($Snapshot | ConvertTo-Json -Depth 100))
    $writer = [IO.File]::Open($path, [IO.FileMode]::CreateNew, [IO.FileAccess]::Write, [IO.FileShare]::None)
    try {
        $writer.Write($bytes)
        $writer.Flush($true)
    } finally { $writer.Dispose() }
    return @{ id = $id; temporary_path = $path; owner_kind = $OwnerKind; owner_token = $OwnerToken }
}

function Complete-ScheduledTriageSnapshotFile {
    param([string] $StateRoot, [string] $Id, [string] $TemporaryPath, [string] $OwnerKind, [string] $OwnerToken)
    $destination = Get-ScheduledTriageSnapshotPath $StateRoot $Id
    $directory = [IO.Path]::GetFullPath((Split-Path -Parent $destination))
    $name = [IO.Path]::GetFileName($TemporaryPath)
    $prefix = [regex]::Escape("$Id.$OwnerKind.$OwnerToken.")
    if ([IO.Path]::GetFullPath((Split-Path -Parent $TemporaryPath)) -cne $directory -or
        $name -cnotmatch "^$prefix[0-9a-f]{32}\.tmp$") {
        throw 'Snapshot installation must use its prepared owner-tagged cache file.'
    }
    Invoke-TriageSnapshotMove $TemporaryPath $destination
}

function Invoke-TriageSnapshotMove {
    # Keep the atomic filesystem boundary distinct from pin validation so interruption
    # between payload installation and the state commit can be exercised deterministically.
    param([string] $Source, [string] $Destination)
    [IO.File]::Move($Source, $Destination, $true)
}

function Invoke-ScheduledTriageCacheCleanup {
    param([string] $StateRoot, $Triage)
    $projection = Get-ScheduledTriageCacheProjection $Triage
    $pins = @(
        if ($null -ne $projection.scan -and $null -ne $projection.scan.snapshot_id) { $projection.scan.snapshot_id }
        if ($null -ne $projection.analysis) {
            if ($null -ne $projection.analysis.working_snapshot_id) { $projection.analysis.working_snapshot_id }
            if ($null -ne $projection.analysis.checkpoint_snapshot_id) { $projection.analysis.checkpoint_snapshot_id }
        }
    )
    $directory = Join-Path $StateRoot triage-cache
    if (-not (Test-Path -LiteralPath $directory)) { return }
    foreach ($file in Get-ChildItem -LiteralPath $directory -File) {
        if ($file.Name -cmatch '^([0-9a-f]{64})\.json$') {
            if ($Matches[1] -cnotin $pins) { Remove-Item -LiteralPath $file.FullName }
        } elseif ($file.Name -cmatch '^[0-9a-f]{64}\.(scan|analysis)\.([0-9a-f-]{36})\.[0-9a-f]{32}\.tmp$') {
            $kind = $Matches[1]; $token = $Matches[2]
            $owned = if ($kind -ceq 'scan') {
                $null -ne $Triage.scan -and $Triage.scan.token -ceq $token
            } elseif ($null -ne $Triage.active_analysis_id) {
                $analysis = $Triage.analyses[$Triage.active_analysis_id]
                $analysis.dispatch.status -ceq 'accepted' -and $analysis.dispatch.token -ceq $token
            } else { $false }
            # Token replacement/completion, not elapsed age, proves that a temporary writer
            # cannot install its file. Current scan/analysis writers retain their temporary data.
            if (-not $owned) { Remove-Item -LiteralPath $file.FullName }
        }
    }
}

Export-ModuleMember -Function Get-ScheduledTriageCacheProjection, Get-ScheduledTriageSnapshotPath,
Read-ScheduledTriageSnapshot, Write-ScheduledTriageSnapshotFile, Complete-ScheduledTriageSnapshotFile,
Invoke-ScheduledTriageCacheCleanup, Get-ScheduledTriageClaimValidation
