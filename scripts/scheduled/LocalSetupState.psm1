#requires -Version 7
# Records native automation creation intent for operator setup, independently of executor
# enrollment. The journal is a sibling of per-repository enrollment directories so installing
# a disabled entry cannot make a genuine first enrollment look like missing/corrupt state.
# Ref: ../../docs/scheduled-triage.md#independent-configuration.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
Import-Module (Join-Path $PSScriptRoot 'LocalState.psm1')
Import-Module (Join-Path $PSScriptRoot 'ScheduledRunGitHub.psm1')
Import-Module (Join-Path $PSScriptRoot 'ScheduledContracts.psm1')

function Get-ScheduledSetupJournalPath {
    [CmdletBinding()]
    param([Parameter(Mandatory)][long] $RepositoryId)
    $enrollment = Get-ScheduledStateRoot -RepositoryId $RepositoryId
    return Join-Path (Split-Path -Parent $enrollment) "setup-$RepositoryId.json"
}

function Assert-ScheduledSetupJournal {
    param([hashtable] $Journal, [long] $RepositoryId)
    if ($Journal.schema_version -ne 1 -or $Journal.repository_id -ne $RepositoryId -or
        $RepositoryId -le 0 -or $Journal.roles -isnot [hashtable]) {
        throw 'Setup journal is corrupt or belongs to another repository; do not recreate entries.'
    }
    $markers = @{ triage = 'folo-scheduled-triage:v1'; repair = 'folo-scheduled-remediation:v1' }
    foreach ($entry in $Journal.roles.GetEnumerator()) {
        $record = $entry.Value
        if (-not $markers.ContainsKey($entry.Key) -or $record -isnot [hashtable]) {
            throw 'Setup journal contains an unknown role.'
        }
        foreach ($field in @('stage', 'desired', 'automation_id', 'digest')) {
            if (-not $record.ContainsKey($field)) { throw "Setup role record is missing $field." }
        }
        $payload = @{ stage = $record.stage; desired = $record.desired; automation_id = $record.automation_id }
        if ($record.stage -cnotin @('creating', 'complete') -or $record.digest -cne (Get-ScheduledDigest $payload) -or
            ($record.stage -ceq 'creating' -and ($null -eq $record.desired -or $null -ne $record.automation_id)) -or
            ($record.stage -ceq 'complete' -and ($record.automation_id -isnot [string] -or
                [string]::IsNullOrWhiteSpace($record.automation_id)))) {
            throw 'Setup role stage, native identity or integrity is invalid.'
        }
        if ($null -ne $record.desired) {
            if ($record.desired -isnot [hashtable]) { throw 'Setup desired entry must be a verified object.' }
            foreach ($field in @('repository', 'project_id', 'host_id', 'name', 'marker', 'cadence_cron', 'prompt')) {
                if (-not $record.desired.ContainsKey($field) -or $record.desired[$field] -isnot [string] -or
                    [string]::IsNullOrWhiteSpace($record.desired[$field])) {
                    throw "Setup desired entry is missing a valid $field."
                }
            }
            if ($record.desired.repository -cnotmatch '^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$' -or
                $record.desired.marker -cne $markers[$entry.Key] -or
                -not $record.desired.prompt.Contains($record.desired.marker)) {
                throw 'Setup desired entry does not identify its repository and role.'
            }
            $modelField = if ($entry.Key -ceq 'triage') { 'model' } else { 'coordinator_model' }
            if (-not $record.desired.ContainsKey($modelField) -or $record.desired[$modelField] -isnot [string] -or
                [string]::IsNullOrWhiteSpace($record.desired[$modelField])) {
                throw 'Setup desired entry requires its operator-selected model.'
            }
        }
    }
}

function Invoke-ScheduledSetupJournal {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string] $Path,
        [Parameter(Mandatory)][long] $RepositoryId,
        [Parameter(Mandatory)][ValidateSet('read', 'begin-create', 'confirm')][string] $Action,
        [ValidateSet('triage', 'repair')][string] $Role,
        [hashtable] $Data = @{}
    )
    if (-not [IO.Path]::IsPathFullyQualified($Path) -or $RepositoryId -le 0) {
        throw 'Setup journal requires an absolute path and canonical repository identity.'
    }
    if ($Action -ceq 'read' -and -not (Test-Path -LiteralPath $Path)) {
        return @{ schema_version = 1; repository_id = $RepositoryId; roles = @{} }
    }
    if ($Action -cne 'read' -and $Data['operator_approved'] -ne $true) {
        throw 'Only explicit operator setup may change the native creation journal.'
    }
    $null = New-Item -ItemType Directory -Path (Split-Path -Parent $Path) -Force
    $lock = [IO.File]::Open("$Path.lock", [IO.FileMode]::OpenOrCreate, [IO.FileAccess]::ReadWrite, [IO.FileShare]::None)
    try {
        $journal = if (Test-Path -LiteralPath $Path) {
            Get-Content -LiteralPath $Path -Raw | ConvertFrom-Json -AsHashtable
        } else { @{ schema_version = 1; repository_id = $RepositoryId; roles = @{} } }
        Assert-ScheduledSetupJournal $journal $RepositoryId
        if ($Action -ceq 'read') { return $journal }
        if ([string]::IsNullOrWhiteSpace($Role)) { throw 'A setup role is required.' }
        if ($Action -ceq 'begin-create') {
            if ($journal.roles.ContainsKey($Role) -and $journal.roles[$Role].stage -ceq 'creating') {
                throw 'An earlier native create remains unresolved; do not call save_workflow again.'
            }
            if (-not $Data.ContainsKey('desired') -or [string]::IsNullOrWhiteSpace($Data.desired.host_id)) {
                throw 'Record the verified desired native entry before creation.'
            }
            $journal.roles[$Role] = @{
                stage = 'creating'; desired = $Data.desired; automation_id = $null
            }
        } else {
            if ([string]::IsNullOrWhiteSpace($Data['automation_id']) -or $Data['ownership_verified'] -ne $true) {
                throw 'Confirm only an actual native automation with verified role ownership.'
            }
            if (-not $journal.roles.ContainsKey($Role)) {
                $journal.roles[$Role] = @{ stage = 'complete'; desired = $null; automation_id = $Data.automation_id }
            } else {
                $journal.roles[$Role].automation_id = $Data.automation_id
                $journal.roles[$Role].stage = 'complete'
            }
        }
        $record = $journal.roles[$Role]
        $record.digest = Get-ScheduledDigest @{
            stage = $record.stage; desired = $record.desired; automation_id = $record.automation_id
        }
        Assert-ScheduledSetupJournal $journal $RepositoryId
        Write-ScheduledRunJournal -Path $Path -Record $journal
        return $journal
    } finally { $lock.Dispose() }
}

Export-ModuleMember -Function Get-ScheduledSetupJournalPath, Invoke-ScheduledSetupJournal, Assert-ScheduledSetupJournal
