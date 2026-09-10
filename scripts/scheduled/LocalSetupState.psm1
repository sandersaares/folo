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

function Get-ScheduledSetupJournalPath {
    [CmdletBinding()]
    param([Parameter(Mandatory)][long] $RepositoryId)
    $enrollment = Get-ScheduledStateRoot -RepositoryId $RepositoryId
    return Join-Path (Split-Path -Parent $enrollment) "setup-$RepositoryId.json"
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
        if ($journal.schema_version -ne 1 -or $journal.repository_id -ne $RepositoryId -or
            $journal.roles -isnot [hashtable]) {
            throw 'Setup journal is corrupt or belongs to another repository; do not recreate entries.'
        }
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
        Write-ScheduledRunJournal -Path $Path -Record $journal
        return $journal
    } finally { $lock.Dispose() }
}

Export-ModuleMember -Function Get-ScheduledSetupJournalPath, Invoke-ScheduledSetupJournal
