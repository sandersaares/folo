Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true

# LocalState and ScheduledIntegration Pester tests import this fixture to populate their isolated
# TestDrive enrollment with persisted schema-v1 attempts. Production reservation is unsupported;
# reading legacy ownership must be tested without a production admission bypass or fake triage.
# No production module imports this fixture or writes state through it.
function Add-TestLegacyAttempt {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string] $StateRoot,
        [Parameter(Mandatory)][DateTimeOffset] $StartedAt,
        [int] $Issue = 1,
        [string] $Finding = ('f' * 64),
        [int] $Generation = 1,
        [string] $CheckId = 'mutants-ubuntu-latest-1',
        [string] $CheckKind = 'mutants',
        [string] $ContractDigest = ('c' * 64),
        [string] $EvidenceKey = 'initial-evidence',
        [hashtable] $Overrides = @{}
    )

    $path = Join-Path $StateRoot 'state.json'
    $state = Get-Content -LiteralPath $path -Raw | ConvertFrom-Json -AsHashtable
    $id = [guid]::NewGuid().ToString()
    $attempt = @{
        attempt_id = $id; issue_number = $Issue; finding_id = $Finding; generation = $Generation
        check_contract_digest = $ContractDigest; check_id = $CheckId; check_kind = $CheckKind
        started_at = $StartedAt.ToUniversalTime().ToString('o')
        session_id = $null; branch = $null; head_sha = $null; pr_number = $null
        phase = 'reserved'; reason = $null; continuations = @(); handled_evidence = @()
        evidence_key = $EvidenceKey; version_evidence = $null; proposed_responses = @(); explanation = $null
        dispatch = @{ token = [guid]::NewGuid().ToString(); status = 'reserved' }
    }
    foreach ($key in $Overrides.Keys) { $attempt[$key] = $Overrides[$key] }
    $state.attempts[$attempt.attempt_id] = $attempt
    $state.health.last_admission = $attempt.started_at
    $state | ConvertTo-Json -Depth 40 | Set-Content -LiteralPath $path
    return $attempt
}

Export-ModuleMember -Function Add-TestLegacyAttempt
