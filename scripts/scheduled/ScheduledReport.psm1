#requires -Version 7

# Coverage/health merging and compatibility handling for existing registered repair records.
# New failures are run-level intake, never assigned semantic problem identities by this module.
# Kept free of GitHub calls so coverage and existing-confirmation transitions are unit-testable.
# Reporter-owned state is independent of worker comments and of artifact retention. See
# ../../.github/workflows/implementation.md#serialized-reporting and
# ../../docs/scheduled-validation.md#health-recovery-and-rollback.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
Import-Module (Join-Path $PSScriptRoot 'ScheduledContracts.psm1')
Import-Module (Join-Path $PSScriptRoot 'ScheduledPlan.psm1')

function Compare-ScheduledObservation {
    [CmdletBinding()]
    [OutputType([int])]
    param(
        [Parameter(Mandatory)][hashtable] $Left,
        [Parameter(Mandatory)][hashtable] $Right
    )

    if ($Left.run_id -eq $Right.run_id) {
        return ([long]$Left.run_attempt).CompareTo([long]$Right.run_attempt)
    }
    # Attempts can rerun an older workflow run after a newer run completed. Use the API's
    # attempt start, not the original run number/creation time or late completion time.
    # Creation time and immutable run ID provide stable ties at API timestamp precision.
    foreach ($observation in @($Left, $Right)) {
        if (-not $observation.ContainsKey('created_at') -or -not $observation.ContainsKey('run_started_at')) {
            throw [FormatException]::new('Observation lacks authoritative run creation or attempt start time.')
        }
    }
    $order = ([datetimeoffset]$Left.run_started_at).CompareTo([datetimeoffset]$Right.run_started_at)
    if ($order -ne 0) { return $order }
    $order = ([datetimeoffset]$Left.created_at).CompareTo([datetimeoffset]$Right.created_at)
    if ($order -ne 0) { return $order }
    return ([long]$Left.run_id).CompareTo([long]$Right.run_id)
}

function Merge-ScheduledObservation {
    [CmdletBinding()]
    [OutputType([hashtable])]
    param(
        [AllowNull()][hashtable] $Existing,
        [Parameter(Mandatory)][hashtable] $Incoming,
        [Parameter(Mandatory)][scriptblock] $IsAncestor
    )

    if ($Incoming.schema_version -ne 1 -or $Incoming.observation.run_id -le 0 -or
        $Incoming.observation.run_attempt -le 0 -or $Incoming.observation.run_number -le 0) {
        throw [FormatException]::new('Observation lacks execution identity.')
    }
    Assert-ScheduledSha $Incoming.source_sha
    $next = $Incoming | ConvertTo-Json -Depth 100 | ConvertFrom-Json -AsHashtable
    if ($null -eq $Existing) {
        if ($Incoming.observation.outcome -cne 'findings') { return $null }
        $next.generation = 1
        $next.status = 'open'
        $next.confirmation = $null
        return $next
    }
    if ($Existing.repository -cne $Incoming.repository -or $Existing.finding_id -cne $Incoming.finding_id) {
        throw [ArgumentException]::new('Cannot merge different findings.')
    }
    if ($Incoming.generation -ne $Existing.generation -or
        (Compare-ScheduledObservation $Incoming.observation $Existing.observation) -le 0) {
        return $Existing
    }
    if (-not (& $IsAncestor $Existing.source_sha $Incoming.source_sha)) { return $Existing }
    # A different contract cannot prove absence of the original defect.
    if ($Existing.check_contract_digest -cne $Incoming.check_contract_digest -and
        $Incoming.observation.outcome -cne 'findings') { return $Existing }

    $next.generation = $Existing.generation
    if ($Incoming.observation.outcome -ceq 'findings') {
        if ($Existing.status -ceq 'confirmed') { $next.generation++ }
        $next.status = 'open'
        $next.confirmation = if ($next.generation -ne $Existing.generation) { $null }
            elseif ($null -ne $Incoming.confirmation) { $Incoming.confirmation } else { $Existing.confirmation }
        return $next
    }
    if ($Incoming.observation.outcome -cne 'passed') {
        # Infrastructure failure must not consume the last actionable reproduction.
        $next.evidence = $Existing.evidence
        $next.status = $Existing.status
        $next.confirmation = if ($null -ne $Incoming.confirmation) { $Incoming.confirmation } else { $Existing.confirmation }
        return $next
    }
    if ($Existing.status -ceq 'confirmed') { return $Existing }
    $confirmation = $Incoming.confirmation
    $authorized = $null -ne $confirmation -and
        $confirmation.ContainsKey('authoritative') -and $confirmation.authoritative -eq $true -and
        $confirmation.ContainsKey('generation') -and $confirmation.generation -eq $Existing.generation -and
        $confirmation.ContainsKey('merge_commit_sha') -and
        $confirmation.ContainsKey('explained') -and $confirmation.explained -eq $true -and
        $confirmation.ContainsKey('scope_complete') -and $confirmation.scope_complete -eq $true -and
        $confirmation.ContainsKey('successful') -and $confirmation.successful -eq $true
    if ($authorized) {
        Assert-ScheduledSha $confirmation.merge_commit_sha
        $authorized = & $IsAncestor $confirmation.merge_commit_sha $Incoming.source_sha
    }
    # A single green rerun is not an explanation for a deterministic failure. Only a registered,
    # merged repair, reachable through its actual merge/squash commit, can confirm resolution.
    $next.status = if ($authorized) { 'confirmed' } else { 'needs-human' }
    $next.evidence = $Existing.evidence
    $next.confirmation = if ($null -ne $confirmation -and $confirmation.ContainsKey('authoritative') -and
        $confirmation.authoritative -eq $true -and $confirmation.generation -eq $Existing.generation) {
        $confirmation
    } else {
        @{
            status = 'unexplained-pass'; generation = $Existing.generation
            source_sha = $Incoming.source_sha; merge_commit_sha = $null
        }
    }
    return $next
}

function Merge-ScheduledCoverage {
    [CmdletBinding()]
    [OutputType([hashtable])]
    param(
        [AllowNull()][hashtable] $Coverage,
        [Parameter(Mandatory)][hashtable] $Manifest,
        [Parameter(Mandatory)][AllowEmptyCollection()][hashtable[]] $Results,
        [Parameter(Mandatory)][hashtable] $Context,
        [Parameter(Mandatory)][scriptblock] $IsAncestor,
        [switch] $Skipped
    )

    $next = if ($null -eq $Coverage) {
        @{ schema_version = 1; repository = $Manifest.repository; receipt = $null; invalidation = $null }
    } else { $Coverage | ConvertTo-Json -Depth 100 | ConvertFrom-Json -AsHashtable }
    if ($Skipped) { return $next }
    if ($next.ContainsKey('observation') -and $null -ne $next.observation) {
        if ((Compare-ScheduledObservation $Context $next.observation) -le 0 -or
            -not (& $IsAncestor $next.observation.source_sha $Manifest.source_sha)) {
            return $next
        }
    }
    $verdict = Test-ScheduledManifest -Manifest $Manifest -Results $Results
    if ($Context.ContainsKey('evidence_complete') -and -not $Context.evidence_complete) {
        $verdict.complete = $false
        $verdict.successful = $false
    }
    $expectedFindingFailure = $Context.ContainsKey('workflow_conclusion') -and
        $Context.workflow_conclusion -ceq 'failure' -and $verdict.complete -and
        @($Results | Where-Object outcome -CEQ findings).Count -gt 0
    if ($Context.ContainsKey('workflow_conclusion') -and $Context.workflow_conclusion -cne 'success' -and
        -not $expectedFindingFailure) {
        $verdict.complete = $false
        $verdict.successful = $false
    }
    foreach ($result in $Results) {
        foreach ($key in @('run_id', 'run_attempt', 'run_number')) {
            if (-not $result.ContainsKey($key) -or $result[$key] -ne $Context[$key]) {
                $verdict.complete = $false
                $verdict.successful = $false
            }
        }
    }
    $observation = $Context.Clone()
    $observation.source_sha = $Manifest.source_sha
    $observation.check_contract_digest = $Manifest.check_contract_digest
    $observation.outcome = if (-not $verdict.complete) { 'incomplete' }
        elseif ($verdict.successful) { 'passed' } else { 'findings' }
    $next.observation = $observation
    if ($Manifest.scope -ceq 'full' -and $verdict.successful) {
        $receipt = $observation.Clone()
        $receipt.scope = 'full'
        $receipt.complete = $true
        $receipt.successful = $true
        $receipt.manifest = $Manifest
        $receipt.manifest_digest = Get-ScheduledDigest $Manifest
        $next.receipt = $receipt
        $next.invalidation = $null
    } elseif (-not $verdict.successful) {
        $next.invalidation = $observation
    }
    return $next
}

function ConvertTo-ScheduledOwnedText {
    [CmdletBinding()]
    [OutputType([string])]
    param(
        [Parameter(Mandatory)][AllowEmptyString()][string] $Text,
        [Parameter(Mandatory)][hashtable] $Record,
        [Parameter(Mandatory)][ValidateSet('reporter', 'coverage', 'health', 'run')][string] $Kind
    )

    $null = Read-ScheduledRecord -Text $Text -Kind $Kind
    $match = [regex]::Match($Text, "<!-- scheduled-${Kind}:v1 (\{[^\r\n]*\}) -->")
    return $Text.Substring(0, $match.Index) + (Write-ScheduledRecord -Record $Record -Kind $Kind) +
        $Text.Substring($match.Index + $match.Length)
}

function Get-ScheduledFindingBody {
    [CmdletBinding()]
    [OutputType([string])]
    param([Parameter(Mandatory)][hashtable] $Record)

    # The immutable original reproduction survives artifact expiry. Updates change only the
    # machine record; neither human edits nor separately owned worker comments are replaced.
    $summary = [Net.WebUtility]::HtmlEncode([string]$Record.evidence.summary)
    $replay = [Net.WebUtility]::HtmlEncode(($Record.evidence.replay | ConvertTo-Json -Depth 50))
    $manifest = [Net.WebUtility]::HtmlEncode(($Record.evidence.manifest | ConvertTo-Json -Depth 50))
    return @"
[Copilot speaking]

Scheduled deterministic check finding in $($Record.package) ($($Record.platform)).

<pre>$summary</pre>

Source: $($Record.source_sha)

The reproduction below is typed check data, not a shell command.

<details><summary>Durable reproduction</summary>
<pre>$replay</pre>
</details>

<details><summary>Original check scope</summary>
<pre>$manifest</pre>
</details>

$(Write-ScheduledRecord -Record $Record -Kind reporter)
"@
}

function Get-ScheduledComponentHealth {
    [CmdletBinding()]
    [OutputType([hashtable])]
    param(
        [AllowNull()][hashtable] $Observation,
        [Parameter(Mandatory)][datetimeoffset] $Now,
        [Parameter(Mandatory)][double] $MaxAgeHours
    )

    if ($null -eq $Observation) { return @{ status = 'unavailable'; reason = 'missing-observation' } }
    if (-not $Observation.ContainsKey('completed_at') -or -not $Observation.ContainsKey('outcome')) {
        return @{ status = 'unavailable'; reason = 'malformed-observation' }
    }
    $at = [datetimeoffset]::MinValue
    if ($Observation.completed_at -is [datetime] -or $Observation.completed_at -is [datetimeoffset]) {
        $at = [datetimeoffset]$Observation.completed_at
    } elseif (-not [datetimeoffset]::TryParse([string]$Observation.completed_at,
            [Globalization.CultureInfo]::InvariantCulture, [Globalization.DateTimeStyles]::None, [ref]$at)) {
        return @{ status = 'unavailable'; reason = 'malformed-observation' }
    }
    if ($at -gt $Now -or ($Now - $at).TotalHours -gt $MaxAgeHours) {
        return @{ status = 'unavailable'; reason = 'stale-or-future-observation' }
    }
    $status = switch -CaseSensitive ($Observation.outcome) {
        { $_ -cin @('passed', 'success', 'fresh', 'no-findings', 'findings') } { 'fresh'; break }
        { $_ -cin @('reused', 'not-run-unchanged') } { 'reused'; break }
        { $_ -cin @('failed', 'failure', 'incomplete', 'execution-error', 'blocked', 'cancelled', 'timed_out') } {
            'failed'; break
        }
        default { 'unavailable' }
    }
    return @{ status = $status; observation = $Observation }
}

function Get-ScheduledHealth {
    [CmdletBinding()]
    [OutputType([hashtable])]
    param(
        [AllowNull()][hashtable] $Scheduler,
        [AllowNull()][hashtable] $Coverage,
        [AllowNull()][hashtable] $Manifest,
        [AllowNull()][hashtable] $Planning,
        [AllowNull()][hashtable] $Reporting,
        [AllowNull()][hashtable] $LocalScan,
        [Parameter(Mandatory)][datetimeoffset] $Now,
        [double] $ExpectedPlanGapHours = 30,
        [double] $ExpectedLocalGapMinutes = 420,
        [int] $MaxAgeDays = 7,
        [switch] $Staged
    )

    $components = @{
        scheduler = @{ status = 'unavailable'; reason = 'missing-scheduler' }
        coverage = @{ status = 'unavailable'; reason = 'missing-coverage' }
        planning = Get-ScheduledComponentHealth $Planning $Now $ExpectedPlanGapHours
        reporting = Get-ScheduledComponentHealth $Reporting $Now $ExpectedPlanGapHours
        local_scan = Get-ScheduledComponentHealth $LocalScan $Now ($ExpectedLocalGapMinutes / 60)
    }
    if ($null -ne $Scheduler -and $Scheduler.ContainsKey('state')) {
        $components.scheduler = @{
            status = if ($Scheduler.state -ceq 'active') { 'fresh' } else { 'failed' }
            state = $Scheduler.state
        }
    }
    if ($null -ne $Manifest) {
        $decision = Get-ScheduledRunDecision -Manifest $Manifest -Coverage $Coverage -Now $Now -MaxAgeDays $MaxAgeDays
        $components.coverage = @{
            status = if ($decision.run) { 'unavailable' } else { 'fresh' }; reason = $decision.reason
        }
        if ($null -ne $Coverage -and $Coverage.ContainsKey('invalidation') -and $null -ne $Coverage.invalidation) {
            $components.coverage.status = 'failed'
        } elseif (-not $decision.run -and $components.planning.status -ceq 'reused') {
            $components.coverage.status = 'reused'
        }
    }
    $states = @($components.Values | ForEach-Object { $_.status })
    $healthy = @($states | Where-Object { $_ -cnotin @('fresh', 'reused') }).Count -eq 0
    return @{
        schema_version = 1; checked_at = $Now.ToString('o'); components = $components
        healthy = $healthy
        # Staging explains absent observations; it never transforms them into proof of health
        # or suppresses an observed failure such as GitHub disabling an inactive scheduler.
        status = if ($Staged -and 'failed' -cnotin $states) { 'staged' }
            elseif ($healthy) { 'healthy' } else { 'failed' }
    }
}

Export-ModuleMember -Function Compare-ScheduledObservation,
Merge-ScheduledObservation, Merge-ScheduledCoverage, ConvertTo-ScheduledOwnedText,
Get-ScheduledFindingBody, Get-ScheduledHealth
