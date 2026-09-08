#requires -Version 7

# Shared wire format for hosted evidence and the personal Local App executor.
# Records have separate ownership; editing one record never replaces surrounding discussion.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true

function ConvertTo-ScheduledCanonicalValue {
    [CmdletBinding()]
    param([AllowNull()][object] $Value)

    if ($null -eq $Value) { return $null }
    if ($Value -is [System.Collections.IDictionary]) {
        $result = [ordered]@{}
        $keys = [string[]]@($Value.Keys)
        [Array]::Sort($keys, [StringComparer]::Ordinal)
        foreach ($key in $keys) {
            $result[$key] = ConvertTo-ScheduledCanonicalValue $Value[$key]
        }
        return $result
    }
    if ($Value -is [pscustomobject]) {
        $values = @{}
        foreach ($property in $Value.PSObject.Properties) { $values[$property.Name] = $property.Value }
        return ConvertTo-ScheduledCanonicalValue $values
    }
    if ($Value -is [System.Collections.IEnumerable] -and $Value -isnot [string]) {
        $items = @()
        foreach ($item in $Value) { $items += ,(ConvertTo-ScheduledCanonicalValue $item) }
        return ,$items
    }
    return $Value
}

function Get-ScheduledDigest {
    [CmdletBinding()]
    [OutputType([string])]
    param([Parameter(Mandatory)][object] $Value)

    $json = ConvertTo-Json -InputObject (ConvertTo-ScheduledCanonicalValue $Value) -Depth 100 -Compress
    return [Convert]::ToHexString(
        [Security.Cryptography.SHA256]::HashData([Text.Encoding]::UTF8.GetBytes($json))).ToLowerInvariant()
}

function Read-ScheduledRecord {
    [CmdletBinding()]
    [OutputType([hashtable])]
    param(
        [Parameter(Mandatory)][AllowEmptyString()][string] $Text,
        [Parameter(Mandatory)][ValidateSet('reporter', 'worker', 'repair', 'coverage', 'health')]
        [string] $Kind
    )

    $records = [regex]::Matches($Text, "<!-- scheduled-${Kind}:v1 (\{[^\r\n]*\}) -->")
    if ($records.Count -ne 1) { throw [FormatException]::new("Expected exactly one scheduled $Kind record.") }
    try {
        $record = ConvertFrom-Json -InputObject $records[0].Groups[1].Value -AsHashtable
    } catch [ArgumentException] {
        throw [FormatException]::new('Invalid scheduled record JSON.', $_.Exception)
    }
    if (-not $record.ContainsKey('schema_version') -or $record.schema_version -ne 1) {
        throw [FormatException]::new('Unsupported scheduled record schema.')
    }
    return $record
}

function Write-ScheduledRecord {
    [CmdletBinding()]
    [OutputType([string])]
    param(
        [Parameter(Mandatory)][hashtable] $Record,
        [Parameter(Mandatory)][ValidateSet('reporter', 'worker', 'repair', 'coverage', 'health')]
        [string] $Kind
    )

    if ($Record.schema_version -ne 1) { throw 'Unsupported scheduled record schema.' }
    $json = ConvertTo-Json -InputObject (ConvertTo-ScheduledCanonicalValue $Record) -Depth 100 -Compress
    # JSON's Unicode escapes prevent evidence text from terminating its owning HTML comment.
    $json = $json.Replace('<', '\u003c').Replace('>', '\u003e')
    return "<!-- scheduled-${Kind}:v1 $json -->"
}

function Assert-ScheduledSha {
    [CmdletBinding()]
    param([Parameter(Mandatory)][string] $Sha)
    if ($Sha -cnotmatch '^[0-9a-f]{40}$') { throw [FormatException]::new("Not an immutable Git commit SHA: $Sha") }
}

function Get-ScheduledPolicy {
    [CmdletBinding()]
    [OutputType([hashtable])]
    param([string] $Path = (Join-Path $PSScriptRoot 'policy.json'))

    $policy = Get-Content -LiteralPath $Path -Raw | ConvertFrom-Json -AsHashtable
    if ($policy.schema_version -ne 1 -or $policy.repository_id -le 0) {
        throw 'Unsupported scheduled policy or missing repository identity.'
    }
    if ($policy.coverage.max_age_days -le 0) { throw 'Coverage maximum age must be positive.' }
    foreach ($switchName in @('hosted_execution_enabled', 'reporting_enabled', 'cutover')) {
        if ($policy.rollout[$switchName] -isnot [bool]) { throw "Rollout switch must be boolean: $switchName" }
    }
    $prerequisiteNames = @('execution_canary', 'reporting_canary', 'native_app_canary',
        'benchmark_exclusion', 'azure_policy')
    foreach ($name in $prerequisiteNames) {
        if ($policy.rollout.prerequisites[$name] -isnot [bool]) { throw "Missing rollout prerequisite: $name" }
    }
    if ($policy.rollout.cutover) {
        if (-not $policy.rollout.hosted_execution_enabled -or -not $policy.rollout.reporting_enabled) {
            throw 'Cutover requires both hosted execution and reporting.'
        }
        foreach ($name in $prerequisiteNames) {
            if (-not $policy.rollout.prerequisites[$name]) { throw "Cutover prerequisite missing: $name" }
        }
    }
    return $policy
}

function ConvertTo-ScheduledIncident {
    [CmdletBinding()]
    [OutputType([hashtable])]
    param(
        [Parameter(Mandatory)][hashtable] $Issue,
        [Parameter(Mandatory)][AllowEmptyCollection()][hashtable[]] $Comments,
        [Parameter(Mandatory)][string] $Repository,
        [Parameter(Mandatory)][hashtable] $Run,
        [string] $ReporterLogin = 'github-actions[bot]',
        [string] $WorkerLogin = 'sandersaares',
        [long] $RepositoryId = 850321188
    )

    if ($Issue.user.login -cne $ReporterLogin) { throw [FormatException]::new('Finding issue is not reporter-owned.') }
    $record = Read-ScheduledRecord -Text $Issue.body -Kind reporter
    if ($record.repository -cne $Repository -or $record.repository_id -ne $RepositoryId) {
        throw [FormatException]::new('Finding repository identity mismatch.')
    }
    if ($record.finding_id -cnotmatch '^[0-9a-f]{64}$' -or $record.generation -lt 1) {
        throw [FormatException]::new('Invalid finding identity or generation.')
    }
    Assert-ScheduledSha $record.source_sha
    if ($record.observation.run_id -le 0 -or $record.observation.run_attempt -lt 1 -or
        $record.observation.run_number -lt 1 -or $record.check_contract_digest -cnotmatch '^[0-9a-f]{64}$') {
        throw [FormatException]::new('Finding lacks authoritative execution identity.')
    }
    if ($record.observation.workflow_path -cnotin @(
            '.github/workflows/scheduled-validation.yml', '.github/workflows/scheduled-verify.yml')) {
        throw [FormatException]::new('Finding does not originate in an approved workflow.')
    }
    if ($Run.repository.id -ne $RepositoryId -or $Run.repository.full_name -cne $Repository -or
        $Run.id -ne $record.observation.run_id -or $Run.run_attempt -ne $record.observation.run_attempt -or
        $Run.run_number -ne $record.observation.run_number -or
        $Run.path -cne $record.observation.workflow_path -or
        $Run.status -cne 'completed' -or $Run.head_branch -cne 'main') {
        throw [FormatException]::new('Originating API run does not validate the reporter record.')
    }
    # Verification runs execute a separately declared source SHA. The trusted reporter records
    # both identities so workflow_dispatch's controller head is not confused with that source.
    $controllerSha = if ($record.ContainsKey('controller_sha')) { $record.controller_sha } else { $record.source_sha }
    if ($Run.head_sha -cne $controllerSha) { throw [FormatException]::new('Originating controller SHA mismatch.') }
    $workers = @($Comments | Where-Object {
            $_.user.login -ceq $WorkerLogin -and $_.body.Contains('<!-- scheduled-worker:')
        })
    $matching = @()
    foreach ($comment in $workers) {
        $worker = Read-ScheduledRecord -Text $comment.body -Kind worker
        if ($worker.finding_id -ceq $record.finding_id -and $worker.generation -eq $record.generation) {
            $matching += $worker
        }
    }
    # One enrolled executor edits its own durable record. Duplicates need explicit recovery.
    if ($matching.Count -gt 1) { throw [FormatException]::new('Ambiguous worker ownership.') }
    $record.issue_number = $Issue.number
    $record.validated_worker = if ($matching.Count -eq 1) { $matching[0] } else { $null }
    return $record
}

Export-ModuleMember -Function Get-ScheduledDigest, Read-ScheduledRecord, Write-ScheduledRecord,
Assert-ScheduledSha, Get-ScheduledPolicy, ConvertTo-ScheduledIncident
