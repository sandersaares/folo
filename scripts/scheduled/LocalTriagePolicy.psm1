#requires -Version 7
# Resolves reviewed triage-only settings for the Local skill, setup and independent health.
# Separating this file from repair policy keeps inactive role installation from invalidating
# a retained repair profile. Shared identity and controller changes still fence triage writes.
# Ref: ../../docs/scheduled-triage.md#independent-configuration.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
Import-Module (Join-Path $PSScriptRoot 'ScheduledContracts.psm1')
Import-Module (Join-Path $PSScriptRoot 'ScheduledTransport.psm1')

function Get-ScheduledTriagePolicy {
    [CmdletBinding()]
    [OutputType([hashtable])]
    param([string] $Path = (Join-Path $PSScriptRoot 'triage-policy.json'))
    $policy = Get-Content -LiteralPath $Path -Raw | ConvertFrom-Json -AsHashtable
    foreach ($field in @('schema_version', 'automation_name', 'automation_marker', 'cadence_cron',
            'mode', 'enrolled_machine_id', 'model', 'reasoning_effort', 'max_active_analyses',
            'max_starts_per_day', 'max_continuations_per_day', 'max_continuations_per_analysis',
            'scan_lease_minutes', 'expected_poll_gap_minutes')) {
        if (-not $policy.ContainsKey($field)) { throw "Missing triage configuration field: $field" }
    }
    if ($policy.schema_version -ne 1 -or $policy.mode -cnotin @('observe', 'paused', 'triage') -or
        $policy.max_active_analyses -ne 1) {
        throw 'Unsupported triage policy or competing analysis capacity.'
    }
    foreach ($field in @('automation_name', 'automation_marker', 'cadence_cron')) {
        if ([string]::IsNullOrWhiteSpace($policy[$field])) { throw "Empty triage setting: $field" }
    }
    foreach ($field in @('max_starts_per_day', 'max_continuations_per_day',
            'max_continuations_per_analysis', 'scan_lease_minutes', 'expected_poll_gap_minutes')) {
        if (($policy[$field] -isnot [int] -and $policy[$field] -isnot [long]) -or $policy[$field] -le 0) {
            throw "Triage limit must be a positive integer: $field"
        }
    }
    if ($policy.mode -ceq 'triage' -and
        ([string]::IsNullOrWhiteSpace($policy.enrolled_machine_id) -or
            [string]::IsNullOrWhiteSpace($policy.model))) {
        throw 'Active triage requires an enrolled machine and operator-selected model.'
    }
    return $policy
}

function Get-ScheduledTriagePolicyDigest {
    [CmdletBinding()]
    [OutputType([string])]
    param(
        [Parameter(Mandatory)][hashtable] $Policy,
        [Parameter(Mandatory)][hashtable] $TriagePolicy
    )
    return Get-ScheduledDigest @{
        schema_version = $Policy.schema_version; repository = $Policy.repository
        repository_id = $Policy.repository_id; worker_login = $Policy.worker_login
        reporter_login = $Policy.reporter_login; triage = $TriagePolicy
    }
}

function Get-ScheduledTriageControllerDigest {
    [CmdletBinding()]
    [OutputType([string])]
    param()
    $root = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..')).Path
    $files = @(
        # Scheduled modules share the GitHub write and local-state boundaries. Include that
        # complete module family and the imported build helpers rather than omitting a
        # transitive writer when a facade changes its imports.
        Get-ChildItem -LiteralPath $PSScriptRoot -File -Filter '*.psm1'
        foreach ($name in @('CargoExecutable.psm1', 'Miri.psm1', 'Mutants.psm1', 'Sharding.psm1')) {
            Get-Item -LiteralPath (Join-Path $root "scripts\build\$name")
        }
        foreach ($package in @('scheduled-run-record', 'scheduled-triage-record')) {
            Get-ChildItem -LiteralPath (Join-Path $root "packages\$package\src") -Recurse -File
            Get-Item -LiteralPath (Join-Path $root "packages\$package\Cargo.toml")
        }
        foreach ($name in @('Cargo.toml', 'Cargo.lock', 'rust-toolchain.toml', 'constants.env',
                '.github\skills\scheduled-triage\SKILL.md')) {
            Get-Item -LiteralPath (Join-Path $root $name)
        }
    )
    $paths = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    foreach ($file in $files) {
        $null = $paths.Add([IO.Path]::GetRelativePath($root, $file.FullName).Replace('\', '/'))
        $directory = $file.Directory.FullName
        while ($directory.StartsWith($root, [StringComparison]::Ordinal)) {
            $attributes = Join-Path $directory .gitattributes
            if (Test-Path -LiteralPath $attributes) {
                $null = $paths.Add([IO.Path]::GetRelativePath($root, $attributes).Replace('\', '/'))
            }
            if ($directory -ceq $root) { break }
            $directory = [IO.Path]::GetDirectoryName($directory)
        }
    }
    if (Test-Path -LiteralPath (Join-Path $root .gitconfig)) { $null = $paths.Add('.gitconfig') }
    $ordered = [string[]]@($paths)
    [Array]::Sort($ordered, [StringComparer]::Ordinal)
    $hashes = @(Get-TriageWorkingFileHash $root $ordered)
    $identity = @{}
    for ($index = 0; $index -lt $ordered.Count; $index++) {
        $identity[$ordered[$index]] = $hashes[$index]
    }
    return Get-ScheduledDigest $identity
}

function Get-TriageWorkingFileHash {
    param([string] $Root, [string[]] $Paths)
    # Git applies the repository's declared text/clean normalization to current working
    # files. This is not a HEAD/tree lookup: dirty script and skill edits still change identity.
    $inputText = (@($Paths | ForEach-Object { ConvertTo-Json -InputObject $_ -Compress }) -join "`n") + "`n"
    $git = (Get-Command git -CommandType Application | Select-Object -First 1).Source
    $output = Invoke-ScheduledJsonExecutable -Executable $git `
        -Arguments @('hash-object', '--stdin-paths') -Directory $Root -InputText $inputText `
        -Environment @{ GIT_OPTIONAL_LOCKS = '0' }
    $hashes = @($output -split '\r?\n' | Where-Object { $_ -ne '' })
    if ($hashes.Count -ne $Paths.Count -or @($hashes | Where-Object { $_ -cnotmatch '^(?:[0-9a-f]{40}|[0-9a-f]{64})$' }).Count -gt 0) {
        throw 'Git did not return complete normalized working-file identities.'
    }
    return $hashes
}

function Get-ScheduledTriagePromptDigest {
    param([Parameter(Mandatory)][AllowEmptyString()][string] $Prompt)
    return Get-ScheduledDigest ($Prompt.Replace("`r`n", "`n"))
}

Export-ModuleMember -Function Get-ScheduledTriagePolicy, Get-ScheduledTriagePolicyDigest,
Get-ScheduledTriageControllerDigest, Get-ScheduledTriagePromptDigest
