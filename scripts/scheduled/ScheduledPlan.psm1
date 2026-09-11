#requires -Version 7

# The hosted planner and reporter share the declared scope, compatibility digest and receipt
# reuse rules here; missing evidence never becomes coverage merely because no error was seen.
# Ref: .github/workflows/implementation.md, "Scheduled controller ownership".
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
Import-Module (Join-Path $PSScriptRoot 'ScheduledContracts.psm1')
Import-Module (Join-Path $PSScriptRoot '..\build\Miri.psm1')

function Get-ScheduledCheckManifest {
    # Declare obligations before execution; discovery results cannot shrink the expected scope.
    [CmdletBinding()]
    [OutputType([hashtable])]
    param(
        [Parameter(Mandatory)][string] $SourceSha,
        [Parameter(Mandatory)][string] $ControllerSha,
        [Parameter(Mandatory)][string] $ContractDigest,
        [ValidateSet('full', 'repair', 'confirmation')][string] $Scope = 'full',
        [string[]] $Packages = @(),
        [string[]] $CheckIds = @()
    )

    Assert-ScheduledSha $SourceSha
    Assert-ScheduledSha $ControllerSha
    foreach ($packageName in $Packages) {
        if ($packageName -cnotmatch '^[A-Za-z0-9_][A-Za-z0-9_-]*$') {
            throw [ArgumentException]::new("Enter a crate name, not a Cargo package pattern or option: '$packageName'.")
        }
    }
    $checks = @()
    foreach ($platform in @('ubuntu-latest', 'windows-latest', 'ubuntu-24.04-arm', 'windows-11-arm')) {
        $checks += @{
            id = "miri-$platform"; kind = 'miri'; platform = $platform; packages = $Packages
            shard = ''; seed_range = ''; flags = @(); test_filter = ''
        }
    }
    foreach ($platform in @('ubuntu-latest', 'windows-latest')) {
        # Preserve the existing eight-way mutation split on each supported mutation platform.
        foreach ($index in 1..8) {
            $checks += @{
                id = "mutants-$platform-$index"; kind = 'mutants'; platform = $platform
                packages = $Packages; shard = "$index/8"; seed_range = ''; flags = @(); test_filter = ''
            }
        }
        $checks += @{
            id = "careful-$platform"; kind = 'careful'; platform = $platform
            packages = $Packages; shard = ''; seed_range = ''; flags = @(); test_filter = ''
        }
    }
    foreach ($family in @(
            @{ package = 'events_once'; shards = 4 },
            @{ package = 'events'; shards = 2 },
            @{ package = 'awaiter_set'; shards = 2 },
            @{ package = 'nm_impl'; shards = 2 })) {
        if ($Packages.Count -gt 0 -and $family.package -cnotin $Packages) { continue }
        foreach ($index in 1..$family.shards) {
            $shard = "$index/$($family.shards)"
            $checks += @{
                id = "miri-many-$($family.package)-$index"; kind = 'miri-many'
                platform = 'ubuntu-latest'; packages = @($family.package); shard = $shard
                seed_range = Get-MiriSeedRange -Spec $shard; flags = @(); test_filter = ''
            }
        }
    }
    if ($CheckIds.Count -gt 0) {
        if ($Scope -eq 'full') { throw 'A partial selection cannot claim full coverage.' }
        foreach ($id in $CheckIds) {
            if ($id -cnotin @($checks.id)) {
                throw [ArgumentException]::new("Check '$id' is unknown or does not support the selected crates. See docs/scheduled-validation.md#running-checks-manually.")
            }
        }
        $checks = @($checks | Where-Object { $_.id -cin $CheckIds })
    }
    foreach ($packageName in $Packages) {
        if ($packageName -cnotin @($checks.packages)) {
            throw [ArgumentException]::new("No selected check runs crate '$packageName'. Select a compatible check for every requested crate.")
        }
    }
    if ($Scope -eq 'full' -and $Packages.Count -gt 0) { throw 'Full coverage requires the workspace.' }
    return @{
        schema_version = 1; repository = 'folo-rs/folo'; repository_id = 850321188
        source_sha = $SourceSha; controller_sha = $ControllerSha
        check_contract_digest = $ContractDigest; scope = $Scope; checks = $checks
    }
}

function Get-ScheduledContractDigest {
    [CmdletBinding()]
    [OutputType([string])]
    param([Parameter(Mandatory)][string] $Root)

    # Hash the checker contract, not the checked source or admission policy. Source SHA binds
    # Cargo content independently; a mechanical version increment or enabling repair admission
    # must not make an otherwise identical checker unable to confirm the original incident.
    # The private decoder manifest is checker infrastructure, not a released package's version
    # plan. Its source and dependency declarations must travel with the parser contract.
    # Its reviewed dependency snapshot is validated against trusted Cargo metadata before parsing;
    # unrelated workspace versions and dependency graphs do not enter this bounded identity.
    $paths = @('constants.env', 'rust-toolchain.toml', '.cargo/mutants.toml',
        '.github/workflows/deep-checks.yml', '.github/actions/setup-environment/action.yml',
        'scripts/scheduled/ScheduledContracts.psm1', 'scripts/scheduled/ScheduledPlan.psm1',
        'scripts/scheduled/ScheduledExecution.psm1', 'scripts/scheduled/ScheduledTransport.psm1',
        'scripts/scheduled/ScheduledJson.psm1', 'scripts/scheduled/Invoke-ScheduledCheck.ps1',
        'justfiles/just_scheduled.just',
        'packages/scheduled-mutation-config/Cargo.toml',
        'packages/scheduled-mutation-config/dependency-contract.json',
        'packages/scheduled-mutation-config/src/dependency_contract.rs',
        'packages/scheduled-mutation-config/src/main.rs',
        'packages/scheduled-mutation-config/src/mutation_config.rs',
        'scripts/build/Mutants.psm1',
        'scripts/build/Miri.psm1', 'scripts/build/Sharding.psm1', 'scripts/build/CargoExecutable.psm1',
        'justfiles/just_quality.just', 'justfiles/just_quality_mutants.just', 'justfiles/just_testing.just')
    $files = @{}
    foreach ($path in $paths) {
        $files[$path] = (Get-FileHash -LiteralPath (Join-Path $Root $path) -Algorithm SHA256).Hash
    }
    return Get-ScheduledDigest @{ files = $files }
}

function Test-ScheduledManifest {
    # A successful receipt requires exactly one compatible result for every declared obligation,
    # independently of whether individual results contain actionable findings.
    [CmdletBinding()]
    [OutputType([hashtable])]
    param(
        [Parameter(Mandatory)][hashtable] $Manifest,
        [Parameter(Mandatory)][AllowEmptyCollection()][hashtable[]] $Results
    )

    $problems = [Collections.Generic.List[string]]::new()
    if ($Manifest.schema_version -ne 1 -or $Manifest.checks.Count -eq 0) {
        $problems.Add('Missing or unsupported expected manifest.')
    }
    $seen = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    foreach ($check in $Manifest.checks) {
        if (-not $seen.Add($check.id)) { $problems.Add("Duplicate expected check: $($check.id)") }
        $matchingResults = @($Results | Where-Object { $_.check_id -ceq $check.id })
        if ($matchingResults.Count -ne 1) {
            $problems.Add("Expected one result: $($check.id)")
            continue
        }
        $result = $matchingResults[0]
        foreach ($key in @('source_sha', 'controller_sha', 'check_contract_digest')) {
            if ($result[$key] -cne $Manifest[$key]) { $problems.Add("$($check.id): mismatched $key") }
        }
        if ($result.schema_version -ne 1 -or
            (Get-ScheduledDigest $result.actual_scope) -cne (Get-ScheduledDigest $check)) {
            $problems.Add("$($check.id): mismatched execution scope")
        }
        if ($result.outcome -cnotin @('passed', 'findings', 'execution-error', 'blocked', 'incomplete', 'not-applicable')) {
            $problems.Add("$($check.id): unknown outcome")
        }
        if ($result.outcome -cin @('blocked', 'incomplete', 'execution-error', 'not-applicable')) {
            # The catalog has no optional legs. An exclusion must be in the reviewed catalog,
            # never a candidate's justification for missing evidence.
            $problems.Add("$($check.id): $($result.outcome)")
        }
    }
    foreach ($result in $Results) {
        if (-not $seen.Contains($result.check_id)) { $problems.Add("Unexpected result: $($result.check_id)") }
    }
    return @{
        complete = $problems.Count -eq 0
        successful = $problems.Count -eq 0 -and @($Results | Where-Object { $_.outcome -cne 'passed' }).Count -eq 0
        problems = @($problems)
    }
}

function Get-ScheduledRunDecision {
    # Reuse preserves the original observation time: polling does not extend evidence freshness.
    # The caller supplies time so expiry and invalidation are deterministic in tests.
    [CmdletBinding()]
    [OutputType([hashtable])]
    param(
        [Parameter(Mandatory)][hashtable] $Manifest,
        [AllowNull()][hashtable] $Coverage,
        [Parameter(Mandatory)][datetimeoffset] $Now,
        [int] $MaxAgeDays = 7
    )

    $reason = 'no-compatible-complete-success'
    if ($null -ne $Coverage -and $Coverage.ContainsKey('schema_version') -and
        $Coverage.schema_version -eq 1 -and $Coverage.ContainsKey('invalidation') -and
        $Coverage.ContainsKey('receipt') -and $null -ne $Coverage.receipt) {
        $receipt = $Coverage.receipt
        if ($receipt -isnot [hashtable]) { return @{ run = $true; reason = 'malformed-receipt'; receipt = $null } }
        foreach ($key in @('source_sha', 'check_contract_digest', 'scope', 'complete', 'successful',
                'manifest', 'run_number', 'run_id', 'run_attempt', 'completed_at')) {
            if (-not $receipt.ContainsKey($key)) {
                return @{ run = $true; reason = 'malformed-receipt'; receipt = $null }
            }
        }
        if ($receipt.manifest -isnot [hashtable] -or -not $receipt.manifest.ContainsKey('checks')) {
            return @{ run = $true; reason = 'malformed-receipt'; receipt = $null }
        }
        if ($null -ne $Coverage.invalidation) {
            foreach ($key in @('source_sha', 'check_contract_digest', 'run_number', 'run_attempt')) {
                if ($Coverage.invalidation -isnot [hashtable] -or -not $Coverage.invalidation.ContainsKey($key)) {
                    return @{ run = $true; reason = 'malformed-invalidation'; receipt = $null }
                }
            }
        }
        $compatible = $receipt.source_sha -ceq $Manifest.source_sha -and
            $receipt.check_contract_digest -ceq $Manifest.check_contract_digest -and
            $receipt.scope -ceq 'full' -and $receipt.complete -eq $true -and $receipt.successful -eq $true
        if ($compatible) {
            $validManifest = (Get-ScheduledDigest $receipt.manifest.checks) -ceq
                (Get-ScheduledDigest $Manifest.checks)
            # The serialized reporter owns execution ordering across workflow families.
            # A retained invalidation is unresolved; per-workflow run numbers cannot order
            # a selected deep validation run against a full deep validation run.
            $applicableInvalidation = $null -ne $Coverage.invalidation -and
                $Coverage.invalidation.source_sha -ceq $Manifest.source_sha -and
                $Coverage.invalidation.check_contract_digest -ceq $Manifest.check_contract_digest
            $completed = [datetimeoffset]::MinValue
            if ($receipt.completed_at -is [datetime] -or $receipt.completed_at -is [datetimeoffset]) {
                $completed = [datetimeoffset]$receipt.completed_at
            } elseif (-not [datetimeoffset]::TryParse(
                    [string]$receipt.completed_at, [Globalization.CultureInfo]::InvariantCulture,
                    [Globalization.DateTimeStyles]::None, [ref]$completed)) {
                return @{ run = $true; reason = 'malformed-receipt'; receipt = $null }
            }
            if ($validManifest -and -not $applicableInvalidation -and $completed -le $Now -and
                ($Now - $completed).TotalDays -lt $MaxAgeDays -and
                $receipt.run_id -gt 0 -and $receipt.run_attempt -gt 0) {
                return @{ run = $false; reason = 'not-run-unchanged'; receipt = $receipt }
            }
            $reason = 'expired-or-invalidated-coverage'
        }
    }
    return @{ run = $true; reason = $reason; receipt = $null }
}

Export-ModuleMember -Function Get-ScheduledCheckManifest, Get-ScheduledContractDigest,
Test-ScheduledManifest, Get-ScheduledRunDecision
