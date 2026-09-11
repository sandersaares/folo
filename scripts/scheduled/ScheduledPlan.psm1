#requires -Version 7

# Invoke-ScheduledPlan.ps1 uses this controller-owned catalog to select fresh deep checks.
# It needs no Rust toolchain or GitHub state, so even toolchain setup failures leave a plan.
# Ref: .github/workflows/implementation.md#immutable-execution.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
Import-Module (Join-Path $PSScriptRoot '..\build\Miri.psm1')

function Get-ScheduledCheck {
    [CmdletBinding()]
    [OutputType([hashtable[]])]
    param(
        [string[]] $Packages = @(),
        [string[]] $CheckIds = @()
    )

    foreach ($packageName in $Packages) {
        if ($packageName -cnotmatch '^[A-Za-z0-9_][A-Za-z0-9_-]*$') {
            throw [ArgumentException]::new("Expected a crate name, not a Cargo option or pattern: '$packageName'.")
        }
    }
    $checks = @()
    foreach ($platform in @('ubuntu-latest', 'windows-latest', 'ubuntu-24.04-arm', 'windows-11-arm')) {
        $checks += @{
            id = "miri-$platform"; kind = 'miri'; platform = $platform; packages = $Packages
            shard = ''; seed_range = ''
        }
    }
    foreach ($platform in @('ubuntu-latest', 'windows-latest')) {
        # Mutation shards bound each runner's workload; tests remain serial within each leg.
        foreach ($index in 1..8) {
            $checks += @{
                id = "mutants-$platform-$index"; kind = 'mutants'; platform = $platform
                packages = $Packages; shard = "$index/8"; seed_range = ''
            }
        }
        $checks += @{
            id = "careful-$platform"; kind = 'careful'; platform = $platform
            packages = $Packages; shard = ''; seed_range = ''
        }
    }
    # These synchronization-heavy families benefit from seed exploration rather than mutations.
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
                seed_range = Get-MiriSeedRange -Spec $shard
            }
        }
    }
    foreach ($id in $CheckIds) {
        if ($id -cnotin @($checks.id)) {
            throw [ArgumentException]::new("Unknown check or incompatible packages: '$id'.")
        }
    }
    if ($CheckIds.Count -gt 0) { $checks = @($checks | Where-Object { $_.id -cin $CheckIds }) }
    foreach ($packageName in $Packages) {
        if ($packageName -cnotin @($checks.packages)) {
            throw [ArgumentException]::new("No selected check runs crate '$packageName'.")
        }
    }
    return $checks
}

Export-ModuleMember -Function Get-ScheduledCheck
