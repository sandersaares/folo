#requires -Version 7

# Deep validation uses this catalog to build its full nightly matrix before tool setup.
# It needs no Rust toolchain or GitHub state.
# Ref: .github/workflows/implementation.md#deep-execution.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
Import-Module (Join-Path $PSScriptRoot '..\build\Miri.psm1')

function Get-ScheduledCheck {
    [CmdletBinding()]
    [OutputType([hashtable[]])]
    param()
    $checks = @()
    foreach ($platform in @('ubuntu-latest', 'windows-latest', 'ubuntu-24.04-arm', 'windows-11-arm')) {
        $checks += @{
            id = "miri-$platform"; kind = 'miri'; platform = $platform; packages = @()
            shard = ''; seed_range = ''
        }
    }
    foreach ($platform in @('ubuntu-latest', 'windows-latest')) {
        # Mutation shards bound each runner's workload; tests remain serial within each leg.
        foreach ($index in 1..8) {
            $checks += @{
                id = "mutants-$platform-$index"; kind = 'mutants'; platform = $platform
                packages = @(); shard = "$index/8"; seed_range = ''
            }
        }
        $checks += @{
            id = "careful-$platform"; kind = 'careful'; platform = $platform
            packages = @(); shard = ''; seed_range = ''
        }
    }
    # These synchronization-heavy families benefit from seed exploration rather than mutations.
    foreach ($family in @(
            @{ package = 'events_once'; shards = 4 },
            @{ package = 'events'; shards = 2 },
            @{ package = 'awaiter_set'; shards = 2 },
            @{ package = 'nm_impl'; shards = 2 })) {
        foreach ($index in 1..$family.shards) {
            $shard = "$index/$($family.shards)"
            $checks += @{
                id = "miri-many-$($family.package)-$index"; kind = 'miri-many'
                platform = 'ubuntu-latest'; packages = @($family.package); shard = $shard
                seed_range = Get-MiriSeedRange -Spec $shard
            }
        }
    }
    return $checks
}

Export-ModuleMember -Function Get-ScheduledCheck
