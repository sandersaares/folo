#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true

# Pester suite for Miri.psm1. Get-MiriSeedRange is pure, so the seed arithmetic is checked
# directly: the unsharded full range, an even split, the remainder-absorbing final shard, and the
# "more shards than seeds" guard. Malformed specs are covered by Sharding.Tests.ps1; here we only
# confirm they still propagate as failures.

BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'Miri.psm1') -Force
}

Describe 'Get-MiriFlag' {
    It 'preserves a replay seed and all flags instead of replacing them with a shard range' {
        Get-MiriFlag -Flags @('-Zmiri-seed=19', '-Zmiri-disable-isolation') -Many -Shard '2/4' -SeedRange '16..32' |
            Should -Be @('-Zmiri-seed=19', '-Zmiri-disable-isolation')
    }

    It 'adds the declared range without losing other flags' {
        Get-MiriFlag -Flags @('-Zmiri-strict-provenance') -Many -SeedRange '32..64' |
            Should -Be @('-Zmiri-strict-provenance', '-Zmiri-many-seeds=32..64')
    }

    It 'rejects conflicting seed choices' {
        { Get-MiriFlag -Flags @('-Zmiri-seed=1', '-Zmiri-many-seeds=0..4') -Many } | Should -Throw
        { Get-MiriFlag -Many -SeedRange '4..4' } | Should -Throw
    }
}

Describe 'Get-MiriSeedRange' {
    It 'returns the full range when no shard is given' {
        Get-MiriSeedRange -Spec '' | Should -Be '..64'
    }

    It 'honours a custom total seed budget for the full range' {
        Get-MiriSeedRange -Spec '' -TotalSeeds 32 | Should -Be '..32'
    }

    It 'splits evenly when the count divides the total' {
        Get-MiriSeedRange -Spec '1/8' | Should -Be '0..8'
        Get-MiriSeedRange -Spec '2/8' | Should -Be '8..16'
        Get-MiriSeedRange -Spec '8/8' | Should -Be '56..64'
    }

    It 'gives the final shard the remainder of an uneven split' {
        # 64 / 6 = 10 per shard; shards 1..5 cover 0..50, and shard 6 absorbs 50..64.
        Get-MiriSeedRange -Spec '5/6' | Should -Be '40..50'
        Get-MiriSeedRange -Spec '6/6' | Should -Be '50..64'
    }

    It 'covers the whole budget with no gaps or overlaps across all shards' {
        $count = 7
        $ranges = 1..$count | ForEach-Object { Get-MiriSeedRange -Spec "$_/$count" }
        # First shard starts at 0, last ends at the total, and each shard resumes where the prior ended.
        ($ranges[0] -split '\.\.')[0] | Should -Be '0'
        ($ranges[-1] -split '\.\.')[1] | Should -Be '64'
        for ($i = 1; $i -lt $count; $i++) {
            $prevEnd = ($ranges[$i - 1] -split '\.\.')[1]
            $thisStart = ($ranges[$i] -split '\.\.')[0]
            $thisStart | Should -Be $prevEnd
        }
    }

    It 'rejects a spec with more shards than seeds' {
        { Get-MiriSeedRange -Spec '1/65' } | Should -Throw '*Too many shards*'
    }

    It 'propagates a malformed spec as a failure' {
        { Get-MiriSeedRange -Spec 'x/8' } | Should -Throw '*must be integers*'
    }
}
