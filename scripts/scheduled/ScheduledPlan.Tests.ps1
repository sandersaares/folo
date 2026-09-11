#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }

# Protects the complete nightly check scope without executing expensive tools.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true

BeforeAll { Import-Module (Join-Path $PSScriptRoot 'ScheduledPlan.psm1') -Force }

Describe 'Fresh deep check catalog' {
    It 'preserves every platform, mutation shard and many-seed family' {
        $checks = @(Get-ScheduledCheck)
        $checks.Count | Should -Be 32
        @($checks.id | Sort-Object -Unique).Count | Should -Be $checks.Count
        @($checks | Where-Object kind -EQ 'miri').platform | Should -Be @(
            'ubuntu-latest', 'windows-latest', 'ubuntu-24.04-arm', 'windows-11-arm')
        foreach ($platform in @('ubuntu-latest', 'windows-latest')) {
            @($checks | Where-Object { $_.kind -eq 'mutants' -and $_.platform -eq $platform }).shard |
                Should -Be @(1..8 | ForEach-Object { "$_/8" })
            @($checks | Where-Object { $_.kind -eq 'careful' -and $_.platform -eq $platform }).Count | Should -Be 1
        }
        foreach ($family in @(
                @{ package = 'events_once'; shards = 4 }, @{ package = 'events'; shards = 2 },
                @{ package = 'awaiter_set'; shards = 2 }, @{ package = 'nm_impl'; shards = 2 })) {
            $selected = @($checks | Where-Object { $_.kind -eq 'miri-many' -and $_.packages -contains $family.package })
            $selected.Count | Should -Be $family.shards
            $bounds = @($selected.seed_range | ForEach-Object { $_ -split '\.\.' })
            $bounds[0] | Should -Be '0'
            $bounds[-1] | Should -Be '64'
            for ($index = 1; $index -lt $selected.Count; $index++) {
                ($selected[$index - 1].seed_range -split '\.\.')[1] |
                    Should -Be ($selected[$index].seed_range -split '\.\.')[0]
            }
        }
    }

    It 'uses workspace scope except for the declared many-seed families' {
        $checks = @(Get-ScheduledCheck | Where-Object kind -NE 'miri-many')
        foreach ($check in $checks) { $check.packages | Should -BeNullOrEmpty }
    }

    It 'returns independent plain declarations without coordination metadata' {
        $check = @(Get-ScheduledCheck | Where-Object id -EQ 'miri-many-events-2')[0]
        @($check.Keys | Sort-Object) | Should -Be @('id', 'kind', 'packages', 'platform', 'seed_range', 'shard')
        $check.seed_range | Should -Be '32..64'
        $check.packages = @('fixture')
        @(Get-ScheduledCheck | Where-Object id -EQ 'miri-many-events-2')[0].packages | Should -Be @('events')
    }
}
