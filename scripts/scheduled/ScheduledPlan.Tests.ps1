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
        @($checks.recipe | Sort-Object -Unique) | Should -Be @('careful', 'miri', 'miri-harder', 'mutants')
        @($checks | Where-Object recipe -EQ 'miri').platform | Should -Be @(
            'ubuntu-latest', 'windows-latest', 'ubuntu-24.04-arm', 'windows-11-arm')
        foreach ($platform in @('ubuntu-latest', 'windows-latest')) {
            @($checks | Where-Object { $_.recipe -eq 'mutants' -and $_.platform -eq $platform }).shard |
                Should -Be @(1..8 | ForEach-Object { "$_/8" })
            @($checks | Where-Object { $_.recipe -eq 'careful' -and $_.platform -eq $platform }).Count | Should -Be 1
        }
        foreach ($family in @(
                @{ package = 'events_once'; shards = 4 }, @{ package = 'events'; shards = 2 },
                @{ package = 'awaiter_set'; shards = 2 }, @{ package = 'nm_impl'; shards = 2 })) {
            $selected = @($checks | Where-Object { $_.recipe -eq 'miri-harder' -and $_.packages -contains $family.package })
            $selected.Count | Should -Be $family.shards
            $selected.shard | Should -Be @(1..$family.shards | ForEach-Object { "$_/$($family.shards)" })
        }
    }

    It 'uses workspace scope except for the declared many-seed families' {
        $checks = @(Get-ScheduledCheck | Where-Object recipe -NE 'miri-harder')
        foreach ($check in $checks) { $check.packages | Should -BeNullOrEmpty }
    }

    It 'returns independent plain declarations without coordination metadata' {
        $check = @(Get-ScheduledCheck | Where-Object id -EQ 'miri-many-events-2')[0]
        @($check.Keys | Sort-Object) | Should -Be @('id', 'packages', 'platform', 'recipe', 'shard')
        $check.recipe | Should -Be 'miri-harder'
        $check.packages = @('fixture')
        @(Get-ScheduledCheck | Where-Object id -EQ 'miri-many-events-2')[0].packages | Should -Be @('events')
    }
}
