#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }

# Protects the nightly check scope and manual selection rules without executing expensive tools.
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

    It 'selects requested checks and applies package scope' {
        $checks = @(Get-ScheduledCheck -Packages cpulist -CheckIds miri-ubuntu-latest, mutants-windows-latest-2)
        $checks.Count | Should -Be 2
        foreach ($check in $checks) { $check.packages | Should -Be @('cpulist') }
    }

    It 'rejects invalid or unsupported selections' -ForEach @(
        @{ packages = @('cpulist'); ids = @('missing-check') }
        @{ packages = @('cpulist'); ids = @('miri-many-events-1') }
        @{ packages = @('events', 'cpulist'); ids = @('miri-many-events-1') }
        @{ packages = @('--workspace'); ids = @() }
        @{ packages = @('foo*'); ids = @() }
        @{ packages = @(''); ids = @() }
    ) {
        { Get-ScheduledCheck -Packages $packages -CheckIds $ids } | Should -Throw
    }

    It 'returns independent plain declarations without coordination metadata' {
        $check = @(Get-ScheduledCheck -Packages events -CheckIds miri-many-events-2)[0]
        @($check.Keys | Sort-Object) | Should -Be @('id', 'kind', 'packages', 'platform', 'seed_range', 'shard')
        $check.seed_range | Should -Be '32..64'
    }
}
