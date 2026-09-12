#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true

# Pester suite for the pure Mutants.psm1 exclusion and sharding helpers, asserted directly
# across platforms. The exclusions are position-sensitive
# (`-e` immediately precedes its value), so the tests check that pairing as well as the
# platform-conditional entries and literal wildcard arguments on every platform.

BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'Mutants.psm1') -Force

    function Get-ExcludeValue($arguments) {
        # Extract the value that follows each `-e` flag so tests can assert on the exclusion set
        # independently of the calling recipe.
        $values = @()
        for ($i = 0; $i -lt $arguments.Length; $i++) {
            if ($arguments[$i] -eq '-e') { $values += $arguments[$i + 1] }
        }
        return $values
    }
}

Describe 'Get-MutantsExcludeArgument' {
    It 'pairs every -e flag with a following value' {
        $excludeArgs = Get-MutantsExcludeArgument -IsWindowsPlatform $true -IsLinuxPlatform $false
        # Even count, and no two -e flags are adjacent.
        ($excludeArgs.Count % 2) | Should -Be 0
        for ($i = 0; $i -lt $excludeArgs.Count; $i += 2) {
            $excludeArgs[$i] | Should -Be '-e'
            $excludeArgs[$i + 1] | Should -Not -Be '-e'
        }
    }

    It 'always excludes the core package/path set' {
        $values = Get-ExcludeValue (Get-MutantsExcludeArgument -IsWindowsPlatform $true -IsLinuxPlatform $true)
        $values | Should -Contain 'many_cpus_benchmarking'
        $values | Should -Contain 'facade'
        $values | Should -Contain 'events'
        $values | Should -Contain 'packages/cargo-bench-history-figures/**'
        $values | Should -Contain 'packages/cbh_detect/src/detect/examples.rs'
        $values | Should -Contain 'packages/cbh_detect/src/detect/scatter.rs'
        $values | Should -Contain 'packages/dure/src/pal/**/windows.rs'
        $values | Should -Contain 'packages/dure/src/pal/**/memory.rs'
        $values | Should -Contain 'packages/dure/src/pal/raw_handle.rs'
        $values | Should -Contain 'packages/dure/src/outbox.rs'
        $values | Should -Contain 'packages/dure-test-helper/**/*.rs'
        $values | Should -Contain 'packages/dure/src/test_support.rs'
    }

    It 'excludes the whole Windows-only dure package off Windows' {
        $values = Get-ExcludeValue (Get-MutantsExcludeArgument -IsWindowsPlatform $false -IsLinuxPlatform $true)
        $values | Should -Contain 'packages/dure/**/*.rs'
    }

    It 'keeps the dure package in scope on Windows' {
        $values = Get-ExcludeValue (Get-MutantsExcludeArgument -IsWindowsPlatform $true -IsLinuxPlatform $false)
        $values | Should -Not -Contain 'packages/dure/**/*.rs'
    }

    It 'does not exclude windows sources when running on Windows' {
        $values = Get-ExcludeValue (Get-MutantsExcludeArgument -IsWindowsPlatform $true -IsLinuxPlatform $false)
        $values | Should -Not -Contain 'windows'
        $values | Should -Not -Contain '**/*windows.rs'
    }

    It 'keeps the always-excluded dure paths literal off Windows' {
        $values = Get-ExcludeValue (Get-MutantsExcludeArgument -IsWindowsPlatform $false -IsLinuxPlatform $true)
        $values | Should -Contain 'packages/dure/src/pal/raw_handle.rs'
        $values | Should -Contain 'packages/dure/src/outbox.rs'
    }

    It 'excludes windows and linux sources on a third platform (e.g. macOS)' {
        $values = Get-ExcludeValue (Get-MutantsExcludeArgument -IsWindowsPlatform $false -IsLinuxPlatform $false)
        $values | Should -Contain 'windows'
        $values | Should -Contain 'linux'
    }

    It 'excludes linux sources but not windows sources when running on Windows' {
        $values = Get-ExcludeValue (Get-MutantsExcludeArgument -IsWindowsPlatform $true -IsLinuxPlatform $false)
        $values | Should -Contain 'linux'
        $values | Should -Not -Contain 'windows'
    }

    It 'passes literal glob patterns on <platform>' -ForEach @(
        @{ platform = 'Windows'; windows = $true; linux = $false }
        @{ platform = 'Linux'; windows = $false; linux = $true }
        @{ platform = 'other platforms'; windows = $false; linux = $false }
    ) {
        $values = Get-ExcludeValue (Get-MutantsExcludeArgument -IsWindowsPlatform $windows -IsLinuxPlatform $linux)
        $values | Should -Contain '**/*facade.rs'
        $values | Should -Contain 'packages/testing/**'
        $values | Should -Contain 'packages/cargo-bench-history-figures/**'
        $values | Should -Contain 'packages/dure/src/pal/**/windows.rs'
        $values | ForEach-Object { $_ | Should -Not -Match "^'" }
    }
}

Describe 'Get-MutantsShardArgument' {
    It 'returns no shard argument for an empty spec' {
        Get-MutantsShardArgument -Spec '' | Should -BeNullOrEmpty
    }

    It 'converts a 1-based spec to cargo-mutants 0-based --shard' {
        Get-MutantsShardArgument -Spec '1/8' | Should -Be @('--shard', '0/8')
        Get-MutantsShardArgument -Spec '8/8' | Should -Be @('--shard', '7/8')
    }

    It 'propagates a malformed spec as a failure' {
        { Get-MutantsShardArgument -Spec '9/8' } | Should -Throw
    }
}
