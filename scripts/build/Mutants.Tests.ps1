#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true

# Pester suite for Mutants.psm1. Both functions are pure, so the exclusion set and the shard
# translation are asserted directly across platforms. The exclusions are position-sensitive
# (`-e` immediately precedes its value), so the tests check that pairing as well as the
# platform-conditional entries and the Linux-only single-quoting.

BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'Mutants.psm1') -Force

    function Get-ExcludeValue($arguments) {
        # Extract the value that follows each `-e` flag so tests can assert on the exclusion set
        # independently of quoting.
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
        $values | Should -Contain "'packages/dure/**/*.rs'"
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

    It 'single-quotes the always-excluded dure paths off Windows' {
        $values = Get-ExcludeValue (Get-MutantsExcludeArgument -IsWindowsPlatform $false -IsLinuxPlatform $true)
        $values | Should -Contain "'packages/dure/src/pal/raw_handle.rs'"
        $values | Should -Contain "'packages/dure/src/outbox.rs'"
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

    It 'does not single-quote glob patterns on Windows' {
        $values = Get-ExcludeValue (Get-MutantsExcludeArgument -IsWindowsPlatform $true -IsLinuxPlatform $false)
        $values | Should -Contain '**/*facade.rs'
        $values | ForEach-Object { $_ | Should -Not -Match "^'" }
    }

    It 'single-quotes glob patterns off Windows so PowerShell does not expand them' {
        $values = Get-ExcludeValue (Get-MutantsExcludeArgument -IsWindowsPlatform $false -IsLinuxPlatform $true)
        $values | Should -Contain "'**/*facade.rs'"
        $values | Should -Contain "'packages/testing/**'"
        $values | Should -Contain "'packages/cargo-bench-history-figures/**'"
        $values | Should -Contain "'packages/dure/src/pal/**/windows.rs'"
        # Plain package names are still passed literally, without quoting.
        $values | Should -Contain 'many_cpus_benchmarking'
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
        { Get-MutantsShardArgument -Spec '9/8' } | Should -Throw '*1 <= N <= M*'
    }
}

Describe 'Typed mutation arguments' {
    It 'keeps wildcard exclusions literal on Linux when passed as argv' {
        $values = Get-ExcludeValue (Get-MutantsExcludeArgument -IsWindowsPlatform $false -IsLinuxPlatform $true -Literal)
        $values | Should -Contain '**/*facade.rs'
        $values | Should -Contain 'packages/dure/**/*.rs'
        $values | ForEach-Object { $_ | Should -Not -Match "^'" }
    }

    It 'anchors the complete mutant name without interpreting shell syntax' {
        $name = 'src/lib.rs:1:2: replace a$();[x] -> u8 with 0'
        $arguments = Get-MutantsReplayArgument -Mutant @{ name = $name }
        $arguments.Count | Should -Be 2
        $arguments[0] | Should -Be '--re'
        $name | Should -Match $arguments[1]
        "$name suffix" | Should -Not -Match $arguments[1]
    }
}
