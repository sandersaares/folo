#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }

# Runs the real shared mutants recipe with a harmless cargo process and helper-call boundary.
# This protects native argument transport, standalone baseline execution and environment behavior
# without compiling test helpers or launching mutation work. The suite runs on Windows and Linux.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true

BeforeAll {
    $script:fixtureRoot = Join-Path $TestDrive 'recipe'
    $script:fixtureScripts = Join-Path $PSScriptRoot 'fixtures\mutants'
    $script:justExecutable = (Get-Command just -CommandType Application | Select-Object -First 1).Source
    $script:binaryDirectory = Join-Path $fixtureRoot 'bin'
    $moduleRoot = Join-Path $fixtureRoot 'scripts\build'
    $null = New-Item -ItemType Directory -Path $moduleRoot, $binaryDirectory -Force
    Copy-Item -LiteralPath (Join-Path $PSScriptRoot 'Mutants.psm1'), (Join-Path $PSScriptRoot 'Sharding.psm1') -Destination $moduleRoot
    $nativeCargo = Join-Path $binaryDirectory $(if ($IsWindows) { 'cargo.exe' } else { 'cargo' })
    # A tiny std-only executable receives native argv directly. A PowerShell cargo stand-in
    # would itself expand wildcards when forwarding them and would hide the true boundary.
    $savedAutoInstall = $env:RUSTUP_AUTO_INSTALL
    try {
        $env:RUSTUP_AUTO_INSTALL = '0'
        & rustc --edition=2024 -D warnings --crate-name mutants_recipe_fixture `
            (Join-Path $fixtureScripts 'cargo.rs') -o $nativeCargo
    } finally { $env:RUSTUP_AUTO_INSTALL = $savedAutoInstall }
    $recipePath = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..\..\justfiles\just_quality_mutants.just'))
    $interpreterPath = Join-Path $fixtureScripts 'Invoke-Recipe.ps1'
    # JSON string escaping also gives Just literal paths without depending on a path's spelling.
    @'
set shell := ["pwsh", "-NoLogo", "-NoProfile", "-NonInteractive", "-Command"]
set windows-shell := ["pwsh", "-NoLogo", "-NoProfile", "-NonInteractive", "-Command"]
set script-interpreter := ["pwsh", "-NoLogo", "-NoProfile", "-NonInteractive", "-File", __INTERPRETER__]
package := ""
import __RECIPE__
'@.Replace('__INTERPRETER__', ($interpreterPath | ConvertTo-Json -Compress)).
        Replace('__RECIPE__', ($recipePath | ConvertTo-Json -Compress)) |
        Set-Content -LiteralPath (Join-Path $fixtureRoot 'justfile')

    # Matching files make accidental wildcard expansion observable at the native boundary.
    $null = New-Item -ItemType Directory -Path (Join-Path $fixtureRoot 'nested'), (Join-Path $fixtureRoot 'packages\testing') -Force
    $null = New-Item -ItemType File -Path (Join-Path $fixtureRoot 'nested\facade.rs'), (Join-Path $fixtureRoot 'packages\testing\sample')

    function Invoke-MutationRecipeFixture {
        [CmdletBinding()]
        [OutputType([hashtable])]
        param(
            [string[]] $Arguments = @('mutants'),
            [hashtable] $Environment = @{},
            [int] $ExitCode = 0
        )

        $directory = Join-Path $fixtureRoot ([guid]::NewGuid().ToString())
        $null = New-Item -ItemType Directory -Path $directory
        $capture = Join-Path $directory 'cargo.txt'
        $trace = Join-Path $directory 'helpers.jsonl'
        $start = [Diagnostics.ProcessStartInfo]::new()
        $start.FileName = $justExecutable
        $start.WorkingDirectory = $fixtureRoot
        $start.UseShellExecute = $false
        $start.RedirectStandardOutput = $true
        $start.RedirectStandardError = $true
        foreach ($argument in $Arguments) { $start.ArgumentList.Add($argument) }
        foreach ($key in @('CARGO_TARGET_DIR', 'MUTANTS_TEMP', 'RUSTFLAGS', 'RUST_TEST_THREADS',
                'MUTATION_TESTING', 'CBH_FAKER', 'DURE_TEST_HELPER')) {
            $null = $start.Environment.Remove($key)
        }
        # Just's generated scripts and all child scratch work stay in the isolated test tree.
        foreach ($key in @('TMP', 'TEMP', 'TMPDIR')) { $start.Environment[$key] = $directory }
        $start.Environment['PATH'] = $binaryDirectory + [IO.Path]::PathSeparator + $start.Environment['PATH']
        $start.Environment['MUTANTS_FIXTURE_CAPTURE'] = $capture
        $start.Environment['MUTANTS_FIXTURE_TRACE'] = $trace
        $start.Environment['MUTANTS_FIXTURE_EXIT'] = [string]$ExitCode
        $start.Environment['MUTANTS_FIXTURE_FAKER'] = Join-Path $directory "faker's `$binary"
        $start.Environment['MUTANTS_FIXTURE_DURE'] = Join-Path $directory "dure's `$binary"
        foreach ($key in $Environment.Keys) { $start.Environment[$key] = $Environment[$key] }
        $process = [Diagnostics.Process]::new()
        $process.StartInfo = $start
        try {
            $null = $process.Start()
            $stdout = $process.StandardOutput.ReadToEndAsync()
            $stderr = $process.StandardError.ReadToEndAsync()
            $process.WaitForExit()
            $diagnostic = $stdout.GetAwaiter().GetResult() + $stderr.GetAwaiter().GetResult()
            $cargo = $null
            if (Test-Path -LiteralPath $capture) {
                $cargo = @{ arguments = @(); environment = @{}; processor_count = 0 }
                foreach ($line in Get-Content -LiteralPath $capture) {
                    if ($line.StartsWith('argument=')) {
                        $cargo.arguments += $line.Substring('argument='.Length)
                    } elseif ($line.StartsWith('environment:')) {
                        $pair = $line.Substring('environment:'.Length) -split '=', 2
                        $cargo.environment[$pair[0]] = $pair[1]
                    } elseif ($line.StartsWith('processor_count=')) {
                        $cargo.processor_count = [int]$line.Substring('processor_count='.Length)
                    }
                }
            }
            return @{
                exit_code = $process.ExitCode; diagnostic = $diagnostic
                cargo = $cargo
                helpers = if (Test-Path -LiteralPath $trace) { @(Get-Content -LiteralPath $trace | ConvertFrom-Json -AsHashtable) } else { @() }
            }
        } finally { $process.Dispose() }
    }
}

Describe 'Shared mutants recipe' {
    It 'runs the unmutated baseline with ordinary concurrency and literal platform exclusions' {
        $result = Invoke-MutationRecipeFixture
        $result.exit_code | Should -Be 0 -Because $result.diagnostic
        $result.diagnostic | Should -Match 'mutation stdout canary'
        $result.diagnostic | Should -Match 'mutation stderr canary'
        $argv = $result.cargo.arguments
        $argv[0] | Should -Be 'mutants'
        $argv | Should -Contain '--workspace'
        $argv | Should -Contain '--baseline=run'
        $argv | Should -Not -Contain '--baseline=skip'
        $argv | Should -Contain '--timeout=60'
        $argv | Should -Contain '--no-shuffle'
        $argv | Should -Contain '--caught'
        $argv | Should -Contain '--unviable'
        $argv | Should -Not -Contain '--output'
        $argv | Should -Not -Contain '--shard'
        $argv[[array]::IndexOf($argv, '--jobs') + 1] | Should -Be ([string]([int][Math]::Floor($result.cargo.processor_count / 6) + 1))
        $argv | Should -Contain '**/*facade.rs'
        $argv | Should -Contain 'packages/testing/**'
        foreach ($argument in $argv) { $argument | Should -Not -Match "^'" }
        if ($IsWindows) {
            $argv | Should -Contain '**/*linux.rs'
            $argv | Should -Not -Contain '**/*windows.rs'
            $result.helpers.name | Should -Be @('_faker-path', '_dure-test-helper-path')
        } else {
            $argv | Should -Contain '**/*windows.rs'
            $argv | Should -Contain 'packages/dure/**/*.rs'
            $result.helpers.name | Should -Be @('_faker-path')
        }
        $result.cargo.environment.RUSTFLAGS | Should -Be '--cfg mutants'
        $result.cargo.environment.MUTATION_TESTING | Should -Be '1'
        $result.cargo.environment.RUST_TEST_THREADS | Should -BeNullOrEmpty
        foreach ($helper in $result.helpers) { $helper.rustflags | Should -BeNullOrEmpty }
    }

    It 'passes the final output path literally with package selection and shard conversion' {
        $output = Join-Path $TestDrive "output's `$literal directory"
        $result = Invoke-MutationRecipeFixture -Arguments @('package=cpulist many_cpus', 'mutants', '2/8', 'false', $output)
        $result.exit_code | Should -Be 0 -Because $result.diagnostic
        $argv = $result.cargo.arguments
        $argv[[array]::IndexOf($argv, '--output') + 1] | Should -BeExactly $output
        $argv[[array]::IndexOf($argv, '--shard') + 1] | Should -Be '1/8'
        $argv | Should -Contain 'cpulist'
        $argv | Should -Contain 'many_cpus'
        $argv | Should -Not -Contain '--workspace'
        @($argv | Where-Object { $_ -eq '-p' }).Count | Should -Be 2
    }

    It 'preserves environment preparation and ordinary test concurrency' {
        $scratch = Join-Path $TestDrive 'mutation scratch'
        $null = New-Item -ItemType Directory -Path $scratch
        $result = Invoke-MutationRecipeFixture -Environment @{
            CARGO_TARGET_DIR = 'inherited-target'; MUTANTS_TEMP = $scratch
            RUSTFLAGS = '--cfg ordinary'; RUST_TEST_THREADS = '7'
        }
        $result.exit_code | Should -Be 0 -Because $result.diagnostic
        $result.cargo.environment.CARGO_TARGET_DIR | Should -BeNullOrEmpty
        $result.cargo.environment.TMP | Should -BeExactly $scratch
        $result.cargo.environment.RUSTFLAGS | Should -Be '--cfg ordinary --cfg mutants'
        $result.cargo.environment.RUST_TEST_THREADS | Should -Be '7'
        $result.cargo.environment.CBH_FAKER | Should -BeLike "*faker's `$*"
        if ($IsWindows) { $result.cargo.environment.DURE_TEST_HELPER | Should -BeLike "*dure's `$*" }
        foreach ($helper in $result.helpers) { $helper.rustflags | Should -Be '--cfg ordinary' }
    }

    It 'serializes tests only in careful mode without duplicating existing mutation flags' {
        $result = Invoke-MutationRecipeFixture -Arguments @('mutants', '', 'true') `
            -Environment @{ RUSTFLAGS = '--cfg mutants'; RUST_TEST_THREADS = '7' }
        $result.exit_code | Should -Be 0 -Because $result.diagnostic
        $result.cargo.environment.RUST_TEST_THREADS | Should -Be '1'
        $result.cargo.environment.RUSTFLAGS | Should -Be '--cfg mutants'
        $argv = $result.cargo.arguments
        $argv[[array]::IndexOf($argv, '--jobs') + 1] | Should -Be ([string]([int][Math]::Floor($result.cargo.processor_count / 6) + 1))
    }

    It 'retains the mutants-careful convenience recipe and optional output default' {
        $result = Invoke-MutationRecipeFixture -Arguments @('mutants-careful')
        $result.exit_code | Should -Be 0 -Because $result.diagnostic
        $result.cargo.environment.RUST_TEST_THREADS | Should -Be '1'
        $result.cargo.arguments | Should -Not -Contain '--output'
    }

    It 'propagates cargo exit <code>' -ForEach @(@{ code = 1 }, @{ code = 2 }, @{ code = 3 }) {
        $result = Invoke-MutationRecipeFixture -ExitCode $code
        $result.cargo | Should -Not -BeNullOrEmpty
        $result.exit_code | Should -Be $code
    }

    It 'does not start cargo when helper preparation fails' {
        $result = Invoke-MutationRecipeFixture -Environment @{ MUTANTS_FIXTURE_FAIL_HELPER = '_faker-path' }
        $result.exit_code | Should -Not -Be 0
        $result.cargo | Should -BeNullOrEmpty
    }

    It 'rejects an invalid shard before starting helper preparation or cargo' {
        $result = Invoke-MutationRecipeFixture -Arguments @('mutants', '9/8')
        $result.exit_code | Should -Not -Be 0
        $result.cargo | Should -BeNullOrEmpty
        $result.helpers | Should -BeNullOrEmpty
    }
}
