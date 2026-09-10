#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
# Protects coverage-measure's Cargo target selection with real nextest discovery on dependency-free
# packages. Unit, integration and example tests must remain selected for binary-only, library-only
# and mixed scopes; benchmark canaries must never compile. No coverage engine or hosted API is mocked
# into producing successful measurements. Ref: ../../docs/build-and-tooling.md#coverage-target-selection.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
$VerbosePreference = 'Continue'

BeforeAll {
    $root = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
    $recipes = (just --justfile (Join-Path $root 'justfile') --dump --dump-format json |
        ConvertFrom-Json -AsHashtable).recipes
    $measurement = @($recipes['coverage-measure'].body | Where-Object { $_ -contains ' llvm-cov nextest ' })
    $measurement.Count | Should -Be 1
    $script:selectors = @([regex]::Matches($measurement[0][-1], '--(?:lib|bins|tests|examples|benches|all-targets)\b') |
        ForEach-Object { $_.Value })
    $script:workspace = Join-Path $TestDrive 'coverage-targets'
    $null = New-Item -ItemType Directory -Path $workspace
    @'
[workspace]
members = ["binary_only", "library_only", "mixed"]
resolver = "3"
'@ | Set-Content -LiteralPath (Join-Path $workspace 'Cargo.toml')
    foreach ($name in @('binary_only', 'library_only', 'mixed')) {
        $directory = Join-Path $workspace $name
        foreach ($folder in @('src', 'tests', 'examples', 'benches')) {
            $null = New-Item -ItemType Directory -Path (Join-Path $directory $folder) -Force
        }
        @"
[package]
name = "$name"
version = "0.0.0"
edition = "2024"
publish = false
"@ | Set-Content -LiteralPath (Join-Path $directory 'Cargo.toml')
        if ($name -ne 'binary_only') {
            '#[test] fn library_case() {}' |
                Set-Content -LiteralPath (Join-Path $directory 'src\lib.rs')
        }
        if ($name -ne 'library_only') {
            'fn main() {}', '#[test] fn binary_case() {}' |
                Set-Content -LiteralPath (Join-Path $directory 'src\main.rs')
        }
        '#[test] fn integration_case() {}' |
            Set-Content -LiteralPath (Join-Path $directory 'tests\integration.rs')
        'fn main() {}', '#[test] fn example_case() {}' |
            Set-Content -LiteralPath (Join-Path $directory 'examples\example.rs')
        'compile_error!("benchmark targets must not be selected for coverage");' |
            Set-Content -LiteralPath (Join-Path $directory 'benches\benchmark.rs')
    }
}

Describe 'Coverage Cargo target selection' {
    It 'uses test and example selection without requiring a library or selecting benchmarks' {
        $selectors | Should -Be @('--tests', '--examples')
    }

    It 'discovers every intended test in <Packages>' -TestCases @(
        @{ Packages = @('binary_only') }
        @{ Packages = @('library_only') }
        @{ Packages = @('mixed') }
        @{ Packages = @('binary_only', 'library_only', 'mixed') }
    ) {
        param($Packages)
        $packageArguments = @($Packages | ForEach-Object { '-p'; $_ })
        $json = cargo nextest list --manifest-path (Join-Path $workspace 'Cargo.toml') `
            --target-dir (Join-Path $workspace 'target') --offline --all-features `
            --message-format json @selectors @packageArguments
        $LASTEXITCODE | Should -Be 0
        $inventory = ($json -join "`n") | ConvertFrom-Json -AsHashtable
        $actual = @(
            foreach ($suite in $inventory['rust-suites'].Values) {
                foreach ($testName in $suite.testcases.Keys) {
                    "$($suite['package-name'])::$testName"
                }
            }
        )
        $expected = @(
            foreach ($name in $Packages) {
                if ($name -ne 'binary_only') { "${name}::library_case" }
                if ($name -ne 'library_only') { "${name}::binary_case" }
                "${name}::integration_case"
                "${name}::example_case"
            }
        )
        @($actual | Sort-Object) | Should -Be @($expected | Sort-Object)
    }
}
