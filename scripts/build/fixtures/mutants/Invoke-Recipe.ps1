#requires -Version 7

# MutantsRecipe.Tests.ps1 selects this interpreter for the real Just recipe. Only helper prebuilds
# are replaced here; a native cargo stand-in on PATH receives the recipe's unchanged arguments.
param([Parameter(Mandatory)][string] $Recipe)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
$VerbosePreference = 'Continue'

function global:just {
    if ($args.Count -ne 1 -or $args[0] -cnotin @('_faker-path', '_dure-test-helper-path')) {
        throw 'The mutation recipe requested an unexpected helper.'
    }
    $helper = [string]$args[0]
    @{ name = $helper; rustflags = $env:RUSTFLAGS } | ConvertTo-Json -Compress |
        Add-Content -LiteralPath $env:MUTANTS_FIXTURE_TRACE
    if ($env:MUTANTS_FIXTURE_FAIL_HELPER -ceq $helper) { throw 'Helper failure canary.' }
    if ($helper -ceq '_faker-path') { return $env:MUTANTS_FIXTURE_FAKER }
    return $env:MUTANTS_FIXTURE_DURE
}

$global:LASTEXITCODE = 0
$env:MUTANTS_FIXTURE_PROCESSOR_COUNT = [string][Environment]::ProcessorCount
& $Recipe
exit $LASTEXITCODE
