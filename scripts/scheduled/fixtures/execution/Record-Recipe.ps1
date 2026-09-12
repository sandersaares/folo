#requires -Version 7
# Records real Just argument binding for ScheduledExecution.Tests.ps1 without running a checker.
param([string] $Recipe, [AllowEmptyString()][string] $Package)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
$VerbosePreference = 'Continue'

@{
    recipe = $Recipe
    package = $Package
    shard = $env:SHARD
    careful = $env:CAREFUL
    output = $env:OUTPUT
} | ConvertTo-Json | Set-Content -LiteralPath $env:SCHEDULED_CAPTURE_PATH

if ($Recipe -eq 'mutants') {
    $null = New-Item -ItemType Directory -Path (Join-Path $env:OUTPUT 'mutants.out') -Force
    if ($env:SCHEDULED_MUTATION_FIXTURE) {
        Copy-Item -LiteralPath $env:SCHEDULED_MUTATION_FIXTURE `
            -Destination (Join-Path $env:OUTPUT 'mutants.out\outcomes.json')
    } else {
        '[]' | Set-Content -LiteralPath (Join-Path $env:OUTPUT 'mutants.out\mutants.json')
    }
}
if ([int]$env:SCHEDULED_TEST_EXIT -ne 0) {
    [Console]::Error.WriteLine('Recipe failure canary.')
}
exit [int]$env:SCHEDULED_TEST_EXIT
