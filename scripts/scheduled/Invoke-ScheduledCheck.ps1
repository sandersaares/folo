#requires -Version 7
<#
.SYNOPSIS
Runs one deep scheduled check (a Miri or mutation-testing leg) and reports its outcome.
.DESCRIPTION
Thin entrypoint for a single `deep-checks.yml` reusable-workflow matrix job: `$CheckJson` is one
element of the plan's `manifest.checks` array (see ScheduledWorkflow.psm1's
`Invoke-ScheduledPlanning`), passed in as `SCHEDULED_CHECK` via `toJSON(matrix)`. The real work -
building the typed command, running it against the extracted candidate source, and parsing raw
checker output into typed evidence - lives in ScheduledExecution.psm1 (Rust-owned; see
../../.github/workflows/implementation.md#immutable-execution and #evidence-decoding), which this
script only wraps for the hosted matrix job and for local reproduction via `just`
(just_scheduled.just). The process exit code is this leg's pass/fail signal to the matrix job; the
JSON result on stdout is what `scheduled-report.yml` later parses from the run's artifact. See
../../docs/scheduled-validation.md#durable-ownership-and-native-calls.
#>
[CmdletBinding()]
param(
    [string] $CheckJson = $env:SCHEDULED_CHECK,
    [string] $ManifestJson = $env:SCHEDULED_MANIFEST,
    [string] $SourceRoot = 'candidate',
    [string] $OutputDirectory = '.scheduled-result'
)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
$VerbosePreference = 'Continue'
Import-Module (Join-Path $PSScriptRoot 'ScheduledExecution.psm1') -Force
$check = ConvertFrom-Json -InputObject $CheckJson -AsHashtable
$manifest = ConvertFrom-Json -InputObject $ManifestJson -AsHashtable
$context = @{
    source_sha = $manifest.source_sha; controller_sha = $manifest.controller_sha
    check_contract_digest = $manifest.check_contract_digest
    run_id = [long]$env:GITHUB_RUN_ID; run_attempt = [int]$env:GITHUB_RUN_ATTEMPT
    run_number = [long]$env:GITHUB_RUN_NUMBER
}
$result = Invoke-ScheduledCheck -Check $check -SourceRoot ([IO.Path]::GetFullPath($SourceRoot)) `
    -OutputDirectory ([IO.Path]::GetFullPath($OutputDirectory)) `
    -Toolchain (Get-ScheduledToolchain -Kind $check.kind) -RunContext $context
$result | ConvertTo-Json -Depth 100
if ($result.outcome -cne 'passed') { exit 1 }
