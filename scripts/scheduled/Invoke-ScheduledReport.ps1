#requires -Version 7
# Called by deep-validation.yml after plan/check failures, while its report job is running.
# Runner-provided PowerShell/gh remain usable even when Rust setup failed.
# Ref: ../../.github/workflows/implementation.md#failure-reporting.
[CmdletBinding()]
param(
    [string] $Repository = $env:GH_REPO,
    [long] $RunId = $env:GITHUB_RUN_ID,
    [int] $RunAttempt = $env:GITHUB_RUN_ATTEMPT,
    [string] $OutputDirectory = '.scheduled-report'
)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
$VerbosePreference = 'Continue'

Import-Module (Join-Path $PSScriptRoot 'ScheduledGitHub.psm1')
Invoke-ScheduledReporting -Repository $Repository -RunId $RunId `
    -RunAttempt $RunAttempt -OutputDirectory $OutputDirectory
