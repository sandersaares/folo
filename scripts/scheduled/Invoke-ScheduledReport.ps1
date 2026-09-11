#requires -Version 7
# Called by scheduled-report.yml after deep validation completes. Use only the trusted
# default-branch controller and runner-provided PowerShell/gh: Rust setup may itself have failed.
# Ref: ../../.github/workflows/implementation.md#failure-reporting.
[CmdletBinding()]
param(
    [string] $Repository = $env:GH_REPO,
    [string] $EventPath = $env:GITHUB_EVENT_PATH,
    [string] $OutputDirectory = '.scheduled-report'
)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
$VerbosePreference = 'Continue'

Import-Module (Join-Path $PSScriptRoot 'ScheduledGitHub.psm1')
$payload = Get-Content -LiteralPath $EventPath -Raw | ConvertFrom-Json -AsHashtable
Invoke-ScheduledReporting -Repository $Repository -RunId $payload.workflow_run.id `
    -RunAttempt $payload.workflow_run.run_attempt -OutputDirectory $OutputDirectory
