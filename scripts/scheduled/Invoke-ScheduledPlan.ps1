#requires -Version 7

# Full and selected deep-validation workflows call this bootstrap entrypoint before tool setup.
# Its plain plan.json identifies the actual tested SHA and the independently scheduled checks.
# Ref: .github/workflows/implementation.md#immutable-execution.
[CmdletBinding()]
param(
    [ValidateSet('full', 'selected')][string] $Mode = 'full',
    [string] $EventPath = $env:GITHUB_EVENT_PATH,
    [string] $OutputDirectory = '.scheduled-plan'
)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
$VerbosePreference = 'Continue'
Import-Module (Join-Path $PSScriptRoot 'ScheduledWorkflow.psm1') -Force
Invoke-ScheduledPlanning -Mode $Mode -EventPath $EventPath -OutputDirectory $OutputDirectory
