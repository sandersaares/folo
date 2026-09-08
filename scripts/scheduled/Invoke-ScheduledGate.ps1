#requires -Version 7
[CmdletBinding()]
param(
    [string] $PlanPath = '.scheduled-plan/plan.json',
    [string] $ResultsDirectory = '.scheduled-results',
    [string] $ContextResult = $env:SCHEDULED_CONTEXT_RESULT,
    [string] $DeepResult = $env:SCHEDULED_DEEP_RESULT
)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
$VerbosePreference = 'Continue'
Import-Module (Join-Path $PSScriptRoot 'ScheduledWorkflow.psm1') -Force
Invoke-ScheduledGate -PlanPath $PlanPath -ResultsDirectory $ResultsDirectory `
    -ContextResult $ContextResult -DeepResult $DeepResult `
    -RunId ([long]$env:GITHUB_RUN_ID) -RunAttempt ([int]$env:GITHUB_RUN_ATTEMPT)
