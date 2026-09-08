#requires -Version 7
[CmdletBinding()]
param(
    [ValidateSet('scheduled', 'validation', 'verify')][string] $Mode = 'scheduled',
    [string] $EventPath = $env:GITHUB_EVENT_PATH,
    [string] $OutputDirectory = '.scheduled-plan',
    [switch] $Force,
    [switch] $Canary
)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
$VerbosePreference = 'Continue'
Import-Module (Join-Path $PSScriptRoot 'ScheduledWorkflow.psm1') -Force
Invoke-ScheduledPlanning -Mode $Mode -EventPath $EventPath -OutputDirectory $OutputDirectory `
    -Force:$Force -Canary:$Canary
