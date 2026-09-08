#requires -Version 7
[CmdletBinding()]
param([string] $PlanPath = '.scheduled-plan/plan.json')
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
$VerbosePreference = 'Continue'
Import-Module (Join-Path $PSScriptRoot 'ScheduledVersion.psm1') -Force
Invoke-ScheduledVersionVerification -PlanPath $PlanPath
