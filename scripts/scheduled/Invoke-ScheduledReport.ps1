#requires -Version 7
[CmdletBinding()]
param(
    [string] $Repository = $env:GH_REPO,
    [string] $EventPath = $env:GITHUB_EVENT_PATH,
    [string] $OutputDirectory = '.scheduled-report',
    [switch] $Apply
)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
$VerbosePreference = 'Continue'
Import-Module (Join-Path $PSScriptRoot 'ScheduledGitHub.psm1')
$report = Invoke-ScheduledReporting -Repository $Repository -EventPath $EventPath `
    -OutputDirectory $OutputDirectory -Apply:$Apply
$report | ConvertTo-Json -Depth 100
if ($report.status -ceq 'incomplete') { exit 1 }
