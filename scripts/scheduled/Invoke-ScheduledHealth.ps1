#requires -Version 7
[CmdletBinding()]
param(
    [string] $Repository = $env:GH_REPO,
    [datetimeoffset] $Now = [datetimeoffset]::UtcNow,
    [string] $OutputDirectory = '.scheduled-health'
)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
$VerbosePreference = 'Continue'
Import-Module (Join-Path $PSScriptRoot 'ScheduledGitHub.psm1')
$health = Get-ScheduledGitHubHealth -Repository $Repository -Now $Now
New-Item -ItemType Directory -Path $OutputDirectory -Force | Out-Null
$health | ConvertTo-Json -Depth 100 | Set-Content -LiteralPath (Join-Path $OutputDirectory 'health.json')
if ($env:GITHUB_STEP_SUMMARY) {
    @(
        "## Scheduled health: $($health.status)"
        ''
        '| Component | State |'
        '|---|---|'
        foreach ($name in @($health.components.Keys | Sort-Object)) {
            "| $name | $($health.components[$name].status) |"
        }
    ) | Add-Content -LiteralPath $env:GITHUB_STEP_SUMMARY
}
$health | ConvertTo-Json -Depth 100
if ($health.status -cnotin @('healthy', 'staged')) { exit 1 }
