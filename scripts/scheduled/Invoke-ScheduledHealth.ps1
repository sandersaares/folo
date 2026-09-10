#requires -Version 7
<#
.SYNOPSIS
Publishes the executor's rolling health surface for `scheduled-health.yml`.
.DESCRIPTION
Reads the reporter-owned coverage/health issue and recent workflow runs through
`Get-ScheduledGitHubHealth` (ScheduledGitHub.psm1) and reports a single combined status so an
operator (or the same `scheduled-intake` skill) can distinguish "healthy", "staged" (hosted
execution/reporting deliberately disabled), inactive Local operation and genuine failure
without inferring it from silence. The component table is duplicated into the job's
`GITHUB_STEP_SUMMARY` purely for human visibility in the Actions UI; the authoritative JSON is
`health.json` in `-OutputDirectory`. A non-healthy, non-staged status fails the job so a broken
health surface is itself visible in workflow history. See
../../.github/workflows/implementation.md#independent-health and
../../docs/scheduled-validation.md#health-recovery-and-rollback.
#>
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
