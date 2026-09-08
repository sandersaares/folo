#requires -Version 7
<#
.SYNOPSIS
Converts one completed `scheduled-validation.yml`/`scheduled-verify.yml` run into durable
GitHub-persisted finding, coverage and health records.
.DESCRIPTION
Runs as the privileged `scheduled-report.yml` `workflow_run` handler, always checked out at
default-branch `main` (never a candidate) so its parsing and issue-writing authority cannot be
influenced by candidate content; see
../../.github/workflows/implementation.md#serialized-reporting. Delegates to
`Invoke-ScheduledReporting` (ScheduledGitHub.psm1), which downloads and validates the originating
run's plan/result artifacts, merges parsed findings into reporter-owned issues, and (only with
`-Apply`, gated further by `policy.rollout.reporting_enabled`) writes them. `-Apply` is the switch
between the workflow's dry-run/report-only path and the path with side effects; the reporting run's
`queue: max` concurrency means a superseded or failed attempt must be recovered explicitly, not
silently reattempted (../../docs/scheduled-validation.md#health-recovery-and-rollback). An
`incomplete` report status fails the job so the recovery condition surfaces in Actions history
rather than only in the durable coverage record.
#>
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
