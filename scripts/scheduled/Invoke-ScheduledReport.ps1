#requires -Version 7
<#
.SYNOPSIS
Converts one completed `scheduled-validation.yml`/`scheduled-verify.yml` run into durable
GitHub-persisted run-level intake, coverage and existing repair-confirmation records.
.DESCRIPTION
Runs as the privileged `scheduled-report.yml` `workflow_run` handler, always checked out at
default-branch `main` (never a candidate) so its parsing and issue-writing authority cannot be
influenced by candidate content; see
../../.github/workflows/implementation.md#serialized-reporting. Delegates to
`Invoke-ScheduledReporting` (ScheduledGitHub.psm1), which downloads and validates the originating
run's job/step inventory and plan/result artifacts, preserves failure evidence for AI triage,
and (only with `-Apply`, gated further by `policy.rollout.reporting_enabled`) writes run-level
issues. It never creates diagnosed problem issues. `-Apply` is the switch
between the workflow's dry-run/report-only path and the path with side effects; the reporting run's
`queue: max` concurrency means a superseded or failed attempt must be recovered explicitly, not
silently reattempted (../../docs/scheduled-validation.md#health-recovery-and-rollback). An
`incomplete` report status means intake could not be safely published and fails the job.
Successfully recording a failed or incomplete execution returns `reported`: that is successful
reporting, not proof of passing deep coverage.
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
