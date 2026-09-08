#requires -Version 7
<#
.SYNOPSIS
Builds the check plan (the deep-checks matrix and the managed/confirmation decision) for one
triggering event, shared across three workflows.
.DESCRIPTION
`-Mode` selects the triggering context: `validation` for ordinary validation.yml PR/merge-group
events (deciding whether the candidate is a managed repair and, if so, which packages/checks it
must confirm), `verify` for scheduled-verify.yml's post-merge confirmation pass, and `scheduled` for
scheduled-validation.yml's full periodic run (subject to coverage-receipt reuse unless `-Force` or
`-Canary`). All three delegate to `Invoke-ScheduledPlanning` in ScheduledWorkflow.psm1, which is the
actual decision logic; this wrapper only resolves the triggering event from `-EventPath` and writes
`plan.json` plus the `GITHUB_OUTPUT` values (`run`, `managed`, `matrix`, ...) the calling workflow's
later steps and matrix job read. See
../../.github/workflows/design.md#deep-validation-operating-modes,
../../.github/workflows/implementation.md#complete-evidence-and-reuse and
../../docs/scheduled-validation.md#validation-and-release-contract.
#>
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
