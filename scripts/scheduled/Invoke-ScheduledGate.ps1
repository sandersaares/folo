#requires -Version 7
<#
.SYNOPSIS
Enforces `scheduled-repair-gate`, the single unconditional required check that a managed repair PR
must clear before the ordinary `required-checks` fan-in can pass.
.DESCRIPTION
Called from standard-validation.yml's gate step after the context and (conditionally run) deep-checks jobs
complete. Delegates to `Invoke-ScheduledGate` in ScheduledWorkflow.psm1, which reads the plan
written by Invoke-ScheduledPlan.ps1 to decide whether this PR/merge-group candidate is a managed
repair at all; ordinary PRs pass with no deep evidence required. For a managed candidate it demands
a successful deep-checks run and validates every `evidence.json` result against the plan's manifest
(ScheduledGate.psm1's `Test-ScheduledRepairEvidence`) so an admission-policy change cannot silently
widen what a repair is allowed to skip. A thrown exception here fails the job, and the gate.
See ../../.github/workflows/implementation.md#managed-repair-gate and
../../docs/scheduled-validation.md#pr-readiness-and-bounded-continuation.
#>
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
