#requires -Version 7
<#
.SYNOPSIS
Independently regenerates and byte-compares a managed repair PR's version plan against its
recorded pre-versioning checkpoint.
.DESCRIPTION
Called from standard-validation.yml alongside the deep-checks matrix for any PR/merge-group candidate the
plan (Invoke-ScheduledPlan.ps1's output) marked `managed`. Delegates to
`Invoke-ScheduledVersionVerification` in ScheduledVersion.psm1, which builds a trusted controller
copy of `cargo-release-plan`, checks out each repair's `version_evidence.pre_version_sha` into a
disposable worktree, regenerates prepare/preview/captured apply there, and compares the resulting Cargo
manifests and lockfile byte-for-byte with the published head. A worker cannot make its own
selected version numbers authoritative merely by publishing them; only this independent
regeneration can. See
../../docs/scheduled-validation.md#durable-ownership-and-native-calls (canonical version evidence)
and ../../.github/workflows/implementation.md#canonical-version-validation.
#>
[CmdletBinding()]
param([string] $PlanPath = '.scheduled-plan/plan.json')
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
$VerbosePreference = 'Continue'
Import-Module (Join-Path $PSScriptRoot 'ScheduledVersion.psm1') -Force
Invoke-ScheduledVersionVerification -PlanPath $PlanPath
