#requires -Version 7

# deep-checks.yml and just scheduled-check use this process boundary to propagate checker failures.
# Only the selected check and actual source SHA are inputs; summary.md is the reporter's input.
# Ref: .github/workflows/implementation.md#immutable-execution.
[CmdletBinding()]
param(
    [string] $CheckJson = $env:SCHEDULED_CHECK,
    [string] $SourceSha = $env:SCHEDULED_SOURCE_SHA,
    [string] $SourceRoot = 'candidate',
    [string] $OutputDirectory = '.scheduled-result'
)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
$VerbosePreference = 'Continue'
Import-Module (Join-Path $PSScriptRoot 'ScheduledExecution.psm1') -Force
$check = ConvertFrom-Json -InputObject $CheckJson -AsHashtable
$exitCode = Invoke-ScheduledCheck -Check $check -SourceRoot ([IO.Path]::GetFullPath($SourceRoot)) `
    -OutputDirectory ([IO.Path]::GetFullPath($OutputDirectory)) -SourceSha $SourceSha
exit $exitCode
