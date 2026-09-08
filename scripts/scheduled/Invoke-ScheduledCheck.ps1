#requires -Version 7
[CmdletBinding()]
param(
    [string] $CheckJson = $env:SCHEDULED_CHECK,
    [string] $ManifestJson = $env:SCHEDULED_MANIFEST,
    [string] $SourceRoot = 'candidate',
    [string] $OutputDirectory = '.scheduled-result'
)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
$VerbosePreference = 'Continue'
Import-Module (Join-Path $PSScriptRoot 'ScheduledExecution.psm1') -Force
$check = ConvertFrom-Json -InputObject $CheckJson -AsHashtable
$manifest = ConvertFrom-Json -InputObject $ManifestJson -AsHashtable
$context = @{
    source_sha = $manifest.source_sha; controller_sha = $manifest.controller_sha
    check_contract_digest = $manifest.check_contract_digest
    run_id = [long]$env:GITHUB_RUN_ID; run_attempt = [int]$env:GITHUB_RUN_ATTEMPT
    run_number = [long]$env:GITHUB_RUN_NUMBER
}
$result = Invoke-ScheduledCheck -Check $check -SourceRoot ([IO.Path]::GetFullPath($SourceRoot)) `
    -OutputDirectory ([IO.Path]::GetFullPath($OutputDirectory)) `
    -Toolchain (Get-ScheduledToolchain -Kind $check.kind) -RunContext $context
$result | ConvertTo-Json -Depth 100
if ($result.outcome -cne 'passed') { exit 1 }
