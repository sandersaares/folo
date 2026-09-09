#requires -Version 7
[CmdletBinding()]
param([Parameter(Mandatory)][string] $SourceRoot)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
$VerbosePreference = 'Continue'

# A fresh process models entrypoint imports without Pester's already-loaded module state.
# Entry imports may reload their own module, but dependencies must retain earlier exports.
foreach ($module in @('Sharding', 'Miri', 'Mutants', 'CargoExecutable')) {
    Import-Module (Join-Path $SourceRoot "scripts\build\$module.psm1") -Force
}
foreach ($module in @('ScheduledContracts', 'ScheduledPlan', 'ScheduledGate', 'ScheduledExecution',
        'ScheduledVersion', 'ScheduledWorkflow', 'ScheduledReport', 'ScheduledGitHub')) {
    Import-Module (Join-Path $SourceRoot "scripts\scheduled\$module.psm1") -Force
}

$digest = Get-ScheduledDigest @{ schema_version = 1; value = 'import fixture' }
$manifest = Get-ScheduledCheckManifest -SourceSha ('a' * 40) -ControllerSha ('b' * 40) -ContractDigest $digest
$check = @($manifest.checks | Where-Object { $_.kind -eq 'mutants' })[0]
$command = Get-ScheduledCommand -Check $check -SourceRoot $SourceRoot `
    -OutputDirectory $SourceRoot -Toolchain (Get-ScheduledToolchain -Kind $check.kind)
$seedRange = Get-MiriSeedRange -Spec '2/4'
$shard = @(Get-MutantsShardArgument -Spec '2/8')
$genericShard = ConvertFrom-ShardSpec -Spec '2/8'

ConvertTo-Json -InputObject @{
    digest = $digest; seed_range = $seedRange; shard = $shard
    shard_index = $genericShard.Index; arguments = $command.arguments
} -Depth 10 -Compress
