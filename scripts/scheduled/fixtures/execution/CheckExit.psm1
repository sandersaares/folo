#requires -Version 7

# ScheduledEntryPoints.Tests.ps1 copies this stand-in to an isolated controller tree to verify
# real script/Just exit propagation without launching expensive candidate checks.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true

function Invoke-ScheduledCheck {
    [CmdletBinding()]
    [OutputType([int])]
    param(
        [hashtable] $Check,
        [string] $SourceRoot,
        [string] $OutputDirectory,
        [string] $SourceSha
    )
    Write-Verbose "Fixture $($Check.id) source=$SourceRoot sha=$SourceSha output=$OutputDirectory"
    return [int]$env:SCHEDULED_TEST_EXIT
}

Export-ModuleMember -Function Invoke-ScheduledCheck
