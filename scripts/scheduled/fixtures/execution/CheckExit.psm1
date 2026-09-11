#requires -Version 7

# ScheduledEntryPoints.Tests.ps1 uses this stand-in to verify the main-checkout inputs and
# real script/workflow exit propagation without launching expensive checks.
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
    if ($SourceRoot -cne (Get-Location).Path -or $SourceSha -cne $env:GITHUB_SHA) {
        throw 'The workflow must pass its current checkout and GitHub commit to the checker.'
    }
    Write-Verbose "Fixture $($Check.id) source=$SourceRoot sha=$SourceSha output=$OutputDirectory"
    return [int]$env:SCHEDULED_TEST_EXIT
}

Export-ModuleMember -Function Invoke-ScheduledCheck
