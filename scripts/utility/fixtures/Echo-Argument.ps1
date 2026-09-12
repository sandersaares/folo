#requires -Version 7
# Stand-in for a real executable in ProcessCapture.Tests.ps1: a `pwsh -File` child that echoes what argv it
# received as JSON, so the test asserts on genuine OS/PowerShell argument-passing behavior
# (quoting, globbing, metacharacters) instead of a mocked command line that could hide an escaping
# regression.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
$VerbosePreference = 'Continue'

ConvertTo-Json -InputObject @($args) -Compress
