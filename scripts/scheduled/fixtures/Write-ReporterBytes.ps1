#requires -Version 7
# ScheduledReportIO.Tests.ps1 substitutes this local child for gh to exercise real pipe limits,
# process exit handling and cleanup without network access or timing-dependent synchronization.
[CmdletBinding()]
param(
    [Parameter(Mandatory)][int] $ByteCount,
    [int] $ExitCode = 0,
    [string] $ErrorText = ''
)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
$VerbosePreference = 'Continue'

[Console]::Error.Write($ErrorText)
$bytes = [Text.Encoding]::ASCII.GetBytes('x' * $ByteCount)
$stream = [Console]::OpenStandardOutput()
try { $stream.Write($bytes, 0, $bytes.Length) } finally { $stream.Dispose() }
exit $ExitCode
