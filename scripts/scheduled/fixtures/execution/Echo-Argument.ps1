#requires -Version 7
# Stand-in for a real checker executable in ScheduledExecution.psm1's process-launch tests
# (Invoke-ScheduledProcess): a real `pwsh -File` child process that echoes exactly what argv it
# received as JSON, so the test asserts on genuine OS/PowerShell argument-passing behavior
# (quoting, globbing, metacharacters) instead of a mocked command line that could hide an escaping
# regression.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
$VerbosePreference = 'Continue'

ConvertTo-Json -InputObject @($args) -Compress
