#requires -Version 7
# Finite native-process fixture for ScheduledTransport.Tests.ps1. Emits controlled bytes or
# echoes UTF-8 input so response limits, truncation and failure propagation need no GitHub access.
param([int] $Bytes = 0, [int] $ExitCode = 0, [switch] $EchoInput)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
$VerbosePreference = 'Continue'
[Console]::InputEncoding = [Text.UTF8Encoding]::new($false)

$stream = [Console]::OpenStandardOutput()
if ($EchoInput) {
    $payload = [Text.Encoding]::UTF8.GetBytes([Console]::In.ReadToEnd())
} else {
    $payload = [byte[]]::new($Bytes)
    [Array]::Fill[byte]($payload, 65)
}
$stream.Write($payload)
$stream.Flush()
if ($ExitCode -ne 0) { [Console]::Error.Write('Response failure canary') }
exit $ExitCode
