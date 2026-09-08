#requires -Version 7
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
$VerbosePreference = 'Continue'

ConvertTo-Json -InputObject @($args) -Compress
