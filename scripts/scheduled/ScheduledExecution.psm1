#requires -Version 7

# Deep validation runs the same Just recipes as developers. This wrapper only supplies matrix
# scope, captures output and writes readable diagnostics; the recipe exit status is authoritative.
# Ref: .github/workflows/implementation.md#deep-execution.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
Import-Module (Join-Path $PSScriptRoot '..\utility\ProcessCapture.psm1')
Import-Module (Join-Path $PSScriptRoot 'ScheduledExecutionDiagnostics.psm1')

function Invoke-ScheduledCheck {
    [CmdletBinding()]
    [OutputType([int])]
    param(
        [Parameter(Mandatory)][hashtable] $Check,
        [Parameter(Mandatory)][string] $SourceRoot,
        [Parameter(Mandatory)][string] $OutputDirectory,
        [Parameter(Mandatory)][string] $SourceSha
    )

    $null = New-Item -ItemType Directory -Path $OutputDirectory -Force
    if (@(Get-ChildItem -LiteralPath $OutputDirectory -Force).Count -ne 0) {
        throw 'Check output must be an empty directory so a rerun cannot reuse stale results.'
    }
    $arguments = @()
    if ($Check.packages.Count -gt 0) { $arguments += "package=$($Check.packages -join ' ')" }
    $arguments += $Check.recipe
    if ($Check.recipe -ceq 'mutants') {
        $arguments += @($Check.shard, 'false', $OutputDirectory)
    } elseif ($Check.shard -ne '') { $arguments += $Check.shard }
    $command = 'just ' + (@($arguments | ForEach-Object { "'" + $_.Replace("'", "''") + "'" }) -join ' ')
    $summary = Join-Path $OutputDirectory 'summary.md'
    $stdout = Join-Path $OutputDirectory 'check.stdout'
    $stderr = Join-Path $OutputDirectory 'check.stderr'
    @(
        "# Deep check: $($Check.id)"
        ''
        "Tested commit: ``$SourceSha``"
        "Platform: $($Check.platform)"
        ''
        '```powershell'
        $command
        '```'
        ''
        'Execution started. An absent final result means execution was interrupted.'
    ) | Set-Content -LiteralPath $summary
    $exitCode = 0
    try {
        if ($SourceSha -cnotmatch '^[0-9a-f]{40}$') { throw 'Expected a full tested commit SHA.' }
        Write-Host "Running $command in $SourceRoot"
        $exitCode = Invoke-CapturedProcess -FilePath (Get-Command just -CommandType Application).Source `
            -ArgumentList $arguments -WorkingDirectory $SourceRoot `
            -StandardOutputPath $stdout -StandardErrorPath $stderr
        Add-Content -LiteralPath $summary -Value "`nJust recipe exit code: $exitCode"
        if ($exitCode -ne 0) {
            Write-ScheduledLogExcerpt -Path $stdout, $stderr -SummaryPath $summary
        }
        if ($Check.recipe -ceq 'mutants') {
            Write-ScheduledMutationSummary -OutputDirectory $OutputDirectory
        }
    } catch {
        # Capture/reporting failures are execution errors, not a second checker verdict.
        if ($exitCode -eq 0) { $exitCode = 1 }
        Add-Content -LiteralPath $summary -Value "`n## Execution failure`n`n$($_.Exception.Message)"
        $_ | Out-String | Set-Content -LiteralPath (Join-Path $OutputDirectory 'execution-error.txt')
    } finally {
        $conclusion = if ($exitCode -eq 0) { 'PASSED' } else { 'FAILED' }
        Add-Content -LiteralPath $summary -Value "`n## Final result: $conclusion`n`nExit code: $exitCode"
        if ($env:GITHUB_STEP_SUMMARY) {
            Get-Content -LiteralPath $summary | Add-Content -LiteralPath $env:GITHUB_STEP_SUMMARY
        }
        Write-Host "$($Check.id): $conclusion. Full diagnostics: $OutputDirectory"
    }
    return $exitCode
}

Export-ModuleMember -Function Invoke-ScheduledCheck
