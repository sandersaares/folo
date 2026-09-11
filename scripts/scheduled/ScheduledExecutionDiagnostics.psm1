#requires -Version 7

# The checker writes readable diagnostic excerpts for the reporter, without a private result
# schema. Ordinary cargo-mutants JSON supplies missed-mutation descriptions and completion checks.
# Ref: .github/workflows/implementation.md#failure-reporting.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true

function Write-ScheduledLogExcerpt {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string[]] $Path,
        [Parameter(Mandatory)][string] $SummaryPath
    )

    foreach ($logPath in $Path) {
        $lines = @(Get-Content -LiteralPath $logPath)
        if ($lines.Count -eq 0) { continue }
        # Retain failure neighborhoods and the tail, not a successful-test inventory.
        # Full logs remain alongside the summary; indices keep overlapping excerpts readable.
        $selected = [Collections.Generic.SortedSet[int]]::new()
        for ($index = 0; $index -lt $lines.Count; $index++) {
            if ($lines[$index] -match '(?i)error|fail|panicked|timeout|timed out|Trying seed|test .*\.\.\.$') {
                for ($context = [Math]::Max(0, $index - 2); $context -le [Math]::Min($lines.Count - 1, $index + 8); $context++) {
                    $null = $selected.Add($context)
                }
            }
        }
        # The tail identifies the last phase when a tool exits without a recognizable error.
        for ($index = [Math]::Max(0, $lines.Count - 40); $index -lt $lines.Count; $index++) {
            $null = $selected.Add($index)
        }
        @(
            "`n### $([IO.Path]::GetFileName($logPath))"
            ''
            # Indentation prevents checker text from ending a Markdown fence in the report.
            foreach ($index in $selected) { "    $($lines[$index])" }
        ) | Add-Content -LiteralPath $SummaryPath
    }
}

function Write-ScheduledMutationSummary {
    [CmdletBinding()]
    [OutputType([int])]
    param(
        [Parameter(Mandatory)][string] $OutputDirectory,
        [Parameter(Mandatory)][AllowEmptyCollection()][object[]] $Inventory
    )

    $summary = Join-Path $OutputDirectory 'summary.md'
    $path = Join-Path $OutputDirectory 'mutants.out\outcomes.json'
    if (-not (Test-Path -LiteralPath $path)) { throw 'Mutation outcomes are missing; execution is incomplete.' }
    $lab = Get-Content -LiteralPath $path -Raw | ConvertFrom-Json -AsHashtable
    $baselines = @($lab.outcomes | Where-Object { $_.scenario -ceq 'Baseline' })
    $baselinePassed = $baselines.Count -eq 1 -and $baselines[0].summary -ceq 'Success'
    if ($baselinePassed) {
        # A baseline requires completed unmutated build and test phases, not merely a label.
        foreach ($phase in @('Build', 'Test')) {
            $completed = @($baselines[0].phase_results | Where-Object { $_.phase -ceq $phase })
            if ($completed.Count -ne 1 -or $completed[0].process_status -cne 'Success') {
                $baselinePassed = $false
            }
        }
    }
    if (-not $baselinePassed) {
        Add-Content -LiteralPath $summary -Value "`nUnmutated baseline failed or did not complete. Mutant results do not establish source defects."
        foreach ($baseline in $baselines) {
            Write-ScheduledMutationLog -OutputDirectory $OutputDirectory -RelativePath $baseline.log_path
        }
        return 1
    }
    $mutants = @($lab.outcomes | Where-Object { $_.scenario -is [hashtable] -and $_.scenario.ContainsKey('Mutant') })
    $failed = $false
    Add-Content -LiteralPath $summary -Value "`n## Mutation results`n`nUnmutated baseline: passed."
    foreach ($scenario in $mutants) {
        $timedOut = @($scenario.phase_results | Where-Object { $_.process_status -ceq 'Timeout' }).Count -gt 0
        if (-not $timedOut -and $scenario.summary -cin @('CaughtMutant', 'Unviable')) { continue }
        $failed = $true
        $mutant = $scenario.scenario.Mutant
        $conclusion = if ($timedOut) { 'Timeout' } else { $scenario.summary }
        @(
            ''
            "- $conclusion`: $($mutant.name)"
            "  Package: $($mutant.package); file: $($mutant.file)"
            "  Replacement: ``$($mutant.replacement)``"
            "  Logs: mutants.out/$($scenario.log_path)"
        ) | Add-Content -LiteralPath $summary
        Write-ScheduledMutationLog -OutputDirectory $OutputDirectory -RelativePath $scenario.log_path
    }
    if ($null -eq $lab.end_time -or $mutants.Count -ne $Inventory.Count -or $mutants.Count -ne $lab.total_mutants) {
        Add-Content -LiteralPath $summary -Value "`nMutation execution did not complete every selected mutant."
        return 1
    }
    if ($failed -or $lab.missed -gt 0 -or $lab.timeout -gt 0) { return 1 }
    Add-Content -LiteralPath $summary -Value "`nEvery selected mutant was caught or unviable."
    return 0
}

function Write-ScheduledMutationLog {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string] $OutputDirectory,
        [Parameter(Mandatory)][string] $RelativePath
    )

    $root = Join-Path $OutputDirectory 'mutants.out'
    $path = [IO.Path]::GetFullPath((Join-Path $root $RelativePath))
    # Tool log references are relative to mutants.out; never collect unrelated local files.
    if ([IO.Path]::IsPathRooted($RelativePath) -or
        [IO.Path]::GetRelativePath($root, $path) -match '^\.\.(?:[\\/]|$)') {
        throw 'Mutation log path lies outside the tool output directory.'
    }
    $summary = Join-Path $OutputDirectory 'summary.md'
    if (Test-Path -LiteralPath $path -PathType Leaf) {
        Write-ScheduledLogExcerpt -Path $path -SummaryPath $summary
    } else {
        Add-Content -LiteralPath $summary -Value "`nTool log unavailable: $RelativePath"
    }
}

Export-ModuleMember -Function Write-ScheduledLogExcerpt, Write-ScheduledMutationSummary
