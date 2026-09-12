#requires -Version 7

# The checker writes readable diagnostic excerpts for the reporter, without a private result
# schema. Ordinary cargo-mutants JSON supplies descriptions; the shared Just recipe owns the verdict.
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
    param([Parameter(Mandatory)][string] $OutputDirectory)

    $summary = Join-Path $OutputDirectory 'summary.md'
    $path = Join-Path $OutputDirectory 'mutants.out\outcomes.json'
    if (-not (Test-Path -LiteralPath $path)) {
        # Empty shards do not write outcomes. Recipe setup failures can leave no tool output;
        # neither case is an invitation for the reporter to invent a second checker verdict.
        $inventoryPath = Join-Path $OutputDirectory 'mutants.out\mutants.json'
        if (Test-Path -LiteralPath $inventoryPath) {
            $inventory = Get-Content -LiteralPath $inventoryPath -Raw | ConvertFrom-Json -AsHashtable -NoEnumerate
            if ($inventory -is [array] -and $inventory.Count -eq 0) {
                Add-Content -LiteralPath $summary -Value "`nNo mutants selected for this shard; no mutation tests or baseline were run."
                return
            }
        }
        Add-Content -LiteralPath $summary -Value "`nMutation outcome details are unavailable; see the recipe output."
        return
    }
    $lab = Get-Content -LiteralPath $path -Raw | ConvertFrom-Json -AsHashtable
    $baselines = @($lab.outcomes | Where-Object { $_.scenario -ceq 'Baseline' })
    Add-Content -LiteralPath $summary -Value "`n## Mutation results"
    foreach ($baseline in $baselines) {
        Add-Content -LiteralPath $summary -Value "`nUnmutated baseline: $($baseline.summary)."
        if ($baseline.summary -cne 'Success') {
            Write-ScheduledMutationLog -OutputDirectory $OutputDirectory -RelativePath $baseline.log_path
            return
        }
    }
    $mutants = @($lab.outcomes | Where-Object { $_.scenario -is [hashtable] -and $_.scenario.ContainsKey('Mutant') })
    foreach ($scenario in $mutants) {
        $timedOut = @($scenario.phase_results | Where-Object { $_.process_status -ceq 'Timeout' }).Count -gt 0
        if (-not $timedOut -and $scenario.summary -cin @('CaughtMutant', 'Unviable')) { continue }
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
    Add-Content -LiteralPath $summary -Value "`nTool totals: $($lab.total_mutants) mutations, $($lab.missed) missed, $($lab.timeout) timed out."
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
