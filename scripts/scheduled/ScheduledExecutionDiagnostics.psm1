#requires -Version 7

# The checker writes readable diagnostic excerpts for the reporter, without a private result
# schema. Ordinary cargo-mutants JSON supplies descriptions; the shared Just recipe owns the verdict.
# Ref: .github/workflows/implementation.md#failure-reporting.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true

# Prefix/tail windows retain setup context and the last failing phase without loading full logs.
$script:LogReadByteLimit = 64KB
# All diagnostic appends share this budget, including descriptions and individual mutation logs.
# The execution wrapper appends its short authoritative recipe result separately.
$script:SummaryByteLimit = 1MB
# Native outcomes can contain large per-mutation records. Oversized JSON remains an artifact,
# rather than allocating its full object graph or interpreting an incomplete prefix.
$script:MutationJsonByteLimit = 8MB
$script:SummaryTruncationText = "`n`nDiagnostic summary truncated at its byte limit. Full logs and mutation details remain in the result artifact.`n"

function Read-ScheduledDiagnosticBuffer {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][IO.Stream] $Stream,
        [Parameter(Mandatory)][ValidateRange(0, [int]::MaxValue)][int] $Count
    )

    $reader = [IO.BinaryReader]::new($Stream, [Text.Encoding]::UTF8, $true)
    try {
        return ,$reader.ReadBytes($Count)
    } finally { $reader.Dispose() }
}

function Add-ScheduledDiagnosticText {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string] $SummaryPath,
        [Parameter(Mandatory)][AllowEmptyString()][string] $Text
    )

    $marker = [Text.Encoding]::UTF8.GetBytes($script:SummaryTruncationText)
    $stream = [IO.File]::Open($SummaryPath, [IO.FileMode]::Append, [IO.FileAccess]::Write, [IO.FileShare]::Read)
    try {
        $remaining = $script:SummaryByteLimit - $stream.Length
        if ($remaining -lt $marker.Length) { return $false }
        $bytes = [Text.Encoding]::UTF8.GetBytes($Text)
        $budget = [int]($remaining - $marker.Length)
        if ($bytes.Length -le $budget) {
            $stream.Write($bytes, 0, $bytes.Length)
            return $true
        }
        # Do not split a UTF-8 character when reserving room for the visible omission notice.
        while ($budget -gt 0 -and ($bytes[$budget] -band 0xC0) -eq 0x80) { $budget-- }
        $stream.Write($bytes, 0, $budget)
        $stream.Write($marker, 0, $marker.Length)
        return $false
    } finally { $stream.Dispose() }
}

function Write-ScheduledLogExcerpt {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string[]] $Path,
        [Parameter(Mandatory)][string] $SummaryPath
    )

    foreach ($logPath in $Path) {
        $stream = [IO.File]::OpenRead($logPath)
        try {
            $prefix = Read-ScheduledDiagnosticBuffer $stream ([int][Math]::Floor($script:LogReadByteLimit / 2))
            $tailBudget = $script:LogReadByteLimit - $prefix.Length
            # Length selects the tail window only; actual reads remain bounded independently.
            $tailStart = [Math]::Max($prefix.Length, $stream.Length - $tailBudget)
            $stream.Position = $tailStart
            $tail = Read-ScheduledDiagnosticBuffer $stream $tailBudget
        } finally { $stream.Dispose() }
        if ($prefix.Length + $tail.Length -eq 0) { continue }
        $relativePath = [IO.Path]::GetRelativePath([IO.Path]::GetDirectoryName([IO.Path]::GetFullPath($SummaryPath)), [IO.Path]::GetFullPath($logPath))
        $heading = "`n### $([IO.Path]::GetFileName($logPath))`n`nFull log in result artifact: $relativePath`n"
        if ($tailStart -gt $prefix.Length) {
            # Show the last phase first so it survives when the aggregate budget is nearly full.
            $excerpt = "Log excerpt truncated; middle output omitted.`n`nLast output:`n$([Text.Encoding]::UTF8.GetString($tail))`n`nFirst output:`n$([Text.Encoding]::UTF8.GetString($prefix))"
        } else {
            $excerpt = [Text.Encoding]::UTF8.GetString([byte[]]($prefix + $tail))
        }
        $text = $heading + "`n" + (($excerpt -split '\r?\n' | ForEach-Object { "    $_" }) -join "`n") + "`n"
        if (-not (Add-ScheduledDiagnosticText $SummaryPath $text)) { return }
    }
}

function Read-ScheduledMutationJson {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string] $Path,
        [Parameter(Mandatory)][string] $SummaryPath
    )

    $stream = [IO.File]::OpenRead($Path)
    try {
        # The extra byte distinguishes a complete file at the limit from an oversized file.
        $bytes = Read-ScheduledDiagnosticBuffer $stream ($script:MutationJsonByteLimit + 1)
    } finally { $stream.Dispose() }
    $reference = "mutants.out/$([IO.Path]::GetFileName($Path)) in the result artifact"
    if ($bytes.Length -gt $script:MutationJsonByteLimit) {
        $null = Add-ScheduledDiagnosticText $SummaryPath "`nMutation details exceeded the $script:MutationJsonByteLimit byte limit. See $reference; no partial JSON was parsed.`n"
        return $null
    }
    try {
        $document = [Text.Encoding]::UTF8.GetString($bytes).TrimStart([char]0xFEFF) |
            ConvertFrom-Json -AsHashtable -NoEnumerate
    } catch {
        $null = Add-ScheduledDiagnosticText $SummaryPath "`nMutation details could not be parsed: $($_.Exception.Message). See $reference.`n"
        return $null
    }
    if ($null -eq $document) {
        $null = Add-ScheduledDiagnosticText $SummaryPath "`nMutation details are unavailable; see $reference.`n"
    }
    return ,$document
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
            $inventory = Read-ScheduledMutationJson $inventoryPath $summary
            if ($null -eq $inventory) { return }
            if ($inventory -is [array] -and $inventory.Count -eq 0) {
                $null = Add-ScheduledDiagnosticText $summary "`nNo mutants selected for this shard; no mutation tests or baseline were run."
                return
            }
        }
        $null = Add-ScheduledDiagnosticText $summary "`nMutation outcome details are unavailable; see the recipe output."
        return
    }
    $lab = Read-ScheduledMutationJson $path $summary
    if ($null -eq $lab) { return }
    $baselines = @($lab.outcomes | Where-Object { $_.scenario -ceq 'Baseline' })
    if (-not (Add-ScheduledDiagnosticText $summary "`n## Mutation results")) { return }
    foreach ($baseline in $baselines) {
        if (-not (Add-ScheduledDiagnosticText $summary "`nUnmutated baseline: $($baseline.summary).")) { return }
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
        $description = @(
            ''
            "- $conclusion`: $($mutant.name)"
            "  Package: $($mutant.package); file: $($mutant.file)"
            "  Replacement: ``$($mutant.replacement)``"
            "  Logs: mutants.out/$($scenario.log_path)"
        ) -join "`n"
        if (-not (Add-ScheduledDiagnosticText $summary $description)) { return }
        Write-ScheduledMutationLog -OutputDirectory $OutputDirectory -RelativePath $scenario.log_path
    }
    $null = Add-ScheduledDiagnosticText $summary "`nTool totals: $($lab.total_mutants) mutations, $($lab.missed) missed, $($lab.timeout) timed out."
}

function Write-ScheduledMutationLog {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string] $OutputDirectory,
        [Parameter(Mandatory)][string] $RelativePath
    )

    $root = Join-Path $OutputDirectory 'mutants.out'
    $path = [IO.Path]::GetFullPath((Join-Path $root $RelativePath))
    # Validate relative tool-log path spelling, not physical filesystem link targets.
    if ([IO.Path]::IsPathRooted($RelativePath) -or
        [IO.Path]::GetRelativePath($root, $path) -match '^\.\.(?:[\\/]|$)') {
        throw 'Mutation log path lies outside the tool output directory.'
    }
    $summary = Join-Path $OutputDirectory 'summary.md'
    if (Test-Path -LiteralPath $path -PathType Leaf) {
        Write-ScheduledLogExcerpt -Path $path -SummaryPath $summary
    } else {
        $null = Add-ScheduledDiagnosticText $summary "`nTool log unavailable: $RelativePath"
    }
}

Export-ModuleMember -Function Write-ScheduledLogExcerpt, Write-ScheduledMutationSummary
