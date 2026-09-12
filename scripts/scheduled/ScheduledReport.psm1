#requires -Version 7
# Formats the report job's observed failures as ordinary Markdown. No issue payload
# schema is required by readers or by human authors. PowerShell is necessary here because a
# Rust setup failure must still be reportable by ScheduledGitHub.psm1.
# Ref: ../../.github/workflows/implementation.md#failure-reporting.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true

function Get-ScheduledLogExcerpt {
    [CmdletBinding()]
    param([Parameter(Mandatory)][AllowEmptyString()][string] $Text)

    # Keep every diagnostic match with nearby context, not the first N findings. Noise from
    # successful steps is supplementary in the full logs. The tail explains unfamiliar failures.
    $lines = @($Text -replace '\x1B\[[0-?]*[ -/]*[@-~]', '' -split '\r?\n')
    $selected = [Collections.Generic.SortedSet[int]]::new()
    for ($index = 0; $index -lt $lines.Count; $index++) {
        if ($lines[$index] -match '(?i)##\[error\]|\berror\b|\bfailed\b|\bfailure\b|panicked|MISSED|TIMEOUT|timed out|seed|undefined behavior|uncovered mutant') {
            # Neighboring lines retain test names, source locations and compiler explanations.
            foreach ($neighbor in ([Math]::Max(0, $index - 2)..[Math]::Min($lines.Count - 1, $index + 3))) {
                $null = $selected.Add($neighbor)
            }
        }
    }
    if ($selected.Count -eq 0) {
        foreach ($index in ([Math]::Max(0, $lines.Count - 20)..($lines.Count - 1))) {
            $null = $selected.Add($index)
        }
    }
    $excerpt = [Collections.Generic.List[string]]::new()
    $previous = -1
    foreach ($index in $selected) {
        if ($index -gt $previous + 1) { $excerpt.Add('[Other log lines omitted; full job logs are linked above.]') }
        $excerpt.Add($lines[$index])
        $previous = $index
    }
    if ($previous -lt $lines.Count - 1) {
        $excerpt.Add('[Other log lines omitted; full job logs are linked above.]')
    }
    return ($excerpt -join "`n").Trim()
}

function ConvertTo-ScheduledTableCell {
    [CmdletBinding()]
    param([AllowEmptyString()][string] $Text)
    return [Net.WebUtility]::HtmlEncode($Text).Replace('|', '&#124;').Replace("`r", '').Replace("`n", '<br>')
}

function Split-ScheduledReportText {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string] $Text,
        [Parameter(Mandatory)][int] $Length
    )
    while ($Text.Length -gt $Length) {
        $boundary = $Text.LastIndexOf("`n", $Length - 1, $Length)
        if ($boundary -le 0) { $boundary = $Length }
        $Text.Substring(0, $boundary)
        $Text = $Text.Substring($boundary).TrimStart("`r", "`n")
    }
    if ($Text.Length -gt 0) { $Text }
}

function Format-ScheduledReport {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][hashtable] $Run,
        [Parameter(Mandatory)][string] $AttemptUrl,
        [Parameter(Mandatory)][hashtable[]] $Failures,
        [string[]] $Notices = @()
    )

    # GitHub issue/comment bodies have a size limit. Leave room for Markdown and repeated
    # context, and continue real diagnostics in ordinary readable comments instead of truncating.
    $messageLimit = 60000
    $sectionLimit = 45000
    $prefix = "[Copilot speaking]`n`nWorkflow attempt: $AttemptUrl"
    $blocks = [Collections.Generic.List[string]]::new()
    $started = ([datetimeoffset]$Run.run_started_at).UtcDateTime.ToString('yyyy-MM-dd HH:mm:ss')
    $blocks.Add("Workflow: $(ConvertTo-ScheduledTableCell $Run.name)`n`nUTC start: $started`n`nTested commit: ``$($Run.head_sha)```n`nWorkflow status: $($Run.status)`n`n[Workflow logs and result artifacts]($AttemptUrl)")
    foreach ($notice in $Notices) { $blocks.Add($notice) }
    $tableHeader = "## Failed jobs`n`n| Job / check | Conclusion | Observed error summary |`n| --- | --- | --- |"
    $table = $tableHeader
    foreach ($failure in $Failures) {
        $summary = [string]$failure.summary
        if ($summary.Length -gt 400) {
            $summary = $summary.Substring(0, 400) + ' ... See the diagnostics below for the complete excerpt.'
        }
        $row = "| [$(ConvertTo-ScheduledTableCell $failure.name)]($($failure.url)) | $(ConvertTo-ScheduledTableCell $failure.conclusion) | $(ConvertTo-ScheduledTableCell $summary) |"
        if ($table.Length + $row.Length -gt $sectionLimit) {
            $blocks.Add($table)
            $table = $tableHeader
        }
        $table += "`n$row"
    }
    $blocks.Add($table)
    foreach ($failure in $Failures) {
        $heading = "## $(ConvertTo-ScheduledTableCell $failure.name)`n`n[Job and full logs]($($failure.url))"
        foreach ($diagnostic in $failure.diagnostics) {
            # Indented text cannot inject hidden HTML, alter headings, or escape a code fence.
            # Split before indentation so every continuation remains independently readable.
            foreach ($part in @(Split-ScheduledReportText -Text $diagnostic -Length $sectionLimit)) {
                $text = (($part -split '\r?\n' | ForEach-Object { "    $_" }) -join "`n")
                $blocks.Add("$heading`n`n$text")
            }
        }
    }
    $message = $prefix
    foreach ($block in $blocks) {
        # Very dense short lines expand when indented. Split again without dropping lines.
        foreach ($part in @(Split-ScheduledReportText -Text $block -Length ($messageLimit - $prefix.Length - 2))) {
            if ($message.Length + $part.Length + 2 -gt $messageLimit) {
                $message
                $message = $prefix
            }
            $message += "`n`n$part"
        }
    }
    $message
}

Export-ModuleMember -Function Get-ScheduledLogExcerpt, Format-ScheduledReport
