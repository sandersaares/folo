#requires -Version 7
# Produces bounded model-facing views without truncating the authoritative snapshot. Index
# summaries cover every issue through a cursor; complete candidate records stream as JSON
# text pages so tool-output limits cannot silently turn a partial read into a full receipt.
# Ref: ../../docs/scheduled-triage.md#helper-interface.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true

function Get-TriageBriefText {
    param([AllowNull()][object] $Value, [int] $Limit = 512)
    $text = [string]$Value
    # Summary text is only a candidate-discovery aid; full records retain the original bytes.
    if ($text.Length -le $Limit) { return $text }
    if ([char]::IsHighSurrogate($text[$Limit - 1])) { $Limit-- }
    return $text.Substring(0, $Limit)
}

function Get-ScheduledTriageIndexPage {
    [CmdletBinding()]
    param([hashtable] $Snapshot, [int] $Offset)
    if ($Offset -lt 0 -or $Offset -gt $Snapshot.index.entries.Count) { throw 'Invalid index offset.' }
    # Leave space for JSON formatting and envelope metadata within ordinary tool-output limits.
    $budget = 12000
    $entries = [Collections.Generic.List[object]]::new()
    $next = $Offset
    while ($next -lt $Snapshot.index.entries.Count) {
        $entry = $Snapshot.index.entries[$next]
        $full = $Snapshot.problems[[string]$entry.issue_number].record
        $diagnosis = if ($null -ne $full.problem) { $full.problem.diagnosis } else {
            @{ summary = $full.legacy.evidence.summary; cause = ''; category = 'reporter-observation'
                repair_disposition = 'unexamined'; scope = @() }
        }
        $scopes = @(if ($null -ne $full.problem) {
            $full.problem.evidence | Where-Object { $_.generation -eq $full.problem.generation } |
                ForEach-Object { $_.diagnosis.scope }
        } else { @{ operation = $full.legacy.check_kind; package = $full.legacy.package; check_id = $full.legacy.check_id
            platform = $full.legacy.platform; replay = $full.legacy.evidence.replay } })
        $symptoms = @(if ($null -ne $full.problem) {
            $full.problem.evidence | ForEach-Object { $_.diagnosis.summary } | Sort-Object -Unique
        } else { $full.legacy.evidence.summary })
        $summary = @{
            issue_number = $entry.issue_number; generation = $entry.generation; scope_revision = $entry.scope_revision
            record_digest = $entry.record_digest; title = Get-TriageBriefText $full.issue.title
            issue_state = $full.issue.state
            problem_status = if ($null -ne $full.problem) { $full.problem.status } else { $full.legacy.status }
            category = $diagnosis.category; summary = Get-TriageBriefText $diagnosis.summary
            cause = Get-TriageBriefText $diagnosis.cause; repair_disposition = $diagnosis.repair_disposition
            scope_count = $scopes.Count
            scope_preview = @($scopes | Select-Object -First 3 | ForEach-Object {
                @{ operation = Get-TriageBriefText $_.operation 256
                    package = Get-TriageBriefText $_.package 64; check_id = Get-TriageBriefText $_.check_id 64
                    platform = Get-TriageBriefText $_.platform 64
                    # JSON retains the meaning of nested typed qualifiers within the abbreviated
                    # preview; the full record remains the authority for replay and scope.
                    replay_preview = if ($null -ne $_.replay) {
                        Get-TriageBriefText ($_.replay | ConvertTo-Json -Depth 100 -Compress) 512
                    } else { $null }
                }
            })
            prior_symptom_count = $symptoms.Count
            prior_symptoms = @($symptoms | Select-Object -First 2 | ForEach-Object { Get-TriageBriefText $_ 256 })
            evidence_issue_numbers = @(if ($null -ne $full.problem) {
                $full.problem.evidence | ForEach-Object { $_.revision.issue_number } | Sort-Object -Unique | Select-Object -First 3
            })
            summary_is_abbreviated = $true
            full_record_available = $true
        }
        $candidate = @($entries.ToArray()) + @($summary)
        $bytes = [Text.Encoding]::UTF8.GetByteCount(($candidate | ConvertTo-Json -Depth 20 -Compress))
        if ($entries.Count -gt 0 -and $bytes -gt $budget) { break }
        if ($bytes -gt $budget) {
            $summary = @{
                issue_number = $entry.issue_number; generation = $entry.generation; scope_revision = $entry.scope_revision
                record_digest = $entry.record_digest
                title = Get-TriageBriefText $full.issue.title 128; summary = Get-TriageBriefText $diagnosis.summary 128
                issue_state = $full.issue.state; summary_is_abbreviated = $true; full_record_available = $true
            }
        }
        $entries.Add($summary)
        $next++
    }
    return @{
        index_digest = $Snapshot.index.digest; entries = $entries.ToArray()
        next_offset = if ($next -lt $Snapshot.index.entries.Count) { $next } else { $null }
    }
}

function Get-ScheduledTriageProblemPage {
    [CmdletBinding()]
    param([hashtable] $Problem, [int] $Offset)
    # ASCII JSON escapes avoid splitting a UTF-16 surrogate pair at a page boundary.
    # Encoded transport pages are redundant with their restored documents; retain discussion,
    # the complete current problem (including occurrence history), and legacy registration.
    $record = $Problem.record
    $machineIds = if ($null -ne $record.details) {
        @($record.details.revisions | ForEach-Object { $_.pages } | ForEach-Object { $_.id })
    } else { @() }
    $view = @{
        issue = $record.issue; problem = $record.problem; legacy = $record.legacy
        comments = @($record.comments | Where-Object {
            $_.id -notin $machineIds
        })
    }
    $page = Get-ScheduledTriageJsonPage -Value $view -Offset $Offset
    $page.full_read_digest = $Problem.full_read_digest
    return $page
}

function Get-ScheduledTriageJsonPage {
    [CmdletBinding()]
    param([Parameter(Mandatory)][object] $Value, [int] $Offset)
    $text = $Value | ConvertTo-Json -Depth 100 -Compress -EscapeHandling EscapeNonAscii
    if ($Offset -lt 0 -or $Offset -ge $text.Length) { throw 'Invalid full-record offset.' }
    # Leave room for JSON-string escaping in the outer tool response, not just the content.
    $length = [Math]::Min(6000, $text.Length - $Offset)
    return @{
        offset = $Offset; total_length = $text.Length
        content = $text.Substring($Offset, $length)
        next_offset = if ($Offset + $length -lt $text.Length) { $Offset + $length } else { $null }
        end_offset = $Offset + $length
    }
}

Export-ModuleMember -Function Get-ScheduledTriageIndexPage, Get-ScheduledTriageProblemPage,
Get-ScheduledTriageJsonPage
