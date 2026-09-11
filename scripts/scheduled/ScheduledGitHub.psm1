#requires -Version 7
# The report job in deep-validation.yml publishes readable failures from its own workflow.
# It uses the runner's PowerShell/gh rather than Rust so setup failures remain reportable.
# GitHub owns report identity; downloaded files are disposable diagnostics.
# Ref: ../../.github/workflows/implementation.md#failure-reporting.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
Import-Module (Join-Path $PSScriptRoot 'ScheduledReport.psm1')

# Result archives include raw tool output as well as summaries. Bound their transfer and disk
# footprint so an ordinary verbose check cannot consume the reporter's available storage.
$script:ArchiveByteLimit = 64MB
# Decode only bounded log prefixes and selected ZIP entries into memory for issue assembly.
# Longer diagnostics remain available through the original job/artifact links.
$script:TextByteLimit = 4MB

function Invoke-ScheduledGitHubJson {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string] $Endpoint,
        [ValidateSet('GET', 'POST', 'PATCH')][string] $Method = 'GET',
        [hashtable] $Body
    )
    $arguments = @('api', $Endpoint, '--method', $Method)
    $response = if ($null -ne $Body) {
        $Body | ConvertTo-Json -Depth 20 -Compress | & gh @arguments --input -
    } else { & gh @arguments }
    if ($LASTEXITCODE -ne 0) { throw "GitHub API request failed: $Method $Endpoint" }
    return ,($response -join "`n" | ConvertFrom-Json -AsHashtable -NoEnumerate)
}

function Get-ScheduledGitHubCollection {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string] $Endpoint,
        [string] $Property
    )
    $separator = if ($Endpoint.Contains('?')) { '&' } else { '?' }
    # Follow every API page, including closed reports and large job/artifact collections.
    # GitHub's maximum page size avoids an artificial workflow-size/report-count limit.
    $page = 1
    do {
        $response = Invoke-ScheduledGitHubJson "${Endpoint}${separator}per_page=100&page=$page"
        if ($Property) {
            if (-not $response.ContainsKey($Property)) { throw "GitHub response lacks $Property." }
            $items = @($response[$Property])
        } else { $items = @($response) }
        $items
        $page++
    } while ($items.Count -eq 100)
}

function Get-ScheduledDownloadStartInfo {
    [CmdletBinding()]
    param([Parameter(Mandatory)][string] $Endpoint)

    # Binary responses bypass PowerShell's text pipeline. gh errors go directly to the runner
    # log rather than an accumulating stderr buffer; no downloaded code is executed.
    $start = [Diagnostics.ProcessStartInfo]::new()
    $start.FileName = (Get-Command gh -CommandType Application | Select-Object -First 1).Source
    $start.UseShellExecute = $false
    $start.RedirectStandardOutput = $true
    foreach ($argument in @('api', $Endpoint, '--allow-escape-sequences')) { $start.ArgumentList.Add($argument) }
    return $start
}

function Copy-ScheduledLimitedStream {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][IO.Stream] $Source,
        [Parameter(Mandatory)][IO.Stream] $Destination,
        [Parameter(Mandatory)][ValidateRange(1, [long]::MaxValue)][long] $ByteLimit
    )
    # Count bytes actually read, not HTTP headers or ZIP entry metadata. One extra byte
    # distinguishes a complete response exactly at the limit from a truncated response.
    $buffer = [byte[]]::new([int][Math]::Min(81920L, $ByteLimit)) # .NET's normal copy buffer size.
    $remaining = $ByteLimit
    while ($remaining -gt 0) {
        $count = $Source.Read($buffer, 0, [int][Math]::Min($buffer.Length, $remaining))
        if ($count -eq 0) { return $false }
        $Destination.Write($buffer, 0, $count)
        $remaining -= $count
    }
    return $Source.ReadByte() -ne -1
}

function Save-ScheduledGitHubFile {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string] $Endpoint,
        [Parameter(Mandatory)][string] $Path,
        [ValidateRange(1, [long]::MaxValue)][long] $ByteLimit = $script:ArchiveByteLimit,
        [switch] $AllowPartial
    )
    $process = [Diagnostics.Process]::new()
    $process.StartInfo = Get-ScheduledDownloadStartInfo $Endpoint
    $file = $null
    $started = $false
    # Callers remove complete downloads and allowed text prefixes after reading. Failed
    # transfers are removed here, before an archive can be mistaken for a complete ZIP.
    $keepFile = $false
    try {
        $file = [IO.File]::Create($Path)
        $started = $process.Start()
        if (-not $started) { throw 'Could not start the GitHub download.' }
        $truncated = Copy-ScheduledLimitedStream $process.StandardOutput.BaseStream $file $ByteLimit
        if ($truncated -and -not $process.HasExited) { $process.Kill($true) }
        $process.WaitForExit()
        if ($truncated -and -not $AllowPartial) {
            throw "Download exceeded the $ByteLimit byte limit; the complete artifact is unavailable."
        }
        if (-not $truncated -and $process.ExitCode -ne 0) {
            throw "GitHub download failed for $Endpoint (exit $($process.ExitCode)); see the reporter's Actions log."
        }
        $keepFile = $true
        return $truncated
    } finally {
        if ($started -and -not $process.HasExited) { $process.Kill($true); $process.WaitForExit() }
        if ($null -ne $file) { $file.Dispose() }
        $process.Dispose()
        if (-not $keepFile) { [IO.File]::Delete($Path) }
    }
}

function Read-ScheduledArtifactText {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string] $Repository,
        [Parameter(Mandatory)][hashtable] $Artifact,
        [Parameter(Mandatory)][string] $OutputDirectory
    )
    if ($Artifact.expired) { throw 'The artifact has expired.' }
    $path = Join-Path $OutputDirectory "$([long]$Artifact.id).zip"
    try {
        # A partial archive is never opened, even when its first entries look usable.
        $null = Save-ScheduledGitHubFile "repos/$Repository/actions/artifacts/$([long]$Artifact.id)/zip" $path
        $archive = [IO.Compression.ZipFile]::OpenRead($path)
        try {
            # Read only the summary in place; extraction is unnecessary for reporting.
            $entries = @($archive.Entries | Where-Object FullName -CEQ 'summary.md')
            if ($entries.Count -ne 1) { throw 'Artifact must contain one root summary.md entry.' }
            $source = $entries[0].Open()
            $content = [IO.MemoryStream]::new()
            try {
                $truncated = Copy-ScheduledLimitedStream $source $content $script:TextByteLimit
                $text = [Text.Encoding]::UTF8.GetString($content.GetBuffer(), 0, [int]$content.Length).TrimStart([char]0xFEFF)
                if ($truncated) {
                    $text += "`n`nCheck summary truncated at the $script:TextByteLimit byte limit. Remaining diagnostics are unavailable here; see the original artifact linked above."
                }
                return $text
            } finally { $source.Dispose(); $content.Dispose() }
        } finally { $archive.Dispose() }
    } finally { [IO.File]::Delete($path) }
}

function Invoke-ScheduledReporting {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][ValidatePattern('^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$')][string] $Repository,
        [Parameter(Mandatory)][ValidateRange(1, [long]::MaxValue)][long] $RunId,
        [Parameter(Mandatory)][ValidateRange(1, [int]::MaxValue)][int] $RunAttempt,
        [Parameter(Mandatory)][string] $OutputDirectory
    )
    $endpoint = "repos/$Repository/actions/runs/$RunId"
    $run = Invoke-ScheduledGitHubJson $endpoint
    if ($run.id -ne $RunId -or $run.run_attempt -ne $RunAttempt -or $run.head_sha -cnotmatch '^[a-f0-9]{40}$') {
        throw 'The run must identify the requested current attempt and tested commit.'
    }

    # Read every attempt so cached dependencies survive reporter-only and failed-job reruns.
    # https://docs.github.com/en/rest/actions/workflow-jobs#list-jobs-for-a-workflow-run
    $jobs = @(Get-ScheduledGitHubCollection "$endpoint/jobs?filter=all" -Property jobs)
    foreach ($job in $jobs) {
        if ($job.run_id -ne $RunId -or $job['run_attempt'] -lt 1) {
            throw 'Job results must identify this run and a positive execution attempt.'
        }
    }
    $jobs = @($jobs | Where-Object run_attempt -LE $RunAttempt |
        Group-Object name -CaseSensitive | ForEach-Object {
            $versions = @($_.Group | Sort-Object run_attempt -Descending)
            $latest = $versions[0]
            # GitHub also copies cached jobs into new attempts with new IDs but unchanged
            # execution timestamps. The original row identifies the artifact-producing attempt.
            if ($latest.status -ceq 'completed' -and $latest['started_at'] -and $latest['completed_at']) {
                $versions | Where-Object {
                    $_.conclusion -ceq $latest.conclusion -and
                        $_['started_at'] -ceq $latest.started_at -and $_['completed_at'] -ceq $latest.completed_at
                } | Sort-Object run_attempt | Select-Object -First 1
            } else { $latest }
        })
    $failedJobs = @($jobs | Where-Object {
        $_.name -cne 'report' -and $_.status -ceq 'completed' -and
            $_.conclusion -cin @('failure', 'timed_out', 'action_required')
    })
    if ($failedJobs.Count -eq 0) { return 'No failed validation jobs; earlier reports are unchanged.' }

    $null = New-Item -ItemType Directory -Path $OutputDirectory -Force
    $OutputDirectory = (Resolve-Path -LiteralPath $OutputDirectory).Path
    $attemptUrl = "https://github.com/$Repository/actions/runs/$RunId/attempts/$RunAttempt"
    $issues = @(Get-ScheduledGitHubCollection "repos/$Repository/issues?state=all&labels=scheduled-run-failure")
    $linkBoundary = '(?=$|[\s<>)\].,;!?])'
    $attemptPattern = [regex]::Escape($attemptUrl) + $linkBoundary
    $bodyAttemptPattern = 'https://github\.com/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/actions/runs/[1-9][0-9]*/attempts/[1-9][0-9]*' + $linkBoundary
    $reports = @($issues | Where-Object {
        if ($_.ContainsKey('pull_request')) { return $false }
        if ([string]$_.body -cmatch $attemptPattern) { return $true }
        # A body identifying another attempt takes precedence over comparison links in comments.
        # Otherwise a closed report could absorb a new failure instead of creating triage work.
        if ([string]$_.body -cmatch $bodyAttemptPattern) { return $false }
        if ($_.ContainsKey('comments') -and $_.comments -eq 0) { return $false }
        # Human reports may identify the attempt in their discussion rather than the body.
        # An unavailable discussion must fail lookup, not authorize another issue.
        $discussion = @(Get-ScheduledGitHubCollection "repos/$Repository/issues/$($_.number)/comments")
        return @($discussion | Where-Object body -CMatch $attemptPattern).Count -gt 0
    } | Sort-Object number)

    $notices = [Collections.Generic.List[string]]::new()
    $artifacts = @()
    try { $artifacts = @(Get-ScheduledGitHubCollection "$endpoint/artifacts" -Property artifacts) }
    catch { $notices.Add("Result artifact inventory is unavailable: $($_.Exception.Message)") }
    $failures = [Collections.Generic.List[hashtable]]::new()
    foreach ($job in $failedJobs) {
        $diagnostics = [Collections.Generic.List[string]]::new()
        $steps = @($job['steps'] | Where-Object { $null -ne $_ -and $_.conclusion -cnotin @('success', 'skipped') })
        $summary = if ($steps.Count -gt 0) {
            'Unsuccessful steps: ' + (($steps | ForEach-Object { "$($_.name) ($($_.conclusion))" }) -join '; ')
        } else { "Job concluded $($job.conclusion); no failed step was recorded." }
        $diagnostics.Add($summary)
        $logPath = Join-Path $OutputDirectory "job-$([long]$job.id).log"
        try {
            $truncated = Save-ScheduledGitHubFile "repos/$Repository/actions/jobs/$([long]$job.id)/logs" `
                $logPath -ByteLimit $script:TextByteLimit -AllowPartial
            if ($truncated) {
                $diagnostics.Add("Job log truncated at the $script:TextByteLimit byte limit. Remaining diagnostics are unavailable here; full log: $($job.html_url)")
            }
            $excerpt = Get-ScheduledLogExcerpt (Get-Content -LiteralPath $logPath -Raw)
            if ([string]::IsNullOrWhiteSpace($excerpt)) { throw 'The job log is empty.' }
            $diagnostics.Add("Observed job log excerpt:`n$excerpt")
            $errorLine = @($excerpt -split '\r?\n' | Where-Object {
                $_ -match '(?i)##\[error\]|\berror\b|failed|panicked|MISSED|TIMEOUT|timed out|undefined behavior'
            } | Select-Object -First 1)
            if ($errorLine.Count -gt 0) { $summary = $errorLine[0] }
        } catch { $diagnostics.Add("Job logs unavailable: $($_.Exception.Message) Full log: $($job.html_url)") }
        finally { [IO.File]::Delete($logPath) }
        $diagnostics.Add("Job execution attempt: $($job.run_attempt)")
        $resultArtifacts = @($artifacts | Where-Object name -CEQ "scheduled-result-$RunId-$($job.run_attempt)-$($job.name)")
        foreach ($artifact in $resultArtifacts) {
            $diagnostics.Add("Result artifact: https://github.com/$Repository/actions/runs/$RunId/artifacts/$($artifact.id)")
            try {
                $text = Read-ScheduledArtifactText $Repository $artifact $OutputDirectory
                if ([string]::IsNullOrWhiteSpace($text)) { throw 'The check summary is empty.' }
                $diagnostics.Add($text)
            } catch { $diagnostics.Add("Check summary unavailable: $($_.Exception.Message)") }
        }
        if ($resultArtifacts.Count -eq 0) {
            $diagnostics.Add('No check-summary artifact identifies this job execution. Setup may have failed before the checker ran.')
        }
        $failures.Add(@{
            name = $job.name; url = $job.html_url; conclusion = $job.conclusion
            summary = $summary; diagnostics = $diagnostics.ToArray()
        })
    }
    $messages = @(Format-ScheduledReport -Run $run -AttemptUrl $attemptUrl `
        -Failures $failures.ToArray() -Notices $notices.ToArray())
    for ($index = 0; $index -lt $messages.Count; $index++) {
        Set-Content -LiteralPath (Join-Path $OutputDirectory "report-$index.md") -Value $messages[$index] -Encoding utf8 -NoNewline
    }
    if ($reports.Count -eq 0) {
        $labels = @(Get-ScheduledGitHubCollection "repos/$Repository/labels")
        if (@($labels | Where-Object name -EQ 'scheduled-run-failure').Count -eq 0) {
            # Error-red distinguishes the failed-run triage queue from repair work.
            try {
                $null = Invoke-ScheduledGitHubJson "repos/$Repository/labels" -Method POST -Body @{
                    name = 'scheduled-run-failure'; color = 'B60205'; description = 'A failed deep-validation attempt awaiting triage'
                }
            } catch {
                # Other run reporters can create the label concurrently. Only its observed
                # presence permits continuing; never overwrite its existing metadata.
                $creationFailure = $_
                try { $label = Invoke-ScheduledGitHubJson "repos/$Repository/labels/scheduled-run-failure" }
                catch { throw $creationFailure }
                if ($null -eq $label -or $label['name'] -ne 'scheduled-run-failure') { throw $creationFailure }
            }
        }
        $date = ([datetimeoffset]$run.run_started_at).UtcDateTime.ToString('yyyy-MM-dd')
        $report = Invoke-ScheduledGitHubJson "repos/$Repository/issues" -Method POST -Body @{
            title = "Scheduled validation failed on $date"; body = $messages[0]; labels = @('scheduled-run-failure')
        }
        $existingText = @($messages[0])
    } else {
        $report = $reports[0]
        $comments = @(Get-ScheduledGitHubCollection "repos/$Repository/issues/$($report.number)/comments")
        $existingText = @([string]$report.body) + @($comments | ForEach-Object { [string]$_.body })
    }
    # Compare ordinary text, without author or ownership markers. This also completes a retry
    # interrupted while adding lengthy diagnostics, and supplements a human-authored report.
    foreach ($message in $messages) {
        if ($message -cnotin $existingText) {
            $null = Invoke-ScheduledGitHubJson "repos/$Repository/issues/$($report.number)/comments" -Method POST -Body @{ body = $message }
        }
    }
    # Exact attempt duplicates need only normal issue reconciliation, not a publication journal.
    foreach ($duplicate in @($reports | Select-Object -Skip 1 | Where-Object state -CEQ 'open')) {
        $null = Invoke-ScheduledGitHubJson "repos/$Repository/issues/$($duplicate.number)/comments" -Method POST -Body @{
            body = "[Copilot speaking]`n`nDuplicate report for $attemptUrl. Continuing triage in $($report.html_url)."
        }
        $null = Invoke-ScheduledGitHubJson "repos/$Repository/issues/$($duplicate.number)" -Method PATCH -Body @{ state = 'closed'; state_reason = 'not_planned' }
    }
    return "Report: $($report.html_url)"
}

Export-ModuleMember -Function Invoke-ScheduledReporting
