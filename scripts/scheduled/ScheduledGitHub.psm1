#requires -Version 7
# The trusted scheduled-report.yml controller collects exact-attempt diagnostics and publishes
# readable run reports. It uses the runner's PowerShell/gh rather than Rust so setup failures
# remain reportable. GitHub owns report identity; downloaded files are disposable diagnostics.
# Ref: ../../.github/workflows/implementation.md#failure-reporting.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
Import-Module (Join-Path $PSScriptRoot 'ScheduledReport.psm1')

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

function Save-ScheduledGitHubFile {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string] $Endpoint,
        [Parameter(Mandatory)][string] $Path
    )
    # Binary ZIP responses must bypass PowerShell's text pipeline. Concurrent stderr draining
    # prevents a failed download from blocking on a full pipe; no downloaded code is executed.
    $start = [Diagnostics.ProcessStartInfo]::new()
    $start.FileName = (Get-Command gh -CommandType Application | Select-Object -First 1).Source
    $start.UseShellExecute = $false
    $start.RedirectStandardOutput = $true
    $start.RedirectStandardError = $true
    foreach ($argument in @('api', $Endpoint, '--allow-escape-sequences')) { $start.ArgumentList.Add($argument) }
    $process = [Diagnostics.Process]::new()
    $process.StartInfo = $start
    $file = [IO.File]::Create($Path)
    try {
        if (-not $process.Start()) { throw 'Could not start the GitHub download.' }
        $errorTask = $process.StandardError.ReadToEndAsync()
        $process.StandardOutput.BaseStream.CopyTo($file)
        $process.WaitForExit()
        $diagnostic = $errorTask.GetAwaiter().GetResult()
        if ($process.ExitCode -ne 0) { throw "GitHub download failed for ${Endpoint}: $diagnostic" }
    } finally {
        $file.Dispose()
        $process.Dispose()
    }
}

function Read-ScheduledArtifactText {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string] $Repository,
        [Parameter(Mandatory)][hashtable] $Artifact,
        [Parameter(Mandatory)][string] $OutputDirectory,
        [Parameter(Mandatory)][ValidateSet('summary.md', 'plan.json')][string] $Name
    )
    if ($Artifact.expired) { throw 'The artifact has expired.' }
    $path = Join-Path $OutputDirectory "$([long]$Artifact.id).zip"
    Save-ScheduledGitHubFile "repos/$Repository/actions/artifacts/$([long]$Artifact.id)/zip" $path
    $archive = [IO.Compression.ZipFile]::OpenRead($path)
    try {
        # Read only the agreed text entry, in place. Never extract candidate paths into either
        # the controller or diagnostics directory, including symlink/path-traversal entries.
        $entries = @($archive.Entries | Where-Object FullName -CEQ $Name)
        if ($entries.Count -ne 1) { throw "Artifact must contain one root $Name entry." }
        $reader = [IO.StreamReader]::new($entries[0].Open())
        try { return $reader.ReadToEnd() } finally { $reader.Dispose() }
    } finally { $archive.Dispose() }
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
    $run = Invoke-ScheduledGitHubJson "$endpoint/attempts/$RunAttempt"
    if ($run.id -ne $RunId -or $run.run_attempt -ne $RunAttempt -or $run.status -cne 'completed' -or
        $run.path -cnotin @('.github/workflows/full-deep-validation.yml', '.github/workflows/selected-deep-validation.yml')) {
        throw 'Expected a completed deep-validation workflow attempt.'
    }
    if ($run.conclusion -ceq 'success') { return 'Validation succeeded; earlier reports are unchanged.' }

    $null = New-Item -ItemType Directory -Path $OutputDirectory -Force
    $OutputDirectory = (Resolve-Path -LiteralPath $OutputDirectory).Path
    $attemptUrl = "https://github.com/$Repository/actions/runs/$RunId/attempts/$RunAttempt"
    # A jobs API failure is not an empty execution. Let it fail the reporter, preserving retry.
    $jobs = @(Get-ScheduledGitHubCollection "$endpoint/attempts/$RunAttempt/jobs" -Property jobs)
    $issues = @(Get-ScheduledGitHubCollection "repos/$Repository/issues?state=all&labels=scheduled-run-failure")
    $attemptPattern = [regex]::Escape($attemptUrl) + '(?=$|[\s<>)\].,;!?])'
    $reports = @($issues | Where-Object {
        -not $_.ContainsKey('pull_request') -and [string]$_.body -cmatch $attemptPattern
    } | Sort-Object number)

    $notices = [Collections.Generic.List[string]]::new()
    $artifacts = @()
    try { $artifacts = @(Get-ScheduledGitHubCollection "$endpoint/artifacts" -Property artifacts) }
    catch { $notices.Add("Result artifact inventory is unavailable: $($_.Exception.Message)") }
    $plan = $null
    # Failed-job reruns retain successful planning outputs. This run's event inputs and
    # controller SHA are immutable, so its latest plan at or before this attempt still identifies
    # the selected source. This does not authorize reuse of prior checker results or logs.
    # Ref: ../../.github/workflows/implementation.md#immutable-execution.
    $planArtifacts = @($artifacts | Where-Object {
        $attempt = 0
        $_.name -cmatch "^scheduled-plan-$RunId-([1-9][0-9]*)$" -and
            [int]::TryParse($Matches[1], [ref]$attempt) -and $attempt -le $RunAttempt
    } | Sort-Object { [int]($_.name -split '-')[-1] } -Descending)
    $planAttempt = $RunAttempt
    if ($planArtifacts.Count -gt 0) {
        $planName = $planArtifacts[0].name
        $planAttempt = [int]($planName -split '-')[-1]
        $planArtifacts = @($planArtifacts | Where-Object name -CEQ $planName)
    }
    $sourceDescription = 'Unavailable: planning diagnostics do not establish the selected tested source. See [run inputs and logs](' + $attemptUrl + '). Workflow/controller commit (not the tested-source identity): `' + $run.head_sha + '`.'
    if ($planArtifacts.Count -eq 1) {
        try {
            $plan = Read-ScheduledArtifactText $Repository $planArtifacts[0] $OutputDirectory plan.json |
                ConvertFrom-Json -AsHashtable
            if ($plan.source_sha -cnotmatch '^[a-f0-9]{40}$' -or -not $plan.ContainsKey('checks')) {
                throw 'Plan has no full source commit or check declarations.'
            }
            if ($plan.controller_sha -cne $run.head_sha) {
                throw 'Plan controller commit does not match the originating workflow run.'
            }
            foreach ($check in $plan.checks) {
                if ($check.id -cnotmatch '^[A-Za-z0-9_-]+$' -or [string]::IsNullOrWhiteSpace($check.platform)) {
                    throw 'Plan has an invalid check declaration.'
                }
            }
            $sourceDescription = '`' + $plan.source_sha + '`'
            if ($planAttempt -lt $RunAttempt) {
                $notices.Add("Tested-source metadata comes from the retained [planning artifact from attempt $planAttempt](https://github.com/$Repository/actions/runs/$RunId/artifacts/$($planArtifacts[0].id)). Workflow inputs and the controller commit are unchanged across reruns; checker results and logs below belong only to attempt $RunAttempt.")
            }
        } catch {
            $plan = $null
            $notices.Add("Tested-source plan is unavailable: $($_.Exception.Message)")
        }
    } elseif ($planArtifacts.Count -gt 1) {
        $notices.Add("Multiple planning artifacts identify attempt $planAttempt; the tested commit cannot be established.")
    }
    $failedJobs = @($jobs | Where-Object conclusion -CNE 'success')
    $failures = [Collections.Generic.List[hashtable]]::new()
    $readResults = [Collections.Generic.HashSet[long]]::new()
    foreach ($job in $failedJobs) {
        $diagnostics = [Collections.Generic.List[string]]::new()
        $conclusion = if ([string]::IsNullOrWhiteSpace($job.conclusion)) { 'incomplete' } else { $job.conclusion }
        $steps = @($job.steps | Where-Object { $_.conclusion -cnotin @('success', 'skipped') })
        $summary = if ($steps.Count -gt 0) {
            'Unsuccessful steps: ' + (($steps | ForEach-Object { "$($_.name) ($($_.conclusion))" }) -join '; ')
        } elseif ($job.conclusion -ceq 'skipped') {
            'Did not run. A prerequisite failure or cancellation may have prevented execution.'
        } else { "Job concluded ${conclusion}; no failed step was recorded." }
        $diagnostics.Add($summary)
        $scope = if ($job.ContainsKey('labels') -and @($job.labels).Count -gt 0) {
            $job.labels -join ', '
        } else { 'Not available' }
        if ($null -ne $plan) {
            foreach ($check in $plan.checks) {
                if ($job.name -cmatch ('(?<![A-Za-z0-9_-])' + [regex]::Escape($check.id) + '(?![A-Za-z0-9_-])')) {
                    $scope = "$($check.platform) / $($check.id)"
                }
            }
        }
        if ($job.conclusion -cne 'skipped') {
            try {
                $logPath = Join-Path $OutputDirectory "job-$([long]$job.id).log"
                Save-ScheduledGitHubFile "repos/$Repository/actions/jobs/$([long]$job.id)/logs" $logPath
                $excerpt = Get-ScheduledLogExcerpt (Get-Content -LiteralPath $logPath -Raw)
                if ([string]::IsNullOrWhiteSpace($excerpt)) { throw 'The job log is empty.' }
                $diagnostics.Add("Observed job log excerpt:`n$excerpt")
                $errorLine = @($excerpt -split '\r?\n' | Where-Object {
                    $_ -match '(?i)##\[error\]|\berror\b|failed|panicked|MISSED|TIMEOUT|timed out|undefined behavior'
                } | Select-Object -First 1)
                if ($errorLine.Count -gt 0) { $summary = $errorLine[0] }
            } catch { $diagnostics.Add("Job logs unavailable: $($_.Exception.Message)") }
        }
        $resultArtifacts = @($artifacts | Where-Object {
            $prefix = "scheduled-result-$RunId-$RunAttempt-"
            $_.name.StartsWith($prefix, [StringComparison]::Ordinal) -and
                $job.name -cmatch ('(?<![A-Za-z0-9_-])' + [regex]::Escape($_.name.Substring($prefix.Length)) + '(?![A-Za-z0-9_-])')
        })
        foreach ($artifact in $resultArtifacts) {
            $null = $readResults.Add([long]$artifact.id)
            $diagnostics.Add("Result artifact: https://github.com/$Repository/actions/runs/$RunId/artifacts/$($artifact.id)")
            try {
                $text = Read-ScheduledArtifactText $Repository $artifact $OutputDirectory summary.md
                if ([string]::IsNullOrWhiteSpace($text)) { throw 'The check summary is empty.' }
                $diagnostics.Add($text)
            } catch { $diagnostics.Add("Check summary unavailable: $($_.Exception.Message)") }
        }
        if ($resultArtifacts.Count -eq 0 -and $job.conclusion -cne 'skipped') {
            $diagnostics.Add('No check-summary artifact identifies this job in this attempt. Setup may have failed before the checker ran.')
        }
        $failures.Add(@{
            name = $job.name; url = $job.html_url; conclusion = [string]$conclusion
            scope = $scope; summary = $summary; diagnostics = $diagnostics.ToArray()
        })
    }
    $unmatched = @($artifacts | Where-Object {
        $prefix = "scheduled-result-$RunId-$RunAttempt-"
        if (-not $_.name.StartsWith($prefix, [StringComparison]::Ordinal)) { return $false }
        $pattern = '(?<![A-Za-z0-9_-])' + [regex]::Escape($_.name.Substring($prefix.Length)) + '(?![A-Za-z0-9_-])'
        -not $readResults.Contains([long]$_.id) -and
            @($jobs | Where-Object { $_.name -cmatch $pattern }).Count -eq 0
    })
    foreach ($artifact in $unmatched) {
        $notices.Add("Check-summary artifact could not be associated with an Actions job: $($artifact.name). See https://github.com/$Repository/actions/runs/$RunId/artifacts/$($artifact.id).")
        try {
            $text = Read-ScheduledArtifactText $Repository $artifact $OutputDirectory summary.md
            if ([string]::IsNullOrWhiteSpace($text)) { throw 'The check summary is empty.' }
            $failures.Add(@{
                name = $artifact.name; url = $attemptUrl; scope = 'Job association unavailable'
                conclusion = 'Unknown'; summary = 'Check summary has no matching Actions job.'
                diagnostics = @($text)
            })
        } catch { $notices.Add("Unassociated check summary unavailable: $($_.Exception.Message)") }
    }
    $messages = @(Format-ScheduledReport -Run $run -AttemptUrl $attemptUrl `
        -SourceDescription $sourceDescription -Failures $failures.ToArray() -Notices $notices.ToArray())
    for ($index = 0; $index -lt $messages.Count; $index++) {
        Set-Content -LiteralPath (Join-Path $OutputDirectory "report-$index.md") -Value $messages[$index] -Encoding utf8 -NoNewline
    }
    if ($reports.Count -eq 0) {
        $labels = @(Get-ScheduledGitHubCollection "repos/$Repository/labels")
        if (@($labels | Where-Object name -CEQ 'scheduled-run-failure').Count -eq 0) {
            # Error-red distinguishes the failed-run triage queue from repair work.
            $null = Invoke-ScheduledGitHubJson "repos/$Repository/labels" -Method POST -Body @{
                name = 'scheduled-run-failure'; color = 'B60205'; description = 'A failed deep-validation attempt awaiting triage'
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
