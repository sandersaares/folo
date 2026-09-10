#requires -Version 7
# Reads the complete run and problem backlog for scheduled-triage. GitHub pages and original
# run identities are verified before the real Rust codec restores committed evidence. The AI
# receives summaries and separately readable full records; no deterministic causal matching
# occurs here. Retrieval failures abort the scan instead of becoming an empty queue.
# Ref: ../../docs/scheduled-triage.md#evidence-and-reasoning.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
Import-Module (Join-Path $PSScriptRoot 'ScheduledContracts.psm1')
Import-Module (Join-Path $PSScriptRoot 'ScheduledRecordTool.psm1')
Import-Module (Join-Path $PSScriptRoot 'LocalGitHub.psm1')
Import-Module (Join-Path $PSScriptRoot 'LocalTriageEvidence.psm1')

function Invoke-ScheduledTriageRead {
    [CmdletBinding()]
    param([Parameter(Mandatory)][string] $Endpoint, [switch] $Collection, [switch] $Pages)
    if ($Collection) { return ,(Get-ScheduledApiCollection -Endpoint $Endpoint) }
    if ($Pages) {
        $result = Invoke-ScheduledApi -Endpoint $Endpoint -Paginate
        foreach ($page in $result) { $page }
        return
    }
    return Invoke-ScheduledApi -Endpoint $Endpoint
}

function Get-TriageReadAdapter {
    param([scriptblock] $Api)
    $transport = $Api
    return {
        param($Endpoint, [switch] $Collection, [switch] $Pages)
        if ($Collection) {
            $responsePages = & $transport -Endpoint $Endpoint -Paginate
            return ,@($responsePages | ForEach-Object { $_ })
        }
        if ($Pages) {
            $result = & $transport -Endpoint $Endpoint -Paginate
            foreach ($page in $result) { $page }
            return
        }
        return & $transport -Endpoint $Endpoint
    }.GetNewClosure()
}

function Get-TriageIssueCollection {
    param($Policy, [string] $Label, [object[]] $KnownIssues, [scriptblock] $Api)
    $issues = & $Api -Endpoint "repos/$($Policy.repository)/issues?state=all&labels=$Label&per_page=100" -Collection
    if ($issues -isnot [System.Collections.IList]) { throw [FormatException]::new('Incomplete issue inventory.') }
    $found = @{}
    foreach ($issue in $issues) {
        if ($issue.Contains('pull_request')) { continue }
        if ($found.ContainsKey([long]$issue.number) -and
            (Get-ScheduledDigest $found[[long]$issue.number]) -cne (Get-ScheduledDigest $issue)) {
            throw [FormatException]::new('Issue inventory changed across pages.')
        }
        $found[[long]$issue.number] = $issue
    }
    foreach ($number in $KnownIssues) {
        if ([string]$number -cnotmatch '^[1-9][0-9]*$') { throw [FormatException]::new('Invalid retained issue ID.') }
        $issue = & $Api -Endpoint "repos/$($Policy.repository)/issues/$number"
        if ($issue.number -ne $number -or $issue.Contains('pull_request')) {
            throw [FormatException]::new('Retained issue identity changed.')
        }
        $found[[long]$number] = $issue
    }
    return ,@($found.Values | Sort-Object number)
}

function Get-TriageCommentCollection {
    param($Policy, [long] $IssueNumber, [scriptblock] $Api)
    $comments = & $Api -Endpoint "repos/$($Policy.repository)/issues/$IssueNumber/comments?per_page=100" -Collection
    if ($comments -isnot [System.Collections.IList]) { throw [FormatException]::new('Incomplete comment inventory.') }
    $seen = @{}
    foreach ($comment in $comments) {
        if ($comment.id -le 0 -or ($seen.ContainsKey([long]$comment.id) -and
            $seen[[long]$comment.id] -cne (Get-ScheduledDigest $comment))) {
            throw [FormatException]::new('Comment inventory contains conflicting identities.')
        }
        $seen[[long]$comment.id] = Get-ScheduledDigest $comment
    }
    return ,@($comments)
}

function Get-ScheduledTriageDocumentCatalog {
    [CmdletBinding()]
    param($Policy, [long] $IssueNumber, [ValidateSet('triage', 'problem')][string] $Kind,
        [object[]] $Comments, [AllowNull()][hashtable] $Root)
    $owned = @($Comments | Where-Object { $_.user.login -ceq $Policy.worker_login } |
        ForEach-Object { @{ id = $_.id; body = [string]$_.body } })
    $restored = Invoke-ScheduledRecordTool -Package scheduled-run-record -Request @{
        op = 'restore_documents'; kind = $Kind; owner = "$($Policy.repository_id)/$IssueNumber"; comments = $owned
    }
    if ($restored.incomplete_revisions.Count -gt 0) {
        throw [FormatException]::new("Issue $IssueNumber has incomplete $Kind detail publication.")
    }
    if ($null -eq $Root) {
        if ($restored.revisions.Count -gt 0) {
            throw [FormatException]::new("Issue $IssueNumber has uncommitted $Kind detail; reconcile its writer.")
        }
    } elseif ($Root.repository_id -ne $Policy.repository_id -or $Root.issue_number -ne $IssueNumber -or
        $Root.role -cne 'triage' -or $Root.index_digest -cne $restored.index_digest) {
        throw [FormatException]::new("Issue $IssueNumber has a foreign or stale $Kind checkpoint.")
    }
    return $restored
}

function Assert-TriageRunProvenance {
    param($Policy, $Evidence, [scriptblock] $Api)
    $run = & $Api -Endpoint "repos/$($Policy.repository)/actions/runs/$($Evidence.run_id)/attempts/$($Evidence.attempt.run_attempt)"
    $workflow = & $Api -Endpoint "repos/$($Policy.repository)/actions/workflows/$($Evidence.workflow.id)"
    $names = @{
        '.github/workflows/full-deep-validation.yml' = 'Full deep validation'
        '.github/workflows/selected-deep-validation.yml' = 'Selected deep validation'
    }
    if (-not $names.ContainsKey($Evidence.workflow.path) -or
        $Evidence.workflow.name -cne $names[$Evidence.workflow.path] -or
        $run.repository.id -ne $Policy.repository_id -or $run.repository.full_name -cne $Policy.repository -or
        $run.id -ne $Evidence.run_id -or $run.run_attempt -ne $Evidence.attempt.run_attempt -or
        $run.workflow_id -ne $Evidence.workflow.id -or $run.path -cne $Evidence.workflow.path -or
        $run.name -cne $Evidence.workflow.name -or $workflow.id -ne $run.workflow_id -or
        $workflow.name -cne $run.name -or $workflow.path -cne $run.path -or
        $run.status -cne 'completed' -or $run.head_branch -cne 'main' -or
        $run.head_sha -cne $Evidence.attempt.controller_sha -or
        $run.run_number -ne $Evidence.attempt.run_number -or
        ($null -ne $Evidence.attempt.started_at -and $run.run_started_at -cne $Evidence.attempt.started_at) -or
        ($null -ne $Evidence.attempt.created_at -and $run.created_at -cne $Evidence.attempt.created_at)) {
        throw [FormatException]::new('Originating API metadata does not validate run evidence.')
    }
    $comparison = & $Api -Endpoint "repos/$($Policy.repository)/compare/$($run.head_sha)...main"
    if ($comparison.status -cnotin @('ahead', 'identical')) {
        throw [FormatException]::new('Run controller is not on main ancestry.')
    }
    return $run
}

function Get-ScheduledTriageInbox {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][hashtable] $Policy,
        [AllowNull()][hashtable] $State,
        [scriptblock] $Api = ${function:Invoke-ScheduledTriageRead}
    )
    $user = & $Api -Endpoint user
    $repository = & $Api -Endpoint "repos/$($Policy.repository)"
    if ($user.login -cne $Policy.worker_login -or $repository.id -ne $Policy.repository_id -or
        $repository.full_name -cne $Policy.repository) {
        throw [FormatException]::new('Triage repository/account identity differs from reviewed policy.')
    }
    $knownRuns = @()
    $knownProblems = @()
    if ($null -ne $State -and $State.ContainsKey('triage')) {
        $knownRuns = @($State.triage.known_run_issues) + @($State.triage.analyses.Values |
            ForEach-Object { $_.revision.issue_number })
        $knownProblems = @($State.triage.known_problem_issues)
        if ($null -ne $State.triage.profile -and $State.triage.profile.user_id -ne $user.id) {
            throw [FormatException]::new('Triage numeric account identity changed.')
        }
    }
    $issues = Get-TriageIssueCollection $Policy scheduled-run-failure $knownRuns $Api
    $runs = @{}
    $runIdentities = @{}
    $pending = [Collections.Generic.List[object]]::new()
    foreach ($issue in $issues) {
        if ($issue.user.login -cne $Policy.reporter_login) {
            throw [FormatException]::new("Run issue $($issue.number) is not reporter-owned.")
        }
        $root = Read-ScheduledRecord -Text ([string]$issue.body) -Kind run
        $checkpoint = Read-ScheduledRecord -Text ([string]$issue.body) -Kind run-publication
        if ($root.repository_id -ne $Policy.repository_id) { throw [FormatException]::new('Foreign run root.') }
        $runKey = "$($root.repository_id)/$($root.workflow_id)/$($root.run_id)"
        if ($runIdentities.ContainsKey($runKey)) { throw [FormatException]::new('Duplicate canonical run issues.') }
        $runIdentities[$runKey] = $issue.number
        $comments = Get-TriageCommentCollection $Policy $issue.number $Api
        $record = Invoke-ScheduledRecordTool -Package scheduled-run-record -Request @{
            op = 'restore'; identity = @{
                repository_id = $root.repository_id; workflow_id = $root.workflow_id; run_id = $root.run_id
            }; comments = @($comments | Where-Object { $_.user.login -ceq $Policy.reporter_login } |
                ForEach-Object { @{ id = $_.id; body = [string]$_.body } })
        }
        $rendered = Invoke-ScheduledRecordTool -Package scheduled-run-record -Request @{ op = 'render'; record = $record.record }
        if ($checkpoint.index_digest -cne $rendered.index_digest -or $record.incomplete_revisions.Count -gt 0) {
            throw [FormatException]::new("Run issue $($issue.number) has uncommitted or missing reporter evidence.")
        }
        $roots = @($comments | Where-Object {
            $_.user.login -ceq $Policy.worker_login -and ([string]$_.body).Contains('<!-- scheduled-triage:v1 ')
        })
        if ($roots.Count -gt 1) { throw [FormatException]::new('Ambiguous triage comment ownership.') }
        $triageRoot = if ($roots.Count -eq 1) { Read-ScheduledRecord $roots[0].body triage } else { $null }
        if ($null -ne $triageRoot -and ($triageRoot.run_id -ne $root.run_id -or
            $triageRoot.workflow_id -ne $root.workflow_id)) {
            throw [FormatException]::new('Triage root belongs to another workflow run.')
        }
        $details = Get-ScheduledTriageDocumentCatalog $Policy $issue.number triage $comments $triageRoot
        $analyses = @($details.revisions | ForEach-Object { $_.document })
        foreach ($entry in $analyses) {
            if ($entry.ContainsKey('problem_links')) {
                $knownProblems += @($entry.problem_links.Values | ForEach-Object { $_.issue_number })
            }
        }
        foreach ($revision in $record.record.revisions) {
            $runObservation = Assert-TriageRunProvenance $Policy $revision.evidence $Api
            if (-not $revision.should_report) { continue }
            $basis = Get-ScheduledTriageEvidenceBasis $Policy $record.record $revision $Api $runObservation
            $matching = @($analyses | Where-Object {
                $_.analysis.revision.digest -ceq $revision.digest -and
                $_.analysis.revision.run_attempt -eq $revision.evidence.attempt.run_attempt
            } | Sort-Object { $_.analysis.checkpoint } -Descending)
            $complete = $false
            if ($matching.Count -gt 0 -and $matching[0].status -ceq 'complete') {
                $entry = $matching[0]
                Assert-ScheduledTriageEvidenceBasis $entry.basis $basis $record.record
                $null = Invoke-ScheduledRecordTool -Package scheduled-triage-record -Request @{
                    op = 'validate_analysis'; evidence = $revision.evidence
                    analysis = $entry.analysis; index = $entry.index; basis = $entry.basis
                }
                if ($entry.analysis.status -cne 'complete' -or $entry.analysis.revision.issue_number -ne $issue.number -or
                    $entry.problem_links.Count -ne $entry.analysis.problems.Count -or
                    $entry.publication_complete -ne $true) {
                    throw [FormatException]::new('Triage completion lacks reconciled problem publication.')
                }
                $complete = $true
            }
            if (-not $complete) {
                $pending.Add(@{
                    repository_id = $Policy.repository_id; workflow_id = $root.workflow_id; run_id = $root.run_id
                    run_attempt = $revision.evidence.attempt.run_attempt; digest = $revision.digest
                    issue_number = $issue.number; started_at = $revision.evidence.attempt.started_at
                    evidence = $revision.evidence; basis = $basis
                })
            }
        }
        $runs[[string]$issue.number] = @{
            issue = $issue; comments = $comments; record = $record.record; index_digest = $rendered.index_digest
            triage_root = $triageRoot; triage_comment_id = if ($roots.Count -eq 1) { $roots[0].id } else { $null }
            details = $details
        }
    }
    $problemIssues = Get-TriageIssueCollection $Policy scheduled-finding $knownProblems $Api
    $problems = @{}
    $entries = @(
        foreach ($issue in $problemIssues) {
            $comments = Get-TriageCommentCollection $Policy $issue.number $Api
            $problem = $null
            $legacy = $null
            $details = $null
            if (([string]$issue.body).Contains('<!-- scheduled-problem:v1 ')) {
                if ($issue.user.login -cnotin @($Policy.worker_login, $Policy.reporter_login)) {
                    throw [FormatException]::new('Problem issue author is not an approved record owner.')
                }
                $root = Read-ScheduledRecord $issue.body problem
                $details = Get-ScheduledTriageDocumentCatalog $Policy $issue.number problem $comments $root
                $current = @($details.revisions | Where-Object { $_.digest -ceq $root.current_digest })
                if ($current.Count -ne 1) { throw [FormatException]::new('Problem current record is missing.') }
                $problem = $current[0].document
                if ($problem.repository_id -ne $Policy.repository_id -or $problem.issue_number -ne $issue.number) {
                    throw [FormatException]::new('Canonical problem identity differs from its issue.')
                }
            }
            if (([string]$issue.body).Contains('<!-- scheduled-reporter:v1 ')) {
                $legacy = Read-ScheduledRecord $issue.body reporter
                $run = & $Api -Endpoint "repos/$($Policy.repository)/actions/runs/$($legacy.observation.run_id)/attempts/$($legacy.observation.run_attempt)"
                $legacy = ConvertTo-ScheduledIncident -Issue $issue -Comments $comments -Repository $Policy.repository `
                    -RepositoryId $Policy.repository_id -ReporterLogin $Policy.reporter_login `
                    -WorkerLogin $Policy.worker_login -Run $run
            }
            if ($null -eq $problem -and $null -eq $legacy) {
                throw [FormatException]::new("Problem index cannot interpret owned evidence for issue $($issue.number).")
            }
            $generation = if ($null -ne $problem) { $problem.generation } else { $legacy.generation }
            $scopeRevision = if ($null -ne $problem) { $problem.scope_revision } else { 1 }
            $summary = if ($null -ne $problem) { $problem.diagnosis } else { @{
                summary = $legacy.evidence.summary; status = $legacy.status
                package = $legacy.package; check_id = $legacy.check_id; evidence = $legacy.evidence
            } }
            $full = @{ issue = $issue; comments = $comments; problem = $problem; legacy = $legacy; details = $details }
            $recordDigest = Get-ScheduledDigest $full
            $problems[[string]$issue.number] = @{ record = $full; full_read_digest = $recordDigest }
            @{
                issue_number = $issue.number; generation = $generation; scope_revision = $scopeRevision
                record_digest = $recordDigest; summary = $summary; full_read_digest = $null
            }
        }
    )
    foreach ($run in $runs.Values) {
        foreach ($entry in @($run.details.revisions | ForEach-Object { $_.document } |
                Where-Object { $_.status -ceq 'complete' })) {
            foreach ($link in $entry.problem_links.Values) {
                if (-not $problems.ContainsKey([string]$link.issue_number)) {
                    throw [FormatException]::new('Completed analysis references an unavailable problem.')
                }
                $record = $problems[[string]$link.issue_number].record
                if ($null -eq $record.details) { throw [FormatException]::new('Problem update was not durably published.') }
                $publications = @($record.details.revisions | Where-Object { $_.digest -ceq $link.problem_digest })
                if ($publications.Count -ne 1 -or @($publications[0].document.evidence | Where-Object {
                    $_.operation_id -ceq $link.operation_id -and
                    (Get-ScheduledDigest $_.revision) -ceq (Get-ScheduledDigest $entry.analysis.revision)
                }).Count -ne 1) {
                    throw [FormatException]::new('Problem evidence does not confirm the claimed analysis publication.')
                }
            }
        }
    }
    $indexDigest = Get-ScheduledDigest $entries
    return @{
        schema_version = 1; repository = $Policy.repository; repository_id = $Policy.repository_id
        successful_scan = $true; pending = $pending.ToArray(); backlog_count = $pending.Count
        oldest_pending_at = if ($pending.Count -gt 0) {
            @($pending | Sort-Object started_at, run_id, run_attempt)[0].started_at
        } else { $null }
        runs = $runs; problems = $problems
        index = @{ digest = $indexDigest; complete = $true; entries = $entries }
    }
}

function Get-ScheduledTriageProblem {
    [CmdletBinding()]
    param([Parameter(Mandatory)][hashtable] $Snapshot, [Parameter(Mandatory)][long] $IssueNumber)
    if (-not $Snapshot.problems.ContainsKey([string]$IssueNumber)) { throw 'Problem is not in the complete index.' }
    return $Snapshot.problems[[string]$IssueNumber]
}

Export-ModuleMember -Function Invoke-ScheduledTriageRead, Get-ScheduledTriageInbox,
Get-ScheduledTriageProblem, Get-ScheduledTriageDocumentCatalog, Get-TriageReadAdapter
