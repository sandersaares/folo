#requires -Version 7
# Shared in-memory GitHub fixture for triage integration/state/publication tests. Responses
# cross real JSON serialization and the production Rust record codec; unexpected endpoints
# fail instead of contacting GitHub. Fixed time keeps recovery and budgets deterministic.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
Import-Module (Join-Path $PSScriptRoot '..\LocalState.psm1')
Import-Module (Join-Path $PSScriptRoot '..\LocalTriagePolicy.psm1')
Import-Module (Join-Path $PSScriptRoot '..\ScheduledContracts.psm1')
Import-Module (Join-Path $PSScriptRoot '..\ScheduledRecordTool.psm1')
Import-Module (Join-Path $PSScriptRoot '..\LocalTriageInbox.psm1')
Import-Module (Join-Path $PSScriptRoot '..\LocalTriagePublication.psm1')

function Copy-TriageFixtureValue {
    param($Value)
    return $Value | ConvertTo-Json -Depth 100 | ConvertFrom-Json -AsHashtable
}

function Initialize-TriageFixture {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string] $Root,
        [ValidateRange(1, 2)][int] $ProblemCount = 1,
        [ValidateSet('', 'cancelled', 'failure')][string] $EmptyWorkflowConclusion = '',
        [switch] $PartialWithSupport
    )
    $policy = Get-ScheduledPolicy
    $policy.repository = 'owner/repository'; $policy.repository_id = 123
    $policy.worker_login = 'worker'; $policy.reporter_login = 'reporter'
    $policy.local.expected_login = 'worker'
    $triagePolicy = Get-ScheduledTriagePolicy
    $triagePolicy.mode = 'triage'; $triagePolicy.enrolled_machine_id = 'executor'
    $triagePolicy.model = 'approved-model'; $triagePolicy.reasoning_effort = 'medium'
    $now = [DateTimeOffset]'2026-09-09T02:00:00Z'
    $null = Invoke-ScheduledLocalAction -StateRoot $Root -Policy $policy -ExecutorId executor -Login worker `
        -Now $now -Action initialize -Data @{ operator_approved = $true }
    $installed = @{
        automation_id = 'triage-entry'; project_id = 'project'; host_id = 'local'
        executor_id = 'executor'; login = 'worker'; user_id = 10; cadence_cron = $triagePolicy.cadence_cron
        timezone = 'UTC'; enabled = $true; policy_digest = Get-ScheduledTriagePolicyDigest $policy $triagePolicy
        controller_digest = Get-ScheduledTriageControllerDigest; prompt_digest = 'prompt'
        model = $triagePolicy.model; reasoning_effort = $triagePolicy.reasoning_effort
    }
    $null = Invoke-ScheduledLocalAction -StateRoot $Root -Policy $policy -TriagePolicy $triagePolicy `
        -ExecutorId executor -Login worker -Now $now -Action triage-register-profile `
        -Data @{ operator_approved = $true; profile = $installed }
    $null = Invoke-ScheduledLocalAction -StateRoot $Root -Policy $policy -TriagePolicy $triagePolicy `
        -ExecutorId executor -Login worker -Now $now -Action triage-set-mode `
        -Data @{ operator_approved = $true; mode = 'triage' }
    $state = Invoke-ScheduledLocalAction -StateRoot $Root -Policy $policy -TriagePolicy $triagePolicy `
        -ExecutorId executor -Login worker -Now $now -Action triage-acquire-scan -Data @{ session_id = 'session' }
    $evidence = @{
        repository = @{ id = 123; name = 'owner/repository' }
        workflow = @{ id = 456; name = 'Full deep validation'; path = '.github/workflows/full-deep-validation.yml' }
        run_id = 789
        attempt = @{
            run_attempt = 1; run_number = 42; created_at = '2026-09-09T01:00:00Z'
            started_at = '2026-09-09T01:00:01Z'; completed_at = '2026-09-09T01:00:02Z'
            workflow_conclusion = 'failure'; run_sha = 'a' * 40; controller_sha = 'a' * 40
            manifest = @{ source_sha = 'a' * 40 }; plan = @{ run = $true }
            results = @(@{ check_id = 'setup'; outcome = 'execution-error' }); evidence_gaps = @()
            jobs = @(@{
                id = 101; name = 'Setup'; status = 'completed'; conclusion = 'failure'
                steps = @(@{ number = 1; name = 'Download'; status = 'completed'; conclusion = 'failure' })
                log = @{ url = 'https://example.invalid/log'; excerpt = 'Dependency download failed'; bytes = 26 }
            })
        }
    }
    if ($ProblemCount -eq 2) {
        $evidence.attempt.jobs[0].log.excerpt = 'Dependency source rejected access. A separate archive was corrupt.'
        $evidence.attempt.jobs[0].log.bytes = $evidence.attempt.jobs[0].log.excerpt.Length
    }
    if ($EmptyWorkflowConclusion -ne '') {
        $evidence.attempt.jobs = @(); $evidence.attempt.results = @()
        $evidence.attempt.manifest = $null; $evidence.attempt.plan = $null
        $evidence.attempt.workflow_conclusion = $EmptyWorkflowConclusion
    }
    $support = $null
    if ($PartialWithSupport) {
        $support = Invoke-ScheduledRecordTool -Package scheduled-run-record -Request @{ op = 'prepare'; evidence = $evidence }
        $evidence = Copy-TriageFixtureValue $evidence
        $evidence.attempt.jobs = @()
        $evidence.attempt.evidence_gaps = @('The collector could not retrieve the entire job inventory')
    }
    $prepared = Invoke-ScheduledRecordTool -Package scheduled-run-record -Request @{ op = 'prepare'; evidence = $evidence }
    $store = @{
        issues = @{}; comments = @{}; labels = @{}; next_issue = 30; next_comment = 1000; next_label = 2000
        writes = [Collections.Generic.List[object]]::new()
        lose_create = $false; lose_comment = $false; lose_update = $false
        hide_create = $false; hidden_issue = $null
        fail_job_read = $false
    }
    $apiJobs = @(if ($null -ne $support) {
        $support.evidence.attempt.jobs | ForEach-Object { Copy-TriageFixtureValue $_ }
    } else { $evidence.attempt.jobs | ForEach-Object { Copy-TriageFixtureValue $_ } })
    foreach ($job in $apiJobs) { $job.run_id = 789; $job.run_attempt = 1; $job.head_sha = 'a' * 40 }
    $store.api_pages = @(@{ total_count = $apiJobs.Count; jobs = $apiJobs })
    $runComments = @(
        $allPages = @($prepared.pages)
        if ($null -ne $support) { $allPages += @($support.pages) }
        foreach ($page in $allPages) {
            $store.next_comment++
            @{ id = $store.next_comment; body = $page.body; user = @{ login = 'reporter' }
                issue_url = 'https://api.github.com/repos/owner/repository/issues/20' }
        }
    )
    $restored = Invoke-ScheduledRecordTool -Package scheduled-run-record -Request @{
        op = 'restore'; identity = $prepared.identity
        comments = @($runComments | ForEach-Object { @{ id = $_.id; body = $_.body } })
    }
    $rendered = Invoke-ScheduledRecordTool -Package scheduled-run-record -Request @{ op = 'render'; record = $restored.record }
    $store.issues[20L] = @{
        number = 20; title = 'Deep validation failed'; body = $rendered.body; state = 'open'
        user = @{ login = 'reporter' }; labels = @(@{ name = 'scheduled-run-failure' })
    }
    $store.comments[20L] = [Collections.Generic.List[object]]::new()
    foreach ($comment in $runComments) { $store.comments[20L].Add($comment) }
    $api = {
        param($Endpoint, $Method = 'GET', $Body, [switch] $Paginate, [switch] $Collection, [switch] $Pages)
        if ($Method -in @('POST', 'PATCH')) {
            $store.writes.Add(@{ method = $Method; endpoint = $Endpoint; body = (Copy-TriageFixtureValue $Body) })
        }
        if ($Endpoint -ceq 'user') { return @{ login = 'worker'; id = 10 } }
        if ($Endpoint -ceq 'repos/owner/repository') { return @{ id = 123; full_name = 'owner/repository' } }
        if ($Endpoint -ceq 'repos/owner/repository/actions/workflows/456') {
            return @{ id = 456; name = 'Full deep validation'; path = '.github/workflows/full-deep-validation.yml' }
        }
        if ($Endpoint -ceq 'repos/owner/repository/actions/runs/789/attempts/1/jobs?per_page=100') {
            if (-not $Pages -and -not $Paginate) { throw 'Job pagination must be requested.' }
            if ($store.fail_job_read) { throw [IO.IOException]::new('Incomplete job read.') }
            foreach ($page in $store.api_pages) { Copy-TriageFixtureValue $page }
            return
        }
        if ($Endpoint -match '^repos/owner/repository/actions/runs/789/attempts/(\d+)$') {
            return Copy-TriageFixtureValue @{
                repository = @{ id = 123; full_name = 'owner/repository' }
                id = 789; run_attempt = [int]$Matches[1]; run_number = 42; workflow_id = 456
                path = '.github/workflows/full-deep-validation.yml'; name = 'Full deep validation'
                head_sha = 'a' * 40; head_branch = 'main'; status = 'completed'
                conclusion = $evidence.attempt.workflow_conclusion
                created_at = '2026-09-09T01:00:00Z'; run_started_at = '2026-09-09T01:00:01Z'
            }
        }
        if ($Endpoint.StartsWith('repos/owner/repository/compare/')) { return @{ status = 'identical' } }
        if ($Endpoint -ceq 'repos/owner/repository/labels?per_page=100') { return ,@($store.labels.Values) }
        if ($Endpoint -ceq 'repos/owner/repository/labels' -and $Method -ceq 'POST') {
            $store.next_label++
            $label = Copy-TriageFixtureValue $Body
            $label.id = $store.next_label
            $store.labels[$label.name] = $label
            return Copy-TriageFixtureValue $label
        }
        if ($Endpoint.StartsWith('repos/owner/repository/issues?')) {
            if (-not $Paginate -and -not $Collection) { throw 'Fixture requires complete pagination.' }
            $labelName = [regex]::Match($Endpoint, 'labels=([^&]+)').Groups[1].Value
            return ,@(foreach ($issue in $store.issues.Values) {
                if (@($issue.labels | Where-Object { $_.name -ieq $labelName }).Count -gt 0) {
                    Copy-TriageFixtureValue $issue
                }
            })
        }
        if ($Endpoint -ceq 'repos/owner/repository/issues' -and $Method -ceq 'POST') {
            $store.next_issue++
            $number = [long]$store.next_issue
            $issue = @{
                number = $number; title = $Body.title; body = $Body.body; state = 'open'
                user = @{ login = 'worker' }; labels = @($Body.labels | ForEach-Object { @{ name = $_ } })
            }
            $store.issues[$number] = $issue
            $store.comments[$number] = [Collections.Generic.List[object]]::new()
            if ($store.hide_create) {
                $store.hidden_issue = $issue; $store.issues.Remove($number)
                throw [IO.IOException]::new('Unobservable issue create.')
            }
            if ($store.lose_create) { $store.lose_create = $false; throw [IO.IOException]::new('Lost issue response.') }
            return Copy-TriageFixtureValue $issue
        }
        if ($Endpoint -match '^repos/owner/repository/issues/(\d+)/comments(?:\?per_page=100)?$') {
            $number = [long]$Matches[1]
            if ($Method -ceq 'GET') {
                if (-not $Paginate -and -not $Collection) { throw 'Fixture requires complete comment pagination.' }
                return ,@($store.comments[$number] | ForEach-Object { Copy-TriageFixtureValue $_ })
            }
            $store.next_comment++
            $comment = @{
                id = $store.next_comment; body = $Body.body; user = @{ login = 'worker' }
                issue_url = "https://api.github.com/repos/owner/repository/issues/$number"
            }
            $store.comments[$number].Add($comment)
            if ($store.lose_comment) { $store.lose_comment = $false; throw [IO.IOException]::new('Lost comment response.') }
            return Copy-TriageFixtureValue $comment
        }
        if ($Endpoint -match '^repos/owner/repository/issues/comments/(\d+)$') {
            $id = [long]$Matches[1]
            $comments = @($store.comments.Values | ForEach-Object { $_ } | Where-Object { $_.id -eq $id })
            if ($comments.Count -ne 1) { throw [IO.IOException]::new('Unknown comment ID.') }
            if ($Method -ceq 'PATCH') {
                $comments[0].body = $Body.body
                if ($store.lose_update) { $store.lose_update = $false; throw [IO.IOException]::new('Lost update response.') }
            }
            return Copy-TriageFixtureValue $comments[0]
        }
        if ($Endpoint -match '^repos/owner/repository/issues/(\d+)$') {
            $number = [long]$Matches[1]
            if (-not $store.issues.ContainsKey($number)) { throw [IO.IOException]::new('Unknown issue ID.') }
            if ($Method -ceq 'PATCH') {
                foreach ($key in $Body.Keys) {
                    if ($key -ceq 'labels') {
                        $store.issues[$number].labels = @($Body.labels | ForEach-Object { @{ name = $_ } })
                    } else { $store.issues[$number][$key] = $Body[$key] }
                }
                if ($store.lose_update) { $store.lose_update = $false; throw [IO.IOException]::new('Lost update response.') }
            }
            return Copy-TriageFixtureValue $store.issues[$number]
        }
        throw "Unexpected fixture endpoint: $Method $Endpoint"
    }.GetNewClosure()
    $revision = @{
        repository_id = 123; workflow_id = 456; run_id = 789; run_attempt = 1; digest = $prepared.digest; issue_number = 20
    }
    $state = Invoke-ScheduledLocalAction -StateRoot $Root -Policy $policy -TriagePolicy $triagePolicy `
        -ExecutorId executor -Login worker -Now $now -Action triage-claim -Data @{
            scan_token = $state.triage.scan.token; revision = $revision; session_id = 'session'; native_verified = $true
        }
    $analysis = $state.triage.analyses[$state.triage.active_analysis_id]
    $context = @{
        state_root = $Root; policy = $policy; triage_policy = $triagePolicy
        executor_id = 'executor'; login = 'worker'; now = $now
        analysis_id = $analysis.id; session_id = 'session'; claim_token = $analysis.claim_token
        dispatch_token = $analysis.dispatch.token; scan_token = $state.triage.scan.token
    }
    $snapshot = Get-ScheduledTriageInbox -Policy $policy -State $state -Api $api
    $null = Invoke-TriageTransaction $context triage-record-index-read @{
        index_digest = $snapshot.index.digest; issue_numbers = @()
    }
    $disposition = @{
        kind = 'infrastructure'; explanation = 'The dependency source rejected the download'
        citations = @('/attempt/jobs/0/log/excerpt'); problem_keys = @('download')
    }
    $diagnosis = @{
        title = 'Dependency download failure'; summary = 'Dependencies were unavailable during setup'
        cause = 'The dependency source rejected the download'; category = 'infrastructure'
        repair_disposition = 'operator-recovery'; repair_reason = 'Restore dependency source access'
        citations = @('/attempt/jobs/0/log/excerpt')
        scope = @(@{ operation = 'dependency download'; package = $null; check_id = $null; platform = 'linux'
                replay = $null; citations = @('/attempt/jobs/0/steps/0') })
    }
    $proposal = @{
        schema_version = 1; analysis_id = $analysis.id; checkpoint = 1; revision = $revision
        status = 'complete'; considered_issues = @(); index_digest = $snapshot.index.digest; reason = ''
        jobs = @(@{ job_id = 101; disposition = $disposition; steps = @(@{ number = 1; disposition = $disposition }) })
        results = @(@{ index = 0; disposition = $disposition }); gaps = @()
        problems = @(@{ key = 'download'; diagnosis = $diagnosis
                matching = @{ kind = 'new'; reason = 'The complete index has no matching problem'; closest_candidates = @() } })
    }
    if ($ProblemCount -eq 2) {
        $other = Copy-TriageFixtureValue $diagnosis
        $other.title = 'Corrupt dependency archive'; $other.summary = 'A separate downloaded archive was corrupt'
        $other.cause = 'The archive contents failed integrity validation'; $other.repair_reason = 'Restore the archive'
        $proposal.problems += @{ key = 'archive'; diagnosis = $other
            matching = @{ kind = 'new'; reason = 'The archive defect is independent of source access'; closest_candidates = @() } }
        $disposition.problem_keys = @('download', 'archive')
    }
    if ($EmptyWorkflowConclusion -ne '') {
        $workflowDisposition = @{
            kind = if ($EmptyWorkflowConclusion -ceq 'cancelled') { 'cancelled' } else { 'blocked' }
            explanation = 'The workflow ended before creating jobs; no checker or source defect is inferred.'
            citations = @('/api_evidence/workflow_conclusion', '/api_evidence/jobs'); problem_keys = @()
        }
        $proposal.jobs = @(); $proposal.results = @(); $proposal.problems = @()
        $proposal.workflow = $workflowDisposition
        $proposal.gaps = @($prepared.evidence.attempt.evidence_gaps | ForEach-Object {
            @{ gap = $_; disposition = $workflowDisposition }
        })
    }
    if ($PartialWithSupport) {
        $citation = '/supporting_revisions/0/evidence/attempt/jobs/0/log/excerpt'
        $disposition.citations = @($citation)
        $diagnosis.citations = @($citation)
        $diagnosis.scope[0].citations = @($citation)
        $proposal.results += @{ index = 0; source_digest = $support.digest; disposition = $disposition }
        $explained = Copy-TriageFixtureValue $disposition
        $explained.explanation = 'The complete exact-attempt API inventory and fuller committed revision account for the original collection gap.'
        $proposal.gaps = @($prepared.evidence.attempt.evidence_gaps | ForEach-Object {
            @{ gap = $_; disposition = $explained }
        })
        $proposal.support_dispositions = @{ $support.digest = $explained }
    }
    $claimed = @($snapshot.pending | Where-Object { $_.digest -ceq $prepared.digest })[0]
    $null = Invoke-TriageTransaction $context triage-checkpoint @{
        checkpoint = @{
            analysis = $proposal; index = $snapshot.index; evidence = $prepared.evidence
            basis = $claimed.basis
        }
    }
    return @{
        context = $context; api = $api; store = $store; snapshot = $snapshot
        proposal = $proposal; evidence = $prepared.evidence; support = $support
    }
}

Export-ModuleMember -Function Initialize-TriageFixture, Copy-TriageFixtureValue
