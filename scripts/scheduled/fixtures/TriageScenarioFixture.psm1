#requires -Version 7
# Extends the strict triage fixture with reporter deliveries and subsequent native analyses.
# Lifecycle tests use real record serialization and publication rather than invented receipts.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
Import-Module (Join-Path $PSScriptRoot 'TriageFixture.psm1')
Import-Module (Join-Path $PSScriptRoot '..\LocalTriageProblem.psm1')
Import-Module (Join-Path $PSScriptRoot '..\LocalTriageCompletion.psm1')
Import-Module (Join-Path $PSScriptRoot '..\LocalTriageView.psm1')
Import-Module (Join-Path $PSScriptRoot '..\LocalTriageInbox.psm1')
Import-Module (Join-Path $PSScriptRoot '..\LocalTriagePublication.psm1')
Import-Module (Join-Path $PSScriptRoot '..\ScheduledRecordTool.psm1')
Import-Module (Join-Path $PSScriptRoot '..\ScheduledContracts.psm1')

function Add-TriageFixtureRevision {
    param($Fixture, $Evidence, [long] $IssueNumber = 20)
    $prepared = Invoke-ScheduledRecordTool -Package scheduled-run-record -Request @{ op = 'prepare'; evidence = $Evidence }
    $store = $Fixture.store
    if (-not $store.comments.ContainsKey($IssueNumber)) {
        $store.comments[$IssueNumber] = [Collections.Generic.List[object]]::new()
        $store.issues[$IssueNumber] = @{
            number = $IssueNumber; title = 'Deep validation failed'; body = ''; state = 'open'
            user = @{ login = 'reporter' }; labels = @(@{ name = 'scheduled-run-failure' })
        }
    }
    foreach ($page in $prepared.pages) {
        if (@($store.comments[$IssueNumber] | Where-Object { $_.body -ceq $page.body }).Count -gt 0) { continue }
        $store.next_comment++
        $store.comments[$IssueNumber].Add(@{
            id = $store.next_comment; body = $page.body; user = @{ login = 'reporter' }
            issue_url = "https://api.github.com/repos/owner/repository/issues/$IssueNumber"
        })
    }
    $record = Invoke-ScheduledRecordTool -Package scheduled-run-record -Request @{
        op = 'restore'; identity = $prepared.identity
        comments = @($store.comments[$IssueNumber] | Where-Object { $_.user.login -ceq 'reporter' } |
            ForEach-Object { @{ id = $_.id; body = $_.body } })
    }
    $rendered = Invoke-ScheduledRecordTool -Package scheduled-run-record -Request @{ op = 'render'; record = $record.record }
    $store.issues[$IssueNumber].body = $rendered.body
    $attempt = $prepared.evidence.attempt
    $runKey = "$($Evidence.run_id)/$($attempt.run_attempt)"
    $store.extra_runs[$runKey] = @{
        repository = @{ id = 123; full_name = 'owner/repository' }; id = $Evidence.run_id
        run_attempt = $attempt.run_attempt; run_number = $attempt.run_number; workflow_id = 456
        path = '.github/workflows/full-deep-validation.yml'; name = 'Full deep validation'
        head_sha = $attempt.controller_sha; head_branch = 'main'; status = 'completed'
        conclusion = $attempt.workflow_conclusion; created_at = $attempt.created_at; run_started_at = $attempt.started_at
    }
    $jobs = @($attempt.jobs | ForEach-Object {
        $job = Copy-TriageFixtureValue $_
        $job.run_id = $Evidence.run_id; $job.run_attempt = $attempt.run_attempt; $job.head_sha = $attempt.controller_sha
        $job
    })
    $store.extra_jobs[$runKey] = @(@{ total_count = $jobs.Count; jobs = $jobs })
    return $prepared
}

function Publish-TriageFixtureProblem {
    param($Fixture, [string] $Key)
    $result = Invoke-ScheduledTriageProblemPreparation $Fixture.context $Fixture.snapshot $Key $Fixture.api
    if ($result.action -ceq 'native-create-issue') {
        $created = & $Fixture.api -Endpoint 'repos/owner/repository/issues' -Method POST -Body $result.payload
        $null = Invoke-TriageTransaction $Fixture.context triage-observe-operation @{
            operation_key = $result.operation_key; target_id = $created.number
        }
        $result = Invoke-ScheduledTriageProblemPreparation $Fixture.context $Fixture.snapshot $Key $Fixture.api
    }
    if ($result.action -cne 'prepared') { throw 'Fixture problem requires reanalysis.' }
    return Publish-ScheduledTriageProblem $Fixture.context $Key $Fixture.api
}

function Invoke-TriageFixtureNextAnalysis {
    param($Fixture, [long] $RunId, [int] $Attempt = 1)
    $context = $Fixture.context
    $null = Invoke-TriageTransaction $context triage-complete-dispatch @{ reason = 'Publication is complete' }
    $null = Complete-ScheduledTriageRetirement $context @{
        scan_token = $context.scan_token; native_idle_verified = $true
    } $Fixture.api
    $null = Invoke-TriageTransaction $context triage-release-scan @{ scan_token = $context.scan_token }
    $context.session_id = 'next-session'
    $state = Invoke-TriageTransaction $context triage-acquire-scan @{ session_id = $context.session_id }
    $context.scan_token = $state.triage.scan.token
    $snapshot = Get-ScheduledTriageInbox $context.policy $state $Fixture.api
    $pending = @($snapshot.pending | Where-Object { $_.run_id -eq $RunId -and $_.run_attempt -eq $Attempt })
    if ($pending.Count -ne 1) { throw 'Fixture needs one exact pending revision.' }
    $revision = @{}
    foreach ($field in @('repository_id', 'workflow_id', 'run_id', 'run_attempt', 'issue_number', 'digest')) {
        $revision[$field] = $pending[0][$field]
    }
    $null = Save-ScheduledTriageSnapshot $context $snapshot @{
        analysis_id = $null; session_id = $context.session_id; scan_token = $context.scan_token
    }
    $state = Invoke-TriageTransaction $context triage-claim @{
        scan_token = $context.scan_token; revision = $revision; native_verified = $true
    }
    $analysis = $state.triage.analyses[$state.triage.active_analysis_id]
    $context.analysis_id = $analysis.id; $context.claim_token = $analysis.claim_token
    $context.dispatch_token = $analysis.dispatch.token
    $Fixture.snapshot = $snapshot; $Fixture.evidence = $pending[0].evidence
    $Fixture.proposal.analysis_id = $analysis.id; $Fixture.proposal.revision = $revision
    $Fixture.proposal.checkpoint = 1
}

function Invoke-TriageFixtureCheckpoint {
    param($Fixture)
    $context = $Fixture.context
    $state = Invoke-TriageTransaction $context read
    $snapshot = Get-ScheduledTriageInbox $context.policy $state $Fixture.api
    $null = Invoke-TriageTransaction $context triage-record-index-read @{
        index_digest = $snapshot.index.digest; issue_numbers = @($snapshot.index.entries | ForEach-Object { $_.issue_number })
    }
    foreach ($entry in $snapshot.index.entries) {
        $offset = 0
        do {
            $page = Get-ScheduledTriageProblemPage $snapshot.problems[[string]$entry.issue_number] $offset
            if ($page.end_offset -le $offset) { throw 'Fixture candidate read did not progress.' }
            $null = Invoke-TriageTransaction $context triage-record-problem-page @{
                issue_number = $entry.issue_number; full_read_digest = $page.full_read_digest
                offset = $offset; end_offset = $page.end_offset; total_length = $page.total_length
            }
            $offset = $page.next_offset
        } while ($null -ne $offset)
        $entry.full_read_digest = $entry.record_digest
    }
    $proposal = $Fixture.proposal
    $proposal.index_digest = $snapshot.index.digest
    $proposal.considered_issues = @($snapshot.index.entries | ForEach-Object { $_.issue_number })
    foreach ($problem in $proposal.problems) {
        if ($problem.matching.kind -ceq 'existing') {
            $entry = @($snapshot.index.entries | Where-Object { $_.issue_number -eq $problem.matching.issue_number })[0]
            $problem.matching.expected_generation = $entry.generation
            $problem.matching.expected_scope_revision = $entry.scope_revision
            $problem.matching.record_digest = $entry.record_digest
            $problem.matching.full_read_digest = $entry.record_digest
        } else {
            $problem.matching.closest_candidates = @($snapshot.index.entries | ForEach-Object {
                @{ issue_number = $_.issue_number; full_read_digest = $_.record_digest
                    reason = 'The existing diagnosis is independent of this observed cause.' }
            })
        }
    }
    $pending = @($snapshot.pending | Where-Object { $_.digest -ceq $proposal.revision.digest })[0]
    $null = Invoke-TriageTransaction $context triage-checkpoint @{
        checkpoint = @{ analysis = $proposal; index = $snapshot.index; evidence = $Fixture.evidence; basis = $pending.basis }
    }
    $Fixture.snapshot = $snapshot
}

function Invoke-TriageFixtureDocument {
    param($Fixture, [long] $IssueNumber, [string] $Kind, $Document)
    $store = $Fixture.store
    $target = if ($Kind -ceq 'problem') { $store.issues[$IssueNumber] } else {
        @($store.comments[$IssueNumber] | Where-Object { $_.body.Contains('<!-- scheduled-triage:v1 ') })[0]
    }
    $root = Read-ScheduledRecord $target.body $Kind
    $before = Get-TriageOwnedBlock $target.body $Kind
    $prepared = Invoke-ScheduledRecordTool -Package scheduled-run-record -Request @{
        op = 'prepare_document'; kind = $Kind; owner = "123/$IssueNumber"; document = $Document
    }
    foreach ($page in $prepared.pages) {
        $store.next_comment++
        $store.comments[$IssueNumber].Add(@{
            id = $store.next_comment; body = $page.body; user = @{ login = 'worker' }
            issue_url = "https://api.github.com/repos/owner/repository/issues/$IssueNumber"
        })
    }
    $catalog = Invoke-ScheduledRecordTool -Package scheduled-run-record -Request @{
        op = 'restore_documents'; kind = $Kind; owner = "123/$IssueNumber"
        comments = @($store.comments[$IssueNumber] | Where-Object { $_.user.login -ceq 'worker' } |
            ForEach-Object { @{ id = $_.id; body = $_.body } })
    }
    $root.current_digest = $prepared.digest; $root.index_digest = $catalog.index_digest
    $target.body = $target.body.Replace($before.Value, (ConvertTo-TriageOwnedBlock $Kind (Write-ScheduledRecord $root $Kind)))
}

Export-ModuleMember -Function Add-TriageFixtureRevision, Publish-TriageFixtureProblem,
Invoke-TriageFixtureNextAnalysis, Invoke-TriageFixtureCheckpoint, Invoke-TriageFixtureDocument
