#requires -Version 7
# JSON entry point used by the native scheduled-triage skill. Cache files live beside enrolled
# state, never in source, and are content-addressed observations rather than executable inputs.
# Native App calls remain explicit skill steps between short durable transactions.
# Ref: ../../docs/scheduled-triage.md.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
Import-Module (Join-Path $PSScriptRoot 'ScheduledContracts.psm1')
Import-Module (Join-Path $PSScriptRoot 'LocalTriagePolicy.psm1')
Import-Module (Join-Path $PSScriptRoot 'LocalTriageInbox.psm1')
Import-Module (Join-Path $PSScriptRoot 'LocalTriagePublication.psm1')
Import-Module (Join-Path $PSScriptRoot 'LocalTriageProblem.psm1')
Import-Module (Join-Path $PSScriptRoot 'LocalTriageCompletion.psm1')
Import-Module (Join-Path $PSScriptRoot 'ScheduledGitHub.psm1')
Import-Module (Join-Path $PSScriptRoot 'LocalState.psm1')
Import-Module (Join-Path $PSScriptRoot 'LocalHealth.psm1')
Import-Module (Join-Path $PSScriptRoot 'LocalTriageView.psm1')
Import-Module (Join-Path $PSScriptRoot 'LocalTriageCache.psm1')

function Read-TriageSnapshot {
    param([string] $StateRoot, [string] $Id, $Analysis)
    $checkpointId = if ($null -ne $Analysis.checkpoint -and $Analysis.checkpoint.ContainsKey('snapshot_id')) {
        $Analysis.checkpoint.snapshot_id
    } else { $null }
    if ($Id -cne $Analysis.working_snapshot_id -and $Id -cne $checkpointId) {
        throw 'Snapshot is not pinned by the accepted analysis.'
    }
    return Read-ScheduledTriageSnapshot $StateRoot $Id
}

function Get-TriageSummary {
    param($Snapshot)
    return @{
        successful_scan = $Snapshot.successful_scan; backlog_count = $Snapshot.backlog_count
        oldest_pending_at = $Snapshot.oldest_pending_at; index_digest = $Snapshot.index.digest
        problem_count = $Snapshot.index.entries.Count
        pending = @($Snapshot.pending | ForEach-Object {
            @{
                repository_id = $_.repository_id; workflow_id = $_.workflow_id; run_id = $_.run_id
                run_attempt = $_.run_attempt; digest = $_.digest; issue_number = $_.issue_number
                started_at = $_.started_at; job_count = $_.evidence.attempt.jobs.Count
            }
        })
    }
}

function Invoke-ScheduledTriageRequest {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string] $RequestPath,
        [DateTimeOffset] $Now = [DateTimeOffset]::UtcNow
    )
    $request = Get-Content -LiteralPath $RequestPath -Raw | ConvertFrom-Json -AsHashtable
    foreach ($field in @('action', 'executor_id', 'data')) {
        if (-not $request.ContainsKey($field)) { throw "Missing triage request field: $field" }
    }
    $policy = Get-ScheduledPolicy
    $triagePolicy = Get-ScheduledTriagePolicy
    $root = Get-ScheduledStateRoot -RepositoryId $policy.repository_id
    # Reject corrupt retained authority before even an identity lookup. This local read
    # uses reviewed identity; the subsequent API lookup still verifies the selected account.
    $readContext = @{
        state_root = $root; policy = $policy; triage_policy = $triagePolicy
        executor_id = $request.executor_id; login = $policy.worker_login; now = $Now
        analysis_id = $null; session_id = $null; claim_token = $null; dispatch_token = $null
    }
    $state = if (Test-Path -LiteralPath $root) {
        Get-ScheduledTriageValidatedState $readContext
    } else { $null }
    $user = Invoke-ScheduledTriageRead -Endpoint user
    if ($user.login -cne $policy.worker_login) { throw 'Selected account does not match reviewed triage identity.' }
    if ($request.action -ceq 'scan') {
        $hasOwner = $request.data.ContainsKey('scan_token') -or
            ($request.data.ContainsKey('analysis_id') -and $null -ne $request.data.analysis_id)
        if ($hasOwner) {
            if ($null -eq $state -or -not $state.ContainsKey('triage')) { throw 'A cached scan requires registered ownership.' }
            $null = Invoke-TriageTransaction $readContext triage-authorize-snapshot $request.data
        }
        $snapshot = Get-ScheduledTriageInbox -Policy $policy -State $state
        $summary = Get-TriageSummary $snapshot
        $summary.snapshot_id = $null
        if ($hasOwner) {
            $summary.snapshot_id = Save-ScheduledTriageSnapshot $readContext $snapshot $request.data
        }
        $summary.mode = $triagePolicy.mode
        $summary.registered = $null -ne $state -and $state.ContainsKey('triage')
        return $summary | ConvertTo-Json -Depth 100
    }
    if ($null -eq $state) { throw 'Explicit enrollment or state recovery is required; triage did not initialize state.' }
    if ($request.action -ceq 'state') {
        if (-not ([string]$request.data.action).StartsWith('triage-', [StringComparison]::Ordinal)) {
            throw 'The triage entry point cannot perform repair actions.'
        }
        if ($request.data.action -ceq 'triage-retire') {
            $retirementContext = $readContext.Clone()
            $retirementContext.analysis_id = $request.data.fields.analysis_id
            $retirementContext.session_id = $request.data.fields.session_id
            return (Complete-ScheduledTriageRetirement $retirementContext $request.data.fields {
                param($Endpoint, $Method = 'GET', $Body, [switch] $Paginate)
                Invoke-ScheduledGitHubApi -Endpoint $Endpoint -Method $Method -Body $Body -Paginate:$Paginate
            }) | ConvertTo-Json -Depth 100
        }
        $result = Invoke-ScheduledLocalAction -StateRoot $root -Policy $policy -TriagePolicy $triagePolicy `
            -ExecutorId $request.executor_id -Login $user.login -Now $Now `
            -Action $request.data.action -Data $request.data.fields
        return $result | ConvertTo-Json -Depth 100
    }
    if ($request.action -ceq 'health') {
        return (Sync-ScheduledRoleHealth -Context @{
            state_root = $root; policy = $policy; role = 'triage'; executor_id = $request.executor_id
            login = $user.login; now = $Now; scan_token = $request.data.scan_token
        }) | ConvertTo-Json -Depth 100
    }
    if ($request.action -ceq 'recovery') {
        return (Get-ScheduledTriageRecovery -Policy $policy -State $state -Api {
            param($Endpoint, $Method = 'GET', $Body, [switch] $Paginate)
            Invoke-ScheduledGitHubApi -Endpoint $Endpoint -Method $Method -Body $Body -Paginate:$Paginate
        }) | ConvertTo-Json -Depth 100
    }
    foreach ($field in @('analysis_id', 'session_id', 'claim_token', 'dispatch_token')) {
        if (-not $request.data.ContainsKey($field)) { throw "Missing registered triage identity: $field" }
    }
    $context = @{
        state_root = $root; policy = $policy; triage_policy = $triagePolicy
        executor_id = $request.executor_id; login = $user.login; now = $Now
        analysis_id = $request.data.analysis_id; session_id = $request.data.session_id
        claim_token = $request.data.claim_token; dispatch_token = $request.data.dispatch_token
    }
    $null = Invoke-TriageTransaction $context triage-authorize-publication
    $api = {
        param($Endpoint, $Method = 'GET', $Body, [switch] $Paginate)
        Invoke-ScheduledGitHubApi -Endpoint $Endpoint -Method $Method -Body $Body -Paginate:$Paginate
    }
    switch -CaseSensitive ($request.action) {
        'evidence' {
            $analysis = $state.triage.analyses[$context.analysis_id]
            $snapshot = Read-TriageSnapshot $root $request.data.snapshot_id $analysis
            $revisions = @($snapshot.pending | Where-Object {
                $_.digest -ceq $analysis.revision.digest -and $_.run_id -eq $analysis.revision.run_id -and
                $_.run_attempt -eq $analysis.revision.run_attempt
            })
            if ($revisions.Count -ne 1) { throw 'Snapshot does not contain the exact claimed revision.' }
            $offset = if ($request.data.ContainsKey('offset')) { [int]$request.data.offset } else { 0 }
            return (Get-ScheduledTriageJsonPage -Value @{
                evidence = $revisions[0].evidence; basis = $revisions[0].basis
            } -Offset $offset) | ConvertTo-Json -Depth 100
        }
        'index' {
            $snapshot = Read-TriageSnapshot $root $request.data.snapshot_id $state.triage.analyses[$context.analysis_id]
            $page = Get-ScheduledTriageIndexPage $snapshot ([int]$request.data.offset)
            $null = Invoke-TriageTransaction $context triage-record-index-read @{
                index_digest = $snapshot.index.digest
                issue_numbers = @($page.entries | ForEach-Object { $_.issue_number })
            }
            return $page | ConvertTo-Json -Depth 100
        }
        'problem' {
            $snapshot = Read-TriageSnapshot $root $request.data.snapshot_id $state.triage.analyses[$context.analysis_id]
            $problem = Get-ScheduledTriageProblem -Snapshot $snapshot -IssueNumber $request.data.issue_number
            $offset = if ($request.data.ContainsKey('offset')) { [int]$request.data.offset } else { 0 }
            $page = Get-ScheduledTriageProblemPage $problem $offset
            $null = Invoke-TriageTransaction $context triage-record-problem-page @{
                issue_number = $request.data.issue_number; full_read_digest = $problem.full_read_digest
                offset = $page.offset; end_offset = $page.end_offset; total_length = $page.total_length
            }
            return $page | ConvertTo-Json -Depth 100
        }
        'checkpoint' {
            $current = $state.triage.analyses[$context.analysis_id]
            $snapshot = Read-TriageSnapshot $root $request.data.snapshot_id $current
            $revisions = @($snapshot.pending | Where-Object {
                $_.digest -ceq $current.revision.digest -and $_.run_id -eq $current.revision.run_id -and
                $_.run_attempt -eq $current.revision.run_attempt
            })
            if ($revisions.Count -ne 1) { throw 'Checkpoint source is not the claimed snapshot revision.' }
            foreach ($entry in $snapshot.index.entries) {
                if ($current.reads.ContainsKey([string]$entry.issue_number) -and
                    $current.reads[[string]$entry.issue_number] -ceq $entry.record_digest) {
                    $entry.full_read_digest = $entry.record_digest
                }
            }
            $result = Invoke-TriageTransaction $context triage-checkpoint @{
                checkpoint = @{
                    analysis = $request.data.analysis; index = $snapshot.index
                    evidence = $revisions[0].evidence; snapshot_id = $request.data.snapshot_id
                    basis = $revisions[0].basis
                }
            }
            return $result.triage.analyses[$context.analysis_id] | ConvertTo-Json -Depth 100
        }
        'native-issue-result' {
            $result = Invoke-TriageTransaction $context triage-observe-operation @{
                operation_key = $request.data.operation_key; target_id = $request.data.issue_number
            }
            return $result.triage.analyses[$context.analysis_id] | ConvertTo-Json -Depth 100
        }
        'prepare-problem' {
            $current = $state.triage.analyses[$context.analysis_id]
            $snapshot = Read-TriageSnapshot $root $current.checkpoint.snapshot_id $current
            return (Invoke-ScheduledTriageProblemPreparation $context $snapshot $request.data.problem_key $api) |
                ConvertTo-Json -Depth 100
        }
        'publish-problem' {
            return (Publish-ScheduledTriageProblem $context $request.data.problem_key $api) |
                ConvertTo-Json -Depth 100
        }
        'finish' {
            return (Complete-ScheduledTriageAnalysis $context $api) | ConvertTo-Json -Depth 100
        }
        default { throw "Unsupported triage request action: $($request.action)" }
    }
}

Export-ModuleMember -Function Invoke-ScheduledTriageRequest
