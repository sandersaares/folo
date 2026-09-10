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
Import-Module (Join-Path $PSScriptRoot 'ScheduledRunGitHub.psm1')
Import-Module (Join-Path $PSScriptRoot 'ScheduledRecordTool.psm1')
Import-Module (Join-Path $PSScriptRoot 'LocalHealth.psm1')
Import-Module (Join-Path $PSScriptRoot 'LocalTriageView.psm1')

function Get-TriageSnapshotPath {
    param([string] $StateRoot, [ValidatePattern('^[0-9a-f]{64}$')][string] $Id)
    return Join-Path $StateRoot "triage-cache\$Id.json"
}

function Read-TriageSnapshot {
    param([string] $StateRoot, [string] $Id)
    $snapshot = Get-Content -LiteralPath (Get-TriageSnapshotPath $StateRoot $Id) -Raw | ConvertFrom-Json -AsHashtable
    if ((Get-ScheduledDigest $snapshot) -cne $Id) { throw 'Cached triage observation changed; repeat the complete scan.' }
    return $snapshot
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
    $user = Invoke-ScheduledTriageRead -Endpoint user
    if ($user.login -cne $policy.worker_login) { throw 'Selected account does not match reviewed triage identity.' }
    $state = if (Test-Path -LiteralPath $root) {
        Invoke-ScheduledLocalAction -StateRoot $root -Policy $policy -TriagePolicy $triagePolicy `
            -ExecutorId $request.executor_id -Login $user.login -Now $Now -Action read
    } else { $null }
    if ($request.action -ceq 'scan') {
        $snapshot = Get-ScheduledTriageInbox -Policy $policy -State $state
        $summary = Get-TriageSummary $snapshot
        $summary.snapshot_id = $null
        if ($null -ne $state -and $state.ContainsKey('triage')) {
            $id = Get-ScheduledDigest $snapshot
            $path = Get-TriageSnapshotPath $root $id
            $null = New-Item -ItemType Directory -Path (Split-Path -Parent $path) -Force
            Write-ScheduledRunJournal -Path $path -Record $snapshot
            $summary.snapshot_id = $id
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
            $snapshot = Read-TriageSnapshot $root $request.data.snapshot_id
            $analysis = $state.triage.analyses[$context.analysis_id]
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
            $snapshot = Read-TriageSnapshot $root $request.data.snapshot_id
            $page = Get-ScheduledTriageIndexPage $snapshot ([int]$request.data.offset)
            $null = Invoke-TriageTransaction $context triage-record-index-read @{
                index_digest = $snapshot.index.digest
                issue_numbers = @($page.entries | ForEach-Object { $_.issue_number })
            }
            return $page | ConvertTo-Json -Depth 100
        }
        'problem' {
            $snapshot = Read-TriageSnapshot $root $request.data.snapshot_id
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
            $snapshot = Read-TriageSnapshot $root $request.data.snapshot_id
            $current = $state.triage.analyses[$context.analysis_id]
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
            # Prepare the utility before entering the short transaction; the transaction validates
            # the same input again against its current ownership and checkpoint.
            $null = Invoke-ScheduledRecordTool -Package scheduled-triage-record -Request @{
                op = 'validate_analysis'; analysis = $request.data.analysis
                index = $snapshot.index; evidence = $revisions[0].evidence; basis = $revisions[0].basis
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
            $snapshot = Read-TriageSnapshot $root $current.checkpoint.snapshot_id
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
