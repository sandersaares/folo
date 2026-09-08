Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true

Import-Module (Join-Path $PSScriptRoot 'ScheduledContracts.psm1')

# Readiness is a current-head decision. The caller supplies complete API collections and
# canonical version evidence; issue prose and worker summaries cannot make checks pass.
function Get-ScheduledPullRequestDecision {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][System.Collections.IDictionary] $Attempt,
        [Parameter(Mandatory)][System.Collections.IDictionary] $PullRequest,
        [Parameter(Mandatory)][string] $MainSha,
        [Parameter(Mandatory)][bool] $ContainsMain,
        [Parameter(Mandatory)][bool] $NativeSessionIdle,
        [Parameter(Mandatory)][bool] $CollectionsComplete,
        [Parameter(Mandatory)][string] $CurrentLogin,
        [Parameter(Mandatory)][AllowNull()][System.Collections.IDictionary] $VersionEvidence,
        [Parameter(Mandatory)][AllowEmptyCollection()][object[]] $CheckRuns,
        [Parameter(Mandatory)][string[]] $RequiredChecks,
        [Parameter(Mandatory)][AllowEmptyCollection()][object[]] $ReviewInput
    )
    $result = @{ action = 'blocked'; reason = $null; evidence_key = $null; evidence = @() }
    $result.pending_human_responses = @(if ($Attempt.Contains('proposed_responses')) {
        $Attempt.proposed_responses | Where-Object { $_.status -ceq 'pending' }
    })
    $result.code_ready = $false
    if (-not $CollectionsComplete) { $result.reason = 'incomplete-api-evidence'; return $result }
    if ($PullRequest.number -ne $Attempt.pr_number -or
        $PullRequest.head.ref -cne $Attempt.branch -or
        $PullRequest.head.sha -cne $Attempt.head_sha) {
        $result.reason = 'human-or-unreconciled-head-change'; return $result
    }
    if ($Attempt.phase -cin @('resolved', 'closed-unmerged')) {
        $result.action = $Attempt.phase; return $result
    }
    if ($PullRequest.merged -eq $true -or $PullRequest.state -ceq 'closed') {
        if (-not $NativeSessionIdle -or $Attempt.dispatch.status -cne 'completed') {
            $result.action = 'waiting'; $result.reason = 'worker-owns-dispatch'; return $result
        }
        $result.action = if ($PullRequest.merged -eq $true) { 'verifying-main' } else { 'closed-unmerged' }
        return $result
    }
    if ($PullRequest.state -cne 'open') { $result.reason = 'unknown-pr-state'; return $result }
    if ($Attempt.phase -ceq 'blocked') { $result.reason = $Attempt.reason; return $result }
    if (-not $NativeSessionIdle -or $Attempt.dispatch.status -cne 'completed') {
        $result.action = 'waiting'; $result.reason = 'worker-owns-dispatch'; return $result
    }
    $events = [Collections.Generic.List[object]]::new()
    if (-not $ContainsMain) { $events.Add(@{ kind = 'base'; id = $MainSha; fingerprint = $MainSha }) }
    if ($PullRequest.mergeable -eq $false) {
        $events.Add(@{ kind = 'conflict'; id = $MainSha; fingerprint = $PullRequest.head.sha })
    }
    if ($null -eq $VersionEvidence -or $VersionEvidence.head_sha -cne $PullRequest.head.sha -or
        $VersionEvidence.base_sha -cne $MainSha -or -not $VersionEvidence.current -or
        -not $VersionEvidence.description_current) {
        $planDigest = if ($null -eq $VersionEvidence) { 'missing' } else { $VersionEvidence.plan_digest }
        $events.Add(@{ kind = 'version-plan'; id = $PullRequest.head.sha
            fingerprint = "$MainSha/$planDigest" })
    }
    $pending = $null -eq $PullRequest.mergeable
    if ($RequiredChecks.Count -eq 0) { $result.reason = 'missing-required-check-contract'; return $result }
    foreach ($name in $RequiredChecks) {
        $runs = @($CheckRuns | Where-Object {
            $_.name -ceq $name -and $_.head_sha -ceq $PullRequest.head.sha
        } | Sort-Object -Property id -Descending)
        if ($runs.Count -eq 0) {
            $pending = $true
            continue
        }
        $run = $runs[0]
        if ($run.status -cne 'completed') { $pending = $true; continue }
        if ($run.conclusion -cne 'success') {
            $events.Add(@{ kind = 'check'; id = [string]$run.id
                fingerprint = "$($run.head_sha)/$($run.conclusion)" })
        }
    }
    # ReviewInput contains normalized top-level comments, review summaries, and individual
    # thread comments. The namespace and payload digest detect edits without conflating IDs.
    $reviewKeys = @{}
    foreach ($inputRecord in $ReviewInput) {
        if ($inputRecord.kind -cnotin @('comment', 'review', 'thread') -or
            [string]::IsNullOrWhiteSpace([string]$inputRecord.id)) {
            throw 'Unknown or incomplete review input.'
        }
        $key = Get-ScheduledDigest -Value @{
            kind = $inputRecord.kind; id = $inputRecord.id; body = $inputRecord.body
            state = $inputRecord.state
        }
        $identity = "$($inputRecord.kind)/$($inputRecord.id)"
        if ($reviewKeys.ContainsKey($identity)) {
            if ($reviewKeys[$identity] -cne $key) { throw 'Review input changed across pages.' }
            continue
        }
        $reviewKeys[$identity] = $key
        if ($inputRecord.resolved -eq $true -or $key -cin $Attempt.handled_evidence) { continue }
        if ($inputRecord.state -cin @('APPROVED', 'DISMISSED') -or
            ([string]::IsNullOrWhiteSpace($inputRecord.body) -and
                $inputRecord.state -cne 'CHANGES_REQUESTED')) { continue }
        $mayReply = $inputRecord.author -ceq 'copilot-pull-request-reviewer[bot]' -or
            $inputRecord.body.StartsWith('[Copilot speaking]', [StringComparison]::Ordinal)
        $events.Add(@{ kind = $inputRecord.kind; id = [string]$inputRecord.id; fingerprint = $key
            may_reply = $mayReply; requires_human_reply_authorization = -not $mayReply
            author_is_current_user = $inputRecord.author -ceq $CurrentLogin })
    }
    if ($PullRequest.draft -eq $true -and -not $pending -and $events.Count -eq 0) {
        $events.Add(@{ kind = 'mark-ready'; id = $PullRequest.head.sha; fingerprint = $MainSha })
    }
    $result.evidence = @($events.ToArray())
    if ($events.Count -gt 0) {
        $key = Get-ScheduledDigest -Value @{
            head_sha = $PullRequest.head.sha; base_sha = $MainSha
            evidence = @($events.ToArray() | Sort-Object -Property kind, id, fingerprint)
        }
        if ($key -cin @($Attempt.continuations | ForEach-Object { $_.evidence_key }) -or
            $key -cin $Attempt.handled_evidence) {
            $result.action = 'waiting'; $result.reason = 'unchanged-handled-evidence'
        } else {
            $result.action = 'continue'; $result.evidence_key = $key
        }
        return $result
    }
    if ($pending) { $result.action = 'waiting'; $result.reason = 'current-head-ci-incomplete'; return $result }
    $result.action = 'awaiting-review'
    $result.code_ready = $true
    if ($result.pending_human_responses.Count -gt 0) { $result.reason = 'human-response-approval' }
    return $result
}

function Get-ScheduledHostedCondition {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] $Policy,
        [Parameter(Mandatory)][bool] $Enabled,
        [Parameter(Mandatory)][AllowNull()][object] $LastPlanAt,
        [Parameter(Mandatory)][DateTimeOffset] $Now
    )
    if (-not $Policy.rollout.hosted_execution_enabled) {
        return 'hosted-staged'
    }
    if (-not $Enabled) { 'github-schedule-disabled' }
    if ($null -eq $LastPlanAt) {
        'hosted-planning-evidence-missing'
    } elseif (-not $Policy.coverage.Contains('expected_plan_gap_hours')) {
        'hosted-planning-threshold-missing'
    } elseif (($Now - [DateTimeOffset]$LastPlanAt).TotalHours -gt $Policy.coverage.expected_plan_gap_hours) {
        'github-planning-inactive'
    }
}

function Get-ScheduledExecutorHealth {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] $State,
        [Parameter(Mandatory)] $Policy,
        [Parameter(Mandatory)][DateTimeOffset] $Now,
        [Parameter(Mandatory)][string] $PolicyDigest,
        [Parameter(Mandatory)][bool] $HostedScheduleEnabled,
        [Parameter(Mandatory)][AllowNull()][object] $LastHostedPlanAt
    )
    $conditions = [Collections.Generic.List[string]]::new()
    foreach ($condition in $State.health.blocked_conditions) { $conditions.Add($condition) }
    if ($State.mode -ceq 'paused') { $conditions.Add('paused') }
    if ($null -eq $State.profile -or $State.profile.policy_digest -cne $PolicyDigest -or
        $State.profile.cadence_cron -cne $Policy.local.cadence_cron) { $conditions.Add('scheduling-drift') }
    if ($null -eq $State.health.last_successful_scan -or
        ($Now - [DateTimeOffset]$State.health.last_successful_scan).TotalMinutes -gt
            $Policy.local.expected_poll_gap_minutes) { $conditions.Add('local-scan-stale') }
    foreach ($condition in @(Get-ScheduledHostedCondition -Policy $Policy -Enabled $HostedScheduleEnabled `
        -LastPlanAt $LastHostedPlanAt -Now $Now)) { $conditions.Add($condition) }
    $active = @($State.attempts.Values | Where-Object { $_.phase -cnotin @('resolved', 'closed-unmerged') })
    return @{
        schema_version = 1; repository = $State.repository; repository_id = $State.repository_id
        executor_id = $State.executor_id
        mode = $State.mode; policy_digest = $PolicyDigest; cadence_cron = $Policy.local.cadence_cron
        last_successful_scan = $State.health.last_successful_scan; backlog_count = $State.health.backlog_count
        oldest_eligible_at = $State.health.oldest_eligible_at; last_admission = $State.health.last_admission
        active_session_id = if ($active.Count -gt 0) { $active[0].session_id } else { $null }
        blocked_conditions = @($conditions.ToArray() | Sort-Object -Unique)
        profile_registered_at = $State.health.profile_registered_at
        last_hosted_plan_at = $LastHostedPlanAt
    }
}

Export-ModuleMember -Function Get-ScheduledPullRequestDecision, Get-ScheduledExecutorHealth,
Get-ScheduledHostedCondition
