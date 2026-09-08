#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'LocalLifecycle.psm1') -Force
}
Describe 'Current-head repair continuation' {
    BeforeEach {
        $attempt = @{
            attempt_id = 'attempt'; session_id = 'native'; pr_number = 7; branch = 'scheduled-repair/finding'
            head_sha = ('a' * 40); phase = 'pr-open'; dispatch = @{ status = 'completed' }
            continuations = @(); handled_evidence = @(); reason = $null
        }
        $script:parameters = @{
            Attempt = $attempt
            PullRequest = @{ number = 7; head = @{ ref = $attempt.branch; sha = $attempt.head_sha }
                state = 'open'; merged = $false; draft = $false; mergeable = $true }
            MainSha = ('b' * 40); ContainsMain = $true; NativeSessionIdle = $true
            CollectionsComplete = $true; CurrentLogin = 'operator'
            VersionEvidence = @{ head_sha = $attempt.head_sha; base_sha = ('b' * 40)
                current = $true; description_current = $true; plan_digest = 'canonical-expanded-plan' }
            CheckRuns = @(
                @{ id = 1; name = 'required-checks'; head_sha = $attempt.head_sha; status = 'completed'; conclusion = 'success' }
                @{ id = 2; name = 'scheduled-repair-gate'; head_sha = $attempt.head_sha; status = 'completed'; conclusion = 'success' }
            )
            RequiredChecks = @('required-checks', 'scheduled-repair-gate')
            ReviewInput = @()
        }
    }
    It 'leaves final approval and merge to humans once current evidence is complete' {
        (Get-ScheduledPullRequestDecision @parameters).action | Should -Be awaiting-review
    }
    It 'does not treat missing stale or pending required checks as ready' {
        $parameters.CheckRuns[1].head_sha = 'old-head'
        (Get-ScheduledPullRequestDecision @parameters).reason | Should -Be current-head-ci-incomplete
        $parameters.CheckRuns[1].head_sha = $attempt.head_sha
        $parameters.CheckRuns[1].status = 'in_progress'
        (Get-ScheduledPullRequestDecision @parameters).action | Should -Be waiting
    }
    It 'continues failed skipped and cancelled current-head checks once per new evidence' -ForEach @(
        @{ Conclusion = 'failure' }, @{ Conclusion = 'skipped' }, @{ Conclusion = 'cancelled' }
    ) {
        $parameters.CheckRuns[1].conclusion = $Conclusion
        $result = Get-ScheduledPullRequestDecision @parameters
        $result.action | Should -Be continue
        $attempt.continuations = @(@{ evidence_key = $result.evidence_key })
        (Get-ScheduledPullRequestDecision @parameters).reason | Should -Be unchanged-handled-evidence
    }
    It 'uses latest run identity instead of an older success at the same head' {
        $newRun = $parameters.CheckRuns[1].Clone()
        $newRun.id = 3; $newRun.conclusion = 'failure'
        $parameters.CheckRuns += $newRun
        (Get-ScheduledPullRequestDecision @parameters).action | Should -Be continue
    }
    It 'rejects human or unreconciled head changes without overwriting them' {
        $parameters.PullRequest.head.sha = ('d' * 40)
        (Get-ScheduledPullRequestDecision @parameters).reason | Should -Be human-or-unreconciled-head-change
    }
    It 'continues synchronization with main and refreshes the canonical version plan' {
        $parameters.ContainsMain = $false
        $parameters.VersionEvidence.description_current = $false
        $result = Get-ScheduledPullRequestDecision @parameters
        $result.action | Should -Be continue
        $result.evidence.kind | Should -Contain base
        $result.evidence.kind | Should -Contain version-plan
    }
    It 'handles inline comments top-level comments and review summaries with reply authority' {
        $parameters.ReviewInput = @(
            @{ kind = 'thread'; id = 10; body = 'Fix this'; state = 'COMMENTED'
                author = 'operator'; resolved = $false }
            @{ kind = 'comment'; id = 10; body = '[Copilot speaking] Fix this'
                state = 'COMMENTED'; author = 'agent-account'; resolved = $false }
            @{ kind = 'review'; id = 10; body = 'Changes needed'; state = 'CHANGES_REQUESTED'
                author = 'another-human'; resolved = $false }
        )
        $result = Get-ScheduledPullRequestDecision @parameters
        $result.evidence.Count | Should -Be 3
        @($result.evidence | Where-Object { $_.may_reply }).Count | Should -Be 1
        @($result.evidence | Where-Object { $_.requires_human_reply_authorization }).Count | Should -Be 2
        $attempt.handled_evidence = @($result.evidence.fingerprint)
        (Get-ScheduledPullRequestDecision @parameters).action | Should -Be awaiting-review
        $parameters.ReviewInput[2].body = 'Edited request'
        (Get-ScheduledPullRequestDecision @parameters).evidence.Count | Should -Be 1
    }
    It 'does not reprocess resolved or dismissed input' {
        $parameters.ReviewInput = @(
            @{ kind = 'thread'; id = 10; body = 'Fixed'; state = 'COMMENTED'; author = 'reviewer'; resolved = $true }
            @{ kind = 'review'; id = 11; body = 'Old'; state = 'DISMISSED'; author = 'reviewer'; resolved = $false }
        )
        (Get-ScheduledPullRequestDecision @parameters).action | Should -Be awaiting-review
    }
    It 'keeps active paused and incomplete sessions out of continuation' {
        $parameters.NativeSessionIdle = $false
        (Get-ScheduledPullRequestDecision @parameters).reason | Should -Be worker-owns-dispatch
        $parameters.NativeSessionIdle = $true
        $attempt.phase = 'blocked'; $attempt.reason = 'quota'
        (Get-ScheduledPullRequestDecision @parameters).reason | Should -Be quota
        $parameters.CollectionsComplete = $false
        (Get-ScheduledPullRequestDecision @parameters).reason | Should -Be incomplete-api-evidence
    }
    It 'does not replace a closed PR and waits for main confirmation after merge' {
        $parameters.PullRequest.state = 'closed'
        (Get-ScheduledPullRequestDecision @parameters).action | Should -Be closed-unmerged
        $parameters.PullRequest.merged = $true
        (Get-ScheduledPullRequestDecision @parameters).action | Should -Be verifying-main
    }
    It 'retains active native ownership on closure and keeps confirmed attempts terminal' {
        $parameters.PullRequest.state = 'closed'
        $parameters.NativeSessionIdle = $false
        (Get-ScheduledPullRequestDecision @parameters).reason | Should -Be worker-owns-dispatch
        $parameters.NativeSessionIdle = $true
        $parameters.PullRequest.merged = $true
        $attempt.phase = 'resolved'
        (Get-ScheduledPullRequestDecision @parameters).action | Should -Be resolved
    }
    It 'separates code readiness from unapproved human responses including the current user' {
        $attempt.proposed_responses = @(@{ kind = 'thread'; id = 10; status = 'pending'; body = 'Proposed reply' })
        $result = Get-ScheduledPullRequestDecision @parameters
        $result.action | Should -Be awaiting-review
        $result.code_ready | Should -BeTrue
        $result.reason | Should -Be human-response-approval
        $result.pending_human_responses.Count | Should -Be 1
    }
    It 'does not end at draft creation or endlessly request mark-ready' {
        $parameters.PullRequest.draft = $true
        $result = Get-ScheduledPullRequestDecision @parameters
        $result.action | Should -Be continue
        $attempt.continuations = @(@{ evidence_key = $result.evidence_key })
        (Get-ScheduledPullRequestDecision @parameters).action | Should -Be waiting
    }
}

Describe 'Executor health' {
    It 'separates pause drift missing hosted evidence and successful scans using fake time' {
        $policy = @{ repository = 'folo-rs/folo'; local = @{
            cadence_cron = '17 */3 * * *'; expected_poll_gap_minutes = 420
        }; rollout = @{ hosted_execution_enabled = $true }; coverage = @{ expected_plan_gap_hours = 30 } }
        $state = @{ repository = $policy.repository; repository_id = 850321188
            executor_id = 'executor'; mode = 'paused'
            profile = @{ policy_digest = 'old'; cadence_cron = 'old' }; attempts = @{}
            health = @{ blocked_conditions = @('budget'); last_successful_scan = $null; backlog_count = 3
                oldest_eligible_at = $null; last_admission = $null; profile_registered_at = '2026-09-01T00:00:00Z' } }
        $result = Get-ScheduledExecutorHealth -State $state -Policy $policy -Now '2026-09-01T12:00:00Z' `
            -PolicyDigest 'new' -HostedScheduleEnabled $false -LastHostedPlanAt $null
        $result.blocked_conditions | Should -Contain paused
        $result.blocked_conditions | Should -Contain scheduling-drift
        $result.blocked_conditions | Should -Contain local-scan-stale
        $result.blocked_conditions | Should -Contain github-schedule-disabled
        $result.blocked_conditions | Should -Contain hosted-planning-evidence-missing
        $result.last_successful_scan | Should -BeNullOrEmpty
    }
    It 'uses hosted cadence rather than local cadence and distinguishes staging from inactivity' {
        $policy = @{ rollout = @{ hosted_execution_enabled = $false }
            coverage = @{ expected_plan_gap_hours = 30 } }
        @(Get-ScheduledHostedCondition -Policy $policy -Enabled $false -LastPlanAt $null `
            -Now '2026-09-02T06:00:00Z') | Should -Be @('hosted-staged')
        $policy.rollout.hosted_execution_enabled = $true
        @(Get-ScheduledHostedCondition -Policy $policy -Enabled $true -LastPlanAt '2026-09-01T00:00:00Z' `
            -Now '2026-09-02T06:00:00Z').Count | Should -Be 0
        @(Get-ScheduledHostedCondition -Policy $policy -Enabled $true -LastPlanAt '2026-09-01T00:00:00Z' `
            -Now '2026-09-02T06:01:00Z') | Should -Be @('github-planning-inactive')
    }
}
