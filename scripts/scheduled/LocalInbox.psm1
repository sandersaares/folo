Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true

Import-Module (Join-Path $PSScriptRoot 'ScheduledContracts.psm1')
Import-Module (Join-Path $PSScriptRoot 'LocalState.psm1')
Import-Module (Join-Path $PSScriptRoot 'LocalGitHub.psm1')
Import-Module (Join-Path $PSScriptRoot 'LocalLifecycle.psm1')

function Get-ScheduledInboxDecision {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] $Policy,
        [Parameter(Mandatory)] $State,
        [Parameter(Mandatory)][AllowEmptyCollection()][object[]] $Incidents,
        [Parameter(Mandatory)][DateTimeOffset] $Now
    )
    $eligible = [Collections.Generic.List[object]]::new()
    $deferred = [Collections.Generic.List[object]]::new()
    foreach ($incident in $Incidents) {
        $owned = @($State.attempts.Values | Where-Object {
            $_.issue_number -eq $incident.issue_number -and (
                $_.generation -eq $incident.generation -or
                $_.phase -cnotin @('resolved', 'closed-unmerged'))
        })
        $reason = $null
        if ($incident.status -cne 'open') { $reason = 'reporter-disposition' }
        elseif ($incident.held) { $reason = 'human-hold' }
        elseif ($owned.Count -gt 0 -or $null -ne $incident.validated_worker) { $reason = 'reconcile-owned-work' }
        elseif ($incident.package -cnotin $Policy.local.allowed_packages -or
            $incident.check_kind -cnotin $Policy.local.allowed_checks) { $reason = 'outside-approved-scope' }
        if ($null -ne $reason) {
            $deferred.Add(@{ issue_number = $incident.issue_number; reason = $reason })
            continue
        }
        # Correctness failures precede coverage misses; age and issue number give stable
        # tie-breaking so a newly reported failure does not starve the old backlog.
        $priority = if ($incident.check_kind -ceq 'mutants' -and
            $incident.observation.outcome -cne 'timeout') { 1 } else { 0 }
        $eligible.Add(@{
            issue_number = $incident.issue_number; finding_id = $incident.finding_id
            generation = $incident.generation; package = $incident.package; check_id = $incident.check_id
            check_kind = $incident.check_kind
            source_sha = $incident.source_sha; check_contract_digest = $incident.check_contract_digest
            evidence_key = Get-ScheduledDigest -Value $incident.observation
            created_at = $incident.created_at; priority = $priority
            evidence = $incident.evidence
        })
    }
    $ordered = @($eligible.ToArray() | Sort-Object -Property priority, created_at, issue_number)
    $blocked = [Collections.Generic.List[string]]::new()
    if ($State.mode -cne 'repair' -or $Policy.local.mode -cne 'repair') { $blocked.Add($State.mode) }
    if ($State.executor_id -cne $Policy.local.enrolled_machine_id) { $blocked.Add('executor-not-enrolled') }
    if ($null -eq $State.profile -or $State.profile.enabled -ne $true) { $blocked.Add('automation-disabled') }
    $active = @($State.attempts.Values | Where-Object { $_.phase -cnotin @('resolved', 'closed-unmerged') })
    if ($active.Count -ge $Policy.local.max_active_workers) { $blocked.Add('active-worker-limit') }
    $starts = @($State.attempts.Values | Where-Object {
        ([DateTimeOffset]$_.started_at).UtcDateTime.Date -eq $Now.UtcDateTime.Date
    })
    if ($starts.Count -ge $Policy.local.max_starts_per_day) { $blocked.Add('start-budget') }
    return @{
        eligible = $ordered; deferred = @($deferred.ToArray()); blocked_conditions = @($blocked.ToArray())
        backlog_count = $ordered.Count
        oldest_eligible_at = if ($ordered.Count -gt 0) {
            ($ordered | Sort-Object -Property created_at | Select-Object -First 1).created_at
        } else { $null }
        # All registered PRs are returned even if the reporter no longer lists their issue.
        # Their disposition and current-head input still need native-session reconciliation.
        registered_attempts = @($State.attempts.Values | Sort-Object -Property started_at, attempt_id)
    }
}

function Invoke-ScheduledInbox {
    [CmdletBinding()]
    param(
        [string] $PolicyPath = (Join-Path $PSScriptRoot 'policy.json'),
        [Parameter(Mandatory)][string] $ExecutorId,
        [DateTimeOffset] $Now = [DateTimeOffset]::UtcNow,
        [string] $StateRoot
    )
    $policy = Get-ScheduledPolicy -Path $PolicyPath
    $login = Invoke-ScheduledApi -Endpoint 'user'
    $repository = Invoke-ScheduledApi -Endpoint "repos/$($policy.repository)"
    if ($login.login -cne $policy.local.expected_login -or
        $repository.id -ne $policy.repository_id -or $repository.full_name -cne $policy.repository) {
        throw 'GitHub identity differs from the reviewed repository/account policy.'
    }
    if (-not $PSBoundParameters.ContainsKey('StateRoot')) {
        $StateRoot = Get-ScheduledStateRoot $policy.repository_id
    }
    $state = Invoke-ScheduledLocalAction -StateRoot $StateRoot `
        -Policy $policy -ExecutorId $ExecutorId -Login $login.login -Now $Now -Action read
    $issues = Get-ScheduledApiCollection `
        -Endpoint "repos/$($policy.repository)/issues?state=open&labels=scheduled-finding&per_page=100"
    $incidents = [Collections.Generic.List[object]]::new()
    $rejected = [Collections.Generic.List[object]]::new()
    $seen = @{}
    foreach ($issue in $issues) {
        if ($issue.Contains('pull_request')) { continue }
        if ($seen.ContainsKey($issue.number)) {
            if ($seen[$issue.number] -cne (Get-ScheduledDigest $issue)) {
                throw 'Backlog changed across pages; repeat the read before claiming work.'
            }
            continue
        }
        $seen[$issue.number] = Get-ScheduledDigest $issue
        # Validation errors are explicit rejected evidence, never an empty/healthy queue.
        # Native auth/network errors outside this conversion fail the scan immediately.
        $comments = Get-ScheduledApiCollection `
            -Endpoint "repos/$($policy.repository)/issues/$($issue.number)/comments?per_page=100"
        try {
            $record = Read-ScheduledRecord -Text $issue.body -Kind reporter
        } catch [FormatException] {
            $rejected.Add(@{ issue_number = $issue.number; reason = $_.Exception.Message })
            continue
        }
        if ([string]$record.observation.run_id -cnotmatch '^[1-9][0-9]*$' -or
            [string]$record.observation.run_attempt -cnotmatch '^[1-9][0-9]*$') {
            $rejected.Add(@{ issue_number = $issue.number; reason = 'invalid-run-reference' })
            continue
        }
        $run = Invoke-ScheduledApi -Endpoint (
            "repos/$($policy.repository)/actions/runs/$($record.observation.run_id)/attempts/$($record.observation.run_attempt)")
        try {
            $incident = ConvertTo-ScheduledIncident -Issue $issue -Comments $comments -Repository $policy.repository `
                -RepositoryId $policy.repository_id -ReporterLogin $policy.reporter_login `
                -WorkerLogin $policy.worker_login -Run $run
            $incident.created_at = $issue.created_at
            $incident.held = @($issue.labels | Where-Object { $_.name -ceq 'scheduled-hold' }).Count -gt 0
            $incidents.Add($incident)
        } catch [FormatException] {
            $rejected.Add(@{ issue_number = $issue.number; reason = $_.Exception.Message })
        }
    }
    $result = Get-ScheduledInboxDecision -Policy $policy -State $state -Incidents $incidents.ToArray() -Now $Now
    $workflow = Invoke-ScheduledApi -Endpoint "repos/$($policy.repository)/actions/workflows/scheduled-validation.yml"
    $result.hosted_schedule_enabled = $workflow.state -ceq 'active'
    $coverageIssues = Get-ScheduledApiCollection `
        -Endpoint "repos/$($policy.repository)/issues?state=open&labels=scheduled-coverage&per_page=100"
    $coverageOwners = @($coverageIssues | Where-Object {
        -not $_.Contains('pull_request') -and $_.user.login -ceq $policy.reporter_login
    })
    $result.last_hosted_plan = $null
    if ($coverageOwners.Count -eq 1) {
        try {
            $coverage = Read-ScheduledRecord -Text $coverageOwners[0].body -Kind coverage
            if ($coverage.repository -cne $policy.repository -or
                $coverage.repository_id -ne $policy.repository_id -or
                -not $coverage.Contains('last_plan') -or $null -eq $coverage.last_plan) {
                throw [FormatException]::new('Missing authoritative hosted planning identity.')
            }
            $null = [DateTimeOffset]$coverage.last_plan.planned_at
            $result.last_hosted_plan = $coverage.last_plan
        } catch [FormatException] {
            $result.blocked_conditions += 'hosted-planning-evidence-invalid'
        }
    } elseif ($policy.rollout.hosted_execution_enabled) {
        $result.blocked_conditions += 'hosted-planning-evidence-missing-or-ambiguous'
    }
    $lastPlanAt = if ($null -eq $result.last_hosted_plan) { $null } else { $result.last_hosted_plan.planned_at }
    $result.blocked_conditions += @(Get-ScheduledHostedCondition -Policy $policy `
        -Enabled $result.hosted_schedule_enabled -LastPlanAt $lastPlanAt -Now $Now)
    $result.rejected = @($rejected.ToArray())
    if ($rejected.Count -gt 0) { $result.blocked_conditions += 'missing-or-invalid-evidence' }
    $result.schema_version = 1
    $result.repository = $policy.repository
    $result.repository_id = $policy.repository_id
    $result.successful_scan = $true
    return $result
}

Export-ModuleMember -Function Get-ScheduledInboxDecision, Invoke-ScheduledInbox
