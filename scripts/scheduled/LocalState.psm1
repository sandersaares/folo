Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true

Import-Module (Join-Path $PSScriptRoot 'ScheduledContracts.psm1')

# Short file transactions protect the durable protocol described in
# docs/scheduled-validation.md. No file handle is expected to survive an App tool call.
function Get-ScheduledStateRoot {
    [CmdletBinding()]
    param([Parameter(Mandatory)][ValidatePattern('^[1-9][0-9]*$')][string] $RepositoryId)

    if ([string]::IsNullOrWhiteSpace($env:LOCALAPPDATA) -or
        -not [IO.Path]::IsPathFullyQualified($env:LOCALAPPDATA)) {
        throw 'A resolved LOCALAPPDATA directory is required for the enrolled Windows executor.'
    }
    return [IO.Path]::GetFullPath((Join-Path $env:LOCALAPPDATA "Folo\ScheduledRemediation\$RepositoryId"))
}

function Assert-LocalField {
    param([System.Collections.IDictionary] $Value, [string[]] $Fields)
    if ($null -eq $Value) { throw 'Expected a state object.' }
    foreach ($field in $Fields) {
        if (-not $Value.Contains($field)) { throw "Missing state/request field '$field'." }
    }
}

function Assert-ScheduledLocalState {
    param([System.Collections.IDictionary] $State)
    Assert-LocalField $State @('schema_version', 'repository', 'repository_id', 'executor_id',
        'login', 'mode', 'revision', 'coordinator', 'attempts', 'profile', 'health')
    if ($State.schema_version -ne 1 -or $State.revision -lt 0 -or
        $State.mode -cnotin @('observe', 'repair', 'paused') -or
        $State.attempts -isnot [System.Collections.IDictionary]) {
        throw 'Unsupported or corrupt executor state; recover without resetting admission history.'
    }
    foreach ($entry in $State.attempts.GetEnumerator()) {
        $attempt = $entry.Value
        Assert-LocalField $attempt @('attempt_id', 'issue_number', 'finding_id', 'generation',
            'started_at', 'session_id', 'branch', 'head_sha', 'pr_number', 'phase', 'dispatch',
            'continuations', 'handled_evidence', 'check_contract_digest', 'reason', 'version_evidence',
            'proposed_responses', 'check_id', 'check_kind')
        if ($attempt.attempt_id -cne $entry.Key -or
            $attempt.phase -cnotin @('reserved', 'opening-session', 'session-registered',
                'dispatching', 'working', 'publishing', 'pr-open', 'awaiting-review',
                'blocked', 'verifying-main', 'resolved', 'closed-unmerged') -or
            $attempt.continuations -isnot [System.Collections.IList] -or
            $attempt.handled_evidence -isnot [System.Collections.IList]) {
            throw 'Corrupt attempt history; operator reconciliation is required.'
        }
        $null = [DateTimeOffset]$attempt.started_at
        foreach ($continuation in $attempt.continuations) {
            Assert-LocalField $continuation @('token', 'evidence_key', 'admitted_at')
            $null = [DateTimeOffset]$continuation.admitted_at
        }
    }
}

function Assert-LocalCoordinator {
    param($State, $Data, [DateTimeOffset] $Now)
    Assert-LocalField $Data @('coordinator_token')
    if ($null -eq $State.coordinator -or
        $State.coordinator.token -cne $Data.coordinator_token -or
        [DateTimeOffset]$State.coordinator.expires_at -le $Now) {
        throw 'Stale coordinator token; reconcile through a fresh scan.'
    }
}

function Get-LocalAttempt {
    param($State, $Data)
    Assert-LocalField $Data @('attempt_id')
    if (-not $State.attempts.Contains($Data.attempt_id)) { throw 'Unknown attempt.' }
    return $State.attempts[$Data.attempt_id]
}

function Assert-LocalWorker {
    param($Attempt, $Data)
    Assert-LocalField $Data @('session_id', 'dispatch_token')
    if ($null -eq $Attempt.dispatch -or $Attempt.session_id -cne $Data.session_id -or
        $Attempt.dispatch.token -cne $Data.dispatch_token -or
        $Attempt.dispatch.status -cne 'accepted') {
        throw 'Worker does not own the current dispatch.'
    }
}

function Assert-LocalAdmission {
    param($State, $Policy)
    if ($State.mode -cne 'repair' -or $Policy.local.mode -cne 'repair' -or
        $Policy.local.enrolled_machine_id -cne $State.executor_id -or
        $Policy.local.expected_login -cne $State.login -or $null -eq $State.profile -or
        -not $State.profile.enabled) {
        throw 'Repair admission is paused, unenrolled, or unconfigured.'
    }
    foreach ($prerequisite in $Policy.rollout.prerequisites.GetEnumerator()) {
        if ($prerequisite.Value -ne $true) { throw "Unproved prerequisite: $($prerequisite.Key)." }
    }
    if ($State.profile.cadence_cron -cne $Policy.local.cadence_cron -or
        $State.profile.policy_digest -cne (Get-ScheduledDigest -Value $Policy)) {
        throw 'Installed scheduling profile differs from reviewed policy.'
    }
}

function Invoke-LocalStateChange {
    param($State, $Policy, [string] $Action, $Data, [DateTimeOffset] $Now)
    $stamp = $Now.ToUniversalTime().ToString('o')
    $day = $Now.UtcDateTime.Date
    switch -CaseSensitive ($Action) {
        'read' { return }
        'acquire-coordinator' {
            Assert-LocalField $Data @('owner_session_id')
            if ($null -ne $State.coordinator -and
                [DateTimeOffset]$State.coordinator.expires_at -gt $Now) {
                throw 'Another coordinator still owns the scan.'
            }
            $State.coordinator = @{
                token = [guid]::NewGuid().ToString()
                owner_session_id = $Data.owner_session_id
                expires_at = $Now.AddMinutes($Policy.local.coordinator_lease_minutes).ToString('o')
            }
        }
        'release-coordinator' {
            Assert-LocalCoordinator $State $Data $Now
            $State.coordinator = $null
        }
        'register-profile' {
            Assert-LocalField $Data @('operator_approved', 'profile')
            if ($Data.operator_approved -ne $true) { throw 'Profile registration requires operator approval.' }
            Assert-LocalField $Data.profile @('automation_id', 'project_id', 'host_id', 'executor_id',
                'login', 'cadence_cron', 'timezone', 'enabled', 'policy_digest', 'prompt_digest',
                'coordinator_model', 'repair_model')
            if ($Data.profile.executor_id -cne $State.executor_id -or
                $Data.profile.login -cne $State.login -or
                [string]::IsNullOrWhiteSpace($Data.profile.host_id)) {
                throw 'Profile identity differs from enrollment.'
            }
            $State.profile = $Data.profile
            $State.health.profile_registered_at = $stamp
        }
        'set-mode' {
            Assert-LocalField $Data @('operator_approved', 'mode')
            if ($Data.operator_approved -ne $true -or $Data.mode -cnotin @('observe', 'repair', 'paused')) {
                throw 'Mode changes require explicit operator approval.'
            }
            $State.mode = $Data.mode
        }
        'record-scan' {
            Assert-LocalCoordinator $State $Data $Now
            Assert-LocalField $Data @('successful', 'backlog_count', 'oldest_eligible_at', 'blocked_conditions')
            $State.health.last_scan_at = $stamp
            $State.health.blocked_conditions = @($Data.blocked_conditions)
            if ($Data.successful -eq $true) {
                $State.health.last_successful_scan = $stamp
                $State.health.backlog_count = $Data.backlog_count
                $State.health.oldest_eligible_at = $Data.oldest_eligible_at
            }
        }
        'reserve-attempt' {
            Assert-LocalCoordinator $State $Data $Now
            Assert-LocalAdmission $State $Policy
            Assert-LocalField $Data @('issue_number', 'finding_id', 'generation', 'check_contract_digest',
                'check_id', 'check_kind', 'package', 'evidence_key')
            if ($Data.check_kind -cnotin $Policy.local.allowed_checks -or
                $Data.package -cnotin $Policy.local.allowed_packages) { throw 'Incident is outside approved scope.' }
            $attempts = @($State.attempts.Values)
            $active = @($attempts | Where-Object { $_.phase -cnotin @('resolved', 'closed-unmerged') })
            if ($active.Count -ge $Policy.local.max_active_workers) { throw 'Active worker limit reached.' }
            # Never replace a closed PR or restart an incident just because a session is idle.
            if (@($attempts | Where-Object {
                $_.issue_number -eq $Data.issue_number -and $_.generation -eq $Data.generation
            }).Count -gt 0) { throw 'Incident already has an attempt; reconcile its native session.' }
            $starts = @($attempts | Where-Object {
                ([DateTimeOffset]$_.started_at).UtcDateTime.Date -eq $day
            })
            $incidentAttempts = @($attempts | Where-Object {
                $_.finding_id -ceq $Data.finding_id -and $_.generation -eq $Data.generation
            })
            if ($starts.Count -ge $Policy.local.max_starts_per_day -or
                $incidentAttempts.Count -ge $Policy.local.max_attempts_per_incident) {
                throw 'Attempt admission budget exhausted.'
            }
            $id = [guid]::NewGuid().ToString()
            $State.attempts[$id] = @{
                attempt_id = $id; issue_number = $Data.issue_number; finding_id = $Data.finding_id
                generation = $Data.generation; check_contract_digest = $Data.check_contract_digest
                check_id = $Data.check_id; check_kind = $Data.check_kind
                started_at = $stamp; session_id = $null; branch = $null; head_sha = $null
                pr_number = $null; phase = 'reserved'; reason = $null; continuations = @()
                handled_evidence = @(); evidence_key = $Data.evidence_key; version_evidence = $null
                proposed_responses = @()
                dispatch = @{ token = [guid]::NewGuid().ToString(); status = 'reserved' }
            }
            $State.health.last_admission = $stamp
        }
        'begin-session-open' {
            Assert-LocalCoordinator $State $Data $Now
            $attempt = Get-LocalAttempt $State $Data
            if ($attempt.phase -cne 'reserved') { throw 'Session-open outcome must be reconciled, not retried.' }
            $attempt.phase = 'opening-session'
        }
        'register-session' {
            Assert-LocalCoordinator $State $Data $Now
            $attempt = Get-LocalAttempt $State $Data
            Assert-LocalField $Data @('session_id', 'issue_number', 'ownership_verified', 'branch', 'head_sha')
            if ($attempt.phase -cne 'opening-session' -or $Data.ownership_verified -ne $true -or
                $Data.issue_number -ne $attempt.issue_number -or
                [string]::IsNullOrWhiteSpace($Data.session_id) -or
                [string]::IsNullOrWhiteSpace($Data.branch) -or
                $Data.head_sha -cnotmatch '^[0-9a-f]{40}$') {
                throw 'Native issue/session/branch ownership has not been established.'
            }
            if (@($State.attempts.Values | Where-Object { $_.session_id -ceq $Data.session_id }).Count -gt 0) {
                throw 'Native session already belongs to another attempt.'
            }
            $attempt.session_id = $Data.session_id
            $attempt.branch = $Data.branch
            $attempt.head_sha = $Data.head_sha
            $attempt.phase = 'session-registered'
        }
        'register-branch' {
            $attempt = Get-LocalAttempt $State $Data
            Assert-LocalWorker $attempt $Data
            Assert-LocalField $Data @('expected_branch', 'branch', 'head_sha')
            if ($attempt.phase -cne 'working' -or $null -ne $attempt.pr_number -or
                $attempt.branch -cne $Data.expected_branch -or $attempt.head_sha -cne $Data.head_sha -or
                -not $Data.branch.StartsWith($Policy.managed_branch_prefix, [StringComparison]::Ordinal)) {
                throw 'Native branch registration must preserve the starting head and managed namespace.'
            }
            $attempt.branch = $Data.branch
        }
        'begin-dispatch' {
            Assert-LocalCoordinator $State $Data $Now
            $attempt = Get-LocalAttempt $State $Data
            if ($attempt.dispatch.status -cne 'reserved' -or $null -eq $attempt.session_id) {
                throw 'Dispatch outcome is unknown or already delivered; do not resend blindly.'
            }
            $attempt.dispatch.status = 'sending'
            $attempt.phase = 'dispatching'
        }
        'accept-dispatch' {
            $attempt = Get-LocalAttempt $State $Data
            Assert-LocalField $Data @('session_id', 'dispatch_token')
            if ($attempt.session_id -cne $Data.session_id -or
                $attempt.dispatch.token -cne $Data.dispatch_token -or
                $attempt.dispatch.status -cnotin @('sending', 'accepted')) {
                throw 'Stale or unregistered worker kickoff.'
            }
            if ($attempt.dispatch.status -ceq 'sending') {
                $attempt.dispatch.status = 'accepted'
                $attempt.phase = 'working'
            }
        }
        'prepare-publication' {
            $attempt = Get-LocalAttempt $State $Data
            Assert-LocalWorker $attempt $Data
            Assert-LocalField $Data @('expected_head', 'head_sha', 'branch', 'check_contract_digest')
            if ($attempt.phase -cne 'working' -or $attempt.head_sha -cne $Data.expected_head -or
                $attempt.branch -cne $Data.branch -or $Data.head_sha -cnotmatch '^[0-9a-f]{40}$' -or
                -not $Data.branch.StartsWith($Policy.managed_branch_prefix, [StringComparison]::Ordinal)) {
                throw 'Publication conflicts with the recorded branch/head or a previous unknown publication.'
            }
            $attempt.head_sha = $Data.head_sha
            $attempt.check_contract_digest = $Data.check_contract_digest
            $attempt.phase = 'publishing'
        }
        'register-pr' {
            $attempt = Get-LocalAttempt $State $Data
            Assert-LocalWorker $attempt $Data
            Assert-LocalField $Data @('pr_number', 'head_sha', 'branch')
            if ($attempt.phase -cne 'publishing' -or $Data.pr_number -le 0 -or
                $attempt.head_sha -cne $Data.head_sha -or $attempt.branch -cne $Data.branch -or
                ($null -ne $attempt.pr_number -and $attempt.pr_number -ne $Data.pr_number)) {
                throw 'PR does not match the publication intent.'
            }
            $attempt.pr_number = $Data.pr_number
            $attempt.phase = 'pr-open'
        }
        'complete-dispatch' {
            $attempt = Get-LocalAttempt $State $Data
            Assert-LocalWorker $attempt $Data
            Assert-LocalField $Data @('phase', 'reason', 'handled_evidence')
            if ($Data.phase -cnotin @('pr-open', 'awaiting-review', 'blocked') -or
                ($Data.phase -cne 'blocked' -and $null -eq $attempt.pr_number) -or
                ($Data.phase -ceq 'blocked' -and [string]::IsNullOrWhiteSpace($Data.reason))) {
                throw 'Dispatch must finish with an existing PR or an explicit blocked reason.'
            }
            $attempt.phase = $Data.phase
            $attempt.reason = $Data.reason
            $attempt.handled_evidence = @(@($attempt.handled_evidence) + @($Data.handled_evidence) |
                Sort-Object -Unique)
            if ($Data.Contains('proposed_responses')) {
                foreach ($response in $Data.proposed_responses) {
                    Assert-LocalField $response @('kind', 'id', 'fingerprint', 'body', 'status')
                    if ($response.status -cnotin @('pending', 'approved', 'posted')) {
                        throw 'Unknown human response disposition.'
                    }
                }
                $attempt.proposed_responses = @($Data.proposed_responses)
            }
            $attempt.dispatch.status = 'completed'
        }
        'record-version-plan' {
            $attempt = Get-LocalAttempt $State $Data
            Assert-LocalWorker $attempt $Data
            Assert-LocalField $Data @('version_evidence')
            Assert-LocalField $Data.version_evidence @('head_sha', 'base_sha', 'pre_version_sha',
                'decisions', 'expanded_plan', 'expanded_plan_digest', 'plan_digest',
                'current', 'description_current')
            if ($Data.version_evidence.head_sha -cne $attempt.head_sha -or
                $Data.version_evidence.base_sha -cnotmatch '^[0-9a-f]{40}$' -or
                $Data.version_evidence.pre_version_sha -cnotmatch '^[0-9a-f]{40}$' -or
                $Data.version_evidence.plan_digest -cnotmatch '^[0-9a-f]{64}$' -or
                $Data.version_evidence.expanded_plan_digest -cne
                    (Get-ScheduledDigest $Data.version_evidence.expanded_plan)) {
                throw 'Canonical version evidence must identify the current publication head and base.'
            }
            $attempt.version_evidence = $Data.version_evidence
        }
        'reserve-continuation' {
            Assert-LocalCoordinator $State $Data $Now
            Assert-LocalAdmission $State $Policy
            $attempt = Get-LocalAttempt $State $Data
            Assert-LocalField $Data @('evidence_key', 'expected_head', 'session_id', 'native_idle_verified')
            if ($attempt.phase -cnotin @('pr-open', 'awaiting-review') -or
                $attempt.dispatch.status -cne 'completed' -or $Data.native_idle_verified -ne $true -or
                $attempt.head_sha -cne $Data.expected_head -or $attempt.session_id -cne $Data.session_id) {
                throw 'Continuation requires the same quiescent owned PR session and exact head.'
            }
            $priorKeys = @($attempt.continuations | ForEach-Object { $_.evidence_key })
            if ([string]::IsNullOrWhiteSpace($Data.evidence_key) -or
                $Data.evidence_key -cin $priorKeys -or $Data.evidence_key -cin $attempt.handled_evidence) {
                throw 'No new actionable evidence; do not repeat unchanged waiting work.'
            }
            $today = @($State.attempts.Values | ForEach-Object { $_.continuations } | Where-Object {
                ([DateTimeOffset]$_.admitted_at).UtcDateTime.Date -eq $day
            })
            if ($today.Count -ge $Policy.local.max_continuations_per_day -or
                $attempt.continuations.Count -ge $Policy.local.max_continuations_per_attempt) {
                throw 'Continuation admission budget exhausted.'
            }
            $token = [guid]::NewGuid().ToString()
            $attempt.continuations += @{ token = $token; evidence_key = $Data.evidence_key; admitted_at = $stamp }
            $attempt.dispatch = @{ token = $token; status = 'reserved' }
            $attempt.evidence_key = $Data.evidence_key
            $State.health.last_admission = $stamp
        }
        'record-pr-disposition' {
            Assert-LocalCoordinator $State $Data $Now
            $attempt = Get-LocalAttempt $State $Data
            Assert-LocalField $Data @('pr_number', 'head_sha', 'disposition', 'hosted_confirmation',
                'native_idle_verified')
            if ($attempt.pr_number -ne $Data.pr_number -or $attempt.head_sha -cne $Data.head_sha -or
                $Data.disposition -cnotin @('merged', 'closed-unmerged', 'confirmed')) {
                throw 'Disposition does not match the registered PR.'
            }
            if ($Data.native_idle_verified -ne $true -or $attempt.dispatch.status -cne 'completed') {
                throw 'PR disposition does not prove the native worker has stopped; retain its slot.'
            }
            if ($attempt.phase -cin @('resolved', 'closed-unmerged')) {
                # Repeated polling must not resurrect a completed or deliberately stopped attempt.
                return
            }
            if ($Data.disposition -ceq 'confirmed' -and $Data.hosted_confirmation -ne $true) {
                throw 'Only matching hosted confirmation establishes resolution.'
            }
            if ($Data.disposition -ceq 'confirmed' -and $attempt.phase -cne 'verifying-main') {
                throw 'Resolution requires recorded human merge followed by hosted confirmation.'
            }
            $attempt.phase = switch ($Data.disposition) {
                'merged' { 'verifying-main' }
                'closed-unmerged' { 'closed-unmerged' }
                'confirmed' { 'resolved' }
            }
            $attempt.dispatch.status = 'completed'
        }
        'block' {
            Assert-LocalCoordinator $State $Data $Now
            $attempt = Get-LocalAttempt $State $Data
            Assert-LocalField $Data @('reason')
            if ([string]::IsNullOrWhiteSpace($Data.reason)) { throw 'A blocked reason is required.' }
            # Blocking never frees the slot or invalidates a worker that may still be running.
            $attempt.phase = 'blocked'
            $attempt.reason = $Data.reason
        }
        default { throw "Unsupported local state action '$Action'." }
    }
}

function Invoke-ScheduledLocalAction {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string] $StateRoot,
        [Parameter(Mandatory)][System.Collections.IDictionary] $Policy,
        [Parameter(Mandatory)][string] $ExecutorId,
        [Parameter(Mandatory)][string] $Login,
        [Parameter(Mandatory)][DateTimeOffset] $Now,
        [Parameter(Mandatory)][string] $Action,
        [System.Collections.IDictionary] $Data = @{}
    )
    if (-not [IO.Path]::IsPathFullyQualified($StateRoot) -or
        [string]::IsNullOrWhiteSpace($ExecutorId) -or [string]::IsNullOrWhiteSpace($Login) -or
        [string]$Policy.repository_id -cnotmatch '^[1-9][0-9]*$') {
        throw 'An absolute state root and enrolled repository/executor/login identities are required.'
    }
    $existed = Test-Path -LiteralPath $StateRoot
    if (-not $existed) {
        if ($Action -cne 'initialize' -or -not $Data.Contains('operator_approved') -or
            $Data.operator_approved -ne $true) {
            throw 'State is missing; explicit first enrollment or operator recovery is required.'
        }
        $null = New-Item -ItemType Directory -Path $StateRoot
    }
    $path = Join-Path $StateRoot 'state.json'
    $temporary = Join-Path $StateRoot ("state.$([guid]::NewGuid()).tmp")
    # No retry loop: contention is visible, and a future repository poll can retry the scan.
    $lock = [IO.File]::Open((Join-Path $StateRoot 'transaction.lock'),
        [IO.FileMode]::OpenOrCreate, [IO.FileAccess]::ReadWrite, [IO.FileShare]::None)
    try {
        if (Test-Path -LiteralPath $path) {
            $state = Get-Content -LiteralPath $path -Raw | ConvertFrom-Json -AsHashtable
            Assert-ScheduledLocalState $state
            if ($state.repository_id -ne $Policy.repository_id -or $state.repository -cne $Policy.repository -or
                $state.executor_id -cne $ExecutorId -or $state.login -cne $Login) {
                throw 'Executor enrollment mismatch; never transfer ownership implicitly.'
            }
            if ($Action -ceq 'initialize') { return $state }
        } else {
            if ($existed -or $Action -cne 'initialize') {
                throw 'Missing state in an existing enrollment; do not reset budgets or claims.'
            }
            $state = @{
                schema_version = 1; repository = $Policy.repository; repository_id = $Policy.repository_id
                executor_id = $ExecutorId; login = $Login; mode = 'observe'; revision = 0
                coordinator = $null; attempts = @{}; profile = $null
                health = @{
                    last_scan_at = $null; last_successful_scan = $null; backlog_count = 0
                    oldest_eligible_at = $null; last_admission = $null; profile_registered_at = $null
                    blocked_conditions = @()
                }
            }
        }
        if ($Action -ceq 'register-profile' -and $Data.operator_approved -eq $true -and
            $null -ne $state.profile -and
            (Get-ScheduledDigest $state.profile) -ceq (Get-ScheduledDigest $Data.profile)) {
            return $state
        }
        if ($Action -cne 'initialize') { Invoke-LocalStateChange $state $Policy $Action $Data $Now }
        if ($Action -ceq 'read') { return $state }
        $state.revision++
        Assert-ScheduledLocalState $state
        $bytes = [Text.Encoding]::UTF8.GetBytes(($state | ConvertTo-Json -Depth 40))
        $writer = [IO.File]::Open($temporary, [IO.FileMode]::CreateNew, [IO.FileAccess]::Write,
            [IO.FileShare]::None)
        try {
            $writer.Write($bytes)
            $writer.Flush($true)
        } finally { $writer.Dispose() }
        [IO.File]::Move($temporary, $path, $true)
        return $state
    } finally {
        $lock.Dispose()
        if (Test-Path -LiteralPath $temporary) { Remove-Item -LiteralPath $temporary }
    }
}

function Get-ScheduledWorkerRecord {
    [CmdletBinding()]
    param([Parameter(Mandatory)] $State, [Parameter(Mandatory)][string] $AttemptId)
    Assert-ScheduledLocalState $State
    $attempt = Get-LocalAttempt $State @{ attempt_id = $AttemptId }
    return @{
        schema_version = 1; repository = $State.repository; repository_id = $State.repository_id
        finding_id = $attempt.finding_id; generation = $attempt.generation; attempt_id = $attempt.attempt_id
        executor_id = $State.executor_id; session_id = $attempt.session_id; branch = $attempt.branch
        head_sha = $attempt.head_sha; pr_number = $attempt.pr_number; state = $attempt.phase
        check_id = $attempt.check_id; check_kind = $attempt.check_kind
        version_evidence = $attempt.version_evidence
    }
}

function Get-ScheduledRepairRecord {
    [CmdletBinding()]
    param([Parameter(Mandatory)] $State, [Parameter(Mandatory)][string] $AttemptId)
    $attempt = Get-LocalAttempt $State @{ attempt_id = $AttemptId }
    if ($attempt.phase -cne 'publishing') { throw 'Persist publication intent before composing a PR record.' }
    return @{
        schema_version = 1; repository = $State.repository; repository_id = $State.repository_id
        issue_number = $attempt.issue_number; finding_id = $attempt.finding_id; generation = $attempt.generation
        attempt_id = $attempt.attempt_id; branch = $attempt.branch; head_sha = $attempt.head_sha
        check_contract_digest = $attempt.check_contract_digest
    }
}

function Invoke-ScheduledLocalRequest {
    [CmdletBinding()]
    param([Parameter(Mandatory)][string] $RequestPath)
    $request = Get-Content -LiteralPath $RequestPath -Raw | ConvertFrom-Json -AsHashtable
    Assert-LocalField $request @('policy_path', 'executor_id', 'login', 'action', 'data')
    $policy = Get-ScheduledPolicy -Path $request.policy_path
    $root = Get-ScheduledStateRoot -RepositoryId $policy.repository_id
    Invoke-ScheduledLocalAction -StateRoot $root -Policy $policy -ExecutorId $request.executor_id `
        -Login $request.login -Now ([DateTimeOffset]::UtcNow) -Action $request.action -Data $request.data |
        ConvertTo-Json -Depth 40
}

Export-ModuleMember -Function Get-ScheduledStateRoot, Invoke-ScheduledLocalAction, Invoke-ScheduledLocalRequest,
Get-ScheduledWorkerRecord, Get-ScheduledRepairRecord
