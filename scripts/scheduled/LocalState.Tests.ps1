#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'LocalState.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'ScheduledContracts.psm1') -Force

    function Invoke-TestAction {
        param([string] $Action, [hashtable] $Data = @{})
        Invoke-ScheduledLocalAction -StateRoot $script:root -Policy $script:policy `
            -ExecutorId 'machine-a' -Login 'operator' -Now $script:now -Action $Action -Data $Data
    }
    function Initialize-TestExecutor {
        $null = Invoke-TestAction initialize @{ operator_approved = $true }
        $null = Invoke-TestAction register-profile @{ operator_approved = $true; profile = @{
            automation_id = 'automation-a'; project_id = 'project-a'; host_id = 'native-local-a'
            executor_id = 'machine-a'; login = 'operator'; cadence_cron = $script:policy.local.cadence_cron
            timezone = 'Europe/Tallinn'; enabled = $true
            policy_digest = (Get-ScheduledDigest $script:policy); prompt_digest = 'prompt'
            coordinator_model = 'intake'; repair_model = 'repair'
        } }
        $null = Invoke-TestAction set-mode @{ operator_approved = $true; mode = 'repair' }
        $state = Invoke-TestAction acquire-coordinator @{ owner_session_id = 'coordinator-a' }
        $script:coordinator = $state.coordinator.token
    }
    function Invoke-TestReservation {
        param([int] $Issue = 1, [string] $Finding = ('f' * 64), [int] $Generation = 1)
        $state = Invoke-TestAction reserve-attempt @{
            coordinator_token = $script:coordinator; issue_number = $Issue
            finding_id = $Finding; generation = $Generation; check_contract_digest = ('c' * 64)
            package = 'cpulist'; check_id = 'mutants'; evidence_key = 'initial-evidence'
        }
        $attempt = @($state.attempts.Values | Where-Object {
            $_.issue_number -eq $Issue -and $_.generation -eq $Generation
        })[0]
        $script:attemptId = $attempt.attempt_id
        $script:dispatch = $attempt.dispatch.token
        return $attempt
    }
    function Invoke-TestWorkerSetup {
        $null = Invoke-TestReservation
        $null = Invoke-TestAction begin-session-open @{
            coordinator_token = $script:coordinator; attempt_id = $script:attemptId
        }
        $null = Invoke-TestAction register-session @{
            coordinator_token = $script:coordinator; attempt_id = $script:attemptId
            session_id = 'worker-a'; issue_number = 1; ownership_verified = $true
            branch = 'scheduled-repair/finding'; head_sha = ('a' * 40)
        }
        $null = Invoke-TestAction begin-dispatch @{
            coordinator_token = $script:coordinator; attempt_id = $script:attemptId
        }
        $null = Invoke-TestAction accept-dispatch @{
            attempt_id = $script:attemptId; session_id = 'worker-a'; dispatch_token = $script:dispatch
        }
    }
    function Publish-TestRepair {
        Invoke-TestWorkerSetup
        $null = Invoke-TestAction prepare-publication @{
            attempt_id = $script:attemptId; session_id = 'worker-a'; dispatch_token = $script:dispatch
            expected_head = ('a' * 40); head_sha = ('b' * 40); branch = 'scheduled-repair/finding'
            check_contract_digest = ('c' * 64)
        }
        $null = Invoke-TestAction register-pr @{
            attempt_id = $script:attemptId; session_id = 'worker-a'; dispatch_token = $script:dispatch
            pr_number = 7; head_sha = ('b' * 40); branch = 'scheduled-repair/finding'
        }
        $null = Invoke-TestAction complete-dispatch @{
            attempt_id = $script:attemptId; session_id = 'worker-a'; dispatch_token = $script:dispatch
            phase = 'pr-open'; reason = $null; handled_evidence = @('initial-evidence')
        }
    }
}

Describe 'Durable local transactions' {
    BeforeEach {
        $script:root = Join-Path $TestDrive ([guid]::NewGuid().ToString())
        $script:now = [DateTimeOffset]'2026-09-01T12:00:00Z'
        $script:policy = Get-ScheduledPolicy
        # The fixture namespace is independent of the installation's App-configured prefix.
        $script:policy.managed_branch_prefix = 'scheduled-repair/'
        $script:policy.local.mode = 'repair'
        $script:policy.local.enrolled_machine_id = 'machine-a'
        $script:policy.local.expected_login = 'operator'
        $script:policy.local.allowed_packages = @('cpulist')
        $script:policy.local.allowed_checks = @('mutants')
        foreach ($key in @($script:policy.rollout.prerequisites.Keys)) {
            $script:policy.rollout.prerequisites[$key] = $true
        }
    }
    It 'requires explicit enrollment and never recreates missing existing state' {
        { Invoke-TestAction read } | Should -Throw
        { Invoke-TestAction initialize } | Should -Throw
        $state = Invoke-TestAction initialize @{ operator_approved = $true }
        $state.mode | Should -Be observe
        (Invoke-TestAction initialize).revision | Should -Be $state.revision
        Remove-Item -LiteralPath (Join-Path $script:root 'state.json')
        { Invoke-TestAction initialize @{ operator_approved = $true } } | Should -Throw
    }
    It 'rejects corrupt state without resetting admission history' {
        Initialize-TestExecutor
        $null = Invoke-TestReservation
        Set-Content -LiteralPath (Join-Path $script:root 'state.json') -Value '{"schema_version":1}'
        { Invoke-TestAction read } | Should -Throw
        { Invoke-TestAction initialize @{ operator_approved = $true } } | Should -Throw
    }
    It 'rejects account and executor changes' {
        Initialize-TestExecutor
        { Invoke-ScheduledLocalAction -StateRoot $script:root -Policy $script:policy `
            -ExecutorId 'machine-b' -Login operator -Now $script:now -Action read } | Should -Throw
        { Invoke-ScheduledLocalAction -StateRoot $script:root -Policy $script:policy `
            -ExecutorId 'machine-a' -Login stranger -Now $script:now -Action read } | Should -Throw
    }
    It 'uses exclusive short transactions and leaves old state intact on errors' {
        Initialize-TestExecutor
        $before = Get-Content -LiteralPath (Join-Path $script:root 'state.json') -Raw
        $lock = [IO.File]::Open((Join-Path $script:root 'transaction.lock'),
            [IO.FileMode]::Open, [IO.FileAccess]::ReadWrite, [IO.FileShare]::None)
        try { { Invoke-TestAction read } | Should -Throw } finally { $lock.Dispose() }
        { Invoke-TestAction unknown } | Should -Throw
        (Get-Content -LiteralPath (Join-Path $script:root 'state.json') -Raw) | Should -BeExactly $before
        @(Get-ChildItem -LiteralPath $script:root -Filter '*.tmp').Count | Should -Be 0
        (Invoke-TestAction read).coordinator.token | Should -Be $script:coordinator
    }
    It 'fences stale coordinators without reclaiming their workers' {
        Initialize-TestExecutor
        $null = Invoke-TestReservation
        { Invoke-TestAction acquire-coordinator @{ owner_session_id = 'other' } } | Should -Throw
        $script:now = $script:now.AddHours(1)
        $state = Invoke-TestAction acquire-coordinator @{ owner_session_id = 'coordinator-b' }
        $state.coordinator.token | Should -Not -Be $script:coordinator
        { Invoke-TestAction release-coordinator @{ coordinator_token = $script:coordinator } } | Should -Throw
        $state.attempts.Count | Should -Be 1
        $state.attempts[$script:attemptId].phase | Should -Be reserved
    }
    It 'registers the idle native session before worker acceptance and rejects stale tokens' {
        Initialize-TestExecutor
        $null = Invoke-TestReservation
        { Invoke-TestAction accept-dispatch @{
            attempt_id = $script:attemptId; session_id = 'worker-a'; dispatch_token = $script:dispatch
        } } | Should -Throw
        $null = Invoke-TestAction begin-session-open @{
            coordinator_token = $script:coordinator; attempt_id = $script:attemptId
        }
        { Invoke-TestAction begin-session-open @{
            coordinator_token = $script:coordinator; attempt_id = $script:attemptId
        } } | Should -Throw
        { Invoke-TestAction register-session @{
            coordinator_token = $script:coordinator; attempt_id = $script:attemptId
            session_id = 'human-session'; issue_number = 1; ownership_verified = $false
            branch = 'human'; head_sha = ('a' * 40)
        } } | Should -Throw
    }
    It 'does not resend kickoff after a lost native response' {
        Initialize-TestExecutor
        Invoke-TestWorkerSetup
        { Invoke-TestAction begin-dispatch @{
            coordinator_token = $script:coordinator; attempt_id = $script:attemptId
        } } | Should -Throw
        { Invoke-TestAction accept-dispatch @{
            attempt_id = $script:attemptId; session_id = 'worker-a'; dispatch_token = 'stale'
        } } | Should -Throw
        $state = Invoke-TestAction accept-dispatch @{
            attempt_id = $script:attemptId; session_id = 'worker-a'; dispatch_token = $script:dispatch
        }
        $state.attempts[$script:attemptId].phase | Should -Be working
    }
    It 'records exact publication identity before PR creation and reconciles only that PR' {
        Initialize-TestExecutor
        Invoke-TestWorkerSetup
        $publication = @{
            attempt_id = $script:attemptId; session_id = 'worker-a'; dispatch_token = $script:dispatch
            expected_head = ('a' * 40); head_sha = ('b' * 40); branch = 'scheduled-repair/finding'
            check_contract_digest = ('c' * 64)
        }
        $state = Invoke-TestAction prepare-publication $publication
        $state.attempts[$script:attemptId].phase | Should -Be publishing
        $state.attempts[$script:attemptId].pr_number | Should -BeNullOrEmpty
        { Invoke-TestAction prepare-publication $publication } | Should -Throw
        { Invoke-TestAction register-pr @{
            attempt_id = $script:attemptId; session_id = 'worker-a'; dispatch_token = $script:dispatch
            pr_number = 7; head_sha = ('d' * 40); branch = 'scheduled-repair/finding'
        } } | Should -Throw
    }
    It 'retains blocked ownership across days rather than spawning a competing worker' {
        Initialize-TestExecutor
        Invoke-TestWorkerSetup
        $null = Invoke-TestAction block @{
            coordinator_token = $script:coordinator; attempt_id = $script:attemptId; reason = 'permission'
        }
        $script:now = $script:now.AddDays(2)
        $script:coordinator = (Invoke-TestAction acquire-coordinator @{ owner_session_id = 'next' }).coordinator.token
        { Invoke-TestReservation -Issue 2 -Finding ('e' * 64) } | Should -Throw
        (Invoke-TestAction read).attempts[$script:attemptId].phase | Should -Be blocked
    }
    It 'bounds continuations and fences the previous dispatch while keeping the same session' {
        Initialize-TestExecutor
        Publish-TestRepair
        $data = @{
            coordinator_token = $script:coordinator; attempt_id = $script:attemptId
            evidence_key = 'failed-check-17'; expected_head = ('b' * 40)
            session_id = 'worker-a'; native_idle_verified = $true
        }
        $state = Invoke-TestAction reserve-continuation $data
        $state.attempts[$script:attemptId].session_id | Should -Be worker-a
        $state.attempts[$script:attemptId].continuations.Count | Should -Be 1
        { Invoke-TestAction reserve-continuation $data } | Should -Throw
        { Invoke-TestAction complete-dispatch @{
            attempt_id = $script:attemptId; session_id = 'worker-a'; dispatch_token = $script:dispatch
            phase = 'pr-open'; reason = $null; handled_evidence = @()
        } } | Should -Throw
    }
    It 'charges continuation before delivery and refuses exhausted budgets' {
        Initialize-TestExecutor
        Publish-TestRepair
        $script:policy.local.max_continuations_per_day = 0
        { Invoke-TestAction reserve-continuation @{
            coordinator_token = $script:coordinator; attempt_id = $script:attemptId
            evidence_key = 'failed-check'; expected_head = ('b' * 40)
            session_id = 'worker-a'; native_idle_verified = $true
        } } | Should -Throw
    }
    It 'stops closed-unmerged attempts and requires hosted confirmation after a merge' {
        Initialize-TestExecutor
        Publish-TestRepair
        $data = @{ coordinator_token = $script:coordinator; attempt_id = $script:attemptId
            pr_number = 7; head_sha = ('b' * 40); disposition = 'confirmed'; hosted_confirmation = $false
            native_idle_verified = $true }
        { Invoke-TestAction record-pr-disposition $data } | Should -Throw
        $data.disposition = 'merged'
        (Invoke-TestAction record-pr-disposition $data).attempts[$script:attemptId].phase |
            Should -Be verifying-main
        $data.disposition = 'confirmed'; $data.hosted_confirmation = $true
        (Invoke-TestAction record-pr-disposition $data).attempts[$script:attemptId].phase | Should -Be resolved
        $data.disposition = 'merged'
        (Invoke-TestAction record-pr-disposition $data).attempts[$script:attemptId].phase | Should -Be resolved
        { Invoke-TestReservation } | Should -Throw
    }
    It 'does not free a closed PR slot while a continuation still owns the native session' {
        Initialize-TestExecutor
        Publish-TestRepair
        $state = Invoke-TestAction reserve-continuation @{
            coordinator_token = $script:coordinator; attempt_id = $script:attemptId
            evidence_key = 'new-ci'; expected_head = ('b' * 40)
            session_id = 'worker-a'; native_idle_verified = $true
        }
        $null = Invoke-TestAction begin-dispatch @{
            coordinator_token = $script:coordinator; attempt_id = $script:attemptId
        }
        $null = Invoke-TestAction accept-dispatch @{
            attempt_id = $script:attemptId; session_id = 'worker-a'
            dispatch_token = $state.attempts[$script:attemptId].dispatch.token
        }
        { Invoke-TestAction record-pr-disposition @{
            coordinator_token = $script:coordinator; attempt_id = $script:attemptId
            pr_number = 7; head_sha = ('b' * 40); disposition = 'closed-unmerged'
            hosted_confirmation = $false; native_idle_verified = $false
        } } | Should -Throw
        { Invoke-TestAction record-pr-disposition @{
            coordinator_token = $script:coordinator; attempt_id = $script:attemptId
            pr_number = 7; head_sha = ('b' * 40); disposition = 'closed-unmerged'
            hosted_confirmation = $false; native_idle_verified = $true
        } } | Should -Throw
        { Invoke-TestReservation -Issue 2 } | Should -Throw
    }
    It 'registers a model bootstrap before queued repair and native branch adoption' {
        Initialize-TestExecutor
        $null = Invoke-TestReservation
        $null = Invoke-TestAction begin-session-open @{
            coordinator_token = $script:coordinator; attempt_id = $script:attemptId
        }
        $null = Invoke-TestAction register-session @{
            coordinator_token = $script:coordinator; attempt_id = $script:attemptId
            session_id = 'worker-a'; issue_number = 1; ownership_verified = $true
            branch = 'app-generated-name'; head_sha = ('a' * 40)
        }
        $null = Invoke-TestAction begin-dispatch @{
            coordinator_token = $script:coordinator; attempt_id = $script:attemptId
        }
        $null = Invoke-TestAction accept-dispatch @{
            attempt_id = $script:attemptId; session_id = 'worker-a'; dispatch_token = $script:dispatch
        }
        { Invoke-TestAction prepare-publication @{
            attempt_id = $script:attemptId; session_id = 'worker-a'; dispatch_token = $script:dispatch
            expected_head = ('a' * 40); head_sha = ('b' * 40); branch = 'app-generated-name'
            check_contract_digest = ('c' * 64)
        } } | Should -Throw
        $state = Invoke-TestAction register-branch @{
            attempt_id = $script:attemptId; session_id = 'worker-a'; dispatch_token = $script:dispatch
            expected_branch = 'app-generated-name'; branch = 'scheduled-repair/finding'; head_sha = ('a' * 40)
        }
        $state.attempts[$script:attemptId].branch | Should -Be scheduled-repair/finding
    }
    It 'charges daily starts across generations but not a new generation to old incident limits' {
        Initialize-TestExecutor
        $state = Invoke-TestAction read
        InModuleScope LocalState -Parameters @{ State = $state; Policy = $script:policy; Now = $script:now } {
            foreach ($generation in @(1, 2)) {
                $State.attempts["old-$generation"] = @{
                    phase = 'resolved'; started_at = $Now.AddDays(-1).ToString('o')
                    issue_number = 1; finding_id = ('f' * 64); generation = $generation
                }
            }
            $data = @{ coordinator_token = $State.coordinator.token; issue_number = 1
                finding_id = ('f' * 64); generation = 3; package = 'cpulist'; check_id = 'mutants'
                check_contract_digest = ('c' * 64); evidence_key = 'new-generation' }
            Invoke-LocalStateChange $State $Policy reserve-attempt $data $Now
            $State.attempts.Count | Should -Be 3
            $latest = @($State.attempts.Values | Where-Object { $_.generation -eq 3 })[0]
            $latest.phase = 'resolved'
            $data.generation = 4
            Invoke-LocalStateChange $State $Policy reserve-attempt $data $Now
            $State.attempts.Count | Should -Be 4
            @($State.attempts.Values | Where-Object { $_.generation -eq 4 })[0].phase = 'resolved'
            $data.generation = 5
            { Invoke-LocalStateChange $State $Policy reserve-attempt $data $Now } | Should -Throw
        }
    }
    It 'mirrors canonical version inputs before the initial PR event' {
        Initialize-TestExecutor
        Invoke-TestWorkerSetup
        $null = Invoke-TestAction prepare-publication @{
            attempt_id = $script:attemptId; session_id = 'worker-a'; dispatch_token = $script:dispatch
            expected_head = ('a' * 40); head_sha = ('b' * 40); branch = 'scheduled-repair/finding'
            check_contract_digest = ('c' * 64)
        }
        $expanded = @{ packages = @() }
        $state = Invoke-TestAction record-version-plan @{
            attempt_id = $script:attemptId; session_id = 'worker-a'; dispatch_token = $script:dispatch
            version_evidence = @{ head_sha = ('b' * 40); base_sha = ('d' * 40)
                pre_version_sha = ('a' * 40); decisions = @{}; expanded_plan = $expanded
                expanded_plan_digest = (Get-ScheduledDigest $expanded); plan_digest = ('e' * 64)
                current = $true; description_current = $false }
        }
        $worker = Get-ScheduledWorkerRecord -State $state -AttemptId $script:attemptId
        $worker.version_evidence.pre_version_sha | Should -Be ('a' * 40)
        $worker.pr_number | Should -BeNullOrEmpty
        (Get-ScheduledRepairRecord -State $state -AttemptId $script:attemptId).head_sha | Should -Be ('b' * 40)
    }
    It 'preserves claims and scan history when setup is reapplied or a scan fails' {
        Initialize-TestExecutor
        $null = Invoke-TestReservation
        $null = Invoke-TestAction record-scan @{ coordinator_token = $script:coordinator
            successful = $true; backlog_count = 4; oldest_eligible_at = '2026-08-01T00:00:00Z'
            blocked_conditions = @('active-worker-limit') }
        $before = Invoke-TestAction read
        $script:now = $script:now.AddMinutes(1)
        $null = Invoke-TestAction register-profile @{ operator_approved = $true; profile = $before.profile }
        $state = Invoke-TestAction record-scan @{ coordinator_token = $script:coordinator
            successful = $false; backlog_count = 0; oldest_eligible_at = $null
            blocked_conditions = @('authentication') }
        $state.health.last_successful_scan | Should -Be $before.health.last_successful_scan
        $state.health.backlog_count | Should -Be 4
        $state.attempts.Count | Should -Be 1
    }
}
