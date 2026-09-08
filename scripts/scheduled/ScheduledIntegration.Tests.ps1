#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
# End-to-end regression for the reporter-to-worker contract: a durable finding record written by
# the reporting side must be readable, admissible and recognizable by the Local admission modules
# across the full reporter -> Local admission -> recognition chain, not just within one module's
# own unit tests where the other side's format is assumed rather than exercised.
BeforeAll {
    foreach ($name in @('ScheduledContracts', 'ScheduledPlan', 'ScheduledReport', 'LocalState', 'LocalInbox', 'ScheduledGate')) {
        Import-Module (Join-Path $PSScriptRoot "$name.psm1")
    }
}

Describe 'Reporter to registered repair contract' {
    It 'carries durable evidence through Local admission to recognition on the initial PR event' {
        $policy = Get-ScheduledPolicy
        $policy.local.mode = 'repair'
        $policy.local.enrolled_machine_id = 'fixture-executor'
        $policy.local.allowed_packages = @('events_once')
        $policy.local.allowed_checks = @('miri-many')
        $policy.repair.allowed_packages = @('events_once')
        foreach ($name in @($policy.rollout.prerequisites.Keys)) {
            $policy.rollout.prerequisites[$name] = $true
        }
        $now = [datetimeoffset]'2026-09-08T12:00:00Z'
        $manifest = Get-ScheduledCheckManifest -SourceSha ('a' * 40) -ControllerSha ('a' * 40) `
            -ContractDigest ('c' * 64)
        $check = @($manifest.checks | Where-Object id -CEQ 'miri-many-events_once-2')[0]
        $findingId = Get-ScheduledFindingId -Repository $policy.repository -Identity @{
            kind = $check.kind; package = 'events_once'; platform = $check.platform; test = 'slot::transition'
            path = 'packages\events_once\src\slot.rs'; function = 'slot::transition'
            mutation = $null; seed = 31; flags = $check.flags
        }
        $record = @{
            schema_version = 1; repository = $policy.repository; repository_id = $policy.repository_id
            finding_id = $findingId; generation = 1; status = 'open'
            package = 'events_once'; platform = $check.platform; applicability = $check
            check_id = $check.id; check_kind = $check.kind
            source_sha = $manifest.source_sha; controller_sha = $manifest.controller_sha
            check_contract_digest = $manifest.check_contract_digest
            observation = @{
                run_id = 10; run_attempt = 1; run_number = 5
                workflow_path = '.github/workflows/scheduled-validation.yml'
                completed_at = $now.ToString('o'); outcome = 'findings'
            }
            evidence = @{
                manifest = $check; replay = @{ test_filter = 'slot::transition'; seed = 31 }
                summary = 'The ownership transition exposes an invalid access.'
            }
            confirmation = $null
        }
        $issue = @{
            number = 42; state = 'open'; created_at = $now.ToString('o'); labels = @()
            user = @{ login = $policy.reporter_login }; body = Get-ScheduledFindingBody $record
        }
        $run = @{
            id = 10; run_attempt = 1; run_number = 5; status = 'completed'
            repository = @{ id = $policy.repository_id; full_name = $policy.repository }
            path = $record.observation.workflow_path; head_branch = 'main'; head_sha = $manifest.source_sha
        }
        $incident = ConvertTo-ScheduledIncident -Issue $issue -Comments @() `
            -Repository $policy.repository -Run $run
        $incident.created_at = $issue.created_at
        $incident.held = $false
        $action = @{
            StateRoot = (Join-Path $TestDrive 'enrolled-state'); Policy = $policy
            ExecutorId = $policy.local.enrolled_machine_id; Login = $policy.local.expected_login; Now = $now
        }
        $null = Invoke-ScheduledLocalAction @action -Action initialize -Data @{ operator_approved = $true }
        $null = Invoke-ScheduledLocalAction @action -Action register-profile -Data @{
            operator_approved = $true
            profile = @{
                automation_id = 'fixture-automation'; project_id = 'fixture-project'; host_id = 'fixture-host'
                executor_id = $action.ExecutorId; login = $action.Login; cadence_cron = $policy.local.cadence_cron
                timezone = 'Europe/Tallinn'; enabled = $true; policy_digest = Get-ScheduledDigest $policy
                prompt_digest = 'fixture-prompt'; coordinator_model = 'intake'; repair_model = 'repair'
            }
        }
        $state = Invoke-ScheduledLocalAction @action -Action set-mode -Data @{ operator_approved = $true; mode = 'repair' }
        $decision = Get-ScheduledInboxDecision -Policy $policy -State $state -Incidents @($incident) -Now $now
        $decision.eligible.Count | Should -Be 1
        $eligible = $decision.eligible[0]
        $state = Invoke-ScheduledLocalAction @action -Action acquire-coordinator -Data @{ owner_session_id = 'coordinator' }
        $coordinatorToken = $state.coordinator.token
        $state = Invoke-ScheduledLocalAction @action -Action reserve-attempt -Data @{
            coordinator_token = $coordinatorToken; issue_number = $eligible.issue_number
            finding_id = $eligible.finding_id; generation = $eligible.generation
            package = $eligible.package; check_id = $eligible.check_id; check_kind = $eligible.check_kind
            check_contract_digest = $eligible.check_contract_digest; evidence_key = Get-ScheduledDigest $eligible.evidence
        }
        $attempt = @($state.attempts.Values)[0]
        $workerData = @{ attempt_id = $attempt.attempt_id; session_id = 'worker'; dispatch_token = $attempt.dispatch.token }
        $coordinatorData = @{ coordinator_token = $coordinatorToken; attempt_id = $attempt.attempt_id }
        $null = Invoke-ScheduledLocalAction @action -Action begin-session-open -Data $coordinatorData
        $branch = "$($policy.managed_branch_prefix)fixture"
        $null = Invoke-ScheduledLocalAction @action -Action register-session -Data ($coordinatorData + @{
            session_id = 'worker'; issue_number = $issue.number; ownership_verified = $true
            branch = $branch; head_sha = $manifest.source_sha
        })
        $null = Invoke-ScheduledLocalAction @action -Action begin-dispatch -Data $coordinatorData
        $null = Invoke-ScheduledLocalAction @action -Action accept-dispatch -Data $workerData
        $state = Invoke-ScheduledLocalAction @action -Action prepare-publication -Data ($workerData + @{
            expected_head = $manifest.source_sha; head_sha = 'b' * 40; branch = $branch
            check_contract_digest = $manifest.check_contract_digest
            explanation = 'The repaired transition retains ownership until notification completes.'
        })
        $worker = Get-ScheduledWorkerRecord -State $state -AttemptId $attempt.attempt_id
        $repair = Get-ScheduledRepairRecord -State $state -AttemptId $attempt.attempt_id -Policy $policy
        $worker.pr_number | Should -BeNullOrEmpty
        $pr = @{
            number = 43; user = @{ login = $policy.worker_login }; base = @{ ref = 'main' }
            head = @{ ref = $branch; sha = $worker.head_sha; repo = @{ id = $policy.repository_id } }
            body = Write-ScheduledRecord -Kind repair -Record $repair
        }
        $scope = Get-ScheduledRepairScope -PullRequest $pr -Issue $issue -Worker $worker -Policy $policy
        $scope.managed | Should -BeTrue
        $scope.packages | Should -Be @('events_once')
        $scope.check_ids.Count | Should -Be 4
        $scope.check_ids | Should -Contain 'miri-many-events_once-4'
        $repair.explanation | Should -BeExactly $worker.explanation
        (Read-ScheduledRecord $issue.body reporter).evidence.replay.seed | Should -Be 31
        $incident.evidence.summary | Should -BeExactly $record.evidence.summary
        $record.status = 'needs-human'
        $issue.body = Get-ScheduledFindingBody $record
        (Get-ScheduledRepairScope -PullRequest $pr -Issue $issue -Worker $worker -Policy $policy).managed | Should -BeTrue
        $incident.status = 'needs-human'
        $freshState = $state.Clone()
        $freshState.attempts = @{}
        $freshDecision = Get-ScheduledInboxDecision -Policy $policy -State $freshState -Incidents @($incident) -Now $now
        $freshDecision.eligible.Count | Should -Be 0
        $freshDecision.deferred.reason | Should -Contain reporter-disposition
    }
}
