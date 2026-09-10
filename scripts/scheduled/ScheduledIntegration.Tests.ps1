#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
# End-to-end compatibility for historical reporter records and registered repairs: retained
# ownership can reach managed PR recognition, but neither a reporter record nor enabled policy
# authorizes new repair admission. Legacy ownership is seeded only in an isolated Pester fixture.
BeforeAll {
    foreach ($name in @('ScheduledContracts', 'ScheduledPlan', 'LocalState', 'LocalInbox', 'ScheduledGate')) {
        Import-Module (Join-Path $PSScriptRoot "$name.psm1")
    }
    Import-Module (Join-Path $PSScriptRoot 'fixtures' 'LocalLegacyState.psm1') -Force
}

Describe 'Historical evidence and retained repair contract' {
    It 'rejects new admission while preserving legacy evidence and registered PR recognition' {
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
        # Historical reporter identity is persisted data, not a new semantic diagnosis.
        $findingId = 'f' * 64
        $record = @{
            schema_version = 1; repository = $policy.repository; repository_id = $policy.repository_id
            finding_id = $findingId; generation = 1; status = 'open'
            package = 'events_once'; platform = $check.platform; applicability = $check
            check_id = $check.id; check_kind = $check.kind
            source_sha = $manifest.source_sha; controller_sha = $manifest.controller_sha
            check_contract_digest = $manifest.check_contract_digest
            observation = @{
                run_id = 10; run_attempt = 1; run_number = 5
                workflow_path = '.github/workflows/full-deep-validation.yml'
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
            user = @{ login = $policy.reporter_login }; body = Write-ScheduledRecord -Kind reporter -Record $record
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
        $decision.eligible.Count | Should -Be 0
        $decision.blocked_conditions | Should -Contain ai-triage-unavailable
        $state = Invoke-ScheduledLocalAction @action -Action acquire-coordinator -Data @{ owner_session_id = 'coordinator' }
        $coordinatorToken = $state.coordinator.token
        $before = Get-Content -LiteralPath (Join-Path $action.StateRoot 'state.json') -Raw
        { Invoke-ScheduledLocalAction @action -Action reserve-attempt -Data @{
            coordinator_token = $coordinatorToken; issue_number = $incident.issue_number
            finding_id = $incident.finding_id; generation = $incident.generation
            package = $incident.package; check_id = $incident.check_id; check_kind = $incident.check_kind
            check_contract_digest = $incident.check_contract_digest; evidence_key = Get-ScheduledDigest $incident.evidence
        } } | Should -Throw -ExceptionType ([NotSupportedException])
        (Get-Content -LiteralPath (Join-Path $action.StateRoot 'state.json') -Raw) | Should -BeExactly $before
        $attempt = Add-TestLegacyAttempt -StateRoot $action.StateRoot -StartedAt $now `
            -Issue $issue.number -Finding $findingId -CheckId $check.id -CheckKind $check.kind `
            -ContractDigest $manifest.check_contract_digest -EvidenceKey (Get-ScheduledDigest $incident.evidence)
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
        $registered = Get-ScheduledInboxDecision -Policy $policy -State $state -Incidents @() -Now $now
        $registered.registered_attempts.session_id | Should -Be worker
        $registered.registered_attempts.branch | Should -Be $branch
        $registered.blocked_conditions | Should -Contain ai-triage-unavailable
        $repair.explanation | Should -BeExactly $worker.explanation
        (Read-ScheduledRecord $issue.body reporter).evidence.replay.seed | Should -Be 31
        $incident.evidence.summary | Should -BeExactly $record.evidence.summary
        $record.status = 'needs-human'
        $issue.body = Write-ScheduledRecord -Kind reporter -Record $record
        (Get-ScheduledRepairScope -PullRequest $pr -Issue $issue -Worker $worker -Policy $policy).managed | Should -BeTrue
        $incident.status = 'needs-human'
        $freshState = $state.Clone()
        $freshState.attempts = @{}
        $freshDecision = Get-ScheduledInboxDecision -Policy $policy -State $freshState -Incidents @($incident) -Now $now
        $freshDecision.eligible.Count | Should -Be 0
        $freshDecision.deferred.reason | Should -Contain reporter-disposition
    }
}
