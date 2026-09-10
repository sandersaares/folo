#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
# Protects the hosted-evidence-only boundary: no policy enables unimplemented AI triage or new
# repairs. Legacy records remain readable for reconciliation, registered work and coverage stay
# visible, and raw run evidence or malformed input cannot masquerade as repair authorization.
BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'LocalInbox.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'ScheduledContracts.psm1') -Force

    function Get-TestIncident {
        param([int] $Number, [string] $Check = 'mutants', [string] $Created = '2026-08-01T00:00:00Z')
        return @{
            schema_version = 1; repository = 'folo-rs/folo'; repository_id = 850321188
            finding_id = ('f' * 64); generation = 1; status = 'open'
            check_id = "$Check-ubuntu-latest-1"; check_kind = $Check
            package = 'cpulist'; platform = 'linux'; applicability = 'source'
            source_sha = ('a' * 40); controller_sha = ('a' * 40); check_contract_digest = ('c' * 64)
            issue_number = $Number; created_at = $Created; held = $false; validated_worker = $null
            observation = @{ run_id = 17; run_attempt = 1; run_number = 9
                workflow_path = '.github/workflows/full-deep-validation.yml'
                completed_at = '2026-09-01T00:00:00Z'; outcome = 'missed' }
            evidence = @{ manifest = 'run-artifact'; replay = @{ mutant = 'fingerprint' }; summary = 'Missed mutant' }
            confirmation = $null
        }
    }
}
Describe 'Fail-closed repair capability and retained work' {
    BeforeEach {
        $policy = Get-ScheduledPolicy
        $policy.local.allowed_packages = @('cpulist')
        $policy.local.allowed_checks = @('mutants', 'miri')
        $script:state = @{ attempts = @{}; mode = 'observe'; executor_id = 'machine'; profile = $null }
    }
    It 'defers the entire historical backlog instead of advertising repair-ready evidence' {
        $result = Get-ScheduledInboxDecision -Policy $policy -State $state -Now '2026-09-08T12:00:00Z' `
            -Incidents @((Get-TestIncident 3 mutants '2026-09-01T00:00:00Z'),
                (Get-TestIncident 1 mutants), (Get-TestIncident 2 miri))
        $result.eligible.Count | Should -Be 0
        $result.deferred.issue_number | Should -Be @(3, 1, 2)
        @($result.deferred | Where-Object reason -CEQ ai-triage-unavailable).Count | Should -Be 3
        $result.oldest_eligible_at | Should -BeNullOrEmpty
        $result.blocked_conditions | Should -Contain ai-triage-unavailable
        $result.blocked_conditions | Should -Contain observe
    }
    It 'does not enable admission through rollout assertions mode or complete allowlists' {
        $policy.local.mode = 'repair'
        $policy.local.enrolled_machine_id = $state.executor_id
        $policy.local.allowed_checks = @('mutants', 'miri', 'miri-many', 'careful')
        $policy.rollout.hosted_execution_enabled = $true
        $policy.rollout.reporting_enabled = $true
        foreach ($key in @($policy.rollout.prerequisites.Keys)) {
            $policy.rollout.prerequisites[$key] = $true
        }
        $policy.rollout.prerequisites.ai_triage = $true
        $policy.local.ai_triage_available = $true
        $state.mode = 'repair'
        $state.profile = @{ enabled = $true }
        $incidents = @($policy.local.allowed_checks | ForEach-Object { Get-TestIncident 1 $_ })
        foreach ($inputQueue in @(@{ incidents = $incidents }, @{ incidents = @() })) {
            $result = Get-ScheduledInboxDecision -Policy $policy -State $state `
                -Now '2026-09-08T12:00:00Z' -Incidents $inputQueue.incidents
            $result.eligible.Count | Should -Be 0
            $result.blocked_conditions | Should -Be @('ai-triage-unavailable')
        }
    }
    It 'keeps ownership holds unsupported scope and dispositions out of fresh starts' {
        $held = Get-TestIncident 1; $held.held = $true
        $owned = Get-TestIncident 2; $owned.validated_worker = @{ session_id = 'existing' }
        $unsupported = Get-TestIncident 3; $unsupported.package = 'not-approved'
        $confirmed = Get-TestIncident 4; $confirmed.status = 'confirmed'
        $result = Get-ScheduledInboxDecision -Policy $policy -State $state -Now '2026-09-08T12:00:00Z' `
            -Incidents @($held, $owned, $unsupported, $confirmed)
        $result.eligible.Count | Should -Be 0
        $result.deferred.reason | Should -Contain human-hold
        $result.deferred.reason | Should -Contain reconcile-owned-work
        $result.deferred.reason | Should -Contain outside-approved-scope
        $result.deferred.reason | Should -Contain reporter-disposition
    }
    It 'retains historical replay identity without converting it into completed triage' {
        $incident = Get-TestIncident 4 miri-many
        $incident.check_id = 'miri-many-events_once-2'
        $incident.package = 'events_once'
        $policy.local.allowed_packages += 'events_once'
        $policy.local.allowed_checks += 'miri-many'
        $result = Get-ScheduledInboxDecision -Policy $policy -State $state -Now '2026-09-08T12:00:00Z' `
            -Incidents @($incident)
        $result.eligible.Count | Should -Be 0
        $result.deferred.reason | Should -Be ai-triage-unavailable
        $incident.check_id | Should -Be miri-many-events_once-2
        $incident.check_kind | Should -Be miri-many
        $incident.ContainsKey('triage') | Should -BeFalse
    }
    It 'returns registered PR attempts even when their issue is absent from the open queue' {
        $state.attempts.a = @{ issue_number = 9; attempt_id = 'a'; phase = 'pr-open'
            pr_number = 10; started_at = '2026-09-01T00:00:00Z' }
        $result = Get-ScheduledInboxDecision -Policy $policy -State $state -Now '2026-09-08T12:00:00Z' -Incidents @()
        $result.registered_attempts.pr_number | Should -Be 10
        $result.blocked_conditions | Should -Contain ai-triage-unavailable
        $result.blocked_conditions | Should -Contain active-worker-limit
    }
    It 'retains all registered phases even when the issue queue is empty' {
        $phases = @('reserved', 'opening-session', 'session-registered', 'dispatching', 'working',
            'publishing', 'pr-open', 'awaiting-review', 'blocked', 'verifying-main', 'resolved', 'closed-unmerged')
        foreach ($phase in $phases) {
            $state.attempts[$phase] = @{ attempt_id = $phase; phase = $phase
                started_at = '2026-09-08T00:00:00Z'; session_id = "session-$phase"; pr_number = 10
                branch = "retained-$phase"; continuations = @(@{ evidence_key = 'consumed' }) }
        }
        $before = Get-ScheduledDigest $state
        $result = Get-ScheduledInboxDecision -Policy $policy -State $state `
            -Now '2026-09-08T12:00:00Z' -Incidents @()
        $result.registered_attempts.Count | Should -Be $phases.Count
        foreach ($attempt in $result.registered_attempts) {
            (Get-ScheduledDigest $attempt) | Should -Be (Get-ScheduledDigest $state.attempts[$attempt.attempt_id])
        }
        (Get-ScheduledDigest $state) | Should -Be $before
        $result.blocked_conditions | Should -Contain start-budget
    }
    It 'does not admit recurrence and still identifies duplicate or overlapping ownership' {
        $incident = Get-TestIncident 1
        $incident.generation = 2
        $state.attempts.old = @{ issue_number = 1; finding_id = $incident.finding_id
            generation = 1; attempt_id = 'old'; phase = 'resolved'; started_at = '2026-08-01T00:00:00Z' }
        $result = Get-ScheduledInboxDecision -Policy $policy -State $state -Now '2026-09-08T12:00:00Z' `
            -Incidents @($incident)
        $result.eligible.Count | Should -Be 0
        $result.deferred.reason | Should -Be ai-triage-unavailable
        $state.attempts.old.phase = 'pr-open'
        (Get-ScheduledInboxDecision -Policy $policy -State $state -Now '2026-09-08T12:00:00Z' `
            -Incidents @($incident)).deferred.reason | Should -Be reconcile-owned-work
        $state.attempts.old.phase = 'closed-unmerged'; $incident.generation = 1
        (Get-ScheduledInboxDecision -Policy $policy -State $state -Now '2026-09-08T12:00:00Z' `
            -Incidents @($incident)).deferred.reason | Should -Be reconcile-owned-work
    }
}
Describe 'GitHub pagination and validated intake' {
    It 'imports only the public entrypoint in a fresh process without unloading shared dependencies' {
        $output = & pwsh -NoProfile -NonInteractive -File (Join-Path $PSScriptRoot 'fixtures' 'LocalImportSmoke.ps1')
        $LASTEXITCODE | Should -Be 0
        $output | Should -Contain 'local-import-smoke-ok'
    }
    It 'consumes all pages rather than a fixed recent-window or maximum issue count' {
        InModuleScope LocalGitHub {
            Mock Invoke-ScheduledApi {
                return ,@(@(@{ number = 1 }, @{ number = 2 }), @(@{ number = 1001 }))
            }
            $items = Get-ScheduledApiCollection -Endpoint 'test'
            $items.number | Should -Be @(1, 2, 1001)
            Should -Invoke Invoke-ScheduledApi -Exactly 1 -ParameterFilter { $Paginate }
        }
    }
    It 'rejects malformed pages rather than silently truncating the scan' {
        InModuleScope LocalGitHub {
            Mock Invoke-ScheduledApi { return ,@(@{ number = 1 }) }
            { Get-ScheduledApiCollection -Endpoint 'test' } | Should -Throw
        }
    }
    It 'validates originating run provenance while excluding PRs and exposing invalid records' {
        $record = Get-TestIncident 1
        $body = Write-ScheduledRecord -Record $record -Kind reporter
        $runBody = Write-ScheduledRecord -Record @{ schema_version = 1 } -Kind run
        InModuleScope LocalInbox -Parameters @{
            Body = $body; RunBody = $runBody; FixtureRoot = (Join-Path $TestDrive 'state')
        } {
            $script:testBody = $Body
            $script:runBody = $RunBody
            $script:fixtureRoot = $FixtureRoot
            Mock Get-ScheduledStateRoot { throw 'The fixture must supply its isolated state root.' }
            Mock Invoke-ScheduledLocalAction {
                return @{ attempts = @{}; mode = 'observe'; executor_id = 'machine'; profile = $null }
            }
            Mock Get-ScheduledApiCollection {
                param($Endpoint)
                if ($Endpoint.Contains('/comments?')) { return ,@() }
                if ($Endpoint.Contains('labels=scheduled-coverage')) { return ,@() }
                return ,@(
                    @{ number = 1; body = $script:testBody; user = @{ login = 'github-actions[bot]' }
                        created_at = '2026-08-01T00:00:00Z'; labels = @() }
                    @{ number = 2; body = 'untrusted'; user = @{ login = 'human' }
                        created_at = '2026-08-01T00:00:00Z'; labels = @() }
                    @{ number = 3; pull_request = @{ url = 'ignored' } }
                    @{ number = 4; body = $script:runBody; user = @{ login = 'github-actions[bot]' }
                        created_at = '2026-08-01T00:00:00Z'
                        labels = @(@{ name = 'scheduled-finding' }, @{ name = 'scheduled-run-failure' }) }
                )
            }
            Mock Invoke-ScheduledApi {
                param($Endpoint)
                switch -Wildcard ($Endpoint) {
                    'user' { return @{ login = 'sandersaares' } }
                    'repos/folo-rs/folo' { return @{ id = 850321188; full_name = 'folo-rs/folo' } }
                    '*/actions/runs/17/attempts/1' {
                        return @{ repository = @{ id = 850321188; full_name = 'folo-rs/folo' }
                            id = 17; run_attempt = 1; run_number = 9
                            path = '.github/workflows/full-deep-validation.yml'; status = 'completed'
                            head_branch = 'main'; head_sha = ('a' * 40) }
                    }
                    '*/workflows/full-deep-validation.yml' { return @{ state = 'disabled_inactivity' } }
                    default { throw "Unexpected endpoint: $Endpoint" }
                }
            }
            $result = Invoke-ScheduledInbox -ExecutorId machine -Now '2026-09-08T12:00:00Z' `
                -StateRoot $script:fixtureRoot
            $result.rejected.Count | Should -Be 2 -Because ($result.rejected | ConvertTo-Json -Compress)
            $result.rejected.reason | Should -Contain run-evidence-not-repair-authorization
            $result.deferred.issue_number | Should -Be 1
            $result.eligible.Count | Should -Be 0
            $result.blocked_conditions | Should -Contain ai-triage-unavailable
            $result.blocked_conditions | Should -Contain github-schedule-disabled
            $result.blocked_conditions | Should -Contain hosted-planning-evidence-missing
            $result.blocked_conditions | Should -Contain missing-or-invalid-evidence
            Should -Invoke Invoke-ScheduledApi -Exactly 1 -ParameterFilter {
                $Endpoint -eq 'repos/folo-rs/folo/actions/runs/17/attempts/1'
            }
            Should -Invoke Invoke-ScheduledLocalAction -Exactly 1 -ParameterFilter {
                $StateRoot -ceq $script:fixtureRoot
            }
            Should -Invoke Get-ScheduledApiCollection -Exactly 0 -ParameterFilter {
                $Endpoint -like '*/issues/4/comments?*'
            }
            Mock ConvertTo-ScheduledIncident { throw [InvalidOperationException]::new('Controller failure') }
            { Invoke-ScheduledInbox -ExecutorId machine -Now '2026-09-08T12:00:00Z' `
                -StateRoot $script:fixtureRoot } | Should -Throw
        }
    }
    It 'accepts only full hosted planning independently of unavailable repair admission' -TestCases @(
        @{ Workflow = 'full-deep-validation.yml'; Accepted = $true }
        @{ Workflow = 'selected-deep-validation.yml'; Accepted = $false }
    ) {
        param($Workflow, $Accepted)
        $policyPath = Join-Path $TestDrive 'coverage-policy.json'
        $policy = Get-ScheduledPolicy
        $policy.rollout.hosted_execution_enabled = $true
        $policy | ConvertTo-Json -Depth 40 | Set-Content -LiteralPath $policyPath
        InModuleScope LocalInbox -Parameters @{
            FixtureRoot = (Join-Path $TestDrive 'state'); PolicyPath = $policyPath
            Workflow = $Workflow; Accepted = $Accepted
        } {
            $script:planningWorkflow = $Workflow
            Mock Invoke-ScheduledLocalAction {
                return @{ attempts = @{}; mode = 'observe'; executor_id = 'machine'; profile = $null }
            }
            Mock Get-ScheduledApiCollection {
                param($Endpoint)
                if ($Endpoint.Contains('labels=scheduled-coverage')) {
                    return ,@(@{ number = 5; user = @{ login = 'github-actions[bot]' }; body = (
                        Write-ScheduledRecord -Kind coverage -Record @{
                            schema_version = 1; repository = 'folo-rs/folo'; repository_id = 850321188
                            last_plan = @{
                                planned_at = '2026-09-08T11:00:00Z'; source_sha = ('a' * 40)
                                workflow_path = ".github/workflows/$script:planningWorkflow"
                            }
                        }) })
                }
                return ,@()
            }
            Mock Invoke-ScheduledApi {
                param($Endpoint)
                switch -Wildcard ($Endpoint) {
                    'user' { return @{ login = 'sandersaares' } }
                    'repos/folo-rs/folo' { return @{ id = 850321188; full_name = 'folo-rs/folo' } }
                    '*/workflows/full-deep-validation.yml' { return @{ state = 'active' } }
                    default { throw "Unexpected endpoint: $Endpoint" }
                }
            }
            $result = Invoke-ScheduledInbox -ExecutorId machine -Now '2026-09-08T12:00:00Z' `
                -StateRoot $FixtureRoot -PolicyPath $PolicyPath
            $result.successful_scan | Should -BeTrue
            $result.hosted_schedule_enabled | Should -BeTrue
            if ($Accepted) {
                [DateTimeOffset]$result.last_hosted_plan.planned_at | Should -Be ([DateTimeOffset]'2026-09-08T11:00:00Z')
                @($result.blocked_conditions | Where-Object { $_.StartsWith('hosted-') }).Count | Should -Be 0
            } else {
                $result.last_hosted_plan | Should -BeNullOrEmpty
                $result.blocked_conditions | Should -Contain hosted-planning-evidence-invalid
            }
            $result.blocked_conditions | Should -Contain ai-triage-unavailable
        }
    }
    It 'fails explicitly on authentication and never records successful scan output' {
        InModuleScope LocalInbox -Parameters @{ FixtureRoot = (Join-Path $TestDrive 'state') } {
            Mock Invoke-ScheduledApi { throw 'authentication denied' }
            { Invoke-ScheduledInbox -ExecutorId machine -StateRoot $FixtureRoot } | Should -Throw
        }
    }
}
