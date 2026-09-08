#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
# Protects the admission-scan contract: reporter-authored, held or already-owned incidents must be
# deferred with an explicit reason rather than silently skipped or wrongly admitted, and a scan
# with any rejected/malformed evidence must surface as a blocked condition rather than a clean run.
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
                workflow_path = '.github/workflows/scheduled-validation.yml'
                completed_at = '2026-09-01T00:00:00Z'; outcome = 'missed' }
            evidence = @{ manifest = 'run-artifact'; replay = @{ mutant = 'fingerprint' }; summary = 'Missed mutant' }
            confirmation = $null
        }
    }
}
Describe 'Full-backlog deterministic selection' {
    BeforeEach {
        $policy = Get-ScheduledPolicy
        $policy.local.allowed_packages = @('cpulist')
        $policy.local.allowed_checks = @('mutants', 'miri')
        $script:state = @{ attempts = @{}; mode = 'observe'; executor_id = 'machine'; profile = $null }
    }
    It 'retains old unresolved incidents and prioritizes correctness then age' {
        $result = Get-ScheduledInboxDecision -Policy $policy -State $state -Now '2026-09-08T12:00:00Z' `
            -Incidents @((Get-TestIncident 3 mutants '2026-09-01T00:00:00Z'),
                (Get-TestIncident 1 mutants), (Get-TestIncident 2 miri))
        $result.eligible.issue_number | Should -Be @(2, 1, 3)
        $result.eligible[0].check_id | Should -Be miri-ubuntu-latest-1
        $result.eligible[1].check_id | Should -Be mutants-ubuntu-latest-1
        $result.eligible[1].check_kind | Should -Be mutants
        $result.oldest_eligible_at | Should -Be '2026-08-01T00:00:00Z'
        $result.blocked_conditions | Should -Contain observe
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
    It 'uses the explicit family while preserving a package-specific many-seed catalog ID' {
        $incident = Get-TestIncident 4 miri-many
        $incident.check_id = 'miri-many-events_once-2'
        $incident.package = 'events_once'
        $policy.local.allowed_packages += 'events_once'
        $policy.local.allowed_checks += 'miri-many'
        $result = Get-ScheduledInboxDecision -Policy $policy -State $state -Now '2026-09-08T12:00:00Z' `
            -Incidents @($incident)
        $result.eligible.Count | Should -Be 1
        $result.eligible[0].check_id | Should -Be miri-many-events_once-2
        $result.eligible[0].check_kind | Should -Be miri-many
        $result.eligible[0].priority | Should -Be 0
    }
    It 'returns registered PR attempts even when their issue is absent from the open queue' {
        $state.attempts.a = @{ issue_number = 9; attempt_id = 'a'; phase = 'pr-open'
            pr_number = 10; started_at = '2026-09-01T00:00:00Z' }
        $result = Get-ScheduledInboxDecision -Policy $policy -State $state -Now '2026-09-08T12:00:00Z' -Incidents @()
        $result.registered_attempts.pr_number | Should -Be 10
        $result.blocked_conditions | Should -Contain active-worker-limit
    }
    It 'admits a validated newer generation after resolution but not a duplicate or active overlap' {
        $incident = Get-TestIncident 1
        $incident.generation = 2
        $state.attempts.old = @{ issue_number = 1; finding_id = $incident.finding_id
            generation = 1; attempt_id = 'old'; phase = 'resolved'; started_at = '2026-08-01T00:00:00Z' }
        $result = Get-ScheduledInboxDecision -Policy $policy -State $state -Now '2026-09-08T12:00:00Z' `
            -Incidents @($incident)
        $result.eligible.Count | Should -Be 1
        $state.attempts.old.phase = 'pr-open'
        (Get-ScheduledInboxDecision -Policy $policy -State $state -Now '2026-09-08T12:00:00Z' `
            -Incidents @($incident)).eligible.Count | Should -Be 0
        $state.attempts.old.phase = 'closed-unmerged'; $incident.generation = 1
        (Get-ScheduledInboxDecision -Policy $policy -State $state -Now '2026-09-08T12:00:00Z' `
            -Incidents @($incident)).eligible.Count | Should -Be 0
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
        InModuleScope LocalInbox -Parameters @{ Body = $body; FixtureRoot = (Join-Path $TestDrive 'state') } {
            $script:testBody = $Body
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
                            path = '.github/workflows/scheduled-validation.yml'; status = 'completed'
                            head_branch = 'main'; head_sha = ('a' * 40) }
                    }
                    '*/workflows/scheduled-validation.yml' { return @{ state = 'disabled_inactivity' } }
                    default { throw "Unexpected endpoint: $Endpoint" }
                }
            }
            $result = Invoke-ScheduledInbox -ExecutorId machine -Now '2026-09-08T12:00:00Z' `
                -StateRoot $script:fixtureRoot
            $result.rejected.Count | Should -Be 1 -Because ($result.rejected | ConvertTo-Json -Compress)
            $result.deferred.issue_number | Should -Be 1
            $result.blocked_conditions | Should -Contain hosted-staged
            $result.blocked_conditions | Should -Contain missing-or-invalid-evidence
            Should -Invoke Invoke-ScheduledApi -Exactly 1 -ParameterFilter {
                $Endpoint -eq 'repos/folo-rs/folo/actions/runs/17/attempts/1'
            }
            Should -Invoke Invoke-ScheduledLocalAction -Exactly 1 -ParameterFilter {
                $StateRoot -ceq $script:fixtureRoot
            }
            Mock ConvertTo-ScheduledIncident { throw [InvalidOperationException]::new('Controller failure') }
            { Invoke-ScheduledInbox -ExecutorId machine -Now '2026-09-08T12:00:00Z' `
                -StateRoot $script:fixtureRoot } | Should -Throw
        }
    }
    It 'fails explicitly on authentication and never records successful scan output' {
        InModuleScope LocalInbox -Parameters @{ FixtureRoot = (Join-Path $TestDrive 'state') } {
            Mock Invoke-ScheduledApi { throw 'authentication denied' }
            { Invoke-ScheduledInbox -ExecutorId machine -StateRoot $FixtureRoot } | Should -Throw
        }
    }
}
