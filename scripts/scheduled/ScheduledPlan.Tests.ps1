#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'ScheduledPlan.psm1') -Force
    function Get-TestManifest {
        Get-ScheduledCheckManifest -SourceSha ('a' * 40) -ControllerSha ('b' * 40) -ContractDigest ('c' * 64)
    }
    function Get-TestResult($manifest) {
        @($manifest.checks | ForEach-Object {
            @{
                schema_version = 1; check_id = $_.id; actual_scope = $_; outcome = 'passed'
                source_sha = $manifest.source_sha; controller_sha = $manifest.controller_sha
                check_contract_digest = $manifest.check_contract_digest
            }
        })
    }
}
Describe 'Expected deep scope' {
    It 'keeps version movement and repair admission separate from checker compatibility' {
        InModuleScope ScheduledPlan {
            Mock Get-FileHash { @{ Hash = $LiteralPath } }
            Mock Get-ScheduledPolicy { throw 'Admission policy is not a checker input.' }
            Get-ScheduledContractDigest -Root $TestDrive | Should -Match '^[0-9a-f]{64}$'
            Should -Invoke Get-FileHash -Times 0 -ParameterFilter { $LiteralPath -like '*Cargo.toml' }
            Should -Invoke Get-FileHash -Times 1 -ParameterFilter { $LiteralPath -like '*constants.env' }
            Should -Invoke Get-FileHash -Times 1 -ParameterFilter { $LiteralPath -like '*rust-toolchain.toml' }
            Should -Invoke Get-FileHash -Times 1 -ParameterFilter { $LiteralPath -like '*mutants.toml' }
            Should -Invoke Get-FileHash -Times 1 -ParameterFilter { $LiteralPath -like '*Read-MutationConfig.py' }
            Should -Invoke Get-ScheduledPolicy -Times 0
        }
    }
    It 'preserves ordinary Miri platforms mutation shards many-seed budgets and careful platforms' {
        $manifest = Get-TestManifest
        $manifest.checks.Count | Should -Be 32
        @($manifest.checks | Where-Object kind -EQ mutants).Count | Should -Be 16
        @($manifest.checks | Where-Object kind -EQ miri).Count | Should -Be 4
        @($manifest.checks | Where-Object kind -EQ careful).Count | Should -Be 2
        @($manifest.checks | Where-Object { $_.id -like 'miri-many-events_once-*' }).seed_range |
            Should -Be @('0..16', '16..32', '32..48', '48..64')
    }
    It 'cannot describe partial runs as full coverage' {
        { Get-ScheduledCheckManifest -SourceSha ('a' * 40) -ControllerSha ('b' * 40) -ContractDigest x -Packages foo } |
            Should -Throw
    }
    It 'rejects every missing duplicate stale unknown or skipped required result' {
        $manifest = Get-TestManifest
        $results = Get-TestResult $manifest
        (Test-ScheduledManifest -Manifest $manifest -Results $results).successful | Should -BeTrue
        (Test-ScheduledManifest -Manifest $manifest -Results $results[1..31]).complete | Should -BeFalse
        (Test-ScheduledManifest -Manifest $manifest -Results ($results + $results[0])).complete | Should -BeFalse
        $results[0].source_sha = 'd' * 40
        (Test-ScheduledManifest -Manifest $manifest -Results $results).complete | Should -BeFalse
    }
    It 'distinguishes complete findings from incomplete execution' {
        $manifest = Get-TestManifest
        $results = Get-TestResult $manifest
        $results[0].outcome = 'findings'
        $verdict = Test-ScheduledManifest -Manifest $manifest -Results $results
        $verdict.complete | Should -BeTrue
        $verdict.successful | Should -BeFalse
        foreach ($outcome in @('skipped', 'incomplete', 'blocked', 'execution-error', 'not-applicable')) {
            $results[0].outcome = $outcome
            (Test-ScheduledManifest -Manifest $manifest -Results $results).complete | Should -BeFalse
        }
    }
}
Describe 'Unchanged main decisions' {
    BeforeEach {
        $manifest = Get-TestManifest
        $script:now = [datetimeoffset]'2026-09-08T12:00:00Z'
        $receipt = @{
            scope = 'full'; complete = $true; successful = $true; manifest = $manifest
            source_sha = $manifest.source_sha; check_contract_digest = $manifest.check_contract_digest
            run_id = 1; run_number = 2; run_attempt = 1; completed_at = '2026-09-07T12:00:00Z'
        }
        $script:coverage = @{ schema_version = 1; receipt = $receipt; invalidation = $null }
    }
    It 'reuses a complete compatible receipt without refreshing its time' {
        $decision = Get-ScheduledRunDecision -Manifest $manifest -Coverage $coverage -Now $now
        $decision.run | Should -BeFalse
        $decision.receipt.completed_at | Should -Be $receipt.completed_at
    }
    It 'reruns missing forced stale future or partial evidence' {
        (Get-ScheduledRunDecision -Manifest $manifest -Coverage $null -Now $now).run | Should -BeTrue
        (Get-ScheduledRunDecision -Manifest $manifest -Coverage $coverage -Now $now -Force).run | Should -BeTrue
        foreach ($completed in @('2026-09-01T12:00:00Z', '2026-09-09T12:00:00Z')) {
            $receipt.completed_at = $completed
            (Get-ScheduledRunDecision -Manifest $manifest -Coverage $coverage -Now $now).run | Should -BeTrue
        }
        $receipt.completed_at = '2026-09-07T12:00:00Z'
        $receipt.scope = 'repair'
        (Get-ScheduledRunDecision -Manifest $manifest -Coverage $coverage -Now $now).run | Should -BeTrue
    }
    It 'does not let an old success conceal a newer failure or retry' {
        $coverage.invalidation = @{
            source_sha = $manifest.source_sha; check_contract_digest = $manifest.check_contract_digest
            run_number = 2; run_attempt = 2
        }
        (Get-ScheduledRunDecision -Manifest $manifest -Coverage $coverage -Now $now).run | Should -BeTrue
        # A different workflow has its own independent sequence and can fail later with a
        # numerically smaller run number. The reporter retains that unresolved invalidation.
        $coverage.invalidation.run_number = 1
        $coverage.invalidation.run_attempt = 1
        (Get-ScheduledRunDecision -Manifest $manifest -Coverage $coverage -Now $now).run | Should -BeTrue
    }
    It 'reruns malformed or unreadable receipts rather than suppressing checks' {
        foreach ($invalid in @(@{}, @{ schema_version = 99 },
                @{ schema_version = 1; receipt = @{}; invalidation = $null })) {
            (Get-ScheduledRunDecision -Manifest $manifest -Coverage $invalid -Now $now).run | Should -BeTrue
        }
        $receipt.completed_at = 'not a timestamp'
        (Get-ScheduledRunDecision -Manifest $manifest -Coverage $coverage -Now $now).run | Should -BeTrue
    }
    It 'preserves timestamp meaning after JSON DateTime conversion' {
        $serialized = $coverage | ConvertTo-Json -Depth 50
        $roundTripped = $serialized | ConvertFrom-Json -AsHashtable
        (Get-ScheduledRunDecision -Manifest $manifest -Coverage $roundTripped -Now $now).run | Should -BeFalse
        $roundTripped.receipt.completed_at = [datetime]'2026-09-01T12:00:00Z'
        (Get-ScheduledRunDecision -Manifest $manifest -Coverage $roundTripped -Now $now).run | Should -BeTrue
    }
}
