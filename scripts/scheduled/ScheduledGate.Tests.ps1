#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
# Protects `scheduled-repair-gate`: a candidate must never pass the gate by claiming to be a
# managed repair without matching evidence, by exceeding its reviewed package/file scope, or by
# presenting evidence for a different check contract/manifest than the one the plan committed to.
BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'ScheduledGate.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'ScheduledContracts.psm1') -Force
    function Get-GateFixture {
        $policy = Get-ScheduledPolicy
        $policy.repair.allowed_packages = @('events_once')
        $policy.local.enrolled_machine_id = 'executor'
        $reporter = @{
            schema_version = 1; repository = $policy.repository; repository_id = $policy.repository_id
            finding_id = 'c' * 64; generation = 1; status = 'open'; package = 'events_once'
            check_id = 'miri-many-events_once-2'; controller_sha = 'a' * 40
            source_sha = 'd' * 40; check_contract_digest = 'e' * 64
            observation = @{
                run_id = 10; run_attempt = 1; run_number = 2
                workflow_path = '.github/workflows/full-deep-validation.yml'
            }
        }
        $worker = @{
            schema_version = 1; repository = $policy.repository; repository_id = $policy.repository_id
            finding_id = $reporter.finding_id; generation = 1; branch = 'sandersaares-scheduled-repair-fix'
            head_sha = 'b' * 40; attempt_id = 'attempt'; session_id = 'session'; executor_id = 'executor'
            pr_number = $null
            explanation = 'The repaired ownership transition preserves the waiter until notification completes.'
        }
        $repair = @{
            schema_version = 1; repository = $policy.repository; repository_id = $policy.repository_id
            finding_id = $reporter.finding_id; generation = 1; branch = $worker.branch
            head_sha = $worker.head_sha; attempt_id = $worker.attempt_id; issue_number = 42
            check_contract_digest = $reporter.check_contract_digest
            explanation = $worker.explanation
        }
        $pr = @{
            number = 43; user = @{ login = 'sandersaares' }; base = @{ ref = 'main' }
            head = @{ ref = $worker.branch; sha = $worker.head_sha; repo = @{ id = $policy.repository_id } }
            body = Write-ScheduledRecord -Kind repair -Record $repair
        }
        $issue = @{
            number = 42; user = @{ login = $policy.reporter_login }; state = 'open'
            body = Write-ScheduledRecord -Kind reporter -Record $reporter
        }
        return @{ Policy = $policy; PullRequest = $pr; Issue = $issue; Worker = $worker }
    }
}
Describe 'Managed repair identification' {
    It 'keeps ordinary human work by the same account ordinary' {
        $fixture = Get-GateFixture
        $fixture.PullRequest.head.ref = 'human-change'
        $fixture.PullRequest.body = 'Normal human content'
        (Get-ScheduledRepairScope @fixture).managed | Should -BeFalse
    }
    It 'recognizes the opening event before PR number reconciliation and requires all family seeds' {
        $fixture = Get-GateFixture
        $scope = Get-ScheduledRepairScope @fixture
        $scope.managed | Should -BeTrue
        $scope.check_ids.Count | Should -Be 4
        $scope.check_ids | Should -Contain 'miri-many-events_once-4'
    }
    It 'rejects removed metadata on reserved branches' {
        $fixture = Get-GateFixture
        $fixture.PullRequest.body = 'Removed'
        { Get-ScheduledRepairScope @fixture } | Should -Throw
    }
    It 'requires matching bounded causal explanations before the opening event' {
        foreach ($explanation in @('', ' ', ('x' * 4001), 'A different explanation')) {
            $fixture = Get-GateFixture
            $fixture.Worker.explanation = $explanation
            { Get-ScheduledRepairScope @fixture } | Should -Throw '*explanation*'
        }
    }
    It 'preserves registered PR deep validation after an unexplained main pass without allowing premature closure' {
        $fixture = Get-GateFixture
        $record = Read-ScheduledRecord -Kind reporter -Text $fixture.Issue.body
        $record.status = 'needs-human'
        $fixture.Issue.body = Write-ScheduledRecord -Kind reporter -Record $record
        (Get-ScheduledRepairScope @fixture).managed | Should -BeTrue
        { Get-ScheduledRepairScope @fixture -Confirmation } | Should -Throw
        $fixture.PullRequest.merged = $true
        $fixture.PullRequest.state = 'closed'
        (Get-ScheduledRepairScope @fixture -Confirmation).managed | Should -BeTrue
    }
    It 'rejects stale head wrong PR wrong generation and unapproved package' {
        foreach ($mutation in @('head', 'pr', 'generation', 'package', 'executor')) {
            $fixture = Get-GateFixture
            switch ($mutation) {
                head { $fixture.Worker.head_sha = 'f' * 40 }
                pr { $fixture.Worker.pr_number = 100 }
                generation { $fixture.Worker.generation = 2 }
                package { $fixture.Policy.repair.allowed_packages = @() }
                executor { $fixture.Worker.executor_id = 'unenrolled-machine' }
            }
            { Get-ScheduledRepairScope @fixture } | Should -Throw
        }
    }
    It 'validates API provenance rather than trusting an evidence marker' {
        $fixture = Get-GateFixture
        $run = @{
            id = 10; run_attempt = 1; run_number = 2; head_branch = 'main'; head_sha = 'a' * 40
            status = 'completed'; path = '.github/workflows/full-deep-validation.yml'
            repository = @{ id = 850321188; full_name = 'folo-rs/folo' }
        }
        $comments = @(@{
                user = @{ login = 'sandersaares' }
                body = Write-ScheduledRecord -Kind worker -Record $fixture.Worker
            })
        $incident = ConvertTo-ScheduledIncident -Issue $fixture.Issue -Comments $comments -Run $run -Repository 'folo-rs/folo'
        $incident.validated_worker.session_id | Should -Be session
        $run.head_sha = 'f' * 40
        { ConvertTo-ScheduledIncident -Issue $fixture.Issue -Comments $comments -Run $run -Repository 'folo-rs/folo' } |
            Should -Throw
    }
    It 'rejects spoofed authors wrong workflow provenance and ambiguous worker records' {
        foreach ($mutation in @('author', 'repository', 'workflow', 'attempt', 'duplicate-worker')) {
            $fixture = Get-GateFixture
            $run = @{
            id = 10; run_attempt = 1; run_number = 2; head_branch = 'main'; head_sha = 'a' * 40
            status = 'completed'; path = '.github/workflows/full-deep-validation.yml'
            repository = @{ id = 850321188; full_name = 'folo-rs/folo' }
            }
            $comment = @{
            user = @{ login = 'sandersaares' }
            body = Write-ScheduledRecord -Kind worker -Record $fixture.Worker
            }
            $comments = @($comment)
            switch ($mutation) {
            author { $fixture.Issue.user.login = 'unrelated-user' }
            repository { $run.repository.id = 1 }
            workflow { $run.path = '.github/workflows/other.yml' }
            attempt { $run.run_attempt = 2 }
            duplicate-worker { $comments += $comment }
            }
            { ConvertTo-ScheduledIncident -Issue $fixture.Issue -Comments $comments -Run $run -Repository 'folo-rs/folo' } |
            Should -Throw
        }
    }
}
Describe 'Combined merge queue scope' {
    It 'uses queue candidate ancestry not a number extracted from the ref' {
        $entries = @(
            @{ headCommit = @{ oid = 'b' * 40 }; pullRequest = @{ number = 11 } },
            @{ headCommit = @{ oid = 'c' * 40 }; pullRequest = @{ number = 12 } },
            @{ headCommit = @{ oid = 'd' * 40 }; pullRequest = @{ number = 13 } })
        $members = Get-ScheduledMergeGroupMember -HeadSha ('c' * 40) -BaseSha ('a' * 40) `
            -Entries $entries -IsAncestor { param($ancestor, $descendant) $ancestor -cle $descendant }
        $members.number | Should -Be @(11, 12)
    }

    Describe 'Published repair scope' {
        It 'permits source fixes and internal version expansion but not external dependency changes' {
            $files = @(@{ filename = 'packages/events_once/src/lib.rs'; status = 'modified' },
                @{ filename = 'Cargo.toml'; status = 'modified' })
            $base = { 'events_once = { version = "=0.1.0", path = "packages/events_once" }' }
            $head = { 'events_once = { version = "=0.1.1", path = "packages/events_once" }' }
            { Assert-ScheduledRepairChange -Files $files -Packages events_once -WorkspacePackages events_once `
                    -ReadBase $base -ReadHead $head } | Should -Not -Throw
            $head = { 'events_once = { version = "0.1.1", path = "packages/events_once" }' }
            { Assert-ScheduledRepairChange -Files $files -Packages events_once -WorkspacePackages events_once `
                    -ReadBase $base -ReadHead $head } | Should -Throw
            $base = { 'serde = { version = "1.0.0" }' }
            $head = { 'serde = { version = "2.0.0" }' }
            { Assert-ScheduledRepairChange -Files $files -Packages events_once -WorkspacePackages events_once `
                    -ReadBase $base -ReadHead $head } | Should -Throw
        }
        It 'rejects check weakening and sibling source edits' {
            foreach ($path in @('.github/workflows/standard-validation.yml', 'scripts/scheduled/policy.json', 'packages/other/src/lib.rs')) {
                { Assert-ScheduledRepairChange -Files @(@{ filename = $path; status = 'modified' }) `
                        -Packages events_once -WorkspacePackages events_once -ReadBase { '' } -ReadHead { '' } } |
                    Should -Throw
            }
        }
        It 'keeps external lockfile resolutions and checksums exact' {
            $before = "[[package]]`nname = `"events_once`"`nversion = `"0.1.0`"`n`n[[package]]`nname = `"serde`"`nversion = `"1.0.0`""
            $after = $before.Replace('0.1.0', '0.1.1')
            (ConvertTo-ScheduledVersionNeutralText -Text $before -Kind lock -WorkspacePackages events_once) |
                Should -Be (ConvertTo-ScheduledVersionNeutralText -Text $after -Kind lock -WorkspacePackages events_once)
            $after = $after.Replace('1.0.0', '2.0.0')
            (ConvertTo-ScheduledVersionNeutralText -Text $before -Kind lock -WorkspacePackages events_once) |
                Should -Not -Be (ConvertTo-ScheduledVersionNeutralText -Text $after -Kind lock -WorkspacePackages events_once)
        }
    }
    It 'fails closed when the actual event candidate is absent from the API snapshot' {
        { Get-ScheduledMergeGroupMember -HeadSha ('c' * 40) -BaseSha ('a' * 40) `
                -Entries @() -IsAncestor { $true } } | Should -Throw
    }
}
