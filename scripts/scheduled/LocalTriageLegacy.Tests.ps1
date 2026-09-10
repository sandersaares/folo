#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
# Protects the explicit bridge from retained reporter registrations into triage-owned records.
# Only matching hosted confirmation and native repair identity establish prior resolution.
BeforeDiscovery { Import-Module (Join-Path $PSScriptRoot 'LocalTriageProblem.psm1') }
BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'LocalTriageProblem.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'ScheduledContracts.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'fixtures\TriageFixture.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'fixtures\TriageScenarioFixture.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'LocalTriageInbox.psm1')
    Import-Module (Join-Path $PSScriptRoot 'LocalTriagePublication.psm1')
    Import-Module (Join-Path $PSScriptRoot 'LocalTriageView.psm1')
    Import-Module (Join-Path $PSScriptRoot 'LocalTriageCompletion.psm1')
}

Describe 'Retained registration publication' {
    BeforeEach {
        $script:fixture = Initialize-TriageFixture -Root (Join-Path $TestDrive ([guid]::NewGuid().ToString('N')))
        $script:legacy = @{
            schema_version = 1; repository = 'owner/repository'; repository_id = 123
            finding_id = 'f' * 64; generation = 1; status = 'open'; check_contract_digest = 'c' * 64
            source_sha = 'a' * 40; controller_sha = 'a' * 40; check_id = 'miri-linux'; check_kind = 'miri'
            package = 'package'; platform = 'linux'; confirmation = $null
            observation = @{ run_id = 789; run_attempt = 1; run_number = 42
                workflow_path = '.github/workflows/full-deep-validation.yml'
                run_started_at = '2026-09-09T01:00:01Z'; created_at = '2026-09-09T01:00:00Z' }
            evidence = @{ summary = 'Source access prevented the checker from running'; replay = @{ target = 'package' } }
        }
        $script:reporter = Write-ScheduledRecord $legacy reporter
        $script:worker = Write-ScheduledRecord -Kind worker -Record @{
            schema_version = 1; finding_id = 'f' * 64; generation = 1; pr_number = 22
            attempt_id = 'retained'; session_id = 'repair'; head_sha = 'b' * 40; explanation = 'Existing repair'
        }
        $fixture.store.issues[30L] = @{
            number = 30; title = 'Earlier checker observation'; body = "[Copilot speaking]`n$reporter"; state = 'open'
            user = @{ login = 'reporter' }; labels = @(@{ name = 'scheduled-finding' })
        }
        $fixture.store.comments[30L] = [Collections.Generic.List[object]]::new()
        $fixture.store.comments[30L].Add(@{ id = 900; body = $worker; user = @{ login = 'worker' }
            issue_url = 'https://api.github.com/repos/owner/repository/issues/30' })
    }

    It 'keeps reporter and repair evidence intact while holding scope-invalidating updates to the same issue' {
        $snapshot = Get-ScheduledTriageInbox $fixture.context.policy (Invoke-TriageTransaction $fixture.context read) $fixture.api
        (Get-ScheduledTriageIndexPage $snapshot 0).entries[0].category | Should -Be reporter-observation
        $fixture.proposal.checkpoint = 2
        $fixture.proposal.problems[0].matching = @{
            kind = 'existing'; issue_number = 30; target_generation = 1; relation = 'repeat'
            reason = 'The complete diagnostics identify the same source-access failure behind the checker observation.'
        }
        Invoke-TriageFixtureCheckpoint $fixture
        $result = Publish-TriageFixtureProblem $fixture download
        $result.link.issue_number | Should -Be 30
        $fixture.store.issues[30L].body | Should -Match ([regex]::Escape($reporter))
        $fixture.store.comments[30L][0].body | Should -BeExactly $worker
        $state = Invoke-TriageTransaction $fixture.context read
        $state.triage.repair_holds['30'].analysis_id | Should -Be $fixture.context.analysis_id
        $snapshot = Get-ScheduledTriageInbox $fixture.context.policy $state $fixture.api
        $snapshot.problems['30'].record.problem.legacy.scope[0].replay.target | Should -Be package
        $snapshot.problems['30'].record.problem.scope_revision | Should -Be 2
        (Complete-ScheduledTriageAnalysis $fixture.context $fixture.api).run_triaged | Should -BeTrue
        @($fixture.store.writes | Where-Object { $_.endpoint -ceq 'repos/owner/repository/issues' }).Count | Should -Be 0
        $fixture.store.issues[30L].body = $reporter
        $fixture.store.comments[30L].Clear()
        $fixture.store.comments[30L].Add(@{ id = 900; body = $worker; user = @{ login = 'worker' }
            issue_url = 'https://api.github.com/repos/owner/repository/issues/30' })
        { Get-ScheduledTriageInbox $fixture.context.policy $state $fixture.api } | Should -Throw
    }

    It 'observes an existing applicable confirmation before reopening a new occurrence on later source' {
        $fixture.proposal.checkpoint = 2
        $fixture.proposal.problems[0].diagnosis.scope = @(@{
            operation = 'miri'; package = 'package'; check_id = 'miri-linux'; platform = 'linux'
            replay = @{ target = 'package' }; citations = @('/attempt/jobs/0/log/excerpt')
        })
        $fixture.proposal.problems[0].matching = @{
            kind = 'existing'; issue_number = 30; target_generation = 1; relation = 'repeat'
            reason = 'The complete diagnostics establish this existing cause.'
        }
        Invoke-TriageFixtureCheckpoint $fixture
        $null = Publish-TriageFixtureProblem $fixture download
        (Complete-ScheduledTriageAnalysis $fixture.context $fixture.api).run_triaged | Should -BeTrue
        $legacy.status = 'confirmed'
        $legacy.confirmation = @{
            authoritative = $true; successful = $true; scope_complete = $true; explained = $true
            generation = 1; pr_number = 22; merge_commit_sha = 'a' * 40
        }
        $fixture.store.issues[30L].body = $fixture.store.issues[30L].body.Replace($reporter, (Write-ScheduledRecord $legacy reporter))
        $fixture.store.issues[30L].state = 'closed'
        $pr = @{
            merged = $true; number = 22; merge_commit_sha = 'a' * 40; base = @{ repo = @{ id = 123 } }
            head = @{ sha = 'b' * 40 }
            body = Write-ScheduledRecord -Kind repair -Record @{
                schema_version = 1; issue_number = 30; finding_id = 'f' * 64
                generation = 1; attempt_id = 'retained'; head_sha = 'b' * 40
            }
        }
        $transport = $fixture.api
        $fixture.api = {
            param($Endpoint, $Method = 'GET', $Body, [switch] $Paginate, [switch] $Collection, [switch] $Pages)
            if ($Endpoint.EndsWith('/pulls/22')) { return $pr }
            if ($Endpoint.EndsWith("compare/$('a' * 40)...$('c' * 40)")) { return @{ status = 'ahead' } }
            & $transport -Endpoint $Endpoint -Method $Method -Body $Body -Paginate:$Paginate -Collection:$Collection -Pages:$Pages
        }.GetNewClosure()
        $next = Copy-TriageFixtureValue $fixture.evidence
        $next.run_id = 790; $next.attempt.run_number = 43; $next.attempt.manifest.source_sha = 'c' * 40
        $next.attempt.created_at = '2026-09-09T01:10:00Z'; $next.attempt.started_at = '2026-09-09T01:10:01Z'
        $next.attempt.completed_at = '2026-09-09T01:10:02Z'
        $null = Add-TriageFixtureRevision $fixture $next 21
        Invoke-TriageFixtureNextAnalysis $fixture 790
        $fixture.proposal.problems[0].matching.relation = 'recurrence'
        Invoke-TriageFixtureCheckpoint $fixture
        $published = Publish-TriageFixtureProblem $fixture download
        $published.link.generation | Should -Be 2
        $fixture.store.issues[30L].state | Should -Be open
        $snapshot = Get-ScheduledTriageInbox $fixture.context.policy (Invoke-TriageTransaction $fixture.context read) $fixture.api
        $record = $snapshot.problems['30'].record.problem
        $record.resolved_occurrences[0].generation | Should -Be 1
        $record.resolved_occurrences[0].evidence[0].pr_number | Should -Be 22
        $record.scope_revision | Should -Be 1
        (Complete-ScheduledTriageAnalysis $fixture.context $fixture.api).run_triaged | Should -BeTrue
    }

    It 'advances scope and holds the retained repair when a <Relation> changes only its required operation' -ForEach @(
        @{ Relation = 'repeat' }, @{ Relation = 'historical' }
    ) {
        $fixture.proposal.checkpoint = 2
        $fixture.proposal.problems[0].diagnosis.scope = @(@{
            operation = 'miri'; package = 'package'; check_id = 'miri-linux'; platform = 'linux'
            replay = @{ target = 'package' }; citations = @('/attempt/jobs/0/log/excerpt')
        })
        $fixture.proposal.problems[0].matching = @{
            kind = 'existing'; issue_number = 30; target_generation = 1; relation = 'repeat'
            reason = 'The existing problem accounts for the observed failure.'
        }
        Invoke-TriageFixtureCheckpoint $fixture
        $null = Publish-TriageFixtureProblem $fixture download
        $before = Invoke-TriageTransaction $fixture.context read
        $fixture.proposal.checkpoint = 3
        $fixture.proposal.problems[0].matching.relation = $Relation
        $fixture.proposal.problems[0].diagnosis.scope[0].operation = 'miri with an additional execution qualifier'
        Invoke-TriageFixtureCheckpoint $fixture
        $null = Publish-TriageFixtureProblem $fixture download
        $state = Invoke-TriageTransaction $fixture.context read
        $snapshot = Get-ScheduledTriageInbox $fixture.context.policy $state $fixture.api
        $snapshot.problems['30'].record.problem.scope_revision | Should -Be 2
        $state.triage.repair_holds['30'].reason | Should -Not -Be $before.triage.repair_holds['30'].reason
    }
}

Describe 'Retained reporter problem interpretation' {
    InModuleScope LocalTriageProblem {
        BeforeEach {
            $script:policy = @{ repository = 'owner/repository'; repository_id = 123 }
            $script:legacy = @{
                source_sha = 'a' * 40; finding_id = 'f' * 64; generation = 1; status = 'confirmed'
                check_kind = 'miri'; check_id = 'miri-linux'; package = 'package'; platform = 'linux'
                observation = @{ run_id = 789; run_attempt = 1
                    run_started_at = '2026-09-09T01:00:01Z'; created_at = '2026-09-09T01:00:00Z' }
                evidence = @{ replay = @{ target = 'package' } }
                confirmation = @{ authoritative = $true; successful = $true; scope_complete = $true
                    explained = $true; generation = 1; pr_number = 22; merge_commit_sha = 'a' * 40 }
                validated_worker = @{ pr_number = 22; attempt_id = 'retained'; head_sha = 'b' * 40
                    explanation = 'The registered change addresses the observed defect.' }
            }
            $script:repair = @{ schema_version = 1; issue_number = 31; finding_id = 'f' * 64
                generation = 1; attempt_id = 'retained'; head_sha = 'b' * 40 }
            $script:pr = @{ merged = $true; base = @{ repo = @{ id = 123 } }
                number = 22; merge_commit_sha = 'a' * 40; head = @{ sha = 'b' * 40 }
                body = Write-ScheduledRecord $repair repair }
            $script:comparison = @{ status = 'identical' }
            $script:api = {
                param($Endpoint)
                if ($Endpoint.EndsWith('/pulls/22')) { return $pr }
                if ($Endpoint.Contains('/compare/')) { return $comparison }
                throw 'Unexpected legacy API request.'
            }
        }

        It 'preserves original scope and imports only applicable existing confirmation' {
            $result = ConvertTo-TriageLegacyProblem $policy 31 $legacy @{ summary = 'AI diagnosis' } closed $api
            $result.status | Should -Be resolved
            $result.resolution.generation | Should -Be 1
            $result.resolution.evidence[0].pr_number | Should -Be 22
            $result.legacy.scope[0].package | Should -Be package
            $result.legacy.scope[0].replay.target | Should -Be package
            $result.evidence.Count | Should -Be 0
        }

        It 'does not invent resolution for closed unconfirmed or incompatible registrations' {
            $legacy.confirmation.authoritative = $false
            (ConvertTo-TriageLegacyProblem $policy 31 $legacy @{} closed $api).status | Should -Be needs-human
            (ConvertTo-TriageLegacyProblem $policy 31 $legacy @{} open $api).status | Should -Be open
            $legacy.confirmation.authoritative = $true
            $pr.head.sha = 'c' * 40
            (ConvertTo-TriageLegacyProblem $policy 31 $legacy @{} closed $api).resolution | Should -BeNullOrEmpty
            $pr.head.sha = 'b' * 40
            $comparison.status = 'diverged'
            (ConvertTo-TriageLegacyProblem $policy 31 $legacy @{} closed $api).resolution | Should -BeNullOrEmpty
        }

        It 'uses the controller identity only when no tested source was established' {
            $evidence = @{ run_id = 789; attempt = @{ run_attempt = 1; controller_sha = 'a' * 40 } }
            $basis = @{ api_evidence = @{ source_sha = $null; started_at = '2026-09-09T01:00:01Z'
                    created_at = '2026-09-09T01:00:00Z' } }
            (Get-TriageSourceObservation $evidence $basis).source_sha | Should -BeExactly ('a' * 40)
        }
    }
}
