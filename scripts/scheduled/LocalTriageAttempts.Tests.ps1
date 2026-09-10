#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
# Exercises concurrent and out-of-order reporter delivery across complete Local publications.
# New failures stay pending independently of a green rerun or an older analysis completion.
BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'fixtures\TriageFixture.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'fixtures\TriageScenarioFixture.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'LocalTriageCompletion.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'LocalTriageInbox.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'LocalTriagePublication.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'ScheduledContracts.psm1') -Force
}

Describe 'Exact attempt acknowledgement across reporter deliveries' {
    BeforeEach {
        $script:fixture = Initialize-TriageFixture -Root (Join-Path $TestDrive ([guid]::NewGuid().ToString('N')))
    }

    It 'acknowledges only its claim when another failed attempt arrives mid-analysis' {
        $next = Copy-TriageFixtureValue $fixture.evidence
        $next.attempt.run_attempt = 2
        $stale = @{
            key = '1/older-presentation'; kind = 'update-issue'; issue_number = 20; target_id = 20
            checkpoint = 1; purpose = 'run-presentation'; preimage = $fixture.snapshot.runs['20'].index_digest
            payload = @{ state = 'closed' }
        }
        $null = Invoke-TriageTransaction $fixture.context triage-prepare-operation @{ operation = $stale }
        $null = Invoke-TriageTransaction $fixture.context triage-begin-operation @{ operation_key = $stale.key }
        $revision = Add-TriageFixtureRevision $fixture $next
        $null = Publish-TriageFixtureProblem $fixture download
        $complete = Complete-ScheduledTriageAnalysis $fixture.context $fixture.api
        $complete.run_triaged | Should -BeFalse
        $complete.pending_revision_count | Should -Be 1
        $state = Invoke-TriageTransaction $fixture.context read
        $state.triage.analyses[$fixture.context.analysis_id].operations[$stale.key].stage | Should -Be superseded
        $pending = (Get-ScheduledTriageInbox $fixture.context.policy $state $fixture.api).pending
        $pending[0].digest | Should -BeExactly $revision.digest
        $pending[0].run_attempt | Should -Be 2
        $fixture.store.issues[20L].state | Should -Be open
    }

    It 'does not confirm a run closure when reporting advances the index between its read and PATCH' {
        $null = Publish-TriageFixtureProblem $fixture download
        $next = Copy-TriageFixtureValue $fixture.evidence
        $next.attempt.run_attempt = 2
        $script:injected = @{ revision = $null }
        $api = {
            param($Endpoint, $Method = 'GET', $Body, [switch] $Paginate)
            if ($Method -ceq 'PATCH' -and $Endpoint -ceq 'repos/owner/repository/issues/20' -and
                $Body.state -ceq 'closed') {
                $injected.revision = Add-TriageFixtureRevision $fixture $next
            }
            & $fixture.api -Endpoint $Endpoint -Method $Method -Body $Body -Paginate:$Paginate
        }
        { Complete-ScheduledTriageAnalysis $fixture.context $api } | Should -Throw -ExceptionType ([FormatException])
        $state = Invoke-TriageTransaction $fixture.context read
        $analysis = $state.triage.analyses[$fixture.context.analysis_id]
        $analysis.phase | Should -Not -Be complete
        $stale = @($analysis.operations.Values | Where-Object { $_.purpose -ceq 'run-presentation' })
        $stale.Count | Should -Be 1
        $stale[0].stage | Should -Be sending
        $current = Get-ScheduledTriageInbox $fixture.context.policy $state $fixture.api
        $current.pending.Count | Should -Be 1
        $current.pending[0].digest | Should -BeExactly $injected.revision.digest
        $current.pending[0].run_attempt | Should -Be 2

        $result = Complete-ScheduledTriageAnalysis $fixture.context $fixture.api
        $result.run_triaged | Should -BeFalse
        $result.pending_revision_count | Should -Be 1
        $fixture.store.issues[20L].state | Should -Be open
        @($fixture.store.issues[20L].labels | ForEach-Object { $_.name }) | Should -Not -Contain scheduled-triaged
        $state = Invoke-TriageTransaction $fixture.context read
        $state.triage.analyses[$fixture.context.analysis_id].operations[$stale[0].key].stage | Should -Be superseded
        @($fixture.store.writes | Where-Object {
            $_.endpoint -ceq 'repos/owner/repository/issues/20' -and $_.method -ceq 'PATCH'
        }).Count | Should -Be 2
    }

    It 'retains an unanalyzed failure across a green rerun then discovers new failure on its closed issue' {
        $green = Copy-TriageFixtureValue $fixture.evidence
        $green.attempt.run_attempt = 2
        $green.attempt.workflow_conclusion = 'success'
        $green.attempt.results[0].outcome = 'passed'
        $green.attempt.jobs[0].conclusion = 'success'; $green.attempt.jobs[0].steps[0].conclusion = 'success'
        $null = Add-TriageFixtureRevision $fixture $green
        $state = Invoke-TriageTransaction $fixture.context read
        (Get-ScheduledTriageInbox $fixture.context.policy $state $fixture.api).backlog_count | Should -Be 1
        $problem = Publish-TriageFixtureProblem $fixture download
        (Complete-ScheduledTriageAnalysis $fixture.context $fixture.api).run_triaged | Should -BeTrue
        $null = Add-TriageFixtureRevision $fixture $fixture.evidence
        (Get-ScheduledTriageInbox $fixture.context.policy $state $fixture.api).backlog_count | Should -Be 0
        $fixture.store.issues[20L].state | Should -Be closed
        $later = Copy-TriageFixtureValue $fixture.evidence; $later.attempt.run_attempt = 3
        $null = Add-TriageFixtureRevision $fixture $later
        (Get-ScheduledTriageInbox $fixture.context.policy $state $fixture.api).pending[0].run_attempt | Should -Be 3
        Invoke-TriageFixtureNextAnalysis $fixture 789 3
        $fixture.proposal.problems[0].matching = @{
            kind = 'existing'; issue_number = $problem.link.issue_number; target_generation = 1
            relation = 'repeat'; reason = 'The same dependency access failure recurs without established resolution.'
        }
        Invoke-TriageFixtureCheckpoint $fixture
        (Publish-TriageFixtureProblem $fixture download).link.issue_number | Should -Be $problem.link.issue_number
        (Complete-ScheduledTriageAnalysis $fixture.context $fixture.api).run_triaged | Should -BeTrue
        $fixture.store.comments[20L] | Where-Object { $_.body.Contains('<!-- scheduled-triage:v1 ') } |
            Measure-Object | Select-Object -ExpandProperty Count | Should -Be 1
        @($fixture.store.writes | Where-Object { $_.endpoint -ceq 'repos/owner/repository/issues' }).Count | Should -Be 1
    }
}

Describe 'Semantic identities across overlapping runs' {
    It 'publishes A and B then updates only B and creates C without absorbing or resolving A' {
        $fixture = Initialize-TriageFixture -Root (Join-Path $TestDrive 'overlap') -ProblemCount 2
        $first = Publish-TriageFixtureProblem $fixture download
        $shared = Publish-TriageFixtureProblem $fixture archive
        (Complete-ScheduledTriageAnalysis $fixture.context $fixture.api).run_triaged | Should -BeTrue
        $original = Copy-TriageFixtureValue $fixture.store.issues[[long]$first.link.issue_number]
        $originalComments = @(Copy-TriageFixtureValue @($fixture.store.comments[[long]$first.link.issue_number]))
        $next = Copy-TriageFixtureValue $fixture.evidence
        $next.run_id = 790; $next.attempt.run_number = 43
        $next.attempt.created_at = '2026-09-09T01:10:00Z'; $next.attempt.started_at = '2026-09-09T01:10:01Z'
        $next.attempt.completed_at = '2026-09-09T01:10:02Z'
        $next.attempt.jobs[0].log.excerpt = 'The archive was corrupt. A separate dependency license requires approval.'
        $null = Add-TriageFixtureRevision $fixture $next 21
        Invoke-TriageFixtureNextAnalysis $fixture 790
        $fixture.proposal.problems[0] = Copy-TriageFixtureValue $fixture.proposal.problems[1]
        $fixture.proposal.problems[0].key = 'download'
        $fixture.proposal.problems[0].matching = @{
            kind = 'existing'; issue_number = $shared.link.issue_number; target_generation = 1
            relation = 'repeat'; reason = 'Archive integrity diagnostics identify the already reported archive defect.'
        }
        $fixture.proposal.problems[1].diagnosis = Copy-TriageFixtureValue $fixture.proposal.problems[1].diagnosis
        $fixture.proposal.problems[1].diagnosis.title = 'Dependency license approval'
        $fixture.proposal.problems[1].diagnosis.summary = 'A dependency requires operator license approval.'
        $fixture.proposal.problems[1].diagnosis.cause = 'The dependency download is gated by a license decision.'
        $fixture.proposal.problems[1].diagnosis.repair_disposition = 'needs-human'
        $fixture.proposal.problems[1].diagnosis.repair_reason = 'An operator must decide whether to accept the license.'
        Invoke-TriageFixtureCheckpoint $fixture
        (Publish-TriageFixtureProblem $fixture download).link.issue_number | Should -Be $shared.link.issue_number
        $third = Publish-TriageFixtureProblem $fixture archive
        $third.link.issue_number | Should -Not -Be $shared.link.issue_number
        (Complete-ScheduledTriageAnalysis $fixture.context $fixture.api).run_triaged | Should -BeTrue
        $untouched = $fixture.store.issues[[long]$first.link.issue_number]
        foreach ($field in $original.Keys) {
            (Get-ScheduledDigest $untouched[$field]) | Should -Be (Get-ScheduledDigest $original[$field]) -Because $field
        }
        (Get-ScheduledDigest @($fixture.store.comments[[long]$first.link.issue_number])) |
            Should -Be (Get-ScheduledDigest $originalComments)
        $snapshot = Get-ScheduledTriageInbox $fixture.context.policy (Invoke-TriageTransaction $fixture.context read) $fixture.api
        $snapshot.problems[[string]$shared.link.issue_number].record.problem.evidence.Count | Should -Be 2
        $snapshot.problems[[string]$third.link.issue_number].record.problem.diagnosis.repair_disposition | Should -Be needs-human
        @($fixture.store.writes | Where-Object { $_.endpoint -ceq 'repos/owner/repository/issues' }).Count | Should -Be 3
    }
}
