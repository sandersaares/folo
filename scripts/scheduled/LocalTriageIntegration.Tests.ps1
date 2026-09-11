#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
# Exercises real serialized hosted evidence, local ownership, AI-supplied analysis and crash-safe
# publication against a strict in-memory transport. These tests do not run an AI or touch GitHub.
BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'fixtures\TriageFixture.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'fixtures\TriageScenarioFixture.psm1')
    Import-Module (Join-Path $PSScriptRoot 'LocalTriageProblem.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'LocalTriagePublication.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'LocalTriageCompletion.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'LocalTriageInbox.psm1') -Force
}

Describe 'Local triage publication and recovery' {
    BeforeEach { $script:fixture = Initialize-TriageFixture -Root (Join-Path $TestDrive ([guid]::NewGuid().ToString('N'))) }

    It 'publishes a problem and closes only its completely accounted-for run revision' {
        $handoff = Invoke-ScheduledTriageProblemPreparation $fixture.context $fixture.snapshot download $fixture.api
        $handoff.action | Should -Be native-create-issue
        $created = & $fixture.api -Endpoint 'repos/owner/repository/issues' -Method POST -Body $handoff.payload
        $null = Invoke-TriageTransaction $fixture.context triage-observe-operation @{
            operation_key = $handoff.operation_key; target_id = $created.number
        }
        (Invoke-ScheduledTriageProblemPreparation $fixture.context $fixture.snapshot download $fixture.api).action |
            Should -Be prepared
        (Invoke-ScheduledTriageProblemPreparation $fixture.context $fixture.snapshot download $fixture.api).action |
            Should -Be prepared
        (Publish-ScheduledTriageProblem $fixture.context download $fixture.api).action | Should -Be published
        $result = Complete-ScheduledTriageAnalysis $fixture.context $fixture.api
        $result.run_triaged | Should -BeTrue
        $fixture.store.issues[20L].state | Should -Be closed
        $fixture.store.issues[[long]$created.number].state | Should -Be open
        $state = Invoke-TriageTransaction $fixture.context read
        $state.triage.analyses[$fixture.context.analysis_id].phase | Should -Be complete
        (Get-ScheduledTriageInbox $fixture.context.policy $state $fixture.api).backlog_count | Should -Be 0
        @($fixture.store.writes | Where-Object { $_.endpoint -match '/pulls|/contents|/git/' }).Count | Should -Be 0
    }

    It 'recovers a lost native issue response without another create' {
        $handoff = Invoke-ScheduledTriageProblemPreparation $fixture.context $fixture.snapshot download $fixture.api
        $fixture.store.lose_create = $true
        { & $fixture.api -Endpoint 'repos/owner/repository/issues' -Method POST -Body $handoff.payload } | Should -Throw
        (Invoke-ScheduledTriageProblemPreparation $fixture.context $fixture.snapshot download $fixture.api).action |
            Should -Be prepared
        @($fixture.store.writes | Where-Object { $_.endpoint -ceq 'repos/owner/repository/issues' }).Count | Should -Be 1
    }

    It 'retains an unobservable native issue outcome instead of creating a duplicate' {
        $handoff = Invoke-ScheduledTriageProblemPreparation $fixture.context $fixture.snapshot download $fixture.api
        $fixture.store.hide_create = $true
        { & $fixture.api -Endpoint 'repos/owner/repository/issues' -Method POST -Body $handoff.payload } | Should -Throw
        { Invoke-ScheduledTriageProblemPreparation $fixture.context $fixture.snapshot download $fixture.api } | Should -Throw
        @($fixture.store.writes | Where-Object { $_.endpoint -ceq 'repos/owner/repository/issues' }).Count | Should -Be 1
        $fixture.store.issues[[long]$fixture.store.hidden_issue.number] = $fixture.store.hidden_issue
        (Invoke-ScheduledTriageProblemPreparation $fixture.context $fixture.snapshot download $fixture.api).action |
            Should -Be prepared
    }

    It 'reconciles lost detail and problem root responses before acknowledging analysis' {
        $handoff = Invoke-ScheduledTriageProblemPreparation $fixture.context $fixture.snapshot download $fixture.api
        $null = & $fixture.api -Endpoint 'repos/owner/repository/issues' -Method POST -Body $handoff.payload
        $null = Invoke-ScheduledTriageProblemPreparation $fixture.context $fixture.snapshot download $fixture.api
        $fixture.store.lose_comment = $true
        { Publish-ScheduledTriageProblem $fixture.context download $fixture.api } | Should -Throw
        $fixture.store.lose_update = $true
        { Publish-ScheduledTriageProblem $fixture.context download $fixture.api } | Should -Throw
        (Publish-ScheduledTriageProblem $fixture.context download $fixture.api).action | Should -Be published
        $fixture.store.lose_comment = $true
        { Complete-ScheduledTriageAnalysis $fixture.context $fixture.api } | Should -Throw
        (Complete-ScheduledTriageAnalysis $fixture.context $fixture.api).run_triaged | Should -BeTrue
    }

    It 'does not change repair state or admit source repairs after successful triage' {
        $state = Invoke-TriageTransaction $fixture.context read
        $state.attempts.Count | Should -Be 0
        { Invoke-TriageTransaction $fixture.context reserve-attempt } | Should -Throw '*ai-triage-unavailable*'
        { Invoke-TriageTransaction $fixture.context prepare-publication } | Should -Throw
        { Invoke-TriageTransaction $fixture.context register-pr } | Should -Throw
        (Invoke-TriageTransaction $fixture.context read).attempts.Count | Should -Be 0
    }

    It 'rejects a new recurrence disposition for already attached evidence after a human closes the problem' {
        $published = Publish-TriageFixtureProblem $fixture download
        $number = [long]$published.link.issue_number
        $fixture.store.issues[$number].state = 'closed'
        $fixture.proposal.checkpoint = 2
        $fixture.proposal.problems[0].matching = @{
            kind = 'existing'; issue_number = $number; target_generation = 1; relation = 'recurrence'
            reason = 'A new recurrence was claimed for the already attached revision.'
        }
        Invoke-TriageFixtureCheckpoint $fixture
        $writes = $fixture.store.writes.Count
        {
            $null = Invoke-ScheduledTriageProblemPreparation $fixture.context $fixture.snapshot download $fixture.api
            $null = Publish-ScheduledTriageProblem $fixture.context download $fixture.api
        } | Should -Throw
        $fixture.store.issues[$number].state | Should -Be closed
        $fixture.store.writes.Count | Should -Be $writes
        { Publish-ScheduledTriageProblem $fixture.context download $fixture.api } | Should -Throw
        $fixture.proposal.checkpoint = 3
        $fixture.proposal.problems[0].matching.relation = 'repeat'
        Invoke-TriageFixtureCheckpoint $fixture
        (Publish-TriageFixtureProblem $fixture download).link.issue_number | Should -Be $number
        $fixture.store.issues[$number].state | Should -Be closed
        $snapshot = Get-ScheduledTriageInbox $fixture.context.policy (Invoke-TriageTransaction $fixture.context read) $fixture.api
        $snapshot.problems[[string]$number].record.problem.evidence.Count | Should -Be 1
        $snapshot.problems[[string]$number].record.problem.status | Should -Be needs-human
    }

    It 'completes an existing duplicate without a new issue or repair admission (human-held: <Held>)' -ForEach @(
        @{ Held = $false }, @{ Held = $true }
    ) {
        $fixture.proposal.checkpoint = 2
        $diagnosis = $fixture.proposal.problems[0].diagnosis
        $diagnosis.category = 'code'; $diagnosis.repair_disposition = 'actionable'
        $diagnosis.cause = 'The downloader mishandles the dependency source response'
        $diagnosis.repair_reason = 'Correct the independently diagnosed response handling'
        $fixture.proposal.jobs[0].disposition.kind = 'actionable'
        $fixture.proposal.jobs[0].steps[0].disposition.kind = 'actionable'
        $fixture.proposal.results[0].disposition.kind = 'actionable'
        Invoke-TriageFixtureCheckpoint $fixture
        $published = Publish-TriageFixtureProblem $fixture download
        $number = [long]$published.link.issue_number
        $null = Complete-ScheduledTriageAnalysis $fixture.context $fixture.api
        $evidence = Copy-TriageFixtureValue $fixture.evidence
        $evidence.run_id = 800
        $evidence.attempt.created_at = '2026-09-09T01:10:00Z'
        $evidence.attempt.started_at = '2026-09-09T01:10:01Z'
        $evidence.attempt.completed_at = '2026-09-09T01:10:02Z'
        $null = Add-TriageFixtureRevision $fixture $evidence 21
        Invoke-TriageFixtureNextAnalysis $fixture 800
        $fixture.proposal.jobs[0].disposition.kind = 'duplicate'
        $fixture.proposal.jobs[0].steps[0].disposition.kind = 'duplicate'
        $fixture.proposal.results[0].disposition.kind = 'duplicate'
        $fixture.proposal.problems[0].matching = @{
            kind = 'existing'; issue_number = $number; target_generation = 1; relation = 'repeat'
            reason = 'The verified full record independently supports the same response-handling cause and scope'
        }
        if ($Held) {
            $fixture.store.issues[$number].state = 'closed'
            $fixture.proposal.problems[0].diagnosis.repair_disposition = 'needs-human'
            $fixture.proposal.problems[0].diagnosis.repair_reason = 'Respect the human-held issue'
        }
        Invoke-TriageFixtureCheckpoint $fixture
        $entry = $fixture.snapshot.index.entries[0]
        $entry.prior_support.full_read_digest | Should -BeExactly $entry.full_read_digest
        $entry.prior_support.occurrences[0].diagnosis.repair_disposition | Should -Be actionable
        (Publish-TriageFixtureProblem $fixture download).link.issue_number | Should -Be $number
        (Complete-ScheduledTriageAnalysis $fixture.context $fixture.api).run_triaged | Should -BeTrue
        @($fixture.store.writes | Where-Object { $_.endpoint -ceq 'repos/owner/repository/issues' }).Count | Should -Be 1
        $state = Invoke-TriageTransaction $fixture.context read
        $problem = (Get-ScheduledTriageInbox $fixture.context.policy $state $fixture.api).problems[[string]$number].record.problem
        $problem.evidence.Count | Should -Be 2
        $problem.scope_revision | Should -Be 1
        if ($Held) {
            $problem.status | Should -Be needs-human
            $problem.diagnosis.repair_disposition | Should -Be needs-human
            $fixture.store.issues[$number].state | Should -Be closed
        } else {
            $problem.diagnosis.repair_disposition | Should -Be actionable
        }
        { Invoke-TriageTransaction $fixture.context reserve-attempt } | Should -Throw -ExceptionType ([NotSupportedException])
        $state.attempts.Count | Should -Be 0
    }

    It 'does not persist unsupported actionability from an existing non-actionable problem' {
        $published = Publish-TriageFixtureProblem $fixture download
        $fixture.proposal.checkpoint = 2
        $fixture.proposal.problems[0].matching = @{
            kind = 'existing'; issue_number = $published.link.issue_number; target_generation = 1; relation = 'repeat'
            reason = 'Same observed failure without any additional source-defect evidence'
        }
        $fixture.proposal.problems[0].diagnosis.category = 'code'
        $fixture.proposal.problems[0].diagnosis.repair_disposition = 'actionable'
        $fixture.proposal.results[0].disposition.kind = 'duplicate'
        $writes = $fixture.store.writes.Count
        { Invoke-TriageFixtureCheckpoint $fixture } | Should -Throw
        $state = Invoke-TriageTransaction $fixture.context read
        $state.triage.analyses[$fixture.context.analysis_id].checkpoint.analysis.checkpoint | Should -Be 1
        $fixture.store.writes.Count | Should -Be $writes
    }
}
