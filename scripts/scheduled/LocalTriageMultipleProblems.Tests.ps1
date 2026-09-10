#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
# Protects multi-problem publication from mistaking its own confirmed issue/comment writes for
# external index drift. External changes must still require semantic reconsideration.
BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'fixtures\TriageFixture.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'LocalTriageProblem.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'LocalTriagePublication.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'LocalTriageCompletion.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'LocalTriageInbox.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'ScheduledRecordTool.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'ScheduledContracts.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'LocalTriageView.psm1') -Force

    function Publish-FixtureProblem($Fixture, $Key) {
        $prepared = Invoke-ScheduledTriageProblemPreparation $Fixture.context $Fixture.snapshot $Key $Fixture.api
        if ($prepared.action -ceq 'native-create-issue') {
            $created = & $Fixture.api -Endpoint 'repos/owner/repository/issues' -Method POST -Body $prepared.payload
            $null = Invoke-TriageTransaction $Fixture.context triage-observe-operation @{
                operation_key = $prepared.operation_key; target_id = $created.number
            }
            $prepared = Invoke-ScheduledTriageProblemPreparation $Fixture.context $Fixture.snapshot $Key $Fixture.api
        }
        $prepared.action | Should -Be prepared
        $published = Publish-ScheduledTriageProblem $Fixture.context $Key $Fixture.api
        $published.action | Should -Be published
        return $published
    }

    function Add-FixtureExistingProblem($Fixture) {
        $state = Invoke-TriageTransaction $Fixture.context read
        $analysis = $state.triage.analyses[$Fixture.context.analysis_id]
        $created = & $Fixture.api -Endpoint 'repos/owner/repository/issues' -Method POST -Body @{
            title = 'Existing access problem'; body = '[Copilot speaking]'; labels = @('scheduled-finding')
        }
        $diagnosis = Copy-TriageFixtureValue $Fixture.proposal.problems[0].diagnosis
        $diagnosis.cause = 'Earlier description of the access failure'
        $apiEvidence = $analysis.checkpoint.basis.api_evidence
        $document = Invoke-ScheduledRecordTool -Package scheduled-triage-record -Request @{
            op = 'update_problem'; existing = $null
            incoming = @{
                issue_number = $created.number; target_generation = 1; revision = $analysis.revision
                diagnosis = $diagnosis; relation = 'repeat'; source_relation = 'identical'
                operation_id = 'earlier-publication'; primary_evidence = $Fixture.evidence
                basis = $analysis.checkpoint.basis
                observation = @{
                    source_sha = $apiEvidence.source_sha; run_id = 789; run_attempt = 1
                    started_at = $apiEvidence.started_at; created_at = $apiEvidence.created_at
                }
            }
        }
        $prepared = Invoke-ScheduledRecordTool -Package scheduled-run-record -Request @{
            op = 'prepare_document'; kind = 'problem'; owner = "123/$($created.number)"; document = $document
        }
        foreach ($page in $prepared.pages) {
            $null = & $Fixture.api -Endpoint "repos/owner/repository/issues/$($created.number)/comments" `
                -Method POST -Body @{ body = $page.body }
        }
        $restored = Invoke-ScheduledRecordTool -Package scheduled-run-record -Request @{
            op = 'restore_documents'; kind = 'problem'; owner = "123/$($created.number)"
            comments = @($Fixture.store.comments[[long]$created.number] | ForEach-Object { @{ id = $_.id; body = $_.body } })
        }
        $root = @{
            schema_version = 1; role = 'triage'; repository_id = 123; issue_number = $created.number
            executor_id = 'executor'; generation = 1; scope_revision = 1; status = 'open'
            repair_disposition = 'operator-recovery'; current_digest = $prepared.digest; index_digest = $restored.index_digest
        }
        $null = & $Fixture.api -Endpoint "repos/owner/repository/issues/$($created.number)" -Method PATCH -Body @{
            body = "[Copilot speaking]`n`n$(ConvertTo-TriageOwnedBlock problem (Write-ScheduledRecord $root problem))"
        }
        $snapshot = Get-ScheduledTriageInbox $Fixture.context.policy $state $Fixture.api
        $null = Invoke-TriageTransaction $Fixture.context triage-record-index-read @{
            index_digest = $snapshot.index.digest; issue_numbers = @($created.number)
        }
        $offset = 0
        do {
            $page = Get-ScheduledTriageProblemPage $snapshot.problems[[string]$created.number] $offset
            $page.end_offset | Should -BeGreaterThan $offset
            $null = Invoke-TriageTransaction $Fixture.context triage-record-problem-page @{
                issue_number = $created.number; full_read_digest = $page.full_read_digest
                offset = $offset; end_offset = $page.end_offset; total_length = $page.total_length
            }
            $offset = $page.next_offset
        } while ($null -ne $offset)
        $proposal = Copy-TriageFixtureValue $Fixture.proposal
        $proposal.checkpoint = 2; $proposal.index_digest = $snapshot.index.digest
        $proposal.considered_issues = @($created.number)
        $entry = $snapshot.index.entries[0]
        $entry.full_read_digest = $entry.record_digest
        $proposal.problems[0].matching = @{
            kind = 'existing'; issue_number = $created.number; expected_generation = 1; expected_scope_revision = 1
            target_generation = 1; record_digest = $entry.record_digest; full_read_digest = $entry.record_digest
            relation = 'repeat'; reason = 'The source-access failure matches this existing problem'
        }
        $proposal.problems[1].matching.closest_candidates = @(@{
            issue_number = $created.number; full_read_digest = $entry.record_digest
            reason = 'Archive corruption is independent of source access'
        })
        $null = Invoke-TriageTransaction $Fixture.context triage-checkpoint @{
            checkpoint = @{ analysis = $proposal; index = $snapshot.index; evidence = $Fixture.evidence
                basis = $analysis.checkpoint.basis }
        }
        $Fixture.snapshot = $snapshot
        return $created.number
    }
}

Describe 'Several problems in one analysis' {
    BeforeEach {
        $script:fixture = Initialize-TriageFixture -Root (Join-Path $TestDrive ([guid]::NewGuid().ToString('N'))) -ProblemCount 2
    }

    It 'publishes unrelated A and B from an empty index without restarting analysis for its own writes' {
        $first = Publish-FixtureProblem $fixture download
        $second = Publish-FixtureProblem $fixture archive
        $first.link.issue_number | Should -Not -Be $second.link.issue_number
        (Complete-ScheduledTriageAnalysis $fixture.context $fixture.api).run_triaged | Should -BeTrue
        $state = Invoke-TriageTransaction $fixture.context read
        $state.triage.analyses.Count | Should -Be 1
        $state.triage.analyses[$fixture.context.analysis_id].checkpoint.analysis.checkpoint | Should -Be 1
        $state.triage.analyses[$fixture.context.analysis_id].continuations.Count | Should -Be 0
        @($fixture.store.writes | Where-Object { $_.endpoint -ceq 'repos/owner/repository/issues' }).Count | Should -Be 2
    }

    It 'still requires reconsideration when an external comment changes the confirmed first problem' {
        $first = Publish-FixtureProblem $fixture download
        $fixture.store.next_comment++
        $fixture.store.comments[[long]$first.link.issue_number].Add(@{
            id = $fixture.store.next_comment; body = 'Additional evidence requires reconsideration'
            user = @{ login = 'human' }; issue_url = "https://api.github.com/repos/owner/repository/issues/$($first.link.issue_number)"
        })
        (Invoke-ScheduledTriageProblemPreparation $fixture.context $fixture.snapshot archive $fixture.api).action |
            Should -Be reanalysis-required
        @($fixture.store.writes | Where-Object { $_.endpoint -ceq 'repos/owner/repository/issues' }).Count | Should -Be 1
    }

    It 'updates an existing problem then creates an independent new problem in the same checkpoint' {
        $existing = Add-FixtureExistingProblem $fixture
        $before = $fixture.store.writes.Count
        $updated = Publish-FixtureProblem $fixture download
        $created = Publish-FixtureProblem $fixture archive
        $updated.link.issue_number | Should -Be $existing
        $created.link.issue_number | Should -Not -Be $existing
        (Complete-ScheduledTriageAnalysis $fixture.context $fixture.api).run_triaged | Should -BeTrue
        @($fixture.store.writes | Select-Object -Skip $before |
            Where-Object { $_.endpoint -ceq 'repos/owner/repository/issues' }).Count | Should -Be 1
    }
}
