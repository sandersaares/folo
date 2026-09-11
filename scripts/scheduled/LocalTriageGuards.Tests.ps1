#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
# Exercises remaining structural guards, including partial-line branches, without live writes.
# Module imports happen during execution, also protecting the independent public entry points.
BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'fixtures\TriageFixture.psm1')
    Import-Module (Join-Path $PSScriptRoot 'fixtures\TriageScenarioFixture.psm1')
    Import-Module (Join-Path $PSScriptRoot 'LocalTriagePublication.psm1')
    Import-Module (Join-Path $PSScriptRoot 'LocalTriageCompletion.psm1')
    Import-Module (Join-Path $PSScriptRoot 'LocalTriageInbox.psm1')
    Import-Module (Join-Path $PSScriptRoot 'LocalTriageProblem.psm1')
    Import-Module (Join-Path $PSScriptRoot 'LocalTriageState.psm1')
    Import-Module (Join-Path $PSScriptRoot 'LocalTriageIndex.psm1')
    Import-Module (Join-Path $PSScriptRoot 'LocalTriageView.psm1')
    Import-Module (Join-Path $PSScriptRoot 'LocalHealth.psm1')
    Import-Module (Join-Path $PSScriptRoot 'LocalHealthState.psm1')
    Import-Module (Join-Path $PSScriptRoot 'ScheduledRoleHealth.psm1')
    Import-Module (Join-Path $PSScriptRoot 'ScheduledContracts.psm1')
    Import-Module (Join-Path $PSScriptRoot 'LocalSetup.psm1')
    Import-Module (Join-Path $PSScriptRoot 'LocalSetupState.psm1')
}

Describe 'Triage structural guards and owned blocks' {
    BeforeEach {
        $script:fixture = Initialize-TriageFixture -Root (Join-Path $TestDrive ([guid]::NewGuid().ToString('N')))
    }

    It 'requires a complete role state and the registered owner on every dispatch path' {
        { Assert-ScheduledTriageState $null } | Should -Throw
        { Assert-ScheduledTriageState @{} } | Should -Throw
        foreach ($action in @('triage-reserve-continuation', 'triage-accept-dispatch')) {
            { Invoke-TriageTransaction $fixture.context $action @{
                analysis_id = 'unknown'; scan_token = $fixture.context.scan_token
                native_idle_verified = $true; evidence_key = 'new'
            } } | Should -Throw
        }
        $state = Invoke-TriageTransaction $fixture.context read
        $analysis = $state.triage.analyses[$fixture.context.analysis_id]
        $analysis.checkpoint = $null
        $data = @{ analysis_id = $analysis.id; session_id = $analysis.session_id
            claim_token = $analysis.claim_token; dispatch_token = $analysis.dispatch.token
            key = 'document'; document = @{} }
        { Invoke-ScheduledTriageStateChange $state $fixture.context.policy $fixture.context.triage_policy `
            triage-prepare-document $data $fixture.context.now } | Should -Throw
        Mock Invoke-TriageTransaction -ModuleName LocalTriageCompletion { $state }
        { Complete-ScheduledTriageAnalysis $fixture.context $fixture.api } | Should -Throw
        { Publish-ScheduledTriageProblem $fixture.context download $fixture.api } | Should -Throw
    }

    It 'rejects ambiguous content blocks and label ownership before a write' {
        InModuleScope LocalTriagePublication -Parameters @{
            Context = $fixture.context
            Transport = { ,@(@{ name = 'scheduled-finding' }, @{ name = 'SCHEDULED-FINDING' }) }
        } {
            $block = ConvertTo-TriageOwnedBlock problem 'content'
            { Get-TriageOwnedBlock "$block$block" problem } | Should -Throw
            $operation = @{ kind = 'create-label'; payload = @{ name = 'scheduled-finding' } }
            { Find-TriageOperationTarget $Context $operation $Transport } | Should -Throw
        }
        $fixture.store.writes.Count | Should -Be 0
    }

    It 'does not interpret foreign reporter roots or ambiguous triage roots' {
        $root = Read-ScheduledRecord $fixture.store.issues[20L].body run
        $before = $fixture.store.issues[20L].body
        $root.repository_id = 124
        $fixture.store.issues[20L].body = Write-ScheduledRecord $root run
        $fixture.store.issues[20L].body += "`n$(Write-ScheduledRecord @{schema_version=1;index_digest='x'} run-publication)"
        { Get-ScheduledTriageInbox $fixture.context.policy (Invoke-TriageTransaction $fixture.context read) $fixture.api } |
            Should -Throw
        $fixture.store.issues[20L].body = $before
        foreach ($id in @(900, 901)) {
            $fixture.store.comments[20L].Add(@{ id = $id; body = '<!-- scheduled-triage:v1 {} -->'; user = @{ login = 'worker' } })
        }
        { Get-ScheduledTriageInbox $fixture.context.policy (Invoke-TriageTransaction $fixture.context read) $fixture.api } |
            Should -Throw
    }

    It 'does not replace an unframed triage root or mistake complete work for new recovery input' {
        $null = Publish-TriageFixtureProblem $fixture download
        $null = Complete-ScheduledTriageAnalysis $fixture.context $fixture.api
        $state = Invoke-TriageTransaction $fixture.context read
        (Get-ScheduledTriageRecovery $fixture.context.policy $state $fixture.api).complete_input | Should -BeTrue
        $comment = @($fixture.store.comments[20L] | Where-Object { $_.body.Contains('<!-- scheduled-triage:v1 ') })[0]
        $comment.body = Write-ScheduledRecord (Read-ScheduledRecord $comment.body triage) triage
        # A fresh publication checkpoint encounters the existing root's lost content boundary.
        $state.triage.analyses[$fixture.context.analysis_id].publication.Remove('1/completion')
        Mock Invoke-TriageTransaction -ModuleName LocalTriageCompletion { $state }
        { Complete-ScheduledTriageAnalysis $fixture.context $fixture.api } | Should -Throw
    }

    It 'preserves an older contribution without treating unrelated source as a recurrence' {
        $problem = Publish-TriageFixtureProblem $fixture download
        $null = Complete-ScheduledTriageAnalysis $fixture.context $fixture.api
        $older = Copy-TriageFixtureValue $fixture.evidence
        $older.run_id = 788; $older.attempt.run_number = 41; $older.attempt.manifest.source_sha = 'c' * 40
        $older.attempt.created_at = '2026-09-09T00:50:00Z'; $older.attempt.started_at = '2026-09-09T00:50:01Z'
        $older.attempt.completed_at = '2026-09-09T00:50:02Z'
        $null = Add-TriageFixtureRevision $fixture $older 21
        Invoke-TriageFixtureNextAnalysis $fixture 788
        $fixture.proposal.problems[0].matching = @{
            kind = 'existing'; issue_number = $problem.link.issue_number; target_generation = 1; relation = 'historical'
            reason = 'The older failure is the same cause on unrelated source, not a new occurrence.'
        }
        Invoke-TriageFixtureCheckpoint $fixture
        $transport = $fixture.api
        $fixture.api = {
            param($Endpoint, $Method = 'GET', $Body, [switch] $Paginate)
            if ($Endpoint.EndsWith("compare/$('a' * 40)...$('c' * 40)")) { return @{ status = 'diverged' } }
            & $transport -Endpoint $Endpoint -Method $Method -Body $Body -Paginate:$Paginate
        }.GetNewClosure()
        (Publish-TriageFixtureProblem $fixture download).link.generation | Should -Be 1
        (Complete-ScheduledTriageAnalysis $fixture.context $fixture.api).run_triaged | Should -BeTrue
    }

    It 'normalizes only a complete verified own issue and comment transition' {
        & (Get-Module LocalTriageIndex) {
            param($Context)
            $plan = @{ original_issue = @{ body = 'human'; state = 'open' }; original_comments = @(@{ id = 7; body = 'before' })
                issue_number = 31; creation_payload = $null }
            $current = @{ body = "[Copilot speaking]`n`nhuman`n`nowned"; state = 'open' }
            $operation = @{ payload = @{ body = 'owned' }; receipt = @{ target = $current } }
            Test-TriageOwnIssueChange $Context $plan $operation $current | Should -BeTrue
            $operation.receipt.target = $null
            Test-TriageOwnIssueChange $Context $plan $operation $current | Should -BeFalse
            $analysis = @{ checkpoint = @{ analysis = @{ checkpoint = 1 } }; operations = @{} }
            Test-TriageOwnCommentChange $analysis $plan @() | Should -BeFalse
            Test-TriageOwnCommentChange $analysis $plan @(@{ id = 7; body = 'changed' }) | Should -BeFalse
            $analysis.operations.a = @{ checkpoint = 1; issue_number = 31; kind = 'create-comment'; stage = 'confirmed'
                receipt = @{} }
            Test-TriageOwnCommentChange $analysis $plan @(@{ id = 7; body = 'before' }) | Should -BeFalse
        } $fixture.context
        $plan = @{ issue_number = 31 }
        (Sync-ScheduledTriageComparison $fixture.context $plan missing @{ index = $fixture.snapshot.index }) | Should -BeTrue
        $snapshot = @{ index = @{ digest = 'changed'; entries = @() }; problems = @{} }
        (Sync-ScheduledTriageComparison $fixture.context $plan missing $snapshot) | Should -BeFalse
    }

    It 'refuses ambiguous contributions in an otherwise codec-valid retained problem' {
        $problem = Publish-TriageFixtureProblem $fixture download
        $snapshot = Get-ScheduledTriageInbox $fixture.context.policy (Invoke-TriageTransaction $fixture.context read) $fixture.api
        $record = Copy-TriageFixtureValue $snapshot.problems[[string]$problem.link.issue_number].record.problem
        $record.evidence += Copy-TriageFixtureValue $record.evidence[0]
        Invoke-TriageFixtureDocument $fixture $problem.link.issue_number problem $record
        $fixture.proposal.checkpoint = 2
        $fixture.proposal.problems[0].matching = @{
            kind = 'existing'; issue_number = $problem.link.issue_number; target_generation = 1; relation = 'repeat'
            reason = 'The existing publication describes this observed access failure.'
        }
        Invoke-TriageFixtureCheckpoint $fixture
        { Invoke-ScheduledTriageProblemPreparation $fixture.context $fixture.snapshot download $fixture.api } | Should -Throw
        { Invoke-TriageTransaction $fixture.context reserve-attempt } | Should -Throw '*ai-triage-unavailable*'
    }

    It 'does not split a surrogate pair in an abbreviated summary' {
        & (Get-Module LocalTriageView) {
            $text = ('x' * 511) + [char]0xD83D + [char]0xDE00
            $summary = Get-TriageBriefText $text
            $summary.Length | Should -Be 511
        }
    }
}
