#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
# Exercises complete-index provenance failures with the real reporter codec and native adapter.
# Inventory or identity errors stay visible instead of becoming an empty successful queue.
BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'fixtures\TriageFixture.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'fixtures\TriageScenarioFixture.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'LocalTriageInbox.psm1')
    Import-Module (Join-Path $PSScriptRoot 'LocalTriagePublication.psm1')
    Import-Module (Join-Path $PSScriptRoot 'LocalTriageCompletion.psm1')
    Import-Module (Join-Path $PSScriptRoot 'ScheduledContracts.psm1')
    Import-Module (Join-Path $PSScriptRoot 'ScheduledRecordTool.psm1')
}

Describe 'Complete reporter and problem inventory' {
    BeforeEach {
        $script:fixture = Initialize-TriageFixture -Root (Join-Path $TestDrive ([guid]::NewGuid().ToString('N')))
        $script:state = Invoke-TriageTransaction $fixture.context read
    }

    It 'uses the paginated native adapter for scalar, collection and page reads' {
        Mock Invoke-ScheduledApi -ModuleName LocalTriageInbox { @{ endpoint = $Endpoint } }
        Mock Get-ScheduledApiCollection -ModuleName LocalTriageInbox { ,@(@{ id = 1 }, @{ id = 2 }) }
        (Invoke-ScheduledTriageRead -Endpoint user).endpoint | Should -Be user
        @(Invoke-ScheduledTriageRead -Endpoint jobs -Pages).Count | Should -Be 1
        (Invoke-ScheduledTriageRead -Endpoint issues -Collection).Count | Should -Be 2
        Should -Invoke Invoke-ScheduledApi -ModuleName LocalTriageInbox -ParameterFilter { $Paginate }
    }

    It 'does not reinterpret an already restored run carrying the historical finding label as a problem' {
        $fixture.store.issues[20L].labels += @{ name = 'scheduled-finding' }
        $script:reads = @{ comments = 0 }
        $api = {
            param($Endpoint, [switch] $Collection, [switch] $Pages)
            if ($Endpoint.Contains('/issues/20/comments')) { $reads.comments++ }
            & $fixture.api -Endpoint $Endpoint -Collection:$Collection -Pages:$Pages
        }
        $snapshot = Get-ScheduledTriageInbox $fixture.context.policy $state $api
        $snapshot.backlog_count | Should -Be 1
        $snapshot.index.entries.Count | Should -Be 0
        $reads.comments | Should -Be 1
    }

    It 'rejects <Case> from the complete inventory' -ForEach @(
        @{ Case = 'foreign run ownership'; Change = { $fixture.store.issues[20L].user.login = 'unrelated' } }
        @{ Case = 'uncommitted reporter index'; Change = { $fixture.store.comments[20L].Clear() } }
        @{ Case = 'duplicate canonical runs'; Change = {
            $copy = Copy-TriageFixtureValue $fixture.store.issues[20L]; $copy.number = 21
            $fixture.store.issues[21L] = $copy; $fixture.store.comments[21L] = $fixture.store.comments[20L]
        } }
        @{ Case = 'conflicting comment IDs'; Change = {
            $copy = Copy-TriageFixtureValue $fixture.store.comments[20L][0]; $copy.body = 'different'
            $fixture.store.comments[20L].Add($copy)
        } }
        @{ Case = 'invalid retained identity'; Change = { $state.triage.known_run_issues = @(-1) } }
        @{ Case = 'different numeric account'; Change = { $state.triage.profile.user_id = 11 } }
        @{ Case = 'uninterpretable finding'; Change = {
            $fixture.store.issues[30L] = @{ number = 30; title = 'Finding'; body = 'Missing record'
                user = @{ login = 'worker' }; state = 'open'; labels = @(@{ name = 'scheduled-finding' }) }
            $fixture.store.comments[30L] = @()
        } }
    ) {
        & $Change
        { Get-ScheduledTriageInbox $fixture.context.policy $state $fixture.api } | Should -Throw
        $fixture.store.writes.Count | Should -Be 0
    }

    It 'rejects partial or changing collection responses instead of selecting a convenient page' -ForEach @(
        @{ Case = 'issues'; Match = 'issues?'; Value = @{ incomplete = $true } }
        @{ Case = 'comments'; Match = 'issues/20/comments'; Value = @{ incomplete = $true } }
        @{ Case = 'repository'; Match = 'repos/owner/repository'; Value = @{ id = 124; full_name = 'owner/repository' } }
        @{ Case = 'retained ID'; Match = 'issues/20'; Value = @{ number = 21 } }
        @{ Case = 'run provenance'; Match = 'attempts/1'; Value = @{ repository = @{ id = 124 } } }
        @{ Case = 'source ancestry'; Match = 'compare/'; Value = @{ status = 'diverged' } }
    ) {
        $api = {
            param($Endpoint, [switch] $Collection, [switch] $Pages)
            $matchesEndpoint = if ($Case -ceq 'repository') { $Endpoint -ceq $Match } else {
                $Endpoint.Contains($Match)
            }
            if ($matchesEndpoint) { return $Value }
            & $fixture.api -Endpoint $Endpoint -Collection:$Collection -Pages:$Pages
        }
        { Get-ScheduledTriageInbox $fixture.context.policy $state $api } | Should -Throw
        $fixture.store.writes.Count | Should -Be 0
    }

    It 'rejects conflicting paginated issue versions' {
        $api = {
            param($Endpoint, [switch] $Collection, [switch] $Pages)
            if ($Endpoint.Contains('issues?')) {
                $copy = Copy-TriageFixtureValue $fixture.store.issues[20L]; $copy.title = 'Concurrent change'
                return ,@($fixture.store.issues[20L], $copy)
            }
            & $fixture.api -Endpoint $Endpoint -Collection:$Collection -Pages:$Pages
        }
        { Get-ScheduledTriageInbox $fixture.context.policy $state $api } | Should -Throw
    }

    It 'requires canonical document ownership and its committed current digest' {
        $problem = Publish-TriageFixtureProblem $fixture download
        $number = [long]$problem.link.issue_number
        $original = $fixture.store.issues[$number].body
        foreach ($damage in @('author', 'root', 'current', 'identity')) {
            $fixture.store.issues[$number].body = $original
            $fixture.store.issues[$number].user.login = 'worker'
            $root = Read-ScheduledRecord $original problem
            switch ($damage) {
                author { $fixture.store.issues[$number].user.login = 'foreign' }
                root { $root.repository_id = 124 }
                current { $root.current_digest = 'c' * 64 }
                identity {
                    # The detail's canonical identity is checked independently of the compact root.
                    $root.issue_number = 32
                }
            }
            if ($damage -cne 'author') {
                $fixture.store.issues[$number].body = Write-ScheduledRecord $root problem
            }
            { Get-ScheduledTriageInbox $fixture.context.policy (Invoke-TriageTransaction $fixture.context read) $fixture.api } |
                Should -Throw
        }
    }

    It 'does not adopt uncommitted or partial document pages and rejects an absent candidate' {
        $prepared = Invoke-ScheduledRecordTool -Package scheduled-run-record -Request @{
            op = 'prepare_document'; kind = 'problem'; owner = '123/30'; document = @{ diagnosis = 'uncommitted' }
        }
        $comments = @($prepared.pages | ForEach-Object { @{ id = 1; body = $_.body; user = @{ login = 'worker' } } })
        { Get-ScheduledTriageDocumentCatalog $fixture.context.policy 30 problem $comments $null } | Should -Throw
        $large = Invoke-ScheduledRecordTool -Package scheduled-run-record -Request @{
            op = 'prepare_document'; kind = 'problem'; owner = '123/30'; document = @{ diagnosis = 'x' * 60000 }
        }
        $large.pages.Count | Should -BeGreaterThan 1
        $partial = @(@{ id = 1; body = $large.pages[0].body; user = @{ login = 'worker' } })
        { Get-ScheduledTriageDocumentCatalog $fixture.context.policy 30 problem $partial $null } | Should -Throw
        { Get-ScheduledTriageProblem $fixture.snapshot 999 } | Should -Throw
    }

    It 'rejects a triage root claiming a different workflow run' {
        $null = Publish-TriageFixtureProblem $fixture download
        $null = Complete-ScheduledTriageAnalysis $fixture.context $fixture.api
        $comment = @($fixture.store.comments[20L] | Where-Object { $_.body.Contains('<!-- scheduled-triage:v1 ') })[0]
        $root = Read-ScheduledRecord $comment.body triage
        $root.run_id = 790
        $comment.body = Write-ScheduledRecord $root triage
        { Get-ScheduledTriageInbox $fixture.context.policy $state $fixture.api } | Should -Throw
    }

    It 'rejects <Case> in codec-valid committed detail rather than trusting a triaged label' -ForEach @(
        @{ Case = 'foreign canonical problem'; Damage = 'identity' }
        @{ Case = 'incomplete publication'; Damage = 'publication' }
        @{ Case = 'unconfirmed problem link'; Damage = 'link' }
        @{ Case = 'run used as a problem link'; Damage = 'run-link' }
    ) {
        $published = Publish-TriageFixtureProblem $fixture download
        $null = Complete-ScheduledTriageAnalysis $fixture.context $fixture.api
        $snapshot = Get-ScheduledTriageInbox $fixture.context.policy (Invoke-TriageTransaction $fixture.context read) $fixture.api
        if ($Damage -ceq 'identity') {
            $record = Copy-TriageFixtureValue $snapshot.problems[[string]$published.link.issue_number].record.problem
            $record.repository_id = 124
            Invoke-TriageFixtureDocument $fixture $published.link.issue_number problem $record
        } else {
            $record = Copy-TriageFixtureValue $snapshot.runs['20'].details.revisions[0].document
            $record.analysis.checkpoint = 2
            if ($Damage -ceq 'publication') { $record.publication_complete = $false } elseif ($Damage -ceq 'run-link') {
                $record.problem_links.download.issue_number = 20
            } else {
                $record.problem_links.download.problem_digest = 'c' * 64
            }
            Invoke-TriageFixtureDocument $fixture 20 triage $record
        }
        { Get-ScheduledTriageInbox $fixture.context.policy $state $fixture.api } | Should -Throw
        { Invoke-TriageTransaction $fixture.context reserve-attempt } | Should -Throw '*ai-triage-unavailable*'
    }
}
