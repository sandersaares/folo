#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
# Tests the exact own-write normalization used between independent problem publications.
# Receipt equality never licenses unrelated metadata changes or a different issue creation.
BeforeDiscovery { Import-Module (Join-Path $PSScriptRoot 'LocalTriageIndex.psm1') }
BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'fixtures\TriageFixture.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'fixtures\TriageScenarioFixture.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'LocalTriageIndex.psm1')
    Import-Module (Join-Path $PSScriptRoot 'LocalTriageProblem.psm1')
    Import-Module (Join-Path $PSScriptRoot 'LocalTriageCompletion.psm1')
    Import-Module (Join-Path $PSScriptRoot 'LocalTriagePublication.psm1')
}

Describe 'Verified own issue normalization' {
    InModuleScope LocalTriageIndex {
        BeforeEach {
            $script:context = @{ login = 'worker' }
            $script:plan = @{
                original_issue = @{ body = '[Copilot speaking] Original'; title = 'Title'; state = 'closed'
                    user = @{ login = 'worker' }; labels = @(@{ name = 'scheduled-finding' }) }
                creation_payload = $null
            }
            $script:operation = @{ payload = @{ body = 'Owned content'; state = 'open' }; receipt = @{} }
            $script:current = $plan.original_issue.Clone()
            $current.body += "`n`nOwned content"; $current.state = 'open'
            $operation.receipt.target = $current
        }

        It 'normalizes a verified reopening but rejects another observed state or unrelated metadata' {
            Test-TriageOwnIssueChange $context $plan $operation $current | Should -BeTrue
            $current.state = 'closed'
            Test-TriageOwnIssueChange $context $plan $operation $current | Should -BeFalse
            $current.state = 'open'; $current.title = 'External title'
            Test-TriageOwnIssueChange $context $plan $operation $current | Should -BeFalse
        }

        It 'does not accept a verified root on an issue that differs from the original creation payload' {
            $plan.creation_payload = @{ body = $plan.original_issue.body; title = 'Different'
                labels = @('scheduled-finding') }
            Test-TriageOwnIssueChange $context $plan $operation $current | Should -BeFalse
        }
    }

    It 'requires reanalysis when an external edit to <Target> accompanies a confirmed root publication' -ForEach @(
        @{ Target = 'current' }, @{ Target = 'another problem' }
    ) {
        $fixture = Initialize-TriageFixture -Root (Join-Path $TestDrive ([guid]::NewGuid().ToString('N'))) -ProblemCount 2
        $problemKey = 'download'
        $changedNumber = $null
        if ($Target -ceq 'another problem') {
            $changedNumber = (Publish-TriageFixtureProblem $fixture download).link.issue_number
            $problemKey = 'archive'
        }
        $handoff = Invoke-ScheduledTriageProblemPreparation $fixture.context $fixture.snapshot $problemKey $fixture.api
        $created = & $fixture.api -Endpoint 'repos/owner/repository/issues' -Method POST -Body $handoff.payload
        if ($null -eq $changedNumber) { $changedNumber = $created.number }
        $null = Invoke-ScheduledTriageProblemPreparation $fixture.context $fixture.snapshot $problemKey $fixture.api
        $api = {
            param($Endpoint, $Method = 'GET', $Body, [switch] $Paginate)
            $result = & $fixture.api -Endpoint $Endpoint -Method $Method -Body $Body -Paginate:$Paginate
            if ($Method -ceq 'PATCH' -and $Endpoint.EndsWith("/issues/$($created.number)")) {
                $fixture.store.issues[[long]$changedNumber].title = 'Concurrent title change'
            }
            return $result
        }
        (Publish-ScheduledTriageProblem $fixture.context $problemKey $api).action | Should -Be reanalysis-required
        (Complete-ScheduledTriageAnalysis $fixture.context $fixture.api).action | Should -Be reanalysis-required
        $fixture.store.issues[20L].state | Should -Be open
        { Invoke-ScheduledTriageProblemPreparation $fixture.context $fixture.snapshot unknown $fixture.api } | Should -Throw
    }
}
