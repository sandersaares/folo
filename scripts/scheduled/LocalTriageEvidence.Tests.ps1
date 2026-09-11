#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
# Rejects missing/conflicting exact-attempt inventories and unsupported completion bases.
# Each case begins with a real serialized reporter record and changes only its tested boundary.
BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'fixtures\TriageFixture.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'LocalTriageEvidence.psm1')
}

Describe 'Exact-attempt completeness evidence' {
    BeforeEach {
        $script:fixture = Initialize-TriageFixture -Root (Join-Path $TestDrive ([guid]::NewGuid().ToString('N')))
        $script:record = $fixture.snapshot.runs['20'].record
        $script:revision = $record.revisions[0]
        $script:run = & $fixture.api -Endpoint 'repos/owner/repository/actions/runs/789/attempts/1'
    }

    It 'rejects <Case> rather than inventing an empty successful inventory' -ForEach @(
        @{ Case = 'absent pages'; Change = { $fixture.store.api_pages = @() } }
        @{ Case = 'missing count'; Change = { $fixture.store.api_pages[0].Remove('total_count') } }
        @{ Case = 'changing totals'; Change = { $fixture.store.api_pages += @{ total_count = 2; jobs = @() } } }
        @{ Case = 'duplicate jobs'; Change = { $fixture.store.api_pages[0].jobs += $fixture.store.api_pages[0].jobs[0] } }
        @{ Case = 'wrong attempt'; Change = { $fixture.store.api_pages[0].jobs[0].run_attempt = 2 } }
        @{ Case = 'missing steps'; Change = { $fixture.store.api_pages[0].jobs[0].Remove('steps') } }
        @{ Case = 'duplicate steps'; Change = { $fixture.store.api_pages[0].jobs[0].steps += $fixture.store.api_pages[0].jobs[0].steps[0] } }
        @{ Case = 'partial jobs'; Change = { $fixture.store.api_pages[0].total_count = 2 } }
    ) {
        & $Change
        { Get-ScheduledTriageEvidenceBasis $fixture.context.policy $record $revision $fixture.api $run } | Should -Throw
    }

    It 'rejects conflicting same-attempt controller and candidate sources' {
        $other = Copy-TriageFixtureValue $revision
        $other.digest = 'b' * 64; $other.evidence.attempt.controller_sha = 'b' * 40
        $record.revisions += $other
        { Get-ScheduledTriageEvidenceBasis $fixture.context.policy $record $revision $fixture.api $run } | Should -Throw
        $other.evidence.attempt.controller_sha = $revision.evidence.attempt.controller_sha
        $other.evidence.attempt.manifest.source_sha = 'c' * 40
        { Get-ScheduledTriageEvidenceBasis $fixture.context.policy $record $revision $fixture.api $run } | Should -Throw
    }

    It 'rejects a support revision missing from the committed index and changed execution facts' {
        $basis = $fixture.snapshot.pending[0].basis
        $invalid = Copy-TriageFixtureValue $basis
        $invalid.supporting_revisions = @(@{ digest = 'missing'; evidence = $revision.evidence })
        { Assert-ScheduledTriageEvidenceBasis $invalid $basis $record } | Should -Throw
        $invalid = Copy-TriageFixtureValue $basis
        $invalid.api_evidence.jobs[0].steps[0].conclusion = 'success'
        { Assert-ScheduledTriageEvidenceBasis $invalid $basis $record } | Should -Throw
        Assert-ScheduledTriageEvidenceBasis $basis $basis $record
    }
}
