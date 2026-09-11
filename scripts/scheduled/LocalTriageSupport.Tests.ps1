#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
# Proves partial reporter collection can use committed same-attempt support without changing
# its primary claim or acknowledging the supporting revision, including disk-state recovery.
BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'fixtures\TriageFixture.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'LocalTriageProblem.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'LocalTriagePublication.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'LocalTriageCompletion.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'LocalTriageInbox.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'ScheduledContracts.psm1') -Force
}

Describe 'Durable same-attempt completion basis' {
    It 'recovers a partial original using persisted support and leaves that support unprocessed' {
        $fixture = Initialize-TriageFixture -Root (Join-Path $TestDrive ([guid]::NewGuid().ToString('N'))) -PartialWithSupport
        $state = Invoke-TriageTransaction $fixture.context read
        $before = $state.triage.analyses[$fixture.context.analysis_id].checkpoint.basis
        $before.api_evidence.jobs.Count | Should -Be 1
        $before.supporting_revisions[0].digest | Should -Be $fixture.support.digest
        $handoff = Invoke-ScheduledTriageProblemPreparation $fixture.context $fixture.snapshot download $fixture.api
        $null = & $fixture.api -Endpoint 'repos/owner/repository/issues' -Method POST -Body $handoff.payload
        $resumed = Copy-TriageFixtureValue $fixture.context
        $null = Invoke-ScheduledTriageProblemPreparation $resumed $fixture.snapshot download $fixture.api
        $null = Publish-ScheduledTriageProblem $resumed download $fixture.api
        $result = Complete-ScheduledTriageAnalysis $resumed $fixture.api
        $result.analysis_status | Should -Be complete
        $result.run_triaged | Should -BeFalse
        $result.pending_revision_count | Should -Be 1
        $state = Invoke-TriageTransaction $resumed read
        $current = $state.triage.analyses[$resumed.analysis_id]
        (Get-ScheduledDigest $current.checkpoint.basis) | Should -Be (Get-ScheduledDigest $before)
        $current.revision.digest | Should -Not -Be $fixture.support.digest
        $snapshot = Get-ScheduledTriageInbox $resumed.policy $state $fixture.api
        $snapshot.pending[0].digest | Should -Be $fixture.support.digest
        $fixture.store.issues[20L].state | Should -Be open
    }
}
