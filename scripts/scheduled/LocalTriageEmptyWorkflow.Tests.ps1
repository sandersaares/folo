#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
# Contrasts proved empty execution with incomplete retrieval. Workflow-level accounting is
# explicit and never creates checker/source diagnoses for jobs that did not exist.
BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'fixtures\TriageFixture.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'LocalTriageInbox.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'LocalTriageCompletion.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'LocalTriagePublication.psm1') -Force
}

Describe 'Exact-attempt empty workflow evidence' {
    It 'records evidenced <Conclusion> without inventing a job or problem' -ForEach @(
        @{ Conclusion = 'cancelled' }, @{ Conclusion = 'failure' }
    ) {
        $fixture = Initialize-TriageFixture -Root (Join-Path $TestDrive ([guid]::NewGuid().ToString('N'))) `
            -EmptyWorkflowConclusion $Conclusion
        $result = Complete-ScheduledTriageAnalysis $fixture.context $fixture.api
        $result.run_triaged | Should -BeTrue
        @($fixture.store.writes | Where-Object { $_.endpoint -ceq 'repos/owner/repository/issues' }).Count | Should -Be 0
        $state = Invoke-TriageTransaction $fixture.context read
        $state.triage.analyses[$fixture.context.analysis_id].checkpoint.analysis.jobs.Count | Should -Be 0
        $state.triage.analyses[$fixture.context.analysis_id].checkpoint.analysis.problems.Count | Should -Be 0
    }

    It 'does not equate absent pages or inconsistent totals with a complete empty inventory' {
        $fixture = Initialize-TriageFixture -Root (Join-Path $TestDrive ([guid]::NewGuid().ToString('N'))) `
            -EmptyWorkflowConclusion cancelled
        $state = Invoke-TriageTransaction $fixture.context read
        $fixture.store.api_pages = @()
        { Get-ScheduledTriageInbox $fixture.context.policy $state $fixture.api } | Should -Throw
        $fixture.store.api_pages = @(@{ total_count = 1; jobs = @() })
        { Get-ScheduledTriageInbox $fixture.context.policy $state $fixture.api } | Should -Throw
        $fixture.store.api_pages = @(@{ total_count = 0; jobs = @() }, @{ total_count = 1; jobs = @() })
        { Get-ScheduledTriageInbox $fixture.context.policy $state $fixture.api } | Should -Throw
    }
}
