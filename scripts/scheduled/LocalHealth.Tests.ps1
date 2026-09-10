#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
# Validates independent role observations and journaled health publication on one existing
# reporter surface. All GitHub responses and time are injected; no health issue is created.
BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'fixtures\TriageFixture.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'LocalHealth.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'LocalTriagePublication.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'ScheduledContracts.psm1') -Force
}

Describe 'Independent Local health publication' {
    BeforeEach {
        $script:fixture = Initialize-TriageFixture -Root (Join-Path $TestDrive ([guid]::NewGuid().ToString('N')))
        Mock Get-ScheduledTriagePolicy -ModuleName LocalHealthState { $fixture.context.triage_policy }
        $coverage = Write-ScheduledRecord -Kind coverage -Record @{
            schema_version = 1; repository = 'owner/repository'; repository_id = 123
        }
        $fixture.store.issues[10L] = @{
            number = 10; title = 'Health'; state = 'open'; body = "[Copilot speaking]`n$coverage"
            user = @{ login = 'reporter' }
            labels = @(@{ name = 'scheduled-health' }, @{ name = 'scheduled-coverage' })
        }
        $fixture.store.comments[10L] = [Collections.Generic.List[object]]::new()
        $script:healthContext = $fixture.context.Clone()
        $healthContext.role = 'triage'
        $null = Invoke-TriageTransaction $fixture.context triage-record-scan @{
            scan_token = $fixture.context.scan_token; successful = $true
            backlog_count = 1; oldest_pending_at = '2026-09-09T01:00:01Z'
            blocked_conditions = @(); run_issues = @(20); problem_issues = @()
        }
    }

    It 'publishes one role comment and does not confuse unchanged registration with a new scan' {
        $first = Sync-ScheduledRoleHealth $healthContext $fixture.api
        $first.action | Should -Be published
        (Sync-ScheduledRoleHealth $healthContext $fixture.api).action | Should -Be unchanged
        $fixture.store.comments[10L].Count | Should -Be 1
        $record = Read-ScheduledRecord $fixture.store.comments[10L][0].body health
        $record.role | Should -Be triage
        $record.backlog_count | Should -Be 1
        $record.active.analysis_id | Should -Be $fixture.context.analysis_id
        @($fixture.store.writes | Where-Object { $_.endpoint -ceq 'repos/owner/repository/issues' }).Count | Should -Be 0
    }

    It 'reconciles a lost comment create and preserves human discussion during a lost update' {
        $fixture.store.lose_comment = $true
        { Sync-ScheduledRoleHealth $healthContext $fixture.api } | Should -Throw
        (Sync-ScheduledRoleHealth $healthContext $fixture.api).action | Should -Be reconciled
        $healthContext.now = $healthContext.now.AddMinutes(1)
        $changed = $fixture.context.Clone(); $changed.now = $healthContext.now
        $null = Invoke-TriageTransaction $changed triage-record-scan @{
            scan_token = $changed.scan_token; successful = $false
            backlog_count = 0; oldest_pending_at = $null
            blocked_conditions = @('api-unavailable'); run_issues = @(); problem_issues = @()
        }
        $fixture.store.lose_update = $true
        { Sync-ScheduledRoleHealth $healthContext $fixture.api } | Should -Throw
        $fixture.store.comments[10L][0].body += "`nHuman discussion stays."
        (Sync-ScheduledRoleHealth $healthContext $fixture.api).action | Should -Be reconciled
        $fixture.store.comments[10L].Count | Should -Be 1
        $fixture.store.comments[10L][0].body | Should -Match 'Human discussion stays'
        $record = Read-ScheduledRecord $fixture.store.comments[10L][0].body health
        $record.last_successful_scan | Should -Be $fixture.context.now
        $record.blocked_conditions | Should -Be @('api-unavailable')
    }

    It 'projects repair health independently of triage activity and does not adopt its comment' {
        $null = Sync-ScheduledRoleHealth $healthContext $fixture.api
        $state = Invoke-TriageTransaction $fixture.context read
        $repair = Get-ScheduledRoleHealthRecord $state repair
        $repair.role | Should -Be repair
        $repair.last_successful_scan | Should -BeNullOrEmpty
        $repair.active.Count | Should -Be 0
        $repair.backlog_count | Should -Be 0
        $fixture.store.comments[10L].Count | Should -Be 1
    }

    It 'blocks a missing or ambiguous surface without creating replacement issues' {
        $fixture.store.issues[10L].labels = @()
        { Sync-ScheduledRoleHealth $healthContext $fixture.api } | Should -Throw
        $fixture.store.writes.Count | Should -Be 0
        $fixture.store.issues[10L].labels = @(@{ name = 'scheduled-health' }, @{ name = 'scheduled-coverage' })
        $duplicate = Copy-TriageFixtureValue $fixture.store.issues[10L]
        $duplicate.number = 11
        $fixture.store.issues[11L] = $duplicate
        { Sync-ScheduledRoleHealth $healthContext $fixture.api } | Should -Throw
    }

    It 'rejects wrong role, stale scan and unenrolled health publication' {
        $invalid = $healthContext.Clone(); $invalid.role = 'unknown'
        { Sync-ScheduledRoleHealth $invalid $fixture.api } | Should -Throw
        $invalid = $healthContext.Clone(); $invalid.scan_token = 'stale'
        { Sync-ScheduledRoleHealth $invalid $fixture.api } | Should -Throw
        $fixture.context.triage_policy.enrolled_machine_id = $null
        { Sync-ScheduledRoleHealth $healthContext $fixture.api } | Should -Throw
        $fixture.store.writes.Count | Should -Be 0
    }
}
