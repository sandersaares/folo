#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
# Validates independent role observations and journaled health publication on one existing
# reporter surface. All GitHub responses and time are injected; no health issue is created.
BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'fixtures\TriageFixture.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'LocalHealth.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'LocalTriagePublication.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'ScheduledContracts.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'LocalHealthState.psm1')
    Import-Module (Join-Path $PSScriptRoot 'ScheduledRoleHealth.psm1') -Force
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

    It 'does not create a replacement for a same-role comment from another repository or enrollment' {
        $state = Invoke-TriageTransaction $fixture.context read
        foreach ($field in @('repository_id', 'repository', 'executor_id')) {
            $record = Get-ScheduledRoleHealthRecord $state triage
            $record[$field] = if ($field -ceq 'repository_id') { 124 } else { 'foreign' }
            $fixture.store.comments[10L].Clear()
            $fixture.store.comments[10L].Add(@{
                id = 900; body = Write-ScheduledRecord $record health; user = @{ login = 'worker' }
                issue_url = 'https://api.github.com/repos/owner/repository/issues/10'
            })
            { Sync-ScheduledRoleHealth $healthContext $fixture.api } | Should -Throw -ExceptionType ([FormatException])
            $fixture.store.comments[10L].Count | Should -Be 1
            $fixture.store.writes.Count | Should -Be 0
        }
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

    It 'uses the production transport adapter without granting issue creation authority' {
        Mock Invoke-ScheduledGitHubApi -ModuleName LocalHealth {
            & $fixture.api -Endpoint $Endpoint -Method $Method -Body $Body -Paginate:$Paginate
        }
        (Sync-ScheduledRoleHealth $healthContext).action | Should -Be published
        Should -Invoke Invoke-ScheduledGitHubApi -ModuleName LocalHealth
    }

    It 'does not replace a known comment moved outside its canonical issue or role' {
        $null = Sync-ScheduledRoleHealth $healthContext $fixture.api
        $comment = $fixture.store.comments[10L][0]
        $comment.issue_url = 'https://api.github.com/repos/owner/repository/issues/11'
        { Sync-ScheduledRoleHealth $healthContext $fixture.api } | Should -Throw
        $comment.issue_url = 'https://api.github.com/repos/owner/repository/issues/10'
        $comment.user.login = 'another-user'
        { Sync-ScheduledRoleHealth $healthContext $fixture.api } | Should -Throw
        $fixture.store.comments[10L].Count | Should -Be 1
    }

    It 'keeps unavailable and conflicting uncertain publications blocked' {
        $fixture.store.lose_comment = $true
        { Sync-ScheduledRoleHealth $healthContext $fixture.api } | Should -Throw
        $comment = $fixture.store.comments[10L][0]
        $fixture.store.comments[10L].Clear()
        { Sync-ScheduledRoleHealth $healthContext $fixture.api } | Should -Throw
        $fixture.store.comments[10L].Add($comment)
        (Sync-ScheduledRoleHealth $healthContext $fixture.api).action | Should -Be reconciled
        $healthContext.now = $healthContext.now.AddMinutes(1)
        $changed = $fixture.context.Clone(); $changed.now = $healthContext.now
        $null = Invoke-TriageTransaction $changed triage-record-scan @{
            scan_token = $changed.scan_token; successful = $false; backlog_count = 1
            oldest_pending_at = $null; blocked_conditions = @('read-failed'); run_issues = @(); problem_issues = @()
        }
        $fixture.store.lose_update = $true
        { Sync-ScheduledRoleHealth $healthContext $fixture.api } | Should -Throw
        $record = Read-ScheduledRecord $comment.body health
        $record.backlog_count = 9
        $comment.body = "[Copilot speaking]`n$(Write-ScheduledRecord $record health)"
        { Sync-ScheduledRoleHealth $healthContext $fixture.api } | Should -Throw
    }

    It 'rejects foreign coverage identity and never truncates discussion to fit a health update' {
        $fixture.store.issues[10L].body = Write-ScheduledRecord -Kind coverage -Record @{
            schema_version = 1; repository = 'owner/another'; repository_id = 124
        }
        { Sync-ScheduledRoleHealth $healthContext $fixture.api } | Should -Throw
        $fixture.store.issues[10L].body = Write-ScheduledRecord -Kind coverage -Record @{
            schema_version = 1; repository = 'owner/repository'; repository_id = 123
        }
        $null = Sync-ScheduledRoleHealth $healthContext $fixture.api
        $fixture.store.comments[10L][0].body += ('x' * 60000)
        $null = Invoke-TriageTransaction $fixture.context triage-block @{ reason = 'Needs further evidence' }
        $before = $fixture.store.writes.Count
        { Sync-ScheduledRoleHealth $healthContext $fixture.api } | Should -Throw
        $fixture.store.writes.Count | Should -Be $before
    }

    It 'projects empty triage ownership and retained repair work without borrowing the other scan' {
        $state = Invoke-TriageTransaction $fixture.context read
        $state.triage.active_analysis_id = $null
        (Get-ScheduledRoleHealthRecord $state triage).active | Should -BeNullOrEmpty
        $state.attempts['retained'] = @{
            attempt_id = 'retained'; session_id = 'repair'; pr_number = 22; phase = 'working'; reason = 'Review'
        }
        (Get-ScheduledRoleHealthRecord $state repair).active[0].attempt_id | Should -Be retained
        $state.profile = @{ login = $state.login }; $state.coordinator = $state.triage.scan
        $data = @{ role = 'repair'; scan_token = $fixture.context.scan_token }
        { Invoke-ScheduledHealthStateChange $state $fixture.context.policy health-authorize $data $fixture.context.now } |
            Should -Throw
        $fixture.context.policy.local.enrolled_machine_id = $state.executor_id
        Invoke-ScheduledHealthStateChange $state $fixture.context.policy health-authorize $data $fixture.context.now
        { Invoke-ScheduledHealthStateChange $state $fixture.context.policy unknown $data $fixture.context.now } | Should -Throw
    }

    It 'requires immutable health intent and matching observed and confirmed receipts' {
        $state = Invoke-TriageTransaction $fixture.context read
        $data = @{ role = 'triage'; scan_token = $fixture.context.scan_token
            intent = @{ issue_number = 0; body = 'not-a-record'; comment_id = 1; record = @{ value = 1 } } }
        { Invoke-ScheduledHealthStateChange $state $fixture.context.policy health-prepare $data $fixture.context.now } |
            Should -Throw
        $data.intent.issue_number = 10
        $data.intent.record = Get-ScheduledRoleHealthRecord $state triage
        $data.intent.body = "[Copilot speaking]`n$(Write-ScheduledRecord $data.intent.record health)"
        $data.intent.preimage = $null
        Invoke-ScheduledHealthStateChange $state $fixture.context.policy health-prepare $data $fixture.context.now
        { Invoke-ScheduledHealthStateChange $state $fixture.context.policy health-prepare $data $fixture.context.now } |
            Should -Throw
        $data.comment_id = 2
        { Invoke-ScheduledHealthStateChange $state $fixture.context.policy health-observe $data $fixture.context.now } |
            Should -Throw
        $data.operation_id = 'foreign'; $data.record_digest = 'different'
        { Invoke-ScheduledHealthStateChange $state $fixture.context.policy health-confirm $data $fixture.context.now } |
            Should -Throw
    }

    It 'reports triage drift and failed scans independently while preserving the legacy repair observation' {
        $null = Sync-ScheduledRoleHealth $healthContext $fixture.api
        $comments = @($fixture.store.comments[10L])
        $scan = Get-ScheduledRoleScan $fixture.context.policy $fixture.context.triage_policy $comments triage
        $scan.outcome | Should -Be passed
        $fixture.context.triage_policy.cadence_cron = '0 * * * *'
        (Get-ScheduledRoleScan $fixture.context.policy $fixture.context.triage_policy $comments triage).blocked_conditions |
            Should -Contain triage-profile-drift
        $record = Read-ScheduledRecord $comments[0].body health
        $record.Remove('last_successful_scan')
        $comments[0].body = Write-ScheduledRecord $record health
        { Get-ScheduledRoleScan $fixture.context.policy $fixture.context.triage_policy $comments triage } | Should -Throw
        $fixture.context.policy.local.enrolled_machine_id = 'executor'
        $record.role = 'repair'
        $comments[0].body = Write-ScheduledRecord $record health
        (Get-ScheduledRoleScan $fixture.context.policy $fixture.context.triage_policy $comments repair).role | Should -Be repair
    }

    It 'preserves legacy repair ownership and rejects duplicate role comments' {
        $state = Invoke-TriageTransaction $fixture.context read
        $record = Get-ScheduledRoleHealthRecord $state repair
        $record.Remove('role')
        $fixture.store.comments[10L].Add(@{
            id = 800; body = Write-ScheduledRecord $record health; user = @{ login = 'worker' }
            issue_url = 'https://api.github.com/repos/owner/repository/issues/10'
        })
        $context = $healthContext.Clone(); $context.role = 'repair'
        & (Get-Module LocalHealth) {
            param($Context, $Transport)
            (Find-RoleHealthComment $Context 10 $Transport $null).id | Should -Be 800
        } $context $fixture.api
        $duplicate = Copy-TriageFixtureValue $fixture.store.comments[10L][0]; $duplicate.id = 801
        $fixture.store.comments[10L].Add($duplicate)
        InModuleScope LocalHealth -Parameters @{ Context = $context; Transport = $fixture.api } {
            { Find-RoleHealthComment $Context 10 $Transport $null } | Should -Throw
        }
        $fixture.context.policy.local.enrolled_machine_id = 'executor'
        { Get-ScheduledRoleScan $fixture.context.policy $fixture.context.triage_policy @($fixture.store.comments[10L]) repair } |
            Should -Throw
        (Get-ScheduledRoleScan $fixture.context.policy $fixture.context.triage_policy @() repair) | Should -BeNullOrEmpty
        $state.Remove('triage')
        { Get-ScheduledRoleHealthRecord $state triage } | Should -Throw
    }

    It 'requires the selected account and refuses a journal for another rolling surface' {
        $api = {
            param($Endpoint, $Method = 'GET', $Body, [switch] $Paginate)
            if ($Endpoint -ceq 'user') { return @{ login = 'foreign' } }
            & $fixture.api -Endpoint $Endpoint -Method $Method -Body $Body -Paginate:$Paginate
        }
        { Sync-ScheduledRoleHealth $healthContext $api } | Should -Throw
        $record = Get-ScheduledRoleHealthRecord (Invoke-TriageTransaction $fixture.context read) triage
        $null = Invoke-TriageTransaction $fixture.context health-prepare @{
            role = 'triage'; scan_token = $fixture.context.scan_token
            intent = @{ issue_number = 11; comment_id = $null; record = $record; preimage = $null
                body = "[Copilot speaking]`n$(Write-ScheduledRecord $record health)" }
        }
        { Sync-ScheduledRoleHealth $healthContext $fixture.api } | Should -Throw
        $operationId = (Invoke-TriageTransaction $fixture.context read).health_publications.triage.id
        $null = Invoke-TriageTransaction $fixture.context health-begin @{
            role = 'triage'; scan_token = $fixture.context.scan_token; operation_id = $operationId
        }
        { Invoke-TriageTransaction $fixture.context health-begin @{
            role = 'triage'; scan_token = $fixture.context.scan_token; operation_id = $operationId
        } } | Should -Throw
        $fixture.store.writes.Count | Should -Be 0
    }

    It 'prefixes an adopted unprefixed comment without deleting its human text' {
        $null = Sync-ScheduledRoleHealth $healthContext $fixture.api
        $comment = $fixture.store.comments[10L][0]
        $comment.body = $comment.body.Replace('[Copilot speaking]', 'Human introduction')
        $null = Invoke-TriageTransaction $fixture.context triage-block @{ reason = 'Required diagnosis remains open' }
        (Sync-ScheduledRoleHealth $healthContext $fixture.api).action | Should -Be published
        $comment.body.StartsWith('[Copilot speaking]') | Should -BeTrue
        $comment.body | Should -Match 'Human introduction'
    }

    It 'reconstructs a prefixed owned update after an interrupted send and external prose edits' {
        $null = Sync-ScheduledRoleHealth $healthContext $fixture.api
        $null = Invoke-TriageTransaction $fixture.context triage-block @{ reason = 'New evidence is needed' }
        $offline = {
            param($Endpoint, $Method = 'GET', $Body, [switch] $Paginate)
            if ($Method -ceq 'PATCH') { throw [IO.IOException]::new('Transport unavailable before the update.') }
            & $fixture.api -Endpoint $Endpoint -Method $Method -Body $Body -Paginate:$Paginate
        }
        { Sync-ScheduledRoleHealth $healthContext $offline } | Should -Throw
        $comment = $fixture.store.comments[10L][0]
        $comment.body = $comment.body.Replace('[Copilot speaking]', 'Human introduction')
        (Sync-ScheduledRoleHealth $healthContext $fixture.api).action | Should -Be published
        $comment.body.StartsWith('[Copilot speaking]') | Should -BeTrue
        $comment.body | Should -Match 'Human introduction'
    }
}
