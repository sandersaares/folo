#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
# Keeps bounded local payloads distinct from durable revision/native/budget identity history.
# Retirement requires current committed proof and native quiescence; missing proof cannot restart work.
BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'fixtures\TriageFixture.psm1')
    Import-Module (Join-Path $PSScriptRoot 'fixtures\TriageScenarioFixture.psm1')
    Import-Module (Join-Path $PSScriptRoot 'LocalTriagePublication.psm1')
    Import-Module (Join-Path $PSScriptRoot 'LocalTriageCompletion.psm1')
    Import-Module (Join-Path $PSScriptRoot 'LocalTriageInbox.psm1')
    Import-Module (Join-Path $PSScriptRoot 'LocalTriageCache.psm1')
    Import-Module (Join-Path $PSScriptRoot 'LocalTriageState.psm1')
    Import-Module (Join-Path $PSScriptRoot 'ScheduledContracts.psm1')
}

Describe 'Quiescent analysis compaction' {
    BeforeEach {
        $script:fixture = Initialize-TriageFixture -Root (Join-Path $TestDrive ([guid]::NewGuid().ToString('N')))
        $script:data = @{ scan_token = $fixture.context.scan_token; native_idle_verified = $true }
        $script:owner = @{ analysis_id = $fixture.context.analysis_id; session_id = $fixture.context.session_id
            claim_token = $fixture.context.claim_token; dispatch_token = $fixture.context.dispatch_token }
        $script:snapshotId = Save-ScheduledTriageSnapshot $fixture.context $fixture.snapshot $owner
    }

    It 'does not compact active or blocked work or discard its cached working view' {
        { Complete-ScheduledTriageRetirement $fixture.context $data $fixture.api } | Should -Throw
        $null = Invoke-TriageTransaction $fixture.context triage-block @{ reason = 'Unresolved diagnosis' }
        { Complete-ScheduledTriageRetirement $fixture.context $data $fixture.api } | Should -Throw
        Test-Path (Get-ScheduledTriageSnapshotPath $fixture.context.state_root $snapshotId) | Should -BeTrue
    }

    It 'retains exact identities, completion references and budgets without full retired payloads' {
        $null = Invoke-TriageTransaction $fixture.context triage-complete-dispatch @{ reason = 'Continue analysis' }
        $state = Invoke-TriageTransaction $fixture.context triage-reserve-continuation @{
            scan_token = $fixture.context.scan_token; native_idle_verified = $true; evidence_key = 'continued'
        }
        $fixture.context.dispatch_token = $state.triage.analyses[$fixture.context.analysis_id].dispatch.token
        $null = Invoke-TriageTransaction $fixture.context triage-begin-dispatch @{ scan_token = $fixture.context.scan_token }
        $null = Invoke-TriageTransaction $fixture.context triage-accept-dispatch
        $null = Publish-TriageFixtureProblem $fixture download
        $null = Complete-ScheduledTriageAnalysis $fixture.context $fixture.api
        { Complete-ScheduledTriageRetirement $fixture.context $data $fixture.api } | Should -Throw
        $null = Invoke-TriageTransaction $fixture.context triage-complete-dispatch @{ reason = 'Complete' }
        $before = (Invoke-TriageTransaction $fixture.context read).triage.analyses[$fixture.context.analysis_id]
        $denied = $data.Clone(); $denied.native_idle_verified = $false
        { Complete-ScheduledTriageRetirement $fixture.context $denied $fixture.api } | Should -Throw
        $retired = (Complete-ScheduledTriageRetirement $fixture.context $data $fixture.api).triage.analyses[$fixture.context.analysis_id]
        foreach ($field in @('id', 'revision', 'session_id', 'claim_token', 'started_at', 'dispatch', 'continuations', 'completion')) {
            (Get-ScheduledDigest $retired[$field]) | Should -BeExactly (Get-ScheduledDigest $before[$field])
        }
        foreach ($field in @('checkpoint', 'comparison', 'operations', 'publication', 'reads', 'read_progress', 'working_snapshot_id')) {
            $retired.ContainsKey($field) | Should -BeFalse
        }
        $retired.continuations.Count | Should -Be 1
        $retired.phase | Should -Be retired
        Test-Path (Get-ScheduledTriageSnapshotPath $fixture.context.state_root $snapshotId) | Should -BeFalse
        $state = Invoke-TriageTransaction $fixture.context read
        (Get-ScheduledTriageInbox $fixture.context.policy $state $fixture.api).backlog_count | Should -Be 0
        (Get-ScheduledTriageRecovery $fixture.context.policy $state $fixture.api).active | Should -BeNullOrEmpty
        $corrupt = Copy-TriageFixtureValue $state.triage
        $corrupt.analyses[$fixture.context.analysis_id].checkpoint = @{}
        { Assert-ScheduledTriageState $corrupt } | Should -Throw
        $null = Invoke-TriageTransaction $fixture.context triage-release-scan @{ scan_token = $fixture.context.scan_token }
        $state = Invoke-TriageTransaction $fixture.context triage-acquire-scan @{ session_id = 'next-poll' }
        { Invoke-TriageTransaction $fixture.context triage-claim @{
            scan_token = $state.triage.scan.token; session_id = 'next-poll'; native_verified = $true; revision = $retired.revision
        } } | Should -Throw
        $fixture.store.comments[20L].RemoveAll([Predicate[object]]{ param($comment) $comment.user.login -ceq 'worker' }) | Out-Null
        { Get-ScheduledTriageInbox $fixture.context.policy (Invoke-TriageTransaction $fixture.context read) $fixture.api } | Should -Throw
        (Invoke-TriageTransaction $fixture.context read).triage.analyses.Count | Should -Be 1
    }

    It 'keeps completed local data when committed remote proof is missing before retirement' {
        $null = Publish-TriageFixtureProblem $fixture download
        $null = Complete-ScheduledTriageAnalysis $fixture.context $fixture.api
        $null = Invoke-TriageTransaction $fixture.context triage-complete-dispatch @{ reason = 'Complete' }
        $fixture.store.comments[20L].RemoveAll([Predicate[object]]{ param($comment) $comment.user.login -ceq 'worker' }) | Out-Null
        { Complete-ScheduledTriageRetirement $fixture.context $data $fixture.api } | Should -Throw
        $state = Invoke-TriageTransaction $fixture.context read
        $state.triage.active_analysis_id | Should -Be $fixture.context.analysis_id
        $state.triage.analyses[$fixture.context.analysis_id].ContainsKey('checkpoint') | Should -BeTrue
        Test-Path (Get-ScheduledTriageSnapshotPath $fixture.context.state_root $snapshotId) | Should -BeTrue
    }
}
