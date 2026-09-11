#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
# Exercises native sender/recipient fencing and externally refreshed continuation eligibility.
# All time and API availability are controlled by the fixture; no sleep or age-based takeover.
BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'fixtures\TriageFixture.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'LocalTriagePublication.psm1') -Force
}

Describe 'Retained triage continuation' {
    BeforeEach {
        $script:fixture = Initialize-TriageFixture -Root (Join-Path $TestDrive ([guid]::NewGuid().ToString('N')))
        $null = Invoke-TriageTransaction $fixture.context triage-block @{ reason = 'Required collection is unavailable' }
        $null = Invoke-TriageTransaction $fixture.context triage-complete-dispatch @{ reason = 'Waiting for collection recovery' }
        $null = Invoke-TriageTransaction $fixture.context triage-release-scan @{ scan_token = $fixture.context.scan_token }
        $state = Invoke-TriageTransaction $fixture.context triage-acquire-scan @{ session_id = 'new-poll' }
        $script:senderToken = $state.triage.scan.token
    }

    It 'accepts the recipient dispatch while the sender still owns the short scan' {
        $state = Invoke-TriageTransaction $fixture.context read
        $recovery = Get-ScheduledTriageRecovery $fixture.context.policy $state $fixture.api
        $state = Invoke-TriageTransaction $fixture.context triage-reserve-continuation @{
            scan_token = $senderToken; native_idle_verified = $true; evidence_key = $recovery.evidence_key
        }
        $receiver = $fixture.context.Clone()
        $receiver.dispatch_token = $state.triage.analyses[$receiver.analysis_id].dispatch.token
        $null = Invoke-TriageTransaction $fixture.context triage-begin-dispatch @{ scan_token = $senderToken }
        { Invoke-TriageTransaction $receiver triage-acquire-scan @{ session_id = $receiver.session_id } } | Should -Throw
        $state = Invoke-TriageTransaction $receiver triage-accept-dispatch
        $state.triage.scan.session_id | Should -Be new-poll
        $state.triage.scan.token | Should -Be $senderToken
        $state.triage.analyses[$receiver.analysis_id].dispatch.status | Should -Be accepted
        $state.triage.analyses[$receiver.analysis_id].session_id | Should -Be $fixture.context.session_id
    }

    It 'does not repeat an unchanged blocked key but resumes when exact-attempt collection recovers' {
        $fixture.store.fail_job_read = $true
        $state = Invoke-TriageTransaction $fixture.context read
        $blocked = Get-ScheduledTriageRecovery $fixture.context.policy $state $fixture.api
        $blocked.complete_input | Should -BeFalse
        $state = Invoke-TriageTransaction $fixture.context triage-reserve-continuation @{
            scan_token = $senderToken; native_idle_verified = $true; evidence_key = $blocked.evidence_key
        }
        $receiver = $fixture.context.Clone()
        $receiver.dispatch_token = $state.triage.analyses[$receiver.analysis_id].dispatch.token
        $null = Invoke-TriageTransaction $fixture.context triage-begin-dispatch @{ scan_token = $senderToken }
        $null = Invoke-TriageTransaction $receiver triage-accept-dispatch
        $null = Invoke-TriageTransaction $receiver triage-complete-dispatch @{ reason = 'Collection remains unavailable' }
        $state = Invoke-TriageTransaction $receiver read
        $unchanged = Get-ScheduledTriageRecovery $receiver.policy $state $fixture.api
        $unchanged.evidence_key | Should -BeExactly $blocked.evidence_key
        { Invoke-TriageTransaction $receiver triage-reserve-continuation @{
            scan_token = $senderToken; native_idle_verified = $true; evidence_key = $unchanged.evidence_key
        } } | Should -Throw
        $fixture.store.fail_job_read = $false
        $recovered = Get-ScheduledTriageRecovery $receiver.policy $state $fixture.api
        $recovered.complete_input | Should -BeTrue
        $recovered.evidence_key | Should -Not -Be $blocked.evidence_key
        $state = Invoke-TriageTransaction $receiver triage-reserve-continuation @{
            scan_token = $senderToken; native_idle_verified = $true; evidence_key = $recovered.evidence_key
        }
        $state.triage.analyses.Count | Should -Be 1
        $state.triage.analyses[$receiver.analysis_id].continuations.Count | Should -Be 2
    }

    It 'does not transfer a retained analysis when a scan expires' {
        $later = $fixture.context.Clone()
        $later.now = $later.now.AddDays(1)
        $state = Invoke-TriageTransaction $later triage-acquire-scan @{ session_id = 'later-poll' }
        { Invoke-TriageTransaction $later triage-claim @{
            scan_token = $state.triage.scan.token; session_id = 'later-poll'
            native_verified = $true; revision = $fixture.proposal.revision
        } } | Should -Throw
        (Invoke-TriageTransaction $later read).triage.active_analysis_id | Should -Be $fixture.context.analysis_id
    }
}
