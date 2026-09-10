#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
# Protects scan/worker/checkpoint cache pins across interleaved native owners and interrupted IO.
# Time is injected; cleanup never derives abandonment from age or deletes an active analysis view.
BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'fixtures\TriageFixture.psm1')
    Import-Module (Join-Path $PSScriptRoot 'LocalTriagePublication.psm1')
    Import-Module (Join-Path $PSScriptRoot 'LocalTriageCache.psm1')
    Import-Module (Join-Path $PSScriptRoot 'ScheduledContracts.psm1')

    function Get-WorkerCacheOwner($Context) {
        @{ analysis_id = $Context.analysis_id; session_id = $Context.session_id
            claim_token = $Context.claim_token; dispatch_token = $Context.dispatch_token }
    }
    function Get-CacheFixtureSnapshot($Fixture, [string] $Label) {
        $snapshot = Copy-TriageFixtureValue $Fixture.snapshot
        $snapshot.observation_label = $Label
        return $snapshot
    }
}

Describe 'Durable cache ownership' {
    BeforeEach {
        $script:fixture = Initialize-TriageFixture -Root (Join-Path $TestDrive ([guid]::NewGuid().ToString('N')))
        $script:owner = Get-WorkerCacheOwner $fixture.context
    }

    It 'transfers a scan pin to the accepted analysis before releasing the scan' {
        $unclaimed = Initialize-TriageFixture -Root (Join-Path $TestDrive 'unclaimed') -Unclaimed
        $scanOwner = @{ analysis_id = $null; session_id = 'session'; scan_token = $unclaimed.context.scan_token }
        $id = Save-ScheduledTriageSnapshot $unclaimed.context $unclaimed.snapshot $scanOwner
        $pending = Write-ScheduledTriageSnapshotFile $unclaimed.context.state_root $unclaimed.snapshot scan $unclaimed.context.scan_token
        $null = Invoke-TriageTransaction $unclaimed.context triage-clean-cache $scanOwner
        Test-Path -LiteralPath $pending.temporary_path | Should -BeTrue
        $state = Invoke-TriageTransaction $unclaimed.context triage-claim @{
            scan_token = $unclaimed.context.scan_token; session_id = 'session'
            native_verified = $true; revision = $unclaimed.revision
        }
        $analysis = $state.triage.analyses[$state.triage.active_analysis_id]
        $analysis.working_snapshot_id | Should -BeExactly $id
        $null = Invoke-TriageTransaction $unclaimed.context triage-release-scan @{ scan_token = $unclaimed.context.scan_token }
        Test-Path -LiteralPath $pending.temporary_path | Should -BeFalse
        Test-Path (Get-ScheduledTriageSnapshotPath $unclaimed.context.state_root $id) | Should -BeTrue
    }

    It 'collects an orphaned analysis temporary file when no analysis owns the role' {
        $unclaimed = Initialize-TriageFixture -Root (Join-Path $TestDrive 'empty-role') -Unclaimed
        $pending = Write-ScheduledTriageSnapshotFile $unclaimed.context.state_root $unclaimed.snapshot analysis ([guid]::NewGuid().ToString())
        $null = Invoke-TriageTransaction $unclaimed.context triage-clean-cache @{
            analysis_id = $null; session_id = 'session'; scan_token = $unclaimed.context.scan_token
        }
        Test-Path -LiteralPath $pending.temporary_path | Should -BeFalse
    }

    It 'preserves a worker uncheckpointed view while another poll owns and releases a different snapshot' {
        $workerId = Save-ScheduledTriageSnapshot $fixture.context (Get-CacheFixtureSnapshot $fixture worker) $owner
        $null = Invoke-TriageTransaction $fixture.context triage-release-scan @{ scan_token = $fixture.context.scan_token }
        $state = Invoke-TriageTransaction $fixture.context triage-acquire-scan @{ session_id = 'poll' }
        $poll = @{ analysis_id = $null; session_id = 'poll'; scan_token = $state.triage.scan.token }
        $pollId = Save-ScheduledTriageSnapshot $fixture.context (Get-CacheFixtureSnapshot $fixture poll) $poll
        Test-Path (Get-ScheduledTriageSnapshotPath $fixture.context.state_root $workerId) | Should -BeTrue
        $null = Invoke-TriageTransaction $fixture.context triage-release-scan @{ scan_token = $poll.scan_token }
        Test-Path (Get-ScheduledTriageSnapshotPath $fixture.context.state_root $pollId) | Should -BeFalse
        (Read-ScheduledTriageSnapshot $fixture.context.state_root $workerId).observation_label | Should -Be worker
    }

    It 'keeps the checkpoint and current working view while replacing only the previous working view' {
        $first = Save-ScheduledTriageSnapshot $fixture.context (Get-CacheFixtureSnapshot $fixture first) $owner
        $state = Invoke-TriageTransaction $fixture.context read
        $checkpoint = Copy-TriageFixtureValue $state.triage.analyses[$fixture.context.analysis_id].checkpoint
        $checkpoint.analysis.checkpoint = 2; $checkpoint.snapshot_id = $first
        $null = Invoke-TriageTransaction $fixture.context triage-checkpoint @{ checkpoint = $checkpoint }
        $second = Save-ScheduledTriageSnapshot $fixture.context (Get-CacheFixtureSnapshot $fixture second) $owner
        $third = Save-ScheduledTriageSnapshot $fixture.context (Get-CacheFixtureSnapshot $fixture third) $owner
        Test-Path (Get-ScheduledTriageSnapshotPath $fixture.context.state_root $first) | Should -BeTrue
        Test-Path (Get-ScheduledTriageSnapshotPath $fixture.context.state_root $second) | Should -BeFalse
        Test-Path (Get-ScheduledTriageSnapshotPath $fixture.context.state_root $third) | Should -BeTrue
        $restored = Invoke-TriageTransaction $fixture.context read
        $restored.triage.analyses[$fixture.context.analysis_id].checkpoint.snapshot_id | Should -BeExactly $first
        $restored.triage.analyses[$fixture.context.analysis_id].working_snapshot_id | Should -BeExactly $third
    }

    It 'does not discard an accepted analysis view when an old scan expires' {
        $id = Save-ScheduledTriageSnapshot $fixture.context $fixture.snapshot $owner
        $later = $fixture.context.Clone(); $later.now = $later.now.AddDays(1)
        $state = Invoke-TriageTransaction $later triage-acquire-scan @{ session_id = 'later-poll' }
        $poll = @{ analysis_id = $null; session_id = 'later-poll'; scan_token = $state.triage.scan.token }
        $null = Save-ScheduledTriageSnapshot $later (Get-CacheFixtureSnapshot $fixture later) $poll
        Test-Path (Get-ScheduledTriageSnapshotPath $fixture.context.state_root $id) | Should -BeTrue
        (Invoke-TriageTransaction $later read).triage.active_analysis_id | Should -Be $fixture.context.analysis_id
    }

    It 'preserves a pending current writer but rejects its installation after dispatch replacement' {
        $old = Write-ScheduledTriageSnapshotFile $fixture.context.state_root `
            (Get-CacheFixtureSnapshot $fixture old) analysis $fixture.context.dispatch_token
        $null = Invoke-TriageTransaction $fixture.context triage-clean-cache $owner
        Test-Path -LiteralPath $old.temporary_path | Should -BeTrue
        $null = Invoke-TriageTransaction $fixture.context triage-complete-dispatch @{ reason = 'The earlier turn is quiescent' }
        $state = Invoke-TriageTransaction $fixture.context triage-reserve-continuation @{
            scan_token = $fixture.context.scan_token; native_idle_verified = $true; evidence_key = 'new-input'
        }
        $receiver = $fixture.context.Clone()
        $receiver.dispatch_token = $state.triage.analyses[$receiver.analysis_id].dispatch.token
        $null = Invoke-TriageTransaction $fixture.context triage-begin-dispatch @{ scan_token = $fixture.context.scan_token }
        $null = Invoke-TriageTransaction $receiver triage-accept-dispatch
        $current = Save-ScheduledTriageSnapshot $receiver (Get-CacheFixtureSnapshot $fixture current) (Get-WorkerCacheOwner $receiver)
        Test-Path -LiteralPath $old.temporary_path | Should -BeFalse
        { Invoke-TriageTransaction $fixture.context triage-pin-snapshot ($owner + @{
            snapshot_id = $old.id; temporary_path = $old.temporary_path; owner_kind = 'analysis'; owner_token = $old.owner_token
        }) } | Should -Throw
        (Read-ScheduledTriageSnapshot $receiver.state_root $current).observation_label | Should -Be current
    }

    It 'leaves a newer pinned view intact when cleanup is interrupted and resumes only unowned deletions' {
        $id = Save-ScheduledTriageSnapshot $fixture.context $fixture.snapshot $owner
        $first = Get-ScheduledTriageSnapshotPath $fixture.context.state_root ('1' * 64)
        $second = Get-ScheduledTriageSnapshotPath $fixture.context.state_root ('2' * 64)
        '{}' | Set-Content -LiteralPath $first
        '{}' | Set-Content -LiteralPath $second
        $script:fault = @{ pending = $true }
        Mock Get-ChildItem -ModuleName LocalTriageCache { @(Get-Item -LiteralPath $first,$second) }
        Mock Remove-Item -ModuleName LocalTriageCache {
            if ($LiteralPath -ceq $second -and $fault.pending) { throw [IO.IOException]::new('Interrupted cleanup.') }
            Microsoft.PowerShell.Management\Remove-Item -LiteralPath $LiteralPath
        }
        { Invoke-TriageTransaction $fixture.context triage-clean-cache $owner } | Should -Throw
        Test-Path -LiteralPath $first | Should -BeFalse
        Test-Path (Get-ScheduledTriageSnapshotPath $fixture.context.state_root $id) | Should -BeTrue
        $fault.pending = $false
        Mock Get-ChildItem -ModuleName LocalTriageCache { @(Get-Item -LiteralPath $second) }
        $null = Invoke-TriageTransaction $fixture.context triage-clean-cache $owner
        Test-Path -LiteralPath $second | Should -BeFalse
        (Invoke-TriageTransaction $fixture.context read).triage.analyses[$fixture.context.analysis_id].working_snapshot_id |
            Should -BeExactly $id
    }

    It 'rejects corrupted pin metadata before touching cached payloads' {
        $id = Save-ScheduledTriageSnapshot $fixture.context $fixture.snapshot $owner
        $path = Join-Path $fixture.context.state_root state.json
        $state = Get-Content -LiteralPath $path -Raw | ConvertFrom-Json -AsHashtable
        $state.triage.analyses[$fixture.context.analysis_id].working_snapshot_id = $null
        $state | ConvertTo-Json -Depth 100 | Set-Content -LiteralPath $path
        { Invoke-TriageTransaction $fixture.context triage-clean-cache $owner } | Should -Throw
        Test-Path (Get-ScheduledTriageSnapshotPath $fixture.context.state_root $id) | Should -BeTrue
    }

    It 'preserves the prior pin when payload installation is interrupted before its state commit' {
        $prior = Save-ScheduledTriageSnapshot $fixture.context $fixture.snapshot $owner
        $next = Get-CacheFixtureSnapshot $fixture next
        $nextId = Get-ScheduledDigest $next
        Mock Invoke-TriageSnapshotMove -ModuleName LocalTriageCache {
            [IO.File]::Move($Source, $Destination, $true)
            throw [IO.IOException]::new('Interrupted before state commit.')
        }
        { Save-ScheduledTriageSnapshot $fixture.context $next $owner } | Should -Throw
        $state = Invoke-TriageTransaction $fixture.context read
        $state.triage.analyses[$fixture.context.analysis_id].working_snapshot_id | Should -BeExactly $prior
        Test-Path (Get-ScheduledTriageSnapshotPath $fixture.context.state_root $prior) | Should -BeTrue
        Test-Path (Get-ScheduledTriageSnapshotPath $fixture.context.state_root $nextId) | Should -BeTrue
        $note = Join-Path $fixture.context.state_root 'triage-cache\operator-note.txt'
        'not a cache payload' | Set-Content -LiteralPath $note
        $null = Invoke-TriageTransaction $fixture.context triage-clean-cache $owner
        Test-Path (Get-ScheduledTriageSnapshotPath $fixture.context.state_root $nextId) | Should -BeFalse
        Test-Path -LiteralPath $note | Should -BeTrue
    }
}
