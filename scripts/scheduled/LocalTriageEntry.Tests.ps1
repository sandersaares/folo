#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
# Exercises the real request JSON/cache boundary with injected time and API observations.
# It cannot contact GitHub, change source or enroll a missing executor.
BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'fixtures\TriageFixture.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'LocalTriage.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'LocalTriageView.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'LocalTriageInbox.psm1')
    Import-Module (Join-Path $PSScriptRoot 'LocalTriagePublication.psm1')

    function Invoke-EntryFixtureRequest($Action, $Data, [switch] $ObserveOnly) {
        if ($Action -ceq 'scan' -and $Data.Count -eq 0 -and -not $ObserveOnly) { $Data = $identity.Clone() }
        $path = Join-Path $TestDrive 'request.json'
        @{ action = $Action; executor_id = 'executor'; data = $Data } |
            ConvertTo-Json -Depth 100 | Set-Content -LiteralPath $path
        return Invoke-ScheduledTriageRequest -RequestPath $path -Now $fixture.context.now |
            ConvertFrom-Json -AsHashtable
    }
}

Describe 'Local triage JSON entry point' {
    BeforeEach {
        $script:fixture = Initialize-TriageFixture -Root (Join-Path $TestDrive ([guid]::NewGuid().ToString('N')))
        Mock Get-ScheduledPolicy -ModuleName LocalTriage { $fixture.context.policy }
        Mock Get-ScheduledTriagePolicy -ModuleName LocalTriage { $fixture.context.triage_policy }
        Mock Get-ScheduledStateRoot -ModuleName LocalTriage { $fixture.context.state_root }
        Mock Invoke-ScheduledTriageRead -ModuleName LocalTriage { @{ login = 'worker'; id = 10 } }
        Mock Get-ScheduledTriageInbox -ModuleName LocalTriage { $fixture.snapshot }
        $script:identity = @{
            analysis_id = $fixture.context.analysis_id; session_id = $fixture.context.session_id
            claim_token = $fixture.context.claim_token; dispatch_token = $fixture.context.dispatch_token
        }
    }

    It 'scans into an immutable cache and streams exact evidence through JSON pages' {
        $scan = Invoke-EntryFixtureRequest scan @{}
        $scan.successful_scan | Should -BeTrue
        $scan.backlog_count | Should -Be 1
        $data = $identity.Clone(); $data.snapshot_id = $scan.snapshot_id; $data.offset = 0
        $text = ''
        do {
            $page = Invoke-EntryFixtureRequest evidence $data
            $page.end_offset | Should -BeGreaterThan $data.offset
            $text += $page.content
            $data.offset = $page.next_offset
        } while ($null -ne $data.offset)
        $decoded = $text | ConvertFrom-Json -AsHashtable
        $decoded.evidence.run_id | Should -Be 789
        $decoded.basis.api_evidence.run_attempt | Should -Be 1
        $unowned = $identity.Clone(); $unowned.snapshot_id = 'f' * 64
        { Invoke-EntryFixtureRequest evidence $unowned } | Should -Throw
    }

    It 'records an empty complete index and validates a changed analysis checkpoint' {
        $scan = Invoke-EntryFixtureRequest scan @{}
        $data = $identity.Clone(); $data.snapshot_id = $scan.snapshot_id; $data.offset = 0
        $page = Invoke-EntryFixtureRequest index $data
        $page.entries.Count | Should -Be 0
        $page.next_offset | Should -BeNullOrEmpty
        $data.analysis = Copy-TriageFixtureValue $fixture.proposal
        $data.analysis.checkpoint = 2
        (Invoke-EntryFixtureRequest checkpoint $data).checkpoint.analysis.checkpoint | Should -Be 2
    }

    It 'routes only role state and refuses source/repair or unregistered worker actions' {
        $result = Invoke-EntryFixtureRequest state @{ action = 'triage-read'; fields = @{} }
        $result.triage.active_analysis_id | Should -Be $fixture.context.analysis_id
        { Invoke-EntryFixtureRequest state @{ action = 'reserve-attempt'; fields = @{} } } | Should -Throw
        { Invoke-EntryFixtureRequest evidence @{} } | Should -Throw
        { Invoke-EntryFixtureRequest unknown $identity } | Should -Throw
    }

    It 'exposes health and recovery without impersonating the registered analysis owner' {
        Mock Sync-ScheduledRoleHealth -ModuleName LocalTriage { @{ action = 'published'; comment_id = 7 } }
        Mock Get-ScheduledTriageRecovery -ModuleName LocalTriage { @{ active = $null; evidence_key = $null } }
        (Invoke-EntryFixtureRequest health @{ scan_token = $fixture.context.scan_token }).comment_id | Should -Be 7
        (Invoke-EntryFixtureRequest recovery @{}).active | Should -BeNullOrEmpty
        Should -Invoke Sync-ScheduledRoleHealth -ModuleName LocalTriage -Exactly -Times 1
    }

    It 'routes prepared problem and completion operations using the persisted checkpoint snapshot' {
        $scan = Invoke-EntryFixtureRequest scan @{}
        $data = $identity.Clone(); $data.snapshot_id = $scan.snapshot_id
        $data.analysis = Copy-TriageFixtureValue $fixture.proposal; $data.analysis.checkpoint = 2
        $null = Invoke-EntryFixtureRequest checkpoint $data
        Mock Invoke-ScheduledTriageProblemPreparation -ModuleName LocalTriage { @{ action = 'prepared' } }
        Mock Publish-ScheduledTriageProblem -ModuleName LocalTriage { @{ action = 'published' } }
        Mock Complete-ScheduledTriageAnalysis -ModuleName LocalTriage { @{ action = 'recorded' } }
        $data.problem_key = 'download'
        (Invoke-EntryFixtureRequest prepare-problem $data).action | Should -Be prepared
        (Invoke-EntryFixtureRequest publish-problem $data).action | Should -Be published
        (Invoke-EntryFixtureRequest finish $data).action | Should -Be recorded
    }

    It 'does not initialize absent state or accept a changed account' {
        Mock Get-ScheduledStateRoot -ModuleName LocalTriage { Join-Path $TestDrive 'not-enrolled' }
        { Invoke-EntryFixtureRequest scan @{} } | Should -Throw
        $scan = Invoke-EntryFixtureRequest scan @{} -ObserveOnly
        $scan.registered | Should -BeFalse
        $scan.snapshot_id | Should -BeNullOrEmpty
        Test-Path (Join-Path $TestDrive 'not-enrolled') | Should -BeFalse
        { Invoke-EntryFixtureRequest state @{ action = 'triage-read'; fields = @{} } } | Should -Throw
        Mock Invoke-ScheduledTriageRead -ModuleName LocalTriage { @{ login = 'different-user'; id = 11 } }
        { Invoke-EntryFixtureRequest scan @{} } | Should -Throw
    }

    It 'rejects mutated cached input and malformed request envelopes' {
        $scan = Invoke-EntryFixtureRequest scan @{}
        $path = Join-Path $fixture.context.state_root "triage-cache\$($scan.snapshot_id).json"
        '{}' | Set-Content -LiteralPath $path
        $data = $identity.Clone(); $data.snapshot_id = $scan.snapshot_id
        { Invoke-EntryFixtureRequest evidence $data } | Should -Throw
        $requestPath = Join-Path $TestDrive 'missing-fields.json'
        '{}' | Set-Content -LiteralPath $requestPath
        { Invoke-ScheduledTriageRequest -RequestPath $requestPath -Now $fixture.context.now } | Should -Throw
    }

    It 'accepts a retained continuation then obtains a usable snapshot without taking the sender scan' {
        $null = Invoke-EntryFixtureRequest state @{ action = 'triage-complete-dispatch'
            fields = $identity + @{ reason = 'Resume retained analysis' } }
        $null = Invoke-EntryFixtureRequest state @{ action = 'triage-release-scan'
            fields = @{ scan_token = $fixture.context.scan_token } }
        $state = Invoke-EntryFixtureRequest state @{ action = 'triage-acquire-scan'
            fields = @{ session_id = 'sender' } }
        $senderScan = $state.triage.scan.token
        $state = Invoke-EntryFixtureRequest state @{ action = 'triage-reserve-continuation'
            fields = $identity + @{ scan_token = $senderScan; native_idle_verified = $true; evidence_key = 'recovered-input' } }
        $identity.dispatch_token = $state.triage.analyses[$identity.analysis_id].dispatch.token
        $null = Invoke-EntryFixtureRequest state @{ action = 'triage-begin-dispatch'
            fields = @{ analysis_id = $identity.analysis_id; scan_token = $senderScan } }
        $null = Invoke-EntryFixtureRequest state @{ action = 'triage-accept-dispatch'; fields = $identity }
        $scan = Invoke-EntryFixtureRequest scan @{}
        $scan.snapshot_id | Should -Not -BeNullOrEmpty
        $data = $identity.Clone(); $data.snapshot_id = $scan.snapshot_id
        $page = Invoke-EntryFixtureRequest evidence $data
        $page.content.Length | Should -BeGreaterThan 0
        $state = Invoke-EntryFixtureRequest state @{ action = 'triage-read'; fields = @{} }
        $state.triage.scan.token | Should -Be $senderScan
        $state.triage.scan.session_id | Should -Be sender
        $state.triage.active_analysis_id | Should -Be $fixture.context.analysis_id
        $state.triage.analyses.Count | Should -Be 1
    }

    It 'routes retained publication before requiring a fresh scan when collection is blocked' {
        $scan = Invoke-EntryFixtureRequest scan @{}
        $data = $identity.Clone(); $data.snapshot_id = $scan.snapshot_id
        $data.analysis = Copy-TriageFixtureValue $fixture.proposal; $data.analysis.checkpoint = 2
        $null = Invoke-EntryFixtureRequest checkpoint $data
        Mock Get-ScheduledTriageInbox -ModuleName LocalTriage {
            throw [IO.IOException]::new('Publication is incomplete.')
        }
        Mock Invoke-ScheduledTriageProblemPreparation -ModuleName LocalTriage { @{ action = 'prepared' } }
        Mock Publish-ScheduledTriageProblem -ModuleName LocalTriage { @{ action = 'published' } }
        Mock Complete-ScheduledTriageAnalysis -ModuleName LocalTriage { @{ action = 'recorded' } }
        { Invoke-EntryFixtureRequest scan @{} } | Should -Throw
        $data.problem_key = 'download'
        (Invoke-EntryFixtureRequest prepare-problem $data).action | Should -Be prepared
        (Invoke-EntryFixtureRequest publish-problem $data).action | Should -Be published
        (Invoke-EntryFixtureRequest finish $data).action | Should -Be recorded
        Should -Invoke Get-ScheduledTriageInbox -ModuleName LocalTriage -Exactly -Times 2
    }

    It 'publishes through the entry adapter and records paginated full candidate reads before a refreshed checkpoint' {
        Mock Invoke-ScheduledGitHubApi -ModuleName LocalTriage {
            & $fixture.api -Endpoint $Endpoint -Method $Method -Body $Body -Paginate:$Paginate
        }
        $scan = Invoke-EntryFixtureRequest scan @{}
        $data = $identity.Clone(); $data.snapshot_id = $scan.snapshot_id
        $data.analysis = Copy-TriageFixtureValue $fixture.proposal; $data.analysis.checkpoint = 2
        $null = Invoke-EntryFixtureRequest checkpoint $data
        $data.problem_key = 'download'
        $prepared = Invoke-EntryFixtureRequest prepare-problem $data
        $prepared.action | Should -Be native-create-issue
        $created = & $fixture.api -Endpoint 'repos/owner/repository/issues' -Method POST -Body $prepared.payload
        $data.operation_key = $prepared.operation_key; $data.issue_number = $created.number
        (Invoke-EntryFixtureRequest native-issue-result $data).operations[$data.operation_key].target_id | Should -Be $created.number
        (Invoke-EntryFixtureRequest prepare-problem $data).action | Should -Be prepared
        (Invoke-EntryFixtureRequest publish-problem $data).action | Should -Be published
        (Invoke-EntryFixtureRequest recovery @{}).active.analysis_id | Should -Be $identity.analysis_id
        $state = Invoke-TriageTransaction $fixture.context read
        $fixture.snapshot = Get-ScheduledTriageInbox $fixture.context.policy $state $fixture.api
        $scan = Invoke-EntryFixtureRequest scan @{}
        $data.snapshot_id = $scan.snapshot_id; $data.offset = 0
        (Invoke-EntryFixtureRequest index $data).entries.Count | Should -Be 1
        $data.Remove('offset')
        do {
            $page = Invoke-EntryFixtureRequest problem $data
            $page.end_offset | Should -BeGreaterThan $page.offset
            $data.offset = $page.next_offset
        } while ($null -ne $data.offset)
        $entry = $fixture.snapshot.index.entries[0]
        $data.offset = 0
        (Invoke-EntryFixtureRequest problem $data).full_read_digest | Should -BeExactly $entry.record_digest
        $data.analysis.checkpoint = 3; $data.analysis.index_digest = $fixture.snapshot.index.digest
        $data.analysis.considered_issues = @($created.number)
        $data.analysis.problems[0].matching = @{
            kind = 'existing'; issue_number = $created.number; expected_generation = 1; expected_scope_revision = 1
            target_generation = 1; record_digest = $entry.record_digest; full_read_digest = $entry.record_digest
            relation = 'repeat'; reason = 'The confirmed publication describes the same diagnosed access failure.'
        }
        (Invoke-EntryFixtureRequest checkpoint $data).checkpoint.analysis.checkpoint | Should -Be 3
        (Invoke-EntryFixtureRequest prepare-problem $data).action | Should -Be prepared
        (Invoke-EntryFixtureRequest publish-problem $data).action | Should -Be published
        (Invoke-EntryFixtureRequest finish $data).analysis_status | Should -Be complete
        $null = Invoke-EntryFixtureRequest state @{
            action = 'triage-complete-dispatch'; fields = $identity + @{ reason = 'Completed' }
        }
        $retired = Invoke-EntryFixtureRequest state @{
            action = 'triage-retire'; fields = @{
                analysis_id = $identity.analysis_id; session_id = $identity.session_id
                scan_token = $fixture.context.scan_token; native_idle_verified = $true
            }
        }
        $retired.triage.analyses[$identity.analysis_id].phase | Should -Be retired
    }

    It 'does not use a snapshot lacking its exact claimed revision for evidence or checkpointing' {
        $fixture.snapshot.pending = @()
        $scan = Invoke-EntryFixtureRequest scan @{}
        $data = $identity.Clone(); $data.snapshot_id = $scan.snapshot_id; $data.analysis = $fixture.proposal
        { Invoke-EntryFixtureRequest evidence $data } | Should -Throw
        { Invoke-EntryFixtureRequest checkpoint $data } | Should -Throw
    }
}

Describe 'Bounded model-facing views' {
    It 'streams escaped Unicode and quotes without losing bytes or advancing on invalid offsets' {
        $value = @{ diagnostic = ('"\' * 10000) + [char]0x03BB }
        $offset = 0
        $text = ''
        do {
            $page = Get-ScheduledTriageJsonPage $value $offset
            $page.end_offset | Should -BeGreaterThan $offset
            $page.content.Length | Should -BeLessOrEqual 6000
            $text += $page.content
            $offset = $page.next_offset
        } while ($null -ne $offset)
        ($text | ConvertFrom-Json -AsHashtable).diagnostic | Should -BeExactly $value.diagnostic
        { Get-ScheduledTriageJsonPage $value -1 } | Should -Throw
        { Get-ScheduledTriageJsonPage $value $text.Length } | Should -Throw
    }
}
