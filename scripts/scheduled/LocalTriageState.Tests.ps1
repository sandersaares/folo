#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
# Pins role ownership, budget and state-integrity transitions with real atomic JSON persistence.
# Time, native quiescence and dispatch acceptance are explicitly supplied by each scenario.
BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'fixtures\TriageFixture.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'LocalTriagePublication.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'LocalState.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'LocalTriageState.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'ScheduledContracts.psm1')
}

Describe 'Durable triage ownership and budgets' {
    BeforeEach {
        $script:fixture = Initialize-TriageFixture -Root (Join-Path $TestDrive ([guid]::NewGuid().ToString('N')))
    }

    It 'rejects unowned worker transitions without changing persisted state' {
        $path = Join-Path $fixture.context.state_root state.json
        $before = Get-Content -LiteralPath $path -Raw
        foreach ($field in @('session_id', 'claim_token', 'dispatch_token', 'analysis_id')) {
            $wrong = $fixture.context.Clone(); $wrong[$field] = 'foreign'
            { Invoke-TriageTransaction $wrong triage-block @{ reason = 'blocked' } } | Should -Throw
        }
        (Get-Content -LiteralPath $path -Raw) | Should -BeExactly $before
    }

    It 'preserves repair state while registering a role profile and rejecting unsupported profile data' {
        $state = Invoke-TriageTransaction $fixture.context read
        $before = Get-Content -LiteralPath (Join-Path $fixture.context.state_root state.json) -Raw
        $null = Invoke-TriageTransaction $fixture.context triage-register-profile @{
            operator_approved = $true; profile = $state.triage.profile
        }
        (Get-Content -LiteralPath (Join-Path $fixture.context.state_root state.json) -Raw) | Should -BeExactly $before
        foreach ($field in @('executor_id', 'login')) {
            $foreign = Copy-TriageFixtureValue $state.triage.profile
            $foreign[$field] = 'foreign'
            { Invoke-TriageTransaction $fixture.context triage-register-profile @{
                operator_approved = $true; profile = $foreign
            } } | Should -Throw
        }
        { Invoke-TriageTransaction $fixture.context triage-register-profile @{
            operator_approved = $false; profile = $state.triage.profile
        } } | Should -Throw
        (Get-Content -LiteralPath (Join-Path $fixture.context.state_root state.json) -Raw) | Should -BeExactly $before
        $invalid = Copy-TriageFixtureValue $state.triage.profile
        $invalid.billing = 'unsupported'
        { Invoke-TriageTransaction $fixture.context triage-register-profile @{
            operator_approved = $true; profile = $invalid
        } } | Should -Throw
        $renamed = Copy-TriageFixtureValue $state.triage.profile
        $renamed.automation_id = 'reconciled-entry'
        $updated = Invoke-TriageTransaction $fixture.context triage-register-profile @{
            operator_approved = $true; profile = $renamed
        }
        ($updated.attempts | ConvertTo-Json) | Should -BeExactly ($state.attempts | ConvertTo-Json)
        $updated.profile | Should -Be $state.profile
        $updated.triage.active_analysis_id | Should -Be $state.triage.active_analysis_id
    }

    It 'does not admit publication in observe or paused mode and does not release its owner' -ForEach @(
        @{ Mode = 'observe' }, @{ Mode = 'paused' }
    ) {
        $null = Invoke-TriageTransaction $fixture.context triage-set-mode @{ operator_approved = $true; mode = $Mode }
        { Invoke-TriageTransaction $fixture.context triage-authorize-publication } | Should -Throw
        (Invoke-TriageTransaction $fixture.context read).triage.active_analysis_id | Should -Be $fixture.context.analysis_id
        { Invoke-TriageTransaction $fixture.context triage-set-mode @{ operator_approved = $false; mode = 'triage' } } | Should -Throw
    }

    It 'charges daily and lifetime continuations before delivery across injected days' {
        $context = $fixture.context.Clone()
        $null = Invoke-TriageTransaction $context triage-complete-dispatch @{ reason = 'More analysis remains' }
        foreach ($attempt in 1..12) {
            if ($attempt -eq 9) {
                $context.now = $context.now.AddDays(1)
                $state = Invoke-TriageTransaction $context triage-acquire-scan @{ session_id = 'later-poll' }
                $context.scan_token = $state.triage.scan.token
            }
            $state = Invoke-TriageTransaction $context triage-reserve-continuation @{
                scan_token = $context.scan_token; native_idle_verified = $true; evidence_key = "progress-$attempt"
            }
            $context.dispatch_token = $state.triage.analyses[$context.analysis_id].dispatch.token
            $null = Invoke-TriageTransaction $context triage-begin-dispatch @{ scan_token = $context.scan_token }
            $null = Invoke-TriageTransaction $context triage-accept-dispatch
            $null = Invoke-TriageTransaction $context triage-complete-dispatch @{ reason = 'Checkpoint retained' }
            if ($attempt -eq 8) {
                { Invoke-TriageTransaction $context triage-reserve-continuation @{
                    scan_token = $context.scan_token; native_idle_verified = $true; evidence_key = 'over-daily'
                } } | Should -Throw
            }
        }
        { Invoke-TriageTransaction $context triage-reserve-continuation @{
            scan_token = $context.scan_token; native_idle_verified = $true; evidence_key = 'over-lifetime'
        } } | Should -Throw
        (Invoke-TriageTransaction $context read).triage.analyses[$context.analysis_id].continuations.Count | Should -Be 12
    }

    It 'does not resend uncertain dispatch or accept a stale token' {
        $null = Invoke-TriageTransaction $fixture.context triage-complete-dispatch @{ reason = 'Resume later' }
        $state = Invoke-TriageTransaction $fixture.context triage-reserve-continuation @{
            scan_token = $fixture.context.scan_token; native_idle_verified = $true; evidence_key = 'new-input'
        }
        $null = Invoke-TriageTransaction $fixture.context triage-begin-dispatch @{ scan_token = $fixture.context.scan_token }
        { Invoke-TriageTransaction $fixture.context triage-begin-dispatch @{ scan_token = $fixture.context.scan_token } } |
            Should -Throw
        { Invoke-TriageTransaction $fixture.context triage-accept-dispatch } | Should -Throw
        $receiver = $fixture.context.Clone()
        $receiver.dispatch_token = $state.triage.analyses[$receiver.analysis_id].dispatch.token
        $accepted = Invoke-TriageTransaction $receiver triage-accept-dispatch
        $accepted.triage.analyses[$receiver.analysis_id].dispatch.status | Should -Be accepted
    }

    It 'rejects corrupt retained ownership instead of resetting it' {
        $state = Invoke-TriageTransaction $fixture.context read
        foreach ($damage in @('schema', 'phase', 'missing-owner', 'competing-owner')) {
            $corrupt = Copy-TriageFixtureValue $state.triage
            switch ($damage) {
                schema { $corrupt.schema_version = 99 }
                phase { $corrupt.analyses[$corrupt.active_analysis_id].phase = 'unknown' }
                missing-owner { $corrupt.active_analysis_id = 'missing' }
                competing-owner {
                    $other = Copy-TriageFixtureValue $corrupt.analyses[$corrupt.active_analysis_id]
                    $other.id = 'other'; $other.checkpoint.analysis.analysis_id = 'other'
                    $other.checkpoint_digest = Get-ScheduledDigest $other.checkpoint
                    $corrupt.analyses.other = $other
                }
            }
            { Assert-ScheduledTriageState $corrupt } | Should -Throw
        }
    }

    It 'requires confirmed publication and actual native quiescence before retiring work' {
        { Invoke-TriageTransaction $fixture.context triage-complete-analysis } | Should -Throw
        { Invoke-TriageTransaction $fixture.context triage-retire @{
            scan_token = $fixture.context.scan_token; native_idle_verified = $true
        } } | Should -Throw
        (Invoke-TriageTransaction $fixture.context read).triage.active_analysis_id | Should -Be $fixture.context.analysis_id
    }

    It 'rejects expired scans and unapproved or incomplete role registration' {
        $later = $fixture.context.Clone(); $later.now = $later.now.AddDays(1)
        { Invoke-TriageTransaction $later triage-release-scan @{ scan_token = $later.scan_token } } | Should -Throw
        $state = Invoke-TriageTransaction $fixture.context read
        $installed = Copy-TriageFixtureValue $state.triage.profile; $installed.user_id = 0
        { Invoke-TriageTransaction $fixture.context triage-register-profile @{
            operator_approved = $true; profile = $installed
        } } | Should -Throw
        $state.Remove('triage')
        { Invoke-ScheduledTriageStateChange $state $fixture.context.policy $fixture.context.triage_policy `
            triage-register-profile @{ operator_approved = $false } $fixture.context.now } | Should -Throw
    }

    It 'enforces new-start budget and exact historical claim identity after ownership is retired' {
        $state = Invoke-TriageTransaction $fixture.context read
        $triage = $state.triage; $analysis = $triage.analyses[$triage.active_analysis_id]
        $analysis.phase = 'retired'; $triage.active_analysis_id = $null; $triage.scan.started_analysis_id = $null
        $data = @{ revision = Copy-TriageFixtureValue $analysis.revision; session_id = $analysis.session_id
            scan_token = $fixture.context.scan_token; native_verified = $true }
        # Bind the internal transition's prepared input so these cases exercise its own
        # identity/budget guards rather than merely rejecting absent cache validation.
        $data.claim_validation = @{ snapshot_id = $triage.scan.snapshot_id
            revision_digest = Get-ScheduledDigest $data.revision }
        $data.native_verified = $false
        { Invoke-ScheduledTriageStateChange $state $fixture.context.policy $fixture.context.triage_policy `
            triage-claim $data $fixture.context.now } | Should -Throw
        $data.native_verified = $true
        $data.revision.workflow_id = 0
        $data.claim_validation.revision_digest = Get-ScheduledDigest $data.revision
        { Invoke-ScheduledTriageStateChange $state $fixture.context.policy $fixture.context.triage_policy `
            triage-claim $data $fixture.context.now } | Should -Throw
        $data.revision.workflow_id = 456
        $data.claim_validation.revision_digest = Get-ScheduledDigest $data.revision
        { Invoke-ScheduledTriageStateChange $state $fixture.context.policy $fixture.context.triage_policy `
            triage-claim $data $fixture.context.now } | Should -Throw
        foreach ($index in 1..7) {
            $copy = Copy-TriageFixtureValue $analysis; $copy.id = "retired-$index"
            $triage.analyses[$copy.id] = $copy
        }
        $data.revision.run_id = 790
        $data.claim_validation.revision_digest = Get-ScheduledDigest $data.revision
        { Invoke-ScheduledTriageStateChange $state $fixture.context.policy $fixture.context.triage_policy `
            triage-claim $data $fixture.context.now } | Should -Throw
    }

    It 'requires summary and full-read receipts before checkpoint validation' {
        $state = Invoke-TriageTransaction $fixture.context read
        $analysis = $state.triage.analyses[$fixture.context.analysis_id]
        $data = @{
            analysis_id = $analysis.id; session_id = $analysis.session_id
            claim_token = $analysis.claim_token; dispatch_token = $analysis.dispatch.token
            checkpoint = Copy-TriageFixtureValue $analysis.checkpoint
        }
        $data.checkpoint.analysis.checkpoint = 2
        $analysis.index_reads.Clear()
        { Invoke-ScheduledTriageStateChange $state $fixture.context.policy $fixture.context.triage_policy `
            triage-checkpoint $data $fixture.context.now } | Should -Throw
        $analysis.index_reads[$data.checkpoint.index.digest] = @(31)
        $data.checkpoint.index.entries = @(@{ issue_number = 31; full_read_digest = 'not-read' })
        { Invoke-ScheduledTriageStateChange $state $fixture.context.policy $fixture.context.triage_policy `
            triage-checkpoint $data $fixture.context.now } | Should -Throw
    }

    It 'rejects unfinished analysis and missing problem publication even with confirmed run writes' {
        $state = Invoke-TriageTransaction $fixture.context read
        $analysis = $state.triage.analyses[$fixture.context.analysis_id]
        $data = @{ analysis_id = $analysis.id; session_id = $analysis.session_id
            claim_token = $analysis.claim_token; dispatch_token = $analysis.dispatch.token }
        $analysis.checkpoint.analysis.status = 'blocked'
        { Invoke-ScheduledTriageStateChange $state $fixture.context.policy $fixture.context.triage_policy `
            triage-complete-analysis $data $fixture.context.now } | Should -Throw
        $analysis.checkpoint.analysis.status = 'complete'
        foreach ($purpose in @('triage-root', 'run-presentation')) {
            $analysis.operations[$purpose] = @{ checkpoint = 1; purpose = $purpose; stage = 'confirmed' }
        }
        { Invoke-ScheduledTriageStateChange $state $fixture.context.policy $fixture.context.triage_policy `
            triage-complete-analysis $data $fixture.context.now } | Should -Throw
    }

    It 'reconciles only a proven accepted dispatch without discarding analysis ownership' {
        $data = @{ scan_token = $fixture.context.scan_token; native_idle_verified = $false }
        { Invoke-TriageTransaction $fixture.context triage-reconcile-dispatch $data } | Should -Throw
        $data.native_idle_verified = $true
        $state = Invoke-TriageTransaction $fixture.context triage-reconcile-dispatch $data
        $state.triage.analyses[$fixture.context.analysis_id].dispatch.status | Should -Be completed
        $state.triage.active_analysis_id | Should -Be $fixture.context.analysis_id
        { Invoke-TriageTransaction $fixture.context triage-reconcile-dispatch $data } | Should -Throw
        foreach ($action in @('triage-reconcile-dispatch', 'triage-begin-dispatch', 'triage-retire')) {
            { Invoke-TriageTransaction $fixture.context $action ($data + @{ analysis_id = 'missing' }) } | Should -Throw
        }
    }

    It 'retains a specific blocker when a dispatch has not produced its first checkpoint' {
        $state = Invoke-TriageTransaction $fixture.context read
        $analysis = $state.triage.analyses[$fixture.context.analysis_id]
        $analysis.checkpoint = $null; $analysis.checkpoint_digest = $null
        $analysis.comparison = $null; $analysis.comparison_digest = $null
        $data = @{ analysis_id = $analysis.id; session_id = $analysis.session_id
            claim_token = $analysis.claim_token; dispatch_token = $analysis.dispatch.token; reason = '' }
        { Invoke-ScheduledTriageStateChange $state $fixture.context.policy $fixture.context.triage_policy `
            triage-complete-dispatch $data $fixture.context.now } | Should -Throw
        { Invoke-ScheduledTriageStateChange $state $fixture.context.policy $fixture.context.triage_policy `
            triage-block $data $fixture.context.now } | Should -Throw
        $data.reason = 'Required evidence is unavailable'
        Invoke-ScheduledTriageStateChange $state $fixture.context.policy $fixture.context.triage_policy `
            triage-complete-dispatch $data $fixture.context.now
        $analysis.reason | Should -Be $data.reason
    }

    It 'reads additive triage state with the reviewed default policy without resetting retained work' {
        $context = $fixture.context
        $state = Invoke-ScheduledLocalAction -StateRoot $context.state_root -Policy $context.policy `
            -ExecutorId $context.executor_id -Login $context.login -Now $context.now -Action triage-read
        $state.triage.active_analysis_id | Should -Be $context.analysis_id
        { Invoke-TriageTransaction $context triage-unsupported } | Should -Throw
    }
}
