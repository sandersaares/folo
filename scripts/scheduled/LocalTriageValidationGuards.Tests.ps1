#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
# Exercises malformed restored references and stale validation proofs without external effects.
BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'fixtures\TriageFixture.psm1')
    Import-Module (Join-Path $PSScriptRoot 'LocalTriageCheckpoint.psm1')
    Import-Module (Join-Path $PSScriptRoot 'LocalTriageState.psm1')
    Import-Module (Join-Path $PSScriptRoot 'LocalTriagePublication.psm1')
    Import-Module (Join-Path $PSScriptRoot 'LocalTriageCache.psm1')
    Import-Module (Join-Path $PSScriptRoot 'LocalTriageCompletion.psm1')
    Import-Module (Join-Path $PSScriptRoot 'LocalHealthIntegrity.psm1')
    Import-Module (Join-Path $PSScriptRoot 'ScheduledContracts.psm1')
}

Describe 'Triage reference validation guards' {
    BeforeEach {
        $script:fixture = Initialize-TriageFixture -Root (Join-Path $TestDrive ([guid]::NewGuid().ToString('N')))
        $script:state = Invoke-TriageTransaction $fixture.context read
        $script:analysis = $state.triage.analyses[$fixture.context.analysis_id]
    }

    It 'rejects missing, changed and noncanonical typed checkpoint inputs' {
        { Get-ScheduledTriageCheckpointValidation @{} } | Should -Throw
        { Assert-ScheduledTriageCheckpointContent $analysis.checkpoint ('f' * 64) } | Should -Throw
        $checkpoint = Copy-TriageFixtureValue $analysis.checkpoint
        $checkpoint.analysis.Remove('workflow')
        { Assert-ScheduledTriageCheckpointContent $checkpoint (Get-ScheduledDigest $checkpoint) } | Should -Throw
    }

    It 'rejects invalid restored outboxes, snapshot identities and completion references' {
        foreach ($damage in @('outbox', 'snapshot', 'completion-missing', 'completion-changed')) {
            $copy = Copy-TriageFixtureValue $state.triage
            $item = $copy.analyses[$fixture.context.analysis_id]
            switch ($damage) {
                outbox { $item.operations = @() }
                snapshot {
                    $item.working_snapshot_id = 'not-a-digest'
                    $copy.cache_digest = Get-ScheduledDigest (Get-ScheduledTriageCacheProjection $copy)
                }
                completion-missing { $item.phase = 'complete' }
                completion-changed {
                    $item.completion = @{ checkpoint = 1; issue_number = 20; comment_id = 900; digest = 'a' * 64 }
                    $item.completion_digest = 'changed'
                }
            }
            { Assert-ScheduledTriageState $copy } | Should -Throw
        }
    }

    It 'requires the current native scan owner and matching prepared-file ownership' {
        { Invoke-TriageTransaction $fixture.context triage-authorize-snapshot @{
            analysis_id = $null; scan_token = $fixture.context.scan_token; session_id = 'another-session'
        } } | Should -Throw
        { Invoke-TriageTransaction $fixture.context triage-pin-snapshot @{
            snapshot_id = 'a' * 64; temporary_path = 'unused'; owner_kind = 'scan'; owner_token = 'foreign'
        } } | Should -Throw
        { Complete-ScheduledTriageSnapshotFile $fixture.context.state_root ('a' * 64) `
            (Join-Path $TestDrive foreign.tmp) analysis $fixture.context.dispatch_token } | Should -Throw
    }

    It 'does not consume unowned checkpoint snapshots or stale publication validation' {
        $checkpoint = Copy-TriageFixtureValue $analysis.checkpoint
        $checkpoint.analysis.checkpoint = 2; $checkpoint.snapshot_id = 'a' * 64
        { Invoke-TriageTransaction $fixture.context triage-checkpoint @{ checkpoint = $checkpoint } } | Should -Throw
        $data = @{ analysis_id = $analysis.id; session_id = $analysis.session_id
            claim_token = $analysis.claim_token; dispatch_token = $analysis.dispatch.token; checkpoint_digest = 'stale' }
        { Invoke-ScheduledTriageStateChange $state $fixture.context.policy $fixture.context.triage_policy `
            triage-authorize-publication $data $fixture.context.now } | Should -Throw
        $data.scan_token = $fixture.context.scan_token; $data.native_idle_verified = $true
        $data.completion_digest = $null; $data.analysis_id = 'unknown'
        { Invoke-ScheduledTriageStateChange $state $fixture.context.policy $fixture.context.triage_policy `
            triage-retire $data $fixture.context.now } | Should -Throw
        $other = $fixture.context.Clone(); $other.analysis_id = 'unknown'
        { Complete-ScheduledTriageRetirement $other @{} $fixture.api } | Should -Throw
    }

    It 'rejects a completion root that names another analysis' {
        $analysis.operations = @{
            root = @{ checkpoint = 1; purpose = 'triage-root'; stage = 'confirmed'; target_id = 900
                payload = @{ body = Write-ScheduledRecord -Kind triage -Record @{
                    schema_version = 1; analysis_id = 'foreign'; run_id = 789; issue_number = 20; status = 'complete'
                } } }
            presentation = @{ checkpoint = 1; purpose = 'run-presentation'; stage = 'confirmed' }
            problem = @{ checkpoint = 1; purpose = 'problem-root:download'; stage = 'confirmed' }
        }
        $data = @{ analysis_id = $analysis.id; session_id = $analysis.session_id
            claim_token = $analysis.claim_token; dispatch_token = $analysis.dispatch.token }
        { Invoke-ScheduledTriageStateChange $state $fixture.context.policy $fixture.context.triage_policy `
            triage-complete-analysis $data $fixture.context.now } | Should -Throw
    }

    It 'reads registered observation and a first uncheckpointed claim without constructing a typed checkpoint' {
        $unclaimed = Initialize-TriageFixture -Root (Join-Path $TestDrive 'unclaimed') -Unclaimed
        (Get-ScheduledTriageValidatedState $unclaimed.context).triage.active_analysis_id | Should -BeNullOrEmpty
        $claimed = Invoke-TriageTransaction $unclaimed.context triage-claim @{
            scan_token = $unclaimed.context.scan_token; session_id = 'session'; native_verified = $true; revision = $unclaimed.revision
        }
        (Get-ScheduledTriageValidatedState $unclaimed.context).triage.active_analysis_id |
            Should -Be $claimed.triage.active_analysis_id
    }

    It 'cleans its temporary payload when installation authorization fails' {
        $script:prepared = @{ path = $null }
        Mock Invoke-TriageTransaction -ModuleName LocalTriagePublication {
            $prepared.path = $Data.temporary_path
            throw [IO.IOException]::new('Installation interrupted.')
        } -ParameterFilter { $Action -ceq 'triage-pin-snapshot' }
        $owner = @{ analysis_id = $fixture.context.analysis_id; session_id = $fixture.context.session_id
            claim_token = $fixture.context.claim_token; dispatch_token = $fixture.context.dispatch_token }
        { Save-ScheduledTriageSnapshot $fixture.context $fixture.snapshot $owner } | Should -Throw
        $prepared.path | Should -Not -BeNullOrEmpty
        Test-Path -LiteralPath $prepared.path | Should -BeFalse
    }

    It 'rejects malformed role-health journals before transport' {
        $state.health_publications = @()
        { Assert-ScheduledHealthJournal $state } | Should -Throw
        $state.health_publications = @{ triage = @{} }
        { Assert-ScheduledHealthJournal $state } | Should -Throw
        $record = @{ schema_version = 1; role = 'triage'; repository = $state.repository
            repository_id = $state.repository_id; executor_id = $state.executor_id }
        $intent = @{ issue_number = 10; comment_id = $null; preimage = $null
            record = $record; body = "[Copilot speaking]`n$(Write-ScheduledRecord $record health)" }
        $state.health_publications.triage = @{ id = 'operation'; stage = 'prepared'; kind = 'create-comment'
            intent = $intent; intent_digest = Get-ScheduledDigest $intent; comment_id = $null; receipt = @{} }
        { Assert-ScheduledHealthJournal $state } | Should -Throw
    }
}
