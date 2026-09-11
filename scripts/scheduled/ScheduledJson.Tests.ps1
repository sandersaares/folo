#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
# Exercises the source-shaped timestamp that broke issue 569 through real native codecs,
# GitHub adapters, embedded records, cache files and retained checkpoints. No live API writes
# or AI role execution occur: the GitHub store and Local ownership are isolated test fixtures.
BeforeAll {
    foreach ($module in @('ScheduledJson', 'ScheduledContracts', 'ScheduledRecordTool',
            'LocalGitHub', 'ScheduledGitHub', 'ScheduledGate', 'LocalTriageCache',
            'LocalTriageCheckpoint', 'LocalTriagePublication')) {
        Import-Module (Join-Path $PSScriptRoot "$module.psm1")
    }
    Import-Module (Join-Path $PSScriptRoot 'fixtures\TriageFixture.psm1')
    # Exact planner timestamp representation from run 34574480961/1, not an injected clock.
    $script:timestamp = '2026-09-11T07:27:47.8355988+00:00'
    $script:payload = '{"planned_at":"2026-09-11T07:27:47.8355988+00:00","started_at":"2026-09-11T07:27:40Z",' +
        '"values":[null,[],["2026-09-11"],true,false,34574480961,-1073740791,0.02],"empty":""}'
}

Describe 'Lossless scheduled JSON values' {
    It 'retains strings, native numbers and nested array shape without date interpretation' {
        $value = ConvertFrom-ScheduledJson $payload
        $value.planned_at | Should -BeOfType string
        $value.planned_at | Should -BeExactly $timestamp
        $value.started_at | Should -BeExactly '2026-09-11T07:27:40Z'
        $value.values.Count | Should -Be 8
        $value.values[0] | Should -BeNullOrEmpty
        ($value.values[1] -is [object[]]) | Should -BeTrue
        $value.values[1].Count | Should -Be 0
        ($value.values[2] -is [object[]]) | Should -BeTrue
        $value.values[2][0] | Should -BeExactly '2026-09-11'
        $value.values[3] | Should -BeTrue
        $value.values[4] | Should -BeFalse
        $value.values[5] | Should -BeOfType long
        $value.values[5] | Should -Be 34574480961
        $value.values[6] | Should -Be -1073740791
        $value.values[7] | Should -Be 0.02
        $value.empty | Should -BeExactly ''
        $copy = $value | ConvertTo-Json -Depth 100 | ConvertFrom-ScheduledJson
        Get-ScheduledDigest $copy | Should -BeExactly (Get-ScheduledDigest $value)
    }

    It 'preserves empty and singleton top-level arrays when requested' {
        $empty = ConvertFrom-ScheduledJson '[]' -NoEnumerate
        ($empty -is [object[]]) | Should -BeTrue
        $empty.Count | Should -Be 0
        $nested = ConvertFrom-ScheduledJson '[[{"id":34574480961}]]' -NoEnumerate
        $nested.Count | Should -Be 1
        ($nested[0] -is [object[]]) | Should -BeTrue
        $nested[0][0].id | Should -Be 34574480961
        @(ConvertFrom-ScheduledJson '[]').Count | Should -Be 0
    }

    It 'rejects malformed JSON rather than returning a successful empty record' {
        { ConvertFrom-ScheduledJson '{"planned_at":' } | Should -Throw -ExceptionType ([ArgumentException])
    }

    It 'preserves source-shaped timestamps through the <Module> GitHub reader' -ForEach @(
        @{ Module = 'LocalGitHub' }, @{ Module = 'ScheduledGitHub' }, @{ Module = 'ScheduledGate' }
    ) {
        InModuleScope $Module -Parameters @{ Json = $payload; Timestamp = $timestamp; Owner = $Module } {
            param($Json, $Timestamp, $Owner)
            $script:responseJson = $Json
            Mock gh { $global:LASTEXITCODE = 0; $script:responseJson }
            $value = switch ($Owner) {
                LocalGitHub { Invoke-ScheduledApi -Endpoint 'repos/owner/repo/issues/1' }
                ScheduledGitHub { Invoke-ScheduledGhJson -Arguments @('api', 'repos/owner/repo/issues/1') }
                ScheduledGate { Invoke-ScheduledReadApi -Endpoint 'repos/owner/repo/issues/1' }
            }
            $value.planned_at | Should -BeOfType string
            $value.planned_at | Should -BeExactly $Timestamp
            $value.values[5] | Should -Be 34574480961
        }
    }
}

Describe 'Lossless evidence consumption' {
    It 'restores, persists, reads and renders the original revision and rejects real timestamp tampering' {
        $fixture = Initialize-TriageFixture -Root (Join-Path $TestDrive 'evidence') `
            -Plan @{ run = $true; planned_at = $timestamp }
        $prepared = Invoke-ScheduledRecordTool -Package scheduled-run-record -Request @{
            op = 'prepare'; evidence = $fixture.evidence
        }
        $restored = Invoke-ScheduledRecordTool -Package scheduled-run-record -Request @{
            op = 'restore'; identity = $prepared.identity
            comments = @($fixture.store.comments[20L] | ForEach-Object { @{ id = $_.id; body = $_.body } })
        }
        $restored.record.revisions[0].digest | Should -BeExactly $prepared.digest
        $restored.record.revisions[0].evidence.attempt.plan.planned_at | Should -BeExactly $timestamp
        $rendered = Invoke-ScheduledRecordTool -Package scheduled-run-record -Request @{ op = 'render'; record = $restored.record }
        $snapshot = @{ record = $restored.record }
        $pending = Write-ScheduledTriageSnapshotFile $fixture.context.state_root $snapshot scan $fixture.context.scan_token
        Complete-ScheduledTriageSnapshotFile $fixture.context.state_root $pending.id $pending.temporary_path scan $pending.owner_token
        $copy = Read-ScheduledTriageSnapshot $fixture.context.state_root $pending.id
        $marker = Write-ScheduledRecord -Kind reporter -Record (@{ schema_version = 1 } + $copy)
        $embedded = Read-ScheduledRecord -Kind reporter -Text $marker
        $again = Invoke-ScheduledRecordTool -Package scheduled-run-record -Request @{ op = 'render'; record = $embedded.record }
        $again.index_digest | Should -BeExactly $rendered.index_digest
        $embedded.record.revisions[0].digest | Should -BeExactly $prepared.digest
        # The instant is unchanged, but the JSON string is not. Never repair a checksum by
        # silently rehashing altered evidence or normalizing timestamp representations.
        $embedded.record.revisions[0].evidence.attempt.plan.planned_at = '2026-09-11T07:27:47.8355988Z'
        { Invoke-ScheduledRecordTool -Package scheduled-run-record -Request @{ op = 'render'; record = $embedded.record } } |
            Should -Throw
        $cachePath = Get-ScheduledTriageSnapshotPath $fixture.context.state_root $pending.id
        $copy.record = $embedded.record
        $copy | ConvertTo-Json -Depth 100 | Set-Content -LiteralPath $cachePath
        { Read-ScheduledTriageSnapshot $fixture.context.state_root $pending.id } | Should -Throw
    }

    It 'retains the validated checkpoint digest across state writes, reads and native validation' {
        $fixture = Initialize-TriageFixture -Root (Join-Path $TestDrive 'checkpoint') `
            -Plan @{ run = $true; planned_at = $timestamp }
        $state = Get-ScheduledTriageValidatedState $fixture.context
        $analysis = $state.triage.analyses[$fixture.context.analysis_id]
        $original = $analysis.checkpoint_digest
        $analysis.checkpoint.evidence.attempt.plan.planned_at | Should -BeExactly $timestamp
        Assert-ScheduledTriageCheckpointContent $analysis.checkpoint $original
        $candidate = Copy-TriageFixtureValue $analysis.checkpoint
        $candidate.analysis.checkpoint = 2
        $candidate.analysis.reason = $timestamp
        $saved = Invoke-TriageTransaction $fixture.context triage-checkpoint @{ checkpoint = $candidate }
        $read = Get-ScheduledTriageValidatedState $fixture.context
        $retained = $read.triage.analyses[$fixture.context.analysis_id]
        $retained.checkpoint.analysis.reason | Should -BeExactly $timestamp
        $retained.checkpoint.evidence.attempt.plan.planned_at | Should -BeExactly $timestamp
        $retained.checkpoint_digest | Should -BeExactly $saved.triage.analyses[$fixture.context.analysis_id].checkpoint_digest
        $retained.checkpoint.evidence.attempt.plan.planned_at = '2026-09-11T07:27:47.8355988Z'
        $read | ConvertTo-Json -Depth 100 | Set-Content -LiteralPath (Join-Path $fixture.context.state_root state.json)
        { Get-ScheduledTriageValidatedState $fixture.context } | Should -Throw
    }
}
