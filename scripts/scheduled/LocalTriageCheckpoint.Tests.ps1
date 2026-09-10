#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
# Exercises persisted checkpoint/plan integrity and the lock-free typed validation boundary.
# A matching checksum is not a substitute for the typed contract or a current locked owner.
BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'fixtures\TriageFixture.psm1')
    Import-Module (Join-Path $PSScriptRoot 'LocalTriagePublication.psm1')
    Import-Module (Join-Path $PSScriptRoot 'LocalTriageCheckpoint.psm1')
    Import-Module (Join-Path $PSScriptRoot 'LocalTriage.psm1')
    Import-Module (Join-Path $PSScriptRoot 'ScheduledContracts.psm1')
    $script:validate = (Get-Command Get-ScheduledTriageCheckpointValidation).ScriptBlock
}

Describe 'Restored analysis and plan integrity' {
    BeforeEach {
        $script:fixture = Initialize-TriageFixture -Root (Join-Path $TestDrive ([guid]::NewGuid().ToString('N')))
        $script:state = Invoke-TriageTransaction $fixture.context triage-prepare-document @{
            key = 'pending-plan'; document = @{ issue_number = 20; explanation = 'Validated plan content' }
        }
        $script:path = Join-Path $fixture.context.state_root state.json
        $script:access = [Collections.Generic.List[string]]::new()
        $script:probe = @{ opened = $false }
        $script:transport = { param($Endpoint) $access.Add($Endpoint); throw 'Unexpected transport.' }
        Mock Get-ScheduledPolicy -ModuleName LocalTriage { $fixture.context.policy }
        Mock Get-ScheduledTriagePolicy -ModuleName LocalTriage { $fixture.context.triage_policy }
        Mock Get-ScheduledStateRoot -ModuleName LocalTriage { $fixture.context.state_root }
        Mock Invoke-ScheduledTriageRead -ModuleName LocalTriage { throw 'Unexpected GitHub read.' }
    }

    It 'rejects persisted <Damage> before publication or entry-point transport' -ForEach @(
        @{ Damage = 'analysis' }, @{ Damage = 'evidence' }, @{ Damage = 'index' }, @{ Damage = 'basis' }
        @{ Damage = 'comparison' }, @{ Damage = 'plan' }, @{ Damage = 'rehashed-contract' }, @{ Damage = 'rehashed-owner' }
    ) {
        $analysis = $state.triage.analyses[$fixture.context.analysis_id]
        switch ($Damage) {
            analysis { $analysis.checkpoint.analysis.reason = 'Altered persisted decision' }
            evidence { $analysis.checkpoint.evidence.attempt.jobs[0].log.excerpt = 'Altered evidence' }
            index { $analysis.checkpoint.index.complete = $false }
            basis { $analysis.checkpoint.basis.api_evidence.total_count = 2 }
            comparison { $analysis.comparison.requires_reanalysis = $true }
            plan { $analysis.publication['1/pending-plan'].issue_number = 21 }
            rehashed-contract {
                $analysis.checkpoint.analysis.jobs = @()
                $analysis.checkpoint_digest = Get-ScheduledDigest $analysis.checkpoint
            }
            rehashed-owner {
                $analysis.checkpoint.analysis.analysis_id = 'another-analysis'
                $analysis.checkpoint_digest = Get-ScheduledDigest $analysis.checkpoint
            }
        }
        $state | ConvertTo-Json -Depth 100 | Set-Content -LiteralPath $path
        $before = Get-Content -LiteralPath $path -Raw
        { Invoke-TriageTransaction $fixture.context triage-authorize-publication } | Should -Throw
        { Get-ScheduledTriageRecovery $fixture.context.policy $state $transport } | Should -Throw
        $requestPath = Join-Path $TestDrive request.json
        @{ action = 'recovery'; executor_id = 'executor'; data = @{} } | ConvertTo-Json | Set-Content -LiteralPath $requestPath
        { Invoke-ScheduledTriageRequest -RequestPath $requestPath -Now $fixture.context.now } | Should -Throw
        Should -Invoke Invoke-ScheduledTriageRead -ModuleName LocalTriage -Exactly -Times 0
        $access.Count | Should -Be 0
        (Get-Content -LiteralPath $path -Raw) | Should -BeExactly $before
    }

    It 'validates without holding the state lock and rejects a checkpoint changed before the locked recheck' {
        Mock Get-ScheduledTriageCheckpointValidation -ModuleName LocalTriageCheckpoint {
            $lock = [IO.File]::Open((Join-Path $fixture.context.state_root transaction.lock),
                [IO.FileMode]::Open, [IO.FileAccess]::ReadWrite, [IO.FileShare]::None)
            $lock.Dispose()
            $probe.opened = $true
            $result = & $validate -Checkpoint $Checkpoint
            $changed = Get-Content -LiteralPath $path -Raw | ConvertFrom-Json -AsHashtable
            $analysis = $changed.triage.analyses[$fixture.context.analysis_id]
            $analysis.checkpoint.analysis.reason = 'A newer retained checkpoint view'
            $analysis.checkpoint_digest = Get-ScheduledDigest $analysis.checkpoint
            $changed | ConvertTo-Json -Depth 100 | Set-Content -LiteralPath $path
            $result
        }
        { Get-ScheduledTriageValidatedState $fixture.context } | Should -Throw
        $probe.opened | Should -BeTrue
        $access.Count | Should -Be 0
    }

    It 'binds lock-free checkpoint preparation to the exact submitted input' {
        $script:candidate = Copy-TriageFixtureValue $state.triage.analyses[$fixture.context.analysis_id].checkpoint
        $candidate.analysis.checkpoint = 2
        Mock Get-ScheduledTriageCheckpointValidation -ModuleName LocalState {
            $lock = [IO.File]::Open((Join-Path $fixture.context.state_root transaction.lock),
                [IO.FileMode]::Open, [IO.FileAccess]::ReadWrite, [IO.FileShare]::None)
            $lock.Dispose()
            $probe.opened = $true
            $result = & $validate -Checkpoint $Checkpoint
            $candidate.analysis.reason = 'Changed after validation'
            $result
        }
        { Invoke-TriageTransaction $fixture.context triage-checkpoint @{ checkpoint = $candidate } } | Should -Throw
        $probe.opened | Should -BeTrue
        (Invoke-TriageTransaction $fixture.context read).triage.analyses[$fixture.context.analysis_id].checkpoint.analysis.checkpoint |
            Should -Be 1
    }
}
