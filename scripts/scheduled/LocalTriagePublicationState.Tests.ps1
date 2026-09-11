#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
# Exercises journal preimages, unknown outcomes, checkpoint supersession and repair holds
# through the durable transaction boundary. Synthetic receipts stand in for verified API reads.
BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'fixtures\TriageFixture.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'LocalTriagePublication.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'ScheduledContracts.psm1') -Force
}

Describe 'Triage publication state requirements' {
    BeforeEach {
        $script:fixture = Initialize-TriageFixture -Root (Join-Path $TestDrive ([guid]::NewGuid().ToString('N')))
        $script:operation = @{
            key = '1/problem/root'; kind = 'update-issue'; issue_number = 31; target_id = 31
            payload = @{ body = 'owned content' }; preimage = ''; checkpoint = 1; purpose = 'problem-root:download'
        }
    }

    It 'records an immutable operation once and rejects conflicting or unrelated writes' {
        $null = Invoke-TriageTransaction $fixture.context triage-prepare-operation @{ operation = $operation }
        $null = Invoke-TriageTransaction $fixture.context triage-prepare-operation @{ operation = $operation }
        $changed = Copy-TriageFixtureValue $operation; $changed.payload.body = 'different'
        { Invoke-TriageTransaction $fixture.context triage-prepare-operation @{ operation = $changed } } | Should -Throw
        $changed = Copy-TriageFixtureValue $operation; $changed.key = 'bad'; $changed.kind = 'create-pr'
        { Invoke-TriageTransaction $fixture.context triage-prepare-operation @{ operation = $changed } } | Should -Throw
        $changed.kind = 'create-label'; $changed.payload = @{ name = 'unrelated-label' }
        { Invoke-TriageTransaction $fixture.context triage-prepare-operation @{ operation = $changed } } | Should -Throw
    }

    It 'fences begin observe and confirm against missing or mismatched operation evidence' {
        { Invoke-TriageTransaction $fixture.context triage-begin-operation @{ operation_key = 'missing' } } | Should -Throw
        $null = Invoke-TriageTransaction $fixture.context triage-prepare-operation @{ operation = $operation }
        { Invoke-TriageTransaction $fixture.context triage-observe-operation @{
            operation_key = $operation.key; target_id = 31
        } } | Should -Throw
        $state = Invoke-TriageTransaction $fixture.context triage-begin-operation @{ operation_key = $operation.key }
        { Invoke-TriageTransaction $fixture.context triage-begin-operation @{ operation_key = $operation.key } } | Should -Throw
        { Invoke-TriageTransaction $fixture.context triage-observe-operation @{
            operation_key = $operation.key; target_id = 32
        } } | Should -Throw
        $op = $state.triage.analyses[$fixture.context.analysis_id].operations[$operation.key]
        $target = @{ number = 31; body = $operation.payload.body }
        $receipt = @{ operation_id = $op.id; target_id = 31; payload_digest = 'not-the-payload'
            target = $target; target_digest = Get-ScheduledDigest $target }
        { Invoke-TriageTransaction $fixture.context triage-confirm-operation @{
            operation_key = $operation.key; receipt = $receipt
        } } | Should -Throw
        $receipt.payload_digest = Get-ScheduledDigest $operation.payload
        $null = Invoke-TriageTransaction $fixture.context triage-confirm-operation @{
            operation_key = $operation.key; receipt = $receipt
        }
        $null = Invoke-TriageTransaction $fixture.context triage-confirm-operation @{
            operation_key = $operation.key; receipt = $receipt
        }
    }

    It 'does not replace a prepared document or checkpoint over an uncertain publication' {
        $null = Invoke-TriageTransaction $fixture.context triage-prepare-document @{
            key = 'document'; document = @{ diagnosis = 'retained' }
        }
        $null = Invoke-TriageTransaction $fixture.context triage-prepare-document @{
            key = 'document'; document = @{ diagnosis = 'retained' }
        }
        { Invoke-TriageTransaction $fixture.context triage-prepare-document @{
            key = 'document'; document = @{ diagnosis = 'replacement' }
        } } | Should -Throw
        $null = Invoke-TriageTransaction $fixture.context triage-prepare-operation @{ operation = $operation }
        $state = Invoke-TriageTransaction $fixture.context read
        $checkpoint = Copy-TriageFixtureValue $state.triage.analyses[$fixture.context.analysis_id].checkpoint
        $checkpoint.analysis.checkpoint = 2
        $null = Invoke-TriageTransaction $fixture.context triage-begin-operation @{ operation_key = $operation.key }
        { Invoke-TriageTransaction $fixture.context triage-checkpoint @{ checkpoint = $checkpoint } } | Should -Throw
    }

    It 'supersedes only unsent operations when analysis is deliberately reconsidered' {
        $null = Invoke-TriageTransaction $fixture.context triage-prepare-operation @{ operation = $operation }
        $state = Invoke-TriageTransaction $fixture.context read
        $checkpoint = Copy-TriageFixtureValue $state.triage.analyses[$fixture.context.analysis_id].checkpoint
        $checkpoint.analysis.checkpoint = 2
        $state = Invoke-TriageTransaction $fixture.context triage-checkpoint @{ checkpoint = $checkpoint }
        $state.triage.analyses[$fixture.context.analysis_id].operations[$operation.key].stage | Should -Be superseded
        { Invoke-TriageTransaction $fixture.context triage-checkpoint @{ checkpoint = $checkpoint } } | Should -Throw
    }

    It 'binds repair-scope holds to confirmed problem updates and requires operator release' {
        { Invoke-TriageTransaction $fixture.context triage-record-repair-hold @{
            issue_number = 31; operation_key = 'missing'; reason = 'Scope changed'; repair_attempt_id = 'retained'
        } } | Should -Throw
        $operation.payload.body = Write-ScheduledRecord -Kind problem -Record @{
            schema_version = 1; generation = 1; scope_revision = 2; repair_disposition = 'actionable'
        }
        $null = Invoke-TriageTransaction $fixture.context triage-prepare-operation @{ operation = $operation }
        $state = Invoke-TriageTransaction $fixture.context triage-begin-operation @{ operation_key = $operation.key }
        $op = $state.triage.analyses[$fixture.context.analysis_id].operations[$operation.key]
        $target = @{ number = 31; body = $op.payload.body }
        $null = Invoke-TriageTransaction $fixture.context triage-confirm-operation @{
            operation_key = $operation.key
            receipt = @{ operation_id = $op.id; target_id = 31; payload_digest = Get-ScheduledDigest $op.payload
                target = $target; target_digest = Get-ScheduledDigest $target }
        }
        $state = Invoke-TriageTransaction $fixture.context triage-record-repair-hold @{
            issue_number = 31; operation_key = $operation.key; reason = 'Scope changed'; repair_attempt_id = 'retained'
        }
        $state.triage.repair_holds['31'].reason | Should -Be 'Scope changed'
        { Invoke-TriageTransaction $fixture.context triage-release-repair-hold @{
            issue_number = 31; operator_approved = $false
        } } | Should -Throw
        (Invoke-TriageTransaction $fixture.context triage-release-repair-hold @{
            issue_number = 31; operator_approved = $true
        }).triage.repair_holds.Count | Should -Be 0
        $state = Invoke-TriageTransaction $fixture.context triage-release-repair-hold @{
            issue_number = 31; operator_approved = $true
        }
        $state.triage.repair_reconciliations['31'].scope_revision | Should -Be 2
        { Invoke-TriageTransaction $fixture.context triage-release-repair-hold @{
            issue_number = 32; operator_approved = $true
        } } | Should -Throw
    }

    It 'rejects unconfirmed comparison baselines and missing reconsideration reasons' {
        { Invoke-TriageTransaction $fixture.context triage-accept-own-index @{
            previous_digest = 'unknown'; index = @{}; operation_key = 'missing'
        } } | Should -Throw
        { Invoke-TriageTransaction $fixture.context triage-require-reanalysis @{ reason = '' } } | Should -Throw
        $null = Invoke-TriageTransaction $fixture.context triage-require-reanalysis @{ reason = 'External evidence changed' }
        { Invoke-TriageTransaction $fixture.context triage-complete-analysis } | Should -Throw
    }

    It 'requires ordered complete candidate reads before issuing a read receipt' {
        { Invoke-TriageTransaction $fixture.context triage-record-problem-page @{
            issue_number = 31; full_read_digest = 'read'; offset = 5; end_offset = 10; total_length = 10
        } } | Should -Throw
        $state = Invoke-TriageTransaction $fixture.context triage-record-problem-page @{
            issue_number = 31; full_read_digest = 'read'; offset = 0; end_offset = 5; total_length = 10
        }
        $state.triage.analyses[$fixture.context.analysis_id].reads.ContainsKey('31') | Should -BeFalse
        $state = Invoke-TriageTransaction $fixture.context triage-record-problem-page @{
            issue_number = 31; full_read_digest = 'read'; offset = 5; end_offset = 10; total_length = 10
        }
        $state.triage.analyses[$fixture.context.analysis_id].reads['31'] | Should -Be read
    }

    It 'merges repeated singleton index receipts without unwrapping their array shape' {
        $null = Invoke-TriageTransaction $fixture.context triage-record-index-read @{
            index_digest = 'same-index'; issue_numbers = @(31)
        }
        $null = Invoke-TriageTransaction $fixture.context triage-record-index-read @{
            index_digest = 'same-index'; issue_numbers = @(31, 32)
        }
        $state = Invoke-TriageTransaction $fixture.context triage-record-index-read @{
            index_digest = 'same-index'; issue_numbers = @()
        }
        $state.triage.analyses[$fixture.context.analysis_id].index_reads['same-index'] | Should -Be @(31, 32)
    }
}
