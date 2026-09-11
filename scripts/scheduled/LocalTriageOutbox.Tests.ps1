#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
# Corrupts serialized retained outbox state to prove recovery cannot bypass live admission.
# Invalid kinds, changed intents and inconsistent receipts fail before any GitHub access.
BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'fixtures\TriageFixture.psm1')
    Import-Module (Join-Path $PSScriptRoot 'LocalTriagePublication.psm1')
    Import-Module (Join-Path $PSScriptRoot 'LocalTriage.psm1')
    Import-Module (Join-Path $PSScriptRoot 'ScheduledContracts.psm1')
}

Describe 'Restored triage publication authority' {
    BeforeEach {
        $script:fixture = Initialize-TriageFixture -Root (Join-Path $TestDrive ([guid]::NewGuid().ToString('N')))
        $script:specification = @{
            key = '1/comment'; kind = 'create-comment'; issue_number = 20; target_id = $null
            checkpoint = 1; purpose = 'triage-detail'; preimage = $null
            payload = @{ body = '[Copilot speaking] Retained detail' }
        }
        $script:state = Invoke-TriageTransaction $fixture.context triage-prepare-operation @{ operation = $specification }
        $script:path = Join-Path $fixture.context.state_root state.json
        $script:access = [Collections.Generic.List[string]]::new()
        $script:transport = {
            param($Endpoint)
            $access.Add($Endpoint)
            throw 'A corrupt outbox must fail before requesting GitHub data.'
        }
    }

    It 'rejects restored <Damage> before publication or recovery can access GitHub' -ForEach @(
        @{ Damage = 'unsupported-kind' }, @{ Damage = 'rehashed-unsupported-kind' }
        @{ Damage = 'payload' }, @{ Damage = 'operation-key' }, @{ Damage = 'operation-id' }
        @{ Damage = 'future-checkpoint' }, @{ Damage = 'stage' }, @{ Damage = 'invalid-target' }
        @{ Damage = 'missing-field' }, @{ Damage = 'missing-receipt' }, @{ Damage = 'receipt-digest' }
    ) {
        $operation = $state.triage.analyses[$fixture.context.analysis_id].operations[$specification.key]
        switch ($Damage) {
            unsupported-kind { $operation.kind = 'delete-issue' }
            rehashed-unsupported-kind {
                $operation.kind = 'delete-issue'
                $replacement = Copy-TriageFixtureValue $specification; $replacement.kind = 'delete-issue'
                $operation.spec_digest = Get-ScheduledDigest $replacement
            }
            payload { $operation.payload.body = '[Copilot speaking] Changed persisted intent' }
            operation-key { $operation.key = 'another-key' }
            operation-id { $operation.id = 'another-operation' }
            future-checkpoint { $operation.checkpoint = 2 }
            stage { $operation.stage = 'unknown' }
            invalid-target { $operation.target_id = 0 }
            missing-field { $operation.Remove('kind') }
            missing-receipt { $operation.stage = 'confirmed' }
            receipt-digest {
                $operation.stage = 'confirmed'; $operation.target_id = 900
                $target = @{ id = 900; body = $operation.payload.body }
                $operation.receipt = @{ target_id = 900; operation_id = $operation.id; payload_digest = 'different'
                    target = $target; target_digest = Get-ScheduledDigest $target }
            }
        }
        $state | ConvertTo-Json -Depth 100 | Set-Content -LiteralPath $path
        $before = Get-Content -LiteralPath $path -Raw
        { Invoke-ScheduledTriageOperation $fixture.context $specification $transport } | Should -Throw
        { Get-ScheduledTriageRecovery $fixture.context.policy $state $transport } | Should -Throw
        $access.Count | Should -Be 0
        (Get-Content -LiteralPath $path -Raw) | Should -BeExactly $before
    }

    It 'rejects a corrupt restored intent before the entry point even checks the selected GitHub account' {
        $state.triage.analyses[$fixture.context.analysis_id].operations[$specification.key].payload.body = 'altered'
        $state | ConvertTo-Json -Depth 100 | Set-Content -LiteralPath $path
        Mock Get-ScheduledPolicy -ModuleName LocalTriage { $fixture.context.policy }
        Mock Get-ScheduledTriagePolicy -ModuleName LocalTriage { $fixture.context.triage_policy }
        Mock Get-ScheduledStateRoot -ModuleName LocalTriage { $fixture.context.state_root }
        Mock Invoke-ScheduledTriageRead -ModuleName LocalTriage { throw 'Unexpected GitHub read.' }
        $requestPath = Join-Path $TestDrive request.json
        @{ action = 'recovery'; executor_id = 'executor'; data = @{} } |
            ConvertTo-Json | Set-Content -LiteralPath $requestPath
        { Invoke-ScheduledTriageRequest -RequestPath $requestPath -Now $fixture.context.now } | Should -Throw
        Should -Invoke Invoke-ScheduledTriageRead -ModuleName LocalTriage -Exactly -Times 0
    }

    It 'retains valid observed creation IDs and confirmed receipts across repeated state restoration' {
        (Get-ScheduledTriageRecovery $fixture.context.policy @{} $transport).active | Should -BeNullOrEmpty
        $access.Count | Should -Be 0
        $state = Invoke-TriageTransaction $fixture.context triage-begin-operation @{ operation_key = $specification.key }
        $operation = $state.triage.analyses[$fixture.context.analysis_id].operations[$specification.key]
        $null = Invoke-TriageTransaction $fixture.context triage-observe-operation @{
            operation_key = $specification.key; target_id = 900
        }
        $state = Invoke-TriageTransaction $fixture.context read
        $state.triage.analyses[$fixture.context.analysis_id].operations[$specification.key].spec_digest |
            Should -BeExactly $operation.spec_digest
        $target = @{ id = 900; body = $specification.payload.body }
        $null = Invoke-TriageTransaction $fixture.context triage-confirm-operation @{
            operation_key = $specification.key
            receipt = @{ target_id = 900; operation_id = $operation.id; payload_digest = Get-ScheduledDigest $specification.payload
                target = $target; target_digest = Get-ScheduledDigest $target }
        }
        $state = Invoke-TriageTransaction $fixture.context read
        $state.triage.analyses[$fixture.context.analysis_id].operations[$specification.key].stage | Should -Be confirmed
        $state.triage.analyses[$fixture.context.analysis_id].operations[$specification.key].target_id | Should -Be 900
    }
}
