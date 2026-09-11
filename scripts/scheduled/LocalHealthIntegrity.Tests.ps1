#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
# Corrupts serialized role-health journals before recovery to prove no transport is authorized.
# Creation observations remain separate from immutable intents, including lost-response recovery.
BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'fixtures\TriageFixture.psm1')
    Import-Module (Join-Path $PSScriptRoot 'LocalTriagePublication.psm1')
    Import-Module (Join-Path $PSScriptRoot 'LocalHealth.psm1')
    Import-Module (Join-Path $PSScriptRoot 'ScheduledContracts.psm1')
}

Describe 'Durable health publication integrity' {
    BeforeEach {
        $script:fixture = Initialize-TriageFixture -Root (Join-Path $TestDrive ([guid]::NewGuid().ToString('N')))
        Mock Get-ScheduledTriagePolicy -ModuleName LocalHealthState { $fixture.context.triage_policy }
        $currentState = Invoke-TriageTransaction $fixture.context read
        $record = Get-ScheduledRoleHealthRecord $currentState triage
        $script:state = Invoke-TriageTransaction $fixture.context health-prepare @{
            role = 'triage'; scan_token = $fixture.context.scan_token
            intent = @{ issue_number = 10; comment_id = $null; preimage = $null; record = $record
                body = "[Copilot speaking]`n$(Write-ScheduledRecord $record health)" }
        }
        $script:context = $fixture.context.Clone(); $context.role = 'triage'
        $script:path = Join-Path $fixture.context.state_root state.json
        $script:access = [Collections.Generic.List[string]]::new()
        $script:transport = { param($Endpoint) $access.Add($Endpoint); throw 'Unexpected transport.' }
    }

    It 'rejects <Damage> before any GitHub access and leaves corrupt state untouched' -ForEach @(
        @{ Damage = 'body' }, @{ Damage = 'record' }, @{ Damage = 'kind' }, @{ Damage = 'stage' }
        @{ Damage = 'target' }, @{ Damage = 'positive-target' }, @{ Damage = 'receipt' }, @{ Damage = 'role' }
        @{ Damage = 'rehashed-owner' }, @{ Damage = 'rehashed-body' }, @{ Damage = 'rehashed-marker' }
    ) {
        $operation = $state.health_publications.triage
        switch ($Damage) {
            body { $operation.intent.body += ' altered' }
            record { $operation.intent.record.executor_id = 'another' }
            kind { $operation.kind = 'update-issue' }
            stage { $operation.stage = 'unknown' }
            target { $operation.comment_id = 0 }
            positive-target { $operation.comment_id = 900 }
            receipt { $operation.stage = 'complete' }
            role { $state.health_publications['foreign'] = $operation; $state.health_publications.Remove('triage') }
            rehashed-owner {
                $operation.intent.record.executor_id = 'another'
                $operation.intent.body = "[Copilot speaking]`n$(Write-ScheduledRecord $operation.intent.record health)"
                $operation.intent_digest = Get-ScheduledDigest $operation.intent
            }
            rehashed-body {
                $copy = Copy-TriageFixtureValue $operation.intent.record; $copy.mode = 'paused'
                $operation.intent.body = "[Copilot speaking]`n$(Write-ScheduledRecord $copy health)"
                $operation.intent_digest = Get-ScheduledDigest $operation.intent
            }
            rehashed-marker {
                $operation.intent.body = '[Copilot speaking] Missing health record'
                $operation.intent_digest = Get-ScheduledDigest $operation.intent
            }
        }
        $state | ConvertTo-Json -Depth 100 | Set-Content -LiteralPath $path
        $before = Get-Content -LiteralPath $path -Raw
        { Sync-ScheduledRoleHealth $context $transport } | Should -Throw
        $access.Count | Should -Be 0
        (Get-Content -LiteralPath $path -Raw) | Should -BeExactly $before
    }

    It 'recovers a lost creation response without changing the immutable creation target' {
        $fixture.store.issues[10L] = @{
            number = 10; title = 'Health'; state = 'open'; user = @{ login = 'reporter' }
            labels = @(@{ name = 'scheduled-health' }, @{ name = 'scheduled-coverage' })
            body = Write-ScheduledRecord -Kind coverage -Record @{
                schema_version = 1; repository = 'owner/repository'; repository_id = 123
            }
        }
        $fixture.store.comments[10L] = [Collections.Generic.List[object]]::new()
        $fixture.store.lose_comment = $true
        { Sync-ScheduledRoleHealth $context $fixture.api } | Should -Throw
        (Sync-ScheduledRoleHealth $context $fixture.api).action | Should -Be reconciled
        $restored = Invoke-TriageTransaction $fixture.context read
        $operation = $restored.health_publications.triage
        $operation.intent.comment_id | Should -BeNullOrEmpty
        $operation.comment_id | Should -Be $fixture.store.comments[10L][0].id
        $operation.receipt.comment_id | Should -Be $operation.comment_id
        $operation.intent_digest | Should -BeExactly $state.health_publications.triage.intent_digest
        $fixture.store.comments[10L].Count | Should -Be 1
        $operation.receipt.operation_id = 'foreign'
        $restored | ConvertTo-Json -Depth 100 | Set-Content -LiteralPath $path
        { Sync-ScheduledRoleHealth $context $transport } | Should -Throw
        $access.Count | Should -Be 0
    }

    It 'requires a sending intent and its observed comment before confirmation' {
        $operation = $state.health_publications.triage
        $data = @{ role = 'triage'; scan_token = $fixture.context.scan_token
            operation_id = $operation.id; comment_id = 900; record_digest = Get-ScheduledDigest $operation.intent.record }
        { Invoke-TriageTransaction $fixture.context health-confirm $data } | Should -Throw
        $null = Invoke-TriageTransaction $fixture.context health-begin $data
        { Invoke-TriageTransaction $fixture.context health-confirm $data } | Should -Throw
        $wrong = $data.Clone(); $wrong.operation_id = 'stale'
        { Invoke-TriageTransaction $fixture.context health-observe $wrong } | Should -Throw
        $null = Invoke-TriageTransaction $fixture.context health-observe $data
        $wrong = $data.Clone(); $wrong.comment_id = 901
        { Invoke-TriageTransaction $fixture.context health-confirm $wrong } | Should -Throw
        $null = Invoke-TriageTransaction $fixture.context health-confirm $data
        $confirmed = Invoke-TriageTransaction $fixture.context health-confirm $data
        $confirmed.health_publications.triage.comment_id | Should -Be 900
        $confirmed.health_publications.triage.stage | Should -Be complete
    }

    It 'records begin and observation when a prepared intent already has matching remote readback' {
        $operation = $state.health_publications.triage
        $fixture.store.issues[10L] = @{
            number = 10; title = 'Health'; state = 'open'; user = @{ login = 'reporter' }
            labels = @(@{ name = 'scheduled-health' }, @{ name = 'scheduled-coverage' })
            body = Write-ScheduledRecord -Kind coverage -Record @{
                schema_version = 1; repository = 'owner/repository'; repository_id = 123
            }
        }
        $fixture.store.comments[10L] = [Collections.Generic.List[object]]::new()
        $fixture.store.comments[10L].Add(@{ id = 900; body = $operation.intent.body
            user = @{ login = 'worker' }; issue_url = 'https://api.github.com/repos/owner/repository/issues/10' })
        (Sync-ScheduledRoleHealth $context $fixture.api).action | Should -Be reconciled
        $confirmed = Invoke-TriageTransaction $fixture.context read
        $confirmed.health_publications.triage.receipt.comment_id | Should -Be 900
        $fixture.store.writes.Count | Should -Be 0
    }
}
