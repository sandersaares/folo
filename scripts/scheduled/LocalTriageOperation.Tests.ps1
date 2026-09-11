#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
# Protects publication ownership, payload and response-loss fences at the real local journal.
# The injected transport never writes to GitHub; unknown outcomes remain durable for recovery.
BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'fixtures\TriageFixture.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'LocalTriagePublication.psm1')
    Import-Module (Join-Path $PSScriptRoot 'ScheduledContracts.psm1')
    Import-Module (Join-Path $PSScriptRoot 'ScheduledRecordTool.psm1')
}

Describe 'Evidence-bound publication operations' {
    BeforeEach {
        $script:fixture = Initialize-TriageFixture -Root (Join-Path $TestDrive ([guid]::NewGuid().ToString('N')))
        $script:spec = @{
            key = '1/comment'; kind = 'create-comment'; issue_number = 20; target_id = $null
            checkpoint = 1; purpose = 'triage-detail'; preimage = $null
            payload = @{ body = '[Copilot speaking] Test detail' }
        }
    }

    It 'uses the default transport adapter and rejects a changed specification after preparation' {
        Mock Invoke-ScheduledGitHubApi -ModuleName LocalTriagePublication {
            & $fixture.api -Endpoint $Endpoint -Method $Method -Body $Body -Paginate:$Paginate
        }
        (Invoke-ScheduledTriageOperation $fixture.context $spec).action | Should -Be confirmed
        $spec.payload.body += ' altered'
        { Invoke-ScheduledTriageOperation $fixture.context $spec $fixture.api } | Should -Throw
        $fixture.store.writes.Count | Should -Be 1
    }

    It 'rejects restored readback <Damage> before any transport or own-write comparison' -ForEach @(
        @{ Damage = 'target' }, @{ Damage = 'missing-digest' }, @{ Damage = 'array-digest' }, @{ Damage = 'scalar-target' }
    ) {
        $null = Invoke-ScheduledTriageOperation $fixture.context $spec $fixture.api
        $state = Invoke-TriageTransaction $fixture.context read
        $receipt = $state.triage.analyses[$fixture.context.analysis_id].operations[$spec.key].receipt
        switch ($Damage) {
            target { $receipt.target.body = 'An external change is not our confirmed write' }
            missing-digest { $receipt.Remove('target_digest') }
            array-digest { $receipt.target_digest = @($receipt.target_digest) }
            scalar-target {
                $receipt.target = 'not a target snapshot'
                $receipt.target_digest = Get-ScheduledDigest $receipt.target
            }
        }
        $path = Join-Path $fixture.context.state_root state.json
        $state | ConvertTo-Json -Depth 100 | Set-Content -LiteralPath $path
        $before = Get-Content -LiteralPath $path -Raw
        $access = [Collections.Generic.List[string]]::new()
        $transport = { param($Endpoint) $access.Add($Endpoint); throw 'Unexpected transport.' }
        { Invoke-ScheduledTriageOperation $fixture.context $spec $transport } | Should -Throw
        $access.Count | Should -Be 0
        (Get-Content -LiteralPath $path -Raw) | Should -BeExactly $before
    }

    It 'blocks ambiguous identical comments rather than selecting one to acknowledge' {
        foreach ($id in @(900, 901)) {
            $fixture.store.comments[20L].Add(@{
                id = $id; body = $spec.payload.body; user = @{ login = 'worker' }
                issue_url = 'https://api.github.com/repos/owner/repository/issues/20'
            })
        }
        { Invoke-ScheduledTriageOperation $fixture.context $spec $fixture.api } | Should -Throw
        $fixture.store.writes.Count | Should -Be 0
    }

    It 'does not truncate an oversized comment or repeat a superseded operation' {
        $spec.payload.body += ('x' * 60000)
        { Invoke-ScheduledTriageOperation $fixture.context $spec $fixture.api } | Should -Throw
        $state = Invoke-TriageTransaction $fixture.context read
        $checkpoint = Copy-TriageFixtureValue $state.triage.analyses[$fixture.context.analysis_id].checkpoint
        $checkpoint.analysis.checkpoint = 2
        $null = Invoke-TriageTransaction $fixture.context triage-checkpoint @{ checkpoint = $checkpoint }
        { Invoke-ScheduledTriageOperation $fixture.context $spec $fixture.api } | Should -Throw
        $fixture.store.writes.Count | Should -Be 0
    }

    It 'requires readback of the exact body after a successful API response' {
        $api = {
            param($Endpoint, $Method = 'GET', $Body, [switch] $Paginate)
            $response = & $fixture.api -Endpoint $Endpoint -Method $Method -Body $Body -Paginate:$Paginate
            if ($Method -ceq 'POST') { $fixture.store.comments[20L][-1].body = 'Unexpected stored body' }
            return $response
        }
        { Invoke-ScheduledTriageOperation $fixture.context $spec $api } | Should -Throw
        $state = Invoke-TriageTransaction $fixture.context read
        $state.triage.analyses[$fixture.context.analysis_id].operations[$spec.key].stage | Should -Be sending
    }

    It 'preserves foreign or changed owned blocks and refuses a missing known target' {
        $spec.kind = 'update-issue'; $spec.target_id = 20; $spec.purpose = 'problem-root:download'
        $spec.block_kind = 'problem'; $spec.preimage = ''
        $spec.payload.body = '<!-- scheduled-problem-content:start -->new<!-- scheduled-problem-content:end -->'
        $fixture.store.issues[20L].body += '<!-- scheduled-problem-content:start -->foreign<!-- scheduled-problem-content:end -->'
        { Invoke-ScheduledTriageOperation $fixture.context $spec $fixture.api } | Should -Throw
        $fixture.store.issues[20L].user.login = 'foreign'
        { Invoke-ScheduledTriageOperation $fixture.context $spec $fixture.api } | Should -Throw
        $api = {
            param($Endpoint, $Method = 'GET', $Body, [switch] $Paginate)
            if ($Endpoint -ceq 'repos/owner/repository/issues/20') { return $null }
            & $fixture.api -Endpoint $Endpoint -Method $Method -Body $Body -Paginate:$Paginate
        }
        { Invoke-ScheduledTriageOperation $fixture.context $spec $api } | Should -Throw
        $fixture.store.writes.Count | Should -Be 0
    }

    It 'does not acknowledge a different reporter index when matching presentation is <AlreadyPresented>' -ForEach @(
        @{ AlreadyPresented = $false }, @{ AlreadyPresented = $true }
    ) {
        $spec.kind = 'update-issue'; $spec.target_id = 20; $spec.purpose = 'run-presentation'
        $spec.preimage = 'stale'; $spec.run_id = 789; $spec.triaged = $true; $spec.payload = @{ state = 'closed' }
        if ($AlreadyPresented) {
            $fixture.store.issues[20L].state = 'closed'
            $fixture.store.issues[20L].labels += @{ name = 'scheduled-triaged' }
        }
        { Invoke-ScheduledTriageOperation $fixture.context $spec $fixture.api } | Should -Throw
        $fixture.store.issues[20L].state | Should -Be $(if ($AlreadyPresented) { 'closed' } else { 'open' })
        $fixture.store.writes.Count | Should -Be 0
    }

    It 'preserves a human state transition after problem preparation and reconciles an actual lost reopening response' {
        $fixture.store.issues[31L] = @{
            number = 31; body = '[Copilot speaking] Original'; state = 'open'
            user = @{ login = 'worker' }; labels = @(@{ name = 'scheduled-finding' })
        }
        $spec.kind = 'update-issue'; $spec.target_id = 31; $spec.issue_number = 31
        $spec.purpose = 'problem-root:download'; $spec.block_kind = 'problem'; $spec.preimage = ''
        $spec.expected_state = 'open'
        $spec.payload = @{ state = 'open'
            body = '<!-- scheduled-problem-content:start -->Recurrence<!-- scheduled-problem-content:end -->' }
        $null = Invoke-TriageTransaction $fixture.context triage-prepare-operation @{ operation = $spec }
        $fixture.store.issues[31L].state = 'closed'
        { Invoke-ScheduledTriageOperation $fixture.context $spec $fixture.api } | Should -Throw
        $fixture.store.issues[31L].state | Should -Be closed
        $fixture.store.writes.Count | Should -Be 0
        $spec.key = '1/reconsidered-state'; $spec.expected_state = 'closed'
        $fixture.store.lose_update = $true
        { Invoke-ScheduledTriageOperation $fixture.context $spec $fixture.api } | Should -Throw
        $fixture.store.issues[31L].state | Should -Be open
        (Invoke-ScheduledTriageOperation $fixture.context $spec $fixture.api).action | Should -Be confirmed
        $fixture.store.writes.Count | Should -Be 1
    }

    It 'preserves non-owned text and prefixes an existing unprefixed publication body' {
        $fixture.store.issues[20L].body = 'Human description'
        $spec.kind = 'update-issue'; $spec.target_id = 20; $spec.purpose = 'problem-root:download'
        $spec.block_kind = 'problem'; $spec.preimage = ''
        $spec.payload.body = '<!-- scheduled-problem-content:start -->new<!-- scheduled-problem-content:end -->'
        (Invoke-ScheduledTriageOperation $fixture.context $spec $fixture.api).action | Should -Be confirmed
        $fixture.store.issues[20L].body.StartsWith('[Copilot speaking]') | Should -BeTrue
        $fixture.store.issues[20L].body | Should -Match 'Human description'
    }

    It 'reports retained prepared and unknown operations even when a full inbox cannot complete' {
        $null = Invoke-TriageTransaction $fixture.context triage-prepare-operation @{ operation = $spec }
        $state = Invoke-TriageTransaction $fixture.context read
        $before = Get-ScheduledTriageRecovery $fixture.context.policy $state $fixture.api
        $before.operations[0].visible | Should -BeFalse
        $fixture.store.lose_comment = $true
        { Invoke-ScheduledTriageOperation $fixture.context $spec $fixture.api } | Should -Throw
        $state = Invoke-TriageTransaction $fixture.context read
        $after = Get-ScheduledTriageRecovery $fixture.context.policy $state $fixture.api
        $after.operations[0].visible | Should -BeTrue
        $after.evidence_key | Should -Not -Be $before.evidence_key
    }

    It 'requires the original document history and observes an already committed current revision' {
        $empty = Invoke-ScheduledRecordTool -Package scheduled-run-record -Request @{
            op = 'restore_documents'; kind = 'problem'; owner = '123/20'; comments = @()
        }
        $document = @{ diagnosis = 'First detail' }
        $published = Publish-ScheduledTriageDocument $fixture.context 20 problem $document detail $empty.index_digest $fixture.api
        (Publish-ScheduledTriageDocument $fixture.context 20 problem $document detail $published.index_digest $fixture.api).digest |
            Should -Be $published.digest
        { Publish-ScheduledTriageDocument $fixture.context 20 problem @{ diagnosis = 'Different history' } `
            other $empty.index_digest $fixture.api } | Should -Throw
    }

    It 'does not advance the detail root when a confirmed page disappears from collection' {
        $empty = Invoke-ScheduledRecordTool -Package scheduled-run-record -Request @{
            op = 'restore_documents'; kind = 'problem'; owner = '123/20'; comments = @()
        }
        $api = {
            param($Endpoint, $Method = 'GET', $Body, [switch] $Paginate)
            if ($Endpoint.EndsWith('/comments?per_page=100') -and $fixture.store.writes.Count -gt 0) { return ,@() }
            & $fixture.api -Endpoint $Endpoint -Method $Method -Body $Body -Paginate:$Paginate
        }
        { Publish-ScheduledTriageDocument $fixture.context 20 problem @{ diagnosis = 'Detail' } `
            detail $empty.index_digest $api } | Should -Throw
        @($fixture.store.writes | Where-Object { $_.method -ceq 'PATCH' }).Count | Should -Be 0
    }
}
