#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
# Pins role ownership, budget and state-integrity transitions with real atomic JSON persistence.
# Time, native quiescence and dispatch acceptance are explicitly supplied by each scenario.
BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'fixtures\TriageFixture.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'LocalTriagePublication.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'LocalState.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'LocalTriageState.psm1') -Force
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
        foreach ($damage in @('schema', 'missing-owner', 'competing-owner')) {
            $corrupt = Copy-TriageFixtureValue $state.triage
            switch ($damage) {
                schema { $corrupt.schema_version = 99 }
                missing-owner { $corrupt.active_analysis_id = 'missing' }
                competing-owner {
                    $other = Copy-TriageFixtureValue $corrupt.analyses[$corrupt.active_analysis_id]
                    $other.id = 'other'; $corrupt.analyses.other = $other
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
}
