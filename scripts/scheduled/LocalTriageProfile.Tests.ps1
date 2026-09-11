#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
# Verifies native-prompt facts remain distinct from executable skill/controller identity and
# that scan/dispatch ownership, not an unrelated cached match, authorizes triage and health.
BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'fixtures\TriageFixture.psm1')
    Import-Module (Join-Path $PSScriptRoot 'LocalTriagePublication.psm1')
    Import-Module (Join-Path $PSScriptRoot 'LocalTriageProfile.psm1')
    Import-Module (Join-Path $PSScriptRoot 'LocalTriagePolicy.psm1')
    Import-Module (Join-Path $PSScriptRoot 'LocalHealth.psm1')
    Import-Module (Join-Path $PSScriptRoot 'ScheduledRoleHealth.psm1')
    Import-Module (Join-Path $PSScriptRoot 'ScheduledContracts.psm1')
}

Describe 'Owned native profile observations' {
    It 'requires native and restored observation identities to retain their scalar types' {
        $native = @{ automation_id = 'entry'; prompt_digest = 'a' * 64 }
        foreach ($field in @('automation_id', 'prompt_digest')) {
            $invalid = $native.Clone(); $invalid[$field] = @($native[$field])
            { Get-ScheduledTriageProfileObservation $invalid scan 'token' 'session' } |
                Should -Throw -ExceptionType ([FormatException])
        }
        { Get-ScheduledTriageProfileObservation $native scan 42 'session' } | Should -Throw -ExceptionType ([FormatException])
        { Get-ScheduledTriageProfileObservation $native scan 'token' 42 } | Should -Throw -ExceptionType ([FormatException])
        $valid = Get-ScheduledTriageProfileObservation $native scan 'token' 'session'
        foreach ($field in @($valid.Keys)) {
            $invalid = $valid.Clone()
            $invalid[$field] = if ($field -ceq 'schema_version') { '1' } else { ,@($valid[$field]) }
            if ($field -cne 'digest') {
                $payload = $invalid.Clone(); $payload.Remove('digest')
                $invalid.digest = Get-ScheduledDigest $payload
            }
            { Assert-ScheduledTriageProfileObservation $invalid scan 'token' 'session' } |
                Should -Throw -ExceptionType ([FormatException])
        }
        $valid.schema_version = [long]1
        Assert-ScheduledTriageProfileObservation $valid scan 'token' 'session'
        $public = Get-ScheduledTriageHealthObservation $valid
        Test-ScheduledTriageHealthObservation $native $public @{
            session_id = 'session'; binding_digest = $public.binding_digest
        } | Should -BeTrue
    }

    It 'blocks missing, changed and foreign native prompt observations without consuming an analysis start' -ForEach @(
        @{ Change = 'missing' }, @{ Change = 'prompt' }, @{ Change = 'automation' }
    ) {
        $fixture = Initialize-TriageFixture -Root (Join-Path $TestDrive $Change) -Unclaimed
        $null = Invoke-TriageTransaction $fixture.context triage-release-scan @{ scan_token = $fixture.context.scan_token }
        $observed = Copy-TriageFixtureValue $fixture.context.profile_observation
        switch ($Change) {
            missing { $observed = $null }
            prompt { $observed.prompt_digest = Get-ScheduledTriagePromptDigest 'Changed native prompt' }
            automation { $observed.automation_id = 'another-entry' }
        }
        $state = Invoke-TriageTransaction $fixture.context triage-acquire-scan @{
            session_id = 'poll'; profile_observation = $observed
        }
        { Invoke-TriageTransaction $fixture.context triage-claim @{
            session_id = 'poll'; scan_token = $state.triage.scan.token
            native_verified = $true; revision = $fixture.revision
        } } | Should -Throw
        $state.triage.analyses.Count | Should -Be 0
        $health = Get-ScheduledRoleHealthRecord $state triage
        ($health | ConvertTo-Json -Depth 100 -Compress).Contains($state.triage.scan.token) | Should -BeFalse
        $comments = @(@{ user = @{ login = 'worker' }; body = Write-ScheduledRecord $health health })
        $projection = Get-ScheduledRoleScan $fixture.context.policy $fixture.context.triage_policy $comments triage
        $projection.blocked_conditions | Should -Contain triage-profile-drift
    }

    It 'requires a new observation for the accepted dispatch and rejects an old owner receipt on restoration' {
        $fixture = Initialize-TriageFixture -Root (Join-Path $TestDrive 'dispatch')
        $before = Invoke-TriageTransaction $fixture.context read
        $old = $before.triage.analyses[$fixture.context.analysis_id].profile_observation
        $null = Invoke-TriageTransaction $fixture.context triage-complete-dispatch @{ reason = 'Continue in the same native owner' }
        $state = Invoke-TriageTransaction $fixture.context triage-reserve-continuation @{
            scan_token = $fixture.context.scan_token; native_idle_verified = $true; evidence_key = 'new-input'
        }
        $receiver = $fixture.context.Clone()
        $receiver.dispatch_token = $state.triage.analyses[$receiver.analysis_id].dispatch.token
        $null = Invoke-TriageTransaction $fixture.context triage-begin-dispatch @{ scan_token = $fixture.context.scan_token }
        { Invoke-TriageTransaction $receiver triage-accept-dispatch @{ profile_observation = $null } } | Should -Throw
        { Invoke-TriageTransaction $receiver triage-accept-dispatch @{ profile_observation = $old } } | Should -Throw
        $null = Invoke-TriageTransaction $receiver triage-accept-dispatch @{ profile_observation = $receiver.profile_observation }
        $null = Invoke-TriageTransaction $receiver triage-authorize-publication
        $state = Invoke-TriageTransaction $receiver read
        $state.triage.analyses[$receiver.analysis_id].profile_observation = $old
        $path = Join-Path $receiver.state_root state.json
        $state | ConvertTo-Json -Depth 100 | Set-Content -LiteralPath $path
        { Invoke-TriageTransaction $receiver read } | Should -Throw
    }

    It 'does not let a worker borrow the coordinator scan observation for publication' {
        $fixture = Initialize-TriageFixture -Root (Join-Path $TestDrive 'unrelated-proof')
        $state = Invoke-TriageTransaction $fixture.context read
        $state.triage.analyses[$fixture.context.analysis_id].profile_observation = $null
        $path = Join-Path $fixture.context.state_root state.json
        $state | ConvertTo-Json -Depth 100 | Set-Content -LiteralPath $path
        { Invoke-TriageTransaction $fixture.context triage-authorize-publication @{
            scan_token = $fixture.context.scan_token
        } } | Should -Throw
    }

    It 'does not let hosted health borrow another scan observation and detects controller drift' {
        $fixture = Initialize-TriageFixture -Root (Join-Path $TestDrive 'health')
        $state = Invoke-TriageTransaction $fixture.context read
        $health = Get-ScheduledRoleHealthRecord $state triage
        $comments = @(@{ user = @{ login = 'worker' }; body = Write-ScheduledRecord $health health })
        (Get-ScheduledRoleScan $fixture.context.policy $fixture.context.triage_policy $comments triage).outcome | Should -Be passed
        $health.profile.controller_digest = 'f' * 64
        $comments[0].body = Write-ScheduledRecord $health health
        (Get-ScheduledRoleScan $fixture.context.policy $fixture.context.triage_policy $comments triage).blocked_conditions |
            Should -Contain triage-profile-drift
        $other = Get-ScheduledTriageProfileObservation $fixture.context.profile_observation scan 'other-token' 'other-session'
        $health.profile_observation = Get-ScheduledTriageHealthObservation $other
        $comments[0].body = Write-ScheduledRecord $health health
        { Get-ScheduledRoleScan $fixture.context.policy $fixture.context.triage_policy $comments triage } | Should -Throw
    }

    It 'treats absent scan observations as unavailable and rejects malformed observation records' {
        $fixture = Initialize-TriageFixture -Root (Join-Path $TestDrive 'absent-scan')
        $state = Invoke-TriageTransaction $fixture.context triage-release-scan @{ scan_token = $fixture.context.scan_token }
        $health = Get-ScheduledRoleHealthRecord $state triage
        $health.profile_scan | Should -BeNullOrEmpty
        $health.profile_observation | Should -BeNullOrEmpty
        { Assert-ScheduledTriageProfileObservation @{} scan 'token' 'session' } | Should -Throw
        { Test-ScheduledTriageHealthObservation $state.triage.profile @{} @{
            binding_digest = 'a' * 64; session_id = 'session'
        } } | Should -Throw
    }
}

Describe 'Portable working controller identity' {
    It 'uses repository text normalization but still detects dirty script, skill and contract changes' {
        $root = Join-Path $TestDrive 'controller'
        $null = New-Item -ItemType Directory -Path $root
        git -C $root init --quiet
        [IO.File]::WriteAllText((Join-Path $root .gitattributes), "* text eol=lf`n")
        [IO.File]::WriteAllText((Join-Path $root 'script.psm1'), "first`nsecond`n")
        [IO.File]::WriteAllText((Join-Path $root 'skill.md'), "first`r`nsecond`r`n")
        InModuleScope LocalTriagePolicy -Parameters @{ Root = $root } {
            $first = @(Get-TriageWorkingFileHash $Root @('script.psm1', 'skill.md', '.gitattributes'))
            $first[0] | Should -BeExactly $first[1]
            [IO.File]::WriteAllText((Join-Path $Root 'script.psm1'), "first`nchanged`n")
            $dirty = @(Get-TriageWorkingFileHash $Root @('script.psm1', 'skill.md'))
            $dirty[0] | Should -Not -Be $first[0]
            [IO.File]::WriteAllText((Join-Path $Root 'skill.md'), "first`nchanged skill`n")
            $skill = @(Get-TriageWorkingFileHash $Root @('skill.md'))
            $skill[0] | Should -Not -Be $first[1]
            [IO.File]::WriteAllText((Join-Path $Root .gitattributes), "* -text`n")
            $contract = @(Get-TriageWorkingFileHash $Root @('.gitattributes'))
            $contract[0] | Should -Not -Be $first[2]
        }
        (Get-ScheduledTriagePromptDigest "native`r`nprompt") | Should -BeExactly (Get-ScheduledTriagePromptDigest "native`nprompt")
        (Get-ScheduledTriagePromptDigest 'changed native prompt') | Should -Not -Be (Get-ScheduledTriagePromptDigest 'native prompt')
    }

    It 'rejects incomplete native Git hash output instead of producing a partial controller identity' {
        InModuleScope LocalTriagePolicy -Parameters @{ Root = $TestDrive } {
            Mock Invoke-ScheduledJsonExecutable { '' }
            { Get-TriageWorkingFileHash $Root @('script.psm1') } | Should -Throw
        }
    }
}
