#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
# Proves native setup's uncertain-create fence survives repeated setup without creating an
# enrollment directory or resetting either role's work. All files are isolated under TestDrive.
BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'LocalSetupState.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'LocalSetup.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'ScheduledContracts.psm1')
}

Describe 'Durable disabled setup intent' {
    BeforeEach {
        $script:path = Join-Path $TestDrive "$([guid]::NewGuid().ToString('N')).json"
        $script:desired = @{
            repository = 'owner/repository'; project_id = 'project'; host_id = 'local'
            executor_id = 'executor'; login = 'operator'; name = 'triage'
            marker = 'folo-scheduled-triage:v1'; cadence_cron = '7 */3 * * *'
            prompt = 'folo-scheduled-triage:v1 Run scheduled-triage'; model = 'chosen'
        }
    }

    It 'reads absence without writing and requires operator approval before creation intent' {
        (Invoke-ScheduledSetupJournal $path 123 read).roles.Count | Should -Be 0
        Test-Path -LiteralPath $path | Should -BeFalse
        { Invoke-ScheduledSetupJournal $path 123 begin-create triage @{ desired = $desired } } | Should -Throw
        Test-Path -LiteralPath $path | Should -BeFalse
    }

    It 'fences a lost native create even when a complete lookup currently contains no entry' {
        $journal = Invoke-ScheduledSetupJournal $path 123 begin-create triage @{
            operator_approved = $true; desired = $desired
        }
        $journal.roles.triage.stage | Should -Be creating
        { Invoke-ScheduledSetupJournal $path 123 begin-create triage @{
            operator_approved = $true; desired = $desired
        } } | Should -Throw
        (Get-ScheduledRoleSetupDecision -Role triage -Desired $desired -Workflows @() `
            -MetadataComplete $true -SetupJournal $journal).reason | Should -Be unknown-native-create
        $journal = Invoke-ScheduledSetupJournal $path 123 confirm triage @{
            operator_approved = $true; ownership_verified = $true; automation_id = 'observed'
        }
        $journal.roles.triage.automation_id | Should -Be observed
        $journal.roles.triage.stage | Should -Be complete
    }

    It 'registers a separately observed repair entry without losing triage intent' {
        $null = Invoke-ScheduledSetupJournal $path 123 begin-create triage @{
            operator_approved = $true; desired = $desired
        }
        $journal = Invoke-ScheduledSetupJournal $path 123 confirm repair @{
            operator_approved = $true; ownership_verified = $true; automation_id = 'repair'
        }
        $journal.roles.triage.stage | Should -Be creating
        $journal.roles.repair.automation_id | Should -Be repair
        (Invoke-ScheduledSetupJournal $path 123 read).roles.Count | Should -Be 2
    }

    It 'rejects foreign or corrupt journals and unverified native identities' {
        $null = Invoke-ScheduledSetupJournal $path 123 begin-create triage @{
            operator_approved = $true; desired = $desired
        }
        { Invoke-ScheduledSetupJournal $path 124 read } | Should -Throw
        { Invoke-ScheduledSetupJournal $path 123 confirm triage @{
            operator_approved = $true; ownership_verified = $false; automation_id = 'unproved'
        } } | Should -Throw
        '{"schema_version":99,"repository_id":123,"roles":{}}' | Set-Content -LiteralPath $path
        { Invoke-ScheduledSetupJournal $path 123 read } | Should -Throw
    }

    It 'uses a sibling journal path without creating a missing enrollment' {
        Mock Get-ScheduledStateRoot -ModuleName LocalSetupState { Join-Path $TestDrive '123' }
        $journalPath = Get-ScheduledSetupJournalPath 123
        $journalPath | Should -Be (Join-Path $TestDrive 'setup-123.json')
        $null = Invoke-ScheduledSetupJournal $journalPath 123 begin-create triage @{
            operator_approved = $true; desired = $desired
        }
        Test-Path -LiteralPath (Join-Path $TestDrive '123') | Should -BeFalse
    }
    It 'requires canonical repository identity and a verified desired entry before preparing creation' {
        { Invoke-ScheduledSetupJournal 'relative.json' 123 read } | Should -Throw
        { Invoke-ScheduledSetupJournal $path 0 read } | Should -Throw
        { Invoke-ScheduledSetupJournal $path 123 begin-create triage @{
            operator_approved = $true
        } } | Should -Throw
        { Invoke-ScheduledSetupJournal -Path $path -RepositoryId 123 -Action begin-create `
            -Data @{ operator_approved = $true; desired = $desired } } | Should -Throw
        Test-Path -LiteralPath $path | Should -BeFalse
    }

    It 'rejects corrupted role payloads instead of authorizing a replacement native creation' -ForEach @(
        @{ Damage = 'stage' }, @{ Damage = 'stage-flip' }, @{ Damage = 'role' }, @{ Damage = 'missing-desired' }
        @{ Damage = 'invalid-desired' }, @{ Damage = 'missing-host' }, @{ Damage = 'missing-model' }
        @{ Damage = 'wrong-marker' }, @{ Damage = 'invalid-id' }
    ) {
        $journal = Invoke-ScheduledSetupJournal $path 123 begin-create triage @{ operator_approved = $true; desired = $desired }
        $record = $journal.roles.triage
        switch ($Damage) {
            stage { $record.stage = 'unknown' }
            stage-flip { $record.stage = 'complete'; $record.automation_id = 'another-entry' }
            role { $journal.roles['unknown'] = $record; $journal.roles.Remove('triage') }
            missing-desired { $record.Remove('desired') }
            invalid-desired { $record.desired = 'invalid' }
            missing-host { $record.desired.Remove('host_id') }
            missing-model { $record.desired.Remove('model') }
            wrong-marker { $record.desired.marker = 'folo-scheduled-remediation:v1' }
            invalid-id { $record.stage = 'complete'; $record.automation_id = 123 }
        }
        if ($Damage -notin @('stage-flip', 'missing-desired')) {
            $record.digest = Get-ScheduledDigest @{ stage = $record.stage; desired = $record.desired; automation_id = $record.automation_id }
        }
        $journal | ConvertTo-Json -Depth 100 | Set-Content -LiteralPath $path
        $before = Get-Content -LiteralPath $path -Raw
        { Invoke-ScheduledSetupJournal $path 123 read } | Should -Throw
        { Get-ScheduledRoleSetupDecision -Role triage -Desired $desired -Workflows @() `
            -MetadataComplete $true -SetupJournal $journal } | Should -Throw
        (Get-Content -LiteralPath $path -Raw) | Should -BeExactly $before
    }

    It 'retains actual opaque native automation identifiers rather than imposing GitHub issue numbering' {
        $id = [guid]::NewGuid().ToString()
        $record = Invoke-ScheduledSetupJournal $path 123 confirm triage @{
            operator_approved = $true; ownership_verified = $true; automation_id = $id
        }
        $record.roles.triage.automation_id | Should -BeExactly $id
        (Invoke-ScheduledSetupJournal $path 123 read).roles.triage.automation_id | Should -BeExactly $id
    }

    It 'validates the separate repair creation model without enrolling or activating it' {
        $desired.marker = 'folo-scheduled-remediation:v1'
        $desired.prompt = 'folo-scheduled-remediation:v1 Run scheduled-intake'
        $desired.coordinator_model = 'existing-choice'
        $record = Invoke-ScheduledSetupJournal $path 123 begin-create repair @{
            operator_approved = $true; desired = $desired
        }
        $record.roles.repair.stage | Should -Be creating
        $record.roles.repair.desired.coordinator_model | Should -Be existing-choice
    }
}
