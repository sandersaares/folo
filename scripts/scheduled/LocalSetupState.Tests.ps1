#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
# Proves native setup's uncertain-create fence survives repeated setup without creating an
# enrollment directory or resetting either role's work. All files are isolated under TestDrive.
BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'LocalSetupState.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'LocalSetup.psm1') -Force
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
}
