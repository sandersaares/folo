#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
# Protects fail-closed role-health projection: absent or malformed blocker inventories are
# unavailable observations, not empty healthy scans. Both roles cross the real record codec.
BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'ScheduledRoleHealth.psm1')
    Import-Module (Join-Path $PSScriptRoot 'ScheduledContracts.psm1')
    Import-Module (Join-Path $PSScriptRoot 'LocalTriagePolicy.psm1')
    Import-Module (Join-Path $PSScriptRoot 'LocalTriageProfile.psm1')

    function Invoke-HealthProjection {
        param($Record, [string] $Role)
        $comments = @(@{ user = @{ login = $policy.worker_login }
            body = Write-ScheduledRecord -Record $Record -Kind health })
        Get-ScheduledRoleScan $policy $triagePolicy $comments $Role
    }
}

Describe 'Complete <Role> health observations' -ForEach @(@{ Role = 'repair' }, @{ Role = 'triage' }) {
    BeforeEach {
        $script:policy = Get-ScheduledPolicy
        $policy.local.enrolled_machine_id = 'executor'
        $script:triagePolicy = Get-ScheduledTriagePolicy
        $triagePolicy.enrolled_machine_id = 'executor'
        $script:record = @{
            schema_version = 1; role = $Role; repository = $policy.repository; repository_id = $policy.repository_id
            executor_id = 'executor'; last_successful_scan = '2026-09-10T12:00:00Z'; blocked_conditions = @()
            profile = @{ policy_digest = Get-ScheduledTriagePolicyDigest $policy $triagePolicy
                cadence_cron = $triagePolicy.cadence_cron; controller_digest = Get-ScheduledTriageControllerDigest
                automation_id = 'entry'; prompt_digest = Get-ScheduledTriagePromptDigest 'Native prompt' }
            profile_scan = @{ binding_digest = Get-ScheduledTriageProfileBindingDigest scan 'scan-token' 'session'; session_id = 'session' }
        }
        $observation = Get-ScheduledTriageProfileObservation @{
            automation_id = 'entry'; prompt_digest = $record.profile.prompt_digest
        } scan 'scan-token' 'session'
        $record.profile_observation = Get-ScheduledTriageHealthObservation $observation
    }

    It 'distinguishes valid empty and nonempty blocker inventories' {
        $healthy = Invoke-HealthProjection $record $Role
        $healthy.outcome | Should -Be passed
        $healthy.blocked_conditions.Count | Should -Be 0
        $record.blocked_conditions = @('evidence-unavailable')
        $blocked = Invoke-HealthProjection $record $Role
        $blocked.outcome | Should -Be failed
        $blocked.blocked_conditions | Should -Be @('evidence-unavailable')
    }

    It 'rejects missing and malformed inventories instead of inferring successful observation' {
        $record.Remove('blocked_conditions')
        { Invoke-HealthProjection $record $Role } | Should -Throw -ExceptionType ([FormatException])
        foreach ($invalid in @(@{ value = $null }, @{ value = 'not-an-inventory' },
                @{ value = @($null) }, @{ value = @('') })) {
            $record.blocked_conditions = $invalid.value
            { Invoke-HealthProjection $record $Role } | Should -Throw -ExceptionType ([FormatException])
        }
    }

    It 'rejects malformed triage profile objects through the structured health boundary without affecting repair' {
        foreach ($field in @('profile', 'profile_scan', 'profile_observation')) {
            $original = $record[$field]
            foreach ($invalid in @(@{ value = 'not-an-object' }, @{ value = 42 }, @{ value = @('not-an-object') })) {
                $record[$field] = $invalid.value
                if ($Role -ceq 'triage') {
                    { Invoke-HealthProjection $record $Role } | Should -Throw -ExceptionType ([FormatException])
                } else {
                    (Invoke-HealthProjection $record $Role).outcome | Should -Be passed
                }
            }
            $record[$field] = $original
        }
    }
}

Describe 'Legacy repair health observation' {
    It 'retains the existing completed-at contract without inventing modern fields' {
        $script:policy = Get-ScheduledPolicy
        $policy.local.enrolled_machine_id = 'executor'
        $script:triagePolicy = Get-ScheduledTriagePolicy
        $record = @{ schema_version = 1; repository = $policy.repository; repository_id = $policy.repository_id
            executor_id = 'executor'; completed_at = '2026-09-10T12:00:00Z'; outcome = 'passed' }
        $result = Invoke-HealthProjection $record repair
        (Get-ScheduledDigest $result) | Should -BeExactly (Get-ScheduledDigest $record)
    }
}
