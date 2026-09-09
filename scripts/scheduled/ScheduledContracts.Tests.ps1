#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
# Protects the wire format shared by every scheduled module: canonical digests must be stable and
# comment-terminator-safe when embedded in GitHub issue/PR bodies, and record/policy validation
# must reject malformed input rather than silently coercing it into a usable record.
BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'ScheduledContracts.psm1') -Force
}

Describe 'Scheduled records' {
    It 'round trips evidence without allowing a comment terminator' {
        $record = @{ schema_version = 1; summary = 'x --> y'; nullable = $null; list = @('seed') }
        $marker = Write-ScheduledRecord -Kind reporter -Record $record
        $marker | Should -Not -Match 'x -->'
        $parsed = Read-ScheduledRecord -Kind reporter -Text "Human introduction`n$marker`nHuman conclusion"
        $parsed.summary | Should -Be $record.summary
        $parsed.list.Count | Should -Be 1
    }
    It 'rejects absent duplicate and incompatible records' {
        $marker = Write-ScheduledRecord -Kind worker -Record @{ schema_version = 1 }
        { Read-ScheduledRecord -Kind worker -Text '' } | Should -Throw
        { Read-ScheduledRecord -Kind worker -Text "$marker`n$marker" } | Should -Throw
        { Read-ScheduledRecord -Kind worker -Text '<!-- scheduled-worker:v1 {"schema_version":2} -->' } | Should -Throw
    }
    It 'canonicalizes maps but retains case and array ordering' {
        (Get-ScheduledDigest @{ b = 2; a = @{ d = 4; c = 3 } }) |
            Should -Be (Get-ScheduledDigest @{ a = @{ c = 3; d = 4 }; b = 2 })
        (Get-ScheduledDigest @('a', 'b')) | Should -Not -Be (Get-ScheduledDigest @('b', 'a'))
        (Get-ScheduledDigest 'Path') | Should -Not -Be (Get-ScheduledDigest 'path')
    }
    It 'keeps hosted execution reporting and Local admission disabled in staged policy' {
        $policy = Get-ScheduledPolicy
        $policy.rollout.hosted_execution_enabled | Should -BeFalse
        $policy.rollout.reporting_enabled | Should -BeFalse
        $policy.local.mode | Should -Be observe
        $policy.local.enrolled_machine_id | Should -BeNullOrEmpty
    }
    It 'validates hosted authorization switches independently' -TestCases @(
        @{ Execution = $false; Reporting = $false }
        @{ Execution = $true; Reporting = $false }
        @{ Execution = $false; Reporting = $true }
        @{ Execution = $true; Reporting = $true }
    ) {
        param($Execution, $Reporting)
        $policy = Get-ScheduledPolicy
        $policy.rollout.hosted_execution_enabled = $Execution
        $policy.rollout.reporting_enabled = $Reporting
        $path = Join-Path $TestDrive 'policy.json'
        $policy | ConvertTo-Json -Depth 20 | Set-Content $path
        $actual = Get-ScheduledPolicy -Path $path
        $actual.rollout.hosted_execution_enabled | Should -Be $Execution
        $actual.rollout.reporting_enabled | Should -Be $Reporting
    }
    It 'rejects a missing or non-boolean <Name> authorization switch' -TestCases @(
        @{ Name = 'hosted_execution_enabled' }
        @{ Name = 'reporting_enabled' }
    ) {
        param($Name)
        $policy = Get-ScheduledPolicy
        $path = Join-Path $TestDrive 'policy.json'
        $policy.rollout.Remove($Name)
        $policy | ConvertTo-Json -Depth 20 | Set-Content $path
        { Get-ScheduledPolicy -Path $path } | Should -Throw
        $policy.rollout[$Name] = 'true'
        $policy | ConvertTo-Json -Depth 20 | Set-Content $path
        { Get-ScheduledPolicy -Path $path } | Should -Throw
    }
    It 'still requires typed <Name> operator verification' -TestCases @(
        @{ Name = 'execution_canary' }
        @{ Name = 'reporting_canary' }
        @{ Name = 'native_app_canary' }
        @{ Name = 'benchmark_exclusion' }
        @{ Name = 'azure_policy' }
    ) {
        param($Name)
        $policy = Get-ScheduledPolicy
        $policy.rollout.prerequisites.Remove($Name)
        $path = Join-Path $TestDrive 'policy.json'
        $policy | ConvertTo-Json -Depth 20 | Set-Content $path
        { Get-ScheduledPolicy -Path $path } | Should -Throw
        $policy.rollout.prerequisites[$Name] = 'true'
        $policy | ConvertTo-Json -Depth 20 | Set-Content $path
        { Get-ScheduledPolicy -Path $path } | Should -Throw
    }
}
