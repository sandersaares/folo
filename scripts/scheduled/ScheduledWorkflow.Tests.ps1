#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'ScheduledWorkflow.psm1') -Force
}
Describe 'Repair gate orchestration' {
    It 'rejects failed context before treating an ordinary PR as cheap success' {
        { Invoke-ScheduledGate -PlanPath absent -ResultsDirectory absent -ContextResult failure `
                -DeepResult skipped -RunId 1 -RunAttempt 1 } | Should -Throw
    }
    It 'passes ordinary PRs without deep artifacts' {
        $path = Join-Path $TestDrive 'ordinary.json'
        @{ managed = $false } | ConvertTo-Json | Set-Content $path
        { Invoke-ScheduledGate -PlanPath $path -ResultsDirectory absent -ContextResult success `
                -DeepResult skipped -RunId 1 -RunAttempt 1 } | Should -Not -Throw
    }
    It 'rejects missing skipped cancelled and failed relevant execution' {
        $path = Join-Path $TestDrive 'managed.json'
        @{ managed = $true } | ConvertTo-Json | Set-Content $path
        foreach ($result in @('skipped', 'cancelled', 'failure', '')) {
            { Invoke-ScheduledGate -PlanPath $path -ResultsDirectory absent -ContextResult success `
                    -DeepResult $result -RunId 1 -RunAttempt 1 } | Should -Throw
        }
    }
    It 'retains existing local deep execution until reviewed cutover' {
        InModuleScope ScheduledWorkflow {
            Mock just {}
            Mock Get-ScheduledPolicy { @{ rollout = @{ cutover = $false } } }
            Invoke-ScheduledLocalDeepCheck -Kind miri -Packages events_once
            Should -Invoke just -Times 1 -Exactly -ParameterFilter { $args[0] -eq 'package=events_once' -and $args[1] -eq 'miri' }
        }
    }
    It 'does not duplicate local deep execution after cutover' {
        InModuleScope ScheduledWorkflow {
            Mock just {}
            Mock Get-ScheduledPolicy { @{ rollout = @{ cutover = $true } } }
            Invoke-ScheduledLocalDeepCheck -Kind mutants -Packages cpulist
            Should -Invoke just -Times 0 -Exactly
        }
    }
}
