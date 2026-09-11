#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }

# Exercises event-pinned, always-fresh planning with no issue history or checker toolchain.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true

BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'ScheduledWorkflow.psm1') -Force
    $script:variables = @('GITHUB_REF', 'GITHUB_EVENT_NAME', 'GITHUB_SHA', 'GITHUB_OUTPUT', 'GITHUB_STEP_SUMMARY')
    $script:saved = @{}
    foreach ($key in $variables) { $saved[$key] = [Environment]::GetEnvironmentVariable($key) }
}
AfterAll {
    foreach ($key in $variables) { [Environment]::SetEnvironmentVariable($key, $saved[$key]) }
}

Describe 'Deep workflow planning' {
    BeforeEach {
        $env:GITHUB_REF = 'refs/heads/main'
        $env:GITHUB_EVENT_NAME = 'schedule'
        $env:GITHUB_SHA = 'a' * 40
        $env:GITHUB_OUTPUT = Join-Path $TestDrive 'outputs'
        $env:GITHUB_STEP_SUMMARY = Join-Path $TestDrive 'summary.md'
        $script:eventPath = Join-Path $TestDrive 'event.json'
        '{}' | Set-Content -LiteralPath $eventPath
        $script:output = Join-Path $TestDrive 'plan'
    }

    It 'runs the entire nightly suite again even when the commit is unchanged' {
        $first = Invoke-ScheduledPlanning -Mode full -EventPath $eventPath -OutputDirectory $output
        $second = Invoke-ScheduledPlanning -Mode full -EventPath $eventPath -OutputDirectory $output
        $first.checks.Count | Should -Be 32
        $second.checks.Count | Should -Be 32
        $second.source_sha | Should -Be $env:GITHUB_SHA
        @($second.Keys | Sort-Object) | Should -Be @('checks', 'controller_sha', 'source_sha')
        $persisted = Get-Content -LiteralPath (Join-Path $output 'plan.json') -Raw | ConvertFrom-Json -AsHashtable
        $persisted.source_sha | Should -Be $env:GITHUB_SHA
        (Get-Content -LiteralPath $env:GITHUB_OUTPUT -Raw) | Should -Match 'matrix='
    }

    It 'allows a manual full run at exact candidate bytes without changing the controller' {
        $env:GITHUB_EVENT_NAME = 'workflow_dispatch'
        @{ inputs = @{ source_sha = 'b' * 40 } } | ConvertTo-Json | Set-Content -LiteralPath $eventPath
        $plan = Invoke-ScheduledPlanning -Mode full -EventPath $eventPath -OutputDirectory $output
        $plan.source_sha | Should -Be ('b' * 40)
        $plan.controller_sha | Should -Be ('a' * 40)
        $plan.checks.Count | Should -Be 32
    }

    It 'selects exact checks and packages for a manual run' {
        $env:GITHUB_EVENT_NAME = 'workflow_dispatch'
        @{ inputs = @{ source_sha = ''; check_ids = 'miri-ubuntu-latest, careful-windows-latest'; packages = 'cpulist, many_cpus' } } |
            ConvertTo-Json | Set-Content -LiteralPath $eventPath
        $plan = Invoke-ScheduledPlanning -Mode selected -EventPath $eventPath -OutputDirectory $output
        $plan.source_sha | Should -Be ('a' * 40)
        $plan.checks.Count | Should -Be 2
        $plan.checks[0].packages | Should -Be @('cpulist', 'many_cpus')
    }

    It 'rejects a non-main controller' {
        $env:GITHUB_REF = 'refs/heads/feature'
        { Invoke-ScheduledPlanning -Mode full -EventPath $eventPath -OutputDirectory $output } | Should -Throw
    }

    It 'does not run selected checks on a main push' {
        $env:GITHUB_EVENT_NAME = 'push'
        { Invoke-ScheduledPlanning -Mode selected -EventPath $eventPath -OutputDirectory $output } | Should -Throw
    }

    It 'rejects malformed SHA and empty selections' -ForEach @(
        @{ inputs = @{ source_sha = 'main'; check_ids = 'miri-ubuntu-latest'; packages = 'cpulist' } }
        @{ inputs = @{ source_sha = 'a' * 39; check_ids = 'miri-ubuntu-latest'; packages = 'cpulist' } }
        @{ inputs = @{ check_ids = ''; packages = 'cpulist' } }
        @{ inputs = @{ check_ids = 'miri-ubuntu-latest'; packages = '' } }
        @{ inputs = @{ check_ids = 'miri-ubuntu-latest,'; packages = 'cpulist' } }
        @{ inputs = @{ check_ids = 'miri-ubuntu-latest'; packages = 'cpulist,' } }
    ) {
        $env:GITHUB_EVENT_NAME = 'workflow_dispatch'
        @{ inputs = $inputs } | ConvertTo-Json | Set-Content -LiteralPath $eventPath
        { Invoke-ScheduledPlanning -Mode selected -EventPath $eventPath -OutputDirectory $output } | Should -Throw
    }
}
