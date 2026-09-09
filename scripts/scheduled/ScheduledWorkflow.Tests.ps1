#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
# Protects planning/gate orchestration: the deep-checks matrix and managed/confirmation decision
# must reflect the triggering event correctly, coverage reuse must never mask a stale or forced
# rerun, and the reporting workflow's queue-without-replacement concurrency contract this module
# relies on must not silently regress.
BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'ScheduledWorkflow.psm1') -Force
}
Describe 'Repair gate orchestration' {
    It 'serializes reporting without replacing pending events' {
        $root = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
        $workflow = Get-Content -LiteralPath (Join-Path $root '.github\workflows\scheduled-report.yml') -Raw
        $workflow | Should -Match '(?m)^    branches: \[main\]\r?$'
        $concurrency = [regex]::Match($workflow, '(?m)^concurrency:\r?\n(?:[ \t]+[^\r\n]*\r?\n)+').Value
        $concurrency | Should -Match '(?m)^  group: scheduled-reporting\r?$'
        $concurrency | Should -Match '(?m)^  cancel-in-progress: false\r?$'
        $concurrency | Should -Match '(?m)^  queue: max\r?$'
        @([regex]::Matches($workflow, '(?m)^[ \t]*queue:')).Count | Should -Be 1
    }
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

    Describe 'Coverage reporting lag' {
        It 'does not reuse success across <Reason>' -TestCases @(
            @{ Status = 'completed'; Conclusion = 'failure'; Attempt = 1; Id = 20; Reason = 'unreported-validation-failure' }
            @{ Status = 'in_progress'; Conclusion = $null; Attempt = 1; Id = 20; Reason = 'unsettled-validation-run' }
            @{ Status = 'completed'; Conclusion = 'success'; Attempt = 2; Id = 10; Reason = 'coverage-run-reattempted' }
        ) {
            param($Status, $Conclusion, $Attempt, $Id, $Reason)
            InModuleScope ScheduledWorkflow -Parameters @{
                Status = $Status; Conclusion = $Conclusion; Attempt = $Attempt; Id = $Id; Reason = $Reason
            } {
                param($Status, $Conclusion, $Attempt, $Id, $Reason)
                $execution = @{
                    id = $Id; run_attempt = $Attempt; head_sha = 'a' * 40; head_branch = 'main'
                    status = $Status; conclusion = $Conclusion; updated_at = '2026-09-08T11:00:00Z'
                }
                Mock Invoke-ScheduledReadApi { @(@{ workflow_runs = @($execution) }) }
                Get-ScheduledCoverageRunRisk -Policy @{ repository = 'folo-rs/folo' } `
                    -Receipt @{ run_id = 10; run_attempt = 1; source_sha = 'a' * 40; completed_at = '2026-09-08T10:00:00Z' } `
                    -CurrentRunId 30 -CurrentRunAttempt 1 | Should -Be $Reason
            }
        }

        It 'ignores its own planning run without refreshing or invalidating prior coverage' {
            InModuleScope ScheduledWorkflow {
                Mock Invoke-ScheduledReadApi { @(@{ workflow_runs = @(@{ id = 30 }) }) }
                Get-ScheduledCoverageRunRisk -Policy @{ repository = 'folo-rs/folo' } `
                    -Receipt @{ run_id = 10; run_attempt = 1; source_sha = 'a' * 40; completed_at = '2026-09-08T10:00:00Z' } `
                    -CurrentRunId 30 -CurrentRunAttempt 1 | Should -BeNullOrEmpty
                Should -Invoke Invoke-ScheduledReadApi -Times 2 -Exactly
            }
        }

        It 'forces a rerun of the coverage-producing workflow even before querying its own attempt' {
            InModuleScope ScheduledWorkflow {
                Mock Invoke-ScheduledReadApi { throw 'No API read needed' }
                Get-ScheduledCoverageRunRisk -Policy @{ repository = 'folo-rs/folo' } `
                    -Receipt @{ run_id = 10; run_attempt = 1 } -CurrentRunId 10 -CurrentRunAttempt 2 |
                    Should -Be 'coverage-run-reattempted'
            }
        }
    }
    It 'keeps content-reader callbacks in their defining module when only the workflow is imported' {
        $root = Join-Path $TestDrive 'callback-workspace'
        New-Item -ItemType Directory -Path (Join-Path $root 'packages/sample') -Force | Out-Null
        '[package]', 'name = "sample"', 'version = "0.1.0"' |
            Set-Content (Join-Path $root 'packages/sample/Cargo.toml')
        InModuleScope ScheduledWorkflow -Parameters @{ Root = $root } {
            param($Root)
            Mock gh -ModuleName ScheduledGate {
                $endpoint = $args[1]
                if ($endpoint -like '*/files?*') {
                    return '[[{"filename":"Cargo.toml","status":"modified"}]]'
                }
                $version = if ($endpoint -like '*ref=base') { '0.1.0' } else { '0.1.1' }
                return @{ content = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes(
                            "sample = { version = `"=$version`", path = `"packages/sample`" }")) } | ConvertTo-Json -Compress
            }
            Assert-ScheduledPullRequestChange -Root $Root -Policy @{ repository = 'folo-rs/folo' } `
                -PullRequest @{ number = 42; base = @{ sha = 'base' }; head = @{ sha = 'head' } } `
                -Scope @{ packages = @('sample') }
            Should -Invoke gh -ModuleName ScheduledGate -Times 3 -Exactly
        }
    }
}
