#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
# Protects the entrypoint scripts' own contract with their calling workflow: launched as real
# `pwsh` processes, directly and through the actual Just recipes, against stub modules.
# Exit-code mapping or output-file mistakes invisible to an in-process module test still fail here.
BeforeAll {
    $script:fixtureRoot = Join-Path $TestDrive 'entrypoints'
    $script:entryRoot = Join-Path $fixtureRoot 'scripts\scheduled'
    New-Item -ItemType Directory -Path $entryRoot -Force | Out-Null
    $repository = Join-Path $PSScriptRoot '..\..'
    Copy-Item -LiteralPath (Join-Path $repository 'justfile'), (Join-Path $repository 'constants.env') -Destination $fixtureRoot
    Copy-Item -LiteralPath (Join-Path $repository 'justfiles') -Destination $fixtureRoot -Recurse
    Copy-Item -LiteralPath (Join-Path $PSScriptRoot 'Invoke-ScheduledCheck.ps1'),
        (Join-Path $PSScriptRoot 'ScheduledJson.psm1'),
        (Join-Path $PSScriptRoot 'Invoke-ScheduledPlan.ps1'),
        (Join-Path $PSScriptRoot 'Invoke-ScheduledHealth.ps1'),
        (Join-Path $PSScriptRoot 'Invoke-ScheduledReport.ps1') -Destination $entryRoot
    @'
function Invoke-ScheduledCheck {
    param($Check, $SourceRoot, $OutputDirectory, $Toolchain, $RunContext)
    $null = New-Item -ItemType Directory -Path $OutputDirectory -Force
    @{ outcome = $env:SCHEDULED_TEST_OUTCOME } | ConvertTo-Json |
        Set-Content -LiteralPath (Join-Path $OutputDirectory 'evidence.json')
    return @{ outcome = $env:SCHEDULED_TEST_OUTCOME }
}
function Get-ScheduledToolchain { param($Kind) return 'fixture-toolchain' }
Export-ModuleMember -Function Invoke-ScheduledCheck, Get-ScheduledToolchain
'@ | Set-Content -LiteralPath (Join-Path $entryRoot 'ScheduledExecution.psm1')
    @'
function Get-ScheduledGitHubHealth {
    param($Repository, $Now)
    return @{
        status = $env:SCHEDULED_TEST_OUTCOME
        components = @{ coverage = @{ status = 'unavailable' } }
    }
}
function Invoke-ScheduledReporting {
    param($Repository, $EventPath, $OutputDirectory, [switch] $Apply)
    return @{ status = $env:SCHEDULED_TEST_OUTCOME }
}
Export-ModuleMember -Function Get-ScheduledGitHubHealth, Invoke-ScheduledReporting
'@ | Set-Content -LiteralPath (Join-Path $entryRoot 'ScheduledGitHub.psm1')
    @'
function Invoke-ScheduledPlanning {
    param($Mode, $EventPath, $OutputDirectory)
    if ($env:SCHEDULED_TEST_OUTCOME -eq 'incomplete') { throw 'Planner failure canary.' }
    @{ mode = $Mode; event_path = $EventPath; output_directory = $OutputDirectory } | ConvertTo-Json -Compress
}
Export-ModuleMember -Function Invoke-ScheduledPlanning
'@ | Set-Content -LiteralPath (Join-Path $entryRoot 'ScheduledWorkflow.psm1')

    function Invoke-EntryPointFixture {
        param([string] $Name, [string] $Outcome, [string[]] $Arguments = @(), [string] $Recipe = '')
        $start = [Diagnostics.ProcessStartInfo]::new()
        $start.FileName = (Get-Command pwsh).Source
        $start.UseShellExecute = $false
        $start.RedirectStandardOutput = $true
        $start.RedirectStandardError = $true
        $start.WorkingDirectory = $fixtureRoot
        $start.Environment['SCHEDULED_TEST_OUTCOME'] = $Outcome
        $start.Environment['SCHEDULED_CHECK'] = '{"kind":"miri"}'
        $start.Environment['SCHEDULED_MANIFEST'] = '{"source_sha":"source","controller_sha":"controller","check_contract_digest":"digest"}'
        $start.Environment['GITHUB_STEP_SUMMARY'] = Join-Path $entryRoot "summary-$Outcome.md"
        $invocation = if ($Recipe -eq '') { @('-File', (Join-Path $entryRoot $Name)) + $Arguments }
            else {
                # The workflow's pwsh step invokes Just, whose [script] recipe invokes the
                # entrypoint. Keep both process boundaries, not a rewritten recipe stand-in.
                @('-Command', "Set-StrictMode -Version Latest; `$ErrorActionPreference = 'Stop'; " +
                    "`$PSNativeCommandUseErrorActionPreference = `$true; just $Recipe; exit `$LASTEXITCODE")
            }
        foreach ($argument in @('-NoProfile') + $invocation) {
            $start.ArgumentList.Add($argument)
        }
        $process = [Diagnostics.Process]::new()
        $process.StartInfo = $start
        try {
            $null = $process.Start()
            $stdout = $process.StandardOutput.ReadToEndAsync()
            $stderr = $process.StandardError.ReadToEndAsync()
            $process.WaitForExit()
            return @{
                code = $process.ExitCode
                stdout = $stdout.GetAwaiter().GetResult()
                stderr = $stderr.GetAwaiter().GetResult()
            }
        } finally { $process.Dispose() }
    }
}

Describe 'Hosted entrypoint exit contracts' {
    It 'propagates <Outcome> through workflow PowerShell and the actual scheduled-check recipe' -TestCases @(
        @{ Outcome = 'passed'; Code = 0 }
        @{ Outcome = 'findings'; Code = 1 }
        @{ Outcome = 'incomplete'; Code = 1 }
        @{ Outcome = 'blocked'; Code = 1 }
        @{ Outcome = 'execution-error'; Code = 1 }
        @{ Outcome = 'not-applicable'; Code = 1 }
    ) {
        param($Outcome, $Code)
        $result = Invoke-EntryPointFixture -Recipe scheduled-check -Outcome $Outcome
        $result.code | Should -Be $Code
        $result.stdout | Should -Match ('"outcome": "' + $Outcome + '"')
        $evidence = Get-Content -LiteralPath (Join-Path $fixtureRoot '.scheduled-result\evidence.json') -Raw |
            ConvertFrom-Json
        $evidence.outcome | Should -Be $Outcome
    }
    It 'propagates reporting status <Outcome> through the actual scheduled-report recipe' -TestCases @(
        @{ Outcome = 'reported'; Code = 0 }
        @{ Outcome = 'incomplete'; Code = 1 }
    ) {
        param($Outcome, $Code)
        (Invoke-EntryPointFixture -Recipe scheduled-report -Outcome $Outcome).code | Should -Be $Code
    }
    It 'propagates planner status <Outcome> through the actual scheduled-plan recipe' -TestCases @(
        @{ Outcome = 'planned'; Code = 0 }
        @{ Outcome = 'incomplete'; Code = 1 }
    ) {
        param($Outcome, $Code)
        (Invoke-EntryPointFixture -Recipe scheduled-plan -Outcome $Outcome).code | Should -Be $Code
    }
    It 'invokes the planner without extra permission switches for <Mode>' -TestCases @(
        @{ Mode = 'selected' }, @{ Mode = 'full' }, @{ Mode = 'validation' }
    ) {
        param($Mode)
        $result = Invoke-EntryPointFixture -Name 'Invoke-ScheduledPlan.ps1' -Outcome $Mode `
            -Arguments @('-Mode', $Mode, '-EventPath', 'event.json', '-OutputDirectory', 'output')
        $result.code | Should -Be 0
        $result.stderr | Should -BeNullOrEmpty
        $result.stdout | Should -Match ('"mode":"' + $Mode + '"')
        $result.stdout | Should -Match '"event_path":"event.json"'
        $result.stdout | Should -Match '"output_directory":"output"'
    }

    It 'distinguishes captured execution failures from reporting failure: <Outcome>' -TestCases @(
        @{ Outcome = 'passed'; Code = 0 }
        @{ Outcome = 'not-run'; Code = 0 }
        @{ Outcome = 'reported'; Code = 0 }
        @{ Outcome = 'incomplete'; Code = 1 }
    ) {
        param($Outcome, $Code)
        $result = Invoke-EntryPointFixture -Name 'Invoke-ScheduledReport.ps1' -Outcome $Outcome `
            -Arguments @('-Repository', 'folo-rs/folo', '-EventPath', 'event.json')
        $result.code | Should -Be $Code
        $result.stderr | Should -BeNullOrEmpty
    }
    It 'exits correctly for check outcome <Outcome>' -TestCases @(
        @{ Outcome = 'passed'; Code = 0 }
        @{ Outcome = 'findings'; Code = 1 }
        @{ Outcome = 'incomplete'; Code = 1 }
        @{ Outcome = 'blocked'; Code = 1 }
        @{ Outcome = 'execution-error'; Code = 1 }
        @{ Outcome = 'not-applicable'; Code = 1 }
    ) {
        param($Outcome, $Code)
        $result = Invoke-EntryPointFixture -Name 'Invoke-ScheduledCheck.ps1' -Outcome $Outcome
        $result.stderr | Should -BeNullOrEmpty
        $result.code | Should -Be $Code
        $result.stdout | Should -Match ('"outcome": "' + [regex]::Escape($Outcome) + '"')
    }
    It 'persists health evidence and summary even when health is failed' {
        $output = Join-Path $entryRoot 'health'
        $result = Invoke-EntryPointFixture -Name 'Invoke-ScheduledHealth.ps1' -Outcome failed `
            -Arguments @('-Repository', 'folo-rs/folo', '-Now', '2026-09-08T12:00:00Z', '-OutputDirectory', $output)
        $result.stderr | Should -BeNullOrEmpty
        $result.code | Should -Be 1
        (Get-Content -LiteralPath (Join-Path $output 'health.json') -Raw | ConvertFrom-Json).status | Should -Be failed
        Get-Content -LiteralPath (Join-Path $entryRoot 'summary-failed.md') -Raw | Should -Match 'coverage \| unavailable'
    }
}
