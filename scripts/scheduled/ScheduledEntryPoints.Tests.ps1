#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
# Protects the entrypoint scripts' own contract with their calling workflow: launched as real
# `pwsh` processes (not dot-sourced) against stub modules, so a mistake in exit-code mapping or
# output-file writing - invisible to a module-level Pester test that only calls functions in
# process - still fails here.
BeforeAll {
    $script:entryRoot = Join-Path $TestDrive 'entrypoints'
    New-Item -ItemType Directory -Path $entryRoot | Out-Null
    Copy-Item -LiteralPath (Join-Path $PSScriptRoot 'Invoke-ScheduledCheck.ps1'),
        (Join-Path $PSScriptRoot 'Invoke-ScheduledPlan.ps1'),
        (Join-Path $PSScriptRoot 'Invoke-ScheduledHealth.ps1'),
        (Join-Path $PSScriptRoot 'Invoke-ScheduledReport.ps1') -Destination $entryRoot
    @'
function Invoke-ScheduledCheck {
    param($Check, $SourceRoot, $OutputDirectory, $Toolchain, $RunContext)
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
    @{ mode = $Mode; event_path = $EventPath; output_directory = $OutputDirectory } | ConvertTo-Json -Compress
}
Export-ModuleMember -Function Invoke-ScheduledPlanning
'@ | Set-Content -LiteralPath (Join-Path $entryRoot 'ScheduledWorkflow.psm1')

    function Invoke-EntryPointFixture {
        param([string] $Name, [string] $Outcome, [string[]] $Arguments = @())
        $start = [Diagnostics.ProcessStartInfo]::new()
        $start.FileName = (Get-Command pwsh).Source
        $start.UseShellExecute = $false
        $start.RedirectStandardOutput = $true
        $start.RedirectStandardError = $true
        $start.Environment['SCHEDULED_TEST_OUTCOME'] = $Outcome
        $start.Environment['SCHEDULED_CHECK'] = '{"kind":"miri"}'
        $start.Environment['SCHEDULED_MANIFEST'] = '{"source_sha":"source","controller_sha":"controller","check_contract_digest":"digest"}'
        $start.Environment['GITHUB_STEP_SUMMARY'] = Join-Path $entryRoot "summary-$Outcome.md"
        foreach ($argument in @('-NoProfile', '-File', (Join-Path $entryRoot $Name)) + $Arguments) {
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
    It 'invokes the planner without extra permission switches for <Mode>' -TestCases @(
        @{ Mode = 'verify' }, @{ Mode = 'scheduled' }, @{ Mode = 'validation' }
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
