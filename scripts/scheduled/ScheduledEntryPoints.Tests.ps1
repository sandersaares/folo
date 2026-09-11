#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }

# Exercises the real check entrypoint and Just recipe as native processes against a fixture
# checker. This catches a dropped nonzero exit across either process boundary.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true

BeforeAll {
    $script:fixtureRoot = Join-Path $TestDrive 'entrypoint'
    $script:entryRoot = Join-Path $fixtureRoot 'scripts\scheduled'
    $null = New-Item -ItemType Directory -Path $entryRoot -Force
    Copy-Item -LiteralPath (Join-Path $PSScriptRoot 'Invoke-ScheduledCheck.ps1') -Destination $entryRoot
    Copy-Item -LiteralPath (Join-Path $PSScriptRoot 'fixtures\execution\CheckExit.psm1') `
        -Destination (Join-Path $entryRoot 'ScheduledExecution.psm1')
    # Import only the real scheduled recipes: unrelated recipe imports have their own tests.
    $recipes = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..\..\justfiles\just_scheduled.just')).Replace('\', '/')
    "set unstable`nset script-interpreter := ['pwsh', '-NoLogo', '-NoProfile', '-File']`nimport '$recipes'" |
        Set-Content -LiteralPath (Join-Path $fixtureRoot 'justfile')
}

Describe 'Scheduled check process exit' {
    It 'propagates <code> through <entry>' -ForEach @(
        @{ code = 0; entry = 'script' }, @{ code = 1; entry = 'script' },
        @{ code = 2; entry = 'script' }, @{ code = 3; entry = 'script' },
        # Windows native failures can be signed; they must still cross both wrappers as failures.
        @{ code = -1073741819; entry = 'script' },
        @{ code = 0; entry = 'just' }, @{ code = 2; entry = 'just' }
    ) {
        $start = [Diagnostics.ProcessStartInfo]::new()
        $start.FileName = (Get-Command pwsh).Source
        $start.WorkingDirectory = $fixtureRoot
        $start.UseShellExecute = $false
        $start.RedirectStandardOutput = $true
        $start.RedirectStandardError = $true
        $start.Environment['SCHEDULED_TEST_EXIT'] = [string]$code
        $start.Environment['SCHEDULED_CHECK'] = '{"id":"fixture"}'
        $start.Environment['SCHEDULED_SOURCE_SHA'] = 'a' * 40
        $arguments = if ($entry -eq 'script') {
            @('-NoProfile', '-File', (Join-Path $entryRoot 'Invoke-ScheduledCheck.ps1'))
        } else { @('-NoProfile', '-Command', 'just scheduled-check; exit $LASTEXITCODE') }
        foreach ($argument in $arguments) { $start.ArgumentList.Add($argument) }
        $process = [Diagnostics.Process]::new()
        $process.StartInfo = $start
        try {
            $null = $process.Start()
            $stdout = $process.StandardOutput.ReadToEndAsync()
            $stderr = $process.StandardError.ReadToEndAsync()
            $process.WaitForExit()
            $diagnostic = $stdout.GetAwaiter().GetResult() + $stderr.GetAwaiter().GetResult()
            if ($code -eq 0) { $process.ExitCode | Should -Be 0 -Because $diagnostic }
            else { $process.ExitCode | Should -Not -Be 0 -Because $diagnostic }
        } finally { $process.Dispose() }
    }
}
