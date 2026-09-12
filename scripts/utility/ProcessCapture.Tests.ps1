#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }

# Tests generic native argument/exit transport and cleanup when output capture fails.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true

BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'ProcessCapture.psm1') -Force
    $script:echoFixture = Join-Path $PSScriptRoot 'fixtures\Echo-Argument.ps1'
}

Describe 'Native process capture' {
    It 'passes literal arguments through a real process without interpreting shell syntax' {
        $values = @('spaces and quotes "here"', 'literal*', 'x; Write-Host injected', 'café')
        $stdout = Join-Path $TestDrive 'arguments.stdout'
        $stderr = Join-Path $TestDrive 'arguments.stderr'
        Invoke-CapturedProcess -FilePath (Get-Command pwsh).Source `
            -ArgumentList (@('-NoProfile', '-File', $echoFixture) + $values) `
            -WorkingDirectory $TestDrive -StandardOutputPath $stdout -StandardErrorPath $stderr |
            Should -Be 0
        @(Get-Content -LiteralPath $stdout -Raw | ConvertFrom-Json) | Should -Be $values
    }

    It 'preserves an unsuccessful native exit code' {
        Invoke-CapturedProcess -FilePath (Get-Command pwsh).Source `
            -ArgumentList @('-NoProfile', '-Command', 'exit 17') -WorkingDirectory $TestDrive `
            -StandardOutputPath (Join-Path $TestDrive 'exit.stdout') `
            -StandardErrorPath (Join-Path $TestDrive 'exit.stderr') | Should -Be 17
    }

    It 'terminates the child and disposes captures when an output copy fails before exit' {
        $faultingStream = [pscustomobject]@{}
        $faultingStream | Add-Member ScriptMethod CopyToAsync {
            param($Destination)
            $Destination.CanWrite | Should -BeTrue
            return [Threading.Tasks.Task]::FromException([IO.IOException]::new('capture failure canary'))
        }
        $pendingStream = [pscustomobject]@{}
        $pendingStream | Add-Member ScriptMethod CopyToAsync {
            param($Destination)
            $Destination.CanWrite | Should -BeTrue
            return [Threading.Tasks.TaskCompletionSource[object]]::new().Task
        }
        $process = [pscustomobject]@{
            StartInfo = $null; HasExited = $false; Killed = $false; Waited = $false; Disposed = $false
            StandardOutput = [pscustomobject]@{ BaseStream = $faultingStream }
            StandardError = [pscustomobject]@{ BaseStream = $pendingStream }
        }
        $process | Add-Member ScriptMethod Start { return $true }
        $process | Add-Member ScriptMethod WaitForExitAsync {
            return [Threading.Tasks.TaskCompletionSource[object]]::new().Task
        }
        $process | Add-Member ScriptMethod Kill {
            param([bool] $EntireProcessTree)
            $this.Killed = $EntireProcessTree
            $this.HasExited = $true
        }
        $process | Add-Member ScriptMethod WaitForExit { $this.Waited = $true }
        $process | Add-Member ScriptMethod Dispose { $this.Disposed = $true }
        Mock Get-CaptureProcess -ModuleName ProcessCapture { return $process }
        {
            Invoke-CapturedProcess -FilePath unused -ArgumentList @() -WorkingDirectory $TestDrive `
                -StandardOutputPath (Join-Path $TestDrive 'capture.stdout') `
                -StandardErrorPath (Join-Path $TestDrive 'capture.stderr')
        } | Should -Throw '*capture failure canary*'
        $process.Killed | Should -BeTrue
        $process.Waited | Should -BeTrue
        $process.Disposed | Should -BeTrue
        foreach ($name in @('capture.stdout', 'capture.stderr')) {
            $stream = [IO.File]::Open((Join-Path $TestDrive $name), 'Open', 'ReadWrite', 'None')
            $stream.Dispose()
        }
    }
}
