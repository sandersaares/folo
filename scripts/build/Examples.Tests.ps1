#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }

# Pester suite for the `run-examples` environment and execution boundary in Examples.psm1.
#
# Get-ExampleTarget walks the filesystem, so tests build a `packages/<pkg>/examples/` fixture under
# TestDrive covering both example shapes plus the mod.rs and skip-list exclusions. Invoke-ExampleRun
# takes an injected scriptblock in place of cargo, so its timeout/exit-code/output classification is
# tested with fast fake commands. The watchdog decision is mocked, not driven by a real-time delay.

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
$VerbosePreference = 'Continue'

BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'Examples.psm1') -Force
}

Describe 'Initialize-ExampleEnvironment' {
    BeforeEach {
        $script:PreviousTargetDirectory = $env:CARGO_TARGET_DIR
        $script:PreviousTesting = $env:IS_TESTING
        Mock -ModuleName Examples cargo {
            @{ target_directory = [System.IO.Path]::GetTempPath() } | ConvertTo-Json -Compress
        }
    }

    AfterEach {
        $env:CARGO_TARGET_DIR = $script:PreviousTargetDirectory
        $env:IS_TESTING = $script:PreviousTesting
    }

    It 'uses Cargo output-directory resolution without the dependency graph' {
        Initialize-ExampleEnvironment

        $env:CARGO_TARGET_DIR | Should -Be ([System.IO.Path]::GetTempPath())
        $env:IS_TESTING | Should -Be '1'
        Should -Invoke -ModuleName Examples cargo -Times 1 -Exactly -ParameterFilter {
            ($args -join ' ') -eq 'metadata --format-version 1 --no-deps --locked'
        }
    }

    It 'lets Cargo resolve an existing relative target-directory override' {
        $env:CARGO_TARGET_DIR = 'relative output'
        Mock -ModuleName Examples cargo {
            $env:CARGO_TARGET_DIR | Should -Be 'relative output'
            @{ target_directory = [System.IO.Path]::GetTempPath() } | ConvertTo-Json -Compress
        }

        Initialize-ExampleEnvironment

        $env:CARGO_TARGET_DIR | Should -Be ([System.IO.Path]::GetTempPath())
    }

    It 'passes the resolved directory and smoke mode to the child job' {
        Initialize-ExampleEnvironment
        $command = { "$env:CARGO_TARGET_DIR|$env:IS_TESTING"; return 0 }

        $result = Invoke-ExampleRun -Package 'pkg' -Example 'ex' -Command $command

        $result.Status | Should -Be 'Success'
        $result.Output | Should -Be "$([System.IO.Path]::GetTempPath())|1"
    }

    It 'rejects unusable metadata without changing the environment' -TestCases @(
        @{ Metadata = 'not json' }
        @{ Metadata = '{}' }
        @{ Metadata = '{"target_directory":""}' }
        @{ Metadata = '{"target_directory":null}' }
    ) {
        param($Metadata)
        $env:CARGO_TARGET_DIR = 'unchanged-target'
        $env:IS_TESTING = 'unchanged-mode'
        $script:MetadataResponse = $Metadata
        Mock -ModuleName Examples cargo { $script:MetadataResponse }

        { Initialize-ExampleEnvironment } | Should -Throw

        $env:CARGO_TARGET_DIR | Should -Be 'unchanged-target'
        $env:IS_TESTING | Should -Be 'unchanged-mode'
    }

    It 'propagates a Cargo failure without changing the environment' {
        $env:CARGO_TARGET_DIR = 'unchanged-target'
        $env:IS_TESTING = 'unchanged-mode'
        Mock -ModuleName Examples cargo { throw 'Cargo failed.' }

        { Initialize-ExampleEnvironment } | Should -Throw

        $env:CARGO_TARGET_DIR | Should -Be 'unchanged-target'
        $env:IS_TESTING | Should -Be 'unchanged-mode'
    }
}

Describe 'Get-ExampleTarget' {
    BeforeEach {
        $script:Root = Join-Path $TestDrive 'packages'

        # alpha: two .rs examples, a mod.rs to ignore, and a subdir example.
        $alpha = Join-Path $script:Root 'alpha' 'examples'
        New-Item -ItemType Directory -Path $alpha -Force | Out-Null
        Set-Content -Path (Join-Path $script:Root 'alpha' 'Cargo.toml') -Value '[package]'
        Set-Content -Path (Join-Path $alpha 'a_one.rs') -Value '// ex'
        Set-Content -Path (Join-Path $alpha 'a_two.rs') -Value '// ex'
        Set-Content -Path (Join-Path $alpha 'mod.rs') -Value '// shared'
        New-Item -ItemType Directory -Path (Join-Path $alpha 'a_dir') -Force | Out-Null
        Set-Content -Path (Join-Path $alpha 'a_dir' 'main.rs') -Value '// ex'

        # beta: one example plus an excluded-by-design example.
        $beta = Join-Path $script:Root 'beta' 'examples'
        New-Item -ItemType Directory -Path $beta -Force | Out-Null
        Set-Content -Path (Join-Path $script:Root 'beta' 'Cargo.toml') -Value '[package]'
        Set-Content -Path (Join-Path $beta 'b_one.rs') -Value '// ex'
        Set-Content -Path (Join-Path $beta 'nm_otel_console.rs') -Value '// infinite loop'

        # gamma: a package with no examples directory (must be skipped).
        New-Item -ItemType Directory -Path (Join-Path $script:Root 'gamma') -Force | Out-Null
        Set-Content -Path (Join-Path $script:Root 'gamma' 'Cargo.toml') -Value '[package]'

        # noise: a directory that is not a package (no Cargo.toml) must be ignored.
        New-Item -ItemType Directory -Path (Join-Path $script:Root 'noise') -Force | Out-Null
    }

    It 'discovers both .rs-file and subdirectory examples across all packages' {
        $targets = @(Get-ExampleTarget -PackagesRoot $script:Root)
        $pairs = $targets | ForEach-Object { "$($_.Package)::$($_.Example)" }
        ($pairs -join ',') | Should -Be 'alpha::a_dir,alpha::a_one,alpha::a_two,beta::b_one'
    }

    It 'ignores mod.rs' {
        $targets = @(Get-ExampleTarget -PackagesRoot $script:Root -PackageFilter 'alpha')
        ($targets | Where-Object { $_.Example -eq 'mod' }) | Should -BeNullOrEmpty
    }

    It 'drops examples on the default skip-list' {
        $targets = @(Get-ExampleTarget -PackagesRoot $script:Root -PackageFilter 'beta')
        ($targets | Where-Object { $_.Example -eq 'nm_otel_console' }) | Should -BeNullOrEmpty
        ($targets | ForEach-Object { $_.Example }) | Should -Be 'b_one'
    }

    It 'honours a caller-supplied exclusion list' {
        $targets = @(Get-ExampleTarget -PackagesRoot $script:Root -PackageFilter 'alpha' -ExcludedExample @('a_two'))
        ($targets | ForEach-Object { $_.Example }) -join ',' | Should -Be 'a_dir,a_one'
    }

    It 'restricts to a space-separated package allow-list' {
        $targets = @(Get-ExampleTarget -PackagesRoot $script:Root -PackageFilter 'beta')
        ($targets | ForEach-Object { $_.Package } | Sort-Object -Unique) | Should -Be 'beta'
    }

    It 'skips packages without an examples directory' {
        $targets = @(Get-ExampleTarget -PackagesRoot $script:Root -PackageFilter 'gamma')
        $targets | Should -BeNullOrEmpty
    }
}

Describe 'Invoke-ExampleRun' {
    It 'reports Success and captures output when the command exits 0' {
        $command = { Write-Output 'line one'; Write-Output 'line two'; return 0 }
        $result = Invoke-ExampleRun -Package 'pkg' -Example 'ex' -Command $command -TimeoutSeconds 30
        $result.Status | Should -Be 'Success'
        $result.ExitCode | Should -Be 0
        $result.Output | Should -Be "line one`nline two"
    }

    It 'reports Success with empty output when the command emits only an exit code' {
        $command = { return 0 }
        $result = Invoke-ExampleRun -Package 'pkg' -Example 'ex' -Command $command -TimeoutSeconds 30
        $result.Status | Should -Be 'Success'
        $result.Output | Should -Be ''
    }

    It 'reports Failed and preserves the non-zero exit code and output' {
        $command = { Write-Output 'boom'; return 7 }
        $result = Invoke-ExampleRun -Package 'pkg' -Example 'ex' -Command $command -TimeoutSeconds 30
        $result.Status | Should -Be 'Failed'
        $result.ExitCode | Should -Be 7
        $result.Output | Should -Be 'boom'
    }

    It 'preserves partial output when the watchdog reports a timeout' {
        # Let the fixture produce output, then inject the watchdog result. The classification
        # depends on that result, not on how quickly either process happens to be scheduled.
        Mock -ModuleName Examples Wait-Job {
            param($Job)
            Microsoft.PowerShell.Core\Wait-Job -Job $Job | Out-Null
        }
        $command = { 'last completed phase'; 42 }

        $result = Invoke-ExampleRun -Package 'pkg' -Example 'ex' -Command $command

        $result.Status | Should -Be 'Timeout'
        $result.ExitCode | Should -BeNullOrEmpty
        $result.Output | Should -Be "last completed phase`n42"
    }
}
