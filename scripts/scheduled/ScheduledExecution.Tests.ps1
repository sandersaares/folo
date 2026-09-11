#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }

# Protects actual command scope, native argument transport, failure propagation and readable
# diagnostics. Expensive tools are mocked only at their process boundary, using ordinary outputs.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true

BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'ScheduledPlan.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'ScheduledExecution.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'ScheduledExecutionCommands.psm1')
    Import-Module (Join-Path $PSScriptRoot 'ScheduledExecutionDiagnostics.psm1')
    $script:fixtures = Join-Path $PSScriptRoot 'fixtures\execution'
    $script:sourceSha = 'a' * 40
}

Describe 'Scheduled command scope' {
    It 'uses the pinned toolchain, a real mutation baseline, exclusions and shards' {
        $check = @(Get-ScheduledCheck -Packages cpulist -CheckIds mutants-windows-latest-2)[0]
        $pin = Get-ScheduledToolchain -Kind mutants
        $command = Get-ScheduledCommand -Check $check -OutputDirectory $TestDrive -Toolchain $pin
        $command.arguments[0] | Should -Be "+$pin"
        $command.arguments | Should -Contain '--baseline=run'
        $command.arguments | Should -Contain '--timeout=60'
        $command.arguments | Should -Contain '--jobs=1'
        $command.arguments | Should -Contain '--package=cpulist'
        $command.arguments | Should -Contain '1/8'
        $command.arguments | Should -Contain '**/*linux.rs'
        $command.arguments | Should -Contain '--config'
        $command.environment.MUTATION_TESTING | Should -Be '1'
        $command.environment.RUSTFLAGS | Should -Be '--cfg mutants'
        $command.environment.CARGO_TARGET_DIR | Should -BeNullOrEmpty
    }

    It 'restricts many-seed execution to library tests and the selected range' {
        $check = @(Get-ScheduledCheck -Packages events -CheckIds miri-many-events-2)[0]
        $command = Get-ScheduledCommand -Check $check -OutputDirectory $TestDrive -Toolchain (Get-ScheduledToolchain -Kind miri)
        $command.arguments | Should -Contain '--lib'
        $command.arguments | Should -Contain '--all-features'
        $command.arguments | Should -Contain '--test-threads=1'
        $command.environment.MIRIFLAGS | Should -Be '-Zmiri-many-seeds=32..64'
    }

    It 'preserves ordinary Miri enabled lib/bin/integration targets and careful packages' {
        $metadata = Get-Content -LiteralPath (Join-Path $fixtures 'target-metadata.json') -Raw | ConvertFrom-Json -AsHashtable
        $check = @(Get-ScheduledCheck -CheckIds miri-ubuntu-latest)[0]
        $scopes = @(Get-ScheduledTestScope -Check $check -Metadata $metadata)
        $scopes.Count | Should -Be 4
        $scopes.target.name | Should -Be @('example', 'example-cli', 'round_trip', 'binary-only')
        $check.kind = 'careful'
        $scopes = @(Get-ScheduledTestScope -Check $check -Metadata $metadata)
        $scopes.package | Should -Be @('example', 'binary-only', 'test-support')
        $check.packages = @('missing')
        { Get-ScheduledTestScope -Check $check -Metadata $metadata } | Should -Throw
    }

    It 'omits unsupported proc-macro unit harnesses without omitting integration targets' {
        $metadata = @{
            workspace_members = @('macro')
            packages = @(@{ id = 'macro'; name = 'macro'; targets = @(
                @{ name = 'macro'; kind = @('proc-macro'); test = $true },
                @{ name = 'integration'; kind = @('test'); test = $true }
            ) })
        }
        $check = @(Get-ScheduledCheck -CheckIds miri-ubuntu-latest)[0]
        $scopes = @(Get-ScheduledTestScope -Check $check -Metadata $metadata)
        $scopes.Count | Should -Be 1
        $scopes[0].target.name | Should -Be 'integration'
    }

    It 'mirrors unmutated baseline configuration for build and test' {
        $check = @(Get-ScheduledCheck -Packages cpulist -CheckIds mutants-ubuntu-latest-1)[0]
        $mutation = Get-ScheduledCommand -Check $check -OutputDirectory $TestDrive -Toolchain (Get-ScheduledToolchain -Kind mutants)
        $configuration = @{
            test_tool = 'nextest'; profile = 'mutants'; all_features = $true; no_default_features = $true
            features = @('extra'); additional_cargo_args = @('--locked')
            additional_cargo_test_args = @('--tests'); cap_lints = $true
        }
        $build = Get-ScheduledBaselineCommand -Check $check -MutationCommand $mutation -Configuration $configuration -Phase Build
        $test = Get-ScheduledBaselineCommand -Check $check -MutationCommand $mutation -Configuration $configuration -Phase Test
        $build.arguments | Should -Contain '--no-run'
        $build.arguments | Should -Not -Contain '--tests'
        $test.arguments | Should -Contain '--tests'
        $test.arguments | Should -Contain '--cargo-profile=mutants'
        $test.arguments | Should -Contain '--features=extra'
        $test.arguments | Should -Contain '--no-default-features'
        $test.arguments | Should -Contain '--all-features'
        $test.arguments | Should -Contain '--locked'
        $test.environment.INSTA_UPDATE | Should -Be 'no'
        $test.environment.INSTA_FORCE_PASS | Should -Be '0'
        $test.environment.MUTATION_TESTING | Should -Be '1'
        $test.environment.CARGO_ENCODED_RUSTFLAGS | Should -Be (@('--cfg', 'mutants', '--cap-lints=warn') -join [char]0x1f)
        $test.timeout_seconds | Should -Be 60
        $build.ContainsKey('timeout_seconds') | Should -BeFalse
    }

    It 'passes literal arguments through a real child process without a shell' {
        $values = @('spaces and quotes "here"', 'literal*', 'x; Write-Host injected', 'café')
        $command = @{
            file = (Get-Command pwsh).Source
            arguments = @('-NoProfile', '-File', (Join-Path $fixtures 'Echo-Argument.ps1')) + $values
            environment = @{}
        }
        $result = Invoke-ScheduledProcess -Command $command -SourceRoot $TestDrive -OutputDirectory $TestDrive -Name 'arguments'
        $result.exit_code | Should -Be 0
        @(Get-Content -LiteralPath $result.stdout_path -Raw | ConvertFrom-Json) | Should -Be $values
    }
}

Describe 'Checker outcome propagation and diagnostics' {
    BeforeEach {
        $script:output = Join-Path $TestDrive ([guid]::NewGuid().ToString())
        $script:check = @(Get-ScheduledCheck -Packages example -CheckIds miri-ubuntu-latest)[0]
        Mock Assert-ScheduledPlatform -ModuleName ScheduledExecution {}
        Mock Invoke-ScheduledProcess -ModuleName ScheduledExecution {
            param($OutputDirectory, $Name)
            $stdout = Join-Path $OutputDirectory "$Name.stdout"
            $stderr = Join-Path $OutputDirectory "$Name.stderr"
            if ($Name -eq 'metadata') {
                Copy-Item -LiteralPath (Join-Path $fixtures 'target-metadata.json') -Destination $stdout
            } else { 'test ordinary ... ok' | Set-Content -LiteralPath $stdout }
            '' | Set-Content -LiteralPath $stderr
            return @{ exit_code = 0; timed_out = $false; stdout_path = $stdout; stderr_path = $stderr }
        }
    }

    It 'passes when every target succeeds and leaves no encoded records' {
        Invoke-ScheduledCheck -Check $check -SourceRoot $TestDrive -OutputDirectory $output -SourceSha $sourceSha | Should -Be 0
        $summary = Get-Content -LiteralPath (Join-Path $output 'summary.md') -Raw
        $summary | Should -Match $sourceSha
        $summary | Should -Match 'Final result: PASSED'
        $summary | Should -Not -Match 'ordinary'
        Test-Path -LiteralPath (Join-Path $output 'commands.txt') | Should -BeTrue
        Test-Path -LiteralPath (Join-Path $output 'evidence.json') | Should -BeFalse
        Test-Path -LiteralPath (Join-Path $output 'execution.json') | Should -BeFalse
        Should -Invoke Invoke-ScheduledProcess -ModuleName ScheduledExecution -Times 4 -Exactly
    }

    It 'fails on an early target finding while continuing later targets and retaining repro details' {
        Mock Invoke-ScheduledProcess -ModuleName ScheduledExecution -ParameterFilter { $Name -eq 'check-0' } {
            param($OutputDirectory, $Name)
            $stdout = Join-Path $OutputDirectory "$Name.stdout"
            $stderr = Join-Path $OutputDirectory "$Name.stderr"
            Copy-Item -LiteralPath (Join-Path $fixtures 'miri.stdout') -Destination $stdout
            Copy-Item -LiteralPath (Join-Path $fixtures 'miri.stderr') -Destination $stderr
            return @{ exit_code = 101; timed_out = $false; stdout_path = $stdout; stderr_path = $stderr }
        }
        Invoke-ScheduledCheck -Check $check -SourceRoot $TestDrive -OutputDirectory $output -SourceSha $sourceSha | Should -Be 101
        $summary = Get-Content -LiteralPath (Join-Path $output 'summary.md') -Raw
        $summary | Should -Match 'fixture_failure'
        $summary | Should -Match 'FAILING SEED: 19'
        $summary | Should -Match 'package=example'
        $summary | Should -Match 'Final result: FAILED'
        Should -Invoke Invoke-ScheduledProcess -ModuleName ScheduledExecution -Times 4 -Exactly
    }

    It 'fails setup rather than reporting unexecuted targets as successful' {
        Mock Invoke-ScheduledProcess -ModuleName ScheduledExecution { throw 'Process start canary.' }
        Invoke-ScheduledCheck -Check $check -SourceRoot $TestDrive -OutputDirectory $output -SourceSha $sourceSha | Should -Be 1
        Get-Content -LiteralPath (Join-Path $output 'summary.md') -Raw | Should -Match 'Execution failure'
        Test-Path -LiteralPath (Join-Path $output 'execution-error.txt') | Should -BeTrue
    }

    It 'fails the <packageName> selection when no executable targets are available' -ForEach @(
        @{ packageName = 'absent' }, @{ packageName = 'test-support' }
    ) {
        $check.packages = @($packageName)
        Invoke-ScheduledCheck -Check $check -SourceRoot $TestDrive -OutputDirectory $output -SourceSha $sourceSha | Should -Be 1
    }
}

Describe 'Mutation completion and findings' {
    BeforeEach {
        $script:output = Join-Path $TestDrive ([guid]::NewGuid().ToString())
        $null = New-Item -ItemType Directory -Path (Join-Path $output 'mutants.out') -Force
        '' | Set-Content -LiteralPath (Join-Path $output 'summary.md')
        $script:lab = Get-Content -LiteralPath (Join-Path $fixtures 'outcomes.json') -Raw | ConvertFrom-Json -AsHashtable
        $script:inventory = @($lab.outcomes | Where-Object { $_.scenario -is [hashtable] } | ForEach-Object { $_.scenario.Mutant })
    }

    It 'fails missed mutants and timeouts with human-readable mutation descriptors' {
        $lab | ConvertTo-Json -Depth 20 | Set-Content -LiteralPath (Join-Path $output 'mutants.out\outcomes.json')
        Write-ScheduledMutationSummary -OutputDirectory $output -Inventory $inventory | Should -Be 1
        $summary = Get-Content -LiteralPath (Join-Path $output 'summary.md') -Raw
        $summary | Should -Match 'MissedMutant'
        $summary | Should -Match 'Timeout'
        $summary | Should -Match 'replace sample -> u32 with 0'
        $summary | Should -Match 'replace sample -> u32 with 1'
    }

    It 'accepts signed Windows failed-test statuses as caught mutations' {
        $lab.outcomes[1].summary = 'CaughtMutant'
        # Native access violations are signed failure statuses, not invalid tool output.
        $lab.outcomes[1].phase_results[1].process_status = @{ Failure = -1073741819 }
        $lab.outcomes[2].summary = 'Unviable'
        $lab.outcomes[2].phase_results = @(@{ phase = 'Build'; process_status = @{ Failure = 1 } })
        $lab.missed = 0
        $lab.timeout = 0
        $lab | ConvertTo-Json -Depth 20 | Set-Content -LiteralPath (Join-Path $output 'mutants.out\outcomes.json')
        Write-ScheduledMutationSummary -OutputDirectory $output -Inventory $inventory | Should -Be 0
    }

    It 'requires actual completed baseline phases even if the summary claims success' {
        $lab.outcomes[0].phase_results = @(@{ phase = 'Build'; process_status = 'Success' })
        $lab | ConvertTo-Json -Depth 20 | Set-Content -LiteralPath (Join-Path $output 'mutants.out\outcomes.json')
        Write-ScheduledMutationSummary -OutputDirectory $output -Inventory $inventory | Should -Be 1
        Get-Content -LiteralPath (Join-Path $output 'summary.md') -Raw | Should -Match 'baseline failed'
    }

    It 'does not treat a timed-out mutation as caught' {
        $lab.outcomes[1].summary = 'CaughtMutant'
        $lab.outcomes[1].phase_results[1].process_status = 'Timeout'
        $lab.outcomes[2].summary = 'CaughtMutant'
        $lab.missed = 0
        $lab.timeout = 0
        $lab | ConvertTo-Json -Depth 20 | Set-Content -LiteralPath (Join-Path $output 'mutants.out\outcomes.json')
        Write-ScheduledMutationSummary -OutputDirectory $output -Inventory $inventory | Should -Be 1
        Get-Content -LiteralPath (Join-Path $output 'summary.md') -Raw | Should -Match 'Timeout:'
    }

    It 'fails a baseline failure without presenting mutant results as source defects' {
        $lab.outcomes[0].summary = 'Failure'
        $null = New-Item -ItemType Directory -Path (Join-Path $output 'mutants.out\log') -Force
        'test baseline_canary ... FAILED' | Set-Content -LiteralPath (Join-Path $output 'mutants.out\log\baseline.log')
        $lab | ConvertTo-Json -Depth 20 | Set-Content -LiteralPath (Join-Path $output 'mutants.out\outcomes.json')
        Write-ScheduledMutationSummary -OutputDirectory $output -Inventory $inventory | Should -Be 1
        $summary = Get-Content -LiteralPath (Join-Path $output 'summary.md') -Raw
        $summary | Should -Match 'baseline failed'
        $summary | Should -Match 'baseline_canary'
        $summary | Should -Not -Match 'replace sample'
    }

    It 'fails missing or incomplete tool results' {
        { Write-ScheduledMutationSummary -OutputDirectory $output -Inventory $inventory } | Should -Throw
        $lab.end_time = $null
        $lab | ConvertTo-Json -Depth 20 | Set-Content -LiteralPath (Join-Path $output 'mutants.out\outcomes.json')
        Write-ScheduledMutationSummary -OutputDirectory $output -Inventory $inventory | Should -Be 1
    }
}

Describe 'Mutation execution baseline discipline' {
    BeforeEach {
        $script:output = Join-Path $TestDrive ([guid]::NewGuid().ToString())
        $script:check = @(Get-ScheduledCheck -Packages example -CheckIds mutants-ubuntu-latest-1)[0]
        Mock Assert-ScheduledPlatform -ModuleName ScheduledExecution {}
        Mock Resolve-CargoExecutable -ModuleName ScheduledExecution { (Get-Command pwsh).Source }
        Mock Get-ScheduledMutationConfig -ModuleName ScheduledExecution {
            @{
                test_tool = 'cargo'; profile = 'mutants'; all_features = $true; no_default_features = $false
                features = @(); additional_cargo_args = @('--locked')
                additional_cargo_test_args = @('--tests'); cap_lints = $false
            }
        }
        Mock Invoke-ScheduledProcess -ModuleName ScheduledExecution {
            param($OutputDirectory, $Name)
            $stdout = Join-Path $OutputDirectory "$Name.stdout"
            $stderr = Join-Path $OutputDirectory "$Name.stderr"
            '' | Set-Content -LiteralPath $stdout
            '' | Set-Content -LiteralPath $stderr
            if ($Name -eq 'check') {
                $null = New-Item -ItemType Directory -Path (Join-Path $OutputDirectory 'mutants.out') -Force
                '[]' | Set-Content -LiteralPath (Join-Path $OutputDirectory 'mutants.out\mutants.json')
            }
            return @{ exit_code = 0; timed_out = $false; stdout_path = $stdout; stderr_path = $stderr }
        }
    }

    It 'executes unmutated build and test when cargo-mutants selects an empty shard' {
        Invoke-ScheduledCheck -Check $check -SourceRoot $TestDrive -OutputDirectory $output -SourceSha $sourceSha | Should -Be 0
        Should -Invoke Invoke-ScheduledProcess -ModuleName ScheduledExecution -ParameterFilter { $Name -eq 'baseline-build' } -Times 1 -Exactly
        Should -Invoke Invoke-ScheduledProcess -ModuleName ScheduledExecution -ParameterFilter { $Name -eq 'baseline-test' } -Times 1 -Exactly
        $summary = Get-Content -LiteralPath (Join-Path $output 'summary.md') -Raw
        $summary | Should -Match 'No mutants selected; the unmutated baseline passed'
    }

    It 'fails an empty-shard baseline <phase> failure' -ForEach @(
        @{ phase = 'baseline-build'; timedOut = $false }
        @{ phase = 'baseline-test'; timedOut = $false }
        @{ phase = 'baseline-test'; timedOut = $true }
    ) {
        Mock Invoke-ScheduledProcess -ModuleName ScheduledExecution -ParameterFilter { $Name -eq $phase } {
            param($OutputDirectory, $Name)
            $stdout = Join-Path $OutputDirectory "$Name.stdout"
            $stderr = Join-Path $OutputDirectory "$Name.stderr"
            'baseline test failure canary' | Set-Content -LiteralPath $stdout
            '' | Set-Content -LiteralPath $stderr
            return @{ exit_code = 1; timed_out = $timedOut; stdout_path = $stdout; stderr_path = $stderr }
        }
        Invoke-ScheduledCheck -Check $check -SourceRoot $TestDrive -OutputDirectory $output -SourceSha $sourceSha | Should -Be 1
        $summary = Get-Content -LiteralPath (Join-Path $output 'summary.md') -Raw
        $summary | Should -Match 'baseline test failure canary'
        $summary | Should -Match 'Final result: FAILED'
    }

    It 'does not mask a mutation process failure with a successful empty-shard baseline' {
        Mock Invoke-ScheduledProcess -ModuleName ScheduledExecution -ParameterFilter { $Name -eq 'check' } {
            param($OutputDirectory, $Name)
            $stdout = Join-Path $OutputDirectory "$Name.stdout"
            $stderr = Join-Path $OutputDirectory "$Name.stderr"
            '' | Set-Content -LiteralPath $stdout
            'mutation setup error' | Set-Content -LiteralPath $stderr
            $null = New-Item -ItemType Directory -Path (Join-Path $OutputDirectory 'mutants.out') -Force
            '[]' | Set-Content -LiteralPath (Join-Path $OutputDirectory 'mutants.out\mutants.json')
            return @{ exit_code = 1; timed_out = $false; stdout_path = $stdout; stderr_path = $stderr }
        }
        Invoke-ScheduledCheck -Check $check -SourceRoot $TestDrive -OutputDirectory $output -SourceSha $sourceSha | Should -Be 1
        Should -Invoke Get-ScheduledMutationConfig -ModuleName ScheduledExecution -Times 0 -Exactly
    }

    It 'fails findings even if a mutation process incorrectly reports success' {
        Mock Invoke-ScheduledProcess -ModuleName ScheduledExecution -ParameterFilter { $Name -eq 'check' } {
            param($OutputDirectory, $Name)
            $stdout = Join-Path $OutputDirectory "$Name.stdout"
            $stderr = Join-Path $OutputDirectory "$Name.stderr"
            '' | Set-Content -LiteralPath $stdout
            '' | Set-Content -LiteralPath $stderr
            $null = New-Item -ItemType Directory -Path (Join-Path $OutputDirectory 'mutants.out') -Force
            $lab = Get-Content -LiteralPath (Join-Path $fixtures 'outcomes.json') -Raw | ConvertFrom-Json -AsHashtable
            $inventory = @($lab.outcomes | Where-Object { $_.scenario -is [hashtable] } | ForEach-Object { $_.scenario.Mutant })
            ConvertTo-Json -InputObject $inventory -Depth 20 | Set-Content -LiteralPath (Join-Path $OutputDirectory 'mutants.out\mutants.json')
            $lab | ConvertTo-Json -Depth 20 | Set-Content -LiteralPath (Join-Path $OutputDirectory 'mutants.out\outcomes.json')
            return @{ exit_code = 0; timed_out = $false; stdout_path = $stdout; stderr_path = $stderr }
        }
        Invoke-ScheduledCheck -Check $check -SourceRoot $TestDrive -OutputDirectory $output -SourceSha $sourceSha | Should -Be 1
        Get-Content -LiteralPath (Join-Path $output 'summary.md') -Raw | Should -Match 'MissedMutant'
    }
}
