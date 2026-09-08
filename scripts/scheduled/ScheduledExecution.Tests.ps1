#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
# Exercise checker command construction and independent evidence reconstruction without running
# deep checks. The real controller decoder remains in the baseline path so trust, native startup
# and TOML/JSON compatibility are covered together.
# Ref: .github/workflows/implementation.md, "Scheduled controller ownership".
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true

BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'ScheduledExecution.psm1') -Force
    $script:fixtures = Join-Path $PSScriptRoot 'fixtures\execution'
    # Keep all test-created files inside the worktree, not a machine temporary directory.
    $script:work = Join-Path $PSScriptRoot ".execution-tests-$([guid]::NewGuid().ToString('N'))"
    $null = New-Item -ItemType Directory -Path $script:work
    $script:root = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
    $script:toolchain = Get-ScheduledToolchain -Kind miri
    $script:context = @{ source_sha = 'a' * 40; controller_sha = 'b' * 40
        check_contract_digest = 'c' * 64; run_id = 1; run_attempt = 1; run_number = 1 }
    function New-Check {
        [Diagnostics.CodeAnalysis.SuppressMessageAttribute('PSUseShouldProcessForStateChangingFunctions', '',
            Justification = 'Creates an in-memory test fixture, not persistent user state.')]
        [CmdletBinding()]
        param([string] $Kind = 'mutants')
        $arm = [Runtime.InteropServices.RuntimeInformation]::OSArchitecture -eq
            [Runtime.InteropServices.Architecture]::Arm64
        return @{ id = 'fixture'; kind = $Kind
            platform = if ($IsWindows) { if ($arm) { 'windows-11-arm' } else { 'windows-latest' } }
                else { if ($arm) { 'ubuntu-24.04-arm' } else { 'ubuntu-latest' } }
            packages = @('example'); shard = ''; seed_range = ''; flags = @(); test_filter = '' }
    }
    function New-Output {
        [Diagnostics.CodeAnalysis.SuppressMessageAttribute('PSUseShouldProcessForStateChangingFunctions', '',
            Justification = 'Creates an isolated test-owned directory that AfterAll removes.')]
        [CmdletBinding()]
        param()
        $path = Join-Path $script:work ([guid]::NewGuid().ToString('N'))
        $null = New-Item -ItemType Directory -Path $path
        return $path
    }
    function Write-Json($Value, [string] $Path) {
        ConvertTo-Json -InputObject $Value -Depth 100 | Set-Content -LiteralPath $Path -Encoding utf8
    }
    function New-Evidence {
        [Diagnostics.CodeAnalysis.SuppressMessageAttribute('PSUseShouldProcessForStateChangingFunctions', '',
            Justification = 'Writes only test-owned evidence fixtures removed by AfterAll.')]
        [CmdletBinding()]
        param($Check, [int] $ExitCode = 3)
        $path = New-Output
        $null = New-Item -ItemType Directory -Path (Join-Path $path 'mutants.out')
        Copy-Item -LiteralPath (Join-Path $script:fixtures 'mutants.json'), (Join-Path $script:fixtures 'outcomes.json') `
            -Destination (Join-Path $path 'mutants.out')
        $toolchain = Get-ScheduledToolchain -Kind $Check.kind
        $command = Get-ScheduledCommand -Check $Check -SourceRoot $script:root -OutputDirectory $path -Toolchain $toolchain
        $execution = @{ schema_version = 1; stage = 'check'; exit_code = $ExitCode; completed = $true
            source_root = $script:root; output_directory = $path; toolchain = $toolchain
            commands = @(@{ name = 'check'; command = $command; exit_code = $ExitCode }) }
        Write-Json $execution (Join-Path $path 'execution.json')
        Set-Content -LiteralPath (Join-Path $path 'check.log') -Value 'fixture'
        return $path
    }
    function Set-Lab {
        [Diagnostics.CodeAnalysis.SuppressMessageAttribute('PSUseShouldProcessForStateChangingFunctions', '',
            Justification = 'Mutates an isolated evidence fixture to exercise classification.')]
        [CmdletBinding()]
        param($Path, [scriptblock] $Change)
        $lab = Get-Content -LiteralPath (Join-Path $Path 'mutants.out\outcomes.json') -Raw | ConvertFrom-Json -AsHashtable
        & $Change $lab
        Write-Json $lab (Join-Path $Path 'mutants.out\outcomes.json')
    }
    function New-TestEvidence {
        [Diagnostics.CodeAnalysis.SuppressMessageAttribute('PSUseShouldProcessForStateChangingFunctions', '',
            Justification = 'Writes only test-owned evidence fixtures removed by AfterAll.')]
        [CmdletBinding()]
        param($Check, [int] $ExitCode = 1)
        $path = New-Output
        $package = $Check.packages[0]
        $scope = $Check.Clone()
        $target = @{ kind = 'lib'; name = $package.Replace('-', '_') }
        if ($Check.kind -in @('miri', 'miri-many')) {
            if ($Check.ContainsKey('target')) { $target = $Check.target }
            $scope.target = $target
        }
        $toolchain = Get-ScheduledToolchain -Kind $Check.kind
        $command = Get-ScheduledCommand -Check $scope -SourceRoot $script:root -OutputDirectory $path -Toolchain $toolchain
        Write-Json @{ version = 1; packages = @(@{ id = "path+$package"; name = $package
                    targets = @(@{ kind = @($target.kind); name = $target.name; test = $true }) })
            workspace_members = @("path+$package") } (Join-Path $path 'metadata.stdout')
        Write-Json @{ schema_version = 1; stage = 'check'; exit_code = $ExitCode; completed = $true
            source_root = $script:root; output_directory = $path; toolchain = $toolchain
            commands = @(@{ name = 'check-0'; package = $package; target = $target; command = $command; exit_code = $ExitCode }) } `
            (Join-Path $path 'execution.json')
        Copy-Item -LiteralPath (Join-Path $script:fixtures 'miri.stdout') -Destination (Join-Path $path 'check-0.stdout')
        Copy-Item -LiteralPath (Join-Path $script:fixtures 'miri.stderr') -Destination (Join-Path $path 'check-0.stderr')
        return $path
    }

    function New-EmptyEvidence {
        [Diagnostics.CodeAnalysis.SuppressMessageAttribute('PSUseShouldProcessForStateChangingFunctions', '',
            Justification = 'Writes isolated baseline evidence fixtures removed by AfterAll.')]
        [CmdletBinding()]
        param($Check)

        $path = New-Evidence $Check 0
        Remove-Item -LiteralPath (Join-Path $path 'mutants.out\outcomes.json')
        Copy-Item -LiteralPath (Join-Path $script:fixtures 'empty-mutants.json') `
            -Destination (Join-Path $path 'mutants.out\mutants.json')
        Copy-Item -LiteralPath (Join-Path $script:fixtures 'empty-mutants.json') -Destination (Join-Path $path 'discovery.stdout')
        Set-Content -LiteralPath (Join-Path $path 'discovery.stderr') -Value ''
        Set-Content -LiteralPath (Join-Path $path 'mutants-version.stdout') -Value 'cargo-mutants 27.1.0'
        Set-Content -LiteralPath (Join-Path $path 'mutants-version.stderr') -Value ''
        Copy-Item -LiteralPath (Join-Path $script:root '.cargo\mutants.toml') -Destination (Join-Path $path 'mutation-config.toml')
        $pin = Get-ScheduledToolchain -Kind mutants
        $execution = Get-Content -LiteralPath (Join-Path $path 'execution.json') -Raw | ConvertFrom-Json -AsHashtable
        $discovery = Get-ScheduledCommand $Check $script:root $path $pin -List
        $execution.commands += @{ name = 'discovery'; command = $discovery; exit_code = 0 }
        $execution.commands += @{ name = 'mutants-version'; exit_code = 0
            command = @{ file = 'cargo'; arguments = @("+$pin", 'mutants', '--version'); environment = $discovery.environment } }
        $configuration = & (Get-Module ScheduledExecution) {
            param($Text)
            Get-ScheduledMutationConfig -Text $Text
        } (Get-Content -LiteralPath (Join-Path $path 'mutation-config.toml') -Raw)
        foreach ($phase in @('Build', 'Test')) {
            $command = & (Get-Module ScheduledExecution) {
                param($Check, $Command, $Configuration, $Phase)
                Get-ScheduledEmptyBaselineCommand $Check $Command $Configuration $Phase
            } $Check $execution.commands[0].command $configuration $phase
            $name = "baseline-$($phase.ToLowerInvariant())"
            $execution.commands += @{ name = $name; command = $command; exit_code = 0; timed_out = $false }
            Set-Content -LiteralPath (Join-Path $path "$name.stdout") -Value ''
            Set-Content -LiteralPath (Join-Path $path "$name.stderr") -Value ''
        }
        Copy-Item -LiteralPath (Join-Path $script:fixtures 'baseline-build.stderr') -Destination (Join-Path $path 'baseline-build.stderr')
        Copy-Item -LiteralPath (Join-Path $script:fixtures 'baseline-test.stdout') -Destination (Join-Path $path 'baseline-test.stdout')
        Write-Json $execution (Join-Path $path 'execution.json')
        return $path
    }

    function New-TargetEvidence {
        [Diagnostics.CodeAnalysis.SuppressMessageAttribute('PSUseShouldProcessForStateChangingFunctions', '',
            Justification = 'Writes only test-owned target evidence fixtures removed by AfterAll.')]
        [CmdletBinding()]
        param($Check, [int] $ExitCode = 1)

        $path = New-Output
        Copy-Item -LiteralPath (Join-Path $script:fixtures 'target-metadata.json') -Destination (Join-Path $path 'metadata.stdout')
        $scopes = @(
            @{ package = 'binary-only'; target = @{ kind = 'bin'; name = 'binary-only' } },
            @{ package = 'example'; target = @{ kind = 'bin'; name = 'example-cli' } },
            @{ package = 'example'; target = @{ kind = 'lib'; name = 'example' } },
            @{ package = 'example'; target = @{ kind = 'test'; name = 'round_trip' } }
        )
        $commands = @()
        foreach ($selected in $scopes) {
            if ($Check.packages.Count -gt 0 -and $selected.package -cnotin $Check.packages) { continue }
            if ($Check.kind -eq 'miri-many' -and $selected.target.kind -cne 'lib') { continue }
            if ($Check.ContainsKey('target') -and ($selected.target.kind -cne $Check.target.kind -or
                    $selected.target.name -cne $Check.target.name)) { continue }
            $scope = $Check.Clone()
            $scope.packages = @($selected.package)
            $scope.target = $selected.target
            $name = "check-$($commands.Count)"
            $commands += @{ name = $name; package = $selected.package; target = $selected.target
                command = Get-ScheduledCommand $scope $script:root $path $script:toolchain; exit_code = $ExitCode }
            $status = if ($ExitCode -eq 0) { 'ok' } else { 'FAILED' }
            $passed = if ($ExitCode -eq 0) { 1 } else { 0 }
            $failed = if ($ExitCode -eq 0) { 0 } else { 1 }
            Set-Content -LiteralPath (Join-Path $path "$name.stdout") `
                -Value "test tests::same_name ... $status`ntest result: $status. $passed passed; $failed failed;"
            Set-Content -LiteralPath (Join-Path $path "$name.stderr") -Value ''
        }
        Write-Json @{ schema_version = 1; stage = 'check'; exit_code = $ExitCode; completed = $true
            source_root = $script:root; output_directory = $path; toolchain = $script:toolchain
            commands = $commands } (Join-Path $path 'execution.json')
        return $path
    }
}

AfterAll {
    Remove-Item -LiteralPath $script:work -Recurse -Force
}

Describe 'Controller-owned mutation configuration decoder' {
    It 'builds only the trusted native helper even when called from a candidate directory' {
        Push-Location (New-Output)
        try {
            $build = & (Get-Module ScheduledExecution) { Get-ScheduledMutationDecoderBuild }
            $build.root | Should -Be $script:root
            $build.command.file | Should -Be (Get-Command cargo -CommandType Application | Select-Object -First 1).Source
            [IO.Path]::IsPathFullyQualified($build.command.file) | Should -BeTrue
            $build.command.arguments | Should -Contain 'scheduled-mutation-config'
            $build.command.arguments | Should -Contain (Join-Path $script:root 'Cargo.toml')
            $build.command.arguments | Should -Contain '--locked'
            $build.command.arguments | Should -Not -Contain '--workspace'
            $build.command.environment.RUSTUP_AUTO_INSTALL | Should -Be '0'
            $build.command.environment.CARGO_BUILD_TARGET | Should -BeNullOrEmpty
            $platform = & (Get-Module ScheduledExecution) { Get-ScheduledHostPlatform }
            $build.target | Should -Be (Join-Path $script:root `
                "target\scheduled-mutation-config\$($platform.os)-$($platform.architecture)")
        } finally {
            Pop-Location
        }
    }

    It 'invokes the native decoder with the actual controller configuration' {
        $configuration = & (Get-Module ScheduledExecution) {
            $text = Get-Content -LiteralPath (Join-Path $PSScriptRoot '..\..\.cargo\mutants.toml') -Raw
            Get-ScheduledMutationConfig -Text $text
        }
        $configuration.all_features | Should -BeTrue
        $configuration.profile | Should -Be 'mutants'
        $configuration.additional_cargo_args | Should -Be @('--locked')
        $configuration.additional_cargo_test_args | Should -Be @('--tests')
    }

    It 'reuses the built helper for repeated parses in the same controller process' {
        InModuleScope ScheduledExecution {
            $expected = Get-ScheduledMutationDecoder
            Mock Get-ScheduledMutationDecoderBuild { throw 'The decoder was already built.' }
            Get-ScheduledMutationDecoder | Should -Be $expected
            Should -Invoke Get-ScheduledMutationDecoderBuild -Times 0
        }
    }

    It 'does not accept a cached target artifact when the controller build fails' {
        InModuleScope ScheduledExecution {
            $saved = $script:mutationDecoderExecutable
            $script:mutationDecoderExecutable = $null
            try {
                Mock Get-ScheduledMutationDecoderBuild {
                    @{ root = 'controller'; target = 'controller-target'
                        command = @{ file = 'cargo'; arguments = @(); environment = @{} } }
                }
                Mock New-Item {}
                Mock Invoke-ScheduledProcess {
                    @{ exit_code = 1; stdout_path = 'build.stdout'; stderr_path = 'build.stderr' }
                }
                Mock Get-Content { 'controller build diagnostic' }
                Mock Resolve-CargoExecutable { throw 'Failed builds cannot supply executables.' }
                { Get-ScheduledMutationDecoder } | Should -Throw -ExceptionType ([InvalidOperationException])
                $script:mutationDecoderExecutable | Should -BeNullOrEmpty
                Should -Invoke Resolve-CargoExecutable -Times 0
            } finally {
                $script:mutationDecoderExecutable = $saved
            }
        }
    }

    It 'refuses dependency drift before making the built decoder available for parsing' {
        InModuleScope ScheduledExecution {
            $saved = $script:mutationDecoderExecutable
            $script:mutationDecoderExecutable = $null
            try {
                Mock Get-Content {
                    param([string[]] $LiteralPath, [switch] $Raw)
                    if ($LiteralPath -like '*dependency-contract.json') { return '{}' }
                    foreach ($path in $LiteralPath) {
                        if ($Raw) { [IO.File]::ReadAllText($path) }
                        else { [IO.File]::ReadAllLines($path) }
                    }
                }
                { Get-ScheduledMutationDecoder } | Should -Throw -ExceptionType ([InvalidOperationException])
                $script:mutationDecoderExecutable | Should -BeNullOrEmpty
            } finally {
                $script:mutationDecoderExecutable = $saved
            }
        }
    }

    It 'rejects candidate configuration before starting any decoder or Cargo build' {
        InModuleScope ScheduledExecution {
            Mock Get-ScheduledMutationDecoder { throw 'Untrusted configuration cannot start code.' }
            { Get-ScheduledMutationConfig -Text 'test_tool = "untrusted"' } | Should -Throw
            Should -Invoke Get-ScheduledMutationDecoder -Times 0
        }
    }

    It 'preserves absent defaults through the native JSON transport' {
        InModuleScope ScheduledExecution {
            Mock Get-Content { '' } -ParameterFilter { $LiteralPath -like '*mutants.toml' }
            $actual = Get-ScheduledMutationConfig -Text ''
            $actual.Count | Should -Be 8
            foreach ($field in @('additional_cargo_args', 'additional_cargo_test_args', 'features')) {
                $actual[$field] -is [array] | Should -BeTrue
                $actual[$field].Count | Should -Be 0
            }
            foreach ($field in @('all_features', 'cap_lints', 'no_default_features')) {
                $actual[$field] -is [bool] | Should -BeTrue
                $actual[$field] | Should -BeFalse
            }
            $actual.profile | Should -BeNullOrEmpty
            $actual.test_tool | Should -BeExactly 'cargo'
        }
    }

    It 'preserves TOML escaping multiline arrays and Unicode through the native transport' {
        InModuleScope ScheduledExecution {
            $text = @'
additional_cargo_args = [
    "--config=build.rustflags=\"--cfg custom\"", # Retain embedded quotes.
    'C:\workspace',
]
additional_cargo_test_args = ["--tests"]
features = ["caf\u00e9", """multi\
                          line"""]
test_tool = "nextest"
exclude_re = ['unrelated.*']
'@
            Mock Get-Content { $text } -ParameterFilter { $LiteralPath -like '*mutants.toml' }
            $actual = Get-ScheduledMutationConfig -Text $text
            $actual.additional_cargo_args | Should -Be @('--config=build.rustflags="--cfg custom"', 'C:\workspace')
            $actual.features | Should -Be @('café', 'multiline')
            $actual.test_tool | Should -BeExactly 'nextest'
            $actual.ContainsKey('exclude_re') | Should -BeFalse
        }
    }

    It 'propagates invalid configuration as a parse failure' -ForEach @(
        @{ Text = 'all_features = 1' },
        @{ Text = 'additional_cargo_args = [true]' },
        @{ Text = 'test_tool = "unsupported"' },
        @{ Text = 'features = [' }
    ) {
        InModuleScope ScheduledExecution -Parameters @{ Text = $Text } {
            Mock Get-Content { $Text } -ParameterFilter { $LiteralPath -like '*mutants.toml' }
            { Get-ScheduledMutationConfig -Text $Text } | Should -Throw -ExceptionType ([FormatException])
        }
    }

    It 'returns a failing native exit and no stdout for invalid utility input' -ForEach @(
        @{ Text = 'features = ['; Arguments = @() },
        @{ Text = '{}'; Arguments = @('--dependency-contract') },
        @{ Text = ''; Arguments = @('--unsupported') }
    ) {
        $start = [Diagnostics.ProcessStartInfo]::new()
        $start.FileName = & (Get-Module ScheduledExecution) { Get-ScheduledMutationDecoder }
        foreach ($argument in $Arguments) { $start.ArgumentList.Add($argument) }
        $start.UseShellExecute = $false
        $start.RedirectStandardInput = $true
        $start.RedirectStandardOutput = $true
        $start.RedirectStandardError = $true
        $process = [Diagnostics.Process]::new()
        $process.StartInfo = $start
        try {
            $process.Start() | Should -BeTrue
            $stdout = $process.StandardOutput.ReadToEndAsync()
            $stderr = $process.StandardError.ReadToEndAsync()
            $process.StandardInput.Write($Text)
            $process.StandardInput.Close()
            $process.WaitForExit()
            $process.ExitCode | Should -Not -Be 0
            $stdout.GetAwaiter().GetResult() | Should -BeNullOrEmpty
            $stderr.GetAwaiter().GetResult() | Should -Not -BeNullOrEmpty
        } finally {
            $process.Dispose()
        }
    }
}

Describe 'Typed scheduled commands' {
    It 'retains earlier exports when entrypoints load shared modules in one process' {
        $fixture = Join-Path $script:fixtures 'Test-ModuleImport.ps1'
        $output = & pwsh -NoProfile -File $fixture -SourceRoot $script:root
        $LASTEXITCODE | Should -Be 0
        # Standalone script preambles enable verbose import diagnostics before the JSON result.
        $observed = $output[-1] | ConvertFrom-Json -AsHashtable
        $observed.digest | Should -Match '^[0-9a-f]{64}$'
        $observed.seed_range | Should -Be '16..32'
        $observed.shard | Should -Be @('--shard', '1/8')
        $observed.shard_index | Should -Be 2
        $observed.arguments | Should -Contain '--baseline=run'
    }

    It 'preserves mutation exclusions, shard, timeout and real baseline without requesting list JSON' {
        $check = New-Check
        $check.platform = 'ubuntu-latest'
        $check.packages = @()
        $check.shard = '2/8'
        $command = Get-ScheduledCommand $check $script:root $script:work (Get-ScheduledToolchain -Kind mutants)
        $command.arguments | Should -Contain '--workspace'
        $command.arguments | Should -Contain '1/8'
        $command.arguments | Should -Contain '**/*windows.rs'
        $command.arguments | Should -Contain 'packages/dure/**/*.rs'
        $command.arguments | Should -Contain '--baseline=run'
        $command.arguments | Should -Contain '--timeout=60'
        $command.arguments | Should -Not -Contain '--json'
        $command.arguments | Should -Not -Contain '--baseline=skip'
        $command.environment.MUTATION_TESTING | Should -Be '1'
        $command.environment.RUSTFLAGS | Should -Be '--cfg mutants'
    }

    It 'preserves an exact Miri replay seed, named test and flags' {
        $check = New-Check 'miri-many'
        $check.packages = @('nm_impl')
        $check.flags = @('-Zmiri-seed=19', '-Zmiri-strict-provenance')
        $check.seed_range = '32..64'
        $check.shard = '2/2'
        $check.test_filter = 'observations::tests::concurrency::sync_concurrent_insert_and_snapshot'
        $command = Get-ScheduledCommand $check $script:root $script:work $script:toolchain
        $command.environment.MIRIFLAGS | Should -Be '-Zmiri-seed=19 -Zmiri-strict-provenance'
        $command.arguments | Should -Contain '--lib'
        $command.arguments | Should -Contain $check.test_filter
        $command.arguments | Should -Contain '--all-features'
        $command.arguments | Should -Contain '--locked'
    }

    It 'keeps ordinary Miri on lib/bin/integration tests and many-seed Miri on libraries' {
        $miri = Get-ScheduledCommand (New-Check 'miri') $script:root $script:work $script:toolchain
        $miri.arguments | Should -Contain '--tests'
        $miri.arguments | Should -Not -Contain '--lib'
        $many = Get-ScheduledCommand (New-Check 'miri-many') $script:root $script:work $script:toolchain
        $many.arguments | Should -Contain '--lib'
        $many.arguments | Should -Not -Contain '--tests'
        $careful = Get-ScheduledCommand (New-Check 'careful') $script:root $script:work $script:toolchain
        $careful.arguments | Should -Not -Contain '--lib'
    }

    It 'does not turn test names or package values into shell syntax or switches' {
        $check = New-Check 'miri'
        $check.packages = @('--workspace; echo "injected"')
        $check.test_filter = 'test;$(throw "injected") & "quoted name"'
        $command = Get-ScheduledCommand $check $script:root $script:work $script:toolchain
        $command.arguments | Should -Contain '--package=--workspace; echo "injected"'
        $command.arguments[-2] | Should -BeExactly $check.test_filter
        $check.test_filter = '--ignored'
        { Get-ScheduledCommand $check $script:root $script:work $script:toolchain } | Should -Throw
    }

    It 'refuses unpinned toolchains, relative paths and baseline overriding flags' {
        $check = New-Check
        { Get-ScheduledCommand $check $script:root $script:work 'nightly' } | Should -Throw
        { Get-ScheduledCommand $check 'relative' $script:work (Get-ScheduledToolchain -Kind mutants) } | Should -Throw
        $check.flags = @('--baseline=skip')
        { Get-ScheduledCommand $check $script:root $script:work (Get-ScheduledToolchain -Kind mutants) } | Should -Throw
    }

    It 'roundtrips actual native argv with spaces, quotes, glob characters and metacharacters' {
        $output = New-Output
        $values = @('a b', "single'quote", 'double"quote', '**/*.rs', '$(); & | >', '')
        $fixture = Join-Path $script:fixtures 'Echo-Argument.ps1'
        $exe = (Get-Command pwsh).Source
        $processResult = & (Get-Module ScheduledExecution) {
            param($Executable, $Fixture, $Values, $Root, $Output)
            Invoke-ScheduledProcess -Command @{ file = $Executable
                arguments = @('-NoProfile', '-File', $Fixture) + $Values; environment = @{} } `
                -SourceRoot $Root -OutputDirectory $Output -Name 'argv'
        } $exe $fixture $values $script:root $output
        $processResult.exit_code | Should -Be 0
        @(Get-Content -LiteralPath $processResult.stdout_path -Raw | ConvertFrom-Json) | Should -Be $values
    }
}

Describe 'Trusted execution pins and host platform' {
    It 'selects the legacy stable mutation pin and nightly deep-check pins from controller files' {
        $channel = [regex]::Match((Get-Content -LiteralPath (Join-Path $script:root 'rust-toolchain.toml') -Raw),
            '(?m)^channel = "([^"]+)"').Groups[1].Value
        Get-ScheduledToolchain -Kind mutants | Should -Be $channel
        $nightly = @(Get-Content -LiteralPath (Join-Path $script:root 'constants.env') |
                Where-Object { $_ -cmatch '^RUST_NIGHTLY=' })[0].Substring('RUST_NIGHTLY='.Length)
        foreach ($kind in @('miri', 'miri-many', 'careful')) {
            Get-ScheduledToolchain -Kind $kind | Should -Be $nightly
        }
        $check = New-Check
        $command = Get-ScheduledCommand $check $script:root $script:work $channel
        $command.arguments[0] | Should -Be "+$channel"
        $command.environment.RUSTUP_TOOLCHAIN | Should -Be $channel
        { Get-ScheduledCommand $check $script:root $script:work $nightly } | Should -Throw
        $check.kind = 'miri'
        { Get-ScheduledCommand $check $script:root $script:work $channel } | Should -Throw
    }

    It 'rejects a self-consistent but untrusted per-kind toolchain record' -ForEach @(
        @{ Kind = 'mutants' }, @{ Kind = 'miri' }, @{ Kind = 'miri-many' }, @{ Kind = 'careful' }
    ) {
        $check = New-Check $Kind
        if ($Kind -eq 'mutants') {
            $path = New-Evidence $check
        } else {
            $path = New-TestEvidence $check 0
            Set-Content -LiteralPath (Join-Path $path 'check-0.stdout') -Value 'test result: ok. 1 passed; 0 failed;'
            Set-Content -LiteralPath (Join-Path $path 'check-0.stderr') -Value ''
        }
        (Get-ScheduledCheckResult $check $path $script:context).outcome | Should -BeIn @('passed', 'findings')
        $executionPath = Join-Path $path 'execution.json'
        $execution = Get-Content -LiteralPath $executionPath -Raw | ConvertFrom-Json -AsHashtable
        $execution.toolchain = if ($Kind -eq 'mutants') { '1.95.0' } else { 'nightly-2025-01-01' }
        foreach ($record in $execution.commands) {
            $record.command.arguments[0] = "+$($execution.toolchain)"
            $record.command.environment.RUSTUP_TOOLCHAIN = $execution.toolchain
        }
        Write-Json $execution $executionPath
        $result = Get-ScheduledCheckResult $check $path $script:context
        $result.outcome | Should -Be 'incomplete'
        $result.findings.Count | Should -Be 0
    }

    It 'uses the per-kind controller pin without accepting a context override' -ForEach @(
        @{ Kind = 'mutants' }, @{ Kind = 'miri' }
    ) {
        $check = New-Check $Kind
        $path = if ($Kind -eq 'mutants') { New-Evidence $check } else { New-TestEvidence $check }
        $context = $script:context.Clone()
        (Get-ScheduledCheckResult $check $path $context).outcome | Should -Be 'findings'
        $context.toolchain = Get-ScheduledToolchain -Kind $Kind
        (Get-ScheduledCheckResult $check $path $context).outcome | Should -Be 'findings'
        $executionPath = Join-Path $path 'execution.json'
        $execution = Get-Content -LiteralPath $executionPath -Raw | ConvertFrom-Json -AsHashtable
        $execution.toolchain = if ($Kind -eq 'mutants') { '1.95.0' } else { 'nightly-2025-01-01' }
        $context.toolchain = $execution.toolchain
        $execution.commands[0].command.arguments[0] = "+$($execution.toolchain)"
        $execution.commands[0].command.environment.RUSTUP_TOOLCHAIN = $execution.toolchain
        Write-Json $execution $executionPath
        (Get-ScheduledCheckResult $check $path $context).outcome | Should -Be 'incomplete'
    }

    It 'rejects invalid explicit trusted pins rather than falling back to the artifact' -ForEach @(
        @{ Pin = 'nightly' }, @{ Pin = '' }, @{ Pin = $null }
    ) {
        $check = New-Check
        $path = New-Evidence $check
        $context = $script:context.Clone()
        $context.toolchain = $Pin
        (Get-ScheduledCheckResult $check $path $context).outcome | Should -Be 'incomplete'
    }

    It 'checks the requested OS and OS architecture together' -ForEach @(
        @{ Runner = 'windows-latest'; Os = 'windows'; Architecture = 'X64'; Expected = $true },
        @{ Runner = 'windows-latest'; Os = 'windows'; Architecture = 'Arm64'; Expected = $false },
        @{ Runner = 'windows-11-arm'; Os = 'windows'; Architecture = 'Arm64'; Expected = $true },
        @{ Runner = 'windows-11-arm'; Os = 'windows'; Architecture = 'X64'; Expected = $false },
        @{ Runner = 'ubuntu-latest'; Os = 'linux'; Architecture = 'X64'; Expected = $true },
        @{ Runner = 'ubuntu-latest'; Os = 'linux'; Architecture = 'Arm64'; Expected = $false },
        @{ Runner = 'ubuntu-24.04-arm'; Os = 'linux'; Architecture = 'Arm64'; Expected = $true },
        @{ Runner = 'ubuntu-24.04-arm'; Os = 'linux'; Architecture = 'X64'; Expected = $false },
        @{ Runner = 'windows-latest'; Os = 'linux'; Architecture = 'X64'; Expected = $false },
        @{ Runner = 'ubuntu-latest'; Os = 'windows'; Architecture = 'X64'; Expected = $false },
        @{ Runner = 'windows-latest'; Os = 'windows'; Architecture = 'X86'; Expected = $false },
        @{ Runner = 'ubuntu-unknown'; Os = 'linux'; Architecture = 'X64'; Expected = $false }
    ) {
        $hostPlatform = @{ os = $Os; architecture = $Architecture; process_architecture = 'X64' }
        $actual = & (Get-Module ScheduledExecution) {
            param($Runner, $HostPlatform)
            Test-ScheduledHostPlatform -Runner $Runner -HostPlatform $HostPlatform
        } $Runner $hostPlatform
        $actual | Should -Be $Expected
    }

    It 'captures OS architecture from RuntimeInformation rather than process bitness' {
        $hostPlatform = & (Get-Module ScheduledExecution) { Get-ScheduledHostPlatform }
        $hostPlatform.architecture | Should -Be ([Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString())
        $hostPlatform.os | Should -Be $(if ($IsWindows) { 'windows' } else { 'linux' })
    }

    It 'does not start checks on a runner with the wrong OS architecture' -ForEach @(
        @{ Runner = 'windows-11-arm'; Os = 'windows'; Architecture = 'X64' },
        @{ Runner = 'windows-latest'; Os = 'windows'; Architecture = 'Arm64' },
        @{ Runner = 'ubuntu-24.04-arm'; Os = 'linux'; Architecture = 'X64' },
        @{ Runner = 'ubuntu-latest'; Os = 'linux'; Architecture = 'Arm64' }
    ) {
        Mock -ModuleName ScheduledExecution Get-ScheduledHostPlatform {
            return @{ os = $Os; architecture = $Architecture }
        }
        Mock -ModuleName ScheduledExecution Invoke-ScheduledProcess {
            throw [InvalidOperationException]::new('A mismatched runner must not start native checks.')
        }
        $check = New-Check 'miri'
        $check.platform = $Runner
        $path = New-Output
        $result = Invoke-ScheduledCheck $check $script:root $path $script:toolchain $script:context
        $result.outcome | Should -Be 'not-applicable'
        $result.findings.Count | Should -Be 0
        $execution = Get-Content -LiteralPath (Join-Path $path 'execution.json') -Raw | ConvertFrom-Json -AsHashtable
        $execution.host.architecture | Should -Be $Architecture
        Should -Invoke -ModuleName ScheduledExecution Invoke-ScheduledProcess -Times 0
    }

    It 'does not start checks with a toolchain that disagrees with the trusted pin' -ForEach @(
        @{ Kind = 'mutants'; WrongPin = '1.95.0' }, @{ Kind = 'miri'; WrongPin = 'nightly-2025-01-01' }
    ) {
        Mock -ModuleName ScheduledExecution Invoke-ScheduledProcess {
            throw [InvalidOperationException]::new('A mismatched toolchain must not start native checks.')
        }
        $check = New-Check $Kind
        $path = New-Output
        $result = Invoke-ScheduledCheck $check $script:root $path $WrongPin $script:context
        $result.outcome | Should -Be 'incomplete'
        Should -Invoke -ModuleName ScheduledExecution Invoke-ScheduledProcess -Times 0
    }
}

Describe 'Pinned cargo-mutants output classification' {
    # Handcrafted scenarios follow v27.1.0's Serialize implementations exactly:
    # https://github.com/sourcefrog/cargo-mutants/blob/v27.1.0/src/outcome.rs
    # https://github.com/sourcefrog/cargo-mutants/blob/v27.1.0/src/mutant.rs
    It 'reports missed mutations and timeouts separately from caught mutations' {
        $check = New-Check
        $path = New-Evidence $check
        $result = Get-ScheduledCheckResult $check $path $script:context
        $result.outcome | Should -Be 'findings'
        $result.baseline | Should -Be 'passed'
        $result.findings.Count | Should -Be 2
        $result.findings[1].summary | Should -Match '^Timeout:'
        $result.findings[0].identity.function | Should -Be 'sample'
        $result.findings[0].identity.mutation | Should -Be 'replace sample -> u32 with 0'
        $result.findings[0].replay.replay_mutant.name | Should -Match ':8:5:'
    }

    It 'classifies an unmutated test failure or timeout as blocked, never as a mutant defect' -ForEach @(
        @{ Status = @{ Failure = 101 }; Summary = 'Failure'; Baseline = 'failed' },
        @{ Status = 'Timeout'; Summary = 'Timeout'; Baseline = 'timeout' }
    ) {
        $check = New-Check
        $path = New-Evidence $check 4
        Set-Lab $path {
            param($lab)
            $lab.outcomes[0].phase_results[1].process_status = $Status
            $lab.outcomes[0].summary = $Summary
        }
        $result = Get-ScheduledCheckResult $check $path $script:context
        $result.outcome | Should -Be 'blocked'
        $result.baseline | Should -Be $Baseline
        $result.findings.Count | Should -Be 0
    }

    It 'requires a baseline and complete mutant inventory before declaring a pass' {
        $check = New-Check
        $path = New-Evidence $check
        Set-Lab $path { param($lab) $lab.outcomes = @($lab.outcomes | Select-Object -Skip 1) }
        (Get-ScheduledCheckResult $check $path $script:context).outcome | Should -Be 'incomplete'
        $path = New-Evidence $check
        Set-Lab $path { param($lab) $lab.end_time = $null }
        (Get-ScheduledCheckResult $check $path $script:context).outcome | Should -Be 'incomplete'
    }

    It 'detects summaries that label a timeout as caught' {
        $check = New-Check
        $path = New-Evidence $check
        Set-Lab $path { param($lab) $lab.outcomes[2].summary = 'CaughtMutant' }
        $result = Get-ScheduledCheckResult $check $path $script:context
        $result.outcome | Should -Be 'incomplete'
        $result.findings.Count | Should -Be 0
    }

    It 'accepts caught and unviable mutants when phase results and exit code agree' {
        $check = New-Check
        $path = New-Evidence $check 0
        Set-Lab $path {
            param($lab)
            $lab.outcomes[1].summary = 'CaughtMutant'
            $lab.outcomes[1].phase_results[1].process_status = @{ Failure = 101 }
            $lab.outcomes[2].summary = 'Unviable'
            $lab.outcomes[2].phase_results = @($lab.outcomes[2].phase_results[0])
            $lab.outcomes[2].phase_results[0].process_status = @{ Failure = 101 }
            $lab.missed = 0; $lab.timeout = 0; $lab.caught = 1; $lab.unviable = 1
        }
        (Get-ScheduledCheckResult $check $path $script:context).outcome | Should -Be 'passed'
    }

    It 'does not trust candidate evidence labels or command scope' {
        $check = New-Check
        $path = New-Evidence $check
        Write-Json @{ outcome = 'passed'; findings = @() } (Join-Path $path 'evidence.json')
        (Get-ScheduledCheckResult $check $path $script:context).outcome | Should -Be 'findings'
        $execution = Get-Content -LiteralPath (Join-Path $path 'execution.json') -Raw | ConvertFrom-Json -AsHashtable
        $execution.commands[0].command.arguments += '--baseline=skip'
        Write-Json $execution (Join-Path $path 'execution.json')
        (Get-ScheduledCheckResult $check $path $script:context).outcome | Should -Be 'incomplete'
    }

    It 'keeps incident identity stable across line-only movement and run metadata changes' {
        $check = New-Check
        $path = New-Evidence $check
        $first = Get-ScheduledCheckResult $check $path $script:context
        foreach ($file in @('mutants.json', 'outcomes.json')) {
            $target = Join-Path $path "mutants.out\$file"
            (Get-Content -LiteralPath $target -Raw).Replace(':8:5:', ':18:5:').Replace('"line": 8', '"line": 18') |
                Set-Content -LiteralPath $target
        }

        $context = $script:context.Clone()
        $context.run_id = 999
        $context.source_sha = 'd' * 40
        $second = Get-ScheduledCheckResult $check $path $context
        foreach ($key in $first.findings[0].identity.Keys) {
            $second.findings[0].identity[$key] | Should -Be $first.findings[0].identity[$key]
        }
    }

    It 'reparses evidence moved to an archive directory and keeps the recorded Windows argv intact' {
        $check = New-Check
        $path = New-Evidence $check
        $execution = Get-Content -LiteralPath (Join-Path $path 'execution.json') -Raw | ConvertFrom-Json -AsHashtable
        $execution.source_root = 'D:\a\folo\candidate'
        $execution.output_directory = 'D:\a\folo\evidence'
        $execution.commands[0].command = Get-ScheduledCommand $check $execution.source_root `
            $execution.output_directory (Get-ScheduledToolchain -Kind mutants)
        Write-Json $execution (Join-Path $path 'execution.json')
        (Get-ScheduledCheckResult $check $path $script:context).outcome | Should -Be 'findings'
    }

    It 'rejects malformed, missing, incompatible and truncated raw output' {
        $check = New-Check
        foreach ($mode in @('malformed', 'missing', 'version', 'truncated')) {
            $path = New-Evidence $check
            switch ($mode) {
                'malformed' { Set-Content -LiteralPath (Join-Path $path 'mutants.out\outcomes.json') -Value '{' }
                'missing' { Remove-Item -LiteralPath (Join-Path $path 'mutants.out\mutants.json') }
                'version' { Set-Lab $path { param($lab) $lab.cargo_mutants_version = '0.0.0' } }
                'truncated' { Set-Lab $path { param($lab) $lab.outcomes = @($lab.outcomes[0]); $lab.total_mutants = 0 } }
            }
            (Get-ScheduledCheckResult $check $path $script:context).outcome | Should -Be 'incomplete'
        }
    }
}

Describe 'Verified empty mutation selections' {
    It 'passes an empty ordinary shard only with successful discovery and real baseline evidence' {
        $check = New-Check
        $check.shard = '8/8'
        $path = New-EmptyEvidence $check
        $result = Get-ScheduledCheckResult $check $path $script:context
        $result.outcome | Should -Be 'passed'
        $result.baseline | Should -Be 'passed'
        $result.findings.Count | Should -Be 0
        Test-Path -LiteralPath (Join-Path $path 'mutants.out\outcomes.json') | Should -BeFalse
    }

    It 'rejects incomplete or inconsistent empty-selection proof' -ForEach @(
        @{ Change = 'missing-baseline' }, @{ Change = 'missing-build-output' }, @{ Change = 'missing-test-output' },
        @{ Change = 'missing-discovery' }, @{ Change = 'nonempty-discovery' }, @{ Change = 'failed-discovery' },
        @{ Change = 'wrong-shard' }, @{ Change = 'wrong-pin' }, @{ Change = 'wrong-package' },
        @{ Change = 'changed-config' }, @{ Change = 'skipped-tests' }, @{ Change = 'wrong-tool-version' },
        @{ Change = 'wrong-timeout' }, @{ Change = 'baseline-traversal' }, @{ Change = 'nonzero-mutation-exit' },
        @{ Change = 'failed-raw-tests' }, @{ Change = 'raw-build-error' }, @{ Change = 'unfinished-tests' }
    ) {
        $check = New-Check
        $check.shard = '8/8'
        $path = New-EmptyEvidence $check
        $executionPath = Join-Path $path 'execution.json'
        $execution = Get-Content -LiteralPath $executionPath -Raw | ConvertFrom-Json -AsHashtable
        switch ($Change) {
            'missing-baseline' { $execution.commands = @($execution.commands[0..2]) }
            'missing-build-output' { Remove-Item -LiteralPath (Join-Path $path 'baseline-build.stderr') }
            'missing-test-output' { Remove-Item -LiteralPath (Join-Path $path 'baseline-test.stdout') }
            'missing-discovery' { Remove-Item -LiteralPath (Join-Path $path 'discovery.stdout') }
            'nonempty-discovery' { Copy-Item -LiteralPath (Join-Path $script:fixtures 'mutants.json') -Destination (Join-Path $path 'discovery.stdout') }
            'failed-discovery' { $execution.commands[1].exit_code = 1 }
            'wrong-shard' { $execution.commands[1].command.arguments += '--shard=0/8' }
            'wrong-pin' { $execution.commands[4].command.arguments[0] = '+1.95.0' }
            'wrong-package' { $execution.commands[4].command.arguments += '--package=other' }
            'changed-config' { Add-Content -LiteralPath (Join-Path $path 'mutation-config.toml') -Value 'test_tool = "nextest"' }
            'skipped-tests' { $execution.commands[4].command.arguments += '--no-run' }
            'wrong-tool-version' { Set-Content -LiteralPath (Join-Path $path 'mutants-version.stdout') -Value 'cargo-mutants 26.0.0' }
            'wrong-timeout' { $execution.commands[4].command.timeout_seconds = 1 }
            'baseline-traversal' { $execution.commands[3].name = 'baseline-/../../outside' }
            'nonzero-mutation-exit' { $execution.commands[0].exit_code = 1 }
            'failed-raw-tests' { Set-Content -LiteralPath (Join-Path $path 'baseline-test.stdout') -Value 'test result: FAILED. 0 passed; 1 failed;' }
            'raw-build-error' { Set-Content -LiteralPath (Join-Path $path 'baseline-build.stderr') -Value 'error: compilation failed' }
            'unfinished-tests' { Set-Content -LiteralPath (Join-Path $path 'baseline-test.stdout') -Value 'running 6 tests' }
        }
        Write-Json $execution $executionPath
        $result = Get-ScheduledCheckResult $check $path $script:context
        $result.outcome | Should -Be 'incomplete'
        $result.findings.Count | Should -Be 0
    }

    It 'blocks failed or timed-out unmutated baselines' -ForEach @(
        @{ Phase = 3; Timeout = $false; Baseline = 'failed' },
        @{ Phase = 4; Timeout = $false; Baseline = 'failed' },
        @{ Phase = 4; Timeout = $true; Baseline = 'timeout' }
    ) {
        $check = New-Check
        $path = New-EmptyEvidence $check
        $executionPath = Join-Path $path 'execution.json'
        $execution = Get-Content -LiteralPath $executionPath -Raw | ConvertFrom-Json -AsHashtable
        $execution.commands = @($execution.commands[0..$Phase])
        $execution.commands[$Phase].exit_code = 101
        $execution.commands[$Phase].timed_out = $Timeout
        $execution.exit_code = 101
        Write-Json $execution $executionPath
        $result = Get-ScheduledCheckResult $check $path $script:context
        $result.outcome | Should -Be 'blocked'
        $result.baseline | Should -Be $Baseline
        $result.findings.Count | Should -Be 0
    }

    It 'never certifies a zero-match exact replay even with successful ordinary baseline files' {
        $check = New-Check
        $path = New-EmptyEvidence $check
        $check.replay_mutant = @(Get-Content -LiteralPath (Join-Path $script:fixtures 'mutants.json') -Raw |
                ConvertFrom-Json -AsHashtable)[0]
        $executionPath = Join-Path $path 'execution.json'
        $execution = Get-Content -LiteralPath $executionPath -Raw | ConvertFrom-Json -AsHashtable
        $execution.commands[0].command = Get-ScheduledCommand $check $script:root $path (Get-ScheduledToolchain -Kind mutants)
        Write-Json $execution $executionPath
        (Get-ScheduledCheckResult $check $path $script:context).outcome | Should -Be 'blocked'
    }

    It 'requires nextest completion when configuration selects that test tool' -ForEach @(
        @{ Completion = 'passed'; Expected = 'passed' },
        @{ Completion = 'failed'; Expected = 'incomplete' },
        @{ Completion = 'missing'; Expected = 'incomplete' }
    ) {
        Mock -ModuleName ScheduledExecution Get-ScheduledMutationConfig {
            return @{ test_tool = 'nextest'; profile = 'mutants'; all_features = $true; cap_lints = $false
                no_default_features = $false; features = @(); additional_cargo_args = @('--locked')
                additional_cargo_test_args = @('--tests') }
        }
        $check = New-Check
        $path = New-EmptyEvidence $check
        Set-Content -LiteralPath (Join-Path $path 'baseline-test.stdout') -Value ''
        $stderr = Get-Content -LiteralPath (Join-Path $script:fixtures 'nextest-test.stderr') -Raw
        if ($Completion -eq 'failed') { $stderr = $stderr.Replace('6 passed, 0 skipped', '5 passed, 1 failed') }
        if ($Completion -eq 'missing') { $stderr = $stderr -replace '(?m)^.*Summary.*$', '' }
        Set-Content -LiteralPath (Join-Path $path 'baseline-test.stderr') -Value $stderr
        (Get-ScheduledCheckResult $check $path $script:context).outcome | Should -Be $Expected
    }

    It 'matches captured baseline argv and preserves configuration, features, test tool and helpers' {
        $provenance = Get-Content -LiteralPath (Join-Path $script:fixtures 'empty-baseline-provenance.json') -Raw |
            ConvertFrom-Json -AsHashtable
        $check = New-Check
        $check.packages = @($provenance.package)
        $path = New-EmptyEvidence $check
        $execution = Get-Content -LiteralPath (Join-Path $path 'execution.json') -Raw | ConvertFrom-Json -AsHashtable
        # Captured argv establishes cargo-mutants' option contract; the Rust pin is selected
        # independently by the controller, not by the toolchain used to capture the fixture.
        $pin = Get-ScheduledToolchain -Kind mutants
        $expectedBuild = @("+$pin") + @($provenance.build_argv | Select-Object -Skip 1)
        $expectedTest = @("+$pin") + @($provenance.test_argv | Select-Object -Skip 1)
        $execution.commands[3].command.arguments | Should -Be $expectedBuild
        $execution.commands[4].command.arguments | Should -Be $expectedTest
        $execution.commands[3].command.ContainsKey('timeout_seconds') | Should -BeFalse
        $execution.commands[4].command.timeout_seconds | Should -Be $provenance.test_timeout_seconds
        $check.flags = @('--features=alpha,beta', '--no-default-features', '--profile=custom', '--test-workspace')
        $check.test_filter = 'a "filter";$(throw)'
        $command = $execution.commands[0].command
        $command.environment.CBH_FAKER = 'C:\helper path\faker.exe'
        $configuration = @{ test_tool = 'nextest'; profile = 'mutants'; all_features = $true; cap_lints = $true
            no_default_features = $false; features = @('gamma'); additional_cargo_args = @('--locked')
            additional_cargo_test_args = @('--tests') }
        $baseline = & (Get-Module ScheduledExecution) {
            param($Check, $Command, $Configuration)
            Get-ScheduledEmptyBaselineCommand $Check $Command $Configuration Test
        } $check $command $configuration
        $baseline.arguments | Should -Be @("+$pin", 'nextest', 'run', '--cargo-profile=custom',
            '--verbose', '--package=cpulist', '--no-default-features', '--all-features',
            '--features=alpha,beta', '--features=gamma', '--locked', $check.test_filter, '--tests')
        $baseline.environment.CBH_FAKER | Should -Be $command.environment.CBH_FAKER
        $baseline.environment.INSTA_UPDATE | Should -Be 'no'
        $baseline.environment.INSTA_FORCE_PASS | Should -Be '0'
        $baseline.environment.CARGO_ENCODED_RUSTFLAGS | Should -Be (@('--cfg', 'mutants', '--cap-lints=warn') -join [char]0x1f)
        $check.packages = @()
        $workspace = & (Get-Module ScheduledExecution) {
            param($Check, $Command, $Configuration)
            Get-ScheduledEmptyBaselineCommand $Check $Command $Configuration Build
        } $check $command $configuration
        $workspace.arguments | Should -Contain '--workspace'
        $workspace.arguments | Should -Contain '--no-run'
        $workspace.arguments | Should -Not -Contain $check.test_filter
    }
}

Describe 'Miri and careful evidence' {
    It 'retains the original input scope when parsing the actual pinned Miri canary capture' {
        $provenance = Get-Content -LiteralPath (Join-Path $script:fixtures 'miri-provenance.json') -Raw |
            ConvertFrom-Json -AsHashtable
        $provenance.exit_code | Should -Be 101
        $provenance.toolchain | Should -Match '^nightly-\d{4}-\d{2}-\d{2}$'
        $provenance.argv | Should -Contain '-Zmiri-many-seeds=19..20'
        $source = Get-Content -LiteralPath (Join-Path $script:fixtures $provenance.source) -Raw
        $source | Should -Match 'fn fixture_failure\(\)'
        $check = New-Check 'miri-many'
        $check.seed_range = '19..20'
        $path = New-TestEvidence $check $provenance.exit_code
        $result = Get-ScheduledCheckResult $check $path $script:context
        $result.outcome | Should -Be 'findings'
        $result.findings.Count | Should -Be 1
        $result.findings[0].replay.test_filter | Should -Be ''
        $result.findings[0].replay.seed_range | Should -Be '19..20'
        $result.findings[0].replay.flags.Count | Should -Be 0
    }

    It 'rejects noncanonical record names before reading any package output' -ForEach @(
        @{ Name = 'check-/../../outside' },
        @{ Name = 'check-\..\..\outside' },
        @{ Name = 'check-00' },
        @{ Name = 'check-1' },
        @{ Name = 'CHECK-0' }
    ) {
        Mock -ModuleName ScheduledExecution Get-ScheduledTestResult {
            throw [InvalidOperationException]::new('Package output must not be opened for an invalid record name.')
        }
        $check = New-Check 'miri'
        $path = New-TestEvidence $check
        $executionPath = Join-Path $path 'execution.json'
        $execution = Get-Content -LiteralPath $executionPath -Raw | ConvertFrom-Json -AsHashtable
        $execution.commands[0].name = $Name
        Write-Json $execution $executionPath
        $result = Get-ScheduledCheckResult $check $path $script:context
        $result.outcome | Should -Be 'incomplete'
        $result.findings.Count | Should -Be 0
        Should -Invoke -ModuleName ScheduledExecution Get-ScheduledTestResult -Times 0
    }

    It 'preserves the complete many-seed invocation rather than pairing shared output lines' {
        $check = New-Check 'miri-many'
        $check.packages = @('nm_impl')
        $check.seed_range = '0..32'
        $check.flags = @('-Zmiri-strict-provenance')
        $path = New-TestEvidence $check
        $result = Get-ScheduledCheckResult $check $path $script:context
        $result.outcome | Should -Be 'findings'
        $finding = $result.findings[0]
        $finding.identity.seed | Should -Be ''
        $replay = Get-ScheduledCommand $finding.replay $script:root $path $script:toolchain
        $replay.environment.MIRIFLAGS | Should -Be '-Zmiri-strict-provenance -Zmiri-many-seeds=0..32'
        $finding.replay.test_filter | Should -Be ''
        $finding.identity.test | Should -Be 'lib:nm_impl::<target>'
        $finding.replay.target.kind | Should -Be 'lib'
        $finding.replay.target.name | Should -Be 'nm_impl'
    }

    It 'does not fabricate a seed when many-seed output omits it' {
        $check = New-Check 'miri-many'
        $path = New-TestEvidence $check
        Set-Content -LiteralPath (Join-Path $path 'check-0.stderr') -Value 'error: Undefined Behavior: Data race detected'
        $result = Get-ScheduledCheckResult $check $path $script:context
        $result.outcome | Should -Be 'findings'
        $result.findings[0].identity.seed | Should -Be ''
        $result.findings[0].replay.flags | Should -Be $check.flags
    }

    It 'does not attribute interleaved interpreters or post-suite leaks to the last test' -ForEach @(
        @{ Fixture = 'miri-interleaved'; Kind = 'miri-many' },
        @{ Fixture = 'miri-post-suite'; Kind = 'miri' }
    ) {
        # These diagnostic-order fixtures model shared interpreter streams and delayed leak checks.
        $check = New-Check $Kind
        $check.seed_range = '16..32'
        $check.shard = '1/2'
        $check.test_filter = 'tests::'
        $check.flags = @('-Zmiri-strict-provenance')
        $path = New-TestEvidence $check
        foreach ($extension in @('stdout', 'stderr')) {
            Copy-Item -LiteralPath (Join-Path $script:fixtures "$Fixture.$extension") `
                -Destination (Join-Path $path "check-0.$extension")
        }
        $result = Get-ScheduledCheckResult $check $path $script:context
        $result.outcome | Should -Be 'findings'
        $result.findings.Count | Should -Be 1
        $finding = $result.findings[0]
        $finding.identity.test | Should -Be 'lib:example::<target>'
        $finding.identity.seed | Should -Be ''
        foreach ($field in @('test_filter', 'seed_range', 'shard', 'flags')) {
            $finding.replay[$field] | Should -Be $check[$field]
        }
        $scope = $check.Clone()
        $scope.target = $finding.replay.target
        $original = Get-ScheduledCommand $scope $script:root $path $script:toolchain
        $replay = Get-ScheduledCommand $finding.replay $script:root $path $script:toolchain
        $replay.arguments | Should -Be $original.arguments
        $replay.environment.MIRIFLAGS | Should -Be $original.environment.MIRIFLAGS
    }

    It 'uses already-exact input attribution without letting interleaved diagnostics override it' {
        $check = New-Check 'miri-many'
        $check.test_filter = 'tests::test_a'
        $check.flags = @('-Zmiri-seed=19', '-Zmiri-strict-provenance')
        $check.seed_range = '16..32'
        $check.shard = '1/2'
        $path = New-TestEvidence $check
        Copy-Item -LiteralPath (Join-Path $script:fixtures 'miri-interleaved.stderr') `
            -Destination (Join-Path $path 'check-0.stderr')
        $result = Get-ScheduledCheckResult $check $path $script:context
        $finding = $result.findings[0]
        $finding.identity.test | Should -Be 'lib:example::tests::test_a'
        $finding.identity.seed | Should -Be '19'
        foreach ($field in @('test_filter', 'seed_range', 'shard', 'flags')) {
            $finding.replay[$field] | Should -Be $check[$field]
        }
    }

    It 'separates compiler or tool errors from attributable test failures' {
        $check = New-Check 'careful'
        $path = New-TestEvidence $check
        Set-Content -LiteralPath (Join-Path $path 'check-0.stdout') -Value ''
        Set-Content -LiteralPath (Join-Path $path 'check-0.stderr') -Value 'error[E0425]: cannot find value'
        (Get-ScheduledCheckResult $check $path $script:context).outcome | Should -Be 'execution-error'
        Set-Content -LiteralPath (Join-Path $path 'check-0.stdout') -Value "test tests::works ... FAILED`ntest result: FAILED. 0 passed; 1 failed;"
        (Get-ScheduledCheckResult $check $path $script:context).outcome | Should -Be 'findings'
    }

    It 'distinguishes passed, no tests, and missing summaries' {
        $check = New-Check 'miri'
        $path = New-TestEvidence $check 0
        Set-Content -LiteralPath (Join-Path $path 'check-0.stderr') -Value ''
        foreach ($case in @(
                @{ text = 'test result: ok. 4 passed; 0 failed;'; expected = 'passed' },
                @{ text = 'test result: ok. 0 passed; 0 failed;'; expected = 'passed' },
                @{ text = ''; expected = 'incomplete' })) {
            Set-Content -LiteralPath (Join-Path $path 'check-0.stdout') -Value $case.text
            (Get-ScheduledCheckResult $check $path $script:context).outcome | Should -Be $case.expected
        }
    }

    It 'requires every enabled workspace test target to have a completed command' {
        $check = New-Check 'miri'
        $path = New-TestEvidence $check 0
        $check.packages = @()
        Write-Json @{ version = 1; workspace_members = @('path+example', 'path+other', 'path+binary')
            packages = @(
                @{ id = 'path+example'; name = 'example'; targets = @(@{ kind = @('lib'); name = 'example'; test = $true }) },
                @{ id = 'path+other'; name = 'other'; targets = @(@{ kind = @('lib'); name = 'other'; test = $true }) },
                @{ id = 'path+binary'; name = 'binary'; targets = @(@{ kind = @('bin'); name = 'binary'; test = $true }) }
            ) } (Join-Path $path 'metadata.stdout')
        (Get-ScheduledCheckResult $check $path $script:context).outcome | Should -Be 'incomplete'
    }
}

Describe 'Cargo test target scope' {
    It 'enumerates ordinary lib/bin/integration targets and skips disabled and non-test targets' {
        $check = New-Check 'miri'
        $check.packages = @()
        $path = New-TargetEvidence $check
        $scopes = @(& (Get-Module ScheduledExecution) {
                param($Check, $Path)
                Get-ScheduledTestScope -Check $Check -OutputDirectory $Path
            } $check $path)
        $scopes.Count | Should -Be 4
        @($scopes | ForEach-Object { "$($_.package)/$($_.target.kind)/$($_.target.name)" }) | Should -Be @(
            'binary-only/bin/binary-only', 'example/bin/example-cli', 'example/lib/example', 'example/test/round_trip'
        )
        $check.kind = 'miri-many'
        $manyScopes = @(& (Get-Module ScheduledExecution) {
                param($Check, $Path)
                Get-ScheduledTestScope -Check $Check -OutputDirectory $Path
            } $check $path)
        $manyScopes.Count | Should -Be 1
        $manyScopes[0].package | Should -Be 'example'
        $manyScopes[0].target.kind | Should -Be 'lib'
    }

    It 'uses exact typed Cargo target selectors without broad target flags' -ForEach @(
        @{ Kind = 'lib'; Name = 'example'; Selector = '--lib' },
        @{ Kind = 'bin'; Name = 'example-cli'; Selector = '--bin=example-cli' },
        @{ Kind = 'test'; Name = 'round_trip'; Selector = '--test=round_trip' }
    ) {
        $check = New-Check 'miri'
        $check.target = @{ kind = $Kind; name = $Name }
        $check.flags = @('-Zmiri-seed=19')
        $check.test_filter = 'tests::same_name'
        $command = Get-ScheduledCommand $check $script:root $script:work $script:toolchain
        $command.arguments | Should -Contain $Selector
        $command.arguments | Should -Not -Contain '--tests'
        $command.arguments | Should -Contain '--exact'
        $command.arguments[-2] | Should -Be 'tests::same_name'
        $command.environment.MIRIFLAGS | Should -Be '-Zmiri-seed=19'
    }

    It 'keeps target names as one argv value and rejects invalid many-seed target selectors' {
        $check = New-Check 'miri'
        $check.target = @{ kind = 'test'; name = 'quoted "name";$(throw)' }
        $command = Get-ScheduledCommand $check $script:root $script:work $script:toolchain
        $command.arguments | Should -Contain '--test=quoted "name";$(throw)'
        $check.kind = 'miri-many'
        { Get-ScheduledCommand $check $script:root $script:work $script:toolchain } | Should -Throw
        $check.kind = 'miri'
        $check.packages = @('example', 'binary-only')
        { Get-ScheduledCommand $check $script:root $script:work $script:toolchain } | Should -Throw
    }

    It 'keeps identically named failures in distinct binaries separate through reporter fingerprints and replay' {
        Import-Module (Join-Path $PSScriptRoot 'ScheduledReport.psm1')
        $check = New-Check 'miri'
        $check.packages = @()
        $path = New-TargetEvidence $check
        $result = Get-ScheduledCheckResult $check $path $script:context
        $result.outcome | Should -Be 'findings'
        $result.findings.Count | Should -Be 4
        $ids = @($result.findings | ForEach-Object { Get-ScheduledFindingId -Repository 'folo-rs/folo' -Identity $_.identity })
        @($ids | Select-Object -Unique).Count | Should -Be 4
        foreach ($finding in $result.findings) {
            $finding.replay.test_filter | Should -Be ''
            $finding.identity.seed | Should -Be ''
            $finding.identity.target.kind | Should -Be $finding.replay.target.kind
            $finding.identity.target.name | Should -Be $finding.replay.target.name
            $command = Get-ScheduledCommand $finding.replay $script:root $path $script:toolchain
            $selector = if ($finding.replay.target.kind -eq 'lib') { '--lib' }
                else { "--$($finding.replay.target.kind)=$($finding.replay.target.name)" }
            $command.arguments | Should -Contain $selector
            $command.arguments | Should -Not -Contain '--exact'
            $command.arguments | Should -Not -Contain 'tests::same_name'
            $command.environment.MIRIFLAGS | Should -Be ''
            $replayPath = New-TargetEvidence $finding.replay 0
            $confirmed = Get-ScheduledCheckResult $finding.replay $replayPath $script:context
            $confirmed.outcome | Should -Be 'passed'
            $confirmed.actual_scope.target.name | Should -Be $finding.replay.target.name
        }
    }

    It 'distinguishes targets with the same target name and source file but a different kind' {
        Import-Module (Join-Path $PSScriptRoot 'ScheduledReport.psm1')
        $check = New-Check 'miri'
        $path = New-TargetEvidence $check
        $metadataPath = Join-Path $path 'metadata.stdout'
        $metadata = Get-Content -LiteralPath $metadataPath -Raw | ConvertFrom-Json -AsHashtable
        $metadata.packages[0].targets[1].name = 'example'
        $metadata.packages[0].targets[1].src_path = $metadata.packages[0].targets[0].src_path
        Write-Json $metadata $metadataPath
        $executionPath = Join-Path $path 'execution.json'
        $execution = Get-Content -LiteralPath $executionPath -Raw | ConvertFrom-Json -AsHashtable
        $execution.commands[0].target.name = 'example'
        $scope = $check.Clone()
        $scope.target = $execution.commands[0].target
        $execution.commands[0].command = Get-ScheduledCommand $scope $script:root $path $script:toolchain
        Write-Json $execution $executionPath
        $result = Get-ScheduledCheckResult $check $path $script:context
        $result.outcome | Should -Be 'findings'
        $binaryId = Get-ScheduledFindingId -Repository 'folo-rs/folo' -Identity $result.findings[0].identity
        $libraryId = Get-ScheduledFindingId -Repository 'folo-rs/folo' -Identity $result.findings[1].identity
        $binaryId | Should -Not -Be $libraryId
        $result.findings[0].replay.target.kind | Should -Be 'bin'
        $result.findings[1].replay.target.kind | Should -Be 'lib'
    }

    It 'requires complete metadata-backed target records rather than one record per package' {
        $check = New-Check 'miri'
        $path = New-TargetEvidence $check 0
        $executionPath = Join-Path $path 'execution.json'
        $execution = Get-Content -LiteralPath $executionPath -Raw | ConvertFrom-Json -AsHashtable
        $execution.commands = @($execution.commands[1])
        $execution.commands[0].name = 'check-0'
        Write-Json $execution $executionPath
        (Get-ScheduledCheckResult $check $path $script:context).outcome | Should -Be 'incomplete'
    }

    It 'rejects target substitutions and records missing target identity' -ForEach @(
        @{ Change = 'substitution' }, @{ Change = 'missing' }
    ) {
        $check = New-Check 'miri'
        $path = New-TargetEvidence $check 0
        $executionPath = Join-Path $path 'execution.json'
        $execution = Get-Content -LiteralPath $executionPath -Raw | ConvertFrom-Json -AsHashtable
        if ($Change -eq 'substitution') { $execution.commands[0].target.name = 'round_trip' }
        else { $execution.commands[0].Remove('target') }
        Write-Json $execution $executionPath
        (Get-ScheduledCheckResult $check $path $script:context).outcome | Should -Be 'incomplete'
    }

    It 'does not declare a replay fixed when its exact test did not run' {
        $check = New-Check 'miri'
        $check.target = @{ kind = 'test'; name = 'round_trip' }
        $check.test_filter = 'tests::same_name'
        $check.flags = @('-Zmiri-seed=19')
        $path = New-TargetEvidence $check 0
        Set-Content -LiteralPath (Join-Path $path 'check-0.stdout') -Value 'test result: ok. 0 passed; 0 failed;'
        (Get-ScheduledCheckResult $check $path $script:context).outcome | Should -Be 'not-applicable'
    }

    It 'retains no-tests-pass for an ordinary scope whose declared targets all disable tests' {
        $check = New-Check 'miri'
        $check.packages = @('test-support')
        $path = New-TargetEvidence $check 0
        (Get-ScheduledCheckResult $check $path $script:context).outcome | Should -Be 'passed'
        $check.target = @{ kind = 'bin'; name = 'test-support' }
        (Get-ScheduledCheckResult $check $path $script:context).outcome | Should -Be 'incomplete'
    }

    It 'executes and reparses every selected target using canonical output names' -ForEach @(
        @{ CheckKind = 'miri'; ExpectedCount = 4 },
        @{ CheckKind = 'miri-many'; ExpectedCount = 1 }
    ) {
        Mock -ModuleName ScheduledExecution Invoke-ScheduledProcess {
            param($Command, $SourceRoot, $OutputDirectory, $Name)
            $stdout = Join-Path $OutputDirectory "$Name.stdout"
            $stderr = Join-Path $OutputDirectory "$Name.stderr"
            Set-Content -LiteralPath $stderr -Value ''
            if ($Name -eq 'metadata') {
                Copy-Item -LiteralPath (Join-Path $PSScriptRoot 'fixtures\execution\target-metadata.json') -Destination $stdout
            } elseif ($Name -match '^check-\d+$') {
                Set-Content -LiteralPath $stdout -Value 'test result: ok. 1 passed; 0 failed;'
            } else { throw [InvalidOperationException]::new("Unexpected call: $Name $SourceRoot $($Command.file)") }
            return @{ exit_code = 0; stdout_path = $stdout; stderr_path = $stderr }
        }
        $check = New-Check $CheckKind
        $check.packages = @()
        $path = New-Output
        $result = Invoke-ScheduledCheck $check $script:root $path $script:toolchain $script:context
        $result.outcome | Should -Be 'passed'
        $execution = Get-Content -LiteralPath (Join-Path $path 'execution.json') -Raw | ConvertFrom-Json -AsHashtable
        $runs = @($execution.commands | Where-Object { $_.name -like 'check-*' })
        $runs.Count | Should -Be $ExpectedCount
        for ($index = 0; $index -lt $ExpectedCount; $index++) {
            $runs[$index].name | Should -Be "check-$index"
            $runs[$index].command.arguments | Should -Not -Contain '--tests'
            Test-Path -LiteralPath (Join-Path $path "check-$index.stdout") | Should -BeTrue
        }
        if ($CheckKind -eq 'miri') {
            $runs[0].package | Should -Be 'binary-only'
            $runs[0].command.arguments | Should -Contain '--bin=binary-only'
            $runs[-1].command.arguments | Should -Contain '--test=round_trip'
        } else {
            $runs[0].target.kind | Should -Be 'lib'
            $runs[0].command.arguments | Should -Contain '--lib'
            $runs[0].command.environment.MIRIFLAGS | Should -Be '-Zmiri-many-seeds=..64'
        }
        Should -Invoke -ModuleName ScheduledExecution Invoke-ScheduledProcess -Times $ExpectedCount -ParameterFilter {
            $Name -like 'check-*'
        }
    }
}

Describe 'Execution failure evidence and replay preflight' {
    BeforeEach {
        Mock -ModuleName ScheduledExecution Invoke-ScheduledProcess {
            param($Command, $SourceRoot, $OutputDirectory, $Name)
            $stdout = Join-Path $OutputDirectory "$Name.stdout"
            $stderr = Join-Path $OutputDirectory "$Name.stderr"
            Set-Content -LiteralPath $stderr -Value ''
            if ($Name -eq 'discovery') {
                Set-Content -LiteralPath $stdout -Value '[]'
            } elseif ($Name -in @('cargo-bench-history-faker', 'dure-test-helper')) {
                $exe = Join-Path $OutputDirectory "$Name.exe"
                Set-Content -LiteralPath $exe -Value 'fake helper'
                @{ reason = 'compiler-artifact'; target = @{ name = $Name }; executable = $exe } |
                    ConvertTo-Json -Compress | Set-Content -LiteralPath $stdout
            } else {
                throw [InvalidOperationException]::new("Unexpected native call: $Name $SourceRoot $($Command.file)")
            }
            return @{ exit_code = 0; stdout_path = $stdout; stderr_path = $stderr }
        }
    }

    It 'blocks zero-match replay before any mutation or baseline execution and writes evidence' {
        $check = New-Check
        $check.replay_mutant = @(Get-Content -LiteralPath (Join-Path $script:fixtures 'mutants.json') -Raw |
                ConvertFrom-Json -AsHashtable)[0]
        $path = New-Output
        $result = Invoke-ScheduledCheck $check $script:root $path (Get-ScheduledToolchain -Kind mutants) $script:context
        $result.outcome | Should -Be 'blocked'
        $result.baseline | Should -Be 'not-run'
        Test-Path -LiteralPath (Join-Path $path 'evidence.json') | Should -BeTrue
        Test-Path -LiteralPath (Join-Path $path 'check.log') | Should -BeTrue
        Should -Invoke -ModuleName ScheduledExecution Invoke-ScheduledProcess -Times 1 -ParameterFilter {
            $Name -eq 'discovery' -and $Command.arguments -contains '--list' -and $Command.arguments -contains '--json'
        }
        Should -Invoke -ModuleName ScheduledExecution Invoke-ScheduledProcess -Times 0 -ParameterFilter { $Name -eq 'check' }
    }

    It 'captures process startup errors without pretending they were test findings' {
        Mock -ModuleName ScheduledExecution Invoke-ScheduledProcess {
            throw [ComponentModel.Win32Exception]::new(2)
        }
        $check = New-Check
        $path = New-Output
        $result = Invoke-ScheduledCheck $check $script:root $path (Get-ScheduledToolchain -Kind mutants) $script:context
        $result.outcome | Should -Be 'execution-error'
        $saved = Get-Content -LiteralPath (Join-Path $path 'evidence.json') -Raw | ConvertFrom-Json -AsHashtable
        $saved.outcome | Should -Be 'execution-error'
        $saved.actual_scope.id | Should -Be $check.id
        $saved.findings.Count | Should -Be 0
    }

    It 'runs a validated nonzero replay with a real baseline command and persists the findings' {
        Mock -ModuleName ScheduledExecution Invoke-ScheduledProcess {
            param($Command, $SourceRoot, $OutputDirectory, $Name)
            $fixtureRoot = Join-Path $PSScriptRoot 'fixtures\execution'
            $stdout = Join-Path $OutputDirectory "$Name.stdout"
            $stderr = Join-Path $OutputDirectory "$Name.stderr"
            Set-Content -LiteralPath $stderr -Value ''
            $exitCode = 0
            if ($Name -in @('cargo-bench-history-faker', 'dure-test-helper')) {
                $exe = Join-Path $OutputDirectory "$Name.exe"
                Set-Content -LiteralPath $exe -Value 'fake helper'
                @{ reason = 'compiler-artifact'; target = @{ name = $Name }; executable = $exe } |
                    ConvertTo-Json -Compress | Set-Content -LiteralPath $stdout
            } elseif ($Name -eq 'discovery') {
                $inventory = @(Get-Content -LiteralPath (Join-Path $fixtureRoot 'mutants.json') -Raw | ConvertFrom-Json -AsHashtable)
                ConvertTo-Json -InputObject @($inventory[0]) -Depth 100 | Set-Content -LiteralPath $stdout
            } elseif ($Name -eq 'check') {
                $null = New-Item -ItemType Directory -Path (Join-Path $OutputDirectory 'mutants.out')
                Copy-Item -LiteralPath (Join-Path $OutputDirectory 'discovery.stdout') `
                    -Destination (Join-Path $OutputDirectory 'mutants.out\mutants.json')
                $lab = Get-Content -LiteralPath (Join-Path $fixtureRoot 'outcomes.json') -Raw | ConvertFrom-Json -AsHashtable
                $lab.outcomes = @($lab.outcomes[0], $lab.outcomes[1])
                $lab.total_mutants = 1; $lab.timeout = 0
                ConvertTo-Json -InputObject $lab -Depth 100 |
                    Set-Content -LiteralPath (Join-Path $OutputDirectory 'mutants.out\outcomes.json')
                Set-Content -LiteralPath $stdout -Value 'one missed mutant'
                $exitCode = 2
            } else { throw [InvalidOperationException]::new("Unexpected call: $Name $SourceRoot $($Command.file)") }
            return @{ exit_code = $exitCode; stdout_path = $stdout; stderr_path = $stderr }
        }
        $check = New-Check
        $check.replay_mutant = @(Get-Content -LiteralPath (Join-Path $script:fixtures 'mutants.json') -Raw |
                ConvertFrom-Json -AsHashtable)[0]
        $path = New-Output
        $result = Invoke-ScheduledCheck $check $script:root $path (Get-ScheduledToolchain -Kind mutants) $script:context
        $result.outcome | Should -Be 'findings'
        $result.baseline | Should -Be 'passed'
        $result.findings.Count | Should -Be 1
        Should -Invoke -ModuleName ScheduledExecution Invoke-ScheduledProcess -Times 1 -ParameterFilter {
            $Name -eq 'check' -and $Command.arguments -contains '--baseline=run' -and
            $Command.arguments -notcontains '--json' -and $Command.environment.ContainsKey('CBH_FAKER')
        }
    }

    It 'executes verified-empty ordinary shards through explicit unmutated baseline phases' -ForEach @(
        @{ FailurePhase = ''; TimedOut = $false; Expected = 'passed'; Baseline = 'passed'; TestCalls = 1 },
        @{ FailurePhase = 'baseline-build'; TimedOut = $false; Expected = 'blocked'; Baseline = 'failed'; TestCalls = 0 },
        @{ FailurePhase = 'baseline-test'; TimedOut = $false; Expected = 'blocked'; Baseline = 'failed'; TestCalls = 1 },
        @{ FailurePhase = 'baseline-test'; TimedOut = $true; Expected = 'blocked'; Baseline = 'timeout'; TestCalls = 1 }
    ) {
        Mock -ModuleName ScheduledExecution Invoke-ScheduledProcess {
            param($Command, $SourceRoot, $OutputDirectory, $Name)
            $stdout = Join-Path $OutputDirectory "$Name.stdout"
            $stderr = Join-Path $OutputDirectory "$Name.stderr"
            Set-Content -LiteralPath $stdout, $stderr -Value ''
            if ($Name -in @('cargo-bench-history-faker', 'dure-test-helper')) {
                $exe = Join-Path $OutputDirectory "$Name.exe"
                Set-Content -LiteralPath $exe -Value 'fake helper'
                @{ reason = 'compiler-artifact'; target = @{ name = $Name }; executable = $exe } |
                    ConvertTo-Json -Compress | Set-Content -LiteralPath $stdout
            } elseif ($Name -eq 'check') {
                $null = New-Item -ItemType Directory -Path (Join-Path $OutputDirectory 'mutants.out')
                Set-Content -LiteralPath (Join-Path $OutputDirectory 'mutants.out\mutants.json') -Value '[]'
            } elseif ($Name -eq 'discovery') {
                Set-Content -LiteralPath $stdout -Value '[]'
            } elseif ($Name -eq 'mutants-version') {
                Set-Content -LiteralPath $stdout -Value 'cargo-mutants 27.1.0'
            } elseif ($Name -eq 'baseline-test') {
                Copy-Item -LiteralPath (Join-Path $PSScriptRoot 'fixtures\execution\baseline-test.stdout') -Destination $stdout
            } elseif ($Name -eq 'baseline-build') {
                Copy-Item -LiteralPath (Join-Path $PSScriptRoot 'fixtures\execution\baseline-build.stderr') -Destination $stderr
            } else {
                throw [InvalidOperationException]::new("Unexpected native call: $Name $SourceRoot $($Command.file)")
            }
            return @{ exit_code = if ($Name -eq $FailurePhase) { 101 } else { 0 }
                timed_out = $Name -eq $FailurePhase -and $TimedOut; stdout_path = $stdout; stderr_path = $stderr }
        }
        $check = New-Check
        $check.shard = '8/8'
        $path = New-Output
        $result = Invoke-ScheduledCheck $check $script:root $path (Get-ScheduledToolchain -Kind mutants) $script:context
        $result.outcome | Should -Be $Expected
        $result.baseline | Should -Be $Baseline
        $result.findings.Count | Should -Be 0
        (Get-ScheduledCheckResult $check $path $script:context).outcome | Should -Be $Expected
        foreach ($name in @('evidence.json', 'check.log', 'mutation-config.toml', 'discovery.stdout', 'mutants-version.stdout')) {
            Test-Path -LiteralPath (Join-Path $path $name) | Should -BeTrue
        }
        Should -Invoke -ModuleName ScheduledExecution Invoke-ScheduledProcess -Times 1 -ParameterFilter {
            $Name -eq 'baseline-build' -and $Command.arguments -contains '--no-run' -and
            $Command.environment.ContainsKey('CBH_FAKER') -and -not $Command.ContainsKey('timeout_seconds')
        }
        Should -Invoke -ModuleName ScheduledExecution Invoke-ScheduledProcess -Times $TestCalls -ParameterFilter {
            $Name -eq 'baseline-test' -and $Command.arguments -contains '--tests' -and $Command.timeout_seconds -eq 60
        }
    }

    It 'preserves replay flags through the entire package-execution path and captures a nonzero result' {
        Mock -ModuleName ScheduledExecution Invoke-ScheduledProcess {
            param($Command, $SourceRoot, $OutputDirectory, $Name)
            $stdout = Join-Path $OutputDirectory "$Name.stdout"
            $stderr = Join-Path $OutputDirectory "$Name.stderr"
            Set-Content -LiteralPath $stderr -Value ''
            $exitCode = 0
            if ($Name -eq 'metadata') {
                @{ version = 1; packages = @(@{ id = 'path+nm_impl'; name = 'nm_impl'
                        targets = @(@{ kind = @('lib'); name = 'nm_impl'; test = $true }) }); workspace_members = @('path+nm_impl') } |
                    ConvertTo-Json -Depth 100 | Set-Content -LiteralPath $stdout
            } elseif ($Name -eq 'check-0') {
                Copy-Item -LiteralPath (Join-Path $PSScriptRoot 'fixtures\execution\miri.stdout') -Destination $stdout
                Copy-Item -LiteralPath (Join-Path $PSScriptRoot 'fixtures\execution\miri.stderr') -Destination $stderr
                $exitCode = 1
            } else { throw [InvalidOperationException]::new("Unexpected call: $Name $SourceRoot $($Command.file)") }
            return @{ exit_code = $exitCode; stdout_path = $stdout; stderr_path = $stderr }
        }
        $check = New-Check 'miri-many'
        $check.packages = @('nm_impl')
        $check.flags = @('-Zmiri-seed=19', '-Zmiri-strict-provenance')
        $check.seed_range = '32..64'
        $check.test_filter = 'fixture_failure'
        $path = New-Output
        $result = Invoke-ScheduledCheck $check $script:root $path $script:toolchain $script:context
        $result.outcome | Should -Be 'findings'
        $result.findings[0].identity.seed | Should -Be '19'
        Should -Invoke -ModuleName ScheduledExecution Invoke-ScheduledProcess -Times 1 -ParameterFilter {
            $Name -eq 'check-0' -and $Command.environment.MIRIFLAGS -eq '-Zmiri-seed=19 -Zmiri-strict-provenance' -and
            $Command.arguments -contains '--exact'
        }
    }
}
