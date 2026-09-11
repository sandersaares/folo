#requires -Version 7

# ScheduledExecution invokes these controller-owned commands with argument arrays, never a shell.
# PowerShell owns process setup and capture; the small Rust TOML utility is built lazily only when
# cargo-mutants omits an empty shard's baseline. No checker tooling is needed during planning.
# Ref: .github/workflows/implementation.md#immutable-execution.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
Import-Module (Join-Path $PSScriptRoot '..\build\Mutants.psm1')
Import-Module (Join-Path $PSScriptRoot '..\build\Miri.psm1')
Import-Module (Join-Path $PSScriptRoot '..\build\CargoExecutable.psm1')

function Get-ScheduledToolchain {
    [CmdletBinding()]
    [OutputType([string])]
    param([Parameter(Mandatory)][ValidateSet('mutants', 'miri', 'miri-many', 'careful')][string] $Kind)

    if ($Kind -ceq 'mutants') {
        $text = Get-Content -LiteralPath (Join-Path $PSScriptRoot '..\..\rust-toolchain.toml') -Raw
        $pins = [regex]::Matches($text, '(?m)^\s*channel\s*=\s*"(\d+\.\d+\.\d+)"\s*$')
        if ($pins.Count -ne 1) { throw 'Expected one stable controller toolchain pin.' }
        return $pins[0].Groups[1].Value
    }
    $text = Get-Content -LiteralPath (Join-Path $PSScriptRoot '..\..\constants.env') -Raw
    $pins = [regex]::Matches($text, '(?m)^RUST_NIGHTLY=(nightly-\d{4}-\d{2}-\d{2})\s*$')
    if ($pins.Count -ne 1) { throw 'Expected one nightly controller toolchain pin.' }
    return $pins[0].Groups[1].Value
}

function Assert-ScheduledPlatform {
    [CmdletBinding()]
    param([Parameter(Mandatory)][string] $Runner)

    $architecture = [Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString()
    $supported = switch -CaseSensitive ($Runner) {
        'ubuntu-latest' { $IsLinux -and $architecture -ceq 'X64' }
        'windows-latest' { $IsWindows -and $architecture -ceq 'X64' }
        'ubuntu-24.04-arm' { $IsLinux -and $architecture -ceq 'Arm64' }
        'windows-11-arm' { $IsWindows -and $architecture -ceq 'Arm64' }
        default { $false }
    }
    if (-not $supported) { throw "The current OS/architecture cannot execute $Runner." }
}

function Get-ScheduledCommand {
    [CmdletBinding()]
    [OutputType([hashtable])]
    param(
        [Parameter(Mandatory)][hashtable] $Check,
        [Parameter(Mandatory)][string] $OutputDirectory,
        [Parameter(Mandatory)][string] $Toolchain
    )

    $arguments = @("+$Toolchain")
    $environment = @{
        RUSTUP_TOOLCHAIN = $Toolchain; CARGO_TERM_COLOR = 'never'; NO_COLOR = '1'
        CARGO_ENCODED_RUSTFLAGS = $null; RUSTFLAGS = ''; RUSTDOCFLAGS = ''; MIRIFLAGS = ''
        MUTATION_TESTING = $null; RUST_TEST_THREADS = '1'
    }
    $packages = @()
    if ($Check.packages.Count -eq 0) { $packages += '--workspace' }
    foreach ($packageName in $Check.packages) { $packages += "--package=$packageName" }
    if ($Check.kind -eq 'mutants') {
        $arguments += @('mutants') + $packages
        $arguments += @(Get-MutantsExcludeArgument -IsWindowsPlatform ($Check.platform -like 'windows-*') `
                -IsLinuxPlatform ($Check.platform -like 'ubuntu-*') -Literal)
        $arguments += @(Get-MutantsShardArgument -Spec $Check.shard)
        # The baseline and mutants share the reviewed configuration, not candidate overrides.
        $arguments += @('--config', [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..\..\.cargo\mutants.toml')),
            '--baseline=run', '--timeout=60', '--no-shuffle', '--caught', '--unviable', '--jobs=1',
            '--output', $OutputDirectory)
        $environment.MUTATION_TESTING = '1'
        $environment.RUSTFLAGS = '--cfg mutants'
        # Each mutant needs its own build tree; inherited CARGO_TARGET_DIR aliases those trees.
        $environment.CARGO_TARGET_DIR = $null
    } else {
        if ($Check.kind -eq 'careful') {
            $arguments += @('careful', 'test') + $packages + @('--no-fail-fast', '--all-features', '--locked')
        } elseif ($Check.kind -in @('miri', 'miri-many')) {
            $arguments += @('miri', 'test') + $packages + @('--no-fail-fast', '--all-features', '--locked')
            if ($Check.ContainsKey('target') -and $null -ne $Check.target) {
                $arguments += if ($Check.target.kind -ceq 'lib') { '--lib' }
                    else { "--$($Check.target.kind)=$($Check.target.name)" }
            } elseif ($Check.kind -ceq 'miri-many') { $arguments += '--lib' }
            else { $arguments += '--tests' }
            $environment.MIRIFLAGS = @(Get-MiriFlag -SeedRange $Check.seed_range `
                    -Shard $Check.shard -Many:($Check.kind -ceq 'miri-many')) -join ' '
        } else { throw "Unknown check kind: $($Check.kind)" }
        $arguments += @('--', '--test-threads=1')
    }
    return @{
        file = (Get-Command cargo -CommandType Application | Select-Object -First 1).Source
        arguments = [string[]]$arguments
        environment = $environment
    }
}

function Invoke-ScheduledProcess {
    # Drain both streams concurrently into ordinary files; large checker output cannot fill a pipe.
    [CmdletBinding()]
    [OutputType([hashtable])]
    param(
        [Parameter(Mandatory)][hashtable] $Command,
        [Parameter(Mandatory)][string] $SourceRoot,
        [Parameter(Mandatory)][string] $OutputDirectory,
        [Parameter(Mandatory)][string] $Name
    )

    $start = [Diagnostics.ProcessStartInfo]::new()
    $start.FileName = $Command.file
    $start.WorkingDirectory = $SourceRoot
    $start.UseShellExecute = $false
    $start.RedirectStandardOutput = $true
    $start.RedirectStandardError = $true
    $start.RedirectStandardInput = $Command.ContainsKey('input_text')
    foreach ($argument in $Command.arguments) { $start.ArgumentList.Add($argument) }
    foreach ($key in $Command.environment.Keys) {
        if ($null -eq $Command.environment[$key]) { $null = $start.Environment.Remove($key) }
        else { $start.Environment[$key] = $Command.environment[$key] }
    }
    $process = [Diagnostics.Process]::new()
    $process.StartInfo = $start
    $stdoutPath = Join-Path $OutputDirectory "$Name.stdout"
    $stderrPath = Join-Path $OutputDirectory "$Name.stderr"
    $stdout = [IO.File]::Create($stdoutPath)
    $stderr = [IO.File]::Create($stderrPath)
    try {
        if (-not $process.Start()) { throw 'Could not start the check.' }
        $copyOut = $process.StandardOutput.BaseStream.CopyToAsync($stdout)
        $copyErr = $process.StandardError.BaseStream.CopyToAsync($stderr)
        if ($start.RedirectStandardInput) {
            $process.StandardInput.Write($Command.input_text)
            $process.StandardInput.Close()
        }
        $timedOut = $false
        if ($Command.ContainsKey('timeout_seconds')) {
            $timedOut = -not $process.WaitForExit([int]($Command.timeout_seconds * 1000))
            if ($timedOut) { $process.Kill($true); $process.WaitForExit() }
        } else { $process.WaitForExit() }
        $null = $copyOut.GetAwaiter().GetResult()
        $null = $copyErr.GetAwaiter().GetResult()
        return @{
            exit_code = if ($timedOut) { 1 } else { $process.ExitCode }
            timed_out = $timedOut; stdout_path = $stdoutPath; stderr_path = $stderrPath
        }
    } finally {
        $stdout.Dispose()
        $stderr.Dispose()
        $process.Dispose()
    }
}

function Get-ScheduledTestScope {
    # Miri does not support proc-macro unit harnesses. Other enabled lib/bin/integration targets
    # each run separately so diagnostics name their package and target; careful includes doctests.
    [CmdletBinding()]
    [OutputType([hashtable[]])]
    param(
        [Parameter(Mandatory)][hashtable] $Check,
        [Parameter(Mandatory)][hashtable] $Metadata
    )

    $members = @($Metadata.packages | Where-Object { $_.id -cin $Metadata.workspace_members })
    if ($members.Count -eq 0) { throw 'Cargo metadata has no workspace packages.' }
    foreach ($packageName in $Check.packages) {
        if ($packageName -cnotin @($members.name)) { throw "Unknown workspace package: $packageName" }
    }
    foreach ($package in $members) {
        if ($Check.packages.Count -gt 0 -and $package.name -cnotin $Check.packages) { continue }
        if ($Check.kind -eq 'careful') {
            @{ package = $package.name; target = $null }
            continue
        }
        foreach ($cargoTarget in $package.targets) {
            if (-not $cargoTarget.test -or 'proc-macro' -cin $cargoTarget.kind) { continue }
            $kind = if (@($cargoTarget.kind | Where-Object {
                        $_ -cin @('lib', 'rlib', 'dylib', 'cdylib', 'staticlib')
                    }).Count -gt 0) { 'lib' }
                elseif ('bin' -cin $cargoTarget.kind) { 'bin' }
                elseif ('test' -cin $cargoTarget.kind) { 'test' }
                else { continue }
            if ($Check.kind -eq 'miri-many' -and $kind -cne 'lib') { continue }
            @{ package = $package.name; target = @{ kind = $kind; name = $cargoTarget.name } }
        }
    }
}

function Get-ScheduledMutationConfig {
    [CmdletBinding()]
    [OutputType([hashtable])]
    param([Parameter(Mandatory)][string] $OutputDirectory)

    $root = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..\..'))
    $toolchain = Get-ScheduledToolchain -Kind mutants
    $command = @{
        file = (Get-Command cargo -CommandType Application | Select-Object -First 1).Source
        arguments = @("+$toolchain", 'build', '--locked', '--package', 'scheduled-mutation-config',
            '--target-dir', (Join-Path $root 'target\scheduled-mutation-config'), '--message-format=json')
        environment = @{ RUSTUP_TOOLCHAIN = $toolchain; RUSTFLAGS = ''; CARGO_ENCODED_RUSTFLAGS = $null }
    }
    $build = Invoke-ScheduledProcess -Command $command -SourceRoot $root `
        -OutputDirectory $OutputDirectory -Name 'mutation-config-build'
    if ($build.exit_code -ne 0) {
        throw "Could not build mutation configuration parser: $(Get-Content -LiteralPath $build.stderr_path -Raw)"
    }
    $executable = Resolve-CargoExecutable -CargoMessage @(Get-Content -LiteralPath $build.stdout_path) `
        -TargetName 'scheduled-mutation-config'
    $configPath = Join-Path $root '.cargo\mutants.toml'
    Copy-Item -LiteralPath $configPath -Destination (Join-Path $OutputDirectory 'mutation-config.toml')
    $decode = @{
        file = $executable; arguments = @(); environment = @{}
        input_text = Get-Content -LiteralPath $configPath -Raw
    }
    $result = Invoke-ScheduledProcess -Command $decode -SourceRoot $root `
        -OutputDirectory $OutputDirectory -Name 'mutation-config'
    if ($result.exit_code -ne 0) {
        throw "Could not decode baseline configuration: $(Get-Content -LiteralPath $result.stderr_path -Raw)"
    }
    return Get-Content -LiteralPath $result.stdout_path -Raw | ConvertFrom-Json -AsHashtable
}

function Get-ScheduledBaselineCommand {
    [CmdletBinding()]
    [OutputType([hashtable])]
    param(
        [Parameter(Mandatory)][hashtable] $Check,
        [Parameter(Mandatory)][hashtable] $MutationCommand,
        [Parameter(Mandatory)][hashtable] $Configuration,
        [Parameter(Mandatory)][ValidateSet('Build', 'Test')][string] $Phase
    )

    # Mirror cargo-mutants' baseline settings, including cfg, feature/profile selection,
    # nextest support, snapshot safeguards and test timeout. Empty shards still test real code.
    $arguments = @($MutationCommand.arguments[0])
    $arguments += if ($Configuration.test_tool -ceq 'nextest') { @('nextest', 'run') } else { 'test' }
    if ($Phase -ceq 'Build') { $arguments += '--no-run' }
    if ($null -ne $Configuration.profile) {
        $arguments += if ($Configuration.test_tool -ceq 'nextest') { "--cargo-profile=$($Configuration.profile)" }
            else { "--profile=$($Configuration.profile)" }
    }
    $arguments += '--verbose'
    if ($Check.packages.Count -eq 0) { $arguments += '--workspace' }
    foreach ($packageName in $Check.packages) { $arguments += "--package=$packageName" }
    if ($Configuration.no_default_features) { $arguments += '--no-default-features' }
    if ($Configuration.all_features) { $arguments += '--all-features' }
    foreach ($feature in $Configuration.features) { $arguments += "--features=$feature" }
    $arguments += @($Configuration.additional_cargo_args)
    if ($Phase -ceq 'Test') { $arguments += @($Configuration.additional_cargo_test_args) }
    $environment = $MutationCommand.environment.Clone()
    $environment.INSTA_UPDATE = 'no'
    $environment.INSTA_FORCE_PASS = '0'
    if ($Configuration.cap_lints) {
        $environment.CARGO_ENCODED_RUSTFLAGS = (@('--cfg', 'mutants', '--cap-lints=warn') -join [char]0x1f)
    }
    $command = @{ file = $MutationCommand.file; arguments = [string[]]$arguments; environment = $environment }
    # Same explicit test budget as the mutation command; baseline builds have no tool timeout.
    if ($Phase -ceq 'Test') { $command.timeout_seconds = 60 }
    return $command
}

Export-ModuleMember -Function Get-ScheduledToolchain, Assert-ScheduledPlatform, Get-ScheduledCommand,
Invoke-ScheduledProcess, Get-ScheduledTestScope, Get-ScheduledMutationConfig, Get-ScheduledBaselineCommand
