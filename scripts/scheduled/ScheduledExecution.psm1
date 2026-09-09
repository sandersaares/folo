#requires -Version 7

# The deep-check entrypoint and independent reporter share typed commands and evidence checks
# here. PowerShell owns native process setup, capture and failure reporting, including failures
# before Rust is available; the controller-built Rust decoder owns TOML semantics and dependency
# identity normalization.
# Ref: .github/workflows/implementation.md, "Scheduled controller ownership".
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
Import-Module (Join-Path $PSScriptRoot '..\build\Mutants.psm1')
Import-Module (Join-Path $PSScriptRoot '..\build\Miri.psm1')
Import-Module (Join-Path $PSScriptRoot '..\build\CargoExecutable.psm1')
$script:mutationDecoderExecutable = $null

function Get-ScheduledToolchain {
    # Execution and evidence reconstruction must use the same reviewed pin, not the caller's
    # active rustup override. This also supplies the native decoder's stable build toolchain.
    [CmdletBinding()]
    [OutputType([string])]
    param([Parameter(Mandatory)][ValidateSet('mutants', 'miri', 'miri-many', 'careful')][string] $Kind)

    # These files belong to the trusted controller checkout, never the artifact's source_root.
    if ($Kind -ceq 'mutants') {
        $toolchainFile = Get-Content -LiteralPath (Join-Path $PSScriptRoot '..\..\rust-toolchain.toml') -Raw
        $channels = [regex]::Matches($toolchainFile, '(?m)^\s*channel\s*=\s*"(\d+\.\d+\.\d+)"\s*(?:#.*)?$')
        if ($channels.Count -ne 1) {
            throw [FormatException]::new('Controller rust-toolchain.toml must contain one numeric stable pin.')
        }
        $toolchain = $channels[0].Groups[1].Value
    } else {
        $constants = Get-Content -LiteralPath (Join-Path $PSScriptRoot '..\..\constants.env')
        $pins = @($constants | Where-Object { $_ -cmatch '^RUST_NIGHTLY=' })
        if ($pins.Count -ne 1) { throw [FormatException]::new('Controller constants must contain one nightly pin.') }
        $toolchain = $pins[0].Substring('RUST_NIGHTLY='.Length)
        if ($toolchain -cnotmatch '^nightly-\d{4}-\d{2}-\d{2}$') {
            throw [FormatException]::new('Controller constants must specify a dated nightly.')
        }
    }
    return $toolchain
}

function Get-ScheduledExpectedToolchain {
    [CmdletBinding()]
    [OutputType([string])]
    param(
        [Parameter(Mandatory)][string] $Kind,
        [Parameter(Mandatory)][hashtable] $RunContext
    )

    $toolchain = Get-ScheduledToolchain -Kind $Kind
    if ($RunContext.ContainsKey('toolchain') -and $RunContext.toolchain -cne $toolchain) {
        throw [FormatException]::new('Run context disagrees with the trusted per-kind toolchain pin.')
    }
    return $toolchain
}

function Get-ScheduledHostPlatform {
    [CmdletBinding()]
    [OutputType([hashtable])]
    param()

    return @{
        os = if ($IsWindows) { 'windows' } elseif ($IsLinux) { 'linux' } else { 'unsupported' }
        # An emulated x64 pwsh on ARM must not certify an x64 runner leg.
        architecture = [Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString()
    }
}

function Test-ScheduledHostPlatform {
    [CmdletBinding()]
    [OutputType([bool])]
    param(
        [Parameter(Mandatory)][string] $Runner,
        [Parameter(Mandatory)][hashtable] $HostPlatform
    )

    $expected = switch -CaseSensitive ($Runner) {
        'ubuntu-latest' { @{ os = 'linux'; architecture = 'X64' } }
        'windows-latest' { @{ os = 'windows'; architecture = 'X64' } }
        'ubuntu-24.04-arm' { @{ os = 'linux'; architecture = 'Arm64' } }
        'windows-11-arm' { @{ os = 'windows'; architecture = 'Arm64' } }
        default { return $false }
    }
    return $HostPlatform.os -ceq $expected.os -and $HostPlatform.architecture -ceq $expected.architecture
}

function Assert-ScheduledExecutionInput {
    # Catalog controls cannot be smuggled through free-form flags or test filters. The reporter
    # applies this same validation to reconstructed commands from either supported platform.
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][hashtable] $Check,
        [Parameter(Mandatory)][string] $SourceRoot,
        [Parameter(Mandatory)][string] $OutputDirectory,
        [Parameter(Mandatory)][string] $Toolchain
    )

    foreach ($path in @($SourceRoot, $OutputDirectory)) {
        # A reporter can reparse Windows execution evidence on Linux, or vice versa.
        if (-not [IO.Path]::IsPathFullyQualified($path) -and $path -notmatch '^[A-Za-z]:[\\/]' -and
            -not $path.StartsWith('/') -and -not $path.StartsWith('\\')) {
            throw [ArgumentException]::new('Execution paths must be absolute.')
        }
    }
    foreach ($name in @('id', 'kind', 'platform', 'packages', 'shard', 'seed_range', 'flags', 'test_filter')) {
        if (-not $Check.ContainsKey($name) -or $null -eq $Check[$name]) {
            throw [ArgumentException]::new("Missing check field: $name")
        }
    }
    $pinPattern = if ($Check.kind -ceq 'mutants') { '^\d+\.\d+\.\d+$' } else { '^nightly-\d{4}-\d{2}-\d{2}$' }
    if ($Check.kind -cnotin @('mutants', 'miri', 'miri-many', 'careful') -or $Toolchain -cnotmatch $pinPattern) {
        throw [ArgumentException]::new('Unsupported check kind or invalid toolchain pin.')
    }
    foreach ($name in @('packages', 'flags')) {
        if ($Check[$name] -isnot [array]) {
            throw [ArgumentException]::new("$name must be an array.")
        }
        foreach ($value in $Check[$name]) {
            if ($value -isnot [string] -or [string]::IsNullOrWhiteSpace($value) -or $value.Contains("`0")) {
                throw [ArgumentException]::new("Invalid $name argument.")
            }
        }
    }
    if ($Check.kind -in @('miri', 'miri-many')) {
        foreach ($flag in $Check.flags) {
            # MIRIFLAGS is whitespace-separated by Miri, not interpreted by a shell.
            if ($flag -notmatch '^-Zmiri-[^\s]+$') {
                throw [ArgumentException]::new('Miri flags must each be one -Zmiri argument.')
            }
        }
    } else {
        foreach ($flag in $Check.flags) {
            # Scope, baseline and output controls belong to the typed catalog, never free flags.
            if ($flag -notmatch '^--(?:all-features|no-default-features|test-workspace|features=[^\s]+|profile=[^\s]+)$') {
                throw [ArgumentException]::new('Unsupported scheduled Cargo flag.')
            }
        }
    }
    if ($Check.ContainsKey('replay_mutant') -and $Check.kind -ne 'mutants') {
        throw [ArgumentException]::new('Only mutation checks can select a replay mutant.')
    }
    if ($Check.ContainsKey('target')) {
        if ($Check.kind -notin @('miri', 'miri-many') -or $Check.packages.Count -ne 1) {
            throw [ArgumentException]::new('A Miri target requires exactly one selected package.')
        }
        $null = Get-ScheduledTargetKey -Target $Check.target
        if ($Check.kind -eq 'miri-many' -and $Check.target.kind -cne 'lib') {
            throw [ArgumentException]::new('Many-seed Miri only runs library targets.')
        }
    }
    if ($Check.test_filter.StartsWith('-') -or $Check.test_filter -match '[\r\n\x00]') {
        throw [ArgumentException]::new('A test filter must be a name, not a libtest option.')
    }
}

function Get-ScheduledCommand {
    # One command constructor serves execution and independent replay verification, so changing
    # a checker option cannot silently change what archived evidence is considered equivalent.
    [CmdletBinding()]
    [OutputType([hashtable])]
    param(
        [Parameter(Mandatory)][hashtable] $Check,
        [Parameter(Mandatory)][string] $SourceRoot,
        [Parameter(Mandatory)][string] $OutputDirectory,
        [Parameter(Mandatory)][string] $Toolchain,
        [switch] $List
    )

    Assert-ScheduledExecutionInput -Check $Check -SourceRoot $SourceRoot `
        -OutputDirectory $OutputDirectory -Toolchain $Toolchain
    $arguments = @("+$Toolchain")
    $environment = @{
        RUSTUP_TOOLCHAIN = $Toolchain; CARGO_TERM_COLOR = 'never'; NO_COLOR = '1'
        CARGO_ENCODED_RUSTFLAGS = $null; RUSTFLAGS = ''; RUSTDOCFLAGS = ''; MIRIFLAGS = ''
        MUTATION_TESTING = $null
        RUST_TEST_THREADS = '1'
    }
    $packageArguments = @()
    if ($Check.packages.Count -eq 0) { $packageArguments += '--workspace' }
    foreach ($package in $Check.packages) { $packageArguments += "--package=$package" }

    if ($Check.kind -eq 'mutants') {
        $arguments += 'mutants'
        $arguments += $packageArguments
        $arguments += @(Get-MutantsExcludeArgument -IsWindowsPlatform ($Check.platform -like 'windows-*') `
                -IsLinuxPlatform ($Check.platform -like 'ubuntu-*') -Literal)
        $arguments += @(Get-MutantsShardArgument -Spec $Check.shard)
        if ($Check.ContainsKey('replay_mutant')) {
            $arguments += @(Get-MutantsReplayArgument -Mutant $Check.replay_mutant)
        }
        $arguments += @($Check.flags)
        if ($List) {
            $arguments += @('--list', '--json')
        } else {
            # The recipe's timeout and exclusions apply, but a scheduled result requires a real
            # baseline. One mutation job makes parallelism independent of runner CPU count;
            # the catalog already splits the workspace across independently scheduled shards.
            $arguments += @('--baseline=run', '--timeout=60', '--no-shuffle', '--caught',
                '--unviable', '--jobs=1', '--output', $OutputDirectory)
            if ($Check.test_filter -ne '') {
                $arguments += @('--', $Check.test_filter)
            }
        }
        $environment.MUTATION_TESTING = '1'
        $environment.RUSTFLAGS = '--cfg mutants'
        $environment.CARGO_TARGET_DIR = $null
    } else {
        if ($List) { throw [ArgumentException]::new('Only cargo-mutants supports discovery.') }
        if ($Check.kind -eq 'careful') {
            $arguments += @('careful', 'test') + $packageArguments +
                @('--no-fail-fast', '--all-features', '--locked') + @($Check.flags)
        } else {
            $arguments += @('miri', 'test') + $packageArguments +
                @('--no-fail-fast', '--all-features', '--locked')
            if ($Check.ContainsKey('target')) {
                $arguments += if ($Check.target.kind -ceq 'lib') { '--lib' }
                    else { "--$($Check.target.kind)=$($Check.target.name)" }
            } elseif ($Check.kind -eq 'miri-many') {
                $arguments += '--lib'
            } else {
                # Ordinary Miri has nextest's lib/bin/integration-test scope, without doctests.
                # Execution expands that scope into separate metadata-enabled target commands.
                $arguments += '--tests'
            }
            $miriFlags = @(Get-MiriFlag -Flags $Check.flags -SeedRange $Check.seed_range `
                    -Shard $Check.shard -Many:($Check.kind -eq 'miri-many'))
            $environment.MIRIFLAGS = $miriFlags -join ' '
        }
        # The test name is a single libtest argument, even when it contains quotes or shell syntax.
        $arguments += '--'
        if ($Check.test_filter -ne '') {
            # Catalog filters are substrings. A seed-bound finding names one exact replay test.
            if (@($Check.flags | Where-Object { $_ -match '^-Zmiri-seed=' }).Count -gt 0) {
                $arguments += '--exact'
            }
            $arguments += $Check.test_filter
        }
        $arguments += '--test-threads=1'
    }
    return @{ file = 'cargo'; arguments = [string[]]$arguments; environment = $environment }
}

function Invoke-ScheduledProcess {
    # Drain both streams concurrently so a verbose checker cannot deadlock on a full pipe.
    # Keep raw bytes for the independent reporter instead of interpreting terminal rendering.
    # No shell participates: ArgumentList preserves each value on Windows and Unix alike.
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
        if (-not $process.Start()) { throw [InvalidOperationException]::new('Could not start the check.') }
        $copyOut = $process.StandardOutput.BaseStream.CopyToAsync($stdout)
        $copyErr = $process.StandardError.BaseStream.CopyToAsync($stderr)
        $timedOut = $false
        if ($Command.ContainsKey('timeout_seconds')) {
            $timedOut = -not $process.WaitForExit([int]($Command.timeout_seconds * 1000))
            if ($timedOut) { $process.Kill($true); $process.WaitForExit() }
        } else {
            $process.WaitForExit()
        }
        $null = $copyOut.GetAwaiter().GetResult()
        $null = $copyErr.GetAwaiter().GetResult()
        return @{ exit_code = $process.ExitCode; timed_out = $timedOut
            stdout_path = $stdoutPath; stderr_path = $stderrPath }
    } finally {
        $stdout.Dispose()
        $stderr.Dispose()
        $process.Dispose()
    }
}

function Get-ScheduledMutationDecoderBuild {
    # Building from the controller working directory keeps Cargo configuration discovery away
    # from candidate worktrees. Resolve Cargo before starting the child, never through its cwd.
    [CmdletBinding()]
    [OutputType([hashtable])]
    param()

    $root = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..')).Path
    $hostPlatform = Get-ScheduledHostPlatform
    # Windows and WSL can share this checkout but cannot share native executables.
    $target = Join-Path $root "target\scheduled-mutation-config\$($hostPlatform.os)-$($hostPlatform.architecture)"
    $toolchain = Get-ScheduledToolchain -Kind mutants
    return @{
        root = $root; target = $target
        command = @{
            file = (Get-Command cargo -CommandType Application | Select-Object -First 1).Source
            arguments = @("+$toolchain", 'build', '--locked', '--package', 'scheduled-mutation-config',
                '--bin', 'scheduled-mutation-config', '--manifest-path', (Join-Path $root 'Cargo.toml'),
                '--target-dir', $target, '--message-format=json')
            environment = @{
                RUSTUP_TOOLCHAIN = $toolchain; RUSTUP_AUTO_INSTALL = '0'
                CARGO_TERM_COLOR = 'never'; NO_COLOR = '1'
                # Metadata shares the controller build's target root, not an inherited
                # candidate path or an empty environment value that Cargo rejects.
                CARGO_TARGET_DIR = $target
                # Candidate checker settings are not native controller build settings.
                CARGO_BUILD_TARGET = $null; CARGO_ENCODED_RUSTFLAGS = $null
                RUSTFLAGS = ''; RUSTDOCFLAGS = ''; MIRIFLAGS = ''; MUTATION_TESTING = $null
                RUSTC = $null; RUSTDOC = $null; RUSTC_WRAPPER = $null; RUSTC_WORKSPACE_WRAPPER = $null
            }
        }
    }
}

function Get-ScheduledMutationDecoder {
    # Only baseline parsing needs Rust. Module imports, planners and empty intake scans remain
    # cheap. Immutable controller code permits one build per process, then direct invocation;
    # Cargo reuses its own package-scoped cache across processes without trusting a stale path.
    [CmdletBinding()]
    [OutputType([string])]
    param()

    if ($null -ne $script:mutationDecoderExecutable -and
        (Test-Path -LiteralPath $script:mutationDecoderExecutable -PathType Leaf)) {
        return $script:mutationDecoderExecutable
    }
    $build = Get-ScheduledMutationDecoderBuild
    $null = New-Item -ItemType Directory -Path $build.target -Force
    # Cargo serializes its shared build cache; process-specific logs also let separate local
    # controller processes prepare the same checkout without clobbering each other's evidence.
    $result = Invoke-ScheduledProcess -Command $build.command -SourceRoot $build.root `
        -OutputDirectory $build.target -Name "decoder-build-$PID"
    if ($result.exit_code -ne 0) {
        $diagnostic = Get-Content -LiteralPath $result.stderr_path -Raw
        $messages = Get-Content -LiteralPath $result.stdout_path -Raw
        throw [InvalidOperationException]::new("Cannot build the controller mutation decoder: $diagnostic`n$messages")
    }
    $executable = Resolve-CargoExecutable -CargoMessage @(Get-Content -LiteralPath $result.stdout_path) `
        -TargetName 'scheduled-mutation-config'
    # Cargo, not an artifact's source_root or an environment-supplied executable, owns this path.
    $executable = (Resolve-Path -LiteralPath $executable).Path

    # A reviewed bounded snapshot lets the planner hash dependency identity without starting
    # Rust. Cargo supplies effective inherited requirements and the resolved transitive graph;
    # the built Rust utility normalizes it before any baseline TOML can be decoded.
    # Ref: .github/workflows/implementation.md, "Scheduled controller ownership".
    $metadataCommand = $build.command.Clone()
    $metadataCommand.arguments = @($build.command.arguments[0], 'metadata', '--locked',
        '--format-version=1', '--manifest-path', (Join-Path $build.root 'Cargo.toml'))
    $metadata = Invoke-ScheduledProcess -Command $metadataCommand -SourceRoot $build.root `
        -OutputDirectory $build.target -Name "decoder-metadata-$PID"
    if ($metadata.exit_code -ne 0) {
        $diagnostic = Get-Content -LiteralPath $metadata.stderr_path -Raw
        throw [InvalidOperationException]::new("Cannot identify controller decoder dependencies: $diagnostic")
    }
    $actual = Invoke-ScheduledMutationDecoder -Executable $executable -DependencyContract `
        -Text (Get-Content -LiteralPath $metadata.stdout_path -Raw)
    $expected = Get-Content -LiteralPath (Join-Path $build.root `
        'packages\scheduled-mutation-config\dependency-contract.json') -Raw
    if ($actual.Replace("`r`n", "`n").Trim() -cne $expected.Replace("`r`n", "`n").Trim()) {
        throw [InvalidOperationException]::new(
            'Controller decoder dependencies differ from dependency-contract.json; refresh and review the bounded snapshot.')
    }
    $script:mutationDecoderExecutable = $executable
    return $executable
}

function Get-ScheduledMutationConfig {
    [CmdletBinding()]
    [OutputType([hashtable])]
    param([Parameter(Mandatory)][AllowEmptyString()][string] $Text)

    # Baseline policy is controller-owned. Candidate configuration is archived as data and must
    # agree before it can certify an empty shard; the reporter never follows source_root paths.
    $trusted = Get-Content -LiteralPath (Join-Path $PSScriptRoot '..\..\.cargo\mutants.toml') -Raw
    if ($Text.Replace("`r`n", "`n") -cne $trusted.Replace("`r`n", "`n")) {
        throw [FormatException]::new('Empty-shard baseline configuration differs from the controller.')
    }
    $json = Invoke-ScheduledMutationDecoder -Executable (Get-ScheduledMutationDecoder) -Text $Text
    return ConvertFrom-Json -InputObject $json -AsHashtable
}

function Invoke-ScheduledMutationDecoder {
    # The private caller supplies only the executable resolved from its trusted controller
    # build. Both dependency attestation and configuration decoding use the same UTF-8 transport.
    [CmdletBinding()]
    [OutputType([string])]
    param(
        [Parameter(Mandatory)][string] $Executable,
        [Parameter(Mandatory)][AllowEmptyString()][string] $Text,
        [switch] $DependencyContract
    )

    $start = [Diagnostics.ProcessStartInfo]::new()
    $start.FileName = $Executable
    if ($DependencyContract) { $start.ArgumentList.Add('--dependency-contract') }
    $start.WorkingDirectory = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..')).Path
    $start.UseShellExecute = $false
    $start.RedirectStandardInput = $true
    $start.RedirectStandardOutput = $true
    $start.RedirectStandardError = $true
    # Preserve TOML string values on Windows as well as Linux, independent of the console code page.
    $start.StandardInputEncoding = [Text.UTF8Encoding]::new($false)
    $start.StandardOutputEncoding = [Text.UTF8Encoding]::new($false)
    $start.StandardErrorEncoding = [Text.UTF8Encoding]::new($false)
    $process = [Diagnostics.Process]::new()
    $process.StartInfo = $start
    try {
        if (-not $process.Start()) {
            throw [InvalidOperationException]::new('Could not start the controller mutation utility.')
        }
        $stdout = $process.StandardOutput.ReadToEndAsync()
        $stderr = $process.StandardError.ReadToEndAsync()
        $process.StandardInput.Write($Text)
        $process.StandardInput.Close()
        $process.WaitForExit()
        $json = $stdout.GetAwaiter().GetResult()
        $diagnostic = $stderr.GetAwaiter().GetResult()
        if ($process.ExitCode -ne 0) {
            $operation = if ($DependencyContract) { 'identify controller decoder dependencies' }
                else { 'decode mutation configuration' }
            throw [FormatException]::new("Cannot ${operation}: $diagnostic")
        }
        return $json
    } finally {
        $process.Dispose()
    }
}

function Get-ScheduledEmptyBaselineCommand {
    [CmdletBinding()]
    [OutputType([hashtable])]
    param(
        [Parameter(Mandatory)][hashtable] $Check,
        [Parameter(Mandatory)][hashtable] $MutationCommand,
        [Parameter(Mandatory)][hashtable] $Configuration,
        [Parameter(Mandatory)][ValidateSet('Build', 'Test')][string] $Phase
    )

    # Equivalent to v27.1.0 cargo.rs cargo_argv/encoded_rustflags and timeouts.rs for_baseline.
    # lab.rs applies the baseline to mutated packages, independently of test_package/test_workspace.
    # With none selected, certify the entire declared package scope rather than invent a mutant.
    $arguments = @($MutationCommand.arguments[0])
    $arguments += if ($Configuration.test_tool -ceq 'nextest') { @('nextest', 'run') } else { 'test' }
    if ($Phase -ceq 'Build') { $arguments += '--no-run' }
    $profiles = @($Check.flags | Where-Object { $_.StartsWith('--profile=') })
    $cargoProfile = if ($profiles.Count -gt 0) { $profiles[-1].Substring('--profile='.Length) } else { $Configuration.profile }
    if ($null -ne $cargoProfile) {
        $arguments += if ($Configuration.test_tool -ceq 'nextest') { "--cargo-profile=$cargoProfile" } else { "--profile=$cargoProfile" }
    }
    $arguments += '--verbose'
    if ($Check.packages.Count -eq 0) { $arguments += '--workspace' }
    foreach ($package in $Check.packages) { $arguments += "--package=$package" }
    if ($Configuration.no_default_features -or $Check.flags -ccontains '--no-default-features') {
        $arguments += '--no-default-features'
    }
    if ($Configuration.all_features -or $Check.flags -ccontains '--all-features') { $arguments += '--all-features' }
    $arguments += @($Check.flags | Where-Object { $_.StartsWith('--features=') })
    foreach ($feature in $Configuration.features) { $arguments += "--features=$feature" }
    $arguments += @($Configuration.additional_cargo_args)
    if ($Phase -ceq 'Test') {
        if ($Check.test_filter -ne '') { $arguments += $Check.test_filter }
        $arguments += @($Configuration.additional_cargo_test_args)
    }
    $environment = $MutationCommand.environment.Clone()
    $environment.INSTA_UPDATE = 'no'
    $environment.INSTA_FORCE_PASS = '0'
    if ($Configuration.cap_lints) {
        $environment.CARGO_ENCODED_RUSTFLAGS = (@('--cfg', 'mutants', '--cap-lints=warn') -join [char]0x1f)
    }
    $command = @{ file = 'cargo'; arguments = [string[]]$arguments; environment = $environment }
    # The scheduled mutation command explicitly supplies this test timeout; baseline builds have none.
    if ($Phase -ceq 'Test') { $command.timeout_seconds = 60 }
    return $command
}

function Write-ScheduledExecutionFile {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][hashtable] $Value,
        [Parameter(Mandatory)][string] $Path
    )
    ConvertTo-Json -InputObject $Value -Depth 100 | Set-Content -LiteralPath $Path -Encoding utf8
}

function Get-ScheduledMutantKey {
    [CmdletBinding()]
    [OutputType([string])]
    param([Parameter(Mandatory)][hashtable] $Mutant)

    foreach ($name in @('name', 'package', 'file', 'function', 'span', 'replacement', 'genre')) {
        if (-not $Mutant.ContainsKey($name)) { throw [FormatException]::new("Missing mutant $name.") }
    }
    if ($null -ne $Mutant.function -and ($Mutant.function -isnot [hashtable] -or
            -not $Mutant.function.ContainsKey('function_name'))) {
        throw [FormatException]::new('Invalid mutant function.')
    }
    if ($Mutant.span -isnot [hashtable]) { throw [FormatException]::new('Invalid mutant span.') }
    foreach ($boundary in @('start', 'end')) {
        if (-not $Mutant.span.ContainsKey($boundary) -or $Mutant.span[$boundary] -isnot [hashtable] -or
            -not $Mutant.span[$boundary].ContainsKey('line') -or -not $Mutant.span[$boundary].ContainsKey('column')) {
            throw [FormatException]::new('Incomplete mutant span.')
        }
    }
    $functionName = if ($null -eq $Mutant.function) { '' } else { $Mutant.function.function_name }
    # A location is necessary for selecting the exact experiment, but is not an incident identity.
    return ConvertTo-Json -InputObject @($Mutant.name, $Mutant.package, $Mutant.file,
        $functionName, $Mutant.span.start.line, $Mutant.span.start.column,
        $Mutant.span.end.line, $Mutant.span.end.column, $Mutant.replacement, $Mutant.genre) -Compress
}

function Get-ScheduledPhaseSummary {
    # Recompute the pinned cargo-mutants summary from phase results instead of trusting a label.
    # Wire format: sourcefrog/cargo-mutants v27.1.0, src/outcome.rs and src/process.rs.
    [CmdletBinding()]
    [OutputType([string])]
    param([Parameter(Mandatory)][hashtable] $Scenario)

    if (-not $Scenario.ContainsKey('phase_results') -or $Scenario.phase_results.Count -eq 0) {
        throw [FormatException]::new('A scenario has no completed phases.')
    }
    if (-not $Scenario.ContainsKey('scenario') -or -not $Scenario.ContainsKey('summary')) {
        throw [FormatException]::new('Incomplete scenario.')
    }
    $timeout = $false
    $buildFailed = $false
    foreach ($phase in $Scenario.phase_results) {
        if (-not $phase.ContainsKey('phase') -or -not $phase.ContainsKey('process_status')) {
            throw [FormatException]::new('Incomplete phase result.')
        }
        if ($phase.phase -cnotin @('Build', 'Test')) {
            throw [FormatException]::new('Unexpected phase: a scheduled check must execute tests.')
        }
        $status = $phase.process_status
        if ($status -ceq 'Timeout') { $timeout = $true }
        elseif ($status -is [hashtable] -and $status.ContainsKey('Failure')) {
            if ($status.Failure -le 0) { throw [FormatException]::new('Invalid failure exit code.') }
            if ($phase.phase -cne 'Test') { $buildFailed = $true }
        } elseif ($status -cne 'Success') {
            throw [FormatException]::new('Unclassified process termination.')
        }
        if ($Scenario.phase_results.Count -gt 2 -or $Scenario.phase_results[0].phase -cne 'Build' -or
            ($Scenario.phase_results.Count -eq 2 -and
                ($Scenario.phase_results[1].phase -cne 'Test' -or $Scenario.phase_results[0].process_status -cne 'Success'))) {
            throw [FormatException]::new('Invalid build/test phase sequence.')
        }
    }
    $last = $Scenario.phase_results[-1]
    if ($Scenario.scenario -ceq 'Baseline') {
        if ($timeout) { return 'Timeout' }
        if ($last.phase -ceq 'Test' -and $last.process_status -ceq 'Success') { return 'Success' }
        return 'Failure'
    }
    if ($buildFailed) { return 'Unviable' }
    if ($timeout) { return 'Timeout' }
    if ($last.phase -cne 'Test') { throw [FormatException]::new('Mutation tests did not run.') }
    if ($last.process_status -ceq 'Success') { return 'MissedMutant' }
    return 'CaughtMutant'
}

function Get-ScheduledMutationResult {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][hashtable] $Result,
        [Parameter(Mandatory)][hashtable] $Check,
        [Parameter(Mandatory)][string] $OutputDirectory,
        [Parameter(Mandatory)][hashtable] $Execution,
        [Parameter(Mandatory)][string] $Toolchain
    )

    $outcomesPath = Join-Path $OutputDirectory 'mutants.out\outcomes.json'
    $inventoryPath = Join-Path $OutputDirectory 'mutants.out\mutants.json'
    if ((Test-Path -LiteralPath $inventoryPath) -and -not (Test-Path -LiteralPath $outcomesPath) -and
        (Get-Content -LiteralPath $inventoryPath -Raw) -cmatch '^\s*\[\s*\]\s*$') {
        Get-ScheduledEmptyMutationResult -Result $Result -Check $Check -OutputDirectory $OutputDirectory `
            -Execution $Execution -Toolchain $Toolchain
        return
    }
    if (-not (Test-Path -LiteralPath $outcomesPath) -or -not (Test-Path -LiteralPath $inventoryPath)) {
        $Result.summary = 'Mutation output or selected-mutant inventory is missing.'
        return
    }
    $lab = Get-Content -LiteralPath $outcomesPath -Raw | ConvertFrom-Json -AsHashtable
    $run = @($Execution.commands | Where-Object { $_.name -ceq 'check' })[0]
    if ($run.exit_code -ne $Execution.exit_code) {
        throw [FormatException]::new('Mutation command and aggregate exit code disagree.')
    }
    $inventory = @(Get-Content -LiteralPath $inventoryPath -Raw | ConvertFrom-Json -AsHashtable)
    foreach ($name in @('outcomes', 'total_mutants', 'missed', 'caught', 'timeout', 'unviable',
            'success', 'end_time', 'cargo_mutants_version')) {
        if (-not $lab.ContainsKey($name)) { throw [FormatException]::new("Missing lab field: $name") }
    }
    if ($lab.cargo_mutants_version -cne '27.1.0') {
        throw [FormatException]::new('Unsupported cargo-mutants output version.')
    }
    foreach ($scenario in $lab.outcomes) {
        if ($scenario -isnot [hashtable] -or -not $scenario.ContainsKey('scenario')) {
            throw [FormatException]::new('Invalid lab scenario.')
        }
    }
    $baselines = @($lab.outcomes | Where-Object { $_.scenario -ceq 'Baseline' })
    if ($baselines.Count -ne 1) {
        $Result.summary = 'The run must contain exactly one unmutated baseline.'
        return
    }
    $baselineSummary = Get-ScheduledPhaseSummary $baselines[0]
    if ($baselineSummary -cne $baselines[0].summary) {
        throw [FormatException]::new('Baseline summary disagrees with its phases.')
    }
    if ($baselineSummary -cne 'Success') {
        $Result.baseline = if ($baselineSummary -ceq 'Timeout') { 'timeout' } else { 'failed' }
        $Result.outcome = 'blocked'
        $Result.summary = 'Unmutated baseline failed; mutant outcomes are not actionable.'
        return
    }
    $Result.baseline = 'passed'
    $expected = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    foreach ($mutant in $inventory) {
        if (-not $expected.Add((Get-ScheduledMutantKey $mutant))) {
            throw [FormatException]::new('Duplicate mutant in inventory.')
        }
    }
    if ($Check.ContainsKey('replay_mutant')) {
        $key = Get-ScheduledMutantKey $Check.replay_mutant
        if ($expected.Count -ne 1 -or -not $expected.Contains($key)) {
            throw [FormatException]::new('Replay output does not contain exactly the intended mutant.')
        }
        $discoveryPath = Join-Path $OutputDirectory 'discovery.stdout'
        if (-not (Test-Path -LiteralPath $discoveryPath)) {
            throw [FormatException]::new('Replay discovery output is missing.')
        }
        $discovered = @(Get-Content -LiteralPath $discoveryPath -Raw | ConvertFrom-Json -AsHashtable)
        if ($discovered.Count -ne 1 -or (Get-ScheduledMutantKey $discovered[0]) -cne $key) {
            throw [FormatException]::new('Replay discovery did not select the intended mutant.')
        }
    }
    $seen = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    $counts = @{ MissedMutant = 0; CaughtMutant = 0; Timeout = 0; Unviable = 0 }
    foreach ($scenario in $lab.outcomes) {
        if ($scenario.scenario -ceq 'Baseline') { continue }
        if ($scenario.scenario -isnot [hashtable] -or -not $scenario.scenario.ContainsKey('Mutant')) {
            throw [FormatException]::new('Unknown mutation scenario.')
        }
        $mutant = $scenario.scenario.Mutant
        $key = Get-ScheduledMutantKey $mutant
        if (-not $expected.Contains($key) -or -not $seen.Add($key)) {
            throw [FormatException]::new('Unexpected or duplicate completed mutant.')
        }
        $summary = Get-ScheduledPhaseSummary $scenario
        if ($summary -cne $scenario.summary) {
            throw [FormatException]::new('Mutant summary disagrees with its phases.')
        }
        $counts[$summary]++
        if ($summary -cin @('MissedMutant', 'Timeout')) {
            $functionName = if ($null -eq $mutant.function) { '' } else { $mutant.function.function_name }
            $replay = $Check.Clone()
            $replay.packages = @($mutant.package)
            $replay.shard = ''
            $replay.replay_mutant = $mutant
            # The description, unlike the full name, excludes source line and column numbers.
            $change = $mutant.name -replace '^.*?:\d+:\d+: ', ''
            $Result.findings += @{
                identity = @{
                    kind = 'mutants'; package = $mutant.package; platform = $Check.platform
                    path = $mutant.file; function = $functionName; mutation = $change
                    test = ''; seed = ''; flags = @($Check.flags)
                }
                summary = "$summary`: $($mutant.name)"; replay = $replay
            }
        }
    }
    if ($counts.MissedMutant -ne $lab.missed -or $counts.CaughtMutant -ne $lab.caught -or
        $counts.Timeout -ne $lab.timeout -or $counts.Unviable -ne $lab.unviable -or
        $lab.success -ne 0 -or $seen.Count -ne $lab.total_mutants) {
        throw [FormatException]::new('Mutation counters disagree with completed scenarios.')
    }
    if ($null -eq $lab.end_time -or $seen.Count -ne $expected.Count) {
        $Result.summary = 'Mutation execution ended before every selected mutant completed.'
        return
    }
    $expectedExit = if ($counts.Timeout -gt 0) { 3 } elseif ($counts.MissedMutant -gt 0) { 2 } else { 0 }
    if ($Result.exit_code -ne $expectedExit) {
        $Result.outcome = 'execution-error'
        $Result.summary = 'Process exit code disagrees with the completed mutation results.'
    } elseif ($seen.Count -eq 0) {
        $Result.outcome = 'not-applicable'
        $Result.summary = 'No mutants selected; this is not evidence of tested coverage.'
    } elseif ($Result.findings.Count -gt 0) {
        $Result.outcome = 'findings'
        $Result.summary = "$($counts.MissedMutant) missed mutations; $($counts.Timeout) timed out."
    } else {
        $Result.outcome = 'passed'
        $Result.summary = 'Every selected mutation was caught or unviable.'
    }
}

function Get-ScheduledEmptyMutationResult {
    # cargo-mutants skips its baseline when discovery is empty. Only independently checked
    # discovery plus explicit baseline evidence can certify such a shard; an empty replay cannot.
    # Ref: .github/workflows/implementation.md, "Scheduled controller ownership".
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][hashtable] $Result,
        [Parameter(Mandatory)][hashtable] $Check,
        [Parameter(Mandatory)][string] $OutputDirectory,
        [Parameter(Mandatory)][hashtable] $Execution,
        [Parameter(Mandatory)][string] $Toolchain
    )

    if ($Check.ContainsKey('replay_mutant')) {
        $Result.outcome = 'blocked'
        $Result.summary = 'An exact mutant replay must select the intended mutant.'
        return
    }
    $mutationRun = @($Execution.commands | Where-Object { $_.name -ceq 'check' })[0]
    if ($mutationRun.exit_code -ne 0) { throw [FormatException]::new('Empty mutation selection did not succeed.') }
    $expected = Get-ScheduledCommand -Check $Check -SourceRoot $Execution.source_root `
        -OutputDirectory $Execution.output_directory -Toolchain $Toolchain -List
    $version = @{ file = 'cargo'; arguments = @("+$Toolchain", 'mutants', '--version')
        environment = $expected.environment }
    foreach ($probe in @(@{ name = 'discovery'; command = $expected }, @{ name = 'mutants-version'; command = $version })) {
        $records = @($Execution.commands | Where-Object { $_.name -ceq $probe.name })
        if ($records.Count -ne 1 -or $records[0].exit_code -ne 0) {
            throw [FormatException]::new('Empty selection lacks successful version and exact-scope discovery records.')
        }
        Assert-ScheduledRecordedCommand -Expected $probe.command -Recorded $records[0].command
        foreach ($extension in @('stdout', 'stderr')) {
            if (-not (Test-Path -LiteralPath (Join-Path $OutputDirectory "$($probe.name).$extension"))) {
                throw [FormatException]::new('Empty-selection probe output is missing.')
            }
        }
    }
    if ((Get-Content -LiteralPath (Join-Path $OutputDirectory 'discovery.stdout') -Raw) -cnotmatch '^\s*\[\s*\]\s*$' -or
        (Get-Content -LiteralPath (Join-Path $OutputDirectory 'mutants-version.stdout') -Raw).Trim() -cne 'cargo-mutants 27.1.0') {
        throw [FormatException]::new('Discovery did not verify an empty selection using the pinned mutation tool.')
    }
    $configPath = Join-Path $OutputDirectory 'mutation-config.toml'
    if (-not (Test-Path -LiteralPath $configPath)) { throw [FormatException]::new('Baseline configuration is missing.') }
    $configuration = Get-ScheduledMutationConfig -Text (Get-Content -LiteralPath $configPath -Raw)
    $phases = @($Execution.commands | Where-Object { $_.name -like 'baseline-*' })
    if ($phases.Count -lt 1 -or $phases.Count -gt 2) {
        throw [FormatException]::new('Empty selection lacks its unmutated build/test baseline.')
    }
    for ($index = 0; $index -lt $phases.Count; $index++) {
        $phase = @('Build', 'Test')[$index]
        $name = @('baseline-build', 'baseline-test')[$index]
        $record = $phases[$index]
        if ($record.name -cne $name -or -not $record.ContainsKey('timed_out') -or $record.timed_out -isnot [bool]) {
            throw [FormatException]::new('Invalid empty-selection baseline phase record.')
        }
        $expected = Get-ScheduledEmptyBaselineCommand -Check $Check -MutationCommand $mutationRun.command `
            -Configuration $configuration -Phase $phase
        Assert-ScheduledRecordedCommand -Expected $expected -Recorded $record.command
        foreach ($extension in @('stdout', 'stderr')) {
            # Names are generated only after exact record-name validation, never taken from the archive.
            if (-not (Test-Path -LiteralPath (Join-Path $OutputDirectory "$name.$extension"))) {
                throw [FormatException]::new('Unmutated baseline output is missing.')
            }
        }
        if ($record.timed_out -and $phase -ceq 'Build') { throw [FormatException]::new('Baseline builds have no timeout.') }
        if ($record.exit_code -ne 0 -or $record.timed_out) {
            if ($phases.Count -ne $index + 1 -or $Execution.exit_code -ne $record.exit_code) {
                throw [FormatException]::new('Baseline failure disagrees with the completed phase sequence.')
            }
            $Result.baseline = if ($record.timed_out) { 'timeout' } else { 'failed' }
            $Result.outcome = 'blocked'
            $Result.summary = 'The explicit unmutated baseline failed; an empty selection cannot certify coverage.'
            return
        }
        $stdout = Get-Content -LiteralPath (Join-Path $OutputDirectory "$name.stdout") -Raw
        $stderr = Get-Content -LiteralPath (Join-Path $OutputDirectory "$name.stderr") -Raw
        $text = "$stdout`n$stderr"
        $suites = [regex]::Matches($text, 'test result: (ok|FAILED)\. (\d+) passed; (\d+) failed;')
        if ($text -match '(?m)^\s*error(?:\[[^\]]+\])?:' -or
            @($suites | Where-Object { $_.Groups[1].Value -cne 'ok' -or [int]$_.Groups[3].Value -ne 0 }).Count -gt 0) {
            throw [FormatException]::new('Baseline raw diagnostics disagree with its successful exit code.')
        }
        $cargoFinished = $text -match '(?m)^\s*Finished [^\r\n]+ profile '
        $nextestSummary = [regex]::Match($text, '(?m)^\s*Summary \[[^\]\r\n]+\] (\d+) tests? run: ([^\r\n]+)\r?$')
        if ($phase -ceq 'Test' -and $configuration.test_tool -ceq 'nextest') {
            if (-not $nextestSummary.Success -or
                $nextestSummary.Groups[2].Value -match '(?:[1-9]\d*) (?:failed|timed out|exec failed)') {
                throw [FormatException]::new('Nextest baseline has no successful completed summary.')
            }
        } elseif (-not $cargoFinished -and ($phase -ceq 'Build' -or $suites.Count -eq 0)) {
            throw [FormatException]::new('Raw baseline output contains no completed Cargo phase.')
        }
    }
    if ($phases.Count -ne 2 -or $Execution.exit_code -ne 0) {
        throw [FormatException]::new('The empty-selection baseline did not complete successfully.')
    }
    $Result.baseline = 'passed'
    $Result.outcome = 'passed'
    $Result.summary = 'Exact-scope discovery selected no mutants and the unmutated package baseline passed.'
}

function Get-ScheduledTestResult {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][hashtable] $Result,
        [Parameter(Mandatory)][hashtable] $Check,
        [Parameter(Mandatory)][string] $OutputDirectory,
        [string] $Name = 'check'
    )

    $stdoutPath = Join-Path $OutputDirectory "$Name.stdout"
    $stderrPath = Join-Path $OutputDirectory "$Name.stderr"
    if (-not (Test-Path -LiteralPath $stdoutPath) -or -not (Test-Path -LiteralPath $stderrPath)) {
        $Result.summary = 'Test process output is missing.'
        return
    }
    $stdout = Get-Content -LiteralPath $stdoutPath -Raw
    $stderr = Get-Content -LiteralPath $stderrPath -Raw
    $text = "$stdout`n$stderr"
    $summaries = [regex]::Matches($text, 'test result: (ok|FAILED)\. (\d+) passed; (\d+) failed;')
    $failedTests = @([regex]::Matches($text, '(?m)^test ([^\r\n]+?) \.\.\. FAILED\s*$') |
            ForEach-Object { $_.Groups[1].Value })
    $miriError = [regex]::Match($stderr, '(?m)^error: (?:Undefined Behavior:|unsupported operation:|the evaluated program leaked memory)[^\r\n]*')
    if ($Check.kind -in @('miri', 'miri-many') -and ($miriError.Success -or $failedTests.Count -gt 0)) {
        if ($Result.exit_code -eq 0) {
            throw [FormatException]::new('Miri defects accompanied a successful exit code.')
        }
        if ($Check.packages.Count -ne 1 -or -not $Check.ContainsKey('target')) {
            $Result.summary = 'Miri defects require a metadata-verified package and target.'
            return
        }
        # Seed interpreters share output, and leaks can be diagnosed after the whole suite.
        # Neither the last test line nor a reported failing seed establishes their association.
        # Only an already exact input scope identifies one test; otherwise replay the target.
        $explicitSeed = @($Check.flags | Where-Object { $_ -match '^-Zmiri-seed=\d+$' })
        $seed = if ($explicitSeed.Count -eq 1) { $explicitSeed[0].Substring('-Zmiri-seed='.Length) } else { '' }
        $test = if ($seed -ne '' -and $Check.test_filter -ne '') { $Check.test_filter } else { '<target>' }
        $target = $Check.target.Clone()
        $Result.findings += @{
            identity = @{
                kind = $Check.kind; package = $Check.packages[0]; platform = $Check.platform
                path = ''; function = ''; mutation = ''
                test = "$($target.kind):$($target.name)::$test"; seed = $seed
                flags = @($Check.flags); target = $target
            }
            summary = if ($miriError.Success) { $miriError.Value } else { 'Miri target reported failing tests.' }
            replay = $Check.Clone()
        }
        $Result.outcome = 'findings'
        $Result.summary = 'Miri defects require replay of the verified input scope.'
        return
    }
    if ($Result.exit_code -ne 0 -and $failedTests.Count -eq 0) {
        $Result.outcome = 'execution-error'
        $Result.summary = 'The test command failed without an attributable test defect.'
        return
    }
    if ($failedTests.Count -gt 0) {
        if ($Result.exit_code -eq 0) {
            throw [FormatException]::new('Failed tests accompanied a successful exit code.')
        }
        if ($Check.packages.Count -ne 1) {
            $Result.summary = 'Failure requires package-scoped confirmation before creating an incident.'
            return
        }
        foreach ($test in ($failedTests | Sort-Object -Unique)) {
            $flags = @($Check.flags)
            $replay = $Check.Clone()
            $replay.shard = ''
            $replay.seed_range = ''
            $replay.test_filter = $test
            $replay.flags = $flags
            $Result.findings += @{
                identity = @{
                    kind = $Check.kind; package = $Check.packages[0]; platform = $Check.platform
                    path = ''; function = ''; mutation = ''; test = $test; seed = ''; flags = $flags
                    target = $null
                }
                summary = "Failed test: $test"
                replay = $replay
            }
        }
        $Result.outcome = 'findings'
        $Result.summary = 'Named test failures require repair.'
    } elseif ($summaries.Count -eq 0) {
        $Result.summary = 'No completed test-suite summaries were emitted.'
    } else {
        $passed = 0
        foreach ($suite in $summaries) {
            if ($suite.Groups[1].Value -cne 'ok' -or $suite.Groups[3].Value -ne '0') {
                throw [FormatException]::new('Test summary reports an unattributed failure.')
            }
            $passed += [int]$suite.Groups[2].Value
        }
        # Ordinary Miri preserves the legacy nextest --no-tests=pass behavior.
        $Result.outcome = if ($passed -gt 0 -or ($Check.kind -eq 'miri' -and $Check.test_filter -eq '')) { 'passed' }
            else { 'not-applicable' }
        $Result.summary = "$passed tests passed."
    }
}

function Get-ScheduledTargetKey {
    [CmdletBinding()]
    [OutputType([string])]
    param([Parameter(Mandatory)][hashtable] $Target)

    if (-not $Target.ContainsKey('kind') -or -not $Target.ContainsKey('name') -or
        $Target.kind -cnotin @('lib', 'bin', 'test') -or $Target.name -isnot [string] -or
        [string]::IsNullOrWhiteSpace($Target.name) -or $Target.name -match '[\r\n\x00]' -or
        $Target.Keys.Count -ne 2) {
        throw [FormatException]::new('Invalid Cargo test target selector.')
    }
    return ConvertTo-Json -InputObject @($Target.kind, $Target.name) -Compress
}

function Get-ScheduledTestScope {
    # Separate Miri processes identify failures by package AND Cargo target, including bin-only
    # packages. Archived metadata also proves that every enabled target in the declared scope ran.
    # Careful retains its package-level Cargo test scope, which additionally includes doctests.
    [CmdletBinding()]
    [OutputType([hashtable[]])]
    param(
        [Parameter(Mandatory)][hashtable] $Check,
        [Parameter(Mandatory)][string] $OutputDirectory
    )

    $metadata = Get-Content -LiteralPath (Join-Path $OutputDirectory 'metadata.stdout') -Raw |
        ConvertFrom-Json -AsHashtable
    foreach ($key in @('version', 'packages', 'workspace_members')) {
        if (-not $metadata.ContainsKey($key)) { throw [FormatException]::new("Missing metadata field: $key") }
    }
    if ($metadata.version -ne 1) { throw [FormatException]::new('Unsupported cargo metadata schema.') }
    $members = @($metadata.packages | Where-Object { $_.id -cin $metadata.workspace_members })
    if ($members.Count -eq 0 -or $members.Count -ne $metadata.workspace_members.Count) {
        throw [FormatException]::new('Cargo metadata lacks the complete workspace package inventory.')
    }
    foreach ($name in $Check.packages) {
        if ($name -cnotin @($members.name)) { throw [FormatException]::new("Unknown workspace package: $name") }
    }
    $selected = [Collections.Generic.SortedDictionary[string, hashtable]]::new([StringComparer]::Ordinal)
    foreach ($package in $members) {
        if ($Check.packages.Count -gt 0 -and $package.name -cnotin $Check.packages) { continue }
        if ($Check.kind -eq 'careful') {
            if ($selected.ContainsKey($package.name)) { throw [FormatException]::new('Duplicate workspace package.') }
            $selected.Add($package.name, @{ package = $package.name; target = $null })
            continue
        }
        foreach ($cargoTarget in $package.targets) {
            if (-not $cargoTarget.ContainsKey('kind') -or -not $cargoTarget.ContainsKey('name') -or
                -not $cargoTarget.ContainsKey('test') -or $cargoTarget.test -isnot [bool]) {
                throw [FormatException]::new('Cargo metadata lacks test-target information.')
            }
            if (-not $cargoTarget.test) { continue }
            $kinds = @($cargoTarget.kind)
            $kind = if (@($kinds | Where-Object {
                        $_ -cin @('lib', 'rlib', 'dylib', 'cdylib', 'staticlib', 'proc-macro')
                    }).Count -gt 0) { 'lib' }
                elseif ($kinds -ccontains 'bin') { 'bin' }
                elseif ($kinds -ccontains 'test') { 'test' }
                else { continue }
            if ($Check.kind -eq 'miri-many' -and $kind -cne 'lib') { continue }
            $target = @{ kind = $kind; name = $cargoTarget.name }
            $targetKey = Get-ScheduledTargetKey -Target $target
            if ($Check.ContainsKey('target') -and
                $targetKey -cne (Get-ScheduledTargetKey -Target $Check.target)) { continue }
            $key = ConvertTo-Json -InputObject @($package.name, $kind, $target.name) -Compress
            if ($selected.ContainsKey($key)) { throw [FormatException]::new('Duplicate Cargo test target.') }
            $selected.Add($key, @{ package = $package.name; target = $target })
        }
    }
    if ($Check.ContainsKey('target') -and $selected.Count -ne 1) {
        throw [FormatException]::new('The selected replay target is absent or not enabled for tests.')
    }
    return $selected.Values
}

function Get-ScheduledPackageResult {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][hashtable] $Result,
        [Parameter(Mandatory)][hashtable] $Check,
        [Parameter(Mandatory)][hashtable] $Execution,
        [Parameter(Mandatory)][string] $OutputDirectory,
        [Parameter(Mandatory)][string] $Toolchain
    )

    if (-not (Test-Path -LiteralPath (Join-Path $OutputDirectory 'metadata.stdout'))) {
        $Result.summary = 'Workspace package inventory is missing.'
        return
    }
    $scopes = @(Get-ScheduledTestScope -Check $Check -OutputDirectory $OutputDirectory)
    $runs = @($Execution.commands | Where-Object { $_.name -like 'check-*' })
    if ($scopes.Count -ne $runs.Count) {
        $Result.summary = 'Not every selected test target has completed evidence.'
        return
    }
    $outcomes = @()
    $expectedExit = 0
    for ($index = 0; $index -lt $scopes.Count; $index++) {
        $run = $runs[$index]
        $selected = $scopes[$index]
        if ($run.name -cne "check-$index" -or $run.package -cne $selected.package) {
            throw [FormatException]::new('Package test evidence is duplicated or out of scope.')
        }
        $scope = $Check.Clone()
        $scope.packages = @($selected.package)
        if ($null -ne $selected.target) {
            if (-not $run.ContainsKey('target') -or $run.target -isnot [hashtable] -or
                (Get-ScheduledTargetKey -Target $run.target) -cne (Get-ScheduledTargetKey -Target $selected.target)) {
                throw [FormatException]::new('Recorded Cargo target does not match the metadata inventory.')
            }
            $scope.target = $selected.target
        }
        $expected = Get-ScheduledCommand -Check $scope -SourceRoot $Execution.source_root `
            -OutputDirectory $Execution.output_directory -Toolchain $Toolchain
        Assert-ScheduledRecordedCommand -Expected $expected -Recorded $run.command
        $packageResult = @{ outcome = 'incomplete'; findings = @(); summary = ''; exit_code = $run.exit_code }
        Get-ScheduledTestResult -Result $packageResult -Check $scope -OutputDirectory $OutputDirectory -Name "check-$index"
        if ($run.exit_code -ne 0) { $expectedExit = $run.exit_code }
        $outcomes += $packageResult.outcome
        $Result.findings += $packageResult.findings
    }
    if ($Execution.exit_code -ne $expectedExit) {
        throw [FormatException]::new('Aggregate exit code disagrees with package processes.')
    }
    $Result.outcome = if ('execution-error' -in $outcomes) { 'execution-error' }
        elseif ('incomplete' -in $outcomes) { 'incomplete' }
        elseif ('findings' -in $outcomes) { 'findings' }
        elseif ('passed' -in $outcomes -or ($scopes.Count -eq 0 -and $Check.kind -eq 'miri')) { 'passed' }
        else { 'not-applicable' }
    $Result.summary = "Test scope outcomes: $($outcomes -join ', ')."
}

function Assert-ScheduledRecordedCommand {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][hashtable] $Expected,
        [Parameter(Mandatory)][hashtable] $Recorded
    )
    if ($Expected.file -cne $Recorded.file -or
        (ConvertTo-Json -InputObject @($Expected.arguments) -Compress) -cne
        (ConvertTo-Json -InputObject @($Recorded.arguments) -Compress)) {
        throw [FormatException]::new('Recorded argv does not match the declared check.')
    }
    foreach ($key in $Expected.environment.Keys) {
        if (-not $Recorded.environment.ContainsKey($key) -or
            $Recorded.environment[$key] -cne $Expected.environment[$key]) {
            throw [FormatException]::new("Recorded environment does not match the check: $key")
        }
        if ($Expected.ContainsKey('timeout_seconds') -ne $Recorded.ContainsKey('timeout_seconds') -or
            ($Expected.ContainsKey('timeout_seconds') -and $Expected.timeout_seconds -ne $Recorded.timeout_seconds)) {
            throw [FormatException]::new('Recorded timeout does not match the command.')
        }
    }
}

function Get-ScheduledCheckResult {
    # The reporter uses this same parser on the archived raw output. evidence.json is a convenience
    # for the executor, never the authority for deciding whether a finding or a clean run exists.
    [CmdletBinding()]
    [OutputType([hashtable])]
    param(
        [Parameter(Mandatory)][hashtable] $Check,
        [Parameter(Mandatory)][string] $OutputDirectory,
        [Parameter(Mandatory)][hashtable] $RunContext
    )

    $result = @{
        schema_version = 1; check_id = $Check.id; actual_scope = $Check
        outcome = 'incomplete'; baseline = if ($Check.kind -eq 'mutants') { 'not-run' } else { 'not-applicable' }
        exit_code = $null; findings = @(); summary = 'Execution metadata is missing.'
        log_path = 'check.log'
    }
    foreach ($key in @('source_sha', 'controller_sha', 'check_contract_digest', 'run_id', 'run_attempt', 'run_number')) {
        if (-not $RunContext.ContainsKey($key)) { throw [ArgumentException]::new("Missing run context: $key") }
        $result[$key] = $RunContext[$key]
    }
    $executionPath = Join-Path $OutputDirectory 'execution.json'
    if (-not (Test-Path -LiteralPath $executionPath)) { return $result }
    try {
        $toolchain = Get-ScheduledExpectedToolchain -Kind $Check.kind -RunContext $RunContext
        $execution = Get-Content -LiteralPath $executionPath -Raw | ConvertFrom-Json -AsHashtable
        foreach ($name in @('schema_version', 'stage', 'exit_code', 'completed', 'commands',
                'source_root', 'output_directory', 'toolchain')) {
            if (-not $execution.ContainsKey($name)) { throw [FormatException]::new("Missing execution $name.") }
        }
        if ($execution.schema_version -ne 1) { throw [FormatException]::new('Unsupported execution schema.') }
        if ($execution.toolchain -cne $toolchain) {
            throw [FormatException]::new('Recorded toolchain disagrees with the trusted controller pin.')
        }
        $result.exit_code = $execution.exit_code
        if ($execution.stage -eq 'unsupported-platform') {
            $result.outcome = 'not-applicable'
            $result.summary = 'This executor does not support the requested runner platform.'
        } elseif ($execution.stage -eq 'execution-error') {
            $result.outcome = 'execution-error'
            $result.summary = 'The process could not execute; see the captured log.'
        } elseif ($execution.stage -ne 'check') {
            $result.outcome = if ($execution.completed) { 'blocked' } else { 'incomplete' }
            $result.summary = "Check preparation did not complete: $($execution.stage)."
        } elseif (-not $execution.completed) {
            $result.summary = 'The check process did not finish.'
        } elseif ($Check.kind -eq 'mutants') {
            $runs = @($execution.commands | Where-Object { $_.name -eq 'check' })
            if ($runs.Count -ne 1 -or $runs[0].name -cne 'check') {
                throw [FormatException]::new('Missing or inconsistent mutation execution record.')
            }
            $expected = Get-ScheduledCommand -Check $Check -SourceRoot $execution.source_root `
                -OutputDirectory $execution.output_directory -Toolchain $toolchain
            Assert-ScheduledRecordedCommand -Expected $expected -Recorded $runs[0].command
            Get-ScheduledMutationResult -Result $result -Check $Check -OutputDirectory $OutputDirectory `
                -Execution $execution -Toolchain $toolchain
        } else {
            Get-ScheduledPackageResult -Result $result -Check $Check -Execution $execution `
                -OutputDirectory $OutputDirectory -Toolchain $toolchain
        }
    } catch [ArgumentException], [FormatException] {
        $result.outcome = 'incomplete'
        $result.findings = @()
        $result.summary = "Invalid raw execution evidence: $($_.Exception.Message)"
    }
    return $result
}

function Invoke-ScheduledCheck {
    # Persist progress before each native stage so setup failures and interrupted jobs still
    # produce reportable evidence instead of being mistaken for successful absence of findings.
    [CmdletBinding()]
    [OutputType([hashtable])]
    param(
        [Parameter(Mandatory)][hashtable] $Check,
        [Parameter(Mandatory)][string] $SourceRoot,
        [Parameter(Mandatory)][string] $OutputDirectory,
        [Parameter(Mandatory)][string] $Toolchain,
        [Parameter(Mandatory)][hashtable] $RunContext
    )

    Assert-ScheduledExecutionInput -Check $Check -SourceRoot $SourceRoot `
        -OutputDirectory $OutputDirectory -Toolchain $Toolchain
    if (-not [IO.Path]::IsPathFullyQualified($SourceRoot) -or -not [IO.Path]::IsPathFullyQualified($OutputDirectory)) {
        throw [ArgumentException]::new('Execution paths must be native absolute paths.')
    }
    $null = New-Item -ItemType Directory -Path $OutputDirectory -Force
    if (@(Get-ChildItem -LiteralPath $OutputDirectory -Force).Count -ne 0) {
        throw [ArgumentException]::new('Evidence output must be an empty directory.')
    }
    $execution = @{ schema_version = 1; stage = 'preparation'; exit_code = $null
        completed = $false; commands = @(); toolchain = $Toolchain
        source_root = $SourceRoot; output_directory = $OutputDirectory }
    $executionPath = Join-Path $OutputDirectory 'execution.json'
    $logPath = Join-Path $OutputDirectory 'check.log'
    Set-Content -LiteralPath $logPath -Value '' -Encoding utf8
    Write-ScheduledExecutionFile $execution $executionPath
    try {
        if ($Toolchain -cne (Get-ScheduledExpectedToolchain -Kind $Check.kind -RunContext $RunContext)) {
            throw [ArgumentException]::new('Requested toolchain disagrees with the trusted controller pin.')
        }
        $execution.host = Get-ScheduledHostPlatform
        if (-not (Test-ScheduledHostPlatform -Runner $Check.platform -HostPlatform $execution.host)) {
            $execution.stage = 'unsupported-platform'
            $execution.completed = $true
            return
        }
        $command = Get-ScheduledCommand -Check $Check -SourceRoot $SourceRoot `
            -OutputDirectory $OutputDirectory -Toolchain $Toolchain
        if ($Check.kind -in @('mutants', 'careful')) {
            foreach ($helper in @(
                    @{ package = 'cargo-bench-history-faker'; variable = 'CBH_FAKER'; directory = 'faker' },
                    @{ package = 'dure-test-helper'; variable = 'DURE_TEST_HELPER'; directory = 'dure-test-helper' })) {
                if ($helper.variable -eq 'DURE_TEST_HELPER' -and -not $IsWindows) { continue }
                $execution.stage = $helper.package
                $setup = @{
                    file = 'cargo'
                    arguments = @("+$Toolchain", 'build', "--package=$($helper.package)", '--target-dir',
                        (Join-Path $SourceRoot "target\$($helper.directory)"),
                        '--message-format=json-render-diagnostics', '--locked')
                    environment = @{ RUSTUP_TOOLCHAIN = $Toolchain; RUSTFLAGS = ''; CARGO_ENCODED_RUSTFLAGS = $null }
                }
                $step = Invoke-ScheduledProcess -Command $setup -SourceRoot $SourceRoot `
                    -OutputDirectory $OutputDirectory -Name $helper.package
                $execution.commands += @{ name = $helper.package; command = $setup; exit_code = $step.exit_code }
                $execution.exit_code = $step.exit_code
                Get-Content -LiteralPath $step.stdout_path, $step.stderr_path | Add-Content -LiteralPath $logPath
                if ($step.exit_code -ne 0) { $execution.completed = $true; return }
                $executable = Resolve-CargoExecutable -CargoMessage (Get-Content -LiteralPath $step.stdout_path) `
                    -TargetName $helper.package
                if (-not [IO.Path]::IsPathFullyQualified($executable) -or -not (Test-Path -LiteralPath $executable)) {
                    throw [IO.FileNotFoundException]::new('The built test helper is unavailable.')
                }
                $command.environment[$helper.variable] = $executable
            }
        }
        if ($Check.ContainsKey('replay_mutant')) {
            $execution.stage = 'discovery'
            $discovery = Get-ScheduledCommand -Check $Check -SourceRoot $SourceRoot `
                -OutputDirectory $OutputDirectory -Toolchain $Toolchain -List
            Write-ScheduledExecutionFile $execution $executionPath
            $step = Invoke-ScheduledProcess -Command $discovery -SourceRoot $SourceRoot `
                -OutputDirectory $OutputDirectory -Name 'discovery'
            $execution.commands += @{ name = 'discovery'; command = $discovery; exit_code = $step.exit_code }
            $execution.exit_code = $step.exit_code
            Get-Content -LiteralPath $step.stdout_path, $step.stderr_path | Add-Content -LiteralPath $logPath
            if ($step.exit_code -ne 0) { $execution.completed = $true; return }
            $selected = @(Get-Content -LiteralPath $step.stdout_path -Raw | ConvertFrom-Json -AsHashtable)
            if ($selected.Count -ne 1 -or
                (Get-ScheduledMutantKey $selected[0]) -cne (Get-ScheduledMutantKey $Check.replay_mutant)) {
                $execution.stage = 'replay-selection-mismatch'
                $execution.completed = $true
                return
            }
        }
        if ($Check.kind -eq 'mutants') {
            $execution.stage = 'check'
            Write-ScheduledExecutionFile $execution $executionPath
            $step = Invoke-ScheduledProcess -Command $command -SourceRoot $SourceRoot `
                -OutputDirectory $OutputDirectory -Name 'check'
            $execution.commands += @{ name = 'check'; command = $command; exit_code = $step.exit_code }
            $execution.exit_code = $step.exit_code
            Get-Content -LiteralPath $step.stdout_path, $step.stderr_path | Add-Content -LiteralPath $logPath
            $inventoryPath = Join-Path $OutputDirectory 'mutants.out\mutants.json'
            if ($step.exit_code -eq 0 -and -not $Check.ContainsKey('replay_mutant') -and
                (Test-Path -LiteralPath $inventoryPath) -and
                -not (Test-Path -LiteralPath (Join-Path $OutputDirectory 'mutants.out\outcomes.json')) -and
                (Get-Content -LiteralPath $inventoryPath -Raw) -cmatch '^\s*\[\s*\]\s*$') {
                $execution.stage = 'empty-discovery'
                $discovery = Get-ScheduledCommand -Check $Check -SourceRoot $SourceRoot `
                    -OutputDirectory $OutputDirectory -Toolchain $Toolchain -List
                $version = @{ file = 'cargo'; arguments = @("+$Toolchain", 'mutants', '--version')
                    environment = $discovery.environment }
                foreach ($probe in @(@{ name = 'discovery'; command = $discovery }, @{ name = 'mutants-version'; command = $version })) {
                    Write-ScheduledExecutionFile $execution $executionPath
                    $step = Invoke-ScheduledProcess -Command $probe.command -SourceRoot $SourceRoot `
                        -OutputDirectory $OutputDirectory -Name $probe.name
                    $execution.commands += @{ name = $probe.name; command = $probe.command; exit_code = $step.exit_code }
                    $execution.exit_code = $step.exit_code
                    Get-Content -LiteralPath $step.stdout_path, $step.stderr_path | Add-Content -LiteralPath $logPath
                    if ($step.exit_code -ne 0) { $execution.completed = $true; return }
                }
                if ((Get-Content -LiteralPath (Join-Path $OutputDirectory 'discovery.stdout') -Raw) -cnotmatch '^\s*\[\s*\]\s*$' -or
                    (Get-Content -LiteralPath (Join-Path $OutputDirectory 'mutants-version.stdout') -Raw).Trim() -cne 'cargo-mutants 27.1.0') {
                    $execution.completed = $true
                    return
                }
                $configText = Get-Content -LiteralPath (Join-Path $SourceRoot '.cargo\mutants.toml') -Raw
                $configuration = Get-ScheduledMutationConfig -Text $configText
                Set-Content -LiteralPath (Join-Path $OutputDirectory 'mutation-config.toml') -Value $configText -NoNewline
                $execution.stage = 'check'
                foreach ($phase in @('Build', 'Test')) {
                    $baselineCommand = Get-ScheduledEmptyBaselineCommand -Check $Check -MutationCommand $command `
                        -Configuration $configuration -Phase $phase
                    Write-ScheduledExecutionFile $execution $executionPath
                    $step = Invoke-ScheduledProcess -Command $baselineCommand -SourceRoot $SourceRoot `
                        -OutputDirectory $OutputDirectory -Name "baseline-$($phase.ToLowerInvariant())"
                    $execution.commands += @{ name = "baseline-$($phase.ToLowerInvariant())"; command = $baselineCommand
                        exit_code = $step.exit_code; timed_out = $step.timed_out }
                    $execution.exit_code = $step.exit_code
                    Get-Content -LiteralPath $step.stdout_path, $step.stderr_path | Add-Content -LiteralPath $logPath
                    if ($step.exit_code -ne 0 -or $step.timed_out) { $execution.completed = $true; return }
                }
            }
        } else {
            $execution.stage = 'metadata'
            $metadataCommand = @{ file = 'cargo'
                arguments = @("+$Toolchain", 'metadata', '--format-version=1', '--no-deps', '--locked')
                environment = @{ RUSTUP_TOOLCHAIN = $Toolchain } }
            Write-ScheduledExecutionFile $execution $executionPath
            $step = Invoke-ScheduledProcess -Command $metadataCommand -SourceRoot $SourceRoot `
                -OutputDirectory $OutputDirectory -Name 'metadata'
            $execution.commands += @{ name = 'metadata'; command = $metadataCommand; exit_code = $step.exit_code }
            $execution.exit_code = $step.exit_code
            Get-Content -LiteralPath $step.stdout_path, $step.stderr_path | Add-Content -LiteralPath $logPath
            if ($step.exit_code -ne 0) { $execution.completed = $true; return }
            $scopes = @(Get-ScheduledTestScope -Check $Check -OutputDirectory $OutputDirectory)
            $execution.stage = 'check'
            for ($index = 0; $index -lt $scopes.Count; $index++) {
                $selected = $scopes[$index]
                $scope = $Check.Clone()
                $scope.packages = @($selected.package)
                if ($null -ne $selected.target) { $scope.target = $selected.target }
                $packageCommand = Get-ScheduledCommand -Check $scope -SourceRoot $SourceRoot `
                    -OutputDirectory $OutputDirectory -Toolchain $Toolchain
                foreach ($key in @('CBH_FAKER', 'DURE_TEST_HELPER')) {
                    if ($command.environment.ContainsKey($key)) { $packageCommand.environment[$key] = $command.environment[$key] }
                }
                Write-ScheduledExecutionFile $execution $executionPath
                $step = Invoke-ScheduledProcess -Command $packageCommand -SourceRoot $SourceRoot `
                    -OutputDirectory $OutputDirectory -Name "check-$index"
                $execution.commands += @{ name = "check-$index"; package = $selected.package; target = $selected.target
                    command = $packageCommand; exit_code = $step.exit_code }
                if ($step.exit_code -ne 0) { $execution.exit_code = $step.exit_code }
                Get-Content -LiteralPath $step.stdout_path, $step.stderr_path | Add-Content -LiteralPath $logPath
            }
        }
        $execution.completed = $true
    } catch [ComponentModel.Win32Exception], [IO.IOException], [ArgumentException], [FormatException] {
        $execution.completed = $true
        $execution.stage = 'execution-error'
        Add-Content -LiteralPath $logPath -Value $_.Exception.ToString()
    } finally {
        Write-ScheduledExecutionFile $execution $executionPath
        $result = Get-ScheduledCheckResult -Check $Check -OutputDirectory $OutputDirectory -RunContext $RunContext
        Write-ScheduledExecutionFile $result (Join-Path $OutputDirectory 'evidence.json')
        # Emit the same result for both ordinary completion and early preparation failures.
        Write-Output $result
    }
}

Export-ModuleMember -Function Invoke-ScheduledCheck, Get-ScheduledCommand, Get-ScheduledCheckResult, Get-ScheduledToolchain
