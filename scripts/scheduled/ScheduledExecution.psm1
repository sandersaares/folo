#requires -Version 7

# Typed execution and independently reproducible evidence for the scheduled check catalog.
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
    # No shell participates: ArgumentList preserves each value on Windows and Unix alike.
    # Drain both pipes concurrently so compiler stderr cannot deadlock a full stdout pipe.
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
        $process.WaitForExit()
        $null = $copyOut.GetAwaiter().GetResult()
        $null = $copyErr.GetAwaiter().GetResult()
        return @{ exit_code = $process.ExitCode; stdout_path = $stdoutPath; stderr_path = $stderrPath }
    } finally {
        $stdout.Dispose()
        $stderr.Dispose()
        $process.Dispose()
    }
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
        [Parameter(Mandatory)][string] $OutputDirectory
    )

    $outcomesPath = Join-Path $OutputDirectory 'mutants.out\outcomes.json'
    $inventoryPath = Join-Path $OutputDirectory 'mutants.out\mutants.json'
    if (-not (Test-Path -LiteralPath $outcomesPath) -or -not (Test-Path -LiteralPath $inventoryPath)) {
        $Result.summary = 'Mutation output or selected-mutant inventory is missing.'
        return
    }
    $lab = Get-Content -LiteralPath $outcomesPath -Raw | ConvertFrom-Json -AsHashtable
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
    if ($miriError.Success -and $Check.kind -in @('miri', 'miri-many')) {
        $running = [regex]::Matches($stdout, '(?m)^test ([^\r\n]+?) \.\.\.')
        $test = if ($running.Count -gt 0) { $running[-1].Groups[1].Value } else { $Check.test_filter }
        if ($test -eq '') {
            $Result.summary = 'Miri failed without a reproducible test identity.'
            return
        }
        $failedTests = @($test)
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
            $seed = ''
            $explicitSeed = @($flags | Where-Object { $_ -match '^-Zmiri-seed=\d+$' })
            if ($explicitSeed.Count -eq 1) { $seed = $explicitSeed[0].Substring('-Zmiri-seed='.Length) }
            $reportedSeed = [regex]::Matches($text, '(?i)(?:-Zmiri-seed=|(?:failing|failed|current) seed[:= ]+)(\d+)')
            if ($reportedSeed.Count -gt 0) { $seed = $reportedSeed[-1].Groups[1].Value }
            if ($Check.kind -eq 'miri-many' -and $seed -eq '') {
                $Result.summary = 'Many-seed failure did not identify its failing seed.'
                return
            }
            $replay = $Check.Clone()
            $replay.shard = ''
            $replay.seed_range = ''
            $replay.test_filter = $test
            if ($seed -ne '') {
                $flags = @($flags | Where-Object { $_ -notmatch '^-Zmiri-(?:many-seeds|seed)=' }) + "-Zmiri-seed=$seed"
            }
            if ($Check.kind -eq 'miri' -and $seed -eq '') {
                # Miri's ordinary execution uses its deterministic default seed.
                $seed = '0'
                $flags += '-Zmiri-seed=0'
            }
            $replay.flags = $flags
            $identityTest = $test
            $identityTarget = $null
            if ($Check.ContainsKey('target')) {
                $identityTarget = $Check.target.Clone()
                # Target qualification remains in the established test identity field so reporters
                # using its semantic allow-list distinguish identical names in different binaries.
                # The replay filter remains the original libtest name, not this qualified identity.
                $identityTest = "$($identityTarget.kind):$($identityTarget.name)::$test"
            }
            $Result.findings += @{
                identity = @{
                    kind = $Check.kind; package = $Check.packages[0]; platform = $Check.platform
                    path = ''; function = ''; mutation = ''; test = $identityTest; seed = $seed; flags = $flags
                    target = $identityTarget
                }
                summary = if ($miriError.Success) { $miriError.Value } else { "Failed test: $test" }
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
            if ($runs.Count -ne 1 -or $runs[0].exit_code -ne $execution.exit_code) {
                throw [FormatException]::new('Missing or inconsistent mutation execution record.')
            }
            $expected = Get-ScheduledCommand -Check $Check -SourceRoot $execution.source_root `
                -OutputDirectory $execution.output_directory -Toolchain $toolchain
            Assert-ScheduledRecordedCommand -Expected $expected -Recorded $runs[0].command
            Get-ScheduledMutationResult -Result $result -Check $Check -OutputDirectory $OutputDirectory
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
