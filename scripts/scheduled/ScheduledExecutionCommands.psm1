#requires -Version 7

# ScheduledExecution invokes these controller-owned commands with argument arrays, never a shell.
# PowerShell owns process setup and capture; each checker owns its configuration and baselines.
# Ref: .github/workflows/implementation.md#immutable-execution.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
Import-Module (Join-Path $PSScriptRoot '..\build\Mutants.psm1')

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
            if ($Check.kind -ceq 'miri-many') {
                $environment.MIRIFLAGS = "-Zmiri-many-seeds=$($Check.seed_range)"
            }
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
    foreach ($argument in $Command.arguments) { $start.ArgumentList.Add($argument) }
    foreach ($key in $Command.environment.Keys) {
        if ($null -eq $Command.environment[$key]) { $null = $start.Environment.Remove($key) }
        else { $start.Environment[$key] = $Command.environment[$key] }
    }
    $process = Get-ScheduledProcess
    $process.StartInfo = $start
    $stdoutPath = Join-Path $OutputDirectory "$Name.stdout"
    $stderrPath = Join-Path $OutputDirectory "$Name.stderr"
    $stdout = $null
    $stderr = $null
    $started = $false
    try {
        $stdout = [IO.File]::Create($stdoutPath)
        $stderr = [IO.File]::Create($stderrPath)
        $started = $process.Start()
        if (-not $started) { throw 'Could not start the check.' }
        $copyOut = $process.StandardOutput.BaseStream.CopyToAsync($stdout)
        $copyErr = $process.StandardError.BaseStream.CopyToAsync($stderr)
        # Observe a failed capture immediately: waiting only for exit can leave a child blocked
        # on a full pipe after the reader failed. The finally block owns that child's lifetime.
        $pending = [Collections.Generic.List[Threading.Tasks.Task]]::new(
            [Threading.Tasks.Task[]]@($copyOut, $copyErr, $process.WaitForExitAsync()))
        while ($pending.Count -gt 0) {
            $completed = [Threading.Tasks.Task]::WhenAny([Threading.Tasks.Task[]]$pending).GetAwaiter().GetResult()
            $null = $completed.GetAwaiter().GetResult()
            $null = $pending.Remove($completed)
        }
        return @{
            exit_code = $process.ExitCode; stdout_path = $stdoutPath; stderr_path = $stderrPath
        }
    } finally {
        try {
            if ($started -and -not $process.HasExited) {
                try { $process.Kill($true) }
                catch [InvalidOperationException] {
                    # The child can exit between HasExited and Kill.
                    if (-not $process.HasExited) { throw }
                }
                $process.WaitForExit()
            }
        } finally {
            if ($null -ne $stdout) { $stdout.Dispose() }
            if ($null -ne $stderr) { $stderr.Dispose() }
            $process.Dispose()
        }
    }
}

function Get-ScheduledProcess {
    # Isolate construction so lifetime/error handling can use deterministic process doubles.
    [CmdletBinding()]
    [OutputType([Diagnostics.Process])]
    param()
    return [Diagnostics.Process]::new()
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

Export-ModuleMember -Function Get-ScheduledToolchain, Assert-ScheduledPlatform, Get-ScheduledCommand,
Invoke-ScheduledProcess, Get-ScheduledTestScope
