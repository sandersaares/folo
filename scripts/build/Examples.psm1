#requires -Version 7

# Backs the `run-examples` recipe: prepares the child environment, discovers stand-alone example
# binaries and runs each one under a timeout, classifying the outcome.
#
# `run-examples` executes every example in the workspace to prove they complete without panicking.
# Two parts are easy to get subtly wrong and worth testing in isolation: (1) discovering the example
# targets, which come in two shapes - `examples/<name>.rs` files and `examples/<name>/main.rs`
# directories - with a hard-coded skip-list of examples that loop forever or panic by design; and
# (2) running an example under a timeout and turning the job's mixed output-plus-exit-code stream
# into a clean result (the exit-code-is-the-last-item slicing is a classic off-by-one). Extracting
# both also collapses the recipe's two near-identical run blocks (files vs. subdirectories) into one.

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
$VerbosePreference = 'Continue'

function Initialize-ExampleEnvironment {
    # Resolve Cargo's configured target directory once, without resolving/downloading the workspace
    # dependency graph. Criterion otherwise starts a full `cargo metadata` subprocess from its
    # constructor, and the measurement trackers also rediscover this directory on output.
    # Export the absolute path before spawning children so neither repeats that work.
    # Ref: docs/build-and-tooling.md#example-execution.
    [CmdletBinding()]
    param()

    $metadata = cargo metadata --format-version 1 --no-deps --locked | ConvertFrom-Json
    if ([string]::IsNullOrWhiteSpace($metadata.target_directory)) {
        throw 'Cargo metadata did not provide an example output directory.'
    }

    $env:CARGO_TARGET_DIR = $metadata.target_directory
    $env:IS_TESTING = '1'
}

function Get-ExampleTarget {
    # Returns the runnable example targets under $PackagesRoot as objects with `Package` and
    # `Example` properties, sorted for deterministic ordering. An empty $PackageFilter scans every
    # package that has a Cargo.toml; otherwise it is a space-separated allow-list (e.g. cargo-delta
    # output). Both example shapes are discovered: `examples/<name>.rs` files (excluding `mod.rs`)
    # and `examples/<name>/main.rs` subdirectories. Names in $ExcludedExample are dropped.
    [CmdletBinding()]
    [OutputType([pscustomobject])]
    param(
        [Parameter(Mandatory)][string] $PackagesRoot,
        [string] $PackageFilter = '',
        # Examples that are expected to loop forever or panic by design, so `run-examples` must not
        # try to run them to completion.
        [string[]] $ExcludedExample = @(
            'nm_otel_console',                    # runs an infinite loop by design
            'nm_otel_readme',                     # runs an infinite loop by design
            'alloc_tracker_panic_on_alloc_demo'   # intentionally panics to demonstrate functionality
        )
    )

    $packages = if ([string]::IsNullOrWhiteSpace($PackageFilter)) {
        Get-ChildItem -LiteralPath $PackagesRoot -Directory |
            Where-Object { Test-Path -LiteralPath (Join-Path $_.FullName 'Cargo.toml') } |
            ForEach-Object { $_.Name } |
            Sort-Object
    } else {
        # Support space-separated package lists (e.g. from cargo-delta output).
        $PackageFilter -split ' ' | Where-Object { $_ -ne '' }
    }

    $targets = [System.Collections.Generic.List[pscustomobject]]::new()
    foreach ($package in $packages) {
        $examplesDir = Join-Path $PackagesRoot $package 'examples'
        if (-not (Test-Path -LiteralPath $examplesDir)) { continue }

        $names = [System.Collections.Generic.List[string]]::new()

        # `examples/<name>.rs` files, excluding a `mod.rs` shared module.
        Get-ChildItem -LiteralPath $examplesDir -Filter '*.rs' |
            Where-Object { $_.Name -ne 'mod.rs' } |
            ForEach-Object { $names.Add($_.BaseName) }

        # `examples/<name>/main.rs` subdirectories.
        Get-ChildItem -LiteralPath $examplesDir -Directory |
            Where-Object { Test-Path -LiteralPath (Join-Path $_.FullName 'main.rs') } |
            ForEach-Object { $names.Add($_.Name) }

        foreach ($name in ($names | Sort-Object)) {
            if ($ExcludedExample -contains $name) { continue }
            $targets.Add([pscustomobject]@{ Package = $package; Example = $name })
        }
    }
    return $targets.ToArray()
}

function Invoke-ExampleRun {
    # Runs a single example via $Command (a scriptblock receiving $Package and $Example whose final
    # pipeline value is the process exit code) under a $TimeoutSeconds watchdog, and returns a result
    # object with `Status` ('Success' | 'Failed' | 'Timeout' | 'Exception'), `ExitCode`, `Output`
    # (captured stdout/stderr minus the exit code, or all partial output on timeout) and `Message`.
    # Injecting $Command keeps the timeout/classification logic testable without invoking cargo.
    [CmdletBinding()]
    [OutputType([pscustomobject])]
    param(
        [Parameter(Mandatory)][string] $Package,
        [Parameter(Mandatory)][string] $Example,
        [Parameter(Mandatory)][scriptblock] $Command,
        [int] $TimeoutSeconds = 30
    )

    $job = $null
    try {
        $job = Start-Job -ScriptBlock $Command -ArgumentList $Package, $Example
        $completed = Wait-Job -Job $job -Timeout $TimeoutSeconds

        if (-not $completed) {
            Stop-Job -Job $job
            # The final pipeline value is an exit code only for a completed command. Retain all
            # available output here so a stalled phase remains diagnosable.
            $output = @(Receive-Job -Job $job) -join "`n"
            return [pscustomobject]@{
                Status   = 'Timeout'
                ExitCode = $null
                Output   = $output
                Message  = "timed out after $TimeoutSeconds seconds"
            }
        }

        $items = @(Receive-Job -Job $job)
        if ($items.Count -eq 0) {
            $exitCode = $null
            $output = ''
        } elseif ($items.Count -eq 1) {
            $exitCode = $items[0]
            $output = ''
        } else {
            $exitCode = $items[-1]
            $output = ($items[0..($items.Count - 2)] -join "`n")
        }

        if ($exitCode -eq 0) {
            return [pscustomobject]@{ Status = 'Success'; ExitCode = 0; Output = $output; Message = '' }
        }
        return [pscustomobject]@{
            Status   = 'Failed'
            ExitCode = $exitCode
            Output   = $output
            Message  = "failed with exit code $exitCode"
        }
    } catch {
        return [pscustomobject]@{
            Status   = 'Exception'
            ExitCode = $null
            Output   = ''
            Message  = $_.Exception.Message
        }
    } finally {
        if ($null -ne $job) { Remove-Job -Job $job -Force }
    }
}

Export-ModuleMember -Function Initialize-ExampleEnvironment, Get-ExampleTarget, Invoke-ExampleRun
