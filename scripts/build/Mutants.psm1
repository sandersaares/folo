#requires -Version 7

# Argument construction for `just mutants`, the cargo-mutants mutation-testing recipe.
#
# The recipe's fiddly, easy-to-break parts are the long list of platform-dependent path/package
# exclusions and the shard-spec conversion; both live here with Pester coverage so a stray edit to
# the exclusion set (or the exact 1-based -> 0-based shard translation cargo-mutants wants) is
# caught by a test rather than by a wasted CI mutation run. The recipe keeps only the orchestration
# that has real side effects (environment tweaks and the final cargo-mutants invocation).
#
# cargo-mutants can theoretically read exclusions from a config file, but it cannot merge that
# static set with the dynamic, platform-derived set below, so every exclusion is specified as a
# command-line `-e` argument here (see cargo-mutants issue #527).

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true

Import-Module (Join-Path $PSScriptRoot 'Sharding.psm1')

function Get-MutantsExcludeArgument {
    # Builds the ordered list of `-e <glob-or-name>` exclusion arguments for cargo-mutants, given
    # the current platform. On non-Windows PowerShell, values that look like wildcard globs are
    # single-quoted so PowerShell's native globbing does not expand them before cargo-mutants sees
    # the literal pattern; on Windows there is no such globbing, so they are passed through as-is.
    # The Windows- and Linux-only source files are excluded on the OTHER platforms (there is no
    # point mutating code that is not even compiled here).
    [CmdletBinding()]
    [OutputType([string[]])]
    param(
        [Parameter(Mandatory)][bool] $IsWindowsPlatform,
        [Parameter(Mandatory)][bool] $IsLinuxPlatform,
        [switch] $Literal
    )

    $literalArguments = $Literal.IsPresent
    function protect([string] $value) {
        if ($literalArguments -or $IsWindowsPlatform) {
            return $value
        }
        # Single-quote so PowerShell on Linux does not glob-expand the literal pattern.
        return "'" + $value + "'"
    }

    $exclude = @(
        # Parts of this package require Criterion to work and other parts are currently not tested
        # as there is no public way to simulate a system topology for `many_cpus`.
        '-e', 'many_cpus_benchmarking',

        # Macros are tested via the impl package; mutations in the middle layer might not be detected.
        '-e', 'linked_macros',

        # We do not test facades, as they are just trivial code that forwards calls to real impls.
        '-e', (protect '**/*facade.rs'),
        '-e', 'facade',

        # We have limited coverage of platform bindings because it can be difficult to set up the
        # right scenarios for each, given they are platform-dependent. Instead, we test higher
        # level code using a mock platform.
        '-e', 'bindings',

        # This is just a different type of bindings, skipped for the same reason as `bindings` above.
        '-e', (protect 'packages/many_cpus_impl/src/pal/linux/filesystem/**'),

        # These packages are literally full of synchronization primitives, so 95% of mutations
        # will just cause tests to time out and never complete - pointless to try mutate this stuff.
        '-e', 'events_once',
        '-e', 'awaiter_set',
        '-e', 'events',

        # All this is code only used in tests/benchmarks - we do not test this code itself.
        '-e', (protect 'packages/testing/**'),
        '-e', (protect 'packages/benchmarks/**'),

        # The benchmark faker (its own `cargo-bench-history-faker` lib+bin package, which the
        # integration tests spawn) is test-support scaffolding. It is published so sibling repos
        # can consume it, but it carries no supported API contract and drives no production
        # behaviour, so mutating it yields no meaningful coverage signal.
        '-e', (protect 'packages/cargo-bench-history-faker/**'),

        # The appendix figure generator is unpublished book infrastructure. Mutating drawing
        # helpers and generated-table formatters yields no production coverage signal; the
        # lockstep tests exist to keep the book honest, not to certify plotters call sites.
        '-e', (protect 'packages/cargo-bench-history-figures/**'),

        # `testing.rs` is an in-workspace test utility (gated behind the `private-test-util`
        # feature, consumed only by the shell crate's tests). It is scaffolding with no public
        # API contract, so mutating it yields no production coverage signal.
        '-e', (protect 'packages/cbh_detect/src/testing.rs'),

        # `examples.rs` and `scatter.rs` are the shared example-series fixtures (gated behind
        # `private-test-util`): the curated data sets the appendix figures and detector tests
        # draw from, plus the deterministic noise source that scatters them. They are fixture
        # scaffolding with no production behaviour, so mutating a generated data point yields
        # no coverage signal - the tests assert on detector verdicts, not on fixture contents.
        '-e', (protect 'packages/cbh_detect/src/detect/examples.rs'),
        '-e', (protect 'packages/cbh_detect/src/detect/scatter.rs'),

        # Some of our systems are single-processor, yet the code may only be meaningfully testable
        # on multi-processor systems. As a "good enough" approximation, we skip mutation testing
        # of code that is only testable in a multi-processor system.
        '-e', (protect 'packages/par_bench/src/resource_usage_ext.rs'),

        # The platform-bound leaves of the `dure` PAL: `windows.rs` is a thin Win32 binding
        # exercised through integration tests rather than unit tests. `memory.rs` is a test
        # fake. `raw_handle.rs` is the same kind of Win32 binding without the `windows.rs`
        # suffix: a handle wrapper whose whole behaviour is `CancelIoEx` and `CloseHandle`.
        # The globs reach any depth because a PAL slice may nest its own submodules.
        # The portable PAL logic above them stays in scope.
        '-e', (protect 'packages/dure/src/pal/**/windows.rs'),
        '-e', (protect 'packages/dure/src/pal/**/memory.rs'),
        '-e', (protect 'packages/dure/src/pal/raw_handle.rs'),

        # The `dure` outbox is a queue guarded by a mutex and a condvar with a writer thread
        # behind it, so nearly every mutation there stops the writer from making progress and
        # hangs the test rather than failing it. Same reason as the synchronization-primitive
        # packages above. The policy it enforces lives in `constants.rs`, which stays in scope.
        '-e', (protect 'packages/dure/src/outbox.rs'),

        # The integration-test helper package and in-crate test support are not product code.
        '-e', (protect 'packages/dure-test-helper/**/*.rs'),
        '-e', (protect 'packages/dure/src/test_support.rs')
    )

    if (-not $IsWindowsPlatform) {
        $exclude += '-e'
        $exclude += (protect '**/*windows.rs')
        $exclude += '-e'
        $exclude += 'windows'

        # `dure` is a Windows-only tool whose crate root is `#![cfg(windows)]`, so on any other
        # platform it compiles to an empty stub with no tests. cargo-mutants reads source rather
        # than compiled code, so it would still generate mutants there - and every one of them
        # would be reported as missed because there is nothing to kill it.
        $exclude += '-e'
        $exclude += (protect 'packages/dure/**/*.rs')
    }

    if (-not $IsLinuxPlatform) {
        $exclude += '-e'
        $exclude += (protect '**/*linux.rs')
        $exclude += '-e'
        $exclude += 'linux'
    }

    return $exclude
}

function Get-MutantsShardArgument {
    # Converts the shared 1-based "N/M" shard spec into cargo-mutants' native 0-based `--shard`
    # argument (`@('--shard', '0/M') .. @('--shard', '(M-1)/M')`). An empty spec means "no
    # sharding" and yields an empty array so every mutant runs in one job. Throws (via Sharding)
    # for a malformed spec.
    [CmdletBinding()]
    [OutputType([string[]])]
    param(
        [Parameter(Mandatory)][AllowEmptyString()][string] $Spec
    )

    if ($Spec -eq '') {
        return @()
    }

    $shard = ConvertFrom-ShardSpec -Spec $Spec
    return @('--shard', ('{0}/{1}' -f ($shard.Index - 1), $shard.Count))
}

function Get-MutantsReplayArgument {
    # Names include the source location for exact selection; incident identities deliberately do
    # not. The discovery pass must confirm that this name still denotes the intended mutation.
    [CmdletBinding()]
    [OutputType([string[]])]
    param([Parameter(Mandatory)][hashtable] $Mutant)

    if (-not $Mutant.ContainsKey('name') -or [string]::IsNullOrWhiteSpace($Mutant.name)) {
        throw [ArgumentException]::new('A replay requires the name from cargo-mutants discovery.')
    }
    # Escape Rust regex metacharacters, not shell syntax. Arguments are never shell expressions.
    $pattern = [regex]::Replace($Mutant.name, '([\\.^$|?*+()[\]{}])', '\$1')
    return @('--re', "^$pattern`$")
}

Export-ModuleMember -Function Get-MutantsExcludeArgument, Get-MutantsShardArgument, Get-MutantsReplayArgument
