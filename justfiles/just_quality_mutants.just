[group('quality')]
[script]
mutants SHARD="" CAREFUL='false':
    Set-StrictMode -Version Latest
    $ErrorActionPreference = "Stop"
    $PSNativeCommandUseErrorActionPreference = $true

    # The fiddly, easy-to-break parts of this recipe - the long platform-dependent exclusion set
    # and the 1-based -> 0-based shard translation - live in the Pester-tested Mutants module so a
    # stray edit is caught by a test, not by a wasted CI mutation run. See
    # scripts/build/Mutants.psm1. The recipe keeps only the orchestration with real side effects
    # (environment tweaks and the cargo-mutants invocation).
    Import-Module "{{ justfile_directory() }}/scripts/build/Mutants.psm1" -Force

    $careful = '{{ CAREFUL }}' -eq 'true'

    # Two mechanisms keep mutation runs fast and meaningful:
    #   * Path/package exclusions (the `-e` args from Get-MutantsExcludeArgument) drop whole files
    #     cargo-mutants should never mutate.
    #   * Individual tests that carry no mutation signal opt out at the test site with
    #     `#[cfg_attr(mutants, ignore = "<why>")]`. This keeps them running under normal
    #     `cargo test`/coverage while skipping them under mutation - see
    #     packages/cargo-bench-history for examples (the Azurite network tests and the
    #     real-engine end-to-end test). cargo-mutants defines no cfg of its own, so this
    #     recipe ensures `RUSTFLAGS` contains `--cfg mutants` below (preserving any
    #     existing flags) to drive that opt-out.
    $mutantArgs = @(Get-MutantsExcludeArgument -IsWindowsPlatform $IsWindows -IsLinuxPlatform $IsLinux)

    $mutantArgs += "--jobs"
    # Experimentally determined heuristics. Careful mode deliberately keeps this same
    # mutation-job parallelism; it only serializes the tests within each job (via
    # `RUST_TEST_THREADS=1` set below), so latent per-test timeouts are not masked by
    # concurrent tests.
    $mutantArgs += [int]([Math]::Floor([Environment]::ProcessorCount / 6)) + 1

    # Optional sharding lets CI split mutation testing across multiple parallel runners. We accept
    # the same 1-based "N/M" format as `miri-harder` (so `1/8 .. 8/8` for 8 shards); the module
    # converts it to cargo-mutants' native 0-based form. Without a SHARD argument, cargo-mutants
    # runs every mutant in a single job (the original behavior).
    $mutantArgs += Get-MutantsShardArgument -Spec "{{ SHARD }}"

    # We must use Invoke-Expression to preserve the quotes around the wildcarded arguments on Linux.
    $expanded_args = [String]::join(" ", $mutantArgs)

    # If the user has overridden `CARGO_TARGET_DIR`, we need to un-override it here because it is
    # critical that different mutations be emitted into different target directories, otherwise
    # the mutation test jobs will conflict with each other, running each others' binaries.
    if ($env:CARGO_TARGET_DIR) {
        $env:CARGO_TARGET_DIR = $null

        # If the user set a custom `CARGO_TARGET_DIR` they probably did it because there is a more
        # performant storage path available than the default, in which case they will also want to
        # specify `MUTANTS_TEMP` to direct mutation testing to the more performant storage.
        if (!$env:MUTANTS_TEMP) {
            Write-Warning "Consider setting a value for the MUTANTS_TEMP environment variable, as you have set CARGO_TARGET_DIR to a custom value. CARGO_TARGET_DIR is ignored during mutation testing."
        }
    }

    # If the user has specified a custom `MUTANTS_TEMP` directory, we need to set it here.
    # This is useful for performance, as it allows mutation testing to use a faster storage path
    # than the default (e.g. because real-time antivirus is not scanning it).
    if ($env:MUTANTS_TEMP) {
        # TMP is the variable used by cargo-mutants to determine where to put the files.
        $env:TMP = $env:MUTANTS_TEMP
    }

    # Disable test-local watchdogs so a timeout cannot count as catching a
    # mutation. Any mutation that would not terminate must be skipped at its
    # mutation site instead.
    $env:MUTATION_TESTING = "1"

    # Mirror the `just test*` recipes: pre-build the `cargo-bench-history-faker` test-support
    # binary and hand its absolute path to the cargo-bench-history integration tests via
    # CBH_FAKER. Without this, those tests build the faker on demand inside every
    # mutant's fresh target tree, and under the CPU contention of a full parallel mutation run
    # that per-mutant build can push an otherwise-fast test past the `--timeout` ceiling and
    # report a spurious TIMEOUT (issue #349). The faker is excluded from mutation (see
    # Get-MutantsExcludeArgument), so a pre-built binary masks no mutant. Build it before
    # RUSTFLAGS gains `--cfg mutants` below so the artifact matches the one `just test`
    # produces and neither run churns the other's cache.
    $env:CBH_FAKER = just _faker-path | Select-Object -Last 1
    if ($IsWindows) {
        $env:DURE_TEST_HELPER = just _dure-test-helper-path | Select-Object -Last 1
    }

    # Enable the `mutants` cfg so tests carrying `#[cfg_attr(mutants, ignore = "<why>")]`
    # opt out of mutation runs. cargo-mutants defines no cfg of its own, but it preserves
    # an existing RUSTFLAGS (chaining its own `--cap-lints=warn` onto it), so the flag we
    # set here reaches rustc in the temporary build. The `cfg(mutants)` name is registered
    # in the workspace `unexpected_cfgs` check-cfg list, so the zero-warnings policy holds.
    $existing_rustflags = if ($env:RUSTFLAGS) { $env:RUSTFLAGS } else { "" }
    if ($existing_rustflags -notlike "*--cfg mutants*") {
        $env:RUSTFLAGS = ($existing_rustflags, "--cfg mutants" | Where-Object { $_ }) -join " "
    }

    if ($careful) {
        # Disable libtest's intra-suite parallelism so each mutation job runs its tests
        # one at a time. Mutation jobs still run concurrently (the `--jobs` heuristic
        # above is unchanged); only test concurrency within a job is removed. Serial
        # tests surface latent per-test timeouts that concurrent execution can otherwise
        # hide, which is the entire point of the `mutants-careful` recipe.
        $env:RUST_TEST_THREADS = "1"
    }

    # --baseline=skip - We assume that regular tests have already been executed
    #     and are not failing, for mutation speedup.
    # --no-shuffle - We do not care about shuffling, that is something rarely useful.
    # --caught --unviable - We want to see a list of all the mutations tried, as it helps us
    #     understand what is covered by mutation testing and how successfully.
    # --timeout - as we skip the baseline tests, we need to manually define a timeout.
    #     This needs to be long enough to run a full package test suite (for all jobs in parallel)
    #     but short enough to call a timeout and avoid things going on forever.
    #     This should only be reached if a mutation causes a serious issue (and may need to be
    #     skipped or the test improved).
    Invoke-Expression "cargo mutants {{ target_package }} --baseline=skip --timeout=60 --no-shuffle --caught --unviable $expanded_args"
    exit $LASTEXITCODE

# Run mutation tests like `mutants` (mutation jobs still run in parallel) but with test concurrency disabled, so only one test runs at a time within each mutation job. Slower, but surfaces per-test timeouts that concurrent test execution can hide.
[group('quality')]
mutants-careful: (mutants "" "true")
