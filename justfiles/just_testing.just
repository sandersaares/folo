[group('testing')]
[script]
bench TARGET="":
    Set-StrictMode -Version Latest
    $ErrorActionPreference = "Stop"
    $PSNativeCommandUseErrorActionPreference = $true

    $target_selector = @()

    if ("{{ TARGET }}" -ne "") {
        $target_selector += "--bench"
        $target_selector += "{{ TARGET }}"
    }

    # Force 64-byte function alignment so hot loops keep a fixed position relative to the L1
    # instruction-cache lines across relinks; without it an unrelated dependency bump can reorder
    # functions and shift a byte-identical loop across a cache-line boundary, showing up as a
    # phantom wall-clock regression (see constants.env and the cargo-bench-history "Measurement
    # stability" guide). $env:BENCH_STABILITY_RUSTFLAGS comes from constants.env via dotenv. Treat
    # any RUSTFLAGS the caller set as an opaque string: remove only a pre-existing
    # -align-all-functions token (in any -C/--codegen spelling), then append the stability value so
    # the 64-byte setting always wins - never duplicated, never silently skipped in favour of a
    # stray caller value.
    $kept_rustflags = ("$env:RUSTFLAGS" -replace '(^|\s)(-C\s*|--codegen(?:\s+|=))llvm-args=-align-all-functions=\d+', '').Trim()
    $env:RUSTFLAGS = (@($kept_rustflags, $env:BENCH_STABILITY_RUSTFLAGS) | Where-Object { $_ }) -join " "

    cargo bench {{ target_package }} --all-features $target_selector --locked

# Run the wall-clock benchmarks tuned for the best achievable numbers rather than for stability
# across history: whole-program optimization (fat LTO, a single codegen unit) and NO alignment
# padding. Use this to see how fast the standard pipeline gets this code on the current machine -
# it drops the stability pessimizations `just bench` accepts, though it is not a guaranteed
# optimum (the compiler can still be suboptimal; PGO would be needed to go further). The results
# are deliberately NOT comparable with the `just bench` timeline that cargo-bench-history collects
# - the build profile differs - so never feed a `bench-best` run into the history store.
[group('testing')]
[script]
bench-best TARGET="":
    Set-StrictMode -Version Latest
    $ErrorActionPreference = "Stop"
    $PSNativeCommandUseErrorActionPreference = $true

    $target_selector = @()

    if ("{{ TARGET }}" -ne "") {
        $target_selector += "--bench"
        $target_selector += "{{ TARGET }}"
    }

    # Best-case means NO forced alignment, so remove any -align-all-functions token that may have
    # been inherited from the environment (e.g. an exported BENCH_STABILITY_RUSTFLAGS); otherwise
    # this would silently run aligned and stop being a true best case. Treat RUSTFLAGS as opaque
    # and strip only that token (in any -C/--codegen spelling).
    $env:RUSTFLAGS = ("$env:RUSTFLAGS" -replace '(^|\s)(-C\s*|--codegen(?:\s+|=))llvm-args=-align-all-functions=\d+', '').Trim()

    # Build under the `bench-best` profile (root Cargo.toml): the `release` base plus fat LTO and a
    # single codegen unit - the most aggressively optimized build the standard pipeline produces.
    # No alignment flag is set, so this path is free to reach layouts `just bench` pins away from -
    # that is the point: best case, not stable case. Note this is not a guaranteed optimum: the
    # compiler can still emit suboptimal code (closing that gap would need PGO), but it does remove
    # the explicit pessimizations we accept elsewhere in the name of measurement stability.

    cargo bench {{ target_package }} --profile bench-best --all-features $target_selector --locked

# Run Callgrind-based instruction-count benchmarks. Requires Linux (Valgrind)
# and the `gungraun-runner` binary on PATH; both are installed by
# `just install-tools`. Discovers any bench file whose name ends with `_cg`
# (short for Callgrind) under `packages/<pkg>/benches/`. Callers on Windows
# must invoke this through WSL themselves.
#
# Unlike `just bench`, this recipe deliberately does NOT force function alignment
# ($env:BENCH_STABILITY_RUSTFLAGS): Callgrind counts executed instructions, which are already
# independent of where functions land in memory, so alignment buys no stability here and only
# perturbs the build. The alignment flag exists purely to stabilize wall-clock timings.
[group('testing')]
[script]
bench-cg TARGET="":
    Set-StrictMode -Version Latest
    $ErrorActionPreference = "Stop"
    $PSNativeCommandUseErrorActionPreference = $true

    Import-Module "{{ justfile_directory() }}/scripts/build/BenchmarkTarget.psm1" -Force

    if (-not $IsLinux) {
        Write-Error "bench-cg requires Linux (Valgrind). On Windows, invoke through WSL from the repo root: wsl -e bash -l -c 'just bench-cg'"
        exit 1
    }

    if (-not (Get-Command gungraun-runner -ErrorAction SilentlyContinue)) {
        Write-Error "bench-cg requires the 'gungraun-runner' binary on PATH. Run 'just install-tools' to install it."
        exit 1
    }

    $package_filter = "{{ package }}"
    $target_filter = "{{ TARGET }}"

    $targets = @(Get-CallgrindBenchTarget -PackagesRoot "packages" -PackageFilter $package_filter -TargetFilter $target_filter)

    foreach ($benchTarget in $targets) {
        Write-Host "Running $($benchTarget.Package)::$($benchTarget.Bench)" -ForegroundColor Cyan
        cargo bench -p $($benchTarget.Package) --bench $($benchTarget.Bench) --all-features --locked
    }

    if ($targets.Count -eq 0) {
        $scope = if ($package_filter -eq "") {
            "the workspace"
        } else {
            $names = @($package_filter -split " " | Where-Object { $_ -ne "" })
            $noun = if ($names.Count -eq 1) { "package" } else { "packages" }
            "$noun '$package_filter'"
        }
        $target_clause = if ($target_filter -eq "") { "" } else { " matching target '$target_filter'" }
        Write-Host "No Callgrind benchmarks found in $scope$target_clause." -ForegroundColor Yellow
    }


[group('testing')]
miri FILTER="":
    cargo +{{ env_var('RUST_NIGHTLY') }} miri nextest run {{ target_package }} --no-fail-fast --all-features {{ FILTER }} --locked --no-tests=pass

[group('testing')]
[script]
miri-harder SHARD="" FILTER="":
    Set-StrictMode -Version Latest
    $ErrorActionPreference = "Stop"
    $PSNativeCommandUseErrorActionPreference = $true

    # The seed-range arithmetic (splitting the seed budget across CI shards, the final shard
    # absorbing the remainder) lives in the Pester-tested Miri module, which shares the "N/M"
    # shard-spec parsing with `just mutants`. See scripts/build/Miri.psm1.
    Import-Module "{{ justfile_directory() }}/scripts/build/Miri.psm1" -Force

    $seedRange = Get-MiriSeedRange -Spec "{{ SHARD }}"
    $env:MIRIFLAGS = "-Zmiri-many-seeds=$seedRange"
    Write-Host "Running miri-harder with seed range: $seedRange"
    cargo +$env:RUST_NIGHTLY miri test {{ target_package }} --no-fail-fast --all-features --lib {{ FILTER }} --locked -- --quiet

[group('testing')]
[script]
miri-example EXAMPLE:
    Set-StrictMode -Version Latest
    $ErrorActionPreference = "Stop"
    $PSNativeCommandUseErrorActionPreference = $true

    if ("{{ package }}" -eq "") {
        cargo +$env:RUST_NIGHTLY miri run --all-features --example {{ EXAMPLE }} --locked
    } else {
        cargo +$env:RUST_NIGHTLY miri run -p {{ package }} --all-features --example {{ EXAMPLE }} --locked
    }

[group('testing')]
[script]
test FILTER="":
    Set-StrictMode -Version Latest
    $ErrorActionPreference = "Stop"
    $PSNativeCommandUseErrorActionPreference = $true

    $env:CBH_FAKER = just _faker-path | Select-Object -Last 1
    if ($IsWindows) {
        $env:DURE_TEST_HELPER = just _dure-test-helper-path | Select-Object -Last 1
    }
    cargo +$env:RUST_MSRV nextest run {{ target_package }} --all-features {{ FILTER }} --locked --no-fail-fast

# Runs all script suites locally, or the union of space-separated script domains selected by CI.
# Invalid explicit selections fail instead of producing a successful empty Pester run.
# Ref: .github/workflows/implementation.md#non-cargo-change-planning.
[group('testing')]
[script]
test-scripts DOMAINS='':
    Set-StrictMode -Version Latest
    $ErrorActionPreference = "Stop"
    $PSNativeCommandUseErrorActionPreference = $true

    Import-Module Pester -MinimumVersion 5.0 -Force
    Import-Module ./scripts/build/ValidationPlan.psm1 -Force
    $config = New-PesterConfiguration
    $config.Run.Path = @(Get-ScriptTestPath -Domains "{{DOMAINS}}")
    $config.Run.Exit = $true
    $config.Output.Verbosity = 'Detailed'
    Invoke-Pester -Configuration $config

# Build the test-support `cargo-bench-history-faker` binary and print its path. The faker
# is a stand-in benchmark engine the cargo-bench-history integration tests drive; it lives
# in its own lib+bin package (see packages/cargo-bench-history-faker), so Cargo does not
# expose it via CARGO_BIN_EXE_* to the tests. The tests build and locate it on demand, but
# a test runner can call this once up front and pass the path via CBH_FAKER so each per-test
# process skips a redundant Cargo freshness check.
#
# The build targets a dedicated `<target>/faker` directory rather than the shared
# workspace `target/` tree (matching the Rust-side `engine_target_dir`). Cargo relinks a
# binary by unlinking it and hard-linking the replacement, so on the shared tree a
# concurrent build could momentarily leave the binary absent under a test that is spawning
# it; its own tree means only builds aimed here touch the file, closing that race (#332).
[group('testing')]
[script]
_faker-path:
    Set-StrictMode -Version Latest
    $ErrorActionPreference = "Stop"
    $PSNativeCommandUseErrorActionPreference = $true

    Import-Module "{{ justfile_directory() }}/scripts/build/CargoExecutable.psm1" -Force

    # Nest under the ambient CARGO_TARGET_DIR (e.g. a coverage run's tree) when set, else the
    # workspace `target/`, so the isolated tree stays git-ignored and cached like everything
    # else. The Rust resolver derives the same path, so a pre-built CBH_FAKER and an
    # on-demand build resolve to the very same isolated binary.
    $fakerTargetDir = Join-Path ($env:CARGO_TARGET_DIR ?? "target") "faker"
    $messages = cargo +$env:RUST_MSRV build -p cargo-bench-history-faker --target-dir $fakerTargetDir --message-format=json-render-diagnostics --locked
    Write-Output (Resolve-CargoExecutable -CargoMessage $messages -TargetName "cargo-bench-history-faker")

# Build the Windows integration-test helper before the test runner starts and
# print its path. This avoids running a nested Cargo build alongside the ConPTY
# scenarios that consume the helper.
[group('testing')]
[script]
_dure-test-helper-path:
    Set-StrictMode -Version Latest
    $ErrorActionPreference = "Stop"
    $PSNativeCommandUseErrorActionPreference = $true

    Import-Module "{{ justfile_directory() }}/scripts/build/CargoExecutable.psm1" -Force

    $targetDir = Join-Path ($env:CARGO_TARGET_DIR ?? "target") "dure-test-helper"
    $messages = cargo +$env:RUST_MSRV build -p dure-test-helper --target-dir $targetDir --message-format=json-render-diagnostics --locked
    Write-Output (Resolve-CargoExecutable -CargoMessage $messages -TargetName "dure-test-helper")

# Run the cargo-bench-history "stress analyze" harness against LOCAL filesystem
# storage: it seeds a giant synthetic benchmark history (default 1000 benchmarks ×
# 2000 commits over 12 months, ~half of which store a run, × 20 discriminant sets
# spanning every engine and the platforms it runs on, plus a short feature branch
# with dirty snapshots and some blessings) into a temporary store, then times each
# analysis mode (history / branch / tip) over it. The dataset is fabricated; the
# point is to observe how long `analyze` takes at scale, not to measure real code.
#
# Pass any harness flags through, e.g. a smaller scenario for a quick check:
#   just bench-history-stress --commits 100 --benchmarks 100
#   just bench-history-stress --verbose --modes history
# Release build, because both seeding and analysis are CPU-bound and a debug build
# distorts the timings badly.
[group('testing')]
[script]
bench-history-stress *ARGS:
    Set-StrictMode -Version Latest
    $ErrorActionPreference = "Stop"
    $PSNativeCommandUseErrorActionPreference = $true

    cargo run --release -p cargo-bench-history-stress --locked -- --storage local {{ ARGS }}

# Run the cargo-bench-history "stress analyze" harness against a REAL Azure storage
# account: it seeds the same giant synthetic history into a fresh blob container
# (created and, unless --keep, deleted automatically), then times each analysis
# mode over it. This exercises the real network round-trips that dominate the cost
# against cloud storage.
#
# Requires `az login` (the harness and azcopy authenticate as your Entra user via
# the Azure CLI) and the `azcopy` binary on PATH for the bulk upload - both are
# installed by `just install-tools`. The account defaults to
# BENCH_HISTORY_TEST_AZURE_ACCOUNT from constants.env (shared with the test-azure job);
# override it by passing a name as the first argument. Any further harness flags
# follow, e.g.:
#   just bench-history-stress-azure                       # default account, full scenario
#   just bench-history-stress-azure mystorageacct --keep  # custom account, keep the container
[group('testing')]
[script]
bench-history-stress-azure ACCOUNT=env_var_or_default('BENCH_HISTORY_TEST_AZURE_ACCOUNT', '') *ARGS:
    Set-StrictMode -Version Latest
    $ErrorActionPreference = "Stop"
    $PSNativeCommandUseErrorActionPreference = $true

    $account = "{{ ACCOUNT }}"
    if ([string]::IsNullOrEmpty($account)) {
        Write-Error "No storage account specified. Pass it (just bench-history-stress-azure <account> ...) or set BENCH_HISTORY_TEST_AZURE_ACCOUNT in constants.env. Deploy one with infra/azure-bench-history-test/deploy.ps1."
        exit 1
    }

    if (-not (Get-Command azcopy -ErrorAction SilentlyContinue)) {
        Write-Error "bench-history-stress-azure requires the 'azcopy' binary on PATH for the bulk upload. Run 'just install-tools' to install it."
        exit 1
    }

    Write-Host "Running the Azure stress harness against account '$account' (Microsoft Entra ID; requires az login)."
    cargo run --release -p cargo-bench-history-stress --locked -- --storage azure --account $account {{ ARGS }}

# Run the cargo-bench-history Azure storage tests against a local Azurite blob
# emulator running in `--oauth basic` mode (a locally-faked Microsoft Entra token
# over HTTPS), as opposed to `test-azure` which targets a REAL account via
# Microsoft Entra ID. Mirrors the CI `test-azurite` job for local verification.
#
# Requires the Azurite emulator (installed by `just install-tools`, or manually via
# `npm install -g azurite`). Because Entra is the only supported auth mode and it
# requires TLS, this recipe uses a self-signed certificate (generated once and cached
# under the Cargo target directory, so it is reused across runs and cleaned up with
# `target/`) and starts Azurite with `--cert/--pwd --oauth basic` over HTTPS; the tests
# trust that cert by disabling certificate validation. If an emulator is already listening on
# 127.0.0.1:10000 this recipe reuses it (which must therefore also be an HTTPS
# `--oauth basic` emulator); otherwise it starts one (in-memory, no persisted state)
# and stops it again when the tests finish - even on failure. BENCH_HISTORY_REQUIRE_AZURITE
# is set so an unreachable emulator is a hard error rather than a silent skip, mirroring
# the CI job. The `azure azurite` test filter selects the Azure-backend tests by the
# naming convention (the `storage::azure` unit tests and the `*_in_azurite` integration
# tests); the `*_in_real_azure` tests it also matches self-skip without ENABLE_AZURE.
[group('testing')]
[script]
test-azurite:
    Set-StrictMode -Version Latest
    $ErrorActionPreference = "Stop"
    $PSNativeCommandUseErrorActionPreference = $true

    Import-Module "{{ justfile_directory() }}/scripts/build/Azurite.psm1" -Force

    $blobHost = "127.0.0.1"
    $blobPort = 10000

    $startedByUs = $false
    $launcher = $null
    $emulatorPid = 0
    $certPassword = "azurite"
    try {
        if (Test-AzuriteReachable -BlobHost $blobHost -BlobPort $blobPort) {
            Write-Host "Azurite already reachable at ${blobHost}:${blobPort}; reusing the running emulator."
        } else {
            $shim = if ($IsWindows) { "azurite-blob.cmd" } else { "azurite-blob" }
            $azurite = Get-Command $shim -ErrorAction SilentlyContinue
            if (-not $azurite) {
                throw "Azurite is not installed. Install it with: npm install -g azurite"
            }

            # Cache the certificate under the Cargo target directory (honoring CARGO_TARGET_DIR and
            # any `build.target-dir` config) so it is generated once and reused, and cleaned up
            # whenever `target/` is removed.
            $targetDir = (cargo metadata --no-deps --format-version 1 | ConvertFrom-Json).target_directory
            $certPath = Join-Path $targetDir "azurite/selfsigned.pfx"
            if (Test-Path $certPath) {
                Write-Host "Reusing the cached self-signed certificate at $certPath."
            } else {
                New-Item -ItemType Directory -Path (Split-Path -Parent $certPath) -Force | Out-Null
                Write-Host "Generating a self-signed certificate at $certPath (cached for future runs)."
                New-AzuriteCertificate -PfxPath $certPath -Password $certPassword
            }

            $azuriteArgs = Get-AzuriteArgument -BlobHost $blobHost -BlobPort $blobPort -CertPath $certPath -Password $certPassword

            Write-Host "Starting Azurite blob emulator on ${blobHost}:${blobPort} (in-memory, HTTPS + OAuth)."
            $startParams = @{
                FilePath     = $azurite.Source
                ArgumentList = $azuriteArgs
                PassThru     = $true
            }
            if ($IsWindows) { $startParams.WindowStyle = "Hidden" }
            $launcher = Start-Process @startParams
            $startedByUs = $true

            $ready = $false
            foreach ($attempt in 1..30) {
                if (Test-AzuriteReachable -BlobHost $blobHost -BlobPort $blobPort) { $ready = $true; break }
                Start-Sleep -Seconds 1
            }
            if (-not $ready) {
                throw "Azurite did not become ready on ${blobHost}:${blobPort} within 30 seconds."
            }

            # The launcher may be a shim that spawns the real emulator as a child, so resolve the
            # process actually holding the port to stop it directly later.
            $emulatorPid = $launcher.Id
            if ($IsWindows) {
                $listener = Get-NetTCPConnection -LocalPort $blobPort -State Listen -ErrorAction SilentlyContinue |
                    Select-Object -First 1
                if ($listener) { $emulatorPid = [int] $listener.OwningProcess }
            }
            Write-Host "Azurite is ready (pid $emulatorPid)."
        }

        $env:BENCH_HISTORY_REQUIRE_AZURITE = "1"
        # Pin the endpoint to the emulator this recipe manages so a developer's pre-existing
        # AZURITE_BLOB_ENDPOINT cannot redirect the tests to a different target (the recipe would
        # then start/reuse one emulator while the tests hit another - non-hermetic).
        $env:AZURITE_BLOB_ENDPOINT = "https://${blobHost}:${blobPort}/devstoreaccount1"
        $env:CBH_FAKER = just _faker-path | Select-Object -Last 1
        Write-Host "Running Azurite-backed tests against ${blobHost}:${blobPort}."
        cargo +$env:RUST_MSRV nextest run -p cargo-bench-history --locked --no-fail-fast azure azurite
    } finally {
        if ($startedByUs) {
            Write-Host "Stopping the Azurite emulator we started."
            if ($emulatorPid -gt 0) { Stop-AzuriteProcessTree -ProcessId $emulatorPid }
            if ($launcher -and $launcher.Id -ne $emulatorPid) {
                Stop-AzuriteProcessTree -ProcessId $launcher.Id
            }
        }
    }

# Run the cargo-bench-history Azure storage tests against a REAL Azure account
# (the Microsoft Entra ID auth path), the counterpart of `test-azurite` which
# covers the local Azurite emulator path. Mirrors the CI `test-azure` job for
# local verification.
#
# Requires `az login` (the tests authenticate as your Entra user via the Azure
# CLI) and a deployed storage account that grants your user the `Storage Blob
# Data Contributor` role - see infra/azure-bench-history-test/ to deploy one. The
# account defaults to BENCH_HISTORY_TEST_AZURE_ACCOUNT from constants.env (shared with
# CI); override it by passing a name: `just test-azure <account>`.
#
# Setting ENABLE_AZURE is what actually runs the real-Azure tests: without it they
# self-skip, so the account name living in constants.env (and thus in every recipe's
# dotenv environment) does not make a plain `just test` run them. This recipe sets
# ENABLE_AZURE only here, so the real-Azure tests run for real only through it.
[group('testing')]
[script]
test-azure ACCOUNT=env_var_or_default('BENCH_HISTORY_TEST_AZURE_ACCOUNT', ''):
    Set-StrictMode -Version Latest
    $ErrorActionPreference = "Stop"
    $PSNativeCommandUseErrorActionPreference = $true

    $account = "{{ ACCOUNT }}"
    if ([string]::IsNullOrEmpty($account)) {
        Write-Error "No storage account specified. Pass it (just test-azure <account>) or set BENCH_HISTORY_TEST_AZURE_ACCOUNT in constants.env. Deploy one with infra/azure-bench-history-test/deploy.ps1."
        exit 1
    }

    $env:BENCH_HISTORY_TEST_AZURE_ACCOUNT = $account
    $env:ENABLE_AZURE = "1"
    $env:CBH_FAKER = just _faker-path | Select-Object -Last 1

    Write-Host "Running real-Azure tests against account '$account' (Microsoft Entra ID; requires az login)."
    cargo +$env:RUST_MSRV nextest run -p cargo-bench-history real_azure --locked --no-fail-fast

# Run the benchmark targets once to verify they do not panic. Their measurements are not
# consumed here; this is purely a "the benches still build and run" smoke check, separated
# from "test" because benchmarks are quite slow.
#
# Benches go through `cargo test --benches` (not nextest): this invokes each bench binary's
# main() once with `--test`, which Criterion treats as "run each bench once" and Gungraun
# treats as its normal one-shot Valgrind run - exactly the smoke check we want here.
[group('testing')]
[script]
test-benches FILTER="":
    Set-StrictMode -Version Latest
    $ErrorActionPreference = "Stop"
    $PSNativeCommandUseErrorActionPreference = $true

    cargo +$env:RUST_MSRV test {{ target_package }} --all-features {{ FILTER }} --locked --no-fail-fast --benches

# Smoke-run only Criterion benchmark targets. Local validation uses this narrower recipe so it
# verifies the practical wall-clock harnesses without running every Linux Callgrind scenario under
# Valgrind; CI uses `test-benches` above to cover both kinds.
[group('testing')]
[script]
test-benches-criterion FILTER="":
    Set-StrictMode -Version Latest
    $ErrorActionPreference = "Stop"
    $PSNativeCommandUseErrorActionPreference = $true

    Import-Module "{{ justfile_directory() }}/scripts/build/BenchmarkTarget.psm1" -Force

    $targets = @(Get-CriterionBenchTarget -PackagesRoot "packages" -PackageFilter "{{ package }}")
    foreach ($benchTarget in $targets) {
        $targetName = "$($benchTarget.Package)::$($benchTarget.Bench)"
        Write-Host "Smoke-running $targetName" -ForegroundColor Cyan
        cargo +$env:RUST_MSRV test -p $benchTarget.Package --bench $benchTarget.Bench `
            --all-features {{ FILTER }} --locked --no-fail-fast
    }

    if ($targets.Count -eq 0) {
        Write-Host "No Criterion benchmarks found in the selected package scope." `
            -ForegroundColor Yellow
    }

[group('testing')]
[script]
test-docs FILTER="":
    Set-StrictMode -Version Latest
    $ErrorActionPreference = "Stop"
    $PSNativeCommandUseErrorActionPreference = $true

    # Extract package name from target_package variable
    $target_package_var = "{{ target_package }}"
    
    if ($target_package_var -eq " --workspace") {
        # Workspace-wide doctests - run normally
        cargo +$env:RUST_MSRV test --all-features --doc {{ FILTER }} --locked
    } else {
        # One or more packages specified via -p flags. We need to check each package
        # individually because binary-only crates do not support doctests.
        $package_names = "{{ package }}".Split(" ", [StringSplitOptions]::RemoveEmptyEntries)

        foreach ($pkg in $package_names) {
            $lib_path = "packages/$pkg/src/lib.rs"

            if (Test-Path $lib_path) {
                cargo +$env:RUST_MSRV test -p $pkg --all-features --doc {{ FILTER }} --locked
            } else {
                # Binary-only crate - skip doctests because Cargo expects library targets
                # for doctests. Binary crates (with only main.rs) cannot contain doctests,
                # so running `cargo test --doc` on them results in an error rather than
                # gracefully running zero tests.
                Write-Host "Skipping doctests for binary-only package: $pkg"
            }
        }
    }

# Run all stand-alone example binaries to ensure they complete without panicking.
# Each example has a 30-second timeout to handle examples that run indefinitely.
# Supports both .rs files and subdirectories with main.rs files.
[group('testing')]
[script]
run-examples:
    Set-StrictMode -Version Latest
    $ErrorActionPreference = "Stop"
    $PSNativeCommandUseErrorActionPreference = $true

    Import-Module "{{ justfile_directory() }}/scripts/build/Examples.psm1" -Force

    $timeoutSeconds = 30
    $failures = @()
    $successCount = 0

    $targets = @(Get-ExampleTarget -PackagesRoot "packages" -PackageFilter "{{ package }}")

    if ($targets.Count -eq 0) {
        Write-Host "No examples found."
        return
    }

    $exampleWord = if ($targets.Count -eq 1) { "example" } else { "examples" }
    Write-Host "Running $($targets.Count) $exampleWord (timeout ${timeoutSeconds}s each)"
    Write-Host ""

    # Children inherit Cargo's resolved output directory instead of performing dependency discovery
    # inside each example's runtime budget. Ref: docs/build-and-tooling.md#example-execution.
    Initialize-ExampleEnvironment

    # Pre-compile each package's examples up front so first-time compilation does not eat into the
    # per-example execution timeout (on slow CI runners a cold compile can exceed it). A package
    # whose examples fail to build is recorded and its examples skipped, so one broken package does
    # not mask the others' results. Each affected example is recorded as its own failure so the
    # summary's total, successful and failed counts stay consistent (total = successful + failed).
    $buildFailed = @{}
    foreach ($pkg in ($targets | ForEach-Object { $_.Package } | Sort-Object -Unique)) {
        Write-Host "Building examples for package '$pkg'..." -ForegroundColor DarkGray
        try {
            cargo build --package $pkg --examples --all-features --locked
        } catch {
            Write-Host "Building examples for package '$pkg' failed" -ForegroundColor Red
            $buildFailed[$pkg] = $true
            foreach ($failedTarget in @($targets | Where-Object { $_.Package -eq $pkg })) {
                $failures += "$($failedTarget.Package)::$($failedTarget.Example) (build failed)"
            }
        }
    }

    # Runs one example in a child job so the parent can enforce a timeout; the final pipeline value
    # is the process exit code, which Invoke-ExampleRun peels off the captured output.
    $cargoRunner = {
        param($pkg, $exampleName)
        & cargo run --package $pkg --example $exampleName --all-features --locked 2>&1
        return $LASTEXITCODE
    }

    foreach ($target in $targets) {
        if ($buildFailed.ContainsKey($target.Package)) { continue }

        Write-Host "Running example '$($target.Example)' in package '$($target.Package)'..." -ForegroundColor Cyan
        $result = Invoke-ExampleRun -Package $target.Package -Example $target.Example -Command $cargoRunner -TimeoutSeconds $timeoutSeconds

        switch ($result.Status) {
            "Success" {
                Write-Host "  OK: '$($target.Example)' in package '$($target.Package)'" -ForegroundColor Green
                $successCount++
            }
            "Failed" {
                Write-Host "  FAIL: '$($target.Example)' in package '$($target.Package)' (exit code $($result.ExitCode))" -ForegroundColor Red
                if (-not [string]::IsNullOrWhiteSpace($result.Output)) {
                    Write-Host "Output:" -ForegroundColor Yellow
                    Write-Host $result.Output -ForegroundColor DarkYellow
                }
                $failures += "$($target.Package)::$($target.Example) (exit code $($result.ExitCode))"
            }
            "Timeout" {
                Write-Host "  TIMEOUT: '$($target.Example)' in package '$($target.Package)' after ${timeoutSeconds}s" -ForegroundColor Red
                if (-not [string]::IsNullOrWhiteSpace($result.Output)) {
                    Write-Host "Partial output:" -ForegroundColor Yellow
                    Write-Host $result.Output -ForegroundColor DarkYellow
                }
                $failures += "$($target.Package)::$($target.Example) (timeout)"
            }
            "Exception" {
                Write-Host "  ERROR: '$($target.Example)' in package '$($target.Package)': $($result.Message)" -ForegroundColor Red
                $failures += "$($target.Package)::$($target.Example) (exception: $($result.Message))"
            }
        }
    }

    Write-Host ""
    Write-Host "Summary:" -ForegroundColor White
    Write-Host "  Total examples: $($targets.Count)" -ForegroundColor White
    Write-Host "  Successful: $successCount" -ForegroundColor Green
    $failedColor = if ($failures.Count -eq 0) { "Green" } else { "Red" }
    Write-Host "  Failed: $($failures.Count)" -ForegroundColor $failedColor

    if ($failures.Count -gt 0) {
        Write-Host ""
        Write-Host "Failed examples:" -ForegroundColor Red
        foreach ($failure in $failures) {
            Write-Host "  - $failure" -ForegroundColor Red
        }
        exit 1
    }
