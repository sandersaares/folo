# Whether `install-tools` installs azcopy / Azurite. Both default to installing, for a complete
# local setup. CI overrides them via the setup-environment composite action (which runs this exact
# recipe): it always sets skip_azcopy=true (only the on-demand `just bench-history-stress-azure`
# uses azcopy) and sets skip_azurite=true on every job except test-azurite, because Azurite's npm
# install adds ~3 minutes and only that job needs the emulator.
skip_azcopy := "false"
skip_azurite := "false"

# Installs the complete set of Rust toolchains and other tools for local PC development.
[group('setup')]
[script]
install-tools:
    Set-StrictMode -Version Latest
    $ErrorActionPreference = "Stop"
    $PSNativeCommandUseErrorActionPreference = $true

    Import-Module "{{ justfile_directory() }}/scripts/setup/RustToolchain.psm1" -Force

    # This is the single source of truth for which tools - and which versions - get installed. CI
    # runs this same recipe on nearly every job via the setup-environment composite action, so
    # there is no separate CI install path to keep in sync. The only per-job variation is the
    # skip_azcopy / skip_azurite flags above, which trim two tools no ordinary CI job needs.

    # First, we install/upgrade the stable toolchain in `rust-toolchain.toml`. Omitting Channel is
    # intentional: rustup reads the manifest, including its requested components.
    Install-RustupToolchain

    # Then we install the other toolchains used by specific commands.
    Install-RustupToolchain -Channel $env:RUST_MSRV
    Install-RustupToolchain -Channel $env:RUST_NIGHTLY `
        -Component @('miri', 'rustfmt', 'rust-src', 'llvm-tools-preview')

    # Dedicated nightly for `just check-external-types` (see constants.env,
    # RUST_NIGHTLY_EXTERNAL_TYPES, for why it must be separate from RUST_NIGHTLY). The minimal
    # profile is enough: the check only drives rustdoc, which ships with the rustc component - it
    # needs neither miri/rustfmt nor rust-src.
    Install-RustupToolchain -Channel $env:RUST_NIGHTLY_EXTERNAL_TYPES -InstallProfile minimal

    # Every tool is pinned to an explicit version from constants.env (dotenv, configured in the
    # root justfile, exposes each `FOO_VERSION` as `$env:FOO_VERSION`). This is deliberate: a plain
    # `cargo install <crate>` never upgrades an already-installed crate - it prints "already
    # installed" and skips - so an unpinned tool would freeze at whatever version first landed. With
    # a pin, `cargo install <crate>@<version>` replaces an installed version that differs from the
    # pin and is a fast no-op when it already matches, so bumping a tool is just editing its version
    # in constants.env. (cargo-mutants is installed further down because it is skipped on ARM.)
    cargo install `
        cargo-audit@$env:CARGO_AUDIT_VERSION `
        cargo-careful@$env:CARGO_CAREFUL_VERSION `
        cargo-delta@$env:CARGO_DELTA_VERSION `
        cargo-ensure-no-default-features@$env:CARGO_ENSURE_NO_DEFAULT_FEATURES_VERSION `
        cargo-freeze-deps@$env:CARGO_FREEZE_DEPS_VERSION `
        cargo-hack@$env:CARGO_HACK_VERSION `
        cargo-llvm-cov@$env:CARGO_LLVM_COV_VERSION `
        cargo-machete@$env:CARGO_MACHETE_VERSION `
        cargo-nextest@$env:CARGO_NEXTEST_VERSION `
        cargo-semver-checks@$env:CARGO_SEMVER_CHECKS_VERSION `
        cargo-sort@$env:CARGO_SORT_VERSION `
        cargo-sort-derives@$env:CARGO_SORT_DERIVES_VERSION `
        cargo-udeps@$env:CARGO_UDEPS_VERSION `
        release-plz@$env:RELEASE_PLZ_VERSION `
        --locked

    # cargo-check-external-types powers `just check-external-types`. It is installed from a git
    # commit rather than the pinned `@version` block above because the latest crates.io release
    # lacks the multi-target/workspace support this repo needs; see constants.env
    # (CARGO_CHECK_EXTERNAL_TYPES_GIT / CARGO_CHECK_EXTERNAL_TYPES_REV) for the full rationale and
    # the condition for switching back to a crates.io pin. `--locked` builds against the commit's
    # own Cargo.lock so the result is reproducible.
    #
    # Unlike the `@version` tools above, a plain reinstall cannot reconcile a git pin: cargo keys
    # "already installed" on the crate *version*, which does not change between the commits we pin,
    # so it would keep an older commit after a rev bump. rust-cache compounds this by restoring the
    # previously compiled binary under a key that excludes the rev. So we read the commit cargo
    # recorded in .crates2.json and reinstall with --force only when it differs from the pin: a rev
    # bump always takes effect, while an unchanged pin stays a no-op instead of recompiling the tool
    # on every CI job. constants.env pins a full 40-character commit SHA, which .crates2.json records
    # verbatim, so an exact comparison suffices.
    $cargoHome = if ($env:CARGO_HOME) { $env:CARGO_HOME } else { Join-Path $HOME ".cargo" }
    $cratesMetadata = Join-Path $cargoHome ".crates2.json"
    $installedRev = ""
    if (Test-Path -LiteralPath $cratesMetadata) {
        $json = Get-Content -LiteralPath $cratesMetadata -Raw | ConvertFrom-Json
        if ($json.PSObject.Properties.Name -contains "installs") {
            $entry = $json.installs.PSObject.Properties.Name |
                Where-Object { $_ -like "cargo-check-external-types *git+*" } |
                Select-Object -First 1
            if ($entry -and $entry -match "#([0-9a-f]{40})\)$") {
                $installedRev = $Matches[1]
            }
        }
    }

    if ($installedRev -ne $env:CARGO_CHECK_EXTERNAL_TYPES_REV) {
        cargo install cargo-check-external-types `
            --git $env:CARGO_CHECK_EXTERNAL_TYPES_GIT `
            --rev $env:CARGO_CHECK_EXTERNAL_TYPES_REV `
            --locked --force
    }

    # cargo-mutants transitively depends on `winapi 0.2.8`, which only defines its
    # types for `target_arch = "x86"` / `"x86_64"` and fails to compile on aarch64
    # with errors like `cannot find type 'ULONG_PTR' in this scope`. Until that
    # transitive dependency is updated upstream, skip the install on ARM. The CI
    # `mutants` job only runs on x86_64 runners, so this only affects local dev on
    # ARM machines (where running mutation testing locally is not supported).
    $arch = [System.Runtime.InteropServices.RuntimeInformation]::ProcessArchitecture
    if ($arch -ne [System.Runtime.InteropServices.Architecture]::Arm64) {
        cargo install cargo-mutants@$env:CARGO_MUTANTS_VERSION --locked
    } else {
        Write-Host "Skipping cargo-mutants install on ARM64 (winapi 0.2.8 has no aarch64 support)."
    }

    # gungraun-runner drives Valgrind for Callgrind-based instruction-count benchmarks. Only
    # useful on Linux (Valgrind is Linux-only); on Windows and macOS the Callgrind bench binaries
    # are no-op stubs.
    #
    # The Callgrind bench binary is just a shim that forwards its CLI args (including the libtest
    # --list / --format / --ignored flags that `cargo bench` and `nextest` use during target
    # enumeration) to gungraun-runner via an encoded payload. Without gungraun-runner on $PATH,
    # any invocation of a `*_cg` bench binary fails with "Failed to run benchmarks: No such
    # file or directory", including the harmless --list call that nextest issues for `--benches`.
    #
    # The version lives in constants.env (GUNGRAUN_RUNNER_VERSION) and must match the exact patch of
    # the `gungraun = "=X.Y.Z"` workspace entry in the root Cargo.toml. gungraun-runner enforces
    # strict string equality on the version (see `gungraun-runner::runner::compare_versions`), so
    # any drift between the installed runner and the library causes `*_cg` benches to fail at
    # runtime with VersionMismatch. Keep GUNGRAUN_RUNNER_VERSION, the workspace Cargo.toml entry,
    # and the install command in `docs/callgrind-benchmarks.md` in sync when bumping.
    if ($IsLinux) {
        cargo install gungraun-runner@$env:GUNGRAUN_RUNNER_VERSION --locked
    }

    # azcopy drives the bulk upload in the cargo-bench-history Azure stress harness
    # (`just bench-history-stress-azure`), which is on-demand only - no CI job needs it, so CI
    # sets skip_azcopy=true. It is not a cargo crate and has no installer, so a helper script
    # fetches the pinned official binary; see scripts/setup/install-azcopy.ps1 (including its rationale
    # for the destination). No-op when azcopy is already present.
    if ("{{skip_azcopy}}" -eq "true") {
        Write-Host "Skipping azcopy install (skip_azcopy=true)."
    } else {
        & "{{justfile_directory()}}/scripts/setup/install-azcopy.ps1"
    }

    # Azurite is the Azure Blob storage emulator that backs the cargo-bench-history
    # Azure backend tests, exercised locally via `just test-azurite`. It is a Node
    # package, so npm is a prerequisite (see DEVELOPMENT.md). We install it on every
    # platform because the emulator and those tests are cross-platform. Its install is
    # best-effort: any failure (npm missing, or the install itself erroring) only warns
    # and continues, so the rest of the toolchain still installs.
    #
    # This is the single source of the pinned Azurite version. CI installs Azurite through this
    # same recipe (the test-azurite job sets install-azurite on the setup-environment action,
    # which clears skip_azurite) and the start-azurite action then only launches it, so there is
    # no second copy of the version to keep in sync.
    if ("{{skip_azurite}}" -eq "true") {
        # CI sets skip_azurite=true on every job except test-azurite: `npm install -g azurite`
        # takes ~3 minutes and only that job needs the emulator, so paying it on every other CI
        # job would be uncached work for no benefit.
        Write-Host "Skipping Azurite install (skip_azurite=true)."
    } elseif (Get-Command npm -ErrorAction SilentlyContinue) {
        # A global npm install can fail for reasons unrelated to the rest of the toolchain
        # - most commonly an npm global prefix pointing at a root-owned directory (e.g. the
        # default /usr/local on a system Node), which yields EACCES without sudo. Since the
        # cargo tools above have already installed, treat such a failure as non-fatal: warn
        # and continue rather than aborting the whole recipe over an optional emulator.
        try {
            npm install -g azurite@3.35.0
        } catch {
            Write-Warning (
                "Failed to install Azurite via 'npm install -g azurite@3.35.0': $_. " +
                "Skipping it - the rest of the toolchain is unaffected and Azurite is only " +
                "needed for 'just test-azurite'. This is usually an npm global-prefix " +
                "permission issue (a root-owned prefix like /usr/local yields EACCES); fix it " +
                "with 'npm config set prefix ~/.npm-global' or use a user-owned Node (nvm), " +
                "then re-run 'just install-tools'.")
        }
    } else {
        Write-Warning "npm not found; skipping Azurite install. Install Node.js/npm (see DEVELOPMENT.md), then re-run 'just install-tools' to enable 'just test-azurite'."
    }

    # Pester 5 powers `just test-scripts`, the PowerShell test suite for the standalone scripts
    # under scripts/. GitHub runners preinstall it, and a fresh local PC may only have the
    # Windows-bundled Pester 3, so install a 5.x into the CurrentUser scope when one is not
    # already available. -SkipPublisherCheck sidesteps the signed Windows-bundled Pester 3.
    if (Get-Module -ListAvailable Pester | Where-Object { $_.Version.Major -ge 5 }) {
        Write-Host "Pester 5+ already available; skipping install."
    } else {
        Install-Module Pester -MinimumVersion 5.0 -Scope CurrentUser -Force -SkipPublisherCheck
    }

    # PSScriptAnalyzer powers `just validate-scripts`, the PowerShell static-analysis gate for the
    # standalone scripts under scripts/ (curated ruleset in PSScriptAnalyzerSettings.psd1 plus the
    # repo-local custom rules in scripts/analyzer/). Unlike Pester it is pinned to an EXACT version
    # (PSSCRIPTANALYZER_VERSION in constants.env): a newer analyzer can add or change rules, which
    # would make which findings gate depend on whatever version a machine happens to have, so the
    # pin keeps `validate-scripts` reproducible across local PCs and CI. Install the pinned version
    # only when a matching one is not already present.
    if (Get-Module -ListAvailable PSScriptAnalyzer | Where-Object { $_.Version -eq [version]$env:PSSCRIPTANALYZER_VERSION }) {
        Write-Host "PSScriptAnalyzer $env:PSSCRIPTANALYZER_VERSION already available; skipping install."
    } else {
        Install-Module PSScriptAnalyzer -RequiredVersion $env:PSSCRIPTANALYZER_VERSION -Scope CurrentUser -Force -SkipPublisherCheck
    }

    # ShellCheck is what actionlint delegates to for linting embedded shell (`run:`) steps. GitHub's
    # Linux runners ship it, so the CI validate-workflows job shellchecks every bash step; installing
    # it here keeps local `just validate-workflows` faithful to CI instead of silently skipping those
    # checks. Like actionlint it is a pinned, checksum-verified standalone binary; see
    # scripts/setup/install-shellcheck.ps1. Installed on every platform with no CI skip. No-op when the
    # pinned ShellCheck is already present.
    & "{{justfile_directory()}}/scripts/setup/install-shellcheck.ps1"

    # actionlint validates the GitHub Actions workflow files (`just validate-workflows`, run both
    # locally and by the CI validate-workflows job). It is not a cargo crate and has no installer, so a
    # helper script fetches the pinned official binary and verifies its checksum; see
    # scripts/setup/install-actionlint.ps1. Unlike azcopy/Azurite this IS needed in CI (the workflow
    # validation job runs it), so it is installed on every platform with no CI skip. No-op when the
    # pinned actionlint is already present.
    & "{{justfile_directory()}}/scripts/setup/install-actionlint.ps1"
