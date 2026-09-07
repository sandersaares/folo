[group('release')]
audit:
    cargo audit
    
[group('release')]
[script]
prepare-release: verify-semver-checks && check-never-published
    Set-StrictMode -Version Latest
    $ErrorActionPreference = "Stop"
    $PSNativeCommandUseErrorActionPreference = $true

    Import-Module "{{ justfile_directory() }}/scripts/release/ReleasePlan.psm1" -Force
    Write-Verbose "Bumping versions and changelogs via 'release-plz update'" -Verbose
    Invoke-ReleasePlzUpdate

# Preflight for `just prepare-release`: proves that `cargo semver-checks` can actually run on
# this machine before `release-plz update` starts trusting it. release-plz runs
# cargo-semver-checks to decide whether a bumped version needs to be a *major* bump, but when the
# tool fails to *run* - most commonly because the installed cargo-semver-checks is too old for the
# current toolchain's rustdoc JSON format (e.g. "unsupported rustdoc format v57 ... supported
# formats are v53, v55, v56") - release-plz silently swallows the error and treats it as "no
# breaking changes". That turns a broken tool into an undetected breaking release. This canary
# runs cargo-semver-checks on one small, fast-to-build package, comparing it against its own `HEAD`
# so the two sides are byte-identical and the *only* way the check can fail is the tool itself
# failing to run. A non-zero exit therefore means "cargo-semver-checks is broken", not "a real
# breaking change was found", so we abort the release before anything is changed. Runnable on its
# own to sanity-check a fresh toolchain/tool combination.
[group('release')]
[script]
verify-semver-checks:
    Set-StrictMode -Version Latest
    $ErrorActionPreference = "Stop"
    $PSNativeCommandUseErrorActionPreference = $true

    Import-Module "{{ justfile_directory() }}/scripts/release/ReleasePlan.psm1" -Force
    Invoke-VerifySemverCheck

# Verifies the checked-in Cargo.lock is consistent with the workspace manifests, WITHOUT
# modifying it. `cargo metadata --locked` resolves the dependency graph against the committed
# lockfile and exits non-zero if the lock would need to change - the classic trigger being a
# manifest version bumped without its Cargo.lock entry updated to match. The whole workspace
# shares one lockfile, so a single stale entry breaks every `--locked` build, which means a
# release tagged with an inconsistent lock fails all `build-binaries` jobs at once (they check
# out the tag and build `--locked`). The release `publish` job runs this gate before anything is
# published or tagged, turning that latent, workspace-wide failure into an early, actionable
# abort; run it locally after a manual version edit to catch the problem before pushing. It only
# reads the lockfile - the fix for a stale lock is to regenerate it locally and commit it, never
# to regenerate it inside a workflow.
[group('release')]
[script]
verify-lockfile:
    Set-StrictMode -Version Latest
    $ErrorActionPreference = "Stop"
    $PSNativeCommandUseErrorActionPreference = $true

    Write-Verbose "Verifying that the checked-in Cargo.lock is consistent with the workspace manifests" -Verbose

    try {
        # Full-graph resolve (no --no-deps) so --locked actually validates the lockfile; the JSON
        # on stdout is discarded, only the exit status matters. Any error message from cargo (e.g.
        # "cannot update the lock file ... because --locked was passed") surfaces above.
        cargo metadata --locked --format-version 1 1> $null
    }
    catch {
        Write-Host ""
        Write-Host "ERROR: the checked-in Cargo.lock is out of date with the workspace manifests (see the error above)." -ForegroundColor Red
        Write-Host ""
        Write-Host "A manifest version was almost certainly changed (e.g. a manual version bump) without updating" -ForegroundColor Red
        Write-Host "the matching Cargo.lock entry. Because the whole workspace shares one lockfile, this makes every" -ForegroundColor Red
        Write-Host "'--locked' build fail, including all release binary builds." -ForegroundColor Red
        Write-Host ""
        Write-Host "Fix: run 'cargo update --workspace' (or 'cargo build') locally to sync Cargo.lock, commit the" -ForegroundColor Red
        Write-Host "result, and re-push. Never regenerate the lockfile inside a workflow - workflows must build the" -ForegroundColor Red
        Write-Host "checked-in lockfile verbatim." -ForegroundColor Red
        exit 1
    }

# Warns about publishable crates that crates.io has never seen. Trusted Publishing cannot
# perform a crate's first-ever publish (it can only be configured on a crate that already
# exists), so a brand-new crate's first release must be done by hand (cargo publish), after
# which Trusted Publishing is configured and subsequent releases flow through the workflow.
# Run as a post-step of `prepare-release`, so the warning surfaces while a release is being
# prepared instead of failing the CI publish job later. Best-effort, never a gate: a transient
# crates.io failure degrades to a warning and continues. See docs/release-automation.md.
[group('release')]
[script]
check-never-published:
    Set-StrictMode -Version Latest
    $ErrorActionPreference = "Stop"
    $PSNativeCommandUseErrorActionPreference = $true
    Import-Module ./scripts/release/ReleaseAutomation.psm1 -Force

    Write-Verbose "Checking crates.io for never-before-published crates" -Verbose
    Test-NeverPublishedCrate -Verbose

# CI version-readiness wrappers.
#
# `RELEASE_PLAN_BASE` supplies the release baseline when set (CI passes the release branch, or
# `merge_group.base_sha` on a queue run); an empty value falls through to the tool default. When
# `GITHUB_OUTPUT` is set, a report is written first so the `semver_targets` step output reaches
# `semver-checks` even if the check then fails. Released-content comparison stays in the Rust
# tool; this recipe is only its command-line entry point.
#
# Fails when a publishable package has unreleased changes without an increment, a version group
# disagrees with itself, a requirement does not name the version its target declares or does not
# pin a group sibling exactly, or a package exposing a public dependency stays compatible while
# that dependency releases a breaking change.
[group('release')]
[script]
validate-versions:
    Set-StrictMode -Version Latest
    $ErrorActionPreference = "Stop"
    $PSNativeCommandUseErrorActionPreference = $true

    Import-Module "{{ justfile_directory() }}/scripts/release/ReleasePlan.psm1" -Force
    Invoke-ValidateVersions

# The module owns the valid-empty skip and the repeated `-p` argument construction, so an empty
# `package` value is a successful no-op rather than a workspace-wide comparison.
#
# Runs cargo-semver-checks over the packages in the `package` variable; an empty value checks nothing.
[group('release')]
[script]
semver-checks:
    Set-StrictMode -Version Latest
    $ErrorActionPreference = "Stop"
    $PSNativeCommandUseErrorActionPreference = $true

    Import-Module "{{ justfile_directory() }}/scripts/release/ReleasePlan.psm1" -Force
    Invoke-SemverCheck -Package "{{ package }}"

# The recipes below are the guided increment workflow, in the order the increment-versions skill
# runs them: collect evidence, determine analysis order, create the plan, expand it, confirm the
# expanded targets are published, and apply. Each is a thin entry point; the decisions live in
# `scripts/release/ReleasePlan.psm1` and in `cargo-release-plan`.

# Target selection matches the CI SemVer job exactly, so local evidence and hosted validation
# cannot disagree about which packages have a supported consumer contract. The module owns
# validation, target selection, and exit handling.
#
# Writes the release-plan report, per-package diffs, and cargo-semver-checks evidence under DIR.
[group('release')]
[script]
release-report DIR:
    Set-StrictMode -Version Latest
    $ErrorActionPreference = "Stop"
    $PSNativeCommandUseErrorActionPreference = $true

    Import-Module "{{ justfile_directory() }}/scripts/release/ReleasePlan.psm1" -Force
    Invoke-ReleaseReport -OutDir "{{ DIR }}"

# Analyzing a package after the packages it depends on is what lets a dependency decision inform
# its dependents. Mutually dependent packages share a batch and are analyzed to a fixed point.
# The increment-versions skill redirects the output to a working file and documents the fields.
#
# Prints the dependency-first analysis batches from the release-plan report REPORT as a JSON array.
[group('release')]
[script]
release-analysis-order REPORT:
    Set-StrictMode -Version Latest
    $ErrorActionPreference = "Stop"
    $PSNativeCommandUseErrorActionPreference = $true

    Import-Module "{{ justfile_directory() }}/scripts/release/ReleasePlan.psm1" -Force
    Get-ReleasePlanAnalysisBatchJson -ReportPath "{{ REPORT }}"

# Also realigns any inconsistent version group the decisions leave unnamed. Expansion is
# plan-driven, so a drifted group that no entry reaches would otherwise stay drifted and keep the
# version check red.
#
# Converts the approved change levels in DECISIONS into the proposed plan PLAN, using report REPORT.
[group('release')]
[script]
create-release-plan REPORT DECISIONS PLAN:
    Set-StrictMode -Version Latest
    $ErrorActionPreference = "Stop"
    $PSNativeCommandUseErrorActionPreference = $true

    Import-Module "{{ justfile_directory() }}/scripts/release/ReleasePlan.psm1" -Force
    New-ReleasePlanFile -ReportPath "{{ REPORT }}" -DecisionPath "{{ DECISIONS }}" -PlanPath "{{ PLAN }}"

# The expanded document is what gets presented for approval and what gets applied, so the
# approved set cannot widen during apply. Group expansion stays in cargo-release-plan rather than
# being reimplemented here.
#
# Expands the proposed plan PLAN into the expanded plan EXPANDED, naming every package it reaches.
[group('release')]
[script]
expand-release-plan PLAN EXPANDED:
    Set-StrictMode -Version Latest
    $ErrorActionPreference = "Stop"
    $PSNativeCommandUseErrorActionPreference = $true

    Import-Module "{{ justfile_directory() }}/scripts/release/ReleasePlan.psm1" -Force
    Invoke-ExpandReleasePlan -PlanPath "{{ PLAN }}" -ExpandedPath "{{ EXPANDED }}"

# Reads the expanded plan, so the checked set is exactly the set apply will edit. Applying cannot
# create a crate on crates.io, so a never-published package has to be published by hand first.
#
# Fails unless every package the expanded plan EXPANDED names is already published on crates.io.
[group('release')]
[script]
check-increment-published EXPANDED:
    Set-StrictMode -Version Latest
    $ErrorActionPreference = "Stop"
    $PSNativeCommandUseErrorActionPreference = $true

    Import-Module "{{ justfile_directory() }}/scripts/release/ReleasePlan.psm1" -Force
    Assert-IncrementPackagePublished -ExpandedPath "{{ EXPANDED }}"

# Takes the expanded plan rather than the proposed one, because the publication gate above runs
# over the expanded plan's packages and applying a proposed plan would edit packages that gate
# never saw. The file is an uncommitted working-tree artifact; the version check verifies manifest
# state, not intent. A failure part way through leaves the work tree partly edited.
#
# Applies the expanded plan EXPANDED: package versions, `=`-pin rewrites, and a lockfile refresh.
[group('release')]
[script]
apply-release-plan EXPANDED:
    Set-StrictMode -Version Latest
    $ErrorActionPreference = "Stop"
    $PSNativeCommandUseErrorActionPreference = $true

    Import-Module "{{ justfile_directory() }}/scripts/release/ReleasePlan.psm1" -Force
    Invoke-ApplyReleasePlan -ExpandedPath "{{ EXPANDED }}"
