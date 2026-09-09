[group('release')]
audit:
    cargo audit

# Canary for the `increment-versions` skill and the CI `semver-checks` job: proves that
# `cargo semver-checks` can actually run on this machine before either trusts its output. When the
# tool fails to *run* - most commonly because the installed cargo-semver-checks is too old for the
# current toolchain's rustdoc JSON format (e.g. "unsupported rustdoc format v57 ... supported
# formats are v53, v55, v56") - a broken tool must never be read as "no breaking changes". This
# canary runs cargo-semver-checks on one small, fast-to-build package against its own `HEAD` as
# the baseline. `--baseline-rev` fixes only the baseline: the candidate is still built from the
# work tree, so the two sides are identical exactly when the work tree has not touched that
# package - which is the normal case, since it is not a package a release change would edit. A
# non-zero exit therefore means "cargo-semver-checks is broken" unless the canary package itself
# was edited, which the diagnostic calls out. Runnable on its own to sanity-check a fresh
# toolchain/tool combination.
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
# Run as a preflight by the `increment-versions` skill, so the warning surfaces while the
# increment is being prepared instead of failing the CI publish job later. Best-effort, never a
# gate: a transient
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
# Fails when a publishable package has unreleased changes without an increment, a derived version
# group disagrees with itself, an in-scope exact requirement is malformed or stale, legacy
# version-group metadata remains, or a package exposing a public dependency stays compatible
# while that dependency releases a breaking change.
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
# runs them: prepare resolution and evidence, determine analysis order, create the plan, preview
# its resolved effects, confirm the publishable targets are published, and apply the captured
# state. Each is a thin entry point; the decisions
# live in `scripts/release/ReleasePlan.psm1` and in `cargo-release-plan`.

# Resolves the intended offline workspace lockfile refresh before collecting evidence. This is
# explicit preparation, not part of read-only release-report or validate-versions.
#
# Prepares the release report and semantic evidence under DIR.
[group('release')]
[script]
release-prepare DIR:
    Set-StrictMode -Version Latest
    $ErrorActionPreference = "Stop"
    $PSNativeCommandUseErrorActionPreference = $true

    Import-Module "{{ justfile_directory() }}/scripts/release/ReleasePlan.psm1" -Force
    Invoke-PrepareReleasePlan -OutDir "{{ DIR }}"

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

# Also realigns any version group whose declarations differ and which the decisions leave unnamed,
# including alignment-only helpers and groups covered by a consistency exemption. Expansion is
# plan-driven, so a drifted group that no entry reaches would otherwise stay drifted.
#
# Converts the decided change levels in DECISIONS into the proposed plan PLAN, using report REPORT.
[group('release')]
[script]
create-release-plan REPORT DECISIONS PLAN:
    Set-StrictMode -Version Latest
    $ErrorActionPreference = "Stop"
    $PSNativeCommandUseErrorActionPreference = $true

    Import-Module "{{ justfile_directory() }}/scripts/release/ReleasePlan.psm1" -Force
    New-ReleasePlanFile -ReportPath "{{ REPORT }}" -DecisionPath "{{ DECISIONS }}" -PlanPath "{{ PLAN }}"

# Structural expansion is available for inspecting version groups. It does not resolve
# dependencies; use preview-release-plan to create the complete resolved artifact.
#
# Expands the proposed plan PLAN into EXPANDED, naming every publishable or alignment-only version
# target it reaches.
[group('release')]
[script]
expand-release-plan PLAN EXPANDED:
    Set-StrictMode -Version Latest
    $ErrorActionPreference = "Stop"
    $PSNativeCommandUseErrorActionPreference = $true

    Import-Module "{{ justfile_directory() }}/scripts/release/ReleasePlan.psm1" -Force
    Invoke-ExpandReleasePlan -PlanPath "{{ PLAN }}" -ExpandedPath "{{ EXPANDED }}"

# Resolves prospective version and requirement edits against PREPARED until the release set
# settles. DIR receives the complete resolved plan.json and its prospective report and diffs.
[group('release')]
[script]
preview-release-plan PREPARED PLAN DIR:
    Set-StrictMode -Version Latest
    $ErrorActionPreference = "Stop"
    $PSNativeCommandUseErrorActionPreference = $true

    Import-Module "{{ justfile_directory() }}/scripts/release/ReleasePlan.psm1" -Force
    Invoke-PreviewReleasePlan -PreparedPath "{{ PREPARED }}" -PlanPath "{{ PLAN }}" -OutDir "{{ DIR }}"

# Reads the expanded plan and resolves every target against current Git-tracked workspace
# membership. Applying cannot create a crate on crates.io, so a never-published publishable
# package has to be published by hand first; alignment-only helpers do not require a registry
# lookup.
#
# Fails unless every publishable target in EXPANDED is already published on crates.io.
[group('release')]
[script]
check-increment-published EXPANDED:
    Set-StrictMode -Version Latest
    $ErrorActionPreference = "Stop"
    $PSNativeCommandUseErrorActionPreference = $true

    Import-Module "{{ justfile_directory() }}/scripts/release/ReleasePlan.psm1" -Force
    Assert-IncrementPackagePublished -ExpandedPath "{{ EXPANDED }}"

# Takes the expanded plan rather than the proposed one, because it is the complete resolved set of
# version targets and the publication gate above validates its publishable subset. The file is an
# uncommitted working-tree artifact; the version check verifies manifest state, not intent. A
# failure part way through leaves the work tree partly edited.
#
# Applies the resolved plan EXPANDED: captured versions, requirement rewrites, and resolved lockfile.
[group('release')]
[script]
apply-release-plan EXPANDED:
    Set-StrictMode -Version Latest
    $ErrorActionPreference = "Stop"
    $PSNativeCommandUseErrorActionPreference = $true

    Import-Module "{{ justfile_directory() }}/scripts/release/ReleasePlan.psm1" -Force
    Invoke-ApplyReleasePlan -ExpandedPath "{{ EXPANDED }}"
