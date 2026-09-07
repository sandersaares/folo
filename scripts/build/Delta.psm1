#requires -Version 7

# cargo-delta orchestration shared by the local `just delta*` recipes and the CI `delta` job.
#
# cargo-delta answers which workspace packages are affected by this branch's changes when
# compared with a caller-selected baseline revision. The validation matrix can then scope itself
# to those packages.
#
# Callers supply the appropriate baseline revision and freshness policy. This module owns
# cargo-delta orchestration: current and baseline analysis, removed-package filtering, JSON report
# parsing, and CI output shaping.

Set-StrictMode -Version Latest

function Read-DeltaAffectedPackage {
    # Parses the JSON emitted by `cargo delta run` and returns its list of affected package names
    # as a string array (empty when nothing is affected). Tolerates a missing or null `Affected`
    # field so a well-formed "nothing changed" report is not mistaken for an error - which also
    # keeps it safe under Set-StrictMode, where blindly reading an absent property would throw.
    [CmdletBinding()]
    [OutputType([string[]])]
    param(
        [Parameter(Mandatory)][AllowEmptyString()][string] $DeltaJson
    )

    if ([string]::IsNullOrWhiteSpace($DeltaJson)) { return @() }

    $delta = $DeltaJson | ConvertFrom-Json
    if ($null -eq $delta) { return @() }
    if (-not ($delta.PSObject.Properties.Name -contains 'Affected')) { return @() }
    if ($null -eq $delta.Affected) { return @() }
    return @($delta.Affected)
}

function Select-ExistingPackage {
    # Filters affected package names down to those that still exist in the current workspace,
    # preserving order. cargo-delta compares the branch against the baseline revision, so a package
    # deleted or renamed away on this branch is reported as affected (its files changed - they were
    # removed) yet cannot be validated here: a scoped `cargo <cmd> -p <name>` would fail with
    # "package ID specification `<name>` did not match any packages". Dropping such a package is
    # correct - there is nothing left on this branch to validate - while every package that still
    # exists (including the rename's replacement and anything depending on it) is retained. Pure,
    # so the membership logic is test-covered independently of the cargo/git orchestration.
    [CmdletBinding()]
    [OutputType([string[]])]
    param(
        [Parameter(Mandatory)][AllowEmptyCollection()][string[]] $Affected,
        [Parameter(Mandatory)][AllowEmptyCollection()][string[]] $WorkspacePackage
    )

    $existing = [System.Collections.Generic.HashSet[string]]::new(
        [string[]] $WorkspacePackage,
        [System.StringComparer]::Ordinal)
    return @($Affected | Where-Object { $existing.Contains($_) })
}

function Get-DeltaOutput {
    # Shapes an affected-package list into the three step outputs the CI `delta` job publishes:
    # `packages` (space-separated, the form `just package="..."` expects), `packages_json` (a JSON
    # array the matrix `contains(fromJson(...))` checks consume), and `skip_all` ('true' when
    # nothing is affected, so dependent jobs can short-circuit). Pure so the exact JSON shaping is
    # test-covered independently of the cargo/git orchestration.
    [CmdletBinding()]
    [OutputType([pscustomobject])]
    param(
        [Parameter(Mandatory)][AllowEmptyCollection()][string[]] $Affected
    )

    $packagesJson = if ($Affected.Count -eq 0) {
        '[]'
    } else {
        '[' + (($Affected | ForEach-Object { ConvertTo-Json $_ -Compress }) -join ',') + ']'
    }

    return [pscustomobject]@{
        Packages     = $Affected -join ' '
        PackagesJson = $packagesJson
        SkipAll      = if ($Affected.Count -eq 0) { 'true' } else { 'false' }
    }
}

function Get-DeltaWorkflowOutput {
    # Shapes the Validation `delta` job output lines while keeping workflow-only branching under
    # Pester coverage. Push-to-main runs must keep the full workspace as the validation backstop;
    # pull requests and merge-queue runs use cargo-delta with the checkout's already-complete
    # history.
    [CmdletBinding()]
    [OutputType([string])]
    param(
        [Parameter(Mandatory)][AllowEmptyString()][string] $EventName,
        [AllowEmptyString()][string] $BaselineRevision = '',
        [scriptblock] $Analyze = {
            param([hashtable] $Argument)
            Invoke-CargoDelta @Argument
        }
    )

    if ($EventName -eq 'push') {
        Write-Host 'Push to main detected, running full workspace validation.'
        return @(
            'packages='
            'packages_json=[]'
            'skip_all=false'
        )
    }

    $deltaArgs = @{ SkipFetch = $true }
    if (-not [string]::IsNullOrWhiteSpace($BaselineRevision)) {
        $deltaArgs['BaselineRevision'] = $BaselineRevision
    }

    $affected = @(& $Analyze $deltaArgs)
    $output = Get-DeltaOutput -Affected $affected

    if ($affected.Count -eq 0) {
        Write-Host 'No packages affected by changes.'
    } else {
        Write-Host "Affected packages: $($output.Packages)"
    }

    return @(
        "packages=$($output.Packages)"
        "packages_json=$($output.PackagesJson)"
        "skip_all=$($output.SkipAll)"
    )
}

function Get-WorkspacePackage {
    # Returns the current workspace's member package names as a string array, via `cargo metadata`.
    # Used to drop packages that cargo-delta reports as affected but that no longer exist on this
    # branch (see Select-ExistingPackage). `--no-deps` keeps it to workspace members and avoids
    # resolving - or touching - the dependency graph and lockfile. `--locked` guarantees the call is
    # read-only, failing loudly rather than regenerating `Cargo.lock` as a surprise side effect.
    [CmdletBinding()]
    [OutputType([string[]])]
    param()

    $metadataJson = cargo metadata --no-deps --format-version 1 --locked | Out-String
    $metadata = $metadataJson | ConvertFrom-Json
    return @($metadata.packages | ForEach-Object { $_.name })
}

function Invoke-CargoDelta {
    # Runs the full cargo-delta pipeline and returns the affected package names as a string array.
    # Analyzes the current checkout, then analyzes $BaselineRevision in a throwaway git worktree
    # so the branch is never switched, runs the comparison, and parses the result. Unless
    # -SkipFetch is given, origin/main is refreshed first (unshallowing a shallow clone) so the
    # default baseline revision is current; CI passes -SkipFetch because it already checks out
    # full history. All intermediate artifacts go in a temp directory that is always cleaned up.
    [CmdletBinding()]
    [OutputType([string[]])]
    param(
        [string] $ConfigPath = (Resolve-Path 'delta.toml').Path,
        [switch] $SkipFetch,
        # Revision whose tree anchors the Git comparison. Local runs and pull requests use
        # origin/main; merge-queue runs pass merge_group.base_sha so scoping matches the queued
        # candidate's base.
        [string] $BaselineRevision = 'origin/main'
    )

    $PSNativeCommandUseErrorActionPreference = $true

    if (-not $SkipFetch) {
        # We need the full history of both branches to compare them. A shallow clone (e.g. a
        # default CI checkout) must be unshallowed first; a full clone just fetches origin/main.
        $isShallow = git rev-parse --is-shallow-repository
        if ($isShallow -eq 'true') {
            git fetch --unshallow origin main | Out-Null
        } else {
            git fetch origin main | Out-Null
        }
    }

    # $BaselineRevision can arrive from a workflow event payload, so an unfetched or misspelled
    # revision is a realistic input. Resolving it before any analysis turns that into a message
    # naming the revision, instead of several minutes of work followed by a git worktree error
    # that reads like a repository fault. The lookup is expected to fail for a bad revision, so
    # its own error is caught rather than left to $PSNativeCommandUseErrorActionPreference.
    $resolvedBaselineRevision = $null
    $revParseArgs = @('rev-parse', '--verify', '--quiet', "$BaselineRevision^{commit}")
    try {
        $resolvedBaselineRevision = git @revParseArgs 2>$null
    } catch {
        $resolvedBaselineRevision = $null
    }
    if ([string]::IsNullOrWhiteSpace($resolvedBaselineRevision)) {
        $message = "Delta baseline revision '{0}' does not resolve to a commit in this repository."
        throw ($message -f $BaselineRevision)
    }

    $tempName = "cargo-delta-$([guid]::NewGuid().ToString('n'))"
    $tempDir = Join-Path ([System.IO.Path]::GetTempPath()) $tempName
    New-Item -ItemType Directory -Path $tempDir -Force | Out-Null
    try {
        $baselineAnalysisPath = Join-Path $tempDir 'baseline.json'
        $currentAnalysisPath = Join-Path $tempDir 'current.json'

        # Analyze the current branch first (we are already on it).
        Write-Host 'Analyzing current branch...'
        cargo delta -c $ConfigPath analyze |
            Set-Content -Path $currentAnalysisPath -Encoding utf8

        # Use a git worktree to analyze the baseline revision without switching branches.
        $worktreeDir = Join-Path $tempDir 'main-worktree'
        git worktree add --quiet $worktreeDir $resolvedBaselineRevision | Out-Null
        try {
            Write-Host "Analyzing baseline revision ($BaselineRevision)..."
            Push-Location $worktreeDir
            try {
                cargo delta -c $ConfigPath analyze |
                    Set-Content -Path $baselineAnalysisPath -Encoding utf8
            } finally {
                Pop-Location
            }
        } finally {
            git worktree remove $worktreeDir --force | Out-Null
        }

        Write-Host 'Computing delta...'
        # Capture stdout directly: when nothing changed, cargo-delta writes its "quitting" notice
        # to stderr and emits no stdout, which the parser treats as "nothing affected".
        $runArgs = @(
            'delta'
            '-c'
            $ConfigPath
            'run'
            '--baseline'
            $baselineAnalysisPath
            '--current'
            $currentAnalysisPath
        )
        $deltaJson = cargo @runArgs | Out-String

        # Re-wrap in @(): Read-DeltaAffectedPackage returns an empty array for a "nothing affected"
        # report, but PowerShell collapses a bare empty-array return to $null on assignment, and
        # Select-ExistingPackage's -Affected is Mandatory (rejects $null). Without this a PR that
        # touches no crate - docs, workflows or scripts only - would fail the delta job outright.
        $affected = @(Read-DeltaAffectedPackage -DeltaJson $deltaJson)
        return Select-ExistingPackage -Affected $affected -WorkspacePackage (Get-WorkspacePackage)
    } finally {
        Remove-Item -Path $tempDir -Recurse -Force -ErrorAction SilentlyContinue
    }
}

Export-ModuleMember -Function @(
    'Read-DeltaAffectedPackage'
    'Select-ExistingPackage'
    'Get-WorkspacePackage'
    'Get-DeltaOutput'
    'Get-DeltaWorkflowOutput'
    'Invoke-CargoDelta'
)
