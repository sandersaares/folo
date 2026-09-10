#requires -Version 7

# Backs Invoke-ScheduledVersion.ps1: independently regenerates cargo-release-plan's
# prepared/resolved output from a repair's recorded pre-versioning checkpoint and compares its
# portable version targets and resulting Cargo bytes with what the worker published. Evidence is
# reproduced from a source checkpoint whose Cargo tree matches the trusted release baseline.
# Worker-selected versions cannot become the generation reference; only this independent
# regeneration can. See ../../.github/workflows/implementation.md#canonical-version-validation and
# ../../docs/scheduled-validation.md#durable-ownership-and-native-calls.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
Import-Module (Join-Path $PSScriptRoot 'ScheduledContracts.psm1')

function Assert-ScheduledVersionArtifact {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][hashtable] $Evidence,
        [Parameter(Mandatory)][string] $BaseSha,
        [Parameter(Mandatory)][hashtable] $Expanded
    )
    foreach ($key in @('pre_version_sha', 'base_sha', 'decisions', 'expanded_plan', 'expanded_plan_digest')) {
        if (-not $Evidence.ContainsKey($key)) { throw "Missing canonical version evidence: $key" }
    }
    Assert-ScheduledSha $Evidence.pre_version_sha
    Assert-ScheduledSha $Evidence.base_sha
    if ($Evidence.base_sha -cne $BaseSha) { throw 'Version evidence is stale for the current release baseline.' }
    if ($Evidence.expanded_plan -isnot [hashtable] -or
        -not $Evidence.expanded_plan.ContainsKey('schema_version') -or
        $Evidence.expanded_plan.schema_version -ne $Expanded.schema_version) {
        throw 'Version evidence uses an unsupported release-plan schema; regenerate it with increment-versions.'
    }
    # Rust's structural export preserves the complete resolved version set without machine-local
    # input/evidence paths. Captured resolution is independently enforced by the Cargo byte check.
    if ($Expanded.ContainsKey('resolved') -or $Evidence.expanded_plan.ContainsKey('resolved')) {
        throw 'Canonical version evidence requires the portable target plan exported by expand from the resolved preview.'
    }
    $expectedDigest = Get-ScheduledDigest $Expanded
    if ($Evidence.expanded_plan_digest -cne $expectedDigest -or
        (Get-ScheduledDigest $Evidence.expanded_plan) -cne $expectedDigest) {
        throw 'Published version plan differs from independently generated resolution; regenerate the complete version evidence.'
    }
}

function Assert-ScheduledVersionFile {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][hashtable] $Expected,
        [Parameter(Mandatory)][hashtable] $Actual
    )
    if ($Expected.Count -ne $Actual.Count) { throw 'Canonical Cargo file set differs from the candidate.' }
    foreach ($path in $Expected.Keys) {
        if (-not $Actual.ContainsKey($path) -or $Actual[$path] -cne $Expected[$path]) {
            throw "Candidate Cargo bytes differ from canonical apply: $path"
        }
    }
}

function Get-ScheduledGitCargoFile {
    [CmdletBinding()]
    [OutputType([hashtable])]
    param([Parameter(Mandatory)][string] $Root, [Parameter(Mandatory)][string] $Revision)
    $result = @{}
    foreach ($line in (& git -C $Root ls-tree -r $Revision)) {
        if ($line -cmatch '^\d+ blob ([0-9a-f]{40})\t((?:.*/)?Cargo\.(?:toml|lock))$') {
            $result[$Matches[2]] = $Matches[1]
        }
    }
    return $result
}

function Assert-ScheduledVersionReference {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string] $Root,
        [Parameter(Mandatory)][string] $BaseSha,
        [Parameter(Mandatory)][string] $PreVersionSha
    )
    $baseline = Get-ScheduledGitCargoFile -Root $Root -Revision $BaseSha
    $reference = Get-ScheduledGitCargoFile -Root $Root -Revision $PreVersionSha
    if ($baseline.Count -ne $reference.Count) {
        throw 'Pre-version checkpoint Cargo file set differs from the trusted release baseline.'
    }
    foreach ($path in $baseline.Keys) {
        if (-not $reference.ContainsKey($path) -or $reference[$path] -cne $baseline[$path]) {
            throw "Pre-version checkpoint Cargo bytes differ from the trusted release baseline: $path"
        }
    }
}

function Assert-ScheduledCanonicalVersion {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string] $Root,
        [Parameter(Mandatory)][string] $HeadSha,
        [Parameter(Mandatory)][string] $BaseSha,
        [Parameter(Mandatory)][hashtable] $Evidence,
        [Parameter(Mandatory)][string] $ReleasePlanExecutable,
        [Parameter(Mandatory)][string] $TrustedControllerRoot,
        [Parameter(Mandatory)][string] $TemporaryRoot,
        [scriptblock] $AssertPublished = {
            param([string] $ExpandedPath)
            Assert-IncrementPackagePublished -ExpandedPath $ExpandedPath
        }
    )
    Assert-ScheduledSha $HeadSha
    Assert-ScheduledSha $BaseSha
    Assert-ScheduledSha $Evidence.pre_version_sha
    if ($Evidence.base_sha -cne $BaseSha) { throw 'Version baseline changed; regenerate the whole plan.' }
    if (-not [IO.Path]::IsPathFullyQualified($ReleasePlanExecutable) -or
        -not (Test-Path -LiteralPath $ReleasePlanExecutable -PathType Leaf)) {
        throw 'Canonical verification requires the trusted controller release-plan executable.'
    }
    & git -C $Root merge-base --is-ancestor $Evidence.pre_version_sha $HeadSha
    # An ancestor alone is not independent evidence: it could already contain arbitrary
    # version movement that report would retain as a sufficient pending release.
    Assert-ScheduledVersionReference -Root $Root -BaseSha $BaseSha -PreVersionSha $Evidence.pre_version_sha
    $postVersionPaths = @(& git -C $Root diff --name-only $Evidence.pre_version_sha $HeadSha)
    foreach ($path in $postVersionPaths) {
        if ($path -cnotmatch '(^|/)Cargo\.(toml|lock)$') {
            throw "Source changed after version planning; regenerate evidence: $path"
        }
    }
    # The reference directory is always newly allocated beneath the caller's dedicated scratch
    # root. Cleanup only removes this owned worktree, never the caller's workspace or root.
    New-Item -ItemType Directory -Path $TemporaryRoot -Force | Out-Null
    $reference = Join-Path ([IO.Path]::GetFullPath($TemporaryRoot)) ([guid]::NewGuid().ToString('N'))
    & git -C $Root worktree add --detach $reference $Evidence.pre_version_sha
    $verificationError = $null
    try {
        $artifactDirectory = Join-Path $reference '.scheduled-version-evidence'
        New-Item -ItemType Directory -Path $artifactDirectory | Out-Null
        $decisionPath = Join-Path $artifactDirectory 'decisions.json'
        $planPath = Join-Path $artifactDirectory 'proposed.json'
        $expandedPath = Join-Path $artifactDirectory 'expanded.json'
        $previewDirectory = Join-Path $artifactDirectory 'preview'
        $resolvedPath = Join-Path $previewDirectory 'plan.json'
        $Evidence.decisions | ConvertTo-Json -Depth 100 | Set-Content -LiteralPath $decisionPath
        Import-Module (Join-Path $TrustedControllerRoot 'scripts/release/ReleasePlan.psm1')
        Push-Location $reference
        try {
            & $ReleasePlanExecutable prepare --base $BaseSha --output $artifactDirectory
            New-ReleasePlanFile -ReportPath (Join-Path $artifactDirectory 'report.json') `
                -DecisionPath $decisionPath -PlanPath $planPath -Confirm:$false
            & $ReleasePlanExecutable preview --prepared (Join-Path $artifactDirectory 'prepared.json') `
                --plan $planPath --output $previewDirectory
            # Export only after resolution reaches its fixed point: an initial group expansion
            # may omit binary lockfile effects. Apply still consumes the full captured artifact.
            & $ReleasePlanExecutable expand --plan $resolvedPath --out $expandedPath
            $expanded = Get-Content -LiteralPath $expandedPath -Raw | ConvertFrom-Json -AsHashtable
            Assert-ScheduledVersionArtifact -Evidence $Evidence -BaseSha $BaseSha -Expanded $expanded
            & $AssertPublished $resolvedPath
            & $ReleasePlanExecutable apply --plan $resolvedPath
            $expected = Get-ScheduledGitCargoFile -Root $reference -Revision HEAD
            $actual = Get-ScheduledGitCargoFile -Root $Root -Revision $HeadSha
            foreach ($path in @($expected.Keys)) {
                # Compare canonical Git bytes, respecting checkout line-ending filters on
                # either OS rather than assuming working-tree bytes equal committed bytes.
                $expected[$path] = (& git hash-object --path=$path (Join-Path $reference $path)).Trim()
            }
            $status = @(& git status --porcelain --untracked-files=no)
            foreach ($line in $status) {
                if ($line.Substring(3) -cnotmatch '(^|/)Cargo\.(toml|lock)$') {
                    throw 'Canonical apply changed an unexpected tracked file.'
                }
            }
            Assert-ScheduledVersionFile -Expected $expected -Actual $actual
        } finally {
            Pop-Location
        }
    } catch {
        $verificationError = $_
        throw
    } finally {
        Remove-ScheduledVersionReference -Root $Root -Reference $reference -VerificationError $verificationError
    }
}

function Remove-ScheduledVersionReference {
    [CmdletBinding()]
    [Diagnostics.CodeAnalysis.SuppressMessageAttribute('PSUseShouldProcessForStateChangingFunctions', '',
        Justification = 'Internal mandatory cleanup after successful worktree creation must not be independently skipped.')]
    param(
        [Parameter(Mandatory)][string] $Root,
        [Parameter(Mandatory)][string] $Reference,
        [AllowNull()][System.Management.Automation.ErrorRecord] $VerificationError
    )
    # A successfully created reference must be removed even after failed verification.
    # Report both failures if cleanup also fails; neither failure may hide the other.
    try {
        & git -C $Root worktree remove --force $Reference
    } catch {
        if ($null -eq $VerificationError) { throw }
        throw [AggregateException]::new(
            "Canonical version verification and cleanup of '$Reference' both failed.",
            [Exception[]] @($VerificationError.Exception, $_.Exception))
    }
}

function Invoke-ScheduledVersionVerification {
    [CmdletBinding()]
    param([Parameter(Mandatory)][string] $PlanPath)
    $plan = Get-Content -LiteralPath $PlanPath -Raw | ConvertFrom-Json -AsHashtable
    if (-not $plan.managed) { throw 'Canonical repair verification requires a managed candidate.' }
    $root = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
    $target = Join-Path $root 'target/scheduled-version'
    & cargo build --manifest-path (Join-Path $root 'Cargo.toml') -p cargo-release-plan --locked --target-dir $target
    $executableName = if ($IsWindows) { 'cargo-release-plan.exe' } else { 'cargo-release-plan' }
    $executable = Join-Path $target "debug/$executableName"
    foreach ($repair in @($plan.repairs | Where-Object managed)) {
        if ($null -eq $repair.version_evidence) { throw 'Managed repair lacks canonical version evidence.' }
        & git -C $root fetch --no-tags origin $repair.head_sha
        Assert-ScheduledCanonicalVersion -Root $root -HeadSha $repair.head_sha -BaseSha $plan.release_base_sha `
            -Evidence $repair.version_evidence -ReleasePlanExecutable $executable `
            -TrustedControllerRoot $root -TemporaryRoot (Join-Path $root '.scheduled-version-reference')
    }
}

Export-ModuleMember -Function Assert-ScheduledCanonicalVersion, Assert-ScheduledVersionArtifact,
Assert-ScheduledVersionFile, Invoke-ScheduledVersionVerification
