#requires -Version 7

# Release-automation logic for the `Release` GitHub workflow (.github/workflows/release.yml)
# and the local `just check-never-published` recipe.
#
# The workflow steps and release recipes are thin `just` wrappers (in justfiles/just_automation.just
# and justfiles/just_release.just) that import this module and call its functions, so the
# non-trivial logic lives here where it can be exercised by the Pester suite
# (ReleaseAutomation.Tests.ps1) against fixtures rather than only by pushing to `main`.
#
# The functions run real external tools where that is safe on fixtures (`cargo metadata`, file
# I/O) and isolate the ones that would touch crates.io / GitHub for real (`release-plz`, `gh`)
# behind small seams the tests mock.

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true

# The transient-fault retry (used by Invoke-ReleasePublish) is the shared workspace helper rather
# than a private copy, so every network-facing script retries the same way.
Import-Module (Join-Path $PSScriptRoot '..' 'utility' 'Retry.psm1') -Force

function Get-ReleaseTarget {
    # The single source of truth for the triple -> runner mapping. The workflow's build matrix
    # is derived from this (via Get-MissingBinaryMatrix), so a target is added in exactly one
    # place. Native runners, one per target, no cross-compilation. GitHub offers `-latest` only
    # for x64 Linux/Windows and macOS (macos-latest is arm64); ARM Linux/Windows have no
    # `-latest` alias, so they are pinned by version. Intel macOS is intentionally absent.
    [CmdletBinding()]
    param()

    @(
        [pscustomobject]@{ Triple = 'x86_64-unknown-linux-gnu';  Os = 'ubuntu-latest' }
        [pscustomobject]@{ Triple = 'aarch64-unknown-linux-gnu'; Os = 'ubuntu-24.04-arm' }
        [pscustomobject]@{ Triple = 'x86_64-pc-windows-msvc';     Os = 'windows-latest' }
        [pscustomobject]@{ Triple = 'aarch64-pc-windows-msvc';    Os = 'windows-11-arm' }
        [pscustomobject]@{ Triple = 'aarch64-apple-darwin';       Os = 'macos-latest' }
    )
}

function Get-DeclaredReleaseTarget {
    # The target triples a crate restricts its prebuilt binaries to, read from its manifest's
    # `[package.metadata.folo] release-targets`. Returns an empty array when the crate declares
    # nothing, which means every target in Get-ReleaseTarget - the default, and what a portable
    # crate wants. A crate that only functions on some platforms names that subset so the workflow
    # does not publish archives whose binary could never run. Takes a `cargo metadata` package
    # object; StrictMode makes an absent property throw, so every hop is guarded explicitly.
    # PowerShell unrolls a single-element result, so callers wrap the call in @().
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][object] $Package
    )

    if ($Package.PSObject.Properties.Name -notcontains 'metadata') { return @() }
    if ($null -eq $Package.metadata) { return @() }
    if ($Package.metadata.PSObject.Properties.Name -notcontains 'folo') { return @() }

    $folo = $Package.metadata.folo
    if ($null -eq $folo) { return @() }
    if ($folo.PSObject.Properties.Name -notcontains 'release-targets') { return @() }

    @($folo.'release-targets')
}

function Get-BinaryTarget {
    # Returns the Cargo binary targets declared by a package metadata object. Keeping this
    # extraction in one function lets release planning and validation agree on what a binary
    # package contains.
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][object] $Package
    )

    if ($Package.PSObject.Properties.Name -notcontains 'targets' -or
        $null -eq $Package.targets) {
        return @()
    }
    @($Package.targets | Where-Object { $_.kind -contains 'bin' })
}

function Test-PathCaseInsensitive {
    # Cargo opens manifests through the filesystem while Git pathspecs are case-sensitive by
    # default. Probe the workspace directory instead of inferring its behavior from the operating
    # system; an inconclusive probe keeps the stricter case-sensitive result.
    param(
        [Parameter(Mandatory)][string] $Directory
    )

    try {
        $entryName = @(
            Get-ChildItem -LiteralPath $Directory -Force -ErrorAction Stop |
                ForEach-Object { $_.Name }
        )
    } catch {
        return $false
    }
    $present = [System.Collections.Generic.HashSet[string]]::new(
        [StringComparer]::Ordinal
    )
    foreach ($name in $entryName) {
        [void] $present.Add($name)
    }
    foreach ($name in $entryName) {
        $flippedBuilder = [Text.StringBuilder]::new($name.Length)
        foreach ($character in $name.ToCharArray()) {
            if ([char]::IsUpper($character)) {
                [void] $flippedBuilder.Append([char]::ToLowerInvariant($character))
            } elseif ([char]::IsLower($character)) {
                [void] $flippedBuilder.Append([char]::ToUpperInvariant($character))
            } else {
                [void] $flippedBuilder.Append($character)
            }
        }
        $flipped = $flippedBuilder.ToString()
        if ($flipped -ceq $name -or $present.Contains($flipped)) {
            continue
        }
        return Test-Path -LiteralPath (Join-Path $Directory $flipped)
    }
    return $false
}

function Get-WorkspaceMember {
    # Returns current Cargo workspace members with publication eligibility and manifest identity.
    # Tracking is opt-in because the increment publication gate needs it, while ordinary release
    # discovery retains its Cargo-defined scope and must not gain a Git failure boundary.
    [CmdletBinding()]
    param(
        [string] $ManifestPath,
        [switch] $IncludeTracking
    )

    $cargoArgs = @('metadata', '--no-deps', '--format-version', '1')
    if ($ManifestPath) { $cargoArgs += @('--manifest-path', $ManifestPath) }

    $configuredTargetDirectory =
        [Environment]::GetEnvironmentVariable('CARGO_TARGET_DIR', 'Process')
    try {
        # Cargo rejects an explicitly present empty value. Treat it as the absence it represents
        # for this subprocess without changing the caller's environment permanently.
        if ($null -ne $configuredTargetDirectory -and
            $configuredTargetDirectory.Length -eq 0) {
            Remove-Item Env:CARGO_TARGET_DIR
        }
        $metadata = & cargo @cargoArgs | ConvertFrom-Json
    } finally {
        if ($null -ne $configuredTargetDirectory) {
            [Environment]::SetEnvironmentVariable(
                'CARGO_TARGET_DIR',
                $configuredTargetDirectory,
                'Process'
            )
        }
    }
    $workspaceMemberId = [System.Collections.Generic.HashSet[string]]::new(
        [System.StringComparer]::Ordinal
    )
    foreach ($id in $metadata.workspace_members) {
        [void] $workspaceMemberId.Add([string] $id)
    }

    $workspaceRoot = [IO.Path]::GetFullPath([string] $metadata.workspace_root)
    $repositoryRoot = $null
    $workspacePrefix = $null
    $caseInsensitivePath = $false
    if ($IncludeTracking) {
        $previousNativeErrorPreference = $PSNativeCommandUseErrorActionPreference
        try {
            $PSNativeCommandUseErrorActionPreference = $false
            $gitOutput = @(& git -C $workspaceRoot rev-parse --show-toplevel 2>&1)
            $gitExitCode = $LASTEXITCODE
        } finally {
            $PSNativeCommandUseErrorActionPreference = $previousNativeErrorPreference
        }
        if ($gitExitCode -ne 0) {
            $diagnostic = @(
                $gitOutput | ForEach-Object { $_.ToString() }
            ) -join [Environment]::NewLine
            if ([string]::IsNullOrWhiteSpace($diagnostic)) {
                $diagnostic = '(no diagnostic output)'
            }
            throw (
                "git rev-parse failed while resolving the repository for workspace " +
                "'$workspaceRoot' with exit code $gitExitCode`: $diagnostic"
            )
        }
        $repositoryRootLine = @(
            $gitOutput |
                ForEach-Object { $_.ToString() } |
                Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
        )
        if ($repositoryRootLine.Count -ne 1) {
            throw (
                "git rev-parse returned an invalid repository root for workspace " +
                "'$workspaceRoot'."
            )
        }
        $repositoryRoot = [IO.Path]::GetFullPath($repositoryRootLine[0])

        $previousNativeErrorPreference = $PSNativeCommandUseErrorActionPreference
        try {
            $PSNativeCommandUseErrorActionPreference = $false
            $gitOutput = @(& git -C $workspaceRoot rev-parse --show-prefix 2>&1)
            $gitExitCode = $LASTEXITCODE
        } finally {
            $PSNativeCommandUseErrorActionPreference = $previousNativeErrorPreference
        }
        if ($gitExitCode -ne 0) {
            $diagnostic = @(
                $gitOutput | ForEach-Object { $_.ToString() }
            ) -join [Environment]::NewLine
            if ([string]::IsNullOrWhiteSpace($diagnostic)) {
                $diagnostic = '(no diagnostic output)'
            }
            throw (
                "git rev-parse failed while resolving the workspace prefix for " +
                "'$workspaceRoot' with exit code $gitExitCode`: $diagnostic"
            )
        }
        $workspacePrefixLine = @(
            $gitOutput |
                ForEach-Object { $_.ToString() } |
                Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
        )
        if ($workspacePrefixLine.Count -gt 1) {
            throw (
                "git rev-parse returned an invalid workspace prefix for " +
                "'$workspaceRoot'."
            )
        }
        $workspacePrefix = if ($workspacePrefixLine.Count -eq 0) {
            ''
        } else {
            $workspacePrefixLine[0].TrimEnd('/', '\')
        }
        $caseInsensitivePath = Test-PathCaseInsensitive -Directory $workspaceRoot
    }

    foreach ($package in $metadata.packages | Sort-Object -Property name) {
        if (-not $workspaceMemberId.Contains([string] $package.id)) {
            continue
        }

        $packageManifestPath = [IO.Path]::GetFullPath([string] $package.manifest_path)
        $tracked = $null
        if ($IncludeTracking) {
            # Cargo's workspace root and package manifests share Cargo's path spelling. Rebase
            # their relative relationship through Git's workspace prefix instead of subtracting
            # Git's independently spelled repository root from a Cargo path. This also retains
            # leading parent components for supported sibling members.
            $workspaceRelativeManifestPath =
                [IO.Path]::GetRelativePath($workspaceRoot, $packageManifestPath)
            $gitWorkspacePath = [IO.Path]::GetFullPath(
                [IO.Path]::Combine($repositoryRoot, $workspacePrefix)
            )
            $gitManifestPath = [IO.Path]::GetFullPath(
                [IO.Path]::Combine($gitWorkspacePath, $workspaceRelativeManifestPath)
            )
            $relativeManifestPath =
                [IO.Path]::GetRelativePath($repositoryRoot, $gitManifestPath)
            $outsideRepository =
                [IO.Path]::IsPathRooted($relativeManifestPath) -or
                $relativeManifestPath -eq '..' -or
                $relativeManifestPath.StartsWith(
                    "..$([IO.Path]::DirectorySeparatorChar)",
                    [StringComparison]::Ordinal
                )
            if ($outsideRepository) {
                $tracked = $false
            } else {
                # Git pathspecs are relative to -C and accept slash separators on every
                # supported host. Explicit literal magic prevents manifest directory names from
                # being interpreted as patterns; `icase` follows a case-insensitive checkout.
                $gitPath = $relativeManifestPath.Replace('\', '/')
                $gitPathspec = if ($caseInsensitivePath) {
                    ":(icase,literal)$gitPath"
                } else {
                    ":(literal)$gitPath"
                }
                $previousNativeErrorPreference = $PSNativeCommandUseErrorActionPreference
                try {
                    $PSNativeCommandUseErrorActionPreference = $false
                    $gitOutput = @(
                        & git -C $repositoryRoot ls-files --error-unmatch -- $gitPathspec 2>&1
                    )
                    $gitExitCode = $LASTEXITCODE
                } finally {
                    $PSNativeCommandUseErrorActionPreference = $previousNativeErrorPreference
                }
                switch ($gitExitCode) {
                    0 { $tracked = $true }
                    1 { $tracked = $false }
                    default {
                        $diagnostic = @(
                            $gitOutput | ForEach-Object { $_.ToString() }
                        ) -join [Environment]::NewLine
                        if ([string]::IsNullOrWhiteSpace($diagnostic)) {
                            $diagnostic = '(no diagnostic output)'
                        }
                        throw (
                            "git ls-files failed while checking workspace manifest " +
                            "'$relativeManifestPath' with exit code $gitExitCode`: $diagnostic"
                        )
                    }
                }
            }
        }

        [pscustomobject]@{
            Name         = [string] $package.name
            Version      = [string] $package.version
            ManifestPath = $packageManifestPath
            Publishable  = ($null -eq $package.publish) -or ($package.publish.Count -gt 0)
            Tracked      = $tracked
            Package      = $package
        }
    }
}

function Get-TrackedWorkspaceMember {
    # The current workspace members whose manifests Git tracks. Version-group membership remains
    # cargo-release-plan's responsibility; this projection only secures the publication gate.
    [CmdletBinding()]
    param(
        [string] $ManifestPath
    )

    Get-WorkspaceMember -ManifestPath $ManifestPath -IncludeTracking |
        Where-Object Tracked
}

function Get-PublishableBinaryCrate {
    # Derives the crates this workflow releases: Cargo workspace members publishable to a registry
    # AND owning a `bin` target. In `cargo metadata` the `publish` field is null (any registry), an
    # empty list (never publish), or a non-empty registry list.
    # Returns {Name, Version, Binary, ReleaseTargets} objects sorted by name, where Binary is the
    # package's single binary target and ReleaseTargets is its declared release-target restriction
    # (empty for the usual "all targets" case). A release archive has one binary path, so packages
    # with several binary targets are rejected rather than silently publishing only one. Runs real
    # Cargo metadata; tests point it at a fixture via -ManifestPath.
    [CmdletBinding()]
    param(
        [string] $ManifestPath
    )

    Get-WorkspaceMember -ManifestPath $ManifestPath |
        Where-Object Publishable |
        Where-Object { $_.Package.targets | Where-Object { $_.kind -contains 'bin' } } |
        ForEach-Object {
            $binaryTargets = @(Get-BinaryTarget -Package $_.Package)
            if ($binaryTargets.Count -ne 1) {
                throw (
                    "Publishable binary package '$($_.Name)' declares $($binaryTargets.Count) " +
                    'binary targets; release automation requires exactly one.'
                )
            }
            [pscustomobject]@{
                Name           = $_.Name
                Version        = $_.Version
                Binary         = [string] $binaryTargets[0].name
                ReleaseTargets = @(Get-DeclaredReleaseTarget -Package $_.Package)
            }
        } |
        Sort-Object -Property Name -Unique
}

function Add-GitReleaseEnableFlag {
    # Pure line-based edit: returns a copy of $Line with `git_release_enable = true` set for each
    # crate in $CrateName. The committed release-plz.toml keeps releases off at the workspace
    # level (most crates are libraries); this turns it on only for the binary crates.
    #
    # `name = "<crate>"` is unique and is the first key of its [[package]] block, so inserting
    # right after that line lands the flag inside the block. The match is exact (trimmed) so
    # `cargo-bench-history` does not collide with `cargo-bench-history-stress`. A crate with no
    # existing entry gets a fresh [[package]] block. An existing `git_release_enable` line is
    # forced to `true` (so a per-package `= false` override cannot defeat enabling a binary
    # crate); idempotent when it is already `true`.
    [CmdletBinding()]
    param(
        [string[]] $Line,
        [string[]] $CrateName
    )

    $lines = [System.Collections.Generic.List[string]]::new()
    foreach ($item in $Line) { $lines.Add($item) }

    foreach ($crate in $CrateName) {
        $needle = 'name = "' + $crate + '"'
        $nameIndex = -1
        for ($i = 0; $i -lt $lines.Count; $i++) {
            if ($lines[$i].Trim() -eq $needle) { $nameIndex = $i; break }
        }

        if ($nameIndex -ge 0) {
            $existingIndex = -1
            for ($j = $nameIndex + 1; $j -lt $lines.Count; $j++) {
                $trimmed = $lines[$j].Trim()
                if ($trimmed.StartsWith('[')) { break }
                if ($trimmed -match '^git_release_enable\s*=') { $existingIndex = $j; break }
            }
            if ($existingIndex -ge 0) {
                # Force an existing assignment to true: a per-package `git_release_enable = false`
                # must not defeat enabling releases for a binary crate. Preserve the original
                # indentation of the line being replaced.
                $indent = if ($lines[$existingIndex] -match '^(\s*)') { $Matches[1] } else { '' }
                $lines[$existingIndex] = "${indent}git_release_enable = true"
            } else {
                $lines.Insert($nameIndex + 1, 'git_release_enable = true')
            }
        } else {
            $lines.Add('')
            $lines.Add('[[package]]')
            $lines.Add($needle)
            $lines.Add('git_release_enable = true')
        }
    }

    # Comma keeps a single-line result an array rather than a bare string.
    , $lines.ToArray()
}

function New-ReleasePlzConfig {
    # Reads the committed release-plz.toml at $SourcePath, injects `git_release_enable = true`
    # for each $CrateName, and writes the result to $OutputPath. The output is UTF-8 without a
    # BOM and uses LF line endings with a trailing newline, deterministically on every platform,
    # so the artifact does not vary with the runner OS. The caller writes this outside the
    # working tree so `cargo publish` never sees a dirty repo.
    [CmdletBinding(SupportsShouldProcess)]
    param(
        [Parameter(Mandatory)][string] $SourcePath,
        [Parameter(Mandatory)][string] $OutputPath,
        [string[]] $CrateName
    )

    $sourceLines = [System.IO.File]::ReadAllText($SourcePath) -split "`r?`n"
    $newLines = Add-GitReleaseEnableFlag -Line $sourceLines -CrateName $CrateName
    $content = ($newLines -join "`n").TrimEnd("`n") + "`n"
    if ($PSCmdlet.ShouldProcess($OutputPath, 'write CI release-plz config')) {
        [System.IO.File]::WriteAllText($OutputPath, $content, [System.Text.UTF8Encoding]::new($false))
    }
}

function Get-BinaryReleaseAsset {
    # Returns the names of the assets already attached to the GitHub release for $Tag, or $null
    # if no such release exists yet. Isolates the real `gh release view` call so the tests can
    # mock it.
    #
    # A non-zero `gh` exit is treated as "no release yet" ONLY when it is the specific "release
    # not found" case; any other failure (auth, network, GitHub API error) is rethrown. Swallowing
    # those would let the caller build an empty/partial matrix, so the binary build is skipped and
    # the workflow looks successful while binaries are still missing.
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string] $Tag
    )

    # Disable the native-error preference locally so a non-zero exit does not terminate here
    # before we can classify it; we inspect the exit code and output ourselves. 2>&1 merges
    # stderr (where gh prints "release not found") into the captured output.
    $PSNativeCommandUseErrorActionPreference = $false
    $output = gh release view $Tag --json assets 2>&1
    $exitCode = $LASTEXITCODE

    if ($exitCode -ne 0) {
        $text = ($output | Out-String).Trim()
        if ($text -match 'release not found') { return $null }
        throw "gh release view '$Tag' failed (exit $exitCode): $text"
    }

    # An existing-but-empty release returns @() (all target asset pairs missing), distinct from
    # $null ("no release yet"). The guard also keeps member enumeration strict-mode-safe.
    $parsed = ($output | Out-String) | ConvertFrom-Json
    if (-not $parsed.assets) { return , @() }
    , @($parsed.assets.name)
}

function New-MissingBinaryRelease {
    # Creates the GitHub tag and release that binary assets need when release-plz did not create
    # them. This occurs after a manual crates.io publish and can also occur when crates.io
    # publication succeeded before forge release creation failed. The release workflow invokes
    # this only after its publish job succeeded, so every current manifest version is already
    # published. Target commits come from cargo-release-plan version anchors, which identify the
    # source revision that introduced each published version.
    [CmdletBinding(SupportsShouldProcess)]
    param(
        [Parameter(Mandatory)][object[]] $Crate,
        [Parameter(Mandatory)][hashtable] $TargetCommitByName
    )

    foreach ($crateInfo in $Crate) {
        $tag = "$($crateInfo.Name)-v$($crateInfo.Version)"
        if ($null -ne (Get-BinaryReleaseAsset -Tag $tag)) {
            Write-Verbose "GitHub release '$tag' already exists."
            continue
        }

        $targetCommit = [string] $TargetCommitByName[[string] $crateInfo.Name]
        if ([string]::IsNullOrWhiteSpace($targetCommit)) {
            throw "Creating missing binary release '$tag' requires its version-anchor commit."
        }
        Write-Verbose (
            "GitHub release '$tag' is missing; creating it at version anchor '$targetCommit'."
        )
        if ($PSCmdlet.ShouldProcess($tag, "create GitHub release at $targetCommit")) {
            gh release create $tag `
                --target $targetCommit `
                --title $tag `
                --notes "Prebuilt binaries for $($crateInfo.Name) $($crateInfo.Version)."
        }
    }
}

function Invoke-BinaryReleaseReconciliation {
    # Generates a release-plan report for the current tree, resolves each binary package's
    # version-anchor commit, and creates any missing GitHub releases at those anchors. Report
    # generation and temporary-file cleanup stay here so the just recipe remains a thin entry
    # point and the orchestration can be tested with an injected Cargo boundary.
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][object[]] $Crate,
        [Parameter(Mandatory)][AllowEmptyString()][string] $Base,
        [scriptblock] $Cargo = { param([string[]] $Argument) & cargo @Argument }
    )

    if ([string]::IsNullOrWhiteSpace($Base)) {
        throw 'Binary release reconciliation requires the checked-out commit as its base.'
    }

    $outDir = Join-Path ([System.IO.Path]::GetTempPath()) "binary-release-plan-$(New-Guid)"
    New-Item -ItemType Directory -Path $outDir | Out-Null
    try {
        $argument = @(
            'run', '-p', 'cargo-release-plan', '--locked', '--',
            'report', '--out-dir', $outDir
        )
        $argument += @('--base', $Base)
        & $Cargo $argument
        if ($LASTEXITCODE -ne 0) {
            throw "cargo-release-plan report failed with exit code $LASTEXITCODE."
        }

        Import-Module (Join-Path $PSScriptRoot 'ReleasePlan.psm1') -Force
        $anchors = @(
            Get-ReleasePlanPackageAnchor `
                -ReportPath (Join-Path $outDir 'report.json') `
                -Name $Crate.Name
        )
        $targetCommitByName = @{}
        foreach ($anchor in $anchors) {
            $targetCommitByName[[string] $anchor.Name] = [string] $anchor.Commit
        }
        New-MissingBinaryRelease -Crate $Crate -TargetCommitByName $targetCommitByName
    } finally {
        Remove-Item -LiteralPath $outDir -Recurse -Force -ErrorAction SilentlyContinue
    }
}

function Get-MissingBinaryMatrix {
    # Reconciles desired vs. actual binary assets. For each crate it computes the expected tag
    # `{Name}-v{Version}` and the per-target archive/checksum pair
    # `{Name}-v{Version}-{triple}.zip` / `{Name}-v{Version}-{triple}.sha256`; every incomplete pair
    # becomes a matrix row
    # {name, bin, version, tag, triple, os}. This is what makes the workflow self-healing: a
    # re-run rebuilds only what is still missing, from the actual published state, with no
    # hand-maintained crate list. A crate that carries a release-target restriction (a
    # `ReleaseTargets` list, as Get-PublishableBinaryCrate projects it) is reconciled against only
    # those targets.
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][object[]] $Crate,
        [object[]] $Target = (Get-ReleaseTarget)
    )

    # Verbose emits the full decision history - every crate, its expected tag, whether the
    # release exists, and the per-target present/missing verdict - so a CI run's log explains
    # exactly how the plan (and its emptiness or non-emptiness) was derived, not just the total.
    Write-Verbose "Reconciling desired vs. uploaded binary asset pairs. Crates: $($Crate.Name -join ', '). Target triples: $(($Target.Triple) -join ', ')."

    # The loop variables must not differ from the collection parameters ($Crate, $Target) by
    # case alone: PowerShell variable names are case-insensitive, so `foreach ($target in $Target)`
    # would make `$target` and `$Target` the same variable and leave `$Target` holding only its
    # last element after the loop - so every crate after the first would reconcile against a single
    # leftover target (the last one) instead of the full set. Hence $crateInfo / $releaseTarget.
    $rows = [System.Collections.Generic.List[object]]::new()
    foreach ($crateInfo in $Crate) {
        $tag = "$($crateInfo.Name)-v$($crateInfo.Version)"
        Write-Verbose "Crate '$($crateInfo.Name)' v$($crateInfo.Version): expected release tag '$tag'."

        # A crate may restrict itself to the targets it functions on (Get-DeclaredReleaseTarget);
        # declaring nothing means the whole table. StrictMode makes the absent property throw, so
        # the projection is guarded - callers may pass objects with only the required
        # {Name, Binary, Version} fields.
        $declaredTargets = @()
        if (($crateInfo.PSObject.Properties.Name -contains 'ReleaseTargets') -and ($null -ne $crateInfo.ReleaseTargets)) {
            $declaredTargets = @($crateInfo.ReleaseTargets)
        }

        $crateTargets = $Target
        if ($declaredTargets.Count -gt 0) {
            # A triple the table does not contain would silently build nothing for that target,
            # leaving the crate short of archives with no failure anywhere. Fail loudly instead.
            $unknown = @($declaredTargets | Where-Object { $_ -notin $Target.Triple })
            if ($unknown.Count -gt 0) {
                $noun = if ($unknown.Count -eq 1) { 'release target' } else { 'release targets' }
                throw "Crate '$($crateInfo.Name)' declares $noun '$($unknown -join ", ")' that the release target table does not offer. Either add the target to Get-ReleaseTarget or correct the crate's [package.metadata.folo] release-targets."
            }

            $crateTargets = @($Target | Where-Object { $_.Triple -in $declaredTargets })
            $skipped = @($Target.Triple | Where-Object { $_ -notin $declaredTargets })
            $skippedText = if ($skipped.Count -gt 0) { $skipped -join ', ' } else { '(none)' }
            Write-Verbose "  Crate restricts its release targets to: $($declaredTargets -join ', ') (declared in [package.metadata.folo] release-targets), so these targets are not built for it: $skippedText."
        }

        $assets = Get-BinaryReleaseAsset -Tag $tag
        if ($null -eq $assets) {
            throw (
                "GitHub release '$tag' is missing. Run the missing-release reconciliation " +
                'before planning binary assets.'
            )
        }

        $uploaded = if ($assets.Count -gt 0) { $assets -join ', ' } else { '(none)' }
        Write-Verbose "  Release '$tag' found; already-uploaded assets: $uploaded."

        foreach ($releaseTarget in $crateTargets) {
            $archiveBase =
                "$($crateInfo.Name)-v$($crateInfo.Version)-$($releaseTarget.Triple)"
            $archive = "$archiveBase.zip"
            $checksum = "$archiveBase.sha256"
            $archivePresent = $assets -contains $archive
            $checksumPresent = $assets -contains $checksum
            if ($archivePresent -and $checksumPresent) {
                Write-Verbose (
                    "  Target $($releaseTarget.Triple): '$archive' and '$checksum' already " +
                    'uploaded - skipping.'
                )
                continue
            }

            $missingAssets = @()
            if (-not $archivePresent) { $missingAssets += $archive }
            if (-not $checksumPresent) { $missingAssets += $checksum }
            Write-Verbose (
                "  Target $($releaseTarget.Triple): missing $($missingAssets -join ', ') - " +
                "queuing a build on runner '$($releaseTarget.Os)'."
            )
            if ($crateInfo.PSObject.Properties.Name -notcontains 'Binary' -or
                [string]::IsNullOrWhiteSpace([string] $crateInfo.Binary)) {
                throw "Binary release candidate '$($crateInfo.Name)' has no binary target name."
            }
            $rows.Add([pscustomobject]@{
                    name    = $crateInfo.Name
                    bin     = [string] $crateInfo.Binary
                    version = $crateInfo.Version
                    tag     = $tag
                    triple  = $releaseTarget.Triple
                    os      = $releaseTarget.Os
                })
        }
    }

    $noun = if ($rows.Count -eq 1) { 'asset pair' } else { 'asset pairs' }
    Write-Verbose "Reconciliation complete: $($rows.Count) incomplete (crate, target) $noun queued to build."

    $rows.ToArray()
}

function ConvertTo-MatrixJson {
    # Renders matrix rows as the compact JSON array that `fromJSON` in the workflow consumes.
    # ConvertTo-Json unwraps a single-element array to a bare object, so a one-row result is
    # re-wrapped; an empty result is the literal `[]`.
    [CmdletBinding()]
    param(
        [object[]] $Row
    )

    if (-not $Row -or $Row.Count -eq 0) { return '[]' }

    # The matrix contract is a top-level array of row objects with scalar workflow fields.
    $matrixJsonDepth = 5
    $json = ConvertTo-Json -InputObject @($Row) -Compress -Depth $matrixJsonDepth
    if ($json.TrimStart().StartsWith('[')) { $json } else { "[$json]" }
}

function Invoke-ReleasePublish {
    # Publishes changed crates to crates.io via `release-plz release` using the composed CI
    # config, with bounded retries. release-plz is idempotent (it skips already-published
    # versions), so a retry or a whole re-run safely resumes a partially-published release. NOT
    # for local use: it performs real publishes. The native-error preference is disabled locally
    # so a non-zero exit is handled here (turned into a retryable failure) rather than aborting.
    [Diagnostics.CodeAnalysis.SuppressMessageAttribute('PSReviewUnusedParameter', 'ConfigPath',
        Justification = 'Consumed inside the -Action retry closure (release-plz --config), which the rule does not trace into.')]
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string] $ConfigPath,
        [int] $Attempt = 3,
        [int] $DelaySeconds = 900
    )

    $PSNativeCommandUseErrorActionPreference = $false
    Invoke-WithRetry -Attempt $Attempt -DelaySeconds $DelaySeconds -Action {
        release-plz release --config $ConfigPath
        if ($LASTEXITCODE -ne 0) {
            throw "release-plz release exited with code $LASTEXITCODE"
        }
    }
}

function Get-PublishableCrate {
    # Every Cargo workspace crate publishable to a registry (unlike Get-PublishableBinaryCrate,
    # not filtered to binaries), as {Name, Version} objects sorted by name. Used by the
    # never-published preflight, which must warn about any brand-new crate, library or binary.
    # Runs real Cargo metadata; tests point it at a fixture workspace via -ManifestPath.
    [CmdletBinding()]
    param(
        [string] $ManifestPath
    )

    Get-WorkspaceMember -ManifestPath $ManifestPath |
        Where-Object Publishable |
        ForEach-Object { [pscustomobject]@{ Name = $_.Name; Version = $_.Version } } |
        Sort-Object -Property Name -Unique
}

function Get-CrateIndexPath {
    # crates.io sparse-index path for a crate, keyed by (lowercased) name length. Pure, so the
    # length-branch logic is unit-tested without touching the network. 1 and 2-char names live
    # under `1/` and `2/`; 3-char under `3/<first-letter>/`; everything else under
    # `<first-two>/<next-two>/`.
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string] $Name
    )

    $n = $Name.ToLowerInvariant()
    switch ($n.Length) {
        1 { "1/$n" }
        2 { "2/$n" }
        3 { "3/$($n.Substring(0, 1))/$n" }
        default { "$($n.Substring(0, 2))/$($n.Substring(2, 2))/$n" }
    }
}

function Get-CratePublishStatus {
    # Best-effort crates.io presence check for one crate. Returns 'Published' (HTTP 200),
    # 'NeverPublished' (HTTP 404), or 'Unknown' (a transient rate-limit / 5xx / network error).
    # Isolates the single HTTP call so the preflight loop and its tests stay off the network.
    # -SkipHttpErrorCheck stops Invoke-WebRequest throwing on 4xx/5xx so 404 is classified rather
    # than caught; the try/catch handles genuine network failures.
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string] $Name
    )

    $url = "https://index.crates.io/$(Get-CrateIndexPath -Name $Name)"
    try {
        $response = Invoke-WebRequest -Uri $url -Method Get -SkipHttpErrorCheck
    } catch {
        return 'Unknown'
    }

    switch ([int] $response.StatusCode) {
        200 { 'Published' }
        404 { 'NeverPublished' }
        default { 'Unknown' }
    }
}

function Test-NeverPublishedCrate {
    # Preflight for the `increment-versions` skill: warns about publishable crates that crates.io
    # has never seen. Trusted Publishing cannot perform a crate's first-ever publish (the crate
    # must already exist so a trusted publisher can be configured on it), so a brand-new crate's
    # first release must be done by hand. Best-effort and never a gate: a status that cannot be
    # confirmed degrades to a warning and continues. The skill treats a never-published crate in
    # the increment set as a stop.
    [CmdletBinding()]
    param(
        [string] $ManifestPath
    )

    foreach ($crate in @(Get-PublishableCrate -ManifestPath $ManifestPath)) {
        Write-Verbose "Checking crates.io publish status for '$($crate.Name)'"
        switch (Get-CratePublishStatus -Name $crate.Name) {
            'Published' { }
            'NeverPublished' {
                Write-Warning "$($crate.Name) has never been published. Its first release must be done manually (cargo publish); afterwards configure Trusted Publishing for it on crates.io and re-publish via the GitHub workflow."
            }
            default {
                Write-Warning "Could not confirm crates.io publish status for '$($crate.Name)'; skipping its never-published preflight. Verify manually if it is a brand-new crate."
            }
        }
    }
}

function Set-GitHubOutput {
    # Emits a `name=value` step output for the workflow (and echoes it for the run log). No-ops
    # the file append when GITHUB_OUTPUT is unset, so the recipes are runnable locally.
    # Empty values are opt-in because most workflow outputs, including release-asset outputs, are
    # contracts whose absence must not be hidden behind a syntactically present output line.
    [CmdletBinding(SupportsShouldProcess)]
    param(
        [Parameter(Mandatory)][string] $Name,
        [Parameter(Mandatory)][AllowEmptyString()][string] $Value,
        [switch] $AllowEmptyValue
    )

    if ($Value.Length -eq 0 -and -not $AllowEmptyValue) {
        throw "GitHub output '$Name' must not be empty."
    }

    Write-Host "$Name=$Value"
    if ($env:GITHUB_OUTPUT -and $PSCmdlet.ShouldProcess($env:GITHUB_OUTPUT, "append output '$Name'")) {
        Add-Content -Path $env:GITHUB_OUTPUT -Value "$Name=$Value" -Encoding utf8
    }
}

Export-ModuleMember -Function `
    Get-ReleaseTarget, `
    Get-DeclaredReleaseTarget, `
    Get-BinaryTarget, `
    Get-TrackedWorkspaceMember, `
    Get-PublishableBinaryCrate, `
    Get-PublishableCrate, `
    Get-CrateIndexPath, `
    Get-CratePublishStatus, `
    Test-NeverPublishedCrate, `
    Add-GitReleaseEnableFlag, `
    New-ReleasePlzConfig, `
    Get-BinaryReleaseAsset, `
    New-MissingBinaryRelease, `
    Invoke-BinaryReleaseReconciliation, `
    Get-MissingBinaryMatrix, `
    ConvertTo-MatrixJson, `
    Invoke-ReleasePublish, `
    Set-GitHubOutput
