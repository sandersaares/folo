#requires -Version 7

# Release-automation logic for the `Release` GitHub workflow (.github/workflows/release.yml)
# and the local `just prepare-release` / `just check-never-published` recipes.
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

function Get-PublishableBinaryCrate {
    # Derives the crates this workflow releases: publishable to a registry AND owning a `bin`
    # target. In `cargo metadata` the `publish` field is null (any registry), an empty list
    # (never publish), or a non-empty registry list, so "publishable" is null-or-non-empty.
    # Returns {Name, Version, ReleaseTargets} objects sorted by name, where ReleaseTargets is the
    # crate's declared release-target restriction (empty for the usual "all targets" case). Runs the
    # real `cargo metadata` (offline with --no-deps); tests point it at a fixture via -ManifestPath.
    [CmdletBinding()]
    param(
        [string] $ManifestPath
    )

    $cargoArgs = @('metadata', '--no-deps', '--format-version', '1')
    if ($ManifestPath) { $cargoArgs += @('--manifest-path', $ManifestPath) }

    $metadata = & cargo @cargoArgs | ConvertFrom-Json
    $metadata.packages |
        Where-Object { ($null -eq $_.publish) -or ($_.publish.Count -gt 0) } |
        Where-Object { $_.targets | Where-Object { $_.kind -contains 'bin' } } |
        ForEach-Object {
            [pscustomobject]@{
                Name           = $_.name
                Version        = $_.version
                ReleaseTargets = @(Get-DeclaredReleaseTarget -Package $_)
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

    # An existing-but-empty release returns @() (all targets missing), distinct from $null
    # ("no release yet", skip). The guard also keeps member enumeration strict-mode-safe.
    $parsed = ($output | Out-String) | ConvertFrom-Json
    if (-not $parsed.assets) { return , @() }
    , @($parsed.assets.name)
}

function Get-MissingBinaryMatrix {
    # Reconciles desired vs. actual binary assets. For each crate it computes the expected tag
    # `{Name}-v{Version}` and, when that release exists, the per-target archives
    # `{Name}-v{Version}-{triple}.zip`; every expected archive not already uploaded becomes a
    # matrix row {name, version, tag, triple, os}. This is what makes the workflow self-healing:
    # a re-run rebuilds only what is still missing, from the actual published state, with no
    # hand-maintained crate list. A crate that carries a release-target restriction (a `ReleaseTargets`
    # list, as Get-PublishableBinaryCrate projects it) is reconciled against only those targets.
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][object[]] $Crate,
        [object[]] $Target = (Get-ReleaseTarget)
    )

    # Verbose emits the full decision history - every crate, its expected tag, whether the
    # release exists, and the per-target present/missing verdict - so a CI run's log explains
    # exactly how the plan (and its emptiness or non-emptiness) was derived, not just the total.
    Write-Verbose "Reconciling desired vs. uploaded binary archives. Crates: $($Crate.Name -join ', '). Target triples: $(($Target.Triple) -join ', ')."

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
        # the projection is guarded - callers may pass bare {Name, Version} objects.
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
            Write-Verbose "  No GitHub release '$tag' found yet; skipping this crate (nothing to reconcile until its release exists)."
            continue
        }

        $uploaded = if ($assets.Count -gt 0) { $assets -join ', ' } else { '(none)' }
        Write-Verbose "  Release '$tag' found; already-uploaded archives: $uploaded."

        foreach ($releaseTarget in $crateTargets) {
            $archive = "$($crateInfo.Name)-v$($crateInfo.Version)-$($releaseTarget.Triple).zip"
            if ($assets -contains $archive) {
                Write-Verbose "  Target $($releaseTarget.Triple): '$archive' already uploaded - skipping."
                continue
            }
            Write-Verbose "  Target $($releaseTarget.Triple): '$archive' missing - queuing a build on runner '$($releaseTarget.Os)'."
            $rows.Add([pscustomobject]@{
                    name    = $crateInfo.Name
                    version = $crateInfo.Version
                    tag     = $tag
                    triple  = $releaseTarget.Triple
                    os      = $releaseTarget.Os
                })
        }
    }

    $noun = if ($rows.Count -eq 1) { 'archive' } else { 'archives' }
    Write-Verbose "Reconciliation complete: $($rows.Count) missing (crate, target) $noun queued to build."

    , $rows.ToArray()
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
    # Every crate publishable to a registry (unlike Get-PublishableBinaryCrate, not filtered to
    # binaries), as {Name, Version} objects sorted by name. Used by the never-published preflight,
    # which must warn about any brand-new crate, library or binary. Runs the real `cargo metadata`
    # (offline with --no-deps); tests point it at a fixture workspace via -ManifestPath.
    [CmdletBinding()]
    param(
        [string] $ManifestPath
    )

    $cargoArgs = @('metadata', '--no-deps', '--format-version', '1')
    if ($ManifestPath) { $cargoArgs += @('--manifest-path', $ManifestPath) }

    $metadata = & cargo @cargoArgs | ConvertFrom-Json
    $metadata.packages |
        Where-Object { ($null -eq $_.publish) -or ($_.publish.Count -gt 0) } |
        ForEach-Object { [pscustomobject]@{ Name = $_.name; Version = $_.version } } |
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
    # Preflight for `just prepare-release`: warns about publishable crates that crates.io has never
    # seen. Trusted Publishing cannot perform a crate's first-ever publish (the crate must already
    # exist so a trusted publisher can be configured on it), so a brand-new crate's first release
    # must be done by hand. Best-effort and never a gate: a status that cannot be confirmed
    # degrades to a warning and continues, so a transient failure never blocks preparing a release.
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
    Get-PublishableBinaryCrate, `
    Get-PublishableCrate, `
    Get-CrateIndexPath, `
    Get-CratePublishStatus, `
    Test-NeverPublishedCrate, `
    Add-GitReleaseEnableFlag, `
    New-ReleasePlzConfig, `
    Get-BinaryReleaseAsset, `
    Get-MissingBinaryMatrix, `
    ConvertTo-MatrixJson, `
    Invoke-ReleasePublish, `
    Set-GitHubOutput
