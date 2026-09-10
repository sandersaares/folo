#requires -Version 7

# GitHub publication boundary for release.yml, called through just_release.just after registry
# publication succeeds. Rust owns release-equivalence decisions; this module owns Git worktree
# lifetime, GitHub writes, main-advance retries and immutable binary-build source selection.
# Ref: .github/workflows/design.md#publication-and-recovery.

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
$VerbosePreference = 'Continue'

Import-Module (Join-Path $PSScriptRoot 'ReleaseAutomation.psm1') -Force
Import-Module (Join-Path $PSScriptRoot '..' 'build' 'CargoExecutable.psm1') -Force

function Get-ReleaseMainCommit {
    # Fetch immediately before candidate selection; a workflow's event SHA need not still be main.
    [CmdletBinding()]
    param()

    git fetch --no-tags origin main | Out-Host
    return [string] (git rev-parse --verify 'FETCH_HEAD^{commit}')
}

function Test-ReleaseSourceAncestry {
    # A false ancestry result is a documented Git exit, distinct from an inability to inspect
    # history. Neither permits publication from a rewritten or unavailable release line.
    [CmdletBinding()]
    [OutputType([bool])]
    param(
        [Parameter(Mandatory)][string] $Source,
        [Parameter(Mandatory)][string] $Candidate
    )

    $PSNativeCommandUseErrorActionPreference = $false
    $output = @(git merge-base --is-ancestor $Source $Candidate 2>&1)
    $exitCode = $LASTEXITCODE
    if ($exitCode -notin @(0, 1)) {
        throw "Release ancestry lookup failed (exit $exitCode): $(($output | Out-String).Trim())"
    }
    return $exitCode -eq 0
}

function Get-ReleaseTagMap {
    # Read authoritative remote references, including peeled annotated tags. One inventory avoids
    # a network request per already-complete package on ordinary no-change releases.
    [Diagnostics.CodeAnalysis.SuppressMessageAttribute('PSUseLiteralInitializerForHashtable', '',
        Justification = 'Git tag names require ordinal case-sensitive lookup; literal hashtables are case-insensitive.')]
    [CmdletBinding()]
    [OutputType([hashtable])]
    param([string] $Tag)

    $arguments = @('ls-remote', '--tags', 'origin')
    if ($Tag) {
        $arguments += @("refs/tags/$Tag", "refs/tags/$Tag^{}")
    }
    $tags = [hashtable]::new([StringComparer]::Ordinal)
    $peeled = [hashtable]::new([StringComparer]::Ordinal)
    $prefix = 'refs/tags/'
    $suffix = '^{}'
    foreach ($line in @(& git @arguments)) {
        $fields = $line -split "`t", 2
        if ($fields.Count -ne 2 -or -not $fields[1].StartsWith($prefix, [StringComparison]::Ordinal)) {
            throw "Unexpected remote tag record: $line"
        }
        $name = $fields[1].Substring($prefix.Length)
        if ($name.EndsWith($suffix, [StringComparison]::Ordinal)) {
            $peeled[$name.Substring(0, $name.Length - $suffix.Length)] = $fields[0]
        } else {
            $tags[$name] = $fields[0]
        }
    }
    foreach ($name in $peeled.Keys) {
        if (-not $tags.ContainsKey($name)) {
            throw "Remote annotated tag '$name' has no reference."
        }
        $tags[$name] = $peeled[$name]
    }
    return $tags
}

function Get-ReleaseTargetVerifier {
    # Build the verifier from the publication controller, never from the candidate being checked.
    [CmdletBinding()]
    [OutputType([string])]
    param()

    Push-Location (Join-Path $PSScriptRoot '..' '..')
    try {
        $messages = @(cargo build --package release-target-check --locked --message-format=json-render-diagnostics)
    } finally {
        Pop-Location
    }
    return Resolve-CargoExecutable -CargoMessage $messages -TargetName 'release-target-check'
}

function Invoke-ReleaseTargetCheck {
    # Keep release relevance in the existing Rust checker, including inherited values and binary
    # dependency closures. The caller supplies a clean, pinned worktree from fetched main.
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string] $Verifier,
        [Parameter(Mandatory)][string] $Worktree,
        [Parameter(Mandatory)][string] $Commit,
        [Parameter(Mandatory)][string[]] $PackageRequest
    )

    $arguments = @(
        '--manifest-path', (Join-Path $Worktree 'Cargo.toml'),
        '--commit', $Commit, '--release-line', $Commit, '--verbose'
    )
    foreach ($request in $PackageRequest) {
        $arguments += @('--package', $request)
    }
    & $Verifier @arguments | Out-Host
    if ($LASTEXITCODE -ne 0) {
        throw "Release target '$Commit' did not preserve the requested package versions and content."
    }
}

function Invoke-ReleaseWorktree {
    # This invocation owns only its uniquely named temporary worktree. Preserve both diagnostics
    # if candidate verification/publication and cleanup fail, as in the delta worktree boundary.
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string] $Commit,
        [Parameter(Mandatory)][scriptblock] $Action
    )

    $path = Join-Path ([IO.Path]::GetTempPath()) "folo-release-$([guid]::NewGuid().ToString('N'))"
    Write-Verbose "Checking release candidate '$Commit' in temporary worktree '$path'."
    git worktree add --quiet --detach $path $Commit | Out-Host
    $operationError = $null
    try {
        & $Action $path
    } catch {
        $operationError = $_
        throw
    } finally {
        try {
            git worktree remove --force $path | Out-Host
        } catch {
            if ($null -eq $operationError) { throw }
            throw [AggregateException]::new(
                "Release operation and cleanup of '$path' both failed.",
                [Exception[]] @($operationError.Exception, $_.Exception))
        }
    }
}

function Invoke-ReleaseGitHubWrite {
    # The caller must classify an unsuccessful write against refreshed remote state. Capture
    # diagnostics rather than throwing before it can distinguish main movement from a fixed error.
    [CmdletBinding()]
    [OutputType([pscustomobject])]
    param([Parameter(Mandatory)][string[]] $Argument)

    $PSNativeCommandUseErrorActionPreference = $false
    $output = @(& gh @Argument 2>&1)
    return [pscustomobject]@{
        ExitCode = $LASTEXITCODE
        Diagnostic = ($output | Out-String).Trim()
    }
}

function Invoke-ReleaseTagCreation {
    # Never use a moving branch name or force-update a reference. Confirm the postcondition even
    # when a competing writer completed the identical operation before this request.
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string] $Repository,
        [Parameter(Mandatory)][string] $Tag,
        [Parameter(Mandatory)][string] $Commit
    )

    Write-Verbose "Creating missing tag '$Tag' at verified release-equivalent main snapshot '$Commit'."
    $result = Invoke-ReleaseGitHubWrite -Argument @(
        'api', '--method', 'POST', "repos/$Repository/git/refs",
        '-f', "ref=refs/tags/$Tag", '-f', "sha=$Commit"
    )
    if ($result.ExitCode -ne 0) {
        Write-Verbose "Tag '$Tag' creation returned exit $($result.ExitCode): $($result.Diagnostic)"
    }
    $observed = Get-ReleaseTagMap -Tag $Tag
    if ($observed.ContainsKey($Tag)) {
        if ($observed[$Tag] -cne $Commit) {
            throw "Tag '$Tag' now points at '$($observed[$Tag])', not '$Commit'; refusing to move it."
        }
        if ($result.ExitCode -ne 0) {
            Write-Verbose "Tag '$Tag' already has the verified target; the concurrent creation is complete."
        }
        return [pscustomobject]@{ ExitCode = 0; Diagnostic = '' }
    }
    if ($result.ExitCode -eq 0) {
        throw "GitHub accepted tag '$Tag', but its remote reference is missing."
    }
    return $result
}

function Invoke-ReleaseReconciliation {
    # Both fresh publication and repair use this path. Requests stay bound to the successful
    # registry-publication snapshot; a new main version is not a substitute for an old request.
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string] $Source,
        [Parameter(Mandatory)][string] $Repository,
        # Bound repeated main movement without waiting for main to become quiescent.
        [ValidateRange('Positive')][int] $Attempt = 3
    )

    $head = [string] (git rev-parse --verify 'HEAD^{commit}')
    if ($head -cne $Source) {
        throw "Release requests must be read from the publication source '$Source', not '$head'."
    }
    $crates = @(Get-PublishableCrate)
    $binaryNames = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    foreach ($binary in @(Get-PublishableBinaryCrate)) {
        $null = $binaryNames.Add($binary.Name)
    }
    $verifier = $null
    for ($index = 1; $index -le $Attempt; $index++) {
        $tags = Get-ReleaseTagMap
        $missing = @($crates | Where-Object {
                -not $tags.ContainsKey("$($_.Name)-v$($_.Version)")
            })
        if ($missing.Count -eq 0) {
            Write-Verbose 'Every requested package already has an immutable release tag.'
            break
        }

        $candidate = Get-ReleaseMainCommit
        if (-not (Test-ReleaseSourceAncestry -Source $Source -Candidate $candidate)) {
            throw "Fetched main '$candidate' does not descend from publication source '$Source'."
        }
        if ($null -eq $verifier) {
            $verifier = Get-ReleaseTargetVerifier
        }
        $requests = @($missing | ForEach-Object { "$($_.Name)@$($_.Version)" })
        Write-Verbose (
            "Attempt $index/${Attempt}: source '$Source' requests $($requests -join ', '); " +
            "verifying fetched main '$candidate' without changing the requests."
        )
        $result = Invoke-ReleaseWorktree -Commit $candidate -Action {
            param($worktree)
            Invoke-ReleaseTargetCheck -Verifier $verifier -Worktree $worktree `
                -Commit $candidate -PackageRequest $requests
            foreach ($crate in $missing) {
                $result = Invoke-ReleaseTagCreation -Repository $Repository `
                    -Tag "$($crate.Name)-v$($crate.Version)" -Commit $candidate
                if ($result.ExitCode -ne 0) { return $result }
            }
            return [pscustomobject]@{ ExitCode = 0; Diagnostic = '' }
        }
        if ($result.ExitCode -eq 0) { break }

        $latest = Get-ReleaseMainCommit
        if ($latest -ceq $candidate -or $index -eq $Attempt) {
            throw (
                "GitHub tag creation failed at '$candidate' (exit $($result.ExitCode)); " +
                "current main is '$latest', attempt $index/$Attempt`: $($result.Diagnostic)"
            )
        }
        Write-Verbose (
            "GitHub rejected tag creation at '$candidate': $($result.Diagnostic). " +
            "Main advanced to '$latest'; remaining tags require fresh verification."
        )
    }

    # Existing tags are authoritative. --verify-tag prevents release creation from implicitly
    # choosing main or constructing another tag when the intended reference is unavailable.
    foreach ($crate in $crates) {
        if (-not $binaryNames.Contains($crate.Name)) { continue }
        $tag = "$($crate.Name)-v$($crate.Version)"
        if ($null -ne (Get-BinaryReleaseAsset -Tag $tag -Repository $Repository)) {
            Write-Verbose "Binary release '$tag' already exists; preserving it."
            continue
        }
        Write-Verbose "Creating missing binary release '$tag' against its existing immutable tag."
        $result = Invoke-ReleaseGitHubWrite -Argument @(
            'release', 'create', $tag, '--repo', $Repository, '--verify-tag',
            '--title', $tag, '--notes', "Prebuilt binaries for $($crate.Name) $($crate.Version)."
        )
        if ($null -eq (Get-BinaryReleaseAsset -Tag $tag -Repository $Repository)) {
            throw "Creating binary release '$tag' failed (exit $($result.ExitCode)): $($result.Diagnostic)"
        }
    }
}

function Get-ReleaseBinaryMatrix {
    # Carry source identity separately from the release label. Checkout uses the immutable commit;
    # upload still uses the versioned release even when its tag is a later equivalent snapshot.
    [CmdletBinding()]
    param([Parameter(Mandatory)][string] $Repository)

    $crates = @(Get-PublishableBinaryCrate)
    if ($crates.Count -eq 0) {
        Write-Verbose 'The publication source contains no publishable binary packages.'
        return
    }
    $rows = @(Get-MissingBinaryMatrix -Crate $crates -Repository $Repository)
    if ($rows.Count -eq 0) { return }
    $tags = Get-ReleaseTagMap
    foreach ($row in $rows) {
        if (-not $tags.ContainsKey($row.tag)) {
            throw "Binary build for '$($row.tag)' has no remote source tag."
        }
        $row | Add-Member -NotePropertyName source_sha -NotePropertyValue $tags[$row.tag]
        Write-Verbose "Binary release '$($row.tag)' will build immutable commit '$($row.source_sha)'."
        $row
    }
}

function Invoke-ReleaseBinaryPlan {
    # Emit the workflow matrix from the same immutable source inventory used for every row.
    [CmdletBinding()]
    param([Parameter(Mandatory)][string] $Repository)

    $rows = @(Get-ReleaseBinaryMatrix -Repository $Repository)
    Write-Host "Incomplete (crate, target) asset pairs: $($rows.Count)"
    Set-GitHubOutput -Name matrix -Value (ConvertTo-MatrixJson -Row $rows)
    Set-GitHubOutput -Name has_binaries -Value $(if ($rows.Count -gt 0) { 'true' } else { 'false' })
}

Export-ModuleMember -Function Invoke-ReleaseReconciliation, Invoke-ReleaseBinaryPlan
