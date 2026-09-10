#requires -Version 7

<#
.SYNOPSIS
    Installs a pinned actionlint CLI used to lint the GitHub Actions workflows.

.DESCRIPTION
    actionlint statically checks the workflow files under `.github/workflows` (run via
    `just validate-workflows`, both locally and in the `validate-workflows` CI job). It is not a
    cargo crate, and its author ships it as a standalone, portable binary with no installer,
    so this script downloads the official static build for the current OS/architecture and
    drops it on PATH.

    The version is pinned ($script:ActionlintVersion) and the download comes from the
    matching GitHub release, so `just install-tools` is reproducible. The published SHA-256
    of each archive is baked in below and verified after download, so a tampered or corrupted
    archive is rejected. Bump the version and the digests together when upgrading; the digests
    are in the release's `actionlint_<version>_checksums.txt`.

    The default destination is the cargo bin directory. That is a deliberate, if pragmatic,
    choice: actionlint has no standard home to prefer instead, and the cargo bin directory is
    the one location this repository's developer environment guarantees is already on PATH -
    every other tool `just install-tools` fetches lands there too.

    Idempotent: if the pinned actionlint already resolves on PATH the script does nothing
    (pass -Force to reinstall anyway).

.PARAMETER Destination
    Directory to install the actionlint binary into. Defaults to the cargo bin directory
    (`~/.cargo/bin`), which is already on PATH in this dev environment.

.PARAMETER Force
    Reinstall even if the pinned actionlint is already present on PATH.

.EXAMPLE
    ./install-actionlint.ps1
#>
[CmdletBinding()]
param(
    [string] $Destination = (Join-Path $HOME '.cargo/bin'),

    [switch] $Force
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
$VerbosePreference = 'Continue'

# The archive download can hit a transient network/disk fault; Invoke-WithRetry re-fetches (and
# re-verifies the checksum) rather than failing `just install-tools` on a single blip.
Import-Module (Join-Path $PSScriptRoot '..' 'utility' 'Retry.psm1') -Force

# The pinned actionlint release. Keep $ActionlintVersion and the per-platform digests in
# $ActionlintBuilds in sync - both come from one GitHub release of rhysd/actionlint.
$script:ActionlintVersion = '1.7.12'

# One entry per supported OS/architecture: the release asset name, the archive format it
# ships as, the binary inside it, and the asset's published lowercase SHA-256.
$script:ActionlintBuilds = @{
    'windows-amd64' = @{ Asset = "actionlint_$($script:ActionlintVersion)_windows_amd64.zip"; Kind = 'zip'; Binary = 'actionlint.exe'; Sha256 = '6e7241b51e6817ea6a047693d8e6fed13b31819c9a0dd6c5a726e1592d22f6e9' }
    'windows-arm64' = @{ Asset = "actionlint_$($script:ActionlintVersion)_windows_arm64.zip"; Kind = 'zip'; Binary = 'actionlint.exe'; Sha256 = 'cadcf7ea4efe3a68728893813643cebe1185e5b1d4be5b96245f65c9a4d5ea41' }
    'linux-amd64'   = @{ Asset = "actionlint_$($script:ActionlintVersion)_linux_amd64.tar.gz"; Kind = 'tar.gz'; Binary = 'actionlint'; Sha256 = '8aca8db96f1b94770f1b0d72b6dddcb1ebb8123cb3712530b08cc387b349a3d8' }
    'linux-arm64'   = @{ Asset = "actionlint_$($script:ActionlintVersion)_linux_arm64.tar.gz"; Kind = 'tar.gz'; Binary = 'actionlint'; Sha256 = '325e971b6ba9bfa504672e29be93c24981eeb1c07576d730e9f7c8805afff0c6' }
    'mac-amd64'     = @{ Asset = "actionlint_$($script:ActionlintVersion)_darwin_amd64.tar.gz"; Kind = 'tar.gz'; Binary = 'actionlint'; Sha256 = '5b44c3bc2255115c9b69e30efc0fecdf498fdb63c5d58e17084fd5f16324c644' }
    'mac-arm64'     = @{ Asset = "actionlint_$($script:ActionlintVersion)_darwin_arm64.tar.gz"; Kind = 'tar.gz'; Binary = 'actionlint'; Sha256 = 'aba9ced2dee8d27fecca3dc7feb1a7f9a52caefa1eb46f3271ea66b6e0e6953f' }
}

function Get-PlatformKey {
    # Identifies the current OS/architecture as an `$ActionlintBuilds` key. `#requires
    # -Version 7` guarantees the $Is* automatic variables exist, so the final OS branch is
    # simply "not macOS and not Linux", i.e. Windows.
    Write-Verbose 'Determining the operating system and processor architecture.'
    if ($IsMacOS) { $os = 'mac' } elseif ($IsLinux) { $os = 'linux' } else { $os = 'windows' }

    $processArch = [System.Runtime.InteropServices.RuntimeInformation]::ProcessArchitecture
    $arch = switch ($processArch) {
        ([System.Runtime.InteropServices.Architecture]::X64) { 'amd64' }
        ([System.Runtime.InteropServices.Architecture]::Arm64) { 'arm64' }
        default { throw "actionlint install does not support processor architecture '$processArch'." }
    }

    $key = "$os-$arch"
    Write-Verbose "Resolved platform key '$key'."
    return $key
}

function Expand-ActionlintArchive {
    param(
        [Parameter(Mandatory)] [string] $ArchivePath,
        [Parameter(Mandatory)] [string] $ArchiveKind,
        [Parameter(Mandatory)] [string] $DestinationDir
    )
    Write-Verbose "Extracting '$ArchivePath' ($ArchiveKind) into '$DestinationDir'."
    if ($ArchiveKind -eq 'tar.gz') {
        tar -xf $ArchivePath -C $DestinationDir
    } else {
        Expand-Archive -Path $ArchivePath -DestinationPath $DestinationDir -Force
    }
}

function Get-InstalledActionlintVersion {
    # Returns the version of the actionlint already on PATH, or $null if none is found.
    # `actionlint -version` prints e.g. "1.7.12" (or "v1.7.12" for source builds) on line one.
    if ($null -eq (Get-Command actionlint -ErrorAction SilentlyContinue)) {
        return $null
    }
    $output = (& actionlint -version 2>$null | Select-Object -First 1)
    if ($output -match 'v?([0-9][0-9.]*)') {
        return $Matches[1]
    }
    return $null
}

if (-not $Force) {
    # Only skip when the actionlint already on PATH is exactly the pinned version. A different
    # (e.g. older, globally-installed) actionlint would defeat the point of pinning, so install
    # the pinned build over it rather than honouring it.
    $installed = Get-InstalledActionlintVersion
    if ($installed -eq $script:ActionlintVersion) {
        Write-Host "actionlint $installed already installed; skipping."
        return
    }
    if ($null -ne $installed) {
        Write-Verbose "Found actionlint $installed on PATH, but the pinned version is $($script:ActionlintVersion); installing the pinned build."
    }
}

$platformKey = Get-PlatformKey
$build = $script:ActionlintBuilds[$platformKey]
if ($null -eq $build) {
    throw "No pinned actionlint build is configured for platform '$platformKey'."
}

$url = "https://github.com/rhysd/actionlint/releases/download/v$($script:ActionlintVersion)/$($build.Asset)"
Write-Host "Installing actionlint $($script:ActionlintVersion) (used by 'just validate-workflows')..."
Write-Verbose "Platform '$platformKey' maps to asset '$($build.Asset)'; downloading from '$url'."

Write-Verbose "Ensuring the destination directory '$Destination' exists."
New-Item -ItemType Directory -Force -Path $Destination | Out-Null

# Stage the download in a throwaway temp directory so a failed extract leaves nothing behind;
# the finally block removes it regardless of outcome.
$work = New-Item -ItemType Directory -Force -Path (Join-Path ([System.IO.Path]::GetTempPath()) ("actionlint-" + [guid]::NewGuid()))
Write-Verbose "Staging the download in temp directory '$($work.FullName)'."
try {
    $archive = Join-Path $work $build.Asset
    Write-Verbose "Downloading actionlint archive to '$archive'."
    # Retry the download and its checksum together: a truncated or corrupted fetch fails
    # verification and is re-fetched, so only a genuinely wrong pinned digest exhausts the attempts.
    Invoke-WithRetry -Attempt 4 -DelaySeconds 3 -BackoffMultiplier 2 -MaxDelaySeconds 30 -Action {
        Invoke-WebRequest -Uri $url -OutFile $archive

        Write-Verbose "Verifying the archive SHA-256 against the pinned digest."
        # Get-FileHash returns uppercase hex; normalize both sides to lowercase so the comparison
        # cannot fail on case alone.
        $actual = (Get-FileHash -Path $archive -Algorithm SHA256).Hash.ToLowerInvariant()
        if ($actual -ne $build.Sha256) {
            throw "actionlint archive SHA-256 mismatch for '$($build.Asset)': expected $($build.Sha256), got $actual."
        }
    }

    Expand-ActionlintArchive -ArchivePath $archive -ArchiveKind $build.Kind -DestinationDir $work

    Write-Verbose "Locating '$($build.Binary)' inside the extracted archive."
    $bin = Get-ChildItem -Path $work -Recurse -Filter $build.Binary | Select-Object -First 1
    if ($null -eq $bin) {
        throw "actionlint binary ('$($build.Binary)') was not found inside the downloaded archive."
    }

    Write-Verbose "Copying '$($bin.FullName)' to '$Destination'."
    Copy-Item -Path $bin.FullName -Destination $Destination -Force

    if (-not $IsWindows) {
        # Some archive extractors drop the stored Unix executable bit; restore it on every Unix
        # host so the installed binary actually runs and resolves on PATH.
        $installed = Join-Path $Destination $build.Binary
        Write-Verbose "Marking '$installed' executable."
        chmod +x $installed
    }

    Write-Host "Installed actionlint $($script:ActionlintVersion) to $Destination."
} finally {
    Write-Verbose "Cleaning up temp directory '$($work.FullName)'."
    Remove-Item -Path $work -Recurse -Force -ErrorAction SilentlyContinue
}
