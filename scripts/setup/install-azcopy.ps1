#requires -Version 7

<#
.SYNOPSIS
    Installs a pinned azcopy CLI used by the cargo-bench-history Azure stress harness.

.DESCRIPTION
    azcopy performs the bulk blob upload in `just bench-history-stress-azure`. It is
    not a cargo crate, and Microsoft ships it as a standalone, portable binary with no
    installer and no canonical install location of its own, so this script downloads
    the official static build for the current OS/architecture and drops it on PATH.

    The version is pinned ($script:AzCopyVersion) and the download comes from the
    matching GitHub release (not the floating `aka.ms/downloadazcopy-v10-*` redirect),
    so `just install-tools` is reproducible. The published SHA-256 of each archive is
    baked in below and verified after download, so a tampered or corrupted archive is
    rejected. Bump the version and the six digests together when upgrading; the digests
    are listed on the release page (and via the GitHub releases API).

    The default destination is the cargo bin directory. That is a deliberate, if
    pragmatic, choice: azcopy has no standard home to prefer instead, and the cargo bin
    directory is the one location this repository's developer environment guarantees is
    already on PATH - every other tool `just install-tools` fetches lands there too.
    Override -Destination to install elsewhere, but that location must be on PATH for
    `just bench-history-stress-azure` to find azcopy.

    Idempotent: if azcopy already resolves on PATH the script does nothing (pass -Force
    to reinstall anyway).

.PARAMETER Destination
    Directory to install the azcopy binary into. Defaults to the cargo bin directory
    (`~/.cargo/bin`), which is already on PATH in this dev environment.

.PARAMETER Force
    Reinstall even if azcopy is already present on PATH.

.EXAMPLE
    ./install-azcopy.ps1
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

# The pinned azcopy release. Keep $AzCopyVersion and the per-platform digests in
# $AzCopyBuilds in sync - both come from one GitHub release of azure-storage-azcopy.
$script:AzCopyVersion = '10.32.7'

# One entry per supported OS/architecture: the release asset name, the archive format
# it ships as, the binary inside it, and the asset's published lowercase SHA-256.
$script:AzCopyBuilds = @{
    'windows-amd64' = @{ Asset = "azcopy_windows_amd64_$($script:AzCopyVersion).zip"; Kind = 'zip'; Binary = 'azcopy.exe'; Sha256 = '7a7e572ea75445acf856cba53e8985374e46ecd3db6324042fc1911c5e27e30a' }
    'windows-arm64' = @{ Asset = "azcopy_windows_arm64_$($script:AzCopyVersion).zip"; Kind = 'zip'; Binary = 'azcopy.exe'; Sha256 = 'e8961b448460e3ae5e2e6ceacf8f0fa3a92704eb3b9faae9fa1cf91813b3d55e' }
    'linux-amd64'   = @{ Asset = "azcopy_linux_amd64_$($script:AzCopyVersion).tar.gz"; Kind = 'tar.gz'; Binary = 'azcopy'; Sha256 = '771dabec5bd8034223010d6f5186956203649e1013ed1d9e6b533fc09ed4a9ee' }
    'linux-arm64'   = @{ Asset = "azcopy_linux_arm64_$($script:AzCopyVersion).tar.gz"; Kind = 'tar.gz'; Binary = 'azcopy'; Sha256 = '71072ff9609123e0f3ff70215303c15ab3e52ae4ad9d2ea5022db19f48204782' }
    'mac-amd64'     = @{ Asset = "azcopy_darwin_amd64_$($script:AzCopyVersion).zip"; Kind = 'zip'; Binary = 'azcopy'; Sha256 = '233069680b3c3a1d7d67d58750c4d3eaf85b97cf4d68b9bc9dabddf502c4ccda' }
    'mac-arm64'     = @{ Asset = "azcopy_darwin_arm64_$($script:AzCopyVersion).zip"; Kind = 'zip'; Binary = 'azcopy'; Sha256 = '9ff234c4e0eed47d74b112320a0774326da213feee603caa93da35f9d09b7d1c' }
}

function Get-PlatformKey {
    # Identifies the current OS/architecture as a `$AzCopyBuilds` key. `#requires
    # -Version 7` guarantees the $Is* automatic variables exist, so the final OS
    # branch is simply "not macOS and not Linux", i.e. Windows.
    Write-Verbose 'Determining the operating system and processor architecture.'
    if ($IsMacOS) { $os = 'mac' } elseif ($IsLinux) { $os = 'linux' } else { $os = 'windows' }

    $processArch = [System.Runtime.InteropServices.RuntimeInformation]::ProcessArchitecture
    $arch = switch ($processArch) {
        ([System.Runtime.InteropServices.Architecture]::X64) { 'amd64' }
        ([System.Runtime.InteropServices.Architecture]::Arm64) { 'arm64' }
        default { throw "azcopy install does not support processor architecture '$processArch'." }
    }

    $key = "$os-$arch"
    Write-Verbose "Resolved platform key '$key'."
    return $key
}

function Expand-AzCopyArchive {
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

function Get-InstalledAzCopyVersion {
    # Returns the version of the azcopy already on PATH, or $null if none is found.
    # `azcopy --version` prints e.g. "azcopy version 10.32.4".
    if ($null -eq (Get-Command azcopy -ErrorAction SilentlyContinue)) {
        return $null
    }
    $output = (& azcopy --version 2>$null) -join ' '
    if ($output -match 'version\s+([0-9][0-9.]*)') {
        return $Matches[1]
    }
    return $null
}

if (-not $Force) {
    # Only skip when the azcopy already on PATH is exactly the pinned version. A
    # different (e.g. older, globally-installed) azcopy would defeat the point of
    # pinning, so install the pinned build over it rather than honouring it.
    $installed = Get-InstalledAzCopyVersion
    if ($installed -eq $script:AzCopyVersion) {
        Write-Host "azcopy $installed already installed; skipping."
        return
    }
    if ($null -ne $installed) {
        Write-Verbose "Found azcopy $installed on PATH, but the pinned version is $($script:AzCopyVersion); installing the pinned build."
    }
}

$platformKey = Get-PlatformKey
$build = $script:AzCopyBuilds[$platformKey]
if ($null -eq $build) {
    throw "No pinned azcopy build is configured for platform '$platformKey'."
}

$url = "https://github.com/Azure/azure-storage-azcopy/releases/download/v$($script:AzCopyVersion)/$($build.Asset)"
Write-Host "Installing azcopy $($script:AzCopyVersion) (used by 'just bench-history-stress-azure')..."
Write-Verbose "Platform '$platformKey' maps to asset '$($build.Asset)'; downloading from '$url'."

Write-Verbose "Ensuring the destination directory '$Destination' exists."
New-Item -ItemType Directory -Force -Path $Destination | Out-Null

# Stage the download in a throwaway temp directory so a failed extract leaves nothing
# behind; the finally block removes it regardless of outcome.
$work = New-Item -ItemType Directory -Force -Path (Join-Path ([System.IO.Path]::GetTempPath()) ("azcopy-" + [guid]::NewGuid()))
Write-Verbose "Staging the download in temp directory '$($work.FullName)'."
try {
    $archive = Join-Path $work $build.Asset
    Write-Verbose "Downloading azcopy archive to '$archive'."
    # Retry the download and its checksum together: a truncated or corrupted fetch fails
    # verification and is re-fetched, so only a genuinely wrong pinned digest exhausts the attempts.
    Invoke-WithRetry -Attempt 4 -DelaySeconds 3 -BackoffMultiplier 2 -MaxDelaySeconds 30 -Action {
        Invoke-WebRequest -Uri $url -OutFile $archive

        Write-Verbose "Verifying the archive SHA-256 against the pinned digest."
        # Get-FileHash returns uppercase hex; normalize both sides to lowercase so the
        # comparison cannot fail on case alone.
        $actual = (Get-FileHash -Path $archive -Algorithm SHA256).Hash.ToLowerInvariant()
        if ($actual -ne $build.Sha256) {
            throw "azcopy archive SHA-256 mismatch for '$($build.Asset)': expected $($build.Sha256), got $actual."
        }
    }

    Expand-AzCopyArchive -ArchivePath $archive -ArchiveKind $build.Kind -DestinationDir $work

    Write-Verbose "Locating '$($build.Binary)' inside the extracted archive."
    $bin = Get-ChildItem -Path $work -Recurse -Filter $build.Binary | Select-Object -First 1
    if ($null -eq $bin) {
        throw "azcopy binary ('$($build.Binary)') was not found inside the downloaded archive."
    }

    Write-Verbose "Copying '$($bin.FullName)' to '$Destination'."
    Copy-Item -Path $bin.FullName -Destination $Destination -Force

    if (-not $IsWindows) {
        # Expand-Archive (used for the macOS .zip builds) discards the stored Unix
        # executable bit, so the extracted azcopy lands non-executable. Restore it on
        # every Unix host so the installed binary actually runs and resolves on PATH.
        $installed = Join-Path $Destination $build.Binary
        Write-Verbose "Marking '$installed' executable."
        chmod +x $installed
    }

    Write-Host "Installed azcopy $($script:AzCopyVersion) to $Destination."
} finally {
    Write-Verbose "Cleaning up temp directory '$($work.FullName)'."
    Remove-Item -Path $work -Recurse -Force -ErrorAction SilentlyContinue
}
