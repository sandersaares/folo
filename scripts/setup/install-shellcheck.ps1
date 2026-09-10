#requires -Version 7

<#
.SYNOPSIS
    Installs a pinned ShellCheck CLI used by actionlint to lint embedded shell scripts.

.DESCRIPTION
    actionlint (see install-actionlint.ps1) delegates linting of `run:` steps that use a POSIX
    shell to ShellCheck when it is present on PATH. GitHub's Linux runners ship ShellCheck, so the
    `validate-workflows` CI job shellchecks every bash step; without ShellCheck installed locally,
    `just validate-workflows` would silently skip those checks and give false confidence. This script
    installs ShellCheck so local linting matches CI.

    ShellCheck is not a cargo crate, and its author ships it as a standalone, portable binary with
    no installer, so this script downloads the official static build for the current OS/architecture
    and drops it on PATH.

    The version is pinned ($script:ShellcheckVersion) and the download comes from the matching
    GitHub release, so `just install-tools` is reproducible. The published SHA-256 of each archive
    is baked in below and verified after download, so a tampered or corrupted archive is rejected.
    Bump the version and the digests together when upgrading; the digests are the `digest` fields of
    the pinned release's assets (visible via `gh api repos/koalaman/shellcheck/releases/tags/v<version>`,
    where `<version>` matches $script:ShellcheckVersion - not `releases/latest`, which would show the
    digests of whatever the newest release happens to be rather than the one this script pins).

    The default destination is the cargo bin directory. That is a deliberate, if pragmatic, choice:
    ShellCheck has no standard home to prefer instead, and the cargo bin directory is the one
    location this repository's developer environment guarantees is already on PATH - every other
    tool `just install-tools` fetches lands there too.

    Idempotent: if the pinned ShellCheck already resolves on PATH the script does nothing (pass
    -Force to reinstall anyway).

.PARAMETER Destination
    Directory to install the ShellCheck binary into. Defaults to the cargo bin directory
    (`~/.cargo/bin`), which is already on PATH in this dev environment.

.PARAMETER Force
    Reinstall even if the pinned ShellCheck is already present on PATH.

.EXAMPLE
    ./install-shellcheck.ps1
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

# The pinned ShellCheck release. Keep $ShellcheckVersion and the per-platform digests in
# $ShellcheckBuilds in sync - both come from one GitHub release of koalaman/shellcheck.
$script:ShellcheckVersion = '0.11.0'

# One entry per supported OS/architecture: the release asset name, the archive format it ships as,
# the binary inside it, and the asset's published lowercase SHA-256. ShellCheck ships no native
# Windows arm64 build, so windows-arm64 reuses the x86_64 zip (Windows runs it under emulation).
$script:ShellcheckBuilds = @{
    'windows-amd64' = @{ Asset = "shellcheck-v$($script:ShellcheckVersion).zip"; Kind = 'zip'; Binary = 'shellcheck.exe'; Sha256 = '8a4e35ab0b331c85d73567b12f2a444df187f483e5079ceffa6bda1faa2e740e' }
    'windows-arm64' = @{ Asset = "shellcheck-v$($script:ShellcheckVersion).zip"; Kind = 'zip'; Binary = 'shellcheck.exe'; Sha256 = '8a4e35ab0b331c85d73567b12f2a444df187f483e5079ceffa6bda1faa2e740e' }
    'linux-amd64'   = @{ Asset = "shellcheck-v$($script:ShellcheckVersion).linux.x86_64.tar.gz"; Kind = 'tar.gz'; Binary = 'shellcheck'; Sha256 = 'b7af85e41cc99489dcc21d66c6d5f3685138f06d34651e6d34b42ec6d54fe6f6' }
    'linux-arm64'   = @{ Asset = "shellcheck-v$($script:ShellcheckVersion).linux.aarch64.tar.gz"; Kind = 'tar.gz'; Binary = 'shellcheck'; Sha256 = '68a8133197a50beb8803f8d42f9908d1af1c5540d4bb05fdfca8c1fa47decefc' }
    'mac-amd64'     = @{ Asset = "shellcheck-v$($script:ShellcheckVersion).darwin.x86_64.tar.gz"; Kind = 'tar.gz'; Binary = 'shellcheck'; Sha256 = 'c2c15e08df0e8fbc374c335b230a7ee958c313fa5714817a59aa59f1aa594f51' }
    'mac-arm64'     = @{ Asset = "shellcheck-v$($script:ShellcheckVersion).darwin.aarch64.tar.gz"; Kind = 'tar.gz'; Binary = 'shellcheck'; Sha256 = '339b930feb1ea764467013cc1f72d09cd6b869ebf1013296ba9055ab2ffbd26f' }
}

function Get-PlatformKey {
    # Identifies the current OS/architecture as a `$ShellcheckBuilds` key. `#requires -Version 7`
    # guarantees the $Is* automatic variables exist, so the final OS branch is simply "not macOS
    # and not Linux", i.e. Windows.
    Write-Verbose 'Determining the operating system and processor architecture.'
    if ($IsMacOS) { $os = 'mac' } elseif ($IsLinux) { $os = 'linux' } else { $os = 'windows' }

    $processArch = [System.Runtime.InteropServices.RuntimeInformation]::ProcessArchitecture
    $arch = switch ($processArch) {
        ([System.Runtime.InteropServices.Architecture]::X64) { 'amd64' }
        ([System.Runtime.InteropServices.Architecture]::Arm64) { 'arm64' }
        default { throw "ShellCheck install does not support processor architecture '$processArch'." }
    }

    $key = "$os-$arch"
    Write-Verbose "Resolved platform key '$key'."
    return $key
}

function Expand-ShellcheckArchive {
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

function Get-InstalledShellcheckVersion {
    # Returns the version of the ShellCheck already on PATH, or $null if none is found.
    # `shellcheck --version` prints a `version: 0.11.0` line among its banner output.
    if ($null -eq (Get-Command shellcheck -ErrorAction SilentlyContinue)) {
        return $null
    }
    $output = (& shellcheck --version 2>$null | Where-Object { $_ -match '^version:' } | Select-Object -First 1)
    if ($output -match 'version:\s*([0-9][0-9.]*)') {
        return $Matches[1]
    }
    return $null
}

if (-not $Force) {
    # Only skip when the ShellCheck already on PATH is exactly the pinned version. A different
    # (e.g. older, globally-installed) ShellCheck would defeat the point of pinning, so install the
    # pinned build over it rather than honouring it.
    $installed = Get-InstalledShellcheckVersion
    if ($installed -eq $script:ShellcheckVersion) {
        Write-Host "ShellCheck $installed already installed; skipping."
        return
    }
    if ($null -ne $installed) {
        Write-Verbose "Found ShellCheck $installed on PATH, but the pinned version is $($script:ShellcheckVersion); installing the pinned build."
    }
}

$platformKey = Get-PlatformKey
$build = $script:ShellcheckBuilds[$platformKey]
if ($null -eq $build) {
    throw "No pinned ShellCheck build is configured for platform '$platformKey'."
}

$url = "https://github.com/koalaman/shellcheck/releases/download/v$($script:ShellcheckVersion)/$($build.Asset)"
Write-Host "Installing ShellCheck $($script:ShellcheckVersion) (used by actionlint via 'just validate-workflows')..."
Write-Verbose "Platform '$platformKey' maps to asset '$($build.Asset)'; downloading from '$url'."

Write-Verbose "Ensuring the destination directory '$Destination' exists."
New-Item -ItemType Directory -Force -Path $Destination | Out-Null

# Stage the download in a throwaway temp directory so a failed extract leaves nothing behind; the
# finally block removes it regardless of outcome.
$work = New-Item -ItemType Directory -Force -Path (Join-Path ([System.IO.Path]::GetTempPath()) ("shellcheck-" + [guid]::NewGuid()))
Write-Verbose "Staging the download in temp directory '$($work.FullName)'."
try {
    $archive = Join-Path $work $build.Asset
    Write-Verbose "Downloading ShellCheck archive to '$archive'."
    # Retry the download and its checksum together: a truncated or corrupted fetch fails
    # verification and is re-fetched, so only a genuinely wrong pinned digest exhausts the attempts.
    Invoke-WithRetry -Attempt 4 -DelaySeconds 3 -BackoffMultiplier 2 -MaxDelaySeconds 30 -Action {
        Invoke-WebRequest -Uri $url -OutFile $archive

        Write-Verbose "Verifying the archive SHA-256 against the pinned digest."
        # Get-FileHash returns uppercase hex; normalize both sides to lowercase so the comparison cannot
        # fail on case alone.
        $actual = (Get-FileHash -Path $archive -Algorithm SHA256).Hash.ToLowerInvariant()
        if ($actual -ne $build.Sha256) {
            throw "ShellCheck archive SHA-256 mismatch for '$($build.Asset)': expected $($build.Sha256), got $actual."
        }
    }

    Expand-ShellcheckArchive -ArchivePath $archive -ArchiveKind $build.Kind -DestinationDir $work

    Write-Verbose "Locating '$($build.Binary)' inside the extracted archive."
    $bin = Get-ChildItem -Path $work -Recurse -Filter $build.Binary | Select-Object -First 1
    if ($null -eq $bin) {
        throw "ShellCheck binary ('$($build.Binary)') was not found inside the downloaded archive."
    }

    Write-Verbose "Copying '$($bin.FullName)' to '$Destination'."
    Copy-Item -Path $bin.FullName -Destination $Destination -Force

    if (-not $IsWindows) {
        # Some archive extractors drop the stored Unix executable bit; restore it on every Unix host
        # so the installed binary actually runs and resolves on PATH.
        $installed = Join-Path $Destination $build.Binary
        Write-Verbose "Marking '$installed' executable."
        chmod +x $installed
    }

    Write-Host "Installed ShellCheck $($script:ShellcheckVersion) to $Destination."
} finally {
    Write-Verbose "Cleaning up temp directory '$($work.FullName)'."
    Remove-Item -Path $work -Recurse -Force -ErrorAction SilentlyContinue
}
