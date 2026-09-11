#requires -Version 7
# Builds pure record utilities from this reviewed controller and transports JSON over standard
# streams. Hosted reporting and Local triage share this boundary, not an evidence-provided
# executable. Import and empty polling do not build tools or install a toolchain.
# Ref: ../../.github/workflows/implementation.md#serialized-reporting.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
Import-Module (Join-Path $PSScriptRoot 'ScheduledJson.psm1')
Import-Module (Join-Path $PSScriptRoot 'ScheduledTransport.psm1')
Import-Module (Join-Path $PSScriptRoot 'ScheduledExecution.psm1')
Import-Module (Join-Path $PSScriptRoot '..\build\CargoExecutable.psm1')
$script:executables = @{}

function Invoke-ScheduledRecordTool {
    [CmdletBinding()]
    [OutputType([hashtable])]
    param(
        [Parameter(Mandatory)][ValidateSet('scheduled-run-record', 'scheduled-triage-record')][string] $Package,
        [Parameter(Mandatory)][hashtable] $Request
    )
    $root = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..')).Path
    if (-not $script:executables.ContainsKey($Package) -or
        -not (Test-Path -LiteralPath $script:executables[$Package] -PathType Leaf)) {
        # Windows and WSL can share a checkout, never a native executable output directory.
        $platform = if ($IsWindows) { 'windows' } elseif ($IsLinux) { 'linux' } else { 'unsupported' }
        $architecture = [Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString()
        $target = Join-Path $root "target\$Package\$platform-$architecture"
        $toolchain = Get-ScheduledToolchain -Kind mutants
        $cargo = (Get-Command cargo -CommandType Application | Select-Object -First 1).Source
        $environment = @{
            RUSTUP_TOOLCHAIN = $toolchain; RUSTUP_AUTO_INSTALL = '0'; CARGO_TARGET_DIR = $target
            CARGO_BUILD_TARGET = $null; CARGO_ENCODED_RUSTFLAGS = $null; CARGO_TERM_COLOR = 'never'; NO_COLOR = '1'
            RUSTFLAGS = ''; RUSTDOCFLAGS = ''; MIRIFLAGS = ''; MUTATION_TESTING = $null
            RUSTC = $null; RUSTDOC = $null; RUSTC_WRAPPER = $null; RUSTC_WORKSPACE_WRAPPER = $null
        }
        $messages = Invoke-ScheduledJsonExecutable -Executable $cargo -Directory $root -InputText '' `
            -Arguments @("+$toolchain", 'build', '--locked', '--package', $Package,
                '--bin', $Package, '--manifest-path', (Join-Path $root 'Cargo.toml'),
                '--target-dir', $target, '--message-format=json') -Environment $environment
        $executable = Resolve-CargoExecutable -CargoMessage @($messages -split '\r?\n') -TargetName $Package
        $script:executables[$Package] = (Resolve-Path -LiteralPath $executable).Path
    }
    $json = Invoke-ScheduledJsonExecutable -Executable $script:executables[$Package] -Directory $root `
        -InputText ($Request | ConvertTo-Json -Depth 100 -Compress)
    return ConvertFrom-ScheduledJson -InputObject $json
}

Export-ModuleMember -Function Invoke-ScheduledRecordTool
