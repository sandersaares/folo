#requires -Version 7

# Resolves a built executable from `cargo build` JSON output.
#
# Test-support binaries can live in separate lib+bin packages, so Cargo does not
# expose them to another package's integration tests via CARGO_BIN_EXE_*. A test
# runner builds each one up front and passes its path through the environment.
# Picking that path out of the streamed `--message-format=json` records is easy
# to get subtly wrong, so the shared parser lives here with Pester coverage.

Set-StrictMode -Version Latest

function Resolve-CargoExecutable {
    # Scans the JSON lines emitted by `cargo build --message-format=json` and returns the path to
    # the named binary (the last matching compiler-artifact, mirroring Cargo's own "last one wins"
    # ordering). Non-JSON lines and messages that carry no target/executable are skipped safely
    # under strict mode. A missing executable fails before any tests start.
    [CmdletBinding()]
    [OutputType([string])]
    param(
        [Parameter(Mandatory)][AllowEmptyCollection()][AllowEmptyString()][string[]] $CargoMessage,
        [Parameter(Mandatory)][string] $TargetName
    )

    $exe = $null
    foreach ($line in $CargoMessage) {
        if ([string]::IsNullOrWhiteSpace($line)) { continue }

        $message = $null
        try {
            $message = $line | ConvertFrom-Json -ErrorAction Stop
        } catch {
            # Tolerate non-JSON lines (rendered diagnostics, blank lines) just as the Rust-side
            # resolver does; only parsed objects flow downstream.
            continue
        }

        if (-not (Test-HasProperty $message 'reason') -or $message.reason -ne 'compiler-artifact') { continue }
        if (-not (Test-HasProperty $message 'executable') -or [string]::IsNullOrEmpty($message.executable)) { continue }
        if (-not (Test-HasProperty $message 'target') -or $null -eq $message.target) { continue }
        if (-not (Test-HasProperty $message.target 'name') -or $message.target.name -ne $TargetName) { continue }

        $exe = $message.executable
    }

    if (-not $exe) {
        throw "could not resolve the $TargetName executable from cargo output"
    }
    return $exe
}

function Test-HasProperty {
    # Strict-mode-safe presence check for a property on a (possibly $null) object, so callers can
    # probe optional fields of heterogeneous cargo messages without tripping StrictMode.
    [CmdletBinding()]
    [OutputType([bool])]
    param(
        [AllowNull()] $InputObject,
        [Parameter(Mandatory)][string] $Name
    )

    if ($null -eq $InputObject) { return $false }
    return $InputObject.PSObject.Properties.Name -contains $Name
}

Export-ModuleMember -Function Resolve-CargoExecutable
