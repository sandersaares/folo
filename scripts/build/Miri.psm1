#requires -Version 7

# Seed-range computation for `just miri-harder`, the sharded many-seeds Miri run.
#
# miri-harder exercises the workspace under a range of Miri PRNG seeds (`-Zmiri-many-seeds`), and
# CI splits that range across parallel runners via the shared 1-based "N/M" shard spec. Turning a
# shard into a concrete `start..end` seed range is fiddly integer arithmetic (floor division plus a
# remainder that the final shard absorbs), so it lives here with Pester coverage rather than inline
# in the recipe. The generic "N/M" parse/validate is delegated to Sharding.psm1; only the
# miri-specific seed slicing (including the "too many shards for the seed budget" guard) is here.

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true

Import-Module (Join-Path $PSScriptRoot 'Sharding.psm1')

function Get-MiriSeedRange {
    # Computes the `-Zmiri-many-seeds` range string for a shard spec. With no spec (an empty
    # string) the full `..$TotalSeeds` range is returned, which Miri reads as seeds 0..TotalSeeds.
    # Otherwise the total seed budget is split evenly across the shards, the final shard absorbing
    # any remainder from uneven division, and the returned "start..end" selects this shard's slice.
    # Throws when the spec is malformed (via Sharding) or when there are more shards than seeds.
    [CmdletBinding()]
    [OutputType([string])]
    param(
        [Parameter(Mandatory)][AllowEmptyString()][string] $Spec,
        [int] $TotalSeeds = 64
    )

    if ($Spec -eq '') {
        return "..$TotalSeeds"
    }

    $shard = ConvertFrom-ShardSpec -Spec $Spec

    $seedsPerShard = [math]::Floor($TotalSeeds / $shard.Count)
    if ($seedsPerShard -lt 1) {
        throw "Invalid SHARD value '$Spec'. Too many shards for $TotalSeeds seeds."
    }

    $start = ($shard.Index - 1) * $seedsPerShard
    if ($shard.Index -eq $shard.Count) {
        # The last shard picks up any remainder left by the uneven floor division above.
        $end = $TotalSeeds
    } else {
        $end = $shard.Index * $seedsPerShard
    }

    return "$start..$end"
}

function Get-MiriFlag {
    # An explicit single seed describes a replay and takes precedence over the catalog's shard.
    # Never append a many-seeds flag to it: Miri would run a different experiment.
    [CmdletBinding()]
    [OutputType([string[]])]
    param(
        [string[]] $Flags = @(),
        [AllowEmptyString()][string] $SeedRange = '',
        [AllowEmptyString()][string] $Shard = '',
        [switch] $Many
    )

    $single = @($Flags | Where-Object { $_ -match '^-Zmiri-seed=' })
    $ranges = @($Flags | Where-Object { $_ -match '^-Zmiri-many-seeds=' })
    if ($single.Count -gt 1 -or $ranges.Count -gt 1 -or ($single.Count -gt 0 -and $ranges.Count -gt 0)) {
        throw [ArgumentException]::new('Miri seed selection must be unambiguous.')
    }
    if ($single.Count -gt 0) {
        if ($single[0] -notmatch '^-Zmiri-seed=\d+$') {
            throw [ArgumentException]::new('Invalid Miri seed.')
        }
        return $Flags
    }
    if ($ranges.Count -gt 0) { $SeedRange = $ranges[0].Substring('-Zmiri-many-seeds='.Length) }
    if (-not $Many -and $ranges.Count -eq 0) { return $Flags }
    if ($SeedRange -eq '') { $SeedRange = Get-MiriSeedRange -Spec $Shard }
    if ($SeedRange -notmatch '^\d*\.\.\d+$') {
        throw [ArgumentException]::new('Invalid Miri seed range.')
    }
    $bounds = $SeedRange -split '\.\.'
    if ([long]$bounds[0] -ge [long]$bounds[1]) {
        throw [ArgumentException]::new('The Miri seed range must be nonempty.')
    }
    if ($ranges.Count -gt 0) { return $Flags }
    return @($Flags) + "-Zmiri-many-seeds=$SeedRange"
}

Export-ModuleMember -Function Get-MiriSeedRange, Get-MiriFlag
