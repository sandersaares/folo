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

Export-ModuleMember -Function Get-MiriSeedRange
