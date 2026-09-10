#requires -Version 7

# Output-directory preparation shared by the benchmark-history automation recipes in
# justfiles/just_bench_history.just. Both analyze recipes (gh-analyze-bench-history,
# gh-analyze-pr-bench-history) must ensure their scratch report directory exists before writing the
# four report artefacts into it, and gh-write-bench-history-machine-key must ensure the parent
# directory of the key file exists before writing the key.
#
# Ensuring a directory exists looks trivial, but the exact cmdlet invocation is a footgun: New-Item's
# directory-creation parameter is -Path, NOT -LiteralPath (unlike Get-/Set-/Remove-Item, New-Item has
# no -LiteralPath), and getting it wrong fails the whole step with "A parameter cannot be found that
# matches parameter name 'LiteralPath'". A justfile [script] block is invisible to both
# PSScriptAnalyzer and Pester, so exactly that mistake once shipped straight to CI in all three
# recipes. Keeping the one line here behind a seam the Pester suite (BenchHistoryPath.Tests.ps1)
# actually executes is what stops it recurring, and the recipes become a thin import + call.

Set-StrictMode -Version Latest

function New-BenchHistoryDirectory {
    # Ensures $Path exists as a directory, creating any missing parents, and is a no-op when it
    # already exists - so a recipe can call it unconditionally on a path that may or may not have been
    # created yet. An empty or null $Path is treated as "nothing to create" rather than an error: that
    # is what `Split-Path -Parent` yields for a bare filename with no directory component, and
    # gh-write-bench-history-machine-key relies on it to skip creation in that case.
    [CmdletBinding(SupportsShouldProcess)]
    [OutputType([void])]
    param(
        [Parameter(Mandatory)]
        [AllowEmptyString()]
        [AllowNull()]
        [string] $Path
    )

    if ([string]::IsNullOrEmpty($Path)) {
        Write-Verbose 'No directory to create (the path has no directory component); nothing to do.'
        return
    }

    # -Path (New-Item has no -LiteralPath); -Force makes an already-existing directory a no-op instead
    # of an error and creates any missing intermediate parents.
    if ($PSCmdlet.ShouldProcess($Path, 'create directory')) {
        New-Item -ItemType Directory -Path $Path -Force | Out-Null
        Write-Verbose "Ensured directory exists: $Path."
    }
}

Export-ModuleMember -Function New-BenchHistoryDirectory
