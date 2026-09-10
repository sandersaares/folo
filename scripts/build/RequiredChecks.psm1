#requires -Version 7

# Fan-in classification for the Standard validation `required-checks` job.
#
# GitHub's required-checks field matches a check-name string and cannot express that only the
# matrix legs selected for a run are required. The ruleset therefore requires only this job.
# Parsing `toJSON(needs)` is the required-checks job's substantive work. The logic lives here so
# the classification is Pester-tested and visible to `just validate-scripts` rather than sitting
# inline in YAML.
# See .github/workflows/design.md ("Required checks fan-in") and
# .github/workflows/implementation.md.

Set-StrictMode -Version Latest
Import-Module (Join-Path $PSScriptRoot 'ValidationPlan.psm1')

function Assert-RequiredCheck {
    # Throws when any merge-blocking dependency did not produce an allowed result, so the
    # fan-in fails closed. Prints the failing job names for the run log.
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][AllowEmptyString()][string] $NeedsJson,
        [Parameter(Mandatory)][AllowEmptyCollection()][AllowEmptyString()][string[]] $MustSucceedJob
    )

    if ($MustSucceedJob.Count -eq 0) {
        throw 'MUST_SUCCEED_JOBS is empty; unconditional merge-blocking jobs cannot be classified.'
    }

    # The list arrives as a whitespace-separated workflow literal, so blank and padded entries
    # are ordinary. Normalizing here keeps an empty entry from being classified as an absent job.
    $normalized = @(
        $MustSucceedJob |
            ForEach-Object { $_.Trim() } |
            Where-Object { -not [string]::IsNullOrEmpty($_) }
    )
    if ($normalized.Count -eq 0) {
        throw 'MUST_SUCCEED_JOBS names no jobs; unconditional gates cannot be classified.'
    }

    $failure = @(Get-RequiredCheckFailure -NeedsJson $NeedsJson -MustSucceedJob $normalized)
    if ($failure.Count -eq 0) {
        Write-Host 'All merge-blocking jobs succeeded or were skipped where skipping is allowed.'
        return
    }

    $listed = $failure -join ', '
    throw "required-checks failed; non-allowed merge-blocking results: $listed"
}

function Get-RequiredCheckFailure {
    # Returns one `job=result` string per merge-blocking dependency whose result is not allowed.
    # Jobs named in `$MustSucceedJob` may only be `success`; every other dependency may be
    # `success` or `skipped`. A missing `result` property is treated as a failure so a malformed
    # `needs` payload cannot pass the fan-in. A job named in `$MustSucceedJob` but absent from
    # `needs` is reported as `absent`. A required dependency absent from the payload would
    # otherwise let the fan-in pass without examining it. An empty `needs` object must fail
    # evaluation rather than be accepted. Keeping this classifier pure lets the policy be tested
    # without GitHub Actions.
    [CmdletBinding()]
    [OutputType([string])]
    param(
        [Parameter(Mandatory)][AllowEmptyString()][string] $NeedsJson,
        [Parameter(Mandatory)][AllowEmptyCollection()][string[]] $MustSucceedJob
    )

    if ([string]::IsNullOrWhiteSpace($NeedsJson)) {
        throw "NEEDS_JSON is empty; the required-checks job cannot classify merge-blocking results."
    }

    $needs = $NeedsJson | ConvertFrom-Json
    if ($null -eq $needs) {
        throw 'NEEDS_JSON did not parse as an object; merge-blocking results cannot be classified.'
    }
    if ($needs -is [System.Array]) {
        throw 'NEEDS_JSON parsed as an array; merge-blocking results cannot be classified.'
    }
    if (@($needs.PSObject.Properties).Count -eq 0) {
        throw 'NEEDS_JSON has no jobs; merge-blocking results cannot be classified.'
    }

    $mustSucceed = [System.Collections.Generic.HashSet[string]]::new(
        [string[]] $MustSucceedJob,
        [System.StringComparer]::Ordinal)

    $failure = [System.Collections.Generic.List[string]]::new()
    if ($mustSucceed.Contains('changes')) {
        # Reconstruct the selection from both planners rather than allowing every conditional
        # job to skip. Missing outputs, lost domains, or an omitted conditional dependency fail.
        # Ref: .github/workflows/implementation.md#merge-blocking-result.
        $planJson = [string] $needs.changes.outputs.plan
        $plan = Read-ValidationPlan -Json $planJson
        $expectedDomains = @(Get-ValidationScriptDomain -PlanJson $planJson `
                -AffectedPackageJson $needs.delta.outputs.packages_json)
        $actualDomains = @(Read-ScriptDomain -Value (
                ConvertFrom-Json -InputObject $needs.delta.outputs.script_domains -NoEnumerate))
        if (($expectedDomains -join ' ') -cne ($actualDomains -join ' ')) {
            throw 'The script execution selection does not match the validation plan and Cargo delta.'
        }
        $selection = @{
            'test-scripts' = $expectedDomains.Count -gt 0
            'validate-scripts' = $plan.script_analysis
            'validate-workflows' = $plan.workflows
        }
        foreach ($name in $selection.Keys) {
            if ($name -cnotin $needs.PSObject.Properties.Name) { $failure.Add("$name=absent") }
            if ($selection[$name]) { $null = $mustSucceed.Add($name) }
        }
    }
    foreach ($job in $needs.PSObject.Properties) {
        $result = ''
        if ($null -ne $job.Value -and
            ($job.Value.PSObject.Properties.Name -contains 'result') -and
            $null -ne $job.Value.result) {
            $result = [string] $job.Value.result
        }
        if ([string]::IsNullOrWhiteSpace($result)) {
            $result = 'missing'
        }

        # GitHub writes these results in lower case, and this fan-in is the only thing standing
        # between a broken dependency and a merge, so the comparison is case-sensitive: an
        # unexpected spelling is an unknown result and must fail rather than be read as a pass.
        $resultIsAllowed = $result -ceq 'success'
        if (-not $resultIsAllowed -and
            $result -ceq 'skipped' -and
            -not $mustSucceed.Contains($job.Name)) {
            $resultIsAllowed = $true
        }
        if (-not $resultIsAllowed) {
            $failure.Add("$($job.Name)=$result")
        }
    }

    $present = [System.Collections.Generic.HashSet[string]]::new(
        [string[]] @($needs.PSObject.Properties.Name),
        [System.StringComparer]::Ordinal)
    foreach ($name in @($mustSucceed | Sort-Object)) {
        if (-not $present.Contains($name)) {
            $failure.Add("$name=absent")
        }
    }
    return @($failure)
}

Export-ModuleMember -Function Assert-RequiredCheck
