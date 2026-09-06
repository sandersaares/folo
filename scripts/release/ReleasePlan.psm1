#requires -Version 7

# Helpers for pull-request release-plan generation and application.
#
# `cargo-release-plan` owns released-content comparison. This module validates its report,
# selects the explicitly supported cargo-semver-checks targets, and provides the deterministic
# mechanics used by the increment-versions skill. See `.github/workflows/implementation.md`.

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true

# Must match packages/cargo-release-plan/src/plan.rs. An incompatible report must fail closed.
$script:ReleasePlanSchemaVersion = [long] 1

# Local working-file format used by the increment-versions skill. Advance it for incompatible
# working-file shape changes, coordinated with the skill that reads and writes the same contract.
$script:ChangeDecisionSchemaVersion = [long] 1

# These values come from cargo-semver-checks' exit-status contract. Both represent completed
# comparisons, so evidence collection keeps the log for either outcome instead of treating denied
# findings as an infrastructure failure.
$script:SemverCheckNoFindingsExitCode = 0
$script:SemverCheckDenyFindingsExitCode = 100

# Keeps transient crates.io index uncertainty from blocking a valid plan while keeping the
# first-publication gate bounded in CI.
$script:PublishStatusRetryAttempt = 3
$script:PublishStatusRetryDelaySeconds = 1

# Publication lookups reuse the workspace's shared transient-retry policy rather than hand-rolling
# a separate loop at the release-plan boundary.
Import-Module (Join-Path $PSScriptRoot '..' 'utility' 'Retry.psm1') -Force

# Packages whose library surface is a supported consumer contract. Implementation partitions,
# test-support packages, and undocumented handoff crates are intentionally absent. A change in a
# grouped implementation package selects the listed public package from that group.
$script:SemverCheckTargetAllowList = [System.Collections.Generic.HashSet[string]]::new(
    [System.StringComparer]::Ordinal
)
@(
    'all_the_time'
    'alloc_tracker'
    'awaiter_set'
    'cargo-bench-history'
    'cargo-detect-package'
    'cargo-freeze-deps'
    'cargo-release-plan'
    'cpulist'
    'dure'
    'events'
    'events_once'
    'fast_time'
    'future_deque'
    'infinity_pool'
    'linked'
    'many_cpus'
    'many_cpus_benchmarking'
    'nm'
    'nm_otel'
    'par_bench'
    'region_cached'
    'region_local'
    'vicinal'
) | ForEach-Object { [void] $script:SemverCheckTargetAllowList.Add($_) }

function Get-ReleasePlanCargoArgument {
    # Argument vector for `cargo run -p cargo-release-plan --locked -- ...`. Forwards
    # `$Base` as `--base` when set; otherwise the tool chooses the release baseline.
    [CmdletBinding()]
    [OutputType([string[]])]
    param(
        [Parameter(Mandatory)][string[]] $Command,
        [string] $Base = $env:RELEASE_PLAN_BASE
    )

    $argument = @('run', '-p', 'cargo-release-plan', '--locked', '--') + $Command
    if (-not [string]::IsNullOrWhiteSpace($Base)) {
        $argument += @('--base', $Base)
    }
    return $argument
}

function Write-ReleasePlanBaseVerbose {
    # Explanatory note for the release baseline this invocation uses.
    [CmdletBinding()]
    param(
        [string] $Base = $env:RELEASE_PLAN_BASE
    )

    if (-not [string]::IsNullOrWhiteSpace($Base)) {
        Write-Verbose "Using RELEASE_PLAN_BASE=$Base as the release baseline (explicit)" -Verbose
    } else {
        Write-Verbose 'Using the release baseline selected by cargo-release-plan' -Verbose
    }
}

function Read-ReleasePlanReport {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string] $ReportPath
    )

    if (-not (Test-Path -LiteralPath $ReportPath)) {
        throw "release-plan report not found at '$ReportPath'."
    }

    $report = Get-Content -LiteralPath $ReportPath -Raw | ConvertFrom-Json
    if ($null -eq $report -or $report -is [System.Array]) {
        throw "release-plan report at '$ReportPath' must be a JSON object."
    }

    $field = @($report.PSObject.Properties.Name)
    if ($field -notcontains 'schema_version' -or $null -eq $report.schema_version) {
        throw "release-plan report at '$ReportPath' is missing schema_version."
    }
    if (($report.schema_version -isnot [long] -and $report.schema_version -isnot [int]) -or
        [long] $report.schema_version -ne $script:ReleasePlanSchemaVersion) {
        throw "release-plan report at '$ReportPath' uses unsupported schema_version '$($report.schema_version)'; expected $script:ReleasePlanSchemaVersion."
    }
    if ($field -notcontains 'packages' -or $report.packages -isnot [System.Array]) {
        throw "release-plan report at '$ReportPath' packages must be an array."
    }
    if ($field -notcontains 'groups' -or $null -eq $report.groups -or
        $report.groups -is [System.Array]) {
        throw "release-plan report at '$ReportPath' groups must be an object."
    }

    $seen = [System.Collections.Generic.HashSet[string]]::new(
        [System.StringComparer]::Ordinal
    )
    foreach ($package in $report.packages) {
        if ($null -eq $package) {
            throw "release-plan report at '$ReportPath' contains a null package."
        }
        $packageField = @($package.PSObject.Properties.Name)
        foreach ($required in @('name', 'declared_version', 'status', 'changed', 'dependencies')) {
            if ($packageField -notcontains $required) {
                throw "release-plan report at '$ReportPath' package is missing $required."
            }
        }
        $name = [string] $package.name
        if ([string]::IsNullOrWhiteSpace($name) -or -not $seen.Add($name)) {
            throw "release-plan report at '$ReportPath' contains an empty or duplicate package name."
        }
        if ([string]::IsNullOrWhiteSpace([string] $package.declared_version)) {
            throw "release-plan report at '$ReportPath' package '$name' has no declared_version."
        }
        if ([string] $package.status -notin @('needs-increment', 'pending-release', 'unchanged')) {
            throw "release-plan report at '$ReportPath' package '$name' has unsupported status '$($package.status)'."
        }
        if ($package.changed -isnot [System.Array]) {
            throw "release-plan report at '$ReportPath' package '$name' changed must be an array."
        }
        if ($package.dependencies -isnot [System.Array]) {
            throw "release-plan report at '$ReportPath' package '$name' dependencies must be an array."
        }
    }

    foreach ($group in $report.groups.PSObject.Properties) {
        if ($null -eq $group.Value -or
            $group.Value.PSObject.Properties.Name -notcontains 'members' -or
            $group.Value.members -isnot [System.Array]) {
            throw "release-plan report at '$ReportPath' group '$($group.Name)' members must be an array."
        }
    }

    return $report
}

function Get-PackageByName {
    param(
        [Parameter(Mandatory)] $Report
    )

    $byName = [ordered]@{}
    foreach ($package in $Report.packages) {
        $byName[[string] $package.name] = $package
    }
    return $byName
}

function Get-AffectedSemverCheckTarget {
    # Returns the explicit consumer-contract targets affected by a release-plan report. A
    # well-formed report with no selected target is a valid empty result.
    [CmdletBinding()]
    [OutputType([string[]])]
    param(
        [Parameter(Mandatory)][string] $ReportPath
    )

    $report = Read-ReleasePlanReport -ReportPath $ReportPath
    $selectedTargets = [System.Collections.Generic.HashSet[string]]::new(
        [System.StringComparer]::Ordinal
    )

    foreach ($package in $report.packages) {
        $packageName = [string] $package.name
        $status = [string] $package.status
        if ($status -notin @('needs-increment', 'pending-release') -or
            @($package.changed).Count -eq 0) {
            continue
        }

        $groupName = $null
        $candidateTargets = @($packageName)
        if ($package.PSObject.Properties.Name -contains 'group' -and
            -not [string]::IsNullOrWhiteSpace([string] $package.group)) {
            $groupName = [string] $package.group
            $group = $report.groups.PSObject.Properties[$groupName]
            if ($null -eq $group) {
                throw "release-plan report at '$ReportPath' package '$packageName' names unknown group '$groupName'."
            }
            $candidateTargets = @($group.Value.members | ForEach-Object { [string] $_ })
        }

        $supportedTargets = @(
            $candidateTargets |
                Where-Object { $script:SemverCheckTargetAllowList.Contains($_) } |
                Sort-Object -Unique
        )
        if ($supportedTargets.Count -eq 0) {
            $candidateText = if ($candidateTargets.Count -gt 0) {
                "'" + ($candidateTargets -join "', '") + "'"
            } else {
                '(none)'
            }
            Write-Verbose (
                "Changed package '$packageName' has candidate SemVer-check targets " +
                "$candidateText, but none are in the supported consumer-contract target " +
                "allow-list, so no cargo-semver-checks target is emitted."
            ) -Verbose
            continue
        }

        foreach ($target in $supportedTargets) {
            if ($selectedTargets.Add($target)) {
                if ($null -ne $groupName) {
                    Write-Verbose (
                        "Changed package '$packageName' belongs to version group '$groupName'; " +
                        "supported consumer-contract target '$target' is emitted because group " +
                        "members are checked through the public package contract."
                    ) -Verbose
                } else {
                    Write-Verbose (
                        "Changed package '$packageName' is emitted as cargo-semver-checks " +
                        "target '$target' because it is in the supported consumer-contract " +
                        "target allow-list."
                    ) -Verbose
                }
            }
        }
    }

    return @($selectedTargets | Sort-Object)
}

function Get-SemverCheckCargoArgument {
    param(
        [Parameter(Mandatory)][string[]] $Package
    )

    $argument = @('semver-checks', '--all-features')
    foreach ($name in $Package) {
        $argument += @('-p', $name)
    }
    return $argument
}

function Assert-SemverCheckExitCode {
    # Both completed cargo-semver-checks outcomes are retained as evidence because a finding exit
    # still means the comparison ran successfully and produced the log the skill needs.
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][int] $ExitCode,
        [Parameter(Mandatory)][string] $LogPath
    )

    if ($ExitCode -eq $script:SemverCheckNoFindingsExitCode -or
        $ExitCode -eq $script:SemverCheckDenyFindingsExitCode) {
        Write-Host (
            "cargo-semver-checks exited with code $ExitCode; " +
            "the log was written to '$LogPath'."
        )
        return
    }

    throw (
        "cargo-semver-checks failed with exit code $ExitCode; " +
        "the log was written to '$LogPath'."
    )
}

function Invoke-ReleaseReport {
    # Collects the release-plan report and cargo-semver-checks evidence for the same explicit
    # consumer-contract target policy used by CI.
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string] $OutDir,
        [string] $Base = $env:RELEASE_PLAN_BASE,
        [scriptblock] $Cargo = { param([string[]] $Argument) & cargo @Argument }
    )

    if ([string]::IsNullOrWhiteSpace($OutDir)) {
        throw 'release-report requires an output directory.'
    }
    New-Item -ItemType Directory -Path $OutDir -Force | Out-Null

    Write-ReleasePlanBaseVerbose -Base $Base
    $reportArgument =
        Get-ReleasePlanCargoArgument -Command @('report', '--out-dir', $OutDir) -Base $Base
    & $Cargo $reportArgument

    $reportPath = Join-Path $OutDir 'report.json'
    $targets = @(Get-AffectedSemverCheckTarget -ReportPath $reportPath)
    $logPath = Join-Path $OutDir 'semver-checks.log'
    if ($targets.Count -eq 0) {
        'No consumer-contract package requires a cargo-semver-checks comparison.' |
            Set-Content -LiteralPath $logPath -Encoding utf8
        Write-Host "No cargo-semver-checks target was selected; the log was written to '$logPath'."
        return
    }

    $argument = Get-SemverCheckCargoArgument -Package $targets
    Write-Verbose "Running cargo $($argument -join ' '); output captured at '$logPath'." -Verbose
    $previousPreference = $PSNativeCommandUseErrorActionPreference
    # cargo-semver-checks reports detected SemVer findings with a nonzero exit, so this
    # invocation must capture output and classify $LASTEXITCODE manually. The finally block
    # restores the caller's native-command error behavior.
    $PSNativeCommandUseErrorActionPreference = $false
    try {
        & $Cargo $argument 2>&1 | Tee-Object -FilePath $logPath
        $exitCode = $LASTEXITCODE
    } finally {
        $PSNativeCommandUseErrorActionPreference = $previousPreference
    }
    Assert-SemverCheckExitCode -ExitCode $exitCode -LogPath $logPath
}

function Invoke-SemverCheck {
    # CI wrapper for one or more space-separated package names.
    [CmdletBinding()]
    param(
        [AllowEmptyString()][string] $Package,
        [scriptblock] $Cargo = { param([string[]] $Argument) & cargo @Argument }
    )

    $targets = @($Package -split '\s+' | Where-Object { $_ })
    if ($targets.Count -eq 0) {
        Write-Host 'No consumer-contract packages require cargo-semver-checks; skipping.'
        return
    }

    $argument = Get-SemverCheckCargoArgument -Package $targets
    Write-Verbose "Running cargo $($argument -join ' ')" -Verbose
    & $Cargo $argument
}

function Invoke-ExpandReleasePlan {
    # Expands a proposed plan into an expanded plan, naming every package it reaches at the
    # version each will carry. Resolution belongs to cargo-release-plan, so the skill presents
    # the tool's own answer rather than a second implementation of the same rules.
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string] $PlanPath,
        [Parameter(Mandatory)][string] $ExpandedPath,
        [scriptblock] $Cargo = { param([string[]] $Argument) & cargo @Argument }
    )

    if ([string]::IsNullOrWhiteSpace($PlanPath)) {
        throw 'expand-release-plan requires a plan JSON path.'
    }
    if (-not (Test-Path -LiteralPath $PlanPath)) {
        throw "expand-release-plan plan file not found: $PlanPath"
    }
    if ([string]::IsNullOrWhiteSpace($ExpandedPath)) {
        throw 'expand-release-plan requires an output path.'
    }

    $expandedDirectory = Split-Path -Parent $ExpandedPath
    if ([string]::IsNullOrWhiteSpace($expandedDirectory)) {
        $expandedDirectory = '.'
    }
    New-Item -ItemType Directory -Path $expandedDirectory -Force | Out-Null

    $expandedLeaf = Split-Path -Leaf $ExpandedPath
    $stagingPath = Join-Path $expandedDirectory "$expandedLeaf.$(New-Guid).staging"
    if (Test-Path -LiteralPath $ExpandedPath) {
        Remove-Item -LiteralPath $ExpandedPath -Force
    }

    Write-Verbose (
        "Expanding version groups from '$PlanPath' to '$ExpandedPath' " +
        'via cargo-release-plan expand.'
    ) -Verbose
    try {
        & $Cargo @(
            'run', '-p', 'cargo-release-plan', '--locked', '--',
            'expand', '--plan', $PlanPath, '--out', $stagingPath
        )
        if ($LASTEXITCODE -ne 0) {
            throw "cargo-release-plan expand failed with exit code $LASTEXITCODE."
        }
        Move-Item -LiteralPath $stagingPath -Destination $ExpandedPath -Force
    } finally {
        Remove-Item -LiteralPath $stagingPath -Force -ErrorAction SilentlyContinue
    }
}

function Invoke-ApplyReleasePlan {
    # Applies an expanded plan, which is the document the caller reviewed.
    #
    # A proposed plan is rejected here rather than passed through: the publication gate that runs
    # immediately before this reads the expanded plan's package set, so applying a proposed plan
    # would edit packages that gate never saw. `cargo-release-plan apply` itself accepts either
    # stage; this is the skill's stricter path, not the tool's rule.
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string] $ExpandedPath,
        [scriptblock] $Cargo = { param([string[]] $Argument) & cargo @Argument }
    )

    if ([string]::IsNullOrWhiteSpace($ExpandedPath)) {
        throw 'apply-release-plan requires an expanded plan JSON path.'
    }
    [void] (Read-ExpandedPlan -ExpandedPath $ExpandedPath)

    Write-Verbose "Applying expanded plan from $ExpandedPath via cargo-release-plan apply" -Verbose
    & $Cargo @('run', '-p', 'cargo-release-plan', '--locked', '--', 'apply', '--plan', $ExpandedPath)
}

function Invoke-ValidateVersions {
    # CI orchestration for `just validate-versions`. The report and check use identical inputs.
    [Diagnostics.CodeAnalysis.SuppressMessageAttribute('PSUseSingularNouns', '',
        Justification = 'The function is the just validate-versions recipe body; the job id is plural.')]
    [CmdletBinding()]
    param(
        [string] $GitHubOutputPath = $env:GITHUB_OUTPUT,
        [string] $Base = $env:RELEASE_PLAN_BASE,
        [scriptblock] $Cargo = { param([string[]] $Argument) & cargo @Argument }
    )

    Write-ReleasePlanBaseVerbose -Base $Base

    if (-not [string]::IsNullOrWhiteSpace($GitHubOutputPath)) {
        Import-Module (Join-Path $PSScriptRoot 'ReleaseAutomation.psm1') -Force

        $outDir = Join-Path ([System.IO.Path]::GetTempPath()) "release-plan-$(New-Guid)"
        New-Item -ItemType Directory -Path $outDir | Out-Null
        try {
            $reportArgument =
                Get-ReleasePlanCargoArgument -Command @('report', '--out-dir', $outDir) -Base $Base
            & $Cargo $reportArgument

            $semverTargets =
                @(Get-AffectedSemverCheckTarget -ReportPath (Join-Path $outDir 'report.json'))
            $previousOutput = $env:GITHUB_OUTPUT
            $env:GITHUB_OUTPUT = $GitHubOutputPath
            try {
                # The required zero-target representation is a present `semver_targets=` output.
                Set-GitHubOutput -Name semver_targets -Value ($semverTargets -join ' ') `
                    -AllowEmptyValue
            } finally {
                $env:GITHUB_OUTPUT = $previousOutput
            }
        } finally {
            Remove-Item -LiteralPath $outDir -Recurse -Force -ErrorAction SilentlyContinue
        }
    }

    $checkArgument =
        Get-ReleasePlanCargoArgument -Command @('check', '--format', 'github') -Base $Base
    & $Cargo $checkArgument
}

function Get-ReachablePackageName {
    param(
        [Parameter(Mandatory)][string] $Start,
        [Parameter(Mandatory)][hashtable] $Dependency
    )

    $reachable = [System.Collections.Generic.HashSet[string]]::new(
        [System.StringComparer]::Ordinal
    )
    $pending = [System.Collections.Generic.Stack[string]]::new()
    $pending.Push($Start)
    while ($pending.Count -gt 0) {
        $name = $pending.Pop()
        if (-not $reachable.Add($name)) {
            continue
        }
        foreach ($dependencyName in $Dependency[$name]) {
            $pending.Push($dependencyName)
        }
    }

    # The comma keeps the set intact. PowerShell enumerates a returned collection, so a
    # single-element set would arrive at the caller as a bare string whose `Contains` tests for a
    # substring rather than for membership, silently merging packages whose names share a prefix.
    return , $reachable
}

function Get-ReleasePlanAnalysisBatch {
    # Returns dependency-first analysis batches by condensing the package dependency graph into
    # strongly connected components, then emitting those components in topological order. Mutually
    # dependent packages share one batch and must be reconsidered together until their decisions
    # stop changing. Property names are the JSON contract the increment-versions skill documents,
    # so they are lower-case.
    [CmdletBinding()]
    [OutputType([pscustomobject])]
    param(
        [Parameter(Mandatory)][string] $ReportPath
    )

    $report = Read-ReleasePlanReport -ReportPath $ReportPath
    $byName = Get-PackageByName -Report $report
    $name = @($byName.Keys | Sort-Object)
    $dependency = @{}
    foreach ($packageName in $name) {
        $dependency[$packageName] = @(
            $byName[$packageName].dependencies |
                ForEach-Object { [string] $_.name } |
                Where-Object { $byName.Contains($_) } |
                Sort-Object -Unique
        )
    }

    $reachable = @{}
    foreach ($packageName in $name) {
        $reachable[$packageName] =
            Get-ReachablePackageName -Start $packageName -Dependency $dependency
    }

    $assigned = [System.Collections.Generic.HashSet[string]]::new(
        [System.StringComparer]::Ordinal
    )
    $component = [System.Collections.Generic.List[object]]::new()
    foreach ($packageName in $name) {
        if ($assigned.Contains($packageName)) {
            continue
        }
        $member = @(
            $name |
                Where-Object {
                    # Mutual reachability is the strongly connected component test.
                    $reachable[$packageName].Contains($_) -and
                    $reachable[$_].Contains($packageName)
                } |
                Sort-Object
        )
        foreach ($memberName in $member) {
            [void] $assigned.Add($memberName)
        }
        $component.Add([pscustomobject]@{
            Id      = $component.Count
            Members = $member
        })
    }

    $componentOf = @{}
    foreach ($entry in $component) {
        foreach ($memberName in $entry.Members) {
            $componentOf[$memberName] = $entry.Id
        }
    }
    $incoming = @{}
    foreach ($entry in $component) {
        $incoming[$entry.Id] = [System.Collections.Generic.HashSet[int]]::new()
        foreach ($memberName in $entry.Members) {
            foreach ($dependencyName in $dependency[$memberName]) {
                # Component edges retain only external dependencies; internal edges are the SCC.
                $dependencyComponent = [int] $componentOf[$dependencyName]
                if ($dependencyComponent -ne $entry.Id) {
                    [void] $incoming[$entry.Id].Add($dependencyComponent)
                }
            }
        }
    }

    $remaining = [System.Collections.Generic.HashSet[int]]::new()
    foreach ($entry in $component) {
        [void] $remaining.Add($entry.Id)
    }
    $order = 0
    while ($remaining.Count -gt 0) {
        # The first sorted member is unique because components are disjoint.
        $next = @(
            $remaining |
                Where-Object { $incoming[$_].Count -eq 0 } |
                Sort-Object { $component[$_].Members[0] }
        )
        if ($next.Count -eq 0) {
            throw 'release-plan analysis-batch dependency graph unexpectedly contains a cycle.'
        }
        foreach ($componentId in $next) {
            $order++
            $members = @($component[$componentId].Members)
            [pscustomobject][ordered]@{
                order    = $order
                packages = $members
                cyclic   = $members.Count -gt 1
            }
            [void] $remaining.Remove($componentId)
            foreach ($otherId in $remaining) {
                [void] $incoming[$otherId].Remove($componentId)
            }
        }
    }
}

function Get-ReleasePlanAnalysisBatchJson {
    # Serializes the analysis batches into the JSON array the increment-versions skill stores as a
    # working file. The serialization sits here rather than in the recipe that prints it, so the
    # documented field-name contract has one producer that tests can exercise directly.
    [CmdletBinding()]
    [OutputType([string])]
    param(
        [Parameter(Mandatory)][string] $ReportPath
    )

    # The batch contract nests a package-name array inside each batch record.
    $analysisBatchJsonDepth = 3

    return Get-ReleasePlanAnalysisBatch -ReportPath $ReportPath |
        ConvertTo-Json -Depth $analysisBatchJsonDepth -AsArray
}

function Read-ChangeDecision {
    param(
        [Parameter(Mandatory)][string] $DecisionPath
    )

    if (-not (Test-Path -LiteralPath $DecisionPath)) {
        throw "change-decision file not found at '$DecisionPath'."
    }
    $decision = Get-Content -LiteralPath $DecisionPath -Raw | ConvertFrom-Json
    if ($null -eq $decision -or $decision -is [System.Array]) {
        throw "change-decision file at '$DecisionPath' must be a JSON object."
    }
    $field = @($decision.PSObject.Properties.Name)
    if ($field -notcontains 'schema_version' -or
        ($decision.schema_version -isnot [long] -and
            $decision.schema_version -isnot [int]) -or
        [long] $decision.schema_version -ne $script:ChangeDecisionSchemaVersion) {
        throw "change-decision file at '$DecisionPath' must use schema_version $script:ChangeDecisionSchemaVersion."
    }
    if ($field -notcontains 'changes' -or $decision.changes -isnot [System.Array]) {
        throw "change-decision file at '$DecisionPath' changes must be an array."
    }

    $seen = [System.Collections.Generic.HashSet[string]]::new(
        [System.StringComparer]::Ordinal
    )
    foreach ($change in $decision.changes) {
        if ($null -eq $change) {
            throw "change-decision file at '$DecisionPath' contains a null change."
        }
        $changeField = @($change.PSObject.Properties.Name)
        if ($changeField.Count -ne 2 -or
            $changeField -notcontains 'name' -or
            $changeField -notcontains 'level') {
            throw "Each change in '$DecisionPath' must contain only name and level."
        }
        $name = [string] $change.name
        if ([string]::IsNullOrWhiteSpace($name) -or -not $seen.Add($name)) {
            throw "Change names in '$DecisionPath' must be non-empty and unique."
        }
        $level = [string] $change.level
        if ($level -cnotin @('breaking', 'nonbreaking', 'patch')) {
            throw "Change '$name' in '$DecisionPath' has unsupported level '$level'."
        }
    }
    return $decision
}

function Read-ExpandedPlan {
    # Validates that a file is an expanded plan and returns the parsed document.
    #
    # The stage matters to every caller here: only an expanded plan names every package apply
    # will edit, because resolution reaches the version-group members a proposed plan leaves
    # unnamed. Accepting a proposed plan would let the publication gate clear a narrower set than
    # the one that gets written, and would apply a set nobody reviewed.
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string] $ExpandedPath
    )

    if (-not (Test-Path -LiteralPath $ExpandedPath)) {
        throw "expanded plan file not found: $ExpandedPath"
    }
    $plan = Get-Content -LiteralPath $ExpandedPath -Raw | ConvertFrom-Json
    if ($null -eq $plan -or $plan -is [System.Array]) {
        throw "expanded plan at '$ExpandedPath' must be a JSON object."
    }
    $field = @($plan.PSObject.Properties.Name)
    if ($field -notcontains 'schema_version' -or
        ($plan.schema_version -isnot [long] -and $plan.schema_version -isnot [int]) -or
        [long] $plan.schema_version -ne $script:ReleasePlanSchemaVersion) {
        throw "expanded plan at '$ExpandedPath' must use schema_version $script:ReleasePlanSchemaVersion."
    }
    if ($field -notcontains 'expanded' -or $plan.expanded -isnot [bool] -or -not $plan.expanded) {
        throw "plan at '$ExpandedPath' is a proposed plan, not an expanded one; run 'just expand-release-plan' and review the result first."
    }
    if ($field -notcontains 'increments' -or $plan.increments -isnot [System.Array]) {
        throw "expanded plan at '$ExpandedPath' increments must be an array."
    }
    return $plan
}

function Read-ExpandedPlanPackageName {
    # Reads the per-package names cargo-release-plan resolved a plan into. Group expansion is
    # the tool's, so this only validates the shape it promises.
    [CmdletBinding()]
    [OutputType([string[]])]
    param(
        [Parameter(Mandatory)][string] $ExpandedPath
    )

    $plan = Read-ExpandedPlan -ExpandedPath $ExpandedPath

    $name = [System.Collections.Generic.HashSet[string]]::new(
        [System.StringComparer]::Ordinal
    )
    foreach ($increment in $plan.increments) {
        if ($null -eq $increment -or
            $increment.PSObject.Properties.Name -notcontains 'name' -or
            [string]::IsNullOrWhiteSpace([string] $increment.name)) {
            throw "expanded plan at '$ExpandedPath' contains an increment without a name."
        }
        [void] $name.Add([string] $increment.name)
    }
    return @($name | Sort-Object)
}

function Get-PublishStatusWithUnknownRetry {
    # Retries only the indeterminate crates.io status. Confirmed publication states are stable
    # enough to return immediately, while Unknown represents the transient read boundary.
    [Diagnostics.CodeAnalysis.SuppressMessageAttribute('PSReviewUnusedParameter', 'GetPublishStatus',
        Justification = 'Consumed inside the Invoke-WithRetry -Action closure, which the rule does not trace into.')]
    [CmdletBinding()]
    [OutputType([string])]
    param(
        [Parameter(Mandatory)][string] $Name,
        [Parameter(Mandatory)][scriptblock] $GetPublishStatus,
        [ValidateRange(1, [int]::MaxValue)][int] $Attempt,
        [ValidateRange(0, [int]::MaxValue)][int] $DelaySeconds
    )

    $unknownStatusMessage = "crates.io publication status for '$Name' was Unknown"
    try {
        return Invoke-WithRetry -Attempt $Attempt -DelaySeconds $DelaySeconds -Action {
            $status = [string] (& $GetPublishStatus $Name)
            switch -CaseSensitive ($status) {
                'Published' { return 'Published' }
                'NeverPublished' { return 'NeverPublished' }
                default { throw $unknownStatusMessage }
            }
        } -RetryOn {
            param($ErrorRecord)
            return $ErrorRecord.Exception.Message -eq $unknownStatusMessage
        }
    } catch {
        if ($_.Exception.Message -eq $unknownStatusMessage) {
            return 'Unknown'
        }
        throw
    }
}

function Assert-IncrementPackagePublished {
    # Fails unless every package that apply would reach has a confirmed crates.io publication.
    # Reads the expanded plan, so the checked set is exactly the set apply will edit.
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string] $ExpandedPath,
        [scriptblock] $GetPublishStatus = {
            param([string] $Name)
            Get-CratePublishStatus -Name $Name
        },
        [ValidateRange(1, [int]::MaxValue)][int] $PublishStatusRetryAttempt =
            $script:PublishStatusRetryAttempt,
        [ValidateRange(0, [int]::MaxValue)][int] $PublishStatusRetryDelaySeconds =
            $script:PublishStatusRetryDelaySeconds
    )

    Import-Module (Join-Path $PSScriptRoot 'ReleaseAutomation.psm1') -Force
    $packageNames = @(Read-ExpandedPlanPackageName -ExpandedPath $ExpandedPath)
    $neverPublished = [System.Collections.Generic.List[string]]::new()
    $unknown = [System.Collections.Generic.List[string]]::new()
    foreach ($name in $packageNames) {
        $status = Get-PublishStatusWithUnknownRetry -Name $name `
            -GetPublishStatus $GetPublishStatus `
            -Attempt $PublishStatusRetryAttempt `
            -DelaySeconds $PublishStatusRetryDelaySeconds
        switch -CaseSensitive ($status) {
            'Published' { }
            'NeverPublished' { $neverPublished.Add($name) }
            default { $unknown.Add($name) }
        }
    }
    if ($neverPublished.Count -gt 0) {
        $noun = if ($neverPublished.Count -eq 1) { 'package' } else { 'packages' }
        $instruction = if ($neverPublished.Count -eq 1) {
            'Publish the package manually first'
        } else {
            'Publish these packages manually first'
        }
        throw (
            "The increment reaches never-published $($noun): $($neverPublished -join ', '). " +
            "$instruction; follow RELEASING.md#first-publish-of-a-new-crate and complete " +
            'the full procedure, including Trusted Publishing and any binary-release follow-up, ' +
            'before retrying.'
        )
    }
    if ($unknown.Count -gt 0) {
        $noun = if ($unknown.Count -eq 1) { 'package' } else { 'packages' }
        throw "Could not confirm crates.io publication for $($noun): $($unknown -join ', ')."
    }
    Write-Host 'Every package the expanded plan names is already published.'
}

function Get-MinimumVersionForChange {
    # Lowest version that can carry $Level relative to $Anchor.
    #
    # Cargo treats the leftmost non-zero component as the major component, so a
    # 0.y.z release advances y for a breaking change and z for a compatible one,
    # and a 0.0.z release admits no compatible change at all. Deriving this from
    # the anchor rather than from the level alone keeps 0.x packages, which are
    # most of this workspace, from being systematically over-incremented.
    param(
        [Parameter(Mandatory)][semver] $Anchor,
        [Parameter(Mandatory)][string] $Level
    )

    switch -CaseSensitive ($Level) {
        'breaking' {
            if ($Anchor.Major -eq 0 -and $Anchor.Minor -eq 0) {
                return [semver]::new(0, 0, $Anchor.Patch + 1)
            }
            if ($Anchor.Major -eq 0) {
                return [semver]::new(0, $Anchor.Minor + 1, 0)
            }
            return [semver]::new($Anchor.Major + 1, 0, 0)
        }
        'nonbreaking' {
            if ($Anchor.Major -eq 0) {
                return [semver]::new(0, $Anchor.Minor, $Anchor.Patch + 1)
            }
            return [semver]::new($Anchor.Major, $Anchor.Minor + 1, 0)
        }
        'patch' { return [semver]::new($Anchor.Major, $Anchor.Minor, $Anchor.Patch + 1) }
        default { throw "Unsupported change level '$Level'." }
    }
}

function Get-CargoIncrementLevel {
    param(
        [Parameter(Mandatory)][semver] $Current,
        [Parameter(Mandatory)][semver] $Minimum
    )

    if ($Minimum.Major -gt $Current.Major) {
        return 'major'
    }
    if ($Minimum.Minor -gt $Current.Minor) {
        return 'minor'
    }
    return 'patch'
}

function Get-DecisionKey {
    # The key a plan entry folds onto: the package's version group when it has one, otherwise the
    # package itself. Mirrors the tool's own decision keys, so a group counts as already planned
    # whichever member named it.
    param(
        [Parameter(Mandatory)] $ByName,
        [Parameter(Mandatory)][string] $Name
    )

    if (-not $ByName.Contains($Name)) {
        return $Name
    }
    $package = $ByName[$Name]
    if ($package.PSObject.Properties.Name -notcontains 'group' -or
        [string]::IsNullOrWhiteSpace([string] $package.group)) {
        return $Name
    }
    return [string] $package.group
}

function Test-PackageShipsPublishedVersion {
    # Whether a package would end the plan still declaring a version crates.io already carries.
    #
    # This is the question every pin-rewrite guard actually asks. Whether the plan happens to
    # name a package is not the same question and cannot stand in for it: a decision dropped as
    # already covered leaves its package unnamed yet already pending release, an exact
    # group-alignment entry names a leader whose version does not move, and a package with no
    # anchor has never published anything for a rewrite to collide with.
    param(
        [Parameter(Mandatory)] $Package,
        [Parameter(Mandatory)][bool] $Moves
    )

    if ($Moves) {
        return $false
    }
    if ($Package.PSObject.Properties.Name -notcontains 'anchor' -or
        $null -eq $Package.anchor -or
        [string]::IsNullOrWhiteSpace([string] $Package.anchor.version)) {
        return $false
    }
    try {
        $anchor = [semver] [string] $Package.anchor.version
        $declared = [semver] [string] $Package.declared_version
    } catch {
        throw "Package '$($Package.name)' has an invalid semantic version in the release-plan report."
    }
    # A version above the anchor is already pending release, so the rewrite ships with it.
    return $declared -le $anchor
}

function Get-PackageMovedByIncrement {
    # The packages a set of plan increments moves off the version they declare today.
    #
    # Resolution reaches every member of a group an entry names, and an exact-version entry
    # leaves a member that already declares that version exactly where it is, so neither the
    # entry names nor their group closure answer this on their own. Takes the generator's own
    # in-progress entries, which are ordered dictionaries rather than parsed JSON objects.
    [OutputType([System.Collections.Generic.HashSet[string]])]
    param(
        [Parameter(Mandatory)] $Report,
        [Parameter(Mandatory)] $ByName,
        [Parameter(Mandatory)][AllowEmptyCollection()][System.Collections.IDictionary[]] $Increment
    )

    $moved = [System.Collections.Generic.HashSet[string]]::new(
        [System.StringComparer]::Ordinal
    )
    foreach ($entry in $Increment) {
        $entryName = [string] $entry['name']
        $reached = [System.Collections.Generic.List[string]]::new()
        $group = $Report.groups.PSObject.Properties[$entryName]
        if ($null -eq $group) {
            $reached.Add($entryName)
        } else {
            foreach ($member in $group.Value.members) {
                $reached.Add([string] $member)
            }
        }
        foreach ($packageName in $reached) {
            if (-not $ByName.Contains($packageName)) {
                continue
            }
            # A level always raises the package; an exact version moves only those not already
            # declaring it.
            if ($entry.Contains('level') -or
                [string] $ByName[$packageName].declared_version -cne [string] $entry['version']) {
                [void] $moved.Add($packageName)
            }
        }
    }
    return , $moved
}

function Get-PlanIncrement {
    # One entry of a proposed plan. Named for the value it returns, like the alignment helper
    # below, because it only builds a value and changes nothing.
    #
    # The tool requires exactly one of `level` or `version` per entry and rejects a plan carrying
    # both or neither, so every entry is built here rather than assembled at each site. That
    # keeps the shape uniform for the code that reads entries back, which is easy to get wrong
    # because an entry under construction is an ordered dictionary while the same entry parsed
    # from JSON is a PSCustomObject.
    [OutputType([System.Collections.IDictionary])]
    param(
        [Parameter(Mandatory)][string] $Name,
        [string] $Level,
        [string] $Version
    )

    $hasLevel = -not [string]::IsNullOrWhiteSpace($Level)
    $hasVersion = -not [string]::IsNullOrWhiteSpace($Version)
    if ($hasLevel -eq $hasVersion) {
        throw "Plan increment '$Name' must carry exactly one of an increment level or a version."
    }

    if ($hasLevel) {
        return [ordered]@{ name = $Name; level = $Level }
    }
    return [ordered]@{ name = $Name; version = $Version }
}

function Get-GroupAlignmentIncrement {
    # Plan entry that puts a drifted group back on one version.
    #
    # Aligning is normally not an increment: the members simply have to agree, and the highest
    # version any of them already declares is the one they agree on, so raising it would publish
    # every member for no substantive change. The target is the report's own
    # highest-declared-member version, so the rule is not restated here.
    #
    # That exact target is only safe while every package left at its current version keeps its
    # released content, and applying a plan rewrites the version requirement of any workspace
    # path dependency on a package the plan moves. A package that stays put while depending on a
    # moving member therefore has its published manifest rewritten under a version the registry
    # already carries. Where that package is a group member, incrementing the group instead
    # resolves it, because every member then moves. Where it is outside the group, no choice here
    # can resolve it: it needs an increment of its own, which is a decision rather than mechanics.
    param(
        [Parameter(Mandatory)][string] $Name,
        [Parameter(Mandatory)] $Group,
        [Parameter(Mandatory)] $ByName,
        [Parameter(Mandatory)] $Report,
        [Parameter(Mandatory)][AllowEmptyCollection()][System.Collections.IDictionary[]] $Increment
    )

    if ($Group.PSObject.Properties.Name -notcontains 'version' -or
        [string]::IsNullOrWhiteSpace([string] $Group.version)) {
        throw "Group '$Name' has no declared version to align its members on."
    }
    $target = [string] $Group.version

    $member = [System.Collections.Generic.HashSet[string]]::new(
        [System.StringComparer]::Ordinal
    )
    $moving = [System.Collections.Generic.HashSet[string]]::new(
        [System.StringComparer]::Ordinal
    )
    $staying = [System.Collections.Generic.List[string]]::new()
    foreach ($groupMember in $Group.members) {
        $memberName = [string] $groupMember
        if (-not $ByName.Contains($memberName)) {
            continue
        }
        [void] $member.Add($memberName)
        if ([string] $ByName[$memberName].declared_version -ceq $target) {
            $staying.Add($memberName)
        } else {
            [void] $moving.Add($memberName)
        }
    }

    # Realigning this group does not move a package outside it, so such a dependent has to be
    # shipping an unpublished version already for the rewritten requirement to reach consumers.
    $movedByPlan = Get-PackageMovedByIncrement -Report $Report -ByName $ByName -Increment $Increment
    $stranded = [System.Collections.Generic.List[string]]::new()
    foreach ($packageName in $ByName.Keys) {
        if ($member.Contains($packageName)) {
            continue
        }
        $dependsOnMover = $false
        foreach ($dependency in $ByName[$packageName].dependencies) {
            if ($moving.Contains([string] $dependency.name)) {
                $dependsOnMover = $true
                break
            }
        }
        if (-not $dependsOnMover) {
            continue
        }
        if (Test-PackageShipsPublishedVersion -Package $ByName[$packageName] `
                -Moves $movedByPlan.Contains($packageName)) {
            $stranded.Add($packageName)
        }
    }
    if ($stranded.Count -gt 0) {
        $noun = if ($stranded.Count -eq 1) { 'package' } else { 'packages' }
        throw "Realigning group '$Name' on version '$target' rewrites the requirement it is pinned at inside published $($noun): $($stranded -join ', '). Decide a change level for $(if ($stranded.Count -eq 1) { 'it' } else { 'them' }) as well, so the rewritten requirement ships under a new version."
    }

    foreach ($memberName in $staying) {
        if (-not (Test-PackageShipsPublishedVersion -Package $ByName[$memberName] -Moves $false)) {
            continue
        }
        foreach ($dependency in $ByName[$memberName].dependencies) {
            $dependencyName = [string] $dependency.name
            if (-not $moving.Contains($dependencyName)) {
                continue
            }
            Write-Verbose (
                "Group '$Name' cannot align on version '$target' because member " +
                "'$memberName' already declares it and depends on member '$dependencyName', " +
                'which the alignment moves; the rewritten requirement would change released ' +
                "content under '$memberName' version '$target'. Incrementing the group instead."
            ) -Verbose
            return Get-PlanIncrement -Name $Name -Level 'patch'
        }
    }

    Write-Verbose (
        "Group '$Name' aligns on version '$target', which its members already declare at the " +
        'highest, because no package that keeps its version depends on one the alignment moves.'
    ) -Verbose
    return Get-PlanIncrement -Name $Name -Version $target
}

function New-ReleasePlanFile {
    # Writes the proposed plan: the approved change levels mapped to cargo-release-plan's
    # mechanical increment levels, plus whatever it takes to make every inconsistent version
    # group consistent. Existing pending-release increments are retained and raised only when
    # insufficient. Expanding this proposal is a separate step, because only an expanded plan
    # names every package the plan reaches.
    [CmdletBinding(SupportsShouldProcess)]
    param(
        [Parameter(Mandatory)][string] $ReportPath,
        [Parameter(Mandatory)][string] $DecisionPath,
        [Parameter(Mandatory)][string] $PlanPath
    )

    $report = Read-ReleasePlanReport -ReportPath $ReportPath
    $decision = Read-ChangeDecision -DecisionPath $DecisionPath
    $byName = Get-PackageByName -Report $report
    $increment = [System.Collections.Generic.List[object]]::new()
    foreach ($change in $decision.changes) {
        $name = [string] $change.name
        if (-not $byName.Contains($name)) {
            throw "Change decision names unknown package '$name'."
        }
        $package = $byName[$name]
        $level = [string] $change.level
        if ($package.PSObject.Properties.Name -notcontains 'anchor' -or
            $null -eq $package.anchor -or
            [string]::IsNullOrWhiteSpace([string] $package.anchor.version)) {
            throw (
                "Package '$name' has no published version anchor. " +
                'Publish the package manually first; follow ' +
                'RELEASING.md#first-publish-of-a-new-crate and complete the full procedure, ' +
                'including Trusted Publishing and any binary-release follow-up, before retrying.'
            )
        }
        try {
            $anchor = [semver] [string] $package.anchor.version
            $current = [semver] [string] $package.declared_version
        } catch {
            throw "Package '$name' has an invalid semantic version in the release-plan report."
        }
        # A prerelease version orders below the release it precedes, so the component comparison
        # that derives a Cargo increment level from a minimum version cannot express "drop the
        # prerelease suffix". Rejecting the input keeps a wrong level from being generated
        # silently; every published package in this workspace declares a release version.
        if (-not [string]::IsNullOrEmpty($anchor.PreReleaseLabel) -or
            -not [string]::IsNullOrEmpty($current.PreReleaseLabel)) {
            throw "Package '$name' declares a prerelease version, which this plan generator does not support."
        }
        $minimum = Get-MinimumVersionForChange -Anchor $anchor -Level $level
        if ($current -ge $minimum) {
            Write-Verbose (
                "Decision for package '$name' at semantic level '$level' is not emitted " +
                "because declared version '$current' already satisfies the minimum version " +
                "'$minimum' derived from anchor '$anchor'."
            ) -Verbose
            continue
        }
        $cargoLevel = Get-CargoIncrementLevel -Current $current -Minimum $minimum
        Write-Verbose (
            "Decision for package '$name' at semantic level '$level' is emitted as " +
            "cargo-release-plan '$cargoLevel' because declared version '$current' is below " +
            "the minimum version '$minimum' derived from anchor '$anchor'."
        ) -Verbose
        $increment.Add((Get-PlanIncrement -Name $name -Level $cargoLevel))
    }

    # Every version group has to end up on one version, and expansion is plan-driven: a group
    # moves only when an entry names one of its members. The decisions above can easily leave a
    # drifted group unnamed, because no member's content changed or because the decided level was
    # already covered by a pending increment, so the groups no entry reaches are realigned here.
    # Leaving this to a decision instead would make an inconsistent group unrecoverable exactly
    # when its decision is skipped as already sufficient.
    # Ref: packages/cargo-release-plan/docs/design.md, "Version groups".
    $planned = [System.Collections.Generic.HashSet[string]]::new(
        [System.StringComparer]::Ordinal
    )
    foreach ($entry in $increment) {
        [void] $planned.Add((Get-DecisionKey -ByName $byName -Name ([string] $entry.name)))
    }
    foreach ($group in @($report.groups.PSObject.Properties | Sort-Object -Property Name)) {
        if ($group.Value.PSObject.Properties.Name -notcontains 'consistent' -or
            $group.Value.consistent -or
            $planned.Contains($group.Name)) {
            continue
        }
        $increment.Add((Get-GroupAlignmentIncrement -Name $group.Name -Group $group.Value `
                    -ByName $byName -Report $report -Increment $increment))
    }

    if ($PSCmdlet.ShouldProcess($PlanPath, 'write generated cargo-release-plan input')) {
        # The proposed plan contract contains top-level metadata and increment entries.
        $releasePlanInputJsonDepth = 4
        $parent = Split-Path -Parent $PlanPath
        if (-not [string]::IsNullOrWhiteSpace($parent)) {
            New-Item -ItemType Directory -Path $parent -Force | Out-Null
        }
        [ordered]@{
            schema_version = $script:ReleasePlanSchemaVersion
            increments     = @($increment)
        } | ConvertTo-Json -Depth $releasePlanInputJsonDepth |
            Set-Content -LiteralPath $PlanPath -Encoding utf8
        Write-Host "Wrote cargo-release-plan input to '$PlanPath'."
    }
}

Export-ModuleMember -Function `
    Invoke-ValidateVersions, `
    Invoke-ReleaseReport, `
    Invoke-SemverCheck, `
    Get-ReleasePlanAnalysisBatchJson, `
    Assert-IncrementPackagePublished, `
    New-ReleasePlanFile, `
    Invoke-ExpandReleasePlan, `
    Invoke-ApplyReleasePlan
