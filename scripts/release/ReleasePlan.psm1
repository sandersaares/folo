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
$script:ReleasePlanSchemaVersion = [long] 3

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
    if ($field -notcontains 'non_publishable_packages' -or
        $report.non_publishable_packages -isnot [System.Array]) {
        throw "release-plan report at '$ReportPath' non_publishable_packages must be an array."
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

    foreach ($package in $report.non_publishable_packages) {
        if ($null -eq $package) {
            throw "release-plan report at '$ReportPath' contains a null non-publishable package."
        }
        $packageField = @($package.PSObject.Properties.Name)
        foreach ($required in @('name', 'declared_version')) {
            if ($packageField -notcontains $required) {
                throw "release-plan report at '$ReportPath' non-publishable package is missing $required."
            }
        }
        $name = [string] $package.name
        if ([string]::IsNullOrWhiteSpace($name) -or -not $seen.Add($name)) {
            throw "release-plan report at '$ReportPath' contains an empty or duplicate package name."
        }
        if ([string]::IsNullOrWhiteSpace([string] $package.declared_version)) {
            throw "release-plan report at '$ReportPath' package '$name' has no declared_version."
        }
    }

    $byName = [System.Collections.Generic.Dictionary[string, object]]::new(
        [System.StringComparer]::Ordinal
    )
    foreach ($package in @($report.packages) + @($report.non_publishable_packages)) {
        $byName.Add([string] $package.name, $package)
    }
    $groupByMember = [System.Collections.Generic.Dictionary[string, string]]::new(
        [System.StringComparer]::Ordinal
    )
    foreach ($group in $report.groups.PSObject.Properties) {
        if ($null -eq $group.Value -or
            $group.Value.PSObject.Properties.Name -notcontains 'members' -or
            $group.Value.members -isnot [System.Array]) {
            throw "release-plan report at '$ReportPath' group '$($group.Name)' members must be an array."
        }
        if ($group.Value.PSObject.Properties.Name -notcontains 'consistent' -or
            $group.Value.consistent -isnot [bool]) {
            throw "release-plan report at '$ReportPath' group '$($group.Name)' consistent must be a Boolean."
        }
        if ($group.Value.PSObject.Properties.Name -notcontains 'version' -or
            [string]::IsNullOrWhiteSpace([string] $group.Value.version)) {
            throw "release-plan report at '$ReportPath' group '$($group.Name)' has no version."
        }

        $members = @($group.Value.members | ForEach-Object { [string] $_ })
        if ($members.Count -lt 2 -or
            @($members | Where-Object { [string]::IsNullOrWhiteSpace($_) }).Count -gt 0) {
            throw "release-plan report at '$ReportPath' group '$($group.Name)' must contain at least two non-empty members."
        }
        $sortedMembers = [string[]] @($members)
        [Array]::Sort($sortedMembers, [StringComparer]::Ordinal)
        if ($group.Name -cne $sortedMembers[0]) {
            throw "release-plan report at '$ReportPath' group '$($group.Name)' is not keyed by its smallest member '$($sortedMembers[0])'."
        }
        $uniqueMembers = [System.Collections.Generic.HashSet[string]]::new(
            [System.StringComparer]::Ordinal
        )
        for ($index = 0; $index -lt $members.Count; $index++) {
            if (-not $uniqueMembers.Add($members[$index]) -or
                $members[$index] -cne $sortedMembers[$index]) {
                throw "release-plan report at '$ReportPath' group '$($group.Name)' members must be unique and ordinally sorted."
            }
        }
        foreach ($member in $members) {
            if (-not $byName.ContainsKey($member)) {
                throw "release-plan report at '$ReportPath' group '$($group.Name)' names missing package '$member'."
            }
            if ($groupByMember.ContainsKey($member)) {
                throw "release-plan report at '$ReportPath' package '$member' belongs to more than one group."
            }
            $package = $byName[$member]
            if ($package.PSObject.Properties.Name -notcontains 'group' -or
                [string]::IsNullOrWhiteSpace([string] $package.group) -or
                [string] $package.group -cne $group.Name) {
                throw "release-plan report at '$ReportPath' group '$($group.Name)' disagrees with package '$member' group reference."
            }
            $groupByMember.Add($member, $group.Name)
        }
    }

    foreach ($package in $byName.Values) {
        if ($package.PSObject.Properties.Name -notcontains 'group') {
            continue
        }
        $groupName = [string] $package.group
        if ([string]::IsNullOrWhiteSpace($groupName) -or
            -not $groupByMember.ContainsKey([string] $package.name) -or
            [string] $groupByMember[[string] $package.name] -cne $groupName) {
            throw "release-plan report at '$ReportPath' package '$($package.name)' has an invalid group reference '$groupName'."
        }
    }

    return $report
}

function Get-PackageByName {
    param(
        [Parameter(Mandatory)] $Report
    )

    $byName = [System.Collections.Specialized.OrderedDictionary]::new(
        [System.StringComparer]::Ordinal
    )
    foreach ($package in $Report.packages) {
        $byName[[string] $package.name] = $package
    }
    return $byName
}

function Get-VersionTargetByName {
    # Every tracked package whose declared version can be set by a plan. Release assessment
    # records and alignment-only records deliberately retain their different shapes.
    param(
        [Parameter(Mandatory)] $Report
    )

    $byName = [System.Collections.Specialized.OrderedDictionary]::new(
        [System.StringComparer]::Ordinal
    )
    foreach ($package in @($Report.packages) + @($Report.non_publishable_packages)) {
        $byName[[string] $package.name] = $package
    }
    return $byName
}

function Get-ReleasePlanPackageAnchor {
    # Returns the version-anchor commit for each requested package in a release-plan report.
    # Binary-release recovery uses this commit so a reconstructed tag identifies the source
    # revision that introduced the published version rather than whichever main commit happens
    # to trigger recovery later.
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string] $ReportPath,
        [Parameter(Mandatory)][string[]] $Name
    )

    $report = Read-ReleasePlanReport -ReportPath $ReportPath
    $byName = Get-PackageByName -Report $report
    foreach ($packageName in $Name) {
        $package = $byName[$packageName]
        if ($null -eq $package) {
            throw "release-plan report at '$ReportPath' does not contain package '$packageName'."
        }
        $packageField = @($package.PSObject.Properties.Name)
        if ($packageField -notcontains 'anchor' -or
            $null -eq $package.anchor -or
            $package.anchor.PSObject.Properties.Name -notcontains 'commit' -or
            [string]::IsNullOrWhiteSpace([string] $package.anchor.commit)) {
            throw (
                "release-plan report at '$ReportPath' package '$packageName' has no version " +
                'anchor commit.'
            )
        }
        [pscustomobject]@{
            Name   = $packageName
            Commit = [string] $package.anchor.commit
        }
    }
}

function Test-PackageIsConsumerContract {
    # Whether the named package presents a library API contract to consumers.
    #
    # Read from the report, which derives it from the package's own manifest, so adding a package
    # to the workspace needs no edit here. A name the report does not carry is not a publishable
    # package of this workspace and has no contract to assess.
    # Ref: packages/cargo-release-plan/README.md, "Plan and report schema".
    param(
        [Parameter(Mandatory)] $Report,
        [Parameter(Mandatory)][string] $Name
    )

    foreach ($package in $Report.packages) {
        if ([string] $package.name -cne $Name) {
            continue
        }
        if ($package.PSObject.Properties.Name -notcontains 'consumer_contract') {
            throw "release-plan report package '$Name' is missing the consumer_contract field."
        }
        return [bool] $package.consumer_contract
    }
    return $false
}

function Get-AffectedSemverCheckTarget {
    # Returns the consumer-contract targets affected by a release-plan report. A well-formed
    # report with no selected target is a valid empty result.
    #
    # Which packages present a consumer contract is declared by each package and carried in the
    # report, never listed here: a list beside the workspace goes stale silently, and the failure
    # is a package that quietly stops being checked.
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
                Where-Object { Test-PackageIsConsumerContract -Report $report -Name $_ } |
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
                "$candidateText, none of which declares a consumer contract, so no " +
                "cargo-semver-checks target is emitted."
            ) -Verbose
            continue
        }

        foreach ($target in $supportedTargets) {
            if ($selectedTargets.Add($target)) {
                if ($null -ne $groupName) {
                    Write-Verbose (
                        "Changed package '$packageName' belongs to version group '$groupName'; " +
                        "consumer-contract target '$target' is emitted because group " +
                        "members are checked through the public package contract."
                    ) -Verbose
                } else {
                    Write-Verbose (
                        "Changed package '$packageName' is emitted as cargo-semver-checks " +
                        "target '$target' because its manifest does not declare a private API."
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

function Get-SemverCheckTargetDirectory {
    # cargo-semver-checks nests generated placeholder workspaces below Cargo's target directory.
    # Keep the Windows target root independent of the checkout depth so MSVC tools do not encounter
    # the legacy MAX_PATH boundary. A workspace-specific name avoids collisions between worktrees.
    # Reassess this override when cargo-semver-checks shortens those generated paths:
    # https://github.com/obi1kenobi/cargo-semver-checks/issues/1725
    [CmdletBinding()]
    [OutputType([string])]
    param(
        [string] $WorkspaceRoot = (Resolve-Path (Join-Path $PSScriptRoot '../..')).Path,
        [string] $TempRoot = [IO.Path]::GetTempPath()
    )

    $workspacePath = [IO.Path]::GetFullPath($WorkspaceRoot)
    $hash = [Security.Cryptography.SHA256]::HashData(
        [Text.Encoding]::UTF8.GetBytes($workspacePath)
    )
    # Local cache names need collision resistance, not a full cryptographic identity; preserving
    # path budget is the purpose of this directory.
    $workspaceIdLength = 16
    $workspaceId = [Convert]::ToHexString($hash).Substring(0, $workspaceIdLength).ToLowerInvariant()
    $candidate = Join-Path ([IO.Path]::GetFullPath($TempRoot)) "fsc-$workspaceId"

    $configured = [Environment]::GetEnvironmentVariable('CARGO_TARGET_DIR', 'Process')
    if (-not [string]::IsNullOrWhiteSpace($configured)) {
        $configuredPath = [IO.Path]::GetFullPath($configured)
        if ($configuredPath.Length -lt $candidate.Length) {
            return $configuredPath
        }
    }
    return $candidate
}

function Invoke-WithSemverCheckTargetDirectory {
    # Scopes the short target directory to commands that run cargo-semver-checks. Other Cargo
    # commands keep the workspace target directory and its normal cache behavior.
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][scriptblock] $Action,
        [AllowNull()][string] $TargetDirectory
    )

    if (-not $PSBoundParameters.ContainsKey('TargetDirectory') -and $IsWindows) {
        $TargetDirectory = Get-SemverCheckTargetDirectory
    }
    if ([string]::IsNullOrWhiteSpace($TargetDirectory)) {
        & $Action
        return
    }

    $previousTargetDirectory =
        [Environment]::GetEnvironmentVariable('CARGO_TARGET_DIR', 'Process')
    try {
        Write-Verbose (
            "Using CARGO_TARGET_DIR=$TargetDirectory for cargo-semver-checks because its " +
            'generated build paths can exceed the Windows path limit.'
        ) -Verbose
        [Environment]::SetEnvironmentVariable(
            'CARGO_TARGET_DIR',
            $TargetDirectory,
            'Process'
        )
        & $Action
    } finally {
        # The environment provider removes a null value. Binding null to the .NET string
        # overload can instead leave an empty variable, which Cargo rejects.
        $env:CARGO_TARGET_DIR = $previousTargetDirectory
    }
}

function Invoke-SemverCheckCargo {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string[]] $Argument,
        [scriptblock] $Cargo = { param([string[]] $CargoArgument) & cargo @CargoArgument },
        [AllowNull()][string] $TargetDirectory
    )

    $action = { & $Cargo $Argument }
    if ($PSBoundParameters.ContainsKey('TargetDirectory')) {
        Invoke-WithSemverCheckTargetDirectory `
            -Action $action `
            -TargetDirectory $TargetDirectory
    } else {
        Invoke-WithSemverCheckTargetDirectory -Action $action
    }
}

function Invoke-VerifySemverCheck {
    # Proves the installed tool can build rustdoc JSON before release decisions trust its output.
    [CmdletBinding()]
    param(
        [scriptblock] $Cargo = { param([string[]] $Argument) & cargo @Argument }
    )

    # A tiny published crate keeps the canary cheap. `--baseline-rev` fixes only the baseline, so
    # the candidate still comes from the work tree; the two sides agree exactly when the work tree
    # has not edited this package, which is why a package unrelated to release tooling is chosen.
    $package = 'folo_utils'
    Write-Verbose (
        "Verifying that cargo-semver-checks can run (canary package '$package', " +
        'compared against its own HEAD).'
    ) -Verbose

    try {
        Invoke-SemverCheckCargo `
            -Argument @('semver-checks', '--baseline-rev', 'HEAD', '-p', $package) `
            -Cargo $Cargo
    } catch {
        Write-Host ''
        Write-Host (
            "ERROR: cargo-semver-checks failed to run on the canary package '$package' " +
            '(see the error above).'
        ) -ForegroundColor Red
        Write-Host ''
        Write-Host (
            'A broken cargo-semver-checks must not be interpreted as an absence of ' +
            'breaking changes.'
        ) -ForegroundColor Red
        Write-Host (
            "If this work tree has edited '$package', this may instead be a genuine finding " +
            'against its own HEAD baseline; check that before concluding the tool is broken.'
        ) -ForegroundColor Red
        Write-Host (
            "Otherwise update the tool with 'cargo install cargo-semver-checks --locked' " +
            "(or 'just install-tools'), then re-run the command."
        ) -ForegroundColor Red
        throw
    }
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
        Invoke-SemverCheckCargo -Argument $argument -Cargo $Cargo 2>&1 |
            Tee-Object -FilePath $logPath
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
    Invoke-SemverCheckCargo -Argument $argument -Cargo $Cargo
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

    $batch = @(Get-ReleasePlanAnalysisBatch -ReportPath $ReportPath)
    return ConvertTo-Json -InputObject $batch -Depth $analysisBatchJsonDepth
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
    # Fails unless every publishable package that apply would reach has a confirmed crates.io
    # publication. Current tracked workspace membership decides publication eligibility; an
    # absent name is not interpreted as a non-publishable helper.
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string] $ExpandedPath,
        [string] $ManifestPath,
        [scriptblock] $GetWorkspaceMember = {
            param([AllowNull()][string] $SelectedManifestPath)
            Get-TrackedWorkspaceMember -ManifestPath $SelectedManifestPath
        },
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
    $workspaceMemberByName = [System.Collections.Generic.Dictionary[string, object]]::new(
        [System.StringComparer]::Ordinal
    )
    foreach ($member in @(& $GetWorkspaceMember $ManifestPath)) {
        $name = [string] $member.Name
        if ([string]::IsNullOrWhiteSpace($name) -or
            $workspaceMemberByName.ContainsKey($name)) {
            throw 'Current tracked workspace membership contains an empty or duplicate package name.'
        }
        $workspaceMemberByName.Add($name, $member)
    }

    $neverPublished = [System.Collections.Generic.List[string]]::new()
    $unknown = [System.Collections.Generic.List[string]]::new()
    foreach ($name in $packageNames) {
        if (-not $workspaceMemberByName.ContainsKey($name)) {
            throw "Expanded plan target '$name' is not a current Git-tracked workspace member."
        }
        $member = $workspaceMemberByName[$name]
        if ($member.PSObject.Properties.Name -notcontains 'Publishable') {
            throw "Current workspace member '$name' is missing publication eligibility."
        }
        if (-not [bool] $member.Publishable) {
            continue
        }
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
    Write-Host 'Every publishable package the expanded plan names is already published.'
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

function Get-VersionCompatibilityKey {
    # The part of a version that must agree for two versions to be semver-compatible.
    #
    # Cargo treats the leftmost non-zero component as the major component, so a 0.y.z release
    # breaks on its minor component and a 0.0.z release breaks on every increment.
    param(
        [Parameter(Mandatory)][semver] $Version
    )

    if ($Version.Major -gt 0) {
        return "$($Version.Major).x.x"
    }
    if ($Version.Minor -gt 0) {
        return "0.$($Version.Minor).x"
    }
    return "0.0.$($Version.Patch)"
}

function Get-IncrementedVersion {
    # A Cargo increment level applied to a version, mirroring the tool's `increment_version`.
    #
    # This is a plain component bump of the version it is given, with no reference to any
    # anchor: the tool resolves a group by applying the level to the group's highest declared
    # version, so predicting the outcome requires the same operation rather than a repeat of the
    # anchor arithmetic that chose the level.
    param(
        [Parameter(Mandatory)][semver] $Version,
        [Parameter(Mandatory)][string] $Level
    )

    switch -CaseSensitive ($Level) {
        'major' { return [semver]::new($Version.Major + 1, 0, 0) }
        'minor' { return [semver]::new($Version.Major, $Version.Minor + 1, 0) }
        'patch' { return [semver]::new($Version.Major, $Version.Minor, $Version.Patch + 1) }
        default { throw "Unsupported Cargo increment level '$Level'." }
    }
}

function Get-CargoIncrementLevelRank {
    # Orders Cargo increment levels so the highest among a group's decisions can be selected,
    # which is how the tool combines them.
    param(
        [Parameter(Mandatory)][string] $Level
    )

    switch -CaseSensitive ($Level) {
        'major' { return 3 }
        'minor' { return 2 }
        'patch' { return 1 }
        default { throw "Unsupported Cargo increment level '$Level'." }
    }
}

function Get-ResolvedVersionForPackage {
    # The version this package ends the plan declaring.
    #
    # Mirrors the tool's resolution rather than re-deriving it from anchors: a group's target
    # starts from the highest version any member declares and applies the highest level any
    # member's decision contributes. Those are different numbers whenever a member lags behind
    # the group, and reading the target off a lagging member's own anchor would under-predict
    # the result for every other member.
    #
    # A realignment entry is one of those contributions. Aligning a drifted group exactly leaves
    # the leading member where it is, but realignment can instead patch-increment the whole
    # group, which moves the leader as well; on a 0.0.z line that increment is itself breaking.
    # Ref: packages/cargo-release-plan/docs/design.md, "Version groups".
    [OutputType([semver])]
    param(
        [Parameter(Mandatory)] $Package,
        [Parameter(Mandatory)] $Report,
        [Parameter(Mandatory)] $ReleaseByName,
        [Parameter(Mandatory)] $TargetByName,
        [Parameter(Mandatory)] $LevelByName,
        [Parameter(Mandatory)] $Alignment
    )

    try {
        $base = [semver] [string] $Package.declared_version
    } catch {
        throw "Package '$($Package.name)' has an invalid semantic version in the release-plan report."
    }
    $rank = 0
    $level = ''

    foreach ($member in (Get-VersionGroupMemberName -Report $Report -TargetByName $TargetByName `
                -Name ([string] $Package.name))) {
        if (-not $TargetByName.Contains($member)) {
            throw "Version group for '$($Package.name)' names missing package '$member'."
        }
        $memberTarget = $TargetByName[$member]
        try {
            $declared = [semver] [string] $memberTarget.declared_version
        } catch {
            throw "Package '$member' has an invalid semantic version in the release-plan report."
        }
        if ($declared -gt $base) {
            $base = $declared
        }

        $semanticLevel = if ($LevelByName.Contains($member)) {
            [string] $LevelByName[$member]
        } else {
            ''
        }
        if ([string]::IsNullOrWhiteSpace($semanticLevel)) {
            continue
        }
        if (-not $ReleaseByName.Contains($member)) {
            throw "Change-level state unexpectedly names non-publishable package '$member'."
        }
        $memberPackage = $ReleaseByName[$member]
        if ($memberPackage.PSObject.Properties.Name -notcontains 'anchor' -or
            $null -eq $memberPackage.anchor -or
            [string]::IsNullOrWhiteSpace([string] $memberPackage.anchor.version)) {
            continue
        }
        $minimum = Get-MinimumVersionForChange `
            -Anchor ([semver] [string] $memberPackage.anchor.version) -Level $semanticLevel
        # The generator drops a decision the declared version already satisfies, so it
        # contributes no level to the group.
        if ($declared -ge $minimum) {
            continue
        }
        $memberLevel = Get-CargoIncrementLevel -Current $declared -Minimum $minimum
        $memberRank = Get-CargoIncrementLevelRank -Level $memberLevel
        if ($memberRank -gt $rank) {
            $rank = $memberRank
            $level = $memberLevel
        }
    }

    $alignmentEntry = $Alignment[
        (Get-DecisionKey -TargetByName $TargetByName -Name ([string] $Package.name))
    ]
    if ($null -ne $alignmentEntry) {
        if ($alignmentEntry.Contains('version')) {
            $aligned = [semver] [string] $alignmentEntry['version']
            if ($aligned -gt $base) {
                $base = $aligned
            }
        } else {
            $alignmentRank = Get-CargoIncrementLevelRank -Level ([string] $alignmentEntry['level'])
            if ($alignmentRank -gt $rank) {
                $rank = $alignmentRank
                $level = [string] $alignmentEntry['level']
            }
        }
    }

    if ([string]::IsNullOrWhiteSpace($level)) {
        return $base
    }
    return Get-IncrementedVersion -Version $base -Level $level
}

function Test-PackageReleasesBreakingChange {
    # Whether the package ends the plan declaring a version incompatible with its last release.
    #
    # Asked of the resolved outcome rather than of the decided level, because a package whose
    # version was already raised in an earlier pull request releases a breaking change while
    # carrying no decision now, and a decision already covered by a pending increment still does.
    #
    # A grouped package is asked about its whole group. Members release as one version, so a
    # decision naming any member moves them all, and asking only about this package's own
    # declared version and decision would miss a sibling's breaking decision entirely.
    param(
        [Parameter(Mandatory)] $Package,
        [Parameter(Mandatory)] $Report,
        [Parameter(Mandatory)] $ReleaseByName,
        [Parameter(Mandatory)] $TargetByName,
        [Parameter(Mandatory)] $LevelByName,
        [Parameter(Mandatory)] $Alignment
    )

    if ($Package.PSObject.Properties.Name -notcontains 'anchor' -or
        $null -eq $Package.anchor -or
        [string]::IsNullOrWhiteSpace([string] $Package.anchor.version)) {
        # Never released, so there is no consumer contract to break.
        return $false
    }
    try {
        $anchor = [semver] [string] $Package.anchor.version
    } catch {
        throw "Package '$($Package.name)' has an invalid semantic version in the release-plan report."
    }

    $resolved = Get-ResolvedVersionForPackage -Package $Package -Report $Report `
        -ReleaseByName $ReleaseByName -TargetByName $TargetByName `
        -LevelByName $LevelByName -Alignment $Alignment

    return (Get-VersionCompatibilityKey -Version $anchor) -cne
        (Get-VersionCompatibilityKey -Version $resolved)
}

function Get-VersionGroupMemberName {
    # The packages whose declared versions resolve together with $Name, including $Name itself.
    [OutputType([string[]])]
    param(
        [Parameter(Mandatory)] $Report,
        [Parameter(Mandatory)] $TargetByName,
        [Parameter(Mandatory)][string] $Name
    )

    $key = Get-DecisionKey -TargetByName $TargetByName -Name $Name
    $group = $Report.groups.PSObject.Properties[$key]
    if ($null -eq $group) {
        return , @($Name)
    }
    return , @($group.Value.members | ForEach-Object { [string] $_ })
}

function Get-ChangeLevelWithPublicDependency {
    # The decided change levels, plus `breaking` for every package whose public API exposes a
    # dependency that releases a breaking change.
    #
    # An incompatible release changes the identity of the exposed types, so a consumer holding
    # the older dependency can no longer hand its values across. That makes the dependent's own
    # contract incompatible however unrelated the dependency's breaking change was to the items
    # it exposes, which is mechanics rather than judgement and so is decided here.
    # `validate-versions` rejects a tree that violates this, so a plan skipping it would be
    # generated only to fail verification.
    # Ref: packages/cargo-release-plan/docs/design.md, "Public dependencies".
    [OutputType([System.Collections.IDictionary])]
    param(
        [Parameter(Mandatory)] $Report,
        [Parameter(Mandatory)] $ReleaseByName,
        [Parameter(Mandatory)] $TargetByName,
        [Parameter(Mandatory)] $Decision,
        [Parameter(Mandatory)] $Alignment
    )

    $level = [ordered]@{}
    foreach ($change in $Decision.changes) {
        $level[[string] $change.name] = [string] $change.level
    }

    # Raising one package can make its own dependents incompatible in turn, so the answers are
    # recomputed until they stop changing. A package only ever moves to `breaking` and never
    # back, so this settles within one pass per package; the bound is asserted rather than
    # assumed so a future change cannot spin here forever.
    $remainingPass = @($Report.packages).Count + 1
    $settled = $false
    while (-not $settled) {
        if ($remainingPass -le 0) {
            throw 'Public-dependency breaking-change propagation did not settle; this is a defect in the plan generator.'
        }
        $remainingPass--
        $settled = $true
        foreach ($package in $Report.packages) {
            $name = [string] $package.name
            if ($package.PSObject.Properties.Name -notcontains 'anchor' -or
                $null -eq $package.anchor -or
                [string]::IsNullOrWhiteSpace([string] $package.anchor.version)) {
                # A package with no release cannot take an increment and has no contract to
                # break. It follows the first-publication path instead.
                continue
            }
            if (Test-PackageReleasesBreakingChange -Package $package -Report $Report `
                    -ReleaseByName $ReleaseByName -TargetByName $TargetByName `
                    -LevelByName $level -Alignment $Alignment) {
                continue
            }
            if ($package.PSObject.Properties.Name -notcontains 'dependencies') {
                continue
            }
            foreach ($dependency in $package.dependencies) {
                if ($dependency.PSObject.Properties.Name -notcontains 'public' -or
                    -not $dependency.public) {
                    continue
                }
                $dependencyName = [string] $dependency.name
                if (-not $ReleaseByName.Contains($dependencyName)) {
                    continue
                }
                if (-not (Test-PackageReleasesBreakingChange `
                            -Package $ReleaseByName[$dependencyName] `
                            -Report $Report -ReleaseByName $ReleaseByName `
                            -TargetByName $TargetByName -LevelByName $level `
                            -Alignment $Alignment)) {
                    continue
                }
                Write-Verbose (
                    "Package '$name' is raised to change level 'breaking' because its public API " +
                    "exposes '$dependencyName', which releases a version incompatible with its " +
                    "anchor '$($ReleaseByName[$dependencyName].anchor.version)'. A consumer holding the " +
                    'older dependency can no longer hand its types across.'
                ) -Verbose
                $level[$name] = 'breaking'
                $settled = $false
                break
            }
        }
    }

    return $level
}

function Get-DecisionIncrement {
    # The plan entries the change levels produce, before any group realignment.
    #
    # A decision the declared version already satisfies is dropped rather than emitted, because
    # the existing increment already covers it.
    [OutputType([System.Collections.Generic.List[object]])]
    param(
        [Parameter(Mandatory)] $ReleaseByName,
        [Parameter(Mandatory)] $LevelByName,
        [switch] $Explain
    )

    $increment = [System.Collections.Generic.List[object]]::new()
    foreach ($entry in $LevelByName.GetEnumerator()) {
        $name = [string] $entry.Key
        if (-not $ReleaseByName.Contains($name)) {
            throw "Change decision names unknown or non-publishable package '$name'."
        }
        $package = $ReleaseByName[$name]
        $level = [string] $entry.Value
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
            if ($Explain) {
                Write-Verbose (
                    "Decision for package '$name' at semantic level '$level' is not emitted " +
                    "because declared version '$current' already satisfies the minimum version " +
                    "'$minimum' derived from anchor '$anchor'."
                ) -Verbose
            }
            continue
        }
        $cargoLevel = Get-CargoIncrementLevel -Current $current -Minimum $minimum
        if ($Explain) {
            Write-Verbose (
                "Decision for package '$name' at semantic level '$level' is emitted as " +
                "cargo-release-plan '$cargoLevel' because declared version '$current' is below " +
                "the minimum version '$minimum' derived from anchor '$anchor'."
            ) -Verbose
        }
        $increment.Add((Get-PlanIncrement -Name $name -Level $cargoLevel))
    }
    return , $increment
}

function Get-GroupVersionState {
    # Computes the highest declared version and whether it can be emitted as an exact group
    # target. Build metadata does not affect precedence, so a non-plain member tied with a plain
    # highest member still requires a patch increment rather than an exact target.
    param(
        [Parameter(Mandatory)][string] $Name,
        [Parameter(Mandatory)] $Group,
        [Parameter(Mandatory)] $TargetByName
    )

    $version = [System.Collections.Generic.HashSet[string]]::new(
        [System.StringComparer]::Ordinal
    )
    $highest = $null
    $highestHasNonPlainMember = $false
    foreach ($groupMember in $Group.members) {
        $memberName = [string] $groupMember
        if (-not $TargetByName.Contains($memberName)) {
            throw "Version group '$Name' names missing package '$memberName'."
        }
        $declaredText = [string] $TargetByName[$memberName].declared_version
        [void] $version.Add($declaredText)
        try {
            $declared = [semver] $declaredText
        } catch {
            throw "Package '$memberName' has an invalid semantic version in the release-plan report."
        }
        $declaredIsNonPlain =
            -not [string]::IsNullOrEmpty($declared.PreReleaseLabel) -or
            -not [string]::IsNullOrEmpty($declared.BuildLabel)
        if ($null -eq $highest -or $declared -gt $highest) {
            $highest = $declared
            $highestHasNonPlainMember = $declaredIsNonPlain
        } elseif ($declared -eq $highest -and $declaredIsNonPlain) {
            $highestHasNonPlainMember = $true
        }
    }
    if ($null -eq $highest) {
        throw "Group '$Name' has no declared version to align its members on."
    }

    return [pscustomobject]@{
        Highest                  = $highest
        HighestHasNonPlainMember = $highestHasNonPlainMember
        DistinctVersionCount     = $version.Count
    }
}

function Get-GroupAlignment {
    # The realignment entry for every version-misaligned group no plan entry already reaches.
    #
    # Every version group has to end up on one version, and expansion is plan-driven: a group
    # moves only when an entry names one of its members. The decisions can easily leave a drifted
    # group unnamed, because no member's content changed or because the decided level was already
    # covered by a pending increment, so the groups no entry reaches are realigned here. Leaving
    # this to a decision instead would make a misaligned group unrecoverable exactly when its
    # decision is skipped as already sufficient.
    # Ref: packages/cargo-release-plan/docs/design.md, "Version groups".
    [OutputType([System.Collections.IDictionary])]
    param(
        [Parameter(Mandatory)] $Report,
        [Parameter(Mandatory)] $ReleaseByName,
        [Parameter(Mandatory)] $TargetByName,
        [Parameter(Mandatory)][AllowEmptyCollection()][System.Collections.IDictionary[]] $Increment
    )

    $planned = [System.Collections.Generic.HashSet[string]]::new(
        [System.StringComparer]::Ordinal
    )
    foreach ($entry in $Increment) {
        [void] $planned.Add(
            (Get-DecisionKey -TargetByName $TargetByName -Name ([string] $entry['name']))
        )
    }
    $unaligned = [System.Collections.Generic.List[object]]::new()
    foreach ($group in $Report.groups.PSObject.Properties | Sort-Object -Property Name) {
        if ($planned.Contains($group.Name)) {
            continue
        }
        $state = Get-GroupVersionState -Name $group.Name -Group $group.Value `
            -TargetByName $TargetByName
        if ($state.DistinctVersionCount -gt 1 -or $state.HighestHasNonPlainMember) {
            $unaligned.Add($group)
        }
    }

    # Each group's alignment is decided against everything else the plan does, and deciding one
    # group can move packages that change another group's answer. Deciding them in one pass would
    # make the outcome depend on the order groups happen to be visited, so the answers are
    # recomputed until they stop changing. A group only ever moves from exact alignment to an
    # increment and never back, so this settles within one pass per realigned group; the bound is
    # asserted rather than assumed so a future change cannot spin here forever.
    $alignment = [ordered]@{}
    $remainingPass = $unaligned.Count + 1
    $settled = $false
    while (-not $settled) {
        if ($remainingPass -le 0) {
            throw 'Version-group realignment did not settle; this is a defect in the plan generator.'
        }
        $remainingPass--
        $settled = $true
        foreach ($group in $unaligned) {
            # Every other entry, so a group's own answer is never an input to itself.
            $context = [System.Collections.Generic.List[object]]::new()
            $context.AddRange([object[]] @($Increment))
            foreach ($decided in $alignment.GetEnumerator()) {
                if ($decided.Key -cne $group.Name) {
                    $context.Add($decided.Value)
                }
            }

            $fresh = Get-GroupAlignmentIncrement -Name $group.Name -Group $group.Value `
                -ReleaseByName $ReleaseByName -TargetByName $TargetByName `
                -Report $Report `
                -Increment ([System.Collections.IDictionary[]] $context.ToArray())
            $current = $alignment[$group.Name]
            if ($null -eq $current -or
                [string] $current['level'] -cne [string] $fresh['level'] -or
                [string] $current['version'] -cne [string] $fresh['version']) {
                $alignment[$group.Name] = $fresh
                $settled = $false
            }
        }
    }
    return $alignment
}

function Get-AlignmentSignature {
    # A comparable form of the realignment decisions, for detecting a settled state.
    [OutputType([System.Collections.IDictionary])]
    param(
        [Parameter(Mandatory)] $Alignment
    )

    $signature = [ordered]@{}
    foreach ($entry in $Alignment.GetEnumerator()) {
        $signature[[string] $entry.Key] =
            "$($entry.Value['level'])|$($entry.Value['version'])"
    }
    return $signature
}

function Test-PlanStateSettled {
    # Whether two rounds of level and alignment decisions agree.
    param(
        [Parameter(Mandatory)] $Left,
        [Parameter(Mandatory)] $Right
    )

    if ($Left.Keys.Count -ne $Right.Keys.Count) {
        return $false
    }
    foreach ($key in $Left.Keys) {
        if (-not $Right.Contains($key)) {
            return $false
        }
        if ([string] $Left[$key] -cne [string] $Right[$key]) {
            return $false
        }
    }
    return $true
}
function Get-DecisionKey {
    # The key a plan entry folds onto: the target's version group when it has one, otherwise the
    # target itself. The lookup includes alignment-only packages because the smallest group member
    # can be non-publishable.
    param(
        [Parameter(Mandatory)] $TargetByName,
        [Parameter(Mandatory)][string] $Name
    )

    if (-not $TargetByName.Contains($Name)) {
        return $Name
    }
    $package = $TargetByName[$Name]
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
        [Parameter(Mandatory)] $TargetByName,
        [Parameter(Mandatory)][AllowEmptyCollection()][System.Collections.IDictionary[]] $Increment
    )

    $moved = [System.Collections.Generic.HashSet[string]]::new(
        [System.StringComparer]::Ordinal
    )
    foreach ($entry in $Increment) {
        # An entry names whatever the decision named, which for a grouped package is usually the
        # package rather than its group. Resolution folds that onto the group and moves every
        # member, so the entry name is normalized before the group is looked up; reading it
        # directly would see only the named member and miss the rest of the group.
        $key = Get-DecisionKey -TargetByName $TargetByName -Name ([string] $entry['name'])
        $reached = [System.Collections.Generic.List[string]]::new()
        $group = $Report.groups.PSObject.Properties[$key]
        if ($null -eq $group) {
            $reached.Add($key)
        } else {
            foreach ($member in $group.Value.members) {
                $reached.Add([string] $member)
            }
        }
        foreach ($packageName in $reached) {
            if (-not $TargetByName.Contains($packageName)) {
                throw "Plan entry '$key' reaches missing package '$packageName'."
            }
            # A level always raises the package; an exact version moves only those not already
            # declaring it.
            if ($entry.Contains('level') -or
                [string] $TargetByName[$packageName].declared_version -cne
                    [string] $entry['version']) {
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
    # every publishable member for no substantive change. The target is derived from every
    # member's declared version, including alignment-only helpers.
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
        [Parameter(Mandatory)] $ReleaseByName,
        [Parameter(Mandatory)] $TargetByName,
        [Parameter(Mandatory)] $Report,
        [Parameter(Mandatory)][AllowEmptyCollection()][System.Collections.IDictionary[]] $Increment
    )

    $member = [System.Collections.Generic.HashSet[string]]::new(
        [System.StringComparer]::Ordinal
    )
    $state = Get-GroupVersionState -Name $Name -Group $Group -TargetByName $TargetByName
    $highest = [semver] $state.Highest
    foreach ($groupMember in $Group.members) {
        $memberName = [string] $groupMember
        [void] $member.Add($memberName)
    }
    $target = $highest.ToString()

    $moving = [System.Collections.Generic.HashSet[string]]::new(
        [System.StringComparer]::Ordinal
    )
    $staying = [System.Collections.Generic.List[string]]::new()
    foreach ($memberName in $member) {
        if ([string] $TargetByName[$memberName].declared_version -ceq $target) {
            $staying.Add($memberName)
        } else {
            [void] $moving.Add($memberName)
        }
    }

    # Choosing between exact alignment and an increment is the only decision here. Whether the
    # resulting plan is safe is settled once for the whole plan by
    # Assert-PlanMovesEveryRewrittenPublishedPackage, because a package can be endangered by an
    # entry belonging to some other group and no per-group view can see that.
    $movedByPlan = Get-PackageMovedByIncrement -Report $Report `
        -TargetByName $TargetByName -Increment $Increment

    foreach ($memberName in $staying) {
        if (-not $ReleaseByName.Contains($memberName) -or
            -not (Test-PackageShipsPublishedVersion `
                -Package $ReleaseByName[$memberName] -Moves $false)) {
            continue
        }
        foreach ($dependency in $ReleaseByName[$memberName].dependencies) {
            $dependencyName = [string] $dependency.name
            # Both this group's own laggards and anything the rest of the plan already moves: a
            # member that keeps its version is endangered by either, and incrementing the group
            # moves it clear of both.
            if (-not $moving.Contains($dependencyName) -and
                -not $movedByPlan.Contains($dependencyName)) {
                continue
            }
            Write-Verbose (
                "Group '$Name' cannot align on version '$target' because member " +
                "'$memberName' already declares it and depends on '$dependencyName', which the " +
                'plan moves; the rewritten requirement would change released content under ' +
                "'$memberName' version '$target'. Incrementing the group instead."
            ) -Verbose
            return Get-PlanIncrement -Name $Name -Level 'patch'
        }
    }

    if ($state.HighestHasNonPlainMember) {
        Write-Verbose (
            "Group '$Name' has non-plain highest version '$target'; patch-incrementing the " +
            'group so rewritten exact requirements retain the required plain version syntax.'
        ) -Verbose
        return Get-PlanIncrement -Name $Name -Level 'patch'
    }

    Write-Verbose (
        "Group '$Name' aligns on version '$target', which its members already declare at the " +
        'highest, because no member that keeps its version depends on a package the plan moves.'
    ) -Verbose
    return Get-PlanIncrement -Name $Name -Version $target
}

function Assert-PlanMovesEveryPackageNeedingIncrement {
    # Fails when the finished plan leaves a package that the report says needs an increment
    # still declaring the version it declares today.
    #
    # `check` fails for exactly those packages, so a plan that does not move one cannot clear the
    # version check and the run would present a plan artifact that is already known not to
    # work. This asks whether the plan moves the package rather than whether a decision named it,
    # because a grouped package is moved by any decision naming one of its members and recording
    # no decision of its own is correct for it.
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] $Report,
        [Parameter(Mandatory)] $TargetByName,
        [Parameter(Mandatory)][AllowEmptyCollection()][System.Collections.IDictionary[]] $Increment
    )

    $moved = Get-PackageMovedByIncrement -Report $Report `
        -TargetByName $TargetByName -Increment $Increment
    $missing = [System.Collections.Generic.List[string]]::new()
    foreach ($package in $Report.packages) {
        $packageName = [string] $package.name
        if ([string] $package.status -cne 'needs-increment') {
            continue
        }
        if (-not $moved.Contains($packageName)) {
            $missing.Add($packageName)
        }
    }

    if ($missing.Count -eq 0) {
        return
    }
    $noun = if ($missing.Count -eq 1) { 'package needs' } else { 'packages need' }
    throw "The plan leaves $($missing.Count) $($noun) an increment without one: $($missing -join ', '). Decide a change level for each, because the version check fails until their versions move."
}

function Assert-PlanMovesEveryRewrittenPublishedPackage {
    # Fails when the finished plan would rewrite an intra-workspace requirement inside a package
    # that keeps a version crates.io already carries.
    #
    # Applying a plan rewrites the requirement of every path dependency on a package it moves, so
    # such a package would publish changed content under a version already published. This is
    # checked once over the whole plan rather than while each group is decided, because the
    # endangering entry frequently belongs to a different group and no per-group view can see it.
    # The realignment choices above avoid reaching this for packages they can move; whatever is
    # left needs a change level of its own, which is a decision rather than mechanics.
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)] $Report,
        [Parameter(Mandatory)] $ReleaseByName,
        [Parameter(Mandatory)] $TargetByName,
        [Parameter(Mandatory)][AllowEmptyCollection()][System.Collections.IDictionary[]] $Increment
    )

    $moved = Get-PackageMovedByIncrement -Report $Report `
        -TargetByName $TargetByName -Increment $Increment
    $stranded = [System.Collections.Generic.List[string]]::new()
    foreach ($packageName in $ReleaseByName.Keys) {
        if (-not (Test-PackageShipsPublishedVersion -Package $ReleaseByName[$packageName] `
                    -Moves $moved.Contains($packageName))) {
            continue
        }
        foreach ($dependency in $ReleaseByName[$packageName].dependencies) {
            if ($moved.Contains([string] $dependency.name)) {
                $stranded.Add($packageName)
                break
            }
        }
    }

    if ($stranded.Count -eq 0) {
        return
    }
    $subject = if ($stranded.Count -eq 1) {
        'a published package that keeps its current version'
    } else {
        'published packages that keep their current version'
    }
    $pronoun = if ($stranded.Count -eq 1) { 'it' } else { 'them' }
    throw "The plan rewrites a workspace requirement inside $($subject): $($stranded -join ', '). Decide a change level for $pronoun as well, so the rewritten requirement ships under a new version."
}

function New-ReleasePlanFile {
    # Writes the proposed plan: the decided change levels mapped to cargo-release-plan's
    # mechanical increment levels, plus whatever it takes to align every group whose declared
    # versions differ. Existing pending-release increments are retained and raised only when
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
    $releaseByName = Get-PackageByName -Report $report
    $targetByName = Get-VersionTargetByName -Report $report

    # Levels and realignment each depend on the other's outcome, so neither can be decided first.
    # Exposing a dependency that breaks is itself a breaking change, which needs the versions the
    # plan lands on; realigning a drifted group needs the entries the levels produce, and can
    # patch-increment a whole group, which moves a leading member that exact alignment would have
    # left alone. Deciding levels once before realignment would miss exactly that, so both are
    # recomputed until they agree. A level only ever rises and a group only ever moves from exact
    # alignment to an increment, so this settles; the bound is asserted rather than assumed.
    $alignment = [ordered]@{}
    $levelByName = [ordered]@{}
    $remainingPass = @($report.packages).Count + @($report.groups.PSObject.Properties).Count + 2
    $settled = $false
    while (-not $settled) {
        if ($remainingPass -le 0) {
            throw 'Change levels and version-group realignment did not settle; this is a defect in the plan generator.'
        }
        $remainingPass--

        $freshLevel = Get-ChangeLevelWithPublicDependency -Report $report `
            -ReleaseByName $releaseByName -TargetByName $targetByName `
            -Decision $decision -Alignment $alignment
        $freshIncrement = Get-DecisionIncrement -ReleaseByName $releaseByName `
            -LevelByName $freshLevel
        $freshAlignment = Get-GroupAlignment -Report $report `
            -ReleaseByName $releaseByName -TargetByName $targetByName `
            -Increment ([System.Collections.IDictionary[]] $freshIncrement.ToArray())

        $settled = (Test-PlanStateSettled -Left $levelByName -Right $freshLevel) -and
            (Test-PlanStateSettled `
                -Left (Get-AlignmentSignature -Alignment $alignment) `
                -Right (Get-AlignmentSignature -Alignment $freshAlignment))
        $levelByName = $freshLevel
        $alignment = $freshAlignment
    }

    # Rebuilt once the inputs have settled so each decision is explained exactly once.
    $increment = Get-DecisionIncrement -ReleaseByName $releaseByName `
        -LevelByName $levelByName -Explain
    foreach ($decided in $alignment.GetEnumerator()) {
        $increment.Add($decided.Value)
    }

    Assert-PlanMovesEveryPackageNeedingIncrement -Report $report `
        -TargetByName $targetByName `
        -Increment $increment
    Assert-PlanMovesEveryRewrittenPublishedPackage -Report $report `
        -ReleaseByName $releaseByName -TargetByName $targetByName `
        -Increment $increment

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
    Invoke-VerifySemverCheck, `
    Invoke-ReleaseReport, `
    Invoke-SemverCheck, `
    Get-ReleasePlanPackageAnchor, `
    Get-ReleasePlanAnalysisBatchJson, `
    Assert-IncrementPackagePublished, `
    New-ReleasePlanFile, `
    Invoke-ExpandReleasePlan, `
    Invoke-ApplyReleasePlan
