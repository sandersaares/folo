#requires -Version 7

# Process boundaries for the increment-versions skill and justfiles/just_release.just.
# Rust owns release policy and artifact validation. PowerShell coordinates Cargo, compatibility
# evidence, CI outputs and crates.io probes. Ref: docs/build-and-tooling.md,
# "Automation language and boundaries", and .github/workflows/implementation.md.
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true

# cargo-semver-checks documents both outcomes as completed comparisons.
$script:SemverCheckNoFindingsExitCode = 0
$script:SemverCheckDenyFindingsExitCode = 100

# Bound transient crates.io read uncertainty without retrying confirmed publication states.
$script:PublishStatusRetryAttempt = 3
$script:PublishStatusRetryDelaySeconds = 1
Import-Module (Join-Path $PSScriptRoot '..' 'utility' 'Retry.psm1') -Force

function Get-ReleasePlanCargoArgument {
    # Only explicit preparation can refresh an inconsistent lockfile while building the helper.
    [CmdletBinding()]
    [OutputType([string[]])]
    param(
        [Parameter(Mandatory)][string[]] $Command,
        [string] $Base,
        [switch] $OfflineResolution
    )

    $lockArgument = if ($OfflineResolution) { '--offline' } else { '--locked' }
    $argument = @('run', '-p', 'cargo-release-plan', $lockArgument, '--') + $Command
    if (-not [string]::IsNullOrWhiteSpace($Base)) {
        $argument += @('--base', $Base)
    }
    return $argument
}

function Invoke-ReleasePlanCargo {
    # Check native status before exposing output, including with injected Cargo implementations.
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string[]] $Command,
        [Parameter(Mandatory)][scriptblock] $Cargo,
        [string] $Base,
        [switch] $OfflineResolution
    )

    $argument = Get-ReleasePlanCargoArgument -Command $Command -Base $Base `
        -OfflineResolution:$OfflineResolution
    $output = & $Cargo $argument
    if ($LASTEXITCODE -ne 0) {
        throw "cargo-release-plan $($Command[0]) failed with exit code $LASTEXITCODE."
    }
    return $output
}

function Get-ReleasePlanJson {
    # Keep a JSON array intact across every PowerShell function boundary, including [] and [x].
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string[]] $Command,
        [Parameter(Mandatory)][scriptblock] $Cargo
    )

    $json = Invoke-ReleasePlanCargo -Command $Command -Cargo $Cargo
    return , (ConvertFrom-Json -InputObject ($json -join "`n") -NoEnumerate)
}

function Write-ReleasePlanBaseVerbose {
    [CmdletBinding()]
    param([string] $Base)

    if (-not [string]::IsNullOrWhiteSpace($Base)) {
        Write-Verbose "Using RELEASE_PLAN_BASE=$Base as the explicit release baseline." -Verbose
    } else {
        Write-Verbose 'Using the release baseline selected by cargo-release-plan.' -Verbose
    }
}

function Get-AffectedSemverCheckTarget {
    [CmdletBinding()]
    [OutputType([string[]])]
    param(
        [Parameter(Mandatory)][string] $ReportPath,
        [scriptblock] $Cargo = { param([string[]] $Argument) & cargo @Argument }
    )

    return , (Get-ReleasePlanJson `
        -Command @('semver-targets', '--report', $ReportPath, '--verbose') -Cargo $Cargo)
}

function Get-ReleasePlanAnalysisBatchJson {
    # Preserve Rust's stdout verbatim; the skill consumes its ordered batch-record array.
    [CmdletBinding()]
    [OutputType([string])]
    param(
        [Parameter(Mandatory)][string] $ReportPath,
        [scriptblock] $Cargo = { param([string[]] $Argument) & cargo @Argument }
    )

    Invoke-ReleasePlanCargo `
        -Command @('analysis-order', '--report', $ReportPath, '--verbose') -Cargo $Cargo
}

function New-ReleasePlanFile {
    [CmdletBinding(SupportsShouldProcess)]
    param(
        [Parameter(Mandatory)][string] $ReportPath,
        [Parameter(Mandatory)][string] $DecisionPath,
        [Parameter(Mandatory)][string] $PlanPath,
        [scriptblock] $Cargo = { param([string[]] $Argument) & cargo @Argument }
    )

    if ($PSCmdlet.ShouldProcess($PlanPath, 'write generated cargo-release-plan input')) {
        Invoke-ReleasePlanCargo -Command @(
            'propose', '--report', $ReportPath, '--decisions', $DecisionPath,
            '--out', $PlanPath, '--verbose'
        ) -Cargo $Cargo
    }
}

function Get-SemverCheckCargoArgument {
    param(
        [Parameter(Mandatory)][string[]] $Package,
        [string] $ManifestPath
    )

    $argument = @('semver-checks', '--all-features')
    if (-not [string]::IsNullOrWhiteSpace($ManifestPath)) {
        $argument += @('--manifest-path', $ManifestPath)
    }
    foreach ($name in $Package) {
        $argument += @('-p', $name)
    }
    return $argument
}

function Get-SemverCheckTargetDirectory {
    # A short workspace-specific Windows path avoids MSVC MAX_PATH failures in the generated
    # placeholder workspaces. Reassess when cargo-semver-checks shortens its generated paths:
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
    # Cache identities need collision resistance while conserving path budget.
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
    # Only cargo-semver-checks receives the short target root; other Cargo commands keep theirs.
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
        [Environment]::SetEnvironmentVariable('CARGO_TARGET_DIR', $TargetDirectory, 'Process')
        & $Action
    } finally {
        # The provider removes null instead of leaving an empty variable that Cargo rejects.
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
        Invoke-WithSemverCheckTargetDirectory -Action $action -TargetDirectory $TargetDirectory
    } else {
        Invoke-WithSemverCheckTargetDirectory -Action $action
    }
}

function Invoke-VerifySemverCheck {
    [CmdletBinding()]
    param([scriptblock] $Cargo = { param([string[]] $Argument) & cargo @Argument })

    # A small package keeps the canary cheap. --baseline-rev fixes only the baseline; editing
    # this package can produce a genuine finding rather than an infrastructure error.
    $package = 'folo_utils'
    Write-Verbose "Verifying cargo-semver-checks against HEAD for canary '$package'." -Verbose
    try {
        Invoke-SemverCheckCargo `
            -Argument @('semver-checks', '--baseline-rev', 'HEAD', '-p', $package) -Cargo $Cargo
        if ($LASTEXITCODE -ne 0) {
            throw "cargo-semver-checks canary failed with exit code $LASTEXITCODE."
        }
    } catch {
        Write-Host (
            "cargo-semver-checks could not complete its '$package' canary. A broken tool is " +
            'not evidence of compatibility. Check for work-tree edits to the canary package; ' +
            "otherwise update with 'cargo install cargo-semver-checks --locked' and retry."
        ) -ForegroundColor Red
        throw
    }
}

function Assert-SemverCheckExitCode {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][int] $ExitCode,
        [Parameter(Mandatory)][string] $LogPath
    )

    if ($ExitCode -ne $script:SemverCheckNoFindingsExitCode -and
        $ExitCode -ne $script:SemverCheckDenyFindingsExitCode) {
        throw "cargo-semver-checks failed with exit code $ExitCode; log: '$LogPath'."
    }
    Write-Host "cargo-semver-checks completed with exit code $ExitCode; log: '$LogPath'."
}

function Invoke-PrepareReleasePlan {
    # Invalidate incomplete preparation so later stages cannot consume stale evidence.
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string] $OutDir,
        [string] $Base = $env:RELEASE_PLAN_BASE,
        [scriptblock] $Cargo = { param([string[]] $Argument) & cargo @Argument }
    )

    New-Item -ItemType Directory -Path $OutDir -Force | Out-Null
    $preparedPath = Join-Path $OutDir 'prepared.json'
    Remove-Item -LiteralPath $preparedPath -Force -ErrorAction SilentlyContinue
    Write-ReleasePlanBaseVerbose -Base $Base
    $completed = $false
    try {
        Invoke-ReleasePlanCargo -Command @('prepare', '--output', $OutDir) `
            -Base $Base -OfflineResolution -Cargo $Cargo
        if (-not (Test-Path -LiteralPath $preparedPath -PathType Leaf)) {
            throw "cargo-release-plan prepare did not produce '$preparedPath'."
        }
        Write-ReleaseSemverEvidence -OutDir $OutDir -Cargo $Cargo
        $completed = $true
    } finally {
        if (-not $completed) {
            Remove-Item -LiteralPath $preparedPath -Force -ErrorAction SilentlyContinue
        }
    }
}

function Invoke-ReleaseReport {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string] $OutDir,
        [string] $Base = $env:RELEASE_PLAN_BASE,
        [scriptblock] $Cargo = { param([string[]] $Argument) & cargo @Argument }
    )

    New-Item -ItemType Directory -Path $OutDir -Force | Out-Null
    Write-ReleasePlanBaseVerbose -Base $Base
    Invoke-ReleasePlanCargo -Command @('report', '--out-dir', $OutDir) -Base $Base -Cargo $Cargo
    Write-ReleaseSemverEvidence -OutDir $OutDir -Cargo $Cargo
}

function Write-ReleaseSemverEvidence {
    # Rust selects targets; this boundary retains logs for successful comparisons and findings.
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string] $OutDir,
        [Parameter(Mandatory)][scriptblock] $Cargo,
        [string] $ManifestPath
    )

    $OutDir = [IO.Path]::GetFullPath($OutDir)
    if (-not [string]::IsNullOrWhiteSpace($ManifestPath)) {
        $ManifestPath = [IO.Path]::GetFullPath($ManifestPath)
    }
    $targets = Get-AffectedSemverCheckTarget -ReportPath (Join-Path $OutDir 'report.json') `
        -Cargo $Cargo
    $logPath = Join-Path $OutDir 'semver-checks.log'
    if ($targets.Count -eq 0) {
        'No consumer-contract package requires a cargo-semver-checks comparison.' |
            Set-Content -LiteralPath $logPath -Encoding utf8
        Write-Host "No cargo-semver-checks target was selected; log: '$logPath'."
        return
    }

    $argument = Get-SemverCheckCargoArgument -Package $targets -ManifestPath $ManifestPath
    Write-Verbose "Running cargo $($argument -join ' '); output captured at '$logPath'." -Verbose
    $previousPreference = $PSNativeCommandUseErrorActionPreference
    # Findings are a documented nonzero exit, so capture and classify them explicitly.
    $PSNativeCommandUseErrorActionPreference = $false
    $locationChanged = $false
    try {
        if (-not [string]::IsNullOrWhiteSpace($ManifestPath)) {
            # Cargo discovers configuration from the working directory, not --manifest-path.
            Push-Location (Split-Path -Parent $ManifestPath)
            $locationChanged = $true
        }
        Invoke-SemverCheckCargo -Argument $argument -Cargo $Cargo 2>&1 |
            Tee-Object -FilePath $logPath
        $exitCode = $LASTEXITCODE
    } finally {
        if ($locationChanged) {
            Pop-Location
        }
        $PSNativeCommandUseErrorActionPreference = $previousPreference
    }
    Assert-SemverCheckExitCode -ExitCode $exitCode -LogPath $logPath
}

function Invoke-SemverCheck {
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
    Invoke-SemverCheckCargo -Argument (Get-SemverCheckCargoArgument -Package $targets) -Cargo $Cargo
    if ($LASTEXITCODE -ne 0) {
        throw "cargo-semver-checks failed with exit code $LASTEXITCODE."
    }
}

function Invoke-ExpandReleasePlan {
    # Rust owns final-path alias validation and staged promotion.
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string] $PlanPath,
        [Parameter(Mandatory)][string] $ExpandedPath,
        [scriptblock] $Cargo = { param([string[]] $Argument) & cargo @Argument }
    )

    Invoke-ReleasePlanCargo -Command @(
        'expand', '--plan', $PlanPath, '--out', $ExpandedPath, '--preserve-input'
    ) -Cargo $Cargo
}

function Invoke-PreviewReleasePlan {
    # Only evidence assessed against the captured prospective workspace may retain a plan.
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string] $PreparedPath,
        [Parameter(Mandatory)][string] $PlanPath,
        [Parameter(Mandatory)][string] $OutDir,
        [scriptblock] $Cargo = { param([string[]] $Argument) & cargo @Argument }
    )

    $expandedPath = Join-Path $OutDir 'plan.json'
    # Rust validates filesystem aliases before creating directories or invalidating a marker.
    # Only a successful preview grants this wrapper ownership for later evidence cleanup.
    $produced = $false
    $completed = $false
    try {
        Invoke-ReleasePlanCargo -Command @(
            'preview', '--prepared', $PreparedPath, '--plan', $PlanPath, '--output', $OutDir
        ) -Cargo $Cargo
        $produced = $true
        $inspection = Get-ReleasePlanJson -Command @(
            'inspect-plan', '--plan', $expandedPath, '--require-resolved'
        ) -Cargo $Cargo
        $manifestPath = [string] $inspection.evidence_manifest_path
        if (-not [IO.Path]::IsPathFullyQualified($manifestPath) -or
            -not (Test-Path -LiteralPath $manifestPath -PathType Leaf)) {
            throw "Prospective evidence manifest is unavailable: '$manifestPath'."
        }
        $capturedPlan = [IO.File]::ReadAllText([IO.Path]::GetFullPath($expandedPath))
        Write-ReleaseSemverEvidence -OutDir $OutDir -ManifestPath $manifestPath -Cargo $Cargo
        if ([IO.File]::ReadAllText([IO.Path]::GetFullPath($expandedPath)) -cne $capturedPlan) {
            throw 'Compatibility evidence collection changed the captured release plan.'
        }
        Invoke-ReleasePlanCargo -Command @(
            'verify-preview', '--plan', $expandedPath, '--manifest-path', $manifestPath
        ) -Cargo $Cargo
        $completed = $true
    } finally {
        if ($produced -and -not $completed -and (Test-Path -LiteralPath $expandedPath)) {
            Remove-Item -LiteralPath $expandedPath -Force
        }
    }
}

function Invoke-ApplyReleasePlan {
    # The Just boundary requires captured resolution; the general Rust apply command also
    # supports proposed manifest-only edits, which must not bypass the publication gate.
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string] $ExpandedPath,
        [scriptblock] $Cargo = { param([string[]] $Argument) & cargo @Argument }
    )

    $null = Get-ReleasePlanJson -Command @(
        'inspect-plan', '--plan', $ExpandedPath, '--require-resolved'
    ) -Cargo $Cargo
    Invoke-ReleasePlanCargo -Command @('apply', '--plan', $ExpandedPath) -Cargo $Cargo
}

function Invoke-ValidateVersions {
    # Publish CI targets before checking so downstream validation also runs on a rejected plan.
    [Diagnostics.CodeAnalysis.SuppressMessageAttribute('PSUseSingularNouns', '',
        Justification = 'Names the plural just validate-versions entry point.')]
    [CmdletBinding()]
    param(
        [string] $GitHubOutputPath = $env:GITHUB_OUTPUT,
        [string] $Base = $env:RELEASE_PLAN_BASE,
        [scriptblock] $Cargo = { param([string[]] $Argument) & cargo @Argument }
    )

    Write-ReleasePlanBaseVerbose -Base $Base
    if (-not [string]::IsNullOrWhiteSpace($GitHubOutputPath)) {
        Import-Module (Join-Path $PSScriptRoot 'ReleaseAutomation.psm1') -Force
        $outDir = Join-Path 'target' "release-plan-$(New-Guid)"
        New-Item -ItemType Directory -Path $outDir -Force | Out-Null
        try {
            Invoke-ReleasePlanCargo -Command @('report', '--out-dir', $outDir) `
                -Base $Base -Cargo $Cargo
            $targets = Get-AffectedSemverCheckTarget `
                -ReportPath (Join-Path $outDir 'report.json') -Cargo $Cargo
            $previousOutput = $env:GITHUB_OUTPUT
            try {
                $env:GITHUB_OUTPUT = $GitHubOutputPath
                Set-GitHubOutput -Name semver_targets -Value ($targets -join ' ') -AllowEmptyValue
            } finally {
                $env:GITHUB_OUTPUT = $previousOutput
            }
        } finally {
            Remove-Item -LiteralPath $outDir -Recurse -Force -ErrorAction SilentlyContinue
        }
    }
    Invoke-ReleasePlanCargo -Command @('check', '--format', 'github') -Base $Base -Cargo $Cargo
}

function Get-PublishStatusWithUnknownRetry {
    [Diagnostics.CodeAnalysis.SuppressMessageAttribute('PSReviewUnusedParameter', 'GetPublishStatus',
        Justification = 'Consumed by the Invoke-WithRetry action closure.')]
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
    # Rust validates expanded targets and publication eligibility before any registry access.
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string] $ExpandedPath,
        [string] $ManifestPath,
        [scriptblock] $Cargo = { param([string[]] $Argument) & cargo @Argument },
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
    $command = @('inspect-plan', '--plan', $ExpandedPath)
    if (-not [string]::IsNullOrWhiteSpace($ManifestPath)) {
        $command += @('--manifest-path', $ManifestPath)
    }
    $inspection = Get-ReleasePlanJson -Command $command -Cargo $Cargo
    $neverPublished = [System.Collections.Generic.List[string]]::new()
    $unknown = [System.Collections.Generic.List[string]]::new()
    foreach ($name in $inspection.publication_targets) {
        $status = Get-PublishStatusWithUnknownRetry -Name $name `
            -GetPublishStatus $GetPublishStatus -Attempt $PublishStatusRetryAttempt `
            -DelaySeconds $PublishStatusRetryDelaySeconds
        switch -CaseSensitive ($status) {
            'Published' { }
            'NeverPublished' { $neverPublished.Add($name) }
            default { $unknown.Add($name) }
        }
    }
    if ($neverPublished.Count -gt 0) {
        throw (
            "The increment reaches never-published packages: $($neverPublished -join ', '). " +
            'Publish these packages manually first, then configure Trusted Publishing.'
        )
    }
    if ($unknown.Count -gt 0) {
        throw "Could not determine crates.io publication status for: $($unknown -join ', ')."
    }
}

Export-ModuleMember -Function `
    Invoke-ValidateVersions, Invoke-VerifySemverCheck, Invoke-ReleaseReport, `
    Invoke-PrepareReleasePlan, Invoke-SemverCheck, Get-ReleasePlanAnalysisBatchJson, `
    Assert-IncrementPackagePublished, New-ReleasePlanFile, Invoke-ExpandReleasePlan, `
    Invoke-PreviewReleasePlan, Invoke-ApplyReleasePlan
