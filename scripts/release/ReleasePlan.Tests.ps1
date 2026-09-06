#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true
$VerbosePreference = 'Continue'

BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'ReleasePlan.psm1') -Force

    $script:ValidReleasePlanSchemaVersion = [long] 2
    $script:ValidChangeDecisionSchemaVersion = [long] 1
    $script:UnsupportedFutureReleasePlanSchemaVersion = [long] 3
    $script:UnsupportedExpandedPlanSchemaVersion = [long] 99

    # Report fixtures include package metadata, anchors, changed entries, dependencies, and groups.
    $script:ReleasePlanReportFixtureJsonDepth = 8
    # Change-decision fixtures include top-level metadata and per-change records.
    $script:ChangeDecisionFixtureJsonDepth = 4
    # Expanded-plan fixtures include top-level metadata and per-increment records.
    $script:ExpandedPlanFixtureJsonDepth = 4
    # Analysis-batch fixtures include top-level records and their package-name arrays.
    $script:AnalysisBatchFixtureJsonDepth = 3

    function Get-TestPackage {
        param(
            [Parameter(Mandatory)][string] $Name,
            [string] $Status = 'unchanged',
            [object[]] $Changed = @(),
            [object[]] $Dependencies = @(),
            [string] $Group,
            [string] $DeclaredVersion = '1.0.0',
            [string] $AnchorVersion = '1.0.0'
        )

        $package = [ordered]@{
            name             = $Name
            declared_version = $DeclaredVersion
            status           = $Status
            changed          = @($Changed)
            dependencies     = @($Dependencies)
        }
        if ($PSBoundParameters.ContainsKey('Group')) {
            $package.group = $Group
        }
        if ($PSBoundParameters.ContainsKey('AnchorVersion')) {
            $package.anchor = @{ commit = 'abc123'; version = $AnchorVersion }
        }
        return $package
    }

    function Write-TestReport {
        param(
            [Parameter(Mandatory)][string] $Path,
            [Parameter(Mandatory)][AllowEmptyCollection()][object[]] $Package,
            [hashtable] $Group = @{},
            [long] $SchemaVersion = $script:ValidReleasePlanSchemaVersion
        )

        [ordered]@{
            schema_version = $SchemaVersion
            packages       = @($Package)
            groups         = $Group
        } | ConvertTo-Json -Depth $script:ReleasePlanReportFixtureJsonDepth |
            Set-Content -LiteralPath $Path -Encoding utf8
    }

    function Write-TestDecision {
        param(
            [Parameter(Mandatory)][string] $Path,
            [Parameter(Mandatory)][AllowEmptyCollection()][object[]] $Change
        )

        [ordered]@{
            schema_version = $script:ValidChangeDecisionSchemaVersion
            changes        = @($Change)
        } | ConvertTo-Json -Depth $script:ChangeDecisionFixtureJsonDepth |
            Set-Content -LiteralPath $Path -Encoding utf8
    }

    function Get-TestReleasePlanCargoArgument {
        param(
            [Parameter(Mandatory)][string[]] $Command,
            [string] $Base
        )

        InModuleScope ReleasePlan -Parameters @{ Command = $Command; Base = $Base } {
            param($Command, $Base)
            Get-ReleasePlanCargoArgument -Command $Command -Base $Base
        }
    }

    function Get-TestAffectedSemverCheckTarget {
        param(
            [Parameter(Mandatory)][string] $ReportPath
        )

        InModuleScope ReleasePlan -Parameters @{ ReportPath = $ReportPath } {
            param($ReportPath)
            Get-AffectedSemverCheckTarget -ReportPath $ReportPath
        }
    }

    function Get-TestAffectedSemverCheckTargetVerboseMessage {
        param(
            [Parameter(Mandatory)][string] $ReportPath
        )

        InModuleScope ReleasePlan -Parameters @{ ReportPath = $ReportPath } {
            param($ReportPath)
            Get-AffectedSemverCheckTarget -ReportPath $ReportPath -Verbose 4>&1 |
                Where-Object { $_ -is [System.Management.Automation.VerboseRecord] } |
                ForEach-Object { $_.Message }
        }
    }

    function Assert-TestSemverCheckExitCode {
        param(
            [Parameter(Mandatory)][int] $ExitCode,
            [Parameter(Mandatory)][string] $LogPath
        )

        InModuleScope ReleasePlan -Parameters @{ ExitCode = $ExitCode; LogPath = $LogPath } {
            param($ExitCode, $LogPath)
            Assert-SemverCheckExitCode -ExitCode $ExitCode -LogPath $LogPath
        }
    }

    function Invoke-TestReleaseReport {
        param(
            [Parameter(Mandatory)][string] $OutDir,
            [string] $Base = '',
            [Parameter(Mandatory)][scriptblock] $Cargo
        )

        InModuleScope ReleasePlan -Parameters @{
            OutDir = $OutDir
            Base = $Base
            Cargo = $Cargo
        } {
            param($OutDir, $Base, $Cargo)
            Invoke-ReleaseReport -OutDir $OutDir -Base $Base -Cargo $Cargo
        }
    }

    function Invoke-TestSemverCheck {
        param(
            [AllowEmptyString()][string] $Package,
            [Parameter(Mandatory)][scriptblock] $Cargo
        )

        InModuleScope ReleasePlan -Parameters @{ Package = $Package; Cargo = $Cargo } {
            param($Package, $Cargo)
            Invoke-SemverCheck -Package $Package -Cargo $Cargo
        }
    }
}

Describe 'Get-ReleasePlanCargoArgument' {
    It 'forwards --base when a release baseline is set' {
        $argument =
            Get-TestReleasePlanCargoArgument -Command @('check', '--format', 'github') -Base 'abc123'
        $argument | Should -Contain '--base'
        $argument | Should -Contain 'abc123'
    }

    It 'omits --base when the release baseline is empty' {
        $argument = Get-TestReleasePlanCargoArgument -Command @('check') -Base ''
        $argument | Should -Not -Contain '--base'
    }
}

Describe 'Get-AffectedSemverCheckTarget' {
    It 'includes supported needs-increment and pending-release packages' {
        $path = Join-Path $TestDrive 'report.json'
        Write-TestReport -Path $path -Package @(
            Get-TestPackage -Name 'events' -Status 'needs-increment' `
                -Changed @(@{ path = 'src/lib.rs' })
            Get-TestPackage -Name 'nm' -Status 'pending-release' `
                -Changed @(@{ path = 'src/lib.rs' })
        )
        Get-TestAffectedSemverCheckTarget -ReportPath $path | Should -Be @('events', 'nm')
    }

    It 'excludes unchanged packages and unsupported handoff crates' {
        $path = Join-Path $TestDrive 'unsupported.json'
        Write-TestReport -Path $path -Package @(
            Get-TestPackage -Name 'events'
            Get-TestPackage -Name 'folo_utils' -Status 'needs-increment' `
                -Changed @(@{ path = 'src/lib.rs' })
        )
        Get-TestAffectedSemverCheckTarget -ReportPath $path | Should -BeNullOrEmpty
    }

    It 'selects the public package when a grouped implementation package changes' {
        $path = Join-Path $TestDrive 'impl.json'
        Write-TestReport -Path $path -Package @(
            Get-TestPackage -Name 'nm_impl' -Status 'needs-increment' -Group 'nm' `
                -Changed @(@{ path = 'src/lib.rs' })
            Get-TestPackage -Name 'nm' -Group 'nm'
        ) -Group @{
            nm = @{ members = @('nm', 'nm_impl'); consistent = $true; version = '1.0.0' }
        }
        Get-TestAffectedSemverCheckTarget -ReportPath $path | Should -Be @('nm')
    }

    It 'explains grouped package mapping to a supported contract target' {
        $path = Join-Path $TestDrive 'verbose-group.json'
        Write-TestReport -Path $path -Package @(
            Get-TestPackage -Name 'nm_impl' -Status 'needs-increment' -Group 'nm' `
                -Changed @(@{ path = 'src/lib.rs' })
            Get-TestPackage -Name 'nm' -Group 'nm'
        ) -Group @{
            nm = @{ members = @('nm', 'nm_impl'); consistent = $true; version = '1.0.0' }
        }

        $messages = Get-TestAffectedSemverCheckTargetVerboseMessage -ReportPath $path

        ($messages -join "`n") | Should -Match "belongs to version group 'nm'"
        ($messages -join "`n") | Should -Match "supported consumer-contract target 'nm'"
    }

    It 'explains unsupported changed-package exclusion' {
        $path = Join-Path $TestDrive 'verbose-exclusion.json'
        Write-TestReport -Path $path -Package @(
            Get-TestPackage -Name 'folo_utils' -Status 'needs-increment' `
                -Changed @(@{ path = 'src/lib.rs' })
        )

        $messages = Get-TestAffectedSemverCheckTargetVerboseMessage -ReportPath $path

        ($messages -join "`n") |
            Should -Match 'none are in the supported consumer-contract target allow-list'
    }

    It 'does not log packages that cannot affect SemVer target selection' {
        $path = Join-Path $TestDrive 'verbose-unchanged.json'
        Write-TestReport -Path $path -Package @(
            Get-TestPackage -Name 'events'
        )

        Get-TestAffectedSemverCheckTargetVerboseMessage -ReportPath $path |
            Should -BeNullOrEmpty
    }

    It 'maps the real cargo-bench-history private group to only its public package' {
        $path = Join-Path $TestDrive 'cbh.json'
        $members = @('cargo-bench-history', 'cargo-bench-history-faker', 'cbh_stats')
        Write-TestReport -Path $path -Package @(
            Get-TestPackage -Name 'cargo-bench-history' -Group 'cargo-bench-history'
            Get-TestPackage -Name 'cargo-bench-history-faker' -Status 'pending-release' `
                -Group 'cargo-bench-history' -Changed @(@{ path = 'src/lib.rs' })
            Get-TestPackage -Name 'cbh_stats' -Status 'needs-increment' `
                -Group 'cargo-bench-history' -Changed @(@{ path = 'src/lib.rs' })
        ) -Group @{
            'cargo-bench-history' = @{
                members = $members
                consistent = $true
                version = '1.0.0'
            }
        }
        Get-TestAffectedSemverCheckTarget -ReportPath $path | Should -Be @('cargo-bench-history')
    }

    It 'guards explicit targets against ungrouped documented-package drift' {
        # Pin the workspace manifest: the test reads real workspace metadata, and the runner's
        # working directory is not guaranteed to be the workspace root.
        $manifest = Join-Path $PSScriptRoot '../../Cargo.toml'
        InModuleScope ReleasePlan -Parameters @{ Manifest = $manifest } {
            param($Manifest)

            $metadata = cargo metadata --no-deps --format-version 1 --manifest-path $Manifest |
                ConvertFrom-Json
            $grouped = [System.Collections.Generic.HashSet[string]]::new(
                [System.StringComparer]::Ordinal
            )
            foreach ($group in $metadata.metadata.'release-plan'.groups.PSObject.Properties) {
                foreach ($member in $group.Value) {
                    [void] $grouped.Add([string] $member)
                }
            }

            $published = @(
                $metadata.packages |
                    Where-Object { $null -eq $_.publish -or @($_.publish).Count -gt 0 }
            )
            $missing = @(
                $published |
                    Where-Object {
                        -not $grouped.Contains([string] $_.name) -and
                        @(
                            $_.targets |
                                Where-Object {
                                    $_.doc -and
                                    ($_.kind -contains 'lib' -or $_.kind -contains 'proc-macro')
                                }
                        ).Count -gt 0 -and
                        -not $script:SemverCheckTargetAllowList.Contains([string] $_.name)
                    } |
                    ForEach-Object { [string] $_.name }
            )
            $stale = @(
                $script:SemverCheckTargetAllowList |
                    Where-Object { [string] $_ -notin @($published.name) }
            )

            $missing | Should -BeNullOrEmpty
            $stale | Should -BeNullOrEmpty
        }
    }

    It 'fails closed on an unsupported schema revision' {
        $path = Join-Path $TestDrive 'future.json'
        Write-TestReport -Path $path -Package @() -SchemaVersion $script:UnsupportedFutureReleasePlanSchemaVersion
        { Get-TestAffectedSemverCheckTarget -ReportPath $path } |
            Should -Throw "*unsupported schema_version*expected $script:ValidReleasePlanSchemaVersion*"
    }

    It 'fails closed when packages is not an array' {
        $path = Join-Path $TestDrive 'object.json'
        [ordered]@{
            schema_version = $script:ValidReleasePlanSchemaVersion
            packages       = [ordered]@{ name = 'events' }
            groups         = [ordered]@{}
        } | ConvertTo-Json -Depth $script:ReleasePlanReportFixtureJsonDepth |
            Set-Content -LiteralPath $path -Encoding utf8
        { Get-TestAffectedSemverCheckTarget -ReportPath $path } |
            Should -Throw '*packages must be an array*'
    }

    It 'joins an empty selected set to the required semver_targets= representation' {
        $path = Join-Path $TestDrive 'empty.json'
        Write-TestReport -Path $path -Package @(
            Get-TestPackage -Name 'events'
        )
        $target = @(Get-TestAffectedSemverCheckTarget -ReportPath $path)
        $target.Count | Should -Be 0
        ($target -join ' ') | Should -BeExactly ''
    }
}

Describe 'Assert-SemverCheckExitCode' {
    It 'accepts absence of a determined version requirement' {
        { Assert-TestSemverCheckExitCode -ExitCode 0 -LogPath 'semver.log' } |
            Should -Not -Throw
    }

    It 'accepts the documented finding exit' {
        { Assert-TestSemverCheckExitCode -ExitCode 100 -LogPath 'semver.log' } |
            Should -Not -Throw
    }

    It 'throws on a tool error' {
        { Assert-TestSemverCheckExitCode -ExitCode 101 -LogPath 'semver.log' } |
            Should -Throw '*exit code 101*'
    }
}

Describe 'Invoke-ReleaseReport' {
    It 'runs SemVer checks only for the explicit report targets' {
        $outDir = Join-Path $TestDrive 'collect'
        $script:calls = [System.Collections.Generic.List[object]]::new()
        $cargo = {
            param([string[]] $Argument)
            $script:calls.Add(@($Argument))
            if ($Argument -contains 'report') {
                $index = [array]::IndexOf($Argument, '--out-dir')
                Write-TestReport -Path (Join-Path $Argument[$index + 1] 'report.json') -Package @(
                    Get-TestPackage -Name 'events' -Status 'needs-increment' `
                        -Changed @(@{ path = 'src/lib.rs' })
                    Get-TestPackage -Name 'folo_utils' -Status 'needs-increment' `
                        -Changed @(@{ path = 'src/lib.rs' })
                )
            } else {
                $global:LASTEXITCODE = 0
                'semver output'
            }
        }

        Invoke-TestReleaseReport -OutDir $outDir -Base 'abc' -Cargo $cargo

        $script:calls.Count | Should -Be 2
        $script:calls[1] | Should -Contain 'events'
        $script:calls[1] | Should -Not -Contain 'folo_utils'
        Get-Content -LiteralPath (Join-Path $outDir 'semver-checks.log') -Raw |
            Should -Match 'semver output'
    }

    It 'writes a log and skips cargo-semver-checks when the target set is empty' {
        $outDir = Join-Path $TestDrive 'empty-collect'
        $script:calls = [System.Collections.Generic.List[object]]::new()
        $cargo = {
            param([string[]] $Argument)
            $script:calls.Add(@($Argument))
            $index = [array]::IndexOf($Argument, '--out-dir')
            Write-TestReport -Path (Join-Path $Argument[$index + 1] 'report.json') -Package @(
                Get-TestPackage -Name 'events'
            )
        }

        Invoke-TestReleaseReport -OutDir $outDir -Cargo $cargo

        $script:calls.Count | Should -Be 1
        Test-Path -LiteralPath (Join-Path $outDir 'semver-checks.log') | Should -BeTrue
    }
}

Describe 'Invoke-SemverCheck' {
    It 'turns package names into repeated cargo -p arguments' {
        $script:argument = $null
        Invoke-TestSemverCheck -Package 'events nm' -Cargo {
            param([string[]] $Argument)
            $script:argument = $Argument
        }
        $script:argument | Should -Be @(
            'semver-checks', '--all-features', '-p', 'events', '-p', 'nm'
        )
    }

    It 'does not invoke cargo for an empty package set' {
        $script:called = $false
        Invoke-TestSemverCheck -Package '' -Cargo { $script:called = $true }
        $script:called | Should -BeFalse
    }
}

Describe 'Invoke-ExpandReleasePlan' {
    It 'moves a successful staging expansion into the caller-visible path' {
        $planPath = Join-Path $TestDrive 'plan.json'
        $expandedPath = Join-Path $TestDrive 'expanded.json'
        '{}' | Set-Content -LiteralPath $planPath -Encoding utf8
        $script:toolOutPath = $null

        Invoke-ExpandReleasePlan -PlanPath $planPath -ExpandedPath $expandedPath -Cargo {
            param([string[]] $Argument)
            $outIndex = [array]::IndexOf($Argument, '--out')
            $script:toolOutPath = $Argument[$outIndex + 1]
            [ordered]@{
                schema_version = $script:ValidReleasePlanSchemaVersion
                increments     = @()
            } | ConvertTo-Json -Depth $script:ExpandedPlanFixtureJsonDepth |
                Set-Content -LiteralPath $script:toolOutPath -Encoding utf8
            $global:LASTEXITCODE = 0
        }

        $script:toolOutPath | Should -Not -Be $expandedPath
        Test-Path -LiteralPath $script:toolOutPath | Should -BeFalse
        Get-Content -LiteralPath $expandedPath -Raw | Should -Match '"increments"'
    }

    It 'removes a stale caller-visible expansion when the tool boundary fails' {
        $planPath = Join-Path $TestDrive 'failing-plan.json'
        $expandedPath = Join-Path $TestDrive 'stale-expanded.json'
        '{}' | Set-Content -LiteralPath $planPath -Encoding utf8
        'stale expanded content' | Set-Content -LiteralPath $expandedPath -Encoding utf8
        try {
            {
                Invoke-ExpandReleasePlan -PlanPath $planPath -ExpandedPath $expandedPath -Cargo {
                    param([string[]] $Argument)
                    $outIndex = [array]::IndexOf($Argument, '--out')
                    [ordered]@{
                        schema_version = $script:ValidReleasePlanSchemaVersion
                        increments     = @()
                    } | ConvertTo-Json -Depth $script:ExpandedPlanFixtureJsonDepth |
                        Set-Content -LiteralPath $Argument[$outIndex + 1] -Encoding utf8
                    $global:LASTEXITCODE = 1
                }
            } | Should -Throw '*exit code 1*'

            Test-Path -LiteralPath $expandedPath | Should -BeFalse
            @(Get-ChildItem -LiteralPath $TestDrive -Filter 'stale-expanded.json.*.staging').Count |
                Should -Be 0
        } finally {
            $global:LASTEXITCODE = 0
        }
    }
}

Describe 'Invoke-ApplyReleasePlan' {
    It 'passes an expanded plan to cargo-release-plan apply' {
        $path = Join-Path $TestDrive 'apply-expanded.json'
        [ordered]@{
            schema_version = $script:ValidReleasePlanSchemaVersion
            expanded       = $true
            increments     = @([ordered]@{ name = 'events'; version = '1.0.1' })
        } | ConvertTo-Json -Depth $script:ExpandedPlanFixtureJsonDepth |
            Set-Content -LiteralPath $path -Encoding utf8
        $script:argument = $null
        Invoke-ApplyReleasePlan -ExpandedPath $path -Cargo {
            param([string[]] $Argument)
            $script:argument = $Argument
        }
        $script:argument | Should -Contain 'apply'
        $script:argument | Should -Contain $path
    }

    It 'rejects a proposed plan, which names a narrower set than it applies' {
        # The publication gate runs over the expanded plan's packages, so applying a proposed one
        # would edit packages that gate never saw.
        $path = Join-Path $TestDrive 'apply-proposed.json'
        [ordered]@{
            schema_version = $script:ValidReleasePlanSchemaVersion
            increments     = @([ordered]@{ name = 'nm'; level = 'patch' })
        } | ConvertTo-Json -Depth $script:ExpandedPlanFixtureJsonDepth |
            Set-Content -LiteralPath $path -Encoding utf8

        $script:argument = $null
        {
            Invoke-ApplyReleasePlan -ExpandedPath $path -Cargo {
                param([string[]] $Argument)
                $script:argument = $Argument
            }
        } | Should -Throw '*proposed plan*'
        $script:argument | Should -BeNullOrEmpty
    }

    It 'rejects a missing plan' {
        { Invoke-ApplyReleasePlan -ExpandedPath (Join-Path $TestDrive 'missing.json') } |
            Should -Throw '*not found*'
    }
}

Describe 'Invoke-ValidateVersions' {
    It 'emits semver_targets= when the report selects nothing, then runs check' {
        $output = Join-Path $TestDrive 'github-output'
        New-Item -ItemType File -Path $output | Out-Null
        $script:calls = [System.Collections.Generic.List[object]]::new()
        $cargo = {
            param([string[]] $Argument)
            $script:calls.Add(@($Argument))
            if ($Argument -contains 'report') {
                $index = [array]::IndexOf($Argument, '--out-dir')
                Write-TestReport -Path (Join-Path $Argument[$index + 1] 'report.json') -Package @(
                    Get-TestPackage -Name 'events'
                )
            }
        }
        Invoke-ValidateVersions -GitHubOutputPath $output -Base 'abc' -Cargo $cargo
        @(Get-Content -LiteralPath $output) | Should -Be @('semver_targets=')
        $script:calls.Count | Should -Be 2
        $script:calls[1] | Should -Contain 'check'
    }

    It 'emits space-separated SemVer targets when the report selects packages' {
        $output = Join-Path $TestDrive 'github-output-populated'
        New-Item -ItemType File -Path $output | Out-Null
        $cargo = {
            param([string[]] $Argument)
            if ($Argument -contains 'report') {
                $index = [array]::IndexOf($Argument, '--out-dir')
                Write-TestReport -Path (Join-Path $Argument[$index + 1] 'report.json') -Package @(
                    Get-TestPackage -Name 'events' -Status 'needs-increment' `
                        -Changed @(@{ path = 'src/lib.rs' })
                    Get-TestPackage -Name 'nm' -Status 'pending-release' `
                        -Changed @(@{ path = 'src/lib.rs' })
                )
            }
        }

        Invoke-ValidateVersions -GitHubOutputPath $output -Base 'abc' -Cargo $cargo

        @(Get-Content -LiteralPath $output) | Should -Be @('semver_targets=events nm')
    }

    It 'removes its temporary report directory even when check fails' {
        $output = Join-Path $TestDrive 'failing-github-output'
        New-Item -ItemType File -Path $output | Out-Null
        $script:outDir = $null
        $cargo = {
            param([string[]] $Argument)
            if ($Argument -contains 'report') {
                $index = [array]::IndexOf($Argument, '--out-dir')
                $script:outDir = $Argument[$index + 1]
                Write-TestReport -Path (Join-Path $script:outDir 'report.json') -Package @(
                    Get-TestPackage -Name 'events'
                )
                return
            }
            throw 'cargo-release-plan check found packages needing an increment.'
        }

        { Invoke-ValidateVersions -GitHubOutputPath $output -Base 'abc' -Cargo $cargo } |
            Should -Throw '*needing an increment*'
        $script:outDir | Should -Not -BeNullOrEmpty
        Test-Path -LiteralPath $script:outDir | Should -BeFalse
    }
}

Describe 'Get-ReleasePlanAnalysisBatch' {
    It 'puts dependencies before dependents and combines dependency cycles' {
        $path = Join-Path $TestDrive 'graph.json'
        Write-TestReport -Path $path -Package @(
            Get-TestPackage -Name 'app' -Dependencies @(@{ name = 'middle' })
            Get-TestPackage -Name 'middle' -Dependencies @(@{ name = 'core' })
            Get-TestPackage -Name 'core' -Dependencies @(@{ name = 'middle' })
            Get-TestPackage -Name 'independent'
        )

        $batch = @(InModuleScope ReleasePlan -Parameters @{ ReportPath = $path } {
            Get-ReleasePlanAnalysisBatch -ReportPath $ReportPath
        })

        $cycle = $batch | Where-Object { ($_.packages -join ', ') -eq 'core, middle' }
        $app = $batch | Where-Object { ($_.packages -join ', ') -eq 'app' }
        $cycle.cyclic | Should -BeTrue
        $app.cyclic | Should -BeFalse
        $cycle.order | Should -BeLessThan $app.order
        @($batch.packages) | Sort-Object |
            Should -Be @('app', 'core', 'independent', 'middle')
    }

    It 'keeps a prefix-named dependency out of its dependent''s batch' {
        $path = Join-Path $TestDrive 'prefix.json'
        Write-TestReport -Path $path -Package @(
            Get-TestPackage -Name 'nm' -Dependencies @(@{ name = 'nm_impl' })
            Get-TestPackage -Name 'nm_impl'
        )

        $batch = @(InModuleScope ReleasePlan -Parameters @{ ReportPath = $path } {
            Get-ReleasePlanAnalysisBatch -ReportPath $ReportPath
        })

        $batch.Count | Should -Be 2
        @($batch | Where-Object { $_.cyclic }).Count | Should -Be 0
        $leaf = $batch | Where-Object { ($_.packages -join ', ') -eq 'nm_impl' }
        $dependent = $batch | Where-Object { ($_.packages -join ', ') -eq 'nm' }
        $leaf.order | Should -BeLessThan $dependent.order
    }

    It 'emits the documented JSON field names for the skill working file' {
        $path = Join-Path $TestDrive 'contract.json'
        Write-TestReport -Path $path -Package @(
            Get-TestPackage -Name 'events'
        )

        # Through the production serializer, so this covers the contract the recipe emits rather
        # than a second serialization that could drift from it.
        $json = Get-ReleasePlanAnalysisBatchJson -ReportPath $path | ConvertFrom-Json

        @($json).Count | Should -Be 1
        @($json[0].PSObject.Properties.Name) | Should -Be @('order', 'packages', 'cyclic')
        $json[0].order | Should -Be 1
        @($json[0].packages) | Should -Be @('events')
        $json[0].cyclic | Should -BeFalse
    }
}

Describe 'Assert-IncrementPackagePublished' {
    BeforeAll {
        function Write-TestExpandedPlan {
            param(
                [Parameter(Mandatory)][string] $Path,
                [Parameter(Mandatory)][string[]] $Name,
                [bool] $Expanded = $true
            )

            [ordered]@{
                schema_version = $script:ValidReleasePlanSchemaVersion
                expanded       = $Expanded
                increments     = @($Name | ForEach-Object { [ordered]@{ name = $_; version = '1.0.1' } })
            } | ConvertTo-Json -Depth $script:ExpandedPlanFixtureJsonDepth |
                Set-Content -LiteralPath $Path -Encoding utf8
        }
    }

    It 'rejects a proposed plan, which names a narrower set than it reaches' {
        # A proposed plan may leave version-group members unnamed, so clearing publication
        # against one would check a narrower set than apply edits.
        $planPath = Join-Path $TestDrive 'unexpanded.json'
        Write-TestExpandedPlan -Path $planPath -Name @('events') -Expanded $false

        {
            Assert-IncrementPackagePublished -ExpandedPath $planPath `
                -GetPublishStatus { 'Published' }
        } | Should -Throw '*proposed plan*'
    }

    It 'checks every package the expansion reached, including group members' {
        $expandedPath = Join-Path $TestDrive 'publish-expanded.json'
        Write-TestExpandedPlan -Path $expandedPath -Name @('nm', 'nm_impl')
        $script:queried = [System.Collections.Generic.List[string]]::new()

        Assert-IncrementPackagePublished -ExpandedPath $expandedPath -GetPublishStatus {
            param([string] $Name)
            $script:queried.Add($Name)
            'Published'
        }

        $script:queried | Should -Be @('nm', 'nm_impl')
    }

    It 'retries an Unknown publish status before accepting a published package' {
        $expandedPath = Join-Path $TestDrive 'flaky-expanded.json'
        Write-TestExpandedPlan -Path $expandedPath -Name @('events')
        $script:statuses = [System.Collections.Generic.Queue[string]]::new()
        $script:statuses.Enqueue('Unknown')
        $script:statuses.Enqueue('Published')
        $script:queryCount = 0

        Assert-IncrementPackagePublished -ExpandedPath $expandedPath `
            -PublishStatusRetryDelaySeconds 0 `
            -GetPublishStatus {
                $script:queryCount++
                $script:statuses.Dequeue()
            } -WarningAction SilentlyContinue

        $script:queryCount | Should -Be 2
    }

    It 'fails after repeated Unknown publish statuses' {
        $expandedPath = Join-Path $TestDrive 'unknown-expanded.json'
        Write-TestExpandedPlan -Path $expandedPath -Name @('events')
        $script:queryCount = 0

        {
            Assert-IncrementPackagePublished -ExpandedPath $expandedPath `
                -PublishStatusRetryAttempt 2 `
                -PublishStatusRetryDelaySeconds 0 `
                -GetPublishStatus {
                    $script:queryCount++
                    'Unknown'
                } -WarningAction SilentlyContinue
        } | Should -Throw '*Could not confirm crates.io publication*events*'
        $script:queryCount | Should -Be 2
    }

    It 'fails when an expanded package was never published' {
        $expandedPath = Join-Path $TestDrive 'new-expanded.json'
        Write-TestExpandedPlan -Path $expandedPath -Name @('events')
        $script:queryCount = 0

        {
            Assert-IncrementPackagePublished -ExpandedPath $expandedPath `
                -PublishStatusRetryAttempt 2 `
                -PublishStatusRetryDelaySeconds 0 `
                -GetPublishStatus {
                    $script:queryCount++
                    'NeverPublished'
                }
        } | Should -Throw '*never-published package: events.*Publish the package manually first*RELEASING.md#first-publish-of-a-new-crate*'
        $script:queryCount | Should -Be 1
    }

    It 'names every never-published package in the plural' {
        $expandedPath = Join-Path $TestDrive 'plural-expanded.json'
        Write-TestExpandedPlan -Path $expandedPath -Name @('events', 'nm')

        {
            Assert-IncrementPackagePublished -ExpandedPath $expandedPath `
                -GetPublishStatus { 'NeverPublished' }
        } | Should -Throw '*never-published packages: events, nm.*Publish these packages manually first*RELEASING.md#first-publish-of-a-new-crate*'
    }

    It 'fails closed on an expanded plan with an unsupported schema revision' {
        $expandedPath = Join-Path $TestDrive 'bad-schema-expanded.json'
        [ordered]@{
            schema_version = $script:UnsupportedExpandedPlanSchemaVersion
            increments     = @()
        } | ConvertTo-Json -Depth $script:ExpandedPlanFixtureJsonDepth |
            Set-Content -LiteralPath $expandedPath -Encoding utf8

        {
            Assert-IncrementPackagePublished -ExpandedPath $expandedPath `
                -GetPublishStatus { 'Published' }
        } | Should -Throw '*schema_version*'
    }

    It 'fails closed on an expanded increment without a name' {
        $expandedPath = Join-Path $TestDrive 'nameless-expanded.json'
        [ordered]@{
            schema_version = $script:ValidReleasePlanSchemaVersion
            expanded       = $true
            increments     = @([ordered]@{ version = '1.0.1' })
        } | ConvertTo-Json -Depth $script:ExpandedPlanFixtureJsonDepth |
            Set-Content -LiteralPath $expandedPath -Encoding utf8

        {
            Assert-IncrementPackagePublished -ExpandedPath $expandedPath `
                -GetPublishStatus { 'Published' }
        } | Should -Throw '*without a name*'
    }
}

Describe 'New-ReleasePlanFile' {
    It 'raises a package exposing the leader of a drifted group that a laggard breaks' {
        # `lib` leads its group at 2.0.0; `lib_impl` lags at 1.0.0 and carries the breaking
        # decision. Resolution applies that level to the group's highest declared version, so the
        # group lands on 3.0.0 and `lib` moves incompatibly away from its own 2.0.0 anchor.
        # Reading the outcome off the laggard's anchor instead predicts 2.0.0 and leaves `app`
        # compatible while the contract it exposes has changed.
        $reportPath = Join-Path $TestDrive 'drift-report.json'
        $decisionPath = Join-Path $TestDrive 'drift-decision.json'
        $planPath = Join-Path $TestDrive 'drift-plan.json'
        Write-TestReport -Path $reportPath -Package @(
            Get-TestPackage -Name 'lib' -Group 'lib' -DeclaredVersion '2.0.0' -AnchorVersion '2.0.0'
            Get-TestPackage -Name 'lib_impl' -Group 'lib' -DeclaredVersion '1.0.0' `
                -AnchorVersion '1.0.0'
            Get-TestPackage -Name 'app' -DeclaredVersion '5.0.0' -AnchorVersion '5.0.0' `
                -Dependencies @(@{ name = 'lib'; req = '^2.0.0'; exact_pin = $false; public = $true })
        ) -Group @{
            lib = @{ members = @('lib', 'lib_impl'); consistent = $false; version = '2.0.0' }
        }
        Write-TestDecision -Path $decisionPath -Change @(
            @{ name = 'lib_impl'; level = 'breaking' }
        )

        New-ReleasePlanFile -ReportPath $reportPath -DecisionPath $decisionPath `
            -PlanPath $planPath
        $plan = Get-Content -LiteralPath $planPath -Raw | ConvertFrom-Json

        ($plan.increments | Where-Object name -EQ 'app').level | Should -Be 'major'
    }

    It 'raises a package whose public dependency is moved by a group sibling' {
        # `lib` carries no decision of its own; its group sibling `lib_impl` does. Resolution
        # moves the whole group, so `lib` releases a breaking change and `app`, which exposes it,
        # must break as well. Asking only about `lib`'s own decision would miss this.
        $reportPath = Join-Path $TestDrive 'sibling-report.json'
        $decisionPath = Join-Path $TestDrive 'sibling-decision.json'
        $planPath = Join-Path $TestDrive 'sibling-plan.json'
        Write-TestReport -Path $reportPath -Package @(
            Get-TestPackage -Name 'lib' -Group 'lib' -DeclaredVersion '1.0.0' -AnchorVersion '1.0.0'
            Get-TestPackage -Name 'lib_impl' -Group 'lib' -DeclaredVersion '1.0.0' `
                -AnchorVersion '1.0.0'
            Get-TestPackage -Name 'app' -DeclaredVersion '2.0.0' -AnchorVersion '2.0.0' `
                -Dependencies @(@{ name = 'lib'; req = '^1.0.0'; exact_pin = $false; public = $true })
        ) -Group @{
            lib = @{ members = @('lib', 'lib_impl'); consistent = $true; version = '1.0.0' }
        }
        Write-TestDecision -Path $decisionPath -Change @(
            @{ name = 'lib_impl'; level = 'breaking' }
        )

        New-ReleasePlanFile -ReportPath $reportPath -DecisionPath $decisionPath `
            -PlanPath $planPath
        $plan = Get-Content -LiteralPath $planPath -Raw | ConvertFrom-Json

        ($plan.increments | Where-Object name -EQ 'app').level | Should -Be 'major'
    }

    It 'raises a package whose public dependency releases a breaking change' {
        # `app` exposes `lib` in its public API, so `lib` moving to an incompatible version makes
        # `app`'s own contract incompatible even though `app` recorded only a patch.
        $reportPath = Join-Path $TestDrive 'public-report.json'
        $decisionPath = Join-Path $TestDrive 'public-decision.json'
        $planPath = Join-Path $TestDrive 'public-plan.json'
        Write-TestReport -Path $reportPath -Package @(
            Get-TestPackage -Name 'lib' -DeclaredVersion '1.0.0' -AnchorVersion '1.0.0'
            Get-TestPackage -Name 'app' -DeclaredVersion '2.0.0' -AnchorVersion '2.0.0' `
                -Dependencies @(@{ name = 'lib'; req = '^1.0.0'; exact_pin = $false; public = $true })
        )
        Write-TestDecision -Path $decisionPath -Change @(
            @{ name = 'lib'; level = 'breaking' }
            @{ name = 'app'; level = 'patch' }
        )

        $messages = New-ReleasePlanFile -ReportPath $reportPath -DecisionPath $decisionPath `
            -PlanPath $planPath -Verbose 4>&1 |
            Where-Object { $_ -is [System.Management.Automation.VerboseRecord] } |
            ForEach-Object { $_.Message }
        $plan = Get-Content -LiteralPath $planPath -Raw | ConvertFrom-Json

        ($messages -join "`n") | Should -Match "Package 'app' is raised to change level 'breaking'"
        ($plan.increments | Where-Object name -EQ 'app').level | Should -Be 'major'
    }

    It 'leaves a package whose breaking dependency is not publicly exposed' {
        $reportPath = Join-Path $TestDrive 'private-report.json'
        $decisionPath = Join-Path $TestDrive 'private-decision.json'
        $planPath = Join-Path $TestDrive 'private-plan.json'
        Write-TestReport -Path $reportPath -Package @(
            Get-TestPackage -Name 'lib' -DeclaredVersion '1.0.0' -AnchorVersion '1.0.0'
            Get-TestPackage -Name 'app' -DeclaredVersion '2.0.0' -AnchorVersion '2.0.0' `
                -Dependencies @(@{ name = 'lib'; req = '^1.0.0'; exact_pin = $false; public = $false })
        )
        Write-TestDecision -Path $decisionPath -Change @(
            @{ name = 'lib'; level = 'breaking' }
            @{ name = 'app'; level = 'patch' }
        )

        New-ReleasePlanFile -ReportPath $reportPath -DecisionPath $decisionPath `
            -PlanPath $planPath
        $plan = Get-Content -LiteralPath $planPath -Raw | ConvertFrom-Json

        ($plan.increments | Where-Object name -EQ 'app').level | Should -Be 'patch'
    }

    It 'propagates a breaking change along a chain of public dependencies' {
        # Raising the middle package makes the outer one incompatible in turn, which only a
        # repeated pass finds.
        $reportPath = Join-Path $TestDrive 'chain-report.json'
        $decisionPath = Join-Path $TestDrive 'chain-decision.json'
        $planPath = Join-Path $TestDrive 'chain-plan.json'
        Write-TestReport -Path $reportPath -Package @(
            # Ordered so the outer package is visited before the middle one is raised.
            Get-TestPackage -Name 'outer' -DeclaredVersion '3.0.0' -AnchorVersion '3.0.0' `
                -Dependencies @(@{ name = 'middle'; req = '^2.0.0'; exact_pin = $false; public = $true })
            Get-TestPackage -Name 'middle' -DeclaredVersion '2.0.0' -AnchorVersion '2.0.0' `
                -Dependencies @(@{ name = 'inner'; req = '^1.0.0'; exact_pin = $false; public = $true })
            Get-TestPackage -Name 'inner' -DeclaredVersion '1.0.0' -AnchorVersion '1.0.0'
        )
        Write-TestDecision -Path $decisionPath -Change @(
            @{ name = 'inner'; level = 'breaking' }
        )

        New-ReleasePlanFile -ReportPath $reportPath -DecisionPath $decisionPath `
            -PlanPath $planPath
        $plan = Get-Content -LiteralPath $planPath -Raw | ConvertFrom-Json

        ($plan.increments | Where-Object name -EQ 'middle').level | Should -Be 'major'
        ($plan.increments | Where-Object name -EQ 'outer').level | Should -Be 'major'
    }

    It 'leaves a package whose public dependency is already pending a breaking release' {
        # The dependency carries no decision because an earlier pull request already moved it, but
        # it still releases a breaking change, so the exposure is still incompatible.
        $reportPath = Join-Path $TestDrive 'pending-report.json'
        $decisionPath = Join-Path $TestDrive 'pending-decision.json'
        $planPath = Join-Path $TestDrive 'pending-plan.json'
        Write-TestReport -Path $reportPath -Package @(
            Get-TestPackage -Name 'lib' -Status 'pending-release' -DeclaredVersion '2.0.0' `
                -AnchorVersion '1.0.0'
            Get-TestPackage -Name 'app' -DeclaredVersion '2.0.0' -AnchorVersion '2.0.0' `
                -Dependencies @(@{ name = 'lib'; req = '^2.0.0'; exact_pin = $false; public = $true })
        )
        Write-TestDecision -Path $decisionPath -Change @()

        New-ReleasePlanFile -ReportPath $reportPath -DecisionPath $decisionPath `
            -PlanPath $planPath
        $plan = Get-Content -LiteralPath $planPath -Raw | ConvertFrom-Json

        ($plan.increments | Where-Object name -EQ 'app').level | Should -Be 'major'
    }

    It 'translates semantic change levels into mechanical Cargo levels' {
        $reportPath = Join-Path $TestDrive 'levels-report.json'
        $decisionPath = Join-Path $TestDrive 'levels-decision.json'
        $planPath = Join-Path $TestDrive 'levels-plan.json'
        Write-TestReport -Path $reportPath -Package @(
            Get-TestPackage -Name 'events' -DeclaredVersion '0.7.0' -AnchorVersion '0.7.0'
            Get-TestPackage -Name 'many_cpus' -DeclaredVersion '2.4.0' -AnchorVersion '2.4.0'
            Get-TestPackage -Name 'nm' -DeclaredVersion '1.0.0' -AnchorVersion '1.0.0'
        )
        Write-TestDecision -Path $decisionPath -Change @(
            @{ name = 'events'; level = 'breaking' }
            @{ name = 'many_cpus'; level = 'breaking' }
            @{ name = 'nm'; level = 'nonbreaking' }
        )

        $messages = New-ReleasePlanFile -ReportPath $reportPath -DecisionPath $decisionPath `
            -PlanPath $planPath -Verbose 4>&1 |
            Where-Object { $_ -is [System.Management.Automation.VerboseRecord] } |
            ForEach-Object { $_.Message }
        $plan = Get-Content -LiteralPath $planPath -Raw | ConvertFrom-Json

        ($messages -join "`n") | Should -Match "package 'events'.*emitted.*'minor'"
        ($messages -join "`n") | Should -Match "declared version '0.7.0'.*anchor '0.7.0'"
        ($plan.increments | Where-Object name -EQ 'events').level | Should -Be 'minor'
        ($plan.increments | Where-Object name -EQ 'many_cpus').level | Should -Be 'major'
        ($plan.increments | Where-Object name -EQ 'nm').level | Should -Be 'minor'
    }

    It 'realigns a drifted group whose decided level was already covered' {
        # The decision is skipped as already sufficient, so nothing else would name the group.
        # Leaving it unnamed would leave the members disagreeing and the check permanently red.
        $reportPath = Join-Path $TestDrive 'covered-report.json'
        $decisionPath = Join-Path $TestDrive 'covered-decision.json'
        $planPath = Join-Path $TestDrive 'covered-plan.json'
        Write-TestReport -Path $reportPath -Package @(
            Get-TestPackage -Name 'nm' -Group 'nm' -Status 'pending-release' `
                -Changed @(@{ source = 'file' }) -DeclaredVersion '1.1.0' -AnchorVersion '1.0.0'
            Get-TestPackage -Name 'nm_impl' -Group 'nm' -DeclaredVersion '1.0.0' `
                -AnchorVersion '1.0.0'
        ) -Group @{
            nm = @{ members = @('nm', 'nm_impl'); consistent = $false; version = '1.1.0' }
        }
        Write-TestDecision -Path $decisionPath -Change @(
            @{ name = 'nm'; level = 'patch' }
        )

        New-ReleasePlanFile -ReportPath $reportPath -DecisionPath $decisionPath `
            -PlanPath $planPath
        $plan = Get-Content -LiteralPath $planPath -Raw | ConvertFrom-Json

        @($plan.increments).Count | Should -Be 1
        $plan.increments[0].name | Should -Be 'nm'
        $plan.increments[0].version | Should -Be '1.1.0'
        $plan.increments[0].PSObject.Properties.Name | Should -Not -Contain 'level'
    }

    It 'realigns a drifted group that no decision names at all' {
        # Aligning is not a release decision: the members only have to agree, and raising the
        # highest one would publish every member for no substantive change. Here the member that
        # keeps its version depends on nothing that moves, so its released content is untouched.
        $reportPath = Join-Path $TestDrive 'untouched-report.json'
        $decisionPath = Join-Path $TestDrive 'untouched-decision.json'
        $planPath = Join-Path $TestDrive 'untouched-plan.json'
        Write-TestReport -Path $reportPath -Package @(
            Get-TestPackage -Name 'nm' -Group 'nm' -DeclaredVersion '1.0.0' -AnchorVersion '1.0.0' `
                -Dependencies @(@{ name = 'nm_impl' })
            Get-TestPackage -Name 'nm_impl' -Group 'nm' -DeclaredVersion '1.1.0' `
                -AnchorVersion '1.1.0'
        ) -Group @{
            nm = @{ members = @('nm', 'nm_impl'); consistent = $false; version = '1.1.0' }
        }
        Write-TestDecision -Path $decisionPath -Change @()

        New-ReleasePlanFile -ReportPath $reportPath -DecisionPath $decisionPath `
            -PlanPath $planPath
        $plan = Get-Content -LiteralPath $planPath -Raw | ConvertFrom-Json

        @($plan.increments).Count | Should -Be 1
        $plan.increments[0].name | Should -Be 'nm'
        $plan.increments[0].version | Should -Be '1.1.0'
    }

    It 'realigns a group whose outside dependent is already pending release without a decision' {
        # The dependent's decision was dropped as already covered, so it is absent from the plan
        # while sitting above its anchor. Plan-entry names would call that stranded; its resolved
        # version says it already ships the rewrite.
        $reportPath = Join-Path $TestDrive 'pending-dependent-report.json'
        $decisionPath = Join-Path $TestDrive 'pending-dependent-decision.json'
        $planPath = Join-Path $TestDrive 'pending-dependent-plan.json'
        Write-TestReport -Path $reportPath -Package @(
            Get-TestPackage -Name 'nm' -Group 'nm' -DeclaredVersion '1.1.0' -AnchorVersion '1.1.0'
            Get-TestPackage -Name 'nm_impl' -Group 'nm' -DeclaredVersion '1.0.0' `
                -AnchorVersion '1.0.0'
            Get-TestPackage -Name 'events' -Status 'pending-release' `
                -Changed @(@{ path = 'src/lib.rs' }) -DeclaredVersion '2.1.0' `
                -AnchorVersion '2.0.0' -Dependencies @(@{ name = 'nm_impl' })
        ) -Group @{
            nm = @{ members = @('nm', 'nm_impl'); consistent = $false; version = '1.1.0' }
        }
        # A patch is already covered by the declared version, so no entry is emitted for events.
        Write-TestDecision -Path $decisionPath -Change @(
            @{ name = 'events'; level = 'patch' }
        )

        New-ReleasePlanFile -ReportPath $reportPath -DecisionPath $decisionPath `
            -PlanPath $planPath
        $plan = Get-Content -LiteralPath $planPath -Raw | ConvertFrom-Json

        @($plan.increments).Count | Should -Be 1
        $plan.increments[0].name | Should -Be 'nm'
        $plan.increments[0].version | Should -Be '1.1.0'
    }

    It 'realigns a group whose outside dependent has never been published' {
        # An anchorless package has published nothing for a rewritten requirement to collide
        # with, and it cannot take a change level, so treating it as stranded would be a dead end.
        $reportPath = Join-Path $TestDrive 'anchorless-dependent-report.json'
        $decisionPath = Join-Path $TestDrive 'anchorless-dependent-decision.json'
        $planPath = Join-Path $TestDrive 'anchorless-dependent-plan.json'
        $newcomer = Get-TestPackage -Name 'events' -DeclaredVersion '0.1.0' `
            -Dependencies @(@{ name = 'nm_impl' })
        $newcomer.Remove('anchor')
        Write-TestReport -Path $reportPath -Package @(
            Get-TestPackage -Name 'nm' -Group 'nm' -DeclaredVersion '1.1.0' -AnchorVersion '1.1.0'
            Get-TestPackage -Name 'nm_impl' -Group 'nm' -DeclaredVersion '1.0.0' `
                -AnchorVersion '1.0.0'
            $newcomer
        ) -Group @{
            nm = @{ members = @('nm', 'nm_impl'); consistent = $false; version = '1.1.0' }
        }
        Write-TestDecision -Path $decisionPath -Change @()

        New-ReleasePlanFile -ReportPath $reportPath -DecisionPath $decisionPath `
            -PlanPath $planPath
        $plan = Get-Content -LiteralPath $planPath -Raw | ConvertFrom-Json

        @($plan.increments).Count | Should -Be 1
        $plan.increments[0].name | Should -Be 'nm'
    }

    It 'increments a grouped dependent rather than stranding it on a published version' {
        # `events` keeps the version its group aligns on, and pins a member the other group's
        # alignment moves. It belongs to a group, so incrementing that group moves it clear
        # instead of failing: only a package no realignment can move needs a decision.
        $reportPath = Join-Path $TestDrive 'named-leader-report.json'
        $decisionPath = Join-Path $TestDrive 'named-leader-decision.json'
        $planPath = Join-Path $TestDrive 'named-leader-plan.json'
        Write-TestReport -Path $reportPath -Package @(
            Get-TestPackage -Name 'nm' -Group 'nm' -DeclaredVersion '1.1.0' -AnchorVersion '1.1.0'
            Get-TestPackage -Name 'nm_impl' -Group 'nm' -DeclaredVersion '1.0.0' `
                -AnchorVersion '1.0.0'
            Get-TestPackage -Name 'events' -Group 'other' -DeclaredVersion '3.0.0' `
                -AnchorVersion '3.0.0' -Dependencies @(@{ name = 'nm_impl' })
            Get-TestPackage -Name 'events_impl' -Group 'other' -DeclaredVersion '2.0.0' `
                -AnchorVersion '2.0.0'
        ) -Group @{
            nm    = @{ members = @('nm', 'nm_impl'); consistent = $false; version = '1.1.0' }
            other = @{ members = @('events', 'events_impl'); consistent = $false; version = '3.0.0' }
        }
        Write-TestDecision -Path $decisionPath -Change @()

        New-ReleasePlanFile -ReportPath $reportPath -DecisionPath $decisionPath `
            -PlanPath $planPath
        $plan = Get-Content -LiteralPath $planPath -Raw | ConvertFrom-Json

        ($plan.increments | Where-Object name -EQ 'nm').version | Should -Be '1.1.0'
        ($plan.increments | Where-Object name -EQ 'other').level | Should -Be 'patch'
    }

    It 'refuses to realign a group that strands an outside published dependent' {
        # Applying the plan rewrites the requirement an outside package pins the moving member
        # at, changing that package's published manifest under a version crates.io already
        # carries. Nothing about the group can fix that, so the operator has to decide a level
        # for the dependent too.
        $reportPath = Join-Path $TestDrive 'stranded-report.json'
        $decisionPath = Join-Path $TestDrive 'stranded-decision.json'
        Write-TestReport -Path $reportPath -Package @(
            Get-TestPackage -Name 'nm' -Group 'nm' -DeclaredVersion '1.1.0' -AnchorVersion '1.1.0'
            Get-TestPackage -Name 'nm_impl' -Group 'nm' -DeclaredVersion '1.0.0' `
                -AnchorVersion '1.0.0'
            Get-TestPackage -Name 'events' -DeclaredVersion '2.0.0' -AnchorVersion '2.0.0' `
                -Dependencies @(@{ name = 'nm_impl' })
        ) -Group @{
            nm = @{ members = @('nm', 'nm_impl'); consistent = $false; version = '1.1.0' }
        }
        Write-TestDecision -Path $decisionPath -Change @()

        {
            New-ReleasePlanFile -ReportPath $reportPath -DecisionPath $decisionPath `
                -PlanPath (Join-Path $TestDrive 'stranded-plan.json')
        } | Should -Throw '*events*'
    }

    It 'realigns a group whose outside dependent already has a decision' {
        # The dependent is moving under its own decision, so the rewritten requirement ships
        # under a new version and the group can align normally.
        $reportPath = Join-Path $TestDrive 'decided-dependent-report.json'
        $decisionPath = Join-Path $TestDrive 'decided-dependent-decision.json'
        $planPath = Join-Path $TestDrive 'decided-dependent-plan.json'
        Write-TestReport -Path $reportPath -Package @(
            Get-TestPackage -Name 'nm' -Group 'nm' -DeclaredVersion '1.1.0' -AnchorVersion '1.1.0'
            Get-TestPackage -Name 'nm_impl' -Group 'nm' -DeclaredVersion '1.0.0' `
                -AnchorVersion '1.0.0'
            Get-TestPackage -Name 'events' -Status 'needs-increment' `
                -Changed @(@{ path = 'src/lib.rs' }) -DeclaredVersion '2.0.0' `
                -AnchorVersion '2.0.0' -Dependencies @(@{ name = 'nm_impl' })
        ) -Group @{
            nm = @{ members = @('nm', 'nm_impl'); consistent = $false; version = '1.1.0' }
        }
        Write-TestDecision -Path $decisionPath -Change @(
            @{ name = 'events'; level = 'patch' }
        )

        New-ReleasePlanFile -ReportPath $reportPath -DecisionPath $decisionPath `
            -PlanPath $planPath
        $plan = Get-Content -LiteralPath $planPath -Raw | ConvertFrom-Json

        @($plan.increments).Count | Should -Be 2
        ($plan.increments | Where-Object name -EQ 'nm').version | Should -Be '1.1.0'
        ($plan.increments | Where-Object name -EQ 'events').level | Should -Be 'patch'
    }

    It 'increments a drifted group when an unmoved member depends on a moving member' {
        # Aligning would raise nm_impl and rewrite the `=` requirement nm publishes for it, while
        # nm keeps a version crates.io already carries. That is changed released content under a
        # published version, so the group has to move as a whole instead.
        $reportPath = Join-Path $TestDrive 'pinned-report.json'
        $decisionPath = Join-Path $TestDrive 'pinned-decision.json'
        $planPath = Join-Path $TestDrive 'pinned-plan.json'
        Write-TestReport -Path $reportPath -Package @(
            Get-TestPackage -Name 'nm' -Group 'nm' -DeclaredVersion '1.1.0' -AnchorVersion '1.1.0' `
                -Dependencies @(@{ name = 'nm_impl' })
            Get-TestPackage -Name 'nm_impl' -Group 'nm' -DeclaredVersion '1.0.0' `
                -AnchorVersion '1.0.0'
        ) -Group @{
            nm = @{ members = @('nm', 'nm_impl'); consistent = $false; version = '1.1.0' }
        }
        Write-TestDecision -Path $decisionPath -Change @()

        New-ReleasePlanFile -ReportPath $reportPath -DecisionPath $decisionPath `
            -PlanPath $planPath
        $plan = Get-Content -LiteralPath $planPath -Raw | ConvertFrom-Json

        @($plan.increments).Count | Should -Be 1
        $plan.increments[0].name | Should -Be 'nm'
        $plan.increments[0].level | Should -Be 'patch'
        $plan.increments[0].PSObject.Properties.Name | Should -Not -Contain 'version'
    }

    It 'leaves a drifted group to the change level that already names a member' {
        # A level entry realigns the group on its own. Adding an exact version beside it would
        # give one group two decision kinds, which the tool rejects.
        $reportPath = Join-Path $TestDrive 'named-report.json'
        $decisionPath = Join-Path $TestDrive 'named-decision.json'
        $planPath = Join-Path $TestDrive 'named-plan.json'
        Write-TestReport -Path $reportPath -Package @(
            Get-TestPackage -Name 'nm' -Group 'nm' -Status 'needs-increment' `
                -Changed @(@{ source = 'file' }) -DeclaredVersion '1.0.0' -AnchorVersion '1.0.0'
            Get-TestPackage -Name 'nm_impl' -Group 'nm' -DeclaredVersion '1.1.0' `
                -AnchorVersion '1.1.0'
        ) -Group @{
            nm = @{ members = @('nm', 'nm_impl'); consistent = $false; version = '1.1.0' }
        }
        Write-TestDecision -Path $decisionPath -Change @(
            @{ name = 'nm'; level = 'patch' }
        )

        New-ReleasePlanFile -ReportPath $reportPath -DecisionPath $decisionPath `
            -PlanPath $planPath
        $plan = Get-Content -LiteralPath $planPath -Raw | ConvertFrom-Json

        @($plan.increments).Count | Should -Be 1
        $plan.increments[0].level | Should -Be 'patch'
    }

    It 'leaves a consistent group alone' {
        $reportPath = Join-Path $TestDrive 'consistent-report.json'
        $decisionPath = Join-Path $TestDrive 'consistent-decision.json'
        $planPath = Join-Path $TestDrive 'consistent-plan.json'
        Write-TestReport -Path $reportPath -Package @(
            Get-TestPackage -Name 'nm' -Group 'nm' -DeclaredVersion '1.0.0' -AnchorVersion '1.0.0'
        ) -Group @{
            nm = @{ members = @('nm'); consistent = $true; version = '1.0.0' }
        }
        Write-TestDecision -Path $decisionPath -Change @()

        New-ReleasePlanFile -ReportPath $reportPath -DecisionPath $decisionPath `
            -PlanPath $planPath
        $plan = Get-Content -LiteralPath $planPath -Raw | ConvertFrom-Json

        @($plan.increments).Count | Should -Be 0
    }

    It 'rejects a change level the skill does not decide' {
        $reportPath = Join-Path $TestDrive 'unsupported-level-report.json'
        $decisionPath = Join-Path $TestDrive 'unsupported-level-decision.json'
        Write-TestReport -Path $reportPath -Package @(
            Get-TestPackage -Name 'nm' -Group 'nm' -DeclaredVersion '1.0.0' -AnchorVersion '1.0.0'
        ) -Group @{
            nm = @{ members = @('nm'); consistent = $false; version = '1.0.0' }
        }
        Write-TestDecision -Path $decisionPath -Change @(
            @{ name = 'nm'; level = 'align' }
        )

        {
            New-ReleasePlanFile -ReportPath $reportPath -DecisionPath $decisionPath `
                -PlanPath (Join-Path $TestDrive 'unsupported-level-plan.json')
        } | Should -Throw "*unsupported level 'align'*"
    }

    It 'directs anchorless packages to the first-publication procedure' {
        $reportPath = Join-Path $TestDrive 'anchorless-report.json'
        $decisionPath = Join-Path $TestDrive 'anchorless-decision.json'
        Write-TestReport -Path $reportPath -Package @(
            Get-TestPackage -Name 'events'
        )
        Write-TestDecision -Path $decisionPath -Change @(
            @{ name = 'events'; level = 'patch' }
        )

        {
            New-ReleasePlanFile -ReportPath $reportPath -DecisionPath $decisionPath `
                -PlanPath (Join-Path $TestDrive 'anchorless-plan.json')
        } | Should -Throw '*Publish the package manually first*RELEASING.md#first-publish-of-a-new-crate*complete the full procedure*'
    }

    It 'rejects a prerelease version rather than deriving a level from it' {
        # A prerelease orders below the release it precedes, so no component comparison can
        # express "drop the suffix"; a derived level would silently overshoot.
        $reportPath = Join-Path $TestDrive 'prerelease-report.json'
        $decisionPath = Join-Path $TestDrive 'prerelease-decision.json'
        Write-TestReport -Path $reportPath -Package @(
            Get-TestPackage -Name 'events' -DeclaredVersion '1.1.0-alpha' -AnchorVersion '1.0.0'
        )
        Write-TestDecision -Path $decisionPath -Change @(
            @{ name = 'events'; level = 'nonbreaking' }
        )

        {
            New-ReleasePlanFile -ReportPath $reportPath -DecisionPath $decisionPath `
                -PlanPath (Join-Path $TestDrive 'prerelease-plan.json')
        } | Should -Throw '*prerelease version*'
    }

    It 'keeps a compatible change to a 0.y package on its patch component' {
        # In Cargo's pre-1.0 compatibility model, nonbreaking changes within a 0.y line keep
        # the minor component unchanged so existing compatible requirements keep matching.
        $reportPath = Join-Path $TestDrive 'zero-minor-report.json'
        $decisionPath = Join-Path $TestDrive 'zero-minor-decision.json'
        $planPath = Join-Path $TestDrive 'zero-minor-plan.json'
        Write-TestReport -Path $reportPath -Package @(
            Get-TestPackage -Name 'events' -DeclaredVersion '0.7.14' -AnchorVersion '0.7.14'
        )
        Write-TestDecision -Path $decisionPath -Change @(
            @{ name = 'events'; level = 'nonbreaking' }
        )

        New-ReleasePlanFile -ReportPath $reportPath -DecisionPath $decisionPath `
            -PlanPath $planPath
        $plan = Get-Content -LiteralPath $planPath -Raw | ConvertFrom-Json

        $plan.increments[0].level | Should -Be 'patch'
    }

    It 'advances the minor component for a breaking change to a 0.y package' {
        $reportPath = Join-Path $TestDrive 'zero-break-report.json'
        $decisionPath = Join-Path $TestDrive 'zero-break-decision.json'
        $planPath = Join-Path $TestDrive 'zero-break-plan.json'
        Write-TestReport -Path $reportPath -Package @(
            Get-TestPackage -Name 'events' -DeclaredVersion '0.7.14' -AnchorVersion '0.7.14'
        )
        Write-TestDecision -Path $decisionPath -Change @(
            @{ name = 'events'; level = 'breaking' }
        )

        New-ReleasePlanFile -ReportPath $reportPath -DecisionPath $decisionPath `
            -PlanPath $planPath
        $plan = Get-Content -LiteralPath $planPath -Raw | ConvertFrom-Json

        $plan.increments[0].level | Should -Be 'minor'
    }

    It 'confines every change level to the patch component of a 0.0.z package' {
        # No 0.0.z release is compatible with another, so there is no component left for a
        # breaking change to advance beyond the one a patch already advances.
        $reportPath = Join-Path $TestDrive 'zero-zero-report.json'
        $decisionPath = Join-Path $TestDrive 'zero-zero-decision.json'
        $planPath = Join-Path $TestDrive 'zero-zero-plan.json'
        Write-TestReport -Path $reportPath -Package @(
            Get-TestPackage -Name 'events' -DeclaredVersion '0.0.5' -AnchorVersion '0.0.5'
            Get-TestPackage -Name 'nm' -DeclaredVersion '0.0.5' -AnchorVersion '0.0.5'
        )
        Write-TestDecision -Path $decisionPath -Change @(
            @{ name = 'events'; level = 'breaking' }
            @{ name = 'nm'; level = 'nonbreaking' }
        )

        New-ReleasePlanFile -ReportPath $reportPath -DecisionPath $decisionPath `
            -PlanPath $planPath
        $plan = Get-Content -LiteralPath $planPath -Raw | ConvertFrom-Json

        ($plan.increments | Where-Object name -EQ 'events').level | Should -Be 'patch'
        ($plan.increments | Where-Object name -EQ 'nm').level | Should -Be 'patch'
    }

    It 'does not lower or repeat an already sufficient pending increment' {
        $reportPath = Join-Path $TestDrive 'pending-report.json'
        $decisionPath = Join-Path $TestDrive 'pending-decision.json'
        $planPath = Join-Path $TestDrive 'pending-plan.json'
        Write-TestReport -Path $reportPath -Package @(
            Get-TestPackage -Name 'events' -Status 'pending-release' `
                -DeclaredVersion '0.8.0' -AnchorVersion '0.7.0'
        )
        Write-TestDecision -Path $decisionPath -Change @(
            @{ name = 'events'; level = 'breaking' }
        )

        $messages = New-ReleasePlanFile -ReportPath $reportPath -DecisionPath $decisionPath `
            -PlanPath $planPath -Verbose 4>&1 |
            Where-Object { $_ -is [System.Management.Automation.VerboseRecord] } |
            ForEach-Object { $_.Message }
        $plan = Get-Content -LiteralPath $planPath -Raw | ConvertFrom-Json

        ($messages -join "`n") | Should -Match "package 'events'.*not emitted"
        ($messages -join "`n") | Should -Match "minimum version '0.8.0'.*anchor '0.7.0'"
        @($plan.increments).Count | Should -Be 0
    }

    It 'emits a group member decision for cargo-release-plan to merge at apply time' {
        $reportPath = Join-Path $TestDrive 'group-report.json'
        $decisionPath = Join-Path $TestDrive 'group-decision.json'
        $planPath = Join-Path $TestDrive 'group-plan.json'
        Write-TestReport -Path $reportPath -Package @(
            Get-TestPackage -Name 'nm' -Group 'nm' `
                -DeclaredVersion '1.2.0' -AnchorVersion '1.0.0'
            Get-TestPackage -Name 'nm_impl' -Group 'nm' `
                -DeclaredVersion '1.2.0' -AnchorVersion '1.2.0'
        ) -Group @{
            nm = @{ members = @('nm', 'nm_impl'); consistent = $true; version = '1.2.0' }
        }
        Write-TestDecision -Path $decisionPath -Change @(
            @{ name = 'nm'; level = 'nonbreaking' }
            @{ name = 'nm_impl'; level = 'patch' }
        )

        New-ReleasePlanFile -ReportPath $reportPath -DecisionPath $decisionPath `
            -PlanPath $planPath
        $plan = Get-Content -LiteralPath $planPath -Raw | ConvertFrom-Json

        @($plan.increments).Count | Should -Be 1
        $plan.increments[0].name | Should -Be 'nm_impl'
        $plan.increments[0].level | Should -Be 'patch'
    }

    It 'rejects a decision entry that carries an exact version' {
        $reportPath = Join-Path $TestDrive 'invalid-report.json'
        $decisionPath = Join-Path $TestDrive 'invalid-decision.json'
        Write-TestReport -Path $reportPath -Package @(
            Get-TestPackage -Name 'events'
        )
        [ordered]@{
            schema_version = $script:ValidChangeDecisionSchemaVersion
            changes        = @(
                [ordered]@{ name = 'events'; level = 'patch'; version = '9.0.0' }
            )
        } | ConvertTo-Json -Depth $script:ChangeDecisionFixtureJsonDepth |
            Set-Content -LiteralPath $decisionPath -Encoding utf8

        {
            New-ReleasePlanFile -ReportPath $reportPath -DecisionPath $decisionPath `
                -PlanPath (Join-Path $TestDrive 'invalid-plan.json')
        } | Should -Throw '*only name and level*'
    }

    It 'rejects a Cargo increment level in place of a semantic change level' {
        $reportPath = Join-Path $TestDrive 'cargo-level-report.json'
        $decisionPath = Join-Path $TestDrive 'cargo-level-decision.json'
        Write-TestReport -Path $reportPath -Package @(
            Get-TestPackage -Name 'events'
        )
        # `minor` is a Cargo increment level; the decision file speaks semantic change levels.
        [ordered]@{
            schema_version = $script:ValidChangeDecisionSchemaVersion
            changes        = @([ordered]@{ name = 'events'; level = 'minor' })
        } | ConvertTo-Json -Depth $script:ChangeDecisionFixtureJsonDepth |
            Set-Content -LiteralPath $decisionPath -Encoding utf8

        {
            New-ReleasePlanFile -ReportPath $reportPath -DecisionPath $decisionPath `
                -PlanPath (Join-Path $TestDrive 'cargo-level-plan.json')
        } | Should -Throw "*unsupported level 'minor'*"
    }

    It 'rejects a semantic change level that differs only by case' {
        $reportPath = Join-Path $TestDrive 'case-report.json'
        $decisionPath = Join-Path $TestDrive 'case-decision.json'
        Write-TestReport -Path $reportPath -Package @(
            Get-TestPackage -Name 'events'
        )
        [ordered]@{
            schema_version = $script:ValidChangeDecisionSchemaVersion
            changes        = @([ordered]@{ name = 'events'; level = 'Breaking' })
        } | ConvertTo-Json -Depth $script:ChangeDecisionFixtureJsonDepth |
            Set-Content -LiteralPath $decisionPath -Encoding utf8

        {
            New-ReleasePlanFile -ReportPath $reportPath -DecisionPath $decisionPath `
                -PlanPath (Join-Path $TestDrive 'case-plan.json')
        } | Should -Throw "*unsupported level 'Breaking'*"
    }
}

Describe 'Get-PlanIncrement' {
    # The tool rejects an entry carrying both an increment level and a version, or neither, so
    # this constructor is the one place that shape is decided. No current call site can pass
    # both, so these assert the guard directly rather than relying on a caller to reach it.
    It 'builds a level entry' {
        $entry = InModuleScope ReleasePlan { Get-PlanIncrement -Name 'nm' -Level 'patch' }
        $entry['name'] | Should -Be 'nm'
        $entry['level'] | Should -Be 'patch'
        $entry.Contains('version') | Should -BeFalse
    }

    It 'builds a version entry' {
        $entry = InModuleScope ReleasePlan { Get-PlanIncrement -Name 'nm' -Version '1.2.0' }
        $entry['version'] | Should -Be '1.2.0'
        $entry.Contains('level') | Should -BeFalse
    }

    It 'rejects an entry carrying both a level and a version' {
        {
            InModuleScope ReleasePlan {
                Get-PlanIncrement -Name 'nm' -Level 'patch' -Version '1.2.0'
            }
        } | Should -Throw '*exactly one*'
    }

    It 'rejects an entry carrying neither a level nor a version' {
        { InModuleScope ReleasePlan { Get-PlanIncrement -Name 'nm' } } | Should -Throw '*exactly one*'
    }
}

Describe 'Generated plan invariants' {
    # The pin-rewrite safety property was reported four separate times in review - once for
    # realignment direction, once for dependents outside the group, once for using plan-entry
    # names as a proxy for a moving package, and once for packages that have never published.
    # Each report was one instance of the same property being analyzed incompletely, and each was
    # found by a reader rather than by the suite.
    #
    # These cases assert the properties of the generator's output over a matrix of report states
    # instead of testing any single guard's internals, so an incomplete analysis fails here
    # whatever form it takes. A scenario is satisfied either by refusing to generate a plan or by
    # generating one that holds every property; silently emitting an unsafe plan is the failure.

    BeforeAll {
        function Get-ScenarioDecisionKey {
            # A package folds onto its group, and any other name stands for itself. Deliberately
            # recomputed here rather than reusing the module's helper, so a wrong answer there
            # cannot make these assertions agree with it.
            param($Report, [string] $Name)

            if (@($Report.groups.PSObject.Properties | ForEach-Object { $_.Name }) -contains $Name) {
                return $Name
            }
            foreach ($package in $Report.packages) {
                if ([string] $package.name -cne $Name) { continue }
                if ($package.PSObject.Properties.Name -contains 'group' -and
                    -not [string]::IsNullOrWhiteSpace([string] $package.group)) {
                    return [string] $package.group
                }
            }
            return $Name
        }

        function Get-ScenarioResolvedVersion {
            # The version each package ends the plan declaring. Mirrors the tool's documented
            # rule - a group takes the highest version any member declares, raised by the highest
            # level named for it - over the small hand-written scenarios below.
            param($Report, $Plan)

            $resolved = @{}
            foreach ($package in $Report.packages) {
                $resolved[[string] $package.name] = [semver] [string] $package.declared_version
            }

            foreach ($entry in $Plan.increments) {
                $key = Get-ScenarioDecisionKey -Report $Report -Name ([string] $entry.name)
                $members = if (@($Report.groups.PSObject.Properties | ForEach-Object { $_.Name }) -contains $key) {
                    @($Report.groups.$key.members | ForEach-Object { [string] $_ })
                } else {
                    @($key)
                }
                $members = @($members | Where-Object { $resolved.ContainsKey($_) })
                if ($members.Count -eq 0) { continue }

                $highest = ($members | ForEach-Object { $resolved[$_] } | Sort-Object)[-1]
                $target = if ($entry.PSObject.Properties.Name -contains 'version') {
                    [semver] [string] $entry.version
                } else {
                    switch -CaseSensitive ([string] $entry.level) {
                        'major' { [semver]::new($highest.Major + 1, 0, 0) }
                        'minor' { [semver]::new($highest.Major, $highest.Minor + 1, 0) }
                        'patch' { [semver]::new($highest.Major, $highest.Minor, $highest.Patch + 1) }
                        default { throw "Scenario saw unsupported increment level '$($entry.level)'." }
                    }
                }
                foreach ($member in $members) { $resolved[$member] = $target }
            }
            return $resolved
        }

        function Assert-PlanInvariant {
            param($Report, $Plan)

            $byName = @{}
            foreach ($package in $Report.packages) { $byName[[string] $package.name] = $package }
            $groupName = @($Report.groups.PSObject.Properties | ForEach-Object { $_.Name })

            $kindByKey = @{}
            foreach ($entry in $Plan.increments) {
                $field = @($entry.PSObject.Properties.Name)
                $name = [string] $entry.name

                # Well-formed: the tool requires exactly one of level or version per entry.
                $name | Should -Not -BeNullOrEmpty
                (($field -contains 'level') -bxor ($field -contains 'version')) |
                    Should -BeTrue -Because "entry '$name' must carry exactly one of level or version"

                # Known target: the tool rejects a name that is neither package nor group.
                ($byName.ContainsKey($name) -or $groupName -contains $name) |
                    Should -BeTrue -Because "entry '$name' must name a package or version group"

                # One decision kind per key: the tool rejects a group given both a level and an
                # exact version, so a generator that emits both produces an unapplyable plan.
                $key = Get-ScenarioDecisionKey -Report $Report -Name $name
                $kind = if ($field -contains 'level') { 'level' } else { 'version' }
                if ($kindByKey.ContainsKey($key)) {
                    $kindByKey[$key] | Should -Be $kind -Because "target '$key' must not mix decision kinds"
                }
                $kindByKey[$key] = $kind
            }

            $resolved = Get-ScenarioResolvedVersion -Report $Report -Plan $Plan

            foreach ($package in $Report.packages) {
                $name = [string] $package.name
                # No regression: a resolved version below the declared one would republish an
                # existing version with different content.
                $resolved[$name] | Should -BeGreaterOrEqual ([semver] [string] $package.declared_version) `
                    -Because "package '$name' must not move backwards"
            }

            # Every version group ends on one version, which is the reason realignment exists.
            foreach ($name in $groupName) {
                $member = @($Report.groups.$name.members |
                        ForEach-Object { [string] $_ } |
                        Where-Object { $resolved.ContainsKey($_) })
                if ($member.Count -lt 2) { continue }
                @($member | ForEach-Object { $resolved[$_].ToString() } | Sort-Object -Unique).Count |
                    Should -Be 1 -Because "group '$name' must end on a single version"
            }

            # A package the report says needs an increment must end the plan on a new version,
            # because the version check fails for exactly those packages until they move.
            foreach ($package in $Report.packages) {
                $name = [string] $package.name
                if ([string] $package.status -cne 'needs-increment') { continue }
                $resolved[$name] | Should -BeGreaterThan ([semver] [string] $package.declared_version) `
                    -Because "package '$name' needs an increment, so the plan must move it"
            }

            # The property four review comments were each one instance of: applying the plan
            # rewrites the requirement of every path dependency on a moving package, so a package
            # that keeps an already-published version would publish changed content under it.
            foreach ($package in $Report.packages) {
                $name = [string] $package.name
                if ($resolved[$name] -ne [semver] [string] $package.declared_version) { continue }
                if ($package.PSObject.Properties.Name -notcontains 'anchor' -or
                    $null -eq $package.anchor) { continue }
                if ($resolved[$name] -gt [semver] [string] $package.anchor.version) { continue }
                foreach ($dependency in $package.dependencies) {
                    $dependencyName = [string] $dependency.name
                    if (-not $resolved.ContainsKey($dependencyName)) { continue }
                    $moves = $resolved[$dependencyName] -ne
                        [semver] [string] $byName[$dependencyName].declared_version
                    $moves | Should -BeFalse -Because "package '$name' stays on published version $($resolved[$name]) while its pin to '$dependencyName' is rewritten"
                }
            }
        }
    }

    # Discovery-time data, because Pester expands -ForEach before BeforeAll runs. Each scenario
    # names the release state it represents; `Throws` marks the states where refusing is the only
    # correct answer, because no plan can make them safe.
    $planScenario = @(
            @{
                Name     = 'drifted group, nothing else changed'
                Throws   = $false
                Package  = @(
                    @{ Name = 'nm'; Group = 'nm'; Declared = '1.0.0'; Anchor = '1.0.0' }
                    @{ Name = 'nm_impl'; Group = 'nm'; Declared = '1.1.0'; Anchor = '1.1.0' }
                )
                Group    = @{ nm = @('nm', 'nm_impl') }
                Change   = @()
            }
            @{
                Name     = 'drifted group whose leader pins the lagging member'
                Throws   = $false
                Package  = @(
                    @{ Name = 'nm'; Group = 'nm'; Declared = '1.1.0'; Anchor = '1.1.0'; Deps = @('nm_impl') }
                    @{ Name = 'nm_impl'; Group = 'nm'; Declared = '1.0.0'; Anchor = '1.0.0' }
                )
                Group    = @{ nm = @('nm', 'nm_impl') }
                Change   = @()
            }
            @{
                Name     = 'drifted group with a published outside dependent'
                Throws   = $true
                Package  = @(
                    @{ Name = 'nm'; Group = 'nm'; Declared = '1.1.0'; Anchor = '1.1.0' }
                    @{ Name = 'nm_impl'; Group = 'nm'; Declared = '1.0.0'; Anchor = '1.0.0' }
                    @{ Name = 'events'; Declared = '2.0.0'; Anchor = '2.0.0'; Deps = @('nm_impl') }
                )
                Group    = @{ nm = @('nm', 'nm_impl') }
                Change   = @()
            }
            @{
                Name     = 'drifted group whose outside dependent is already pending release'
                Throws   = $false
                Package  = @(
                    @{ Name = 'nm'; Group = 'nm'; Declared = '1.1.0'; Anchor = '1.1.0' }
                    @{ Name = 'nm_impl'; Group = 'nm'; Declared = '1.0.0'; Anchor = '1.0.0' }
                    @{ Name = 'events'; Declared = '2.1.0'; Anchor = '2.0.0'; Deps = @('nm_impl') }
                )
                Group    = @{ nm = @('nm', 'nm_impl') }
                Change   = @()
            }
            @{
                Name     = 'drifted group whose outside dependent has never published'
                Throws   = $false
                Package  = @(
                    @{ Name = 'nm'; Group = 'nm'; Declared = '1.1.0'; Anchor = '1.1.0' }
                    @{ Name = 'nm_impl'; Group = 'nm'; Declared = '1.0.0'; Anchor = '1.0.0' }
                    @{ Name = 'events'; Declared = '0.1.0'; Anchor = $null; Deps = @('nm_impl') }
                )
                Group    = @{ nm = @('nm', 'nm_impl') }
                Change   = @()
            }
            @{
                Name     = 'drifted group whose outside dependent takes its own decision'
                Throws   = $false
                Package  = @(
                    @{ Name = 'nm'; Group = 'nm'; Declared = '1.1.0'; Anchor = '1.1.0' }
                    @{ Name = 'nm_impl'; Group = 'nm'; Declared = '1.0.0'; Anchor = '1.0.0' }
                    @{ Name = 'events'; Declared = '2.0.0'; Anchor = '2.0.0'; Deps = @('nm_impl'); Status = 'needs-increment' }
                )
                Group    = @{ nm = @('nm', 'nm_impl') }
                Change   = @(@{ name = 'events'; level = 'patch' })
            }
            @{
                Name     = 'drifted group named by a decision that is already covered'
                Throws   = $false
                Package  = @(
                    @{ Name = 'nm'; Group = 'nm'; Declared = '1.1.0'; Anchor = '1.0.0'; Status = 'pending-release' }
                    @{ Name = 'nm_impl'; Group = 'nm'; Declared = '1.0.0'; Anchor = '1.0.0' }
                )
                Group    = @{ nm = @('nm', 'nm_impl') }
                Change   = @(@{ name = 'nm'; level = 'patch' })
            }
            @{
                Name     = 'two drifted groups where one pins the other'
                Throws   = $false
                Package  = @(
                    @{ Name = 'nm'; Group = 'nm'; Declared = '1.1.0'; Anchor = '1.1.0' }
                    @{ Name = 'nm_impl'; Group = 'nm'; Declared = '1.0.0'; Anchor = '1.0.0' }
                    @{ Name = 'events'; Group = 'events'; Declared = '3.0.0'; Anchor = '3.0.0'; Deps = @('nm_impl') }
                    @{ Name = 'events_impl'; Group = 'events'; Declared = '2.0.0'; Anchor = '2.0.0' }
                )
                Group    = @{ nm = @('nm', 'nm_impl'); events = @('events', 'events_impl') }
                Change   = @()
            }
            @{
                Name     = 'consistent group with one decided member'
                Throws   = $false
                Package  = @(
                    @{ Name = 'nm'; Group = 'nm'; Declared = '1.0.0'; Anchor = '1.0.0'; Status = 'needs-increment' }
                    @{ Name = 'nm_impl'; Group = 'nm'; Declared = '1.0.0'; Anchor = '1.0.0' }
                )
                Group    = @{ nm = @('nm', 'nm_impl') }
                Change   = @(@{ name = 'nm'; level = 'breaking' })
            }
            @{
                Name     = 'group keyed apart from its members, decision naming a member'
                Throws   = $false
                Package  = @(
                    @{ Name = 'nm'; Group = 'family'; Declared = '1.0.0'; Anchor = '1.0.0' }
                    @{ Name = 'nm_impl'; Group = 'family'; Declared = '1.0.0'; Anchor = '1.0.0'; Status = 'needs-increment' }
                    @{ Name = 'other'; Group = 'other-family'; Declared = '3.0.0'; Anchor = '3.0.0'; Deps = @('nm') }
                    @{ Name = 'other_impl'; Group = 'other-family'; Declared = '2.0.0'; Anchor = '2.0.0' }
                )
                Group    = @{ family = @('nm', 'nm_impl'); 'other-family' = @('other', 'other_impl') }
                Change   = @(@{ name = 'nm_impl'; level = 'patch' })
            }
            @{
                Name     = 'package needing an increment with no decision recorded'
                Throws   = $true
                Package  = @(
                    @{ Name = 'nm'; Declared = '1.0.0'; Anchor = '1.0.0'; Status = 'needs-increment' }
                )
                Group    = @{}
                Change   = @()
            }
            @{
                Name     = 'grouped package needing an increment covered by a sibling decision'
                Throws   = $false
                Package  = @(
                    @{ Name = 'nm'; Group = 'family'; Declared = '1.0.0'; Anchor = '1.0.0'; Status = 'needs-increment' }
                    @{ Name = 'nm_impl'; Group = 'family'; Declared = '1.0.0'; Anchor = '1.0.0'; Status = 'needs-increment' }
                )
                Group    = @{ family = @('nm', 'nm_impl') }
                Change   = @(@{ name = 'nm'; level = 'patch' })
            }
            @{
                Name     = 'ungrouped package pinning a decided package'
                Throws   = $false
                Package  = @(
                    @{ Name = 'nm'; Declared = '1.0.0'; Anchor = '1.0.0'; Status = 'needs-increment' }
                    @{ Name = 'events'; Declared = '2.1.0'; Anchor = '2.0.0'; Deps = @('nm') }
                )
                Group    = @{}
                Change   = @(@{ name = 'nm'; level = 'patch' })
            }
        )

    It 'holds every plan property for <Name>' -ForEach $planScenario {
        $package = @($Package | ForEach-Object {
                $argument = @{
                    Name            = $_.Name
                    DeclaredVersion = $_.Declared
                    Status          = $(if ($_.ContainsKey('Status')) { $_.Status } else { 'unchanged' })
                }
                if ($_.ContainsKey('Group')) { $argument.Group = $_.Group }
                if ($_.ContainsKey('Deps')) {
                    $argument.Dependencies = @($_.Deps | ForEach-Object { @{ name = $_ } })
                }
                if ($_.ContainsKey('Status') -and $_.Status -ne 'unchanged') {
                    $argument.Changed = @(@{ path = 'src/lib.rs' })
                }
                if ($null -ne $_.Anchor) { $argument.AnchorVersion = $_.Anchor }
                $built = Get-TestPackage @argument
                if ($null -eq $_.Anchor) { $built.Remove('anchor') }
                $built
            })

        $groupTable = @{}
        foreach ($entry in $Group.GetEnumerator()) {
            $member = @($entry.Value)
            $declared = @($package |
                    Where-Object { $member -contains [string] $_.name } |
                    ForEach-Object { [semver] [string] $_.declared_version })
            $highest = ($declared | Sort-Object)[-1]
            $groupTable[$entry.Key] = @{
                members    = $member
                consistent = @($declared | ForEach-Object { $_.ToString() } | Sort-Object -Unique).Count -eq 1
                version    = $highest.ToString()
            }
        }

        $safeName = $Name -replace '[^A-Za-z0-9]', '-'
        $reportPath = Join-Path $TestDrive "invariant-$safeName-report.json"
        $decisionPath = Join-Path $TestDrive "invariant-$safeName-decision.json"
        $planPath = Join-Path $TestDrive "invariant-$safeName-plan.json"
        Write-TestReport -Path $reportPath -Package $package -Group $groupTable
        Write-TestDecision -Path $decisionPath -Change @($Change)

        if ($Throws) {
            {
                New-ReleasePlanFile -ReportPath $reportPath -DecisionPath $decisionPath `
                    -PlanPath $planPath
            } | Should -Throw
            return
        }

        New-ReleasePlanFile -ReportPath $reportPath -DecisionPath $decisionPath -PlanPath $planPath
        $report = Get-Content -LiteralPath $reportPath -Raw | ConvertFrom-Json
        $plan = Get-Content -LiteralPath $planPath -Raw | ConvertFrom-Json
        Assert-PlanInvariant -Report $report -Plan $plan
    }
}
