#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }

# Protects the Just/skill caller contract: injected Cargo output is authoritative, failed commands
# cannot supply evidence, and compatibility execution and registry probes remain hermetic.
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true
$VerbosePreference = 'Continue'

BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'ReleasePlan.psm1') -Force
    $script:previousBase = $env:RELEASE_PLAN_BASE
    $env:RELEASE_PLAN_BASE = 'baseline-must-not-reach-artifact-commands'
    $global:LASTEXITCODE = 0
}

AfterAll {
    $env:RELEASE_PLAN_BASE = $script:previousBase
}

Describe 'Rust artifact command contracts' {
    It 'preserves analysis JSON including nested arrays without reserializing: <RustJson>' -ForEach @(
        @{ RustJson = '[]' },
        @{ RustJson = '[{"order":1,"packages":["alpha"],"cyclic":false}]' },
        @{ RustJson = '[{"order":1,"packages":["alpha"],"cyclic":false},{"order":2,"packages":["beta","gamma"],"cyclic":true}]' }
    ) {
        $script:seen = $null
        $actual = Get-ReleasePlanAnalysisBatchJson -ReportPath 'report with spaces.json' -Cargo {
            param($Argument)
            $script:seen = $Argument
            $global:LASTEXITCODE = 0
            $RustJson
        }
        $actual | Should -BeExactly $RustJson
        $script:seen | Should -Be @(
            'run', '-p', 'cargo-release-plan', '--locked', '--',
            'analysis-order', '--report', 'report with spaces.json', '--verbose'
        )
    }

    It 'preserves the semver-targets array for <Json>' -TestCases @(
        @{ Json = '[]'; ExpectedCount = 0 },
        @{ Json = '["alpha"]'; ExpectedCount = 1 },
        @{ Json = '["alpha","beta"]'; ExpectedCount = 2 }
    ) {
        param($Json, $ExpectedCount)
        InModuleScope ReleasePlan -Parameters @{ Json = $Json; ExpectedCount = $ExpectedCount } {
            param($Json, $ExpectedCount)
            $fixtureJson = $Json
            $actual = Get-AffectedSemverCheckTarget -ReportPath 'absent report.json' -Cargo {
                param($Argument)
                $Argument | Should -Be @(
                    'run', '-p', 'cargo-release-plan', '--locked', '--',
                    'semver-targets', '--report', 'absent report.json', '--verbose'
                )
                $global:LASTEXITCODE = 0
                $fixtureJson
            }
            ($actual -is [array]) | Should -BeTrue
            $actual.Count | Should -Be $ExpectedCount
            ConvertTo-Json -InputObject $actual -Compress | Should -BeExactly $Json
        }
    }

    It 'rejects failed JSON commands before parsing their stdout' {
        InModuleScope ReleasePlan {
            Mock ConvertFrom-Json { throw 'parser must not run' }
            {
                Get-AffectedSemverCheckTarget -ReportPath 'report.json' -Cargo {
                    $global:LASTEXITCODE = 23
                    'invalid json'
                }
            } | Should -Throw
            Should -Invoke ConvertFrom-Json -Times 0
        }
    }

    It 'rejects invalid JSON after a successful subprocess' {
        InModuleScope ReleasePlan {
            {
                Get-AffectedSemverCheckTarget -ReportPath 'report.json' -Cargo {
                    $global:LASTEXITCODE = 0
                    'invalid json'
                }
            } | Should -Throw
        }
    }

    It 'passes report files and directories unchanged to propose: <Report>' -ForEach @(
        @{ Report = 'report.json' }, @{ Report = 'prepared evidence' }
    ) {
        $script:seen = $null
        $result = New-ReleasePlanFile -ReportPath $Report -DecisionPath 'decisions.json' `
            -PlanPath 'nested\plan.json' -Cargo {
                param($Argument)
                $script:seen = $Argument
                $global:LASTEXITCODE = 0
                'proposal summary'
            }
        $result | Should -BeExactly 'proposal summary'
        $script:seen | Should -Be @(
            'run', '-p', 'cargo-release-plan', '--locked', '--',
            'propose', '--report', $Report, '--decisions', 'decisions.json',
            '--out', 'nested\plan.json', '--verbose'
        )
    }

    It 'retains the proposal WhatIf boundary' {
        New-ReleasePlanFile -ReportPath 'report' -DecisionPath 'decisions' -PlanPath 'plan' `
            -WhatIf -Cargo { throw 'must not execute' }
    }

    It 'propagates artifact command errors: <Command>' -ForEach @(
        @{ Command = 'analysis-order' }, @{ Command = 'propose' }, @{ Command = 'apply' }
    ) {
        $cargo = { $global:LASTEXITCODE = 29; 'partial output' }
        {
            switch ($Command) {
                'analysis-order' { Get-ReleasePlanAnalysisBatchJson -ReportPath report -Cargo $cargo }
                'propose' {
                    New-ReleasePlanFile -ReportPath report -DecisionPath decisions `
                        -PlanPath plan -Cargo $cargo
                }
                'apply' { Invoke-ApplyReleasePlan -ExpandedPath plan -Cargo $cargo }
            }
        } | Should -Throw
    }

    It 'delegates all apply artifact validation without resolving again' {
        $script:seen = [Collections.Generic.List[object]]::new()
        Invoke-ApplyReleasePlan -ExpandedPath 'reviewed plan.json' -Cargo {
            param($Argument)
            $script:seen.Add($Argument)
            $global:LASTEXITCODE = 0
            if ($Argument[5] -eq 'inspect-plan') {
                '{"publication_targets":[],"evidence_manifest_path":null}'
            }
        }
        $script:seen.Count | Should -Be 2
        $script:seen[0] | Should -Be @(
            'run', '-p', 'cargo-release-plan', '--locked', '--',
            'inspect-plan', '--plan', 'reviewed plan.json', '--require-resolved'
        )
        $script:seen[1] | Should -Be @(
            'run', '-p', 'cargo-release-plan', '--locked', '--',
            'apply', '--plan', 'reviewed plan.json'
        )
    }

    It 'preserves the reviewed artifact after apply failure without retrying or resolving' {
        $planPath = Join-Path $TestDrive 'reviewed.json'
        Set-Content $planPath 'reviewed artifact'
        $script:commands = [Collections.Generic.List[string]]::new()
        {
            Invoke-ApplyReleasePlan -ExpandedPath $planPath -Cargo {
                param($Argument)
                $script:commands.Add($Argument[5])
                $global:LASTEXITCODE = 0
                if ($Argument[5] -eq 'inspect-plan') {
                    '{"publication_targets":[],"evidence_manifest_path":null}'
                } else {
                    $global:LASTEXITCODE = 9
                    'partial apply diagnostic'
                }
            }
        } | Should -Throw
        $script:commands | Should -Be @('inspect-plan', 'apply')
        Get-Content $planPath | Should -Be 'reviewed artifact'
    }
}

Describe 'Preparation and report process boundaries' {
    It 'uses offline preparation then locked artifact-only target selection' {
        $script:seen = [Collections.Generic.List[object]]::new()
        Invoke-PrepareReleasePlan -OutDir $TestDrive -Base 'origin/main' -Cargo {
            param($Argument)
            $script:seen.Add($Argument)
            $global:LASTEXITCODE = 0
            switch ($Argument[5]) {
                'prepare' { Set-Content (Join-Path $TestDrive 'prepared.json') 'captured' }
                'semver-targets' { '[]' }
                default { throw 'unexpected cargo operation' }
            }
        }
        $script:seen[0] | Should -Be @(
            'run', '-p', 'cargo-release-plan', '--offline', '--',
            'prepare', '--output', $TestDrive, '--base', 'origin/main'
        )
        $script:seen[1] | Should -Be @(
            'run', '-p', 'cargo-release-plan', '--locked', '--',
            'semver-targets', '--report', (Join-Path $TestDrive 'report.json'), '--verbose'
        )
        Test-Path (Join-Path $TestDrive 'prepared.json') | Should -BeTrue
        Get-Content (Join-Path $TestDrive 'semver-checks.log') | Should -Match 'No consumer-contract'
    }

    It 'invalidates preparation after <Failure>' -ForEach @(
        @{ Failure = 'exit' }, @{ Failure = 'throw' }, @{ Failure = 'missing snapshot' },
        @{ Failure = 'target selection' }, @{ Failure = 'compatibility' }
    ) {
        $preparedPath = Join-Path $TestDrive 'prepared.json'
        Set-Content $preparedPath 'stale snapshot'
        $script:commands = [Collections.Generic.List[string]]::new()
        {
            Invoke-PrepareReleasePlan -OutDir $TestDrive -Base '' -Cargo {
                param($Argument)
                $command = if ($Argument[0] -eq 'run') { $Argument[5] } else { $Argument[0] }
                $script:commands.Add($command)
                $global:LASTEXITCODE = 0
                switch ($command) {
                    'prepare' {
                        if ($Failure -eq 'missing snapshot') { return }
                        Set-Content $preparedPath 'partial snapshot'
                        if ($Failure -eq 'exit') { $global:LASTEXITCODE = 2 }
                        if ($Failure -eq 'throw') { throw [IO.IOException]::new('process canary') }
                    }
                    'semver-targets' {
                        if ($Failure -eq 'target selection') { $global:LASTEXITCODE = 2 }
                        '["alpha"]'
                    }
                    'semver-checks' { $global:LASTEXITCODE = 2; 'tool failure' }
                }
            }
        } | Should -Throw
        Test-Path $preparedPath | Should -BeFalse
        if ($Failure -in @('exit', 'throw', 'missing snapshot')) {
            $script:commands | Should -Be @('prepare')
        }
    }

    It 'stops a failed report before target selection' {
        $script:count = 0
        {
            Invoke-ReleaseReport -OutDir $TestDrive -Cargo {
                $script:count++
                $global:LASTEXITCODE = 3
                'report failed'
            }
        } | Should -Throw
        $script:count | Should -Be 1
    }

    It 'collects the exact reported targets without a workspace-wide comparison' {
        $script:seen = [Collections.Generic.List[object]]::new()
        Invoke-ReleaseReport -OutDir $TestDrive -Base 'release-base' -Cargo {
            param($Argument)
            $script:seen.Add($Argument)
            $global:LASTEXITCODE = 0
            if ($Argument[0] -eq 'semver-checks') { 'comparison evidence' }
            elseif ($Argument[5] -eq 'semver-targets') { '["beta","zeta"]' }
        }
        $script:seen[0] | Should -Be @(
            'run', '-p', 'cargo-release-plan', '--locked', '--',
            'report', '--out-dir', $TestDrive, '--base', 'release-base'
        )
        $script:seen[2] | Should -Be @('semver-checks', '--all-features', '-p', 'beta', '-p', 'zeta')
        Get-Content (Join-Path $TestDrive 'semver-checks.log') | Should -Be 'comparison evidence'
    }
}

Describe 'Compatibility evidence lifecycle' {
    It 'classifies comparison exit <ExitCode> as completed=<Completed>' -ForEach @(
        @{ ExitCode = 0; Completed = $true },
        @{ ExitCode = 100; Completed = $true },
        @{ ExitCode = 1; Completed = $false }
    ) {
        InModuleScope ReleasePlan -Parameters @{
            OutDir = $TestDrive; ExitCode = $ExitCode; Completed = $Completed
        } {
            param($OutDir, $ExitCode, $Completed)
            $comparisonExitCode = $ExitCode
            $action = {
                Write-ReleaseSemverEvidence -OutDir $OutDir -Cargo {
                    param($Argument)
                    $global:LASTEXITCODE = 0
                    if ($Argument[0] -eq 'run') { '["alpha"]' }
                    else { $global:LASTEXITCODE = $comparisonExitCode; 'completed log' }
                }
            }
            if ($Completed) { & $action } else { $action | Should -Throw }
            Get-Content (Join-Path $OutDir 'semver-checks.log') | Should -Be 'completed log'
        }
    }

    It 'scopes prospective Cargo discovery and restores location and preferences after failure' {
        InModuleScope ReleasePlan -Parameters @{ OutDir = $TestDrive } {
            param($OutDir)
            $manifest = Join-Path $OutDir 'Cargo.toml'
            Set-Content $manifest 'prospective manifest'
            $previousLocation = (Get-Location).Path
            $previousPreference = $PSNativeCommandUseErrorActionPreference
            {
                Write-ReleaseSemverEvidence -OutDir $OutDir -ManifestPath $manifest -Cargo {
                    param($Argument)
                    $global:LASTEXITCODE = 0
                    if ($Argument[0] -eq 'run') { '["alpha"]'; return }
                    (Get-Location).Path | Should -Be $OutDir
                    $Argument | Should -Be @(
                        'semver-checks', '--all-features', '--manifest-path', $manifest,
                        '-p', 'alpha'
                    )
                    $PSNativeCommandUseErrorActionPreference | Should -BeFalse
                    throw [IO.IOException]::new('compatibility canary')
                }
            } | Should -Throw
            (Get-Location).Path | Should -Be $previousLocation
            $PSNativeCommandUseErrorActionPreference | Should -Be $previousPreference
        }
    }

    It 'invokes the canary once and propagates failure=<Fails>' -ForEach @(
        @{ Fails = $false }, @{ Fails = $true }
    ) {
        $script:count = 0
        $action = {
            Invoke-VerifySemverCheck -Cargo {
                param($Argument)
                $script:count++
                $Argument | Should -Be @('semver-checks', '--baseline-rev', 'HEAD', '-p', 'folo_utils')
                $global:LASTEXITCODE = [int] $Fails
            }
        }
        if ($Fails) { $action | Should -Throw } else { & $action }
        $script:count | Should -Be 1
    }

    It 'does not invoke semver-checks for an empty Just package selection' {
        Invoke-SemverCheck -Package '  ' -Cargo { throw 'must not execute' }
    }

    It 'uses repeated package arguments and propagates the semver-checks exit' {
        {
            Invoke-SemverCheck -Package 'alpha beta' -Cargo {
                param($Argument)
                $Argument | Should -Be @('semver-checks', '--all-features', '-p', 'alpha', '-p', 'beta')
                $global:LASTEXITCODE = 100
            }
        } | Should -Throw
    }
}

Describe 'Semver target directory lifecycle' {
    It 'keeps paths stable per workspace and distinct across workspaces' {
        InModuleScope ReleasePlan -Parameters @{ Root = $TestDrive } {
            param($Root)
            $previous = $env:CARGO_TARGET_DIR
            try {
                $env:CARGO_TARGET_DIR = $null
                $cacheRoot = Join-Path $Root 'cache'
                $first = Get-SemverCheckTargetDirectory -WorkspaceRoot $Root -TempRoot $cacheRoot
                $first | Should -Be (
                    Get-SemverCheckTargetDirectory -WorkspaceRoot $Root -TempRoot $cacheRoot
                )
                $other = Get-SemverCheckTargetDirectory `
                    -WorkspaceRoot (Join-Path $Root 'other') -TempRoot $cacheRoot
                $other | Should -Not -Be $first
                Split-Path -Parent $first | Should -Be $cacheRoot
                $env:CARGO_TARGET_DIR = $Root
                Get-SemverCheckTargetDirectory -WorkspaceRoot $Root -TempRoot $cacheRoot |
                    Should -Be $Root
            } finally {
                $env:CARGO_TARGET_DIR = $previous
            }
        }
    }

    It 'restores an absent or configured target after failure: <Configured>' -ForEach @(
        @{ Configured = $false }, @{ Configured = $true }
    ) {
        InModuleScope ReleasePlan -Parameters @{ Root = $TestDrive; Configured = $Configured } {
            param($Root, $Configured)
            $previous = $env:CARGO_TARGET_DIR
            $selected = if ($Configured) { Join-Path $Root 'caller' } else { $null }
            try {
                $env:CARGO_TARGET_DIR = $selected
                {
                    Invoke-SemverCheckCargo -Argument @('semver-checks') `
                        -TargetDirectory $Root -Cargo {
                            $env:CARGO_TARGET_DIR | Should -Be $Root
                            throw [IO.IOException]::new('directory canary')
                        }
                } | Should -Throw
                [Environment]::GetEnvironmentVariable('CARGO_TARGET_DIR', 'Process') |
                    Should -Be $selected
            } finally {
                $env:CARGO_TARGET_DIR = $previous
            }
        }
    }

    It 'does not override the target when explicitly disabled' {
        InModuleScope ReleasePlan {
            $previous = $env:CARGO_TARGET_DIR
            Invoke-WithSemverCheckTargetDirectory -TargetDirectory $null -Action {
                $env:CARGO_TARGET_DIR | Should -Be $previous
            }
        }
    }
}

Describe 'Staged expansion and captured preview' {
    It 'removes stale and staged expansion after <Failure>' -ForEach @(
        @{ Failure = 'exit' }, @{ Failure = 'throw' }, @{ Failure = 'missing output' }
    ) {
        $expanded = Join-Path $TestDrive 'expanded.json'
        Set-Content $expanded 'stale'
        {
            Invoke-ExpandReleasePlan -PlanPath (Join-Path $TestDrive 'input.json') `
                -ExpandedPath $expanded -Cargo {
                    param($Argument)
                    $global:LASTEXITCODE = 0
                    $Argument[5] | Should -Be 'expand'
                    $Argument | Should -Not -Contain '--base'
                    if ($Failure -eq 'missing output') { return }
                    Set-Content $Argument[9] 'partial'
                    if ($Failure -eq 'throw') { throw [IO.IOException]::new('expansion canary') }
                    $global:LASTEXITCODE = 2
                }
        } | Should -Throw
        Test-Path $expanded | Should -BeFalse
        @(Get-ChildItem $TestDrive -Filter '*.staging').Count | Should -Be 0
    }

    It 'promotes successful expansion without rewriting trusted output' {
        $expanded = Join-Path $TestDrive 'expanded.json'
        Invoke-ExpandReleasePlan -PlanPath (Join-Path $TestDrive 'input.json') `
            -ExpandedPath $expanded -Cargo {
                param($Argument)
                $global:LASTEXITCODE = 0
                Set-Content $Argument[9] 'trusted expanded artifact'
            }
        Get-Content $expanded | Should -Be 'trusted expanded artifact'
    }

    It 'rejects expansion that would overwrite its input' {
        $path = Join-Path $TestDrive 'input.json'
        Set-Content $path 'original'
        {
            Invoke-ExpandReleasePlan -PlanPath $path -ExpandedPath $path `
                -Cargo { throw 'must not execute' }
        } | Should -Throw
        Get-Content $path | Should -Be 'original'
    }

    It 'retains preview only after inspection, comparison and verification: <Failure>' -ForEach @(
        @{ Failure = 'none' }, @{ Failure = 'preview' }, @{ Failure = 'inspection' },
        @{ Failure = 'missing manifest' }, @{ Failure = 'relative manifest' },
        @{ Failure = 'comparison' }, @{ Failure = 'changed artifact' },
        @{ Failure = 'verification' }
    ) {
        $output = Join-Path $TestDrive 'output'
        $manifest = Join-Path $TestDrive 'Cargo.toml'
        Set-Content $manifest 'prospective manifest'
        $expanded = Join-Path $output 'plan.json'
        $script:commands = [Collections.Generic.List[string]]::new()
        $action = {
            Invoke-PreviewReleasePlan -PreparedPath (Join-Path $TestDrive 'prepared.json') `
                -PlanPath (Join-Path $TestDrive 'proposed.json') -OutDir $output -Cargo {
                    param($Argument)
                    $command = if ($Argument[0] -eq 'run') { $Argument[5] } else { $Argument[0] }
                    $script:commands.Add($command)
                    $global:LASTEXITCODE = 0
                    $Argument | Should -Not -Contain '--base'
                    switch ($command) {
                        'preview' {
                            Set-Content $expanded 'captured artifact'
                            if ($Failure -eq 'preview') { $global:LASTEXITCODE = 2 }
                        }
                        'inspect-plan' {
                            $Argument | Should -Contain '--require-resolved'
                            if ($Failure -eq 'inspection') { $global:LASTEXITCODE = 2 }
                            $path = switch ($Failure) {
                                'missing manifest' { Join-Path $TestDrive 'absent.toml' }
                                'relative manifest' { 'relative.toml' }
                                default { $manifest }
                            }
                            @{ publication_targets = @(); evidence_manifest_path = $path } |
                                ConvertTo-Json -Compress
                        }
                        'semver-targets' { '["alpha"]' }
                        'semver-checks' {
                            if ($Failure -eq 'comparison') { $global:LASTEXITCODE = 1 }
                            if ($Failure -eq 'changed artifact') { Set-Content $expanded 'changed' }
                            'comparison log'
                        }
                        'verify-preview' {
                            if ($Failure -eq 'verification') { $global:LASTEXITCODE = 2 }
                        }
                        default { throw 'unexpected Cargo operation' }
                    }
                }
        }
        if ($Failure -eq 'none') {
            & $action
            $script:commands | Should -Be @(
                'preview', 'inspect-plan', 'semver-targets', 'semver-checks', 'verify-preview'
            )
            Get-Content $expanded | Should -Be 'captured artifact'
        } else {
            $action | Should -Throw
            Test-Path $expanded | Should -BeFalse
        }
    }

    It 'verifies preview even when the target set is empty' {
        $manifest = Join-Path $TestDrive 'Cargo.toml'
        Set-Content $manifest 'prospective manifest'
        $script:commands = [Collections.Generic.List[string]]::new()
        Invoke-PreviewReleasePlan -PreparedPath 'prepared.json' -PlanPath 'proposed.json' `
            -OutDir $TestDrive -Cargo {
                param($Argument)
                $script:commands.Add($Argument[5])
                $global:LASTEXITCODE = 0
                switch ($Argument[5]) {
                    'preview' { Set-Content (Join-Path $TestDrive 'plan.json') 'captured' }
                    'inspect-plan' {
                        @{ publication_targets = @(); evidence_manifest_path = $manifest } |
                            ConvertTo-Json -Compress
                    }
                    'semver-targets' { '[]' }
                    'verify-preview' {}
                    default { throw 'unexpected Cargo operation' }
                }
            }
        $script:commands | Should -Be @('preview', 'inspect-plan', 'semver-targets', 'verify-preview')
    }

    It 'does not delete a preview input aliased by the output path' {
        $plan = Join-Path $TestDrive 'plan.json'
        Set-Content $plan 'input'
        {
            Invoke-PreviewReleasePlan -PreparedPath 'prepared.json' -PlanPath $plan `
                -OutDir $TestDrive -Cargo { throw 'must not execute' }
        } | Should -Throw
        Get-Content $plan | Should -Be 'input'
    }
}

Describe 'CI version validation output' {
    It 'runs only the locked check without GitHub output' {
        $script:seen = $null
        Invoke-ValidateVersions -GitHubOutputPath '' -Base 'base-ref' -Cargo {
            param($Argument)
            $script:seen = $Argument
            $global:LASTEXITCODE = 0
        }
        $script:seen | Should -Be @(
            'run', '-p', 'cargo-release-plan', '--locked', '--',
            'check', '--format', 'github', '--base', 'base-ref'
        )
    }

    It 'writes <Expected> before a failing check and removes its report directory' -ForEach @(
        @{ Json = '[]'; Expected = 'semver_targets=' },
        @{ Json = '["alpha"]'; Expected = 'semver_targets=alpha' },
        @{ Json = '["alpha","beta"]'; Expected = 'semver_targets=alpha beta' }
    ) {
        $githubOutput = Join-Path $TestDrive 'github-output'
        Remove-Item -LiteralPath $githubOutput -Force -ErrorAction SilentlyContinue
        $previousOutput = $env:GITHUB_OUTPUT
        $script:reportDirectory = $null
        Push-Location $TestDrive
        try {
            {
                Invoke-ValidateVersions -GitHubOutputPath $githubOutput -Base base-ref -Cargo {
                    param($Argument)
                    $global:LASTEXITCODE = 0
                    switch ($Argument[5]) {
                        'report' { $script:reportDirectory = $Argument[7] }
                        'semver-targets' {
                            $Argument | Should -Not -Contain '--base'
                            $Json
                        }
                        'check' {
                            Get-Content $githubOutput | Should -Be $Expected
                            $global:LASTEXITCODE = 2
                        }
                        default { throw 'unexpected Cargo operation' }
                    }
                }
            } | Should -Throw
            Get-Content $githubOutput | Should -Be $Expected
            Test-Path $script:reportDirectory | Should -BeFalse
            $env:GITHUB_OUTPUT | Should -Be $previousOutput
        } finally {
            Pop-Location
        }
    }

    It 'cleans up after <Failure> fails without emitting targets or running check' -ForEach @(
        @{ Failure = 'report' }, @{ Failure = 'semver-targets' }
    ) {
        $githubOutput = Join-Path $TestDrive 'failed-github-output'
        Remove-Item -LiteralPath $githubOutput -Force -ErrorAction SilentlyContinue
        $script:reportDirectory = $null
        $script:commands = [Collections.Generic.List[string]]::new()
        $previousOutput = $env:GITHUB_OUTPUT
        Push-Location $TestDrive
        try {
            {
                Invoke-ValidateVersions -GitHubOutputPath $githubOutput -Base 'base-ref' -Cargo {
                    param($Argument)
                    $command = $Argument[5]
                    $script:commands.Add($command)
                    $global:LASTEXITCODE = 0
                    if ($command -eq 'report') {
                        $script:reportDirectory = $Argument[7]
                    }
                    if ($command -eq $Failure) {
                        $global:LASTEXITCODE = 7
                        'unusable output'
                    }
                }
            } | Should -Throw
            $script:commands | Should -Not -Contain 'check'
            Test-Path $script:reportDirectory | Should -BeFalse
            Test-Path $githubOutput | Should -BeFalse
            $env:GITHUB_OUTPUT | Should -Be $previousOutput
        } finally {
            Pop-Location
        }
    }
}

Describe 'Publication probes after Rust artifact inspection' {
    It 'queries only Rust-selected publishable names and forwards the workspace manifest' {
        $script:queries = [Collections.Generic.List[string]]::new()
        Assert-IncrementPackagePublished -ExpandedPath 'expanded.json' `
            -ManifestPath 'workspace\Cargo.toml' -Cargo {
                param($Argument)
                $Argument | Should -Be @(
                    'run', '-p', 'cargo-release-plan', '--locked', '--',
                    'inspect-plan', '--plan', 'expanded.json', '--manifest-path', 'workspace\Cargo.toml'
                )
                $global:LASTEXITCODE = 0
                '{"publication_targets":["alpha","beta"],"evidence_manifest_path":null}'
            } -GetPublishStatus {
                param($Name)
                $script:queries.Add($Name)
                'Published'
            }
        $script:queries | Should -Be @('alpha', 'beta')
    }

    It 'does not query the registry for helper-only plans' {
        Assert-IncrementPackagePublished -ExpandedPath 'expanded.json' -Cargo {
            $global:LASTEXITCODE = 0
            '{"publication_targets":[],"evidence_manifest_path":null}'
        } -GetPublishStatus { throw 'must not query' }
    }

    It 'does not query the registry if Rust rejects the artifact' {
        $script:queried = $false
        {
            Assert-IncrementPackagePublished -ExpandedPath 'expanded.json' -Cargo {
                $global:LASTEXITCODE = 4
                '{"publication_targets":["alpha"],"evidence_manifest_path":null}'
            } -GetPublishStatus { $script:queried = $true; 'Published' }
        } | Should -Throw
        $script:queried | Should -BeFalse
    }

    It 'retries only Unknown status: <Status>' -ForEach @(
        @{ Status = 'recover'; Count = 2; Fails = $false },
        @{ Status = 'unknown'; Count = 2; Fails = $true },
        @{ Status = 'never'; Count = 1; Fails = $true },
        @{ Status = 'throw'; Count = 1; Fails = $true }
    ) {
        $script:count = 0
        $action = {
            Assert-IncrementPackagePublished -ExpandedPath 'expanded.json' `
                -PublishStatusRetryAttempt 2 -PublishStatusRetryDelaySeconds 0 -Cargo {
                    $global:LASTEXITCODE = 0
                    '{"publication_targets":["alpha"],"evidence_manifest_path":null}'
                } -GetPublishStatus {
                    $script:count++
                    switch ($Status) {
                        'recover' { if ($script:count -eq 1) { 'Unknown' } else { 'Published' } }
                        'unknown' { 'Unknown' }
                        'never' { 'NeverPublished' }
                        'throw' { throw [IO.IOException]::new('registry canary') }
                    }
                }
        }
        if ($Fails) { $action | Should -Throw } else { & $action }
        $script:count | Should -Be $Count
    }

    It 'checks every target even when multiple packages have never been published' {
        $script:queries = [Collections.Generic.List[string]]::new()
        {
            Assert-IncrementPackagePublished -ExpandedPath 'expanded.json' -Cargo {
                $global:LASTEXITCODE = 0
                '{"publication_targets":["alpha","beta"],"evidence_manifest_path":null}'
            } -GetPublishStatus {
                param($Name)
                $script:queries.Add($Name)
                'NeverPublished'
            }
        } | Should -Throw
        $script:queries | Should -Be @('alpha', 'beta')
    }
}
