#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
# Protects the version-verification contract: a worker's published version plan must be accepted
# only when it matches an independently regenerated expansion against the recorded baseline
# byte-for-byte, so a worker cannot make its own version numbers authoritative by publishing them.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
$VerbosePreference = 'Continue'

BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'ScheduledVersion.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'ScheduledContracts.psm1') -Force
    # Current cargo-release-plan wire contract; older artifacts require regeneration.
    $script:ReleasePlanSchemaVersion = 4
}
Describe 'Canonical version evidence' {
    It 'requires the exact independently regenerated expansion and baseline' {
        $expanded = @{ schema_version = $script:ReleasePlanSchemaVersion; expanded = $true; increments = @(@{ name = 'sample'; version = '0.2.1' }) }
        $evidence = @{
            pre_version_sha = 'a' * 40; base_sha = 'b' * 40; decisions = @()
            expanded_plan = $expanded; expanded_plan_digest = Get-ScheduledDigest $expanded
        }
        { Assert-ScheduledVersionArtifact -Evidence $evidence -BaseSha ('b' * 40) -Expanded $expanded } |
            Should -Not -Throw
        { Assert-ScheduledVersionArtifact -Evidence $evidence -BaseSha ('c' * 40) -Expanded $expanded } |
            Should -Throw
        $changed = @{ schema_version = $script:ReleasePlanSchemaVersion; expanded = $true; increments = @(@{ name = 'sample'; version = '0.3.0' }) }
        { Assert-ScheduledVersionArtifact -Evidence $evidence -BaseSha ('b' * 40) -Expanded $changed } |
            Should -Throw
    }
    It 'rejects extra missing or altered manifests and lockfile bytes' {
        $expected = @{ 'Cargo.lock' = 'lock'; 'Cargo.toml' = 'manifest'; 'packages/dependent/Cargo.toml' = 'dependent' }
        { Assert-ScheduledVersionFile -Expected $expected -Actual $expected } | Should -Not -Throw
        foreach ($mutation in @('extra', 'missing', 'lock', 'dependent')) {
            $actual = $expected.Clone()
            switch ($mutation) {
                extra { $actual['packages/new/Cargo.toml'] = 'new' }
                missing { $actual.Remove('Cargo.lock') }
                lock { $actual['Cargo.lock'] = 'external-resolution-change' }
                dependent { $actual['packages/dependent/Cargo.toml'] = 'feature-change' }
            }
            { Assert-ScheduledVersionFile -Expected $expected -Actual $actual } | Should -Throw
        }
    }
    It 'accepts an empty expansion without bypassing Cargo byte comparison' {
        $expanded = @{ schema_version = $script:ReleasePlanSchemaVersion; expanded = $true; increments = @() }
        $evidence = @{
            pre_version_sha = 'a' * 40; base_sha = 'b' * 40
            decisions = @{ schema_version = 1; changes = @() }
            expanded_plan = $expanded; expanded_plan_digest = Get-ScheduledDigest $expanded
        }
        { Assert-ScheduledVersionArtifact -Evidence $evidence -BaseSha ('b' * 40) -Expanded $expanded } |
            Should -Not -Throw
        $unchanged = @{ 'Cargo.toml' = 'already-versioned'; 'Cargo.lock' = 'already-refreshed' }
        { Assert-ScheduledVersionFile -Expected $unchanged -Actual $unchanged } | Should -Not -Throw
    }

    It 'rejects stale schema and machine-bound artifacts rather than discarding their fields' {
        $expanded = @{ schema_version = $script:ReleasePlanSchemaVersion; expanded = $true; increments = @() }
        foreach ($kind in @('schema', 'machine-bound')) {
            $recorded = $expanded.Clone()
            if ($kind -eq 'schema') {
                $recorded.schema_version = $script:ReleasePlanSchemaVersion - 1
            } else {
                $recorded.resolved = @{ inputs = @{ root = 'worker-specific-path' } }
            }
            $evidence = @{
                pre_version_sha = 'a' * 40; base_sha = 'b' * 40; decisions = @()
                expanded_plan = $recorded; expanded_plan_digest = Get-ScheduledDigest $recorded
            }
            { Assert-ScheduledVersionArtifact -Evidence $evidence -BaseSha ('b' * 40) -Expanded $expanded } |
                Should -Throw
        }
    }
}

Describe 'Resolved canonical regeneration' {
    It 'reproduces portable targets and exact Cargo bytes in an independent worktree' {
        $controller = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
        Import-Module (Join-Path $controller 'scripts\build\CargoExecutable.psm1') -Force
        Import-Module (Join-Path $controller 'scripts\release\ReleasePlan.psm1') -Force
        $messages = @(& cargo build --manifest-path (Join-Path $controller 'Cargo.toml') `
                -p cargo-release-plan --bin cargo-release-plan --locked --message-format=json)
        $executable = Resolve-CargoExecutable -CargoMessage $messages -TargetName 'cargo-release-plan'

        $root = Join-Path $TestDrive 'worker'
        $artifacts = Join-Path $TestDrive 'worker-evidence'
        $temporary = Join-Path $TestDrive 'references'
        New-Item -ItemType Directory -Path (Join-Path $root 'src'), $artifacts | Out-Null
        $manifest = Join-Path $root 'Cargo.toml'
        @'
[package]
name = "sample"
version = "1.0.0"
edition = "2024"
include = ["src/**"]

[package.metadata.release-plan]
private-api = true

[workspace]
'@ | Set-Content -LiteralPath $manifest
        'pub fn value() -> u8 { 1 }' | Set-Content -LiteralPath (Join-Path $root 'src\lib.rs')
        $gitArguments = @(
            '-c', 'user.name=Scheduled fixture', '-c', 'user.email=scheduled@example.invalid'
            '-c', 'commit.gpgsign=false', '-c', 'gc.auto=0', '-C', $root
        )
        & git @gitArguments init --quiet
        & cargo generate-lockfile --offline --manifest-path $manifest
        & git @gitArguments add .
        & git @gitArguments commit --quiet -m baseline
        $baseline = (& git @gitArguments rev-parse HEAD).Trim()
        'pub fn value() -> u8 { 2 }' | Set-Content -LiteralPath (Join-Path $root 'src\lib.rs')
        & git @gitArguments add .
        & git @gitArguments commit --quiet -m 'source repair'
        $preVersion = (& git @gitArguments rev-parse HEAD).Trim()

        $decisions = @{ schema_version = 1; changes = @(@{ name = 'sample'; level = 'patch' }) }
        $decisionPath = Join-Path $artifacts 'decisions.json'
        $proposedPath = Join-Path $artifacts 'proposed.json'
        $preview = Join-Path $artifacts 'preview'
        $resolvedPath = Join-Path $preview 'plan.json'
        $portablePath = Join-Path $artifacts 'expanded.json'
        $decisions | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath $decisionPath
        & $executable prepare --manifest-path $manifest --base $baseline --output $artifacts
        New-ReleasePlanFile -ReportPath (Join-Path $artifacts 'report.json') `
            -DecisionPath $decisionPath -PlanPath $proposedPath -Confirm:$false
        & $executable preview --manifest-path $manifest --prepared (Join-Path $artifacts 'prepared.json') `
            --plan $proposedPath --output $preview
        & $executable expand --manifest-path $manifest --plan $resolvedPath --out $portablePath
        $expanded = Get-Content -LiteralPath $portablePath -Raw | ConvertFrom-Json -AsHashtable
        $expanded.ContainsKey('resolved') | Should -BeFalse
        $expanded.increments[0].version | Should -Be '1.0.1'
        & $executable apply --manifest-path $manifest --plan $resolvedPath
        & git @gitArguments add Cargo.toml Cargo.lock
        & git @gitArguments commit --quiet -m 'resolved versions'
        $head = (& git @gitArguments rev-parse HEAD).Trim()
        $evidence = @{
            pre_version_sha = $preVersion; base_sha = $baseline; decisions = $decisions
            expanded_plan = $expanded; expanded_plan_digest = Get-ScheduledDigest $expanded
        }
        $assertPublished = {
            param([string] $Path)
            $plan = Get-Content -LiteralPath $Path -Raw | ConvertFrom-Json -AsHashtable
            $plan.ContainsKey('resolved') | Should -BeTrue
        }
        Assert-ScheduledCanonicalVersion -Root $root -HeadSha $head -BaseSha $baseline `
            -Evidence $evidence -ReleasePlanExecutable $executable -TrustedControllerRoot $controller `
            -TemporaryRoot $temporary -AssertPublished $assertPublished
        @(Get-ChildItem -LiteralPath $temporary -Directory).Count | Should -Be 0

        # Matching portable targets do not excuse ungenerated changes in the committed lockfile.
        '# Not part of the captured result.' | Add-Content -LiteralPath (Join-Path $root 'Cargo.lock')
        & git @gitArguments add Cargo.lock
        & git @gitArguments commit --quiet -m 'altered lockfile'
        $head = (& git @gitArguments rev-parse HEAD).Trim()
        {
            Assert-ScheduledCanonicalVersion -Root $root -HeadSha $head -BaseSha $baseline `
                -Evidence $evidence -ReleasePlanExecutable $executable -TrustedControllerRoot $controller `
                -TemporaryRoot $temporary -AssertPublished $assertPublished
        } | Should -Throw
        @(Get-ChildItem -LiteralPath $temporary -Directory).Count | Should -Be 0
    }
}

Describe 'Independent pre-version checkpoint' {
    It 'rejects an already-versioned reference when head equality is <SameHead>' -TestCases @(
        @{ SameHead = $true }, @{ SameHead = $false }
    ) {
        param($SameHead)
        $root = Join-Path $TestDrive "already-versioned-$SameHead"
        New-Item -ItemType Directory -Path $root | Out-Null
        $gitArguments = @(
            '-c', 'user.name=Scheduled fixture', '-c', 'user.email=scheduled@example.invalid'
            '-c', 'commit.gpgsign=false', '-c', 'gc.auto=0', '-C', $root
        )
        & git @gitArguments init --quiet
        '[package]', 'name = "sample"', 'version = "1.0.0"' |
            Set-Content -LiteralPath (Join-Path $root 'Cargo.toml')
        & git @gitArguments add .
        & git @gitArguments commit --quiet -m baseline
        $baseline = (& git @gitArguments rev-parse HEAD).Trim()
        '[package]', 'name = "sample"', 'version = "9.0.0"' |
            Set-Content -LiteralPath (Join-Path $root 'Cargo.toml')
        & git @gitArguments add .
        & git @gitArguments commit --quiet -m 'unproven version movement'
        $reference = (& git @gitArguments rev-parse HEAD).Trim()
        if (-not $SameHead) {
            'Source repair' | Set-Content -LiteralPath (Join-Path $root 'source.txt')
            & git @gitArguments add .
            & git @gitArguments commit --quiet -m 'source repair'
        }
        $head = (& git @gitArguments rev-parse HEAD).Trim()
        {
            Assert-ScheduledCanonicalVersion -Root $root -BaseSha $baseline -HeadSha $head `
                -Evidence @{ base_sha = $baseline; pre_version_sha = $reference } `
                -ReleasePlanExecutable (Get-Command pwsh).Source `
                -TrustedControllerRoot $root -TemporaryRoot (Join-Path $root 'scratch')
        } | Should -Throw '*trusted release baseline*'
        Test-Path -LiteralPath (Join-Path $root 'scratch') | Should -BeFalse
    }

    It 'allows source-only changes with the complete trusted baseline Cargo tree' {
        InModuleScope ScheduledVersion {
            Mock Get-ScheduledGitCargoFile {
                return @{ 'Cargo.toml' = 'manifest'; 'Cargo.lock' = 'lock'; 'packages/sample/Cargo.toml' = 'package' }
            }
            { Assert-ScheduledVersionReference -Root $TestDrive -BaseSha ('a' * 40) -PreVersionSha ('b' * 40) } |
                Should -Not -Throw
            Should -Invoke Get-ScheduledGitCargoFile -Times 1 -ParameterFilter { $Revision -eq ('a' * 40) }
            Should -Invoke Get-ScheduledGitCargoFile -Times 1 -ParameterFilter { $Revision -eq ('b' * 40) }
        }

    }

    It 'rejects a pre-version checkpoint already carrying <Kind>' -TestCases @(
        @{ Kind = 'arbitrary version movement'; ChangedPath = 'packages/sample/Cargo.toml' }
        @{ Kind = 'an unrelated dependency lock'; ChangedPath = 'Cargo.lock' }
        @{ Kind = 'an extra manifest'; ChangedPath = 'packages/extra/Cargo.toml' }
    ) {
        param($ChangedPath)
        InModuleScope ScheduledVersion -Parameters @{ ChangedPath = $ChangedPath } {
            param($ChangedPath)
            $baseline = @{ 'Cargo.toml' = 'manifest'; 'Cargo.lock' = 'lock'; 'packages/sample/Cargo.toml' = 'package' }
            $changed = $baseline.Clone()
            $changed[$ChangedPath] = 'worker-supplied-bytes'
            Mock Get-ScheduledGitCargoFile {
                if ($Revision -eq ('b' * 40)) { return $changed }
                return $baseline
            }
            { Assert-ScheduledVersionReference -Root $TestDrive -BaseSha ('a' * 40) -PreVersionSha ('b' * 40) } |
                Should -Throw '*trusted release baseline*'
        }
    }

    Describe 'Reference worktree cleanup' {
        It 'does not remove a reference when worktree creation fails' {
            InModuleScope ScheduledVersion {
                Mock Assert-ScheduledVersionReference {}
                Mock git {
                    if ($args[2] -eq 'worktree' -and $args[3] -eq 'add') {
                        throw [InvalidOperationException]::new('Creation canary')
                    }
                }
                {
                    Assert-ScheduledCanonicalVersion -Root $TestDrive -BaseSha ('a' * 40) -HeadSha ('b' * 40) `
                        -Evidence @{ base_sha = 'a' * 40; pre_version_sha = 'b' * 40 } `
                        -ReleasePlanExecutable (Get-Command pwsh).Source `
                        -TrustedControllerRoot $TestDrive -TemporaryRoot (Join-Path $TestDrive 'scratch')
                } | Should -Throw '*Creation canary*'
                Should -Invoke git -Times 0 -Exactly -ParameterFilter {
                    $args[2] -eq 'worktree' -and $args[3] -eq 'remove'
                }
            }
        }

        It 'preserves verification failure with cleanup failure <CleanupFails>' -TestCases @(
            @{ CleanupFails = $false }, @{ CleanupFails = $true }
        ) {
            param($CleanupFails)
            InModuleScope ScheduledVersion -Parameters @{ CleanupFails = $CleanupFails } {
                param($CleanupFails)
                Mock Assert-ScheduledVersionReference {}
                Mock git {
                    if ($args[2] -eq 'worktree' -and $args[3] -eq 'add') {
                        New-Item -ItemType Directory -Path $args[5] | Out-Null
                    }
                    if ($CleanupFails -and $args[2] -eq 'worktree' -and $args[3] -eq 'remove') {
                        throw [IO.IOException]::new('Cleanup canary')
                    }
                }
                Mock Import-Module { throw [InvalidOperationException]::new('Verification canary') } `
                    -ParameterFilter { $Name -like '*ReleasePlan.psm1' }
                $failure = $null
                try {
                    Assert-ScheduledCanonicalVersion -Root $TestDrive -BaseSha ('a' * 40) -HeadSha ('b' * 40) `
                        -Evidence @{ base_sha = 'a' * 40; pre_version_sha = 'b' * 40; decisions = @() } `
                        -ReleasePlanExecutable (Get-Command pwsh).Source `
                        -TrustedControllerRoot $TestDrive -TemporaryRoot (Join-Path $TestDrive 'scratch')
                } catch {
                    $failure = $_
                }
                if ($CleanupFails) {
                    $failure.Exception | Should -BeOfType ([AggregateException])
                    $failure.Exception.InnerExceptions.Count | Should -Be 2
                    $failure.Exception.InnerExceptions[0] | Should -BeOfType ([InvalidOperationException])
                    $failure.Exception.InnerExceptions[0].Message | Should -Be 'Verification canary'
                    $failure.Exception.InnerExceptions[1] | Should -BeOfType ([IO.IOException])
                    $failure.Exception.InnerExceptions[1].Message | Should -Be 'Cleanup canary'
                } else {
                    $failure.Exception | Should -BeOfType ([InvalidOperationException])
                    $failure.Exception.Message | Should -Be 'Verification canary'
                }
                Should -Invoke git -Times 1 -Exactly -ParameterFilter {
                    $args[2] -eq 'worktree' -and $args[3] -eq 'remove'
                }
            }
        }

        It 'surfaces cleanup failure after successful verification' {
            InModuleScope ScheduledVersion {
                Mock git { throw [IO.IOException]::new('Cleanup canary') }
                {
                    Remove-ScheduledVersionReference -Root $TestDrive -Reference (Join-Path $TestDrive 'reference') `
                        -VerificationError $null
                } | Should -Throw '*Cleanup canary*'
            }
        }

    }
}
