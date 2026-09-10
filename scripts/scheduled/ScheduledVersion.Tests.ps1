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
    It 'rejects missing canonical evidence field <Field>' -TestCases @(
        @{ Field = 'pre_version_sha' }, @{ Field = 'base_sha' }, @{ Field = 'decisions' }
        @{ Field = 'expanded_plan' }, @{ Field = 'expanded_plan_digest' }
    ) {
        param($Field)
        $expanded = @{ schema_version = $script:ReleasePlanSchemaVersion; expanded = $true; increments = @() }
        $evidence = @{
            pre_version_sha = 'a' * 40; base_sha = 'b' * 40; decisions = @()
            expanded_plan = $expanded; expanded_plan_digest = Get-ScheduledDigest $expanded
        }
        $evidence.Remove($Field)
        { Assert-ScheduledVersionArtifact -Evidence $evidence -BaseSha ('b' * 40) -Expanded $expanded } |
            Should -Throw
    }

    It 'rejects <Kind> without trusting the recorded digest alone' -TestCases @(
        @{ Kind = 'altered digest' }, @{ Kind = 'altered plan with matching claimed digest' }
        @{ Kind = 'missing schema' }, @{ Kind = 'non-object plan' }
        @{ Kind = 'machine-bound regenerated plan' }, @{ Kind = 'invalid checkpoint' }
    ) {
        param($Kind)
        $expanded = @{ schema_version = $script:ReleasePlanSchemaVersion; expanded = $true; increments = @() }
        $evidence = @{
            pre_version_sha = 'a' * 40; base_sha = 'b' * 40; decisions = @()
            expanded_plan = $expanded.Clone(); expanded_plan_digest = Get-ScheduledDigest $expanded
        }
        switch ($Kind) {
            'altered digest' { $evidence.expanded_plan_digest = 'not-the-generated-digest' }
            'altered plan with matching claimed digest' {
                $evidence.expanded_plan.increments = @(@{ name = 'extra'; version = '9.0.0' })
            }
            'missing schema' { $evidence.expanded_plan.Remove('schema_version') }
            'non-object plan' { $evidence.expanded_plan = @() }
            'machine-bound regenerated plan' { $expanded.resolved = @{ inputs = @{} } }
            'invalid checkpoint' { $evidence.pre_version_sha = 'not-a-commit' }
        }
        { Assert-ScheduledVersionArtifact -Evidence $evidence -BaseSha ('b' * 40) -Expanded $expanded } |
            Should -Throw
    }

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

    It 'rejects equal-sized Cargo trees with substituted paths or case-altered bytes' {
        $expected = @{ 'Cargo.toml' = 'manifest'; 'Cargo.lock' = 'lock' }
        $replaced = @{ 'Cargo.toml' = 'manifest'; 'nested/Cargo.lock' = 'lock' }
        $changedCase = @{ 'Cargo.toml' = 'MANIFEST'; 'Cargo.lock' = 'lock' }
        { Assert-ScheduledVersionFile -Expected $expected -Actual $replaced } | Should -Throw
        { Assert-ScheduledVersionFile -Expected $expected -Actual $changedCase } | Should -Throw
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

Describe 'Canonical regeneration process boundaries' {
    It 'rejects <Kind> before allocating a reference worktree' -TestCases @(
        @{ Kind = 'stale baseline' }, @{ Kind = 'relative executable' }
        @{ Kind = 'missing executable' }, @{ Kind = 'source changed after planning' }
        @{ Kind = 'non-ancestor checkpoint' }
    ) {
        param($Kind)
        InModuleScope ScheduledVersion -Parameters @{ Kind = $Kind } {
            param($Kind)
            $executable = (Get-Command pwsh).Source
            $evidence = @{ base_sha = 'a' * 40; pre_version_sha = 'b' * 40 }
            switch ($Kind) {
                'stale baseline' { $evidence.base_sha = 'd' * 40 }
                'relative executable' { $executable = 'release-plan.exe' }
                'missing executable' { $executable = Join-Path $TestDrive 'absent.exe' }
            }
            Mock Assert-ScheduledVersionReference {}
            Mock git {
                if ($args[2] -eq 'merge-base' -and $Kind -eq 'non-ancestor checkpoint') {
                    throw [InvalidOperationException]::new('Ancestry canary')
                }
                if ($args[2] -eq 'diff') { 'src/lib.rs' }
            }
            $temporary = Join-Path $TestDrive 'no-reference'
            {
                Assert-ScheduledCanonicalVersion -Root $TestDrive -HeadSha ('c' * 40) `
                    -BaseSha ('a' * 40) -Evidence $evidence -ReleasePlanExecutable $executable `
                    -TrustedControllerRoot $TestDrive -TemporaryRoot $temporary
            } | Should -Throw
            Test-Path -LiteralPath $temporary | Should -BeFalse
            Should -Invoke git -Times 0 -Exactly -ParameterFilter { $args[2] -eq 'worktree' }
        }
    }

    It 'uses only controller tooling and removes its owned reference after <Outcome>' -TestCases @(
        @{ Outcome = 'success' }, @{ Outcome = 'prepare' }, @{ Outcome = 'preview' }
        @{ Outcome = 'expand' }, @{ Outcome = 'publication' }, @{ Outcome = 'apply' }
        @{ Outcome = 'unexpected tracked file' }
    ) {
        param($Outcome)
        $controller = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
        InModuleScope ScheduledVersion -Parameters @{ Outcome = $Outcome; Controller = $controller } {
            param($Outcome, $Controller)
            Import-Module (Join-Path $Controller 'scripts\release\ReleasePlan.psm1')
            $fixture = Join-Path $TestDrive "controller-tool-$Outcome"
            $candidate = Join-Path $TestDrive "candidate-$Outcome"
            $temporary = Join-Path $TestDrive "references-$Outcome"
            New-Item -ItemType Directory -Path $fixture, $candidate, $temporary | Out-Null
            'caller-owned' | Set-Content -LiteralPath (Join-Path $temporary 'keep.txt')
            $expanded = @{ schema_version = 4; expanded = $true; increments = @() }
            $expanded | ConvertTo-Json -Depth 4 |
                Set-Content -LiteralPath (Join-Path $fixture 'expanded.json')
            $Outcome | Set-Content -LiteralPath (Join-Path $fixture 'outcome.txt')
            $executable = Join-Path $fixture 'release-plan.ps1'
            @'
# Implements only the established release-plan process boundary for canonical-verifier tests.
# Fixtures stay in the owned reference; no Cargo resolution or publication is performed.
param([Parameter(ValueFromRemainingArguments)][string[]] $Argument)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
$VerbosePreference = 'Continue'
@{ arguments = @($Argument); cwd = (Get-Location).Path } |
    ConvertTo-Json -Depth 3 -Compress |
    Add-Content -LiteralPath (Join-Path $PSScriptRoot 'calls.jsonl')
if ($Argument[0] -eq (Get-Content -LiteralPath (Join-Path $PSScriptRoot 'outcome.txt'))) {
    throw [IO.IOException]::new('Process canary')
}
switch ($Argument[0]) {
    prepare {
        $output = $Argument[[array]::IndexOf($Argument, '--output') + 1]
        '{"schema_version":4,"inputs":{}}' |
            Set-Content -LiteralPath (Join-Path $output 'prepared.json')
        '{"schema_version":4,"packages":[],"non_publishable_packages":[],"groups":{}}' |
            Set-Content -LiteralPath (Join-Path $output 'report.json')
    }
    preview {
        $output = $Argument[[array]::IndexOf($Argument, '--output') + 1]
        New-Item -ItemType Directory -Path $output | Out-Null
        '{"schema_version":4,"expanded":true,"increments":[],"resolved":{"captured":true}}' |
            Set-Content -LiteralPath (Join-Path $output 'plan.json')
    }
    expand {
        $output = $Argument[[array]::IndexOf($Argument, '--out') + 1]
        Copy-Item -LiteralPath (Join-Path $PSScriptRoot 'expanded.json') -Destination $output
    }
}
$global:LASTEXITCODE = 0
'@ | Set-Content -LiteralPath $executable
            $script:reference = $null
            $script:publicationPath = $null
            Mock Assert-ScheduledVersionReference {}
            Mock Get-ScheduledGitCargoFile { return @{ 'Cargo.toml' = 'canonical-blob' } }
            Mock git {
                if ($args[2] -eq 'worktree' -and $args[3] -eq 'add') {
                    $script:reference = $args[5]
                    New-Item -ItemType Directory -Path $script:reference | Out-Null
                    'manifest' | Set-Content -LiteralPath (Join-Path $script:reference 'Cargo.toml')
                } elseif ($args[2] -eq 'worktree' -and $args[3] -eq 'remove') {
                    Remove-Item -LiteralPath $args[5] -Recurse -Force
                } elseif ($args[0] -eq 'hash-object') {
                    'canonical-blob'
                } elseif ($args[0] -eq 'status' -and $Outcome -eq 'unexpected tracked file') {
                    ' M src/lib.rs'
                }
            }
            Mock Assert-IncrementPackagePublished {
                $script:publicationPath = $ExpandedPath
                (Get-Content -LiteralPath $ExpandedPath -Raw | ConvertFrom-Json).resolved.captured |
                    Should -BeTrue
                $calls = @(Get-Content -LiteralPath (Join-Path $fixture 'calls.jsonl') |
                    ForEach-Object { $_ | ConvertFrom-Json })
                @($calls | ForEach-Object { $_.arguments[0] }) | Should -Be @(
                    'prepare', 'preview', 'expand'
                )
                if ($Outcome -eq 'publication') {
                    throw [InvalidOperationException]::new('Publication canary')
                }
            }
            $evidence = @{
                pre_version_sha = 'b' * 40; base_sha = 'a' * 40
                decisions = @{ schema_version = 1; changes = @() }
                expanded_plan = $expanded; expanded_plan_digest = Get-ScheduledDigest $expanded
            }
            $location = (Get-Location).Path
            $action = {
                Assert-ScheduledCanonicalVersion -Root $candidate -HeadSha ('c' * 40) `
                    -BaseSha ('a' * 40) -Evidence $evidence -ReleasePlanExecutable $executable `
                    -TrustedControllerRoot $Controller -TemporaryRoot $temporary
            }
            if ($Outcome -eq 'success') {
                & $action
            } elseif ($Outcome -eq 'unexpected tracked file') {
                $action | Should -Throw
            } else {
                $action | Should -Throw '*canary*'
            }
            (Get-Location).Path | Should -Be $location
            $script:reference | Should -Not -BeNullOrEmpty
            Split-Path -Parent $script:reference | Should -Be $temporary
            Test-Path -LiteralPath $script:reference | Should -BeFalse
            Get-Content -LiteralPath (Join-Path $temporary 'keep.txt') | Should -Be 'caller-owned'
            $calls = @(Get-Content -LiteralPath (Join-Path $fixture 'calls.jsonl') |
                ForEach-Object { $_ | ConvertFrom-Json })
            $expectedOperations = switch ($Outcome) {
                'prepare' { @('prepare') }
                'preview' { @('prepare', 'preview') }
                'expand' { @('prepare', 'preview', 'expand') }
                'publication' { @('prepare', 'preview', 'expand') }
                default { @('prepare', 'preview', 'expand', 'apply') }
            }
            @($calls | ForEach-Object { $_.arguments[0] }) | Should -Be $expectedOperations
            @($calls.cwd | Sort-Object -Unique) | Should -Be @($script:reference)
            $artifacts = Join-Path $script:reference '.scheduled-version-evidence'
            $calls[0].arguments | Should -Be @('prepare', '--base', ('a' * 40), '--output', $artifacts)
            if ($calls.Count -gt 1) {
                $calls[1].arguments | Should -Be @(
                    'preview', '--prepared', (Join-Path $artifacts 'prepared.json'),
                    '--plan', (Join-Path $artifacts 'proposed.json'),
                    '--output', (Join-Path $artifacts 'preview')
                )
            }
            if ($calls.Count -gt 2) {
                $calls[2].arguments | Should -Be @(
                    'expand', '--plan', (Join-Path $artifacts 'preview\plan.json'),
                    '--out', (Join-Path $artifacts 'expanded.json')
                )
            }
            if ($calls.Count -gt 3) {
                $calls[3].arguments | Should -Be @('apply', '--plan', $script:publicationPath)
                $script:publicationPath | Should -Be (Join-Path $artifacts 'preview\plan.json')
            }
            Should -Invoke git -Times 1 -Exactly -ParameterFilter {
                ($args -join '|') -eq (@(
                    '-C', $candidate, 'worktree', 'remove', '--force', $script:reference
                ) -join '|')
            }
            $comparisonCount = if ($Outcome -in @('success', 'unexpected tracked file')) { 2 } else { 0 }
            Should -Invoke Get-ScheduledGitCargoFile -Times $comparisonCount -Exactly
        }
    }
}

Describe 'Scheduled version verification entry point' {
    It 'builds trusted tooling once and verifies only managed repairs' {
        InModuleScope ScheduledVersion {
            $planPath = Join-Path $TestDrive 'verification.json'
            @{
                managed = $true; release_base_sha = 'a' * 40
                repairs = @(
                    @{ managed = $false; head_sha = 'd' * 40; version_evidence = $null }
                    @{ managed = $true; head_sha = 'b' * 40; version_evidence = @{ identity = 'first' } }
                    @{ managed = $true; head_sha = 'c' * 40; version_evidence = @{ identity = 'second' } }
                )
            } | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $planPath
            $script:operations = [System.Collections.Generic.List[string]]::new()
            Mock cargo { $script:operations.Add('build') }
            Mock git { $script:operations.Add("fetch:$($args[-1])") }
            Mock Assert-ScheduledCanonicalVersion { $script:operations.Add("verify:$HeadSha") }

            Invoke-ScheduledVersionVerification -PlanPath $planPath

            $expectedRoot = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
            $target = Join-Path $expectedRoot 'target\scheduled-version'
            $script:operations | Should -Be @(
                'build', "fetch:$('b' * 40)", "verify:$('b' * 40)",
                "fetch:$('c' * 40)", "verify:$('c' * 40)"
            )
            Should -Invoke cargo -Times 1 -Exactly -ParameterFilter {
                ($args -join '|') -eq (@(
                    'build', '--manifest-path', (Join-Path $expectedRoot 'Cargo.toml'),
                    '-p', 'cargo-release-plan', '--locked', '--target-dir', $target
                ) -join '|')
            }
            Should -Invoke git -Times 2 -Exactly -ParameterFilter {
                $args[0] -eq '-C' -and $args[1] -eq $expectedRoot -and
                ($args[2..5] -join '|') -eq 'fetch|--no-tags|origin|' + $args[5]
            }
            Should -Invoke Assert-ScheduledCanonicalVersion -Times 2 -Exactly -ParameterFilter {
                $Root -eq $expectedRoot -and $BaseSha -eq ('a' * 40) -and
                $TrustedControllerRoot -eq $expectedRoot -and
                $TemporaryRoot -eq (Join-Path $expectedRoot '.scheduled-version-reference') -and
                $ReleasePlanExecutable -like "$target*"
            }
            Should -Invoke Assert-ScheduledCanonicalVersion -Times 1 -Exactly -ParameterFilter {
                $HeadSha -eq ('b' * 40) -and $Evidence.identity -eq 'first'
            }
            Should -Invoke Assert-ScheduledCanonicalVersion -Times 1 -Exactly -ParameterFilter {
                $HeadSha -eq ('c' * 40) -and $Evidence.identity -eq 'second'
            }
        }
    }

    It 'stops before later side effects when <Failure>' -TestCases @(
        @{ Failure = 'candidate is unmanaged'; Builds = 0; Fetches = 0 }
        @{ Failure = 'build fails'; Builds = 1; Fetches = 0 }
        @{ Failure = 'evidence is absent'; Builds = 1; Fetches = 0 }
        @{ Failure = 'fetch fails'; Builds = 1; Fetches = 1 }
    ) {
        param($Failure, $Builds, $Fetches)
        InModuleScope ScheduledVersion -Parameters @{
            Failure = $Failure; Builds = $Builds; Fetches = $Fetches
        } {
            param($Failure, $Builds, $Fetches)
            $planPath = Join-Path $TestDrive 'rejected-verification.json'
            @{
                managed = $Failure -ne 'candidate is unmanaged'; release_base_sha = 'a' * 40
                repairs = @(@{
                    managed = $true; head_sha = 'b' * 40
                    version_evidence = $(if ($Failure -eq 'evidence is absent') { $null } else { @{} })
                })
            } | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $planPath
            Mock cargo {
                if ($Failure -eq 'build fails') { throw [IO.IOException]::new('Build canary') }
            }
            Mock git { throw [IO.IOException]::new('Fetch canary') }
            Mock Assert-ScheduledCanonicalVersion {}
            { Invoke-ScheduledVersionVerification -PlanPath $planPath } | Should -Throw
            Should -Invoke cargo -Times $Builds -Exactly
            Should -Invoke git -Times $Fetches -Exactly
            Should -Invoke Assert-ScheduledCanonicalVersion -Times 0 -Exactly
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
