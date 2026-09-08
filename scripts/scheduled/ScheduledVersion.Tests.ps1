#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
# Protects the version-verification contract: a worker's published version plan must be accepted
# only when it matches an independently regenerated expansion against the recorded baseline
# byte-for-byte, so a worker cannot make its own version numbers authoritative by publishing them.
BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'ScheduledVersion.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'ScheduledContracts.psm1') -Force
}
Describe 'Canonical version evidence' {
    It 'requires the exact independently regenerated expansion and baseline' {
        $expanded = @{ schema_version = 2; expanded = $true; increments = @(@{ name = 'sample'; version = '0.2.1' }) }
        $evidence = @{
            pre_version_sha = 'a' * 40; base_sha = 'b' * 40; decisions = @()
            expanded_plan = $expanded; expanded_plan_digest = Get-ScheduledDigest $expanded
        }
        { Assert-ScheduledVersionArtifact -Evidence $evidence -BaseSha ('b' * 40) -Expanded $expanded } |
            Should -Not -Throw
        { Assert-ScheduledVersionArtifact -Evidence $evidence -BaseSha ('c' * 40) -Expanded $expanded } |
            Should -Throw
        $changed = @{ schema_version = 2; expanded = $true; increments = @(@{ name = 'sample'; version = '0.3.0' }) }
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
        $expanded = @{ schema_version = 2; expanded = $true; increments = @() }
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
}
