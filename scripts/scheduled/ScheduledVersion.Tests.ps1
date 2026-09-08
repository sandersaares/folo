#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'ScheduledVersion.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'ScheduledContracts.psm1') -Force
}
Describe 'Canonical version evidence' {
    It 'requires the exact independently regenerated expansion and baseline' {
        $expanded = @{ schema_version = 2; increments = @(@{ name = 'sample'; version = '0.2.1' }) }
        $evidence = @{
            pre_version_sha = 'a' * 40; base_sha = 'b' * 40; decisions = @()
            expanded_plan = $expanded; expanded_plan_digest = Get-ScheduledDigest $expanded
        }
        { Assert-ScheduledVersionArtifact -Evidence $evidence -BaseSha ('b' * 40) -Expanded $expanded } |
            Should -Not -Throw
        { Assert-ScheduledVersionArtifact -Evidence $evidence -BaseSha ('c' * 40) -Expanded $expanded } |
            Should -Throw
        $changed = @{ schema_version = 2; increments = @(@{ name = 'sample'; version = '0.3.0' }) }
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
}
