#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }

# Pester suite for BinstallValidation.psm1. Get-PublishableBinaryPackage runs a real
# `cargo metadata` against a self-contained fixture workspace whose members cover every branch of
# the validator (a compliant binary crate, a drifted one, one with no binstall block, one that
# names an unbuildable release target, a library, and a non-publishable binary). Test-BinstallMetadata
# and Test-ReleaseTargetMetadata are then exercised on those real package objects, and
# Invoke-BinstallValidation is checked both against the fixture (which must fail) and with a mocked
# package set (which must pass), so no test depends on the live repo manifest.

BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'BinstallValidation.psm1') -Force

    $script:FixtureManifest = Join-Path $PSScriptRoot 'fixtures/binstall-workspace/Cargo.toml'
    $script:Packages = @(Get-PublishableBinaryPackage -ManifestPath $script:FixtureManifest)

    # A hand-built package mirroring the shape of `cargo metadata` output for a compliant crate.
    $script:CompliantPackage = [pscustomobject]@{
        name     = 'crafted'
        targets  = @([pscustomobject]@{ name = 'crafted-bin'; kind = @('bin') })
        metadata = [pscustomobject]@{
            binstall = [pscustomobject]@{
                'pkg-url' = '{ repo }/releases/download/{ name }-v{ version }/{ name }-v{ version }-{ target }.zip'
                'bin-dir' = '{ bin }{ binary-ext }'
                'pkg-fmt' = 'zip'
            }
        }
    }
}

Describe 'Get-PublishableBinaryPackage (real cargo metadata on a fixture workspace)' {
    It 'selects publishable crates that own a bin target' {
        $script:Packages.name | Should -Contain 'good-tool'
        $script:Packages.name | Should -Contain 'bad-tool'
        $script:Packages.name | Should -Contain 'nometa-tool'
    }

    It 'excludes library-only crates' {
        $script:Packages.name | Should -Not -Contain 'some-lib'
    }

    It 'excludes non-publishable crates even when they own a bin target' {
        $script:Packages.name | Should -Not -Contain 'nopublish-tool'
    }

    It 'preserves the package metadata so binstall can be inspected' {
        $good = $script:Packages | Where-Object name -EQ 'good-tool'
        $good.metadata.binstall.'pkg-fmt' | Should -Be 'zip'
    }
}

Describe 'Test-BinstallMetadata (real fixture packages)' {
    It 'reports no problems for a compliant crate' {
        $good = $script:Packages | Where-Object name -EQ 'good-tool'
        @(Test-BinstallMetadata -Package $good).Count | Should -Be 0
    }

    It 'flags a missing [package.metadata.binstall] section' {
        $nometa = $script:Packages | Where-Object name -EQ 'nometa-tool'
        $problems = @(Test-BinstallMetadata -Package $nometa)
        $problems.Count | Should -Be 1
        $problems[0] | Should -Match 'missing \[package.metadata.binstall\] section'
    }

    It 'flags both a missing key and a wrong value' {
        $bad = $script:Packages | Where-Object name -EQ 'bad-tool'
        $problems = @(Test-BinstallMetadata -Package $bad)
        $problems.Count | Should -Be 2
        ($problems -join "`n") | Should -Match "missing 'bin-dir'"
        ($problems -join "`n") | Should -Match "'pkg-fmt' is 'tgz' but must be 'zip'"
    }
}

Describe 'Test-BinstallMetadata (crafted package objects)' {
    It 'accepts a hand-built compliant package' {
        @(Test-BinstallMetadata -Package $script:CompliantPackage).Count | Should -Be 0
    }

    It 'flags a package that has no metadata field at all' {
        $pkg = [pscustomobject]@{ name = 'crafted' }
        $problems = @(Test-BinstallMetadata -Package $pkg)
        $problems.Count | Should -Be 1
        $problems[0] | Should -Match 'missing \[package.metadata.binstall\] section'
    }
}

Describe 'Test-ReleaseTargetMetadata' {
    It 'reports no problems for a crate that declares no release targets' {
        $good = $script:Packages | Where-Object name -EQ 'good-tool'
        @(Test-ReleaseTargetMetadata -Package $good).Count | Should -Be 0
    }

    It 'flags a release target the workflow does not build' {
        $bad = $script:Packages | Where-Object name -EQ 'badtarget-tool'
        $problems = @(Test-ReleaseTargetMetadata -Package $bad)
        $problems.Count | Should -Be 1
        $problems[0] | Should -Match "release target 's390x-unknown-linux-gnu' is not one the release workflow builds"
    }

    It 'accepts a restriction drawn from the workflow target table' {
        $pkg = [pscustomobject]@{
            name     = 'crafted'
            metadata = [pscustomobject]@{
                folo = [pscustomobject]@{ 'release-targets' = @('x86_64-pc-windows-msvc') }
            }
        }
        @(Test-ReleaseTargetMetadata -Package $pkg).Count | Should -Be 0
    }
}

Describe 'Test-BinaryTargetMetadata' {
    It 'accepts one binary target even when its name differs from the package' {
        @(Test-BinaryTargetMetadata -Package $script:CompliantPackage).Count | Should -Be 0
    }

    It 'rejects a package with several binary targets' {
        $pkg = [pscustomobject]@{
            name = 'crafted'
            targets = @(
                [pscustomobject]@{ name = 'first'; kind = @('bin') }
                [pscustomobject]@{ name = 'second'; kind = @('bin') }
            )
        }
        $problems = @(Test-BinaryTargetMetadata -Package $pkg)
        $problems.Count | Should -Be 1
        ($problems[0] -is [string]) | Should -BeTrue
    }

    It 'rejects a package with no target metadata' {
        $problems =
            @(Test-BinaryTargetMetadata -Package ([pscustomobject]@{ name = 'crafted' }))
        $problems.Count | Should -Be 1
        ($problems[0] -is [string]) | Should -BeTrue
    }
}

Describe 'Invoke-BinstallValidation' {
    It 'throws and names every non-compliant crate when run against the fixture' {
        $err = { Invoke-BinstallValidation -ManifestPath $script:FixtureManifest } | Should -Throw -PassThru
        $message = $err.Exception.Message
        $message | Should -Match 'bad-tool'
        $message | Should -Match 'nometa-tool'
        $message | Should -Match 'badtarget-tool'
        # The compliant crate must not appear in the failure report.
        ($message -split "`n" | Where-Object { $_ -match '^good-tool:' }) | Should -BeNullOrEmpty
    }

    It 'does not throw when every publishable binary crate is compliant' {
        Mock Get-PublishableBinaryPackage -ModuleName BinstallValidation {
            @([pscustomobject]@{
                    name     = 'crafted'
                    targets  = @([pscustomobject]@{ name = 'crafted-bin'; kind = @('bin') })
                    metadata = [pscustomobject]@{
                        binstall = [pscustomobject]@{
                            'pkg-url' = '{ repo }/releases/download/{ name }-v{ version }/{ name }-v{ version }-{ target }.zip'
                            'bin-dir' = '{ bin }{ binary-ext }'
                            'pkg-fmt' = 'zip'
                        }
                    }
                })
        }
        { Invoke-BinstallValidation } | Should -Not -Throw
    }
}
