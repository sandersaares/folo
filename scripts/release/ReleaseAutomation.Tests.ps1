#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true
$VerbosePreference = 'Continue'

# Pester suite for ReleaseAutomation.psm1. Where it is safe on fixtures, the tests drive the
# real external tool: Get-PublishableBinaryCrate runs an actual `cargo metadata` against a
# fixture workspace, and New-ReleasePlzConfig / Set-GitHubOutput perform real file I/O (so
# encoding and line endings are asserted on the bytes on disk). The tools that would touch
# crates.io / GitHub for real -- `release-plz` and `gh` -- are isolated behind seams the tests
# mock in the module's scope.

BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'ReleaseAutomation.psm1') -Force

    $script:FixtureDir = Join-Path $PSScriptRoot 'fixtures'
    $script:MetadataManifest = Join-Path $script:FixtureDir 'metadata-workspace/Cargo.toml'
    $script:SampleToml = Join-Path $script:FixtureDir 'release-plz.sample.toml'
}

Describe 'Get-PublishableBinaryCrate (real cargo metadata on a fixture workspace)' {
    BeforeAll {
        $script:Crates = Get-PublishableBinaryCrate -ManifestPath $script:MetadataManifest
    }

    It 'selects publishable crates that own a bin target' {
        $script:Crates.Name | Should -Contain 'pub-bin'
        $script:Crates.Name | Should -Contain 'demo-tool'
    }

    It 'excludes library-only crates' {
        $script:Crates.Name | Should -Not -Contain 'pub-lib'
        $script:Crates.Name | Should -Not -Contain 'demo-tool-core'
    }

    It 'excludes non-publishable crates even when they own a bin target' {
        $script:Crates.Name | Should -Not -Contain 'nopub-bin'
    }

    It 'reports the manifest version for each selected crate' {
        ($script:Crates | Where-Object Name -EQ 'demo-tool').Version | Should -Be '2.3.4'
        ($script:Crates | Where-Object Name -EQ 'pub-bin').Version | Should -Be '0.1.0'
    }

    It 'returns crates sorted by name' {
        $script:Crates.Name | Should -Be @('demo-tool', 'pub-bin', 'win-tool')
    }

    It 'projects a declared release-target restriction onto the crate' {
        ($script:Crates | Where-Object Name -EQ 'win-tool').ReleaseTargets |
            Should -Be @('x86_64-pc-windows-msvc', 'aarch64-pc-windows-msvc')
    }

    It 'leaves the restriction empty for a crate that declares none' {
        @(($script:Crates | Where-Object Name -EQ 'pub-bin').ReleaseTargets).Count | Should -Be 0
    }
}

Describe 'Get-DeclaredReleaseTarget (crafted package objects)' {
    It 'returns nothing for a package with no metadata field at all' {
        @(Get-DeclaredReleaseTarget -Package ([pscustomobject]@{ name = 'crafted' })).Count | Should -Be 0
    }

    It 'returns nothing for a package whose metadata has no folo section' {
        $pkg = [pscustomobject]@{
            name     = 'crafted'
            metadata = [pscustomobject]@{ binstall = [pscustomobject]@{ 'pkg-fmt' = 'zip' } }
        }
        @(Get-DeclaredReleaseTarget -Package $pkg).Count | Should -Be 0
    }

    It 'returns nothing for a folo section that declares no release targets' {
        $pkg = [pscustomobject]@{
            name     = 'crafted'
            metadata = [pscustomobject]@{ folo = [pscustomobject]@{ 'something-else' = 'value' } }
        }
        @(Get-DeclaredReleaseTarget -Package $pkg).Count | Should -Be 0
    }

    It 'yields one triple per declared target, unrolled by the pipeline' {
        $pkg = [pscustomobject]@{
            name     = 'crafted'
            metadata = [pscustomobject]@{
                folo = [pscustomobject]@{ 'release-targets' = @('x86_64-pc-windows-msvc') }
            }
        }
        $declared = @(Get-DeclaredReleaseTarget -Package $pkg)
        $declared.Count | Should -Be 1
        $declared[0] | Should -Be 'x86_64-pc-windows-msvc'
    }
}

Describe 'Add-GitReleaseEnableFlag (pure line-based injection)' {
    BeforeAll {
        $script:SourceLines = [System.IO.File]::ReadAllText($script:SampleToml) -split "`r?`n"
    }

    It 'inserts the flag immediately after the crate name line, inside its block' {
        $result = Add-GitReleaseEnableFlag -Line $script:SourceLines -CrateName 'demo-tool'
        $nameIndex = [array]::IndexOf($result, 'name = "demo-tool"')
        $result[$nameIndex + 1] | Should -Be 'git_release_enable = true'
    }

    It 'preserves other keys already in the block' {
        $result = Add-GitReleaseEnableFlag -Line $script:SourceLines -CrateName 'demo-tool'
        $result | Should -Contain 'version_group = "demo"'
    }

    It 'matches the crate name exactly so a name-prefix sibling is untouched' {
        $result = Add-GitReleaseEnableFlag -Line $script:SourceLines -CrateName 'demo-tool'
        $coreIndex = [array]::IndexOf($result, 'name = "demo-tool-core"')
        $result[$coreIndex + 1] | Should -Be 'version_group = "demo"'
    }

    It 'appends a new [[package]] block for a crate with no existing entry' {
        $result = Add-GitReleaseEnableFlag -Line $script:SourceLines -CrateName 'pub-bin'
        $nameIndex = [array]::IndexOf($result, 'name = "pub-bin"')
        $nameIndex | Should -BeGreaterThan -1
        $result[$nameIndex - 1] | Should -Be '[[package]]'
        $result[$nameIndex + 1] | Should -Be 'git_release_enable = true'
    }

    It 'is idempotent: a second pass does not duplicate the flag' {
        $once = Add-GitReleaseEnableFlag -Line $script:SourceLines -CrateName 'demo-tool'
        $twice = Add-GitReleaseEnableFlag -Line $once -CrateName 'demo-tool'
        @($twice | Where-Object { $_ -eq 'git_release_enable = true' }).Count | Should -Be 1
    }

    It 'forces an existing git_release_enable = false to true' {
        $lines = @(
            '[[package]]'
            'name = "demo-tool"'
            'git_release_enable = false'
            'version_group = "demo"'
        )
        $result = Add-GitReleaseEnableFlag -Line $lines -CrateName 'demo-tool'
        $result | Should -Contain 'git_release_enable = true'
        $result | Should -Not -Contain 'git_release_enable = false'
        # Replaced in place, not duplicated, and the sibling key is preserved.
        @($result | Where-Object { $_ -match '^git_release_enable' }).Count | Should -Be 1
        $result | Should -Contain 'version_group = "demo"'
    }

    It 'enables every requested crate in one pass' {
        $result = Add-GitReleaseEnableFlag -Line $script:SourceLines -CrateName @('demo-tool', 'pub-bin')
        @($result | Where-Object { $_ -eq 'git_release_enable = true' }).Count | Should -Be 2
    }
}

Describe 'New-ReleasePlzConfig (real file write)' {
    BeforeEach {
        $script:OutPath = Join-Path $TestDrive ("rp-" + [guid]::NewGuid() + ".toml")
    }

    AfterEach {
        if (Test-Path $script:OutPath) { Remove-Item $script:OutPath -Force }
    }

    It 'writes UTF-8 without a byte-order mark' {
        New-ReleasePlzConfig -SourcePath $script:SampleToml -OutputPath $script:OutPath -CrateName 'demo-tool'
        $bytes = [System.IO.File]::ReadAllBytes($script:OutPath)
        # A UTF-8 BOM is EF BB BF.
        ($bytes[0] -eq 0xEF -and $bytes[1] -eq 0xBB -and $bytes[2] -eq 0xBF) | Should -BeFalse
    }

    It 'writes LF line endings with no carriage returns' {
        New-ReleasePlzConfig -SourcePath $script:SampleToml -OutputPath $script:OutPath -CrateName 'demo-tool'
        $bytes = [System.IO.File]::ReadAllBytes($script:OutPath)
        ($bytes -contains 0x0D) | Should -BeFalse
    }

    It 'ends with a single trailing newline' {
        New-ReleasePlzConfig -SourcePath $script:SampleToml -OutputPath $script:OutPath -CrateName 'demo-tool'
        $text = [System.IO.File]::ReadAllText($script:OutPath)
        $text.EndsWith("`n") | Should -BeTrue
        $text.EndsWith("`n`n") | Should -BeFalse
    }

    It 'injects the flag for the requested crates' {
        New-ReleasePlzConfig -SourcePath $script:SampleToml -OutputPath $script:OutPath -CrateName @('demo-tool', 'pub-bin')
        $lines = [System.IO.File]::ReadAllText($script:OutPath) -split "`n"
        $demoIndex = [array]::IndexOf($lines, 'name = "demo-tool"')
        $lines[$demoIndex + 1] | Should -Be 'git_release_enable = true'
        $pubIndex = [array]::IndexOf($lines, 'name = "pub-bin"')
        $lines[$pubIndex + 1] | Should -Be 'git_release_enable = true'
    }
}

Describe 'Get-PublishableCrate (real cargo metadata on a fixture workspace)' {
    BeforeAll {
        $script:AllCrates = Get-PublishableCrate -ManifestPath $script:MetadataManifest
    }

    It 'includes publishable crates regardless of target kind (libraries and binaries)' {
        $script:AllCrates.Name | Should -Contain 'pub-bin'
        $script:AllCrates.Name | Should -Contain 'pub-lib'
        $script:AllCrates.Name | Should -Contain 'demo-tool'
        $script:AllCrates.Name | Should -Contain 'demo-tool-core'
    }

    It 'excludes non-publishable crates' {
        $script:AllCrates.Name | Should -Not -Contain 'nopub-bin'
    }

    It 'returns crates sorted by name with versions' {
        $script:AllCrates.Name | Should -Be @('demo-tool', 'demo-tool-core', 'pub-bin', 'pub-lib', 'win-tool')
        ($script:AllCrates | Where-Object Name -EQ 'demo-tool').Version | Should -Be '2.3.4'
    }
}

Describe 'Get-CrateIndexPath (pure sparse-index path)' {
    It 'places a one-character name under 1/' {
        Get-CrateIndexPath -Name 'a' | Should -Be '1/a'
    }

    It 'places a two-character name under 2/' {
        Get-CrateIndexPath -Name 'ab' | Should -Be '2/ab'
    }

    It 'places a three-character name under the 3/x/ prefix' {
        Get-CrateIndexPath -Name 'abc' | Should -Be '3/a/abc'
    }

    It 'places a longer name under the first-two/next-two prefix' {
        Get-CrateIndexPath -Name 'cargo-bench-history' | Should -Be 'ca/rg/cargo-bench-history'
    }

    It 'lowercases the name before computing the path' {
        Get-CrateIndexPath -Name 'Cargo-Detect-Package' | Should -Be 'ca/rg/cargo-detect-package'
    }
}

Describe 'Get-CratePublishStatus (mocked HTTP)' {
    It 'reports Published on HTTP 200' {
        Mock Invoke-WebRequest -ModuleName ReleaseAutomation { [pscustomobject]@{ StatusCode = 200 } }
        Get-CratePublishStatus -Name 'serde' | Should -Be 'Published'
    }

    It 'reports NeverPublished on HTTP 404' {
        Mock Invoke-WebRequest -ModuleName ReleaseAutomation { [pscustomobject]@{ StatusCode = 404 } }
        Get-CratePublishStatus -Name 'brand-new-crate' | Should -Be 'NeverPublished'
    }

    It 'reports Unknown on an unexpected status code' {
        Mock Invoke-WebRequest -ModuleName ReleaseAutomation { [pscustomobject]@{ StatusCode = 503 } }
        Get-CratePublishStatus -Name 'serde' | Should -Be 'Unknown'
    }

    It 'reports Unknown when the request throws (network error)' {
        Mock Invoke-WebRequest -ModuleName ReleaseAutomation { throw 'connection refused' }
        Get-CratePublishStatus -Name 'serde' | Should -Be 'Unknown'
    }

    It 'queries the crate''s sparse-index URL' {
        Mock Invoke-WebRequest -ModuleName ReleaseAutomation { [pscustomobject]@{ StatusCode = 200 } }
        Get-CratePublishStatus -Name 'cargo-bench-history' | Out-Null
        Should -Invoke Invoke-WebRequest -ModuleName ReleaseAutomation -Times 1 -Exactly `
            -ParameterFilter { $Uri -eq 'https://index.crates.io/ca/rg/cargo-bench-history' }
    }
}

Describe 'Test-NeverPublishedCrate (mocked crate list and status)' {
    BeforeEach {
        Mock Get-PublishableCrate -ModuleName ReleaseAutomation {
            @(
                [pscustomobject]@{ Name = 'existing-crate'; Version = '1.0.0' }
                [pscustomobject]@{ Name = 'brand-new-crate'; Version = '0.1.0' }
                [pscustomobject]@{ Name = 'flaky-crate'; Version = '0.1.0' }
            )
        }
        Mock Get-CratePublishStatus -ModuleName ReleaseAutomation {
            switch ($Name) {
                'existing-crate' { 'Published' }
                'brand-new-crate' { 'NeverPublished' }
                default { 'Unknown' }
            }
        }
    }

    It 'warns once per never-published crate and once per unconfirmed crate, but not for published ones' {
        Test-NeverPublishedCrate -WarningVariable warnings -WarningAction SilentlyContinue 4>$null
        @($warnings).Count | Should -Be 2
        ($warnings -join "`n") | Should -Match 'brand-new-crate has never been published'
        ($warnings -join "`n") | Should -Match "Could not confirm crates.io publish status for 'flaky-crate'"
        ($warnings -join "`n") | Should -Not -Match 'existing-crate'
    }

    It 'checks the status of every publishable crate' {
        Test-NeverPublishedCrate -WarningAction SilentlyContinue 4>$null
        Should -Invoke Get-CratePublishStatus -ModuleName ReleaseAutomation -Times 3 -Exactly
    }
}

Describe 'Get-ReleaseTarget' {
    BeforeAll {
        $script:Targets = Get-ReleaseTarget
    }

    It 'maps each triple to its runner' {
        ($script:Targets | Where-Object Triple -EQ 'x86_64-unknown-linux-gnu').Os | Should -Be 'ubuntu-latest'
        ($script:Targets | Where-Object Triple -EQ 'aarch64-unknown-linux-gnu').Os | Should -Be 'ubuntu-24.04-arm'
        ($script:Targets | Where-Object Triple -EQ 'x86_64-pc-windows-msvc').Os | Should -Be 'windows-latest'
        ($script:Targets | Where-Object Triple -EQ 'aarch64-pc-windows-msvc').Os | Should -Be 'windows-11-arm'
        ($script:Targets | Where-Object Triple -EQ 'aarch64-apple-darwin').Os | Should -Be 'macos-latest'
    }

    It 'builds Apple Silicon but not Intel macOS' {
        $script:Targets.Triple | Should -Contain 'aarch64-apple-darwin'
        $script:Targets.Triple | Should -Not -Contain 'x86_64-apple-darwin'
    }
}

Describe 'Get-MissingBinaryMatrix (mocked gh release view)' {
    BeforeAll {
        $script:TwoTargets = @(
            [pscustomobject]@{ Triple = 'x86_64-unknown-linux-gnu'; Os = 'ubuntu-latest' }
            [pscustomobject]@{ Triple = 'aarch64-apple-darwin'; Os = 'macos-latest' }
        )
    }

    BeforeEach {
        Mock gh -ModuleName ReleaseAutomation {
            $tag = $args[2]
            switch ($tag) {
                'have-all-v1.0.0' {
                    $global:LASTEXITCODE = 0
                    '{"assets":[{"name":"have-all-v1.0.0-x86_64-unknown-linux-gnu.zip"},{"name":"have-all-v1.0.0-aarch64-apple-darwin.zip"}]}'
                }
                'have-some-v2.0.0' {
                    $global:LASTEXITCODE = 0
                    '{"assets":[{"name":"have-some-v2.0.0-x86_64-unknown-linux-gnu.zip"}]}'
                }
                'empty-release-v4.0.0' {
                    $global:LASTEXITCODE = 0
                    '{"assets":[]}'
                }
                'restricted-v6.0.0' {
                    $global:LASTEXITCODE = 0
                    '{"assets":[]}'
                }
                'no-release-v3.0.0' {
                    # gh prints this to stderr and exits 1 when the release does not exist.
                    $global:LASTEXITCODE = 1
                    'release not found'
                }
                'api-error-v5.0.0' {
                    # A non-"not found" failure (auth / network / API) must NOT be swallowed.
                    $global:LASTEXITCODE = 1
                    'HTTP 503: Service Unavailable (https://api.github.com/repos/o/r/releases)'
                }
                default {
                    $global:LASTEXITCODE = 1
                    "unexpected tag: $tag"
                }
            }
        }
    }

    It 'emits nothing when every target archive is already uploaded' {
        $crate = [pscustomobject]@{ Name = 'have-all'; Version = '1.0.0' }
        $rows = Get-MissingBinaryMatrix -Crate $crate -Target $script:TwoTargets
        $rows.Count | Should -Be 0
    }

    It 'emits only the missing (crate, target) pairs' {
        $crate = [pscustomobject]@{ Name = 'have-some'; Version = '2.0.0' }
        $rows = Get-MissingBinaryMatrix -Crate $crate -Target $script:TwoTargets
        $rows.Count | Should -Be 1
        $rows[0].triple | Should -Be 'aarch64-apple-darwin'
        $rows[0].os | Should -Be 'macos-latest'
        $rows[0].tag | Should -Be 'have-some-v2.0.0'
        $rows[0].name | Should -Be 'have-some'
        $rows[0].version | Should -Be '2.0.0'
    }

    It 'skips crates whose release does not exist yet' {
        $crate = [pscustomobject]@{ Name = 'no-release'; Version = '3.0.0' }
        $rows = Get-MissingBinaryMatrix -Crate $crate -Target $script:TwoTargets
        $rows.Count | Should -Be 0
    }

    It 'rethrows a gh failure that is not a missing release' {
        # An auth/network/API error must propagate, not be treated as "no release" - otherwise
        # the workflow could build an empty matrix and look successful while binaries are missing.
        $crate = [pscustomobject]@{ Name = 'api-error'; Version = '5.0.0' }
        { Get-MissingBinaryMatrix -Crate $crate -Target $script:TwoTargets } | Should -Throw '*503*'
    }

    It 'emits one row per target when the release exists but has no assets' {
        $crate = [pscustomobject]@{ Name = 'empty-release'; Version = '4.0.0' }
        $rows = Get-MissingBinaryMatrix -Crate $crate -Target $script:TwoTargets
        $rows.Count | Should -Be 2
    }

    It 'defaults to the full target set from Get-ReleaseTarget' {
        $crate = [pscustomobject]@{ Name = 'empty-release'; Version = '4.0.0' }
        $rows = Get-MissingBinaryMatrix -Crate $crate
        @($rows).Count | Should -Be @(Get-ReleaseTarget).Count
    }

    It 'reconciles several crates in one pass' {
        $crates = @(
            [pscustomobject]@{ Name = 'have-all'; Version = '1.0.0' }
            [pscustomobject]@{ Name = 'have-some'; Version = '2.0.0' }
            [pscustomobject]@{ Name = 'no-release'; Version = '3.0.0' }
        )
        $rows = Get-MissingBinaryMatrix -Crate $crates -Target $script:TwoTargets
        $rows.Count | Should -Be 1
    }

    It 'reconciles a later crate against the full target set, not just the last target' {
        # Regression: the per-crate target loop must see every target for every crate. A loop
        # variable that case-collides with the $Target parameter would leave $Target holding only
        # its last element after the first crate, so a later empty release would be reconciled
        # against a single leftover target instead of all of them. Put the empty release LAST so a
        # per-crate loop that lost its targets would emit one row (the last target) instead of two.
        $crates = @(
            [pscustomobject]@{ Name = 'have-all'; Version = '1.0.0' }
            [pscustomobject]@{ Name = 'empty-release'; Version = '4.0.0' }
        )
        $rows = Get-MissingBinaryMatrix -Crate $crates -Target $script:TwoTargets
        $emptyRows = @($rows | Where-Object { $_.name -eq 'empty-release' })
        $emptyRows.Count | Should -Be $script:TwoTargets.Count
        $emptyRows.triple | Should -Contain 'x86_64-unknown-linux-gnu'
        $emptyRows.triple | Should -Contain 'aarch64-apple-darwin'
    }

    Context 'per-crate release-target restriction' {
        It 'reconciles a restricted crate against only the targets it declares' {
            $crate = [pscustomobject]@{
                Name           = 'restricted'
                Version        = '6.0.0'
                ReleaseTargets = @('aarch64-apple-darwin')
            }
            $rows = Get-MissingBinaryMatrix -Crate $crate -Target $script:TwoTargets
            $rows.Count | Should -Be 1
            $rows[0].triple | Should -Be 'aarch64-apple-darwin'
            $rows[0].os | Should -Be 'macos-latest'
        }

        It 'treats an empty restriction as the full target set' {
            $crate = [pscustomobject]@{ Name = 'restricted'; Version = '6.0.0'; ReleaseTargets = @() }
            $rows = Get-MissingBinaryMatrix -Crate $crate -Target $script:TwoTargets
            $rows.Count | Should -Be $script:TwoTargets.Count
        }

        It 'throws when a crate declares a target the table does not offer' {
            # Silently building nothing for that target would leave the crate short of archives
            # with no failure anywhere, so a typo must be loud.
            $crate = [pscustomobject]@{
                Name           = 'restricted'
                Version        = '6.0.0'
                ReleaseTargets = @('aarch64-apple-darwin', 's390x-unknown-linux-gnu')
            }
            { Get-MissingBinaryMatrix -Crate $crate -Target $script:TwoTargets } |
                Should -Throw '*s390x-unknown-linux-gnu*'
        }

        It 'restricts only the declaring crate, leaving its neighbours on the full set' {
            $crates = @(
                [pscustomobject]@{ Name = 'restricted'; Version = '6.0.0'; ReleaseTargets = @('aarch64-apple-darwin') }
                [pscustomobject]@{ Name = 'empty-release'; Version = '4.0.0' }
            )
            $rows = Get-MissingBinaryMatrix -Crate $crates -Target $script:TwoTargets
            @($rows | Where-Object { $_.name -eq 'restricted' }).Count | Should -Be 1
            @($rows | Where-Object { $_.name -eq 'empty-release' }).Count | Should -Be $script:TwoTargets.Count
        }
    }

    Context 'verbose decision history (-Verbose)' {
        BeforeAll {
            function Get-VerboseMessage {
                # Runs the reconciliation with -Verbose and returns just the verbose-stream
                # messages (dropping the matrix rows the function also emits on the success stream).
                param([object[]] $Crate)
                Get-MissingBinaryMatrix -Crate $Crate -Target $script:TwoTargets -Verbose 4>&1 |
                    Where-Object { $_ -is [System.Management.Automation.VerboseRecord] } |
                    ForEach-Object { $_.Message }
            }
        }

        It 'is silent on the verbose stream without -Verbose' {
            $crate = [pscustomobject]@{ Name = 'have-some'; Version = '2.0.0' }
            $previousVerbosePreference = $VerbosePreference
            try {
                $VerbosePreference = 'SilentlyContinue'
                $verbose = Get-MissingBinaryMatrix -Crate $crate -Target $script:TwoTargets 4>&1 |
                    Where-Object { $_ -is [System.Management.Automation.VerboseRecord] }
            } finally {
                $VerbosePreference = $previousVerbosePreference
            }
            $verbose | Should -BeNullOrEmpty
        }

        It 'logs the expected tag, the present verdict and the missing verdict for a partial release' {
            $messages = Get-VerboseMessage -Crate ([pscustomobject]@{ Name = 'have-some'; Version = '2.0.0' })
            ($messages -join "`n") | Should -Match "expected release tag 'have-some-v2.0.0'"
            ($messages -join "`n") | Should -Match 'x86_64-unknown-linux-gnu.*already uploaded'
            ($messages -join "`n") | Should -Match 'aarch64-apple-darwin.*missing.*macos-latest'
        }

        It 'explains why a crate with no release yet is skipped' {
            $messages = Get-VerboseMessage -Crate ([pscustomobject]@{ Name = 'no-release'; Version = '3.0.0' })
            ($messages -join "`n") | Should -Match "No GitHub release 'no-release-v3.0.0' found yet"
        }

        It 'reports the final missing-archive count' {
            $messages = Get-VerboseMessage -Crate ([pscustomobject]@{ Name = 'empty-release'; Version = '4.0.0' })
            ($messages -join "`n") | Should -Match 'Reconciliation complete: 2 missing \(crate, target\) archives'
        }

        It 'names both the declared and the skipped targets of a restricted crate' {
            $crate = [pscustomobject]@{
                Name           = 'restricted'
                Version        = '6.0.0'
                ReleaseTargets = @('aarch64-apple-darwin')
            }
            $messages = Get-VerboseMessage -Crate $crate
            ($messages -join "`n") | Should -Match 'restricts its release targets to: aarch64-apple-darwin'
            ($messages -join "`n") | Should -Match 'not built for it: x86_64-unknown-linux-gnu'
        }

        It 'uses the singular noun for a single missing archive' {
            $messages = Get-VerboseMessage -Crate ([pscustomobject]@{ Name = 'have-some'; Version = '2.0.0' })
            ($messages -join "`n") | Should -Match 'Reconciliation complete: 1 missing \(crate, target\) archive queued to build\.'
        }
    }
}

Describe 'ConvertTo-MatrixJson' {
    It 'renders an empty result as a JSON empty array' {
        ConvertTo-MatrixJson -Row @() | Should -Be '[]'
    }

    It 'wraps a single row as a one-element JSON array' {
        $json = ConvertTo-MatrixJson -Row @([pscustomobject]@{ name = 'a'; triple = 't' })
        $json.StartsWith('[') | Should -BeTrue
        $parsed = $json | ConvertFrom-Json
        @($parsed).Count | Should -Be 1
        @($parsed)[0].name | Should -Be 'a'
    }

    It 'renders multiple rows as a JSON array of that length' {
        $rows = @(
            [pscustomobject]@{ name = 'a' }
            [pscustomobject]@{ name = 'b' }
            [pscustomobject]@{ name = 'c' }
        )
        $parsed = ConvertTo-MatrixJson -Row $rows | ConvertFrom-Json
        @($parsed).Count | Should -Be 3
    }
}

Describe 'Invoke-ReleasePublish (mocked release-plz)' {
    It 'invokes release-plz once with the composed config on success' {
        $configPath = Join-Path $TestDrive 'ci.toml'
        Mock release-plz -ModuleName ReleaseAutomation { $global:LASTEXITCODE = 0 }
        Invoke-ReleasePublish -ConfigPath $configPath -Attempt 3 -DelaySeconds 0
        Should -Invoke release-plz -ModuleName ReleaseAutomation -Times 1 -Exactly `
            -ParameterFilter { ($args -contains 'release') -and ($args -contains '--config') -and ($args -contains $configPath) }
    }

    It 'retries on a non-zero exit and then succeeds' {
        $configPath = Join-Path $TestDrive 'ci.toml'
        $script:attempts = 0
        Mock release-plz -ModuleName ReleaseAutomation {
            $script:attempts++
            $global:LASTEXITCODE = if ($script:attempts -lt 2) { 1 } else { 0 }
        }
        Invoke-ReleasePublish -ConfigPath $configPath -Attempt 3 -DelaySeconds 0
        Should -Invoke release-plz -ModuleName ReleaseAutomation -Times 2 -Exactly
    }

    It 'throws after every attempt fails' {
        $configPath = Join-Path $TestDrive 'ci.toml'
        Mock release-plz -ModuleName ReleaseAutomation { $global:LASTEXITCODE = 1 }
        { Invoke-ReleasePublish -ConfigPath $configPath -Attempt 3 -DelaySeconds 0 } | Should -Throw
        Should -Invoke release-plz -ModuleName ReleaseAutomation -Times 3 -Exactly
    }
}

Describe 'Set-GitHubOutput' {
    It 'appends name=value to the GITHUB_OUTPUT file when it is set' {
        $original = $env:GITHUB_OUTPUT
        $file = Join-Path $TestDrive ("out-" + [guid]::NewGuid())
        try {
            $env:GITHUB_OUTPUT = $file
            Set-GitHubOutput -Name 'matrix' -Value '[]'
            Set-GitHubOutput -Name 'has_binaries' -Value 'false'
            $lines = Get-Content $file
            $lines | Should -Contain 'matrix=[]'
            $lines | Should -Contain 'has_binaries=false'
        } finally {
            if ($null -ne $original) { $env:GITHUB_OUTPUT = $original } else { Remove-Item Env:GITHUB_OUTPUT -ErrorAction SilentlyContinue }
            if (Test-Path $file) { Remove-Item $file -Force }
        }
    }

    It 'does not throw when GITHUB_OUTPUT is unset' {
        $original = $env:GITHUB_OUTPUT
        try {
            Remove-Item Env:GITHUB_OUTPUT -ErrorAction SilentlyContinue
            { Set-GitHubOutput -Name 'matrix' -Value '[]' } | Should -Not -Throw
        } finally {
            if ($null -ne $original) { $env:GITHUB_OUTPUT = $original }
        }
    }

    It 'rejects an empty release-asset output before writing it' {
        $original = $env:GITHUB_OUTPUT
        $file = Join-Path $TestDrive 'empty-output'
        New-Item -ItemType File -Path $file | Out-Null
        try {
            $env:GITHUB_OUTPUT = $file
            { Set-GitHubOutput -Name 'matrix' -Value '' } |
                Should -Throw "*GitHub output 'matrix' must not be empty*"
            @(Get-Content -LiteralPath $file).Count | Should -Be 0
        } finally {
            if ($null -ne $original) {
                $env:GITHUB_OUTPUT = $original
            } else {
                Remove-Item Env:GITHUB_OUTPUT -ErrorAction SilentlyContinue
            }
        }
    }
}
