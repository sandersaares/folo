#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }

# Exercises the real Rust verifier and Git worktree boundary used by release.yml. The remote is
# a disposable local bare repository, and GitHub writes are replaced with local references and
# an in-memory release inventory. No fixture can publish to GitHub or crates.io.

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
$VerbosePreference = 'Continue'

BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'ReleasePublication.psm1') -Force
    $script:verifier = InModuleScope ReleasePublication { Get-ReleaseTargetVerifier }

    function Invoke-FixtureGit {
        param([Parameter(Mandatory)][string[]] $Argument)
        & git -c user.name='Release fixture' -c user.email=fixture@example.invalid `
            -c commit.gpgsign=false -c gc.auto=0 -c core.autocrlf=false @Argument
    }
}

Describe 'Native release target integration' {
    BeforeEach {
        $script:previousGlobal = $env:GIT_CONFIG_GLOBAL
        $script:previousNoSystem = $env:GIT_CONFIG_NOSYSTEM
        $script:pushed = $false
        $emptyConfig = Join-Path $TestDrive 'empty-git-config'
        Set-Content -LiteralPath $emptyConfig -Value '' -NoNewline
        $env:GIT_CONFIG_GLOBAL = $emptyConfig
        $env:GIT_CONFIG_NOSYSTEM = '1'
        $script:remote = Join-Path $TestDrive "remote-$([guid]::NewGuid().ToString('N')).git"
        $script:sourceDirectory = Join-Path $TestDrive "source-$([guid]::NewGuid().ToString('N'))"
        $null = New-Item -ItemType Directory -Path (Join-Path $sourceDirectory 'src')
        $null = Invoke-FixtureGit -Argument @('init', '--quiet', '--bare', '--initial-branch=main', $remote)
        Push-Location $sourceDirectory
        $script:pushed = $true
        $null = Invoke-FixtureGit -Argument @('init', '--quiet', '--initial-branch=main')
        Set-Content -LiteralPath 'Cargo.toml' -Value @'
[package]
name = "release-fixture"
version = "1.0.0"
edition = "2024"
include = ["src/**"]

[workspace]
'@
        Set-Content -LiteralPath '.gitignore' -Value '/target'
        Set-Content -LiteralPath 'src\main.rs' -Value 'fn main() {}'
        cargo generate-lockfile --offline
        $null = Invoke-FixtureGit -Argument @('add', '.')
        $null = Invoke-FixtureGit -Argument @('commit', '--quiet', '-m', 'Publish fixture version')
        $script:source = Invoke-FixtureGit -Argument @('rev-parse', 'HEAD')
        $null = Invoke-FixtureGit -Argument @('remote', 'add', 'origin', $remote)
        $null = New-Item -ItemType Directory -Path '.github\workflows'
        Set-Content -LiteralPath '.github\workflows\fixture.yml' -Value 'name: Fixture'
        $script:releases = @{}
        $script:writes = [Collections.Generic.List[object]]::new()

        Mock Get-ReleaseTargetVerifier -ModuleName ReleasePublication { $verifier }
        Mock Get-BinaryReleaseAsset -ModuleName ReleasePublication {
            if ($releases.ContainsKey($Tag)) { return , @() }
            return $null
        }
        Mock Invoke-ReleaseGitHubWrite -ModuleName ReleasePublication {
            $writes.Add(@($Argument))
            if ($Argument[0] -eq 'api') {
                $reference = ($Argument | Where-Object { $_.StartsWith('ref=') }).Substring('ref='.Length)
                $commit = ($Argument | Where-Object { $_.StartsWith('sha=') }).Substring('sha='.Length)
                & git --git-dir $remote update-ref $reference $commit
            } else {
                $releases[$Argument[2]] = $true
            }
            [pscustomobject]@{ ExitCode = 0; Diagnostic = '' }
        }
        Mock gh -ModuleName ReleasePublication { throw 'The fixture must not contact GitHub.' }
    }

    AfterEach {
        if ($pushed) { Pop-Location }
        $env:GIT_CONFIG_GLOBAL = $previousGlobal
        $env:GIT_CONFIG_NOSYSTEM = $previousNoSystem
    }

    It 'publishes a later equivalent main snapshot through the real verifier' {
        $null = Invoke-FixtureGit -Argument @('add', '.')
        $null = Invoke-FixtureGit -Argument @('commit', '--quiet', '-m', 'Change workflow only')
        $candidate = Invoke-FixtureGit -Argument @('rev-parse', 'HEAD')
        $null = Invoke-FixtureGit -Argument @('push', '--quiet', 'origin', 'main')
        $null = Invoke-FixtureGit -Argument @('checkout', '--quiet', '--detach', $source)

        Invoke-ReleaseReconciliation -Source $source -Repository 'fixture/not-github'

        $tag = Invoke-FixtureGit -Argument @('--git-dir', $remote, 'rev-parse', 'refs/tags/release-fixture-v1.0.0')
        $tag | Should -Be $candidate
        (Invoke-FixtureGit -Argument @('rev-parse', 'HEAD')) | Should -Be $source
        $releases.ContainsKey('release-fixture-v1.0.0') | Should -BeTrue
        @((Invoke-FixtureGit -Argument @('worktree', 'list', '--porcelain')) |
                Where-Object { $_.StartsWith('worktree ') }).Count | Should -Be 1
        $writes[1] | Should -Contain '--verify-tag'
    }

    It 'rejects same-version source changes and cleans the real candidate worktree' {
        Set-Content -LiteralPath 'src\main.rs' -Value 'fn main() { std::hint::black_box(1); }'
        $null = Invoke-FixtureGit -Argument @('add', '.')
        $null = Invoke-FixtureGit -Argument @('commit', '--quiet', '-m', 'Invalid unchanged version')
        $null = Invoke-FixtureGit -Argument @('push', '--quiet', 'origin', 'main')
        $null = Invoke-FixtureGit -Argument @('checkout', '--quiet', '--detach', $source)

        { Invoke-ReleaseReconciliation -Source $source -Repository 'fixture/not-github' 2>$null } |
            Should -Throw

        $writes.Count | Should -Be 0
        @((Invoke-FixtureGit -Argument @('worktree', 'list', '--porcelain')) |
                Where-Object { $_.StartsWith('worktree ') }).Count | Should -Be 1
    }
}
