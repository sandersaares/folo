#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }

# Protects release.yml's publication/recovery contract without writing to GitHub or crates.io.
# Mutable fixture state models main advancement and partial remote writes; the Rust verifier's
# own tests cover package equivalence. Worktree cleanup and native argument boundaries stay here.

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
$VerbosePreference = 'Continue'

BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'ReleasePublication.psm1') -Force
}

Describe 'Release-equivalent GitHub publication' {
    BeforeEach {
        $script:state = @{
            Source = 'a' * 40
            Main = 'b' * 40
            Generation = 1
            Tags = @{ 'library-v1.0.0' = 'a' * 40 }
            Releases = @{}
            Checks = [Collections.Generic.List[object]]::new()
            Writes = [Collections.Generic.List[object]]::new()
        }
        Mock git -ModuleName ReleasePublication {
            $global:LASTEXITCODE = 0
            if ($args[0] -eq 'rev-parse') { return $state.Source }
            if ($args[0] -ne 'merge-base') { throw "Unexpected Git call: $args" }
        }
        Mock Get-PublishableCrate -ModuleName ReleasePublication {
            @(
                [pscustomobject]@{ Name = 'library'; Version = '1.0.0' }
                [pscustomobject]@{ Name = 'app'; Version = '1.0.0' }
            )
        }
        Mock Get-PublishableBinaryCrate -ModuleName ReleasePublication {
            [pscustomobject]@{ Name = 'app'; Version = '1.0.0' }
        }
        Mock Get-ReleaseMainCommit -ModuleName ReleasePublication { $state.Main }
        Mock Get-ReleaseTagMap -ModuleName ReleasePublication { $state.Tags.Clone() }
        Mock Get-ReleaseTargetVerifier -ModuleName ReleasePublication { 'controller-verifier' }
        Mock Invoke-ReleaseWorktree -ModuleName ReleasePublication {
            & $Action 'candidate-worktree'
        }
        Mock Invoke-ReleaseTargetCheck -ModuleName ReleasePublication {
            $state.Checks.Add(@{ Commit = $Commit; Requests = @($PackageRequest) })
        }
        Mock Get-BinaryReleaseAsset -ModuleName ReleasePublication {
            if ($state.Releases.ContainsKey($Tag)) { return , @() }
            return $null
        }
        Mock Invoke-ReleaseGitHubWrite -ModuleName ReleasePublication {
            $state.Writes.Add(@($Argument))
            if ($Argument[0] -eq 'api') {
                $tag = ($Argument | Where-Object { $_.StartsWith('ref=') }).Substring('ref=refs/tags/'.Length)
                $commit = ($Argument | Where-Object { $_.StartsWith('sha=') }).Substring('sha='.Length)
                $state.Tags[$tag] = $commit
            } else {
                $state.Releases[$Argument[2]] = $true
            }
            [pscustomobject]@{ ExitCode = 0; Diagnostic = '' }
        }
        Mock gh -ModuleName ReleasePublication { throw 'Tests must not call GitHub.' }
    }

    It 'uses a verified later main snapshot rather than the publication anchor' {
        Invoke-ReleaseReconciliation -Source $state.Source -Repository 'owner/repo'

        $state.Tags['app-v1.0.0'] | Should -Be $state.Main
        $state.Checks.Count | Should -Be 1
        $state.Checks[0].Requests | Should -Be @('app@1.0.0')
        $state.Checks[0].Commit | Should -Be $state.Main
        $state.Writes[0] | Should -Contain "sha=$($state.Main)"
        $state.Writes[0] | Should -Not -Contain 'sha=main'
        $state.Writes[1] | Should -Contain '--verify-tag'
        $state.Writes[1] | Should -Not -Contain '--target'
        Should -Invoke Get-ReleaseTargetVerifier -ModuleName ReleasePublication -Times 1 -Exactly
    }

    It 'preserves an existing historical tag when only its binary release is missing' {
        $state.Tags['app-v1.0.0'] = 'c' * 40

        Invoke-ReleaseReconciliation -Source $state.Source -Repository 'owner/repo'

        $state.Tags['app-v1.0.0'] | Should -Be ('c' * 40)
        $state.Writes.Count | Should -Be 1
        $state.Writes[0][0] | Should -Be 'release'
        $state.Writes[0] | Should -Contain '--verify-tag'
        Should -Invoke Get-ReleaseMainCommit -ModuleName ReleasePublication -Times 0 -Exactly
        Should -Invoke Invoke-ReleaseTargetCheck -ModuleName ReleasePublication -Times 0 -Exactly
    }

    It 'does no writes or verifier build when the release already exists, including no assets' {
        $state.Tags['app-v1.0.0'] = $state.Source
        $state.Releases['app-v1.0.0'] = $true

        Invoke-ReleaseReconciliation -Source $state.Source -Repository 'owner/repo'

        $state.Writes.Count | Should -Be 0
        Should -Invoke Get-ReleaseTargetVerifier -ModuleName ReleasePublication -Times 0 -Exactly
    }

    It 'rejects a checkout other than the supplied publication source' {
        { Invoke-ReleaseReconciliation -Source ('d' * 40) -Repository 'owner/repo' } | Should -Throw
        Should -Invoke Get-PublishableCrate -ModuleName ReleasePublication -Times 0 -Exactly
        $state.Writes.Count | Should -Be 0
    }

    It 'rejects failed equivalence or a superseded version before creating any tag' {
        Mock Invoke-ReleaseTargetCheck -ModuleName ReleasePublication { throw 'Candidate rejected.' }

        { Invoke-ReleaseReconciliation -Source $state.Source -Repository 'owner/repo' } | Should -Throw

        $state.Writes.Count | Should -Be 0
        Should -Invoke Get-ReleaseMainCommit -ModuleName ReleasePublication -Times 1 -Exactly
    }

    It 'rejects a main history that no longer contains the publication source' {
        Mock git -ModuleName ReleasePublication {
            if ($args[0] -eq 'rev-parse') {
                $global:LASTEXITCODE = 0
                return $state.Source
            }
            $global:LASTEXITCODE = 1
        }

        { Invoke-ReleaseReconciliation -Source $state.Source -Repository 'owner/repo' } | Should -Throw

        $state.Writes.Count | Should -Be 0
        Should -Invoke Get-ReleaseTargetVerifier -ModuleName ReleasePublication -Times 0 -Exactly
    }

    It 'surfaces a write failure without retrying an unchanged main' {
        Mock Invoke-ReleaseGitHubWrite -ModuleName ReleasePublication {
            [pscustomobject]@{ ExitCode = 1; Diagnostic = 'HTTP 403: fixture denial' }
        }

        { Invoke-ReleaseReconciliation -Source $state.Source -Repository 'owner/repo' } |
            Should -Throw '*HTTP 403: fixture denial*'

        $state.Checks.Count | Should -Be 1
        Should -Invoke Invoke-ReleaseGitHubWrite -ModuleName ReleasePublication -Times 1 -Exactly
    }

    It 'reselects and reverifies after main advances during a failed tag creation' {
        Mock Invoke-ReleaseGitHubWrite -ModuleName ReleasePublication {
            $state.Writes.Add(@($Argument))
            if ($state.Writes.Count -eq 1) {
                $state.Main = 'c' * 40
                return [pscustomobject]@{ ExitCode = 1; Diagnostic = 'HTTP 403' }
            }
            if ($Argument[0] -eq 'api') {
                $state.Tags['app-v1.0.0'] = $state.Main
            } else {
                $state.Releases['app-v1.0.0'] = $true
            }
            [pscustomobject]@{ ExitCode = 0; Diagnostic = '' }
        }

        Invoke-ReleaseReconciliation -Source $state.Source -Repository 'owner/repo'

        @($state.Checks | ForEach-Object { $_.Commit }) | Should -Be @(('b' * 40), ('c' * 40))
        $state.Checks[1].Requests | Should -Be @('app@1.0.0')
        $state.Tags['app-v1.0.0'] | Should -Be ('c' * 40)
        Should -Invoke Get-ReleaseTargetVerifier -ModuleName ReleasePublication -Times 1 -Exactly
    }

    It 'retains partial tag creation and verifies only remaining requests after main moves' {
        $state.Tags.Clear()
        Mock Invoke-ReleaseGitHubWrite -ModuleName ReleasePublication {
            $state.Writes.Add(@($Argument))
            if ($state.Writes.Count -eq 2) {
                $state.Main = 'c' * 40
                return [pscustomobject]@{ ExitCode = 1; Diagnostic = 'HTTP 403' }
            }
            if ($Argument[0] -eq 'api') {
                $tag = ($Argument | Where-Object { $_.StartsWith('ref=') }).Substring('ref=refs/tags/'.Length)
                $state.Tags[$tag] = $state.Main
            } else {
                $state.Releases['app-v1.0.0'] = $true
            }
            [pscustomobject]@{ ExitCode = 0; Diagnostic = '' }
        }

        Invoke-ReleaseReconciliation -Source $state.Source -Repository 'owner/repo'

        $state.Tags['library-v1.0.0'] | Should -Be ('b' * 40)
        $state.Tags['app-v1.0.0'] | Should -Be ('c' * 40)
        $state.Checks[0].Requests | Should -Be @('library@1.0.0', 'app@1.0.0')
        $state.Checks[1].Requests | Should -Be @('app@1.0.0')
    }

    It 'bounds repeated main movement without changing the requested version' {
        Mock Invoke-ReleaseGitHubWrite -ModuleName ReleasePublication {
            $state.Generation++
            $state.Main = $state.Generation.ToString().PadLeft(40, '0')
            [pscustomobject]@{ ExitCode = 1; Diagnostic = 'HTTP 403' }
        }

        { Invoke-ReleaseReconciliation -Source $state.Source -Repository 'owner/repo' -Attempt 2 } |
            Should -Throw

        $state.Checks.Count | Should -Be 2
        $state.Checks[0].Requests | Should -Be @('app@1.0.0')
        $state.Checks[1].Requests | Should -Be @('app@1.0.0')
    }

    It 'accepts a concurrent creation only when the reference has the verified commit' {
        Mock Invoke-ReleaseGitHubWrite -ModuleName ReleasePublication {
            if ($Argument[0] -eq 'api') {
                $state.Tags['app-v1.0.0'] = $state.Main
                return [pscustomobject]@{ ExitCode = 1; Diagnostic = 'Reference already exists.' }
            }
            $state.Releases['app-v1.0.0'] = $true
            [pscustomobject]@{ ExitCode = 0; Diagnostic = '' }
        }

        Invoke-ReleaseReconciliation -Source $state.Source -Repository 'owner/repo'

        $state.Tags['app-v1.0.0'] | Should -Be $state.Main
        $state.Checks.Count | Should -Be 1
    }

    It 'refuses to move a competing tag that names another commit' {
        Mock Invoke-ReleaseGitHubWrite -ModuleName ReleasePublication {
            $state.Tags['app-v1.0.0'] = 'd' * 40
            [pscustomobject]@{ ExitCode = 1; Diagnostic = 'Reference already exists.' }
        }

        { Invoke-ReleaseReconciliation -Source $state.Source -Repository 'owner/repo' } | Should -Throw

        $state.Tags['app-v1.0.0'] | Should -Be ('d' * 40)
        Should -Invoke Invoke-ReleaseGitHubWrite -ModuleName ReleasePublication -Times 1 -Exactly
    }

    It 'rejects a successful response whose tag reference is absent' {
        Mock Invoke-ReleaseGitHubWrite -ModuleName ReleasePublication {
            [pscustomobject]@{ ExitCode = 0; Diagnostic = '' }
        }

        { Invoke-ReleaseReconciliation -Source $state.Source -Repository 'owner/repo' } | Should -Throw
    }

    It 'surfaces an unsuccessful binary release creation' {
        $state.Tags['app-v1.0.0'] = $state.Source
        Mock Invoke-ReleaseGitHubWrite -ModuleName ReleasePublication {
            [pscustomobject]@{ ExitCode = 1; Diagnostic = 'HTTP 503: fixture outage' }
        }

        { Invoke-ReleaseReconciliation -Source $state.Source -Repository 'owner/repo' } |
            Should -Throw '*HTTP 503: fixture outage*'
    }

    It 'does not report a failed response as success unless the binary release exists' {
        $state.Tags['app-v1.0.0'] = $state.Source
        Mock Invoke-ReleaseGitHubWrite -ModuleName ReleasePublication {
            $state.Releases['app-v1.0.0'] = $true
            [pscustomobject]@{ ExitCode = 1; Diagnostic = 'Release already exists.' }
        }

        Invoke-ReleaseReconciliation -Source $state.Source -Repository 'owner/repo'

        Should -Invoke Invoke-ReleaseGitHubWrite -ModuleName ReleasePublication -Times 1 -Exactly
    }
}

Describe 'Remote tag and temporary worktree boundaries' {
    It 'peels annotated tags independently of listing order' {
        Mock git -ModuleName ReleasePublication {
            $global:LASTEXITCODE = 0
            @(
                "commit`trefs/tags/annotated^{}"
                "object`trefs/tags/annotated"
                "other-commit`trefs/tags/lightweight"
            )
        }
        $tags = InModuleScope ReleasePublication { Get-ReleaseTagMap }
        $tags.annotated | Should -Be 'commit'
        $tags.lightweight | Should -Be 'other-commit'
    }

    It 'rejects malformed tag inventory rather than treating it as missing releases' {
        Mock git -ModuleName ReleasePublication { 'not a tag record' }
        { InModuleScope ReleasePublication { Get-ReleaseTagMap } } | Should -Throw
    }

    It 'keeps differently cased Git tag names distinct' {
        Mock git -ModuleName ReleasePublication {
            @("first`trefs/tags/App", "second`trefs/tags/app")
        }
        $tags = InModuleScope ReleasePublication { Get-ReleaseTagMap }
        $tags.Count | Should -Be 2
        $tags['App'] | Should -Be 'first'
        $tags['app'] | Should -Be 'second'
    }

    It 'rejects a peeled tag without its reference' {
        Mock git -ModuleName ReleasePublication { "commit`trefs/tags/app^{}" }
        { InModuleScope ReleasePublication { Get-ReleaseTagMap } } | Should -Throw
    }

    It 'propagates ancestry lookup failures with their original diagnostic' {
        Mock git -ModuleName ReleasePublication {
            $global:LASTEXITCODE = 128
            'fatal: fixture history is unavailable'
        }
        {
            InModuleScope ReleasePublication {
                Test-ReleaseSourceAncestry -Source 'source' -Candidate 'candidate'
            }
        } | Should -Throw '*fatal: fixture history is unavailable*'
    }

    It 'uses exact remote tag references for post-write confirmation' {
        Mock git -ModuleName ReleasePublication { "commit`trefs/tags/app-v1.0.0" }
        InModuleScope ReleasePublication { Get-ReleaseTagMap -Tag 'app-v1.0.0' } | Out-Null
        Should -Invoke git -ModuleName ReleasePublication -Times 1 -Exactly -ParameterFilter {
            $args -contains 'refs/tags/app-v1.0.0' -and $args -contains 'refs/tags/app-v1.0.0^{}'
        }
    }

    It 'removes its temporary worktree when the action fails' {
        Mock git -ModuleName ReleasePublication {}
        {
            InModuleScope ReleasePublication {
                Invoke-ReleaseWorktree -Commit 'commit' -Action { throw 'Candidate failed.' }
            }
        } | Should -Throw
        Should -Invoke git -ModuleName ReleasePublication -Times 1 -Exactly -ParameterFilter {
            $args[0] -eq 'worktree' -and $args[1] -eq 'remove' -and $args[2] -eq '--force'
        }
    }

    It 'preserves both the operation and cleanup failures' {
        Mock git -ModuleName ReleasePublication {
            if ($args[1] -eq 'remove') { throw 'Cleanup failed.' }
        }
        $failure = try {
            InModuleScope ReleasePublication {
                Invoke-ReleaseWorktree -Commit 'commit' -Action { throw 'Candidate failed.' }
            }
        } catch { $_ }
        $failure.Exception | Should -BeOfType ([AggregateException])
        $failure.Exception.InnerExceptions.Count | Should -Be 2
    }
}

Describe 'Immutable binary build planning' {
    BeforeEach {
        $script:outputs = @{}
        Mock Get-PublishableBinaryCrate -ModuleName ReleasePublication {
            [pscustomobject]@{ Name = 'app'; Version = '1.0.0' }
        }
        Mock Get-MissingBinaryMatrix -ModuleName ReleasePublication {
            [pscustomobject]@{ name = 'app'; version = '1.0.0'; tag = 'app-v1.0.0' }
        }
        Mock Get-ReleaseTagMap -ModuleName ReleasePublication { @{ 'app-v1.0.0' = 'c' * 40 } }
        Mock Set-GitHubOutput -ModuleName ReleasePublication { $outputs[$Name] = $Value }
    }

    It 'pins each checkout to the resolved commit without changing the release tag' {
        Invoke-ReleaseBinaryPlan -Repository 'owner/repo'

        $matrix = ConvertFrom-Json $outputs.matrix -NoEnumerate
        $matrix[0].source_sha | Should -Be ('c' * 40)
        $matrix[0].tag | Should -Be 'app-v1.0.0'
        $outputs.has_binaries | Should -Be 'true'
    }

    It 'rejects a matrix entry whose source tag is missing' {
        Mock Get-ReleaseTagMap -ModuleName ReleasePublication { @{} }
        { Invoke-ReleaseBinaryPlan -Repository 'owner/repo' } | Should -Throw
        $outputs.Count | Should -Be 0
    }

    It 'emits an empty matrix without querying tags when every asset exists' {
        Mock Get-MissingBinaryMatrix -ModuleName ReleasePublication { @() }
        Invoke-ReleaseBinaryPlan -Repository 'owner/repo'
        $outputs.matrix | Should -Be '[]'
        $outputs.has_binaries | Should -Be 'false'
        Should -Invoke Get-ReleaseTagMap -ModuleName ReleasePublication -Times 0 -Exactly
    }

    It 'handles a workspace with no binary packages' {
        Mock Get-PublishableBinaryCrate -ModuleName ReleasePublication { @() }
        Invoke-ReleaseBinaryPlan -Repository 'owner/repo'
        $outputs.matrix | Should -Be '[]'
        Should -Invoke Get-MissingBinaryMatrix -ModuleName ReleasePublication -Times 0 -Exactly
    }
}

Describe 'Release workflow ownership' {
    It 'keeps both release-plz forge operations disabled' {
        $config = Get-Content -LiteralPath (Join-Path $PSScriptRoot '..' '..' 'release-plz.toml') -Raw
        $config | Should -Match '(?m)^git_tag_enable = false$'
        $config | Should -Match '(?m)^git_release_enable = false$'
        $config | Should -Not -Match '(?m)^git_(tag|release)_enable = true$'
        $config | Should -Match ([regex]::Escape('git_tag_name = "{{ package }}-v{{ version }}"'))
    }

    It 'checks out the pinned matrix source and uploads to the versioned tag' {
        $workflow = Get-Content -LiteralPath (Join-Path $PSScriptRoot '..' '..' '.github' 'workflows' 'release.yml') -Raw
        $workflow | Should -Match 'ref: \$\{\{ matrix\.source_sha \}\}'
        $workflow | Should -Match 'ref: refs/tags/\$\{\{ matrix\.tag \}\}'
        $workflow | Should -Match 'just gh-reconcile-releases'
        $workflow | Should -Not -Match 'gh-compose-release-config|gh-create-missing-binary-releases'
    }
}
