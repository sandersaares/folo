#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }

# Pester suite for BenchHistoryIssue.psm1. The one tool that would touch GitHub for real -- `gh` --
# is isolated behind the module's function seams and mocked here in the module's scope, so the
# find-or-file logic (dedup by exact title, update-in-place vs. create, error propagation) is
# exercised without opening or editing a real issue. The body-file existence guard is checked
# against a real temp file so the on-disk behaviour is asserted, not faked.

BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'BenchHistoryIssue.psm1') -Force

    # A real body file so Publish-RollingIssue's Test-Path guard passes; `gh` is mocked, so its
    # contents never matter.
    $script:BodyFile = Join-Path ([System.IO.Path]::GetTempPath()) ("bh-body-$([guid]::NewGuid().ToString('n')).md")
    Set-Content -LiteralPath $script:BodyFile -Value 'rendered body' -Encoding utf8
}

AfterAll {
    Remove-Item -LiteralPath $script:BodyFile -ErrorAction SilentlyContinue
}

Describe 'Get-OpenIssueByTitle (mocked gh issue list)' {
    Context 'when an open issue with the exact title exists' {
        BeforeEach {
            Mock gh -ModuleName BenchHistoryIssue {
                $global:LASTEXITCODE = 0
                '[{"number":7,"title":"Something else","url":"https://github.com/o/r/issues/7"},{"number":42,"title":"Benchmark regressions detected","url":"https://github.com/o/r/issues/42"}]'
            }
        }

        It 'returns that issue' {
            $issue = Get-OpenIssueByTitle -Title 'Benchmark regressions detected' -Label 'regression'
            $issue.number | Should -Be 42
            $issue.url | Should -Be 'https://github.com/o/r/issues/42'
        }

        It 'narrows the list to the given label and open state' {
            Get-OpenIssueByTitle -Title 'Benchmark regressions detected' -Label 'regression' | Out-Null
            Should -Invoke gh -ModuleName BenchHistoryIssue -ParameterFilter {
                ($args -contains '--label') -and ($args -contains 'regression') -and
                ($args -contains '--state') -and ($args -contains 'open')
            }
        }
    }

    Context 'when no listed issue has a matching title' {
        BeforeEach {
            Mock gh -ModuleName BenchHistoryIssue {
                $global:LASTEXITCODE = 0
                '[{"number":7,"title":"Something else","url":"https://github.com/o/r/issues/7"}]'
            }
        }

        It 'returns null' {
            Get-OpenIssueByTitle -Title 'Benchmark regressions detected' -Label 'regression' | Should -BeNullOrEmpty
        }
    }

    Context 'when the list is empty' {
        BeforeEach {
            Mock gh -ModuleName BenchHistoryIssue { $global:LASTEXITCODE = 0; '[]' }
        }

        It 'returns null' {
            Get-OpenIssueByTitle -Title 'Benchmark regressions detected' -Label 'regression' | Should -BeNullOrEmpty
        }
    }

    Context 'when gh fails' {
        BeforeEach {
            Mock gh -ModuleName BenchHistoryIssue { $global:LASTEXITCODE = 1; 'HTTP 503: Service Unavailable' }
            Mock Start-Sleep -ModuleName Retry { }
        }

        It 'throws, surfacing the gh output' {
            { Get-OpenIssueByTitle -Title 'Benchmark regressions detected' -Label 'regression' } |
                Should -Throw '*503*'
        }
    }

    Context 'when gh prints a warning to stderr but still exits 0' {
        BeforeEach {
            # Reproduce the real gotcha: gh emits a note on stderr (deprecation, rate-limit, etc.)
            # while returning valid JSON on stdout and exiting 0. Write-Error lands on PowerShell's
            # error stream (stream 2) - where a native gh's stderr also goes - so a naive `2>&1`
            # merge would corrupt the JSON; the module must parse stdout only. `-ErrorAction
            # Continue` keeps the note non-terminating even under the suite's `$ErrorActionPreference
            # = 'Stop'`, so it stays a stream-2 write the module redirects away rather than a throw.
            Mock gh -ModuleName BenchHistoryIssue {
                Write-Error 'gh: a new release of gh is available' -ErrorAction Continue
                $global:LASTEXITCODE = 0
                '[{"number":42,"title":"Benchmark regressions detected","url":"https://github.com/o/r/issues/42"}]'
            }
        }

        It 'ignores the stderr note and still parses the issue from stdout' {
            $issue = Get-OpenIssueByTitle -Title 'Benchmark regressions detected' -Label 'regression'
            $issue.number | Should -Be 42
        }
    }

    Context 'when gh fails and writes its error to stderr' {
        BeforeEach {
            Mock gh -ModuleName BenchHistoryIssue {
                Write-Error 'GraphQL: rate limit exceeded' -ErrorAction Continue
                $global:LASTEXITCODE = 1
            }
            Mock Start-Sleep -ModuleName Retry { }
        }

        It 'surfaces the stderr text in the thrown error' {
            { Get-OpenIssueByTitle -Title 'Benchmark regressions detected' -Label 'regression' } |
                Should -Throw '*rate limit exceeded*'
        }
    }
}

Describe 'Publish-RollingIssue (mocked gh)' {
    Context 'when an open issue with the title already exists' {
        BeforeEach {
            Mock gh -ModuleName BenchHistoryIssue {
                switch ("$($args[0]) $($args[1])") {
                    'issue list' {
                        $global:LASTEXITCODE = 0
                        '[{"number":42,"title":"Benchmark regressions detected","url":"https://github.com/o/r/issues/42"}]'
                    }
                    'issue edit' { $global:LASTEXITCODE = 0; 'https://github.com/o/r/issues/42' }
                    'issue create' { $global:LASTEXITCODE = 0; 'https://github.com/o/r/issues/99' }
                    default { $global:LASTEXITCODE = 1; "unexpected: $args" }
                }
            }
        }

        It 'updates the existing issue instead of creating a duplicate' {
            Publish-RollingIssue -Title 'Benchmark regressions detected' -Label 'regression' -BodyFile $script:BodyFile | Out-Null
            Should -Invoke gh -ModuleName BenchHistoryIssue -ParameterFilter { $args[1] -eq 'edit' }
            Should -Invoke gh -ModuleName BenchHistoryIssue -ParameterFilter { $args[1] -eq 'create' } -Times 0 -Exactly
        }

        It 'edits the matched issue number and passes the body file' {
            Publish-RollingIssue -Title 'Benchmark regressions detected' -Label 'regression' -BodyFile $script:BodyFile | Out-Null
            Should -Invoke gh -ModuleName BenchHistoryIssue -ParameterFilter {
                ($args[1] -eq 'edit') -and ($args -contains 42) -and
                ($args -contains '--body-file') -and ($args -contains $script:BodyFile)
            }
        }

        It 'returns the existing issue url' {
            Publish-RollingIssue -Title 'Benchmark regressions detected' -Label 'regression' -BodyFile $script:BodyFile |
                Should -Be 'https://github.com/o/r/issues/42'
        }
    }

    Context 'when no open issue with the title exists' {
        BeforeEach {
            Mock gh -ModuleName BenchHistoryIssue {
                switch ("$($args[0]) $($args[1])") {
                    'issue list' { $global:LASTEXITCODE = 0; '[]' }
                    'issue create' { $global:LASTEXITCODE = 0; 'https://github.com/o/r/issues/99' }
                    default { $global:LASTEXITCODE = 1; "unexpected: $args" }
                }
            }
        }

        It 'creates a new issue with the given title and label' {
            Publish-RollingIssue -Title 'Benchmark regressions detected' -Label 'regression' -BodyFile $script:BodyFile | Out-Null
            Should -Invoke gh -ModuleName BenchHistoryIssue -ParameterFilter {
                ($args[1] -eq 'create') -and
                ($args -contains '--title') -and ($args -contains 'Benchmark regressions detected') -and
                ($args -contains '--label') -and ($args -contains 'regression') -and
                ($args -contains '--body-file') -and ($args -contains $script:BodyFile)
            }
        }

        It 'narrows the dedup search to the same label it files under' {
            Publish-RollingIssue -Title 'Benchmark regressions detected' -Label 'regression' -BodyFile $script:BodyFile | Out-Null
            Should -Invoke gh -ModuleName BenchHistoryIssue -ParameterFilter {
                ($args[1] -eq 'list') -and ($args -contains '--label') -and ($args -contains 'regression')
            }
        }

        It 'returns the created issue url' {
            Publish-RollingIssue -Title 'Benchmark regressions detected' -Label 'regression' -BodyFile $script:BodyFile |
                Should -Be 'https://github.com/o/r/issues/99'
        }
    }

    Context 'error handling' {
        It 'throws when the body file does not exist' {
            $missing = Join-Path ([System.IO.Path]::GetTempPath()) "bh-missing-$([guid]::NewGuid().ToString('n')).md"
            { Publish-RollingIssue -Title 'Benchmark regressions detected' -Label 'regression' -BodyFile $missing } |
                Should -Throw '*does not exist*'
        }

        It 'throws when gh create fails, surfacing its output' {
            Mock gh -ModuleName BenchHistoryIssue {
                switch ("$($args[0]) $($args[1])") {
                    'issue list' { $global:LASTEXITCODE = 0; '[]' }
                    'issue create' { $global:LASTEXITCODE = 1; 'could not create issue' }
                    default { $global:LASTEXITCODE = 1; "unexpected: $args" }
                }
            }
            { Publish-RollingIssue -Title 'Benchmark regressions detected' -Label 'regression' -BodyFile $script:BodyFile } |
                Should -Throw '*could not create issue*'
        }

        It 'throws when gh edit fails, surfacing its output' {
            Mock gh -ModuleName BenchHistoryIssue {
                switch ("$($args[0]) $($args[1])") {
                    'issue list' {
                        $global:LASTEXITCODE = 0
                        '[{"number":42,"title":"Benchmark regressions detected","url":"https://github.com/o/r/issues/42"}]'
                    }
                    'issue edit' { $global:LASTEXITCODE = 1; 'could not edit issue' }
                    default { $global:LASTEXITCODE = 1; "unexpected: $args" }
                }
            }
            { Publish-RollingIssue -Title 'Benchmark regressions detected' -Label 'regression' -BodyFile $script:BodyFile } |
                Should -Throw '*could not edit issue*'
        }
    }
}

Describe 'Close-RollingIssue (mocked gh)' {
    Context 'when open issues with the exact title exist' {
        BeforeEach {
            Mock gh -ModuleName BenchHistoryIssue {
                switch ("$($args[0]) $($args[1])") {
                    'issue list' {
                        $global:LASTEXITCODE = 0
                        '[{"number":7,"title":"Something else"},{"number":42,"title":"bench-history workflow failed"},{"number":43,"title":"bench-history workflow failed"}]'
                    }
                    'issue close' { $global:LASTEXITCODE = 0; '' }
                    default { $global:LASTEXITCODE = 1; "unexpected: $args" }
                }
            }
        }

        It 'closes every matching issue and only those' {
            Close-RollingIssue -Title 'bench-history workflow failed' -Label 'ci-failure' -Comment 'green again' | Out-Null
            Should -Invoke gh -ModuleName BenchHistoryIssue -ParameterFilter {
                ($args[1] -eq 'close') -and ($args -contains 42)
            }
            Should -Invoke gh -ModuleName BenchHistoryIssue -ParameterFilter {
                ($args[1] -eq 'close') -and ($args -contains 43)
            }
            # The non-matching issue #7 is never touched.
            Should -Invoke gh -ModuleName BenchHistoryIssue -ParameterFilter {
                ($args[1] -eq 'close') -and ($args -contains 7)
            } -Times 0 -Exactly
        }

        It 'closes with reason completed and the given audit comment' {
            Close-RollingIssue -Title 'bench-history workflow failed' -Label 'ci-failure' -Comment 'green again' | Out-Null
            Should -Invoke gh -ModuleName BenchHistoryIssue -ParameterFilter {
                ($args[1] -eq 'close') -and ($args -contains '--reason') -and ($args -contains 'completed') -and
                ($args -contains '--comment') -and ($args -contains 'green again')
            }
        }

        It 'narrows the list to the given label and open state' {
            Close-RollingIssue -Title 'bench-history workflow failed' -Label 'ci-failure' -Comment 'green again' | Out-Null
            Should -Invoke gh -ModuleName BenchHistoryIssue -ParameterFilter {
                ($args[1] -eq 'list') -and ($args -contains '--label') -and ($args -contains 'ci-failure') -and
                ($args -contains '--state') -and ($args -contains 'open')
            }
        }

        It 'returns the closed issue numbers' {
            $closed = @(Close-RollingIssue -Title 'bench-history workflow failed' -Label 'ci-failure' -Comment 'green again')
            $closed.Count | Should -Be 2
            $closed | Should -Contain 42
            $closed | Should -Contain 43
        }
    }

    Context 'when no open issue has a matching title' {
        BeforeEach {
            Mock gh -ModuleName BenchHistoryIssue {
                switch ("$($args[0]) $($args[1])") {
                    'issue list' { $global:LASTEXITCODE = 0; '[{"number":7,"title":"Something else"}]' }
                    'issue close' { $global:LASTEXITCODE = 0; '' }
                    default { $global:LASTEXITCODE = 1; "unexpected: $args" }
                }
            }
        }

        It 'closes nothing and returns an empty array' {
            $closed = @(Close-RollingIssue -Title 'bench-history workflow failed' -Label 'ci-failure' -Comment 'green again')
            $closed.Count | Should -Be 0
            Should -Invoke gh -ModuleName BenchHistoryIssue -ParameterFilter { $args[1] -eq 'close' } -Times 0 -Exactly
        }
    }

    Context 'when the open-issue list is empty' {
        BeforeEach {
            Mock gh -ModuleName BenchHistoryIssue {
                switch ("$($args[0]) $($args[1])") {
                    'issue list' { $global:LASTEXITCODE = 0; '[]' }
                    'issue close' { $global:LASTEXITCODE = 0; '' }
                    default { $global:LASTEXITCODE = 1; "unexpected: $args" }
                }
            }
        }

        It 'closes nothing and returns an empty array' {
            @(Close-RollingIssue -Title 'bench-history workflow failed' -Label 'ci-failure' -Comment 'green again').Count | Should -Be 0
        }
    }

    Context 'when gh list fails' {
        BeforeEach {
            Mock gh -ModuleName BenchHistoryIssue { $global:LASTEXITCODE = 1; 'HTTP 503: Service Unavailable' }
            Mock Start-Sleep -ModuleName Retry { }
        }

        It 'throws, surfacing the gh output' {
            { Close-RollingIssue -Title 'bench-history workflow failed' -Label 'ci-failure' -Comment 'green again' } |
                Should -Throw '*503*'
        }
    }

    Context 'when gh close fails' {
        BeforeEach {
            Mock gh -ModuleName BenchHistoryIssue {
                switch ("$($args[0]) $($args[1])") {
                    'issue list' {
                        $global:LASTEXITCODE = 0
                        '[{"number":42,"title":"bench-history workflow failed"}]'
                    }
                    'issue close' { $global:LASTEXITCODE = 1; 'could not close issue' }
                    default { $global:LASTEXITCODE = 1; "unexpected: $args" }
                }
            }
        }

        It 'throws, surfacing the gh output' {
            { Close-RollingIssue -Title 'bench-history workflow failed' -Label 'ci-failure' -Comment 'green again' } |
                Should -Throw '*could not close issue*'
        }
    }
}
