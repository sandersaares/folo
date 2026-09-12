#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
# Exercises the same-workflow report job with fake GitHub responses and repository-local files.
# No test writes to GitHub or requires Rust; setup-only failures and interrupted publication
# must remain useful and retryable. Archive tests preserve bounded reads and cleanup.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
$VerbosePreference = 'Continue'

Import-Module (Join-Path $PSScriptRoot 'ScheduledGitHub.psm1') -Force

Describe 'Same-workflow failure reporting' {
    InModuleScope ScheduledGitHub {
        BeforeAll {
            $script:directory = Join-Path (Get-Location).Path "target\scheduled-reporter-tests-$([guid]::NewGuid().ToString('N'))"
            $null = New-Item -ItemType Directory -Path $script:directory -Force
        }
        AfterAll { Remove-Item -LiteralPath $script:directory -Recurse -Force }
        BeforeEach {
            $script:run = @{
                id = 10; run_attempt = 1; status = 'in_progress'; conclusion = $null
                name = 'Deep validation'
                head_sha = 'a' * 40; run_started_at = '2026-09-11T00:01:00Z'
            }
            $script:jobs = @(@{
                id = 20; name = 'plan'; html_url = 'https://github.com/example/repo/actions/runs/10/job/20'
                run_id = 10; run_attempt = 1; status = 'completed'
                started_at = '2026-09-11T00:01:10Z'; completed_at = '2026-09-11T00:01:20Z'
                conclusion = 'failure'; steps = @(@{ name = 'Install Rust'; conclusion = 'failure' })
            }, @{
                id = 21; name = 'independent-success'; html_url = 'https://github.com/example/repo/actions/runs/10/job/21'
                run_id = 10; run_attempt = 1; status = 'completed'
                started_at = '2026-09-11T00:01:10Z'; completed_at = '2026-09-11T00:01:20Z'
                conclusion = 'success'; steps = @()
            }, @{
                id = 22; name = 'report'; html_url = 'https://github.com/example/repo/actions/runs/10/job/22'
                run_id = 10; run_attempt = 1; status = 'in_progress'; conclusion = $null; steps = @()
            })
            $script:artifacts = @()
            $script:issues = @()
            $script:comments = @()
            $script:writes = [Collections.Generic.List[hashtable]]::new()
            $script:artifactReads = [Collections.Generic.List[long]]::new()
            Mock Invoke-ScheduledGitHubJson {
                param($Endpoint, $Method, $Body)
                if ($Method -in @('POST', 'PATCH')) {
                    $script:writes.Add(@{ endpoint = $Endpoint; method = $Method; body = $Body })
                    return @{ number = 50; html_url = 'https://github.com/example/repo/issues/50'; body = $Body['body'] }
                }
                switch -Regex ($Endpoint) {
                    '^repos/example/repo/actions/runs/10$' { return $script:run }
                    '/jobs\?filter=all&per_page=100&page=1$' { return @{ jobs = $script:jobs } }
                    '/artifacts\?per_page=100&page=1$' { return @{ artifacts = $script:artifacts } }
                    '/issues\?state=all&labels=scheduled-run-failure&per_page=100&page=1$' { return ,$script:issues }
                    '/issues/\d+/comments\?per_page=100&page=1$' { return ,$script:comments }
                    '/labels\?per_page=100&page=1$' { return ,@(@{ name = 'scheduled-run-failure' }) }
                    default { throw "Unexpected test API request: $Endpoint" }
                }
            }
            Mock Save-ScheduledGitHubFile {
                param($Path)
                Set-Content -LiteralPath $Path -Value "##[error]Rust toolchain download failed`nHTTP 503"
            }
            Mock Read-ScheduledArtifactText {
                param($Artifact)
                $script:artifactReads.Add([long]$Artifact.id)
                return "# Check miri-linux`nTested source: $('a' * 40)`nPackages: events`nMISSED mutant: return false`nseed: 29`nReplay: just package=events miri"
            }
        }
        It 'reports setup failure without Rust or result artifacts and omits successful jobs' {
            Invoke-ScheduledReporting example/repo 10 1 $script:directory | Should -Match '/issues/50'
            $script:writes.Count | Should -Be 1
            $issue = $script:writes[0].body
            $issue.title | Should -BeExactly 'Scheduled validation failed on 2026-09-11'
            $issue.labels | Should -Contain 'scheduled-run-failure'
            $issue.body | Should -Match '^\[Copilot speaking\]'
            $issue.body | Should -Match '/runs/10/attempts/1'
            $issue.body | Should -Match 'Rust toolchain download failed'
            $issue.body | Should -Match 'HTTP 503'
            $issue.body | Should -Match 'No check-summary artifact'
            $issue.body | Should -Match 'Tested commit: `a{40}`'
            $issue.body | Should -Match 'Workflow status: in_progress'
            $issue.body | Should -Not -Match 'independent-success|/job/22'
            Test-Path -LiteralPath (Join-Path $script:directory 'job-20.log') | Should -BeFalse
            Should -Invoke Read-ScheduledArtifactText -Times 0
        }
        It 'does not report <Conclusion> jobs without a completed failure' -ForEach @(
            @{ Conclusion = 'success' }, @{ Conclusion = 'skipped' },
            @{ Conclusion = 'cancelled' }, @{ Conclusion = 'neutral' }
        ) {
            $script:jobs[0].conclusion = $Conclusion
            $null = Invoke-ScheduledReporting example/repo 10 1 $script:directory
            $script:writes.Count | Should -Be 0
            Should -Invoke Read-ScheduledArtifactText -Times 0
        }
        It 'ignores in-progress jobs and a completed failed reporter' {
            $script:jobs[0].status = 'in_progress'
            $script:jobs[2].status = 'completed'
            $script:jobs[2].conclusion = 'failure'
            $null = Invoke-ScheduledReporting example/repo 10 1 $script:directory
            $script:writes.Count | Should -Be 0
            Should -Invoke Save-ScheduledGitHubFile -Times 0
        }
        It 'reports completed <Conclusion> setup and check failures' -ForEach @(
            @{ Conclusion = 'failure' }, @{ Conclusion = 'timed_out' }, @{ Conclusion = 'action_required' }
        ) {
            $script:jobs[0].conclusion = $Conclusion
            $null = Invoke-ScheduledReporting example/repo 10 1 $script:directory
            $script:writes[0].body.body | Should -Match ([regex]::Escape("| $Conclusion |"))
        }
        It 'reads summaries only for failed effective job names and their execution attempts' {
            $script:jobs[0].name = 'miri-linux'
            $script:artifacts = @(
                @{ id = 31; name = 'scheduled-result-10-1-miri-linux'; expired = $false }
                @{ id = 32; name = 'scheduled-result-10-2-miri-linux'; expired = $false }
                @{ id = 33; name = 'scheduled-result-10-1-independent-success'; expired = $false }
                @{ id = 34; name = 'scheduled-result-11-1-miri-linux'; expired = $false }
                @{ id = 35; name = 'scheduled-result-10-1-miri-linux-other'; expired = $false }
            )
            $null = Invoke-ScheduledReporting example/repo 10 1 $script:directory
            $text = $script:writes[0].body.body
            $text | Should -Match 'Tested commit: `a{40}`'
            $text | Should -Match 'MISSED mutant: return false'
            $text | Should -Match 'seed: 29'
            $text | Should -Match 'Replay: just package=events miri'
            $script:artifactReads | Should -Be @(31)
        }
        It 'reports cached failed dependencies when only the reporter is rerun' {
            $script:run.run_attempt = 2
            $script:jobs[2].run_attempt = 2
            $script:jobs[0].name = 'miri-linux'
            $script:artifacts = @(
                @{ id = 31; name = 'scheduled-result-10-1-miri-linux'; expired = $false }
                @{ id = 33; name = 'scheduled-result-10-2-miri-linux'; expired = $false }
                @{ id = 32; name = 'scheduled-result-10-3-miri-linux'; expired = $false }
            )
            $null = Invoke-ScheduledReporting example/repo 10 2 $script:directory
            $text = $script:writes[0].body.body
            $text | Should -Match '/runs/10/attempts/2'
            $text | Should -Match 'Job execution attempt: 1'
            $text | Should -Match 'MISSED mutant'
            $script:artifactReads | Should -Be @(31)
            Should -Invoke Save-ScheduledGitHubFile -Times 1 -Exactly -ParameterFilter {
                $Endpoint -ceq 'repos/example/repo/actions/jobs/20/logs'
            }
        }
        It 'uses each effective failure execution rather than the current reporter attempt' {
            $script:run.run_attempt = 2
            $script:jobs[2].run_attempt = 2
            $script:jobs[0].name = 'miri-linux'
            $script:jobs[1].name = 'miri-windows'
            $script:jobs[1].conclusion = 'failure'
            $script:jobs[1].run_attempt = 2
            $script:artifacts = @(
                @{ id = 31; name = 'scheduled-result-10-1-miri-linux'; expired = $false }
                @{ id = 32; name = 'scheduled-result-10-2-miri-windows'; expired = $false }
                @{ id = 33; name = 'scheduled-result-10-1-miri-windows'; expired = $false }
            )
            $null = Invoke-ScheduledReporting example/repo 10 2 $script:directory
            $text = $script:writes[0].body.body
            $text | Should -Match '\[miri-linux\]'
            $text | Should -Match '\[miri-windows\]'
            $script:artifactReads | Should -Be @(31, 32)
        }
        It 'recognizes cached job copies with new IDs and unchanged execution timestamps' {
            $script:run.run_attempt = 2
            $script:jobs[2].run_attempt = 2
            $script:jobs[0].name = 'miri-linux'
            $copy = $script:jobs[0].Clone()
            $copy.id = 40
            $copy.html_url = 'https://github.com/example/repo/actions/runs/10/job/40'
            $copy.run_attempt = 2
            $script:jobs += $copy
            $script:artifacts = @(
                @{ id = 31; name = 'scheduled-result-10-1-miri-linux'; expired = $false }
                @{ id = 32; name = 'scheduled-result-10-2-miri-linux'; expired = $false }
            )
            $null = Invoke-ScheduledReporting example/repo 10 2 $script:directory
            $script:artifactReads | Should -Be @(31)
            $script:writes[0].body.body | Should -Match 'Job execution attempt: 1'
            Should -Invoke Save-ScheduledGitHubFile -Times 1 -Exactly -ParameterFilter {
                $Endpoint -ceq 'repos/example/repo/actions/jobs/20/logs'
            }
        }
        It 'selects a genuinely reexecuted failed job instead of its earlier failure' {
            $script:run.run_attempt = 2
            $script:jobs[0].name = 'miri-linux'
            $rerun = $script:jobs[0].Clone()
            $rerun.id = 40
            $rerun.html_url = 'https://github.com/example/repo/actions/runs/10/job/40'
            $rerun.run_attempt = 2
            $rerun.started_at = '2026-09-11T00:02:10Z'
            $rerun.completed_at = '2026-09-11T00:02:20Z'
            $script:jobs += $rerun
            $script:artifacts = @(
                @{ id = 31; name = 'scheduled-result-10-1-miri-linux'; expired = $false }
                @{ id = 32; name = 'scheduled-result-10-2-miri-linux'; expired = $false }
            )
            $null = Invoke-ScheduledReporting example/repo 10 2 $script:directory
            $script:artifactReads | Should -Be @(32)
            @([regex]::Matches($script:writes[0].body.body, '\| \[miri-linux\]')).Count | Should -Be 1
            Should -Invoke Save-ScheduledGitHubFile -Times 1 -Exactly -ParameterFilter {
                $Endpoint -ceq 'repos/example/repo/actions/jobs/40/logs'
            }
        }
        It 'does not resurrect an older failure after a check succeeds on rerun' {
            $script:run.run_attempt = 2
            $rerun = $script:jobs[0].Clone()
            $rerun.id = 40
            $rerun.run_attempt = 2
            $rerun.conclusion = 'success'
            $script:jobs += $rerun
            $script:artifacts = @(@{ id = 31; name = 'scheduled-result-10-1-miri-linux'; expired = $false })
            $null = Invoke-ScheduledReporting example/repo 10 2 $script:directory
            $script:writes.Count | Should -Be 0
            Should -Invoke Read-ScheduledArtifactText -Times 0
        }
        It 'does not report an older failure while its newer execution is still running' {
            $script:run.run_attempt = 2
            $rerun = $script:jobs[0].Clone()
            $rerun.id = 40
            $rerun.run_attempt = 2
            $rerun.status = 'in_progress'
            $rerun.conclusion = $null
            $script:jobs += $rerun
            $null = Invoke-ScheduledReporting example/repo 10 2 $script:directory
            $script:writes.Count | Should -Be 0
        }
        It 'rejects a stale requested reporter attempt' {
            $script:run.run_attempt = 2
            { Invoke-ScheduledReporting example/repo 10 1 $script:directory } | Should -Throw
            $script:writes.Count | Should -Be 0
        }
        It 'rejects <Case> job data before publishing' -ForEach @(
            @{ Case = 'another run'; JobRun = 11; JobAttempt = 1 }
            @{ Case = 'a missing execution attempt'; JobRun = 10; JobAttempt = $null }
        ) {
            $script:jobs[0].run_id = $JobRun
            $script:jobs[0].run_attempt = $JobAttempt
            { Invoke-ScheduledReporting example/repo 10 1 $script:directory } | Should -Throw
            $script:writes.Count | Should -Be 0
        }
        It 'ignores future executions while retaining the requested attempts history' {
            $script:jobs[0].name = 'miri-linux'
            $future = $script:jobs[0].Clone()
            $future.id = 40
            $future.run_attempt = 2
            $future.conclusion = 'success'
            $script:jobs += $future
            $script:artifacts = @(@{ id = 31; name = 'scheduled-result-10-1-miri-linux'; expired = $false })
            $null = Invoke-ScheduledReporting example/repo 10 1 $script:directory
            $script:artifactReads | Should -Be @(31)
            $script:writes[0].body.body | Should -Match 'Job execution attempt: 1'
        }
        It 'requests current run metadata and complete job history' {
            $null = Invoke-ScheduledReporting example/repo 10 1 $script:directory
            Should -Invoke Invoke-ScheduledGitHubJson -Times 1 -Exactly -ParameterFilter {
                $Endpoint -ceq 'repos/example/repo/actions/runs/10'
            }
            Should -Invoke Invoke-ScheduledGitHubJson -Times 1 -Exactly -ParameterFilter {
                $Endpoint -ceq 'repos/example/repo/actions/runs/10/jobs?filter=all&per_page=100&page=1'
            }
            Should -Invoke Invoke-ScheduledGitHubJson -Times 0 -Exactly -ParameterFilter {
                $Endpoint -match '/attempts/'
            }
        }
        It 'reports setup failure even when the API has no step details' {
            $script:jobs[0].Remove('steps')
            $null = Invoke-ScheduledReporting example/repo 10 1 $script:directory
            $text = $script:writes[0].body.body
            $text | Should -Match 'Tested commit: `a{40}`'
            $text | Should -Match 'Job execution attempt: 1'
            $text | Should -Match 'Rust toolchain download failed'
            Should -Invoke Read-ScheduledArtifactText -Times 0
        }
        It 'reuses a human report without overwriting it or requiring an author/schema' {
            $script:issues = @(@{
                number = 42; state = 'closed'; html_url = 'https://github.com/example/repo/issues/42'
                body = 'Investigate this attempt: https://github.com/example/repo/actions/runs/10/attempts/1'
            })
            $script:comments = @(@{ body = 'Compare https://github.com/example/repo/actions/runs/10/attempts/2' })
            $null = Invoke-ScheduledReporting example/repo 10 1 $script:directory
            $script:writes.Count | Should -Be 1
            $script:writes[0].endpoint | Should -BeExactly 'repos/example/repo/issues/42/comments'
        }
        It 'does not reuse a closed <BodyAttempt> report because a comment links a newer failure' -ForEach @(
            @{ BodyAttempt = 'https://github.com/example/repo/actions/runs/10/attempts/1' }
            @{ BodyAttempt = 'https://github.com/example/repo/actions/runs/9/attempts/2' }
        ) {
            $script:run.run_attempt = 2
            $script:issues = @(@{
                number = 42; state = 'closed'; html_url = 'https://github.com/example/repo/issues/42'
                body = "Failed attempt: $BodyAttempt"; comments = 1
            })
            $script:comments = @(@{ body = 'Compare https://github.com/example/repo/actions/runs/10/attempts/2' })
            $null = Invoke-ScheduledReporting example/repo 10 2 $script:directory
            $script:writes.Count | Should -Be 1
            $script:writes[0].endpoint | Should -BeExactly 'repos/example/repo/issues'
            $script:writes[0].body.body | Should -Match '/runs/10/attempts/2'
            Should -Invoke Invoke-ScheduledGitHubJson -Times 0 -Exactly -ParameterFilter {
                $Endpoint -match '/issues/42/comments'
            }
        }
        It 'reuses a human report whose exact attempt link appears on a later discussion page' {
            $script:issues = @(@{
                number = 42; state = 'closed'; html_url = 'https://github.com/example/repo/issues/42'
                body = 'Planning failed before any checker ran. See the discussion for the run.'; comments = 101
            })
            Mock Invoke-ScheduledGitHubJson {
                return ,@(1..100 | ForEach-Object { @{ body = "Investigation note $_" } })
            } -ParameterFilter {
                $Endpoint -ceq 'repos/example/repo/issues/42/comments?per_page=100&page=1'
            }
            Mock Invoke-ScheduledGitHubJson {
                return ,@(@{ body = 'Failed attempt: https://github.com/example/repo/actions/runs/10/attempts/1' })
            } -ParameterFilter {
                $Endpoint -ceq 'repos/example/repo/issues/42/comments?per_page=100&page=2'
            }
            $null = Invoke-ScheduledReporting example/repo 10 1 $script:directory
            $script:writes.Count | Should -Be 1
            $script:writes[0].endpoint | Should -BeExactly 'repos/example/repo/issues/42/comments'
        }
        It 'does not fetch nonexistent discussions on unrelated reports' {
            $script:issues = @(@{
                number = 42; state = 'open'; html_url = 'https://github.com/example/repo/issues/42'
                body = 'An unrelated failed run'; comments = 0
            })
            $null = Invoke-ScheduledReporting example/repo 10 1 $script:directory
            $script:writes[0].endpoint | Should -BeExactly 'repos/example/repo/issues'
            Should -Invoke Invoke-ScheduledGitHubJson -Times 0 -Exactly -ParameterFilter {
                $Endpoint -match '/issues/42/comments'
            }
        }
        It 'does not confuse a different attempt linked in discussion with the current attempt' {
            $script:issues = @(@{
                number = 42; state = 'open'; html_url = 'https://github.com/example/repo/issues/42'
                body = 'Run details are in the discussion.'; comments = 1
            })
            $script:comments = @(@{ body = 'https://github.com/example/repo/actions/runs/10/attempts/12' })
            $null = Invoke-ScheduledReporting example/repo 10 1 $script:directory
            $script:writes[0].endpoint | Should -BeExactly 'repos/example/repo/issues'
        }
        It 'does not create another report when an existing report discussion cannot be searched' {
            $script:issues = @(@{
                number = 42; state = 'open'; html_url = 'https://github.com/example/repo/issues/42'
                body = 'Run details are in the discussion.'; comments = 1
            })
            Mock Invoke-ScheduledGitHubJson { throw [IO.IOException]::new() } -ParameterFilter {
                $Endpoint -match '/issues/42/comments'
            }
            { Invoke-ScheduledReporting example/repo 10 1 $script:directory } | Should -Throw
            $script:writes.Count | Should -Be 0
        }
        It 'does not repeat an already published report when the reporter is rerun' {
            $null = Invoke-ScheduledReporting example/repo 10 1 $script:directory
            $script:issues = @(@{
                number = 50; state = 'open'; html_url = 'https://github.com/example/repo/issues/50'
                body = $script:writes[0].body.body
            })
            $script:writes.Clear()
            $null = Invoke-ScheduledReporting example/repo 10 1 $script:directory
            $script:writes.Count | Should -Be 0
        }
        It 'reuses an issue created before its create response was lost' {
            Mock Invoke-ScheduledGitHubJson {
                param($Body)
                $script:issues = @(@{
                    number = 50; state = 'open'; comments = 0
                    html_url = 'https://github.com/example/repo/issues/50'; body = $Body.body
                })
                throw [IO.IOException]::new()
            } -ParameterFilter { $Method -ceq 'POST' -and $Endpoint -ceq 'repos/example/repo/issues' }
            { Invoke-ScheduledReporting example/repo 10 1 $script:directory } | Should -Throw
            Invoke-ScheduledReporting example/repo 10 1 $script:directory | Should -Match '/issues/50'
            Should -Invoke Invoke-ScheduledGitHubJson -Times 1 -Exactly -ParameterFilter {
                $Method -ceq 'POST' -and $Endpoint -ceq 'repos/example/repo/issues'
            }
            $script:writes.Count | Should -Be 0
        }
        It 'creates a separate report for a failed source rerun' {
            $script:issues = @(@{
                number = 42; state = 'closed'; html_url = 'https://github.com/example/repo/issues/42'
                body = 'https://github.com/example/repo/actions/runs/10/attempts/1'
            })
            $script:run.run_attempt = 2
            $null = Invoke-ScheduledReporting example/repo 10 2 $script:directory
            $script:writes[0].endpoint | Should -BeExactly 'repos/example/repo/issues'
            $script:writes[0].body.body | Should -Match '/attempts/2'
        }
        It 'does not match the current attempt as a prefix of a different attempt' {
            $script:issues = @(@{
                number = 42; state = 'closed'; html_url = 'https://github.com/example/repo/issues/42'
                body = 'https://github.com/example/repo/actions/runs/10/attempts/12'
            })
            $null = Invoke-ScheduledReporting example/repo 10 1 $script:directory
            $script:writes[0].endpoint | Should -BeExactly 'repos/example/repo/issues'
        }
        It 'leaves earlier issues unchanged after a successful source rerun' {
            $script:run.status = 'completed'
            $script:run.conclusion = 'success'
            $script:jobs[0].conclusion = 'success'
            $null = Invoke-ScheduledReporting example/repo 10 1 $script:directory
            $script:writes.Count | Should -Be 0
            Should -Invoke Invoke-ScheduledGitHubJson -Times 2
        }
        It 'makes unavailable logs and artifact inventory explicit while still reporting failure' {
            Mock Save-ScheduledGitHubFile { throw [IO.IOException]::new() }
            Mock Invoke-ScheduledGitHubJson { throw [IO.IOException]::new() } -ParameterFilter { $Endpoint -match '/artifacts\?' }
            $null = Invoke-ScheduledReporting example/repo 10 1 $script:directory
            $script:writes[0].body.body | Should -Match 'Job logs unavailable'
            $script:writes[0].body.body | Should -Match 'Result artifact inventory is unavailable'
        }
        It 'keeps a useful partial log and links its explicit truncation gap before deleting the download' {
            Mock Save-ScheduledGitHubFile {
                param($Path)
                Set-Content -LiteralPath $Path -Value '##[error]Retained diagnostic before the log limit'
                return $true
            }
            $null = Invoke-ScheduledReporting example/repo 10 1 $script:directory
            $text = $script:writes[0].body.body
            $text | Should -Match 'Retained diagnostic before the log limit'
            $text | Should -Match 'Job log truncated at the \d+ byte limit'
            $text | Should -Match 'Remaining diagnostics are unavailable'
            $text | Should -Match 'full log: https://github.com/example/repo/actions/runs/10/job/20'
            Test-Path -LiteralPath (Join-Path $script:directory 'job-20.log') | Should -BeFalse
        }
        It 'deletes failed downloads and still reports the diagnostic gap with its original link' {
            Mock Save-ScheduledGitHubFile {
                param($Path)
                Set-Content -LiteralPath $Path -Value 'incomplete download'
                throw [IO.IOException]::new()
            }
            $null = Invoke-ScheduledReporting example/repo 10 1 $script:directory
            $text = $script:writes[0].body.body
            $text | Should -Match 'Job logs unavailable'
            $text | Should -Match 'Full log: https://github.com/example/repo/actions/runs/10/job/20'
            Test-Path -LiteralPath (Join-Path $script:directory 'job-20.log') | Should -BeFalse
        }
        It 'reports oversized artifact gaps with the original result link' {
            $script:jobs[0].name = 'miri-linux'
            $script:artifacts = @(@{ id = 31; name = 'scheduled-result-10-1-miri-linux'; expired = $false })
            Mock Read-ScheduledArtifactText {
                throw [IO.InvalidDataException]::new('Download exceeded the byte limit; complete artifact unavailable.')
            }
            $null = Invoke-ScheduledReporting example/repo 10 1 $script:directory
            $text = $script:writes[0].body.body
            $text | Should -Match 'Tested commit: `a{40}`'
            $text | Should -Match 'Check summary unavailable'
            $text | Should -Match 'byte limit'
            $text | Should -Match 'Result artifact: https://github.com/example/repo/actions/runs/10/artifacts/31'
        }
        It 'does not reinterpret failed job-list or issue-list API calls as empty work' {
            Mock Invoke-ScheduledGitHubJson { throw [IO.IOException]::new() } -ParameterFilter { $Endpoint -match '/jobs\?' }
            { Invoke-ScheduledReporting example/repo 10 1 $script:directory } | Should -Throw
            $script:writes.Count | Should -Be 0
        }
        It 'propagates an issue search failure before publishing' {
            Mock Invoke-ScheduledGitHubJson { throw [IO.IOException]::new() } -ParameterFilter { $Endpoint -match '/issues\?' }
            { Invoke-ScheduledReporting example/repo 10 1 $script:directory } | Should -Throw
            $script:writes.Count | Should -Be 0
        }
        It 'propagates issue publication failures' {
            Mock Invoke-ScheduledGitHubJson { throw [IO.IOException]::new() } -ParameterFilter { $Method -ceq 'POST' }
            { Invoke-ScheduledReporting example/repo 10 1 $script:directory } | Should -Throw
        }
        It 'accepts a concurrently created <LabelName> label without changing its metadata' -ForEach @(
            @{ LabelName = 'scheduled-run-failure' }, @{ LabelName = 'Scheduled-Run-Failure' }
        ) {
            $script:labelName = $LabelName
            Mock Invoke-ScheduledGitHubJson { return ,@() } -ParameterFilter {
                $Endpoint -ceq 'repos/example/repo/labels?per_page=100&page=1'
            }
            Mock Invoke-ScheduledGitHubJson { throw [IO.IOException]::new() } -ParameterFilter {
                $Method -ceq 'POST' -and $Endpoint -ceq 'repos/example/repo/labels'
            }
            Mock Invoke-ScheduledGitHubJson {
                return @{ name = $script:labelName; color = '123456'; description = 'Existing description' }
            } -ParameterFilter { $Endpoint -ceq 'repos/example/repo/labels/scheduled-run-failure' }
            $null = Invoke-ScheduledReporting example/repo 10 1 $script:directory
            $script:writes[0].endpoint | Should -BeExactly 'repos/example/repo/issues'
            Should -Invoke Invoke-ScheduledGitHubJson -Times 1 -Exactly -ParameterFilter {
                $Endpoint -ceq 'repos/example/repo/labels/scheduled-run-failure'
            }
            Should -Invoke Invoke-ScheduledGitHubJson -Times 0 -Exactly -ParameterFilter {
                $Method -ceq 'PATCH' -and $Endpoint -match '/labels'
            }
        }
        It 'preserves the original label creation failure when the exact label is still unavailable' {
            $script:creationFailure = [IO.IOException]::new()
            Mock Invoke-ScheduledGitHubJson { return ,@() } -ParameterFilter {
                $Endpoint -ceq 'repos/example/repo/labels?per_page=100&page=1'
            }
            Mock Invoke-ScheduledGitHubJson { throw $script:creationFailure } -ParameterFilter {
                $Method -ceq 'POST' -and $Endpoint -ceq 'repos/example/repo/labels'
            }
            Mock Invoke-ScheduledGitHubJson { throw [InvalidOperationException]::new() } -ParameterFilter {
                $Endpoint -ceq 'repos/example/repo/labels/scheduled-run-failure'
            }
            $failure = { Invoke-ScheduledReporting example/repo 10 1 $script:directory } | Should -Throw -PassThru
            $failure.Exception | Should -Be $script:creationFailure
            $script:writes.Count | Should -Be 0
        }
        It 'propagates continuation publication failures' {
            Mock Read-ScheduledArtifactText {
                return ((1..2000 | ForEach-Object { "MISSED mutant $_ in example::operation - replace the return expression with another value" }) -join "`n")
            }
            $script:jobs[0].name = 'miri-linux'
            $script:artifacts = @(@{ id = 31; name = 'scheduled-result-10-1-miri-linux'; expired = $false })
            Mock Invoke-ScheduledGitHubJson { throw [IO.IOException]::new() } -ParameterFilter {
                $Method -ceq 'POST' -and $Endpoint.EndsWith('/comments')
            }
            { Invoke-ScheduledReporting example/repo 10 1 $script:directory } | Should -Throw
            $script:writes[0].endpoint | Should -BeExactly 'repos/example/repo/issues'
        }
        It 'does not infer another failure from a summary with a different job name' {
            $script:jobs[0].name = 'miri-linux-other'
            $script:artifacts = @(@{ id = 31; name = 'scheduled-result-10-1-miri-linux'; expired = $false })
            $null = Invoke-ScheduledReporting example/repo 10 1 $script:directory
            $script:writes[0].body.body | Should -Match 'No check-summary artifact'
            $script:writes[0].body.body | Should -Not -Match 'MISSED mutant'
            Should -Invoke Read-ScheduledArtifactText -Times 0
        }
        It 'makes malformed or expired diagnostic summaries explicit without losing the failed job' {
            $script:jobs[0].name = 'miri-linux'
            $script:artifacts = @(@{ id = 31; name = 'scheduled-result-10-1-miri-linux'; expired = $true })
            Mock Read-ScheduledArtifactText { throw [IO.InvalidDataException]::new() }
            $null = Invoke-ScheduledReporting example/repo 10 1 $script:directory
            $script:writes[0].body.body | Should -Match 'Check summary unavailable'
            $script:writes[0].body.body | Should -Match 'Rust toolchain download failed'
        }
        It 'reads all pages for jobs, artifacts and ordinary closed issues' {
            Mock Invoke-ScheduledGitHubJson {
                param($Endpoint)
                $items = if ($Endpoint -match 'page=1$') { @(1..100 | ForEach-Object { @{ number = $_ } }) }
                    else { @(@{ number = 101 }) }
                if ($Endpoint -match '/jobs\?') { return @{ jobs = $items } }
                if ($Endpoint -match '/artifacts\?') { return @{ artifacts = $items } }
                return ,$items
            }
            foreach ($property in @('jobs', 'artifacts')) {
                $items = @(Get-ScheduledGitHubCollection "repos/example/repo/$property" -Property $property)
                $items.Count | Should -Be 101
                $items[-1].number | Should -Be 101
            }
            $items = @(Get-ScheduledGitHubCollection 'repos/example/repo/issues?state=all')
            $items.Count | Should -Be 101
            $items[-1].number | Should -Be 101
        }
        It 'waits for all history pages before selecting the effective job result' {
            $script:run.run_attempt = 2
            $script:firstPage = @($script:jobs[0]) + @(1..99 | ForEach-Object {
                $job = $script:jobs[1].Clone()
                $job.id = 100 + $_
                $job.name = "success-$_"
                $job
            })
            $script:lastJob = $script:jobs[0].Clone()
            $script:lastJob.id = 40
            $script:lastJob.run_attempt = 2
            $script:lastJob.conclusion = 'success'
            Mock Invoke-ScheduledGitHubJson { return @{ jobs = $script:firstPage } } -ParameterFilter {
                $Endpoint -ceq 'repos/example/repo/actions/runs/10/jobs?filter=all&per_page=100&page=1'
            }
            Mock Invoke-ScheduledGitHubJson { return @{ jobs = @($script:lastJob) } } -ParameterFilter {
                $Endpoint -ceq 'repos/example/repo/actions/runs/10/jobs?filter=all&per_page=100&page=2'
            }
            $null = Invoke-ScheduledReporting example/repo 10 2 $script:directory
            $script:writes.Count | Should -Be 0
            Should -Invoke Invoke-ScheduledGitHubJson -Times 1 -Exactly -ParameterFilter {
                $Endpoint -ceq 'repos/example/repo/actions/runs/10/jobs?filter=all&per_page=100&page=2'
            }
        }
        It 'propagates an API failure on a later page rather than publishing partial inventory' {
            Mock Invoke-ScheduledGitHubJson {
                param($Endpoint)
                if ($Endpoint -match 'page=1$') { return @{ jobs = @(1..100 | ForEach-Object { @{ id = $_ } }) } }
                throw [IO.IOException]::new()
            }
            { Get-ScheduledGitHubCollection 'repos/example/repo/jobs' -Property jobs } | Should -Throw
        }
        It 'reconciles exact-attempt duplicates with a normal linked comment and closure' {
            $script:issues = @(51, 50 | ForEach-Object { @{
                number = $_; state = 'open'; html_url = "https://github.com/example/repo/issues/$_"
                body = 'https://github.com/example/repo/actions/runs/10/attempts/1'
            } })
            $null = Invoke-ScheduledReporting example/repo 10 1 $script:directory
            @($script:writes | Where-Object method -EQ PATCH).Count | Should -Be 1
            ($script:writes | Where-Object method -EQ PATCH).endpoint | Should -BeExactly 'repos/example/repo/issues/51'
            ($script:writes | Where-Object endpoint -EQ 'repos/example/repo/issues/51/comments').body.body | Should -Match '/issues/50'
        }
        It 'completes missing continuation comments after interrupted publication without repeating saved text' {
            Mock Read-ScheduledArtifactText {
                return ((1..2000 | ForEach-Object { "MISSED mutant $_ in example::operation - replace the return expression with another value" }) -join "`n")
            }
            $script:jobs[0].name = 'miri-linux'
            $script:artifacts = @(@{ id = 31; name = 'scheduled-result-10-1-miri-linux'; expired = $false })
            $null = Invoke-ScheduledReporting example/repo 10 1 $script:directory
            $allMessages = @($script:writes | ForEach-Object { $_.body.body })
            $allMessages.Count | Should -BeGreaterThan 2
            $script:issues = @(@{
                number = 50; state = 'open'; html_url = 'https://github.com/example/repo/issues/50'; body = $allMessages[0]
            })
            $script:comments = @(@{ body = $allMessages[1] })
            $script:writes.Clear()
            $null = Invoke-ScheduledReporting example/repo 10 1 $script:directory
            $script:writes.Count | Should -Be ($allMessages.Count - 2)
            ($script:writes | ForEach-Object { $_.body.body }) | Should -Not -Contain $allMessages[1]
            ($script:writes[-1].body.body) | Should -Match 'MISSED mutant 2000'
        }
    }
}

Describe 'Artifact summary reading' {
    InModuleScope ScheduledGitHub {
        BeforeAll {
            $script:directory = Join-Path (Get-Location).Path "target\scheduled-archive-tests-$([guid]::NewGuid().ToString('N'))"
            $null = New-Item -ItemType Directory -Path $script:directory -Force
        }
        AfterAll { Remove-Item -LiteralPath $script:directory -Recurse -Force }
        It 'reads only the root text entry without extracting executable or traversal paths' {
            Mock Save-ScheduledGitHubFile {
                param($Path)
                $archive = [IO.Compression.ZipFile]::Open($Path, [IO.Compression.ZipArchiveMode]::Create)
                try {
                    foreach ($name in @('summary.md', '../outside.ps1', 'nested/summary.md')) {
                        $writer = [IO.StreamWriter]::new($archive.CreateEntry($name).Open())
                        try { $writer.Write("Readable $name") } finally { $writer.Dispose() }
                    }
                } finally { $archive.Dispose() }
            }
            $text = Read-ScheduledArtifactText example/repo @{ id = 1; expired = $false } $script:directory
            $text | Should -BeExactly 'Readable summary.md'
            @(Get-ChildItem -LiteralPath $script:directory -Recurse -File).Count | Should -Be 0
        }
        It 'reports expired archives without attempting a download' {
            Mock Save-ScheduledGitHubFile { throw 'Unexpected download' }
            { Read-ScheduledArtifactText example/repo @{ id = 2; expired = $true } $script:directory } | Should -Throw
            Should -Invoke Save-ScheduledGitHubFile -Times 0
        }
    }

    Describe 'GitHub command boundary' {
        InModuleScope ScheduledGitHub {
            BeforeAll {
                $script:realRetryCommand = Get-Command Invoke-WithRetry
            }
            BeforeEach {
                $script:savedExitCode = Get-Variable LASTEXITCODE -Scope Global -ValueOnly -ErrorAction SilentlyContinue
                $script:requestCount = 0
                # Other script suites load their own nested Retry modules. Mock our direct
                # dependency, not whichever module named Retry Pester happens to find.
                Mock Invoke-WithRetry {
                    param($Action, $Attempt, $DelaySeconds, $BackoffMultiplier, $MaxDelaySeconds, $RetryOn)
                    $DelaySeconds | Should -Be 3
                    & $script:realRetryCommand -Action $Action -Attempt $Attempt -DelaySeconds 0 `
                        -BackoffMultiplier $BackoffMultiplier -MaxDelaySeconds $MaxDelaySeconds -RetryOn $RetryOn
                }
            }
            AfterEach { $global:LASTEXITCODE = $script:savedExitCode }
            It 'preserves empty and singleton API arrays for collection pagination' {
                Mock gh { $global:LASTEXITCODE = 0; '[]' }
                @(Get-ScheduledGitHubCollection 'repos/example/repo/issues').Count | Should -Be 0
                Mock gh { $global:LASTEXITCODE = 0; '[{"number":42}]' }
                $items = @(Get-ScheduledGitHubCollection 'repos/example/repo/issues')
                $items.Count | Should -Be 1
                $items[0].number | Should -Be 42
            }
            It 'rejects malformed successful API output rather than assuming an empty collection' {
                Mock gh { $global:LASTEXITCODE = 0; 'HTTP 503 is not JSON' }
                { Invoke-ScheduledGitHubJson 'repos/example/repo/issues' } | Should -Throw
                Should -Invoke gh -Times 1 -Exactly
            }
            It 'propagates a failed GitHub command even if it emitted valid JSON' {
                Mock gh { $global:LASTEXITCODE = 1; '[]' }
                { Invoke-ScheduledGitHubJson 'repos/example/repo/issues' } | Should -Throw
            }
            It 'retries an idempotent GET after <FailureText>' -ForEach @(
                @{ FailureText = 'gh: Service unavailable (HTTP 503)' }
                @{ FailureText = 'gh: API rate limit exceeded (HTTP 403)' }
                @{ FailureText = 'connection reset by peer' }
            ) {
                $script:failureText = $FailureText
                Mock gh {
                    $script:requestCount++
                    if ($script:requestCount -eq 1) {
                        $global:LASTEXITCODE = 1
                        Write-Error $script:failureText -ErrorAction Continue
                    } else { $global:LASTEXITCODE = 0; '{"ok":true}' }
                }
                (Invoke-ScheduledGitHubJson 'repos/example/repo/issues').ok | Should -BeTrue
                Should -Invoke gh -Times 2 -Exactly
                Should -Invoke Invoke-WithRetry -Times 1 -Exactly
            }
            It 'does not retry <FailureText> and preserves the original diagnostic' -ForEach @(
                @{ FailureText = 'gh: Requires authentication (HTTP 401)' }
                @{ FailureText = 'gh: Forbidden (HTTP 403)' }
                @{ FailureText = 'gh: Not Found (HTTP 404)' }
            ) {
                $script:failureText = $FailureText
                Mock gh {
                    $global:LASTEXITCODE = 1
                    Write-Error $script:failureText -ErrorAction Continue
                }
                $failure = { Invoke-ScheduledGitHubJson 'repos/example/repo/issues' } | Should -Throw -PassThru
                $failure.Exception.Message | Should -Match ([regex]::Escape($FailureText))
                Should -Invoke gh -Times 1 -Exactly
            }
            It 'keeps <Method> single-shot even when the failure looks transient' -ForEach @(
                @{ Method = 'POST' }, @{ Method = 'PATCH' }
            ) {
                Mock gh {
                    $global:LASTEXITCODE = 1
                    Write-Error 'gh: Service unavailable (HTTP 503)' -ErrorAction Continue
                }
                { Invoke-ScheduledGitHubJson 'repos/example/repo/issues' -Method $Method -Body @{ body = 'test' } } |
                    Should -Throw
                Should -Invoke gh -Times 1 -Exactly
                Should -Invoke Invoke-WithRetry -Times 0 -Exactly
            }
            It 'uses the existing bounded backoff and rethrows the final read failure' {
                Mock gh { $global:LASTEXITCODE = 1; 'gh: Service unavailable (HTTP 503)' }
                $failure = { Invoke-ScheduledGitHubJson 'repos/example/repo/issues' } | Should -Throw -PassThru
                $failure.Exception.Message | Should -Match 'HTTP 503'
                Should -Invoke gh -Times 4 -Exactly
                Should -Invoke Invoke-WithRetry -Times 1 -Exactly -ParameterFilter {
                    $Attempt -eq 4 -and $DelaySeconds -eq 3 -and $BackoffMultiplier -eq 2 -and $MaxDelaySeconds -eq 30
                }
            }
            It 'does not mix successful stderr notes into JSON or retry them' {
                Mock gh {
                    Write-Error 'gh: rate limit information' -ErrorAction Continue
                    $global:LASTEXITCODE = 0
                    '{"ok":true}'
                }
                (Invoke-ScheduledGitHubJson 'repos/example/repo/issues').ok | Should -BeTrue
                Should -Invoke gh -Times 1 -Exactly
            }
        }

        Describe 'Report job entry point' {
            BeforeAll {
                $script:entryPoint = Join-Path $PSScriptRoot 'Invoke-ScheduledReport.ps1'
            }
            BeforeEach {
                $script:environment = @{
                    GH_REPO = $env:GH_REPO
                    GITHUB_RUN_ID = $env:GITHUB_RUN_ID
                    GITHUB_RUN_ATTEMPT = $env:GITHUB_RUN_ATTEMPT
                }
                $env:GH_REPO = 'example/repo'
                $env:GITHUB_RUN_ID = '10'
                $env:GITHUB_RUN_ATTEMPT = '2'
                Mock Import-Module {}
                Mock Invoke-ScheduledReporting { param($RunAttempt) $RunAttempt }
            }
            AfterEach {
                foreach ($key in $script:environment.Keys) {
                    [Environment]::SetEnvironmentVariable($key, $script:environment[$key])
                }
            }
            It 'uses the current report job environment without an event payload' {
                & $script:entryPoint | Should -Be 2
                Should -Invoke Invoke-ScheduledReporting -Times 1 -Exactly -ParameterFilter {
                    $Repository -ceq 'example/repo' -and $RunId -eq 10 -and $RunAttempt -eq 2
                }
            }
            It 'accepts explicit run and attempt arguments' {
                & $script:entryPoint -Repository other/repo -RunId 20 -RunAttempt 3 | Should -Be 3
                Should -Invoke Invoke-ScheduledReporting -Times 1 -Exactly -ParameterFilter {
                    $Repository -ceq 'other/repo' -and $RunId -eq 20 -and $RunAttempt -eq 3
                }
            }
        }
    }
}
