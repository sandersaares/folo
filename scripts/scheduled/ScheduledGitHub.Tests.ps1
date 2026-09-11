#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
# Exercises the completion reporter with fake GitHub responses and repository-local files.
# No test writes to GitHub or requires Rust; setup-only failures and interrupted publication
# must remain useful and retryable. Archive tests preserve the controller/data boundary.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
$VerbosePreference = 'Continue'

Import-Module (Join-Path $PSScriptRoot 'ScheduledGitHub.psm1') -Force

Describe 'Completed attempt reporting' {
    InModuleScope ScheduledGitHub {
        BeforeAll {
            $script:directory = Join-Path (Get-Location).Path "target\scheduled-reporter-tests-$([guid]::NewGuid().ToString('N'))"
            $null = New-Item -ItemType Directory -Path $script:directory -Force
        }
        AfterAll { Remove-Item -LiteralPath $script:directory -Recurse -Force }
        BeforeEach {
            $script:run = @{
                id = 10; run_attempt = 1; status = 'completed'; conclusion = 'failure'
                path = '.github/workflows/full-deep-validation.yml'; name = 'Full deep validation'
                head_sha = 'a' * 40; run_started_at = '2026-09-11T00:01:00Z'
            }
            $script:jobs = @(@{
                id = 20; name = 'plan'; html_url = 'https://github.com/example/repo/actions/runs/10/job/20'
                conclusion = 'failure'; steps = @(@{ name = 'Install Rust'; conclusion = 'failure' })
            }, @{
                id = 21; name = 'independent-success'; html_url = 'https://github.com/example/repo/actions/runs/10/job/21'
                conclusion = 'success'; steps = @()
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
                    '^repos/example/repo/actions/runs/10/attempts/[12]$' { return $script:run }
                    '/attempts/[12]/jobs\?per_page=100&page=1$' { return @{ jobs = $script:jobs } }
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
                param($Artifact, $Name)
                $script:artifactReads.Add([long]$Artifact.id)
                if ($Name -ceq 'plan.json') {
                    return (@{
                        source_sha = 'b' * 40; controller_sha = 'a' * 40
                        checks = @(@{ id = 'miri-linux'; platform = 'ubuntu-latest' })
                    } | ConvertTo-Json -Depth 10)
                }
                return "# Check miri-linux`nTested source: $('b' * 40)`nPackages: events`nMISSED mutant: return false`nseed: 29`nReplay: just package=events miri"
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
            $issue.body | Should -Match 'do not establish the selected tested source'
            $issue.body | Should -Not -Match 'independent-success'
            Should -Invoke Read-ScheduledArtifactText -Times 0
        }
        It 'reports cancellation and checks that never ran' {
            $script:run.conclusion = 'cancelled'
            $script:jobs[0].conclusion = 'cancelled'
            $script:jobs[0].steps[0].conclusion = 'cancelled'
            $script:jobs[1].name = 'miri-linux'
            $script:jobs[1].conclusion = 'skipped'
            $null = Invoke-ScheduledReporting example/repo 10 1 $script:directory
            $script:writes[0].body.body | Should -Match 'cancelled'
            $script:writes[0].body.body | Should -Match 'Did not run'
        }
        It 'uses the selected tested source and only exact-attempt artifacts' {
            $script:run.path = '.github/workflows/selected-deep-validation.yml'
            $script:jobs[0].name = 'checks / miri-linux'
            $script:artifacts = @(
                @{ id = 30; name = 'scheduled-plan-10-1'; expired = $false }
                @{ id = 31; name = 'scheduled-result-10-1-miri-linux'; expired = $false }
                @{ id = 32; name = 'scheduled-result-10-2-miri-linux'; expired = $false }
                @{ id = 33; name = 'scheduled-result-10-1-independent-success'; expired = $false }
            )
            $null = Invoke-ScheduledReporting example/repo 10 1 $script:directory
            $text = $script:writes[0].body.body
            $text | Should -Match 'Tested commit: `b{40}`'
            $text | Should -Match 'ubuntu-latest / miri-linux'
            $text | Should -Match 'MISSED mutant: return false'
            $text | Should -Match 'seed: 29'
            $text | Should -Match 'Replay: just package=events miri'
            $script:artifactReads | Should -Be @(30, 31)
        }
        It 'retains selected source metadata when failed-job reruns reuse a prior successful plan' {
            $script:run.path = '.github/workflows/selected-deep-validation.yml'
            $script:run.run_attempt = 2
            $script:jobs[0].name = 'checks / miri-linux'
            $script:artifacts = @(
                @{ id = 30; name = 'scheduled-plan-10-1'; expired = $false }
                @{ id = 31; name = 'scheduled-result-10-1-miri-linux'; expired = $false }
                @{ id = 32; name = 'scheduled-result-10-3-miri-linux'; expired = $false }
            )
            $null = Invoke-ScheduledReporting example/repo 10 2 $script:directory
            $text = $script:writes[0].body.body
            $text | Should -Match 'Tested commit: `b{40}`'
            $text | Should -Match 'planning artifact from attempt 1'
            $text | Should -Match '/runs/10/artifacts/30'
            $text | Should -Match 'checker results and logs below belong only to attempt 2'
            $text | Should -Match 'Rust toolchain download failed'
            $text | Should -Match 'No check-summary artifact'
            $text | Should -Not -Match 'MISSED mutant|/attempts/1'
            $script:artifactReads | Should -Be @(30)
        }
        It 'ignores future and different-run plans when reporting an older attempt' {
            $script:run.path = '.github/workflows/selected-deep-validation.yml'
            $script:artifacts = @(
                @{ id = 30; name = 'scheduled-plan-10-2'; expired = $false }
                @{ id = 31; name = 'scheduled-plan-11-1'; expired = $false }
            )
            $null = Invoke-ScheduledReporting example/repo 10 1 $script:directory
            $text = $script:writes[0].body.body
            $text | Should -Match 'Tested commit: Unavailable'
            $text | Should -Not -Match 'b{40}'
            $script:artifactReads.Count | Should -Be 0
        }
        It 'prefers the current plan over prior plans and ignores later plans' {
            $script:run.run_attempt = 2
            $script:jobs[0].name = 'checks / miri-linux'
            $script:artifacts = @(
                @{ id = 30; name = 'scheduled-plan-10-1'; expired = $false }
                @{ id = 31; name = 'scheduled-plan-10-3'; expired = $false }
                @{ id = 32; name = 'scheduled-plan-10-2'; expired = $false }
                @{ id = 33; name = 'scheduled-result-10-2-miri-linux'; expired = $false }
            )
            $null = Invoke-ScheduledReporting example/repo 10 2 $script:directory
            $text = $script:writes[0].body.body
            $text | Should -Match 'Tested commit: `b{40}`'
            $text | Should -Match 'MISSED mutant: return false'
            $text | Should -Not -Match 'retained'
            $script:artifactReads | Should -Be @(32, 33)
        }
        It 'rejects retained plans whose controller does not match the originating workflow run' {
            $script:run.run_attempt = 2
            $script:artifacts = @(@{ id = 30; name = 'scheduled-plan-10-1'; expired = $false })
            Mock Read-ScheduledArtifactText {
                return (@{ source_sha = 'b' * 40; controller_sha = 'c' * 40; checks = @() } |
                    ConvertTo-Json -Depth 10)
            }
            $null = Invoke-ScheduledReporting example/repo 10 2 $script:directory
            $text = $script:writes[0].body.body
            $text | Should -Match 'Tested commit: Unavailable'
            $text | Should -Match 'Tested-source plan is unavailable'
            $text | Should -Not -Match 'b{40}'
        }
        It 'does not identify the controller commit as a selected candidate when planning failed' {
            $script:run.path = '.github/workflows/selected-deep-validation.yml'
            $null = Invoke-ScheduledReporting example/repo 10 1 $script:directory
            $text = $script:writes[0].body.body
            $text | Should -Match 'Tested commit: Unavailable'
            $text | Should -Match 'selected tested source'
            $text | Should -Match '\[run inputs and logs\]\(https://github.com/example/repo/actions/runs/10/attempts/1\)'
            $text | Should -Match 'Workflow/controller commit \(not the tested-source identity\): `a{40}`'
        }
        It 'requests the originating attempt rather than the latest source rerun' {
            $null = Invoke-ScheduledReporting example/repo 10 1 $script:directory
            Should -Invoke Invoke-ScheduledGitHubJson -Times 1 -Exactly -ParameterFilter {
                $Endpoint -ceq 'repos/example/repo/actions/runs/10/attempts/1'
            }
            Should -Invoke Invoke-ScheduledGitHubJson -Times 1 -Exactly -ParameterFilter {
                $Endpoint -ceq 'repos/example/repo/actions/runs/10/attempts/1/jobs?per_page=100&page=1'
            }
            Should -Invoke Invoke-ScheduledGitHubJson -Times 0 -Exactly -ParameterFilter {
                $Endpoint -ceq 'repos/example/repo/actions/runs/10'
            }
        }
        It 'reuses a human report without overwriting it or requiring an author/schema' {
            $script:issues = @(@{
                number = 42; state = 'closed'; html_url = 'https://github.com/example/repo/issues/42'
                body = 'Investigate this attempt: https://github.com/example/repo/actions/runs/10/attempts/1'
            })
            $null = Invoke-ScheduledReporting example/repo 10 1 $script:directory
            $script:writes.Count | Should -Be 1
            $script:writes[0].endpoint | Should -BeExactly 'repos/example/repo/issues/42/comments'
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
            $script:run.conclusion = 'success'
            $null = Invoke-ScheduledReporting example/repo 10 1 $script:directory
            $script:writes.Count | Should -Be 0
            Should -Invoke Invoke-ScheduledGitHubJson -Times 1
        }
        It 'makes unavailable logs and artifact inventory explicit while still reporting failure' {
            Mock Save-ScheduledGitHubFile { throw [IO.IOException]::new() }
            Mock Invoke-ScheduledGitHubJson { throw [IO.IOException]::new() } -ParameterFilter { $Endpoint -match '/artifacts\?' }
            $null = Invoke-ScheduledReporting example/repo 10 1 $script:directory
            $script:writes[0].body.body | Should -Match 'Job logs unavailable'
            $script:writes[0].body.body | Should -Match 'Result artifact inventory is unavailable'
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
        It 'retains check summaries whose job association is unavailable' {
            $script:jobs[0].name = 'miri-linux-other'
            $script:artifacts = @(@{ id = 31; name = 'scheduled-result-10-1-miri-linux'; expired = $false })
            $null = Invoke-ScheduledReporting example/repo 10 1 $script:directory
            $script:writes[0].body.body | Should -Match 'could not be associated'
            $script:writes[0].body.body | Should -Match 'MISSED mutant: return false'
        }
        It 'makes malformed or expired diagnostic summaries explicit without losing the failed job' {
            $script:jobs[0].name = 'miri-linux'
            $script:artifacts = @(
                @{ id = 30; name = 'scheduled-plan-10-1'; expired = $false }
                @{ id = 31; name = 'scheduled-result-10-1-miri-linux'; expired = $true }
            )
            Mock Read-ScheduledArtifactText { throw [IO.InvalidDataException]::new() }
            $null = Invoke-ScheduledReporting example/repo 10 1 $script:directory
            $script:writes[0].body.body | Should -Match 'Tested-source plan is unavailable'
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

Describe 'Artifact controller isolation' {
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
                    foreach ($name in @('summary.md', '../controller/evil.ps1', 'nested/summary.md')) {
                        $writer = [IO.StreamWriter]::new($archive.CreateEntry($name).Open())
                        try { $writer.Write("Readable $name") } finally { $writer.Dispose() }
                    }
                } finally { $archive.Dispose() }
            }
            $text = Read-ScheduledArtifactText example/repo @{ id = 1; expired = $false } $script:directory summary.md
            $text | Should -BeExactly 'Readable summary.md'
            @(Get-ChildItem -LiteralPath $script:directory -Recurse -File).Count | Should -Be 1
        }
        It 'reports expired archives without attempting a download' {
            Mock Save-ScheduledGitHubFile { throw 'Unexpected download' }
            { Read-ScheduledArtifactText example/repo @{ id = 2; expired = $true } $script:directory summary.md } | Should -Throw
            Should -Invoke Save-ScheduledGitHubFile -Times 0
        }
    }

    Describe 'GitHub command boundary' {
        InModuleScope ScheduledGitHub {
            BeforeEach {
                $script:savedExitCode = Get-Variable LASTEXITCODE -Scope Global -ValueOnly -ErrorAction SilentlyContinue
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
                Mock gh { $global:LASTEXITCODE = 0; 'not JSON' }
                { Invoke-ScheduledGitHubJson 'repos/example/repo/issues' } | Should -Throw
            }
            It 'propagates a failed GitHub command even if it emitted valid JSON' {
                Mock gh { $global:LASTEXITCODE = 1; '[]' }
                { Invoke-ScheduledGitHubJson 'repos/example/repo/issues' } | Should -Throw
            }
        }

        Describe 'Completion event entry point' {
            BeforeAll {
                $script:eventDirectory = Join-Path (Get-Location).Path "target\scheduled-event-tests-$([guid]::NewGuid().ToString('N'))"
                $null = New-Item -ItemType Directory -Path $script:eventDirectory -Force
                $script:eventPath = Join-Path $script:eventDirectory 'event.json'
                $script:entryPoint = Join-Path $PSScriptRoot 'Invoke-ScheduledReport.ps1'
            }
            AfterAll { Remove-Item -LiteralPath $script:eventDirectory -Recurse -Force }
            It 'uses the originating event attempt when retrying the reporter' {
                @{ workflow_run = @{ id = 10; run_attempt = 1 } } |
                    ConvertTo-Json | Set-Content -LiteralPath $script:eventPath
                Mock Import-Module {}
                Mock Invoke-ScheduledReporting { param($RunAttempt) $RunAttempt }
                & $script:entryPoint -Repository example/repo -EventPath $script:eventPath `
                    -OutputDirectory $script:eventDirectory | Should -Be 1
                Should -Invoke Invoke-ScheduledReporting -Times 1 -Exactly -ParameterFilter {
                    $Repository -ceq 'example/repo' -and $RunId -eq 10 -and $RunAttempt -eq 1
                }
            }
        }
    }
}
