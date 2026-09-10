#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
# Drives the real Rust record utility through the hosted publication adapter against an
# in-memory GitHub transport. Protects one-issue identity, complete-page commit ordering,
# lost-response recovery, human-text preservation and fail-closed history reconstruction.
BeforeDiscovery { Import-Module (Join-Path $PSScriptRoot 'ScheduledRunGitHub.psm1') }
BeforeAll { Import-Module (Join-Path $PSScriptRoot 'ScheduledRunGitHub.psm1') }

Describe 'Durable run publication' {
    InModuleScope ScheduledRunGitHub {
        BeforeAll {
            function Copy-TestRunValue($Value) {
                return $Value | ConvertTo-Json -Depth 100 | ConvertFrom-Json -AsHashtable
            }
            function Invoke-TestRunApi {
                param($Endpoint, $Method = 'GET', $Body, [switch] $Paginate)
                if ($Method -in @('POST', 'PATCH')) {
                    $script:writes.Add(@{ endpoint = $Endpoint; method = $Method; body = (Copy-TestRunValue $Body) })
                }
                if ($Endpoint -eq 'repos/owner/repo/labels?per_page=100' -and $Method -eq 'GET') {
                    if (-not $Paginate) { throw 'Label inventory must be paginated.' }
                    return @($script:labels.Values)
                }
                if ($Endpoint -eq 'repos/owner/repo/labels' -and $Method -eq 'POST') {
                    if ($script:failLabelWrite) { throw [IO.IOException]::new('Label publication failed.') }
                    $script:labels[$Body.name] = Copy-TestRunValue $Body
                    return $script:labels[$Body.name]
                }
                if ($Endpoint -eq 'repos/owner/repo/labels/scheduled-run-failure' -and $Method -eq 'GET') {
                    return $script:labels['scheduled-run-failure']
                }
                if ($Endpoint -eq 'repos/owner/repo/issues' -and $Method -eq 'POST') {
                    foreach ($label in $Body.labels) {
                        if (-not $script:labels.ContainsKey($label)) { throw 'Required reporting label is missing.' }
                    }
                    $script:nextIssue++
                    $number = $script:nextIssue
                    $script:issues[$number] = @{
                        number = $number; title = $Body.title; body = $Body.body; state = 'open'
                        labels = @($Body.labels | ForEach-Object { @{ name = $_ } }); user = @{ login = 'github-actions[bot]' }
                    }
                    $script:comments[$number] = [Collections.Generic.List[object]]::new()
                    if ($script:hideIssueReply) {
                        $script:hideIssueReply = $false
                        $script:hiddenIssue = $script:issues[$number]
                        $script:issues.Remove($number)
                        throw [IO.IOException]::new('Unknown issue response.')
                    }
                    if ($script:loseIssueReply) {
                        $script:loseIssueReply = $false
                        throw [IO.IOException]::new('Lost issue response.')
                    }
                    return Copy-TestRunValue $script:issues[$number]
                }
                if ($Endpoint.StartsWith('repos/owner/repo/issues?') -and $Method -eq 'GET') {
                    if (-not $Paginate) { throw 'Issue inventory must be paginated.' }
                    return @(foreach ($issue in $script:issues.Values) { Copy-TestRunValue $issue })
                }
                if ($Endpoint -match '^repos/owner/repo/issues/(\d+)/comments(?:\?per_page=100)?$') {
                    $number = [int]$Matches[1]
                    if ($Method -eq 'GET') {
                        if (-not $Paginate) { throw 'Comment inventory must be paginated.' }
                        return @(foreach ($comment in $script:comments[$number]) { Copy-TestRunValue $comment })
                    }
                    if ($Method -eq 'POST') {
                        $script:nextComment++
                        $comment = @{ id = $script:nextComment; body = $Body.body; user = @{ login = 'github-actions[bot]' } }
                        if ($script:hidePageReply) {
                            $script:hidePageReply = $false
                            $script:hiddenPage = $comment
                            throw [IO.IOException]::new('Unknown page response.')
                        }
                        $script:comments[$number].Add($comment)
                        if ($script:losePageReply) {
                            $script:losePageReply = $false
                            throw [IO.IOException]::new('Lost page response.')
                        }
                        return Copy-TestRunValue $comment
                    }
                }
                if ($Endpoint -match '^repos/owner/repo/issues/(\d+)$') {
                    $number = [int]$Matches[1]
                    if ($Method -eq 'PATCH') {
                        if ($script:failIndexWrite) {
                            $script:failIndexWrite = $false
                            throw [IO.IOException]::new('Index write interrupted.')
                        }
                        foreach ($key in $Body.Keys) {
                            if ($key -eq 'labels') {
                                $script:issues[$number].labels = @($Body.labels | ForEach-Object { @{ name = $_ } })
                            } else { $script:issues[$number][$key] = $Body[$key] }
                        }
                        if ($script:loseIndexReply) {
                            $script:loseIndexReply = $false
                            throw [IO.IOException]::new('Lost index response.')
                        }
                    }
                    return Copy-TestRunValue $script:issues[$number]
                }
                throw "Unexpected test API request: $Method $Endpoint"
            }
            function Invoke-TestRunPublication {
                param([switch] $DryRun)
                Sync-ScheduledRunIntake -Policy $script:policy -Run $script:run -Plan @{ decision = @{ run = $true } } `
                    -Manifest @{} -Results $script:results -Jobs $script:jobs -EvidenceGaps @() `
                    -OutputDirectory $script:output -Api ${function:Invoke-TestRunApi} -Apply:(-not $DryRun)
            }
        }
        BeforeEach {
            $script:policy = @{
                repository = 'owner/repo'; repository_id = 123; reporter_login = 'github-actions[bot]'
                rollout = @{ reporting_enabled = $true }
            }
            $script:run = @{
                id = 789; workflow_id = 456; name = 'Full deep validation'; path = '.github/workflows/full-deep-validation.yml'
                run_attempt = 1; run_number = 42; head_sha = 'a' * 40; conclusion = 'failure'
                created_at = '2026-09-09T01:00:00Z'; run_started_at = '2026-09-09T01:00:01Z'; updated_at = '2026-09-09T01:00:02Z'
            }
            $script:results = @(@{ outcome = 'findings'; check_id = 'miri'; summary = 'Invalid access.' })
            $script:jobs = @(@{
                id = 101; run_id = 789; name = 'Miri'; status = 'completed'; conclusion = 'failure'
                steps = @(@{ number = 1; name = 'Miri'; status = 'completed'; conclusion = 'failure' })
                log = @{ url = 'https://api.github.com/repos/owner/repo/actions/jobs/101/logs'
                    excerpt = 'Invalid access.'; bytes = 15; truncated = $false; excerpt_truncated = $false }
            })
            $script:issues = @{}
            $script:labels = @{}
            $script:comments = @{}
            $script:writes = [Collections.Generic.List[object]]::new()
            $script:nextIssue = 40; $script:nextComment = 100
            $script:loseIssueReply = $false; $script:losePageReply = $false
            $script:failLabelWrite = $false
            $script:hidePageReply = $false; $script:hiddenPage = $null; $script:loseIndexReply = $false
            $script:hideIssueReply = $false; $script:hiddenIssue = $null; $script:failIndexWrite = $false
            $script:output = Join-Path $TestDrive ([guid]::NewGuid().ToString('N'))
            $null = New-Item -ItemType Directory -Path $output
        }
        It 'creates one issue and commits evidence only after its pages exist' {
            $result = Invoke-TestRunPublication
            $result.requires_triage | Should -BeTrue
            $issues.Count | Should -Be 1
            $issues[41].title | Should -BeExactly 'Deep validation failed'
            $issues[41].labels.name | Should -Be @('scheduled-run-failure')
            $labels.Keys | Should -Be @('scheduled-run-failure')
            $writes[0].endpoint | Should -BeExactly 'repos/owner/repo/labels'
            $writes[1].body.body | Should -Match 'publication is pending'
            $writes[2].endpoint | Should -BeExactly 'repos/owner/repo/issues/41/comments'
            $writes[-1].method | Should -BeExactly 'PATCH'
            $result.record.revisions.Count | Should -Be 1
            $issues[41].body | Should -Match 'Complete evidence revisions: 1'
            $result.record.revisions[0].evidence.attempt.jobs[0].steps[0].name | Should -BeExactly 'Miri'
        }
        It 'is idempotent and preserves human body text labels and comments' {
            $null = Invoke-TestRunPublication
            $issues[41].body += "`nHuman notes."
            $issues[41].labels += @{ name = 'operator-label' }
            $comments[41].Add(@{ id = 900; user = @{ login = 'human' }; body = 'Keep this discussion.' })
            $writes.Clear()
            $null = Invoke-TestRunPublication
            $writes.Count | Should -Be 0
            $issues[41].body | Should -Match 'Human notes'
            $issues[41].labels.name | Should -Contain 'operator-label'
            $comments[41][-1].body | Should -BeExactly 'Keep this discussion.'
        }
        It 'does not create a new issue for a clean execution' {
            $run.conclusion = 'success'; $results[0].outcome = 'passed'
            $jobs[0].conclusion = 'success'; $jobs[0].steps[0].conclusion = 'success'
            (Invoke-TestRunPublication).requires_triage | Should -BeFalse
            $writes.Count | Should -Be 0
            $journal = Get-Content -LiteralPath (Join-Path $output 'publication-123-456-789.json') -Raw |
                ConvertFrom-Json -AsHashtable
            $journal.stage | Should -Be prepared
            $journal.issue_number | Should -BeNullOrEmpty
        }
        It 'does not forget an uncertain issue write when a rerun passes' {
            $script:hideIssueReply = $true
            { Invoke-TestRunPublication } | Should -Throw
            $run.conclusion = 'success'; $results[0].outcome = 'passed'
            $jobs[0].conclusion = 'success'; $jobs[0].steps[0].conclusion = 'success'
            $writes.Clear()
            { Invoke-TestRunPublication } | Should -Throw
            $writes.Count | Should -Be 0
        }
        It 'preserves the shared coverage-create fence when repeating clean run intake' {
            $run.conclusion = 'success'; $results[0].outcome = 'passed'
            $jobs[0].conclusion = 'success'; $jobs[0].steps[0].conclusion = 'success'
            $null = Invoke-TestRunPublication
            $journalPath = Join-Path $output 'publication-123-456-789.json'
            $journal = Get-Content -LiteralPath $journalPath -Raw | ConvertFrom-Json -AsHashtable
            $journal.coverage_creation = @{ stage = 'creating-issue'; issue_number = $null }
            Write-ScheduledRunJournal -Path $journalPath -Record $journal
            $null = Invoke-TestRunPublication
            $saved = Get-Content -LiteralPath $journalPath -Raw | ConvertFrom-Json -AsHashtable
            $saved.coverage_creation.stage | Should -Be creating-issue
            $saved.coverage_creation.issue_number | Should -BeNullOrEmpty
            $writes.Count | Should -Be 0
        }
        It 'retains a retryable pre-issue journal after label publication fails' {
            $script:failLabelWrite = $true
            { Invoke-TestRunPublication } | Should -Throw
            $issues.Count | Should -Be 0
            $journal = Get-Content -LiteralPath (Join-Path $output 'publication-123-456-789.json') -Raw |
                ConvertFrom-Json -AsHashtable
            $journal.stage | Should -Be prepared
            $script:failLabelWrite = $false
            $result = Invoke-TestRunPublication
            $result.record.revisions.Count | Should -Be 1
            $issues.Count | Should -Be 1
        }
        It 'never writes while reporting is disabled or only a dry run is requested' {
            $policy.rollout.reporting_enabled = $false
            (Invoke-TestRunPublication).actions[0].action | Should -BeExactly 'dry-run'
            $policy.rollout.reporting_enabled = $true
            (Invoke-TestRunPublication -DryRun).actions[0].action | Should -BeExactly 'dry-run'
            $writes.Count | Should -Be 0
        }
        It 'recovers lost issue and page responses without duplicate writes' {
            $script:loseIssueReply = $true; $script:losePageReply = $true
            $result = Invoke-TestRunPublication
            $result.record.revisions.Count | Should -Be 1
            $issues.Count | Should -Be 1
            $comments[41].Count | Should -Be 1
            @($writes | Where-Object method -EQ POST).Count | Should -Be 3
        }
        It 'blocks an unresolved page write until that operation can be reconciled' {
            $script:hidePageReply = $true
            { Invoke-TestRunPublication } | Should -Throw '*publication outcome is unknown*'
            $writes.Clear()
            { Invoke-TestRunPublication } | Should -Throw '*previous evidence-page write is unresolved*'
            $writes.Count | Should -Be 0
            $comments[41].Add($script:hiddenPage)
            $result = Invoke-TestRunPublication
            $result.record.revisions.Count | Should -Be 1
            @($writes | Where-Object method -EQ POST).Count | Should -Be 0
        }
        It 'never blindly repeats an unresolved issue creation' {
            $script:hideIssueReply = $true
            { Invoke-TestRunPublication } | Should -Throw '*issue creation outcome is unknown*'
            $writes.Clear()
            { Invoke-TestRunPublication } | Should -Throw '*Prior run issue creation remains unresolved*'
            $writes.Count | Should -Be 0
            $issues[41] = $script:hiddenIssue
            $null = Invoke-TestRunPublication
            @($writes | Where-Object { $_.endpoint -eq 'repos/owner/repo/issues' -and $_.method -eq 'POST' }).Count |
                Should -Be 0
        }
        It 'recovers a lost index response without reposting pages' {
            $script:loseIndexReply = $true
            { Invoke-TestRunPublication } | Should -Throw '*Lost index response*'
            $writes.Clear()
            $null = Invoke-TestRunPublication
            $writes.Count | Should -Be 0
        }
        It 'does not silently rebuild deleted committed history' {
            $null = Invoke-TestRunPublication
            $comments[41].Clear()
            $writes.Clear()
            { Invoke-TestRunPublication } | Should -Throw '*Committed run history differs*'
            $writes.Count | Should -Be 0
        }
        It 'reopens new failed evidence after an interrupted index update' {
            $null = Invoke-TestRunPublication
            $issues[41].state = 'closed'; $issues[41].labels += @{ name = 'scheduled-triaged' }
            $run.run_attempt = 2
            $script:failIndexWrite = $true
            { Invoke-TestRunPublication } | Should -Throw '*Index write interrupted*'
            $writes.Clear()
            $null = Invoke-TestRunPublication
            $issues[41].state | Should -BeExactly 'open'
            $issues[41].labels.name | Should -Not -Contain 'scheduled-triaged'
            @($writes | Where-Object method -EQ POST).Count | Should -Be 0
        }
        It 'retains green reruns and reopens only for new failed evidence' {
            $first = Invoke-TestRunPublication
            $issues[41].state = 'closed'; $issues[41].labels += @{ name = 'scheduled-triaged' }
            $run.run_attempt = 2; $run.conclusion = 'success'; $results[0].outcome = 'passed'
            $jobs[0].conclusion = 'success'; $jobs[0].steps[0].conclusion = 'success'
            $green = Invoke-TestRunPublication
            $green.record.revisions.Count | Should -Be 2
            $green.record.revisions.digest | Should -Contain $first.digest
            $issues[41].state | Should -BeExactly 'closed'
            $issues[41].labels.name | Should -Contain 'scheduled-triaged'
            $run.run_attempt = 3; $run.conclusion = 'failure'; $results[0].outcome = 'findings'
            $jobs[0].conclusion = 'failure'; $jobs[0].steps[0].conclusion = 'failure'
            $null = Invoke-TestRunPublication
            $issues[41].state | Should -BeExactly 'open'
            $issues[41].labels.name | Should -Not -Contain 'scheduled-triaged'
        }
        It 'paginates large evidence without dropping records or exceeding body limits' {
            $results[0].details = 'x' * 100000
            $result = Invoke-TestRunPublication
            $comments[41].Count | Should -BeGreaterThan 1
            $result.record.revisions[0].evidence.attempt.results[0].details.Length | Should -Be 100000
            foreach ($comment in $comments[41]) { $comment.body.Length | Should -BeLessThan 65536 }
        }
    }
}
