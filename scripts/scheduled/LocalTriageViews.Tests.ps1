#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
# Verifies that complete-index summaries and full-record cursors remain bounded without
# discarding authoritative evidence or mistaking human marker-like text for machine pages.
BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'LocalTriageView.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'LocalTriagePolicy.psm1') -Force
}

Describe 'Bounded complete-index views' {
    It 'covers every candidate across bounded pages including legacy and closed problems' {
        $snapshot = @{ index = @{ digest = 'index'; entries = @() }; problems = @{} }
        foreach ($number in 1..12) {
            $scope = @{ operation = 'miri'; package = 'package'; check_id = 'miri'; platform = 'linux'; replay = $null }
            $diagnosis = @{ category = 'code'; summary = 'summary' * 300; cause = 'cause' * 300
                repair_disposition = 'needs-human'; scope = @($scope) }
            $problem = @{ generation = 1; status = 'resolved'; diagnosis = $diagnosis
                evidence = @(@{ generation = 1; diagnosis = $diagnosis; revision = @{ issue_number = 20 } }) }
            $legacy = @{ status = 'confirmed'; check_kind = 'miri'; package = 'package'; check_id = 'miri'; platform = 'linux'
                evidence = @{ summary = 'legacy summary'; replay = @{ target = 'legacy-target' } } }
            $snapshot.index.entries += @{ issue_number = $number; generation = 1; scope_revision = 1; record_digest = "record-$number" }
            $snapshot.problems[[string]$number] = @{
                record = @{ issue = @{ title = 'title'; state = 'closed' }
                    problem = if ($number -eq 1) { $null } else { $problem }; legacy = $legacy }
            }
        }
        $offset = 0
        $numbers = @()
        do {
            $page = Get-ScheduledTriageIndexPage $snapshot $offset
            $page.entries.Count | Should -BeGreaterThan 0
            $numbers += @($page.entries | ForEach-Object { $_.issue_number })
            $page.entries[0].issue_state | Should -Be closed
            $page.entries[0].summary_is_abbreviated | Should -BeTrue
            foreach ($entry in $page.entries) {
                $entry.evidence_issue_numbers -is [array] | Should -BeTrue
                $entry.scope_preview[0].operation | Should -Be miri
                if ($entry.issue_number -eq 1) {
                    ($entry.scope_preview[0].replay_preview | ConvertFrom-Json).target | Should -Be legacy-target
                } else {
                    $entry.scope_preview[0].replay_preview | Should -BeNullOrEmpty
                }
            }
            $offset = $page.next_offset
        } while ($null -ne $offset)
        $numbers | Should -Be @(1..12)
        { Get-ScheduledTriageIndexPage $snapshot -1 } | Should -Throw
        { Get-ScheduledTriageIndexPage $snapshot 99 } | Should -Throw

        # JSON escaping can dominate a single otherwise bounded summary. Preserve its
        # identity and full-read route when even the preview exceeds the response budget.
        $snapshot.index.entries = @($snapshot.index.entries[1])
        $full = $snapshot.problems['2'].record
        $escaped = [string][char]1
        $full.issue.title = $escaped * 512
        $full.problem.diagnosis.summary = $escaped * 512
        $full.problem.diagnosis.cause = $escaped * 512
        $scope = @{ operation = 'miri'; package = $escaped * 64; check_id = $escaped * 64
            platform = $escaped * 64; replay = $null }
        $full.problem.evidence[0].diagnosis.scope = @($scope, $scope, $scope)
        $page = Get-ScheduledTriageIndexPage $snapshot 0
        $page.entries.Count | Should -Be 1
        $page.entries[0].issue_number | Should -Be 2
        $page.entries[0].record_digest | Should -BeExactly $snapshot.index.entries[0].record_digest
        $page.entries[0].title.Length | Should -Be 128
        $page.entries[0].full_record_available | Should -BeTrue
    }

    It 'distinguishes operation and typed replay qualifiers within bounded candidate previews' {
        $snapshot = @{ index = @{ digest = 'index'; entries = @() }; problems = @{} }
        foreach ($number in 1..2) {
            $diagnosis = @{ category = 'code'; summary = 'same symptom'; cause = 'same candidate cause'
                repair_disposition = 'actionable'; scope = @(@{
                    operation = "miri qualifier $number"; package = 'package'; check_id = 'miri'; platform = 'linux'
                    replay = @{ seed = $number; settings = @{ feature = 'custom' } }
                }) }
            $snapshot.index.entries += @{ issue_number = $number; generation = 1; scope_revision = 1; record_digest = "record-$number" }
            $snapshot.problems[[string]$number] = @{ record = @{
                issue = @{ title = 'same title'; state = 'open' }; legacy = $null
                problem = @{ generation = 1; status = 'open'; diagnosis = $diagnosis
                    evidence = @(@{ generation = 1; diagnosis = $diagnosis; revision = @{ issue_number = 20 } }) }
            } }
        }
        $page = Get-ScheduledTriageIndexPage $snapshot 0 | ConvertTo-Json -Depth 20 | ConvertFrom-Json
        foreach ($entry in $page.entries) {
            $entry.scope_preview[0].operation | Should -Be "miri qualifier $($entry.issue_number)"
            $replay = $entry.scope_preview[0].replay_preview | ConvertFrom-Json
            $replay.seed | Should -Be $entry.issue_number
            $replay.settings.feature | Should -Be custom
        }
        $scope = $snapshot.problems['1'].record.problem.diagnosis.scope[0]
        $scope.operation = 'operation ' * 1000
        $scope.replay = @{ qualifier = 'setting ' * 1000 }
        $page = Get-ScheduledTriageIndexPage $snapshot 0
        $page.entries[0].scope_preview[0].operation.Length | Should -BeLessOrEqual 256
        $page.entries[0].scope_preview[0].replay_preview.Length | Should -BeLessOrEqual 512
        $page.entries[0].summary_is_abbreviated | Should -BeTrue
        [Text.Encoding]::UTF8.GetByteCount(($page.entries | ConvertTo-Json -Depth 20 -Compress)) | Should -BeLessOrEqual 12000
    }

    It 'retains human marker-like discussion while omitting only already decoded transport pages' {
        $human = "[Copilot speaking]`n<!-- scheduled-problem-detail:v1 not an owned page -->"
        $problem = @{
            full_read_digest = 'digest'
            record = @{
                issue = @{ number = 1 }; problem = @{ status = 'open' }; legacy = $null
                comments = @(@{ id = 1; body = 'encoded transport' }, @{ id = 2; body = $human })
                details = @{ revisions = @(@{ pages = @(@{ id = 1 }) }) }
            }
        }
        $page = Get-ScheduledTriageProblemPage $problem 0
        $record = $page.content | ConvertFrom-Json -AsHashtable
        $record.comments.Count | Should -Be 1
        $record.comments[0].body | Should -BeExactly $human
        $problem.record.details = $null
        $page = Get-ScheduledTriageProblemPage $problem 0
        ($page.content | ConvertFrom-Json -AsHashtable).comments.Count | Should -Be 2
    }
}

Describe 'Explicit triage policy' {
    It 'rejects missing fields invalid capacity and unconfigured activation' {
        $original = Get-ScheduledTriagePolicy
        foreach ($change in @('missing', 'capacity', 'empty-name', 'budget', 'activation')) {
            $policy = $original | ConvertTo-Json | ConvertFrom-Json -AsHashtable
            switch ($change) {
                missing { $policy.Remove('mode') }
                capacity { $policy.max_active_analyses = 2 }
                empty-name { $policy.automation_name = '' }
                budget { $policy.max_starts_per_day = 0 }
                activation { $policy.mode = 'triage' }
            }
            $path = Join-Path $TestDrive "$change.json"
            $policy | ConvertTo-Json | Set-Content -LiteralPath $path
            { Get-ScheduledTriagePolicy -Path $path } | Should -Throw
        }
    }
}
