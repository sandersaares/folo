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
            $scope = @{ package = 'package'; check_id = 'miri'; platform = 'linux' }
            $diagnosis = @{ category = 'code'; summary = 'summary' * 300; cause = 'cause' * 300
                repair_disposition = 'needs-human'; scope = @($scope) }
            $problem = @{ generation = 1; status = 'resolved'; diagnosis = $diagnosis
                evidence = @(@{ generation = 1; diagnosis = $diagnosis; revision = @{ issue_number = 20 } }) }
            $legacy = @{ status = 'confirmed'; package = 'package'; check_id = 'miri'; platform = 'linux'
                evidence = @{ summary = 'legacy summary' } }
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
            foreach ($entry in $page.entries) { $entry.evidence_issue_numbers -is [array] | Should -BeTrue }
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
        $scope = @{ package = $escaped * 64; check_id = $escaped * 64; platform = $escaped * 64 }
        $full.problem.evidence[0].diagnosis.scope = @($scope, $scope, $scope)
        $page = Get-ScheduledTriageIndexPage $snapshot 0
        $page.entries.Count | Should -Be 1
        $page.entries[0].issue_number | Should -Be 2
        $page.entries[0].title.Length | Should -Be 128
        $page.entries[0].full_record_available | Should -BeTrue
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
