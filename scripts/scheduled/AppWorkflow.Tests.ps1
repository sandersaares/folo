#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
$VerbosePreference = 'Continue'

# The scheduled script-test domain protects the checked-in App entry points without
# running an agent, contacting GitHub or creating native automations. These checks
# cover routing, native setup inputs and local links, not the quality of AI diagnosis.
BeforeAll {
    $root = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..\..'))
    $skills = @{}
    foreach ($role in @('scheduled-triage', 'scheduled-intake', 'scheduled-repair')) {
        $skills[$role] = Get-Content -LiteralPath (Join-Path $root ".github\skills\$role\SKILL.md") -Raw
    }
    $setupPath = Join-Path $root '.github\prompts\setup-scheduled-remediation.prompt.md'
    $script:setup = Get-Content -LiteralPath $setupPath -Raw
    $guidePath = Join-Path $root 'docs\scheduled-validation.md'
    $script:guide = Get-Content -LiteralPath $guidePath -Raw
}

Describe 'Scheduled App entry-point routing' {
    It 'routes each role to a named skill with sequential stages' {
        foreach ($role in $skills.Keys) {
            $skills[$role] | Should -Match "(?m)^name: $role`r?$"
            $stages = @([regex]::Matches($skills[$role], '(?m)^# Stage (\d+):') |
                ForEach-Object { [int] $_.Groups[1].Value })
            $stages.Count | Should -BeGreaterThan 0
            ($stages -join ',') | Should -Be ((1..$stages.Count) -join ',')
        }
        $setup | Should -Match '`scheduled-triage`'
        $setup | Should -Match '`scheduled-intake`'
        $skills['scheduled-intake'] | Should -Match '`open_issue_session`'
        $skills['scheduled-intake'] | Should -Match '`open_pr_session`'
        $skills['scheduled-intake'] | Should -Match '`send_session_message`'
        $skills['scheduled-intake'] | Should -Match '`scheduled-repair`'
        $skills['scheduled-repair'] | Should -Match '`increment-versions`'
        $skills['scheduled-repair'] | Should -Match '`reply_and_resolve_review_thread`'
    }

    It 'uses ordinary branches without hosted PR scope selection' {
        $allText = ($skills.Values -join "`n") + $setup + $guide
        $allText | Should -Not -Match 'scheduled-repair-|rename_branch|source_sha|check_ids'
        $allText | Should -Not -Match 'full-deep-validation|selected-deep-validation|deep-checks\.yml'
        $allText | Should -Not -Match 'scheduled-report\.yml|Scheduled reporting|workflow_run'
        $skills['scheduled-repair'] | Should -Match 'ordinary repair'
    }

    It 'documents one no-input full hosted run on main and same-workflow report recovery' {
        $commands = @([regex]::Matches($guide, '(?m)^gh workflow run[^\r\n]+'))
        $commands.Count | Should -Be 1
        $commands[0].Value | Should -BeExactly 'gh workflow run deep-validation.yml --ref main'
        $guide | Should -Match 'gh run rerun --job'
        ($guide -replace '\s+', ' ') | Should -Match 'may also rerun failed checks'
    }

    It 'keeps deep verification and the complete PR lifecycle in the worker instructions' {
        $repair = $skills['scheduled-repair'] -replace '\s+', ' '
        $repair | Should -Match 'just package="\{\{PACKAGES\}\}" validate-deep-local'
        $repair | Should -Match 'same commands in WSL'
        $repair | Should -Match 'not PR-head validation evidence'
        $repair | Should -Match 'Human review may resolve that limitation'
        $repair | Should -Match '`needs-human`'
        $repair | Should -Match 'tested commit and scope'
        $repair | Should -Match 'normal required checks'
        $repair | Should -Match 'top-level comments, review summaries and inline threads'
        $repair | Should -Match 'awaits human review/approval/merge'
        $repair | Should -Match 'closed without merging does not resolve the issue'
        $repair | Should -Match 'do not start a timer or hidden watcher'
    }

    It 'keeps the setup example disabled and uses the native creation dialog' {
        $examples = @([regex]::Matches($setup, '(?s)```json\s*(.*?)\s*```'))
        $examples.Count | Should -Be 1
        $create = $examples[0].Groups[1].Value | ConvertFrom-Json -AsHashtable
        $create.enabled | Should -BeFalse
        $create.user_confirmation | Should -BeExactly dialog
        $create.interval | Should -BeExactly manual
        $create.cron_expression | Should -Not -BeNullOrEmpty
        $create.clear_remote_branch | Should -BeTrue
        $create.ContainsKey('host_id') | Should -BeFalse
        $create.ContainsKey('model') | Should -BeFalse
        $create.ContainsKey('workflow_id') | Should -BeFalse
        $create.Keys | Sort-Object | Should -Be @(
            'clear_remote_branch', 'cron_expression', 'enabled', 'interval', 'mode',
            'name', 'project_id', 'prompt', 'user_confirmation'
        )
        $setup | Should -Match '`list_projects`'
        $setup | Should -Match '`list_workflows`'
        $setup | Should -Match '`save_workflow`'
    }

    It 'does not route work through the removed Local protocol' {
        $allText = ($skills.Values -join "`n") + $setup
        $allText | Should -Not -Match 'Local\w+\.psm1|triage-policy\.json|scheduled-local'
        $allText | Should -Not -Match 'scheduled-run:v\d|scheduled-reporter:v\d|reserve-attempt'
        @(Get-ChildItem -LiteralPath $PSScriptRoot -File -Filter 'Local*').Count | Should -Be 0
        Test-Path -LiteralPath (Join-Path $PSScriptRoot 'triage-policy.json') | Should -BeFalse
    }

    It 'keeps the affected Markdown links within existing repository documents' {
        $documents = @($setupPath, $guidePath)
        foreach ($role in $skills.Keys) {
            $documents += Join-Path $root ".github\skills\$role\SKILL.md"
        }
        foreach ($document in $documents) {
            $text = Get-Content -LiteralPath $document -Raw
            foreach ($link in [regex]::Matches($text, '\]\(([^)]+)\)')) {
                $target = ($link.Groups[1].Value -split '#', 2)[0]
                if ($target -eq '' -or $target -match '^[a-z]+:') { continue }
                $path = Join-Path (Split-Path $document -Parent) $target.Replace('/', '\')
                Test-Path -LiteralPath $path -PathType Leaf | Should -BeTrue -Because $link.Value
            }
        }
    }
}
