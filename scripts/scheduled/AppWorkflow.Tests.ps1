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

    It 'carries explicit shared model and effort settings into new repair-session kickoff' {
        $setupText = $setup -replace '\s+', ' '
        $setupText | Should -Match 'model/effort choice for triage, repair coordination and new repair sessions'
        $setupText | Should -Match 'actual operator choice, including a shared selection'
        $setupText | Should -Match 'For explicit overrides, instruct `scheduled-intake` in that saved prompt to pass them through `kickoff\.model` and `kickoff\.reasoning_effort`'
        $setupText | Should -Match 'when opening a new repair session with `open_issue_session` or `open_pr_session`'
        $setupText | Should -Match "following the skill's waiting bootstrap rules"
        $intake = $skills['scheduled-intake'] -replace '\s+', ' '
        $intake | Should -Match 'explicit operator-selected model/effort needs the supported kickoff fields'
        $intake | Should -Match 'Use `kickoff\.mode: interactive` for this waiting bootstrap'
    }

    It 'omits kickoff for App defaults without opening repair sessions during setup' {
        $setupText = $setup -replace '\s+', ' '
        $setupText | Should -Match 'For App defaults, instruct it to omit kickoff'
        $setupText | Should -Match 'Setup itself must not open repair sessions'
        $setupText | Should -Match 'preserve existing session settings'
        $intake = $skills['scheduled-intake'] -replace '\s+', ' '
        $intake | Should -Match 'For a new session, omit kickoff when the operator chose App defaults'
        $intake | Should -Match 'Preserve existing session settings'
    }

    It 'hands interrupted dialog interaction to the operator without running an automation' {
        $setupText = $setup -replace '\s+', ' '
        $setupText | Should -Match 'Prefer asking the operator to review and save the dialog over driving it with Computer Use'
        $setupText | Should -Match 'repeated active-input interruptions require an operator handoff rather than further retries'
        $setupText | Should -Match 'save disabled without using \*\*Create and run\*\*'
        $setupText | Should -Match 'Opening the dialog does not establish that it was saved'
    }

    It 'reuses only a verified Local host ID for the same environment' {
        $setupText = $setup -replace '\s+', ' '
        $setupText | Should -Match 'direct create instead requires the actual observed `host_id`'
        $setupText | Should -Match 'never substitute a project ID or a machine name'
        $setupText | Should -Match 'After verifying a saved entry, reuse its observed Local host ID to prefill later dialogs targeting the same environment'
    }

    It 'separates operator confirmations from metadata and reconciles conflicting settings' {
        $setupText = $setup -replace '\s+', ' '
        $setupText | Should -Match 'If timezone or next-run information is not exposed, ask the operator to confirm the intended local-time interpretation'
        $setupText | Should -Match 'Do not assume UTC, invent an environment ID or require UI fields the App does not expose'
        $setupText | Should -Match 'Reread `list_workflows` to verify the persisted repository, Local environment, prompt, selected model/effort, mode, schedule and disabled state'
        $setupText | Should -Match 'distinguish metadata-verified settings from operator confirmations'
        $setupText | Should -Match 'If confirmation conflicts with saved metadata, reconcile the discrepancy before declaring setup complete'
    }

    It 'limits smoke-check claims without authorizing live setup exercises' {
        $setupText = $setup -replace '\s+', ' '
        $setupText | Should -Match 'Saving an entry does not prove unattended permissions, machine availability or successful execution'
        $setupText | Should -Match 'empty-backlog run is only a smoke check; it does not demonstrate issue claiming, repair-session creation or PR publication'
        $setupText | Should -Match 'Do not perform a live exercise to close those evidence gaps during setup'
        $setupText | Should -Match 'Enabling is a separate explicit decision'
        $setupText | Should -Match 'Do not invoke `run_workflow`, create a per-PR timer or perform a live exercise'
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
