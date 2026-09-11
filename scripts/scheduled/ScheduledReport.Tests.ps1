#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
# Protects human-readable reporting: preserve every failure, exclude successful-job noise,
# and continue large diagnostic lists as ordinary comments without encoded records.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
$VerbosePreference = 'Continue'

BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'ScheduledReport.psm1') -Force
}

Describe 'Readable failure rendering' {
    BeforeEach {
        $script:run = @{ name = 'Full deep validation'; run_started_at = '2026-09-11T01:02:03Z'; conclusion = 'failure' }
        $script:attempt = 'https://github.com/example/repo/actions/runs/10/attempts/1'
        $script:failure = @{
            name = 'miri / miri-linux'; url = "$attempt/jobs/20"; scope = 'ubuntu-latest / miri-linux'
            conclusion = 'failure'; summary = 'error: test failed'
            diagnostics = @("test example::failed_test ... FAILED`nseed: 17`nReplay: just package=events miri")
        }
    }
    It 'renders identity, linked failed-job table and useful diagnostics' {
        $text = @(Format-ScheduledReport $run $attempt ('a' * 40) @($failure)) -join "`n"
        $text | Should -Match '^\[Copilot speaking\]'
        $text | Should -Match ([regex]::Escape($attempt))
        $text | Should -Match 'UTC start: 2026-09-11 01:02:03'
        $text | Should -Match 'a{40}'
        $text | Should -Match '\| Job / check \| Platform / shard \| Conclusion \| Observed error summary \|'
        $text | Should -Match 'example::failed_test'
        $text | Should -Match 'seed: 17'
        $text | Should -Match 'Replay: just package=events miri'
        $text | Should -Not -Match '<!--|base64|schema_version'
    }
    It 'describes absent execution without inventing a source defect' {
        $run.conclusion = 'cancelled'
        $text = @(Format-ScheduledReport $run $attempt 'Not established' @()) -join "`n"
        $text | Should -Match 'No unsuccessful jobs were returned'
        $text | Should -Match 'no checker result is inferred'
        $text | Should -Match 'cancelled'
    }
    It 'retains the first and last failures in lengthy ordinary continuation comments' {
        $failure.diagnostics = @((1..2000 | ForEach-Object {
            "MISSED mutant $_ in example::operation - replace return expression with a different value"
        }) -join "`n")
        $messages = @(Format-ScheduledReport $run $attempt ('a' * 40) @($failure))
        $messages.Count | Should -BeGreaterThan 1
        foreach ($message in $messages) {
            $message.Length | Should -BeLessOrEqual 60000
            $message | Should -Match '^\[Copilot speaking\]'
            $message | Should -Match ([regex]::Escape($attempt))
        }
        $text = $messages -join "`n"
        @([regex]::Matches($text, 'MISSED mutant (\d+) in') | ForEach-Object {
            [int]$_.Groups[1].Value
        }) | Should -Be (1..2000)
        $text | Should -Not -Match '<!--|base64|schema_version'
    }
    It 'escapes table content and keeps artifact HTML visible rather than active' {
        $failure.name = 'miri | <script>'
        $failure.diagnostics = @('<!-- not an ownership marker -->')
        $text = @(Format-ScheduledReport $run $attempt ('a' * 40) @($failure)) -join "`n"
        $text | Should -Match 'miri &#124; &lt;script&gt;'
        $text | Should -Match '(?m)^    <!-- not an ownership marker -->'
    }
    It 'keeps every failed job when the unsuccessful-job table spans comments' {
        $failures = @(1..250 | ForEach-Object { @{
            name = "check-$_"; url = "$attempt/jobs/$_"; scope = 'ubuntu-latest'
            conclusion = 'failure'; summary = 'An unsuccessful step was observed.'
            diagnostics = @("Observed error for check $_")
        } })
        $messages = @(Format-ScheduledReport $run $attempt ('a' * 40) $failures)
        $messages.Count | Should -BeGreaterThan 1
        @([regex]::Matches(($messages -join "`n"), '\| \[check-(\d+)\]') | ForEach-Object {
            [int]$_.Groups[1].Value
        }) | Should -Be (1..250)
    }
}

Describe 'Failure log excerpts' {
    It 'keeps distant error contexts without copying entire successful steps' {
        $lines = @('useful setup context', '##[error]toolchain download failed', 'HTTP 503')
        $lines += @(1..50 | ForEach-Object { "successful dependency $_" })
        $lines += @('test example::late_failure', 'thread panicked: undefined behavior', 'seed 37')
        $text = Get-ScheduledLogExcerpt ($lines -join "`n")
        $text | Should -Match 'HTTP 503'
        $text | Should -Match 'late_failure'
        $text | Should -Match 'seed 37'
        $text | Should -Not -Match 'successful dependency 25'
        $text | Should -Match 'Other log lines omitted'
    }
    It 'retains an unfamiliar failure tail and strips terminal control sequences' {
        $text = Get-ScheduledLogExcerpt ("`e[31munknown termination`e[0m")
        $text | Should -BeExactly 'unknown termination'
    }
    It 'does not clip a long error list to its first matches' {
        $text = Get-ScheduledLogExcerpt ((1..300 | ForEach-Object { "MISSED mutant $_" }) -join "`n")
        $text | Should -Match 'MISSED mutant 1'
        $text | Should -Match 'MISSED mutant 300'
    }
}

Describe 'Completion reporting workflow' {
    BeforeAll {
        $script:workflow = Get-Content -LiteralPath (Join-Path $PSScriptRoot '..\..\.github\workflows\scheduled-report.yml') -Raw
    }
    It 'runs the default-branch controller with diagnostics outside its checkout' {
        $workflow | Should -Match 'workflow_run:'
        $workflow | Should -Match 'types: \[completed\]'
        $workflow | Should -Match 'ref: \$\{\{ github.event.repository.default_branch \}\}'
        $workflow | Should -Match 'path: controller'
        $workflow | Should -Match '\./controller/scripts/scheduled/Invoke-ScheduledReport.ps1'
        $workflow | Should -Match 'path: \.scheduled-report'
        $workflow | Should -Not -Match 'setup-environment|Install-Rust|cargo run|rustup'
    }
    It 'uses the Actions token and serializes the originating attempt independently of other runs' {
        $workflow | Should -Match 'issues: write'
        $workflow | Should -Match 'GH_TOKEN: \$\{\{ github.token \}\}'
        $workflow | Should -Match 'group: scheduled-report-\$\{\{ github.event.workflow_run.id \}\}-\$\{\{ github.event.workflow_run.run_attempt \}\}'
        $workflow | Should -Not -Match 'pull-requests: write|contents: write'
    }
}
