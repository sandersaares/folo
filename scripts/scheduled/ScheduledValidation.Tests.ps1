#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }

# Protects the single main-only workflow, complete matrix execution and same-run failure report.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true

BeforeAll {
    $script:workflows = Join-Path $PSScriptRoot '..\..\.github\workflows'
    $script:workflow = Get-Content -LiteralPath (Join-Path $workflows 'deep-validation.yml') -Raw
    $script:standard = Get-Content -LiteralPath (Join-Path $workflows 'standard-validation.yml') -Raw
}

Describe 'Hosted deep validation wiring' {
    It 'runs only the full main scope on its schedule or a no-input manual dispatch' {
        $workflow | Should -Match '(?m)^name: Deep validation\r?$'
        $workflow | Should -Match '(?m)^  schedule:'
        $workflow | Should -Match '(?m)^  workflow_dispatch:'
        $workflow | Should -Match "github.repository == 'folo-rs/folo' && github.ref == 'refs/heads/main'"
        $workflow | Should -Not -Match '(?m)^  (workflow_run|workflow_call|push|pull_request):'
        $workflow | Should -Not -Match 'source_sha|controller_sha|inputs:|coverage|receipt|plan\.json'
    }

    It 'keeps planning, execution and reporting in one workflow' {
        foreach ($removed in @('full-deep-validation.yml', 'selected-deep-validation.yml', 'deep-checks.yml', 'scheduled-report.yml')) {
            Test-Path -LiteralPath (Join-Path $workflows $removed) | Should -BeFalse
        }
        $workflow | Should -Match '(?m)^  plan:'
        $workflow | Should -Match '(?m)^  checks:'
        $workflow | Should -Match '(?m)^  report:'
        $workflow | Should -Match 'needs: \[plan, checks, hack, machete, test-arm\]'
        $workflow | Should -Match 'if: failure\(\)'
        $workflow | Should -Match 'issues: write'
        $workflow | Should -Not -Match 'path: (controller|candidate)|uses: \./\.github/workflows/'
    }

    It 'runs <job> only in deep validation with its full platform and workspace scope' -ForEach @(
        @{ job = 'hack'; platforms = 'ubuntu-latest, macos-latest, windows-latest'; recipes = @('hack') },
        @{ job = 'machete'; platforms = 'ubuntu-latest, windows-latest'; recipes = @('machete') },
        @{ job = 'test-arm'; platforms = 'ubuntu-24.04-arm, windows-11-arm, macos-latest'; recipes = @('test', 'test-benches') }
    ) {
        $pattern = '(?ms)^  ' + [regex]::Escape($job) + ':\r?\n(?<body>.*?)(?=^  [a-z][a-z0-9-]*:\r?$|\z)'
        $match = [regex]::Match($workflow, $pattern)
        $match.Success | Should -BeTrue
        $body = $match.Groups['body'].Value
        $body | Should -Match '(?m)^    needs: plan\r?$'
        $body | Should -Not -Match '(?m)^    if:|needs\.delta|package='
        $body | Should -Match ('platform: \[' + [regex]::Escape($platforms) + '\]')
        $body | Should -Match 'fail-fast: false'
        foreach ($recipe in $recipes) {
            $body | Should -Match ('(?m)^          just ' + [regex]::Escape($recipe) + '\r?$')
        }
        $standard | Should -Not -Match ('(?m)^  ' + [regex]::Escape($job) + ':')
        $standard | Should -Not -Match ('(?m)^      - ' + [regex]::Escape($job) + '\r?$')
    }

    It 'retains ARM panic diagnostics, benchmark prerequisites and test-result publishing' {
        $arm = [regex]::Match($workflow, '(?ms)^  test-arm:\r?\n(?<body>.*?)(?=^  report:)').Groups['body'].Value
        $arm | Should -Match '(?m)^    env:\r?\n      RUST_BACKTRACE: "1"\r?$'
        $arm | Should -Match 'install-valgrind: "true"'
        $arm | Should -Match 'uses: codecov/codecov-action@'
        $arm | Should -Match 'files: target/nextest/default/junit.xml'
        $arm | Should -Match 'report_type: test_results'
    }

    It 'continues independent checks and preserves failures and diagnostics' {
        $workflow | Should -Match 'fail-fast: false'
        $workflow | Should -Not -Match 'continue-on-error'
        $workflow | Should -Match 'if: always\(\)'
        $workflow | Should -Match 'name: scheduled-result-\$\{\{ github.run_id \}\}-\$\{\{ github.run_attempt \}\}-\$\{\{ matrix.id \}\}'
        $workflow | Should -Match 'Invoke-ScheduledCheck\.ps1\r?\n\s+exit \$LASTEXITCODE'
        $workflow | Should -Not -Match 'just scheduled-'
    }
}
