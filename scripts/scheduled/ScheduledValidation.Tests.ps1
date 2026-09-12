#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }

# Protects the single main-only workflow, complete matrix execution and same-run failure report.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true

BeforeAll {
    $script:workflows = Join-Path $PSScriptRoot '..\..\.github\workflows'
    $script:workflow = Get-Content -LiteralPath (Join-Path $workflows 'deep-validation.yml') -Raw
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
        $workflow | Should -Match 'needs: \[plan, checks\]'
        $workflow | Should -Match 'if: failure\(\)'
        $workflow | Should -Match 'issues: write'
        $workflow | Should -Not -Match 'path: (controller|candidate)|uses: \./\.github/workflows/'
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
