#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }

# Protects the workflow boundaries: nightly fresh execution, immutable controller/candidate
# checkouts, read-only candidate permissions and unconditional diagnostic preservation.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true

BeforeAll { $script:workflows = Join-Path $PSScriptRoot '..\..\.github\workflows' }

Describe 'Hosted deep execution wiring' {
    It 'keeps nightly and manual full execution without source-success reuse gates' {
        $workflow = Get-Content -LiteralPath (Join-Path $workflows 'full-deep-validation.yml') -Raw
        $workflow | Should -Match '(?m)^  schedule:'
        $workflow | Should -Match '(?m)^  workflow_dispatch:'
        $workflow | Should -Match '(?m)^      source_sha:'
        $workflow | Should -Not -Match 'needs\.plan\.outputs\.run|coverage|receipt|manifest|issues:'
    }

    It 'keeps selected execution manual and independent of PR and main push gates' {
        $workflow = Get-Content -LiteralPath (Join-Path $workflows 'selected-deep-validation.yml') -Raw
        $workflow | Should -Match '(?m)^  workflow_dispatch:'
        $workflow | Should -Not -Match '(?m)^  (push|pull_request|schedule):'
        $workflow | Should -Not -Match 'managed|registration|issues:|pull-requests:'
    }

    It 'pins both checkouts, uses read-only authority and preserves failed-job diagnostics' {
        $workflow = Get-Content -LiteralPath (Join-Path $workflows 'deep-checks.yml') -Raw
        $workflow | Should -Match 'ref: \$\{\{ inputs\.controller_sha \}\}'
        $workflow | Should -Match 'ref: \$\{\{ inputs\.source_sha \}\}'
        $workflow | Should -Match 'path: candidate'
        $workflow | Should -Match 'contents: read'
        $workflow | Should -Not -Match ':\s*write|continue-on-error|secrets: inherit'
        $workflow | Should -Match 'fail-fast: false'
        $workflow | Should -Match 'if: always\(\)'
        $workflow | Should -Match 'name: scheduled-result-\$\{\{ github.run_id \}\}-\$\{\{ github.run_attempt \}\}-\$\{\{ matrix.id \}\}'
        $workflow | Should -Match 'SCHEDULED_SOURCE_SHA: \$\{\{ inputs.source_sha \}\}'
    }
}
