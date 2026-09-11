#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }

# Pester suite for RequiredChecks.psm1. The allowed-result policy (must-succeed vs may-skip)
# is the contract the Standard validation `required-checks` fan-in publishes to GitHub, so it is
# exercised against realistic `toJSON(needs)` payloads here rather than only in CI.

BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'RequiredChecks.psm1') -Force
}

Describe 'Assert-RequiredCheck' {
    It 'does not throw when every job succeeded or skipped where allowed' {
        $json = '{"delta":{"result":"success"},"test-arm":{"result":"skipped"}}'
        { Assert-RequiredCheck -NeedsJson $json -MustSucceedJob @('delta') } | Should -Not -Throw
    }

    It 'throws when a dependency produced a non-allowed result' {
        $json = '{"validate-versions":{"result":"failure"}}'
        { Assert-RequiredCheck -NeedsJson $json -MustSucceedJob @('validate-versions') } |
            Should -Throw
    }

    It 'throws when a must-succeed job drifted out of the needs list' {
        $json = '{"delta":{"result":"success"}}'
        { Assert-RequiredCheck -NeedsJson $json -MustSucceedJob @('delta', 'validate-versions') } |
            Should -Throw
    }

    It 'throws when the must-succeed list is empty' {
        $json = '{"delta":{"result":"success"}}'
        { Assert-RequiredCheck -NeedsJson $json -MustSucceedJob @() } |
            Should -Throw '*MUST_SUCCEED_JOBS is empty*'
    }

    It 'ignores blank and padded must-succeed entries' {
        # The workflow supplies the list as a literal split on newlines, so a trailing blank
        # line must not be classified as a job that is absent from the needs payload.
        $json = '{"delta":{"result":"success"}}'
        { Assert-RequiredCheck -NeedsJson $json -MustSucceedJob @('  delta  ', '', '   ') } |
            Should -Not -Throw
    }

    It 'throws when the must-succeed list names no job at all' {
        $json = '{"delta":{"result":"success"}}'
        { Assert-RequiredCheck -NeedsJson $json -MustSucceedJob @('', '  ') } |
            Should -Throw '*names no jobs*'
    }
}

Describe 'Get-RequiredCheckFailure' {
    It 'returns nothing when every job succeeded' {
        InModuleScope RequiredChecks {
            $json = '{"delta":{"result":"success"},"test-scripts":{"result":"success"}}'
            Get-RequiredCheckFailure -NeedsJson $json -MustSucceedJob @('delta', 'test-scripts') |
                Should -BeNullOrEmpty
        }
    }

    It 'treats skipped as allowed for a job that may skip' {
        InModuleScope RequiredChecks {
            $json = '{"delta":{"result":"success"},"test-arm":{"result":"skipped"}}'
            Get-RequiredCheckFailure -NeedsJson $json -MustSucceedJob @('delta') |
                Should -BeNullOrEmpty
        }
    }

    It 'rejects skipped for an unconditional gate' {
        InModuleScope RequiredChecks {
            $json = '{"validate-versions":{"result":"skipped"},"test-arm":{"result":"skipped"}}'
            Get-RequiredCheckFailure -NeedsJson $json -MustSucceedJob @('validate-versions') |
                Should -Be @('validate-versions=skipped')
        }
    }

    It 'reports a failed job as job=result' {
        InModuleScope RequiredChecks {
            $json = '{"delta":{"result":"success"},"clippy-dev":{"result":"failure"}}'
            Get-RequiredCheckFailure -NeedsJson $json -MustSucceedJob @('delta') |
                Should -Be @('clippy-dev=failure')
        }
    }

    It 'reports cancelled and other non-allowed results' {
        InModuleScope RequiredChecks {
            $json = '{"test-x64":{"result":"cancelled"},"hack":{"result":"neutral"}}'
            $result = @(Get-RequiredCheckFailure -NeedsJson $json -MustSucceedJob @('delta'))
            $result | Should -Contain 'test-x64=cancelled'
            $result | Should -Contain 'hack=neutral'
        }
    }

    It 'rejects a result whose spelling is not the exact GitHub value' {
        # GitHub writes these results in lower case. Any other casing is an unknown value, and
        # this fan-in is the last gate before a merge, so it has to fail closed rather than read
        # a look-alike as a pass.
        InModuleScope RequiredChecks {
            $json = '{"delta":{"result":"Success"},"clippy-dev":{"result":"SKIPPED"}}'
            $result = @(Get-RequiredCheckFailure -NeedsJson $json -MustSucceedJob @('delta'))
            $result | Should -Contain 'delta=Success'
            $result | Should -Contain 'clippy-dev=SKIPPED'
        }
    }

    It 'treats a missing result property as a failure' {
        InModuleScope RequiredChecks {
            $json = '{"delta":{"outputs":{}}}'
            Get-RequiredCheckFailure -NeedsJson $json -MustSucceedJob @('delta') |
                Should -Be @('delta=missing')
        }
    }

    It 'throws when the payload is empty' {
        InModuleScope RequiredChecks {
            { Get-RequiredCheckFailure -NeedsJson '' -MustSucceedJob @('delta') } |
                Should -Throw '*NEEDS_JSON is empty*'
        }
    }

    It 'throws when the payload is an empty object' {
        InModuleScope RequiredChecks {
            { Get-RequiredCheckFailure -NeedsJson '{}' -MustSucceedJob @('delta') } |
                Should -Throw '*has no jobs*'
        }
    }

    It 'reports a must-succeed job that is absent from the needs payload' {
        InModuleScope RequiredChecks {
            $json = '{"test-scripts":{"result":"success"}}'
            Get-RequiredCheckFailure -NeedsJson $json -MustSucceedJob @('delta', 'test-scripts') |
                Should -Be @('delta=absent')
        }
    }

    It 'reports every absent must-succeed job in a deterministic order' {
        InModuleScope RequiredChecks {
            $json = '{"delta":{"result":"success"}}'
            $job = @('semver-checks', 'delta', 'validate-versions')
            Get-RequiredCheckFailure -NeedsJson $json -MustSucceedJob $job |
                Should -Be @('semver-checks=absent', 'validate-versions=absent')
        }
    }
}

Describe 'Planned tooling results' {
    BeforeEach {
        $script:plan = @{ workflows = $false; script_analysis = $false; script_domains = @() }
        $script:needs = @{
            changes = @{ result = 'success'; outputs = @{} }
            delta = @{ result = 'success'; outputs = @{ packages_json = '[]'; script_domains = '[]' } }
            'test-scripts' = @{ result = 'skipped' }
            'validate-scripts' = @{ result = 'skipped' }
            'validate-workflows' = @{ result = 'skipped' }
        }
        function Assert-PlannedResult {
            $needs.changes.outputs.plan = ConvertTo-Json -InputObject $plan -Compress
            Assert-RequiredCheck -NeedsJson (ConvertTo-Json -InputObject $needs -Depth 10) `
                -MustSucceedJob @('changes', 'delta')
        }
    }

    It 'accepts planned skips but rejects a selected workflow check that skipped' {
        { Assert-PlannedResult } | Should -Not -Throw
        $plan.workflows = $true
        { Assert-PlannedResult } | Should -Throw
        $needs['validate-workflows'].result = 'success'
        { Assert-PlannedResult } | Should -Not -Throw
    }

    It 'requires script analysis when selected' {
        $plan.script_analysis = $true
        { Assert-PlannedResult } | Should -Throw
        $needs['validate-scripts'].result = 'success'
        { Assert-PlannedResult } | Should -Not -Throw
    }

    It 'requires path-selected script tests and rejects lost domains' {
        $plan.script_domains = @('book')
        { Assert-PlannedResult } | Should -Throw
        $needs.delta.outputs.script_domains = '["book"]'
        { Assert-PlannedResult } | Should -Throw
        $needs['test-scripts'].result = 'success'
        { Assert-PlannedResult } | Should -Not -Throw
    }

    It 'requires integration tests for an affected native helper' {
        $needs.delta.outputs.packages_json = '["release-target-check"]'
        { Assert-PlannedResult } | Should -Throw
        $needs.delta.outputs.script_domains = '["release"]'
        { Assert-PlannedResult } | Should -Throw
        $needs['test-scripts'].result = 'success'
        { Assert-PlannedResult } | Should -Not -Throw
    }

    It 'rejects omitted conditional jobs even for a no-work plan' -ForEach @(
        'test-scripts', 'validate-scripts', 'validate-workflows'
    ) {
        $needs.Remove($_)
        { Assert-PlannedResult } | Should -Throw
    }

    It 'rejects failed or cancelled planners even when all tooling jobs skipped' -ForEach @(
        'failure', 'cancelled', 'skipped'
    ) {
        $needs.changes.result = $_
        { Assert-PlannedResult } | Should -Throw
    }

    It 'rejects absent planner output' {
        $needs.delta.outputs.Remove('script_domains')
        { Assert-PlannedResult } | Should -Throw
    }
}
