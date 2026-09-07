#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }

# Pester suite for Delta.psm1. The parsing, shaping, and workflow-output logic used by the
# `just delta*` recipes and the CI `delta` job is exercised directly here: Read-DeltaAffectedPackage
# against realistic `cargo delta run` JSON (including the "nothing affected" and
# malformed-but-tolerated shapes that must not throw under strict mode), Get-DeltaOutput against
# the three CI step outputs it produces, and Get-DeltaWorkflowOutput against workflow branching.
# Invoke-CargoDelta drives real cargo/git, so the full path is covered by the recipes running in CI
# rather than unit-tested here.

BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'Delta.psm1') -Force
}

Describe 'Read-DeltaAffectedPackage' {
    It 'returns the affected package names' {
        $json = '{"Affected":["events_once","infinity_pool"]}'
        $result = Read-DeltaAffectedPackage -DeltaJson $json
        $result | Should -Be @('events_once', 'infinity_pool')
    }

    It 'returns a single affected package as a one-element array' {
        $result = @(Read-DeltaAffectedPackage -DeltaJson '{"Affected":["events"]}')
        $result.Count | Should -Be 1
        $result[0] | Should -Be 'events'
    }

    It 'returns an empty array when the affected list is empty' {
        Read-DeltaAffectedPackage -DeltaJson '{"Affected":[]}' | Should -BeNullOrEmpty
    }

    It 'returns an empty array when there is no Affected field (strict-mode safe)' {
        Read-DeltaAffectedPackage -DeltaJson '{"Other":1}' | Should -BeNullOrEmpty
    }

    It 'returns an empty array for a null Affected field' {
        Read-DeltaAffectedPackage -DeltaJson '{"Affected":null}' | Should -BeNullOrEmpty
    }

    It 'ignores unrelated fields in the report' {
        $json = '{"Affected":["par_bench"],"Summary":"whatever","Count":1}'
        Read-DeltaAffectedPackage -DeltaJson $json | Should -Be @('par_bench')
    }
}

Describe 'Invoke-CargoDelta empty-result composition (regression)' {
    # Invoke-CargoDelta itself drives real cargo/git and is CI-covered, but its internal hand-off
    # from Read-DeltaAffectedPackage to Select-ExistingPackage is pure and reproduced here: a
    # "nothing affected" report must survive that hand-off instead of failing the delta job on a
    # PR that touches no crate (docs, workflows or scripts only).
    It 'wraps an empty affected list so Select-ExistingPackage never binds null' {
        # Read-DeltaAffectedPackage returns @() here, which PowerShell collapses to $null on a bare
        # assignment; the @() wrap Invoke-CargoDelta applies keeps it an array so the Mandatory
        # -Affected parameter binds instead of throwing "argument is null".
        $affected = @(Read-DeltaAffectedPackage -DeltaJson '{"Affected":[]}')
        { Select-ExistingPackage -Affected $affected -WorkspacePackage @('here') } |
            Should -Not -Throw
    }

    It 'produces a skip_all output for a report that affects nothing' {
        $affected = @(Read-DeltaAffectedPackage -DeltaJson '{"Affected":[]}')
        $existing = Select-ExistingPackage -Affected $affected -WorkspacePackage @('here')
        (Get-DeltaOutput -Affected @($existing)).SkipAll | Should -Be 'true'
    }
}

Describe 'Invoke-CargoDelta baseline revision validation' {
    It 'names an unresolvable baseline revision before the pipeline runs' {
        # The baseline revision can arrive from a workflow event payload, so it is rejected before
        # any analysis runs rather than surfacing as a git worktree error minutes later.
        $configPath = (Resolve-Path (Join-Path $PSScriptRoot '..\..\delta.toml')).Path
        $revision = 'no-such-revision-for-delta-tests'
        Mock git -ModuleName Delta {
            if ($args.Count -eq 4 -and
                $args[0] -eq 'rev-parse' -and
                $args[1] -eq '--verify' -and
                $args[2] -eq '--quiet' -and
                $args[3] -eq "$revision^{commit}") {
                return $null
            }
            throw "Unexpected git call while validating a baseline revision: $($args -join ' ')"
        }

        { Invoke-CargoDelta -ConfigPath $configPath -BaselineRevision $revision -SkipFetch } |
            Should -Throw "*baseline revision '$revision' does not resolve to a commit*"
        Should -Invoke git -ModuleName Delta -Times 1 -Exactly -ParameterFilter {
            $args.Count -eq 4 -and
            $args[0] -eq 'rev-parse' -and
            $args[1] -eq '--verify' -and
            $args[2] -eq '--quiet' -and
            $args[3] -eq "$revision^{commit}"
        }
    }
}


Describe 'Select-ExistingPackage' {
    It 'drops packages that no longer exist in the workspace' {
        $result = Select-ExistingPackage `
            -Affected @('mock_bench_engine', 'cargo-bench-history-faker', 'cbh_engines') `
            -WorkspacePackage @('cargo-bench-history-faker', 'cbh_engines', 'cbh_cli')
        $result | Should -Be @('cargo-bench-history-faker', 'cbh_engines')
    }

    It 'preserves the affected order' {
        $result = Select-ExistingPackage `
            -Affected @('c', 'a', 'b') `
            -WorkspacePackage @('a', 'b', 'c')
        $result | Should -Be @('c', 'a', 'b')
    }

    It 'returns an empty array when every affected package was removed' {
        Select-ExistingPackage -Affected @('gone') -WorkspacePackage @('here') |
            Should -BeNullOrEmpty
    }

    It 'returns an empty array for an empty affected list' {
        Select-ExistingPackage -Affected @() -WorkspacePackage @('a') | Should -BeNullOrEmpty
    }

    It 'is case-sensitive (a differently cased name is treated as absent)' {
        Select-ExistingPackage -Affected @('Events') -WorkspacePackage @('events') |
            Should -BeNullOrEmpty
    }
}

Describe 'Get-DeltaOutput' {
    Context 'when packages are affected' {
        BeforeAll {
            $script:Output = Get-DeltaOutput -Affected @('events_once', 'infinity_pool')
        }

        It 'joins packages with spaces' {
            $script:Output.Packages | Should -Be 'events_once infinity_pool'
        }

        It 'emits a JSON array of the package names' {
            $script:Output.PackagesJson | Should -Be '["events_once","infinity_pool"]'
        }

        It 'does not skip when something is affected' {
            $script:Output.SkipAll | Should -Be 'false'
        }
    }

    Context 'when nothing is affected' {
        BeforeAll {
            $script:Output = Get-DeltaOutput -Affected @()
        }

        It 'produces an empty package string' {
            $script:Output.Packages | Should -Be ''
        }

        It 'produces an empty JSON array' {
            $script:Output.PackagesJson | Should -Be '[]'
        }

        It 'signals skip_all' {
            $script:Output.SkipAll | Should -Be 'true'
        }
    }

    It 'produces valid JSON that round-trips to the original list' {
        $affected = @('a-b', 'c_d', 'e')
        $json = (Get-DeltaOutput -Affected $affected).PackagesJson
        $roundTripped = @($json | ConvertFrom-Json)
        ($roundTripped -join ',') | Should -Be ($affected -join ',')
    }
}

Describe 'Get-DeltaWorkflowOutput' {
    It 'returns full-workspace outputs for push without running cargo-delta' {
        $result = @(
            Get-DeltaWorkflowOutput `
                -EventName 'push' `
                -BaselineRevision 'ignored-for-push' `
                -Analyze { throw 'cargo-delta should not run for push validation.' }
        )

        $result | Should -Be @('packages=', 'packages_json=[]', 'skip_all=false')
    }

    It 'uses the default baseline revision for pull_request when the baseline is empty' {
        $script:deltaArgs = $null
        $result = @(
            Get-DeltaWorkflowOutput `
                -EventName 'pull_request' `
                -BaselineRevision '   ' `
                -Analyze {
                    param([hashtable] $Argument)
                    $script:deltaArgs = $Argument
                    @()
                }
        )

        $result | Should -Be @('packages=', 'packages_json=[]', 'skip_all=true')
        $script:deltaArgs['SkipFetch'] | Should -BeTrue
        $script:deltaArgs.ContainsKey('BaselineRevision') | Should -BeFalse
    }

    It 'passes an explicit merge_group baseline revision through to cargo-delta' {
        $script:deltaArgs = $null
        $result = @(
            Get-DeltaWorkflowOutput `
                -EventName 'merge_group' `
                -BaselineRevision 'abc123' `
                -Analyze {
                    param([hashtable] $Argument)
                    $script:deltaArgs = $Argument
                    @('events_once')
                }
        )

        $result | Should -Be @(
            'packages=events_once'
            'packages_json=["events_once"]'
            'skip_all=false'
        )
        $script:deltaArgs['SkipFetch'] | Should -BeTrue
        $script:deltaArgs['BaselineRevision'] | Should -Be 'abc123'
    }
}
