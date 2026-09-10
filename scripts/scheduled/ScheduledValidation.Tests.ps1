#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
# Protects fixed local validation depth and the hosted validation graph without executing any
# checker. Planner tests isolate GitHub reads so disabled execution, manual requests and
# managed repair gates remain independent of ordinary shallow CI.
# Ref: ../../.github/workflows/design.md#shallow-and-deep-validation.
BeforeAll {
    $root = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
    $script:recipes = (just --justfile (Join-Path $root 'justfile') --dump --dump-format json |
        ConvertFrom-Json -AsHashtable).recipes
    $workflow = Get-Content -LiteralPath (Join-Path $root '.github\workflows\standard-validation.yml') -Raw
    $jobDefinitions = ($workflow -split '(?m)^jobs:\r?$', 2)[1]
    $jobs = @{}
    foreach ($match in [regex]::Matches($jobDefinitions, '(?ms)^  ([\w-]+):\r?\n(.*?)(?=^  [\w-]+:|\z)')) {
        $jobs[$match.Groups[1].Value] = $match.Groups[2].Value
    }
    Import-Module (Join-Path $PSScriptRoot 'ScheduledContracts.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'ScheduledPlan.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'ScheduledWorkflow.psm1') -Force
}

Describe 'Policy-independent local validation recipes' {
    It 'preserves the shallow suite without indirect policy or deep calls' {
        $expected = @('format-check', 'validate-workflows', 'validate-binstall', 'validate-scripts',
            'check dev', 'clippy dev', 'test', 'test-benches-criterion', 'test-docs', 'docs',
            'docs-default-features', 'machete', 'default-features-check', 'check-external-types',
            'check release', 'clippy release', 'build release')
        @($recipes['validate-local'].body | ForEach-Object { $_[-1] }) |
            Should -Be @($expected | ForEach-Object { "`" $_" })
        $recipes['validate-local'].dependencies | Should -BeNullOrEmpty
    }

    It 'runs the explicit deep suite without shallow validation or scheduling policy' {
        @($recipes['validate-deep-local'].body | ForEach-Object { $_[-1] }) |
            Should -Be @('" miri', '" mutants', '" miri-harder', '" careful')
        $recipes['validate-deep-local'].dependencies | Should -BeNullOrEmpty
        $recipes.ContainsKey('validate-deep') | Should -BeFalse
        $recipes.ContainsKey('_scheduled-routine-deep') | Should -BeFalse
    }

    It 'forwards the requested package scope in both suites' {
        foreach ($name in @('validate-local', 'validate-deep-local')) {
            foreach ($line in $recipes[$name].body) {
                $line[0] | Should -BeExactly 'just package="'
                $line[1][0] | Should -Be @('variable', 'package')
            }
        }
    }
}

Describe 'Shallow hosted validation with managed deep evidence' {
    It 'uses the approved workflow names and matching file paths' {
        $expectedNames = @{
            'standard-validation.yml' = 'Standard validation'
            'full-deep-validation.yml' = 'Full deep validation'
            'selected-deep-validation.yml' = 'Selected deep validation'
            'deep-checks.yml' = 'Deep checks'
        }
        foreach ($file in $expectedNames.Keys) {
            $text = Get-Content -LiteralPath (Join-Path $root ".github\workflows\$file") -Raw
            $text | Should -MatchExactly "(?m)^name: $([regex]::Escape($expectedNames[$file]))\r?$"
        }
    }

    It 'receives only full and selected deep workflow completions' {
        $text = Get-Content -LiteralPath (Join-Path $root '.github\workflows\scheduled-report.yml') -Raw
        $names = [regex]::Match($text, '(?m)^    workflows: \[([^\]]+)\]\r?$').Groups[1].Value -split ', '
        $names | Should -Be @('Full deep validation', 'Selected deep validation')
        $text | Should -Match '(?m)^    branches: \[main\]\r?$'
        $text | Should -Match '(?m)^    types: \[completed\]\r?$'
    }

    It 'routes <File> to its explicit <Mode> planner mode' -TestCases @(
        @{ File = 'standard-validation.yml'; Mode = 'validation' }
        @{ File = 'full-deep-validation.yml'; Mode = 'full' }
        @{ File = 'selected-deep-validation.yml'; Mode = 'selected' }
    ) {
        param($File, $Mode)
        $text = Get-Content -LiteralPath (Join-Path $root ".github\workflows\$File") -Raw
        $text | Should -MatchExactly "(?m)^          ./scripts/scheduled/Invoke-ScheduledPlan.ps1 -Mode $Mode\r?$"
    }

    It 'shares a concurrency key with the standard validation close companion' {
        $cancel = Get-Content -LiteralPath (Join-Path $root '.github\workflows\cancel-standard-validation.yml') -Raw
        $expected = '  group: standard-validation-${{ github.head_ref || github.ref }}'
        foreach ($text in @($workflow, $cancel)) {
            [regex]::Match($text, '(?m)^  group: .+\r?$').Value.TrimEnd("`r") | Should -BeExactly $expected
        }
        $cancel | Should -Match '(?m)^    types: \[closed\]\r?$'
    }

    It 'keeps Deep checks as a reusable execution helper, not a manual entry point' {
        $text = Get-Content -LiteralPath (Join-Path $root '.github\workflows\deep-checks.yml') -Raw
        $events = ($text -split '(?m)^on:\r?$', 2)[1] -split '(?m)^[a-z]', 2
        @([regex]::Matches($events[0], '(?m)^  ([\w_]+):\r?$') |
            ForEach-Object { $_.Groups[1].Value }) | Should -Be @('workflow_call')
    }

    It 'retains every shallow job while removing ordinary deep jobs' {
        $expected = @('scheduled-context', 'scheduled-repair-checks', 'scheduled-repair-gate',
            'scheduled-version-check', 'delta', 'test-scripts', 'validate-workflows',
            'validate-versions', 'semver-checks', 'format-check', 'default-features-check',
            'check-external-types', 'clippy-dev', 'test-x64', 'test-docs', 'docs', 'test-arm',
            'machete', 'clippy-release', 'build-release', 'check-frozen', 'run-examples', 'hack',
            'test-azurite', 'test-azure', 'test-azure-gh', 'coverage-notify', 'required-checks', 'alert')
        @($jobs.Keys | Sort-Object) | Should -Be @($expected | Sort-Object)
        $workflow | Should -Not -Match '(?m)^\s+run:.*\b(miri|miri-harder|mutants|careful)\b'
        $jobs['scheduled-repair-checks'] | Should -Match '(?m)^    uses: \./\.github/workflows/deep-checks\.yml\r?$'
        $jobs['scheduled-repair-checks'] | Should -Match "(?m)^    if: needs\.scheduled-context\.outputs\.run == 'true'\r?$"
    }

    It 'keeps the required-checks and alert dependency sets complete' {
        $required = @([regex]::Matches($jobs['required-checks'], '(?m)^      - ([\w-]+)\r?$') |
            ForEach-Object { $_.Groups[1].Value })
        $alerts = @([regex]::Matches($jobs.alert, '(?m)^      - ([\w-]+)\r?$') |
            ForEach-Object { $_.Groups[1].Value })
        $expected = @($jobs.Keys | Where-Object {
                $_ -notin @('required-checks', 'alert', 'coverage-notify',
                    'scheduled-repair-checks', 'scheduled-version-check')
            } | Sort-Object)
        @($required | Sort-Object) | Should -Be $expected
        @($alerts | Sort-Object) | Should -Be @(($expected + 'coverage-notify') | Sort-Object)
        $jobs['required-checks'] | Should -Match '(?m)^    name: required-checks\r?$'
        $jobs['required-checks'] | Should -Match '(?m)^    if: always\(\)\r?$'
        $jobs['required-checks'] | Should -Match '(?m)^          MUST_SUCCEED_JOBS:.*\bscheduled-context\b.*\bscheduled-repair-gate\b'
        $jobs['scheduled-repair-gate'] | Should -Match '(?m)^    if: always\(\)\r?$'
        $jobs['scheduled-repair-gate'] | Should -Match '(?m)^    needs: \[scheduled-context, scheduled-repair-checks, scheduled-version-check\]\r?$'
    }
}

Describe 'Hosted planning authorization' {
    BeforeEach {
        $savedEnvironment = @{}
        foreach ($name in @('GITHUB_RUN_ID', 'GITHUB_RUN_ATTEMPT', 'GITHUB_RUN_NUMBER', 'GITHUB_OUTPUT',
                'GITHUB_REF', 'GITHUB_EVENT_NAME', 'GITHUB_STEP_SUMMARY')) {
            $savedEnvironment[$name] = [Environment]::GetEnvironmentVariable($name)
        }
        $env:GITHUB_RUN_ID = '10'
        $env:GITHUB_RUN_ATTEMPT = '1'
        $env:GITHUB_RUN_NUMBER = '5'
        $env:GITHUB_REF = 'refs/heads/main'
        $env:GITHUB_EVENT_NAME = 'schedule'
        $env:GITHUB_STEP_SUMMARY = Join-Path $TestDrive 'summary.md'
        Set-Content -LiteralPath $env:GITHUB_STEP_SUMMARY -Value '' -NoNewline
        $env:GITHUB_OUTPUT = Join-Path $TestDrive 'outputs'
        Set-Content -LiteralPath $env:GITHUB_OUTPUT -Value '' -NoNewline
        $eventPath = Join-Path $TestDrive 'event.json'
        @{ repository = @{ id = 850321188 } } | ConvertTo-Json | Set-Content $eventPath
        Mock git -ModuleName ScheduledWorkflow { 'a' * 40 }
        Mock Write-Verbose -ModuleName ScheduledWorkflow {}
        Mock Get-ScheduledContractDigest -ModuleName ScheduledWorkflow { 'c' * 64 }
        Mock Invoke-ScheduledReadApi -ModuleName ScheduledWorkflow { throw 'Unexpected GitHub request.' }
        Mock Get-ScheduledCoverageIndex -ModuleName ScheduledWorkflow { $null }
        Mock Get-ScheduledConfirmationScope -ModuleName ScheduledWorkflow { throw 'Unexpected confirmation request.' }
    }

    AfterEach {
        foreach ($name in $savedEnvironment.Keys) {
            Set-Item -LiteralPath "Env:$name" -Value $savedEnvironment[$name]
        }
    }

    It 'keeps recurring execution disabled when explicitly configured' {
        $script:disabledPolicy = Get-ScheduledPolicy
        $disabledPolicy.rollout.hosted_execution_enabled = $false
        $disabledPolicy.rollout.reporting_enabled = $false
        Mock Get-ScheduledPolicy -ModuleName ScheduledWorkflow { $disabledPolicy }
        $plan = Invoke-ScheduledPlanning -Mode full -EventPath $eventPath `
            -OutputDirectory (Join-Path $TestDrive 'plan') -Now '2026-09-08T12:00:00Z'
        $plan.decision.run | Should -BeFalse
        Should -Invoke Get-ScheduledCoverageIndex -ModuleName ScheduledWorkflow -Times 0 -Exactly
    }

    It 'plans the full nightly suite with enabled defaults and no baseline or Local enrollment' {
        $plan = Invoke-ScheduledPlanning -Mode full -EventPath $eventPath `
            -OutputDirectory (Join-Path $TestDrive 'first-nightly') -Now '2026-09-08T12:00:00Z'
        $plan.decision.run | Should -BeTrue
        $plan.manifest.scope | Should -Be full
        $plan.manifest.checks.Count | Should -Be 32
        @($plan.manifest.checks.kind | Sort-Object -Unique) | Should -Be @('careful', 'miri', 'miri-many', 'mutants')
        Should -Invoke Get-ScheduledCoverageIndex -ModuleName ScheduledWorkflow -Times 1
        Should -Invoke Get-ScheduledConfirmationScope -ModuleName ScheduledWorkflow -Times 0
    }

    It 'runs fresh full manual checks with recurring execution=<Execution> and no cache prerequisite' -TestCases @(
        @{ Execution = $false }, @{ Execution = $true }
    ) {
        param($Execution)
        $script:manualPolicy = Get-ScheduledPolicy
        $manualPolicy.rollout.hosted_execution_enabled = $Execution
        Mock Get-ScheduledPolicy -ModuleName ScheduledWorkflow { $manualPolicy }
        $env:GITHUB_EVENT_NAME = 'workflow_dispatch'
        Mock Get-ScheduledCoverageIndex -ModuleName ScheduledWorkflow { throw 'Unrelated broken coverage index.' }
        $plan = Invoke-ScheduledPlanning -Mode full -EventPath $eventPath `
            -OutputDirectory (Join-Path $TestDrive 'plan') -Now '2026-09-08T12:00:00Z'
        $plan.decision.run | Should -BeTrue
        $plan.manifest.scope | Should -Be full
        $plan.manifest.checks.Count | Should -Be 32
        $plan.confirmations | Should -BeNullOrEmpty
        $plan.repairs | Should -BeNullOrEmpty
        Get-Content -LiteralPath $env:GITHUB_STEP_SUMMARY -Raw | Should -Match '^## Full deep validation'
        Should -Invoke Write-Verbose -ModuleName ScheduledWorkflow -Times 1 -ParameterFilter {
            $Message -cmatch '^Planning Full deep validation '
        }
        Should -Invoke Get-ScheduledCoverageIndex -ModuleName ScheduledWorkflow -Times 0 -Exactly
    }

    It 'retains automatic unchanged-source reuse when recurring execution is enabled' {
        $script:enabledPolicy = Get-ScheduledPolicy
        $enabledPolicy.rollout.hosted_execution_enabled = $true
        Mock Get-ScheduledPolicy -ModuleName ScheduledWorkflow { $enabledPolicy }
        $manifest = Get-ScheduledCheckManifest -SourceSha ('a' * 40) -ControllerSha ('a' * 40) -ContractDigest ('c' * 64)
        $script:automaticCoverage = @{
            schema_version = 1; invalidation = $null
            receipt = @{
                source_sha = 'a' * 40; check_contract_digest = 'c' * 64; scope = 'full'
                complete = $true; successful = $true; manifest = $manifest
                run_id = 9; run_attempt = 1; run_number = 4; completed_at = '2026-09-08T11:00:00Z'
            }
        }
        Mock Get-ScheduledCoverageIndex -ModuleName ScheduledWorkflow { $automaticCoverage }
        Mock Invoke-ScheduledReadApi -ModuleName ScheduledWorkflow { @(@{ workflow_runs = @() }) }
        $plan = Invoke-ScheduledPlanning -Mode full -EventPath $eventPath `
            -OutputDirectory (Join-Path $TestDrive 'plan') -Now '2026-09-08T12:00:00Z'
        $plan.decision.run | Should -BeFalse
        $plan.decision.reason | Should -Be not-run-unchanged
        Should -Invoke Get-ScheduledCoverageIndex -ModuleName ScheduledWorkflow -Times 1
    }

    It 'fails a non-main workflow selection instead of silently skipping <Mode>' -TestCases @(
        @{ Mode = 'full' }, @{ Mode = 'selected' }
    ) {
        param($Mode)
        $env:GITHUB_EVENT_NAME = 'workflow_dispatch'
        $env:GITHUB_REF = 'refs/heads/topic'
        $rejectedPlan = Join-Path $TestDrive "rejected-$Mode"
        { Invoke-ScheduledPlanning -Mode $Mode -EventPath $eventPath `
                -OutputDirectory $rejectedPlan } | Should -Throw
        Test-Path -LiteralPath $rejectedPlan | Should -BeFalse
    }

    Describe 'Manual crate checks with the actual default policy' {
        BeforeEach {
            $env:GITHUB_EVENT_NAME = 'workflow_dispatch'
            $script:manualEvent = @{
                repository = @{ id = 850321188 }
                inputs = @{ source_sha = ''; check_ids = 'miri-ubuntu-latest'; packages = 'cpulist' }
            }
            $policy = Get-ScheduledPolicy
            $policy.repair.allowed_packages | Should -BeNullOrEmpty
            $policy.local.allowed_packages | Should -BeNullOrEmpty
            $policy.local.enrolled_machine_id | Should -BeNullOrEmpty
            $policy.rollout.hosted_execution_enabled | Should -BeTrue
            $policy.rollout.reporting_enabled | Should -BeTrue
            @($policy.rollout.prerequisites.Values | Where-Object { $_ }).Count | Should -Be 0
        }

        It 'plans cpulist Miri at <Source> independently of repair permissions' -TestCases @(
            @{ Source = ''; Expected = 'a' * 40 },
            @{ Source = 'a' * 40; Expected = 'a' * 40 },
            @{ Source = 'b' * 40; Expected = 'b' * 40 }
        ) {
            param($Source, $Expected)
            $manualEvent.inputs.source_sha = $Source
            $manualEvent | ConvertTo-Json -Depth 10 | Set-Content $eventPath
            Mock Invoke-ScheduledReadApi -ModuleName ScheduledWorkflow {
                param($Endpoint)
                @{ sha = ($Endpoint -split '/')[-1] }
            }
            $plan = Invoke-ScheduledPlanning -Mode selected -EventPath $eventPath `
                -OutputDirectory (Join-Path $TestDrive 'plan')
            $plan.decision.run | Should -BeTrue
            $plan.manifest.source_sha | Should -BeExactly $Expected
            $plan.manifest.controller_sha | Should -BeExactly ('a' * 40)
            $plan.manifest.scope | Should -Be confirmation
            @($plan.manifest.checks.id) | Should -Be @('miri-ubuntu-latest')
            $plan.manifest.checks[0].packages | Should -Be @('cpulist')
            $plan.managed | Should -BeFalse
            $plan.confirmations | Should -BeNullOrEmpty
            $plan.repairs | Should -BeNullOrEmpty
            Get-Content -LiteralPath $env:GITHUB_OUTPUT | Should -Contain 'run=true'
            Get-Content -LiteralPath $env:GITHUB_STEP_SUMMARY -Raw | Should -Match $Expected
            Get-Content -LiteralPath $env:GITHUB_STEP_SUMMARY -Raw | Should -Match '^## Selected deep validation'
            Should -Invoke Write-Verbose -ModuleName ScheduledWorkflow -Times 1 -ParameterFilter {
                $Message -cmatch '^Planning Selected deep validation ' -and $Message -cmatch 'manual-checks'
            }
            Should -Invoke Get-ScheduledConfirmationScope -ModuleName ScheduledWorkflow -Times 0
            Should -Invoke Get-ScheduledCoverageIndex -ModuleName ScheduledWorkflow -Times 0
        }

        It 'accepts spaces around comma-separated names' {
            $manualEvent.inputs.packages = ' cpulist , events '
            $manualEvent.inputs.check_ids = ' miri-ubuntu-latest , careful-windows-latest '
            $manualEvent | ConvertTo-Json -Depth 10 | Set-Content $eventPath
            $plan = Invoke-ScheduledPlanning -Mode selected -EventPath $eventPath `
                -OutputDirectory (Join-Path $TestDrive 'plan')
            $plan.manifest.checks.Count | Should -Be 2
            $plan.manifest.checks[0].packages | Should -Be @('cpulist', 'events')
        }

        It 'uses the captured main commit when an API dispatch omits the optional source field' {
            $manualEvent.inputs.Remove('source_sha')
            $manualEvent | ConvertTo-Json -Depth 10 | Set-Content $eventPath
            $plan = Invoke-ScheduledPlanning -Mode selected -EventPath $eventPath `
                -OutputDirectory (Join-Path $TestDrive 'plan')
            $plan.decision.run | Should -BeTrue
            $plan.manifest.source_sha | Should -BeExactly ('a' * 40)
            Should -Invoke Invoke-ScheduledReadApi -ModuleName ScheduledWorkflow -Times 0
        }

        It 'rejects invalid or incompatible requested names <Packages> / <Checks>' -TestCases @(
            @{ Packages = ''; Checks = 'miri-ubuntu-latest' },
            @{ Packages = 'cpulist'; Checks = '' },
            @{ Packages = 'cpulist,'; Checks = 'miri-ubuntu-latest' },
            @{ Packages = 'cpulist'; Checks = 'miri-ubuntu-latest, ' },
            @{ Packages = '*'; Checks = 'miri-ubuntu-latest' },
            @{ Packages = '--workspace'; Checks = 'mutants-ubuntu-latest-1' },
            @{ Packages = 'cpulist'; Checks = 'miri' },
            @{ Packages = 'cpulist'; Checks = 'miri-many-events-1' },
            @{ Packages = 'events,cpulist'; Checks = 'miri-many-events-1' }
        ) {
            param($Packages, $Checks)
            $manualEvent.inputs.packages = $Packages
            $manualEvent.inputs.check_ids = $Checks
            $manualEvent | ConvertTo-Json -Depth 10 | Set-Content $eventPath
            { Invoke-ScheduledPlanning -Mode selected -EventPath $eventPath `
                    -OutputDirectory (Join-Path $TestDrive 'plan') } | Should -Throw
            Get-Content -LiteralPath $env:GITHUB_OUTPUT | Should -Not -Contain 'run=true'
        }

        It 'rejects a symbolic or unresolved source before starting checks' -TestCases @(
            @{ Source = 'main' }, @{ Source = 'd' * 40 }
        ) {
            param($Source)
            $manualEvent.inputs.source_sha = $Source
            $manualEvent | ConvertTo-Json -Depth 10 | Set-Content $eventPath
            Mock Invoke-ScheduledReadApi -ModuleName ScheduledWorkflow { @{ sha = 'e' * 40 } }
            { Invoke-ScheduledPlanning -Mode selected -EventPath $eventPath `
                    -OutputDirectory (Join-Path $TestDrive 'plan') } | Should -Throw
        }
    }

    It 'keeps automatic merged repair confirmation disabled' {
        $plan = Invoke-ScheduledPlanning -Mode selected -EventPath $eventPath `
            -OutputDirectory (Join-Path $TestDrive 'plan') -Now '2026-09-08T12:00:00Z'
        $plan.decision.run | Should -BeFalse
        Should -Invoke Get-ScheduledConfirmationScope -ModuleName ScheduledWorkflow -Times 0 -Exactly
    }

    It 'plans an ordinary PR without deep execution and emits only execution and scope outputs' {
        @{
            repository = @{ id = 850321188 }
            pull_request = @{ number = 42; head = @{ sha = 'b' * 40 } }
        } | ConvertTo-Json -Depth 10 | Set-Content $eventPath
        Mock Invoke-ScheduledReadApi -ModuleName ScheduledWorkflow { @{ head = @{ sha = 'b' * 40 } } }
        Mock Get-ScheduledPullRequestScope -ModuleName ScheduledWorkflow { @{ managed = $false } }
        $plan = Invoke-ScheduledPlanning -Mode validation -EventPath $eventPath `
            -OutputDirectory (Join-Path $TestDrive 'plan') -Now '2026-09-08T12:00:00Z'
        $plan.decision.run | Should -BeFalse
        $plan.managed | Should -BeFalse
        $plan.manifest.source_sha | Should -BeExactly ('b' * 40)
        Should -Invoke Write-Verbose -ModuleName ScheduledWorkflow -Times 1 -ParameterFilter {
            $Message -cmatch '^Planning Standard validation '
        }
        $outputNames = @(Get-Content -LiteralPath $env:GITHUB_OUTPUT |
            ForEach-Object { ($_ -split '=', 2)[0] })
        @($outputNames | Sort-Object) |
            Should -Be @('controller_sha', 'managed', 'manifest', 'matrix', 'run', 'source_sha')
    }

    Describe 'Managed repair publication' {
        BeforeEach {
            $script:repairPolicy = Get-Content -LiteralPath (Join-Path $PSScriptRoot 'policy.json') -Raw |
                ConvertFrom-Json -AsHashtable
            foreach ($name in @('benchmark_exclusion', 'azure_policy', 'native_app_canary')) {
                $repairPolicy.rollout.prerequisites[$name] = $true
            }
            @{
                repository = @{ id = 850321188 }
                pull_request = @{ number = 42; head = @{ sha = 'b' * 40 } }
            } | ConvertTo-Json -Depth 10 | Set-Content $eventPath
            Mock Get-ScheduledPolicy -ModuleName ScheduledWorkflow { $repairPolicy }
            Mock Invoke-ScheduledReadApi -ModuleName ScheduledWorkflow { @{ head = @{ sha = 'b' * 40 } } }
            Mock Get-ScheduledPullRequestScope -ModuleName ScheduledWorkflow {
                @{ managed = $true; packages = @('events_once'); check_ids = @('miri-many-events_once-1') }
            }
            Mock Assert-ScheduledPullRequestChange -ModuleName ScheduledWorkflow {}
        }

        It 'still requires the relevant deep check independently of recurring execution authorization' {
            $plan = Invoke-ScheduledPlanning -Mode validation -EventPath $eventPath `
                -OutputDirectory (Join-Path $TestDrive 'plan') -Now '2026-09-08T12:00:00Z'
            $plan.decision.run | Should -BeTrue
            $plan.managed | Should -BeTrue
            $plan.manifest.scope | Should -Be repair
            @($plan.manifest.checks.id) | Should -Be @('miri-many-events_once-1')
            $plan.manifest.checks[0].packages | Should -Be @('events_once')
            Should -Invoke Assert-ScheduledPullRequestChange -ModuleName ScheduledWorkflow -Times 1 -Exactly
        }

        It 'rejects publication without <Name>' -TestCases @(
            @{ Name = 'benchmark_exclusion' }
            @{ Name = 'azure_policy' }
            @{ Name = 'native_app_canary' }
        ) {
            param($Name)
            $repairPolicy.rollout.prerequisites[$Name] = $false
            {
                Invoke-ScheduledPlanning -Mode validation -EventPath $eventPath `
                    -OutputDirectory (Join-Path $TestDrive 'plan') -Now '2026-09-08T12:00:00Z'
            } | Should -Throw
        }
    }
}
