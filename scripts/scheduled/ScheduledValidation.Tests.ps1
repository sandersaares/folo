#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
# Protects fixed local validation depth and the hosted validation graph without executing any
# checker. Planner tests isolate GitHub reads so disabled execution, explicit canaries and
# managed repair gates remain independent of ordinary shallow CI.
# Ref: ../../.github/workflows/design.md#shallow-and-deep-validation.
BeforeAll {
    $root = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
    $script:recipes = (just --justfile (Join-Path $root 'justfile') --dump --dump-format json |
        ConvertFrom-Json -AsHashtable).recipes
    $workflow = Get-Content -LiteralPath (Join-Path $root '.github\workflows\validation.yml') -Raw
    $jobDefinitions = ($workflow -split '(?m)^jobs:\r?$', 2)[1]
    $jobs = @{}
    foreach ($match in [regex]::Matches($jobDefinitions, '(?ms)^  ([\w-]+):\r?\n(.*?)(?=^  [\w-]+:|\z)')) {
        $jobs[$match.Groups[1].Value] = $match.Groups[2].Value
    }
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
        foreach ($name in @('GITHUB_RUN_ID', 'GITHUB_RUN_ATTEMPT', 'GITHUB_RUN_NUMBER', 'GITHUB_OUTPUT')) {
            $savedEnvironment[$name] = [Environment]::GetEnvironmentVariable($name)
        }
        $env:GITHUB_RUN_ID = '10'
        $env:GITHUB_RUN_ATTEMPT = '1'
        $env:GITHUB_RUN_NUMBER = '5'
        $env:GITHUB_OUTPUT = Join-Path $TestDrive 'outputs'
        Set-Content -LiteralPath $env:GITHUB_OUTPUT -Value '' -NoNewline
        $eventPath = Join-Path $TestDrive 'event.json'
        @{ repository = @{ id = 850321188 } } | ConvertTo-Json | Set-Content $eventPath
        Mock git -ModuleName ScheduledWorkflow { 'a' * 40 }
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

    It 'does not authorize recurring execution merely because a recheck is forced' {
        $plan = Invoke-ScheduledPlanning -Mode scheduled -EventPath $eventPath `
            -OutputDirectory (Join-Path $TestDrive 'plan') -Now '2026-09-08T12:00:00Z' -Force
        $plan.decision.run | Should -BeFalse
        $plan.canary | Should -BeFalse
        Should -Invoke Get-ScheduledCoverageIndex -ModuleName ScheduledWorkflow -Times 0 -Exactly
    }

    It 'retains explicit read-only canary execution while recurring execution is disabled' {
        $plan = Invoke-ScheduledPlanning -Mode scheduled -EventPath $eventPath `
            -OutputDirectory (Join-Path $TestDrive 'plan') -Now '2026-09-08T12:00:00Z' -Canary -Force
        $plan.decision.run | Should -BeTrue
        $plan.canary | Should -BeTrue
        $plan.manifest.scope | Should -Be full
        Should -Invoke Get-ScheduledCoverageIndex -ModuleName ScheduledWorkflow -Times 1 -Exactly
    }

    It 'keeps automatic merged repair confirmation disabled' {
        $plan = Invoke-ScheduledPlanning -Mode verify -EventPath $eventPath `
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
