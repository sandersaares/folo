#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }

# Protects Standard validation's change domains, whole-candidate Git comparisons and explicit
# no-work results. Native Git fixtures cover deletions/renames without GitHub or a Rust setup.
BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'ValidationPlan.psm1') -Force
    $script:allDomains = @('analyzer', 'bench-history', 'book', 'build', 'release', 'scheduled', 'setup', 'utility')
    function ConvertTo-PlanJson($Plan) { ConvertTo-Json -InputObject $Plan -Compress }
}

Describe 'Non-Cargo change domains' {
    It 'does not select tooling for ordinary Rust or documentation changes' {
        $plan = Get-ValidationPlan -ChangedPath @('packages/events_once/src/lib.rs', 'README.md',
            'docs/testing.md', '.github/workflows/design.md', 'Cargo.lock')
        $plan.workflows | Should -BeFalse
        $plan.script_analysis | Should -BeFalse
        $plan.script_domains | Should -BeNullOrEmpty
        ConvertTo-PlanJson $plan | Should -Match '"script_domains":\[\]'
    }

    It 'selects the owner and its consumers for <Path>' -ForEach @(
        @{ Path = 'scripts/book/BookSite.psm1'; Domains = @('book'); Analysis = $true; Workflows = $false },
        @{ Path = 'scripts/book/BookSite.Tests.ps1'; Domains = @('book'); Analysis = $true; Workflows = $false },
        @{ Path = 'scripts/bench-history/fixtures/result.json'; Domains = @('bench-history'); Analysis = $false; Workflows = $false },
        @{ Path = 'scripts/build/Miri.psm1'; Domains = @('build', 'scheduled'); Analysis = $true; Workflows = $false },
        @{ Path = 'scripts/build/CargoExecutable.psm1'; Domains = @('build', 'release', 'scheduled'); Analysis = $true; Workflows = $false },
        @{ Path = 'scripts/release/ReleasePlan.psm1'; Domains = @('release', 'scheduled'); Analysis = $true; Workflows = $false },
        @{ Path = 'PSScriptAnalyzerSettings.psd1'; Domains = @('analyzer'); Analysis = $true; Workflows = $false },
        @{ Path = '.github/workflows/release.yml'; Domains = @('release', 'scheduled'); Analysis = $false; Workflows = $true },
        @{ Path = '.github/actionlint.yaml'; Domains = @('scheduled'); Analysis = $false; Workflows = $true },
        @{ Path = '.github/actions/setup-workflow-lint/action.yml'; Domains = @('scheduled'); Analysis = $false; Workflows = $true },
        @{ Path = 'justfiles/just_bench_history.just'; Domains = @('bench-history'); Analysis = $false; Workflows = $false },
        @{ Path = 'justfiles/just_release.just'; Domains = @('release', 'scheduled'); Analysis = $false; Workflows = $false },
        @{ Path = 'justfiles/just_quality.just'; Domains = @('build', 'scheduled'); Analysis = $true; Workflows = $true },
        @{ Path = '.cargo/mutants.toml'; Domains = @('build', 'scheduled'); Analysis = $false; Workflows = $false },
        @{ Path = 'Cargo.toml'; Domains = @('scheduled'); Analysis = $false; Workflows = $false },
        @{ Path = 'packages/cpulist/Cargo.toml'; Domains = @('scheduled'); Analysis = $false; Workflows = $false },
        @{ Path = 'packages/scheduled-mutation-config/dependency-contract.json'; Domains = @('scheduled'); Analysis = $false; Workflows = $false }
    ) {
        $plan = Get-ValidationPlan -ChangedPath @($Path)
        $plan.script_domains | Should -Be $Domains
        $plan.script_analysis | Should -Be $Analysis
        $plan.workflows | Should -Be $Workflows
    }

    It 'selects every tooling check for shared input <_>' -ForEach @(
        'scripts/build/ValidationPlan.psm1', 'scripts/build/ValidationPlan.Tests.ps1',
        'scripts/build/RequiredChecks.psm1', 'scripts/build/RequiredChecks.Tests.ps1',
        'scripts/setup/install-actionlint.ps1', 'scripts/setup/install-shellcheck.ps1',
        'scripts/setup/RustToolchain.psm1', 'scripts/utility/Retry.psm1',
        '.github/actions/setup-environment/action.yml', 'justfile',
        'justfiles/just_setup.just', 'justfiles/just_testing.just', 'constants.env',
        'rust-toolchain.toml', '.gitattributes', '.gitconfig'
    ) {
        $plan = Get-ValidationPlan -ChangedPath @($_)
        $plan.workflows | Should -BeTrue
        $plan.script_analysis | Should -BeTrue
        $plan.script_domains | Should -Be $allDomains
    }

    It 'does not silently exclude unfamiliar scripts or recipes' -ForEach @(
        'scripts/new-domain/New.Tests.ps1', 'scripts/standalone.ps1', 'justfiles/new.just'
    ) {
        (Get-ValidationPlan -ChangedPath @($_)).script_domains | Should -Be $allDomains
    }

    It 'unions and deduplicates domains' {
        $plan = Get-ValidationPlan -ChangedPath @('scripts/book/BookSite.psm1',
            'scripts/book/BookSite.Tests.ps1', 'scripts/build/Miri.psm1')
        $plan.script_domains | Should -Be @('book', 'build', 'scheduled')
    }

    It 'runs all tooling on main without needing a comparison' {
        $plan = Get-ValidationWorkflowPlan -EventName push -EventData @{ ref = 'refs/heads/main' }
        $plan.workflows | Should -BeTrue
        $plan.script_analysis | Should -BeTrue
        $plan.script_domains | Should -Be $allDomains
    }

    It 'rejects unsupported workflow events and non-main pushes' {
        { Get-ValidationWorkflowPlan -EventName workflow_dispatch -EventData @{} } | Should -Throw
        { Get-ValidationWorkflowPlan -EventName push -EventData @{ ref = 'refs/heads/feature' } } | Should -Throw
    }
}

Describe 'Cargo helper integration selection' {
    It 'adds scheduled tests for affected helper <_>' -ForEach @(
        'scheduled-mutation-config', 'scheduled-run-record', 'scheduled-triage-record'
    ) {
        $plan = ConvertTo-PlanJson (Get-ValidationPlan -ChangedPath @('scripts/book/BookSite.psm1'))
        $packages = ConvertTo-Json -InputObject @($_) -Compress
        @(Get-ValidationScriptDomain -PlanJson $plan -AffectedPackageJson $packages) |
            Should -Be @('book', 'scheduled')
    }

    It 'adds release and dependent tests for affected helper <_>' -ForEach @(
        'cargo-release-plan', 'release-target-check'
    ) {
        $plan = ConvertTo-PlanJson (Get-ValidationPlan -ChangedPath @('scripts/book/BookSite.psm1'))
        $packages = ConvertTo-Json -InputObject @($_) -Compress
        @(Get-ValidationScriptDomain -PlanJson $plan -AffectedPackageJson $packages) |
            Should -Be @('book', 'release', 'scheduled')
    }

    It 'does not select scripts for unrelated Cargo dependency impact' {
        $plan = ConvertTo-PlanJson (Get-ValidationPlan -ChangedPath @('Cargo.lock'))
        @(Get-ValidationScriptDomain -PlanJson $plan -AffectedPackageJson '["events_once"]') |
            Should -BeNullOrEmpty
    }

    It 'preserves path-selected scripts when Cargo selects nothing' {
        $plan = ConvertTo-PlanJson (Get-ValidationPlan -ChangedPath @('scripts/book/BookSite.psm1'))
        @(Get-ValidationScriptDomain -PlanJson $plan -AffectedPackageJson '[]') | Should -Be @('book')
    }

    It 'rejects missing or malformed package outputs' -ForEach @('', 'null', '{}', '"crate"', '[1]') {
        $plan = ConvertTo-PlanJson (Get-ValidationPlan -ChangedPath @())
        { Get-ValidationScriptDomain -PlanJson $plan -AffectedPackageJson $_ } | Should -Throw
    }

    It 'rejects missing or malformed path plans' -ForEach @(
        '', 'null', '{}', '{"workflows":false,"script_analysis":false}',
        '{"workflows":"false","script_analysis":false,"script_domains":[]}',
        '{"workflows":false,"script_analysis":false,"script_domains":"book"}',
        '{"workflows":false,"script_analysis":false,"script_domains":["unknown"]}'
    ) {
        { Read-ValidationPlan -Json $_ } | Should -Throw
    }
}

Describe 'Explicit Pester scope' {
    It 'retains the full local default' {
        Get-ScriptTestPath -Root $TestDrive | Should -Be $TestDrive
    }

    It 'resolves and deduplicates selected suites and rejects unknown or empty domains' {
        $path = Join-Path $TestDrive 'book'
        $null = New-Item -ItemType Directory -Path $path -Force
        Set-Content -LiteralPath (Join-Path $path 'Book.Tests.ps1') -Value '# fixture'
        @(Get-ScriptTestPath -Domains 'book book' -Root $TestDrive) | Should -Be @($path)
        { Get-ScriptTestPath -Domains 'unknown' -Root $TestDrive } | Should -Throw
        { Get-ScriptTestPath -Domains 'release' -Root $TestDrive } | Should -Throw
        $null = New-Item -ItemType Directory -Path (Join-Path $TestDrive 'release') -Force
        { Get-ScriptTestPath -Domains 'release' -Root $TestDrive } | Should -Throw
    }

    It 'covers every current test directory with the full selection' {
        $root = Join-Path $PSScriptRoot '..'
        $actual = @(Get-ChildItem -LiteralPath $root -Directory | Where-Object {
                @(Get-ChildItem -LiteralPath $_.FullName -Filter '*.Tests.ps1' -Recurse -File).Count -gt 0
            } | ForEach-Object { $_.Name } | Sort-Object)
        $actual | Should -Be $allDomains
        @(Get-ScriptTestPath -Domains ($allDomains -join ' ')).Count | Should -Be $allDomains.Count
    }
}

Describe 'Complete Git change sets' {
    BeforeEach {
        $repo = Join-Path $TestDrive ([guid]::NewGuid().ToString('N'))
        $null = New-Item -ItemType Directory -Path $repo
        Push-Location $repo
        git init --quiet --initial-branch=main
        git config user.name 'Validation fixture'
        git config user.email 'fixture@example.invalid'
        git config commit.gpgsign false
        $null = New-Item -ItemType Directory -Path 'scripts/book', 'scripts/build', 'scripts/setup'
        Set-Content -LiteralPath 'scripts/book/old name.ps1' -Value '# book fixture'
        Set-Content -LiteralPath 'README.md' -Value 'fixture'
        git add .
        git commit --quiet -m 'Fixture baseline'
        $base = git rev-parse HEAD
        function Save-FixtureCommit {
            git add --all
            git commit --quiet -m 'Fixture change'
            return git rev-parse HEAD
        }
        function Get-FixturePlan([string] $Kind, [string] $Base, [string] $Head) {
            $eventData = if ($Kind -ceq 'pull_request') {
                @{ pull_request = @{ base = @{ sha = $Base }; head = @{ sha = $Head } } }
            } else {
                @{ merge_group = @{ base_sha = $Base; head_sha = $Head } }
            }
            Get-ValidationWorkflowPlan -EventName $Kind -EventData $eventData
        }
    }
    AfterEach { Pop-Location }

    It 'includes early PR commits even when the latest commit only touches documentation' {
        Add-Content -LiteralPath 'scripts/book/old name.ps1' -Value '# changed'
        $null = Save-FixtureCommit
        Add-Content -LiteralPath 'README.md' -Value 'later'
        $head = Save-FixtureCommit
        (Get-FixturePlan pull_request $base $head).script_domains | Should -Be @('book')
    }

    It 'excludes changes made only on the advanced PR base branch' {
        Add-Content -LiteralPath 'scripts/book/old name.ps1' -Value '# changed'
        $head = Save-FixtureCommit
        git switch --quiet -c advanced-base $base
        Set-Content -LiteralPath 'scripts/setup/unrelated.ps1' -Value '# base-only'
        $newBase = Save-FixtureCommit
        (Get-FixturePlan pull_request $newBase $head).script_domains | Should -Be @('book')
    }

    It 'includes both domains of a rename and the entire merge group' {
        Move-Item -LiteralPath 'scripts/book/old name.ps1' -Destination 'scripts/build/new name.ps1'
        $null = Save-FixtureCommit
        Add-Content -LiteralPath 'README.md' -Value 'another queued change'
        $head = Save-FixtureCommit
        (Get-FixturePlan merge_group $base $head).script_domains | Should -Be @('book', 'build', 'scheduled')
    }

    It 'retains deletions and emits an explicit empty plan for identical commits' {
        Remove-Item -LiteralPath 'scripts/book/old name.ps1'
        $head = Save-FixtureCommit
        (Get-FixturePlan pull_request $base $head).script_domains | Should -Be @('book')
        $empty = Get-FixturePlan merge_group $head $head
        $empty.script_domains | Should -BeNullOrEmpty
        $empty.script_analysis | Should -BeFalse
        $empty.workflows | Should -BeFalse
    }

    It 'fails for missing or unavailable event revisions' {
        { Get-ValidationWorkflowPlan -EventName pull_request -EventData @{} } | Should -Throw
        { Get-FixturePlan merge_group $base ('f' * 40) } | Should -Throw
    }
}
