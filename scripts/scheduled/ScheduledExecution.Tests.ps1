#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }

# Drives the capture wrapper through real Just, with harmless stand-in recipes. Checker
# behavior belongs to the shared recipes; these tests protect routing, logs and exit status.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true

BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'ScheduledPlan.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'ScheduledExecution.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'ScheduledExecutionDiagnostics.psm1') -Force
    $script:fixtures = Join-Path $PSScriptRoot 'fixtures\execution'
    $script:sourceSha = 'a' * 40
    $script:recipeRoot = Join-Path $TestDrive 'recipes'
    $null = New-Item -ItemType Directory -Path $recipeRoot
    Copy-Item -LiteralPath (Join-Path $fixtures 'justfile'), (Join-Path $fixtures 'Record-Recipe.ps1') -Destination $recipeRoot
}

Describe 'Shared recipe invocation' {
    BeforeEach {
        $script:output = Join-Path $TestDrive ("output space's " + [guid]::NewGuid())
        $script:environment = @{}
        foreach ($name in @('SCHEDULED_CAPTURE_PATH', 'SCHEDULED_TEST_EXIT', 'SCHEDULED_MUTATION_FIXTURE')) {
            $environment[$name] = [Environment]::GetEnvironmentVariable($name)
        }
        $env:SCHEDULED_CAPTURE_PATH = Join-Path $TestDrive 'invocation.json'
        $env:SCHEDULED_TEST_EXIT = '0'
        $env:SCHEDULED_MUTATION_FIXTURE = $null
    }

    AfterEach {
        foreach ($name in $environment.Keys) {
            [Environment]::SetEnvironmentVariable($name, $environment[$name])
        }
    }

    It 'runs the existing <recipe> recipe with the matrix scope' -ForEach @(
        @{ id = 'miri-ubuntu-latest'; recipe = 'miri'; packages = @('example', 'another'); shard = '' },
        @{ id = 'miri-many-events-2'; recipe = 'miri-harder'; packages = @('events'); shard = '2/2' },
        @{ id = 'mutants-windows-latest-2'; recipe = 'mutants'; packages = @('example'); shard = '2/8' },
        @{ id = 'careful-windows-latest'; recipe = 'careful'; packages = @(); shard = '' }
    ) {
        $check = @(Get-ScheduledCheck | Where-Object id -EQ $id)[0]
        $check.packages = $packages
        Invoke-ScheduledCheck -Check $check -SourceRoot $recipeRoot -OutputDirectory $output -SourceSha $sourceSha |
            Should -Be 0
        $invocation = Get-Content -LiteralPath $env:SCHEDULED_CAPTURE_PATH -Raw | ConvertFrom-Json
        $invocation.recipe | Should -Be $recipe
        $invocation.package | Should -Be ($packages -join ' ')
        if ($shard) { $invocation.shard | Should -Be $shard }
        if ($recipe -eq 'mutants') {
            $invocation.careful | Should -Be 'false'
            $invocation.output | Should -Be $output
        }
        $summary = Get-Content -LiteralPath (Join-Path $output 'summary.md') -Raw
        $summary | Should -Match "just .*'$recipe'"
        $summary | Should -Match $sourceSha
        $summary | Should -Match 'Final result: PASSED'
    }

    It 'preserves recipe failure <_> and includes its output' -ForEach @(1, 2, 3, 4) {
        $code = $_
        $env:SCHEDULED_TEST_EXIT = [string]$code
        $check = @(Get-ScheduledCheck | Where-Object recipe -EQ 'miri')[0]
        Invoke-ScheduledCheck -Check $check -SourceRoot $recipeRoot -OutputDirectory $output -SourceSha $sourceSha |
            Should -Be $code
        $summary = Get-Content -LiteralPath (Join-Path $output 'summary.md') -Raw
        $summary | Should -Match 'Recipe failure canary'
        $summary | Should -Match 'Final result: FAILED'
    }

    It 'reports an empty mutation shard without reconstructing a baseline' {
        $check = @(Get-ScheduledCheck | Where-Object recipe -EQ 'mutants')[0]
        Invoke-ScheduledCheck -Check $check -SourceRoot $recipeRoot -OutputDirectory $output -SourceSha $sourceSha |
            Should -Be 0
        Get-Content -LiteralPath (Join-Path $output 'summary.md') -Raw |
            Should -Match 'No mutants selected for this shard; no mutation tests or baseline were run'
    }

    It 'preserves a failed mutation recipe and renders its native findings' {
        $env:SCHEDULED_TEST_EXIT = '3'
        $env:SCHEDULED_MUTATION_FIXTURE = Join-Path $fixtures 'outcomes.json'
        $check = @(Get-ScheduledCheck | Where-Object recipe -EQ 'mutants')[0]
        Invoke-ScheduledCheck -Check $check -SourceRoot $recipeRoot -OutputDirectory $output -SourceSha $sourceSha |
            Should -Be 3
        $summary = Get-Content -LiteralPath (Join-Path $output 'summary.md') -Raw
        $summary | Should -Match 'MissedMutant'
        $summary | Should -Match 'Timeout'
    }

    It 'reports capture setup errors explicitly' {
        $check = @(Get-ScheduledCheck | Where-Object recipe -EQ 'miri')[0]
        Mock Invoke-CapturedProcess -ModuleName ScheduledExecution { throw 'Process start canary.' }
        Invoke-ScheduledCheck -Check $check -SourceRoot $recipeRoot -OutputDirectory $output -SourceSha $sourceSha |
            Should -Be 1
        Get-Content -LiteralPath (Join-Path $output 'summary.md') -Raw | Should -Match 'Process start canary'
    }
}

Describe 'Mutation diagnostic formatting' {
    BeforeEach {
        $script:output = Join-Path $TestDrive ([guid]::NewGuid().ToString())
        $null = New-Item -ItemType Directory -Path (Join-Path $output 'mutants.out') -Force
        '' | Set-Content -LiteralPath (Join-Path $output 'summary.md')
        $script:lab = Get-Content -LiteralPath (Join-Path $fixtures 'outcomes.json') -Raw | ConvertFrom-Json -AsHashtable
    }

    It 'describes native findings without producing another check verdict' {
        $lab | ConvertTo-Json -Depth 20 | Set-Content -LiteralPath (Join-Path $output 'mutants.out\outcomes.json')
        Write-ScheduledMutationSummary -OutputDirectory $output | Should -BeNullOrEmpty
        $summary = Get-Content -LiteralPath (Join-Path $output 'summary.md') -Raw
        $summary | Should -Match 'replace sample -> u32 with 0'
        $summary | Should -Match 'replace sample -> u32 with 1'
    }

    It 'describes a baseline failure without presenting mutants as source defects' {
        $lab.outcomes[0].summary = 'Failure'
        $null = New-Item -ItemType Directory -Path (Join-Path $output 'mutants.out\log')
        'baseline failure canary' | Set-Content -LiteralPath (Join-Path $output 'mutants.out\log\baseline.log')
        $lab | ConvertTo-Json -Depth 20 | Set-Content -LiteralPath (Join-Path $output 'mutants.out\outcomes.json')
        Write-ScheduledMutationSummary -OutputDirectory $output
        $summary = Get-Content -LiteralPath (Join-Path $output 'summary.md') -Raw
        $summary | Should -Match 'baseline failure canary'
        $summary | Should -Not -Match 'replace sample'
    }

    It 'identifies unavailable native details without inventing a result' {
        Write-ScheduledMutationSummary -OutputDirectory $output
        Get-Content -LiteralPath (Join-Path $output 'summary.md') -Raw | Should -Match 'details are unavailable'
    }
}
