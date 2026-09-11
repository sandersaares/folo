#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }

# Protects the Standard validation fan-in and first-event external-service exclusions.
# These source contracts complement actionlint's YAML validation without invoking GitHub jobs.
# Ref: .github/workflows/implementation.md#merge-blocking-result.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true

BeforeAll {
    $root = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
    $script:standard = Get-Content -LiteralPath (Join-Path $root '.github/workflows/standard-validation.yml') -Raw
    $script:benchmarks = Get-Content -LiteralPath (Join-Path $root '.github/workflows/pr-bench-history.yml') -Raw

    function Get-WorkflowJob([string] $Workflow, [string] $Name) {
        $pattern = '(?ms)^  ' + [regex]::Escape($Name) + ':\r?\n(?<body>.*?)(?=^  [a-z][a-z0-9-]*:\r?$|\z)'
        $match = [regex]::Match($Workflow, $pattern)
        $match.Success | Should -BeTrue
        return $match.Groups['body'].Value
    }
}

Describe 'Standard validation integration' {
    It 'keeps deep execution and repair registration out of ordinary PR validation' {
        $standard | Should -Not -Match 'scheduled-context|scheduled-repair-gate|scheduled-version-check|deep-checks\.yml'
        $standard | Should -Not -Match 'scripts/scheduled/|<!-- scheduled-repair:'
    }

    It 'keeps every must-succeed job in the fan-in and every dependency in the workflow' {
        $jobNames = @([regex]::Matches($standard, '(?m)^  ([a-z][a-z0-9-]*):\r?$') |
            ForEach-Object { $_.Groups[1].Value })
        $fanIn = Get-WorkflowJob $standard 'required-checks'
        $fanIn | Should -Match '(?m)^    name: required-checks\r?$'
        $dependencyBlock = [regex]::Match($fanIn, '(?m)^    needs:\r?\n(?<jobs>(?:      - [^\r\n]+\r?\n)+)')
        $dependencyBlock.Success | Should -BeTrue
        $dependencies = @([regex]::Matches($dependencyBlock.Groups['jobs'].Value, '(?m)^      - ([^\r\n]+)') |
            ForEach-Object { $_.Groups[1].Value })
        $mustSucceed = [regex]::Match($fanIn, '(?m)^\s+MUST_SUCCEED_JOBS: ([^\r\n]+)').Groups[1].Value -split '\s+'
        $mustSucceed | Should -Be @('changes', 'delta', 'validate-versions', 'semver-checks')
        foreach ($job in $mustSucceed) { $dependencies | Should -Contain $job }
        foreach ($job in $dependencies) { $jobNames | Should -Contain $job }
        $dependencies | Should -Not -Contain 'alert'
        $dependencies | Should -Not -Contain 'coverage-notify'
    }

    It 'retains unconditional version validation' {
        Get-WorkflowJob $standard 'validate-versions' | Should -Not -Match '(?m)^    if:'
    }

    It 'excludes repair branches from credentialed job <_> without a registry' -ForEach @('test-azure', 'test-azure-gh') {
        $job = Get-WorkflowJob $standard $_
        $job | Should -Match "!contains\(github\.head_ref, 'scheduled-repair-'\)"
        $job | Should -Match "github\.event_name != 'merge_group'"
        $job | Should -Match 'github\.event\.pull_request\.head\.repo\.full_name == github\.repository'
        $job | Should -Not -Match 'scheduled-context|pull_request\.body'
        $job | Should -Match '(?m)^    needs: delta\r?$'
    }

    It 'excludes repair branches from production benchmarks before any dependent job starts' {
        $job = Get-WorkflowJob $benchmarks 'delta'
        $job | Should -Match "!contains\(github\.head_ref, 'scheduled-repair-'\)"
        $job | Should -Match 'github\.event\.pull_request\.head\.repo\.full_name == github\.repository'
        $job | Should -Not -Match 'pull_request\.body|sandersaares-scheduled'
    }
}
