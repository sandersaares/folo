#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
# Verifies that analyzer diagnostics preserve the exact full-rule invocation and failure outcome.
BeforeAll {
    Import-Module PSScriptAnalyzer -RequiredVersion 1.25.0
    Import-Module (Join-Path $PSScriptRoot 'ScriptAnalysis.psm1')
}

Describe 'Workspace script-analysis diagnostics' {
    BeforeEach {
        $script:root = Join-Path $TestDrive ([guid]::NewGuid().ToString('N'))
        $script:diagnostics = Join-Path $root diagnostics
        Mock Import-Module -ModuleName ScriptAnalysis { }
        Mock Invoke-ScriptAnalyzer -ModuleName ScriptAnalysis {
            Write-Verbose 'Analyzing the complete script tree and its rules.' -Verbose
        }
    }

    It 'keeps recursive default/custom rules enabled and retains runtime and rule context' {
        Invoke-WorkspaceScriptAnalysis $root 1.25.0 $diagnostics
        Should -Invoke Invoke-ScriptAnalyzer -ModuleName ScriptAnalysis -Exactly -Times 1 -ParameterFilter {
            $Path -ceq (Join-Path $root scripts) -and $Recurse -and $IncludeDefaultRules -and
            $Settings -ceq (Join-Path $root PSScriptAnalyzerSettings.psd1) -and
            $CustomRulePath -ceq (Join-Path $root 'scripts\analyzer\FoloAnalyzerRules.psm1')
        }
        $directory = @(Get-ChildItem -LiteralPath $diagnostics -Directory)[0].FullName
        $environment = Get-Content -LiteralPath (Join-Path $directory environment.json) -Raw | ConvertFrom-Json
        $environment.powershell | Should -Be $PSVersionTable.PSVersion.ToString()
        $environment.analyzer[0].Name | Should -Be PSScriptAnalyzer
        Get-Content -LiteralPath (Join-Path $directory analyzer-verbose.log) -Raw | Should -Match 'complete script tree'
    }

    It 'fails on ordinary findings instead of converting them to diagnostic-only warnings' -ForEach @(
        @{ Count = 1 }, @{ Count = 2 }
    ) {
        Mock Invoke-ScriptAnalyzer -ModuleName ScriptAnalysis {
            foreach ($index in 1..$Count) {
                @{ ScriptName = if ($index -eq 1) { Join-Path $root 'scripts\bad.ps1' } else { 'other.ps1' }
                    Line = $index; Severity = 'Warning'; RuleName = 'ExampleRule'; Message = 'An actual finding' }
            }
        }
        { Invoke-WorkspaceScriptAnalysis $root 1.25.0 $diagnostics } | Should -Throw
        Should -Invoke Invoke-ScriptAnalyzer -ModuleName ScriptAnalysis -Exactly -Times 1
    }

    It 'retains <ExceptionType> and its inner exception plus the last file/rule context without retrying' -ForEach @(
        @{ ExceptionType = [System.NullReferenceException] }
        @{ ExceptionType = [System.InvalidOperationException] }
        @{ ExceptionType = [System.ArgumentException] }
    ) {
        Mock Invoke-ScriptAnalyzer -ModuleName ScriptAnalysis {
            Write-Verbose 'Analyzing failing.ps1 with ExampleRule.' -Verbose
            throw $ExceptionType::new('analyzer-canary',
                [IO.IOException]::new('inner-canary'))
        }
        { Invoke-WorkspaceScriptAnalysis $root 1.25.0 $diagnostics } | Should -Throw
        $directory = @(Get-ChildItem -LiteralPath $diagnostics -Directory)[0].FullName
        $exception = Get-Content -LiteralPath (Join-Path $directory exception.log) -Raw
        $exception | Should -Match 'analyzer-canary'
        $exception | Should -Match ([regex]::Escape($ExceptionType.FullName))
        $exception | Should -Match 'inner-canary'
        Get-Content -LiteralPath (Join-Path $directory analyzer-verbose.log) -Raw | Should -Match 'failing.ps1'
        Test-Path -LiteralPath (Join-Path $directory error-record.json) | Should -BeTrue
        Should -Invoke Invoke-ScriptAnalyzer -ModuleName ScriptAnalysis -Exactly -Times 1
    }
}
