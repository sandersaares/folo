#requires -Version 7
# Runs the native PowerShell analyzer for validate-scripts without changing its rule set or exit
# semantics. Native error/verbose streams retain the file/rule context of hosted engine failures.
# Ref: ../../docs/build-and-tooling.md#powershell-linting.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true

function Invoke-WorkspaceScriptAnalysis {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string] $RepositoryRoot,
        [Parameter(Mandatory)][string] $AnalyzerVersion,
        [Parameter(Mandatory)][string] $DiagnosticsDirectory
    )
    Import-Module PSScriptAnalyzer -RequiredVersion $AnalyzerVersion -Force
    $directory = Join-Path $DiagnosticsDirectory ([guid]::NewGuid().ToString('N'))
    $null = New-Item -ItemType Directory -Path $directory -Force
    $trace = Join-Path $directory analyzer-verbose.log
    @{
        powershell = $PSVersionTable.PSVersion.ToString()
        edition = $PSVersionTable.PSEdition; platform = [Environment]::OSVersion.ToString()
        culture = [Globalization.CultureInfo]::CurrentCulture.Name
        analyzer = @((Get-Module PSScriptAnalyzer) | Select-Object Name, Version, Path)
        pester = @((Get-Module Pester -ListAvailable) | Select-Object Name, Version, Path)
        module_path = $env:PSModulePath; repository = $RepositoryRoot
    } | ConvertTo-Json -Depth 10 | Set-Content -LiteralPath (Join-Path $directory environment.json)
    try {
        $results = @(Invoke-ScriptAnalyzer -Path (Join-Path $RepositoryRoot scripts) -Recurse `
            -Settings (Join-Path $RepositoryRoot PSScriptAnalyzerSettings.psd1) `
            -CustomRulePath (Join-Path $RepositoryRoot 'scripts\analyzer\FoloAnalyzerRules.psm1') `
            -IncludeDefaultRules -Verbose 4> $trace)
    } catch [System.Management.Automation.RuntimeException], [System.NullReferenceException] {
        # Preserve the original failure. The normal formatter omits managed/inner stacks,
        # while the trace identifies the last files and rules the engine started.
        $_.Exception.ToString() | Set-Content -LiteralPath (Join-Path $directory exception.log)
        @{
            error_id = $_.FullyQualifiedErrorId; category = [string]$_.CategoryInfo
            script_stack = $_.ScriptStackTrace
            position = if ($null -ne $_.InvocationInfo) { $_.InvocationInfo.PositionMessage } else { $null }
        } | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (Join-Path $directory error-record.json)
        Write-Host "PSScriptAnalyzer engine failure; diagnostics: $directory"
        Write-Host $_.Exception.ToString()
        throw
    }
    if ($results.Count -gt 0) {
        foreach ($finding in $results) {
            $name = [string]$finding.ScriptName
            if ($name.StartsWith($RepositoryRoot, [StringComparison]::OrdinalIgnoreCase)) {
                $name = $name.Substring($RepositoryRoot.Length).TrimStart('\', '/')
            }
            Write-Host ("{0}:{1} [{2}] {3}: {4}" -f $name, $finding.Line, $finding.Severity, $finding.RuleName, $finding.Message)
        }
        $noun = if ($results.Count -eq 1) { 'issue' } else { 'issues' }
        throw "PSScriptAnalyzer reported $($results.Count) $noun. Fix the findings; no rule was skipped."
    }
    Write-Host "PSScriptAnalyzer: no issues in scripts/. Diagnostics: $directory"
}

Export-ModuleMember -Function Invoke-WorkspaceScriptAnalysis
