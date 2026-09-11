#requires -Version 7

# The full and selected workflows call this planner before installing checker tools.
# The event pins the reviewed main controller; manual input changes only the tested checkout.
# Every invocation emits fresh work, without consulting previous runs or issue state.
# Ref: .github/workflows/implementation.md#immutable-execution.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
Import-Module (Join-Path $PSScriptRoot 'ScheduledPlan.psm1')

function Invoke-ScheduledPlanning {
    [CmdletBinding()]
    [OutputType([hashtable])]
    param(
        [Parameter(Mandatory)][ValidateSet('full', 'selected')][string] $Mode,
        [Parameter(Mandatory)][string] $EventPath,
        [Parameter(Mandatory)][string] $OutputDirectory
    )

    if ($env:GITHUB_REF -cne 'refs/heads/main') {
        throw 'Select main under Use workflow from; use source_sha to test a different commit.'
    }
    if ($env:GITHUB_EVENT_NAME -cnotin @('schedule', 'workflow_dispatch') -or
        ($Mode -ceq 'selected' -and $env:GITHUB_EVENT_NAME -cne 'workflow_dispatch')) {
        throw 'Deep checks require a nightly schedule or manual dispatch.'
    }
    $workflowEvent = Get-Content -LiteralPath $EventPath -Raw | ConvertFrom-Json -AsHashtable
    $controllerSha = $env:GITHUB_SHA
    if ($controllerSha -cnotmatch '^[0-9a-f]{40}$') { throw 'The event must identify a full controller commit SHA.' }
    $sourceSha = $controllerSha
    $checkIds = @()
    $packages = @()
    if ($env:GITHUB_EVENT_NAME -ceq 'workflow_dispatch') {
        $inputs = if ($workflowEvent.ContainsKey('inputs')) { $workflowEvent.inputs } else { @{} }
        if ($inputs.ContainsKey('source_sha') -and -not [string]::IsNullOrWhiteSpace($inputs.source_sha)) {
            $sourceSha = $inputs.source_sha.Trim()
        }
        if ($Mode -ceq 'selected') {
            foreach ($key in @('check_ids', 'packages')) {
                if (-not $inputs.ContainsKey($key) -or [string]::IsNullOrWhiteSpace($inputs[$key])) {
                    throw "Selected execution requires $key."
                }
            }
            $checkIds = @($inputs.check_ids -split ',' | ForEach-Object { $_.Trim() })
            $packages = @($inputs.packages -split ',' | ForEach-Object { $_.Trim() })
            if ('' -cin $checkIds -or '' -cin $packages) { throw 'Selections must not contain empty entries.' }
        }
    }
    if ($sourceSha -cnotmatch '^[0-9a-f]{40}$') { throw 'source_sha must be a full commit SHA, not a branch name.' }
    $plan = @{
        source_sha = $sourceSha
        controller_sha = $controllerSha
        checks = @(Get-ScheduledCheck -Packages $packages -CheckIds $checkIds)
    }
    $null = New-Item -ItemType Directory -Path $OutputDirectory -Force
    $plan | ConvertTo-Json -Depth 10 | Set-Content -LiteralPath (Join-Path $OutputDirectory 'plan.json')
    if ($env:GITHUB_OUTPUT) {
        @(
            "source_sha=$sourceSha"
            "controller_sha=$controllerSha"
            "matrix=$(@{ include = $plan.checks } | ConvertTo-Json -Depth 10 -Compress)"
        ) | Add-Content -LiteralPath $env:GITHUB_OUTPUT
    }
    if ($env:GITHUB_STEP_SUMMARY) {
        @(
            "## Fresh $Mode deep validation"
            ''
            "Tested commit: ``$sourceSha``"
            "Controller commit: ``$controllerSha``"
            ''
            '| Check | Platform | Shard | Packages |'
            '| --- | --- | --- | --- |'
            foreach ($check in $plan.checks) {
                $scope = if ($check.packages.Count -eq 0) { 'workspace' } else { $check.packages -join ', ' }
                "| $($check.id) | $($check.platform) | $($check.shard) | $scope |"
            }
        ) | Add-Content -LiteralPath $env:GITHUB_STEP_SUMMARY
    }
    Write-Verbose "Fresh $Mode execution: event=$($env:GITHUB_EVENT_NAME), controller=$controllerSha, source=$sourceSha."
    return $plan
}

Export-ModuleMember -Function Invoke-ScheduledPlanning
