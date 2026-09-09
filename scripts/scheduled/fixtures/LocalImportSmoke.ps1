Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
$VerbosePreference = 'Continue'

# A fresh process must reproduce the skill's single-import entrypoint, rather than
# relying on dependency imports performed by other Pester suites in the same session.
Import-Module (Join-Path $PSScriptRoot '..' 'LocalInbox.psm1')
$module = Get-Module LocalInbox
& $module {
    function script:Invoke-ScheduledApi {
        param([string] $Endpoint)
        switch -Wildcard ($Endpoint) {
            'user' { return @{ login = 'sandersaares' } }
            'repos/folo-rs/folo' { return @{ id = 850321188; full_name = 'folo-rs/folo' } }
            '*/workflows/scheduled-validation.yml' { return @{ state = 'active' } }
            default { throw "Unexpected smoke-test transport request: $Endpoint" }
        }
    }
    function script:Get-ScheduledApiCollection {
        param([string] $Endpoint)
        if (-not $Endpoint.Contains('/issues?')) { throw 'Unexpected collection request.' }
        return ,@()
    }
    function script:Get-ScheduledStateRoot {
        param([string] $RepositoryId)
        return "unused-fixture-$RepositoryId"
    }
    function script:Invoke-ScheduledLocalAction {
        return @{ attempts = @{}; mode = 'observe'; executor_id = 'smoke'; profile = $null }
    }
    $null = Get-ScheduledDigest -Value @{ fixture = 'fresh-process' }
}
foreach ($dependency in @('LocalState', 'LocalLifecycle')) {
    & (Get-Module -Name $dependency -All) {
        $null = Get-ScheduledDigest -Value @{ fixture = 'sibling-dependency' }
    }
}
$snapshot = Invoke-ScheduledInbox -ExecutorId smoke -Now '2026-09-08T00:00:00Z'
if (-not $snapshot.successful_scan -or $snapshot.backlog_count -ne 0 -or
    $snapshot.blocked_conditions -cnotcontains 'ai-triage-unavailable') {
    throw 'The fresh-process empty inbox must expose unsupported AI triage.'
}
Write-Output 'local-import-smoke-ok'
