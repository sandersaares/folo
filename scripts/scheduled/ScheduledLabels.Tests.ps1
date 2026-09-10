#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
# Exercises reporting label bootstrap with an in-memory API, including first publication,
# write authorization, preserved operator metadata and raced/lost create responses.
# No test may contact GitHub or require Local App configuration.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
$VerbosePreference = 'Continue'

BeforeDiscovery { Import-Module (Join-Path $PSScriptRoot 'ScheduledGitHub.psm1') }
BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'ScheduledGitHub.psm1')
    Import-Module (Join-Path $PSScriptRoot 'ScheduledRunGitHub.psm1') -Force
    function Invoke-LabelTestApi {
        param($Endpoint, $Method = 'GET', $Body, [switch] $Paginate)
        $script:calls.Add(@{ endpoint = $Endpoint; method = $Method; body = $Body })
        if ($Endpoint -eq 'repos/owner/repo/labels?per_page=100') {
            if (-not $Paginate) { throw 'Label lookup requires complete pagination.' }
            if ($script:failRead) { throw [IO.IOException]::new('Inventory unavailable.') }
            return ,@(@(@{ name = 'unrelated' }), @($script:labels.Values))
        }
        if ($Endpoint -eq 'repos/owner/repo/labels' -and $Method -eq 'POST') {
            $journal = Get-Content -LiteralPath (Join-Path $script:output "label-$($Body.name).json") -Raw |
                ConvertFrom-Json -AsHashtable
            $journal.stage | Should -Be creating-label
            if ($script:failCreate) { throw [IO.IOException]::new('Create denied.') }
            $script:labels[$Body.name] = if ($script:race) {
                @{ name = $Body.name; color = 'abcdef'; description = 'Operator created this label.' }
            } else { $Body.Clone() }
            if ($script:race -or $script:loseReply) { throw [IO.IOException]::new('Create response unavailable.') }
            return $script:labels[$Body.name]
        }
        if ($Endpoint -match '^repos/owner/repo/labels/(scheduled-[a-z-]+)$') {
            if ($script:wrongIdentity) { return @{ name = 'wrong-label' } }
            if (-not $script:labels.ContainsKey($Matches[1])) { throw [IO.IOException]::new('Label not found.') }
            return $script:labels[$Matches[1]]
        }
        throw "Unexpected label API call: $Method $Endpoint"
    }
}

Describe 'Authorized reporting label bootstrap' {
    BeforeEach {
        $script:policy = @{ repository = 'owner/repo'; rollout = @{ reporting_enabled = $true } }
        $script:labels = @{}
        $script:calls = [Collections.Generic.List[object]]::new()
        $script:output = Join-Path $TestDrive ([guid]::NewGuid().ToString('N'))
        $null = New-Item -ItemType Directory -Path $output
        $script:race = $false; $script:loseReply = $false; $script:failCreate = $false
        $script:failRead = $false; $script:wrongIdentity = $false
        $script:arguments = @{
            Policy = $policy; OutputDirectory = $output; Api = ${function:Invoke-LabelTestApi}; Apply = $true
        }
    }
    It 'creates only the required missing <Name> label and confirms it by unique name' -TestCases @(
        @{ Name = 'scheduled-run-failure' }, @{ Name = 'scheduled-coverage' }, @{ Name = 'scheduled-health' }
    ) {
        param($Name)
        Initialize-ScheduledReportingLabel @arguments -Name $Name
        $labels.Keys | Should -Be @($Name)
        $labels[$Name].description | Should -Not -BeNullOrEmpty
        $calls.method | Should -Be @('GET', 'POST', 'GET')
        $calls[-1].endpoint | Should -Be "repos/owner/repo/labels/$Name"
        $journal = Get-Content -LiteralPath (Join-Path $output "label-$Name.json") -Raw | ConvertFrom-Json
        $journal.stage | Should -Be complete
        $journal.repository | Should -Be owner/repo
        $calls.Clear()
        Initialize-ScheduledReportingLabel @arguments -Name $Name
        $calls.method | Should -Be @('GET')
    }
    It 'preserves existing label metadata even with different capitalization on a later page' {
        $labels['scheduled-health'] = @{ name = 'Scheduled-Health'; color = '123abc'; description = 'Operator preference' }
        Initialize-ScheduledReportingLabel @arguments -Name scheduled-health
        $calls.method | Should -Be @('GET')
        $labels['scheduled-health'].name | Should -BeExactly 'Scheduled-Health'
        $labels['scheduled-health'].color | Should -BeExactly '123abc'
        $labels['scheduled-health'].description | Should -BeExactly 'Operator preference'
        @(Get-ChildItem -LiteralPath $output).Count | Should -Be 0
    }
    It 'does not access label state with Apply=<Apply> and Reporting=<Reporting>' -TestCases @(
        @{ Apply = $false; Reporting = $false }
        @{ Apply = $false; Reporting = $true }
        @{ Apply = $true; Reporting = $false }
    ) {
        param($Apply, $Reporting)
        $policy.rollout.reporting_enabled = $Reporting
        $arguments.Apply = $Apply
        Initialize-ScheduledReportingLabel @arguments -Name scheduled-health
        $calls.Count | Should -Be 0
        @(Get-ChildItem -LiteralPath $output).Count | Should -Be 0
    }
    It 'reconciles a <Situation> response without updating or repeating creation' -TestCases @(
        @{ Situation = 'race' }, @{ Situation = 'lost' }
    ) {
        param($Situation)
        $script:race = $Situation -eq 'race'
        $script:loseReply = $Situation -eq 'lost'
        Initialize-ScheduledReportingLabel @arguments -Name scheduled-health
        $calls.method | Should -Be @('GET', 'POST', 'GET')
        if ($race) { $labels['scheduled-health'].description | Should -BeExactly 'Operator created this label.' }
    }
    It 'fails explicitly on <Failure> and never records successful publication' -TestCases @(
        @{ Failure = 'read' }, @{ Failure = 'create' }, @{ Failure = 'identity' }
    ) {
        param($Failure)
        $script:failRead = $Failure -eq 'read'
        $script:failCreate = $Failure -eq 'create'
        $script:wrongIdentity = $Failure -eq 'identity'
        { Initialize-ScheduledReportingLabel @arguments -Name scheduled-health } | Should -Throw
        if ($failRead) {
            $calls.method | Should -Be @('GET')
        } else {
            $journal = Get-Content -LiteralPath (Join-Path $output 'label-scheduled-health.json') -Raw | ConvertFrom-Json
            $journal.stage | Should -Be creating-label
        }
    }
}

Describe 'Coverage bootstrap at the actual issue-write boundary' {
    InModuleScope ScheduledGitHub {
        BeforeEach {
            $script:bootstrapPolicy = @{
                repository = 'owner/repo'; reporter_login = 'github-actions[bot]'
                rollout = @{ reporting_enabled = $true }
            }
            $script:bootstrapLabels = @{}
            $script:bootstrapWrites = [Collections.Generic.List[object]]::new()
            $script:failLabelWrite = $false
            $script:record = @{ schema_version = 1; repository = 'owner/repo'; receipt = $null; invalidation = $null }
            Mock Invoke-ScheduledGitHubApi {
                if ($Endpoint -eq 'repos/owner/repo/labels?per_page=100') {
                    return @($script:bootstrapLabels.Values)
                }
                if ($Method -eq 'POST') {
                    $script:bootstrapWrites.Add(@{ endpoint = $Endpoint; body = $Body })
                    if ($Endpoint -eq 'repos/owner/repo/labels') {
                        if ($script:failLabelWrite) { throw [IO.IOException]::new('Label write denied.') }
                        $script:bootstrapLabels[$Body.name] = $Body
                        return $Body
                    }
                    if ($Endpoint -eq 'repos/owner/repo/issues') {
                        foreach ($label in $Body.labels) {
                            if (-not $script:bootstrapLabels.ContainsKey($label)) { throw 'Required label is missing.' }
                        }
                        return @{ number = 42 }
                    }
                }
                if ($Endpoint -match '^repos/owner/repo/labels/(scheduled-[a-z-]+)$') {
                    if (-not $script:bootstrapLabels.ContainsKey($Matches[1])) { throw [IO.IOException]::new('Missing label.') }
                    return $script:bootstrapLabels[$Matches[1]]
                }
                throw "Unexpected coverage API call: $Method $Endpoint"
            }
        }
        It 'creates required coverage labels before the first issue without inventing a receipt' {
            $result = Sync-ScheduledIssue -Policy $bootstrapPolicy -Issue $null -Record $record `
                -Kind coverage -OutputDirectory $TestDrive -Apply
            $result.action | Should -Be POST
            $result.number | Should -Be 42
            @($bootstrapLabels.Keys | Sort-Object) | Should -Be @('scheduled-coverage', 'scheduled-health')
            $bootstrapWrites[-1].endpoint | Should -BeExactly 'repos/owner/repo/issues'
            $published = Read-ScheduledRecord -Text $bootstrapWrites[-1].body.body -Kind coverage
            $published.receipt | Should -BeNullOrEmpty
            $published.invalidation | Should -BeNullOrEmpty
        }
        It 'never attempts issue creation after label bootstrap fails' {
            $script:failLabelWrite = $true
            { Sync-ScheduledIssue -Policy $bootstrapPolicy -Issue $null -Record $record `
                -Kind coverage -OutputDirectory $TestDrive -Apply } | Should -Throw
            Should -Invoke Invoke-ScheduledGitHubApi -Times 0 -ParameterFilter { $Endpoint -eq 'repos/owner/repo/issues' }
        }
        It 'propagates issue publication failures after successful label bootstrap' {
            Mock Invoke-ScheduledGitHubApi { throw [IO.IOException]::new('Issue write denied.') } `
                -ParameterFilter { $Endpoint -eq 'repos/owner/repo/issues' }
            { Sync-ScheduledIssue -Policy $bootstrapPolicy -Issue $null -Record $record `
                -Kind coverage -OutputDirectory $TestDrive -Apply } | Should -Throw
            $bootstrapLabels.Count | Should -Be 2
        }
    }
}
