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
                repository = 'owner/repo'; repository_id = 123; reporter_login = 'github-actions[bot]'
                rollout = @{ reporting_enabled = $true }
            }
            $script:bootstrapLabels = @{}
            $script:bootstrapWrites = [Collections.Generic.List[object]]::new()
            $script:failLabelWrite = $false
            $script:loseCoverageReply = $false
            $script:badCoverageNumber = $false
            $script:hideCoverageIssue = $false
            $script:coverageIssue = $null
            $script:record = @{
                schema_version = 1; repository = 'owner/repo'; repository_id = 123
                receipt = $null; invalidation = $null
            }
            $script:publicationPath = Join-Path $TestDrive 'publication-123-456-789.json'
            @{
                schema_version = 1; identity = @{ repository_id = 123; workflow_id = 456; run_id = 789 }
                stage = 'prepared'; issue_number = $null; pending_pages = @{}
            } | ConvertTo-Json -Depth 20 | Set-Content -LiteralPath $publicationPath
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
                        $intent = Get-Content -LiteralPath $publicationPath -Raw | ConvertFrom-Json -AsHashtable
                        $intent.coverage_creation.stage | Should -Be creating-issue
                        foreach ($label in $Body.labels) {
                            if (-not $script:bootstrapLabels.ContainsKey($label)) { throw 'Required label is missing.' }
                        }
                        $script:coverageIssue = @{
                            number = 42; state = 'open'; user = @{ login = 'github-actions[bot]' }
                            body = $Body.body
                        }
                        if ($script:loseCoverageReply) { throw [IO.IOException]::new('Lost coverage response.') }
                        if ($script:badCoverageNumber) { return @{ number = 0 } }
                        return @{ number = 42 }
                    }
                }
                if ($Endpoint -eq 'repos/owner/repo/issues?labels=scheduled-coverage&state=all&per_page=100') {
                    if ($script:hideCoverageIssue -or $null -eq $script:coverageIssue) { return @() }
                    return @($script:coverageIssue)
                }
                if ($Endpoint -eq 'repos/owner/repo/issues/42') { return $script:coverageIssue }
                if ($Endpoint -match '^repos/owner/repo/labels/(scheduled-[a-z-]+)$') {
                    if (-not $script:bootstrapLabels.ContainsKey($Matches[1])) { throw [IO.IOException]::new('Missing label.') }
                    return $script:bootstrapLabels[$Matches[1]]
                }
                throw "Unexpected coverage API call: $Method $Endpoint"
            }
        }
        It 'creates required coverage labels before the first issue without inventing a receipt' {
            $result = Sync-ScheduledIssue -Policy $bootstrapPolicy -Issue $null -Record $record `
                -Kind coverage -OutputDirectory $TestDrive -PublicationJournalPath $publicationPath -Apply
            $result.action | Should -Be POST
            $result.number | Should -Be 42
            @($bootstrapLabels.Keys | Sort-Object) | Should -Be @('scheduled-coverage', 'scheduled-health')
            $bootstrapWrites[-1].endpoint | Should -BeExactly 'repos/owner/repo/issues'
            $published = Read-ScheduledRecord -Text $bootstrapWrites[-1].body.body -Kind coverage
            $published.receipt | Should -BeNullOrEmpty
            $published.invalidation | Should -BeNullOrEmpty
            $journal = Get-Content -LiteralPath $publicationPath -Raw | ConvertFrom-Json -AsHashtable
            $journal.coverage_creation.stage | Should -Be complete
            $journal.coverage_creation.issue_number | Should -Be 42
        }
        It 'rejects <ValueKind> <Name> before accessing GitHub for new authorized coverage' -TestCases @(
            @{ Name = 'OutputDirectory'; ValueKind = 'omitted' }
            @{ Name = 'OutputDirectory'; ValueKind = 'empty' }
            @{ Name = 'OutputDirectory'; ValueKind = 'whitespace' }
            @{ Name = 'PublicationJournalPath'; ValueKind = 'omitted' }
            @{ Name = 'PublicationJournalPath'; ValueKind = 'empty' }
            @{ Name = 'PublicationJournalPath'; ValueKind = 'whitespace' }
        ) {
            param($Name, $ValueKind)
            $arguments = @{
                Policy = $bootstrapPolicy; Issue = $null; Record = $record; Kind = 'coverage'
                OutputDirectory = $TestDrive; PublicationJournalPath = $publicationPath; Apply = $true
            }
            switch ($ValueKind) {
                omitted { $arguments.Remove($Name) }
                empty { $arguments[$Name] = '' }
                whitespace { $arguments[$Name] = ' ' }
            }
            $failure = { Sync-ScheduledIssue @arguments } |
                Should -Throw -ExceptionType ([ArgumentException]) -PassThru
            $failure.Exception.ParamName | Should -Be $Name
            Should -Invoke Invoke-ScheduledGitHubApi -Times 0
            $bootstrapWrites.Count | Should -Be 0
        }
        It 'does not require write context for new coverage with Apply=<Apply> and Reporting=<Reporting>' -TestCases @(
            @{ Apply = $false; Reporting = $false }
            @{ Apply = $false; Reporting = $true }
            @{ Apply = $true; Reporting = $false }
        ) {
            param($Apply, $Reporting)
            $bootstrapPolicy.rollout.reporting_enabled = $Reporting
            $result = Sync-ScheduledIssue -Policy $bootstrapPolicy -Issue $null -Record $record `
                -Kind coverage -Apply:$Apply
            $result.action | Should -Be dry-run
            $unusedPath = Join-Path $TestDrive 'not-needed'
            (Sync-ScheduledIssue -Policy $bootstrapPolicy -Issue $null -Record $record -Kind coverage `
                -OutputDirectory $unusedPath -PublicationJournalPath $unusedPath -Apply:$Apply).action |
                Should -Be dry-run
            Should -Invoke Invoke-ScheduledGitHubApi -Times 0
        }
        It 'rejects a <Kind> journal path with an IO error before reading it or accessing GitHub' -TestCases @(
            @{ Kind = 'missing' }, @{ Kind = 'directory' }
        ) {
            param($Kind)
            $path = if ($Kind -eq 'missing') { Join-Path $TestDrive 'missing-publication.json' } else { $TestDrive }
            Mock Get-Content { throw 'Journal must not be read.' }
            { Sync-ScheduledIssue -Policy $bootstrapPolicy -Issue $null -Record $record -Kind coverage `
                -OutputDirectory $TestDrive -PublicationJournalPath $path -Apply } |
                Should -Throw -ExceptionType ([IO.IOException])
            Should -Invoke Get-Content -Times 0
            Should -Invoke Invoke-ScheduledGitHubApi -Times 0
        }
        It 'never attempts issue creation after label bootstrap fails' {
            $script:failLabelWrite = $true
            { Sync-ScheduledIssue -Policy $bootstrapPolicy -Issue $null -Record $record `
                -Kind coverage -OutputDirectory $TestDrive -PublicationJournalPath $publicationPath -Apply } | Should -Throw
            Should -Invoke Invoke-ScheduledGitHubApi -Times 0 -ParameterFilter { $Endpoint -eq 'repos/owner/repo/issues' }
            $journal = Get-Content -LiteralPath $publicationPath -Raw | ConvertFrom-Json -AsHashtable
            $journal.ContainsKey('coverage_creation') | Should -BeFalse
            $script:failLabelWrite = $false
            (Sync-ScheduledIssue -Policy $bootstrapPolicy -Issue $null -Record $record `
                -Kind coverage -OutputDirectory $TestDrive -PublicationJournalPath $publicationPath -Apply).number |
                Should -Be 42
        }
        It 'propagates issue publication failures after successful label bootstrap' {
            Mock Invoke-ScheduledGitHubApi { throw [IO.IOException]::new('Issue write denied.') } `
                -ParameterFilter { $Endpoint -eq 'repos/owner/repo/issues' }
            { Sync-ScheduledIssue -Policy $bootstrapPolicy -Issue $null -Record $record `
                -Kind coverage -OutputDirectory $TestDrive -PublicationJournalPath $publicationPath -Apply } | Should -Throw
            $bootstrapLabels.Count | Should -Be 2
        }
        It 'reconciles a lost coverage-create response by rereading the owned issue' {
            $script:loseCoverageReply = $true
            $result = Sync-ScheduledIssue -Policy $bootstrapPolicy -Issue $null -Record $record `
                -Kind coverage -OutputDirectory $TestDrive -PublicationJournalPath $publicationPath -Apply
            $result.number | Should -Be 42
            Should -Invoke Invoke-ScheduledGitHubApi -Times 1 -Exactly -ParameterFilter {
                $Endpoint -eq 'repos/owner/repo/issues' -and $Method -eq 'POST'
            }
            $journal = Get-Content -LiteralPath $publicationPath -Raw | ConvertFrom-Json -AsHashtable
            $journal.coverage_creation.stage | Should -Be complete
            $journal.coverage_creation.issue_number | Should -Be 42
            $journal.stage | Should -Be prepared
        }
        It 'reconciles a create response without an issue number rather than trusting it' {
            $script:badCoverageNumber = $true
            (Sync-ScheduledIssue -Policy $bootstrapPolicy -Issue $null -Record $record `
                -Kind coverage -OutputDirectory $TestDrive -PublicationJournalPath $publicationPath -Apply).number |
                Should -Be 42
            Should -Invoke Invoke-ScheduledGitHubApi -Times 1 -Exactly -ParameterFilter {
                $Endpoint -eq 'repos/owner/repo/issues' -and $Method -eq 'POST'
            }
        }
        It 'rejects an invalid coverage journal <Field> before any new POST' -TestCases @(
            @{ Field = 'identity' }, @{ Field = 'stage' }
        ) {
            param($Field)
            $journal = Get-Content -LiteralPath $publicationPath -Raw | ConvertFrom-Json -AsHashtable
            if ($Field -eq 'identity') { $journal.identity.repository_id = 999 }
            else { $journal.coverage_creation = @{ stage = 'unexpected'; issue_number = $null } }
            $journal | ConvertTo-Json -Depth 20 | Set-Content -LiteralPath $publicationPath
            { Sync-ScheduledIssue -Policy $bootstrapPolicy -Issue $null -Record $record `
                -Kind coverage -OutputDirectory $TestDrive -PublicationJournalPath $publicationPath -Apply } | Should -Throw
            Should -Invoke Invoke-ScheduledGitHubApi -Times 0
        }
        It 'never repeats an unresolved coverage POST and resumes only once its issue is visible' {
            $script:loseCoverageReply = $true; $script:hideCoverageIssue = $true
            foreach ($retry in 1..2) {
                { Sync-ScheduledIssue -Policy $bootstrapPolicy -Issue $null -Record $record `
                    -Kind coverage -OutputDirectory $TestDrive -PublicationJournalPath $publicationPath -Apply } | Should -Throw
            }
            Should -Invoke Invoke-ScheduledGitHubApi -Times 1 -Exactly -ParameterFilter {
                $Endpoint -eq 'repos/owner/repo/issues' -and $Method -eq 'POST'
            }
            $script:hideCoverageIssue = $false
            $result = Sync-ScheduledIssue -Policy $bootstrapPolicy -Issue $null -Record $record `
                -Kind coverage -OutputDirectory $TestDrive -PublicationJournalPath $publicationPath -Apply
            $result.action | Should -Be reconciled
            $result.number | Should -Be 42
            Should -Invoke Invoke-ScheduledGitHubApi -Times 1 -Exactly -ParameterFilter {
                $Endpoint -eq 'repos/owner/repo/issues' -and $Method -eq 'POST'
            }
        }
        It 'restores uncertain coverage creation on a fresh reporter attempt before allowing writes' {
            $script:loseCoverageReply = $true; $script:hideCoverageIssue = $true
            { Sync-ScheduledIssue -Policy $bootstrapPolicy -Issue $null -Record $record `
                -Kind coverage -OutputDirectory $TestDrive -PublicationJournalPath $publicationPath -Apply } | Should -Throw
            $script:previousOutput = $TestDrive
            $freshOutput = Join-Path $TestDrive 'fresh-reporter'
            $null = New-Item -ItemType Directory -Path $freshOutput
            $script:publicationPath = Join-Path $freshOutput 'publication-123-456-789.json'
            Mock Get-ScheduledArtifact { $script:previousOutput }
            Mock Invoke-ScheduledGitHubApi {
                @{
                    id = 999; run_attempt = 1; repository = @{ id = 123 }; head_repository = @{ id = 123 }
                    path = '.github/workflows/scheduled-report.yml'; head_branch = 'main'; status = 'completed'
                }
            } -ParameterFilter { $Endpoint -eq 'repos/owner/repo/actions/runs/999/attempts/1' }
            Mock Invoke-ScheduledGitHubApi { @(@{ artifacts = @() }) } `
                -ParameterFilter { $Endpoint -eq 'repos/owner/repo/actions/runs/999/artifacts?per_page=100' }
            $savedRun = $env:GITHUB_RUN_ID; $savedAttempt = $env:GITHUB_RUN_ATTEMPT
            try {
                $env:GITHUB_RUN_ID = '999'; $env:GITHUB_RUN_ATTEMPT = '2'
                Restore-ScheduledRunPublicationState -Policy $bootstrapPolicy `
                    -Run @{ workflow_id = 456; id = 789 } -OutputDirectory $freshOutput
            } finally {
                $env:GITHUB_RUN_ID = $savedRun; $env:GITHUB_RUN_ATTEMPT = $savedAttempt
            }
            $restored = Get-Content -LiteralPath $publicationPath -Raw | ConvertFrom-Json -AsHashtable
            $restored.stage | Should -Be prepared
            $restored.coverage_creation.stage | Should -Be creating-issue
            { Sync-ScheduledIssue -Policy $bootstrapPolicy -Issue $null -Record $record `
                -Kind coverage -OutputDirectory $freshOutput -PublicationJournalPath $publicationPath -Apply } | Should -Throw
            Should -Invoke Invoke-ScheduledGitHubApi -Times 1 -Exactly -ParameterFilter {
                $Endpoint -eq 'repos/owner/repo/issues' -and $Method -eq 'POST'
            }
        }
        It 'uses a known issue number after its create response but failed readback' {
            Mock Invoke-ScheduledGitHubApi { throw [IO.IOException]::new('Readback failed.') } `
                -ParameterFilter { $Endpoint -eq 'repos/owner/repo/issues/42' }
            { Sync-ScheduledIssue -Policy $bootstrapPolicy -Issue $null -Record $record `
                -Kind coverage -OutputDirectory $TestDrive -PublicationJournalPath $publicationPath -Apply } | Should -Throw
            Mock Invoke-ScheduledGitHubApi { $script:coverageIssue } `
                -ParameterFilter { $Endpoint -eq 'repos/owner/repo/issues/42' }
            (Sync-ScheduledIssue -Policy $bootstrapPolicy -Issue $null -Record $record `
                -Kind coverage -OutputDirectory $TestDrive -PublicationJournalPath $publicationPath -Apply).number |
                Should -Be 42
            Should -Invoke Invoke-ScheduledGitHubApi -Times 1 -Exactly -ParameterFilter {
                $Endpoint -eq 'repos/owner/repo/issues' -and $Method -eq 'POST'
            }
        }
        It 'does not accept recovered coverage whose owned record changed' {
            $null = Sync-ScheduledIssue -Policy $bootstrapPolicy -Issue $null -Record $record `
                -Kind coverage -OutputDirectory $TestDrive -PublicationJournalPath $publicationPath -Apply
            $record.extra = 'newer-evidence'
            { Sync-ScheduledIssue -Policy $bootstrapPolicy -Issue $null -Record $record `
                -Kind coverage -OutputDirectory $TestDrive -PublicationJournalPath $publicationPath -Apply } | Should -Throw
            Should -Invoke Invoke-ScheduledGitHubApi -Times 1 -Exactly -ParameterFilter {
                $Endpoint -eq 'repos/owner/repo/issues' -and $Method -eq 'POST'
            }
        }
    }
}
