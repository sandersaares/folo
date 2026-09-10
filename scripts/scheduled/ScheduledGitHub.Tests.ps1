#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
# Protects the privileged reporting/health adapter: artifact extraction must resist a path-traversal
# or symlink-escaping zip regardless of what a candidate run produced, reporting must never publish
# without `-Apply`/rollout consent, and reporter-owned issue state must merge deterministically
# rather than duplicate run intake or drop evidence across repeated or incomplete executions.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
$VerbosePreference = 'Continue'

BeforeDiscovery {
    Import-Module (Join-Path $PSScriptRoot 'ScheduledGitHub.psm1') -Force
}
BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'ScheduledGitHub.psm1')
    Import-Module (Join-Path $PSScriptRoot 'ScheduledContracts.psm1')
    $script:fixtureRoot = Join-Path $PSScriptRoot "fixtures\reporter\$([guid]::NewGuid().ToString('N'))"
    $null = New-Item -ItemType Directory -Path $fixtureRoot -Force
}
AfterAll { Remove-Item -LiteralPath $fixtureRoot -Recurse -Force }

Describe 'Authoritative GitHub metadata' {
    InModuleScope ScheduledGitHub {
        BeforeAll {
            function Get-GitHubTestPolicy {
                @{
                    repository = 'folo-rs/folo'; repository_id = 850321188
                    reporter_login = 'github-actions[bot]'; worker_login = 'sandersaares'
                    rollout = @{ reporting_enabled = $false }
                    coverage = @{ max_artifact_bytes = 104857600 }
                }
            }
            function Get-GitHubTestRun {
                @{
                    id = 10; run_attempt = 2; run_number = 5; workflow_id = 100; status = 'completed'
                    name = 'Full deep validation'; path = '.github/workflows/full-deep-validation.yml'
                    head_branch = 'main'; head_sha = 'a' * 40; event = 'schedule'; conclusion = 'success'
                    repository = @{ id = 850321188; full_name = 'folo-rs/folo' }
                    head_repository = @{ id = 850321188 }
                    created_at = '2026-09-08T09:00:00Z'
                    run_started_at = '2026-09-08T10:00:00Z'; updated_at = '2026-09-08T12:00:00Z'
                }
            }
        }
        BeforeEach {
            $script:policy = Get-GitHubTestPolicy
            $run = Get-GitHubTestRun
            $script:workflow = @{ id = 100; name = $run.name; path = $run.path }
            $script:workflowEvent = @{
                action = 'completed'
                repository = @{ id = 850321188; full_name = 'folo-rs/folo'; default_branch = 'main' }
                workflow_run = Get-GitHubTestRun
            }
            Mock Invoke-ScheduledGitHubApi { throw 'Unexpected API call.' }
        }
        It 'accepts a metadata-verified approved workflow' {
            { Assert-ScheduledReportingRun -WorkflowEvent $workflowEvent -Run $run -Workflow $workflow -Policy $policy } |
                Should -Not -Throw
        }
        It 'accepts approved <Name> workflow identity for automatic and manual runs' -TestCases @(
            @{ Name = 'Full deep validation'; File = 'full-deep-validation.yml'; Trigger = 'schedule' }
            @{ Name = 'Selected deep validation'; File = 'selected-deep-validation.yml'; Trigger = 'push' }
        ) {
            param($Name, $File, $Trigger)
            $workflow.name = $Name
            $workflow.path = ".github/workflows/$File"
            foreach ($candidate in @($run, $workflowEvent.workflow_run)) {
                $candidate.name = $Name
                $candidate.path = $workflow.path
            }
            foreach ($eventName in @($Trigger, 'workflow_dispatch')) {
                $run.event = $eventName
                $workflowEvent.workflow_run.event = $eventName
                { Assert-ScheduledReportingRun $workflowEvent $run $workflow $policy } | Should -Not -Throw
            }
        }
        It 'rejects an unrecognized or wrong-family name in <Target> metadata' -TestCases @(
            @{ Target = 'event' }, @{ Target = 'run' }, @{ Target = 'workflow' }
        ) {
            param($Target)
            $candidate = switch ($Target) {
                event { $workflowEvent.workflow_run }
                run { $run }
                workflow { $workflow }
            }
            foreach ($name in @('Unrelated workflow', 'full deep validation', 'Selected deep validation')) {
                $candidate.name = $name
                { Assert-ScheduledReportingRun $workflowEvent $run $workflow $policy } | Should -Throw
            }
        }
        It 'rejects a foreign or case-mismatched path even when every name and ID agrees' {
            foreach ($path in @('.github/workflows/foreign.yml', '.github/workflows/Full-deep-validation.yml')) {
                $workflow.path = $path
                $run.path = $path
                $workflowEvent.workflow_run.path = $path
                { Assert-ScheduledReportingRun $workflowEvent $run $workflow $policy } | Should -Throw
            }
        }
        It 'rejects mismatched and invalid current workflow API IDs' {
            foreach ($id in @(0, 999)) {
                $workflow.id = $id
                { Assert-ScheduledReportingRun $workflowEvent $run $workflow $policy } | Should -Throw
            }
        }
        It 'uses the approved path to reject the wrong automatic event for <Name>' -TestCases @(
            @{ Name = 'Full deep validation'; File = 'full-deep-validation.yml'; Trigger = 'push' }
            @{ Name = 'Selected deep validation'; File = 'selected-deep-validation.yml'; Trigger = 'schedule' }
        ) {
            param($Name, $File, $Trigger)
            $workflow.name = $Name
            $workflow.path = ".github/workflows/$File"
            foreach ($candidate in @($run, $workflowEvent.workflow_run)) {
                $candidate.name = $Name
                $candidate.path = $workflow.path
                $candidate.event = $Trigger
            }
            { Assert-ScheduledReportingRun $workflowEvent $run $workflow $policy } | Should -Throw
        }
        It 'rejects fork branch workflow-ID path attempt and source mismatches' {
            foreach ($field in @('head_sha', 'run_attempt', 'workflow_id', 'path', 'head_branch')) {
                $changed = Get-GitHubTestRun
                $changed[$field] = if ($field -in @('run_attempt', 'workflow_id')) { 999 } else { 'wrong' }
                { Assert-ScheduledReportingRun -WorkflowEvent $workflowEvent -Run $changed -Workflow $workflow -Policy $policy } |
                    Should -Throw
            }
            $run.head_repository.id = 123
            { Assert-ScheduledReportingRun -WorkflowEvent $workflowEvent -Run $run -Workflow $workflow -Policy $policy } |
                Should -Throw
        }
        It 'uses compare API ancestry semantics rather than commit-message inference' {
            Mock Invoke-ScheduledGitHubApi { @{ status = 'ahead' } }
            Test-ScheduledGitHubAncestor 'folo-rs/folo' ('a' * 40) ('b' * 40) | Should -BeTrue
            Mock Invoke-ScheduledGitHubApi { @{ status = 'diverged' } }
            Test-ScheduledGitHubAncestor 'folo-rs/folo' ('a' * 40) ('b' * 40) | Should -BeFalse
        }
        It 'requires default-branch reporting identity and a reachable checkout before writes' {
            $names = @('GITHUB_EVENT_NAME', 'GITHUB_REPOSITORY', 'GITHUB_REPOSITORY_ID', 'GITHUB_WORKFLOW_REF')
            $saved = @{}
            foreach ($name in $names) { $saved[$name] = [Environment]::GetEnvironmentVariable($name) }
            try {
                $env:GITHUB_EVENT_NAME = 'workflow_run'
                $env:GITHUB_REPOSITORY = $policy.repository
                $env:GITHUB_REPOSITORY_ID = [string]$policy.repository_id
                $env:GITHUB_WORKFLOW_REF = "$($policy.repository)/.github/workflows/scheduled-report.yml@refs/heads/main"
                { Assert-ScheduledWriteController $policy ('a' * 40) ('a' * 40) } | Should -Not -Throw
                $env:GITHUB_WORKFLOW_REF = "$($policy.repository)/.github/workflows/scheduled-report.yml@refs/heads/candidate"
                { Assert-ScheduledWriteController $policy ('a' * 40) ('a' * 40) } | Should -Throw
                $env:GITHUB_WORKFLOW_REF = "$($policy.repository)/.github/workflows/scheduled-report.yml@refs/heads/main"
                Mock Invoke-ScheduledGitHubApi { @{ status = 'diverged' } }
                { Assert-ScheduledWriteController $policy ('b' * 40) ('a' * 40) } | Should -Throw
            } finally {
                foreach ($name in $names) { Set-Item -LiteralPath "Env:$name" -Value $saved[$name] }
            }
        }
        It 'never downloads an old attempt expired oversized or foreign artifact' {
            Mock Save-ScheduledArtifactArchive {}
            Mock Expand-ScheduledArtifact {}
            $artifact = @{
                id = 20; name = 'scheduled-plan-10-2'; expired = $false; size_in_bytes = 500
                created_at = '2026-09-08T11:00:00Z'
                workflow_run = @{ id = 10; repository_id = 850321188; head_repository_id = 850321188; head_sha = 'a' * 40 }
            }

            foreach ($field in @('name', 'expired', 'size_in_bytes', 'created_at')) {
                $changed = $artifact.Clone()
                $changed[$field] = switch ($field) {
                    name { 'scheduled-plan-10-1' }
                    expired { $true }
                    size_in_bytes { 104857601 }
                    created_at { '2026-09-07T00:00:00Z' }
                }
                { Get-ScheduledArtifact $run @($changed) 'scheduled-plan-10-2' $policy '.' } | Should -Throw
            }
            $artifact.workflow_run.head_repository_id = 1
            { Get-ScheduledArtifact $run @($artifact) 'scheduled-plan-10-2' $policy '.' } | Should -Throw
            Should -Invoke Save-ScheduledArtifactArchive -Times 0
        }
        It 'accepts exactly one current attempt artifact and passes bounded download parameters' {
            Mock Save-ScheduledArtifactArchive {}
            Mock Expand-ScheduledArtifact {}
            $artifact = @{
                id = 20; name = 'scheduled-plan-10-2'; expired = $false; size_in_bytes = 500
                created_at = '2026-09-08T11:00:00Z'
                workflow_run = @{ id = 10; repository_id = 850321188; head_repository_id = 850321188; head_sha = 'a' * 40 }
            }
            Get-ScheduledArtifact $run @($artifact) 'scheduled-plan-10-2' $policy '.' | Should -Not -BeNullOrEmpty
            Should -Invoke Save-ScheduledArtifactArchive -Times 1 -ParameterFilter { $ArtifactId -eq 20 -and $MaxBytes -eq 104857600 }
            { Get-ScheduledArtifact $run @($artifact, $artifact) 'scheduled-plan-10-2' $policy '.' } | Should -Throw
        }
        It 'reads all issue pages and excludes human-owned issues and PRs' {
            Mock Invoke-ScheduledGitHubApi {
                ,@(
                    @(@{ number = 1; user = @{ login = 'human' } }),
                    @(@{ number = 2; user = @{ login = 'github-actions[bot]' } },
                        @{ number = 3; user = @{ login = 'github-actions[bot]' }; pull_request = @{} })
                )
            }
            $issues = @(Get-ScheduledOwnedIssue $policy scheduled-coverage)
            $issues.Count | Should -Be 1
            $issues[0].number | Should -Be 2
            Should -Invoke Invoke-ScheduledGitHubApi -Times 1 -ParameterFilter { $Paginate }
        }
    }
}

Describe 'Reporter rerun publication recovery' {
    InModuleScope ScheduledGitHub {
        BeforeEach {
            $script:savedRunId = $env:GITHUB_RUN_ID
            $script:savedAttempt = $env:GITHUB_RUN_ATTEMPT
            $env:GITHUB_RUN_ID = '999'; $env:GITHUB_RUN_ATTEMPT = '2'
            $script:recoveryPolicy = @{ repository = 'owner/repo'; repository_id = 123 }
            $script:sourceRun = @{ workflow_id = 456; id = 789 }
            $caseRoot = Join-Path $TestDrive ([guid]::NewGuid().ToString('N'))
            $script:previousDirectory = Join-Path $caseRoot 'previous'
            $script:currentDirectory = Join-Path $caseRoot 'current'
            $null = New-Item -ItemType Directory -Path $previousDirectory,$currentDirectory -Force
            $script:previousRun = @{
                id = 999; run_attempt = 1; repository = @{ id = 123 }; head_repository = @{ id = 123 }
                path = '.github/workflows/scheduled-report.yml'; head_branch = 'main'; status = 'completed'
            }
            Mock Invoke-ScheduledGitHubApi {
                if ($Endpoint.EndsWith('/attempts/1')) { return $previousRun }
                return @(@{ artifacts = @() })
            }
            Mock Get-ScheduledArtifact { $previousDirectory }
        }
        AfterEach {
            $env:GITHUB_RUN_ID = $savedRunId
            $env:GITHUB_RUN_ATTEMPT = $savedAttempt
        }
        It 'restores prior write uncertainty before a fresh runner can publish' {
            $name = 'publication-123-456-789.json'
            '{"schema_version":1,"stage":"posting-page"}' | Set-Content -LiteralPath (Join-Path $previousDirectory $name)
            Restore-ScheduledRunPublicationState -Policy $recoveryPolicy -Run $sourceRun -OutputDirectory $currentDirectory
            (Get-Content -LiteralPath (Join-Path $currentDirectory $name) -Raw | ConvertFrom-Json).stage |
                Should -BeExactly 'posting-page'
            Should -Invoke Get-ScheduledArtifact -Times 1 -ParameterFilter {
                $Run.id -eq 999 -and $Run.run_attempt -eq 1 -and $Name -eq 'scheduled-report-999-1'
            }
        }
        It 'blocks missing recovery evidence instead of assuming the previous writer did nothing' {
            Mock Get-ScheduledArtifact { throw [IO.IOException]::new('Recovery artifact expired.') }
            {
                Restore-ScheduledRunPublicationState -Policy $recoveryPolicy -Run $sourceRun -OutputDirectory $currentDirectory
            } | Should -Throw '*Recovery artifact expired*'
        }
        It 'requires a journal when the prior report was incomplete or created a run issue' -TestCases @(
            @{ Status = 'incomplete'; Intake = @{} }
            @{ Status = 'reported'; Intake = @{ number = 41 } }
        ) {
            param($Status, $Intake)
            @{ status = $Status; run_intake = $Intake } | ConvertTo-Json |
                Set-Content -LiteralPath (Join-Path $previousDirectory 'report.json')
            {
                Restore-ScheduledRunPublicationState -Policy $recoveryPolicy -Run $sourceRun -OutputDirectory $currentDirectory
            } | Should -Throw '*no recoverable publication journal*'
        }
        It 'allows a completed no-issue report to have no publication journal' {
            '{"status":"passed","run_intake":{"requires_triage":false}}' |
                Set-Content -LiteralPath (Join-Path $previousDirectory 'report.json')
            {
                Restore-ScheduledRunPublicationState -Policy $recoveryPolicy -Run $sourceRun -OutputDirectory $currentDirectory
            } | Should -Not -Throw
        }
        It 'rejects recovery artifacts from an untrusted workflow' {
            $previousRun.path = '.github/workflows/standard-validation.yml'
            {
                Restore-ScheduledRunPublicationState -Policy $recoveryPolicy -Run $sourceRun -OutputDirectory $currentDirectory
            } | Should -Throw '*not the trusted reporter workflow*'
            Should -Invoke Get-ScheduledArtifact -Times 0
        }
    }
}

Describe 'Archive extraction boundaries' {
    BeforeAll {
        function Write-ReportTestZip($path, $names, $contents = 'test') {
            $zip = [IO.Compression.ZipFile]::Open($path, [IO.Compression.ZipArchiveMode]::Create)
            try {
                foreach ($name in $names) {
                    $entry = $zip.CreateEntry($name)
                    $writer = [IO.StreamWriter]::new($entry.Open())
                    try { $writer.Write($contents) } finally { $writer.Dispose() }
                }
            } finally { $zip.Dispose() }
        }
    }
    It 'extracts ordinary JSON and raw logs without executing them' {
        $archive = Join-Path $fixtureRoot 'safe.zip'
        $destination = Join-Path $fixtureRoot 'safe'
        Write-ReportTestZip $archive @('plan.json', 'nested/check.stdout') '$(throw "never execute")'
        Expand-ScheduledArtifact $archive $destination 10000
        Get-Content -LiteralPath (Join-Path $destination 'plan.json') -Raw | Should -BeExactly '$(throw "never execute")'
    }
    It 'rejects traversal absolute drive alternate stream and case aliases before extraction' {
        $cases = @(
            @('../escape.json'), @('/absolute.json'), @('C:\outside.json'), @('file:stream'),
            @('a/../../escape'), @('file.json', 'FILE.json'), @('a./b')
        )
        $index = 0
        foreach ($names in $cases) {
            $archive = Join-Path $fixtureRoot "bad-$index.zip"
            $destination = Join-Path $fixtureRoot "bad-$index"
            Write-ReportTestZip $archive $names
            { Expand-ScheduledArtifact $archive $destination 10000 } | Should -Throw
            Test-Path -LiteralPath $destination | Should -BeFalse
            $index++
        }
    }
    It 'rejects symlinks and expanded size overflow' {
        $archive = Join-Path $fixtureRoot 'link.zip'
        $zip = [IO.Compression.ZipFile]::Open($archive, [IO.Compression.ZipArchiveMode]::Create)
        try {
            $entry = $zip.CreateEntry('link')
            $entry.ExternalAttributes = 0xA0000000 # Unix symlink type in the upper attributes.
        } finally { $zip.Dispose() }
        { Expand-ScheduledArtifact $archive (Join-Path $fixtureRoot 'link') 10000 } | Should -Throw
        $archive = Join-Path $fixtureRoot 'large.zip'
        Write-ReportTestZip $archive @('large.txt') ('x' * 10000)
        { Expand-ScheduledArtifact $archive (Join-Path $fixtureRoot 'large') 1000 } | Should -Throw
    }

    Describe 'Wired reporting with mocked GitHub transport' {
        InModuleScope ScheduledGitHub {
            BeforeAll {
                $script:reporterTestRoot = Join-Path $PSScriptRoot "fixtures\reporter\$([guid]::NewGuid().ToString('N'))"
                $null = New-Item -ItemType Directory -Path $reporterTestRoot -Force
                $script:runIntakeImplementation = (Get-Command Sync-ScheduledRunIntake).ScriptBlock
            }
            AfterAll { Remove-Item -LiteralPath $reporterTestRoot -Recurse -Force }
            BeforeEach {
                $script:caseRoot = Join-Path $reporterTestRoot ([guid]::NewGuid().ToString('N'))
                $script:planRoot = Join-Path $caseRoot 'plan'
                $null = New-Item -ItemType Directory -Path $planRoot -Force
                $script:apiRun = @{
                    id = 10; run_attempt = 2; run_number = 5; workflow_id = 100; status = 'completed'
                    name = 'Full deep validation'; path = '.github/workflows/full-deep-validation.yml'
                    head_branch = 'main'; head_sha = 'a' * 40; event = 'schedule'; conclusion = 'success'
                    repository = @{ id = 850321188; full_name = 'folo-rs/folo' }; head_repository = @{ id = 850321188 }
                    created_at = '2026-09-08T09:00:00Z'
                    run_started_at = '2026-09-08T10:00:00Z'; updated_at = '2026-09-08T12:00:00Z'
                }
                $script:apiPolicy = @{
                    repository = 'folo-rs/folo'; repository_id = 850321188
                    reporter_login = 'github-actions[bot]'; worker_login = 'sandersaares'
                    rollout = @{
                        phase = 'staged'; hosted_execution_enabled = $false; reporting_enabled = $false
                        prerequisites = @{ native_app_canary = $false }
                    }
                    coverage = @{ max_artifact_bytes = 104857600 }
                    repair = @{ allowed_packages = @('events') }
                }
                $script:eventFile = Join-Path $caseRoot 'event.json'
                @{
                    action = 'completed'; workflow_run = $apiRun
                    repository = @{ id = 850321188; full_name = 'folo-rs/folo'; default_branch = 'main' }
                } | ConvertTo-Json -Depth 50 | Set-Content -LiteralPath $eventFile
                $script:expectedManifest = Get-ScheduledCheckManifest -SourceSha ('a' * 40) -ControllerSha ('a' * 40) -ContractDigest ('c' * 64)
                $script:apiPlan = @{
                    schema_version = 1; run_id = 10; run_attempt = 2; run_number = 5
                    planned_at = '2026-09-08T10:01:00Z'
                    manifest = $expectedManifest; decision = @{ run = $true; reason = 'forced' }
                }
                $apiPlan | ConvertTo-Json -Depth 50 | Set-Content -LiteralPath (Join-Path $planRoot 'plan.json')
                Mock Get-ScheduledPolicy { $apiPolicy }
                Mock Get-ScheduledContractDigest { 'c' * 64 }
                Mock git { $global:LASTEXITCODE = 0; 'a' * 40 }
                Mock Get-ScheduledOwnedIssue { @() }
                Mock Restore-ScheduledRunPublicationState {}
                Mock Sync-ScheduledRunIntake {
                    param($Policy, $Run, $Plan, $Manifest, $Results, $Jobs, $EvidenceGaps, $OutputDirectory,
                        [switch] $Skipped, [switch] $Apply)
                    if ($Apply) {
                        Write-ScheduledRunJournal `
                            -Path (Join-Path $OutputDirectory "publication-$($Policy.repository_id)-$($Run.workflow_id)-$($Run.id).json") `
                            -Record @{ schema_version = 1; identity = @{ repository_id = $Policy.repository_id }; stage = 'prepared' }
                    }
                    $script:capturedRunEvidence = @{
                        run = $Run; plan = $Plan; manifest = $Manifest; results = $Results
                        jobs = $Jobs; evidence_gaps = $EvidenceGaps
                    }
                    $requiresTriage = $EvidenceGaps.Count -gt 0 -or
                        (-not $Skipped -and ($Run.conclusion -cne 'success' -or
                            @($Results | Where-Object outcome -CNE passed).Count -gt 0))
                    return @{
                        requires_triage = $requiresTriage
                        actions = if ($requiresTriage) { @(@{ action = 'run-intake' }) } else { @() }
                    }
                }
                Mock Save-ScheduledGitHubResponse {
                    [IO.File]::WriteAllText($Path, 'Failed job diagnostic.')
                    return @{ bytes = 22; truncated = $false }
                }
                Mock Invoke-ScheduledGitHubApi {
                    param($Endpoint, $Method)
                    if ($Method -in @('POST', 'PATCH')) { throw 'Unexpected write.' }
                    if ($Endpoint -like '*/attempts/2') { return $apiRun }
                    if ($Endpoint -like '*/actions/workflows/100') {
                        # Workflow metadata comes from its own API lookup, not the run's name.
                        $name = if ($apiRun.path -ceq '.github/workflows/full-deep-validation.yml') {
                            'Full deep validation'
                        } else { 'Selected deep validation' }
                        return @{ id = 100; name = $name; path = $apiRun.path }
                    }
                    if ($Endpoint -like '*/git/ref/heads/main') { return @{ object = @{ sha = 'a' * 40 } } }
                    if ($Endpoint -like '*/artifacts?*') { return @{ artifacts = @() } }
                    if ($Endpoint.StartsWith('repos/folo-rs/folo/issues?state=all&labels=scheduled-run-failure')) { return @() }
                    if ($Endpoint -like '*/attempts/2/jobs?*') {
                        return @{ total_count = 1; jobs = @(@{
                            id = 77; run_id = $apiRun.id; run_attempt = $apiRun.run_attempt; head_sha = $apiRun.head_sha
                            name = 'Execution'; status = 'completed'; conclusion = $apiRun.conclusion
                            steps = @(@{ number = 1; name = 'Run checks'; status = 'completed'; conclusion = $apiRun.conclusion })
                        }) }
                    }
                    throw "Unexpected endpoint: $Endpoint"
                }
                Mock Get-ScheduledArtifact { $planRoot }
                Mock Get-ScheduledCheckResult {
                    param($Check, $RunContext)
                    $result = $RunContext.Clone()
                    $result.schema_version = 1
                    $result.check_id = $Check.id
                    $result.actual_scope = $Check
                    $result.outcome = 'passed'
                    $result.baseline = 'passed'
                    $result.exit_code = 0
                    $result.findings = @()
                    $result.summary = 'Complete'
                    $result.log_path = 'check.log'
                    return $result
                }
            }
            It 'produces a dry-run full receipt from every expected independently parsed check' {
                $report = Invoke-ScheduledReporting 'folo-rs/folo' $eventFile (Join-Path $caseRoot 'report') -Apply
                $report.problems | Should -BeNullOrEmpty
                $report.status | Should -Be passed
                $report.applied | Should -BeFalse
                $report.writes_authorized | Should -BeFalse
                $report.coverage.receipt.manifest.checks.Count | Should -Be 32
                $report.coverage.receipt.created_at | Should -Be $apiRun.created_at
                $report.coverage.receipt.workflow_id | Should -Be $apiRun.workflow_id
                $report.coverage.receipt.workflow_path | Should -BeExactly $apiRun.path
                $report.coverage.last_plan.planned_at | Should -Be ([datetimeoffset]$apiPlan.planned_at).ToString('o')
                $report.coverage.last_plan.source_sha | Should -Be $expectedManifest.source_sha
                $report.coverage.last_plan.check_contract_digest | Should -Be $expectedManifest.check_contract_digest
                $report.coverage.last_plan.run | Should -BeTrue
                (Get-ScheduledRunDecision -Manifest $expectedManifest -Coverage $report.coverage `
                    -Now ([datetimeoffset]'2026-09-08T12:00:00Z')).run | Should -BeFalse
                Should -Invoke Get-ScheduledCheckResult -Times 32
                Should -Invoke Invoke-ScheduledGitHubApi -Times 0 -ParameterFilter { $Method -in @('POST', 'PATCH') }
                Test-Path -LiteralPath (Join-Path $caseRoot 'report\report.json') | Should -BeTrue
            }
            Describe 'Manual diagnostics with explicit disabled reporting' {
                BeforeEach {
                    $script:apiPolicy = Get-Content -LiteralPath (Join-Path $PSScriptRoot 'policy.json') -Raw |
                        ConvertFrom-Json -AsHashtable
                    $apiPolicy.rollout.reporting_enabled = $false
                    $apiRun.name = 'Selected deep validation'
                    $apiRun.path = '.github/workflows/selected-deep-validation.yml'
                    $apiRun.event = 'workflow_dispatch'
                    @{
                        action = 'completed'; workflow_run = $apiRun
                        repository = @{ id = 850321188; full_name = 'folo-rs/folo'; default_branch = 'main' }
                    } | ConvertTo-Json -Depth 50 | Set-Content -LiteralPath $eventFile
                    $apiPlan.manifest = Get-ScheduledCheckManifest -SourceSha ('b' * 40) -ControllerSha ('a' * 40) `
                        -ContractDigest ('c' * 64) -Scope confirmation -Packages cpulist -CheckIds miri-ubuntu-latest
                    $apiPlan.confirmations = @()
                    $apiPlan | ConvertTo-Json -Depth 50 | Set-Content -LiteralPath (Join-Path $planRoot 'plan.json')
                    Mock Get-ScheduledOwnedIssue { throw 'Unrelated repair or coverage state must not be read.' }
                    Mock Get-ScheduledTrustedConfirmation { throw 'A diagnostic is not repair confirmation.' }
                    Mock Merge-ScheduledCoverage { throw 'A diagnostic cannot change main coverage.' }
                    Mock Sync-ScheduledIssue { throw 'A diagnostic cannot change repair or coverage issues.' }
                }

                It 'accepts cpulist Miri evidence for the exact candidate without issue or repair authority' {
                    $report = Invoke-ScheduledReporting 'folo-rs/folo' $eventFile (Join-Path $caseRoot 'manual') -Apply
                    $report.problems | Should -BeNullOrEmpty
                    $report.status | Should -Be passed
                    $report.diagnostic | Should -BeTrue
                    $report.applied | Should -BeFalse
                    $report.writes_authorized | Should -BeFalse
                    $report.ContainsKey('coverage') | Should -BeFalse
                    $report.manifest.source_sha | Should -BeExactly ('b' * 40)
                    $report.results.Count | Should -Be 1
                    $report.results[0].actual_scope.packages | Should -Be @('cpulist')
                    $saved = Get-Content -LiteralPath (Join-Path $caseRoot 'manual\report.json') -Raw | ConvertFrom-Json
                    $saved.manifest.source_sha | Should -BeExactly ('b' * 40)
                    $saved.results[0].outcome | Should -Be passed
                    Should -Invoke Get-ScheduledCheckResult -Times 1
                    Should -Invoke Get-ScheduledOwnedIssue -Times 0
                    Should -Invoke Get-ScheduledTrustedConfirmation -Times 0
                    Should -Invoke Sync-ScheduledRunIntake -Times 0
                    Should -Invoke Restore-ScheduledRunPublicationState -Times 0
                    Should -Invoke Invoke-ScheduledGitHubApi -Times 0 -ParameterFilter { $Endpoint -like '*/issues*' }
                    Should -Invoke Invoke-ScheduledGitHubApi -Times 0 -ParameterFilter { $Method -in @('POST', 'PATCH') }
                }

                It 'retains full manual evidence without creating or invalidating a main coverage receipt' {
                    $apiRun.name = 'Full deep validation'; $apiRun.path = '.github/workflows/full-deep-validation.yml'
                    @{
                        action = 'completed'; workflow_run = $apiRun
                        repository = @{ id = 850321188; full_name = 'folo-rs/folo'; default_branch = 'main' }
                    } | ConvertTo-Json -Depth 50 | Set-Content -LiteralPath $eventFile
                    $apiPlan.manifest = $expectedManifest
                    $apiPlan | ConvertTo-Json -Depth 50 | Set-Content -LiteralPath (Join-Path $planRoot 'plan.json')
                    $report = Invoke-ScheduledReporting 'folo-rs/folo' $eventFile (Join-Path $caseRoot 'full-manual') -Apply
                    $report.status | Should -Be passed
                    $report.results.Count | Should -Be 32
                    $report.ContainsKey('coverage') | Should -BeFalse
                    Should -Invoke Merge-ScheduledCoverage -Times 0
                    Should -Invoke Sync-ScheduledIssue -Times 0
                }

                It 'retains failing check evidence without attempting issue publication' {
                    Mock Get-ScheduledCheckResult {
                        param($Check, $RunContext)
                        $result = $RunContext.Clone()
                        $result.schema_version = 1; $result.check_id = $Check.id; $result.actual_scope = $Check
                        $result.outcome = 'findings'; $result.summary = 'Miri reported undefined behavior.'
                        return $result
                    }
                    $report = Invoke-ScheduledReporting 'folo-rs/folo' $eventFile (Join-Path $caseRoot 'failed-manual') -Apply
                    $report.status | Should -Be reported
                    $report.results[0].outcome | Should -Be findings
                    $report.jobs.Count | Should -Be 1
                    $report.actions | Should -BeNullOrEmpty
                    Should -Invoke Sync-ScheduledRunIntake -Times 0
                }

                It 'allows manual run intake with checked-in hosted defaults but no repair or coverage access' {
                    $script:apiPolicy = Get-Content -LiteralPath (Join-Path $PSScriptRoot 'policy.json') -Raw |
                        ConvertFrom-Json -AsHashtable
                    Mock Assert-ScheduledWriteController {}
                    Mock Get-ScheduledArtifact { throw [IO.IOException]::new('Missing check artifact.') } `
                        -ParameterFilter { $Name -like 'scheduled-result-*' }
                    $report = Invoke-ScheduledReporting 'folo-rs/folo' $eventFile (Join-Path $caseRoot 'authorized') -Apply
                    $report.status | Should -Be reported
                    $report.applied | Should -BeTrue
                    $report.run_intake.requires_triage | Should -BeTrue
                    $report.ContainsKey('coverage') | Should -BeFalse
                    Should -Invoke Assert-ScheduledWriteController -Times 1
                    Should -Invoke Restore-ScheduledRunPublicationState -Times 1
                    Should -Invoke Sync-ScheduledRunIntake -Times 1 -ParameterFilter { $Apply }
                    Should -Invoke Get-ScheduledOwnedIssue -Times 0
                    Should -Invoke Get-ScheduledTrustedConfirmation -Times 0
                    Should -Invoke Merge-ScheduledCoverage -Times 0
                    Should -Invoke Sync-ScheduledIssue -Times 0
                }

                It 'does not read issue state when reporting is enabled but Apply is absent' {
                    $apiPolicy.rollout.reporting_enabled = $true
                    Mock Get-ScheduledArtifact { throw [IO.IOException]::new('Missing check artifact.') } `
                        -ParameterFilter { $Name -like 'scheduled-result-*' }
                    $report = Invoke-ScheduledReporting 'folo-rs/folo' $eventFile (Join-Path $caseRoot 'no-apply')
                    $report.status | Should -Be incomplete
                    $report.applied | Should -BeFalse
                    $report.writes_authorized | Should -BeFalse
                    $report.jobs.Count | Should -Be 1
                    $report.problems.Count | Should -BeGreaterThan 0
                    Should -Invoke Sync-ScheduledRunIntake -Times 0
                    Should -Invoke Restore-ScheduledRunPublicationState -Times 0
                    Should -Invoke Get-ScheduledOwnedIssue -Times 0
                    Should -Invoke Invoke-ScheduledGitHubApi -Times 0 -ParameterFilter { $Endpoint -like '*/issues*' }
                }

                It 'reports incomplete evidence for <Damage> rather than accepting a manual exception' -TestCases @(
                    @{ Damage = 'skipped' }, @{ Damage = 'confirmation' }, @{ Damage = 'contract' },
                    @{ Damage = 'unknown-check' }, @{ Damage = 'empty-packages' },
                    @{ Damage = 'stale-source' }, @{ Damage = 'missing-artifact' }
                ) {
                    param($Damage)
                    switch ($Damage) {
                        'skipped' { $apiPlan.decision.run = $false }
                        'confirmation' { $apiPlan.confirmations = @(@{ finding_id = 'f' * 64 }) }
                        'contract' { $apiPlan.manifest.check_contract_digest = 'd' * 64 }
                        'unknown-check' { $apiPlan.manifest.checks[0].id = 'unknown' }
                        'empty-packages' { $apiPlan.manifest.checks[0].packages = @() }
                        'stale-source' {
                            Mock Get-ScheduledCheckResult {
                                param($Check, $RunContext)
                                $result = $RunContext.Clone()
                                $result.schema_version = 1; $result.check_id = $Check.id; $result.actual_scope = $Check
                                $result.source_sha = 'd' * 40; $result.outcome = 'passed'
                                return $result
                            }
                        }
                        'missing-artifact' {
                            Mock Get-ScheduledArtifact { throw [IO.IOException]::new('Missing check artifact.') } `
                                -ParameterFilter { $Name -like 'scheduled-result-*' }
                        }
                    }
                    $apiPlan | ConvertTo-Json -Depth 50 | Set-Content -LiteralPath (Join-Path $planRoot 'plan.json')
                    $report = Invoke-ScheduledReporting 'folo-rs/folo' $eventFile (Join-Path $caseRoot $Damage) -Apply
                    $report.status | Should -Be incomplete
                    $report.problems.Count | Should -BeGreaterThan 0
                    $report.actions | Should -BeNullOrEmpty
                    $report.ContainsKey('coverage') | Should -BeFalse
                    Test-Path -LiteralPath (Join-Path $caseRoot "$Damage\report.json") | Should -BeTrue
                    Should -Invoke Sync-ScheduledRunIntake -Times 0
                }
            }
            It 'normalizes relative report paths before supplying artifact roots to the parser' {
                $relativeOutput = [IO.Path]::GetRelativePath((Get-Location).Path, (Join-Path $caseRoot 'relative-output'))
                $report = Invoke-ScheduledReporting 'folo-rs/folo' $eventFile $relativeOutput
                $report.status | Should -Be passed
                Should -Invoke Get-ScheduledArtifact -Times 1 -ParameterFilter {
                    $Name -like 'scheduled-plan-*' -and [IO.Path]::IsPathFullyQualified($OutputDirectory)
                }
            }
            It 'rejects ambiguous authoritative coverage for automatic reporting' {
                Mock Sync-ScheduledIssue { throw 'Ambiguous coverage must not be published.' }
                Mock Get-ScheduledOwnedIssue { @(@{ number = 1 }, @{ number = 2 }) } `
                    -ParameterFilter { $Label -ceq 'scheduled-coverage' }
                $report = Invoke-ScheduledReporting 'folo-rs/folo' $eventFile (Join-Path $caseRoot 'ambiguous')
                $report.status | Should -Be incomplete
                $report.problems.Count | Should -BeGreaterThan 0
                Should -Invoke Sync-ScheduledIssue -Times 0
                Should -Invoke Get-ScheduledArtifact -Times 0
            }
            It 'serializes a setup-only failure through the real Rust intake contract' {
                $apiRun.conclusion = 'failure'
                @{
                    action = 'completed'; workflow_run = $apiRun
                    repository = @{ id = 850321188; full_name = 'folo-rs/folo'; default_branch = 'main' }
                } | ConvertTo-Json -Depth 50 | Set-Content -LiteralPath $eventFile
                Mock Get-ScheduledArtifact { throw [IO.IOException]::new('Setup produced no plan artifact.') }
                Mock Sync-ScheduledRunIntake {
                    param($Policy, $Run, $Plan, $Manifest, $Results, $Jobs, $EvidenceGaps, $Artifacts,
                        $TransientPaths, $OutputDirectory, $Api, [switch] $Skipped, [switch] $Apply)
                    & $script:runIntakeImplementation @PSBoundParameters
                }
                $report = Invoke-ScheduledReporting 'folo-rs/folo' $eventFile (Join-Path $caseRoot 'setup-only')
                $report.status | Should -BeExactly 'reported'
                $report.applied | Should -BeFalse
                $report.coverage.receipt | Should -BeNullOrEmpty
                $report.run_intake.actions[0].payload.labels | Should -Be @('scheduled-run-failure')
                $comments = @(
                    $id = 100
                    foreach ($page in $report.run_intake.planned_pages) {
                        @{ id = $id; body = $page.body }
                        $id++
                    }
                )
                $restored = Invoke-ScheduledRunRecord @{
                    op = 'restore'; identity = @{ repository_id = 850321188; workflow_id = 100; run_id = 10 }
                    comments = $comments
                }
                $evidence = $restored.record.revisions[0].evidence.attempt
                $evidence.plan | Should -BeNullOrEmpty
                $evidence.jobs[0].steps[0].name | Should -BeExactly 'Run checks'
                $evidence.jobs[0].log.excerpt | Should -BeExactly 'Failed job diagnostic.'
                $evidence.evidence_gaps | Should -Contain 'Setup produced no plan artifact.'
                Should -Invoke Invoke-ScheduledGitHubApi -Times 0 -ParameterFilter { $Method -in @('POST', 'PATCH') }
            }
            It 'routes enabled reporting without requiring hosted execution or the native App pilot' {
                $apiPolicy.rollout.reporting_enabled = $true
                Mock Assert-ScheduledWriteController {}
                Mock Invoke-ScheduledGitHubApi {
                    @(@{ name = 'scheduled-coverage' }, @{ name = 'scheduled-health' })
                } -ParameterFilter { $Endpoint -like '*/labels?*' }
                Mock Invoke-ScheduledGitHubApi {
                    $script:createdCoverage = @{
                        number = 42; user = @{ login = 'github-actions[bot]' }; body = $Body.body
                    }
                    @{ number = 42 }
                } -ParameterFilter { $Method -ceq 'POST' }
                Mock Invoke-ScheduledGitHubApi { $script:createdCoverage } `
                    -ParameterFilter { $Endpoint -ceq 'repos/folo-rs/folo/issues/42' }
                $report = Invoke-ScheduledReporting 'folo-rs/folo' $eventFile (Join-Path $caseRoot 'report') -Apply
                $report.problems | Should -BeNullOrEmpty
                $report.applied | Should -BeTrue
                $report.writes_authorized | Should -BeTrue
                Should -Invoke Assert-ScheduledWriteController -Times 1
                Should -Invoke Invoke-ScheduledGitHubApi -Times 1 -ParameterFilter { $Method -ceq 'POST' }
            }
            It 'invalidates coverage on missing raw evidence without manufacturing a clean result' {
                Mock Get-ScheduledArtifact { throw [IO.IOException]::new('Unavailable artifact.') } -ParameterFilter { $Name -like 'scheduled-result-*' }
                $report = Invoke-ScheduledReporting 'folo-rs/folo' $eventFile (Join-Path $caseRoot 'report')
                $report.status | Should -Be reported
                $report.run_intake.requires_triage | Should -BeTrue
                $report.coverage.receipt | Should -BeNullOrEmpty
                $report.coverage.invalidation.outcome | Should -Be incomplete
                Should -Invoke Get-ScheduledCheckResult -Times 0
            }
            It 'rejects old-attempt plans and candidate-controlled reduced full manifests' {
                $apiPlan.run_attempt = 1
                $apiPlan.manifest.checks = @($apiPlan.manifest.checks[0])
                $apiPlan | ConvertTo-Json -Depth 50 | Set-Content -LiteralPath (Join-Path $planRoot 'plan.json')
                $report = Invoke-ScheduledReporting 'folo-rs/folo' $eventFile (Join-Path $caseRoot 'report')
                $report.status | Should -Be reported
                $report.coverage.receipt | Should -BeNullOrEmpty
                Should -Invoke Get-ScheduledCheckResult -Times 0
            }
            It 'never creates a coverage success after a skipped plan' {
                $apiPlan.decision = @{ run = $false; reason = 'not-run-unchanged' }
                $apiPlan | ConvertTo-Json -Depth 50 | Set-Content -LiteralPath (Join-Path $planRoot 'plan.json')
                $report = Invoke-ScheduledReporting 'folo-rs/folo' $eventFile (Join-Path $caseRoot 'report')
                $report.status | Should -Be not-run
                $report.coverage.receipt | Should -BeNullOrEmpty
                Should -Invoke Get-ScheduledCheckResult -Times 0
            }
            It 'preserves coverage for no-work selected deep validation without package admission' {
                $initial = Invoke-ScheduledReporting 'folo-rs/folo' $eventFile (Join-Path $caseRoot 'initial')
                $script:existingCoverage = @{
                    number = 99; state = 'open'; user = @{ login = 'github-actions[bot]' }
                    body = Write-ScheduledRecord $initial.coverage coverage
                }
                Mock Get-ScheduledOwnedIssue { @($existingCoverage) } -ParameterFilter { $Label -ceq 'scheduled-coverage' }
                Mock Invoke-ScheduledGitHubApi { $existingCoverage } -ParameterFilter { $Endpoint -ceq 'repos/folo-rs/folo/issues/99' }
                $apiRun.name = 'Selected deep validation'; $apiRun.path = '.github/workflows/selected-deep-validation.yml'
                $apiRun.event = 'push'; $apiPolicy.repair.allowed_packages = @()
                $apiRun.workflow_id = 200
                $apiRun.created_at = '2026-09-10T09:00:00Z'
                $apiRun.run_started_at = '2026-09-10T10:00:00Z'
                $apiRun.updated_at = '2026-09-10T12:00:00Z'
                $apiPlan.planned_at = '2026-09-10T10:01:00Z'
                Mock Invoke-ScheduledGitHubApi {
                    @{ id = 200; name = 'Selected deep validation'; path = '.github/workflows/selected-deep-validation.yml' }
                } -ParameterFilter { $Endpoint -like '*/actions/workflows/200' }
                @{
                    action = 'completed'; workflow_run = $apiRun
                    repository = @{ id = 850321188; full_name = 'folo-rs/folo'; default_branch = 'main' }
                } | ConvertTo-Json -Depth 50 | Set-Content -LiteralPath $eventFile
                $apiPlan.manifest = Get-ScheduledCheckManifest -SourceSha ('a' * 40) -ControllerSha ('a' * 40) `
                    -ContractDigest ('c' * 64) -Scope confirmation
                foreach ($reason in @('staged', 'no-pending-main-confirmation')) {
                    $apiPlan.decision = @{ run = $false; reason = $reason }
                    $apiPlan | ConvertTo-Json -Depth 50 | Set-Content -LiteralPath (Join-Path $planRoot 'plan.json')
                    $report = Invoke-ScheduledReporting 'folo-rs/folo' $eventFile (Join-Path $caseRoot $reason)
                    $report.problems | Should -BeNullOrEmpty
                    $report.status | Should -Be not-run
                    $report.coverage.invalidation | Should -BeNullOrEmpty
                    (Get-ScheduledDigest $report.coverage.receipt) | Should -BeExactly (Get-ScheduledDigest $initial.coverage.receipt)
                    $persisted = Read-ScheduledRecord $existingCoverage.body coverage
                    (Get-ScheduledDigest $report.coverage.last_plan) | Should -BeExactly (Get-ScheduledDigest $persisted.last_plan)
                    (Get-ScheduledDigest $report.coverage.planning) | Should -BeExactly (Get-ScheduledDigest $persisted.planning)
                    $health = Get-ScheduledHealth -Coverage $report.coverage -Manifest $expectedManifest `
                        -Planning $report.coverage.planning -Now ([datetimeoffset]$apiRun.updated_at) -RepairDisabled
                    $health.components.planning.status | Should -Be unavailable
                    $health.components.coverage.status | Should -Be fresh
                }
                Should -Invoke Get-ScheduledCheckResult -Times 32
            }
            It 'replaces a selected planning checkpoint with a full-workflow checkpoint' {
                $initial = Invoke-ScheduledReporting 'folo-rs/folo' $eventFile (Join-Path $caseRoot 'initial-full')
                $initial.coverage.last_plan.workflow_path = '.github/workflows/selected-deep-validation.yml'
                $initial.coverage.last_plan.workflow_id = 200
                $initial.coverage.last_plan.created_at = '2026-09-10T10:00:00Z'
                $initial.coverage.last_plan.planned_at = '2026-09-10T11:00:00Z'
                $script:existingCoverage = @{
                    number = 99; state = 'open'; user = @{ login = 'github-actions[bot]' }
                    body = Write-ScheduledRecord $initial.coverage coverage
                }
                Mock Get-ScheduledOwnedIssue { @($existingCoverage) } -ParameterFilter { $Label -ceq 'scheduled-coverage' }
                Mock Invoke-ScheduledGitHubApi { $existingCoverage } -ParameterFilter { $Endpoint -ceq 'repos/folo-rs/folo/issues/99' }
                $report = Invoke-ScheduledReporting 'folo-rs/folo' $eventFile (Join-Path $caseRoot 'full')
                $report.status | Should -Be passed
                $report.coverage.last_plan.workflow_path | Should -BeExactly '.github/workflows/full-deep-validation.yml'
                $report.coverage.last_plan.workflow_id | Should -Be 100
                [datetimeoffset]$report.coverage.last_plan.planned_at | Should -Be ([datetimeoffset]$apiPlan.planned_at)
            }
            It 'retains run evidence and coverage when an existing repair confirmation is stale' {
                $script:staleIssue = @{
                    number = 42; state = 'open'; user = @{ login = 'github-actions[bot]' }
                    body = Write-ScheduledRecord @{
                        schema_version = 1; repository = 'folo-rs/folo'; repository_id = 850321188
                        finding_id = 'f' * 64; generation = 1; status = 'open'
                        package = 'events'; check_id = 'miri-windows-latest'
                    } reporter
                }
                Mock Get-ScheduledOwnedIssue { @($staleIssue) } -ParameterFilter { $Label -ceq 'scheduled-finding' }
                Mock Get-ScheduledTrustedConfirmation { throw [FormatException]::new('Retired executor.') }
                Mock Get-ScheduledCheckResult {
                    param($Check, $RunContext)
                    $result = $RunContext.Clone()
                    $result.schema_version = 1; $result.check_id = $Check.id; $result.actual_scope = $Check
                    $result.outcome = 'findings'
                    $result.findings = @(@{
                        identity = @{
                            kind = 'miri'; package = 'events'; platform = $Check.platform; path = ''
                            function = ''; mutation = ''; test = 'new-defect'; seed = ''; flags = @()
                        }
                        summary = 'Independent defect'; replay = $Check
                    })
                    return $result
                } -ParameterFilter { $Check.id -ceq 'miri-ubuntu-latest' }
                $report = Invoke-ScheduledReporting 'folo-rs/folo' $eventFile (Join-Path $caseRoot 'stale')
                $report.status | Should -Be reported
                $report.problems | Should -Contain 'Issue 42 confirmation: Retired executor.'
                $report.coverage.invalidation.outcome | Should -Be findings
                $report.run_intake.requires_triage | Should -BeTrue
                @($capturedRunEvidence.results.findings | Where-Object summary -CEQ 'Independent defect').Count | Should -Be 1
                @($report.actions | Where-Object { $_.ContainsKey('endpoint') -and $_.endpoint -ceq 'repos/folo-rs/folo/issues/42' }).Count | Should -Be 0
            }
            It 'persists explicit incomplete output when triggering metadata is untrusted' {
                $apiRun.head_repository.id = 999
                $report = Invoke-ScheduledReporting 'folo-rs/folo' $eventFile (Join-Path $caseRoot 'report')
                $report.status | Should -Be incomplete
                $report.actions.Count | Should -Be 0
                Should -Invoke Get-ScheduledArtifact -Times 0
            }
            It 'retains reparsed <ResultOutcome> evidence for AI triage when the workflow is <Conclusion>' -TestCases @(
                @{ Conclusion = 'success'; ResultOutcome = 'findings' }
                @{ Conclusion = 'failure'; ResultOutcome = 'findings' }
                @{ Conclusion = 'failure'; ResultOutcome = 'incomplete' }
                @{ Conclusion = 'failure'; ResultOutcome = 'execution-error' }
            ) {
                param($Conclusion, $ResultOutcome)
                $script:parsedOutcome = $ResultOutcome
                $apiRun.conclusion = $Conclusion
                @{
                    action = 'completed'; workflow_run = $apiRun
                    repository = @{ id = 850321188; full_name = 'folo-rs/folo'; default_branch = 'main' }
                } | ConvertTo-Json -Depth 50 | Set-Content -LiteralPath $eventFile
                Mock Get-ScheduledCheckResult {
                    param($Check, $RunContext)
                    $result = $RunContext.Clone()
                    $result.schema_version = 1; $result.check_id = $Check.id; $result.actual_scope = $Check
                    $result.outcome = $parsedOutcome
                    $result.findings = @(@{
                        identity = @{
                            kind = 'miri'; package = 'events'; platform = $Check.platform; path = ''
                            function = ''; mutation = ''; test = 'example'; seed = ''; flags = @()
                        }
                        summary = 'Example failure'; replay = $Check
                    })
                    return $result
                } -ParameterFilter { $Check.id -ceq 'miri-ubuntu-latest' }
                $report = Invoke-ScheduledReporting 'folo-rs/folo' $eventFile (Join-Path $caseRoot 'report')
                if ($ResultOutcome -eq 'findings') {
                    $report.problems | Should -BeNullOrEmpty
                    $report.status | Should -Be reported
                    $report.coverage.invalidation.outcome | Should -Be findings
                } else {
                    $report.status | Should -Be reported
                    $report.coverage.invalidation.outcome | Should -Be incomplete
                }
                $report.coverage.receipt | Should -BeNullOrEmpty
                $report.run_intake.requires_triage | Should -BeTrue
                $capturedRunEvidence.run.run_attempt | Should -Be 2
                $capturedRunEvidence.run.path | Should -BeExactly $apiRun.path
                $capturedRunEvidence.jobs.Count | Should -Be 1
                $failure = @($capturedRunEvidence.results | Where-Object check_id -CEQ 'miri-ubuntu-latest')[0]
                $failure.actual_scope.kind | Should -BeExactly 'miri'
                $failure.findings[0].summary | Should -BeExactly 'Example failure'
                @($report.actions | Where-Object {
                    $_.ContainsKey('payload') -and $_.payload.ContainsKey('labels') -and
                    $_.payload.labels -contains 'scheduled-finding'
                }).Count | Should -Be 0
            }
            It 'never certifies apparently green artifacts from a failed workflow' {
                $apiRun.conclusion = 'failure'
                @{
                    action = 'completed'; workflow_run = $apiRun
                    repository = @{ id = 850321188; full_name = 'folo-rs/folo'; default_branch = 'main' }
                } | ConvertTo-Json -Depth 50 | Set-Content -LiteralPath $eventFile
                $report = Invoke-ScheduledReporting 'folo-rs/folo' $eventFile (Join-Path $caseRoot 'report')
                $report.status | Should -Be reported
                $report.coverage.receipt | Should -BeNullOrEmpty
                $report.coverage.invalidation.outcome | Should -Be incomplete
                $report.run_intake.requires_triage | Should -BeTrue
            }
            It 'consumes declared confirmations and persists confirmed needs-human and retry merge dispositions' {
                $apiRun.name = 'Selected deep validation'
                $apiRun.path = '.github/workflows/selected-deep-validation.yml'
                $apiRun.event = 'push'
                @{
                    action = 'completed'; workflow_run = $apiRun
                    repository = @{ id = 850321188; full_name = 'folo-rs/folo'; default_branch = 'main' }
                } | ConvertTo-Json -Depth 50 | Set-Content -LiteralPath $eventFile
                $apiPlan.manifest = Get-ScheduledCheckManifest -SourceSha ('a' * 40) -ControllerSha ('a' * 40) `
                    -ContractDigest ('c' * 64) -Scope confirmation -Packages @('events') -CheckIds @('miri-ubuntu-latest')
                $apiPlan.confirmations = @(@{
                    issue_number = 42; finding_id = 'f' * 64; generation = 1; pr_number = 3
                    merge_commit_sha = 'a' * 40; source_sha = 'a' * 40
                    packages = @('events'); check_ids = @('miri-ubuntu-latest'); worker = @{}
                })
                $apiPlan | ConvertTo-Json -Depth 50 | Set-Content -LiteralPath (Join-Path $planRoot 'plan.json')
                $record = @{
                    schema_version = 1; repository = 'folo-rs/folo'; repository_id = 850321188
                    finding_id = 'f' * 64; generation = 1; status = 'open'; package = 'events'
                    source_sha = 'a' * 40; controller_sha = 'a' * 40; check_contract_digest = 'c' * 64
                    check_id = 'miri-ubuntu-latest'; check_kind = 'miri'; confirmation = $null
                    observation = @{
                        run_id = 9; run_attempt = 1; run_number = 4; workflow_id = 100
                        created_at = '2026-09-08T08:00:00Z'; run_started_at = '2026-09-08T08:00:00Z'
                        completed_at = '2026-09-08T09:00:00Z'; outcome = 'findings'
                    }
                    evidence = @{ replay = @{ test_filter = 'example' }; summary = 'Original defect'; manifest = @{} }
                }
                $script:confirmationIssue = @{
                    number = 42; state = 'open'; body = Write-ScheduledRecord $record reporter
                    user = @{ login = 'github-actions[bot]' }
                }
                Mock Get-ScheduledOwnedIssue { @($confirmationIssue) } -ParameterFilter { $Label -ceq 'scheduled-finding' }
                Mock Invoke-ScheduledGitHubApi { $confirmationIssue } -ParameterFilter { $Endpoint -ceq 'repos/folo-rs/folo/issues/42' }
                Mock Get-ScheduledTrustedConfirmation { $trustedDisposition.Clone() }
                Mock Get-ScheduledCheckResult {
                    param($Check, $RunContext)
                    $result = $RunContext.Clone()
                    $result.schema_version = 1; $result.check_id = $Check.id; $result.actual_scope = $Check
                    $result.outcome = $confirmationOutcome; $result.findings = @()
                    return $result
                }
                foreach ($disposition in @('confirmed', 'needs-human', 'failed', 'retry')) {
                    $script:confirmationOutcome = switch ($disposition) {
                        failed { 'findings' }
                        retry { 'incomplete' }
                        default { 'passed' }
                    }
                    $apiRun.conclusion = if ($disposition -ceq 'failed') { 'failure' } else { 'success' }
                    $eventValue = Get-Content -LiteralPath $eventFile -Raw | ConvertFrom-Json -AsHashtable
                    $eventValue.workflow_run.conclusion = $apiRun.conclusion
                    $eventValue | ConvertTo-Json -Depth 50 | Set-Content -LiteralPath $eventFile
                    $script:trustedDisposition = @{
                        authoritative = $true; generation = 1; merge_commit_sha = 'a' * 40; pr_number = 3
                        scope_complete = $disposition -cne 'retry'; successful = $disposition -cnotin @('failed', 'retry')
                        explained = $disposition -ceq 'confirmed'; status = $disposition
                    }
                    $report = Invoke-ScheduledReporting 'folo-rs/folo' $eventFile (Join-Path $caseRoot $disposition)
                    $report.problems | Should -BeNullOrEmpty
                    $action = @($report.actions | Where-Object { $_.ContainsKey('endpoint') -and $_.endpoint -ceq 'repos/folo-rs/folo/issues/42' })
                    $action.Count | Should -Be 1
                    $updated = Read-ScheduledRecord $action[0].payload.body reporter
                    $updated.confirmation.status | Should -BeExactly $disposition
                    $updated.confirmation.merge_commit_sha | Should -BeExactly ('a' * 40)
                    $expectedState = if ($disposition -cin @('failed', 'retry')) { 'open' } else { $disposition }
                    $updated.status | Should -BeExactly $expectedState
                    $updated.confirmation.scope_complete | Should -Be ($disposition -cne 'retry')
                }
                Should -Invoke Get-ScheduledTrustedConfirmation -Times 4 -ParameterFilter { $Declaration.issue_number -eq 42 }
                $script:confirmationOutcome = 'passed'
                $script:trustedDisposition.status = 'confirmed'
                $script:trustedDisposition.scope_complete = $true
                $script:trustedDisposition.successful = $true
                $script:trustedDisposition.explained = $true
                $script:apiRun.name = 'Full deep validation'
                $script:apiRun.path = '.github/workflows/full-deep-validation.yml'
                $script:apiRun.event = 'schedule'
                $script:apiRun.conclusion = 'failure'
                Mock Get-ScheduledCheckResult {
                    param($Check, $RunContext)
                    $result = $RunContext.Clone()
                    $result.schema_version = 1; $result.check_id = $Check.id; $result.actual_scope = $Check
                    $result.outcome = 'findings'; $result.findings = @(@{ summary = 'Independent failure'; replay = $Check })
                    return $result
                } -ParameterFilter { $Check.id -ceq 'miri-windows-latest' }
                @{
                    action = 'completed'; workflow_run = $apiRun
                    repository = @{ id = 850321188; full_name = 'folo-rs/folo'; default_branch = 'main' }
                } | ConvertTo-Json -Depth 50 | Set-Content -LiteralPath $eventFile
                $script:apiPlan.manifest = Get-ScheduledCheckManifest -SourceSha ('a' * 40) `
                    -ControllerSha ('a' * 40) -ContractDigest $apiPlan.manifest.check_contract_digest
                $script:apiPlan.confirmations = @()
                $apiPlan | ConvertTo-Json -Depth 50 | Set-Content -LiteralPath (Join-Path $planRoot 'plan.json')
                $record.status = 'needs-human'
                $script:confirmationIssue.body = Write-ScheduledRecord $record reporter
                $fullReport = Invoke-ScheduledReporting 'folo-rs/folo' $eventFile (Join-Path $caseRoot 'full-main')
                $fullReport.problems | Should -BeNullOrEmpty
                $fullReport.status | Should -BeExactly 'reported'
                $fullAction = @($fullReport.actions | Where-Object { $_.ContainsKey('endpoint') -and $_.endpoint -ceq 'repos/folo-rs/folo/issues/42' })
                $fullAction.Count | Should -Be 1
                (Read-ScheduledRecord $fullAction[0].payload.body reporter).status | Should -BeExactly confirmed
                Should -Invoke Get-ScheduledTrustedConfirmation -Times 1 -ParameterFilter { $null -eq $Declaration }
                Should -Invoke Invoke-ScheduledGitHubApi -Times 0 -ParameterFilter { $Method -in @('POST', 'PATCH') }
            }
        }
    }
}

Describe 'Safe durable writes' {
    InModuleScope ScheduledGitHub {
        BeforeEach {
            $script:policy = @{
                repository = 'folo-rs/folo'; reporter_login = 'github-actions[bot]'
                rollout = @{ reporting_enabled = $false }
            }
            $record = @{ schema_version = 1; repository = 'folo-rs/folo'; receipt = $null; invalidation = $null }
            $body = "[Copilot speaking]`nHuman before`n$(Write-ScheduledRecord $record coverage)`nHuman after"
            $issue = @{ number = 1; body = $body; state = 'open'; user = @{ login = 'github-actions[bot]' } }
            Mock Invoke-ScheduledGitHubApi { $issue }
        }
        It 'keeps Apply read-only until the explicit rollout switch is enabled' {
            $record.extra = 'update'
            (Sync-ScheduledIssue -Policy $policy -Issue $issue -Record $record -Kind coverage -Apply).action | Should -Be dry-run
            Should -Invoke Invoke-ScheduledGitHubApi -Times 0 -ParameterFilter { $Method -in @('POST', 'PATCH') }
        }
        It 'never edits surrounding discussion and updates only an owned record' {
            $policy.rollout.reporting_enabled = $true
            $record.extra = 'update'
            $null = Sync-ScheduledIssue -Policy $policy -Issue $issue -Record $record -Kind coverage -Apply
            Should -Invoke Invoke-ScheduledGitHubApi -Times 1 -ParameterFilter {
                $Method -ceq 'PATCH' -and $Body.body.Contains('Human before') -and $Body.body.EndsWith('Human after')
            }
        }
        It 'does not require <ValueKind> bootstrap context when updating an existing issue' -TestCases @(
            @{ ValueKind = 'omitted' }, @{ ValueKind = 'empty' }, @{ ValueKind = 'whitespace' },
            @{ ValueKind = 'missing-path' }
        ) {
            param($ValueKind)
            $policy.rollout.reporting_enabled = $true
            $record.extra = 'update'
            $arguments = @{ Policy = $policy; Issue = $issue; Record = $record; Kind = 'coverage'; Apply = $true }
            if ($ValueKind -ne 'omitted') {
                $value = switch ($ValueKind) {
                    empty { '' }
                    whitespace { ' ' }
                    missing-path { Join-Path $TestDrive 'not-needed' }
                }
                $arguments.OutputDirectory = $value
                $arguments.PublicationJournalPath = $value
            }
            (Sync-ScheduledIssue @arguments).action | Should -Be PATCH
            Should -Invoke Invoke-ScheduledGitHubApi -Times 1 -Exactly -ParameterFilter {
                $Method -ceq 'PATCH' -and $Endpoint -ceq 'repos/folo-rs/folo/issues/1'
            }
            Should -Invoke Invoke-ScheduledGitHubApi -Times 0 -ParameterFilter { $Endpoint -like '*/labels*' }
        }
        It 'refuses concurrent record changes without losing a human body edit' {
            $live = $issue.Clone()
            $live.body = Write-ScheduledRecord @{ schema_version = 1; different = $true } coverage
            Mock Invoke-ScheduledGitHubApi { $live }
            { Sync-ScheduledIssue -Policy $policy -Issue $issue -Record $record -Kind coverage -Apply } | Should -Throw
            Should -Invoke Invoke-ScheduledGitHubApi -Times 0 -ParameterFilter { $Method -ceq 'PATCH' }
        }
        It 'does not write unchanged records and prefixes every new issue body' {
            (Sync-ScheduledIssue -Policy $policy -Issue $issue -Record $record -Kind coverage).action | Should -Be unchanged
            $new = Sync-ScheduledIssue -Policy $policy -Issue $null -Record $record -Kind coverage
            $new.payload.body.StartsWith('[Copilot speaking]') | Should -BeTrue
            Should -Invoke Invoke-ScheduledGitHubApi -Times 0 -ParameterFilter { $Method -in @('POST', 'PATCH') }
        }
        It 'creates one coverage and health surface for separately owned records' {
            $coverage = Sync-ScheduledIssue -Policy $policy -Issue $null -Record $record -Kind coverage
            $coverage.payload.labels | Should -Be @('scheduled-coverage', 'scheduled-health')
            $coverage.payload.title | Should -Be 'Scheduled coverage and health'
            (Read-ScheduledRecord -Text $coverage.payload.body -Kind coverage).receipt | Should -BeNullOrEmpty
            $coverage.payload.body.StartsWith('[Copilot speaking]') | Should -BeTrue
            Should -Invoke Invoke-ScheduledGitHubApi -Times 0
        }
        It 'cannot create a diagnosed problem issue even when reporting writes are enabled' {
            $policy.rollout.reporting_enabled = $true
            { Sync-ScheduledIssue -Policy $policy -Issue $null -Record $record -Kind reporter -Apply } |
                Should -Throw '*cannot create problem issues*'
            Should -Invoke Invoke-ScheduledGitHubApi -Times 0
        }
        It 'updates only the coverage body and never writes a local health comment' {
            $policy.rollout.reporting_enabled = $true
            $record.extra = 'updated'
            $null = Sync-ScheduledIssue -Policy $policy -Issue $issue -Record $record -Kind coverage -Apply
            Should -Invoke Invoke-ScheduledGitHubApi -Times 1 -ParameterFilter {
                $Method -ceq 'PATCH' -and $Endpoint -ceq 'repos/folo-rs/folo/issues/1' -and $Body.body.Contains('Human before')
            }
            Should -Invoke Invoke-ScheduledGitHubApi -Times 0 -ParameterFilter { $Endpoint -like '*/comments*' }
        }
    }
}

Describe 'GitHub CLI serialization' {
    It 'preserves argument arrays and serializes data through stdin' {
        InModuleScope ScheduledGitHub {
            Mock Invoke-ScheduledGhJson {
                param($Arguments, $InputJson)
                $script:capturedArguments = $Arguments
                $script:capturedInput = $InputJson
                @{ number = 42 }
            }
            $text = '[Copilot speaking]' + "`n" + 'literal; $(never execute) "quoted"'
            $answer = Invoke-ScheduledGitHubApi 'repos/folo-rs/folo/issues' -Method POST -Body @{ body = $text }
            $answer.number | Should -Be 42
            $capturedArguments | Should -Contain '--input'
            $capturedArguments | Should -Not -Contain $text
            ($capturedInput | ConvertFrom-Json -AsHashtable).body | Should -BeExactly $text
        }
    }
    It 'requests paginated JSON and exposes transport failures' {
        InModuleScope ScheduledGitHub {
            Mock gh {
                $script:capturedArguments = @($args)
                $global:LASTEXITCODE = 0
                '[[{"number":1}],[{"number":2}]]'
            }
            $pages = Invoke-ScheduledGitHubApi 'repos/folo-rs/folo/issues?per_page=100' -Paginate
            @($pages | ForEach-Object { $_ }).Count | Should -Be 2
            $capturedArguments | Should -Contain '--paginate'
            $capturedArguments | Should -Contain '--slurp'
            Mock gh { $global:LASTEXITCODE = 1; '{}' }
            { Invoke-ScheduledGitHubApi 'repos/folo-rs/folo/issues' } | Should -Throw
        }
    }
}

Describe 'Merged repair confirmation authority' {
    InModuleScope ScheduledGitHub {
        BeforeEach {
            $script:confirmationPolicy = @{
                repository = 'folo-rs/folo'; repository_id = 850321188; reporter_login = 'github-actions[bot]'
                worker_login = 'sandersaares'; managed_branch_prefix = 'sandersaares-scheduled-repair-'
                repair = @{ allowed_packages = @('events'); allowed_checks = @('miri'); max_explanation_characters = 4000 }
                local = @{ enrolled_machine_id = 'executor' }
            }
            $script:confirmationRecord = @{
                schema_version = 1; repository = 'folo-rs/folo'; repository_id = 850321188
                finding_id = 'f' * 64; generation = 1; status = 'open'; package = 'events'
                check_id = 'miri-ubuntu-latest'; controller_sha = 'a' * 40; check_contract_digest = 'c' * 64
            }
            $script:confirmationIssue = @{
                number = 2; state = 'open'; user = @{ login = 'github-actions[bot]' }
                body = Write-ScheduledRecord $confirmationRecord reporter
            }
            $script:confirmationWorker = @{
                schema_version = 1; repository = 'folo-rs/folo'; repository_id = 850321188
                finding_id = 'f' * 64; generation = 1; branch = 'sandersaares-scheduled-repair-test'
                head_sha = 'd' * 40; attempt_id = 'attempt'; session_id = 'session'; executor_id = 'executor'; pr_number = 3
                explanation = 'Fixes incorrect ownership.'
            }
            $script:confirmationRepair = @{
                schema_version = 1; repository = 'folo-rs/folo'; repository_id = 850321188
                finding_id = 'f' * 64; generation = 1; branch = 'sandersaares-scheduled-repair-test'
                head_sha = 'd' * 40; attempt_id = 'attempt'; issue_number = 2; explanation = 'Fixes incorrect ownership.'
            }
            $script:confirmationPr = @{
                number = 3; state = 'closed'; merged = $true; merge_commit_sha = 'b' * 40
                head = @{ ref = 'sandersaares-scheduled-repair-test'; sha = 'd' * 40; repo = @{ id = 850321188 } }
                base = @{ ref = 'main'; repo = @{ id = 850321188 } }
                body = Write-ScheduledRecord $confirmationRepair repair
            }
            $script:confirmationManifest = Get-ScheduledCheckManifest -SourceSha ('b' * 40) -ControllerSha ('b' * 40) `
                -ContractDigest ('c' * 64) -Scope confirmation -Packages @('events') -CheckIds @('miri-ubuntu-latest')
            $script:confirmationResults = @($confirmationManifest.checks | ForEach-Object {
                @{
                    schema_version = 1; source_sha = 'b' * 40; controller_sha = 'b' * 40
                    check_contract_digest = 'c' * 64; check_id = $_.id; actual_scope = $_; outcome = 'passed'
                }
            })
            Mock Invoke-ScheduledGitHubApi {
                param($Endpoint)
                if ($Endpoint -like '*/comments?*') {
                    return @(@{ user = @{ login = 'sandersaares' }; body = Write-ScheduledRecord $confirmationWorker worker })
                }
                if ($Endpoint -like '*/pulls/3') { return $confirmationPr }
                if ($Endpoint -like '*/compare/*') { return @{ status = 'diverged' } }
                throw 'Unexpected API endpoint.'
            }
        }
        It 'closes using a reachable squash commit without requiring the original head to be reachable' {
            $confirmation = Get-ScheduledTrustedConfirmation $confirmationIssue $confirmationRecord `
                $confirmationManifest $confirmationResults $confirmationPolicy
            $confirmation.authoritative | Should -BeTrue
            $confirmation.explained | Should -BeTrue
            $confirmation.merge_commit_sha | Should -BeExactly ('b' * 40)
            Should -Invoke Invoke-ScheduledGitHubApi -Times 0 -ParameterFilter { $Endpoint -like '*compare*' }
        }
        It 'does not close a triage-owned later occurrence broader scope or human-held problem' -ForEach @(
            @{ Generation = 2; ScopeRevision = 1; Disposition = 'actionable'; Expected = 'open' }
            @{ Generation = 1; ScopeRevision = 2; Disposition = 'actionable'; Expected = 'open' }
            @{ Generation = 1; ScopeRevision = 1; Disposition = 'needs-human'; Expected = 'open' }
            @{ Generation = 1; ScopeRevision = 1; Disposition = 'actionable'; Expected = 'closed' }
        ) {
            $confirmationPolicy.rollout = @{ reporting_enabled = $false }
            $problem = @{
                schema_version = 1; repository_id = 850321188; issue_number = 2; role = 'triage'
                generation = $Generation; scope_revision = $ScopeRevision; repair_disposition = $Disposition
            }
            $confirmationIssue.body += "`n$(Write-ScheduledRecord $problem problem)"
            Mock Invoke-ScheduledGitHubApi { $confirmationIssue } -ParameterFilter { $Endpoint -ceq 'repos/folo-rs/folo/issues/2' }
            $record = $confirmationRecord.Clone(); $record.status = 'confirmed'
            $result = Sync-ScheduledIssue -Policy $confirmationPolicy -Issue $confirmationIssue -Record $record -Kind reporter
            $result.payload.state | Should -Be $Expected
            $result.payload.body | Should -Match 'scheduled-problem:v1'
            $problem.repository_id = 124
            $confirmationIssue.body = "$(Write-ScheduledRecord $confirmationRecord reporter)`n$(Write-ScheduledRecord $problem problem)"
            { Sync-ScheduledIssue -Policy $confirmationPolicy -Issue $confirmationIssue -Record $record -Kind reporter } |
                Should -Throw
            Should -Invoke Invoke-ScheduledGitHubApi -Times 0 -ParameterFilter { $Method -in @('POST', 'PATCH') }
        }
        It 'rejects original-head ancestry as a substitute for merge-commit reachability' {
            $confirmationPr.merge_commit_sha = 'e' * 40
            Get-ScheduledTrustedConfirmation $confirmationIssue $confirmationRecord `
                $confirmationManifest $confirmationResults $confirmationPolicy | Should -BeNullOrEmpty
        }
        It 'requires every implicated check and a registered explanation' {
            $confirmationResults[0].outcome = 'incomplete'
            (Get-ScheduledTrustedConfirmation $confirmationIssue $confirmationRecord `
                $confirmationManifest $confirmationResults $confirmationPolicy).status | Should -Be retry
            $confirmationResults[0].outcome = 'passed'
            $confirmationRepair.Remove('explanation')
            $confirmationPr.body = Write-ScheduledRecord $confirmationRepair repair
            { Get-ScheduledTrustedConfirmation $confirmationIssue $confirmationRecord `
                $confirmationManifest $confirmationResults $confirmationPolicy } | Should -Throw '*explanation*'
        }
        It 'rejects stale worker generation and a foreign author' {
            $confirmationWorker.generation = 2
            Get-ScheduledTrustedConfirmation $confirmationIssue $confirmationRecord `
                $confirmationManifest $confirmationResults $confirmationPolicy | Should -BeNullOrEmpty
            Mock Invoke-ScheduledGitHubApi {
                @(@{ user = @{ login = 'other' }; body = Write-ScheduledRecord $confirmationWorker worker })
            }
            Get-ScheduledTrustedConfirmation $confirmationIssue $confirmationRecord `
                $confirmationManifest $confirmationResults $confirmationPolicy | Should -BeNullOrEmpty
        }
        It 'allows complete full-workspace evidence to cover the implicated package' {
            $confirmationManifest.scope = 'full'
            $confirmationResults[0].actual_scope.packages = @()
            (Get-ScheduledTrustedConfirmation $confirmationIssue $confirmationRecord `
                $confirmationManifest $confirmationResults $confirmationPolicy).scope_complete | Should -BeTrue
        }
        It 'confirms a registered merged fix after an earlier unexplained pass' {
            $confirmationRecord.status = 'needs-human'
            $confirmationIssue.body = Write-ScheduledRecord $confirmationRecord reporter
            (Get-ScheduledTrustedConfirmation $confirmationIssue $confirmationRecord `
                $confirmationManifest $confirmationResults $confirmationPolicy).status | Should -Be confirmed
        }
        It 'accepts the package union used to confirm several incidents in one plan' {
            $confirmationManifest.checks[0].packages = @('events', 'events_once')
            (Get-ScheduledTrustedConfirmation $confirmationIssue $confirmationRecord `
                $confirmationManifest $confirmationResults $confirmationPolicy).successful | Should -BeTrue
        }
        It 'revalidates declared identity scope and actual merge against live GitHub registration' {
            $declaration = @{
                issue_number = 2; finding_id = $confirmationRecord.finding_id; generation = 1; pr_number = 3
                merge_commit_sha = 'b' * 40; source_sha = 'b' * 40; packages = @('events')
                check_ids = @('miri-ubuntu-latest'); worker = $confirmationWorker
            }
            (Get-ScheduledTrustedConfirmation $confirmationIssue $confirmationRecord `
                $confirmationManifest $confirmationResults $confirmationPolicy -Declaration $declaration).successful | Should -BeTrue
            $declaration.merge_commit_sha = 'd' * 40
            { Get-ScheduledTrustedConfirmation $confirmationIssue $confirmationRecord `
                $confirmationManifest $confirmationResults $confirmationPolicy -Declaration $declaration } | Should -Throw
            $declaration.merge_commit_sha = 'b' * 40
            $declaration.check_ids = @()
            { Get-ScheduledTrustedConfirmation $confirmationIssue $confirmationRecord `
                $confirmationManifest $confirmationResults $confirmationPolicy -Declaration $declaration } | Should -Throw
        }
        It 'accepts an enrolled worker explanation and records complete family failures' {
            $confirmationWorker.explanation = 'Identifies and repairs the intermittent aliasing defect.'
            $confirmationRepair.explanation = $confirmationWorker.explanation
            $confirmationPr.body = Write-ScheduledRecord $confirmationRepair repair
            (Get-ScheduledTrustedConfirmation $confirmationIssue $confirmationRecord `
                $confirmationManifest $confirmationResults $confirmationPolicy).explained | Should -BeTrue
            $confirmationResults[0].outcome = 'findings'
            $attempt = Get-ScheduledTrustedConfirmation $confirmationIssue $confirmationRecord `
                $confirmationManifest $confirmationResults $confirmationPolicy
            $attempt.status | Should -Be failed
            $attempt.merge_commit_sha | Should -BeExactly ('b' * 40)
        }
        It 'accepts a causal explanation for an ordinary deterministic repair' {
            $confirmationRecord.check_id = 'careful-ubuntu-latest'
            $confirmationIssue.body = Write-ScheduledRecord $confirmationRecord reporter
            $confirmationPolicy.repair.allowed_checks = @('careful')
            $confirmationManifest = Get-ScheduledCheckManifest -SourceSha ('b' * 40) -ControllerSha ('b' * 40) `
                -ContractDigest ('c' * 64) -Scope confirmation -Packages @('events') -CheckIds @('careful-ubuntu-latest')
            $confirmationResults = @(@{
                schema_version = 1; source_sha = 'b' * 40; controller_sha = 'b' * 40
                check_contract_digest = 'c' * 64; check_id = 'careful-ubuntu-latest'
                actual_scope = $confirmationManifest.checks[0]; outcome = 'passed'
            })
            (Get-ScheduledTrustedConfirmation $confirmationIssue $confirmationRecord `
                $confirmationManifest $confirmationResults $confirmationPolicy).status | Should -Be confirmed
        }
    }
}

Describe 'Read-only health adapter' {
    InModuleScope ScheduledGitHub {
        BeforeEach {
            $script:healthPolicy = @{
                repository = 'folo-rs/folo'; repository_id = 850321188
                worker_login = 'sandersaares'; reporter_login = 'github-actions[bot]'
                coverage = @{ expected_plan_gap_hours = 30; max_age_days = 7 }
                local = @{ expected_poll_gap_minutes = 420; enrolled_machine_id = 'executor'; mode = 'observe' }
            }
            $manifest = Get-ScheduledCheckManifest -SourceSha ('a' * 40) -ControllerSha ('a' * 40) -ContractDigest ('c' * 64)
            $context = @{
                workflow_id = 100; run_id = 10; run_number = 10; run_attempt = 1
                workflow_path = '.github/workflows/full-deep-validation.yml'
                created_at = '2026-09-08T09:00:00Z'; completed_at = '2026-09-08T10:00:00Z'
            }
            $results = @($manifest.checks | ForEach-Object {
                @{
                    schema_version = 1; check_id = $_.id; actual_scope = $_; outcome = 'passed'
                    source_sha = 'a' * 40; controller_sha = 'a' * 40; check_contract_digest = 'c' * 64
                    run_id = 10; run_number = 10; run_attempt = 1
                }
            })
            $script:healthCoverage = Merge-ScheduledCoverage -Coverage $null -Manifest $manifest -Results $results `
                -Context $context -IsAncestor { param($old, $new) $old -ceq $new }
            $healthCoverage.repository_id = 850321188
            $healthCoverage.planning = @{ outcome = 'not-run-unchanged'; completed_at = '2026-09-08T11:00:00Z' }
            $healthCoverage.last_plan = $context.Clone()
            $healthCoverage.last_plan.planned_at = $healthCoverage.planning.completed_at
            $healthCoverage.reporting = @{ outcome = 'passed'; completed_at = '2026-09-08T11:00:00Z' }
            $script:healthLocal = @{
                schema_version = 1; repository = 'folo-rs/folo'; repository_id = 850321188; executor_id = 'executor'
                last_successful_scan = '2026-09-08T11:00:00Z'; blocked_conditions = @()
            }
            Mock Get-ScheduledPolicy { $healthPolicy }
            Mock Get-ScheduledContractDigest { 'c' * 64 }
            Mock Get-ScheduledOwnedIssue {
                @(@{ number = 1; body = Write-ScheduledRecord $healthCoverage coverage })
            } -ParameterFilter { $Label -ceq 'scheduled-coverage' }
            Mock Get-ScheduledOwnedIssue {
                @(@{ number = 1; state = 'open'; body = Write-ScheduledRecord $healthCoverage coverage })
            } -ParameterFilter { $Label -ceq 'scheduled-health' }
            Mock Invoke-ScheduledGitHubApi {
                param($Endpoint)
                if ($Endpoint -like '*/full-deep-validation.yml') { return @{ state = 'active' } }
                if ($Endpoint -like '*/git/ref/heads/main') { return @{ object = @{ sha = 'a' * 40 } } }
                if ($Endpoint -like '*/scheduled-report.yml/runs?*per_page=100') {
                    return @(@{ total_count = 0; workflow_runs = @() })
                }
                if ($Endpoint -like '*/scheduled-report.yml/runs?*') {
                    return @{ workflow_runs = @(@{
                            id = 12; run_attempt = 1; conclusion = 'success'; updated_at = '2026-09-08T11:00:00Z'
                        }) }
                }
                if ($Endpoint -like '*/comments?*') {
                    return @(@{ user = @{ login = 'sandersaares' }; body = Write-ScheduledRecord $healthLocal health })
                }
                throw 'Unexpected endpoint.'
            }
        }
        It 'reads durable coverage and the separately owned enrolled executor heartbeat without writes' {
            $health = Get-ScheduledGitHubHealth 'folo-rs/folo' ([datetimeoffset]'2026-09-08T12:00:00Z')
            $health.problems | Should -BeNullOrEmpty
            $health.healthy | Should -BeTrue
            $health.components.coverage.status | Should -Be reused
            $health.components.repair_scan.status | Should -Be fresh
            Should -Invoke Invoke-ScheduledGitHubApi -Times 1 -ParameterFilter { $Endpoint -like '*/issues/1/comments?*' }
            Should -Invoke Invoke-ScheduledGitHubApi -Times 0 -ParameterFilter { $Method -in @('POST', 'PATCH') }
        }
        It 'reports stalled or foreign triage independently of a current repair heartbeat' {
            $healthPolicy.schema_version = 1
            $script:healthTriagePolicy = Get-ScheduledTriagePolicy
            $healthTriagePolicy.mode = 'triage'; $healthTriagePolicy.enrolled_machine_id = 'executor'
            $healthTriagePolicy.model = 'chosen'; $healthTriagePolicy.reasoning_effort = 'medium'
            Mock Get-ScheduledTriagePolicy { $healthTriagePolicy }
            $script:healthTriage = $healthLocal.Clone()
            $healthTriage.role = 'triage'; $healthTriage.last_successful_scan = '2026-09-01T11:00:00Z'
            $healthTriage.profile = @{
                policy_digest = Get-ScheduledTriagePolicyDigest $healthPolicy $healthTriagePolicy
                cadence_cron = $healthTriagePolicy.cadence_cron; model = 'chosen'; reasoning_effort = 'medium'; enabled = $true
                controller_digest = Get-ScheduledTriageControllerDigest
                automation_id = 'entry'; prompt_digest = Get-ScheduledTriagePromptDigest 'Native prompt'
            }
            $binding = Get-ScheduledDigest @{ kind = 'scan'; token = 'private-scan'; session_id = 'session' }
            $healthTriage.profile_scan = @{ binding_digest = $binding; session_id = 'session' }
            $healthTriage.profile_observation = @{
                schema_version = 1; kind = 'scan'; binding_digest = $binding; session_id = 'session'
                automation_id = 'entry'; prompt_digest = $healthTriage.profile.prompt_digest
            }
            $healthTriage.profile_observation.digest = Get-ScheduledDigest $healthTriage.profile_observation
            Mock Invoke-ScheduledGitHubApi {
                @($healthLocal, $healthTriage) | ForEach-Object {
                    @{ user = @{ login = 'sandersaares' }; body = Write-ScheduledRecord $_ health }
                }
            } -ParameterFilter { $Endpoint -like '*/comments?*' }
            $health = Get-ScheduledGitHubHealth 'folo-rs/folo' ([datetimeoffset]'2026-09-08T12:00:00Z')
            $health.components.repair_scan.status | Should -Be fresh
            $health.components.triage_scan.status | Should -Be unavailable
            $health.healthy | Should -BeFalse
            $healthTriage.last_successful_scan = '2026-09-08T11:00:00Z'
            $health = Get-ScheduledGitHubHealth 'folo-rs/folo' ([datetimeoffset]'2026-09-08T12:00:00Z')
            $health.components.triage_scan.status | Should -Be fresh
            $health.components.repair_scan.status | Should -Be fresh
            foreach ($field in @('profile', 'profile_scan', 'profile_observation')) {
                $original = $healthTriage[$field]
                $healthTriage[$field] = 'not-an-object'
                $health = Get-ScheduledGitHubHealth 'folo-rs/folo' ([datetimeoffset]'2026-09-08T12:00:00Z')
                $health.components.triage_scan.status | Should -Be unavailable
                $health.components.repair_scan.status | Should -Be fresh
                $health.problems | Should -Not -BeNullOrEmpty
                $healthTriage[$field] = $original
            }
            foreach ($field in @('policy_digest', 'controller_digest', 'cadence_cron', 'automation_id',
                    'prompt_digest', 'model', 'reasoning_effort', 'enabled')) {
                $original = $healthTriage.profile[$field]
                $healthTriage.profile.Remove($field)
                $health = Get-ScheduledGitHubHealth 'folo-rs/folo' ([datetimeoffset]'2026-09-08T12:00:00Z')
                $health.components.triage_scan.status | Should -Be unavailable
                $health.components.repair_scan.status | Should -Be fresh
                $health.problems | Should -Not -BeNullOrEmpty
                $healthTriage.profile[$field] = @{}
                $health = Get-ScheduledGitHubHealth 'folo-rs/folo' ([datetimeoffset]'2026-09-08T12:00:00Z')
                $health.components.triage_scan.status | Should -Be unavailable
                $health.components.repair_scan.status | Should -Be fresh
                $health.problems | Should -Not -BeNullOrEmpty
                $healthTriage.profile[$field] = $original
            }
            $healthTriage.Remove('blocked_conditions')
            $health = Get-ScheduledGitHubHealth 'folo-rs/folo' ([datetimeoffset]'2026-09-08T12:00:00Z')
            $health.components.triage_scan.status | Should -Be unavailable
            $health.problems | Should -Not -BeNullOrEmpty
            $health.components.repair_scan.status | Should -Be fresh
            $healthTriage.blocked_conditions = @()
            $healthTriage.repository_id = 124
            $health = Get-ScheduledGitHubHealth 'folo-rs/folo' ([datetimeoffset]'2026-09-08T12:00:00Z')
            $health.problems | Should -Not -BeNullOrEmpty
            $health.components.repair_scan.status | Should -Be fresh
            Should -Invoke Invoke-ScheduledGitHubApi -Times 0 -ParameterFilter { $Method -in @('POST', 'PATCH') }
        }
        It 'persists the health entrypoint observation for the workflow upload without GitHub writes' {
            $moduleRoot = Split-Path (Get-Module ScheduledGitHub).Path -Parent
            $directory = Join-Path $moduleRoot "fixtures\reporter\$([guid]::NewGuid().ToString('N'))"
            $savedSummaryPath = $env:GITHUB_STEP_SUMMARY
            try {
                $env:GITHUB_STEP_SUMMARY = ''
                $output = & (Join-Path $moduleRoot 'Invoke-ScheduledHealth.ps1') -Repository 'folo-rs/folo' `
                    -Now ([datetimeoffset]'2026-09-08T12:00:00Z') -OutputDirectory $directory
                $record = Get-Content -LiteralPath (Join-Path $directory 'health.json') -Raw | ConvertFrom-Json -AsHashtable
                $record.status | Should -Be healthy
                ($output | ConvertFrom-Json -AsHashtable).checked_at | Should -Be $record.checked_at
                Should -Invoke Invoke-ScheduledGitHubApi -Times 0 -ParameterFilter { $Method -in @('POST', 'PATCH') }
            } finally {
                $env:GITHUB_STEP_SUMMARY = $savedSummaryPath
                if (Test-Path -LiteralPath $directory) { Remove-Item -LiteralPath $directory -Recurse -Force }
            }
        }
        It 'fails closed for GitHub outages or a different executor identity' {
            $healthLocal.executor_id = 'other'
            $health = Get-ScheduledGitHubHealth 'folo-rs/folo' ([datetimeoffset]'2026-09-08T12:00:00Z')
            $health.components.repair_scan.status | Should -Be unavailable
            $healthLocal.executor_id = 'executor'
            $healthLocal.Remove('blocked_conditions')
            $health = Get-ScheduledGitHubHealth 'folo-rs/folo' ([datetimeoffset]'2026-09-08T12:00:00Z')
            $health.components.repair_scan.status | Should -Be unavailable
            $health.problems | Should -Not -BeNullOrEmpty
            Mock Invoke-ScheduledGitHubApi { throw [IO.IOException]::new('GitHub unavailable.') }
            $health = Get-ScheduledGitHubHealth 'folo-rs/folo' ([datetimeoffset]'2026-09-08T12:00:00Z')
            $health.healthy | Should -BeFalse
            $health.problems.Count | Should -Be 1
        }
        It 'detects a failed reporter even when its prior durable write was successful' {
            Mock Invoke-ScheduledGitHubApi {
                @{ workflow_runs = @(@{
                        id = 13; run_attempt = 1; conclusion = 'failure'; updated_at = '2026-09-08T11:00:00Z'
                    }) }
            } -ParameterFilter { $Endpoint -like '*/scheduled-report.yml/runs?*per_page=1' }
            $health = Get-ScheduledGitHubHealth 'folo-rs/folo' ([datetimeoffset]'2026-09-08T12:00:00Z')
            $health.components.reporting.status | Should -Be failed
            $health.healthy | Should -BeFalse
        }
        It 'reports disabled Local health with the actual hosted-enabled defaults' {
            $script:healthPolicy = Get-Content -LiteralPath (Join-Path $PSScriptRoot 'policy.json') -Raw |
                ConvertFrom-Json -AsHashtable
            Mock Get-ScheduledOwnedIssue { throw 'Disabled Local operation must not require a health registration.' } `
                -ParameterFilter { $Label -ceq 'scheduled-health' }
            $health = Get-ScheduledGitHubHealth 'folo-rs/folo' ([datetimeoffset]'2026-09-08T12:00:00Z')
            $health.healthy | Should -BeTrue
            $health.components.repair_scan.status | Should -Be disabled
            $health.components.coverage.status | Should -Be reused
            Should -Invoke Invoke-ScheduledGitHubApi -Times 0 -ParameterFilter { $Endpoint -like '*/comments*' }
        }
        It 'retains missing baseline and reporter failures while Local is deliberately off' {
            $script:healthPolicy = Get-Content -LiteralPath (Join-Path $PSScriptRoot 'policy.json') -Raw |
                ConvertFrom-Json -AsHashtable
            Mock Get-ScheduledOwnedIssue { @() } -ParameterFilter { $Label -ceq 'scheduled-coverage' }
            Mock Invoke-ScheduledGitHubApi {
                @{ workflow_runs = @(@{
                    id = 13; run_attempt = 1; conclusion = 'failure'; updated_at = '2026-09-08T11:00:00Z'
                }) }
            } -ParameterFilter { $Endpoint -like '*/scheduled-report.yml/runs?*per_page=1' }
            $health = Get-ScheduledGitHubHealth 'folo-rs/folo' ([datetimeoffset]'2026-09-08T12:00:00Z')
            $health.healthy | Should -BeFalse
            $health.components.repair_scan.status | Should -Be disabled
            $health.components.coverage.status | Should -Be unavailable
            $health.components.planning.status | Should -Be unavailable
            $health.components.reporting.status | Should -Be failed
        }
        It 'does not accept selected-workflow activity as nightly planning freshness' {
            $healthCoverage.last_plan.workflow_path = '.github/workflows/selected-deep-validation.yml'
            $health = Get-ScheduledGitHubHealth 'folo-rs/folo' ([datetimeoffset]'2026-09-08T12:00:00Z')
            $health.healthy | Should -BeFalse
            $health.components.planning.status | Should -Be unavailable
        }
        It 'reports a foreign coverage index rather than trusting its planning or receipt' {
            $healthCoverage.repository_id = 123
            $health = Get-ScheduledGitHubHealth 'folo-rs/folo' ([datetimeoffset]'2026-09-08T12:00:00Z')
            $health.status | Should -Be failed
            $health.problems | Should -Not -BeNullOrEmpty
        }
        It 'requires an enrolled executor heartbeat even in <Mode> mode' -TestCases @(
            @{ Mode = 'observe' }, @{ Mode = 'paused' }, @{ Mode = 'repair' }
        ) {
            param($Mode)
            $healthPolicy.local.mode = $Mode
            $healthLocal.last_successful_scan = '2026-09-01T11:00:00Z'
            $health = Get-ScheduledGitHubHealth 'folo-rs/folo' ([datetimeoffset]'2026-09-08T12:00:00Z')
            $health.components.repair_scan.status | Should -Be unavailable
            $health.healthy | Should -BeFalse
            $healthLocal.last_successful_scan = '2026-09-08T11:00:00Z'
            $healthLocal.blocked_conditions = @('failed-scan')
            $health = Get-ScheduledGitHubHealth 'folo-rs/folo' ([datetimeoffset]'2026-09-08T12:00:00Z')
            $health.components.repair_scan.status | Should -Be failed
        }
        It 'keeps cancelled and failed reporting origins visible after a later successful run' {
            Mock Invoke-ScheduledGitHubApi {
                @(@{ total_count = 1; workflow_runs = @(@{
                    id = 8; run_attempt = 1; conclusion = 'cancelled'
                }) })
            } -ParameterFilter { $Endpoint -like '*/scheduled-report.yml/runs?*status=cancelled*' }
            $health = Get-ScheduledGitHubHealth 'folo-rs/folo' ([datetimeoffset]'2026-09-08T12:00:00Z')
            $health.components.reporting.status | Should -Be failed
            $health.components.reporting.observation.unresolved_runs[0].run_id | Should -Be 8
            $health.healthy | Should -BeFalse
            Should -Invoke Invoke-ScheduledGitHubApi -Times 3 -ParameterFilter {
                $Endpoint -like '*/scheduled-report.yml/runs?*per_page=100' -and $Paginate
            }
        }
        It 'does not silently accept a truncated reporting recovery inventory' {
            Mock Invoke-ScheduledGitHubApi {
                @(@{ total_count = 2; workflow_runs = @(@{
                    id = 8; run_attempt = 1; conclusion = 'failure'
                }) })
            } -ParameterFilter { $Endpoint -like '*/scheduled-report.yml/runs?*status=failure*' }
            $health = Get-ScheduledGitHubHealth 'folo-rs/folo' ([datetimeoffset]'2026-09-08T12:00:00Z')
            $health.healthy | Should -BeFalse
            $health.problems | Should -Contain 'Reporting recovery inventory is incomplete.'
        }
        It 'reports staged without expecting a durable issue before reporting is enabled' {
            $healthPolicy.rollout = @{ phase = 'staged'; hosted_execution_enabled = $false; reporting_enabled = $false }
            Mock Get-ScheduledOwnedIssue { @() } -ParameterFilter { $Label -ceq 'scheduled-coverage' }
            $health = Get-ScheduledGitHubHealth 'folo-rs/folo' ([datetimeoffset]'2026-09-08T12:00:00Z')
            $health.status | Should -Be staged
            $health.healthy | Should -BeFalse
            $health.problems | Should -BeNullOrEmpty
            Should -Invoke Invoke-ScheduledGitHubApi -Times 0 -ParameterFilter { $Method -in @('POST', 'PATCH') }
        }
        It 'reports missing or duplicate health surfaces without inventing a replacement issue' {
            foreach ($count in @(0, 2)) {
                $script:healthSurfaceCount = $count
                Mock Get-ScheduledOwnedIssue {
                    @(for ($index = 0; $index -lt $healthSurfaceCount; $index++) {
                        @{ number = $index + 2; state = 'open'; body = '' }
                    })
                } -ParameterFilter { $Label -ceq 'scheduled-health' }
                $health = Get-ScheduledGitHubHealth 'folo-rs/folo' ([datetimeoffset]'2026-09-08T12:00:00Z')
                $health.status | Should -Be failed
                $health.components.repair_scan.status | Should -Be unavailable
                $health.problems.Count | Should -Be 1
            }
            Should -Invoke Invoke-ScheduledGitHubApi -Times 0 -ParameterFilter { $Method -in @('POST', 'PATCH') }
        }
        It 'rejects a Local health surface that is not the coverage issue' {
            Mock Get-ScheduledOwnedIssue { @(@{ number = 2; state = 'open' }) } `
                -ParameterFilter { $Label -ceq 'scheduled-health' }
            $health = Get-ScheduledGitHubHealth 'folo-rs/folo' ([datetimeoffset]'2026-09-08T12:00:00Z')
            $health.healthy | Should -BeFalse
            $health.problems | Should -Not -BeNullOrEmpty
            Should -Invoke Invoke-ScheduledGitHubApi -Times 0 -ParameterFilter { $Endpoint -like '*/comments*' }
        }
        It 'does not confuse executor registration with a successful scan' {
            $healthLocal.Remove('last_successful_scan')
            $health = Get-ScheduledGitHubHealth 'folo-rs/folo' ([datetimeoffset]'2026-09-08T12:00:00Z')
            $health.healthy | Should -BeFalse
            $health.components.repair_scan.status | Should -Be unavailable
        }
    }
}
