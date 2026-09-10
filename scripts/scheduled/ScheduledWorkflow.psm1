#requires -Version 7

# Orchestration behind Invoke-ScheduledPlan.ps1 and Invoke-ScheduledGate.ps1: decides the deep-check
# matrix and managed/confirmation status for a triggering event (`Invoke-ScheduledPlanning`), and
# enforces `scheduled-repair-gate` against that plan's evidence (`Invoke-ScheduledGate`). Thin
# workflow entrypoints use reviewed controller inputs; candidate text is never executable here -
# coverage/plan history is read as data through the GitHub API, not by running anything from the
# candidate checkout. See ../../.github/workflows/design.md#shallow-and-deep-validation,
# ../../.github/workflows/implementation.md#complete-evidence-and-reuse and #managed-repair-gate,
# and ../../docs/scheduled-validation.md#validation-and-release-contract.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
Import-Module (Join-Path $PSScriptRoot 'ScheduledContracts.psm1')
Import-Module (Join-Path $PSScriptRoot 'ScheduledPlan.psm1')
Import-Module (Join-Path $PSScriptRoot 'ScheduledGate.psm1')

function Get-ScheduledCoverageIndex {
    [CmdletBinding()]
    param([Parameter(Mandatory)][hashtable] $Policy)
    $pages = Invoke-ScheduledReadApi "repos/$($Policy.repository)/issues?labels=scheduled-coverage&state=all&per_page=100" -Paginate
    $issues = @($pages | ForEach-Object { $_ } | Where-Object { $_.user.login -ceq $Policy.reporter_login })
    if ($issues.Count -eq 0) { return $null }
    if ($issues.Count -ne 1) { throw 'Ambiguous coverage index; operator reconciliation required.' }
    try {
        return Read-ScheduledRecord -Text $issues[0].body -Kind coverage
    } catch [FormatException] {
        # A corrupt optional cache is a reason to execute, never a reason to assume coverage.
        Write-Verbose "Coverage index is malformed; forcing fresh execution: $($_.Exception.Message)"
        return $null
    }
}

function Get-ScheduledCoverageRunRisk {
    [CmdletBinding()]
    [OutputType([string])]
    param(
        [Parameter(Mandatory)][hashtable] $Policy,
        [Parameter(Mandatory)][hashtable] $Receipt,
        [Parameter(Mandatory)][long] $CurrentRunId,
        [Parameter(Mandatory)][int] $CurrentRunAttempt
    )
    if ($Receipt.run_id -eq $CurrentRunId -and $Receipt.run_attempt -lt $CurrentRunAttempt) {
        return 'coverage-run-reattempted'
    }
    # Reporting is serialized but asynchronous. Check the execution API before reusing its
    # durable index so an unreported failure or retry cannot hide behind an older green receipt.
    $completed = [datetimeoffset]$Receipt.completed_at
    foreach ($workflow in @('scheduled-validation.yml', 'scheduled-verify.yml')) {
        $pages = Invoke-ScheduledReadApi "repos/$($Policy.repository)/actions/workflows/$workflow/runs?head_sha=$($Receipt.source_sha)&branch=main&per_page=100" -Paginate
        foreach ($execution in @($pages | ForEach-Object { $_.workflow_runs })) {
            if ($execution.id -eq $CurrentRunId) { continue }
            # Manual diagnostics have no authority over automatic main coverage, including
            # while their reporter is pending.
            # Ref: ../../.github/workflows/implementation.md#manual-checks.
            if ($execution.event -ceq 'workflow_dispatch') { continue }
            if ($execution.head_sha -cne $Receipt.source_sha -or $execution.head_branch -cne 'main') {
                throw 'Execution API returned an incompatible coverage candidate.'
            }
            if ($execution.id -eq $Receipt.run_id -and $execution.run_attempt -gt $Receipt.run_attempt) {
                return 'coverage-run-reattempted'
            }
            if ($execution.status -cne 'completed') { return 'unsettled-validation-run' }
            if ([datetimeoffset]$execution.updated_at -ge $completed -and $execution.conclusion -cne 'success') {
                return 'unreported-validation-failure'
            }
        }
    }
}

function Get-ScheduledConfirmationScope {
    [CmdletBinding()]
    param([Parameter(Mandatory)][hashtable] $Policy, [Parameter(Mandatory)][string] $SourceSha)

    $pages = Invoke-ScheduledReadApi "repos/$($Policy.repository)/issues?labels=scheduled-finding&state=open&per_page=100" -Paginate
    foreach ($issue in @($pages | ForEach-Object { $_ })) {
        if ($issue.user.login -cne $Policy.reporter_login) { continue }
        $reporter = Read-ScheduledRecord -Text $issue.body -Kind reporter
        $commentPages = Invoke-ScheduledReadApi "repos/$($Policy.repository)/issues/$($issue.number)/comments?per_page=100" -Paginate
        $run = Invoke-ScheduledReadApi "repos/$($Policy.repository)/actions/runs/$($reporter.observation.run_id)/attempts/$($reporter.observation.run_attempt)"
        $incident = ConvertTo-ScheduledIncident -Issue $issue -Comments @($commentPages | ForEach-Object { $_ }) `
            -Repository $Policy.repository -RepositoryId $Policy.repository_id -Run $run `
            -ReporterLogin $Policy.reporter_login -WorkerLogin $Policy.worker_login
        $worker = $incident.validated_worker
        if ($null -eq $worker -or $null -eq $worker.pr_number) { continue }
        $pr = Invoke-ScheduledReadApi "repos/$($Policy.repository)/pulls/$($worker.pr_number)"
        if ($pr.state -cne 'closed' -or -not $pr.merged) { continue }
        Assert-ScheduledSha $pr.merge_commit_sha
        if ($reporter.ContainsKey('confirmation') -and $null -ne $reporter.confirmation -and
            $reporter.confirmation.merge_commit_sha -ceq $pr.merge_commit_sha -and
            $reporter.confirmation.status -cne 'retry') {
            continue
        }
        $comparison = Invoke-ScheduledReadApi "repos/$($Policy.repository)/compare/$($pr.merge_commit_sha)...${SourceSha}"
        if ($comparison.status -cnotin @('identical', 'ahead')) {
            throw 'Merged repair is not contained in the confirmation main candidate.'
        }
        $scope = Get-ScheduledRepairScope -PullRequest $pr -Issue $issue -Worker $worker -Policy $Policy -Confirmation
        if (-not $scope.managed) { throw 'Registered merged repair lost its recognition metadata.' }
        @{
            issue_number = $issue.number; finding_id = $incident.finding_id; generation = $incident.generation
            pr_number = $pr.number; merge_commit_sha = $pr.merge_commit_sha; source_sha = $SourceSha
            check_ids = $scope.check_ids; packages = $scope.packages; worker = $worker
        }
    }
}

function Invoke-ScheduledPlanning {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][ValidateSet('scheduled', 'validation', 'verify')][string] $Mode,
        [Parameter(Mandatory)][string] $EventPath,
        [Parameter(Mandatory)][string] $OutputDirectory,
        [datetimeoffset] $Now = [datetimeoffset]::UtcNow
    )
    $workflowEvent = Get-Content -LiteralPath $EventPath -Raw | ConvertFrom-Json -AsHashtable
    $policy = Get-ScheduledPolicy
    if ($workflowEvent.repository.id -ne $policy.repository_id) { throw 'Wrong repository for scheduled controller.' }
    if ($Mode -ne 'validation' -and $env:GITHUB_REF -cne 'refs/heads/main') {
        throw 'Select main under Use workflow from. To test another commit, enter its full SHA in Scheduled verification.'
    }
    $diagnostic = $env:GITHUB_EVENT_NAME -ceq 'workflow_dispatch'
    $root = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
    $controllerSha = (& git -C $root rev-parse HEAD).Trim()
    $contractDigest = Get-ScheduledContractDigest -Root $root
    $sourceSha = $controllerSha
    $releaseBaseSha = $controllerSha
    $scope = 'full'
    $packages = @()
    $checkIds = @()
    $repairs = @()
    $confirmations = @()
    $managed = $false
    $run = $false
    $reason = 'staged'
    $receipt = $null

    if ($Mode -eq 'validation') {
        $scope = 'repair'
        if ($workflowEvent.ContainsKey('pull_request')) {
            # Re-read body/registration so metadata-only reruns see the latest exact-head record.
            $pr = Invoke-ScheduledReadApi "repos/$($policy.repository)/pulls/$($workflowEvent.pull_request.number)"
            if ($pr.head.sha -cne $workflowEvent.pull_request.head.sha) { throw 'PR head moved since this event.' }
            $sourceSha = $pr.head.sha
            $prScope = Get-ScheduledPullRequestScope -PullRequest $pr -Policy $policy
            if ($prScope.managed) {
                Assert-ScheduledPullRequestChange -PullRequest $pr -Scope $prScope -Policy $policy -Root $root
            }
            $repairs += $prScope
        } elseif ($workflowEvent.ContainsKey('merge_group')) {
            $sourceSha = $workflowEvent.merge_group.head_sha
            $releaseBaseSha = $workflowEvent.merge_group.base_sha
            $repository = $policy.repository
            $readApi = Get-Command Invoke-ScheduledReadApi
            $isAncestor = {
                param($ancestor, $descendant)
                if ($ancestor -ceq $descendant) { return $true }
                $comparison = & $readApi "repos/$repository/compare/${ancestor}...${descendant}"
                return $comparison.status -cin @('ahead', 'identical')
            }.GetNewClosure()
            $entries = @(Get-ScheduledQueueEntry -Repository $repository)
            $members = @(Get-ScheduledMergeGroupMember -HeadSha $sourceSha -BaseSha $workflowEvent.merge_group.base_sha `
                    -Entries $entries -IsAncestor $isAncestor)
            foreach ($member in $members) {
                $pr = Invoke-ScheduledReadApi "repos/$repository/pulls/$($member.number)"
                $prScope = Get-ScheduledPullRequestScope -PullRequest $pr -Policy $policy
                if ($prScope.managed) {
                    Assert-ScheduledPullRequestChange -PullRequest $pr -Scope $prScope -Policy $policy -Root $root
                }
                $repairs += $prScope
            }
        }
        $managedRepairs = @($repairs | Where-Object managed)
        $managed = $managedRepairs.Count -gt 0
        if ($managed) {
            # A managed repair's credential/benchmark exclusions only hold while these operator
            # prerequisites are recorded; the gate must not let a repair proceed on the strength of
            # excluding production-backed tests alone. See
            # ../../.github/workflows/design.md#managed-publication.
            foreach ($name in @('benchmark_exclusion', 'azure_policy', 'native_app_canary')) {
                if (-not $policy.rollout.prerequisites[$name]) { throw "Managed publication prerequisite missing: $name" }
            }
            $packages = @($managedRepairs.packages | Sort-Object -Unique)
            $checkIds = @($managedRepairs.check_ids | Sort-Object -Unique)
            $run = $true
            $reason = 'managed-repair'
        } else {
            $reason = 'ordinary-validation'
        }
    } elseif ($Mode -eq 'verify') {
        $scope = 'confirmation'
        if ($diagnostic) {
            if ($workflowEvent.inputs.ContainsKey('source_sha') -and
                -not [string]::IsNullOrWhiteSpace($workflowEvent.inputs.source_sha)) {
                $sourceSha = $workflowEvent.inputs.source_sha.Trim()
                Assert-ScheduledSha $sourceSha
                # Only tested bytes may come from a branch/PR; controller code remains on main.
                # Ref: ../../.github/workflows/implementation.md#manual-checks.
                $commit = Invoke-ScheduledReadApi "repos/$($policy.repository)/commits/$sourceSha"
                if ($commit.sha -cne $sourceSha) { throw 'GitHub did not resolve the requested source commit.' }
            }
            $checkIds = @($workflowEvent.inputs.check_ids -split ',' | ForEach-Object { $_.Trim() })
            $packages = @($workflowEvent.inputs.packages -split ',' | ForEach-Object { $_.Trim() })
            if ('' -cin $checkIds -or '' -cin $packages) {
                throw 'Enter check IDs and crate names separated by commas, without empty entries.'
            }
            $run = $true
            $reason = 'explicit-verification'
        } elseif ($policy.rollout.hosted_execution_enabled -and $policy.rollout.prerequisites.native_app_canary) {
            # Automatic merged-repair confirmation is the one action `hosted_execution_enabled`
            # authorizes beyond recurring execution, and only once native App readiness is
            # separately recorded; without native_app_canary this branch is skipped and no
            # confirmation scope is produced. See
            # ../../.github/workflows/implementation.md#operating-policy.
            $confirmations = @(Get-ScheduledConfirmationScope -Policy $policy -SourceSha $sourceSha)
            $run = $confirmations.Count -gt 0
            if ($run) {
                $packages = @($confirmations.packages | Sort-Object -Unique)
                $checkIds = @($confirmations.check_ids | Sort-Object -Unique)
            }
            $reason = if ($run) { 'merged-repair-confirmation' } else { 'no-pending-main-confirmation' }
        }
    }
    $manifest = Get-ScheduledCheckManifest -SourceSha $sourceSha -ControllerSha $controllerSha `
        -ContractDigest $contractDigest -Scope $scope -Packages $packages -CheckIds $checkIds
    if ($Mode -eq 'scheduled') {
        if ($diagnostic) {
            # Run workflow is the authorization for fresh read-only checks, not a cache lookup
            # or permission for issue writes/repairs.
            # Ref: ../../.github/workflows/implementation.md#manual-checks.
            $run = $true
            $reason = 'manual-checks'
        } elseif ($policy.rollout.hosted_execution_enabled) {
            $coverage = Get-ScheduledCoverageIndex -Policy $policy
            $decision = Get-ScheduledRunDecision -Manifest $manifest -Coverage $coverage `
                -Now $Now -MaxAgeDays $policy.coverage.max_age_days
            $run = $decision.run
            $reason = $decision.reason
            $receipt = $decision.receipt
            if (-not $run) {
                $risk = Get-ScheduledCoverageRunRisk -Policy $policy -Receipt $receipt `
                    -CurrentRunId ([long]$env:GITHUB_RUN_ID) -CurrentRunAttempt ([int]$env:GITHUB_RUN_ATTEMPT)
                if ($risk) {
                    $run = $true
                    $reason = $risk
                    $receipt = $null
                }
            }
        }
    }
    $plan = @{
        schema_version = 1; manifest = $manifest; decision = @{ run = $run; reason = $reason; receipt = $receipt }
        managed = $managed; repairs = $repairs; confirmations = $confirmations
        release_base_sha = $releaseBaseSha
        run_id = [long]$env:GITHUB_RUN_ID; run_attempt = [int]$env:GITHUB_RUN_ATTEMPT
        run_number = [long]$env:GITHUB_RUN_NUMBER; planned_at = $Now.ToString('o')
    }
    New-Item -ItemType Directory -Path $OutputDirectory -Force | Out-Null
    $plan | ConvertTo-Json -Depth 100 | Set-Content -LiteralPath (Join-Path $OutputDirectory 'plan.json')
    $matrix = @{ include = $manifest.checks } | ConvertTo-Json -Depth 50 -Compress
    if ($env:GITHUB_OUTPUT) {
        @(
            "run=$($run.ToString().ToLowerInvariant())"
            "managed=$($managed.ToString().ToLowerInvariant())"
            "matrix=$matrix"
            "manifest=$($manifest | ConvertTo-Json -Depth 50 -Compress)"
            "source_sha=$sourceSha"
            "controller_sha=$controllerSha"
        ) | Add-Content -LiteralPath $env:GITHUB_OUTPUT
    }
    if ($diagnostic -and $env:GITHUB_STEP_SUMMARY) {
        @(
            '## Manual checks'
            ''
            "Tested commit: ``$sourceSha``."
            ''
            'Fresh execution requested. These results do not update shared main coverage or confirm a repair.'
        ) | Add-Content -LiteralPath $env:GITHUB_STEP_SUMMARY
    }
    Write-Verbose "Planning $Mode source=$sourceSha contract=${contractDigest}: run=$run because $reason."
    return $plan
}

function Invoke-ScheduledGate {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string] $PlanPath,
        [Parameter(Mandatory)][string] $ResultsDirectory,
        [Parameter(Mandatory)][string] $ContextResult,
        [Parameter(Mandatory)][string] $DeepResult,
        [Parameter(Mandatory)][long] $RunId,
        [Parameter(Mandatory)][int] $RunAttempt
    )
    if ($ContextResult -cne 'success') { throw 'Scheduled context did not succeed.' }
    $plan = Get-Content -LiteralPath $PlanPath -Raw | ConvertFrom-Json -AsHashtable
    if (-not $plan.managed) {
        Write-Verbose 'Ordinary validation: no managed repair evidence required.'
        return
    }
    if ($DeepResult -cne 'success') { throw "Relevant deep execution was $DeepResult." }
    $results = @()
    foreach ($file in Get-ChildItem -LiteralPath $ResultsDirectory -Filter evidence.json -Recurse -File) {
        $results += Get-Content -LiteralPath $file.FullName -Raw | ConvertFrom-Json -AsHashtable
    }
    $verdict = Test-ScheduledRepairEvidence -Manifest $plan.manifest -Results $results -RunId $RunId -RunAttempt $RunAttempt
    if (-not $verdict.successful) { throw "Managed repair evidence rejected: $($verdict.problems -join '; ')" }
}

Export-ModuleMember -Function Invoke-ScheduledPlanning, Invoke-ScheduledGate,
Get-ScheduledCoverageIndex, Get-ScheduledConfirmationScope
