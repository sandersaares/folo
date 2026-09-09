#requires -Version 7

# The privileged write-side GitHub adapter: runs only from the always-default-branch
# `scheduled-report.yml`/`scheduled-health.yml` jobs (never a candidate checkout), and is the only
# module authorized to write run-level intake, coverage and existing confirmation state, or to
# download and parse a run's evidence artifacts. It reads candidate artifacts as data using the
# default-branch parser (ScheduledContracts.psm1/ScheduledExecution.psm1), never by executing
# anything from the candidate. See
# ../../.github/workflows/implementation.md#scheduled-controller-ownership,
# #serialized-reporting, #independent-health and #operating-policy, and
# ../../docs/scheduled-validation.md#health-recovery-and-rollback.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
Import-Module (Join-Path $PSScriptRoot 'ScheduledContracts.psm1')
Import-Module (Join-Path $PSScriptRoot 'ScheduledPlan.psm1')
Import-Module (Join-Path $PSScriptRoot 'ScheduledReport.psm1')
Import-Module (Join-Path $PSScriptRoot 'ScheduledExecution.psm1')
Import-Module (Join-Path $PSScriptRoot 'ScheduledGate.psm1')
Import-Module (Join-Path $PSScriptRoot 'ScheduledTransport.psm1')
Import-Module (Join-Path $PSScriptRoot 'ScheduledRunGitHub.psm1')

function Invoke-ScheduledGhJson {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string[]] $Arguments,
        [AllowNull()][string] $InputJson
    )
    try {
        $json = if ($PSBoundParameters.ContainsKey('InputJson')) { $InputJson | & gh @Arguments }
            else { & gh @Arguments }
    } catch [System.Management.Automation.NativeCommandExitException] {
        throw [IO.IOException]::new('GitHub API request failed.', $_.Exception)
    }
    if ($LASTEXITCODE -ne 0) { throw [IO.IOException]::new('GitHub API request failed.') }
    return ConvertFrom-Json -InputObject ($json -join "`n") -AsHashtable
}

function Invoke-ScheduledGitHubApi {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string] $Endpoint,
        [ValidateSet('GET', 'POST', 'PATCH')][string] $Method = 'GET',
        [AllowNull()][hashtable] $Body,
        [switch] $Paginate
    )

    if ($Endpoint -notmatch '^repos/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/') {
        throw [ArgumentException]::new('Only repository API endpoints are supported.')
    }
    [string[]]$arguments = @('api', '--method', $Method, '-H', 'Accept: application/vnd.github+json', $Endpoint)
    if ($Paginate) { $arguments += @('--paginate', '--slurp') }
    if ($null -ne $Body) {
        if ($Method -ceq 'GET') { throw [ArgumentException]::new('GET cannot carry a write payload.') }
        $arguments += @('--input', '-')
        return Invoke-ScheduledGhJson -Arguments $arguments -InputJson (ConvertTo-Json -InputObject $Body -Depth 100 -Compress)
    }
    return Invoke-ScheduledGhJson -Arguments $arguments
}

function Test-ScheduledGitHubAncestor {
    [CmdletBinding()]
    [OutputType([bool])]
    param(
        [Parameter(Mandatory)][string] $Repository,
        [Parameter(Mandatory)][string] $Ancestor,
        [Parameter(Mandatory)][string] $Descendant
    )
    Assert-ScheduledSha $Ancestor
    Assert-ScheduledSha $Descendant
    if ($Ancestor -ceq $Descendant) { return $true }
    $comparison = Invoke-ScheduledGitHubApi "repos/$Repository/compare/${Ancestor}...${Descendant}"
    return $comparison.status -cin @('identical', 'ahead')
}

function Assert-ScheduledReportingRun {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][hashtable] $WorkflowEvent,
        [Parameter(Mandatory)][hashtable] $Run,
        [Parameter(Mandatory)][hashtable] $Workflow,
        [Parameter(Mandatory)][hashtable] $Policy
    )

    $allowed = @{
        'Scheduled validation' = '.github/workflows/scheduled-validation.yml'
        'Scheduled verification' = '.github/workflows/scheduled-verify.yml'
    }
    if ($WorkflowEvent.action -cne 'completed' -or
        $WorkflowEvent.repository.full_name -cne $Policy.repository -or $WorkflowEvent.repository.id -ne $Policy.repository_id -or
        $WorkflowEvent.repository.default_branch -cne 'main') { throw [FormatException]::new('Unexpected reporting event repository.') }
    foreach ($candidate in @($WorkflowEvent.workflow_run, $Run)) {
        if ($candidate.repository.id -ne $Policy.repository_id -or
            $candidate.repository.full_name -cne $Policy.repository -or
            $candidate.head_repository.id -ne $Policy.repository_id -or
            $candidate.status -cne 'completed' -or $candidate.head_branch -cne 'main' -or
            -not $allowed.ContainsKey($candidate.name) -or $candidate.path -cne $allowed[$candidate.name] -or
            $candidate.workflow_id -ne $Workflow.id -or $candidate.path -cne $Workflow.path -or
            $candidate.name -cne $Workflow.name) { throw [FormatException]::new('Run is not from an approved default-branch workflow.') }
        Assert-ScheduledSha $candidate.head_sha
        if ($candidate.id -le 0 -or $candidate.run_attempt -le 0 -or $candidate.run_number -le 0) {
            throw [FormatException]::new('Run identity is incomplete.')
        }
        $events = if ($candidate.name -ceq 'Scheduled validation') { @('schedule', 'workflow_dispatch') }
            else { @('push', 'workflow_dispatch') }
        if ($candidate.event -cnotin $events) { throw [FormatException]::new('Unsupported triggering workflow event.') }
    }
    foreach ($key in @('id', 'run_attempt', 'run_number', 'workflow_id', 'head_sha', 'event', 'conclusion')) {
        if ($WorkflowEvent.workflow_run[$key] -cne $Run[$key]) { throw [FormatException]::new("Event/API run mismatch: $key") }
    }
    if ([datetimeoffset]$WorkflowEvent.workflow_run.created_at -ne [datetimeoffset]$Run.created_at) {
        throw [FormatException]::new('Event/API run creation time mismatch.')
    }
}

function Assert-ScheduledWriteController {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][hashtable] $Policy,
        [Parameter(Mandatory)][string] $CheckoutSha,
        [Parameter(Mandatory)][string] $DefaultSha
    )
    if ($env:GITHUB_EVENT_NAME -cne 'workflow_run' -or $env:GITHUB_REPOSITORY -cne $Policy.repository -or
        $env:GITHUB_REPOSITORY_ID -cne [string]$Policy.repository_id -or
        $env:GITHUB_WORKFLOW_REF -cne "$($Policy.repository)/.github/workflows/scheduled-report.yml@refs/heads/main") {
        throw [FormatException]::new('Writes require the trusted default-branch reporting workflow.')
    }
    if (-not (Test-ScheduledGitHubAncestor $Policy.repository $CheckoutSha $DefaultSha)) {
        throw [FormatException]::new('Reporter checkout is not on the current default branch.')
    }
}

function Save-ScheduledArtifactArchive {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string] $Repository,
        [Parameter(Mandatory)][long] $ArtifactId,
        [Parameter(Mandatory)][string] $Path,
        [Parameter(Mandatory)][long] $MaxBytes
    )
    $null = Save-ScheduledGitHubResponse -Endpoint "repos/$Repository/actions/artifacts/$ArtifactId/zip" `
        -Path $Path -MaxBytes $MaxBytes
}

function Expand-ScheduledArtifact {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string] $Archive,
        [Parameter(Mandatory)][string] $Destination,
        [Parameter(Mandatory)][long] $MaxBytes
    )

    if ((Get-Item -LiteralPath $Archive).Length -gt $MaxBytes -or (Test-Path -LiteralPath $Destination)) {
        throw [FormatException]::new('Artifact size exceeds policy or extraction destination is not empty.')
    }
    $zip = [IO.Compression.ZipFile]::OpenRead($Archive)
    try {
        $entries = @()
        [long]$total = 0
        # Reject aliases on every platform rather than inferring filesystem case behavior.
        $seen = [Collections.Generic.HashSet[string]]::new([StringComparer]::OrdinalIgnoreCase)
        foreach ($entry in $zip.Entries) {
            $name = $entry.FullName.Replace('\', '/')
            $parts = @($name.TrimEnd('/') -split '/')
            $unixType = ($entry.ExternalAttributes -shr 16) -band 0xF000
            if ($name.StartsWith('/') -or $name.Contains(':') -or
                @($parts | Where-Object { $_ -in @('', '.', '..') -or $_.EndsWith('.') -or $_.EndsWith(' ') -or
                        $_ -match '^(?i:CON|PRN|AUX|NUL|COM[0-9]|LPT[0-9])(?:\.|$)' }).Count -gt 0 -or
                $unixType -eq 0xA000 -or (($entry.ExternalAttributes -band 0x400) -ne 0) -or
                -not $seen.Add($name.TrimEnd('/'))) {
                throw [FormatException]::new('Unsafe or ambiguous artifact entry.')
            }
            $total += $entry.Length
            if ($entry.Length -lt 0 -or $total -gt $MaxBytes) { throw [FormatException]::new('Expanded artifact exceeds policy size limit.') }
            $entries += @{ entry = $entry; path = Join-Path $Destination ($parts -join [IO.Path]::DirectorySeparatorChar) }
        }
        $null = New-Item -ItemType Directory -Path $Destination
        foreach ($item in $entries) {
            if ($item.entry.FullName.EndsWith('/')) {
                $null = New-Item -ItemType Directory -Path $item.path -Force
            } else {
                $null = New-Item -ItemType Directory -Path (Split-Path $item.path -Parent) -Force
                $inputStream = $item.entry.Open()
                $outputStream = [IO.File]::Open($item.path, [IO.FileMode]::CreateNew)
                try {
                    $buffer = [byte[]]::new(81920) # .NET's normal stream-copy buffer size.
                    [long]$written = 0
                    while (($count = $inputStream.Read($buffer, 0, $buffer.Length)) -gt 0) {
                        $written += $count
                        if ($written -gt $item.entry.Length) { throw [FormatException]::new('ZIP entry exceeds its declared length.') }
                        $outputStream.Write($buffer, 0, $count)
                    }
                    if ($written -ne $item.entry.Length) { throw [FormatException]::new('ZIP entry is truncated.') }
                } finally {
                    $inputStream.Dispose()
                    $outputStream.Dispose()
                }
            }
        }
    } finally { $zip.Dispose() }
}

function Get-ScheduledArtifact {
    [CmdletBinding()]
    [OutputType([string])]
    param(
        [Parameter(Mandatory)][hashtable] $Run,
        [Parameter(Mandatory)][AllowEmptyCollection()][hashtable[]] $Artifacts,
        [Parameter(Mandatory)][string] $Name,
        [Parameter(Mandatory)][hashtable] $Policy,
        [Parameter(Mandatory)][string] $OutputDirectory
    )

    $matchingArtifacts = @($Artifacts | Where-Object { $_.name -ceq $Name })
    if ($matchingArtifacts.Count -ne 1) { throw [FormatException]::new("Expected exactly one current-attempt artifact: $Name") }
    $artifact = $matchingArtifacts[0]
    if ($artifact.expired -or $artifact.size_in_bytes -le 0 -or
        $artifact.size_in_bytes -gt $Policy.coverage.max_artifact_bytes -or
        $artifact.workflow_run.id -ne $Run.id -or
        $artifact.workflow_run.repository_id -ne $Policy.repository_id -or
        $artifact.workflow_run.head_repository_id -ne $Policy.repository_id -or
        $artifact.workflow_run.head_sha -cne $Run.head_sha -or
        [datetimeoffset]$artifact.created_at -lt [datetimeoffset]$Run.run_started_at -or
        [datetimeoffset]$artifact.created_at -gt [datetimeoffset]$Run.updated_at) {
        throw [FormatException]::new('Artifact does not belong to this authoritative run attempt or exceeds policy.')
    }
    $archive = Join-Path $OutputDirectory "$($artifact.id).zip"
    $directory = Join-Path $OutputDirectory "artifact-$($artifact.id)"
    Save-ScheduledArtifactArchive -Repository $Policy.repository -ArtifactId $artifact.id -Path $archive `
        -MaxBytes $Policy.coverage.max_artifact_bytes
    Expand-ScheduledArtifact -Archive $archive -Destination $directory -MaxBytes $Policy.coverage.max_artifact_bytes
    return $directory
}

function Get-ScheduledOwnedIssue {
    [CmdletBinding()]
    [OutputType([hashtable[]])]
    param(
        [Parameter(Mandatory)][hashtable] $Policy,
        [Parameter(Mandatory)][ValidateSet('scheduled-finding', 'scheduled-coverage', 'scheduled-health', 'scheduled-run-failure')][string] $Label
    )
    $pages = Invoke-ScheduledGitHubApi "repos/$($Policy.repository)/issues?labels=$Label&state=all&per_page=100" -Paginate
    return @($pages | ForEach-Object { $_ } | Where-Object {
            -not $_.ContainsKey('pull_request') -and $_.user.login -ceq $Policy.reporter_login
        })
}

function Restore-ScheduledRunPublicationState {
    [CmdletBinding()]
    param([hashtable] $Policy, [hashtable] $Run, [string] $OutputDirectory)
    $attempt = [int]$env:GITHUB_RUN_ATTEMPT
    if ($attempt -le 1) { return }
    if ($env:GITHUB_RUN_ID -cnotmatch '^[1-9][0-9]*$') {
        throw [FormatException]::new('Reporter rerun identity is unavailable.')
    }
    $journalName = "publication-$($Policy.repository_id)-$($Run.workflow_id)-$($Run.id).json"
    $destination = Join-Path $OutputDirectory $journalName
    if (Test-Path -LiteralPath $destination) { return }
    $previousAttempt = $attempt - 1
    $previous = Invoke-ScheduledGitHubApi "repos/$($Policy.repository)/actions/runs/$env:GITHUB_RUN_ID/attempts/$previousAttempt"
    if ($previous.id -ne [long]$env:GITHUB_RUN_ID -or $previous.run_attempt -ne $previousAttempt -or
        $previous.repository.id -ne $Policy.repository_id -or $previous.head_repository.id -ne $Policy.repository_id -or
        $previous.path -cne '.github/workflows/scheduled-report.yml' -or $previous.head_branch -cne 'main' -or
        $previous.status -cne 'completed') {
        throw [FormatException]::new('Previous attempt is not the trusted reporter workflow.')
    }
    $pages = Invoke-ScheduledGitHubApi "repos/$($Policy.repository)/actions/runs/$env:GITHUB_RUN_ID/artifacts?per_page=100" -Paginate
    $artifacts = @($pages | ForEach-Object { $_.artifacts })
    $root = Join-Path $OutputDirectory "recovery-$([guid]::NewGuid().ToString('N'))"
    $null = New-Item -ItemType Directory -Path $root
    # A fresh runner must retain uncertainty from its previous writer before making new POSTs.
    # Missing/expired recovery evidence is an operator blocker, not proof that no write occurred.
    $directory = Get-ScheduledArtifact -Run $previous -Artifacts $artifacts -Policy $Policy `
        -Name "scheduled-report-$env:GITHUB_RUN_ID-$previousAttempt" -OutputDirectory $root
    $source = Join-Path $directory $journalName
    if (Test-Path -LiteralPath $source) {
        Copy-Item -LiteralPath $source -Destination $destination
        return
    }
    $reportPath = Join-Path $directory 'report.json'
    if (Test-Path -LiteralPath $reportPath) {
        $report = Get-Content -LiteralPath $reportPath -Raw | ConvertFrom-Json -AsHashtable
        $hasRunIssue = $report.ContainsKey('run_intake') -and $null -ne $report.run_intake -and
            $report.run_intake.ContainsKey('number')
        if ($report.status -cin @('passed', 'not-run', 'reported') -and -not $hasRunIssue) { return }
    }
    throw [IO.IOException]::new('Previous reporting attempt has no recoverable publication journal; reconcile its writes before retrying.')
}

function Get-ScheduledTrustedConfirmation {
    [CmdletBinding()]
    [OutputType([hashtable])]
    param(
        [Parameter(Mandatory)][hashtable] $Issue,
        [Parameter(Mandatory)][hashtable] $Record,
        [Parameter(Mandatory)][hashtable] $Manifest,
        [Parameter(Mandatory)][AllowEmptyCollection()][hashtable[]] $Results,
        [Parameter(Mandatory)][hashtable] $Policy,
        [AllowNull()][hashtable] $Declaration
    )

    $pages = Invoke-ScheduledGitHubApi "repos/$($Policy.repository)/issues/$($Issue.number)/comments?per_page=100" -Paginate
    $workers = @()
    foreach ($comment in @($pages | ForEach-Object { $_ })) {
        if ($comment.user.login -cne $Policy.worker_login -or
            -not ([string]$comment.body).Contains('<!-- scheduled-worker:')) { continue }
        $worker = Read-ScheduledRecord -Text $comment.body -Kind worker
        if ($worker.finding_id -ceq $Record.finding_id -and $worker.generation -eq $Record.generation) {
            $workers += $worker
        }
    }
    if ($Record.status -cnotin @('open', 'needs-human')) { return $null }
    if ($workers.Count -ne 1 -or $null -eq $workers[0].pr_number) { return $null }
    $pr = Invoke-ScheduledGitHubApi "repos/$($Policy.repository)/pulls/$($workers[0].pr_number)"
    if (-not $pr.merged -or $pr.state -cne 'closed' -or $pr.base.repo.id -ne $Policy.repository_id) { return $null }
    # The gate also binds issue, generation, attempt, executor, reserved branch and exact PR head.
    $scope = Get-ScheduledRepairScope -PullRequest $pr -Issue $Issue -Worker $workers[0] -Policy $Policy -Confirmation
    if (-not $scope.managed) { return $null }
    if ($null -ne $Declaration) {
        if ($Declaration.issue_number -ne $Issue.number -or $Declaration.finding_id -cne $Record.finding_id -or
            $Declaration.generation -ne $Record.generation -or $Declaration.pr_number -ne $pr.number -or
            $Declaration.merge_commit_sha -cne $pr.merge_commit_sha -or
            $Declaration.source_sha -cne $Manifest.source_sha) { throw [FormatException]::new('Confirmation declaration differs from live registration.') }
        foreach ($key in @('check_ids', 'packages')) {
            if ((Get-ScheduledDigest @($Declaration[$key] | Sort-Object)) -cne
                (Get-ScheduledDigest @($scope[$key] | Sort-Object))) { throw [FormatException]::new('Confirmation declaration has incomplete repair scope.') }
        }
        foreach ($key in @('finding_id', 'generation', 'attempt_id', 'head_sha', 'branch', 'pr_number')) {
            if ($Declaration.worker[$key] -cne $workers[0][$key]) { throw [FormatException]::new('Confirmation worker registration changed after planning.') }
        }
    }
    $expected = Get-ScheduledCheckManifest -SourceSha $Manifest.source_sha -ControllerSha $Manifest.controller_sha `
        -ContractDigest $Manifest.check_contract_digest -Scope confirmation -Packages $scope.packages -CheckIds $scope.check_ids
    $selected = @($Results | Where-Object { $_.check_id -cin $scope.check_ids })
    foreach ($expectedCheck in $expected.checks) {
        $declaredChecks = @($Manifest.checks | Where-Object { $_.id -ceq $expectedCheck.id })
        if ($declaredChecks.Count -eq 1 -and ($declaredChecks[0].packages.Count -eq 0 -or
            @($scope.packages | Where-Object { $_ -cnotin $declaredChecks[0].packages }).Count -eq 0)) {
            # A union plan or full-workspace plan covers the implicated package. Preserve every
            # other expected scope field so a broader package set cannot excuse missing shards.
            $expectedCheck.packages = @($declaredChecks[0].packages)
        }
    }
    $verdict = Test-ScheduledManifest -Manifest $expected -Results $selected
    if (-not (Test-ScheduledGitHubAncestor $Policy.repository $pr.merge_commit_sha $Manifest.source_sha)) { return $null }
    $repair = Read-ScheduledRecord -Text $pr.body -Kind repair
    # Potentially intermittent Miri failures need a durable explanation from the registered
    # repair or worker, not a green candidate artifact. Ordinary deterministic repairs use
    # the reviewed merged change plus complete main confirmation.
    $requiresExplanation = $expected.checks[0].kind -cin @('miri', 'miri-many') -and
        (-not $Policy.repair.ContainsKey('require_explained_nondeterminism') -or
            $Policy.repair.require_explained_nondeterminism -eq $true)
    $explained = -not $requiresExplanation
    foreach ($resolution in @($repair, $workers[0])) {
        if ($resolution.ContainsKey('explanation') -and
            -not [string]::IsNullOrWhiteSpace([string]$resolution.explanation)) { $explained = $true }
    }
    return @{
        authoritative = $true; generation = $Record.generation; merge_commit_sha = $pr.merge_commit_sha
        pr_number = $pr.number; source_sha = $Manifest.source_sha
        scope_complete = $verdict.complete; successful = $verdict.successful; explained = $explained
        status = if (-not $verdict.complete) { 'retry' } elseif (-not $verdict.successful) { 'failed' }
            elseif ($explained) { 'confirmed' } else { 'needs-human' }
    }
}

function Sync-ScheduledIssue {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][hashtable] $Policy,
        [AllowNull()][hashtable] $Issue,
        [Parameter(Mandatory)][hashtable] $Record,
        [Parameter(Mandatory)][ValidateSet('reporter', 'coverage')][string] $Kind,
        [switch] $Apply
    )

    if ($null -eq $Issue) {
        if ($Kind -ceq 'reporter') {
            throw [FormatException]::new('Hosted reporting cannot create problem issues; failures require AI triage.')
        }
        $body = if ($Kind -ceq 'reporter') { Get-ScheduledFindingBody $Record } else {
            "[Copilot speaking]`n`nDurable scheduled coverage index. A skipped run never refreshes the receipt.`n`n" +
                (Write-ScheduledRecord -Record $Record -Kind coverage)
        }
        $title = if ($Kind -ceq 'reporter') { "Scheduled $($Record.check_id): $($Record.package)" }
            else { 'Scheduled coverage and health' }
        $labels = if ($Kind -ceq 'reporter') { @('scheduled-finding') }
            else { @('scheduled-coverage', 'scheduled-health') }
        $payload = @{ title = $title; body = $body; labels = @($labels) }
        $endpoint = "repos/$($Policy.repository)/issues"
        $method = 'POST'
    } else {
        if ($Issue.user.login -cne $Policy.reporter_login) { throw [FormatException]::new('Cannot edit an issue owned by another author.') }
        # Re-read immediately before an exact-span edit, retaining intervening human body changes.
        $live = Invoke-ScheduledGitHubApi "repos/$($Policy.repository)/issues/$($Issue.number)"
        $before = Read-ScheduledRecord -Text $Issue.body -Kind $Kind
        $current = Read-ScheduledRecord -Text $live.body -Kind $Kind
        if ($live.user.login -cne $Policy.reporter_login -or
            (Get-ScheduledDigest $before) -cne (Get-ScheduledDigest $current)) {
            throw [FormatException]::new('Reporter record changed during reconciliation.')
        }
        $body = ConvertTo-ScheduledOwnedText -Text $live.body -Record $Record -Kind $Kind
        $state = if ($Kind -ceq 'reporter' -and $Record.status -ceq 'confirmed') { 'closed' } else { 'open' }
        if ($body -ceq $live.body -and $state -ceq $live.state) { return @{ action = 'unchanged'; number = $Issue.number } }
        $payload = @{ body = $body; state = $state }
        $endpoint = "repos/$($Policy.repository)/issues/$($Issue.number)"
        $method = 'PATCH'
    }
    # GitHub caps issue bodies; never discard the typed reproduction to fit a write.
    if ($body.Length -gt 65536) { throw [FormatException]::new('Durable issue evidence exceeds the GitHub body limit.') }
    # `reporting_enabled` authorizes issue writes independently of execution/admission (see
    # ../../.github/workflows/implementation.md#operating-policy); without it every call here is a
    # dry run regardless of `-Apply`, and the same rule gates the other reporting_enabled checks
    # below in this module.
    if ($Apply -and $Policy.rollout.reporting_enabled) {
        $updated = Invoke-ScheduledGitHubApi -Endpoint $endpoint -Method $method -Body $payload
        return @{ action = $method; number = $updated.number }
    }
    return @{ action = 'dry-run'; method = $method; endpoint = $endpoint; payload = $payload }
}

function Invoke-ScheduledReporting {
    [CmdletBinding()]
    [OutputType([hashtable])]
    param(
        [Parameter(Mandatory)][string] $Repository,
        [Parameter(Mandatory)][string] $EventPath,
        [Parameter(Mandatory)][string] $OutputDirectory,
        [switch] $Apply
    )

    $policy = Get-ScheduledPolicy
    if ($Repository -cne $policy.repository) { throw 'Repository differs from trusted policy.' }
    $OutputDirectory = [IO.Path]::GetFullPath($OutputDirectory)
    $null = New-Item -ItemType Directory -Path $OutputDirectory -Force
    $report = @{
        schema_version = 1; status = 'incomplete'; problems = @(); actions = @()
        applied = $false; writes_authorized = $false
    }
    try {
        $workflowEvent = Get-Content -LiteralPath $EventPath -Raw | ConvertFrom-Json -AsHashtable
        $eventRun = $workflowEvent.workflow_run
        $run = Invoke-ScheduledGitHubApi "repos/$Repository/actions/runs/$($eventRun.id)/attempts/$($eventRun.run_attempt)"
        $workflow = Invoke-ScheduledGitHubApi "repos/$Repository/actions/workflows/$($run.workflow_id)"
        Assert-ScheduledReportingRun -WorkflowEvent $workflowEvent -Run $run -Workflow $workflow -Policy $policy
        $root = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
        $checkoutSha = (& git -C $root rev-parse HEAD).Trim()
        $defaultRef = Invoke-ScheduledGitHubApi "repos/$Repository/git/ref/heads/main"
        if ($Apply -and $policy.rollout.reporting_enabled) {
            Assert-ScheduledWriteController -Policy $policy -CheckoutSha $checkoutSha -DefaultSha $defaultRef.object.sha
        }
        if (-not (Test-ScheduledGitHubAncestor $Repository $run.head_sha $defaultRef.object.sha)) {
            throw [FormatException]::new('Originating controller is not on default-branch ancestry.')
        }
        $applyWrites = $Apply -and $policy.rollout.reporting_enabled
        $report.writes_authorized = [bool]$applyWrites
        if ($applyWrites) {
            Restore-ScheduledRunPublicationState -Policy $policy -Run $run -OutputDirectory $OutputDirectory
        }
        $api = {
            param($Endpoint, $Method = 'GET', $Body, [switch] $Paginate)
            Invoke-ScheduledGitHubApi -Endpoint $Endpoint -Method $Method -Body $Body -Paginate:$Paginate
        }
        $logReader = {
            param($Endpoint, $Path, $MaxBytes)
            Save-ScheduledGitHubResponse -Endpoint $Endpoint -Path $Path -MaxBytes $MaxBytes -Truncate
        }
        $jobEvidence = Get-ScheduledRunJobEvidence -Policy $policy -Run $run -OutputDirectory $OutputDirectory `
            -Api $api -LogReader $logReader
        $report.problems += $jobEvidence.evidence_gaps
        $context = @{
            run_id = $run.id; run_attempt = $run.run_attempt; run_number = $run.run_number
            workflow_id = $run.workflow_id; workflow_path = $run.path
            created_at = $run.created_at; completed_at = $run.updated_at
            workflow_conclusion = $run.conclusion
            run_started_at = $run.run_started_at
        }
        $contractDigest = Get-ScheduledContractDigest -Root $root
        $coverageIssues = @(Get-ScheduledOwnedIssue -Policy $policy -Label scheduled-coverage)
        if ($coverageIssues.Count -gt 1) { throw [FormatException]::new('More than one reporter-owned coverage issue exists.') }
        $coverageIssue = if ($coverageIssues.Count -eq 1) { $coverageIssues[0] } else { $null }
        $coverage = if ($null -ne $coverageIssue) { Read-ScheduledRecord -Text $coverageIssue.body -Kind coverage } else { $null }
        $issues = @(Get-ScheduledOwnedIssue -Policy $policy -Label scheduled-finding)
        $index = @{}
        $ambiguous = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
        foreach ($issue in $issues) {
            try {
                $record = Read-ScheduledRecord -Text $issue.body -Kind reporter
                if ($record.repository -cne $Repository -or $record.repository_id -ne $policy.repository_id -or
                    -not $record.ContainsKey('finding_id') -or [string]::IsNullOrWhiteSpace($record.finding_id)) {
                    throw [FormatException]::new('Malformed or foreign existing repair identity.')
                }
                if ($ambiguous.Contains($record.finding_id) -or $index.ContainsKey($record.finding_id)) {
                    $index.Remove($record.finding_id)
                    $null = $ambiguous.Add($record.finding_id)
                    throw [FormatException]::new('Ambiguous existing repair identity.')
                }
                $index[$record.finding_id] = @{ issue = $issue; record = $record }
            } catch [FormatException], [ArgumentException] {
                # An unrelated retained registration must not discard the new run's evidence.
                $report.problems += "Issue $($issue.number) confirmation: $($_.Exception.Message)"
            }
        }
        $apiCommand = Get-Command Test-ScheduledGitHubAncestor
        $isAncestor = { param($ancestor, $descendant)
            & $apiCommand $Repository $ancestor $descendant
        }.GetNewClosure()
        $scope = if ($run.name -ceq 'Scheduled validation') { 'full' } else { 'confirmation' }
        $manifest = Get-ScheduledCheckManifest -SourceSha $run.head_sha -ControllerSha $run.head_sha `
            -ContractDigest $contractDigest -Scope $scope
        $results = @()
        $confirmations = @()
        $skipped = $false
        $validatedPlan = $null
        $artifacts = @()
        $downloadRoot = $null
        try {
            $pages = Invoke-ScheduledGitHubApi "repos/$Repository/actions/runs/$($run.id)/artifacts?per_page=100" -Paginate
            $artifacts = @($pages | ForEach-Object { $_.artifacts })
            # Local repeat invocations cannot accidentally reuse a partially downloaded archive.
            $downloadRoot = Join-Path $OutputDirectory "run-$($run.id)-$($run.run_attempt)-$([guid]::NewGuid().ToString('N'))"
            $null = New-Item -ItemType Directory -Path $downloadRoot
            $planDirectory = Get-ScheduledArtifact -Run $run -Artifacts $artifacts -Policy $policy `
                -Name "scheduled-plan-$($run.id)-$($run.run_attempt)" -OutputDirectory $downloadRoot
            $plan = Get-Content -LiteralPath (Join-Path $planDirectory 'plan.json') -Raw | ConvertFrom-Json -AsHashtable
            foreach ($key in @('run_id', 'run_attempt', 'run_number')) {
                if ($plan[$key] -ne $context[$key]) { throw [FormatException]::new('Plan belongs to another run attempt.') }
            }
            if ($plan.schema_version -ne 1 -or $plan.manifest.schema_version -ne 1 -or
                $plan.manifest.repository -cne $Repository -or
                $plan.manifest.controller_sha -cne $run.head_sha -or
                $plan.manifest.check_contract_digest -cne $contractDigest -or
                $plan.manifest.scope -cne $scope) { throw [FormatException]::new('Plan identity or contract is incompatible.') }
            Assert-ScheduledSha $plan.manifest.source_sha
            if (-not (& $isAncestor $plan.manifest.source_sha $run.head_sha) -or
                ($scope -ceq 'full' -and $plan.manifest.source_sha -cne $run.head_sha)) {
                throw [FormatException]::new('Planned source is not the authoritative main candidate.')
            }
            $packages = @()
            $checkIds = @()
            if ($plan.decision.run -isnot [bool]) { throw [FormatException]::new('Plan decision is malformed.') }
            # No-work verification retains the unselected catalog. Package-specific many-seed
            # entries are not a global package selection when no confirmation was admitted.
            if ($scope -ceq 'confirmation' -and $plan.decision.run) {
                $packages = @($plan.manifest.checks.packages | Sort-Object -Unique)
                $checkIds = @($plan.manifest.checks.id)
                foreach ($packageName in $packages) {
                    if ($packageName -cnotin $policy.repair.allowed_packages) { throw [FormatException]::new('Unapproved confirmation package.') }
                }
                if ($packages.Count -eq 0 -and $plan.decision.run) { throw [FormatException]::new('Confirmation has no package scope.') }
            }
            $manifest = Get-ScheduledCheckManifest -SourceSha $plan.manifest.source_sha -ControllerSha $run.head_sha `
                -ContractDigest $contractDigest -Scope $scope -Packages $packages -CheckIds $checkIds
            if ((Get-ScheduledDigest $manifest.checks) -cne (Get-ScheduledDigest $plan.manifest.checks)) {
                throw [FormatException]::new('Artifact manifest differs from the trusted check catalog.')
            }
            if ($plan.ContainsKey('confirmations')) {
                if ($plan.confirmations -isnot [array]) { throw [FormatException]::new('Plan confirmations must be an array.') }
                $confirmations = @($plan.confirmations)
            }
            if (-not $plan.ContainsKey('planned_at')) { throw [FormatException]::new('Plan has no planning timestamp.') }
            $plannedAt = [datetimeoffset]$plan.planned_at
            if ($plannedAt -lt [datetimeoffset]$run.run_started_at -or
                $plannedAt -gt [datetimeoffset]$run.updated_at) {
                throw [FormatException]::new('Planning timestamp is outside its originating attempt.')
            }
            $validatedPlan = $plan
            $skipped = -not $plan.decision.run
            if (-not $skipped) {
                $resultContext = $context.Clone()
                foreach ($key in @('source_sha', 'controller_sha', 'check_contract_digest')) {
                    $resultContext[$key] = $manifest[$key]
                }
                foreach ($check in $manifest.checks) {
                    try {
                        $directory = Get-ScheduledArtifact -Run $run -Artifacts $artifacts -Policy $policy `
                            -Name "scheduled-result-$($run.id)-$($run.run_attempt)-$($check.id)" -OutputDirectory $downloadRoot
                        $results += Get-ScheduledCheckResult -Check $check -OutputDirectory $directory -RunContext $resultContext
                    } catch [FormatException], [ArgumentException], [IO.IOException], [System.Management.Automation.ItemNotFoundException] {
                        $report.problems += "$($check.id): $($_.Exception.Message)"
                    }
                }
            }
        } catch [FormatException], [ArgumentException], [IO.IOException], [System.Management.Automation.ItemNotFoundException] {
            $report.problems += $_.Exception.Message
        }
        $verdict = Test-ScheduledManifest -Manifest $manifest -Results $results
        if ($report.problems.Count -gt 0) { $skipped = $false }
        $jobFailureContradictsResults = $jobEvidence.has_unsuccessful_jobs -and ($verdict.successful -or $skipped)
        $expectedFindingFailure = $run.conclusion -ceq 'failure' -and $verdict.complete -and
            @($results | Where-Object outcome -CEQ findings).Count -gt 0
        $unexpectedWorkflowFailure = ($run.conclusion -cne 'success' -and -not $expectedFindingFailure) -or
            $jobFailureContradictsResults
        if ($unexpectedWorkflowFailure) {
            # Failed jobs remain evidence for AI triage even if apparently green artifacts exist.
            # This is not a semantic diagnosis, and cannot mint a passing coverage receipt.
            $verdict.complete = $false
            $verdict.successful = $false
            $skipped = $false
            $report.problems += if ($jobFailureContradictsResults) { 'Job/step failures contradict passing or skipped check evidence.' }
                else { "Originating workflow concluded $($run.conclusion)." }
        }
        $context.evidence_complete = $verdict.complete -and $report.problems.Count -eq 0
        $executionEvidenceComplete = $context.evidence_complete
        if (-not $skipped) {
            $confirmedScope = @{}
            foreach ($declaration in $confirmations) {
                try {
                    if ($scope -cne 'confirmation' -or -not $index.ContainsKey($declaration.finding_id) -or
                        $confirmedScope.ContainsKey($declaration.finding_id)) { throw [FormatException]::new('Unknown or duplicate declared confirmation.') }
                    $entry = $index[$declaration.finding_id]
                    $confirmed = Get-ScheduledTrustedConfirmation -Issue $entry.issue -Record $entry.record `
                        -Manifest $manifest -Results $results -Policy $policy -Declaration $declaration
                    if ($null -eq $confirmed) { throw [FormatException]::new('Declared confirmation has no authoritative merged registration.') }
                    if (-not $executionEvidenceComplete) {
                        $confirmed.scope_complete = $false
                        $confirmed.successful = $false
                        $confirmed.status = 'retry'
                    }
                    $confirmed.observation = $context.Clone()
                    $confirmedScope[$declaration.finding_id] = $confirmed
                } catch [FormatException], [ArgumentException], [IO.IOException] {
                    $report.problems += $_.Exception.Message
                }
            }
            $pendingIssues = @{}
            # Retained registrations may still receive authoritative confirmation. Parsed
            # failures are preserved in run intake instead of creating/updating problem identities.
            foreach ($entry in $index.Values) {
                if ($null -eq $entry.issue -or $entry.record.status -ceq 'confirmed') { continue }
                $confirmation = if ($confirmedScope.ContainsKey($entry.record.finding_id)) {
                    $confirmedScope[$entry.record.finding_id]
                } else { $null }
                if ($null -eq $confirmation) {
                    # Undeclared diagnostic verification cannot resolve registered problems.
                    if ($scope -cne 'full' -or -not $executionEvidenceComplete) { continue }
                    $matching = @($results | Where-Object { $_.check_id -ceq $entry.record.check_id -and $_.outcome -ceq 'passed' })
                    if ($matching.Count -ne 1) { continue }
                    if ($matching[0].actual_scope.packages.Count -gt 0 -and
                        $entry.record.package -cnotin $matching[0].actual_scope.packages) { continue }
                    # Complete full-main evidence can arrive before the dedicated verification
                    # report. Use the same live merged-registration checks in either order.
                    try {
                        $confirmation = Get-ScheduledTrustedConfirmation -Issue $entry.issue -Record $entry.record `
                            -Manifest $manifest -Results $results -Policy $policy
                    } catch [FormatException], [ArgumentException], [IO.IOException] {
                        $report.problems += "Issue $($entry.issue.number) confirmation: $($_.Exception.Message)"
                        continue
                    }
                }
                if ($null -eq $confirmation) { continue }
                $incoming = $entry.record | ConvertTo-Json -Depth 100 | ConvertFrom-Json -AsHashtable
                $incoming.source_sha = $manifest.source_sha
                $incoming.controller_sha = $manifest.controller_sha
                $incoming.check_contract_digest = $manifest.check_contract_digest
                $incoming.observation = $context.Clone()
                $incoming.observation.outcome = if ($null -eq $confirmation -or $confirmation.successful) { 'passed' } else { 'incomplete' }
                $incoming.confirmation = $confirmation
                $merged = Merge-ScheduledObservation -Existing $entry.record -Incoming $incoming -IsAncestor $isAncestor
                $pendingIssues[$entry.record.finding_id] = @{ issue = $entry.issue; record = $merged }
            }
            foreach ($pending in $pendingIssues.Values) {
                $report.actions += Sync-ScheduledIssue -Policy $policy -Issue $pending.issue -Record $pending.record -Kind reporter -Apply:$applyWrites
            }
        }
        $intakeGaps = @($report.problems)
        if (-not $skipped) { $intakeGaps += $verdict.problems }
        $intake = Sync-ScheduledRunIntake -Policy $policy -Run $run -Plan $validatedPlan `
            -Manifest $manifest -Results $results -Jobs $jobEvidence.jobs `
            -EvidenceGaps $intakeGaps -Skipped:$skipped `
            -Artifacts $artifacts -TransientPaths (@($OutputDirectory, $downloadRoot) + $jobEvidence.transient_paths) `
            -OutputDirectory $OutputDirectory -Api $api -Apply:$applyWrites
        $report.actions += $intake.actions
        $report.run_intake = $intake
        $nextCoverage = Merge-ScheduledCoverage -Coverage $coverage -Manifest $manifest -Results $results `
            -Context $context -IsAncestor $isAncestor -Skipped:$skipped
        $nextCoverage.repository_id = $policy.repository_id
        if ($null -ne $validatedPlan -and (-not $nextCoverage.ContainsKey('last_plan') -or
            (Compare-ScheduledObservation $context $nextCoverage.last_plan) -gt 0)) {
            $nextCoverage.last_plan = $context.Clone()
            $nextCoverage.last_plan.planned_at = ([datetimeoffset]$validatedPlan.planned_at).ToString('o')
            $nextCoverage.last_plan.source_sha = $manifest.source_sha
            $nextCoverage.last_plan.check_contract_digest = $manifest.check_contract_digest
            $nextCoverage.last_plan.reason = $validatedPlan.decision.reason
            $nextCoverage.last_plan.run = $validatedPlan.decision.run
            $nextCoverage.planning = @{
                outcome = if ($skipped) { $validatedPlan.decision.reason } else { 'passed' }
                completed_at = $nextCoverage.last_plan.planned_at; run_id = $context.run_id; run_attempt = $context.run_attempt
            }
        }
        if (-not $nextCoverage.ContainsKey('reporting') -or
            [datetimeoffset]$context.completed_at -ge [datetimeoffset]$nextCoverage.reporting.completed_at) {
            $nextCoverage.reporting = @{
                # Capturing a failed execution as durable intake is successful reporting.
                # Coverage and run evidence retain their own failure/incompleteness.
                outcome = 'passed'
                completed_at = $context.completed_at; run_id = $context.run_id; run_attempt = $context.run_attempt
            }
        }
        $report.actions += Sync-ScheduledIssue -Policy $policy -Issue $coverageIssue -Record $nextCoverage -Kind coverage -Apply:$applyWrites
        $report.status = if ($intake.requires_triage) { 'reported' }
            elseif ($skipped) { 'not-run' } else { 'passed' }
        $report.applied = [bool]$applyWrites
        $report.coverage = $nextCoverage
    } catch [FormatException], [ArgumentException], [IO.IOException] {
        $report.status = 'incomplete'
        $report.problems += $_.Exception.Message
    } finally {
        $report | ConvertTo-Json -Depth 100 | Set-Content -LiteralPath (Join-Path $OutputDirectory 'report.json') -Encoding utf8
    }
    return $report
}

function Get-ScheduledGitHubHealth {
    [CmdletBinding()]
    [OutputType([hashtable])]
    param(
        [Parameter(Mandatory)][string] $Repository,
        [Parameter(Mandatory)][datetimeoffset] $Now
    )
    $policy = Get-ScheduledPolicy
    if ($Repository -cne $policy.repository) { throw 'Repository differs from trusted policy.' }
    $staged = $policy.ContainsKey('rollout') -and
        $policy.rollout.hosted_execution_enabled -eq $false -and $policy.rollout.reporting_enabled -eq $false
    $scheduler = $null
    $coverage = $null
    $planning = $null
    $reporting = $null
    $localScan = $null
    $manifest = $null
    $problems = @()
    try {
        $scheduler = Invoke-ScheduledGitHubApi "repos/$Repository/actions/workflows/scheduled-validation.yml"
        $reference = Invoke-ScheduledGitHubApi "repos/$Repository/git/ref/heads/main"
        $root = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
        $manifest = Get-ScheduledCheckManifest -SourceSha $reference.object.sha -ControllerSha $reference.object.sha `
            -ContractDigest (Get-ScheduledContractDigest -Root $root)
        $issues = @(Get-ScheduledOwnedIssue -Policy $policy -Label scheduled-coverage)
        if ($issues.Count -eq 0 -and $staged) {
            $health = Get-ScheduledHealth -Scheduler $scheduler -Manifest $manifest -Now $Now -Staged
            $health.rollout = $policy.rollout
            $health.problems = @()
            return $health
        }
        if ($issues.Count -ne 1) { throw [FormatException]::new('Coverage index is absent or ambiguous.') }
        $coverage = Read-ScheduledRecord -Text $issues[0].body -Kind coverage
        if ($coverage.repository -cne $Repository -or $coverage.repository_id -ne $policy.repository_id) {
            throw [FormatException]::new('Coverage index repository differs from trusted policy.')
        }
        if ($coverage.ContainsKey('planning')) { $planning = $coverage.planning }
        # A reporter unable to write its durable state cannot record its own failure there.
        # Actions run metadata makes that failure visible without waiting for the receipt to age.
        $reportRuns = Invoke-ScheduledGitHubApi "repos/$Repository/actions/workflows/scheduled-report.yml/runs?branch=main&status=completed&per_page=1"
        if ($reportRuns.workflow_runs.Count -eq 0) {
            $reporting = $null
        } else {
            $latestReport = $reportRuns.workflow_runs[0]
            $reporting = @{
                outcome = $latestReport.conclusion; completed_at = $latestReport.updated_at
                run_id = $latestReport.id; run_attempt = $latestReport.run_attempt
            }
        }
        # Queue overflow and failed writers require replay of the existing reporting run.
        # A later success must not hide these unrecovered origins; API rerun status clears
        # each item only when that same run succeeds.
        $unresolvedReports = @()
        foreach ($conclusion in @('failure', 'cancelled', 'timed_out')) {
            $pages = @(Invoke-ScheduledGitHubApi `
                "repos/$Repository/actions/workflows/scheduled-report.yml/runs?branch=main&status=$conclusion&per_page=100" -Paginate)
            $failedRuns = @($pages | ForEach-Object { $_.workflow_runs })
            if ($pages.Count -eq 0 -or $failedRuns.Count -lt $pages[0].total_count) {
                throw [FormatException]::new('Reporting recovery inventory is incomplete.')
            }
            foreach ($failedRun in $failedRuns) {
                $unresolvedReports += @{
                    run_id = $failedRun.id; run_attempt = $failedRun.run_attempt
                    conclusion = $failedRun.conclusion
                }
            }
        }
        if ($unresolvedReports.Count -gt 0) {
            $reporting = @{
                outcome = 'failure'; completed_at = $Now.ToString('o')
                unresolved_runs = $unresolvedReports; latest_run = $reporting
            }
        }
        $healthIssues = @(Get-ScheduledOwnedIssue -Policy $policy -Label scheduled-health)
        if ($healthIssues.Count -ne 1 -or $healthIssues[0].state -cne 'open') {
            throw [FormatException]::new('The open reporter-owned health issue is absent or ambiguous.')
        }
        if ($healthIssues[0].number -ne $issues[0].number) {
            throw [FormatException]::new('Coverage and executor health must share the registered issue.')
        }
        $pages = Invoke-ScheduledGitHubApi "repos/$Repository/issues/$($healthIssues[0].number)/comments?per_page=100" -Paginate
        $healthComments = @($pages | ForEach-Object { $_ } | Where-Object {
                $_.user.login -ceq $policy.worker_login -and ([string]$_.body).Contains('<!-- scheduled-health:')
            })
        if ($healthComments.Count -gt 1) { throw [FormatException]::new('Local health comment ownership is ambiguous.') }
        if ($healthComments.Count -eq 1) {
            $localRecord = Read-ScheduledRecord -Text $healthComments[0].body -Kind health
            if ($localRecord.repository -ceq $Repository -and $localRecord.repository_id -eq $policy.repository_id -and
                -not [string]::IsNullOrWhiteSpace($policy.local.enrolled_machine_id) -and
                $localRecord.executor_id -ceq $policy.local.enrolled_machine_id) {
                if ($localRecord.ContainsKey('last_successful_scan')) {
                    $localScan = @{
                        completed_at = $localRecord.last_successful_scan
                        outcome = if ($localRecord.blocked_conditions.Count -gt 0) { 'failed' } else { 'passed' }
                        details = $localRecord
                    }
                } else { $localScan = $localRecord }
            }
        }
    } catch [FormatException], [ArgumentException], [IO.IOException] { $problems += $_.Exception.Message }
    $health = Get-ScheduledHealth -Scheduler $scheduler -Coverage $coverage -Manifest $manifest `
        -Planning $planning -Reporting $reporting -LocalScan $localScan -Now $Now `
        -ExpectedPlanGapHours $policy.coverage.expected_plan_gap_hours `
        -ExpectedLocalGapMinutes $policy.local.expected_poll_gap_minutes -MaxAgeDays $policy.coverage.max_age_days -Staged:$staged
    $health.problems = $problems
    if ($problems.Count -gt 0) { $health.status = 'failed'; $health.healthy = $false }
    if ($policy.ContainsKey('rollout')) { $health.rollout = $policy.rollout }
    return $health
}

Export-ModuleMember -Function Invoke-ScheduledGitHubApi, Test-ScheduledGitHubAncestor,
Assert-ScheduledReportingRun, Assert-ScheduledWriteController, Save-ScheduledArtifactArchive,
Expand-ScheduledArtifact, Get-ScheduledArtifact, Get-ScheduledOwnedIssue,
Get-ScheduledTrustedConfirmation, Sync-ScheduledIssue, Invoke-ScheduledReporting, Get-ScheduledGitHubHealth
