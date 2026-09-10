#requires -Version 7

# Collects Actions job evidence for run-level intake, including failures before any checker
# artifact exists. The reporter supplies its already validated run and its GitHub API callback;
# this module never executes candidate code or decides which symptoms share a root cause.
# Bounded log capture remains process orchestration; structured records are built by Rust.
# Reporting label bootstrap shares the write authorization and durable journal boundary.
# Ref: ../../.github/workflows/implementation.md#run-level-failure-intake.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
Import-Module (Join-Path $PSScriptRoot 'ScheduledTransport.psm1')
Import-Module (Join-Path $PSScriptRoot 'ScheduledContracts.psm1')
Import-Module (Join-Path $PSScriptRoot 'ScheduledExecution.psm1')
Import-Module (Join-Path $PSScriptRoot '..\build\CargoExecutable.psm1')
$script:runRecordExecutable = $null

# Retain a bounded diagnostic sample while leaving the original Actions log available by URL.
# This prevents noisy failed jobs from making reporting storage or GitHub payloads unbounded.
$script:JobLogCaptureLimit = 1MB
$script:JobLogExcerptCharacters = 4096

function Invoke-ScheduledRunRecord {
    [CmdletBinding()]
    [OutputType([hashtable])]
    param([Parameter(Mandatory)][hashtable] $Request)
    $root = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..')).Path
    if ($null -eq $script:runRecordExecutable -or
        -not (Test-Path -LiteralPath $script:runRecordExecutable -PathType Leaf)) {
        # Only the controller can supply the utility, its Cargo configuration and target root.
        # Separate Windows/WSL binaries even when both environments share this checkout.
        $platform = if ($IsWindows) { 'windows' } elseif ($IsLinux) { 'linux' } else { 'unsupported' }
        $architecture = [Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString()
        $target = Join-Path $root "target\scheduled-run-record\$platform-$architecture"
        $toolchain = Get-ScheduledToolchain -Kind mutants
        $cargo = (Get-Command cargo -CommandType Application | Select-Object -First 1).Source
        $environment = @{
            RUSTUP_TOOLCHAIN = $toolchain; RUSTUP_AUTO_INSTALL = '0'; CARGO_TARGET_DIR = $target
            CARGO_BUILD_TARGET = $null; CARGO_ENCODED_RUSTFLAGS = $null; CARGO_TERM_COLOR = 'never'; NO_COLOR = '1'
            RUSTFLAGS = ''; RUSTDOCFLAGS = ''; MIRIFLAGS = ''; MUTATION_TESTING = $null
            RUSTC = $null; RUSTDOC = $null; RUSTC_WRAPPER = $null; RUSTC_WORKSPACE_WRAPPER = $null
        }
        $messages = Invoke-ScheduledJsonExecutable -Executable $cargo -Directory $root -InputText '' `
            -Arguments @("+$toolchain", 'build', '--locked', '--package', 'scheduled-run-record',
                '--bin', 'scheduled-run-record', '--manifest-path', (Join-Path $root 'Cargo.toml'),
                '--target-dir', $target, '--message-format=json') -Environment $environment
        $executable = Resolve-CargoExecutable -CargoMessage @($messages -split '\r?\n') -TargetName 'scheduled-run-record'
        $script:runRecordExecutable = (Resolve-Path -LiteralPath $executable).Path
    }
    $json = Invoke-ScheduledJsonExecutable -Executable $script:runRecordExecutable -Directory $root `
        -InputText ($Request | ConvertTo-Json -Depth 100 -Compress)
    return ConvertFrom-Json -InputObject $json -AsHashtable
}

function Get-ScheduledRunJobEvidence {
    [CmdletBinding()]
    [OutputType([hashtable])]
    param(
        [Parameter(Mandatory)][hashtable] $Policy,
        [Parameter(Mandatory)][hashtable] $Run,
        [Parameter(Mandatory)][string] $OutputDirectory,
        [Parameter(Mandatory)][scriptblock] $Api,
        [scriptblock] $LogReader = {
            param($Endpoint, $Path, $MaxBytes)
            Save-ScheduledGitHubResponse -Endpoint $Endpoint -Path $Path -MaxBytes $MaxBytes -Truncate
        }
    )
    $jobs = [Collections.Generic.List[object]]::new()
    $gaps = [Collections.Generic.List[string]]::new()
    $unsuccessful = $false
    $seen = [Collections.Generic.HashSet[long]]::new()
    $directory = Join-Path ([IO.Path]::GetFullPath($OutputDirectory)) "jobs-$([guid]::NewGuid().ToString('N'))"
    $null = New-Item -ItemType Directory -Path $directory -Force
    try {
        $pages = @(& $Api -Endpoint (
                "repos/$($Policy.repository)/actions/runs/$($Run.id)/attempts/$($Run.run_attempt)/jobs?per_page=100") -Paginate)
        if ($pages.Count -eq 0) { throw [FormatException]::new('Job inventory has no response pages.') }
        $pages | ConvertTo-Json -Depth 100 |
            Set-Content -LiteralPath (Join-Path $directory 'inventory.json') -Encoding utf8
        $total = $null
        foreach ($page in $pages) {
            if (-not $page.ContainsKey('total_count') -or [string]$page.total_count -cnotmatch '^(0|[1-9][0-9]*)$' -or
                -not $page.ContainsKey('jobs') -or $page.jobs -isnot [array]) {
                throw [FormatException]::new('Job inventory page is malformed.')
            }
            if ($null -eq $total) { $total = [long]$page.total_count }
            elseif ($total -ne $page.total_count) { throw [FormatException]::new('Job inventory changed across pages.') }
            foreach ($job in $page.jobs) {
                if ($job -isnot [hashtable] -or -not $job.ContainsKey('id') -or
                    [string]$job.id -cnotmatch '^[1-9][0-9]*$' -or
                    -not $job.ContainsKey('run_id') -or $job.run_id -ne $Run.id -or
                    ($job.ContainsKey('run_attempt') -and $job.run_attempt -ne $Run.run_attempt) -or
                    ($job.ContainsKey('head_sha') -and $job.head_sha -cne $Run.head_sha)) {
                    throw [FormatException]::new('Job metadata does not identify this run attempt.')
                }
                if (-not $seen.Add([long]$job.id)) {
                    throw [FormatException]::new('Job inventory contains a duplicate job ID.')
                }
                $jobs.Add($job)
            }
        }
        if ($jobs.Count -ne $total -or $total -eq 0) {
            throw [FormatException]::new('Job inventory is empty or pagination is incomplete.')
        }
    } catch [FormatException], [ArgumentException], [IO.IOException] {
        $gaps.Add("Job inventory: $($_.Exception.Message)")
    }

    foreach ($job in $jobs) {
        if (-not $job.ContainsKey('status') -or -not $job.ContainsKey('conclusion') -or
            -not $job.ContainsKey('steps') -or $job.steps -isnot [array]) {
            $gaps.Add("Job $($job.id): missing status, conclusion or step inventory.")
            $unsuccessful = $true
            continue
        }
        # Skipped jobs have no execution log. Their scope remains in the inventory for analysis,
        # while successful jobs need no extra download: failures in their steps are still visible.
        $failedStep = $false
        foreach ($step in $job.steps) {
            if ($step -isnot [hashtable] -or -not $step.ContainsKey('status') -or
                -not $step.ContainsKey('conclusion')) {
                $gaps.Add("Job $($job.id): malformed step inventory.")
                $failedStep = $true
            } elseif ($step.status -cne 'completed' -or $step.conclusion -cnotin @('success', 'skipped')) {
                $failedStep = $true
            }
        }
        if ($job.status -ceq 'completed' -and $job.conclusion -cin @('success', 'skipped') -and -not $failedStep) {
            continue
        }
        $unsuccessful = $true
        $url = "https://api.github.com/repos/$($Policy.repository)/actions/jobs/$($job.id)/logs"
        $path = Join-Path $directory "$($job.id).log"
        try {
            $capture = & $LogReader `
                -Endpoint "repos/$($Policy.repository)/actions/jobs/$($job.id)/logs" -Path $path `
                -MaxBytes ([Math]::Min($script:JobLogCaptureLimit, $Policy.coverage.max_artifact_bytes))
            $text = [IO.File]::ReadAllText($path, [Text.Encoding]::UTF8) -replace '\x1b\[[0-9;]*[A-Za-z]', ''
            $excerptTruncated = $text.Length -gt $script:JobLogExcerptCharacters
            if ($excerptTruncated) { $text = $text.Substring($text.Length - $script:JobLogExcerptCharacters) }
            $job.log = @{
                url = $url; excerpt = $text; bytes = $capture.bytes
                truncated = $capture.truncated; excerpt_truncated = $excerptTruncated
            }
            if ($capture.bytes -eq 0) { $gaps.Add("Job $($job.id): execution log is empty.") }
        } catch [FormatException], [ArgumentException], [IO.IOException] {
            $job.log = @{ url = $url; unavailable = $true }
            $gaps.Add("Job $($job.id) log: $($_.Exception.Message)")
        }
    }
    return @{
        jobs = $jobs.ToArray(); evidence_gaps = $gaps.ToArray(); has_unsuccessful_jobs = $unsuccessful
        transient_paths = @($directory)
    }
}

function Get-ScheduledRunIssue {
    [CmdletBinding()]
    param([hashtable] $Policy, [hashtable] $Identity, [scriptblock] $Api)
    $pages = & $Api -Endpoint "repos/$($Policy.repository)/issues?state=all&labels=scheduled-run-failure&per_page=100" -Paginate
    $matching = @(
        foreach ($issue in @($pages | ForEach-Object { $_ })) {
            if ($issue.ContainsKey('pull_request') -or $issue.user.login -cne $Policy.reporter_login) { continue }
            # A damaged owned root cannot be silently ignored: it may be this run's lost create.
            $root = Read-ScheduledRecord -Text ([string]$issue.body) -Kind run
            if ($root.repository_id -eq $Identity.repository_id -and $root.workflow_id -eq $Identity.workflow_id -and
                $root.run_id -eq $Identity.run_id) { $issue }
        }
    )
    if ($matching.Count -gt 1) { throw [FormatException]::new('More than one owned issue identifies this scheduled run.') }
    if ($matching.Count -eq 1) { return $matching[0] }
    return $null
}

function Get-ScheduledRunComment {
    [CmdletBinding()]
    param([hashtable] $Policy, [long] $IssueNumber, [scriptblock] $Api)
    $pages = & $Api -Endpoint "repos/$($Policy.repository)/issues/$IssueNumber/comments?per_page=100" -Paginate
    return @($pages | ForEach-Object { $_ } | Where-Object { $_.user.login -ceq $Policy.reporter_login } |
        ForEach-Object { @{ id = $_.id; body = [string]$_.body } })
}

function Get-ScheduledRunBodyBlock {
    [CmdletBinding()]
    param([Parameter(Mandatory)][string] $Text)
    $blocks = [regex]::Matches($Text, '(?s)<!-- scheduled-run-content:start -->.*?<!-- scheduled-run-content:end -->')
    if ($blocks.Count -ne 1) { throw [FormatException]::new('Run issue has no unique reporter-owned content block.') }
    return $blocks[0]
}

function ConvertTo-ScheduledRunBodyBlock {
    [CmdletBinding()]
    param([Parameter(Mandatory)][string] $Body)
    $prefix = '[Copilot speaking]'
    if (-not $Body.StartsWith($prefix)) { throw [FormatException]::new('Run utility omitted the communication prefix.') }
    return "<!-- scheduled-run-content:start -->`n$($Body.Substring($prefix.Length).TrimStart())`n<!-- scheduled-run-content:end -->"
}

function Get-ScheduledRunPageMap {
    [CmdletBinding()]
    param([AllowEmptyCollection()][object[]] $Comments)
    $pages = @{}
    foreach ($comment in $Comments) {
        if (-not $comment.body.StartsWith("[Copilot speaking]`n<!-- scheduled-run-evidence:v1 ")) { continue }
        $header = Read-ScheduledRecord -Text $comment.body -Kind run-evidence
        $operation = "scheduled-run-evidence/v1/$($header.identity.repository_id)/$($header.identity.workflow_id)/$($header.identity.run_id)/$($header.run_attempt)/$($header.digest)/$($header.page)"
        if ($pages.ContainsKey($operation)) {
            if ($pages[$operation].body -cne $comment.body) {
                throw [FormatException]::new('Existing evidence page has conflicting content.')
            }
            if ($comment.id -lt $pages[$operation].id) { $pages[$operation] = $comment }
        } else { $pages[$operation] = $comment }
    }
    return $pages
}

function Write-ScheduledRunJournal {
    [CmdletBinding()]
    param([string] $Path, [hashtable] $Record)
    # Persist intent before each external write. The journal is part of the reporter artifact
    # and must be restored when recovering an uncertain write in a fresh workflow attempt.
    $temporary = "$Path.$([guid]::NewGuid().ToString('N')).tmp"
    $bytes = [Text.Encoding]::UTF8.GetBytes(($Record | ConvertTo-Json -Depth 20 -Compress))
    $stream = [IO.File]::Open($temporary, [IO.FileMode]::CreateNew, [IO.FileAccess]::Write, [IO.FileShare]::None)
    try { $stream.Write($bytes); $stream.Flush($true) } finally { $stream.Dispose() }
    [IO.File]::Move($temporary, $Path, $true)
}

function Sync-ScheduledRunIntake {
    [CmdletBinding()]
    [OutputType([hashtable])]
    param(
        [Parameter(Mandatory)][hashtable] $Policy,
        [Parameter(Mandatory)][hashtable] $Run,
        [AllowNull()][hashtable] $Plan,
        [Parameter(Mandatory)][hashtable] $Manifest,
        [Parameter(Mandatory)][AllowEmptyCollection()][hashtable[]] $Results,
        [Parameter(Mandatory)][AllowEmptyCollection()][object[]] $Jobs,
        [Parameter(Mandatory)][AllowEmptyCollection()][string[]] $EvidenceGaps,
        [AllowEmptyCollection()][object[]] $Artifacts = @(),
        [AllowEmptyCollection()][AllowNull()][object[]] $TransientPaths = @(),
        [Parameter(Mandatory)][string] $OutputDirectory,
        [Parameter(Mandatory)][scriptblock] $Api,
        [switch] $Skipped,
        [switch] $Apply
    )
    $evidence = @{
        repository = @{ id = $Policy.repository_id; name = $Policy.repository }
        workflow = @{ id = $Run.workflow_id; name = $Run.name; path = $Run.path }
        run_id = $Run.id
        attempt = @{
            run_attempt = $Run.run_attempt; run_number = $Run.run_number
            created_at = $Run['created_at']; started_at = $Run['run_started_at']; completed_at = $Run['updated_at']
            workflow_conclusion = $Run.conclusion; run_sha = $Run.head_sha; controller_sha = $Run.head_sha
            manifest = if ($null -ne $Plan) { $Manifest } else { $null }; plan = $Plan
            validated_no_work = [bool]$Skipped; results = $Results; jobs = $Jobs; evidence_gaps = $EvidenceGaps
            artifacts = $Artifacts
            run_url = "https://github.com/$($Policy.repository)/actions/runs/$($Run.id)/attempts/$($Run.run_attempt)"
        }
    }
    $prepared = Invoke-ScheduledRunRecord @{
        op = 'prepare'; evidence = $evidence
        transient_path_prefixes = @($TransientPaths | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    }
    $actions = [Collections.Generic.List[object]]::new()
    $issue = Get-ScheduledRunIssue -Policy $Policy -Identity $prepared.identity -Api $Api
    $applyWrites = $Apply -and $Policy.rollout.reporting_enabled
    $journalPath = Join-Path $OutputDirectory "publication-$($Policy.repository_id)-$($Run.workflow_id)-$($Run.id).json"
    $journal = @{
        schema_version = 1; identity = $prepared.identity; digest = $prepared.digest
        stage = 'prepared'; issue_number = $null; operation_id = $null; pending_pages = @{}
    }
    if (Test-Path -LiteralPath $journalPath) {
        $journal = Get-Content -LiteralPath $journalPath -Raw | ConvertFrom-Json -AsHashtable
        if ($journal.schema_version -ne 1 -or $journal.pending_pages -isnot [hashtable] -or
            (Get-ScheduledDigest $journal.identity) -cne (Get-ScheduledDigest $prepared.identity)) {
            throw [FormatException]::new('Publication journal identity or schema is invalid.')
        }
        $journal.digest = $prepared.digest
    }
    if ($null -eq $issue -and -not $prepared.should_report) {
        if ($journal.stage -cne 'prepared') {
            throw [IO.IOException]::new('Prior run issue creation remains unresolved; reconcile it before retrying.')
        }
        # A clean execution can still fail while bootstrapping coverage labels. Retain the
        # no-intake intent so a fresh reporter attempt can restore its publication state.
        if ($applyWrites) { Write-ScheduledRunJournal -Path $journalPath -Record $journal }
        return @{ requires_triage = $false; actions = @(); digest = $prepared.digest }
    }
    if ($null -eq $issue) {
        if ($journal.stage -cne 'prepared') {
            throw [IO.IOException]::new('Prior run issue creation remains unresolved; reconcile it before retrying.')
        }
        $seed = Invoke-ScheduledRunRecord @{ op = 'render'; record = @{ identity = $prepared.identity; revisions = @() } }
        $payload = @{
            title = $seed.title; body = "[Copilot speaking]`n`n$(ConvertTo-ScheduledRunBodyBlock $seed.body)"
            labels = @('scheduled-run-failure')
        }
        if (-not $applyWrites) {
            return @{
                requires_triage = $prepared.should_report; digest = $prepared.digest; planned_pages = $prepared.pages
                actions = @(@{ action = 'dry-run'; method = 'POST'; endpoint = "repos/$($Policy.repository)/issues"; payload = $payload })
            }
        }
        # Persist recoverable pre-issue intent even if label bootstrap fails. The run issue
        # has not been attempted yet, so a rerun can safely resume from this stage.
        Write-ScheduledRunJournal -Path $journalPath -Record $journal
        Initialize-ScheduledReportingLabel -Policy $Policy -Name scheduled-run-failure `
            -OutputDirectory $OutputDirectory -Api $Api -Apply:$applyWrites
        $journal.stage = 'creating-issue'
        Write-ScheduledRunJournal -Path $journalPath -Record $journal
        try {
            $created = & $Api -Endpoint "repos/$($Policy.repository)/issues" -Method POST -Body $payload
            $issue = & $Api -Endpoint "repos/$($Policy.repository)/issues/$($created.number)"
        } catch [IO.IOException], [FormatException], [ArgumentException] {
            $issue = Get-ScheduledRunIssue -Policy $Policy -Identity $prepared.identity -Api $Api
            if ($null -eq $issue) { throw [IO.IOException]::new('Run issue creation outcome is unknown; do not retry the POST.', $_.Exception) }
        }
        $actions.Add(@{ action = 'issue'; number = $issue.number })
    }
    $journal.issue_number = $issue.number
    if ($issue.user.login -cne $Policy.reporter_login) { throw [FormatException]::new('Run issue is owned by another author.') }
    $originalBlock = Get-ScheduledRunBodyBlock ([string]$issue.body)
    $root = Read-ScheduledRecord -Text $originalBlock.Value -Kind run
    if ($root.repository_id -ne $prepared.identity.repository_id -or $root.workflow_id -ne $prepared.identity.workflow_id -or
        $root.run_id -ne $prepared.identity.run_id) { throw [FormatException]::new('Run issue identity changed.') }
    $checkpoint = Read-ScheduledRecord -Text $originalBlock.Value -Kind run-publication
    $comments = @(Get-ScheduledRunComment -Policy $Policy -IssueNumber $issue.number -Api $Api)
    $before = Invoke-ScheduledRunRecord @{ op = 'restore'; identity = $prepared.identity; comments = $comments }
    $beforeRendered = Invoke-ScheduledRunRecord @{ op = 'render'; record = $before.record }
    $alreadyComplete = @($before.record.revisions | Where-Object {
        $_.digest -ceq $prepared.digest -and $_.evidence.attempt.run_attempt -eq $Run.run_attempt
    }).Count -eq 1
    $pendingIndex = $false
    if ($checkpoint.index_digest -cne $beforeRendered.index_digest) {
        $withoutCurrent = @{
            identity = $before.record.identity
            revisions = @($before.record.revisions | Where-Object {
                $_.digest -cne $prepared.digest -or $_.evidence.attempt.run_attempt -ne $Run.run_attempt
            })
        }
        $previousRendered = Invoke-ScheduledRunRecord @{ op = 'render'; record = $withoutCurrent }
        if (-not $alreadyComplete -or $previousRendered.index_digest -cne $checkpoint.index_digest) {
            throw [FormatException]::new('Committed run history differs from its evidence comments; reconcile missing or unpublished pages.')
        }
        $pendingIndex = $true
    }
    if (-not $applyWrites) {
        return @{
            requires_triage = $prepared.should_report; digest = $prepared.digest; planned_pages = $prepared.pages
            actions = @(@{ action = 'dry-run'; number = $issue.number; operation = 'reconcile-evidence' })
        }
    }
    $pageMap = Get-ScheduledRunPageMap -Comments $comments
    foreach ($page in $prepared.pages) {
        if ($pageMap.ContainsKey($page.operation_id)) {
            if ($pageMap[$page.operation_id].body -cne $page.body) {
                throw [FormatException]::new('Prepared evidence conflicts with an existing page operation.')
            }
            $journal.pending_pages.Remove($page.operation_id)
            continue
        }
        if ($journal.pending_pages.ContainsKey($page.operation_id)) {
            throw [IO.IOException]::new('A previous evidence-page write is unresolved; do not post a duplicate operation.')
        }
        $journal.stage = 'posting-page'; $journal.operation_id = $page.operation_id
        $journal.pending_pages[$page.operation_id] = $true
        Write-ScheduledRunJournal -Path $journalPath -Record $journal
        try {
            $null = & $Api -Endpoint "repos/$($Policy.repository)/issues/$($issue.number)/comments" `
                -Method POST -Body @{ body = $page.body }
        } catch [IO.IOException], [FormatException], [ArgumentException] {
            $recovered = @(Get-ScheduledRunComment -Policy $Policy -IssueNumber $issue.number -Api $Api)
            $pageMap = Get-ScheduledRunPageMap -Comments $recovered
            if (-not $pageMap.ContainsKey($page.operation_id) -or $pageMap[$page.operation_id].body -cne $page.body) {
                throw [IO.IOException]::new('Evidence page publication outcome is unknown; reconcile the journal before retrying.', $_.Exception)
            }
        }
        $actions.Add(@{ action = 'evidence-page'; number = $issue.number; operation_id = $page.operation_id })
    }
    $comments = @(Get-ScheduledRunComment -Policy $Policy -IssueNumber $issue.number -Api $Api)
    $restored = Invoke-ScheduledRunRecord @{ op = 'restore'; identity = $prepared.identity; comments = $comments }
    if (@($restored.record.revisions | Where-Object {
        $_.digest -ceq $prepared.digest -and $_.evidence.attempt.run_attempt -eq $Run.run_attempt
    }).Count -ne 1) { throw [FormatException]::new('Prepared evidence is not completely persisted; the run index was not advanced.') }
    foreach ($page in $prepared.pages) { $journal.pending_pages.Remove($page.operation_id) }
    $rendered = Invoke-ScheduledRunRecord @{ op = 'render'; record = $restored.record }
    $live = & $Api -Endpoint "repos/$($Policy.repository)/issues/$($issue.number)"
    $liveBlock = Get-ScheduledRunBodyBlock ([string]$live.body)
    if ($live.user.login -cne $Policy.reporter_login -or $liveBlock.Value -cne $originalBlock.Value) {
        throw [FormatException]::new('Run index changed during publication; reconcile before replacing it.')
    }
    $body = $live.body.Substring(0, $liveBlock.Index) + (ConvertTo-ScheduledRunBodyBlock $rendered.body) +
        $live.body.Substring($liveBlock.Index + $liveBlock.Length)
    if (-not $body.StartsWith('[Copilot speaking]')) { $body = "[Copilot speaking]`n`n$body" }
    if ($body.Length -gt 65536) { throw [FormatException]::new('Run issue body exceeds the GitHub limit; human text was not truncated.') }
    $reopen = $prepared.should_report -and (-not $alreadyComplete -or $pendingIndex)
    $labels = @($live.labels | ForEach-Object { $_.name } | Where-Object { -not $reopen -or $_ -cne 'scheduled-triaged' })
    if ($body -cne $live.body -or ($reopen -and ($live.state -cne 'open' -or $labels.Count -ne $live.labels.Count))) {
        $payload = @{ body = $body }
        if ($reopen) { $payload.state = 'open'; $payload.labels = $labels }
        $journal.stage = 'updating-index'; $journal.operation_id = $null
        Write-ScheduledRunJournal -Path $journalPath -Record $journal
        $null = & $Api -Endpoint "repos/$($Policy.repository)/issues/$($issue.number)" -Method PATCH -Body $payload
        $actions.Add(@{ action = 'index'; number = $issue.number })
    }
    $journal.stage = 'complete'; $journal.operation_id = $null
    Write-ScheduledRunJournal -Path $journalPath -Record $journal
    return @{
        requires_triage = $prepared.should_report; actions = $actions.ToArray(); number = $issue.number
        digest = $prepared.digest; record = $restored.record; incomplete_revisions = $restored.incomplete_revisions
    }
}

function Initialize-ScheduledReportingLabel {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][hashtable] $Policy,
        [Parameter(Mandatory)][ValidateSet('scheduled-run-failure', 'scheduled-coverage', 'scheduled-health')][string] $Name,
        [Parameter(Mandatory)][string] $OutputDirectory,
        [Parameter(Mandatory)][scriptblock] $Api,
        [switch] $Apply
    )

    # Label reads are part of publishing, not a prerequisite for read-only diagnostics.
    # Ref: ../../.github/workflows/implementation.md#reporting-label-bootstrap.
    if (-not $Apply -or -not $Policy.rollout.reporting_enabled) { return }
    $endpoint = "repos/$($Policy.repository)/labels"
    $pages = & $Api -Endpoint "${endpoint}?per_page=100" -Paginate
    $existing = @($pages | ForEach-Object { $_ } | Where-Object { $_.name -ieq $Name })
    if ($existing.Count -gt 0) { return }

    # Only missing labels receive defaults. Color is neutral because labels identify record
    # roles rather than severity; an operator's existing spelling/color/description is retained.
    $descriptions = @{
        'scheduled-run-failure' = 'Deep validation run evidence requiring operator analysis'
        'scheduled-coverage' = 'Authoritative deep validation coverage'
        'scheduled-health' = 'Scheduled validation and executor health'
    }
    $journalPath = Join-Path $OutputDirectory "label-$Name.json"
    $journal = @{ schema_version = 1; repository = $Policy.repository; name = $Name; stage = 'creating-label' }
    Write-ScheduledRunJournal -Path $journalPath -Record $journal
    try {
        $null = & $Api -Endpoint $endpoint -Method POST `
            -Body @{ name = $Name; color = 'ededed'; description = $descriptions[$Name] }
    } catch [IO.IOException] {
        # GitHub enforces unique label names. A raced or lost create response is resolved by
        # reading that name, never by updating its metadata or interpreting the error as success.
        Write-Verbose "Label creation response for $Name is uncertain; reading its unique name before continuing."
    }
    $label = & $Api -Endpoint "$endpoint/$Name"
    if ($null -eq $label -or $label.name -ine $Name) {
        throw [FormatException]::new("GitHub did not confirm the required reporting label: $Name")
    }
    $journal.stage = 'complete'
    Write-ScheduledRunJournal -Path $journalPath -Record $journal
}

Export-ModuleMember -Function Get-ScheduledRunJobEvidence, Invoke-ScheduledRunRecord, Sync-ScheduledRunIntake,
Initialize-ScheduledReportingLabel, Write-ScheduledRunJournal
