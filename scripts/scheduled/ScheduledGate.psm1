#requires -Version 7

# Managed repairs join the ordinary required-checks fan-in; author identity alone is not a claim.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
Import-Module (Join-Path $PSScriptRoot 'ScheduledContracts.psm1')
Import-Module (Join-Path $PSScriptRoot 'ScheduledPlan.psm1')

function Test-ScheduledManagedPullRequest {
    [CmdletBinding()]
    [OutputType([bool])]
    param(
        [Parameter(Mandatory)][hashtable] $PullRequest,
        [Parameter(Mandatory)][hashtable] $Policy
    )
    return $PullRequest.head.ref.StartsWith($Policy.managed_branch_prefix, [StringComparison]::Ordinal) -or
        ([string]$PullRequest.body).Contains('<!-- scheduled-repair:')
}

function Get-ScheduledRepairScope {
    [CmdletBinding()]
    [OutputType([hashtable])]
    param(
        [Parameter(Mandatory)][hashtable] $PullRequest,
        [AllowNull()][hashtable] $Issue,
        [AllowNull()][hashtable] $Worker,
        [Parameter(Mandatory)][hashtable] $Policy,
        [switch] $Confirmation
    )

    if (-not (Test-ScheduledManagedPullRequest -PullRequest $PullRequest -Policy $Policy)) {
        return @{ managed = $false; check_ids = @(); packages = @() }
    }
    $repair = Read-ScheduledRecord -Text $PullRequest.body -Kind repair
    if ($null -eq $Issue -or $null -eq $Worker) { throw [FormatException]::new('Managed repair has no registered issue/worker.') }
    $reporter = Read-ScheduledRecord -Text $Issue.body -Kind reporter
    foreach ($record in @($repair, $reporter, $Worker)) {
        if ($record.repository -cne $Policy.repository -or $record.repository_id -ne $Policy.repository_id) {
            throw [FormatException]::new('Managed repair repository mismatch.')
        }
        if ($record.finding_id -cne $reporter.finding_id -or $record.generation -ne $reporter.generation) {
            throw [FormatException]::new('Managed repair incident generation mismatch.')
        }
    }
    $allowedStatus = if ($Confirmation) { @('open', 'needs-human') } else { @('open') }
    if ($Confirmation -and (-not $PullRequest.ContainsKey('merged') -or
        -not $PullRequest.merged -or $PullRequest.state -cne 'closed')) {
        throw [FormatException]::new('Confirmation requires an actually merged repair.')
    }
    if ($Issue.user.login -cne $Policy.reporter_login -or $reporter.status -cnotin $allowedStatus -or
        $Issue.state -cne 'open' -or $repair.issue_number -ne $Issue.number) {
        throw [FormatException]::new('Managed repair lacks an open authoritative finding.')
    }
    if ($PullRequest.head.repo.id -ne $Policy.repository_id -or $PullRequest.base.ref -cne 'main') {
        throw [FormatException]::new('Managed repair must target main from its enrolled repository.')
    }
    Assert-ScheduledSha $PullRequest.head.sha
    foreach ($record in @($repair, $Worker)) {
        if (-not $record.ContainsKey('explanation') -or $record.explanation -isnot [string] -or
            [string]::IsNullOrWhiteSpace($record.explanation) -or
            $record.explanation.Length -gt $Policy.repair.max_explanation_characters) {
            throw [FormatException]::new('Managed repair requires a bounded causal explanation.')
        }
        if ($record.branch -cne $PullRequest.head.ref -or $record.head_sha -cne $PullRequest.head.sha) {
            throw [FormatException]::new('Managed repair metadata is stale for this published head.')
        }
    }
    if ($repair.explanation -cne $Worker.explanation) {
        throw [FormatException]::new('Published repair explanation differs from registered worker evidence.')
    }
    if ($repair.attempt_id -cne $Worker.attempt_id -or [string]::IsNullOrWhiteSpace($Worker.session_id) -or
        [string]::IsNullOrWhiteSpace($Worker.executor_id) -or
        ($null -ne $Worker.pr_number -and $Worker.pr_number -ne $PullRequest.number)) {
        throw [FormatException]::new('Managed repair attempt/session/PR registration mismatch.')
    }
    if (-not $Policy.ContainsKey('local') -or
        [string]::IsNullOrWhiteSpace($Policy.local.enrolled_machine_id) -or
        $Worker.executor_id -cne $Policy.local.enrolled_machine_id) {
        throw [FormatException]::new('Managed repair executor is not the currently enrolled machine.')
    }
    if (-not $PullRequest.head.ref.StartsWith($Policy.managed_branch_prefix, [StringComparison]::Ordinal)) {
        throw [FormatException]::new('Managed repair is outside the reserved branch namespace.')
    }
    if ($reporter.package -cnotin $Policy.repair.allowed_packages) {
        throw [FormatException]::new('Managed repair package is not approved for hosted verification.')
    }
    $catalog = Get-ScheduledCheckManifest -SourceSha $PullRequest.head.sha -ControllerSha $reporter.controller_sha `
        -ContractDigest $reporter.check_contract_digest -Scope repair -Packages @($reporter.package)
    $origin = @($catalog.checks | Where-Object { $_.id -ceq $reporter.check_id })
    if ($origin.Count -ne 1 -or $origin[0].kind -cnotin $Policy.repair.allowed_checks) {
        throw [FormatException]::new('Managed repair check is unknown or outside approved scope.')
    }
    # A full implicated family on its original platform prevents one green seed or mutation
    # shard from erasing the untested remainder. Scope remains package-specific.
    $checkIds = @($catalog.checks | Where-Object {
            $_.kind -ceq $origin[0].kind -and $_.platform -ceq $origin[0].platform
        } | ForEach-Object { $_.id })
    return @{
        managed = $true; check_ids = $checkIds; packages = @($reporter.package)
        issue_number = $Issue.number; finding_id = $reporter.finding_id
        generation = $reporter.generation; attempt_id = $repair.attempt_id
        version_evidence = if ($Worker.ContainsKey('version_evidence')) { $Worker.version_evidence } else { $null }
        head_sha = $PullRequest.head.sha
    }
}

function Get-ScheduledMergeGroupMember {
    [CmdletBinding()]
    [OutputType([hashtable[]])]
    param(
        [Parameter(Mandatory)][string] $HeadSha,
        [Parameter(Mandatory)][string] $BaseSha,
        [Parameter(Mandatory)][AllowEmptyCollection()][hashtable[]] $Entries,
        [Parameter(Mandatory)][scriptblock] $IsAncestor
    )

    Assert-ScheduledSha $HeadSha
    Assert-ScheduledSha $BaseSha
    # Queue entry headCommit is the synthetic queue candidate, not the original PR head.
    # Matching the event SHA and its bounded queue ancestry handles combined/squashed entries.
    $tips = @($Entries | Where-Object { $null -ne $_.headCommit -and $_.headCommit.oid -ceq $HeadSha })
    if ($tips.Count -ne 1) { throw 'Merge group membership is unavailable or ambiguous.' }
    $members = @()
    foreach ($entry in $Entries) {
        if ($null -eq $entry.headCommit) { continue }
        $sha = $entry.headCommit.oid
        if ($sha -ceq $BaseSha) { continue }
        if ((& $IsAncestor $BaseSha $sha) -and (& $IsAncestor $sha $HeadSha)) {
            if ($null -eq $entry.pullRequest) { throw 'Queue entry lacks its pull request.' }
            $members += $entry.pullRequest
        }
    }
    if ($members.Count -eq 0) { throw 'Merge group has no established members.' }
    return $members
}

function Test-ScheduledRepairEvidence {
    [CmdletBinding()]
    [OutputType([hashtable])]
    param(
        [Parameter(Mandatory)][hashtable] $Manifest,
        [Parameter(Mandatory)][AllowEmptyCollection()][hashtable[]] $Results,
        [Parameter(Mandatory)][long] $RunId,
        [Parameter(Mandatory)][int] $RunAttempt
    )

    $verdict = Test-ScheduledManifest -Manifest $Manifest -Results $Results
    foreach ($result in $Results) {
        if ($result.run_id -ne $RunId -or $result.run_attempt -ne $RunAttempt) {
            $verdict.successful = $false
            $verdict.problems += 'Result belongs to another run or attempt.'
        }
    }
    return $verdict
}

function Invoke-ScheduledReadApi {
    [CmdletBinding()]
    param([Parameter(Mandatory)][string] $Endpoint, [switch] $Paginate)
    $arguments = @('api', $Endpoint)
    if ($Paginate) { $arguments += @('--paginate', '--slurp') }
    $json = & gh @arguments
    return ConvertFrom-Json -InputObject ($json -join "`n") -AsHashtable
}

function Get-ScheduledQueueEntry {
    [CmdletBinding()]
    param([Parameter(Mandatory)][string] $Repository)
    $parts = $Repository.Split('/')
    $query = @'
query($owner:String!,$name:String!,$cursor:String) {
  repository(owner:$owner,name:$name) {
    mergeQueue(branch:"main") {
      entries(first:100,after:$cursor) {
        pageInfo { hasNextPage endCursor }
        nodes { headCommit { oid } baseCommit { oid } pullRequest { number } }
      }
    }
  }
}
'@
    $entries = @()
    $cursor = ''
    do {
        $arguments = @('api', 'graphql', '-f', "query=$query", '-f', "owner=$($parts[0])", '-f', "name=$($parts[1])")
        if ($cursor) { $arguments += @('-f', "cursor=$cursor") }
        $response = (& gh @arguments) -join "`n" | ConvertFrom-Json -AsHashtable
        if ($response.ContainsKey('errors')) { throw 'Cannot establish merge queue membership.' }
        $connection = $response.data.repository.mergeQueue.entries
        $entries += $connection.nodes
        $cursor = $connection.pageInfo.endCursor
    } while ($connection.pageInfo.hasNextPage)
    return $entries
}

function Get-ScheduledPullRequestScope {
    [CmdletBinding()]
    param([Parameter(Mandatory)][hashtable] $PullRequest, [Parameter(Mandatory)][hashtable] $Policy)

    if (-not (Test-ScheduledManagedPullRequest -PullRequest $PullRequest -Policy $Policy)) {
        return @{ managed = $false; packages = @(); check_ids = @() }
    }
    $repair = Read-ScheduledRecord -Text $PullRequest.body -Kind repair
    if ($repair.issue_number -isnot [long] -and $repair.issue_number -isnot [int]) {
        throw 'Repair issue number must be numeric.'
    }
    $issue = Invoke-ScheduledReadApi "repos/$($Policy.repository)/issues/$($repair.issue_number)"
    $pages = Invoke-ScheduledReadApi "repos/$($Policy.repository)/issues/$($repair.issue_number)/comments?per_page=100" -Paginate
    $comments = @($pages | ForEach-Object { $_ })
    $reporter = Read-ScheduledRecord -Text $issue.body -Kind reporter
    $run = Invoke-ScheduledReadApi "repos/$($Policy.repository)/actions/runs/$($reporter.observation.run_id)/attempts/$($reporter.observation.run_attempt)"
    $incident = ConvertTo-ScheduledIncident -Issue $issue -Comments $comments -Repository $Policy.repository `
        -RepositoryId $Policy.repository_id -Run $run -ReporterLogin $Policy.reporter_login -WorkerLogin $Policy.worker_login
    return Get-ScheduledRepairScope -PullRequest $PullRequest -Issue $issue -Worker $incident.validated_worker -Policy $Policy
}

function ConvertTo-ScheduledVersionNeutralText {
    [CmdletBinding()]
    [OutputType([string])]
    param(
        [Parameter(Mandatory)][string] $Text,
        [Parameter(Mandatory)][ValidateSet('manifest', 'lock')][string] $Kind,
        [Parameter(Mandatory)][string[]] $WorkspacePackages
    )
    $lines = [Collections.Generic.List[string]]::new()
    $workspaceEntry = $false
    foreach ($line in ($Text -split '\r?\n')) {
        $normalized = $line
        if ($Kind -eq 'manifest') {
            if ($line -cmatch '^version\s*=\s*"\d+\.\d+\.\d+"$') {
                $normalized = $line -creplace '"\d+\.\d+\.\d+"', '"VERSION"'
            } else {
                foreach ($name in $WorkspacePackages) {
                    if ($line -cmatch ('^' + [regex]::Escape($name) + '\s*=\s*\{')) {
                        $normalized = $line -creplace '(version\s*=\s*"=?)\d+\.\d+\.\d+(")', '${1}VERSION${2}'
                        break
                    }
                }
            }
        } else {
            if ($line -ceq '[[package]]') { $workspaceEntry = $false }
            if ($line -cmatch '^name = "([^"]+)"$') { $workspaceEntry = $Matches[1] -cin $WorkspacePackages }
            if ($workspaceEntry -and $line -cmatch '^version = "\d+\.\d+\.\d+"$') {
                $normalized = 'version = "VERSION"'
            }
            foreach ($name in $WorkspacePackages) {
                $pattern = '^ "' + [regex]::Escape($name) + ' \d+\.\d+\.\d+",$'
                if ($line -cmatch $pattern) { $normalized = " `"$name VERSION`","; break }
            }
        }
        $lines.Add($normalized)
    }
    return $lines -join "`n"
}

function Assert-ScheduledRepairChange {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][hashtable[]] $Files,
        [Parameter(Mandatory)][string[]] $Packages,
        [Parameter(Mandatory)][string[]] $WorkspacePackages,
        [Parameter(Mandatory)][scriptblock] $ReadBase,
        [Parameter(Mandatory)][scriptblock] $ReadHead
    )

    foreach ($file in $Files) {
        $path = $file.filename
        if ($file.status -ceq 'renamed') { throw 'Managed repairs do not move package ownership boundaries.' }
        if ($path -ceq 'Cargo.lock' -or $path -ceq 'Cargo.toml' -or
            $path -cmatch '^packages/([^/]+)/Cargo.toml$') {
            if ($file.status -cne 'modified') { throw 'Managed repairs cannot add or remove Cargo manifests.' }
            $kind = if ($path -ceq 'Cargo.lock') { 'lock' } else { 'manifest' }
            $before = ConvertTo-ScheduledVersionNeutralText -Text (& $ReadBase $path) -Kind $kind -WorkspacePackages $WorkspacePackages
            $after = ConvertTo-ScheduledVersionNeutralText -Text (& $ReadHead $path) -Kind $kind -WorkspacePackages $WorkspacePackages
            if ($before -cne $after) {
                throw "Managed Cargo edit is not solely an internal version expansion: $path"
            }
            continue
        }
        $allowed = $false
        foreach ($name in $Packages) {
            if ($path.StartsWith("packages/$name/", [StringComparison]::Ordinal)) { $allowed = $true; break }
        }
        if (-not $allowed) { throw "Managed repair changes an unapproved path: $path" }
    }
}

function Assert-ScheduledPullRequestChange {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][hashtable] $PullRequest,
        [Parameter(Mandatory)][hashtable] $Scope,
        [Parameter(Mandatory)][hashtable] $Policy,
        [Parameter(Mandatory)][string] $Root
    )
    $workspacePackages = @(Get-ChildItem -Path (Join-Path $Root 'packages/*/Cargo.toml') -File | ForEach-Object {
            $text = Get-Content -LiteralPath $_.FullName -Raw
            [regex]::Match($text, '(?m)^name\s*=\s*"([^"]+)"').Groups[1].Value
        })
    $pages = Invoke-ScheduledReadApi "repos/$($Policy.repository)/pulls/$($PullRequest.number)/files?per_page=100" -Paginate
    $repository = $Policy.repository
    $baseSha = $PullRequest.base.sha
    $headSha = $PullRequest.head.sha
    # A closure is a dynamic module. Capture the command, not only its name, so the callback
    # retains the defining module's API boundary when called from another module.
    $readApi = Get-Command Invoke-ScheduledReadApi
    $readBase = {
        param($path)
        $value = & $readApi "repos/$repository/contents/${path}?ref=$baseSha"
        return [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($value.content))
    }.GetNewClosure()
    $readHead = {
        param($path)
        $value = & $readApi "repos/$repository/contents/${path}?ref=$headSha"
        return [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($value.content))
    }.GetNewClosure()
    Assert-ScheduledRepairChange -Files @($pages | ForEach-Object { $_ }) -Packages $Scope.packages `
        -WorkspacePackages $workspacePackages -ReadBase $readBase -ReadHead $readHead
}

Export-ModuleMember -Function Test-ScheduledManagedPullRequest, Get-ScheduledRepairScope,
Get-ScheduledMergeGroupMember, Test-ScheduledRepairEvidence, Invoke-ScheduledReadApi,
Get-ScheduledQueueEntry, Get-ScheduledPullRequestScope, Assert-ScheduledRepairChange,
Assert-ScheduledPullRequestChange, ConvertTo-ScheduledVersionNeutralText
