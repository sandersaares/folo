#requires -Version 7
# Projects and reconciles role-specific health on the reporter's existing rolling issue.
# Both Local skills and operator setup use the same ownership rules; health never creates an
# issue, changes enrollment, or turns profile registration into a successful scan.
# Ref: ../../docs/scheduled-triage.md#readiness-and-health.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
Import-Module (Join-Path $PSScriptRoot 'ScheduledContracts.psm1')
Import-Module (Join-Path $PSScriptRoot 'LocalState.psm1')
Import-Module (Join-Path $PSScriptRoot 'ScheduledGitHub.psm1')

function Get-ScheduledRoleHealthRecord {
    [CmdletBinding()]
    param([hashtable] $State, [ValidateSet('triage', 'repair')][string] $Role)
    if ($Role -ceq 'triage') {
        if (-not $State.ContainsKey('triage')) { throw 'Triage role has not been registered.' }
        $health = $State.triage.health
        $installed = $State.triage.profile
        $active = if ($null -ne $State.triage.active_analysis_id) {
            $analysis = $State.triage.analyses[$State.triage.active_analysis_id]
            @{
                analysis_id = $analysis.id; session_id = $analysis.session_id; phase = $analysis.phase
                revision = $analysis.revision; reason = $analysis.reason
                checkpoint = if ($null -ne $analysis.checkpoint) {
                    $analysis.checkpoint.analysis.checkpoint
                } else { $null }
                continuations = $analysis.continuations.Count
            }
        } else { $null }
        $oldest = $health.oldest_pending_at
        $mode = $State.triage.mode
    } else {
        $health = $State.health
        $installed = $State.profile
        $active = @($State.attempts.Values | Where-Object { $_.phase -cnotin @('resolved', 'closed-unmerged') } |
            ForEach-Object { @{ attempt_id = $_.attempt_id; session_id = $_.session_id; pr_number = $_.pr_number
                phase = $_.phase; reason = $_.reason } })
        $oldest = $health.oldest_eligible_at
        $mode = $State.mode
    }
    return @{
        schema_version = 1; role = $Role; repository = $State.repository; repository_id = $State.repository_id
        executor_id = $State.executor_id; mode = $mode; profile = $installed
        last_scan_at = $health.last_scan_at; last_successful_scan = $health.last_successful_scan
        backlog_count = $health.backlog_count; oldest_pending_at = $oldest
        blocked_conditions = @($health.blocked_conditions); active = $active
    }
}

function Invoke-RoleHealthTransaction {
    param($Context, [string] $Action, [hashtable] $Data = @{})
    $fields = @{ role = $Context.role; scan_token = $Context.scan_token }
    foreach ($key in $Data.Keys) { $fields[$key] = $Data[$key] }
    return Invoke-ScheduledLocalAction -StateRoot $Context.state_root -Policy $Context.policy `
        -ExecutorId $Context.executor_id -Login $Context.login -Now $Context.now -Action $Action -Data $fields
}

function Find-RoleHealthComment {
    param($Context, [long] $IssueNumber, [scriptblock] $Api, [AllowNull()] $KnownId)
    $comments = if ($null -ne $KnownId) {
        @(& $Api -Endpoint "repos/$($Context.policy.repository)/issues/comments/$KnownId")
    } else {
        $pages = & $Api -Endpoint "repos/$($Context.policy.repository)/issues/$IssueNumber/comments?per_page=100" -Paginate
        @($pages | ForEach-Object { $_ })
    }
    $owned = @(
        foreach ($comment in $comments) {
            if ($comment.user.login -cne $Context.login -or
                -not ([string]$comment.body).Contains('<!-- scheduled-health:v1 ')) { continue }
            $record = Read-ScheduledRecord $comment.body health
            if ($comment.issue_url -cne "https://api.github.com/repos/$($Context.policy.repository)/issues/$IssueNumber") {
                throw [FormatException]::new('Health comment belongs to another issue.')
            }
            $role = if ($record.ContainsKey('role')) { $record.role } else { 'repair' }
            if ($role -ceq $Context.role -and $record.repository_id -eq $Context.policy.repository_id -and
                $record.repository -ceq $Context.policy.repository -and $record.executor_id -ceq $Context.executor_id) {
                $comment
            }
        }
    )
    if ($owned.Count -gt 1) { throw [FormatException]::new('Role health comment ownership is ambiguous.') }
    if ($null -ne $KnownId -and $owned.Count -ne 1) {
        throw [FormatException]::new('Registered role health comment is unavailable; do not replace it.')
    }
    if ($owned.Count -eq 1) { return $owned[0] }
    return $null
}

function Sync-ScheduledRoleHealth {
    [CmdletBinding()]
    param([hashtable] $Context, [scriptblock] $Api = {
        param($Endpoint, $Method = 'GET', $Body, [switch] $Paginate)
        Invoke-ScheduledGitHubApi -Endpoint $Endpoint -Method $Method -Body $Body -Paginate:$Paginate
    })
    $null = Invoke-RoleHealthTransaction $Context health-authorize
    $user = & $Api -Endpoint user
    if ($user.login -cne $Context.login) { throw 'Selected GitHub account changed before health publication.' }
    $pages = & $Api -Endpoint "repos/$($Context.policy.repository)/issues?state=open&labels=scheduled-health&per_page=100" -Paginate
    $issues = @($pages | ForEach-Object { $_ } | Where-Object {
        -not $_.ContainsKey('pull_request') -and $_.user.login -ceq $Context.policy.reporter_login -and
        @($_.labels | Where-Object { $_.name -ieq 'scheduled-coverage' }).Count -eq 1
    })
    if ($issues.Count -ne 1) { throw [FormatException]::new('The shared reporter-owned health surface is unavailable or ambiguous.') }
    $coverage = Read-ScheduledRecord $issues[0].body coverage
    if ($coverage.repository_id -ne $Context.policy.repository_id -or $coverage.repository -cne $Context.policy.repository) {
        throw [FormatException]::new('Health surface repository identity differs.')
    }
    $state = Invoke-RoleHealthTransaction $Context read
    $operation = if ($state.ContainsKey('health_publications') -and $state.health_publications.ContainsKey($Context.role)) {
        $state.health_publications[$Context.role]
    } else { $null }
    $knownId = if ($null -ne $operation) { $operation.intent.comment_id } else { $null }
    $comment = Find-RoleHealthComment $Context $issues[0].number $Api $knownId
    $desired = Get-ScheduledRoleHealthRecord -State $state -Role $Context.role
    if ($null -eq $operation -or $operation.stage -ceq 'complete') {
        $current = if ($null -ne $comment) { Read-ScheduledRecord $comment.body health } else { $null }
        if ($null -ne $current -and (Get-ScheduledDigest $current) -ceq (Get-ScheduledDigest $desired)) {
            return @{ action = 'unchanged'; comment_id = $comment.id }
        }
        $body = if ($null -eq $comment) { "[Copilot speaking]`n`n$(Write-ScheduledRecord $desired health)" } else {
            $match = [regex]::Match($comment.body, '<!-- scheduled-health:v1 (\{[^\r\n]*\}) -->')
            $comment.body.Substring(0, $match.Index) + (Write-ScheduledRecord $desired health) +
                $comment.body.Substring($match.Index + $match.Length)
        }
        $intent = @{
            issue_number = $issues[0].number; comment_id = if ($null -ne $comment) { $comment.id } else { $null }
            record = $desired; body = $body
            preimage = if ($null -ne $comment) { Get-ScheduledDigest $current } else { $null }
        }
        $state = Invoke-RoleHealthTransaction $Context health-prepare @{ intent = $intent }
        $operation = $state.health_publications[$Context.role]
    }
    if ($operation.intent.issue_number -ne $issues[0].number) { throw 'Retained health publication belongs to another surface.' }
    if ($null -ne $comment) {
        $current = Read-ScheduledRecord $comment.body health
        if ((Get-ScheduledDigest $current) -ceq (Get-ScheduledDigest $operation.intent.record)) {
            $null = Invoke-RoleHealthTransaction $Context health-confirm @{
                operation_id = $operation.id; comment_id = $comment.id
                record_digest = Get-ScheduledDigest $current
            }
            return @{ action = 'reconciled'; comment_id = $comment.id }
        }
        if ((Get-ScheduledDigest $current) -cne $operation.intent.preimage) {
            throw [FormatException]::new('Health record changed during an uncertain write.')
        }
    } elseif ($operation.stage -ceq 'sending') {
        throw [IO.IOException]::new('Health comment creation is unresolved; no duplicate POST is authorized.')
    }
    if ($operation.stage -ceq 'prepared') { $null = Invoke-RoleHealthTransaction $Context health-begin }
    $endpoint = if ($null -eq $comment) {
        "repos/$($Context.policy.repository)/issues/$($operation.intent.issue_number)/comments"
    } else { "repos/$($Context.policy.repository)/issues/comments/$($comment.id)" }
    $method = if ($null -eq $comment) { 'POST' } else { 'PATCH' }
    $body = $operation.intent.body
    if ($null -ne $comment) {
        # Preserve discussion appended outside the owned record while an update was pending.
        $match = [regex]::Match($comment.body, '<!-- scheduled-health:v1 (\{[^\r\n]*\}) -->')
        $body = $comment.body.Substring(0, $match.Index) +
            (Write-ScheduledRecord $operation.intent.record health) +
            $comment.body.Substring($match.Index + $match.Length)
    }
    if (-not $body.StartsWith('[Copilot speaking]')) { $body = "[Copilot speaking]`n`n$body" }
    if ([Text.Encoding]::UTF8.GetByteCount($body) -gt 60000) {
        throw 'Health publication exceeds the body budget; human discussion was not truncated.'
    }
    $response = & $Api -Endpoint $endpoint -Method $method -Body @{ body = $body }
    $null = Invoke-RoleHealthTransaction $Context health-observe @{ comment_id = $response.id }
    $confirmed = Find-RoleHealthComment $Context $issues[0].number $Api $response.id
    $record = Read-ScheduledRecord $confirmed.body health
    $null = Invoke-RoleHealthTransaction $Context health-confirm @{
        operation_id = $operation.id; comment_id = $confirmed.id; record_digest = Get-ScheduledDigest $record
    }
    return @{ action = 'published'; comment_id = $confirmed.id }
}

Export-ModuleMember -Function Get-ScheduledRoleHealthRecord, Sync-ScheduledRoleHealth
