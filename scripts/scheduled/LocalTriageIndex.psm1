#requires -Version 7
# Advances a comparison baseline only across this analysis's verified publications. Every
# external issue/comment/body/label change still requires AI reconsideration; refreshing the
# complete index is never skipped merely because a multi-problem analysis is partially written.
# Ref: ../../docs/scheduled-triage.md#ownership-and-recovery.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
Import-Module (Join-Path $PSScriptRoot 'ScheduledContracts.psm1')
Import-Module (Join-Path $PSScriptRoot 'LocalTriageInbox.psm1')
Import-Module (Join-Path $PSScriptRoot 'LocalTriagePublication.psm1')

function Test-TriageOwnIssueChange {
    param($Context, $Plan, $Operation, $Current)
    $original = $Plan.original_issue
    $block = Get-TriageOwnedBlock $original.body problem
    $expectedBody = if ($null -eq $block) { "$($original.body)`n`n$($Operation.payload.body)" } else {
        $original.body.Substring(0, $block.Index) + $Operation.payload.body +
            $original.body.Substring($block.Index + $block.Length)
    }
    if (-not $expectedBody.StartsWith('[Copilot speaking]')) { $expectedBody = "[Copilot speaking]`n`n$expectedBody" }
    if ($Current.body -cne $expectedBody -or $null -eq $Operation.receipt['target'] -or
        (Get-ScheduledDigest $Current) -cne (Get-ScheduledDigest $Operation.receipt.target)) { return $false }
    # These API-maintained fields change when our body/state/comment writes succeed.
    # All other metadata, including title, labels and assignment, remains compared exactly.
    $automatic = @('body', 'updated_at', 'comments')
    if ($Operation.payload.ContainsKey('state')) {
        $automatic += @('state', 'state_reason', 'closed_at', 'closed_by')
        if ($Current.state -cne $Operation.payload.state) { return $false }
    }
    foreach ($key in @(@($original.Keys) + @($Current.Keys) | Sort-Object -Unique)) {
        if ($key -cin $automatic) { continue }
        if (-not $original.ContainsKey($key) -or -not $Current.ContainsKey($key) -or
            (Get-ScheduledDigest @{ value = $original[$key] }) -cne (Get-ScheduledDigest @{ value = $Current[$key] })) {
            return $false
        }
    }
    if ($null -ne $Plan['creation_payload']) {
        if ($original.body -cne $Plan.creation_payload.body -or
            $original.title -cne $Plan.creation_payload.title -or $original.user.login -cne $Context.login -or
            (Get-ScheduledDigest @($original.labels | ForEach-Object { $_.name } | Sort-Object)) -cne
                (Get-ScheduledDigest @($Plan.creation_payload.labels | Sort-Object))) {
            return $false
        }
    }
    return $true
}

function Test-TriageOwnCommentChange {
    param($Analysis, $Plan, $Current)
    $expected = @{}
    foreach ($comment in $Plan.original_comments) { $expected[[long]$comment.id] = $comment }
    foreach ($operation in $Analysis.operations.Values) {
        if ($operation.checkpoint -eq $Analysis.checkpoint.analysis.checkpoint -and
            $operation.issue_number -eq $Plan.issue_number -and $operation.kind -ceq 'create-comment' -and
            $operation.stage -ceq 'confirmed') {
            if ($null -eq $operation.receipt['target']) { return $false }
            $expected[[long]$operation.target_id] = $operation.receipt.target
        }
    }
    if ($expected.Count -ne $Current.Count) { return $false }
    foreach ($comment in $Current) {
        if (-not $expected.ContainsKey([long]$comment.id) -or
            (Get-ScheduledDigest $expected[[long]$comment.id]) -cne (Get-ScheduledDigest $comment)) { return $false }
    }
    return $true
}

function Sync-ScheduledTriageComparison {
    [CmdletBinding()]
    param($Context, $Plan, [string] $OperationKey, $Snapshot)
    $state = Invoke-TriageTransaction $Context read
    $analysis = $state.triage.analyses[$Context.analysis_id]
    $baseline = $analysis.comparison.index
    if ($Snapshot.index.digest -ceq $baseline.digest) { return $true }
    $expected = @{}
    foreach ($entry in $baseline.entries) { $expected[[long]$entry.issue_number] = $entry.record_digest }
    $current = @{}
    foreach ($entry in $Snapshot.index.entries) { $current[[long]$entry.issue_number] = $entry.record_digest }
    $own = $true
    foreach ($number in @(@($expected.Keys) + @($current.Keys) | Sort-Object -Unique)) {
        if ($number -eq $Plan.issue_number) { continue }
        if (-not $expected.ContainsKey($number) -or -not $current.ContainsKey($number) -or
            $expected[$number] -cne $current[$number]) { $own = $false }
    }
    if (-not $current.ContainsKey([long]$Plan.issue_number)) { $own = $false }
    if ($own) {
        $record = $Snapshot.problems[[string]$Plan.issue_number].record
        $operation = $analysis.operations[$OperationKey]
        $own = (Test-TriageOwnIssueChange $Context $Plan $operation $record.issue) -and
            (Test-TriageOwnCommentChange $analysis $Plan $record.comments)
    }
    if (-not $own) {
        $null = Invoke-TriageTransaction $Context triage-require-reanalysis @{ reason = 'external-index-change' }
        return $false
    }
    $null = Invoke-TriageTransaction $Context triage-accept-own-index @{
        previous_digest = $baseline.digest; index = $Snapshot.index; operation_key = $OperationKey
    }
    return $true
}

Export-ModuleMember -Function Sync-ScheduledTriageComparison
