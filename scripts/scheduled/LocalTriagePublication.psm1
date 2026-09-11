#requires -Version 7
# Applies only journaled triage issue/comment operations. Native issue creation is returned to
# the skill; comments and exact owned-block updates use the injected GitHub transport. Every
# uncertain non-idempotent write is reconciled before another send, including fresh sessions.
# Ref: ../../docs/scheduled-triage.md#ownership-and-recovery.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
Import-Module (Join-Path $PSScriptRoot 'ScheduledContracts.psm1')
Import-Module (Join-Path $PSScriptRoot 'ScheduledRecordTool.psm1')
Import-Module (Join-Path $PSScriptRoot 'LocalState.psm1')
Import-Module (Join-Path $PSScriptRoot 'LocalTriageState.psm1')
Import-Module (Join-Path $PSScriptRoot 'LocalTriageCheckpoint.psm1')
Import-Module (Join-Path $PSScriptRoot 'LocalTriageCache.psm1')
Import-Module (Join-Path $PSScriptRoot 'ScheduledGitHub.psm1')
Import-Module (Join-Path $PSScriptRoot 'LocalTriageInbox.psm1')

function Invoke-TriageTransaction {
    param($Context, [string] $Action, [hashtable] $Data = @{})
    $validatedDigest = $null
    if ($Action -ceq 'triage-authorize-publication') {
        $state = Get-ScheduledTriageValidatedState $Context
        if ($state.ContainsKey('triage') -and $null -ne $state.triage.active_analysis_id) {
            $validatedDigest = $state.triage.analyses[$state.triage.active_analysis_id].checkpoint_digest
        }
    }
    $request = @{
        analysis_id = $Context.analysis_id; session_id = $Context.session_id
        claim_token = $Context.claim_token; dispatch_token = $Context.dispatch_token
    }
    foreach ($key in $Data.Keys) { $request[$key] = $Data[$key] }
    if ($Action -cin @('triage-acquire-scan', 'triage-accept-dispatch') -and
        -not $Data.ContainsKey('profile_observation') -and $Context.ContainsKey('profile_observation')) {
        # In-process callers may carry the native facts observed for this invocation.
        # Never substitute the approved/stored profile for a missing live observation.
        $request.profile_observation = $Context.profile_observation
    }
    if ($Action -ceq 'triage-authorize-publication') { $request.checkpoint_digest = $validatedDigest }
    return Invoke-ScheduledLocalAction -StateRoot $Context.state_root -Policy $Context.policy `
        -TriagePolicy $Context.triage_policy -ExecutorId $Context.executor_id -Login $Context.login `
        -Now $Context.now -Action $Action -Data $request
}

function Get-ScheduledTriageValidatedState {
    param($Context)
    $state = Invoke-TriageTransaction $Context read
    if (-not $state.ContainsKey('triage') -or $null -eq $state.triage.active_analysis_id) { return $state }
    $analysis = $state.triage.analyses[$state.triage.active_analysis_id]
    if ($null -eq $analysis.checkpoint) { return $state }
    Assert-ScheduledTriageCheckpointContent $analysis.checkpoint $analysis.checkpoint_digest
    # The utility/build work is outside the lock; the subsequent short transaction rejects
    # an owner/checkpoint change before this validated snapshot can authorize transport.
    return Invoke-TriageTransaction $Context triage-verify-checkpoint @{
        expected_analysis_id = $analysis.id; checkpoint_digest = $analysis.checkpoint_digest
    }
}

function Save-ScheduledTriageSnapshot {
    param($Context, [hashtable] $Snapshot, [hashtable] $Owner)
    $null = Invoke-TriageTransaction $Context triage-authorize-snapshot $Owner
    $kind = if ($Owner.ContainsKey('analysis_id') -and $null -ne $Owner.analysis_id) { 'analysis' } else { 'scan' }
    $token = if ($kind -ceq 'analysis') { $Owner.dispatch_token } else { $Owner.scan_token }
    $prepared = Write-ScheduledTriageSnapshotFile $Context.state_root $Snapshot $kind $token
    try {
        $data = $Owner.Clone()
        $data.snapshot_id = $prepared.id; $data.temporary_path = $prepared.temporary_path
        $data.owner_kind = $prepared.owner_kind; $data.owner_token = $prepared.owner_token
        $null = Invoke-TriageTransaction $Context triage-pin-snapshot $data
        return $prepared.id
    } finally {
        if (Test-Path -LiteralPath $prepared.temporary_path) { Remove-Item -LiteralPath $prepared.temporary_path }
    }
}

function Get-TriageOwnedBlock {
    param([string] $Text, [ValidateSet('triage', 'problem', 'health')][string] $Kind)
    $blocks = [regex]::Matches($Text,
        "(?s)<!-- scheduled-${Kind}-content:start -->.*?<!-- scheduled-${Kind}-content:end -->")
    if ($blocks.Count -gt 1) { throw [FormatException]::new('Ambiguous owned publication block.') }
    if ($blocks.Count -eq 1) { return $blocks[0] }
    return $null
}

function ConvertTo-TriageOwnedBlock {
    param([ValidateSet('triage', 'problem', 'health')][string] $Kind, [string] $Content)
    return "<!-- scheduled-${Kind}-content:start -->`n$Content`n<!-- scheduled-${Kind}-content:end -->"
}

function Get-TriageOperation {
    param($Context, [string] $Key)
    $state = Invoke-TriageTransaction $Context read
    $analysis = $state.triage.analyses[$Context.analysis_id]
    if ($analysis.operations.ContainsKey($Key)) { return $analysis.operations[$Key] }
    return $null
}

function Find-TriageOperationTarget {
    param($Context, $Operation, [scriptblock] $Api)
    $repository = $Context.policy.repository
    if ($Operation.kind -ceq 'create-label') {
        $pages = & $Api -Endpoint "repos/$repository/labels?per_page=100" -Paginate
        $labels = @($pages | ForEach-Object { $_ } | Where-Object { $_.name -ieq $Operation.payload.name })
        if ($labels.Count -gt 1) { throw [FormatException]::new('Label inventory is ambiguous.') }
        if ($labels.Count -eq 1) { return $labels[0] }
        return $null
    }
    if ($null -ne $Operation.target_id) {
        $endpoint = if ($Operation.kind -cin @('create-issue', 'update-issue')) {
            "repos/$repository/issues/$($Operation.target_id)"
        } else { "repos/$repository/issues/comments/$($Operation.target_id)" }
        return & $Api -Endpoint $endpoint
    }
    $endpoint = if ($Operation.kind -ceq 'create-issue') {
        "repos/$repository/issues?state=all&labels=scheduled-finding&per_page=100"
    } else { "repos/$repository/issues/$($Operation.issue_number)/comments?per_page=100" }
    $pages = & $Api -Endpoint $endpoint -Paginate
    $candidates = @($pages | ForEach-Object { $_ } | Where-Object {
        $_.user.login -ceq $Context.login -and (
            ($Operation.kind -ceq 'create-issue' -and
                ([string]$_.body).Contains($Operation.creation_marker)) -or
            ($Operation.kind -ceq 'create-comment' -and $_.body -ceq $Operation.payload.body))
    })
    if ($candidates.Count -gt 1) {
        throw [FormatException]::new('Publication operation has ambiguous remote ownership.')
    }
    if ($candidates.Count -eq 1) { return $candidates[0] }
    return $null
}

function Test-TriageOperationTarget {
    param($Context, $Operation, $Target)
    if ($null -eq $Target) { return $false }
    if ($Operation.kind -ceq 'create-label') { return $Target.name -ieq $Operation.payload.name }
    $allowedAuthors = if ($Operation.kind -ceq 'update-issue') {
        @($Context.login, $Context.policy.reporter_login)
    } else { @($Context.login) }
    if ($Target.user.login -cnotin $allowedAuthors) { throw [FormatException]::new('Publication owner changed.') }
    if ($Operation.kind -ceq 'create-issue') {
        return ([string]$Target.body).Contains($Operation.creation_marker)
    }
    if ($Operation.kind -ceq 'create-comment') { return $Target.body -ceq $Operation.payload.body }
    if ($Operation.purpose -ceq 'run-presentation') {
        # A matching label/state is not confirmation if reporting advanced the evidence.
        # This guard applies to initial reconciliation and the post-write readback alike.
        $root = Read-ScheduledRecord $Target.body run
        $index = Read-ScheduledRecord $Target.body run-publication
        if ($root.repository_id -ne $Context.policy.repository_id -or
            $root.run_id -ne $Operation.run_id -or $index.index_digest -cne $Operation.preimage) {
            throw [FormatException]::new('Run evidence changed during presentation; rescan instead of acknowledging it.')
        }
        $names = @($Target.labels | ForEach-Object { $_.name })
        return $Target.state -ceq $Operation.payload.state -and
            (('scheduled-triaged' -iin $names) -eq $Operation.triaged)
    }
    $block = Get-TriageOwnedBlock -Text $Target.body -Kind $Operation.block_kind
    return $null -ne $block -and $block.Value -ceq $Operation.payload.body -and
        (-not $Operation.payload.ContainsKey('state') -or $Target.state -ceq $Operation.payload.state)
}

function Complete-TriageOperationReceipt {
    param($Context, $Operation, $Target)
    $id = if ($Operation.kind -cin @('create-issue', 'update-issue')) { $Target.number } else { $Target.id }
    $null = Invoke-TriageTransaction $Context triage-confirm-operation @{
        operation_key = $Operation.key
        receipt = @{ operation_id = $Operation.id; target_id = $id
            payload_digest = Get-ScheduledDigest $Operation.payload; target = $Target
            target_digest = Get-ScheduledDigest $Target }
    }
    return @{ action = 'confirmed'; target_id = $id; operation_key = $Operation.key }
}

function Invoke-ScheduledTriageOperation {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][hashtable] $Context,
        [Parameter(Mandatory)][hashtable] $Specification,
        [scriptblock] $Api = {
            param($Endpoint, $Method = 'GET', $Body, [switch] $Paginate)
            Invoke-ScheduledGitHubApi -Endpoint $Endpoint -Method $Method -Body $Body -Paginate:$Paginate
        }
    )
    $null = Invoke-TriageTransaction $Context triage-authorize-publication
    $operation = Get-TriageOperation $Context $Specification.key
    if ($null -eq $operation) {
        $null = Invoke-TriageTransaction $Context triage-prepare-operation @{ operation = $Specification }
        $operation = Get-TriageOperation $Context $Specification.key
    }
    if ($operation.spec_digest -cne (Get-ScheduledDigest $Specification)) {
        throw [FormatException]::new('Prepared operation differs from the retained publication intent.')
    }
    if ($operation.stage -ceq 'confirmed') {
        return @{ action = 'confirmed'; target_id = $operation.target_id; operation_key = $operation.key }
    }
    $target = Find-TriageOperationTarget $Context $operation $Api
    if (Test-TriageOperationTarget $Context $operation $target) {
        if ($operation.stage -ceq 'prepared') {
            $null = Invoke-TriageTransaction $Context triage-begin-operation @{ operation_key = $operation.key }
        }
        return Complete-TriageOperationReceipt $Context $operation $target
    }
    if ($operation.stage -ceq 'sending' -and $operation.kind -cin @('create-issue', 'create-comment')) {
        throw [IO.IOException]::new('Publication outcome is unknown; no duplicate POST is authorized.')
    }
    if ($operation.stage -cnotin @('prepared', 'sending')) { throw 'Publication intent is not executable.' }
    $payload = $operation.payload.Clone()
    if ($operation.kind -cin @('update-issue', 'update-comment')) {
        if ($null -eq $target) { throw [FormatException]::new('Known publication target is missing.') }
        if ($operation.purpose -ceq 'run-presentation') {
            # Modify only the role label and state; never rewrite the reporter's issue body.
            $payload.labels = @($target.labels | ForEach-Object { $_.name } |
                Where-Object { $_ -ine 'scheduled-triaged' })
            if ($operation.triaged) { $payload.labels += 'scheduled-triaged' }
        } else {
            if ($operation.kind -ceq 'update-issue' -and $payload.ContainsKey('state') -and (
                -not $operation.ContainsKey('expected_state') -or $target.state -cne $operation.expected_state)) {
                throw [FormatException]::new('Problem state changed after preparation; reconsider the external transition.')
            }
            $block = Get-TriageOwnedBlock -Text $target.body -Kind $operation.block_kind
            $current = if ($null -eq $block) { '' } else { $block.Value }
            if ($current -cne $operation.preimage) {
                throw [FormatException]::new('Owned record changed; preserve it and reconcile the publication intent.')
            }
            $payload.body = if ($null -eq $block) { "$($target.body)`n`n$($operation.payload.body)" } else {
                $target.body.Substring(0, $block.Index) + $operation.payload.body +
                    $target.body.Substring($block.Index + $block.Length)
            }
            if (-not $payload.body.StartsWith('[Copilot speaking]')) { $payload.body = "[Copilot speaking]`n`n$($payload.body)" }
        }
    }
    if ($payload.ContainsKey('body') -and [Text.Encoding]::UTF8.GetByteCount($payload.body) -gt 60000) {
        throw [FormatException]::new('Publication exceeds the payload budget; no evidence or human text was truncated.')
    }
    if ($operation.stage -ceq 'prepared') {
        $null = Invoke-TriageTransaction $Context triage-begin-operation @{ operation_key = $operation.key }
    }
    if ($operation.kind -ceq 'create-issue') {
        return @{
            action = 'native-create-issue'; operation_key = $operation.key
            payload = @{ repo_full_name = $Context.policy.repository; title = $payload.title
                body = $payload.body; labels = @('scheduled-finding') }
        }
    }
    $endpoint = switch ($operation.kind) {
        'create-label' { "repos/$($Context.policy.repository)/labels" }
        'create-comment' { "repos/$($Context.policy.repository)/issues/$($operation.issue_number)/comments" }
        'update-comment' { "repos/$($Context.policy.repository)/issues/comments/$($operation.target_id)" }
        'update-issue' { "repos/$($Context.policy.repository)/issues/$($operation.target_id)" }
        default { throw 'Unsupported triage write kind.' }
    }
    $method = if ($operation.kind -cin @('create-comment', 'create-label')) { 'POST' } else { 'PATCH' }
    $response = & $Api -Endpoint $endpoint -Method $method -Body $payload
    $id = if ($operation.kind -ceq 'update-issue') { $response.number } else { $response.id }
    $null = Invoke-TriageTransaction $Context triage-observe-operation @{
        operation_key = $operation.key; target_id = $id
    }
    $operation = Get-TriageOperation $Context $operation.key
    $target = Find-TriageOperationTarget $Context $operation $Api
    if (-not (Test-TriageOperationTarget $Context $operation $target)) {
        throw [IO.IOException]::new('GitHub readback has not confirmed the intended publication.')
    }
    return Complete-TriageOperationReceipt $Context $operation $target
}

function Publish-ScheduledTriageDocument {
    [CmdletBinding()]
    param($Context, [long] $IssueNumber, [ValidateSet('triage', 'problem')][string] $Kind,
        [hashtable] $Document, [string] $Key, [string] $ExpectedIndexDigest, [scriptblock] $Api)
    $prepared = Invoke-ScheduledRecordTool -Package scheduled-run-record -Request @{
        op = 'prepare_document'; kind = $Kind; owner = "$($Context.policy.repository_id)/$IssueNumber"
        document = $Document
    }
    $state = Invoke-TriageTransaction $Context read
    $checkpoint = $state.triage.analyses[$Context.analysis_id].checkpoint.analysis.checkpoint
    # The committed index must survive both before and after appending this exact revision.
    # Only pages belonging to this prepared write may explain an uncommitted difference.
    $beforePages = & $Api -Endpoint "repos/$($Context.policy.repository)/issues/$IssueNumber/comments?per_page=100" -Paginate
    $beforeComments = @($beforePages | ForEach-Object { $_ } | Where-Object { $_.user.login -ceq $Context.login } |
        ForEach-Object { @{ id = $_.id; body = [string]$_.body } })
    Assert-TriageDocumentHistory $Context $IssueNumber $Kind $beforeComments $prepared.digest $ExpectedIndexDigest
    foreach ($page in $prepared.pages) {
        $specification = @{
            key = "$checkpoint/$Key/$($page.operation_id)"; kind = 'create-comment'; issue_number = $IssueNumber
            target_id = $null; payload = @{ body = $page.body }; preimage = $null
            checkpoint = $checkpoint; purpose = "$Kind-detail"
        }
        $null = Invoke-ScheduledTriageOperation -Context $Context -Specification $specification -Api $Api
    }
    $pages = & $Api -Endpoint "repos/$($Context.policy.repository)/issues/$IssueNumber/comments?per_page=100" -Paginate
    $comments = @($pages | ForEach-Object { $_ } | Where-Object { $_.user.login -ceq $Context.login } |
        ForEach-Object { @{ id = $_.id; body = [string]$_.body } })
    $restored = Invoke-ScheduledRecordTool -Package scheduled-run-record -Request @{
        op = 'restore_documents'; kind = $Kind; owner = "$($Context.policy.repository_id)/$IssueNumber"; comments = $comments
    }
    if ($restored.incomplete_revisions.Count -gt 0 -or
        @($restored.revisions | Where-Object { $_.digest -ceq $prepared.digest }).Count -ne 1) {
        throw [FormatException]::new('Detail publication is not complete; its root cannot advance.')
    }
    Assert-TriageDocumentHistory $Context $IssueNumber $Kind $comments $prepared.digest $ExpectedIndexDigest
    return @{ digest = $prepared.digest; index_digest = $restored.index_digest; revisions = $restored.revisions }
}

function Assert-TriageDocumentHistory {
    param($Context, [long] $IssueNumber, [string] $Kind, [object[]] $Comments,
        [string] $CurrentDigest, [string] $ExpectedIndexDigest)
    $withoutCurrent = @($Comments | Where-Object {
        -not ([string]$_.body).StartsWith("[Copilot speaking]`n<!-- scheduled-$Kind-detail`:v1 ") -or
            (Read-ScheduledRecord $_.body "$Kind-detail").digest -cne $CurrentDigest
    })
    $restored = Invoke-ScheduledRecordTool -Package scheduled-run-record -Request @{
        op = 'restore_documents'; kind = $Kind; owner = "$($Context.policy.repository_id)/$IssueNumber"
        comments = $withoutCurrent
    }
    if ($restored.incomplete_revisions.Count -gt 0 -or $restored.index_digest -cne $ExpectedIndexDigest) {
        # A completed current revision may already be indexed after a lost root response.
        $full = Invoke-ScheduledRecordTool -Package scheduled-run-record -Request @{
            op = 'restore_documents'; kind = $Kind; owner = "$($Context.policy.repository_id)/$IssueNumber"
            comments = $Comments
        }
        if ($full.incomplete_revisions.Count -gt 0 -or $full.index_digest -cne $ExpectedIndexDigest) {
            throw [FormatException]::new('Committed detail history changed; do not replace the checkpoint.')
        }
    }
}

function Get-ScheduledTriageRecovery {
    [CmdletBinding()]
    param($Policy, $State, [scriptblock] $Api)
    if (-not $State.ContainsKey('triage')) {
        return @{ active = $null; evidence_key = $null; operations = @() }
    }
    Assert-ScheduledTriageState $State.triage
    if ($null -eq $State.triage.active_analysis_id) {
        return @{ active = $null; evidence_key = $null; operations = @() }
    }
    $analysis = $State.triage.analyses[$State.triage.active_analysis_id]
    if ($null -ne $analysis.checkpoint) {
        Assert-ScheduledTriageCheckpointContent $analysis.checkpoint $analysis.checkpoint_digest
    }
    $context = @{ policy = $Policy; login = $State.login }
    $observations = @(
        foreach ($operation in @($analysis.operations.Values | Sort-Object key)) {
            if ($operation.stage -cnotin @('prepared', 'sending')) { continue }
            $target = Find-TriageOperationTarget $context $operation $Api
            @{
                key = $operation.key; stage = $operation.stage
                visible = $null -ne $target
                target_digest = if ($null -ne $target) { Get-ScheduledDigest $target } else { $null }
            }
        }
    )
    $externalInput = @{ complete = $false }
    $inputError = $null
    try {
        $snapshot = Get-ScheduledTriageInbox -Policy $Policy -State $State -Api (Get-TriageReadAdapter $Api)
        $run = $snapshot.runs[[string]$analysis.revision.issue_number]
        $pending = @($snapshot.pending | Where-Object {
            $_.digest -ceq $analysis.revision.digest -and $_.run_id -eq $analysis.revision.run_id -and
            $_.run_attempt -eq $analysis.revision.run_attempt
        })
        $externalInput = @{
            complete = $true; reporter_index = $run.index_digest; problem_index = $snapshot.index.digest
            claimed_basis = if ($pending.Count -eq 1) { Get-ScheduledDigest $pending[0].basis } else { $null }
        }
    } catch [FormatException], [IO.IOException], [ArgumentException] {
        # An incomplete scan cannot replace publication recovery or manufacture a heartbeat.
        # Error text is diagnostic only: changing transport messages must not create retry keys.
        $inputError = $_.Exception.Message
    }
    # This read is available when an unfinished owned publication prevents a complete inbox
    # scan. It does not accept dispatch, change ownership or repeat a write.
    $key = Get-ScheduledDigest @{
        analysis_id = $analysis.id; phase = $analysis.phase; checkpoint = $analysis.checkpoint
        operations = $observations; external_input = $externalInput
    }
    return @{
        active = @{
            analysis_id = $analysis.id; session_id = $analysis.session_id; revision = $analysis.revision
            phase = $analysis.phase; dispatch = $analysis.dispatch; claim_token = $analysis.claim_token
        }
        evidence_key = $key; operations = $observations
        complete_input = $externalInput.complete; input_error = $inputError
    }
}

Export-ModuleMember -Function Invoke-TriageTransaction, Get-ScheduledTriageValidatedState, Save-ScheduledTriageSnapshot,
Invoke-ScheduledTriageOperation,
Publish-ScheduledTriageDocument, ConvertTo-TriageOwnedBlock, Get-TriageOwnedBlock,
Get-ScheduledTriageRecovery
