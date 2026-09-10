#requires -Version 7
# Converts an AI-selected match into a fixed, evidence-bound problem publication. Preparation
# is persisted before pages or roots are written; a fresh index can require AI reconsideration,
# but never silently converts an unmatched decision into a deterministic causal match.
# Ref: ../../docs/scheduled-triage.md#evidence-and-reasoning.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
Import-Module (Join-Path $PSScriptRoot 'ScheduledContracts.psm1')
Import-Module (Join-Path $PSScriptRoot 'LocalTriageInbox.psm1')
Import-Module (Join-Path $PSScriptRoot 'LocalTriagePublication.psm1')
Import-Module (Join-Path $PSScriptRoot 'ScheduledRecordTool.psm1')
Import-Module (Join-Path $PSScriptRoot 'LocalTriageIndex.psm1')

function Get-TriageProblemKey {
    param($Analysis, [string] $Key)
    return "$($Analysis.checkpoint.analysis.checkpoint)/problem/$([Uri]::EscapeDataString($Key))"
}

function Get-TriageSourceObservation {
    param($Evidence, $Basis)
    $source = if ($null -ne $Basis.api_evidence.source_sha) {
        $Basis.api_evidence.source_sha
    } else { $Evidence.attempt.controller_sha }
    Assert-ScheduledSha $source
    return @{
        source_sha = $source; run_id = $Evidence.run_id; run_attempt = $Evidence.attempt.run_attempt
        started_at = $Basis.api_evidence.started_at; created_at = $Basis.api_evidence.created_at
    }
}

function ConvertTo-TriageLegacyProblem {
    param($Policy, [long] $IssueNumber, $Legacy, $Diagnosis, [string] $IssueState, [scriptblock] $Api)
    $observation = @{
        source_sha = $Legacy.source_sha; run_id = $Legacy.observation.run_id
        run_attempt = $Legacy.observation.run_attempt; started_at = $Legacy.observation.run_started_at
        created_at = $Legacy.observation.created_at
    }
    $resolution = $null
    $status = if ($IssueState -ceq 'closed') { 'needs-human' } else { 'open' }
    $confirmation = $Legacy.confirmation
    if ($Legacy.status -ceq 'confirmed' -and $null -ne $confirmation -and
        $confirmation['authoritative'] -eq $true -and $confirmation['successful'] -eq $true -and
        $confirmation['scope_complete'] -eq $true -and $confirmation['explained'] -eq $true -and
        $confirmation['generation'] -eq $Legacy.generation -and $confirmation['pr_number'] -gt 0 -and
        $null -ne $Legacy.validated_worker) {
        $pr = & $Api -Endpoint "repos/$($Policy.repository)/pulls/$($confirmation.pr_number)"
        $repair = Read-ScheduledRecord $pr.body repair
        $worker = $Legacy.validated_worker
        if ($pr.merged -eq $true -and $pr.base.repo.id -eq $Policy.repository_id -and
            $pr.merge_commit_sha -ceq $confirmation.merge_commit_sha -and
            $repair.issue_number -eq $IssueNumber -and $repair.finding_id -ceq $Legacy.finding_id -and
            $repair.generation -eq $Legacy.generation -and $worker.pr_number -eq $pr.number -and
            $repair.attempt_id -ceq $worker.attempt_id -and $repair.head_sha -ceq $worker.head_sha -and
            $pr.head.sha -ceq $worker.head_sha -and -not [string]::IsNullOrWhiteSpace($worker.explanation)) {
            $comparison = & $Api -Endpoint "repos/$($Policy.repository)/compare/$($pr.merge_commit_sha)...$($Legacy.source_sha)"
            if ($comparison.status -cin @('ahead', 'identical')) {
                $status = 'resolved'
                $resolution = @{
                    generation = $Legacy.generation; observation = $observation
                    explanation = $worker.explanation
                    evidence = @(@{
                        kind = 'registered-repair'; issue_number = $IssueNumber; generation = $Legacy.generation
                        finding_id = $Legacy.finding_id; pr_number = $pr.number
                        merge_commit_sha = $pr.merge_commit_sha; reporter_record_digest = Get-ScheduledDigest $Legacy
                    })
                }
            }
        }
    }
    # Importing a legacy identity is not AI completion or new repair authorization. Its
    # original evidence remains in the reporter record and the explicit legacy scope snapshot.
    return @{
        schema_version = 1; repository_id = $Policy.repository_id; issue_number = $IssueNumber
        generation = $Legacy.generation; scope_revision = 1; status = $status
        diagnosis = $Diagnosis; observation = $observation; evidence = @()
        resolution = $resolution; resolved_occurrences = @()
        legacy = @{
            finding_id = $Legacy.finding_id; generation = $Legacy.generation
            record = $Legacy
            scope = @(@{
                operation = $Legacy.check_kind; package = $Legacy.package; check_id = $Legacy.check_id
                platform = $Legacy.platform; replay = $Legacy.evidence.replay
                citations = @('/evidence')
            })
        }
    }
}

function Initialize-ScheduledTriageLabel {
    param($Context, [string] $Name, [scriptblock] $Api)
    $state = Invoke-TriageTransaction $Context read
    $checkpoint = $state.triage.analyses[$Context.analysis_id].checkpoint.analysis.checkpoint
    $description = switch ($Name) {
        'scheduled-finding' { 'Independently diagnosed scheduled validation problem' }
        'scheduled-triaged' { 'Complete analysis of all published failed run revisions' }
        default { throw 'Triage cannot initialize an unrelated label.' }
    }
    $specification = @{
        key = "$checkpoint/label/$Name"; kind = 'create-label'; issue_number = $null; target_id = $null
        checkpoint = $checkpoint; purpose = 'label'; preimage = $null
        # Neutral defaults describe role, not severity. Existing label metadata is never changed.
        payload = @{ name = $Name; color = 'ededed'; description = $description }
    }
    $null = Invoke-ScheduledTriageOperation -Context $Context -Specification $specification -Api $Api
}

function Invoke-ScheduledTriageProblemPreparation {
    [CmdletBinding()]
    param($Context, $Snapshot, [string] $ProblemKey, [scriptblock] $Api)
    $null = Invoke-TriageTransaction $Context triage-authorize-publication
    $state = Invoke-TriageTransaction $Context read
    $analysis = $state.triage.analyses[$Context.analysis_id]
    $choices = @($analysis.checkpoint.analysis.problems | Where-Object { $_.key -ceq $ProblemKey })
    if ($choices.Count -ne 1 -or $choices[0].matching.kind -ceq 'ambiguous') {
        throw 'Problem publication needs one unambiguous AI decision.'
    }
    $choice = $choices[0]
    $key = Get-TriageProblemKey $analysis $ProblemKey
    $planKey = "$($analysis.checkpoint.analysis.checkpoint)/problem:$ProblemKey"
    if ($analysis.publication.ContainsKey($planKey)) {
        return @{ action = 'prepared'; key = $ProblemKey; issue_number = $analysis.publication[$planKey].issue_number }
    }
    $createKey = "$key/create"
    $hasCreate = $analysis.operations.ContainsKey($createKey)
    if (-not $hasCreate) {
        $fresh = Get-ScheduledTriageInbox -Policy $Context.policy -State $state -Api (Get-TriageReadAdapter $Api)
        if ($analysis.comparison.requires_reanalysis -or $fresh.index.digest -cne $analysis.comparison.index.digest) {
            $null = Invoke-TriageTransaction $Context triage-require-reanalysis @{ reason = 'problem-index-changed' }
            return @{ action = 'reanalysis-required'; reason = 'problem-index-changed'; key = $ProblemKey }
        }
        $Snapshot = $fresh
    }
    $existing = $null
    $originalBlock = ''
    $legacy = $null
    $originalComments = @()
    $creationPayload = $null
    if ($choice.matching.kind -ceq 'new') {
        if (-not $hasCreate) { Initialize-ScheduledTriageLabel $Context scheduled-finding $Api }
        $creationMarker = Write-ScheduledRecord -Kind triage-operation -Record @{
            schema_version = 1; repository_id = $Context.policy.repository_id; role = 'triage'
            analysis_id = $analysis.id; operation_key = $createKey
        }
        $create = @{
            key = $createKey; kind = 'create-issue'; issue_number = $null; target_id = $null
            checkpoint = $analysis.checkpoint.analysis.checkpoint; purpose = "problem-create:$ProblemKey"
            preimage = $null; creation_marker = $creationMarker
            payload = @{
                title = $choice.diagnosis.title
                body = "[Copilot speaking]`n`n$([Net.WebUtility]::HtmlEncode($choice.diagnosis.summary))`n`n$creationMarker"
                labels = @('scheduled-finding')
            }
        }
        $result = Invoke-ScheduledTriageOperation -Context $Context -Specification $create -Api $Api
        if ($result.action -ceq 'native-create-issue') { return $result }
        $number = $result.target_id
        $creationPayload = $create.payload
        $issue = & $Api -Endpoint "repos/$($Context.policy.repository)/issues/$number"
        $relation = 'repeat'
        $targetGeneration = 1
    } else {
        $number = $choice.matching.issue_number
        $record = $Snapshot.problems[[string]$number].record
        $issue = $record.issue
        $originalComments = $record.comments
        $existing = $record.problem
        $legacy = $record.legacy
        if ($null -eq $existing) {
            $existing = ConvertTo-TriageLegacyProblem $Context.policy $number $legacy $choice.diagnosis $issue.state $Api
        } elseif ($null -ne $legacy -and $legacy.status -ceq 'confirmed' -and
            $existing.generation -eq $legacy.generation -and $existing.scope_revision -eq 1) {
            $confirmed = ConvertTo-TriageLegacyProblem $Context.policy $number $legacy $existing.diagnosis $issue.state $Api
            if ($null -ne $confirmed.resolution) {
                # This observes the retained reporter's existing confirmation authority, not
                # a new source-problem resolution policy or a green-run inference.
                $existing.status = 'resolved'
                $existing.resolution = $confirmed.resolution
                $existing.observation = $confirmed.observation
            }
        }
        $block = Get-TriageOwnedBlock -Text $issue.body -Kind problem
        if ($null -ne $block) { $originalBlock = $block.Value }
        $relation = $choice.matching.relation
        $targetGeneration = $choice.matching.target_generation
    }
    $empty = Invoke-ScheduledRecordTool -Package scheduled-run-record -Request @{
        op = 'restore_documents'; kind = 'problem'; owner = "$($Context.policy.repository_id)/$number"; comments = @()
    }
    $expectedIndex = $empty.index_digest
    if ($originalBlock -ne '') { $expectedIndex = (Read-ScheduledRecord $originalBlock problem).index_digest }
    $observation = Get-TriageSourceObservation $analysis.checkpoint.evidence $analysis.checkpoint.basis
    $sourceRelation = 'identical'
    if ($null -ne $existing) {
        $source = if ($relation -ceq 'recurrence' -and $null -ne $existing.resolution) {
            $existing.resolution.observation.source_sha
        } else { $existing.observation.source_sha }
        Assert-ScheduledSha $source
        $comparison = & $Api -Endpoint "repos/$($Context.policy.repository)/compare/$source...$($observation.source_sha)"
        $sourceRelation = switch ($comparison.status) {
            'identical' { 'identical' }
            'ahead' { 'descendant' }
            { $_ -cin @('behind', 'diverged') } { 'unrelated' }
            default { throw [FormatException]::new('Source ancestry is unavailable.') }
        }
    }
    $operationId = "$($Context.policy.repository_id)/$($analysis.id)/$key"
    $document = Invoke-ScheduledRecordTool -Package scheduled-triage-record -Request @{
        op = 'update_problem'; existing = $existing
        incoming = @{
            issue_number = $number; target_generation = $targetGeneration
            revision = $analysis.revision; diagnosis = $choice.diagnosis
            observation = $observation; relation = $relation; source_relation = $sourceRelation
            operation_id = $operationId; primary_evidence = $analysis.checkpoint.evidence; basis = $analysis.checkpoint.basis
        }
    }
    $evidenceGeneration = if ($relation -ceq 'historical') { $targetGeneration } else { $document.generation }
    $contribution = @($document.evidence | Where-Object {
        $_.generation -eq $evidenceGeneration -and
        (Get-ScheduledDigest $_.revision) -ceq (Get-ScheduledDigest $analysis.revision) -and
        (Get-ScheduledDigest $_.diagnosis) -ceq (Get-ScheduledDigest $choice.diagnosis)
    })
    if ($contribution.Count -ne 1) { throw 'Prepared problem has ambiguous contribution evidence.' }
    $plan = @{
        issue_number = $number; document = $document; original_block = $originalBlock
        expected_index = $expectedIndex; operation_id = $contribution[0].operation_id
        original_state = $issue.state; reopen = $relation -ceq 'recurrence'
        owned_repair = $null -ne $legacy -and $null -ne $legacy.validated_worker
        original_issue = $issue; original_comments = $originalComments; creation_payload = $creationPayload
    }
    $null = Invoke-TriageTransaction $Context triage-prepare-document @{ key = "problem:$ProblemKey"; document = $plan }
    return @{ action = 'prepared'; key = $ProblemKey; issue_number = $number }
}

function Publish-ScheduledTriageProblem {
    [CmdletBinding()]
    param($Context, [string] $ProblemKey, [scriptblock] $Api)
    $state = Invoke-TriageTransaction $Context read
    $analysis = $state.triage.analyses[$Context.analysis_id]
    $key = Get-TriageProblemKey $analysis $ProblemKey
    $planKey = "$($analysis.checkpoint.analysis.checkpoint)/problem:$ProblemKey"
    if (-not $analysis.publication.ContainsKey($planKey)) { throw 'Prepare the problem before publishing.' }
    $plan = $analysis.publication[$planKey]
    $published = Publish-ScheduledTriageDocument -Context $Context -IssueNumber $plan.issue_number `
        -Kind problem -Document $plan.document -Key $key -ExpectedIndexDigest $plan.expected_index -Api $Api
    $disposition = if ($plan.document.status -ceq 'needs-human') { 'needs-human' } else {
        $plan.document.diagnosis.repair_disposition
    }
    $root = @{
        schema_version = 1; role = 'triage'; repository_id = $Context.policy.repository_id
        issue_number = $plan.issue_number; executor_id = $Context.executor_id
        generation = $plan.document.generation; scope_revision = $plan.document.scope_revision
        current_digest = $published.digest; index_digest = $published.index_digest
        repair_disposition = $disposition; status = $plan.document.status
    }
    $content = @(
        [Net.WebUtility]::HtmlEncode($plan.document.diagnosis.summary)
        ''
        [Net.WebUtility]::HtmlEncode($plan.document.diagnosis.cause)
        ''
        "Repair disposition: $disposition."
        [Net.WebUtility]::HtmlEncode($plan.document.diagnosis.repair_reason)
        ''
        (Write-ScheduledRecord -Kind problem -Record $root)
    ) -join "`n"
    $payload = @{ body = ConvertTo-TriageOwnedBlock problem $content }
    if ($plan.reopen) { $payload.state = 'open' }
    $specification = @{
        key = "$key/root"; kind = 'update-issue'; issue_number = $plan.issue_number; target_id = $plan.issue_number
        checkpoint = $analysis.checkpoint.analysis.checkpoint; purpose = "problem-root:$ProblemKey"
        block_kind = 'problem'; preimage = $plan.original_block; payload = $payload
    }
    $null = Invoke-ScheduledTriageOperation -Context $Context -Specification $specification -Api $Api
    if ($plan.owned_repair -and ($disposition -cne 'actionable' -or $plan.document.scope_revision -gt 1)) {
        $null = Invoke-TriageTransaction $Context triage-record-repair-hold @{
            issue_number = $plan.issue_number
            reason = if ($plan.document.scope_revision -gt 1) {
                'Required verification scope differs from the retained repair registration.'
            } else { $plan.document.diagnosis.repair_reason }
            operation_key = $specification.key
        }
    }
    $link = @{
        issue_number = $plan.issue_number; problem_digest = $published.digest
        generation = $plan.document.generation; operation_id = $plan.operation_id
    }
    $null = Invoke-TriageTransaction $Context triage-prepare-document @{
        key = "problem-result:$ProblemKey"; document = $link
    }
    $state = Invoke-TriageTransaction $Context read
    $snapshot = Get-ScheduledTriageInbox -Policy $Context.policy -State $state -Api (Get-TriageReadAdapter $Api)
    if (-not (Sync-ScheduledTriageComparison $Context $plan $specification.key $snapshot)) {
        return @{ action = 'reanalysis-required'; reason = 'external-index-change'; key = $ProblemKey; link = $link }
    }
    return @{ action = 'published'; key = $ProblemKey; link = $link }
}

Export-ModuleMember -Function Invoke-ScheduledTriageProblemPreparation, Publish-ScheduledTriageProblem,
Initialize-ScheduledTriageLabel
