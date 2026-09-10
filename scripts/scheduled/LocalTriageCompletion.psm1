#requires -Version 7
# Commits an analysis only after its problem writes are reconciled. The triage root and its
# immutable detail are separate from reporter evidence; run labels/state are presentation
# derived from every current failed revision, not authority for acknowledging new evidence.
# Ref: ../../docs/scheduled-triage.md#ownership-and-recovery.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
Import-Module (Join-Path $PSScriptRoot 'ScheduledContracts.psm1')
Import-Module (Join-Path $PSScriptRoot 'LocalTriageInbox.psm1')
Import-Module (Join-Path $PSScriptRoot 'LocalTriagePublication.psm1')
Import-Module (Join-Path $PSScriptRoot 'LocalTriageProblem.psm1')
Import-Module (Join-Path $PSScriptRoot 'ScheduledRecordTool.psm1')

function Complete-ScheduledTriageAnalysis {
    [CmdletBinding()]
    param($Context, [scriptblock] $Api)
    $null = Invoke-TriageTransaction $Context triage-authorize-publication
    $state = Invoke-TriageTransaction $Context read
    $analysis = $state.triage.analyses[$Context.analysis_id]
    if ($null -eq $analysis.checkpoint) { throw 'Checkpoint validated analysis before publishing its disposition.' }
    if ($analysis.comparison.requires_reanalysis) {
        return @{ action = 'reanalysis-required'; reason = $analysis.comparison.reason }
    }
    $checkpoint = $analysis.checkpoint.analysis.checkpoint
    $key = "$checkpoint/completion"
    if (-not $analysis.publication.ContainsKey($key)) {
        $snapshot = Get-ScheduledTriageInbox -Policy $Context.policy -State $state -Api (Get-TriageReadAdapter $Api)
        if ($snapshot.index.digest -cne $analysis.comparison.index.digest) {
            $null = Invoke-TriageTransaction $Context triage-require-reanalysis @{ reason = 'external-index-change' }
            return @{ action = 'reanalysis-required'; reason = 'external-index-change' }
        }
        $run = $snapshot.runs[[string]$analysis.revision.issue_number]
        $links = @{}
        foreach ($problem in $analysis.checkpoint.analysis.problems) {
            $resultKey = "$checkpoint/problem-result:$($problem.key)"
            if ($analysis.publication.ContainsKey($resultKey)) { $links[$problem.key] = $analysis.publication[$resultKey] }
        }
        if ($analysis.checkpoint.analysis.status -ceq 'complete' -and
            $links.Count -ne $analysis.checkpoint.analysis.problems.Count) {
            throw 'Required problem publication is incomplete.'
        }
        $original = ''
        if ($null -ne $run.triage_comment_id) {
            $comment = @($run.comments | Where-Object { $_.id -eq $run.triage_comment_id })[0]
            $block = Get-TriageOwnedBlock $comment.body triage
            if ($null -eq $block) { throw [FormatException]::new('Triage root has no unique owned content block.') }
            $original = $block.Value
        }
        $plan = @{
            issue_number = $analysis.revision.issue_number; comment_id = $run.triage_comment_id
            expected_index = $run.details.index_digest; original_block = $original
            document = @{
                analysis = $analysis.checkpoint.analysis; index = $analysis.checkpoint.index
                basis = $analysis.checkpoint.basis
                status = $analysis.checkpoint.analysis.status
                problem_links = $links; publication_complete = $links.Count -eq $analysis.checkpoint.analysis.problems.Count
                executor_id = $Context.executor_id; session_id = $Context.session_id
            }
        }
        $null = Invoke-TriageTransaction $Context triage-prepare-document @{ key = 'completion'; document = $plan }
    } else { $plan = $analysis.publication[$key] }
    $details = Publish-ScheduledTriageDocument -Context $Context -IssueNumber $plan.issue_number `
        -Kind triage -Document $plan.document -Key 'triage' -ExpectedIndexDigest $plan.expected_index -Api $Api
    $root = @{
        schema_version = 1; role = 'triage'; repository_id = $Context.policy.repository_id
        issue_number = $plan.issue_number; workflow_id = $analysis.revision.workflow_id
        run_id = $analysis.revision.run_id; executor_id = $Context.executor_id
        index_digest = $details.index_digest; current_digest = $details.digest
        analysis_id = $analysis.id; session_id = $analysis.session_id
        status = $plan.document.status; reason = $analysis.checkpoint.analysis.reason
    }
    $content = "Analysis: $($root.status).`n`n" +
        [Net.WebUtility]::HtmlEncode([string]$root.reason) + "`n`n" +
        (Write-ScheduledRecord -Kind triage -Record $root)
    $block = ConvertTo-TriageOwnedBlock triage $content
    $specification = @{
        key = "$checkpoint/triage/root"; issue_number = $plan.issue_number; target_id = $plan.comment_id
        checkpoint = $checkpoint; purpose = 'triage-root'; block_kind = 'triage'
        preimage = $plan.original_block
        kind = if ($null -eq $plan.comment_id) { 'create-comment' } else { 'update-comment' }
        payload = @{ body = if ($null -eq $plan.comment_id) { "[Copilot speaking]`n`n$block" } else { $block } }
    }
    $null = Invoke-ScheduledTriageOperation -Context $Context -Specification $specification -Api $Api

    # This fresh complete restoration includes any attempt that arrived while the model worked.
    $state = Invoke-TriageTransaction $Context read
    $current = Get-ScheduledTriageInbox -Policy $Context.policy -State $state -Api (Get-TriageReadAdapter $Api)
    $run = $current.runs[[string]$plan.issue_number]
    $pending = @($current.pending | Where-Object { $_.issue_number -eq $plan.issue_number })
    $triaged = $pending.Count -eq 0
    if ($triaged) { Initialize-ScheduledTriageLabel $Context scheduled-triaged $Api }
    $presentation = @{
        key = "$checkpoint/run-presentation/$($run.index_digest)"; kind = 'update-issue'
        issue_number = $plan.issue_number; target_id = $plan.issue_number; checkpoint = $checkpoint
        purpose = 'run-presentation'; preimage = $run.index_digest; triaged = $triaged
        run_id = $analysis.revision.run_id; payload = @{ state = if ($triaged) { 'closed' } else { 'open' } }
    }
    $null = Invoke-TriageTransaction $Context triage-supersede-presentation @{
        current_key = $presentation.key; current_index = $run.index_digest
    }
    $null = Invoke-ScheduledTriageOperation -Context $Context -Specification $presentation -Api $Api
    if ($plan.document.status -ceq 'complete') {
        $null = Invoke-TriageTransaction $Context triage-complete-analysis
    }
    return @{
        action = 'recorded'; analysis_status = $plan.document.status; issue_number = $plan.issue_number
        run_triaged = $triaged; pending_revision_count = $pending.Count
    }
}

Export-ModuleMember -Function Complete-ScheduledTriageAnalysis
