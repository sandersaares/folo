#requires -Version 7
# Establishes exact-attempt completeness without assigning causes or replacing a claimed digest.
# A fuller committed reporter revision may supply diagnostics, while a durable API snapshot
# supplies the complete job/step inventory. Newer attempts cannot supply this execution proof.
# Ref: ../../docs/scheduled-triage.md#evidence-and-reasoning.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
Import-Module (Join-Path $PSScriptRoot 'ScheduledRecordTool.psm1')
Import-Module (Join-Path $PSScriptRoot 'ScheduledContracts.psm1')

function Get-ScheduledTriageEvidenceBasis {
    [CmdletBinding()]
    param($Policy, $Record, $Revision, [scriptblock] $Api, $Run)
    $evidence = $Revision.evidence
    $attempt = $evidence.attempt.run_attempt
    $pages = @(& $Api -Endpoint (
        "repos/$($Policy.repository)/actions/runs/$($evidence.run_id)/attempts/$attempt/jobs?per_page=100") -Pages)
    if ($pages.Count -eq 0) { throw [FormatException]::new('Exact-attempt job pagination is unavailable.') }
    $jobs = [Collections.Generic.List[object]]::new()
    $ids = [Collections.Generic.HashSet[long]]::new()
    $total = $null
    foreach ($page in $pages) {
        if (-not $page.ContainsKey('total_count') -or -not $page.ContainsKey('jobs') -or
            $page.jobs -isnot [System.Collections.IList] -or [string]$page.total_count -cnotmatch '^(0|[1-9][0-9]*)$') {
            throw [FormatException]::new('Exact-attempt job page is incomplete.')
        }
        if ($null -eq $total) { $total = $page.total_count }
        if ($total -ne $page.total_count) { throw [FormatException]::new('Job inventory changed across pages.') }
        foreach ($job in $page.jobs) {
            if ($job.id -le 0 -or -not $ids.Add([long]$job.id) -or $job.run_id -ne $evidence.run_id -or
                ($job.ContainsKey('run_attempt') -and $job.run_attempt -ne $attempt) -or
                ($job.ContainsKey('head_sha') -and $job.head_sha -cne $evidence.attempt.controller_sha) -or
                -not $job.ContainsKey('status') -or -not $job.ContainsKey('conclusion') -or
                -not $job.ContainsKey('steps') -or $job.steps -isnot [System.Collections.IList]) {
                throw [FormatException]::new('Job identity or step inventory does not prove the claimed attempt.')
            }
            $numbers = [Collections.Generic.HashSet[long]]::new()
            foreach ($step in $job.steps) {
                if ($step.number -le 0 -or -not $numbers.Add([long]$step.number) -or
                    -not $step.ContainsKey('status') -or -not $step.ContainsKey('conclusion')) {
                    throw [FormatException]::new('Exact-attempt step inventory is incomplete.')
                }
            }
            $job.steps = @($job.steps | Sort-Object number)
            $jobs.Add($job)
        }
    }
    if ($jobs.Count -ne $total) { throw [FormatException]::new('Exact-attempt job pagination did not cover the complete inventory.') }
    $sources = @($Record.revisions | Where-Object { $_.evidence.attempt.run_attempt -eq $attempt })
    $sourceSha = $null
    foreach ($source in $sources) {
        if ($source.evidence.attempt.controller_sha -cne $evidence.attempt.controller_sha) {
            throw [FormatException]::new('Supporting controller identity conflicts with the claimed attempt.')
        }
        if ($null -ne $source.evidence.attempt.manifest) {
            $known = $source.evidence.attempt.manifest['source_sha']
            if ($null -ne $known) {
                Assert-ScheduledSha $known
                if ($null -ne $sourceSha -and $known -cne $sourceSha) {
                    throw [FormatException]::new('Supporting candidate source identities conflict.')
                }
                $sourceSha = $known
            }
        }
    }
    $apiEvidence = @{
        repository_id = $Record.identity.repository_id; workflow_id = $Record.identity.workflow_id
        run_id = $Record.identity.run_id; run_attempt = $attempt
        controller_sha = $evidence.attempt.controller_sha; source_sha = $sourceSha
        started_at = $Run.run_started_at; created_at = $Run.created_at
        workflow_conclusion = $Run.conclusion
        total_count = $total; jobs = @($jobs.ToArray() | Sort-Object id)
    }
    $fingerprint = Invoke-ScheduledRecordTool -Package scheduled-run-record `
        -Request @{ op = 'fingerprint'; value = $apiEvidence }
    return @{
        schema_version = 1; api_evidence = $apiEvidence; api_digest = $fingerprint.digest
        supporting_revisions = @($sources | Where-Object { $_.digest -cne $Revision.digest } |
            ForEach-Object { @{ digest = $_.digest; evidence = $_.evidence } })
    }
}

function Assert-ScheduledTriageEvidenceBasis {
    [CmdletBinding()]
    param($Basis, $CurrentBasis, $Record)
    foreach ($support in $Basis.supporting_revisions) {
        $committed = @($Record.revisions | Where-Object { $_.digest -ceq $support.digest })
        if ($committed.Count -ne 1 -or
            (Get-ScheduledDigest $committed[0].evidence) -cne (Get-ScheduledDigest $support.evidence)) {
            throw [FormatException]::new('Supporting evidence is not in the committed reporter index.')
        }
    }
    # Compare execution facts, not volatile runner metadata or fetch times. The original
    # complete API snapshot remains in the analysis detail and bound by its own digest.
    $project = {
        param($Evidence)
        @{
            repository_id = $Evidence.repository_id; workflow_id = $Evidence.workflow_id
            run_id = $Evidence.run_id; run_attempt = $Evidence.run_attempt
            controller_sha = $Evidence.controller_sha; total_count = $Evidence.total_count
            started_at = $Evidence.started_at; created_at = $Evidence.created_at
            workflow_conclusion = $Evidence.workflow_conclusion
            jobs = @($Evidence.jobs | ForEach-Object {
                @{ id = $_.id; status = $_.status; conclusion = $_.conclusion
                    steps = @($_.steps | ForEach-Object { @{ number = $_.number; status = $_.status; conclusion = $_.conclusion } }) }
            })
        }
    }
    if ((Get-ScheduledDigest (& $project $Basis.api_evidence)) -cne
        (Get-ScheduledDigest (& $project $CurrentBasis.api_evidence))) {
        throw [FormatException]::new('The persisted completion inventory differs from the exact-attempt API.')
    }
}

Export-ModuleMember -Function Get-ScheduledTriageEvidenceBasis, Assert-ScheduledTriageEvidenceBasis
