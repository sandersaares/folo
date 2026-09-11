#requires -Version 7
# Owns the triage role's durable transitions inside LocalState's exclusive short transaction.
# Native session actions happen between transactions; a scan lease never replaces an analysis
# owner. Repair attempts, dispatches and counters are not writable through this role.
# Ref: ../../docs/scheduled-triage.md#ownership-and-recovery.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
Import-Module (Join-Path $PSScriptRoot 'ScheduledContracts.psm1')
Import-Module (Join-Path $PSScriptRoot 'LocalTriagePolicy.psm1')
Import-Module (Join-Path $PSScriptRoot 'ScheduledRecordTool.psm1')
Import-Module (Join-Path $PSScriptRoot 'LocalTriageCache.psm1')
Import-Module (Join-Path $PSScriptRoot 'LocalTriageProfile.psm1')

function Assert-TriageField {
    param([System.Collections.IDictionary] $Value, [string[]] $Fields)
    if ($null -eq $Value) { throw 'Expected a triage object.' }
    foreach ($field in $Fields) {
        if (-not $Value.Contains($field)) { throw "Missing triage field: $field" }
    }
}

function Assert-ScheduledTriageState {
    param([System.Collections.IDictionary] $Triage)
    Assert-TriageField $Triage @('schema_version', 'mode', 'profile', 'scan', 'health',
        'active_analysis_id', 'analyses', 'known_run_issues', 'known_problem_issues', 'health_publication', 'repair_holds', 'cache_digest')
    if ($Triage.schema_version -ne 1 -or $Triage.mode -cnotin @('observe', 'paused', 'triage') -or
        $Triage.analyses -isnot [System.Collections.IDictionary]) {
        throw 'Corrupt triage state; recover ownership and accounting instead of resetting.'
    }
    foreach ($entry in $Triage.analyses.GetEnumerator()) {
        $analysis = $entry.Value
        $identityFields = @('id', 'revision', 'session_id', 'claim_token', 'started_at',
            'phase', 'dispatch', 'continuations', 'completion', 'completion_digest', 'profile_observation')
        Assert-TriageField $analysis $identityFields
        if ($analysis.id -cne $entry.Key -or $analysis.phase -cnotin @(
                'analyzing', 'publishing', 'blocked', 'complete', 'retired')) {
            throw 'Corrupt registered triage analysis.'
        }
        $null = [DateTimeOffset]$analysis.started_at
        Assert-ScheduledTriageProfileObservation $analysis.profile_observation dispatch $analysis.dispatch.token $analysis.session_id
        foreach ($continuation in $analysis.continuations) {
            Assert-TriageField $continuation @('token', 'key', 'admitted_at')
            $null = [DateTimeOffset]$continuation.admitted_at
        }
        Assert-TriageCompletionReference $analysis
        if ($analysis.phase -ceq 'retired') {
            if (@($analysis.Keys | Where-Object { $_ -cnotin $identityFields }).Count -gt 0 -or
                $analysis.dispatch.status -cne 'completed' -or $null -eq $analysis.completion) {
                throw 'Retired analysis must retain only reconciled identity, accounting and completion references.'
            }
            continue
        }
        Assert-TriageField $analysis @('checkpoint', 'checkpoint_digest', 'comparison', 'comparison_digest',
            'operations', 'publication', 'publication_digest', 'reads', 'read_progress', 'index_reads', 'reason', 'working_snapshot_id')
        if ($analysis.operations -isnot [System.Collections.IDictionary]) { throw 'Corrupt triage outbox.' }
        if (($null -eq $analysis.checkpoint -and $null -ne $analysis.checkpoint_digest) -or
            ($null -ne $analysis.checkpoint -and (Get-ScheduledDigest $analysis.checkpoint) -cne $analysis.checkpoint_digest) -or
            ($null -eq $analysis.comparison -and $null -ne $analysis.comparison_digest) -or
            ($null -ne $analysis.comparison -and (Get-ScheduledDigest $analysis.comparison) -cne $analysis.comparison_digest) -or
            $analysis.publication -isnot [System.Collections.IDictionary] -or
            (Get-ScheduledDigest $analysis.publication) -cne $analysis.publication_digest) {
            throw 'Retained triage checkpoint, comparison or publication content changed.'
        }
        if ($null -ne $analysis.checkpoint) {
            Assert-TriageField $analysis.checkpoint @('analysis', 'index', 'evidence', 'basis')
            Assert-TriageField $analysis.checkpoint.analysis @('analysis_id', 'revision', 'checkpoint')
            if ($analysis.checkpoint.analysis.analysis_id -cne $analysis.id -or
                (Get-ScheduledDigest $analysis.checkpoint.analysis.revision) -cne (Get-ScheduledDigest $analysis.revision)) {
                throw 'Retained checkpoint does not belong to its registered analysis and revision.'
            }
        }
        foreach ($operationEntry in $analysis.operations.GetEnumerator()) {
            Assert-TriageOperation $analysis $operationEntry.Key $operationEntry.Value
        }
    }
    if ($null -ne $Triage.active_analysis_id -and
        -not $Triage.analyses.Contains($Triage.active_analysis_id)) {
        throw 'Registered triage ownership is missing.'
    }
    $retained = @($Triage.analyses.Values | Where-Object { $_.phase -cne 'retired' })
    if ($retained.Count -gt 1 -or
        ($retained.Count -eq 1 -and $retained[0].id -cne $Triage.active_analysis_id) -or
        ($retained.Count -eq 0 -and $null -ne $Triage.active_analysis_id)) {
        throw 'Triage ownership does not identify exactly its retained native analysis.'
    }
    if ($null -ne $Triage.scan) {
        Assert-TriageField $Triage.scan @('snapshot_id', 'profile_observation')
        Assert-ScheduledTriageProfileObservation $Triage.scan.profile_observation scan $Triage.scan.token $Triage.scan.session_id
    }
    $projection = Get-ScheduledTriageCacheProjection $Triage
    foreach ($id in @(
        if ($null -ne $projection.scan) { $projection.scan.snapshot_id }
        if ($null -ne $projection.analysis) { $projection.analysis.working_snapshot_id; $projection.analysis.checkpoint_snapshot_id }
    )) {
        if ($null -ne $id -and [string]$id -cnotmatch '^[0-9a-f]{64}$') { throw 'Invalid owned snapshot identity.' }
    }
    if ((Get-ScheduledDigest $projection) -cne $Triage.cache_digest) {
        throw 'Durable snapshot ownership changed; do not delete cache payloads.'
    }
}

function Assert-TriageCompletionReference {
    param($Analysis)
    if ($null -eq $Analysis.completion) {
        if ($null -ne $Analysis.completion_digest -or $Analysis.phase -cin @('complete', 'retired')) {
            throw 'Completed analysis has no reconciled publication reference.'
        }
        return
    }
    Assert-TriageField $Analysis.completion @('checkpoint', 'issue_number', 'comment_id', 'digest')
    if ((Get-ScheduledDigest $Analysis.completion) -cne $Analysis.completion_digest -or
        $Analysis.completion.issue_number -ne $Analysis.revision.issue_number -or
        [string]$Analysis.completion.comment_id -cnotmatch '^[1-9][0-9]*$' -or
        [string]$Analysis.completion.checkpoint -cnotmatch '^[1-9][0-9]*$' -or
        $Analysis.completion.digest -cnotmatch '^[0-9a-f]{64}$') {
        throw 'Retained analysis completion reference is invalid.'
    }
}

function Assert-TriageOperation {
    param($Analysis, [string] $Key, [System.Collections.IDictionary] $Operation)
    Assert-TriageField $Operation @('key', 'kind', 'issue_number', 'target_id', 'payload',
        'preimage', 'checkpoint', 'purpose', 'id', 'stage', 'spec_digest', 'receipt')
    if ($Operation.key -cne $Key -or [string]::IsNullOrWhiteSpace($Key) -or
        $Operation.kind -cnotin @('create-issue', 'create-comment', 'update-issue', 'update-comment', 'create-label') -or
        $Operation.stage -cnotin @('prepared', 'sending', 'confirmed', 'superseded') -or
        $Operation.payload -isnot [System.Collections.IDictionary] -or $null -eq $Analysis.checkpoint -or
        $Operation.checkpoint -lt 1 -or $Operation.checkpoint -gt $Analysis.checkpoint.analysis.checkpoint -or
        $Operation.id -cne "$($Analysis.revision.repository_id)/$($Analysis.id)/$Key" -or
        ($null -ne $Operation.target_id -and [string]$Operation.target_id -cnotmatch '^[1-9][0-9]*$')) {
        throw 'Corrupt triage publication identity, kind, or stage.'
    }
    $specification = $Operation | ConvertTo-Json -Depth 100 | ConvertFrom-Json -AsHashtable
    foreach ($field in @('id', 'stage', 'spec_digest', 'receipt', 'superseded_by_index')) {
        $specification.Remove($field)
    }
    # Creation intents have no remote ID. Observation adds that ID without changing the
    # immutable specification; update intents retain their original, digest-bound target.
    if ($Operation.kind -cin @('create-issue', 'create-comment', 'create-label')) {
        $specification.target_id = $null
    }
    if ($Operation.spec_digest -cne (Get-ScheduledDigest $specification)) {
        throw 'Restored triage publication differs from its immutable intent.'
    }
    if ($null -ne $Operation.receipt) {
        Assert-TriageField $Operation.receipt @('target_id', 'payload_digest', 'operation_id')
        if ($Operation.stage -cnotin @('confirmed', 'superseded') -or $null -eq $Operation.target_id -or
            $Operation.receipt.target_id -ne $Operation.target_id -or
            $Operation.receipt.operation_id -cne $Operation.id -or
            $Operation.receipt.payload_digest -cne (Get-ScheduledDigest $Operation.payload)) {
            throw 'Restored triage publication receipt does not confirm its intent.'
        }
    } elseif ($Operation.stage -ceq 'confirmed') {
        throw 'Confirmed triage publication has no durable receipt.'
    }
}

function Assert-TriageScan {
    param($Triage, $Data, [DateTimeOffset] $Now)
    Assert-TriageField $Data @('scan_token')
    if ($null -eq $Triage.scan -or $Triage.scan.token -cne $Data.scan_token -or
        [DateTimeOffset]$Triage.scan.expires_at -le $Now) {
        throw 'Stale triage scan token.'
    }
}

function Assert-TriageAdmission {
    param($State, $Policy, $TriagePolicy, $Data, [switch] $Scan)
    $triage = $State.triage
    $installed = $triage.profile
    if ($triage.mode -cne 'triage' -or $TriagePolicy.mode -cne 'triage' -or
        $TriagePolicy.enrolled_machine_id -cne $State.executor_id -or
        $Policy.worker_login -cne $State.login -or $null -eq $installed -or -not $installed.enabled -or
        [string]::IsNullOrWhiteSpace($TriagePolicy.model) -or $installed.model -cne $TriagePolicy.model -or
        $installed.reasoning_effort -cne $TriagePolicy.reasoning_effort -or
        $installed.cadence_cron -cne $TriagePolicy.cadence_cron -or
        $installed.policy_digest -cne (Get-ScheduledTriagePolicyDigest $Policy $TriagePolicy) -or
        $installed.controller_digest -cne (Get-ScheduledTriageControllerDigest)) {
        throw 'Triage is inactive, unenrolled, or differs from its approved profile.'
    }
    # The transition chooses its proof scope; an extra caller-supplied scan token must
    # not let a worker borrow the coordinator's native observation.
    if ($Scan) {
        $scope = $triage.scan
        $observedProfileMatches = Test-ScheduledTriageProfileObservation $installed $scope.profile_observation scan $Data.scan_token $scope.session_id
    } else {
        $scope = $triage.analyses[$Data.analysis_id]
        $observedProfileMatches = Test-ScheduledTriageProfileObservation $installed $scope.profile_observation dispatch $Data.dispatch_token $scope.session_id
    }
    if (-not $observedProfileMatches) { throw 'Current native prompt/profile observation is missing or differs from approval.' }
}

function Get-TriageOwnedAnalysis {
    param($Triage, $Data)
    Assert-TriageField $Data @('analysis_id', 'session_id', 'claim_token', 'dispatch_token')
    if ($Triage.active_analysis_id -cne $Data.analysis_id) { throw 'Another analysis owns triage.' }
    $analysis = $Triage.analyses[$Data.analysis_id]
    if ($analysis.session_id -cne $Data.session_id -or $analysis.claim_token -cne $Data.claim_token -or
        $analysis.dispatch.token -cne $Data.dispatch_token -or $analysis.dispatch.status -cne 'accepted') {
        throw 'Session does not own the accepted triage dispatch.'
    }
    return $analysis
}

function Get-TriageSnapshotOwner {
    param($State, $Policy, $TriagePolicy, $Data, [DateTimeOffset] $Now)
    if ($Data.ContainsKey('analysis_id') -and $null -ne $Data.analysis_id) {
        Assert-TriageAdmission $State $Policy $TriagePolicy $Data
        $analysis = Get-TriageOwnedAnalysis $State.triage $Data
        return @{ kind = 'analysis'; token = $analysis.dispatch.token }
    }
    Assert-TriageScan $State.triage $Data $Now
    Assert-TriageField $Data @('session_id')
    if ($Data.session_id -cne $State.triage.scan.session_id) { throw 'Snapshot belongs to another native scan.' }
    return @{ kind = 'scan'; token = $State.triage.scan.token }
}

function Assert-TriageRetirement {
    param($Triage, $Data, [DateTimeOffset] $Now)
    Assert-TriageScan $Triage $Data $Now
    Assert-TriageField $Data @('analysis_id', 'session_id', 'native_idle_verified', 'completion_digest')
    if ($Triage.active_analysis_id -cne $Data.analysis_id) { throw 'Unknown completed analysis.' }
    $analysis = $Triage.analyses[$Data.analysis_id]
    if ($analysis.phase -cne 'complete' -or $analysis.dispatch.status -cne 'completed' -or
        $analysis.session_id -cne $Data.session_id -or $Data.native_idle_verified -ne $true -or
        $null -eq $analysis.completion -or $analysis.completion_digest -cne $Data.completion_digest) {
        throw 'Publication and native quiescence must be established before retiring analysis.'
    }
}

function Invoke-ScheduledTriageStateChange {
    param($State, $Policy, $TriagePolicy, [string] $Action, $Data, [DateTimeOffset] $Now, $CheckpointValidation)
    $stamp = $Now.ToUniversalTime().ToString('o')
    if (-not $State.Contains('triage')) {
        if ($Action -ceq 'triage-read') { return }
        if ($Action -cne 'triage-register-profile' -or $Data['operator_approved'] -ne $true) {
            throw 'Triage role is not registered; explicit operator setup is required.'
        }
        $State.triage = @{
            schema_version = 1; mode = 'observe'; profile = $null; scan = $null
            active_analysis_id = $null; analyses = @{}; known_run_issues = @(); known_problem_issues = @()
            health_publication = @{}; repair_holds = @{}
            cache_digest = $null
            health = @{
                last_scan_at = $null; last_successful_scan = $null; backlog_count = $null
                oldest_pending_at = $null; blocked_conditions = @(); profile_registered_at = $null
            }
        }
    }
    $triage = $State.triage
    switch -CaseSensitive ($Action) {
        'triage-read' { return }
        'triage-authorize-publication' {
            Assert-TriageAdmission $State $Policy $TriagePolicy $Data
            $analysis = Get-TriageOwnedAnalysis $triage $Data
            Assert-TriageField $Data @('checkpoint_digest')
            if ($analysis.checkpoint_digest -cne $Data.checkpoint_digest) {
                throw 'Checkpoint changed after typed publication validation.'
            }
            return
        }
        'triage-verify-checkpoint' {
            Assert-TriageField $Data @('expected_analysis_id', 'checkpoint_digest')
            if ($triage.active_analysis_id -cne $Data.expected_analysis_id -or
                $triage.analyses[$Data.expected_analysis_id].checkpoint_digest -cne $Data.checkpoint_digest) {
                throw 'Registered checkpoint changed while its typed content was being validated.'
            }
            return
        }
        'triage-authorize-snapshot' {
            $null = Get-TriageSnapshotOwner $State $Policy $TriagePolicy $Data $Now
            return
        }
        'triage-authorize-retirement' {
            Assert-TriageRetirement $triage $Data $Now
            return
        }
        'triage-pin-snapshot' {
            $owner = Get-TriageSnapshotOwner $State $Policy $TriagePolicy $Data $Now
            Assert-TriageField $Data @('snapshot_id', 'temporary_path', 'owner_kind', 'owner_token')
            if ($Data.snapshot_id -cnotmatch '^[0-9a-f]{64}$' -or
                $Data.owner_kind -cne $owner.kind -or $Data.owner_token -cne $owner.token) {
                throw 'Prepared snapshot does not belong to the current native owner.'
            }
            if ($owner.kind -ceq 'analysis') {
                $triage.analyses[$Data.analysis_id].working_snapshot_id = $Data.snapshot_id
            } else { $triage.scan.snapshot_id = $Data.snapshot_id }
        }
        'triage-clean-cache' {
            $null = Get-TriageSnapshotOwner $State $Policy $TriagePolicy $Data $Now
        }
        'triage-register-profile' {
            Assert-TriageField $Data @('operator_approved', 'profile')
            $fields = @('automation_id', 'project_id', 'host_id', 'executor_id',
                'login', 'user_id', 'cadence_cron', 'timezone', 'enabled', 'policy_digest',
                'controller_digest', 'prompt_digest', 'model', 'reasoning_effort')
            Assert-TriageField $Data.profile $fields
            if (@($Data.profile.Keys | Where-Object { $_ -cnotin $fields }).Count -gt 0 -or
                $Data.profile.enabled -isnot [bool]) {
                throw 'Profile contains unsupported fields; credentials and inferred billing fields are not profile data.'
            }
            if ($Data.operator_approved -ne $true -or $Data.profile.executor_id -cne $State.executor_id -or
                $Data.profile.login -cne $State.login -or $Data.profile.user_id -le 0 -or
                [string]::IsNullOrWhiteSpace($Data.profile.host_id) -or
                [string]::IsNullOrWhiteSpace($Data.profile.model) -or
                $Data.profile.prompt_digest -cnotmatch '^[0-9a-f]{64}$') {
                throw 'An operator-selected native triage profile must match enrollment.'
            }
            if ($null -ne $triage.profile -and
                (Get-ScheduledDigest $triage.profile) -ceq (Get-ScheduledDigest $Data.profile)) { return }
            $triage.profile = $Data.profile
            $triage.health.profile_registered_at = $stamp
        }
        'triage-set-mode' {
            if ($Data['operator_approved'] -ne $true -or $Data['mode'] -cnotin @('observe', 'paused', 'triage')) {
                throw 'Triage mode requires an explicit operator decision.'
            }
            $triage.mode = $Data.mode
        }
        'triage-acquire-scan' {
            Assert-TriageField $Data @('session_id')
            if ([string]::IsNullOrWhiteSpace($Data.session_id) -or
                ($null -ne $triage.scan -and [DateTimeOffset]$triage.scan.expires_at -gt $Now)) {
                throw 'Another triage poll owns the scan.'
            }
            $triage.scan = @{
                token = [guid]::NewGuid().ToString(); session_id = $Data.session_id
                expires_at = $Now.AddMinutes($TriagePolicy.scan_lease_minutes).ToString('o')
                started_analysis_id = $null
                snapshot_id = $null
                profile_observation = $null
            }
            $triage.scan.profile_observation = Get-ScheduledTriageProfileObservation `
                $Data['profile_observation'] scan $triage.scan.token $Data.session_id
        }
        'triage-release-scan' {
            Assert-TriageScan $triage $Data $Now
            $triage.scan = $null
        }
        'triage-record-scan' {
            Assert-TriageScan $triage $Data $Now
            Assert-TriageField $Data @('successful', 'backlog_count', 'oldest_pending_at',
                'blocked_conditions', 'run_issues', 'problem_issues')
            $triage.health.last_scan_at = $stamp
            $triage.health.blocked_conditions = @($Data.blocked_conditions)
            if ($Data.successful -eq $true) {
                $triage.health.last_successful_scan = $stamp
                $triage.health.backlog_count = $Data.backlog_count
                $triage.health.oldest_pending_at = $Data.oldest_pending_at
                $triage.known_run_issues = @(@($triage.known_run_issues) + @($Data.run_issues) | Sort-Object -Unique)
                $triage.known_problem_issues = @(@($triage.known_problem_issues) + @($Data.problem_issues) | Sort-Object -Unique)
            }
        }
        'triage-claim' {
            Assert-TriageScan $triage $Data $Now
            Assert-TriageAdmission $State $Policy $TriagePolicy $Data -Scan
            Assert-TriageField $Data @('revision', 'session_id', 'native_verified', 'claim_validation')
            Assert-TriageField $Data.revision @('repository_id', 'workflow_id', 'run_id',
                'run_attempt', 'digest', 'issue_number')
            if ($null -eq $triage.scan.snapshot_id -or
                $triage.scan.snapshot_id -cne $Data.claim_validation.snapshot_id -or
                (Get-ScheduledDigest $Data.revision) -cne $Data.claim_validation.revision_digest) {
                throw 'Claim inputs changed after exact pending-snapshot validation.'
            }
            if ($null -ne $triage.active_analysis_id -or $null -ne $triage.scan.started_analysis_id -or
                $Data.session_id -cne $triage.scan.session_id -or $Data.native_verified -ne $true -or
                $Data.revision.repository_id -ne $Policy.repository_id -or
                $Data.revision.digest -cnotmatch '^[0-9a-f]{64}$') {
                throw 'Triage requires a verified native owner and an unclaimed exact revision.'
            }
            foreach ($field in @('workflow_id', 'run_id', 'run_attempt', 'issue_number')) {
                if ([string]$Data.revision[$field] -cnotmatch '^[1-9][0-9]*$') { throw 'Invalid revision identity.' }
            }
            $today = @($triage.analyses.Values | Where-Object {
                ([DateTimeOffset]$_.started_at).UtcDateTime.Date -eq $Now.UtcDateTime.Date
            })
            if ($today.Count -ge $TriagePolicy.max_starts_per_day) { throw 'Triage start budget exhausted.' }
            if (@($triage.analyses.Values | Where-Object {
                (Get-ScheduledDigest $_.revision) -ceq (Get-ScheduledDigest $Data.revision)
            }).Count -gt 0) { throw 'Existing revision ownership must be reconciled, not claimed again.' }
            $id = [guid]::NewGuid().ToString()
            $triage.analyses[$id] = @{
                id = $id; revision = $Data.revision; session_id = $Data.session_id
                claim_token = [guid]::NewGuid().ToString(); started_at = $stamp; phase = 'analyzing'
                dispatch = @{ token = [guid]::NewGuid().ToString(); status = 'accepted' }
                continuations = @(); checkpoint = $null; checkpoint_digest = $null
                comparison = $null; comparison_digest = $null; operations = @{}; publication = @{}
                publication_digest = Get-ScheduledDigest @{}
                working_snapshot_id = $triage.scan.snapshot_id
                completion = $null; completion_digest = $null
                profile_observation = $null
                reads = @{}; read_progress = @{}; index_reads = @{}; reason = $null
            }
            $observation = $triage.scan.profile_observation
            $triage.analyses[$id].profile_observation = Get-ScheduledTriageProfileObservation @{
                automation_id = $observation.automation_id; prompt_digest = $observation.prompt_digest
            } dispatch $triage.analyses[$id].dispatch.token $Data.session_id
            $triage.active_analysis_id = $id
            $triage.scan.started_analysis_id = $id
        }
        'triage-checkpoint' {
            Assert-TriageAdmission $State $Policy $TriagePolicy $Data
            $analysis = Get-TriageOwnedAnalysis $triage $Data
            Assert-TriageField $Data @('checkpoint')
            Assert-TriageField $Data.checkpoint @('analysis', 'index', 'evidence', 'basis')
            if ($Data.checkpoint.ContainsKey('snapshot_id') -and
                $Data.checkpoint.snapshot_id -cne $analysis.working_snapshot_id -and (
                    $null -eq $analysis.checkpoint -or -not $analysis.checkpoint.ContainsKey('snapshot_id') -or
                    $Data.checkpoint.snapshot_id -cne $analysis.checkpoint.snapshot_id)) {
                throw 'Checkpoint snapshot is not pinned by its accepted analysis.'
            }
            $index = $Data.checkpoint.index
            if (-not $analysis.index_reads.ContainsKey($index.digest) -or
                (Get-ScheduledDigest @($analysis.index_reads[$index.digest] | Sort-Object)) -cne
                    (Get-ScheduledDigest @($index.entries | ForEach-Object { $_.issue_number } | Sort-Object))) {
                throw 'Read every summary batch of the complete problem index before checkpointing.'
            }
            foreach ($entry in $index.entries) {
                if ($null -ne $entry.full_read_digest -and (
                    -not $analysis.reads.ContainsKey([string]$entry.issue_number) -or
                    $analysis.reads[[string]$entry.issue_number] -cne $entry.full_read_digest)) {
                    throw 'Full candidate read receipt is missing or stale.'
                }
            }
            if ($null -eq $CheckpointValidation -or
                $CheckpointValidation.input_digest -cne (Get-ScheduledDigest $Data.checkpoint) -or
                $CheckpointValidation.digest -cne (Get-ScheduledDigest $CheckpointValidation.checkpoint)) {
                throw 'Checkpoint differs from the input validated outside the transaction.'
            }
            if ($Data.checkpoint.analysis.analysis_id -cne $analysis.id -or
                (Get-ScheduledDigest $Data.checkpoint.analysis.revision) -cne (Get-ScheduledDigest $analysis.revision) -or
                ($null -ne $analysis.checkpoint -and
                    $Data.checkpoint.analysis.checkpoint -le $analysis.checkpoint.analysis.checkpoint)) {
                throw 'Checkpoint must advance the claimed analysis, not replace its evidence.'
            }
            if (@($analysis.operations.Values | Where-Object { $_.stage -ceq 'sending' }).Count -gt 0) {
                throw 'Reconcile uncertain publication before changing analysis.'
            }
            foreach ($operation in $analysis.operations.Values) {
                if ($operation.stage -ceq 'prepared') { $operation.stage = 'superseded' }
            }
            $analysis.checkpoint = $CheckpointValidation.checkpoint
            $analysis.checkpoint_digest = $CheckpointValidation.digest
            $analysis.completion = $null; $analysis.completion_digest = $null
            $analysis.phase = 'analyzing'
            $comparisonIndex = $Data.checkpoint.index | ConvertTo-Json -Depth 100 | ConvertFrom-Json -AsHashtable
            foreach ($entry in $comparisonIndex.entries) { $entry.full_read_digest = $null }
            $analysis.comparison = @{ index = $comparisonIndex; requires_reanalysis = $false; reason = $null }
            $analysis.comparison_digest = Get-ScheduledDigest $analysis.comparison
        }
        'triage-accept-own-index' {
            $analysis = Get-TriageOwnedAnalysis $triage $Data
            Assert-TriageField $Data @('previous_digest', 'index', 'operation_key')
            if ($null -eq $analysis.comparison -or
                $analysis.comparison.index.digest -cne $Data.previous_digest -or
                -not $analysis.operations.ContainsKey($Data.operation_key) -or
                $analysis.operations[$Data.operation_key].stage -cne 'confirmed' -or
                -not $analysis.operations[$Data.operation_key].purpose.StartsWith('problem-root:', [StringComparison]::Ordinal)) {
                throw 'Only a confirmed problem publication can advance its comparison baseline.'
            }
            $analysis.comparison.index = $Data.index
            $analysis.comparison_digest = Get-ScheduledDigest $analysis.comparison
        }
        'triage-require-reanalysis' {
            $analysis = Get-TriageOwnedAnalysis $triage $Data
            if ($null -eq $analysis.comparison -or [string]::IsNullOrWhiteSpace($Data['reason'])) {
                throw 'Index reconsideration needs an existing checkpoint and a reason.'
            }
            $analysis.comparison.requires_reanalysis = $true
            $analysis.comparison.reason = $Data.reason
            $analysis.comparison_digest = Get-ScheduledDigest $analysis.comparison
        }
        'triage-record-index-read' {
            $analysis = Get-TriageOwnedAnalysis $triage $Data
            Assert-TriageField $Data @('index_digest', 'issue_numbers')
            $previous = @(if ($analysis.index_reads.ContainsKey($Data.index_digest)) {
                $analysis.index_reads[$Data.index_digest]
            })
            $analysis.index_reads[$Data.index_digest] = @($previous + @($Data.issue_numbers) | Sort-Object -Unique)
        }
        'triage-record-problem-page' {
            $analysis = Get-TriageOwnedAnalysis $triage $Data
            Assert-TriageField $Data @('issue_number', 'full_read_digest', 'offset', 'end_offset', 'total_length')
            $key = "$($Data.issue_number)/$($Data.full_read_digest)"
            $expected = if ($analysis.read_progress.ContainsKey($key)) { $analysis.read_progress[$key] } else { 0 }
            if ($Data.issue_number -le 0 -or $Data.offset -ne $expected -or
                $Data.end_offset -le $Data.offset -or $Data.end_offset -gt $Data.total_length) {
                throw 'Full candidate reads must consume every page in order.'
            }
            if ($Data.end_offset -eq $Data.total_length) {
                $analysis.reads[[string]$Data.issue_number] = $Data.full_read_digest
                $analysis.read_progress.Remove($key)
            } else { $analysis.read_progress[$key] = $Data.end_offset }
        }
        'triage-prepare-document' {
            Assert-TriageAdmission $State $Policy $TriagePolicy $Data
            $analysis = Get-TriageOwnedAnalysis $triage $Data
            Assert-TriageField $Data @('key', 'document')
            if ($null -eq $analysis.checkpoint) { throw 'Analysis must be validated before publication preparation.' }
            $key = "$($analysis.checkpoint.analysis.checkpoint)/$($Data.key)"
            if ($analysis.publication.ContainsKey($key)) {
                if ((Get-ScheduledDigest $analysis.publication[$key]) -cne (Get-ScheduledDigest $Data.document)) {
                    throw 'Prepared publication cannot be silently replaced.'
                }
                return
            }
            $analysis.publication[$key] = $Data.document
            $analysis.publication_digest = Get-ScheduledDigest $analysis.publication
        }
        'triage-record-repair-hold' {
            $analysis = Get-TriageOwnedAnalysis $triage $Data
            Assert-TriageField $Data @('issue_number', 'reason', 'operation_key')
            if ([string]::IsNullOrWhiteSpace($Data.reason) -or
                -not $analysis.operations.ContainsKey($Data.operation_key) -or
                $analysis.operations[$Data.operation_key].stage -cne 'confirmed' -or
                $analysis.operations[$Data.operation_key].target_id -ne $Data.issue_number) {
                throw 'A repair hold must refer to a confirmed canonical problem update.'
            }
            $triage.repair_holds[[string]$Data.issue_number] = @{
                reason = $Data.reason; analysis_id = $analysis.id; operation_key = $Data.operation_key
            }
        }
        'triage-release-repair-hold' {
            Assert-TriageField $Data @('operator_approved', 'issue_number')
            if ($Data.operator_approved -ne $true) { throw 'Repair scope reconciliation requires operator approval.' }
            $triage.repair_holds.Remove([string]$Data.issue_number)
        }
        'triage-prepare-operation' {
            Assert-TriageAdmission $State $Policy $TriagePolicy $Data
            $analysis = Get-TriageOwnedAnalysis $triage $Data
            Assert-TriageField $Data @('operation')
            $operation = $Data.operation | ConvertTo-Json -Depth 100 | ConvertFrom-Json -AsHashtable
            Assert-TriageField $operation @('key', 'kind', 'issue_number', 'target_id', 'payload',
                'preimage', 'checkpoint', 'purpose')
            if ($null -eq $analysis.checkpoint -or
                $operation.checkpoint -ne $analysis.checkpoint.analysis.checkpoint -or
                $operation.kind -cnotin @('create-issue', 'create-comment', 'update-issue', 'update-comment', 'create-label') -or
                [string]::IsNullOrWhiteSpace($operation.key) -or
                ($operation.kind -cin @('create-issue', 'create-comment') -and
                    -not ([string]$operation.payload.body).StartsWith('[Copilot speaking]'))) {
                throw 'Publication must derive from the current validated analysis.'
            }
            if ($operation.kind -ceq 'create-label' -and
                $operation.payload.name -cnotin @('scheduled-finding', 'scheduled-triaged')) {
                throw 'Triage cannot bootstrap unrelated labels.'
            }
            if ($analysis.operations.Contains($operation.key)) {
                $existing = $analysis.operations[$operation.key]
                if ($existing.spec_digest -cne (Get-ScheduledDigest $operation)) {
                    throw 'Publication operation identity conflicts with an existing intent.'
                }
                return
            }
            $operation.spec_digest = Get-ScheduledDigest $operation
            $operation.id = "$($State.repository_id)/$($analysis.id)/$($operation.key)"
            $operation.stage = 'prepared'
            $operation.receipt = $null
            $analysis.operations[$operation.key] = $operation
            $analysis.phase = 'publishing'
        }
        'triage-begin-operation' {
            Assert-TriageAdmission $State $Policy $TriagePolicy $Data
            $analysis = Get-TriageOwnedAnalysis $triage $Data
            if (-not $analysis.operations.Contains($Data['operation_key'])) { throw 'Unknown publication intent.' }
            $operation = $analysis.operations[$Data.operation_key]
            if ($operation.stage -cne 'prepared') { throw 'Unknown write must be reconciled before another send.' }
            $operation.stage = 'sending'
        }
        'triage-observe-operation' {
            $analysis = Get-TriageOwnedAnalysis $triage $Data
            Assert-TriageField $Data @('operation_key', 'target_id')
            $operation = $analysis.operations[$Data.operation_key]
            if ($operation.stage -cne 'sending' -or $Data.target_id -le 0 -or
                ($null -ne $operation.target_id -and $operation.target_id -ne $Data.target_id)) {
                throw 'Observed GitHub ID conflicts with the uncertain operation.'
            }
            $operation.target_id = $Data.target_id
        }
        'triage-confirm-operation' {
            $analysis = Get-TriageOwnedAnalysis $triage $Data
            Assert-TriageField $Data @('operation_key', 'receipt')
            $operation = $analysis.operations[$Data.operation_key]
            Assert-TriageField $Data.receipt @('target_id', 'payload_digest', 'operation_id')
            if ($operation.stage -cnotin @('sending', 'confirmed') -or $Data.receipt.target_id -le 0 -or
                $Data.receipt.operation_id -cne $operation.id -or
                $Data.receipt.payload_digest -cne (Get-ScheduledDigest $operation.payload) -or
                ($null -ne $operation.target_id -and $operation.target_id -ne $Data.receipt.target_id)) {
                throw 'GitHub readback does not confirm the prepared operation.'
            }
            $operation.target_id = $Data.receipt.target_id
            $operation.receipt = $Data.receipt
            $operation.stage = 'confirmed'
            if ($operation.kind -ceq 'create-issue') {
                $triage.known_problem_issues = @(@($triage.known_problem_issues) +
                    @($operation.target_id) | Sort-Object -Unique)
            }
        }
        'triage-complete-analysis' {
            $analysis = Get-TriageOwnedAnalysis $triage $Data
            if ($analysis.comparison.requires_reanalysis) { throw 'External index changes require AI reconsideration.' }
            if ($null -eq $analysis.checkpoint -or $analysis.checkpoint.analysis.status -cne 'complete') {
                throw 'Unfinished diagnosis cannot complete analysis.'
            }
            $operations = @($analysis.operations.Values | Where-Object {
                $_.checkpoint -eq $analysis.checkpoint.analysis.checkpoint -and $_.stage -cne 'superseded'
            })
            if ($operations.Count -eq 0 -or @($operations | Where-Object { $_.stage -cne 'confirmed' }).Count -gt 0 -or
                @($operations | Where-Object { $_.purpose -ceq 'triage-root' }).Count -ne 1 -or
                @($operations | Where-Object { $_.purpose -ceq 'run-presentation' }).Count -ne 1) {
                throw 'All publication and current run presentation must be reconciled before completion.'
            }
            foreach ($problem in $analysis.checkpoint.analysis.problems) {
                if (@($operations | Where-Object { $_.purpose -ceq "problem-root:$($problem.key)" }).Count -ne 1) {
                    throw 'Problem publication is incomplete.'
                }
            }
            $rootOperation = @($operations | Where-Object { $_.purpose -ceq 'triage-root' })[0]
            $root = Read-ScheduledRecord $rootOperation.payload.body triage
            if ($root.analysis_id -cne $analysis.id -or $root.run_id -ne $analysis.revision.run_id -or
                $root.issue_number -ne $analysis.revision.issue_number -or $root.status -cne 'complete') {
                throw 'Publication root does not confirm this completed analysis.'
            }
            $analysis.completion = @{
                checkpoint = $analysis.checkpoint.analysis.checkpoint; issue_number = $analysis.revision.issue_number
                comment_id = $rootOperation.target_id; digest = $root.current_digest
            }
            $analysis.completion_digest = Get-ScheduledDigest $analysis.completion
            $analysis.phase = 'complete'
        }
        'triage-supersede-presentation' {
            $analysis = Get-TriageOwnedAnalysis $triage $Data
            Assert-TriageField $Data @('current_key', 'current_index')
            foreach ($operation in $analysis.operations.Values) {
                if ($operation.purpose -ceq 'run-presentation' -and
                    $operation.checkpoint -eq $analysis.checkpoint.analysis.checkpoint -and
                    $operation.key -cne $Data.current_key -and $operation.preimage -cne $Data.current_index) {
                    # Presentation for an older reporter index cannot acknowledge a new revision.
                    # Retain its uncertain outcome, but reconcile current presentation separately.
                    $operation.stage = 'superseded'
                    $operation.superseded_by_index = $Data.current_index
                }
            }
        }
        'triage-complete-dispatch' {
            $analysis = Get-TriageOwnedAnalysis $triage $Data
            Assert-TriageField $Data @('reason')
            if ($null -eq $analysis.checkpoint -and [string]::IsNullOrWhiteSpace($Data.reason)) {
                throw 'An unfinished dispatch must retain progress or a blocker.'
            }
            $analysis.reason = $Data.reason
            $analysis.dispatch.status = 'completed'
        }
        'triage-reserve-continuation' {
            Assert-TriageScan $triage $Data $Now
            Assert-TriageAdmission $State $Policy $TriagePolicy $Data -Scan
            Assert-TriageField $Data @('analysis_id', 'session_id', 'native_idle_verified', 'evidence_key')
            if ($triage.active_analysis_id -cne $Data.analysis_id) { throw 'Unknown registered analysis.' }
            $analysis = $triage.analyses[$Data.analysis_id]
            if ($analysis.session_id -cne $Data.session_id -or $Data.native_idle_verified -ne $true -or
                $analysis.dispatch.status -cne 'completed' -or $analysis.phase -cin @('complete', 'retired') -or
                [string]::IsNullOrWhiteSpace($Data.evidence_key) -or
                $Data.evidence_key -cin @($analysis.continuations | ForEach-Object { $_.key })) {
                throw 'Continuation requires new progress and the same quiescent registered owner.'
            }
            $today = @($triage.analyses.Values | ForEach-Object { $_.continuations } | Where-Object {
                ([DateTimeOffset]$_.admitted_at).UtcDateTime.Date -eq $Now.UtcDateTime.Date
            })
            if ($today.Count -ge $TriagePolicy.max_continuations_per_day -or
                $analysis.continuations.Count -ge $TriagePolicy.max_continuations_per_analysis) {
                throw 'Triage continuation budget exhausted.'
            }
            $token = [guid]::NewGuid().ToString()
            $analysis.continuations += @{ token = $token; key = $Data.evidence_key; admitted_at = $stamp }
            $analysis.dispatch = @{ token = $token; status = 'reserved' }
            $analysis.profile_observation = $null
        }
        'triage-reconcile-dispatch' {
            Assert-TriageScan $triage $Data $Now
            Assert-TriageField $Data @('analysis_id', 'session_id', 'native_idle_verified', 'dispatch_token')
            if ($triage.active_analysis_id -cne $Data.analysis_id) { throw 'Unknown retained dispatch.' }
            $analysis = $triage.analyses[$Data.analysis_id]
            if ($analysis.session_id -cne $Data.session_id -or $Data.native_idle_verified -ne $true -or
                $analysis.dispatch.token -cne $Data.dispatch_token -or $analysis.dispatch.status -cne 'accepted') {
                throw 'Only proven accepted work in the same quiescent native session can resume.'
            }
            # This acknowledges turn quiescence, never analysis completion or abandoned ownership.
            $analysis.dispatch.status = 'completed'
        }
        'triage-begin-dispatch' {
            Assert-TriageScan $triage $Data $Now
            Assert-TriageAdmission $State $Policy $TriagePolicy $Data -Scan
            if ($triage.active_analysis_id -cne $Data['analysis_id']) { throw 'Unknown analysis dispatch.' }
            $analysis = $triage.analyses[$Data.analysis_id]
            if ($analysis.dispatch.status -cne 'reserved') { throw 'Unknown delivery must be reconciled, not repeated.' }
            $analysis.dispatch.status = 'sending'
        }
        'triage-accept-dispatch' {
            Assert-TriageField $Data @('analysis_id', 'session_id', 'claim_token', 'dispatch_token')
            if ($triage.active_analysis_id -cne $Data.analysis_id) { throw 'Stale analysis acceptance.' }
            $analysis = $triage.analyses[$Data.analysis_id]
            if ($analysis.session_id -cne $Data.session_id -or $analysis.claim_token -cne $Data.claim_token -or
                $analysis.dispatch.token -cne $Data.dispatch_token -or
                $analysis.dispatch.status -cnotin @('sending', 'accepted')) {
                throw 'Stale triage dispatch acceptance.'
            }
            $analysis.profile_observation = Get-ScheduledTriageProfileObservation `
                $Data['profile_observation'] dispatch $Data.dispatch_token $Data.session_id
            Assert-TriageAdmission $State $Policy $TriagePolicy $Data
            $analysis.dispatch.status = 'accepted'
        }
        'triage-block' {
            $analysis = Get-TriageOwnedAnalysis $triage $Data
            if ([string]::IsNullOrWhiteSpace($Data['reason'])) { throw 'A specific triage blocker is required.' }
            $analysis.phase = 'blocked'
            $analysis.reason = $Data.reason
        }
        'triage-retire' {
            Assert-TriageRetirement $triage $Data $Now
            $analysis = $triage.analyses[$Data.analysis_id]
            $tombstone = @{}
            foreach ($field in @('id', 'revision', 'session_id', 'claim_token', 'started_at',
                    'dispatch', 'continuations', 'completion', 'completion_digest', 'profile_observation')) {
                $tombstone[$field] = $analysis[$field]
            }
            $tombstone.phase = 'retired'
            $triage.analyses[$analysis.id] = $tombstone
            $triage.active_analysis_id = $null
        }
        default { throw "Unsupported triage state action: $Action" }
    }
    $triage.cache_digest = Get-ScheduledDigest (Get-ScheduledTriageCacheProjection $triage)
    Assert-ScheduledTriageState $triage
}

Export-ModuleMember -Function Assert-ScheduledTriageState, Invoke-ScheduledTriageStateChange
