#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
# Protects finding identity and merge semantics: two observations of the same defect must resolve
# to the same finding id regardless of incidental evidence (line/column shifts, run metadata), and
# merging must never let a newer, weaker observation silently supersede stronger prior evidence.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
$VerbosePreference = 'Continue'

BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'ScheduledContracts.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'ScheduledPlan.psm1')
    Import-Module (Join-Path $PSScriptRoot 'ScheduledReport.psm1') -Force
    function Get-ReportTestIdentity {
        @{
            kind = 'miri'; package = 'events'; platform = 'ubuntu-latest'; path = 'src\lib.rs'
            function = ''; mutation = ''; test = 'example'; seed = '42'; flags = @('-Zmiri-seed=42')
        }
    }
    function Get-ReportTestRecord {
        @{
            schema_version = 1; repository = 'folo-rs/folo'; repository_id = 850321188
            finding_id = Get-ScheduledFindingId 'folo-rs/folo' (Get-ReportTestIdentity)
            generation = 1; status = 'open'; check_id = 'miri-ubuntu-latest'; package = 'events'
            platform = 'ubuntu-latest'; applicability = @{}; source_sha = 'a' * 40
            controller_sha = 'a' * 40; check_contract_digest = 'c' * 64
            observation = @{
                workflow_id = 100; run_id = 10; run_number = 10; run_attempt = 1
                workflow_path = '.github/workflows/scheduled-validation.yml'
                created_at = '2026-09-08T09:00:00Z'; run_started_at = '2026-09-08T09:00:00Z'
                completed_at = '2026-09-08T10:00:00Z'; outcome = 'findings'
            }
            evidence = @{ manifest = @{ id = 'miri-ubuntu-latest' }; replay = @{ test_filter = 'example' }; summary = 'Example' }
            confirmation = $null
        }
    }
    function Get-ReportTestManifest {
        @{
            schema_version = 1; repository = 'folo-rs/folo'; source_sha = 'a' * 40
            controller_sha = 'a' * 40; check_contract_digest = 'c' * 64; scope = 'full'
            checks = @(@{ id = 'miri-ubuntu-latest'; kind = 'miri'; platform = 'ubuntu-latest'; packages = @() })
        }
    }
    function Get-ReportTestResult($manifest, $context) {
        @{
            schema_version = 1; check_id = $manifest.checks[0].id; actual_scope = $manifest.checks[0]
            source_sha = $manifest.source_sha; controller_sha = $manifest.controller_sha
            check_contract_digest = $manifest.check_contract_digest; outcome = 'passed'
            run_id = $context.run_id; run_attempt = $context.run_attempt; run_number = $context.run_number
        }
    }
}

Describe 'Stable finding identity' {
    It 'ignores SHA clock run and line-only movement while preserving semantic identity' {
        $identity = Get-ReportTestIdentity
        $id = Get-ScheduledFindingId 'folo-rs/folo' $identity
        $identity.source_sha = 'b' * 40
        $identity.run_id = 99
        $identity.line = 123
        $identity.column = 4
        $identity.completed_at = '2026-09-09T00:00:00Z'
        $identity.path = 'src/lib.rs'
        Get-ScheduledFindingId 'folo-rs/folo' $identity | Should -BeExactly $id
        $identity.test = 'other'
        Get-ScheduledFindingId 'folo-rs/folo' $identity | Should -Not -Be $id
    }
    It 'requires complete semantic identity' {
        { Get-ScheduledFindingId 'folo-rs/folo' @{} } | Should -Throw
    }
}

Describe 'Reporter state transitions' {
    BeforeEach {
        $script:existing = Get-ReportTestRecord
        $incoming = Get-ReportTestRecord
        $incoming.observation.run_id = 11
        $incoming.observation.run_number = 11
        $incoming.observation.created_at = '2026-09-08T10:00:00Z'
        $incoming.observation.run_started_at = '2026-09-08T10:00:00Z'
        $incoming.observation.completed_at = '2026-09-08T11:00:00Z'
        $incoming.source_sha = 'b' * 40
        $script:ancestor = { param($old, $new) $old -ceq $new -or ($old -ceq ('a' * 40) -and $new -ceq ('b' * 40)) }
    }
    It 'opens failures and cannot create an incident from a pass' {
        (Merge-ScheduledObservation -Existing $null -Incoming $incoming -IsAncestor $ancestor).status | Should -Be open
        $incoming.observation.outcome = 'passed'
        Merge-ScheduledObservation -Existing $null -Incoming $incoming -IsAncestor $ancestor | Should -BeNullOrEmpty
    }
    It 'is idempotent for the same run attempt' {
        $actual = Merge-ScheduledObservation $existing $existing $ancestor
        Get-ScheduledDigest $actual | Should -BeExactly (Get-ScheduledDigest $existing)
    }
    It 'rejects delayed failures passes and older generations' {
        foreach ($outcome in @('findings', 'passed')) {
            $incoming.observation.outcome = $outcome
            $incoming.observation.run_number = 9
            $incoming.observation.run_started_at = '2026-09-08T08:00:00Z'
            (Merge-ScheduledObservation $existing $incoming $ancestor).observation.run_id | Should -Be 10
        }
        $incoming.observation.run_number = 11
        $incoming.observation.run_started_at = '2026-09-08T10:00:00Z'
        $existing.generation = 2
        (Merge-ScheduledObservation $existing $incoming $ancestor).generation | Should -Be 2
    }
    It 'orders reruns by attempt rather than completion timestamp' {
        $incoming.observation.run_id = 10
        $incoming.observation.run_number = 10
        $incoming.observation.run_attempt = 2
        (Merge-ScheduledObservation $existing $incoming $ancestor).observation.run_attempt | Should -Be 2
    }
    It 'rejects newer observations on older or unrelated source branches' {
        $incoming.source_sha = 'd' * 40
        (Merge-ScheduledObservation $existing $incoming $ancestor).source_sha | Should -BeExactly ('a' * 40)
    }
    It 'does not compare run numbers belonging to different workflows' {
        $incoming.observation.workflow_id = 200
        $incoming.observation.run_number = 1
        (Merge-ScheduledObservation $existing $incoming $ancestor).observation.workflow_id | Should -Be 200
    }
    It 'does not reopen a confirmed incident for older verification that finishes later on the same source' {
        $existing.status = 'confirmed'
        $incoming.source_sha = $existing.source_sha
        $incoming.observation.workflow_id = 200
        $incoming.observation.workflow_path = '.github/workflows/scheduled-verify.yml'
        $incoming.observation.run_number = 100
        $incoming.observation.created_at = '2026-09-08T08:00:00Z'
        $incoming.observation.run_started_at = '2026-09-08T08:00:00Z'
        $incoming.observation.completed_at = '2026-09-08T12:00:00Z'
        (Merge-ScheduledObservation $existing $incoming $ancestor).status | Should -Be confirmed
    }
    It 'does not invent cross-workflow order when creation evidence is unavailable' {
        $incoming.observation.workflow_id = 200
        $incoming.observation.Remove('created_at')
        { Merge-ScheduledObservation $existing $incoming $ancestor } | Should -Throw
    }
    It 'reopens recurrence from a newly executed attempt of an older run' {
        $existing.status = 'confirmed'
        $incoming.source_sha = $existing.source_sha
        $incoming.observation.run_id = 9
        $incoming.observation.run_number = 9
        $incoming.observation.run_attempt = 2
        $incoming.observation.created_at = '2026-09-08T08:00:00Z'
        $incoming.observation.run_started_at = '2026-09-08T11:00:00Z'
        $reopened = Merge-ScheduledObservation $existing $incoming $ancestor
        $reopened.status | Should -Be open
        $reopened.generation | Should -Be 2
    }
    It 'requires an explanation instead of closing after a single clean rerun' {
        $incoming.observation.outcome = 'passed'
        (Merge-ScheduledObservation $existing $incoming $ancestor).status | Should -Be needs-human
    }
    It 'confirms only the actual reachable squash merge commit' {
        $incoming.observation.outcome = 'passed'
        $incoming.confirmation = @{
            authoritative = $true; generation = 1; merge_commit_sha = 'b' * 40
            explained = $true; scope_complete = $true; successful = $true
        }
        $confirmed = Merge-ScheduledObservation $existing $incoming $ancestor
        $confirmed.status | Should -Be confirmed
        $confirmed.evidence.replay.test_filter | Should -Be example
        $incoming.confirmation.merge_commit_sha = 'd' * 40
        (Merge-ScheduledObservation $existing $incoming $ancestor).status | Should -Be needs-human
    }
    It 'rejects artifact assertions and incomplete or unexplained confirmations' {
        $incoming.observation.outcome = 'passed'
        foreach ($field in @('authoritative', 'explained', 'scope_complete', 'successful')) {
            $incoming.confirmation = @{
                authoritative = $true; generation = 1; merge_commit_sha = 'b' * 40
                explained = $true; scope_complete = $true; successful = $true
            }
            $incoming.confirmation[$field] = $false
            (Merge-ScheduledObservation $existing $incoming $ancestor).status | Should -Be needs-human
        }
    }
    It 'creates a new generation for recurrence after confirmed resolution' {
        $existing.status = 'confirmed'
        (Merge-ScheduledObservation $existing $incoming $ancestor).generation | Should -Be 2
    }
    It 'does not use incompatible check contracts to confirm absence' {
        $incoming.observation.outcome = 'passed'
        $incoming.check_contract_digest = 'd' * 64
        (Merge-ScheduledObservation $existing $incoming $ancestor).status | Should -Be open
    }
    It 'preserves reproduction when execution is incomplete' {
        $incoming.observation.outcome = 'incomplete'
        $incoming.evidence = @{}
        (Merge-ScheduledObservation $existing $incoming $ancestor).evidence.summary | Should -Be Example
        $existing.source_sha | Should -BeExactly ('a' * 40)
    }
    It 'retains the actual merge and attempt disposition for retries failures and unexplained passes' {
        foreach ($outcome in @('passed', 'findings', 'incomplete')) {
            $incoming.observation.outcome = $outcome
            $incoming.confirmation = @{
                authoritative = $true; generation = 1; merge_commit_sha = 'b' * 40
                explained = $false; successful = $outcome -ceq 'passed'; scope_complete = $outcome -cne 'incomplete'
                status = if ($outcome -ceq 'incomplete') { 'retry' } elseif ($outcome -ceq 'findings') { 'failed' } else { 'needs-human' }
            }
            $merged = Merge-ScheduledObservation $existing $incoming $ancestor
            $merged.confirmation.merge_commit_sha | Should -BeExactly ('b' * 40)
            $merged.confirmation.status | Should -BeExactly $incoming.confirmation.status
        }
    }
}

Describe 'Coverage receipts' {
    BeforeEach {
        $manifest = Get-ReportTestManifest
        $context = (Get-ReportTestRecord).observation
        $result = Get-ReportTestResult $manifest $context
        $ancestor = { param($old, $new) $old -ceq $new }
        $script:coverage = Merge-ScheduledCoverage -Coverage $null -Manifest $manifest -Results @($result) -Context $context -IsAncestor $ancestor
    }
    It 'records exact full successful scope and originating attempt time' {
        $coverage.receipt.complete | Should -BeTrue
        $coverage.receipt.run_attempt | Should -Be 1
        $coverage.receipt.completed_at | Should -Be $context.completed_at
        $coverage.receipt.manifest_digest | Should -BeExactly (Get-ScheduledDigest $manifest)
    }
    It 'does not refresh coverage on unchanged skips' {
        $context.run_id = 11
        $context.run_number = 11
        $context.completed_at = '2026-09-09T10:00:00Z'
        $skipped = Merge-ScheduledCoverage $coverage $manifest @() $context $ancestor -Skipped
        Get-ScheduledDigest $skipped | Should -BeExactly (Get-ScheduledDigest $coverage)
    }
    It 'invalidates success on newer missing results or a failed rerun' {
        $context.run_attempt = 2
        $invalidated = Merge-ScheduledCoverage $coverage $manifest @() $context $ancestor
        $invalidated.invalidation.run_attempt | Should -Be 2
        $invalidated.invalidation.outcome | Should -Be incomplete
        $invalidated.receipt.run_attempt | Should -Be 1
    }
    It 'never lets delayed success replace a newer failure' {
        $context.run_attempt = 2
        $invalidated = Merge-ScheduledCoverage $coverage $manifest @() $context $ancestor
        $context.run_attempt = 1
        $delayed = Merge-ScheduledCoverage $invalidated $manifest @($result) $context $ancestor
        $delayed.invalidation.run_attempt | Should -Be 2
    }
    It 'invalidates reuse after a later verification failure with a lower workflow-local run number' {
        $context.workflow_id = 200
        $context.run_id = 11
        $context.run_number = 1
        $context.workflow_path = '.github/workflows/scheduled-verify.yml'
        $context.created_at = '2026-09-08T10:00:00Z'
        $context.completed_at = '2026-09-08T11:00:00Z'
        $invalidated = Merge-ScheduledCoverage $coverage $manifest @() $context $ancestor
        $invalidated.invalidation.run_number | Should -Be 1
        $invalidated.receipt.run_number | Should -Be 10
        (Get-ScheduledRunDecision -Manifest $manifest -Coverage $invalidated `
            -Now ([datetimeoffset]'2026-09-08T12:00:00Z')).run | Should -BeTrue
        $invalidated.invalidation.created_at | Should -Be $context.created_at
        $invalidated.invalidation.workflow_path | Should -Be $context.workflow_path
    }
    It 'keeps an older verification failure historical when it finishes after newer full coverage' {
        $context.workflow_id = 200
        $context.run_id = 9
        $context.run_number = 100
        $context.created_at = '2026-09-08T08:00:00Z'
        $context.completed_at = '2026-09-08T12:00:00Z'
        $historical = Merge-ScheduledCoverage $coverage $manifest @() $context $ancestor
        $historical.invalidation | Should -BeNullOrEmpty
        $historical.receipt.run_id | Should -Be 10
    }
    It 'retains current invalidation through partial success and clears it only with newer full success' {
        $context.run_attempt = 2
        $invalidated = Merge-ScheduledCoverage $coverage $manifest @() $context $ancestor
        $context.run_attempt = 3
        $manifest.scope = 'repair'
        $partialResult = Get-ReportTestResult $manifest $context
        $partial = Merge-ScheduledCoverage $invalidated $manifest @($partialResult) $context $ancestor
        $partial.invalidation.run_attempt | Should -Be 2
        $partial.receipt.run_attempt | Should -Be 1
        $manifest.scope = 'full'
        (Get-ScheduledRunDecision -Manifest $manifest -Coverage $partial `
            -Now ([datetimeoffset]'2026-09-08T12:00:00Z')).run | Should -BeTrue
        $context.run_attempt = 4
        $fullResult = Get-ReportTestResult $manifest $context
        $restored = Merge-ScheduledCoverage $partial $manifest @($fullResult) $context $ancestor
        $restored.invalidation | Should -BeNullOrEmpty
        (Get-ScheduledRunDecision -Manifest $manifest -Coverage $restored `
            -Now ([datetimeoffset]'2026-09-08T12:00:00Z')).run | Should -BeFalse
        $context.run_attempt = 2
        (Merge-ScheduledCoverage $restored $manifest @() $context $ancestor).invalidation | Should -BeNullOrEmpty
    }
    It 'does not accept another run result or partial success as full coverage' {
        $result.run_id = 999
        (Merge-ScheduledCoverage -Coverage $null -Manifest $manifest -Results @($result) -Context $context -IsAncestor $ancestor).receipt |
            Should -BeNullOrEmpty
        $result.run_id = $context.run_id
        $manifest.scope = 'repair'
        (Merge-ScheduledCoverage -Coverage $null -Manifest $manifest -Results @($result) -Context $context -IsAncestor $ancestor).receipt |
            Should -BeNullOrEmpty
    }
}

Describe 'Reporter-owned text' {
    It 'updates one exact span without overwriting human prose or worker state' {
        $record = Get-ReportTestRecord
        $worker = Write-ScheduledRecord -Record @{ schema_version = 1; note = 'keep' } -Kind worker
        $text = "Human before`n$(Write-ScheduledRecord -Record $record -Kind reporter)`n$worker`nHuman after"
        $record.status = 'needs-human'
        $updated = ConvertTo-ScheduledOwnedText -Text $text -Record $record -Kind reporter
        $updated | Should -BeExactly "Human before`n$(Write-ScheduledRecord -Record $record -Kind reporter)`n$worker`nHuman after"
    }
    It 'refuses ambiguous or missing owned blocks' {
        $record = Get-ReportTestRecord
        $marker = Write-ScheduledRecord -Record $record -Kind reporter
        { ConvertTo-ScheduledOwnedText "$marker`n$marker" $record reporter } | Should -Throw
        { ConvertTo-ScheduledOwnedText 'Human body' $record reporter } | Should -Throw
    }
    It 'retains minimal reproduction without executable evidence markup' {
        $record = Get-ReportTestRecord
        $record.evidence.summary = '</pre><script>danger</script>'
        $body = Get-ScheduledFindingBody $record
        $body.StartsWith('[Copilot speaking]') | Should -BeTrue
        $body.Contains('<script>') | Should -BeFalse
        (Read-ScheduledRecord $body reporter).evidence.replay.test_filter | Should -Be example
    }
}

Describe 'Deterministic health' {
    BeforeEach {
        $script:now = [datetimeoffset]'2026-09-08T12:00:00Z'
        $script:fresh = @{ outcome = 'passed'; completed_at = '2026-09-08T10:00:00Z' }
        $manifest = Get-ReportTestManifest
        $context = (Get-ReportTestRecord).observation
        $result = Get-ReportTestResult $manifest $context
        $script:coverage = Merge-ScheduledCoverage -Coverage $null -Manifest $manifest -Results @($result) -Context $context `
            -IsAncestor { param($old, $new) $old -ceq $new }
    }
    It 'reports independent fresh components' {
        $health = Get-ScheduledHealth -Scheduler @{ state = 'active' } -Coverage $coverage -Manifest $manifest `
            -Planning $fresh -Reporting $fresh -LocalScan $fresh -Now $now
        $health.healthy | Should -BeTrue
        $health.components.coverage.status | Should -Be fresh
    }
    It 'distinguishes reused coverage from fresh execution' {
        $planning = @{ outcome = 'not-run-unchanged'; completed_at = $fresh.completed_at }
        $health = Get-ScheduledHealth -Coverage $coverage -Manifest $manifest -Planning $planning -Now $now
        $health.components.coverage.status | Should -Be reused
        $health.components.local_scan.status | Should -Be unavailable
        $health.healthy | Should -BeFalse
    }
    It 'uses independent freshness thresholds for hosted planning and local scans' {
        $observation = @{ outcome = 'passed'; completed_at = '2026-09-08T02:00:00Z' }
        $health = Get-ScheduledHealth -Planning $observation -Reporting $observation -LocalScan $observation `
            -Now $now -ExpectedPlanGapHours 30 -ExpectedLocalGapMinutes 420
        $health.components.planning.status | Should -Be fresh
        $health.components.reporting.status | Should -Be fresh
        $health.components.local_scan.status | Should -Be unavailable
        $health = Get-ScheduledHealth -Planning $observation -LocalScan $observation `
            -Now $now -ExpectedPlanGapHours 8 -ExpectedLocalGapMinutes 720
        $health.components.planning.status | Should -Be unavailable
        $health.components.local_scan.status | Should -Be fresh
    }
    It 'does not treat unavailable malformed stale or failed observations as healthy' {
        foreach ($observation in @(@{}, @{ outcome = 'passed'; completed_at = 'invalid' },
                @{ outcome = 'passed'; completed_at = '2026-08-01T00:00:00Z' },
                @{ outcome = 'failed'; completed_at = '2026-09-08T11:00:00Z' })) {
            $health = Get-ScheduledHealth -Planning $observation -Now $now
            $health.components.planning.status | Should -BeIn @('failed', 'unavailable')
        }
    }
    It 'separates scheduler inactivity from a complete coverage receipt' {
        $health = Get-ScheduledHealth -Scheduler @{ state = 'disabled_inactivity' } -Coverage $coverage -Manifest $manifest -Now $now
        $health.components.scheduler.status | Should -Be failed
        $health.components.coverage.status | Should -Be fresh
    }
    It 'identifies deliberate staging without declaring missing evidence healthy or hiding failures' {
        $health = Get-ScheduledHealth -Scheduler @{ state = 'active' } -Now $now -Staged
        $health.status | Should -Be staged
        $health.healthy | Should -BeFalse
        $health.components.coverage.status | Should -Be unavailable
        $health = Get-ScheduledHealth -Scheduler @{ state = 'disabled_inactivity' } -Now $now -Staged
        $health.status | Should -Be failed
    }
    It 'accepts JSON-deserialized DateTime observations without culture-sensitive string conversion' {
        $observation = '{"outcome":"passed","completed_at":"2026-09-08T10:00:00Z"}' | ConvertFrom-Json -AsHashtable
        $health = Get-ScheduledHealth -Planning $observation -LocalScan $observation -Now $now
        $health.components.planning.status | Should -Be fresh
        $health.components.local_scan.status | Should -Be fresh
    }
}
