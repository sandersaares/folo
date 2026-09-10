#requires -Version 7
# Binds concrete native automation/prompt observations to a scan or accepted dispatch.
# The App skill supplies observed facts; these helpers never query App storage or invent them.
# Ref: ../../docs/scheduled-triage.md#independent-configuration.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
Import-Module (Join-Path $PSScriptRoot 'ScheduledContracts.psm1')

function Get-ScheduledTriageProfileObservation {
    param([AllowNull()][hashtable] $NativeProfile, [ValidateSet('scan', 'dispatch')][string] $Kind,
        [string] $Token, [string] $SessionId)
    if ($null -eq $NativeProfile) { return $null }
    if ($NativeProfile.Count -ne 2 -or -not $NativeProfile.ContainsKey('automation_id') -or
        -not $NativeProfile.ContainsKey('prompt_digest') -or $NativeProfile.automation_id -isnot [string] -or
        [string]::IsNullOrWhiteSpace($NativeProfile.automation_id) -or $NativeProfile.prompt_digest -cnotmatch '^[0-9a-f]{64}$' -or
        [string]::IsNullOrWhiteSpace($Token) -or [string]::IsNullOrWhiteSpace($SessionId)) {
        throw [FormatException]::new('Native profile observation needs concrete automation and prompt identities.')
    }
    $observation = @{
        schema_version = 1; kind = $Kind; token = $Token; session_id = $SessionId
        automation_id = $NativeProfile.automation_id; prompt_digest = $NativeProfile.prompt_digest
    }
    $observation.digest = Get-ScheduledDigest $observation
    return $observation
}

function Assert-ScheduledTriageProfileObservation {
    param([AllowNull()][hashtable] $Observation, [string] $Kind, [string] $Token, [string] $SessionId)
    if ($null -eq $Observation) { return }
    foreach ($field in @('schema_version', 'kind', 'token', 'session_id', 'automation_id', 'prompt_digest', 'digest')) {
        if (-not $Observation.ContainsKey($field)) { throw [FormatException]::new("Native observation is missing $field.") }
    }
    $payload = @{
        schema_version = $Observation.schema_version; kind = $Observation.kind; token = $Observation.token
        session_id = $Observation.session_id; automation_id = $Observation.automation_id; prompt_digest = $Observation.prompt_digest
    }
    if ($Observation.Count -ne 7 -or $Observation.schema_version -ne 1 -or $Observation.kind -cne $Kind -or
        $Kind -cnotin @('scan', 'dispatch') -or [string]::IsNullOrWhiteSpace($Token) -or
        [string]::IsNullOrWhiteSpace($SessionId) -or $Observation.token -cne $Token -or
        $Observation.session_id -cne $SessionId -or [string]::IsNullOrWhiteSpace($Observation.automation_id) -or
        $Observation.prompt_digest -cnotmatch '^[0-9a-f]{64}$' -or
        $Observation.digest -cne (Get-ScheduledDigest $payload)) {
        throw [FormatException]::new('Native profile observation is stale, foreign, or changed.')
    }
}

function Test-ScheduledTriageProfileObservation {
    param([AllowNull()][hashtable] $RegisteredProfile, [AllowNull()][hashtable] $Observation,
        [string] $Kind, [string] $Token, [string] $SessionId)
    Assert-ScheduledTriageProfileObservation $Observation $Kind $Token $SessionId
    return $null -ne $RegisteredProfile -and $null -ne $Observation -and
        $RegisteredProfile.ContainsKey('automation_id') -and $RegisteredProfile.ContainsKey('prompt_digest') -and
        $RegisteredProfile.prompt_digest -cmatch '^[0-9a-f]{64}$' -and
        $RegisteredProfile.automation_id -ceq $Observation.automation_id -and
        $RegisteredProfile.prompt_digest -ceq $Observation.prompt_digest
}

function Get-ScheduledTriageProfileBindingDigest {
    param([string] $Kind, [string] $Token, [string] $SessionId)
    return Get-ScheduledDigest @{ kind = $Kind; token = $Token; session_id = $SessionId }
}

function Get-ScheduledTriageHealthObservation {
    param([AllowNull()][hashtable] $Observation)
    if ($null -eq $Observation) { return $null }
    Assert-ScheduledTriageProfileObservation $Observation scan $Observation.token $Observation.session_id
    # Published health needs an ownership fingerprint, not the local authorization token.
    $public = @{
        schema_version = 1; kind = 'scan'; session_id = $Observation.session_id
        binding_digest = Get-ScheduledTriageProfileBindingDigest scan $Observation.token $Observation.session_id
        automation_id = $Observation.automation_id; prompt_digest = $Observation.prompt_digest
    }
    $public.digest = Get-ScheduledDigest $public
    return $public
}

function Test-ScheduledTriageHealthObservation {
    param([AllowNull()][hashtable] $RegisteredProfile, [AllowNull()][hashtable] $Observation,
        [AllowNull()][hashtable] $Scan)
    if ($null -eq $Observation -or $null -eq $Scan -or $null -eq $RegisteredProfile) { return $false }
    foreach ($field in @('schema_version', 'kind', 'session_id', 'binding_digest', 'automation_id', 'prompt_digest', 'digest')) {
        if (-not $Observation.ContainsKey($field)) { throw [FormatException]::new("Published native observation is missing $field.") }
    }
    $payload = @{
        schema_version = $Observation.schema_version; kind = $Observation.kind; session_id = $Observation.session_id
        binding_digest = $Observation.binding_digest; automation_id = $Observation.automation_id; prompt_digest = $Observation.prompt_digest
    }
    if ($Observation.Count -ne 7 -or $Observation.schema_version -ne 1 -or $Observation.kind -cne 'scan' -or
        $Observation.binding_digest -cnotmatch '^[0-9a-f]{64}$' -or
        $Observation.binding_digest -cne $Scan['binding_digest'] -or $Observation.session_id -cne $Scan['session_id'] -or
        [string]::IsNullOrWhiteSpace($Observation.session_id) -or $Observation.prompt_digest -cnotmatch '^[0-9a-f]{64}$' -or
        $Observation.digest -cne (Get-ScheduledDigest $payload)) {
        throw [FormatException]::new('Published native profile observation does not belong to this scan.')
    }
    return $RegisteredProfile['automation_id'] -ceq $Observation.automation_id -and
        $RegisteredProfile['prompt_digest'] -ceq $Observation.prompt_digest
}

Export-ModuleMember -Function Get-ScheduledTriageProfileObservation, Assert-ScheduledTriageProfileObservation,
Test-ScheduledTriageProfileObservation, Get-ScheduledTriageProfileBindingDigest,
Get-ScheduledTriageHealthObservation, Test-ScheduledTriageHealthObservation
