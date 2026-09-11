#requires -Version 7
# Binds concrete native automation/prompt observations to a scan or accepted dispatch.
# The App skill supplies observed facts; these helpers never query App storage or invent them.
# Ref: ../../docs/scheduled-triage.md#independent-configuration.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
Import-Module (Join-Path $PSScriptRoot 'ScheduledContracts.psm1')

function Assert-TriageProfileSchema {
    param([hashtable] $Record, [string[]] $Fields)
    # Validate representations before equality or hashing: a self-consistent digest is not
    # schema proof, and PowerShell comparisons otherwise coerce numbers, strings and arrays.
    if ($Record.Count -ne $Fields.Count) { throw [FormatException]::new('Unexpected profile observation fields.') }
    foreach ($field in $Fields) {
        $value = $Record[$field]
        if ($field -ceq 'schema_version') {
            if (($value -isnot [int] -and $value -isnot [long]) -or $value -ne 1) {
                throw [FormatException]::new('Unsupported profile observation schema.')
            }
        } elseif ($value -isnot [string] -or [string]::IsNullOrWhiteSpace($value)) {
            throw [FormatException]::new("Profile observation needs a nonempty string for $field.")
        } elseif (($field -ceq 'digest' -or $field.EndsWith('_digest', [StringComparison]::Ordinal)) -and
            $value -cnotmatch '^[0-9a-f]{64}$') {
            throw [FormatException]::new("Profile observation has a malformed $field.")
        }
    }
}

function Get-ScheduledTriageProfileObservation {
    param([AllowNull()][hashtable] $NativeProfile, [ValidateSet('scan', 'dispatch')][string] $Kind,
        $Token, $SessionId)
    if ($null -eq $NativeProfile) { return $null }
    Assert-TriageProfileSchema $NativeProfile @('automation_id', 'prompt_digest')
    if ($Token -isnot [string] -or $SessionId -isnot [string] -or
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
    param([AllowNull()][hashtable] $Observation, [string] $Kind, $Token, $SessionId)
    if ($null -eq $Observation) { return }
    Assert-TriageProfileSchema $Observation @('schema_version', 'kind', 'token', 'session_id', 'automation_id', 'prompt_digest', 'digest')
    $payload = @{
        schema_version = $Observation.schema_version; kind = $Observation.kind; token = $Observation.token
        session_id = $Observation.session_id; automation_id = $Observation.automation_id; prompt_digest = $Observation.prompt_digest
    }
    if ($Observation.kind -cne $Kind -or $Token -isnot [string] -or $SessionId -isnot [string] -or
        $Kind -cnotin @('scan', 'dispatch') -or [string]::IsNullOrWhiteSpace($Token) -or
        [string]::IsNullOrWhiteSpace($SessionId) -or $Observation.token -cne $Token -or
        $Observation.session_id -cne $SessionId -or
        $Observation.digest -cne (Get-ScheduledDigest $payload)) {
        throw [FormatException]::new('Native profile observation is stale, foreign, or changed.')
    }
}

function Test-ScheduledTriageProfileObservation {
    param([AllowNull()][hashtable] $RegisteredProfile, [AllowNull()][hashtable] $Observation,
        [string] $Kind, $Token, $SessionId)
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
    Assert-ScheduledTriageProfileObservation $Observation scan $Observation['token'] $Observation['session_id']
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
    # Every present object must be well formed even when another observation is unavailable.
    if ($null -ne $Scan) {
        Assert-TriageProfileSchema $Scan @('binding_digest', 'session_id')
    }
    if ($null -ne $Observation) {
        Assert-TriageProfileSchema $Observation @('schema_version', 'kind', 'session_id', 'binding_digest', 'automation_id', 'prompt_digest', 'digest')
        $payload = @{
            schema_version = $Observation.schema_version; kind = $Observation.kind; session_id = $Observation.session_id
            binding_digest = $Observation.binding_digest; automation_id = $Observation.automation_id; prompt_digest = $Observation.prompt_digest
        }
        if ($Observation.kind -cne 'scan' -or $Observation.digest -cne (Get-ScheduledDigest $payload)) {
            throw [FormatException]::new('Invalid published native profile observation.')
        }
    }
    if ($null -eq $Observation -or $null -eq $Scan -or $null -eq $RegisteredProfile) { return $false }
    if ($Observation.binding_digest -cne $Scan['binding_digest'] -or $Observation.session_id -cne $Scan['session_id']) {
        throw [FormatException]::new('Published native profile observation does not belong to this scan.')
    }
    return $RegisteredProfile['automation_id'] -ceq $Observation.automation_id -and
        $RegisteredProfile['prompt_digest'] -ceq $Observation.prompt_digest
}

Export-ModuleMember -Function Get-ScheduledTriageProfileObservation, Assert-ScheduledTriageProfileObservation,
Test-ScheduledTriageProfileObservation, Get-ScheduledTriageProfileBindingDigest,
Get-ScheduledTriageHealthObservation, Test-ScheduledTriageHealthObservation
