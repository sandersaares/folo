# Ref: .github/workflows/implementation.md#scheduled-controller-ownership.
# Ref: .github/workflows/implementation.md#immutable-execution.
# Execute immutable check scope and preserve evidence without authorizing Local App repairs.
[group('scheduled')]
[script]
scheduled-check:
    Set-StrictMode -Version Latest
    $ErrorActionPreference = 'Stop'
    $PSNativeCommandUseErrorActionPreference = $true
    ./scripts/scheduled/Invoke-ScheduledCheck.ps1

# Ref: .github/workflows/implementation.md#complete-evidence-and-reuse.
# Fix expected scope before execution without renewing reused evidence.
[group('scheduled')]
[script]
scheduled-plan:
    Set-StrictMode -Version Latest
    $ErrorActionPreference = 'Stop'
    $PSNativeCommandUseErrorActionPreference = $true
    ./scripts/scheduled/Invoke-ScheduledPlan.ps1

# Ref: .github/workflows/implementation.md#serialized-reporting.
# Evaluate downloaded evidence as data; write authority belongs to the hosted caller.
[group('scheduled')]
[script]
scheduled-report:
    Set-StrictMode -Version Latest
    $ErrorActionPreference = 'Stop'
    $PSNativeCommandUseErrorActionPreference = $true
    ./scripts/scheduled/Invoke-ScheduledReport.ps1

# Ref: docs/scheduled-validation.md#durable-ownership-and-native-calls.
# Persist short transactions around native App calls without process-held claims.
[group('scheduled')]
[script]
scheduled-local REQUEST_PATH:
    Set-StrictMode -Version Latest
    $ErrorActionPreference = 'Stop'
    $PSNativeCommandUseErrorActionPreference = $true
    Import-Module ./scripts/scheduled/LocalState.psm1 -Force
    Invoke-ScheduledLocalRequest -RequestPath '{{ REQUEST_PATH }}'

# Ref: docs/scheduled-validation.md#pr-readiness-and-bounded-continuation.
# Scan the complete backlog and existing ownership before admitting bounded work.
[group('scheduled')]
[script]
scheduled-inbox EXECUTOR_ID:
    Set-StrictMode -Version Latest
    $ErrorActionPreference = 'Stop'
    $PSNativeCommandUseErrorActionPreference = $true
    Import-Module ./scripts/scheduled/LocalInbox.psm1 -Force
    Invoke-ScheduledInbox -PolicyPath ./scripts/scheduled/policy.json -ExecutorId '{{ EXECUTOR_ID }}' | ConvertTo-Json -Depth 40
