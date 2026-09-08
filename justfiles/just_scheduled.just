# Deterministic scheduled execution is separate from the personal Local App worker.
[group('scheduled')]
[script]
scheduled-check:
    Set-StrictMode -Version Latest
    $ErrorActionPreference = 'Stop'
    $PSNativeCommandUseErrorActionPreference = $true
    ./scripts/scheduled/Invoke-ScheduledCheck.ps1

[group('scheduled')]
[script]
scheduled-plan:
    Set-StrictMode -Version Latest
    $ErrorActionPreference = 'Stop'
    $PSNativeCommandUseErrorActionPreference = $true
    ./scripts/scheduled/Invoke-ScheduledPlan.ps1

[group('scheduled')]
[script]
scheduled-report:
    Set-StrictMode -Version Latest
    $ErrorActionPreference = 'Stop'
    $PSNativeCommandUseErrorActionPreference = $true
    ./scripts/scheduled/Invoke-ScheduledReport.ps1

# Local deep checks remain explicit after the reviewed rollout switches routine validation.
[group('quality')]
validate-deep:
    just package="{{ package }}" miri
    just package="{{ package }}" mutants
    just package="{{ package }}" miri-harder
    just package="{{ package }}" careful

[group('scheduled')]
[script]
_scheduled-routine-deep KIND:
    Set-StrictMode -Version Latest
    $ErrorActionPreference = 'Stop'
    $PSNativeCommandUseErrorActionPreference = $true
    Import-Module ./scripts/scheduled/ScheduledWorkflow.psm1 -Force
    Invoke-ScheduledLocalDeepCheck -Kind '{{ KIND }}' -Packages '{{ package }}'

[group('scheduled')]
[script]
scheduled-local REQUEST_PATH:
    Set-StrictMode -Version Latest
    $ErrorActionPreference = 'Stop'
    $PSNativeCommandUseErrorActionPreference = $true
    Import-Module ./scripts/scheduled/LocalState.psm1 -Force
    Invoke-ScheduledLocalRequest -RequestPath '{{ REQUEST_PATH }}'

[group('scheduled')]
[script]
scheduled-inbox EXECUTOR_ID:
    Set-StrictMode -Version Latest
    $ErrorActionPreference = 'Stop'
    $PSNativeCommandUseErrorActionPreference = $true
    Import-Module ./scripts/scheduled/LocalInbox.psm1 -Force
    Invoke-ScheduledInbox -PolicyPath ./scripts/scheduled/policy.json -ExecutorId '{{ EXECUTOR_ID }}' | ConvertTo-Json -Depth 40
