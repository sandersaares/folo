# Run candidate checks with read-only authority; retain readable diagnostics on failure.
# Ref: .github/workflows/implementation.md#immutable-execution.
[group('scheduled')]
[script]
scheduled-check:
    Set-StrictMode -Version Latest
    $ErrorActionPreference = 'Stop'
    $PSNativeCommandUseErrorActionPreference = $true
    # A child script's exit does not terminate this Just script; use a native process boundary.
    pwsh -NoLogo -NoProfile -NonInteractive -File ./scripts/scheduled/Invoke-ScheduledCheck.ps1
    exit $LASTEXITCODE

# Plan fresh full execution without building Rust or consulting previous runs.
# Ref: .github/workflows/implementation.md#immutable-execution.
[group('scheduled')]
[script]
scheduled-plan:
    Set-StrictMode -Version Latest
    $ErrorActionPreference = 'Stop'
    $PSNativeCommandUseErrorActionPreference = $true
    pwsh -NoLogo -NoProfile -NonInteractive -File ./scripts/scheduled/Invoke-ScheduledPlan.ps1
    exit $LASTEXITCODE

# Read diagnostics as data; only the separate trusted reporter has issue-write authority.
# Ref: .github/workflows/implementation.md#failure-reporting.
[group('scheduled')]
[script]
scheduled-report:
    Set-StrictMode -Version Latest
    $ErrorActionPreference = 'Stop'
    $PSNativeCommandUseErrorActionPreference = $true
    pwsh -NoLogo -NoProfile -NonInteractive -File ./scripts/scheduled/Invoke-ScheduledReport.ps1
    exit $LASTEXITCODE
