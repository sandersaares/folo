#requires -Version 7

# The read-only deep-check job invokes tools in the candidate checkout from this controller.
# Exit codes, incomplete mutation execution and baseline failures fail the job; readable summaries
# and ordinary tool files survive for the separate reporter, including when preparation fails.
# Ref: .github/workflows/implementation.md#immutable-execution.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
Import-Module (Join-Path $PSScriptRoot 'ScheduledExecutionCommands.psm1')
Import-Module (Join-Path $PSScriptRoot 'ScheduledExecutionDiagnostics.psm1')
Import-Module (Join-Path $PSScriptRoot '..\build\CargoExecutable.psm1')

function Invoke-ScheduledCheck {
    [CmdletBinding()]
    [OutputType([int])]
    param(
        [Parameter(Mandatory)][hashtable] $Check,
        [Parameter(Mandatory)][string] $SourceRoot,
        [Parameter(Mandatory)][string] $OutputDirectory,
        [Parameter(Mandatory)][string] $SourceSha
    )

    $null = New-Item -ItemType Directory -Path $OutputDirectory -Force
    if (@(Get-ChildItem -LiteralPath $OutputDirectory -Force).Count -ne 0) {
        throw 'Check output must be an empty directory so a rerun cannot reuse stale results.'
    }
    $summary = Join-Path $OutputDirectory 'summary.md'
    $packages = if ($Check.packages.Count -eq 0) { 'workspace' } else { $Check.packages -join ', ' }
    @(
        "# Deep check: $($Check.id)"
        ''
        "Tested commit: ``$SourceSha``"
        "Check: $($Check.kind)"
        "Platform: $($Check.platform)"
        "Shard: $($Check.shard)"
        "Packages: $packages"
        "Miri seed range: $($Check.seed_range)"
        ''
        'Execution started. An absent final result means execution was interrupted.'
    ) | Set-Content -LiteralPath $summary
    $exitCode = 0
    try {
        if ($SourceSha -cnotmatch '^[0-9a-f]{40}$') { throw 'Expected a full tested commit SHA.' }
        Assert-ScheduledPlatform -Runner $Check.platform
        $toolchain = Get-ScheduledToolchain -Kind $Check.kind
        Add-Content -LiteralPath $summary -Value "`nToolchain: ``$toolchain``"
        $command = Get-ScheduledCommand -Check $Check -OutputDirectory $OutputDirectory -Toolchain $toolchain
        if ($Check.kind -in @('mutants', 'careful')) {
            # These binaries are test scaffolding excluded from mutation. Build outside each
            # mutant's fresh target tree to avoid per-mutant nested compilation timeouts.
            foreach ($helper in @(
                    @{ package = 'cargo-bench-history-faker'; variable = 'CBH_FAKER' },
                    @{ package = 'dure-test-helper'; variable = 'DURE_TEST_HELPER' })) {
                if ($helper.variable -eq 'DURE_TEST_HELPER' -and -not $IsWindows) { continue }
                $setup = @{
                    file = $command.file
                    arguments = @("+$toolchain", 'build', "--package=$($helper.package)", '--target-dir',
                        (Join-Path $SourceRoot "target\$($helper.package)"),
                        '--message-format=json-render-diagnostics', '--locked')
                    environment = @{ RUSTUP_TOOLCHAIN = $toolchain; RUSTFLAGS = ''; CARGO_ENCODED_RUSTFLAGS = $null }
                }
                $step = Invoke-ScheduledStage -Command $setup -SourceRoot $SourceRoot `
                    -OutputDirectory $OutputDirectory -Name $helper.package
                if ($step.exit_code -ne 0) { throw "Test-helper setup failed: $($helper.package)." }
                $executable = Resolve-CargoExecutable -CargoMessage @(Get-Content -LiteralPath $step.stdout_path) `
                    -TargetName $helper.package
                if (-not [IO.Path]::IsPathFullyQualified($executable) -or -not (Test-Path -LiteralPath $executable)) {
                    throw 'Cargo did not produce an available test-helper executable.'
                }
                $command.environment[$helper.variable] = $executable
            }
        }
        if ($Check.kind -eq 'mutants') {
            $step = Invoke-ScheduledStage -Command $command -SourceRoot $SourceRoot `
                -OutputDirectory $OutputDirectory -Name 'check'
            $exitCode = $step.exit_code
            $inventoryPath = Join-Path $OutputDirectory 'mutants.out\mutants.json'
            $outcomesPath = Join-Path $OutputDirectory 'mutants.out\outcomes.json'
            if (-not (Test-Path -LiteralPath $inventoryPath)) { throw 'Mutation inventory is missing.' }
            $inventory = Get-Content -LiteralPath $inventoryPath -Raw | ConvertFrom-Json -AsHashtable -NoEnumerate
            if ($inventory -isnot [array]) { throw 'Mutation inventory is not an array.' }
            if ($exitCode -eq 0 -and $inventory.Count -eq 0 -and -not (Test-Path -LiteralPath $outcomesPath)) {
                # An empty shard has no mutation work. Leave configuration and baseline behavior
                # with cargo-mutants instead of reimplementing its test runner.
                # Ref: .github/workflows/implementation.md#immutable-execution.
                Add-Content -LiteralPath $summary -Value "`nNo mutants selected for this shard; no mutation tests or baseline were run."
            } else {
                $mutationExit = Write-ScheduledMutationSummary -OutputDirectory $OutputDirectory -Inventory $inventory
                if ($mutationExit -ne 0) { $exitCode = 1 }
            }
        } else {
            $metadataCommand = @{
                file = $command.file
                arguments = @("+$toolchain", 'metadata', '--format-version=1', '--no-deps', '--locked')
                environment = @{ RUSTUP_TOOLCHAIN = $toolchain }
            }
            $step = Invoke-ScheduledStage -Command $metadataCommand -SourceRoot $SourceRoot `
                -OutputDirectory $OutputDirectory -Name 'metadata'
            if ($step.exit_code -ne 0) { throw 'Cargo metadata setup failed; no checker targets ran.' }
            $metadata = Get-Content -LiteralPath $step.stdout_path -Raw | ConvertFrom-Json -AsHashtable
            if ($Check.kind -in @('miri', 'miri-many')) {
                Add-Content -LiteralPath $summary -Value "`nMiri does not execute proc-macro unit harnesses. Enabled integration-test targets remain in scope."
            }
            $scopes = @(Get-ScheduledTestScope -Check $Check -Metadata $metadata)
            if ($scopes.Count -eq 0) { throw 'No supported test targets in the selected scope.' }
            for ($index = 0; $index -lt $scopes.Count; $index++) {
                $scope = $Check.Clone()
                $scope.packages = @($scopes[$index].package)
                $scope.target = $scopes[$index].target
                $packageCommand = Get-ScheduledCommand -Check $scope -OutputDirectory $OutputDirectory -Toolchain $toolchain
                foreach ($key in @('CBH_FAKER', 'DURE_TEST_HELPER')) {
                    if ($command.environment.ContainsKey($key)) { $packageCommand.environment[$key] = $command.environment[$key] }
                }
                $step = Invoke-ScheduledStage -Command $packageCommand -SourceRoot $SourceRoot `
                    -OutputDirectory $OutputDirectory -Name "check-$index"
                # Do not overwrite an earlier failure when a later package or target succeeds.
                if ($step.exit_code -ne 0) { $exitCode = $step.exit_code }
            }
        }
    } catch {
        $exitCode = 1
        Add-Content -LiteralPath $summary -Value "`n## Execution failure`n`n$($_.Exception.Message)"
        $_ | Out-String | Set-Content -LiteralPath (Join-Path $OutputDirectory 'execution-error.txt')
    } finally {
        $conclusion = if ($exitCode -eq 0) { 'PASSED' } else { 'FAILED' }
        Add-Content -LiteralPath $summary -Value "`n## Final result: $conclusion`n`nExit code: $exitCode"
        if ($env:GITHUB_STEP_SUMMARY) {
            Get-Content -LiteralPath $summary | Add-Content -LiteralPath $env:GITHUB_STEP_SUMMARY
        }
        Write-Host "$($Check.id): $conclusion. Full diagnostics: $OutputDirectory"
    }
    return $exitCode
}

function Invoke-ScheduledStage {
    # Append the command before starting it so interrupted jobs retain reproduction inputs.
    [CmdletBinding()]
    [OutputType([hashtable])]
    param(
        [Parameter(Mandatory)][hashtable] $Command,
        [Parameter(Mandatory)][string] $SourceRoot,
        [Parameter(Mandatory)][string] $OutputDirectory,
        [Parameter(Mandatory)][string] $Name
    )

    $summary = Join-Path $OutputDirectory 'summary.md'
    $beforeStage = Get-Content -LiteralPath $summary -Raw
    $arguments = @($Command.arguments | ForEach-Object { "'" + $_.Replace("'", "''") + "'" }) -join ' '
    $description = @(
        "`n## $Name"
        ''
        '```powershell'
        foreach ($key in @($Command.environment.Keys | Sort-Object)) {
            $value = $Command.environment[$key]
            if ($null -eq $value) { "`$env:$key = `$null" }
            else { "`$env:$key = '" + $value.Replace("'", "''") + "'" }
        }
        "cargo $arguments"
        '```'
        ''
    )
    $description | Add-Content -LiteralPath (Join-Path $OutputDirectory 'commands.txt')
    $description | Add-Content -LiteralPath $summary
    Write-Host "Starting $Name in $SourceRoot"
    $result = Invoke-ScheduledProcess -Command $Command -SourceRoot $SourceRoot `
        -OutputDirectory $OutputDirectory -Name $Name
    Add-Content -LiteralPath $summary -Value "Exit code: $($result.exit_code)."
    if ($result.exit_code -ne 0) {
        Write-ScheduledLogExcerpt -Path $result.stdout_path, $result.stderr_path -SummaryPath $summary
    } else {
        # Keep successful-command inventories out of a failure report. During execution the
        # current command remains visible in case the runner interrupts this process.
        Set-Content -LiteralPath $summary -Value $beforeStage -NoNewline
    }
    return $result
}

Export-ModuleMember -Function Invoke-ScheduledCheck
