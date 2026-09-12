#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }

# Drives the capture wrapper through real Just, with harmless stand-in recipes. Checker
# behavior belongs to the shared recipes; these tests protect routing, logs and exit status.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true

BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'ScheduledPlan.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'ScheduledExecution.psm1') -Force
    Import-Module (Join-Path $PSScriptRoot 'ScheduledExecutionDiagnostics.psm1') -Force
    $script:fixtures = Join-Path $PSScriptRoot 'fixtures\execution'
    $script:sourceSha = 'a' * 40
    $script:recipeRoot = Join-Path $TestDrive 'recipes'
    $null = New-Item -ItemType Directory -Path $recipeRoot
    Copy-Item -LiteralPath (Join-Path $fixtures 'justfile'), (Join-Path $fixtures 'Record-Recipe.ps1') -Destination $recipeRoot
}

Describe 'Shared recipe invocation' {
    BeforeEach {
        $script:output = Join-Path $TestDrive ("output space's " + [guid]::NewGuid())
        $script:environment = @{}
        foreach ($name in @('SCHEDULED_CAPTURE_PATH', 'SCHEDULED_TEST_EXIT', 'SCHEDULED_MUTATION_FIXTURE')) {
            $environment[$name] = [Environment]::GetEnvironmentVariable($name)
        }
        $env:SCHEDULED_CAPTURE_PATH = Join-Path $TestDrive 'invocation.json'
        $env:SCHEDULED_TEST_EXIT = '0'
        $env:SCHEDULED_MUTATION_FIXTURE = $null
    }

    AfterEach {
        foreach ($name in $environment.Keys) {
            [Environment]::SetEnvironmentVariable($name, $environment[$name])
        }
    }

    It 'runs the existing <recipe> recipe with the matrix scope' -ForEach @(
        @{ id = 'miri-ubuntu-latest'; recipe = 'miri'; packages = @('example', 'another'); shard = '' },
        @{ id = 'miri-many-events-2'; recipe = 'miri-harder'; packages = @('events'); shard = '2/2' },
        @{ id = 'mutants-windows-latest-2'; recipe = 'mutants'; packages = @('example'); shard = '2/8' },
        @{ id = 'careful-windows-latest'; recipe = 'careful'; packages = @(); shard = '' }
    ) {
        $check = @(Get-ScheduledCheck | Where-Object id -EQ $id)[0]
        $check.packages = $packages
        Invoke-ScheduledCheck -Check $check -SourceRoot $recipeRoot -OutputDirectory $output -SourceSha $sourceSha |
            Should -Be 0
        $invocation = Get-Content -LiteralPath $env:SCHEDULED_CAPTURE_PATH -Raw | ConvertFrom-Json
        $invocation.recipe | Should -Be $recipe
        $invocation.package | Should -Be ($packages -join ' ')
        if ($shard) { $invocation.shard | Should -Be $shard }
        if ($recipe -eq 'mutants') {
            $invocation.careful | Should -Be 'false'
            $invocation.output | Should -Be $output
        }
        $summary = Get-Content -LiteralPath (Join-Path $output 'summary.md') -Raw
        $summary | Should -Match "just .*'$recipe'"
        $summary | Should -Match $sourceSha
        $summary | Should -Match 'Final result: PASSED'
    }

    It 'preserves recipe failure <_> and includes its output' -ForEach @(1, 2, 3, 4) {
        $code = $_
        $env:SCHEDULED_TEST_EXIT = [string]$code
        $check = @(Get-ScheduledCheck | Where-Object recipe -EQ 'miri')[0]
        Invoke-ScheduledCheck -Check $check -SourceRoot $recipeRoot -OutputDirectory $output -SourceSha $sourceSha |
            Should -Be $code
        $summary = Get-Content -LiteralPath (Join-Path $output 'summary.md') -Raw
        $summary | Should -Match 'Recipe failure canary'
        $summary | Should -Match 'Final result: FAILED'
    }

    It 'reports an empty mutation shard without reconstructing a baseline' {
        $check = @(Get-ScheduledCheck | Where-Object recipe -EQ 'mutants')[0]
        Invoke-ScheduledCheck -Check $check -SourceRoot $recipeRoot -OutputDirectory $output -SourceSha $sourceSha |
            Should -Be 0
        Get-Content -LiteralPath (Join-Path $output 'summary.md') -Raw |
            Should -Match 'No mutants selected for this shard; no mutation tests or baseline were run'
    }

    It 'preserves a failed mutation recipe and renders its native findings' {
        $env:SCHEDULED_TEST_EXIT = '3'
        $env:SCHEDULED_MUTATION_FIXTURE = Join-Path $fixtures 'outcomes.json'
        $check = @(Get-ScheduledCheck | Where-Object recipe -EQ 'mutants')[0]
        Invoke-ScheduledCheck -Check $check -SourceRoot $recipeRoot -OutputDirectory $output -SourceSha $sourceSha |
            Should -Be 3
        $summary = Get-Content -LiteralPath (Join-Path $output 'summary.md') -Raw
        $summary | Should -Match 'MissedMutant'
        $summary | Should -Match 'Timeout'
    }

    It 'reports capture setup errors explicitly' {
        $check = @(Get-ScheduledCheck | Where-Object recipe -EQ 'miri')[0]
        Mock Invoke-CapturedProcess -ModuleName ScheduledExecution { throw 'Process start canary.' }
        Invoke-ScheduledCheck -Check $check -SourceRoot $recipeRoot -OutputDirectory $output -SourceSha $sourceSha |
            Should -Be 1
        Get-Content -LiteralPath (Join-Path $output 'summary.md') -Raw | Should -Match 'Process start canary'
    }
}

Describe 'Mutation diagnostic formatting' {
    BeforeEach {
        $script:output = Join-Path $TestDrive ([guid]::NewGuid().ToString())
        $null = New-Item -ItemType Directory -Path (Join-Path $output 'mutants.out') -Force
        '' | Set-Content -LiteralPath (Join-Path $output 'summary.md')
        $script:lab = Get-Content -LiteralPath (Join-Path $fixtures 'outcomes.json') -Raw | ConvertFrom-Json -AsHashtable
    }

    It 'describes native findings without producing another check verdict' {
        $lab | ConvertTo-Json -Depth 20 | Set-Content -LiteralPath (Join-Path $output 'mutants.out\outcomes.json')
        Write-ScheduledMutationSummary -OutputDirectory $output | Should -BeNullOrEmpty
        $summary = Get-Content -LiteralPath (Join-Path $output 'summary.md') -Raw
        $summary | Should -Match 'replace sample -> u32 with 0'
        $summary | Should -Match 'replace sample -> u32 with 1'
    }

    It 'describes a baseline failure without presenting mutants as source defects' {
        $lab.outcomes[0].summary = 'Failure'
        $null = New-Item -ItemType Directory -Path (Join-Path $output 'mutants.out\log')
        'baseline failure canary' | Set-Content -LiteralPath (Join-Path $output 'mutants.out\log\baseline.log')
        $lab | ConvertTo-Json -Depth 20 | Set-Content -LiteralPath (Join-Path $output 'mutants.out\outcomes.json')
        Write-ScheduledMutationSummary -OutputDirectory $output
        $summary = Get-Content -LiteralPath (Join-Path $output 'summary.md') -Raw
        $summary | Should -Match 'baseline failure canary'
        $summary | Should -Not -Match 'replace sample'
    }

    It 'identifies unavailable native details without inventing a result' {
        Write-ScheduledMutationSummary -OutputDirectory $output
        Get-Content -LiteralPath (Join-Path $output 'summary.md') -Raw | Should -Match 'details are unavailable'
    }
}

Describe 'Bounded diagnostic generation' {
    BeforeAll {
        $script:diagnosticModule = Get-Module ScheduledExecutionDiagnostics
        $script:originalLimits = & $diagnosticModule {
            @{ log = $script:LogReadByteLimit; summary = $script:SummaryByteLimit; json = $script:MutationJsonByteLimit }
        }
    }
    BeforeEach {
        $script:diagnosticOutput = Join-Path $TestDrive ([guid]::NewGuid().ToString())
        $null = New-Item -ItemType Directory -Path (Join-Path $diagnosticOutput 'mutants.out')
        $script:diagnosticSummary = Join-Path $diagnosticOutput 'summary.md'
        [IO.File]::WriteAllText($diagnosticSummary, '')
        & $diagnosticModule {
            $script:LogReadByteLimit = 128
            $script:SummaryByteLimit = 1024
            $script:MutationJsonByteLimit = 8192
        }
    }
    AfterAll {
        & $diagnosticModule {
            param($limits)
            $script:LogReadByteLimit = $limits.log
            $script:SummaryByteLimit = $limits.summary
            $script:MutationJsonByteLimit = $limits.json
        } $originalLimits
    }

    It 'retains all <Count> log bytes at or below the input limit' -ForEach @(
        @{ Count = 127 }, @{ Count = 128 }
    ) {
        $path = Join-Path $diagnosticOutput 'check.stdout'
        [IO.File]::WriteAllText($path, ('x' * $Count))
        Write-ScheduledLogExcerpt -Path $path -SummaryPath $diagnosticSummary
        $text = [IO.File]::ReadAllText($diagnosticSummary)
        $text | Should -Match ('x' * $Count)
        $text | Should -Not -Match 'truncated'
    }
    It 'retains huge-line failure context and the tail without reading the middle' {
        $path = Join-Path $diagnosticOutput 'check.stdout'
        [IO.File]::WriteAllText($path, ("prefix failure " + ('x' * 32768) + "`nlast failure canary"))
        Write-ScheduledLogExcerpt -Path $path -SummaryPath $diagnosticSummary
        $text = [IO.File]::ReadAllText($diagnosticSummary)
        $text | Should -Match 'prefix failure'
        $text | Should -Match 'last failure canary'
        $text | Should -Match 'Log excerpt truncated'
        $text | Should -Match 'Full log in result artifact: check.stdout'
        $text | Should -Not -Match ('x' * 200)
        (Get-Item -LiteralPath $diagnosticSummary).Length | Should -BeLessOrEqual 1024
    }
    It 'identifies a log exceeding the input limit by one byte' {
        $path = Join-Path $diagnosticOutput 'check.stdout'
        [IO.File]::WriteAllText($path, ('x' * 129))
        Write-ScheduledLogExcerpt -Path $path -SummaryPath $diagnosticSummary
        [IO.File]::ReadAllText($diagnosticSummary) | Should -Match 'middle output omitted'
    }
    It 'preserves UTF-8 characters split between adjacent read windows' {
        $path = Join-Path $diagnosticOutput 'check.stdout'
        $content = ('x' * 63) + [char]0x00E9 + 'tail'
        [IO.File]::WriteAllText($path, $content)
        Write-ScheduledLogExcerpt -Path $path -SummaryPath $diagnosticSummary
        [IO.File]::ReadAllText($diagnosticSummary) | Should -Match ([regex]::Escape($content))
    }
    It 'bounds many matching lines and retains the final failure' {
        $path = Join-Path $diagnosticOutput 'matches.log'
        [IO.File]::WriteAllText($path, ((1..500 | ForEach-Object { "error event_$_" }) -join "`n"))
        Write-ScheduledLogExcerpt -Path $path -SummaryPath $diagnosticSummary
        $text = [IO.File]::ReadAllText($diagnosticSummary)
        $text | Should -Match 'event_500'
        $text | Should -Not -Match 'event_250'
        $text | Should -Match 'middle output omitted'
        (Get-Item -LiteralPath $diagnosticSummary).Length | Should -BeLessOrEqual 1024
    }
    It 'shares one summary budget across many excerpts and emits one aggregate gap' {
        $paths = @(1..20 | ForEach-Object {
            $path = Join-Path $diagnosticOutput "$_.log"
            [IO.File]::WriteAllText($path, ("error context $_`n" + ('x' * 2048) + "`nfinal failure $_"))
            $path
        })
        Write-ScheduledLogExcerpt -Path $paths -SummaryPath $diagnosticSummary
        $text = [IO.File]::ReadAllText($diagnosticSummary)
        $text | Should -Match 'Diagnostic summary truncated'
        $text | Should -Match 'result artifact'
        @([regex]::Matches($text, 'Diagnostic summary truncated')).Count | Should -Be 1
        (Get-Item -LiteralPath $diagnosticSummary).Length | Should -BeLessOrEqual 1024
        Write-ScheduledLogExcerpt -Path $paths -SummaryPath $diagnosticSummary
        [IO.File]::ReadAllText($diagnosticSummary) | Should -BeExactly $text
    }
    It 'reserves the omission notice at the exact aggregate boundary' {
        InModuleScope ScheduledExecutionDiagnostics -Parameters @{ SummaryPath = $diagnosticSummary } {
            param($SummaryPath)
            $markerBytes = [Text.Encoding]::UTF8.GetByteCount($script:SummaryTruncationText)
            $payload = 'x' * ($script:SummaryByteLimit - $markerBytes)
            Add-ScheduledDiagnosticText $SummaryPath $payload | Should -BeTrue
            [IO.File]::ReadAllText($SummaryPath) | Should -BeExactly $payload
            Add-ScheduledDiagnosticText $SummaryPath 'more diagnostics' | Should -BeFalse
            (Get-Item -LiteralPath $SummaryPath).Length | Should -Be $script:SummaryByteLimit
            [IO.File]::ReadAllText($SummaryPath) | Should -Match 'Diagnostic summary truncated'
        }
    }
    It 'bounds combined mutation descriptions and per-mutation log excerpts' {
        & $diagnosticModule { $script:MutationJsonByteLimit = 64KB }
        $null = New-Item -ItemType Directory -Path (Join-Path $diagnosticOutput 'mutants.out\log')
        $outcomes = @(1..20 | ForEach-Object {
            [IO.File]::WriteAllText((Join-Path $diagnosticOutput "mutants.out\log\$_.log"),
                ("mutation failure $_`n" + ('x' * 2048) + "`nfinal failure $_"))
            @{
                scenario = @{ Mutant = @{ name = "replace expression $_"; package = 'example'; file = 'lib.rs'; replacement = '0' } }
                summary = 'MissedMutant'; phase_results = @(); log_path = "log/$_.log"
            }
        })
        @{ outcomes = $outcomes; total_mutants = 20; missed = 20; timeout = 0 } |
            ConvertTo-Json -Depth 20 | Set-Content -LiteralPath (Join-Path $diagnosticOutput 'mutants.out\outcomes.json')
        Write-ScheduledMutationSummary -OutputDirectory $diagnosticOutput | Should -BeNullOrEmpty
        $text = [IO.File]::ReadAllText($diagnosticSummary)
        $text | Should -Match 'MissedMutant'
        $text | Should -Match 'final failure 1'
        $text | Should -Match 'Diagnostic summary truncated'
        (Get-Item -LiteralPath $diagnosticSummary).Length | Should -BeLessOrEqual 1024
        (Get-Item -LiteralPath (Join-Path $diagnosticOutput 'mutants.out\log\1.log')).Length |
            Should -BeGreaterThan 2048
        $text | Should -Not -Match '(?m)^## Final result:|^Exit code:'
    }
    It 'bounds a large mutation description even when the native JSON is within its limit' {
        $outcome = @{
            scenario = @{ Mutant = @{ name = 'replace ' + ('x' * 4096); package = 'example'; file = 'lib.rs'; replacement = '0' } }
            summary = 'MissedMutant'; phase_results = @(); log_path = 'log/example.log'
        }
        $json = @{ outcomes = @($outcome); total_mutants = 1; missed = 1; timeout = 0 } |
            ConvertTo-Json -Depth 20 -Compress
        [IO.File]::WriteAllText((Join-Path $diagnosticOutput 'mutants.out\outcomes.json'), $json)
        Write-ScheduledMutationSummary -OutputDirectory $diagnosticOutput | Should -BeNullOrEmpty
        $text = [IO.File]::ReadAllText($diagnosticSummary)
        $text | Should -Match 'MissedMutant: replace'
        $text | Should -Match 'Diagnostic summary truncated'
        (Get-Item -LiteralPath $diagnosticSummary).Length | Should -BeLessOrEqual 1024
    }
    It 'accepts complete native JSON with <ExtraCapacity> spare bytes at the limit' -ForEach @(
        @{ ExtraCapacity = 0 }, @{ ExtraCapacity = 1 }
    ) {
        $json = '{"outcomes":[],"total_mutants":0,"missed":0,"timeout":0}'
        & $diagnosticModule { param($limit) $script:MutationJsonByteLimit = $limit } `
            ([Text.Encoding]::UTF8.GetByteCount($json) + $ExtraCapacity)
        [IO.File]::WriteAllText((Join-Path $diagnosticOutput 'mutants.out\outcomes.json'), $json)
        Write-ScheduledMutationSummary -OutputDirectory $diagnosticOutput | Should -BeNullOrEmpty
        [IO.File]::ReadAllText($diagnosticSummary) | Should -Match 'Tool totals: 0 mutations'
        [IO.File]::ReadAllText($diagnosticSummary) | Should -Not -Match 'exceeded'
    }
    It 'does not parse an oversized <FileName> even when its prefix is valid JSON' -ForEach @(
        @{ FileName = 'outcomes.json' }, @{ FileName = 'mutants.json' }
    ) {
        & $diagnosticModule { $script:MutationJsonByteLimit = 16 }
        [IO.File]::WriteAllText((Join-Path $diagnosticOutput "mutants.out\$FileName"), ('[]' + (' ' * 64)))
        Mock ConvertFrom-Json -ModuleName ScheduledExecutionDiagnostics { throw 'Unexpected partial JSON parsing.' }
        Write-ScheduledMutationSummary -OutputDirectory $diagnosticOutput | Should -BeNullOrEmpty
        $text = [IO.File]::ReadAllText($diagnosticSummary)
        $text | Should -Match 'exceeded the 16 byte limit'
        $text | Should -Match ([regex]::Escape("mutants.out/$FileName"))
        $text | Should -Not -Match 'No mutants selected|Tool totals:|Final result:'
        Should -Invoke ConvertFrom-Json -ModuleName ScheduledExecutionDiagnostics -Times 0 -Exactly
    }
    It 'describes malformed native details as a gap without changing the recipe verdict' {
        [IO.File]::WriteAllText((Join-Path $diagnosticOutput 'mutants.out\outcomes.json'), '{"outcomes":')
        Write-ScheduledMutationSummary -OutputDirectory $diagnosticOutput | Should -BeNullOrEmpty
        $text = [IO.File]::ReadAllText($diagnosticSummary)
        $text | Should -Match 'Mutation details could not be parsed'
        $text | Should -Match 'result artifact'
        $text | Should -Not -Match '(?m)^## Final result:|^Exit code:'
    }
}
