#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
# Protects the reporter's bounded native-pipe and ZIP-entry reads, including exact byte
# boundaries and downloaded-file cleanup. All fixtures and subprocesses remain local.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
$VerbosePreference = 'Continue'
Import-Module (Join-Path $PSScriptRoot 'ScheduledGitHub.psm1') -Force

Describe 'Bounded GitHub downloads' {
    InModuleScope ScheduledGitHub {
        BeforeAll {
            $script:directory = Join-Path (Get-Location).Path "target\scheduled-download-tests-$([guid]::NewGuid().ToString('N'))"
            $null = New-Item -ItemType Directory -Path $script:directory -Force
            $script:path = Join-Path $script:directory 'response'
            $script:executable = (Get-Command pwsh -CommandType Application | Select-Object -First 1).Source
            $script:fixture = Join-Path $PSScriptRoot 'fixtures\Write-ReporterBytes.ps1'
            $script:realRetryCommand = Get-Command Invoke-WithRetry
        }
        AfterAll { Remove-Item -LiteralPath $script:directory -Recurse -Force }
        BeforeEach {
            $script:responseBytes = 16
            $script:responseExitCode = 0
            $script:responseErrorText = ''
            $script:failedTransfers = 0
            $script:downloadStarts = 0
            # Exercise real retry decisions without depending on a globally selected Retry
            # module instance or allowing a real delay in the full script suite.
            Mock Invoke-WithRetry {
                param($Action, $Attempt, $DelaySeconds, $BackoffMultiplier, $MaxDelaySeconds, $RetryOn)
                $DelaySeconds | Should -Be 3
                & $script:realRetryCommand -Action $Action -Attempt $Attempt -DelaySeconds 0 `
                    -BackoffMultiplier $BackoffMultiplier -MaxDelaySeconds $MaxDelaySeconds -RetryOn $RetryOn
            }
            Mock Get-ScheduledDownloadStartInfo {
                $script:downloadStarts++
                $start = [Diagnostics.ProcessStartInfo]::new()
                $start.FileName = $script:executable
                $start.UseShellExecute = $false
                $start.RedirectStandardOutput = $true
                $start.RedirectStandardError = $true
                $exitCode = $script:responseExitCode
                $errorText = $script:responseErrorText
                if ($script:failedTransfers -gt 0 -and $script:downloadStarts -gt $script:failedTransfers) {
                    $exitCode = 0
                    $errorText = ''
                }
                foreach ($argument in @('-NoProfile', '-File', $script:fixture, '-ByteCount',
                        "$script:responseBytes", '-ExitCode', "$exitCode", '-ErrorText', $errorText)) {
                    $start.ArgumentList.Add($argument)
                }
                return $start
            }
        }
        AfterEach { [IO.File]::Delete($script:path) }
        It 'preserves a complete response of <Count> bytes within the limit' -ForEach @(
            @{ Count = 0 }, @{ Count = 15 }, @{ Count = 16 }
        ) {
            $script:responseBytes = $Count
            Save-ScheduledGitHubFile repos/example/repo/response $script:path -ByteLimit 16 | Should -BeFalse
            [IO.File]::ReadAllText($script:path) | Should -BeExactly ('x' * $Count)
        }
        It 'rejects and removes an oversized binary response' {
            $script:responseBytes = 17
            $failure = { Save-ScheduledGitHubFile repos/example/repo/response $script:path -ByteLimit 16 } |
                Should -Throw -PassThru
            $failure.Exception.Message | Should -Match 'limit'
            Test-Path -LiteralPath $script:path | Should -BeFalse
        }
        It 'retains only a bounded text prefix and terminates a verbose download' {
            $script:responseBytes = 1MB
            Save-ScheduledGitHubFile repos/example/repo/response $script:path -ByteLimit 16 -AllowPartial |
                Should -BeTrue
            [IO.File]::ReadAllText($script:path) | Should -BeExactly ('x' * 16)
        }
        It 'cleans partial output after a failed download' {
            $script:responseExitCode = 1
            { Save-ScheduledGitHubFile repos/example/repo/response $script:path -ByteLimit 16 } | Should -Throw
            Test-Path -LiteralPath $script:path | Should -BeFalse
        }
        It 'retries a transfer after <FailureText> without retaining its partial output' -ForEach @(
            @{ FailureText = 'gh: Service unavailable (HTTP 503)' }
            @{ FailureText = 'gh: API rate limit exceeded (HTTP 403)' }
            @{ FailureText = 'connection reset by peer' }
        ) {
            $script:responseExitCode = 1
            $script:responseErrorText = $FailureText
            $script:failedTransfers = 1
            Save-ScheduledGitHubFile repos/example/repo/response $script:path -ByteLimit 16 | Should -BeFalse
            $script:downloadStarts | Should -Be 2
            [IO.File]::ReadAllText($script:path) | Should -BeExactly ('x' * 16)
            Should -Invoke Invoke-WithRetry -Times 1 -Exactly -ParameterFilter {
                $Attempt -eq 4 -and $DelaySeconds -eq 3 -and $BackoffMultiplier -eq 2 -and $MaxDelaySeconds -eq 30
            }
        }
        It 'does not retry deterministic transfer refusals and preserves their diagnostic' {
            $script:responseExitCode = 1
            $script:responseErrorText = 'gh: Not Found (HTTP 404)'
            $failure = { Save-ScheduledGitHubFile repos/example/repo/response $script:path -ByteLimit 16 } |
                Should -Throw -PassThru
            $failure.Exception.Message | Should -Match 'Not Found \(HTTP 404\)'
            $script:downloadStarts | Should -Be 1
        }
        It 'bounds stderr while stdout is waiting and cleans the interrupted transfer' {
            $savedLimit = $script:ErrorTextLimit
            try {
                $script:ErrorTextLimit = 64
                $script:responseErrorText = 'x' * 10000
                { Save-ScheduledGitHubFile repos/example/repo/response $script:path -ByteLimit 16 } | Should -Throw
                Test-Path -LiteralPath $script:path | Should -BeFalse
                $script:downloadStarts | Should -Be 1
            } finally { $script:ErrorTextLimit = $savedLimit }
        }
        It 'cleans partial output when reading the pipe fails' {
            Mock Copy-ScheduledLimitedStream { throw [IO.IOException]::new() }
            { Save-ScheduledGitHubFile repos/example/repo/response $script:path -ByteLimit 16 } | Should -Throw
            Test-Path -LiteralPath $script:path | Should -BeFalse
        }
        It 'does not pass an oversized partial ZIP to the archive reader' {
            $savedLimit = $script:ArchiveByteLimit
            try {
                $script:ArchiveByteLimit = 16
                $script:responseBytes = 17
                $failure = {
                    Read-ScheduledArtifactText example/repo @{ id = 30; expired = $false } $script:directory
                } | Should -Throw -PassThru
                $failure.Exception.Message | Should -Match 'limit'
                Test-Path -LiteralPath (Join-Path $script:directory '30.zip') | Should -BeFalse
            } finally { $script:ArchiveByteLimit = $savedLimit }
        }
    }
}

Describe 'Bounded decompressed artifact text' {
    InModuleScope ScheduledGitHub {
        BeforeAll {
            $script:directory = Join-Path (Get-Location).Path "target\scheduled-entry-tests-$([guid]::NewGuid().ToString('N'))"
            $null = New-Item -ItemType Directory -Path $script:directory -Force
            $script:savedTextLimit = $script:TextByteLimit
        }
        AfterAll {
            $script:TextByteLimit = $script:savedTextLimit
            Remove-Item -LiteralPath $script:directory -Recurse -Force
        }
        BeforeEach {
            $script:TextByteLimit = 16
            $script:entryText = 'x' * 16
            $script:entryName = 'summary.md'
            Mock Save-ScheduledGitHubFile {
                param($Path)
                $archive = [IO.Compression.ZipFile]::Open($Path, [IO.Compression.ZipArchiveMode]::Create)
                try {
                    $writer = [IO.StreamWriter]::new($archive.CreateEntry($script:entryName).Open())
                    try { $writer.Write($script:entryText) } finally { $writer.Dispose() }
                } finally { $archive.Dispose() }
                return $false
            }
        }
        It 'reads all <Count> entry bytes and deletes the archive on success' -ForEach @(
            @{ Count = 0 }, @{ Count = 15 }, @{ Count = 16 }
        ) {
            $script:entryText = 'x' * $Count
            Read-ScheduledArtifactText example/repo @{ id = 30; expired = $false } $script:directory |
                Should -BeExactly $script:entryText
            Test-Path -LiteralPath (Join-Path $script:directory '30.zip') | Should -BeFalse
        }
        It 'preserves a bounded prefix of an oversized summary with an explicit diagnostic gap' {
            $script:entryText = 'x' * 17
            $text = Read-ScheduledArtifactText example/repo @{ id = 30; expired = $false } $script:directory
            $text | Should -Match ('^' + ('x' * 16) + '\s')
            $text | Should -Match 'Check summary truncated at the 16 byte limit'
            $text | Should -Match 'Remaining diagnostics are unavailable'
            Test-Path -LiteralPath (Join-Path $script:directory '30.zip') | Should -BeFalse
        }
        It 'counts UTF-8 bytes rather than decoded characters' {
            $script:entryText = [string]::new([char]0x00E9, 9)
            $text = Read-ScheduledArtifactText example/repo @{ id = 30; expired = $false } $script:directory
            $text | Should -Match 'Check summary truncated'
        }
        It 'deletes invalid ZIP data when opening the archive fails' {
            Mock Save-ScheduledGitHubFile {
                param($Path)
                Set-Content -LiteralPath $Path -Value 'not a ZIP'
                return $false
            }
            { Read-ScheduledArtifactText example/repo @{ id = 30; expired = $false } $script:directory } |
                Should -Throw
            Test-Path -LiteralPath (Join-Path $script:directory '30.zip') | Should -BeFalse
        }
        It 'deletes archives that lack the selected entry' {
            $script:entryName = 'other.txt'
            { Read-ScheduledArtifactText example/repo @{ id = 30; expired = $false } $script:directory } |
                Should -Throw
            Test-Path -LiteralPath (Join-Path $script:directory '30.zip') | Should -BeFalse
        }
        It 'deletes partial files even if downloading fails before archive opening' {
            Mock Save-ScheduledGitHubFile {
                param($Path)
                Set-Content -LiteralPath $Path -Value 'incomplete transfer'
                throw [IO.IOException]::new()
            }
            { Read-ScheduledArtifactText example/repo @{ id = 30; expired = $false } $script:directory } |
                Should -Throw
            Test-Path -LiteralPath (Join-Path $script:directory '30.zip') | Should -BeFalse
        }
    }
}
