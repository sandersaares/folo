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
        }
        AfterAll { Remove-Item -LiteralPath $script:directory -Recurse -Force }
        BeforeEach {
            $script:responseBytes = 16
            $script:responseExitCode = 0
            Mock Get-ScheduledDownloadStartInfo {
                $start = [Diagnostics.ProcessStartInfo]::new()
                $start.FileName = $script:executable
                $start.UseShellExecute = $false
                $start.RedirectStandardOutput = $true
                foreach ($argument in @('-NoProfile', '-File', $script:fixture, '-ByteCount',
                        "$script:responseBytes", '-ExitCode', "$script:responseExitCode")) {
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
                    Read-ScheduledArtifactText example/repo @{ id = 30; expired = $false } $script:directory summary.md
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
            Read-ScheduledArtifactText example/repo @{ id = 30; expired = $false } $script:directory summary.md |
                Should -BeExactly $script:entryText
            Test-Path -LiteralPath (Join-Path $script:directory '30.zip') | Should -BeFalse
        }
        It 'preserves a bounded prefix of an oversized summary with an explicit diagnostic gap' {
            $script:entryText = 'x' * 17
            $text = Read-ScheduledArtifactText example/repo @{ id = 30; expired = $false } $script:directory summary.md
            $text | Should -Match ('^' + ('x' * 16) + '\s')
            $text | Should -Match 'Check summary truncated at the 16 byte limit'
            $text | Should -Match 'Remaining diagnostics are unavailable'
            Test-Path -LiteralPath (Join-Path $script:directory '30.zip') | Should -BeFalse
        }
        It 'rejects oversized planning JSON even if its retained prefix would parse' {
            $script:entryText = '{}' + (' ' * 16)
            $script:entryName = 'plan.json'
            { Read-ScheduledArtifactText example/repo @{ id = 30; expired = $false } $script:directory plan.json } |
                Should -Throw
            Test-Path -LiteralPath (Join-Path $script:directory '30.zip') | Should -BeFalse
        }
        It 'counts UTF-8 bytes rather than decoded characters' {
            $script:entryText = [string]::new([char]0x00E9, 9)
            $text = Read-ScheduledArtifactText example/repo @{ id = 30; expired = $false } $script:directory summary.md
            $text | Should -Match 'Check summary truncated'
        }
        It 'deletes invalid ZIP data when opening the archive fails' {
            Mock Save-ScheduledGitHubFile {
                param($Path)
                Set-Content -LiteralPath $Path -Value 'not a ZIP'
                return $false
            }
            { Read-ScheduledArtifactText example/repo @{ id = 30; expired = $false } $script:directory summary.md } |
                Should -Throw
            Test-Path -LiteralPath (Join-Path $script:directory '30.zip') | Should -BeFalse
        }
        It 'deletes archives that lack the selected entry' {
            $script:entryName = 'other.txt'
            { Read-ScheduledArtifactText example/repo @{ id = 30; expired = $false } $script:directory summary.md } |
                Should -Throw
            Test-Path -LiteralPath (Join-Path $script:directory '30.zip') | Should -BeFalse
        }
        It 'deletes partial files even if downloading fails before archive opening' {
            Mock Save-ScheduledGitHubFile {
                param($Path)
                Set-Content -LiteralPath $Path -Value 'incomplete transfer'
                throw [IO.IOException]::new()
            }
            { Read-ScheduledArtifactText example/repo @{ id = 30; expired = $false } $script:directory summary.md } |
                Should -Throw
            Test-Path -LiteralPath (Join-Path $script:directory '30.zip') | Should -BeFalse
        }
    }
}
