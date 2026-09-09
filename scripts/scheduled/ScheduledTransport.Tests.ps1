#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
# Protects the reporter's native boundaries without networking: finite subprocess fixtures prove
# that binary output is bounded, intentional log truncation is explicit, and UTF-8 data and
# nonzero exits survive process transport unchanged.
BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'ScheduledTransport.psm1') -Force
    $script:fixture = Join-Path $PSScriptRoot 'fixtures\Stream-Response.ps1'
}

Describe 'Bounded native response capture' {
    It 'preserves a complete response at the exact size limit' {
        InModuleScope ScheduledTransport -Parameters @{ Fixture = $fixture } {
            param($Fixture)
            $path = Join-Path $TestDrive 'exact.log'
            $result = Save-ScheduledProcessResponse -Executable (Get-Command pwsh).Source `
                -Arguments @('-NoProfile', '-File', $Fixture, '-Bytes', '256') -Path $path -MaxBytes 256
            $result.bytes | Should -Be 256
            $result.truncated | Should -BeFalse
            [IO.File]::ReadAllBytes($path).Length | Should -Be 256
        }
    }
    It 'rejects an oversized artifact but explicitly truncates a log' {
        InModuleScope ScheduledTransport -Parameters @{ Fixture = $fixture } {
            param($Fixture)
            $arguments = @('-NoProfile', '-File', $Fixture, '-Bytes', '100000')
            {
                Save-ScheduledProcessResponse -Executable (Get-Command pwsh).Source `
                    -Arguments $arguments -Path (Join-Path $TestDrive 'artifact.zip') -MaxBytes 256
            } | Should -Throw '*size limit*'
            $path = Join-Path $TestDrive 'truncated.log'
            $result = Save-ScheduledProcessResponse -Executable (Get-Command pwsh).Source `
                -Arguments $arguments -Path $path -MaxBytes 256 -Truncate
            $result.truncated | Should -BeTrue
            $result.bytes | Should -Be 256
            [IO.File]::ReadAllBytes($path).Length | Should -Be 256
        }
    }
    It 'does not accept a failed response as a captured log' {
        InModuleScope ScheduledTransport -Parameters @{ Fixture = $fixture } {
            param($Fixture)
            $arguments = @('-NoProfile', '-File', $Fixture, '-ExitCode', '1')
            {
                Save-ScheduledProcessResponse -Executable (Get-Command pwsh).Source `
                    -Arguments $arguments `
                    -Path (Join-Path $TestDrive 'failed.log') -MaxBytes 256 -Truncate
            } | Should -Throw '*Response failure canary*'
        }
    }
    It 'constrains the GitHub route and preserves typed arguments' {
        InModuleScope ScheduledTransport {
            Mock Save-ScheduledProcessResponse { @{ bytes = 0; truncated = $false } }
            $null = Save-ScheduledGitHubResponse -Endpoint 'repos/owner/repo/actions/jobs/10/logs' `
                -Path (Join-Path $TestDrive 'job.log') -MaxBytes 256 -Truncate
            Should -Invoke Save-ScheduledProcessResponse -Times 1 -ParameterFilter {
                $Arguments[0] -eq 'api' -and $Arguments[1] -eq 'repos/owner/repo/actions/jobs/10/logs' -and $Truncate
            }
            { Save-ScheduledGitHubResponse -Endpoint user -Path unused -MaxBytes 256 } | Should -Throw
        }
    }
}

Describe 'Controller JSON transport' {
    It 'preserves UTF-8 payloads independently of the console code page' {
        $payload = '{"text":"caf' + [char]0xe9 + '","empty":[]}'
        $actual = Invoke-ScheduledJsonExecutable -Executable (Get-Command pwsh).Source `
            -Directory $TestDrive -InputText $payload `
            -Arguments @('-NoProfile', '-File', $fixture, '-EchoInput')
        $actual | Should -BeExactly $payload
    }
    It 'surfaces utility rejection rather than accepting partial output' {
        {
            Invoke-ScheduledJsonExecutable -Executable (Get-Command pwsh).Source `
                -Directory $TestDrive -InputText '{}' `
                -Arguments @('-NoProfile', '-File', $fixture, '-EchoInput', '-ExitCode', '1')
        } | Should -Throw '*Response failure canary*'
    }
}
