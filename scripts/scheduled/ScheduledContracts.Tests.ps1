#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
# Protects the wire format shared by every scheduled module: canonical digests must be stable and
# comment-terminator-safe when embedded in GitHub issue/PR bodies, and record/policy validation
# must reject malformed input rather than silently coercing it into a usable record.
BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'ScheduledContracts.psm1') -Force
}

Describe 'Scheduled records' {
    It 'round trips evidence without allowing a comment terminator' {
        $record = @{ schema_version = 1; summary = 'x --> y'; nullable = $null; list = @('seed') }
        $marker = Write-ScheduledRecord -Kind reporter -Record $record
        $marker | Should -Not -Match 'x -->'
        $parsed = Read-ScheduledRecord -Kind reporter -Text "Human introduction`n$marker`nHuman conclusion"
        $parsed.summary | Should -Be $record.summary
        $parsed.list.Count | Should -Be 1
    }
    It 'rejects absent duplicate and incompatible records' {
        $marker = Write-ScheduledRecord -Kind worker -Record @{ schema_version = 1 }
        { Read-ScheduledRecord -Kind worker -Text '' } | Should -Throw
        { Read-ScheduledRecord -Kind worker -Text "$marker`n$marker" } | Should -Throw
        { Read-ScheduledRecord -Kind worker -Text '<!-- scheduled-worker:v1 {"schema_version":2} -->' } | Should -Throw
    }
    It 'canonicalizes maps but retains case and array ordering' {
        (Get-ScheduledDigest @{ b = 2; a = @{ d = 4; c = 3 } }) |
            Should -Be (Get-ScheduledDigest @{ a = @{ c = 3; d = 4 }; b = 2 })
        (Get-ScheduledDigest @('a', 'b')) | Should -Not -Be (Get-ScheduledDigest @('b', 'a'))
        (Get-ScheduledDigest 'Path') | Should -Not -Be (Get-ScheduledDigest 'path')
    }
    It 'keeps existing enforcement enabled in staged policy' {
        $policy = Get-ScheduledPolicy
        $policy.rollout.cutover | Should -BeFalse
        $policy.local.mode | Should -Be observe
    }
    It 'fails closed on cutover without prerequisites' {
        $policy = Get-ScheduledPolicy
        $policy.rollout.cutover = $true
        $path = Join-Path $TestDrive 'policy.json'
        $policy | ConvertTo-Json -Depth 20 | Set-Content $path
        { Get-ScheduledPolicy -Path $path } | Should -Throw
    }
}
