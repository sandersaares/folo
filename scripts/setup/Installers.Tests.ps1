#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }

# Smoke-tests the shared standalone installers from an unrelated working directory. Tool shims
# report each script's own pin, so import/path resolution and idempotence need no downloads.
BeforeAll {
    function actionlint {}
    function shellcheck {}
    function azcopy {}
}

Describe 'Standalone setup installers' {
    It 'imports its shared dependency and accepts installed <_> without changing the machine' -ForEach @(
        'actionlint', 'shellcheck', 'azcopy'
    ) {
        $installer = Join-Path $PSScriptRoot "install-$_.ps1"
        $text = Get-Content -LiteralPath $installer -Raw
        $pin = [regex]::Match($text, '\$script:\w+Version = ''([^'']+)''').Groups[1].Value
        $pin | Should -Not -BeNullOrEmpty
        Mock actionlint { $pin }
        Mock shellcheck { "version: $pin" }
        Mock azcopy { "azcopy version $pin" }
        Mock New-Item { throw 'An installed tool must not start installation.' }
        Mock Invoke-WebRequest { throw 'Installer smoke tests must not download tools.' }
        Push-Location $TestDrive
        try {
            & $installer -Destination (Join-Path $TestDrive 'unused') -Verbose:$false
        } finally {
            Pop-Location
        }
        Should -Invoke New-Item -Times 0 -Exactly
        Should -Invoke Invoke-WebRequest -Times 0 -Exactly
    }
}
