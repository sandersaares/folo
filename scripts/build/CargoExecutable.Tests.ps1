#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }

# Pester suite for CargoExecutable.psm1. Resolve-CargoExecutable is pure, so it
# is fed hand-built `cargo build --message-format=json` streams.

BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'CargoExecutable.psm1') -Force

    $script:FakerArtifact = '{"reason":"compiler-artifact","target":{"name":"cargo-bench-history-faker","kind":["bin"]},"executable":"/tmp/target/faker/debug/cargo-bench-history-faker"}'
    $script:DepArtifact = '{"reason":"compiler-artifact","target":{"name":"serde","kind":["lib"]},"executable":null}'
    $script:BuildFinished = '{"reason":"build-finished","success":true}'
}

Describe 'Resolve-CargoExecutable' {
    It 'returns the executable path from the faker artifact' {
        $exe = Resolve-CargoExecutable -CargoMessage @($script:FakerArtifact) -TargetName 'cargo-bench-history-faker'
        $exe | Should -Be '/tmp/target/faker/debug/cargo-bench-history-faker'
    }

    It 'ignores dependency artifacts and non-artifact messages' {
        $messages = @($script:DepArtifact, $script:FakerArtifact, $script:BuildFinished)
        $exe = Resolve-CargoExecutable -CargoMessage $messages -TargetName 'cargo-bench-history-faker'
        $exe | Should -Be '/tmp/target/faker/debug/cargo-bench-history-faker'
    }

    It 'does not throw on a build-finished message that lacks target/executable (strict-mode safe)' {
        $messages = @($script:BuildFinished, $script:FakerArtifact)
        { Resolve-CargoExecutable -CargoMessage $messages -TargetName 'cargo-bench-history-faker' } | Should -Not -Throw
    }

    It 'tolerates interleaved non-JSON rendered-diagnostic lines' {
        $messages = @('warning: unused variable', '', $script:FakerArtifact, 'Compiling cargo-bench-history-faker v0.0.5')
        $exe = Resolve-CargoExecutable -CargoMessage $messages -TargetName 'cargo-bench-history-faker'
        $exe | Should -Be '/tmp/target/faker/debug/cargo-bench-history-faker'
    }

    It 'returns the last matching faker artifact when several are present' {
        $first = '{"reason":"compiler-artifact","target":{"name":"cargo-bench-history-faker","kind":["bin"]},"executable":"/first/cargo-bench-history-faker"}'
        $last = '{"reason":"compiler-artifact","target":{"name":"cargo-bench-history-faker","kind":["bin"]},"executable":"/last/cargo-bench-history-faker"}'
        $exe = Resolve-CargoExecutable -CargoMessage @($first, $last) -TargetName 'cargo-bench-history-faker'
        $exe | Should -Be '/last/cargo-bench-history-faker'
    }

    It 'ignores a faker artifact whose executable is null' {
        $nullExe = '{"reason":"compiler-artifact","target":{"name":"cargo-bench-history-faker","kind":["lib"]},"executable":null}'
        { Resolve-CargoExecutable -CargoMessage @($nullExe) -TargetName 'cargo-bench-history-faker' } | Should -Throw '*could not resolve*'
    }

    It 'throws when no faker artifact is present' {
        $messages = @($script:DepArtifact, $script:BuildFinished)
        { Resolve-CargoExecutable -CargoMessage $messages -TargetName 'cargo-bench-history-faker' } | Should -Throw '*could not resolve the cargo-bench-history-faker executable*'
    }

    It 'selects the requested executable when several binaries are present' {
        $helper = '{"reason":"compiler-artifact","target":{"name":"dure-test-helper","kind":["bin"]},"executable":"/tmp/target/dure-test-helper"}'
        $exe = Resolve-CargoExecutable -CargoMessage @($script:FakerArtifact, $helper) -TargetName 'dure-test-helper'
        $exe | Should -Be '/tmp/target/dure-test-helper'
    }
}
