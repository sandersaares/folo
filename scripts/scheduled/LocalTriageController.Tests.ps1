#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
# Verifies that every shared write-path module and imported executable helper affects enrollment.
# Hash values are injected; the real file inventory is inspected without editing controller files.
BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'LocalTriagePolicy.psm1')
}

Describe 'Complete reviewed controller identity' {
    It 'changes when a transitive health, GitHub or executable-helper module changes' {
        $script:changed = ''
        Mock Get-TriageWorkingFileHash -ModuleName LocalTriagePolicy {
            foreach ($path in $Paths) {
                if ($changed -ne '' -and $path -ceq $changed) { 'b' * 40 } else { 'a' * 40 }
            }
        }
        $before = Get-ScheduledTriageControllerDigest
        foreach ($path in @(
            'scripts/scheduled/LocalHealth.psm1', 'scripts/scheduled/LocalHealthState.psm1',
            'scripts/scheduled/LocalHealthIntegrity.psm1', 'scripts/scheduled/ScheduledGitHub.psm1',
            'scripts/scheduled/ScheduledRunGitHub.psm1', 'scripts/scheduled/ScheduledTransport.psm1',
            'scripts/scheduled/ScheduledRecordTool.psm1', 'scripts/build/CargoExecutable.psm1',
            'scripts/build/Miri.psm1', 'scripts/build/Mutants.psm1', 'scripts/build/Sharding.psm1',
            '.github/skills/scheduled-triage/SKILL.md', '.gitattributes'
        )) {
            $script:changed = $path
            (Get-ScheduledTriageControllerDigest) | Should -Not -Be $before -Because $path
        }
    }
}
