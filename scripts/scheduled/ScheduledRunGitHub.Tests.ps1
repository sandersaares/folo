#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
# Protects run-level capture before artifact parsing: every job/step is inventoried, failed
# setup jobs retain diagnostics, and pagination or log failures remain explicit evidence gaps.
BeforeDiscovery { Import-Module (Join-Path $PSScriptRoot 'ScheduledRunGitHub.psm1') }
BeforeAll { Import-Module (Join-Path $PSScriptRoot 'ScheduledRunGitHub.psm1') }

Describe 'Run job evidence collection' {
    InModuleScope ScheduledRunGitHub {
        BeforeEach {
            $script:policy = @{ repository = 'owner/repo'; coverage = @{ max_artifact_bytes = 104857600 } }
            $script:run = @{ id = 10; run_attempt = 2; head_sha = 'a' * 40 }
            $script:jobs = @(
                @{ id = 20; run_id = 10; run_attempt = 2; head_sha = 'a' * 40; name = 'Plan'
                    status = 'completed'; conclusion = 'success'
                    steps = @(@{ number = 1; name = 'Plan'; status = 'completed'; conclusion = 'success' }) },
                @{ id = 21; run_id = 10; run_attempt = 2; head_sha = 'a' * 40; name = 'Mutation shard'
                    status = 'completed'; conclusion = 'failure'
                    steps = @(@{ number = 1; name = 'Download dependency'; status = 'completed'; conclusion = 'failure' }) }
            )
            $script:pages = @(@{ total_count = 2; jobs = $jobs })
            $script:api = {
                param($Endpoint, [switch] $Paginate)
                $Endpoint | Should -BeExactly 'repos/owner/repo/actions/runs/10/attempts/2/jobs?per_page=100'
                $Paginate | Should -BeTrue
                return $script:pages
            }
            Mock Save-ScheduledGitHubResponse {
                [IO.File]::WriteAllText($Path, 'Dependency download failed.')
                return @{ bytes = 27; truncated = $false }
            }
        }
        It 'captures failed setup and all steps without needing a checker artifact' {
            $result = Get-ScheduledRunJobEvidence -Policy $policy -Run $run -OutputDirectory $TestDrive -Api $api
            $result.jobs.Count | Should -Be 2
            $result.jobs[1].steps[0].name | Should -BeExactly 'Download dependency'
            $result.jobs[1].log.excerpt | Should -BeExactly 'Dependency download failed.'
            $result.jobs[1].log.url | Should -BeExactly 'https://api.github.com/repos/owner/repo/actions/jobs/21/logs'
            $result.evidence_gaps | Should -BeNullOrEmpty
            Should -Invoke Save-ScheduledGitHubResponse -Times 1 -Exactly
        }
        It 'collects every page and flags a truncated inventory' {
            $script:pages = @(@{ total_count = 2; jobs = @($jobs[0]) }, @{ total_count = 2; jobs = @($jobs[1]) })
            (Get-ScheduledRunJobEvidence -Policy $policy -Run $run -OutputDirectory $TestDrive -Api $api).jobs.Count |
                Should -Be 2
            $script:pages = @($script:pages[0])
            $result = Get-ScheduledRunJobEvidence -Policy $policy -Run $run -OutputDirectory $TestDrive -Api $api
            $result.evidence_gaps | Should -Contain 'Job inventory: Job inventory is empty or pagination is incomplete.'
        }
        It 'retains a failed step even when its job conclusion is success' {
            $jobs[1].conclusion = 'success'
            $result = Get-ScheduledRunJobEvidence -Policy $policy -Run $run -OutputDirectory $TestDrive -Api $api
            $result.jobs[1].log.excerpt | Should -Not -BeNullOrEmpty
        }
        It 'records unavailable logs without dropping the failed job' {
            Mock Save-ScheduledGitHubResponse { throw [IO.IOException]::new('Expired log.') }
            $result = Get-ScheduledRunJobEvidence -Policy $policy -Run $run -OutputDirectory $TestDrive -Api $api
            $result.jobs.Count | Should -Be 2
            $result.jobs[1].log.unavailable | Should -BeTrue
            $result.evidence_gaps | Should -Contain 'Job 21 log: Expired log.'
        }
        It 'records API failures instead of reporting an empty complete inventory' {
            $result = Get-ScheduledRunJobEvidence -Policy $policy -Run $run -OutputDirectory $TestDrive `
                -Api { throw [IO.IOException]::new('Jobs unavailable.') }
            $result.jobs.Count | Should -Be 0
            $result.evidence_gaps | Should -Contain 'Job inventory: Jobs unavailable.'
        }
        It 'never downloads logs for a foreign job' {
            $jobs[1].run_id = 99
            $result = Get-ScheduledRunJobEvidence -Policy $policy -Run $run -OutputDirectory $TestDrive -Api $api
            $result.evidence_gaps | Should -Contain 'Job inventory: Job metadata does not identify this run attempt.'
            Should -Invoke Save-ScheduledGitHubResponse -Times 0 -Exactly
        }
        It 'preserves explicit capture and excerpt truncation information' {
            Mock Save-ScheduledGitHubResponse {
                [IO.File]::WriteAllText($Path, 'x' * 5000)
                return @{ bytes = 5000; truncated = $true }
            }
            $result = Get-ScheduledRunJobEvidence -Policy $policy -Run $run -OutputDirectory $TestDrive -Api $api
            $result.jobs[1].log.truncated | Should -BeTrue
            $result.jobs[1].log.excerpt_truncated | Should -BeTrue
            $result.jobs[1].log.excerpt.Length | Should -Be 4096
        }
    }
}
