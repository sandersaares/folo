#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0' }
BeforeAll {
    Import-Module (Join-Path $PSScriptRoot 'LocalGitHub.psm1') -Force
}
Describe 'Complete PR snapshots' {
    It 'paginates both threads and their comments along with review summaries and top-level input' {
        InModuleScope LocalGitHub {
            Mock Invoke-ScheduledApi {
                param($Endpoint, $Variables)
                switch -Wildcard ($Endpoint) {
                    '*/pulls/7' {
                        return @{ number = 7; head = @{ sha = ('a' * 40); ref = 'managed-branch' }
                            state = 'open'; merged = $false; draft = $false; mergeable = $true }
                    }
                    '*/commits/main' { return @{ sha = ('b' * 40) } }
                    '*/compare/*' { return @{ status = 'ahead' } }
                    '*/comments?*' {
                        return ,@(,@(@{ id = 1; body = 'Top-level'; user = @{ login = 'human' } }))
                    }
                    '*/reviews?*' {
                        return ,@(,@(@{ id = 2; body = 'Review summary'; state = 'CHANGES_REQUESTED'
                            user = @{ login = 'reviewer' } }))
                    }
                    '*/check-runs?*' {
                        return ,@(@{ check_runs = @(@{ id = 3; name = 'required-checks' }) })
                    }
                    'graphql' {
                        if ($Variables.ContainsKey('number')) {
                            $thread = if ($null -eq $Variables.cursor) { 'thread-a' } else { 'thread-b' }
                            return @{ data = @{ repository = @{ pullRequest = @{ reviewThreads = @{
                                nodes = @(@{ id = $thread; isResolved = $false })
                                pageInfo = @{ hasNextPage = $null -eq $Variables.cursor; endCursor = 'threads-next' }
                            } } } } }
                        }
                        $id = if ($null -eq $Variables.cursor) { 10 } else { 11 }
                        return @{ data = @{ node = @{ comments = @{
                            nodes = @(@{ databaseId = $id; body = "$($Variables.id) comment $id"
                                author = @{ login = 'reviewer' } })
                            pageInfo = @{ hasNextPage = $null -eq $Variables.cursor; endCursor = 'comments-next' }
                        } } } }
                    }
                    default { throw "Unexpected endpoint $Endpoint" }
                }
            }
            $snapshot = Get-ScheduledPullRequestSnapshot -Repository folo-rs/folo -PullRequestNumber 7
            $snapshot.collections_complete | Should -BeTrue
            $snapshot.contains_main | Should -BeTrue
            $snapshot.review_input.Count | Should -Be 6
            @($snapshot.review_input | Where-Object { $_.kind -eq 'thread' }).Count | Should -Be 4
            Should -Invoke Invoke-ScheduledApi -Exactly 6 -ParameterFilter { $Endpoint -eq 'graphql' }
            Should -Invoke Invoke-ScheduledApi -Exactly 2 -ParameterFilter { $Endpoint -eq 'repos/folo-rs/folo/commits/main' }
        }
    }
    It 'rejects a cursor that cannot make progress without waiting for a timeout' {
        InModuleScope LocalGitHub {
            Mock Invoke-ScheduledApi {
                return @{ data = @{ list = @{
                    nodes = @(); pageInfo = @{ hasNextPage = $true; endCursor = 'same' }
                } } }
            }
            { Get-ScheduledGraphCollection -Query 'query' -Variables @{} -ConnectionPath @('data', 'list') } |
                Should -Throw
            Should -Invoke Invoke-ScheduledApi -Exactly 2
        }
    }
    It 'retains single-page JSON nesting from the GitHub CLI' {
        InModuleScope LocalGitHub {
            Mock gh {
                $global:LASTEXITCODE = 0
                '[[{"id":1}]]'
            }
            $result = Get-ScheduledApiCollection -Endpoint 'test'
            $result.Count | Should -Be 1
            $result[0].id | Should -Be 1
        }
    }
}
