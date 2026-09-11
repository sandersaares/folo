Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
Import-Module (Join-Path $PSScriptRoot 'ScheduledJson.psm1')

# Read-side GitHub adapter for the personal Local App executor (LocalInbox.psm1 and the
# `scheduled-intake`/`scheduled-repair` skills call it between durable-state transactions; see
# ../../docs/scheduled-validation.md#durable-ownership-and-native-calls). It never authenticates,
# publishes or claims anything itself - only read APIs are used here. Native App session creation
# and publication actions remain the skills' responsibility, driven by LocalState.psm1's protocol.
function Invoke-ScheduledApi {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string] $Endpoint,
        [switch] $Paginate,
        [string] $Query,
        [hashtable] $Variables = @{}
    )
    $arguments = @('api', $Endpoint)
    if ($Paginate) { $arguments += @('--paginate', '--slurp') }
    if ($Query) {
        $arguments += @('-f', "query=$Query")
        foreach ($key in $Variables.Keys) {
            if ($null -ne $Variables[$key]) { $arguments += @('-F', "$key=$($Variables[$key])") }
        }
    }
    $output = & gh @arguments
    if ($LASTEXITCODE -ne 0) { throw "GitHub read failed for $Endpoint." }
    $value = ConvertFrom-ScheduledJson -InputObject ($output -join "`n") -NoEnumerate
    if ($value -is [System.Collections.IDictionary] -and $value.Contains('errors')) {
        throw 'GraphQL returned incomplete data with errors.'
    }
    return ,$value
}

function Get-ScheduledApiCollection {
    [CmdletBinding()]
    param([Parameter(Mandatory)][string] $Endpoint)
    $pages = Invoke-ScheduledApi -Endpoint $Endpoint -Paginate
    if ($pages -isnot [System.Collections.IList]) { throw 'Expected paginated GitHub collection.' }
    $items = [Collections.Generic.List[object]]::new()
    foreach ($page in $pages) {
        if ($page -isnot [System.Collections.IList]) { throw 'Incomplete GitHub pagination.' }
        foreach ($item in $page) { $items.Add($item) }
    }
    return ,$items.ToArray()
}

function Get-ScheduledGraphCollection {
    param([string] $Query, [hashtable] $Variables, [string[]] $ConnectionPath)
    $items = [Collections.Generic.List[object]]::new()
    $cursors = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    $Variables.cursor = $null
    do {
        $response = Invoke-ScheduledApi -Endpoint graphql -Query $Query -Variables $Variables
        $connection = $response
        foreach ($key in $ConnectionPath) {
            if ($null -eq $connection -or -not $connection.Contains($key)) {
                throw 'Missing GraphQL connection; cannot establish complete review evidence.'
            }
            $connection = $connection[$key]
        }
        foreach ($node in $connection.nodes) { $items.Add($node) }
        if (-not $connection.pageInfo.hasNextPage) { break }
        $cursor = $connection.pageInfo.endCursor
        if ([string]::IsNullOrWhiteSpace($cursor) -or -not $cursors.Add($cursor)) {
            throw 'GraphQL pagination made no progress.'
        }
        $Variables.cursor = $cursor
    } while ($true)
    return ,$items.ToArray()
}

function Get-ScheduledPullRequestSnapshot {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][ValidatePattern('^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$')][string] $Repository,
        [Parameter(Mandatory)][ValidateRange(1, [int]::MaxValue)][int] $PullRequestNumber
    )
    $pr = Invoke-ScheduledApi -Endpoint "repos/$Repository/pulls/$PullRequestNumber"
    $main = Invoke-ScheduledApi -Endpoint "repos/$Repository/commits/main"
    $comparison = Invoke-ScheduledApi -Endpoint "repos/$Repository/compare/$($main.sha)...$($pr.head.sha)"
    $comments = Get-ScheduledApiCollection -Endpoint "repos/$Repository/issues/$PullRequestNumber/comments?per_page=100"
    $reviews = Get-ScheduledApiCollection -Endpoint "repos/$Repository/pulls/$PullRequestNumber/reviews?per_page=100"
    $checkPages = Invoke-ScheduledApi -Endpoint "repos/$Repository/commits/$($pr.head.sha)/check-runs?per_page=100" -Paginate
    $checks = [Collections.Generic.List[object]]::new()
    foreach ($page in $checkPages) {
        if (-not $page.Contains('check_runs')) { throw 'Incomplete check-run collection.' }
        foreach ($check in $page.check_runs) { $checks.Add($check) }
    }
    $parts = $Repository.Split('/')
    $query = @'
query($owner:String!,$repo:String!,$number:Int!,$cursor:String) {
  repository(owner:$owner,name:$repo) {
    pullRequest(number:$number) {
      reviewThreads(first:100,after:$cursor) {
        nodes { id isResolved }
        pageInfo { hasNextPage endCursor }
      }
    }
  }
}
'@
    $threads = Get-ScheduledGraphCollection -Query $query `
        -Variables @{ owner = $parts[0]; repo = $parts[1]; number = $PullRequestNumber } `
        -ConnectionPath @('data', 'repository', 'pullRequest', 'reviewThreads')
    $reviewInput = [Collections.Generic.List[object]]::new()
    foreach ($comment in $comments) {
        $reviewInput.Add(@{ kind = 'comment'; id = $comment.id; body = [string]$comment.body
            state = 'COMMENTED'; author = $comment.user.login; resolved = $false })
    }
    foreach ($review in $reviews) {
        $reviewInput.Add(@{ kind = 'review'; id = $review.id; body = [string]$review.body
            state = $review.state.ToUpperInvariant(); author = $review.user.login; resolved = $false })
    }
    $threadQuery = @'
query($id:ID!,$cursor:String) {
  node(id:$id) {
    ... on PullRequestReviewThread {
      comments(first:100,after:$cursor) {
        nodes { databaseId body author { login } }
        pageInfo { hasNextPage endCursor }
      }
    }
  }
}
'@
    foreach ($thread in $threads) {
        $threadComments = Get-ScheduledGraphCollection -Query $threadQuery -Variables @{ id = $thread.id } `
            -ConnectionPath @('data', 'node', 'comments')
        foreach ($threadComment in $threadComments) {
            $reviewInput.Add(@{ kind = 'thread'; id = $threadComment.databaseId; body = [string]$threadComment.body
                state = 'COMMENTED'; author = if ($null -ne $threadComment.author) {
                    $threadComment.author.login
                } else { '' }; resolved = $thread.isResolved; thread_id = $thread.id })
        }
    }
    # A changed head/base during pagination invalidates this snapshot; no mixed-head decision.
    $latest = Invoke-ScheduledApi -Endpoint "repos/$Repository/pulls/$PullRequestNumber"
    $latestMain = Invoke-ScheduledApi -Endpoint "repos/$Repository/commits/main"
    if ($latest.head.sha -cne $pr.head.sha -or $latestMain.sha -cne $main.sha -or
        $latest.state -cne $pr.state -or $latest.merged -ne $pr.merged) {
        throw 'PR or main changed during review collection; repeat the read.'
    }
    return @{
        pull_request = $latest; main_sha = $main.sha
        contains_main = $comparison.status -cin @('ahead', 'identical')
        check_runs = @($checks.ToArray()); review_input = @($reviewInput.ToArray())
        collections_complete = $true
    }
}

Export-ModuleMember -Function Invoke-ScheduledApi, Get-ScheduledApiCollection, Get-ScheduledPullRequestSnapshot
