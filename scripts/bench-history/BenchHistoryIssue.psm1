#requires -Version 7

# Rolling-issue filing for the benchmark-history workflow (.github/workflows/bench-history.yml).
#
# Replaces the former JasonEtco/create-an-issue action for BOTH issues this workflow files (the
# regression alert and the workflow-failure alert). Filing "one issue with a fixed title,
# updated in place when it already exists" is a short `gh` sequence, and hand-rolling it both drops
# a third-party dependency AND lets the rendered body live anywhere `gh` can read it (--body-file
# takes any path), so no scratch file has to sit in the repo checkout where `analyze`'s
# `git status --porcelain` dirty-check would see it. Both callers reach this one seam: the
# regression path via the thin gh-file-rolling-issue `just` recipe (its job already has `just`),
# the lightweight failure-alert job by importing this module directly (it skips the build-env
# setup). Keeping the find-or-file logic here is what lets the Pester suite
# (BenchHistoryIssue.Tests.ps1) exercise it against a mocked `gh` rather than only via a push to
# `main`. The one real GitHub-touching tool (`gh`) is isolated behind small seams the tests mock.

Set-StrictMode -Version Latest

# A transient GitHub blip (5xx, rate-limit, dropped connection) on a read should not fail the
# whole job, so the read-side gh calls below retry via the shared helper.
Import-Module (Join-Path $PSScriptRoot '..' 'utility' 'Retry.psm1') -Force

function Invoke-GhCapture {
    # Runs `gh` with the given arguments, capturing stdout and stderr SEPARATELY. stderr is
    # redirected to a temp file so it never contaminates stdout: `gh` can print warnings - e.g.
    # deprecation or rate-limit notes - to stderr while still exiting 0, and folding those into
    # stdout (a bare `2>&1`) would break the JSON/URL parsing the callers do. Returns the captured
    # stdout as a single string on success; on a non-zero exit, throws with whatever `gh` wrote
    # (stderr first, then any stdout) so the failure is never swallowed. Inspecting the exit code
    # ourselves - rather than letting a non-zero `gh` abort - is why the native-error toggle is off.
    # This is the single seam the Pester suite mocks (via `Mock gh`).
    #
    # Pass -RetryOnFailure ONLY for idempotent READS: it retries a transient-looking failure (HTTP
    # 5xx, rate limit, a dropped connection) a few times before giving up, while a deterministic
    # error still fails fast. Mutations must NOT set it - a retried create/edit/close could double
    # its effect.
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][object[]] $Arguments,
        [switch] $RetryOnFailure
    )

    $PSNativeCommandUseErrorActionPreference = $false
    # Name the invocation once for the failure message; also keeps the $Arguments read at the
    # function's own scope rather than only inside the retry closure below.
    $commandLine = "gh $($Arguments -join ' ')"
    $stderrFile = New-TemporaryFile
    try {
        $stderrPath = $stderrFile.FullName
        $invoke = {
            $stdout = (gh @Arguments 2>$stderrPath | Out-String)
            $exitCode = $LASTEXITCODE
            if ($exitCode -ne 0) {
                $stderr = Get-Content -LiteralPath $stderrPath -Raw
                $parts = @()
                if ($stderr -and $stderr.Trim()) { $parts += $stderr.Trim() }
                if ($stdout -and $stdout.Trim()) { $parts += $stdout.Trim() }
                throw "$commandLine failed (exit $exitCode): $($parts -join ' ')"
            }
            return $stdout
        }

        if ($RetryOnFailure) {
            return (Invoke-WithRetry -Attempt 4 -DelaySeconds 3 -BackoffMultiplier 2 -MaxDelaySeconds 30 `
                    -RetryOn { param($e) Test-TransientFailure -Message $e.Exception.Message } -Action $invoke)
        }
        return (& $invoke)
    }
    finally {
        Remove-Item -LiteralPath $stderrFile.FullName -Force -ErrorAction SilentlyContinue
    }
}

function Get-OpenIssueByTitle {
    # Returns the first OPEN issue whose title equals $Title exactly, or $null when none matches.
    # The list is narrowed to $Label, then the exact-title match is done client-side - the same
    # list-then-match approach the workflow's `resolve-alert` job uses to find the failure-alert
    # issue, which avoids the eventual-consistency lag of the GitHub search index that a
    # `gh issue list --search`/`gh search issues` query would hit. Isolates the real
    # `gh issue list` call so the tests can mock it.
    #
    # $Limit must stay well above the number of open issues the label can plausibly carry, because
    # `gh` returns them newest-first: the rolling issue is updated in place rather than refiled, so
    # it only ages relative to its label-mates, and anything past the limit is invisible here - a
    # miss would silently file a duplicate. The standing labels these callers use (`ci-failure`,
    # `regression`) are shared with the per-run failure issues that standard-validation.yml and release.yml
    # file and never auto-close, so a backlog is possible even though a healthy repository keeps
    # only a handful open. `gh` pages internally to satisfy the limit and stops once the results
    # are exhausted, so a generous ceiling costs a single request in the healthy case.
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string] $Title,
        [Parameter(Mandatory)][string] $Label,
        [int] $Limit = 1000
    )

    # Ask `gh` for the open issues carrying $Label as JSON; Invoke-GhCapture keeps stderr off
    # stdout so ConvertFrom-Json below always sees clean JSON even if `gh` emitted a warning.
    $output = Invoke-GhCapture -RetryOnFailure -Arguments @(
        'issue', 'list', '--state', 'open', '--label', $Label, '--limit', $Limit, '--json', 'number,title,url'
    )

    # A no-match list is the literal `[]`, which ConvertFrom-Json yields as an empty array; the
    # @() wrapper keeps a single-object result enumerable under strict mode.
    $issues = $output | ConvertFrom-Json
    foreach ($issue in @($issues)) {
        if ($issue.title -eq $Title) { return $issue }
    }
    return $null
}

function Publish-RollingIssue {
    # Files exactly ONE rolling issue: when an open issue with the exact $Title already exists its
    # body is updated in place (so a persisting condition never spams duplicates), otherwise a new
    # issue is created with $Label. The body is read by `gh` from $BodyFile, which may be any path
    # (for example the runner temp dir) - this is what frees the workflow from writing scratch files
    # into the repo checkout. $Label is the single label applied on creation, and also the label the
    # dedup search is narrowed to, so the next run finds the filed issue instead of duplicating it.
    # It must already exist in the repository - `gh issue create` fails outright on an unknown label
    # - so callers pass one of the repository's standing labels and reinstate it idempotently before
    # calling rather than inventing a workflow-specific one. Returns the issue URL.
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string] $Title,
        [Parameter(Mandatory)][string] $Label,
        [Parameter(Mandatory)][string] $BodyFile
    )

    if (-not (Test-Path -LiteralPath $BodyFile)) {
        throw "Issue body file '$BodyFile' does not exist."
    }

    Write-Verbose "Searching for an existing open issue titled '$Title' among issues labelled '$Label' before filing, so a regression that persists across runs updates one rolling issue instead of opening a duplicate every run."
    $existing = Get-OpenIssueByTitle -Title $Title -Label $Label

    if ($existing) {
        Write-Verbose "Found open issue #$($existing.number) ($($existing.url)); updating its body from '$BodyFile' rather than creating a duplicate."
        Invoke-GhCapture -Arguments @('issue', 'edit', $existing.number, '--body-file', $BodyFile) | Out-Null
        return $existing.url
    }

    Write-Verbose "No open issue titled '$Title' found; creating a new one with label '$Label' and body from '$BodyFile'."
    $output = Invoke-GhCapture -Arguments @('issue', 'create', '--title', $Title, '--label', $Label, '--body-file', $BodyFile)

    # `gh issue create` prints the new issue's URL on success; extract it (stderr is already kept
    # off stdout by Invoke-GhCapture), falling back to the trimmed output if no URL is present.
    $text = $output.Trim()
    $match = [regex]::Match($text, 'https://\S+')
    if ($match.Success) { return $match.Value }
    return $text
}

function Close-RollingIssue {
    # Closes EVERY open issue whose title equals $Title exactly among those carrying $Label, each
    # with an audit $Comment. The mirror image of Publish-RollingIssue: the workflow's `resolve-alert` job
    # calls this once the pipeline is green again so a fixed run does not leave the rolling
    # failure-alert issue rotting open. Closing ALL matches (not just the first) sweeps any backlog
    # of historical duplicates in one pass - the same list-then-exact-title approach
    # Get-OpenIssueByTitle uses, which sidesteps the search-index lag a `gh search` would hit.
    # Returns the numbers it closed (an empty array when none were open). The real `gh` calls go
    # through Invoke-GhCapture so the Pester suite can mock them.
    [CmdletBinding()]
    [OutputType([object[]])]
    param(
        [Parameter(Mandatory)][string] $Title,
        [Parameter(Mandatory)][string] $Label,
        [Parameter(Mandatory)][string] $Comment,
        [int] $Limit = 1000
    )

    # `--limit` defeats the 30-result default so a backlog of historical duplicates all come back;
    # see Get-OpenIssueByTitle for why the ceiling is generous rather than merely comfortable.
    $output = Invoke-GhCapture -RetryOnFailure -Arguments @(
        'issue', 'list', '--state', 'open', '--label', $Label, '--limit', $Limit, '--json', 'number,title'
    )

    # A no-match list is the literal `[]`, which ConvertFrom-Json yields as an empty array; the
    # @() wrappers keep a single-object result enumerable and .Count-safe under strict mode.
    $issues = $output | ConvertFrom-Json
    $matching = @(@($issues) | Where-Object { $_.title -eq $Title })

    if ($matching.Count -eq 0) {
        Write-Verbose "No open issue titled '$Title' labelled '$Label' to close; nothing to do."
        return @()
    }

    $closed = foreach ($issue in $matching) {
        Write-Verbose "Closing issue #$($issue.number) ('$Title') because the tracked condition has cleared; leaving comment: $Comment"
        Invoke-GhCapture -Arguments @(
            'issue', 'close', $issue.number, '--reason', 'completed', '--comment', $Comment
        ) | Out-Null
        $issue.number
    }
    return @($closed)
}

Export-ModuleMember -Function Get-OpenIssueByTitle, Publish-RollingIssue, Close-RollingIssue
