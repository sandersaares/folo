# Git workflow

This chapter covers conventions for working with git and GitHub pull requests.

## Creating GitHub pull requests

When creating PRs with `gh pr create`, do not pass the `--body` flag with an
inline string because PowerShell mangles backticks and special characters.
Instead, write the PR body to a temporary file and use `--body-file path/to/file.md`.

## Addressing pull request review comments

When addressing PR review comments, reply to each comment thread with the
disposition (what you did to address it) and mark the thread as resolved after
pushing the commit that addresses it.

## Version bumps

A pull request that changes a package's released content must increment that
package's version. The increment *is* the release: merge publishes. Do not bump
casually, and do not leave released-content changes without an increment.

Run the `increment-versions` skill to propose and apply levels. The author may
raise a level above the `cargo-semver-checks` floor; they may not lower one.

See [release-versioning.md](release-versioning.md).
