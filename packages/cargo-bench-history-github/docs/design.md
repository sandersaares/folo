# Design

`cargo-bench-history-github` is the unsupported GitHub automation companion for
`cargo-bench-history`. It owns the GitHub-specific envelope and lifecycle around reports while
the main tool remains independent of GitHub.

The complete action design lives in
[`../../cargo-bench-history/docs/reusable-action.md`](../../cargo-bench-history/docs/reusable-action.md).
This package implements that document's report-sink commands. It deliberately has no stable API
or command-line contract; the separately versioned action pins a tested companion version.

## Responsibilities

The companion:

* publishes and updates the rolling regression issue and pull-request comment;
* marks an existing report stale while a new benchmark run is in flight;
* replaces a recovered regression issue with an all-clear state and optionally closes it;
* reports and resolves workflow failures;
* retires pull-request placeholders after failure or when nothing benchmarkable changed; and
* reconciles an ambiguous create by looking up the hidden identity marker before retrying.

It embeds the Markdown summary rendered by `cargo-bench-history` verbatim. It never interprets
findings or re-derives analysis vocabulary.

## Identity

Every rolling artifact carries a hidden marker derived from the action instance and artifact
kind. Issues distinguish `regression` from `failure-alert`; pull requests carry one
`pr-comment` artifact per instance. Displayed titles are not identities and may be edited.

No issue labels are applied. Rolling issues are found by enumerating open issues and matching
the hidden marker in the body.

## Authentication

The real adapter reads `GITHUB_TOKEN`, falling back to `GH_TOKEN`, and uses the GitHub REST API.
No personal access token or other long-lived credential is introduced.

