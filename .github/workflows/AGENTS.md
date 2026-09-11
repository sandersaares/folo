# GitHub workflows - agent instructions

Instructions for editing the workflows in this directory. For the design and its rationale,
see [design.md](design.md). Keep this file limited to actionable instructions; put
high-level design in `design.md` and per-job mechanics in inline YAML comments.

## When you change a workflow

- Update [design.md](design.md) when you change the design; do not record design or history
  here.
- Explain meaningful job/step decisions inline and link their precise owning heading in
  `.github/workflows/design.md` or `.github/workflows/implementation.md`. A shared comment may
  cover a cohesive step group; generic checkout/setup needs no repeated narration unless it
  establishes a trust or execution boundary.
- Validate before pushing with `just validate-workflows` (actionlint, which delegates to
  ShellCheck for embedded shell).

## Shell

- Every `run:` step uses `shell: pwsh`; prefer PowerShell over Bash. The `setup-environment`
  composite is the only exception - it bootstraps PowerShell itself.
- Every `run: pwsh` step opens with the standard preamble (`Set-StrictMode -Version Latest` plus
  the two error-preference lines); see `docs/build-and-tooling.md`.
- Keep steps and `just` recipes thin. Prefer nonpublished Rust utilities for structured parsing
  and policy logic; use PowerShell where Rust execution is impractical at the calling boundary.
  Follow [the language guidance](../../docs/build-and-tooling.md#automation-language-and-boundaries).
  Put reusable PowerShell orchestration under `scripts/`, covered by Pester (`just test-scripts`)
  and `just validate-scripts` (PSScriptAnalyzer); inline YAML is invisible to both.

## Toolchain versions

- Never hardcode toolchain versions. They are defined in `constants.env` and
  `rust-toolchain.toml`; call `just install-tools` / `just <command>` so versions flow
  through automatically.

## Job gating

- Gate non-Cargo checks on the explicit change plan, never on `delta.skip_all`.
  Maintain script-domain inputs and shared consumers in `scripts/build/ValidationPlan.psm1`;
  native-helper impact comes from Cargo delta and is unioned with path-selected domains.
  New test domains must join the full-suite selection. Preserve full tooling validation on
  pushes to `main`, and update the planner/fan-in tests when selection changes.
- Keep `validate-versions` unconditional, including its live binstall metadata check. It generates
  release state for every publishable package against its version anchor, so the PR's changed
  package set cannot skip a package that already needed an increment.
- Azure OIDC jobs (`test-azure`, `test-azure-gh`) must not run on `merge_group`. The test
  identity's federated subjects are `pull_request` and the `main` branch ref only.
- A workflow edit must consume the consumer-contract package set from the release-plan report
  rather than restating it in YAML. A package states this in its own manifest by declaring
  a private API, described in
  [the release validation guide](implementation.md#release-validation).

## Required-checks fan-in

- When adding a merge-blocking job to `standard-validation.yml`, add it to the `required-checks`
  job's `needs:` list. Never add it to the GitHub ruleset. Matrix jobs with a job-level
  `if:` that can be false can only be required through this fan-in. Advisory jobs
  (`coverage-notify`) and `alert` stay off that list. If the new job has no skip
  condition, also add its id to `MUST_SUCCEED_JOBS` in that job so a skipped result cannot
  green the fan-in. Change-selected jobs stay in `needs:` and must succeed whenever the plan
  selects them; update `RequiredChecks.psm1` when adding a new planned job. Keep `changes` and
  `delta` in the must-succeed list so unavailable plans cannot authorize skips.
- Add the new job to the `alert` job's `needs:` list as well. That list covers everything worth
  an issue after a failed push to `main`, including the advisory jobs the fan-in excludes, so the
  two lists are maintained together rather than derived from each other. Never add
  `required-checks` itself to `alert`: the fan-in reports a cancelled dependency as a failure, so
  depending on it would file an issue about a cancelled run.
- The job's GitHub check name is the literal `required-checks` (`name: required-checks`).
  Do not rename it.
- Merge-queue runs use the same pruned job set as pull requests. A `github.event_name ==
  'push'` guard that means "full matrix" must stay keyed on `push`, not on
  `!= 'pull_request'`, or a `merge_group` run would take the full matrix.
- Keep PR/push CI shallow. Repair PRs use ordinary required checks and human review of relevant
  deep-check results; do not introduce a repair registry or special merge gate.
- Keep deep validation full-scope and main-only, with failure reporting in the same workflow.
- Treat repair branches like other same-repository branches; do not add naming-based gates.
- Keep issue handoffs human-readable and all ownership on GitHub. App setup must not implicitly
  enable or run automations. See [Scheduled validation](../../docs/scheduled-validation.md).
