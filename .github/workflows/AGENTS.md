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

- A job whose inputs are not Cargo packages (the workflow files, or anything under
  `scripts/`) must run unconditionally - do not gate it on the `delta` job or `skip_all`, or
  a change touching only those files would be validated by nothing. Package-scoped jobs gate
  on `delta`. `validate-versions` is in this class: it generates release state for every publishable
  package's released content against its version anchor, so delta's changed-package set
  cannot skip a package that already needed an increment.
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
  green the fan-in.
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
- Keep `scheduled-repair-gate` unconditional and in the fan-in's must-succeed list. Read managed
  scope from the trusted default-branch policy and registered issue/attempt/head metadata.
  A personal account's unmarked PR is not a managed repair.
- Preserve independent hosted execution, reporting and Local admission controls; documentation
  or setup changes must not activate them. Keep shallow/deep local recipes policy-independent.
  Ordinary PR/push CI stays shallow; managed repairs retain their relevant deep gate.
  See [Operating policy](implementation.md#operating-policy).
- Privileged scheduled reporting must check out the default-branch controller, not a triggering
  candidate. Treat downloaded evidence as data and preserve reporter/worker record ownership.
- Build scheduled evidence utilities only from the trusted controller, never candidate source
  or downloaded artifacts. Include the Rust decoder and `.cargo/mutants.toml` in the checker
  contract digest; see [Evidence decoding](implementation.md#evidence-decoding).
