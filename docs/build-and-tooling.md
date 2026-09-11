# Build and tooling

This chapter covers the day-to-day mechanics of running commands in this workspace:
which command runner to use, how to validate changes, how to work across the two
target operating systems, and the scripting conventions for shell snippets and
recipes.

## Standard commands

We use the [`just`](https://github.com/casey/just) command runner for many common
commands. Look inside `*.just` files in `justfiles/` to see the list of available
commands. Some relevant ones are:

* `just build` - build the entire workspace.
* `just package="cpulist many_cpus" build` - target specific packages with a command.
* `just test` - test the entire workspace; this does **not** run doctests, use
  `just test-docs` for that.
* `just docs` - build API documentation.

The `package` argument must be the first argument to any `just` command, if used.

`just package=foo miri` uses nextest's automatically selected `default-miri`
profile, which reports slow tests and fails tests that exceed the per-test
deadline. See [Miri workload guidance](testing.md#keep-miri-workloads-small) for
the runtime budget and how to reduce or exclude unsuitable workloads.

Avoid running `just bench` (wall-clock Criterion benchmarks) without explicit
confirmation: they take a lot of time, and the numbers are also noisy and
machine-dependent - running them on a shared machine produces results that should
not be acted on. `just validate-local` runs `just test-benches-criterion`
instead, giving every Criterion target one smoke iteration without collecting
measurements. The combined `just test-benches` recipe used by CI also runs
Gungraun targets, which means a normal one-shot Valgrind run on Linux and a no-op
stub elsewhere.

`just bench-cg` (Callgrind / Gungraun) is different: it runs each scenario once
under Valgrind's CPU simulator, so the instruction counts and simulated cache
numbers are deterministic and unaffected by other processes on the machine. It is
safe to run `just bench-cg` (or `just package=foo bench-cg`) any time without
asking - including as a smoke test of a new Callgrind benchmark.

We generally prefer using Just commands over raw Cargo commands if there is a
suitable Just command defined in one of the `*.just` files.

Do **not** execute `just gh-release` — it performs real crates.io publishes and is
a CI-only entry point (driven by the release workflow); never run it manually.

Do **not** use VS Code tasks, relying instead on `just` and, if necessary, `cargo`
commands.

## Validating changes

Validate changes via `just validate-local`. This runs a number of different checks
and will uncover most issues. If you only touched a few packages, scope it to them
via `package="foo bar"`. It also smoke-runs the selected packages' Criterion
targets through `just test-benches-criterion`; see Standard commands for the
distinction between local and combined CI benchmark smoke passes.

`just validate-local` is always shallow validation. Use
`just package="foo bar" validate-deep-local` to run Miri, mutation testing,
many-seed Miri and careful checking on the current platform. These recipes are
independent: deep validation does not
implicitly rerun the shallow suite.

The **Standard validation** workflow performs shallow PR/push/merge-queue checks;
**Deep validation** runs the full deep suite on merged `main`.
CI composes their constituent commands into separately reported jobs
and diagnostic-producing matrix entries rather than running one monolithic local
recipe. Repair authors also run the particular deep checks needed to verify
their repair locally and link the results for review. Scheduling belongs to workflow orchestration, not
to the definitions of the local recipes. To run just mutation testing, use
`just package="foo bar" mutants`. Mutation timeouts and missed mutations remain
anomalies; changing enforcement cadence does not relax test-quality requirements.

See [scheduled validation](scheduled-validation.md) for readable failure reporting,
GitHub issue triage, repair ownership and reproducible Local App setup. Empty App
scans need only GitHub access; prepare Rust/WSL tooling in repair sessions when
applicable rather than on every polling-session creation.

We operate under a **zero warnings allowed** requirement - fix all warnings that
validation generates.

### Coverage target selection

`coverage-measure` uses Cargo's `--tests --examples` selection. `--tests` includes
library and binary unit tests and integration tests; `--examples` includes example
test harnesses. This works for library-only, binary-only and mixed package selections
without requiring every selected package to define a library.
See [Cargo target selection](https://doc.rust-lang.org/cargo/commands/cargo-test.html#target-selection).

Benchmark targets stay out of coverage and run through the separate `test-benches`
smoke pass. Do not opt benchmark targets into `test = true`, because Cargo includes
such targets in `--tests`. Doctests retain their separate `test-docs` pass.
The library-only selectors in many-seed Miri and exact library replays are deliberate
scope restrictions, not general test or coverage selection.

### Example execution

`just run-examples` prebuilds the selected examples before starting their runtime
watchdogs. The recipe prepares their environment through
`scripts/build/Examples.psm1`: it resolves Cargo's configured target directory once
with `cargo metadata --no-deps --locked`, exports the resulting absolute
`CARGO_TARGET_DIR`, and enables the examples' `IS_TESTING` smoke paths.

Passing the resolved directory avoids nested Cargo invocations from Criterion's
constructor and from measurement-report output. Criterion's own discovery uses
full-workspace metadata, which can resolve and download unrelated dependencies;
that preparation does not belong inside an example's runtime budget. Cargo remains
the authority for custom output-directory configuration.

Examples should complete within single-digit seconds. Criterion-based examples
use its single-iteration test mode by default rather than collecting benchmark
samples or performing statistical analysis. The watchdog is a last-chance
safeguard, not the expected execution budget. If it fires, the runner retains and
prints partial child output to identify the last completed phase.

## Multiplatform codebase

This is a multiplatform codebase. In some packages you will find folders named
`linux` and `windows`, which contain platform-specific code. When modifying files
of one platform, you make the equivalent modifications in the other.

On a typical Windows PC with WSL installed, you can invoke any Linux commands
using the syntax `wsl -e bash -l -c "command"`. For example, to run the
standard validation on both Windows and Linux, execute:

1. `just validate-local`
2. `wsl -e bash -l -c "just validate-local"`

## Automation language and boundaries

Prefer **nonpublished Rust utilities** for automation logic, especially structured
configuration parsing, data transformations and policy decisions. Reuse workspace
dependencies and validation conventions; an internal automation task is not a reason
to publish a new crate or introduce another language runtime.

Use PowerShell when executing Rust is impractical at the calling boundary. Examples
include bootstrapping the Rust toolchain, reporting a failed toolchain setup, or
coordinating native App operations before a prepared Rust environment is available.
Explain the actual constraint in the owning implementation guide and script rather
than treating familiarity with shell scripting as justification.

Keep the distinction between logic and process orchestration clear. A thin
PowerShell wrapper can prepare command arguments, invoke a trusted Rust utility and
propagate its outcome. Parsing and semantic decisions belong in the utility when
that environment can execute it.

For example, the failure-reporting job inside `deep-validation.yml` must work even
when Rust setup failed. It uses the runner's existing PowerShell and GitHub CLI
rather than building a reporting executable. Like the check jobs, it runs from the
workflow's main checkout; it simply needs Actions-read and issue-write permissions
to collect diagnostics and file an issue. See
[failure reporting](../.github/workflows/implementation.md#failure-reporting).

## Scripting

You can assume PowerShell 7 (`pwsh`) is available on every operating system and
environment. Where a script is justified, prefer PowerShell 7 commands to Bash
commands.

### Script purpose and decision comments

Every script, module and executable test fixture needs an inline purpose comment
explaining why it exists and how it participates in the repository's workflows.
Name its caller or entry point, the responsibility it owns and any important
authority or lifetime boundary. A filename, a list of exported functions or
"helpers for the workflow" is not an explanation. Test scripts identify the
contract or failure class they protect.

Document non-obvious decisions where they are implemented: why evidence is rejected,
why a state transition retains ownership, why a subprocess is isolated, or why an
operation must precede another. Link the relevant design or implementation heading
instead of duplicating a long rationale. Comments must describe supported behavior,
not development history or merely repeat the following statement.

Apply the same standard to workflow steps and just recipes. A step that implements
a design obligation needs a nearby justification with a link to that document's
relevant heading. Shared comments may explain a cohesive step group; ordinary
boilerplate does not need repeated narration.

### Every PowerShell snippet starts with the standard preamble

Every PowerShell snippet - each `[script]` block in a `.just` file, each `shell: pwsh`
`run:` step in a workflow, and every standalone `.ps1`/`.psm1` under `scripts/` - must start
with the standard preamble:

```powershell
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true
```

`Set-StrictMode -Version Latest` turns silent footguns (referencing an uninitialized variable,
reading a non-existent property, indexing out of bounds) into hard errors. The two preference
lines ensure that commands producing nonzero exit codes are treated as errors and fail the
script. (Standalone scripts additionally set `$VerbosePreference = 'Continue'`; module files set
strict mode once at the top rather than per function.)

### PowerShell linting

`just validate-scripts` runs [PSScriptAnalyzer](https://github.com/PowerShell/PSScriptAnalyzer)
over everything under `scripts/`, gating on Error/Warning findings. The rule set lives in
`PSScriptAnalyzerSettings.psd1`, supplemented by repo-local custom rules in
`scripts/analyzer/FoloAnalyzerRules.psm1` - which catch classes the built-in rules (and strict
mode) miss, such as a `foreach` whose loop variable case-insensitively collides with the
collection it enumerates. It runs as part of `just validate-local` and the CI `validate-scripts` job.
Silence a genuine false positive with a justified
`[Diagnostics.CodeAnalysis.SuppressMessageAttribute(...)]`, never by relaxing the gate; the tree
is expected to be finding-free.

The wrapper retains runtime/module metadata, full managed/inner exception diagnostics
and the analyzer's file/rule trace under `target/script-analysis/`. Standard validation
uploads those diagnostics even when the analyzer itself fails. Diagnostic collection
does not retry, suppress rules or turn an engine failure into a successful result.

PSScriptAnalyzer can only see `.ps1`/`.psm1` files, so **nontrivial** inline PowerShell
is not linted where it sits. Keep justfile `[script]` blocks and workflow `pwsh`
steps thin. Put automation logic in a nonpublished Rust utility when practical.
When the calling environment requires PowerShell, put nontrivial orchestration in
a module under `scripts/`, covered by Pester (`just test-scripts`) and the analyzer.
The recipe or workflow step then imports and invokes that boundary. A preamble and
a command or two may stay inline.

`just test-scripts` discovers the full PowerShell suite. Pass space-separated script-directory
domains, for example `just test-scripts "book release"`, to run their union. CI selects these
domains from changed inputs and native-helper dependency impact; script analysis and workflow
lint have independent selections. Main pushes retain full validation. See
[non-Cargo change planning](../.github/workflows/implementation.md#non-cargo-change-planning)
for ownership, shared-input rules and the lightweight workflow-lint environment.
