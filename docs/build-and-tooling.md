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

`just validate-local` includes package-scoped **mutation testing** as its final
step. Uncaught mutations are a very common cause of CI failures, so we run them
before pushing rather than discovering them in CI. Mutation testing is by far the
slowest step, so it runs last (after the cheaper checks have had a chance to fail
fast); scope it with `package="foo bar"` to keep it tractable. To run just the
mutation step on its own, use `just package="foo bar" mutants`.

We operate under a **zero warnings allowed** requirement - fix all warnings that
validation generates.

## Multiplatform codebase

This is a multiplatform codebase. In some packages you will find folders named
`linux` and `windows`, which contain platform-specific code. When modifying files
of one platform, you make the equivalent modifications in the other.

On a typical Windows PC with WSL installed, you can invoke any Linux commands
using the syntax `wsl -e bash -l -c "command"`. For example, to run the
standard validation on both Windows and Linux, execute:

1. `just validate-local`
2. `wsl -e bash -l -c "just validate-local"`

## Scripting

You can assume PowerShell 7 (`pwsh`) is available on every operating system and
environment. Prefer PowerShell 7 commands to Bash commands.

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
collection it enumerates. It runs as part of `just validate-local` and the CI `test-scripts` job.
Silence a genuine false positive with a justified
`[Diagnostics.CodeAnalysis.SuppressMessageAttribute(...)]`, never by relaxing the gate; the tree
is expected to be finding-free.

PSScriptAnalyzer can only see `.ps1`/`.psm1` files, so **nontrivial** inline PowerShell is not
linted where it sits. Keep inline snippets (justfile `[script]` blocks and workflow `pwsh` steps)
thin: anything with real logic - branching, loops, parsing, error handling, non-trivial data
manipulation - belongs in a module under `scripts/`, covered by a Pester suite
(`just test-scripts`) and thus by the linter, with the recipe or workflow step reduced to a thin
wrapper that imports the module and calls it. A snippet that is just the preamble plus a command
or two may stay inline.
