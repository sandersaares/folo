# Reusable GitHub Action — design (issue #284)

This is the design for packaging the `cargo-bench-history` *collect → analyze → report*
flow as a reusable, Marketplace-published GitHub Action — a sub-component of the tool,
referenced from [`DESIGN.md`](DESIGN.md). It describes the intended end state of that
action: its command surface, distribution model, configuration surface, and hosting. The
tool's own data model, commands, storage, auth, and analysis modes live in `DESIGN.md`;
this document does not restate them, only how they are wrapped for external consumers.

## 1. Problem & goals

Folo drives the tool from a set of in-tree workflows, all consuming the committed
`.cargo/bench_history.toml` (prod account baked in) through automation-only `just` recipes:

* [`bench-history.yml`](../../../.github/workflows/bench-history.yml) collects the
  workspace's benchmarks into Azure on **every push to `main`**, analyzes the accumulated
  history for trend regressions, and files a rolling **issue**.
* [`pr-bench-history.yml`](../../../.github/workflows/pr-bench-history.yml) runs the same
  measure-and-report loop **on every pull request**, retuned to ask "does this PR move any
  benchmark relative to `main`?" — collecting only the touched packages, analyzing the PR
  head against `main` in the tool's **branch mode**, and posting a rolling **PR comment**.
* [`bench-history-backfill.yml`](../../../.github/workflows/bench-history-backfill.yml) runs
  **nightly** and replays collection across a window of recent history, densifying each
  machine key's series so comparisons have enough neighbouring points to judge against. It
  has **no analysis phase and no sink** — the next push-triggered run analyzes whatever it
  managed to fill in.

The tool is generic, so the same flows should be consumable by **any** repository
without checking out our workspace or copying our `just` recipes. **The goal is a published,
versioned Action** — provisionally `folo-rs/cargo-bench-history-action` — that
generalizes the per-push history flow, the per-PR branch flow, and the nightly densification
pass into a small set of **parameterized commands**, takes its configuration from a
**committed `bench_history.toml` (or a `config` override)** rather than our `just` recipes,
and is listed on the GitHub Actions Marketplace. A consumer either calls the ready-made
**reusable workflows** that wire those commands into the standard job graph (§4.7), or
composes their own workflow YAML from the individual commands — instead of forking Folo's
hardcoded, repo-specific workflows.

**A load-bearing goal: trigger-agnostic, conflict-free.** The action must be usable from a
`schedule`, from `push` / `workflow_dispatch`, from `pull_request`, or from **several of
these at once in the same repo, without the invocations conflicting**. A repo may run a
scheduled collection *and* collect on every push to `main`; both can target the same commit.
The action therefore must not hard-code a single write-collision policy — see §4.5, where
the collect write mode is a caller-selected input, because no single policy fits both a
deliberately re-measuring run and an idempotent, cache-friendly collector. (The tool's own
bare default is a *hard error* on a duplicate commit — write-once immutability — and the
action keeps that same default, so a trigger-agnostic setup opts into `skip` explicitly, as
the README matrix does.)

**Both report sinks are first-class.** The per-push history view lands in a rolling
**issue**; the per-PR branch view lands in a rolling **PR comment** with its own lifecycle
(a while-you-wait placeholder, a staleness banner, and cleanup). These are genuinely
different reporting shapes, so the action exposes each as its **own command** (§4) rather
than a single analyze with a mode switch — the *kind* of analysis is inferred by the tool
from git topology (§4.5), while the *sink and its lifecycle* are what the caller selects.

Three further goals shape *how much* the consumer has to own, and where the logic lives.
They are stated here because they cut across every later section:

* **Minimal consumer surface.** Adopting the flow must cost a consumer a handful of lines,
  not a workflow. Folo's own bench-history CI is roughly **1,200 lines of YAML** across four
  workflows; none of that job wiring — the matrix, the artifact handoff, the concurrency
  groups, the fork gate, the sink lifecycle jobs — is repo-specific in *substance*, only in
  its parameters. The action therefore ships the whole job graph as **reusable workflows**
  layered over the composite action (§4.7), so the common case is a `uses:` line and a few
  inputs, and hand-assembly from the individual commands stays available for repos that need
  a different graph.
* **Logic belongs in Rust, not in shell.** Shell (PowerShell or otherwise) is the hardest
  layer in this system to test and the easiest to let drift from the tool it wraps. Folo's
  own sink layer demonstrates the cost: about **100 KB of PowerShell modules backed by 125 KB
  of Pester tests**, of which the largest module is nearly half pure Markdown composition —
  prose that must restate vocabulary the Rust side already owns. Every piece of behaviour
  that *can* live in a Rust binary should (§5.1), leaving the action's YAML as thin,
  near-logicless wiring. This is a testability goal first and a correctness goal second: a
  formatter that lives beside the data model it renders cannot drift from it.
* **Standard reports by default.** Consumers should not each invent their own comment and
  issue wording. The action posts **one standard, tested set of messages** (§5.2) so every
  consuming repo produces recognisably the same report, with narrow, declarative override
  slots for the few things that genuinely differ per repo, and a machine-readable escape
  hatch for anyone who wants to compose their own.

Non-goals (explicitly out of scope here):

* **No new analysis behaviour.** The action is a packaging layer; it changes how the tool
  is *invoked and distributed*, not what it computes. The push/PR distinction, branch mode,
  ghost exclusion, best-of noise reduction, and the machine-key model all already exist in
  the tool and its workflows — the action surfaces them, it does not invent them. (Moving
  *report composition* into the tool, §5.1, is a presentation change, not an analysis one:
  the findings are identical, only who renders them moves.)
* **Publishing the tool itself.** The action installs a released `cargo-bench-history` on
  demand, so it depends on the package being available from crates.io — with `binstall`-able
  prebuilt binaries for the fast path (see
  [`../../../docs/release-automation.md`](../../../docs/release-automation.md)). *How* the tool
  gets published is out of scope here: this design consumes a published tool, it does not
  define the release pipeline.

## 2. Where the action lives — a dedicated repository

**Decision: a dedicated repository `folo-rs/cargo-bench-history-action`, with
`action.yml` at its root.**

The Marketplace requires the action's metadata file to sit at the **repository
root**, and a Marketplace listing is tied to a whole repository's release/tag
stream. The monorepo already spends its tag namespace on automated `release-plz` package
releases and per-binary-package GitHub Releases (`cargo-bench-history-vX.Y.Z`, etc.; see
[`../../../docs/release-automation.md`](../../../docs/release-automation.md)), so it cannot
also carry the clean, independently-moving `v1` / `vX.Y.Z` action tags the Marketplace and
the floating-major convention expect. A dedicated repo gives the action its own semver
stream, its own README/Marketplace page, and a `uses:
folo-rs/cargo-bench-history-action@v1` reference that does not drag in the monorepo.

*Rejected — an in-monorepo sub-path action* (`folo-rs/folo/.github/actions/
bench-history@<ref>`). Sub-path actions work for `uses:` but **cannot be published
to the Marketplace** (root-only requirement) and would have to be versioned by
monorepo-wide tags that collide with the release automation. We keep one *internal* thin
composite action in the monorepo only if it helps us dogfood (see §10), but the
**published** artifact is the dedicated repo.

*Development flow.* The action is small — `action.yml` and the reusable workflows (§4.7) are
thin wiring, because the behaviour they invoke lives in Rust: the tool composes the report
bodies and a small companion binary performs the GitHub writes (§5.1). It is authored directly
in the new repo. Because most of the action's value lives in behaviour that is awkward to
test — a history that only becomes interesting once it accrues over many commits, and real
GitHub side effects (filing an issue, posting and updating a PR comment) — its CI leans on a
layered strategy (§9): fast Rust unit tests of the composition and transport, a local-storage
end-to-end pass on every push, and a synthetic-history sandbox pass (built on the published
`cargo-bench-history-faker` engine and the tool's hidden `import` command) that drives the real
GitHub-write paths. None of it needs the monorepo to be testable.

## 3. Binary distribution

External callers cannot `cargo run -p cargo-bench-history` against our workspace, so the
action must obtain a real `cargo-bench-history` binary. The published tool is the
`cargo-bench-history` package (the binary) plus the small private-use `cbh_*` packages it
depends on (all `publish = true`; see `DESIGN.md` §9), published together from CI to crates.io.
Every published release also carries **prebuilt, `cargo-binstall`-consumable binaries**
(per-target archives + `.sha256`, attached to the release; see
[`../../../docs/release-automation.md`](../../../docs/release-automation.md)), so the action
has both a from-source path and a download-a-binary fast path.

The action exposes an **`install-method`** input with four modes:

| `install-method` | How | When |
| --- | --- | --- |
| `binstall` | Install `cargo-binstall`, then `cargo binstall cargo-bench-history --version <v> --locked` | **Default.** Downloads the prebuilt archive from the package's GitHub Release (seconds), automatically falling back to a source build if no asset matches the runner target. Best for `cargo-bench-history`, whose Azure SDK + `mimalloc` dependencies are slow to compile. |
| `cargo-install` | `cargo install cargo-bench-history --version <v> --locked` | Pure source build, no extra tooling. Always works once published; pays the full cold-cache compile. The escape hatch when a prebuilt asset is unavailable or unwanted. |
| `path` | `cargo install --path <tool-path>/packages/cargo-bench-history --locked` | Dogfooding (§10): build the tool from a checkout of *this* workspace so Folo's own collection exercises `main`'s HEAD, not a release. |
| `none` | (nothing) | The caller has already put `cargo-bench-history` on `PATH` — a prior step, a devcontainer, a warmed tool cache — and the action just runs it. `tool-version` is ignored; the caller owns which build is used. |

**Which version, and why a tool release never forces an action release.** For the three
installing methods the version comes from the **`tool-version`** input (§7), which defaults to a
known-good release the action pins at release time (§8), so `@v1` is reproducible out of the
box. A caller can pin or bump `tool-version` at any time **without waiting for a new action
release**, and `tool-version: latest` opts into the newest published release for callers who
prefer currency over a pin. A new tool release therefore never *requires* a new action release —
the baked-in default advances only when we deliberately cut an action release to move the tested
action⇄tool pairing forward. The `none` method sidesteps versioning entirely.

**`binstall` — the fast default.** `cargo-binstall` resolves the package's GitHub Release,
verifies the `.sha256`, and unpacks the binary; if the runner's target has no published
asset it transparently source-builds, so the mode is always correct, only sometimes slow.
The action caches the resolved binaries with `actions/cache` keyed by `(runner.os,
runner.arch, resolved versions)` so even a source-fallback compile is paid at most once per
version per platform. The key uses the **resolved concrete versions**, never the literal
input: caching under `latest` would pin the first version ever resolved and quietly serve it
forever. `--locked` pins the published `Cargo.lock` for reproducibility.

**Two runtime binaries, resolved from one manifest.** A sink-using flow needs the tool *and*
the companion (§5.1). Each action release therefore carries a **release manifest** naming the
exact versions it was tested against — the tool, the companion, and (for the action's own CI
only) the faker. The two are versioned on different principles, because they answer to
different owners:

* The **tool** is a public, semver-stable dependency. The manifest supplies its default, and
  a consumer may override it via `tool-version` or track `latest`, as above.
* The **companion** is an action-internal implementation detail with no stable CLI, so it is
  **pinned by the action release** and is *not* caller-overridable. Letting `tool-version`
  drag the companion along would let a consumer pair an action with a helper that speaks a
  different internal protocol.

Each command installs only what it uses: `collect` and `backfill` need the tool alone, the
sink commands need the companion, and `analyze-*` needs both. `install-method: none` therefore
requires whichever of the two that command uses to be on `PATH` already, and `path` accepts a
location for each. Because `alert` exists precisely to report that a run failed, it must not
be able to fail for want of an install — its companion is fetched by the same cached,
checksum-verified path as everything else, and a release gate (§8.1) refuses to move the
floating major tag until every binary named in the manifest is resolvable.

**All install modes stay testable.** Method selection, version resolution, and the binary
cache are unit-tested in the same Rust layer as the rest of the action's logic (§5.1), with
mocked tool output; and the CI matrix (§9) additionally runs each *real* method (`binstall`,
`cargo-install`, `path`; `none` against a pre-seeded `PATH`) against a published version, so
both the branching and the actual installs stay covered.

**No test scaffolding is ever shipped to consumers.** `cargo install cargo-bench-history` (or a
`binstall` of it) installs **only** the real tool. The end-to-end test engine is a *separate
package*, `cargo-bench-history-faker`, with its own binary; installing the tool never pulls it
in (see `DESIGN.md` §9). The faker is itself `publish = true` but **unsupported** — its crate
root is `#![doc(hidden)]` and neither its library API nor its CLI carries a semver contract — so
it is on crates.io (and `binstall`-able) purely so a sandbox repo can run it without vendoring
(§9). A consumer of the action never installs it: it appears in the release manifest only for
the action's own test jobs. So the action needs no `--bin`
selector or any other guard against test binaries leaking onto a consumer's `PATH`; the install
commands above are the plain package-name form.

## 4. Action shape — one root action, a `command` selector

**Decision: a single composite action at the repo root with a required `command`
input**, one value per pipeline stage of the three flows. The values map **1:1 onto the
workflow jobs** the monorepo already factors the flows into (and, today, onto its `gh-*`
`just` recipes):

| `command` | Role |
| --- | --- |
| `collect` | Measure and store (per platform, in a matrix). |
| `backfill` | Densify recent history for this machine key; no analysis, no sink (§4.8). |
| `analyze-history` | Trend analysis of `main`, → rolling **issue** (once, after the matrix). |
| `analyze-pr` | Branch-vs-base analysis of a PR, → rolling **PR comment** (once). |
| `pr-comment-preflight` | Keep the PR comment honest at run *start* (staleness + in-progress seeding). |
| `pr-comment-cleanup` | Drop the PR comment when the PR touches nothing benchmarkable. |
| `alert` / `resolve-alert` | The failure-issue open / close lifecycle for the history flow. |

A single monolithic "do everything" action cannot express these shapes: `collect` runs
*per platform in a matrix* while every `analyze-*` runs *once, after the matrix*, and the
report-sink lifecycle steps (`pr-comment-preflight`, `cleanup`, `alert`, `resolve-alert`) run in
their own jobs at different points. A `command` selector keeps a single Marketplace listing
(only the root action is listed; sub-path actions are not) while letting each invocation play
one role. The split of `analyze` into `analyze-history` and `analyze-pr` is deliberate:
each carries a **cohesive, independently-validated input group** and a **different report
sink**, so a caller never has to reconcile issue-only inputs with comment-only inputs on one
command.

Inputs that do not apply to the selected command are **rejected, not ignored**: the action
validates the combination up front (e.g. `command: collect` with `issue-title` set, or
`command: analyze-pr` without a PR number) and fails with a clear error. Silently ignoring a
misplaced input is how a caller ends up believing a sink is configured when nothing will post.

These commands are the *building blocks*. Wiring them into the standard job graph is itself
boilerplate that no consumer should have to write, so the same repo also publishes reusable
workflows that do it — see §4.7.

*Rejected — a single `analyze` with a `mode`/`report-target` switch*: the tool already
infers history-vs-branch from git topology (§4.5), so a `mode` input would be redundant, and
folding the issue and PR-comment sinks (with their very different lifecycles) into one command
would produce one bloated, half-applicable input set. *Rejected — a single all-in-one
action*: cannot express matrix-collect + single-analyze + separate lifecycle jobs.

### 4.1 `collect`

1. **Install** the tool per `install-method` (§3).
2. **Collect** — one invocation, no surrounding logic:
   `cargo-bench-history collect [--config <config>] [--local=<path>] (--workspace [--exclude
   <pkg>…] | --package <pkg>…) [--bench <name>…] [--all-features] [--best-of <N>]
   [--machine-key <k>] [--overwrite | --skip-existing] --verbose`.
   * **Scope.** With no `packages` input the collect is workspace-wide (`--workspace`, minus
     any `exclude`), as the push flow runs it. With a `packages` input it collects **only
     those packages** (`--package` per name), as the PR flow runs it — the caller computes the
     touched, benchmarkable set (§6) and passes it in. A non-empty `packages` scoping is safe
     because branch-mode analysis only ever flags a series with a data point at the
     branch-unique head commit, i.e. exactly the packages collected (§4.5).
   * **Noise reduction.** `best-of <N>` (default 1; the workflows pass 3) runs the suite N
     times per commit and keeps each metric's minimum sample — runner interference is
     one-sided, so the minimum is the reading least perturbed by transient load.
   * **Write mode.** The write-collision policy is the caller's **`on-existing`** input, not
     a hard-coded flag (§4.5).
   * **`--config`** is passed **only when the `config` input is set**; otherwise the tool
     discovers the repo's committed `.cargo/bench_history.toml` (§5).
3. **Emit this leg's machine key.** After a *successful* collect, the action resolves this
   runner's real hardware fingerprint (`cargo-bench-history machine-key`) and exposes it as a
   `machine-key` **output**, so the caller can hand the exact keys measured this run to the
   later single analyze job (§4.6). A failed leg measured nothing, so it emits no key.

**Recollect (repair one historical point).** A `recollect-commit` input switches `collect`
to re-measure a single past commit and *overwrite* its stored point instead of appending the
pushed tip — the manual repair path for a data point corrupted by a badly degraded runner.
The action checks out that commit's code in a throwaway worktree while running the *current*
tool, so only the measured code, never the collection logic, comes from the past (the tool's
`backfill` command over a one-commit range with `--overwrite`). It needs full history
(`fetch-depth: 0`). This is a push/history-flow capability only; there is no recollect on a
PR, whose points are transient.

### 4.2 `analyze-history` (→ rolling issue)

1. **Install** the tool per `install-method`.
2. **Verify full git history (validate, don't mutate).** `analyze` splits the target's
   first-parent ancestry at its merge-base with the base, so a shallow clone that stops short
   of the branch point makes the merge-base unresolvable — a **hard error** in the tool
   (`DESIGN.md` §7.3/§8.5). The action runs `git rev-parse --is-shallow-repository` up front
   and **fails fast** with an actionable message — "check out with `fetch-depth: 0`" — rather
   than surfacing the tool's later error or silently `git fetch --unshallow`-ing someone
   else's working tree (the idiomatic place to control depth is the caller's own
   `actions/checkout`).
3. **Choose a scratch directory *outside* the checkout.** All rendered artefacts — the
   reports, the condensed summary, and the `--cache` mirror — are written under a
   runner-temp scratch dir (`${RUNNER_TEMP}/bench-history`), never inside the working tree.
   This is load-bearing: `analyze` labels the analyzed tip with the repo's dirty state via
   `git status --porcelain`, so an artefact written into the checkout would make an
   otherwise-clean `main` look dirty and mis-annotate the tip.
4. **Analyze**:
   `cargo-bench-history analyze [--config <config>] [--local=<path> | --cache <dir>]
   --engine all --target-triple all --machine-key <k>… --no-text --markdown <scratch>/report.md
   --json <scratch>/report.json --markdown-summary <scratch>/summary.md [--since <window>]
   --verbose`.
   * **Analysis mode is inferred, not passed.** With the context left at `main`'s tip (its own
     merge-base, clean), the tool auto-selects **history** mode — long-range change-point and
     drift detection reporting regressions only (§4.5). The action passes no mode flag because
     none exists.
   * **Machine keys.** The facets default to surveying every engine and triple (`all`), but
     the machine key is **not** `all`: it is the exact set of fingerprints collected this run,
     threaded from the collect matrix (§4.6), so the survey is scoped to the machines that
     actually measured this commit and never mixes in a stray key from the shared store.
   * **Cache.** `--cache <dir>` (a read-through mirror of the cloud history persisted across
     runs via `actions/cache`) turns repeated full-history downloads into a warm-cache read;
     it applies to the **cloud backend only** and **conflicts with `--local`**, so the action
     passes at most one.
   * A single pass emits all three machine-readable artefacts via the per-format output
     toggles: the full Markdown report, the full JSON report, and the **condensed
     top-findings Markdown summary** (`--markdown-summary`) sized to fit an issue body.
5. **Surface the notable flag** as a step output, read as a first-class value from the
   analysis rather than scraped out of a report by shell (§5.1), and written with the report
   paths and a regression count to `$GITHUB_OUTPUT` (§7). This is the signal the rest of the
   flow gates on, so it never depends on report prose that is free to change.
6. **File the rolling issue** when `issue-on-regression: true` *and* `notable == true`. The
   full Markdown + JSON reports are uploaded as a single run **artifact** (they can exceed
   GitHub's 65,536-character issue-body limit); the rolling issue is filed by the **companion
   transport binary** (§5.1) — search open issues for the fixed title, then create-or-update —
   whose body is the tool-composed summary plus a link to that artifact. Owning the transport
   rather than delegating to a third-party action avoids a bundled dependency and keeps the
   dedup logic explicit and unit-tested. The regression issue is
   **not** auto-closed on a later clean run: a regression may be knowingly accepted or slow to
   self-clear, so it stays open until dismissed by hand (matching the monorepo split, issue
   #292). The caller grants `issues: write`.

**Empty-run degeneracy.** When every collect leg failed there are no machine keys to thread,
hence no new data at this commit; the action writes a non-notable placeholder report set and
skips the tool, leaving the notification to the failure lifecycle (`alert`/`resolve-alert`, §4.4).

### 4.3 `analyze-pr` (→ rolling PR comment)

Structurally the same install → validate-history → scratch-outside-checkout → analyze →
notable pipeline as `analyze-history`, retuned for the PR branch view and a different sink:

* **Checkout is the PR head's *real* commit, with full history** — not the synthetic
  `pull_request` merge ref, whose first parent is the base branch and would corrupt the
  comparison. The caller checks out `pull_request.head.sha` with `fetch-depth: 0` (which also
  populates the remote-tracking ref the merge-base needs).
* **Branch mode is inferred from an explicit base.** The action passes `--context HEAD --base
  <base>`, where the base is **the PR's own base ref** taken from the event — not a hardcoded
  branch name, so a repo whose trunk is not `main`, or a PR targeting a release branch, is
  compared against the right thing. Because the PR head is ahead of its merge-base with the
  base, the tool auto-selects **branch mode** — it judges the branch by its **tip commit**
  against the recent base level, discarding the branch's own intermediate history (only the tip
  lands on the base, so intermediate commits say nothing about the merge's effect) and
  comparing just the newest commit's runs, in both directions (`DESIGN.md` §8.5). `--base` is
  passed explicitly because the PR head is checked out detached, so there is no local branch to
  auto-resolve.
* **Base-lag is surfaced, not hidden.** Because every engine is now machine-keyed (§4.6) and
  the tool never compares across machine keys, a PR built on one runner of a rotating public
  pool may find usable base data only under *its* key — while the newest base commits ran on a
  *different* machine. Branch-mode findings therefore carry a per-result warning when the
  comparison base lags the merge-base (distinguishing "a newer base run exists but under a
  different machine key" from "no base data at more recent commits"), so a comment comparing the
  tip against a state several commits back says so instead of looking authoritative. The action
  needs nothing for this — it is inherent to branch-mode analysis — but it is why the machine-key
  handoff and the honest scope disclosure matter on GitHub's shared runners.
* **Both directions are reported, without a flag.** Branch mode always reports regressions
  *and* improvements; history mode always suppresses improvements. The direction filter is a
  property of the mode, applied *before* the false-discovery correction so the correction only
  ever sees candidates that mode would actually report. There is no flag to set — an earlier
  `--include-improvements` was removed once it was made inert in branch mode and redundant in
  history mode, so passing it is now a hard argument error.
* **Scoping falls out of collection, never a name filter.** Analysis is deliberately *not*
  package-scoped: benchmark identities are engine-dependent, so an id-prefix filter would
  silently drop some engines' series. Instead the tool's **always-on ghost exclusion** analyzes
  only benchmarks present at the context commit (the PR head), and since only the touched
  packages were collected there, every untouched package drops out as a ghost automatically —
  for every engine. This needs nothing from the action: ghost exclusion is inherent to analysis
  and has no opt-out flag, so the scoping is correct by construction.
* **Cache is restore-only.** PR runs read the shared history cache but never save, keeping the
  baseline warm without accumulating per-PR cache entries (safe against the append-only store
  even when slightly stale).
* **Sink: a rolling PR comment.** The condensed summary is posted as a single comment on the
  PR, deduped by a hidden marker and updated in place on every push, by the companion
  transport binary (§5.1). The
  comment is strictly advisory (findings never affect the check's exit code), reports
  improvements alongside regressions or a plain "no regressions" state, and **states its
  collection scope** — which packages were benchmarked — so a clean result is never mistaken
  for the whole suite being clean. It records **which commit it measured** (a bare full SHA
  that GitHub autolinks, plus a hidden full-SHA marker) so staleness can be judged later
  (§4.4). A run *failure* surfaces only as the red check — no comment — because a PR failure
  is transient, not the persistent condition the issue lifecycle tracks. The caller grants
  `pull-requests: write`.
* **Never publishes already-stale results.** A run takes hours, so its results can be obsolete
  by the time it posts. Two guards cover the finish side. First, the `analyze-pr` job is gated
  on `!cancelled()` (not `always()`): a *superseded* run — cancelled by the next push's
  concurrency group (§8) — never reaches the post step, while a merely partially-failed collect
  still reports what landed. Second, for the narrow window where a new push *races* the final
  post faster than cancellation can stop it, `analyze-pr` **re-reads the live PR head just
  before posting** and, when it no longer matches the analyzed (frozen) SHA, injects the same
  staleness banner into the body *before* posting — so a superseded result never appears fresh.
  The check **fails closed**: if the live head cannot be read at all, the body is posted with a
  "freshness could not be verified" note rather than with an implicit claim of freshness. The
  degradation is in the *precision* of the banner (an exact commit distance may be
  unavailable), never in whether the reader is warned.

### 4.4 Report-sink lifecycle commands

Because a full run takes hours and a new push *cancels* the in-flight one (§8 concurrency),
each sink needs upkeep beyond the single analyze that publishes results. These are **explicit
commands** so the caller schedules them in their own jobs at the right point:

**PR-comment sink (branch flow).**

* **`pr-comment-preflight`** runs at the *start* of each run, in parallel with the multi-hour
  collect. When the PR has **no comment yet**, it seeds a *"benchmarking in progress"*
  placeholder (same hidden dedup marker, disclosing the collection scope) so the author knows
  results are coming; on later pushes it refreshes that placeholder's scope. When a comment
  **already carries results**, it prepends a staleness banner — *"N commits behind HEAD"* (the
  distance from the GitHub compare API's `ahead_by`, which needs no clone and still resolves a
  force-pushed commit), degrading to a numberless *"out of date"* when the two share no history
  or the marker is absent. The banner is sentinel-bounded so a re-run *replaces* rather than
  stacks it, and the next completed `analyze-pr` — which rewrites the body from scratch —
  drops it automatically. The banner wording is shared with `analyze-pr`'s finish-side
  self-check (§4.3): this start-of-run pass only flags a *prior* run's stale results, while the
  self-check guards *this* run's own results at post time, so both angles are covered. This
  command is gated on the same non-empty scope as collect, so it never races the cleanup path.
* **`pr-comment-cleanup`** runs when a PR touches **no** benchmarkable package (including a PR
  that touched one earlier and then reverted): it removes any rolling comment a prior push left
  behind and posts nothing, so a stale, misleading comment never lingers. It is a no-op when
  there was no comment.
* **`pr-comment-finalize`** runs *last*, gated on failure, and closes the one hole the two
  above leave open. A placeholder promises results are coming; if collection or analysis then
  fails, `analyze-pr` deliberately posts nothing (§4.3), so without a terminal step the
  placeholder would sit on the PR claiming work is in progress forever — the run that would
  have replaced it is gone, and only a *further push* would ever revisit it. Finalize replaces
  the placeholder with a short terminal notice pointing at the failed run. It fires only for
  genuine failure, not cancellation: a superseded run is *expected* to leave the placeholder
  standing, because the run that cancelled it is about to refresh it.

**Issue sink (history flow).** Symmetrically, the *failure* of a run — distinct from a
regression *finding* — is a recurring condition on a rolling target, so it gets its own
deduplicated tracking issue that **does** auto-close on recovery (unlike the manual regression
issue of §4.2):

* **`alert`** opens or refreshes a dedup failure issue when the run fails.
* **`resolve-alert`** closes it when a subsequent run succeeds.

Both use the same rolling-issue path in the companion binary as §4.2, keyed on a distinct
failure identity. Every message named in this section — placeholder, staleness banner,
terminal failure notice, failure alert — has no analysis behind it, which is precisely why
the companion owns the whole catalogue (§5.1) rather than splitting it with the tool.

### 4.5 Collect write mode, inferred analysis mode, recollect

**Collect write mode is an input, not a constant.** Because the action is trigger-agnostic
(§1), `collect` maps the `on-existing` input onto the tool's write-collision flags — `skip`
→ `--skip-existing`, `overwrite` → `--overwrite`, `error` → neither (**the default**, the
tool's own strict write-once behaviour). This is the concrete mechanism that lets a scheduled
collection and an on-push collection land on the same commit without a hard failure: such a
setup sets `on-existing: skip`, and `skip` additionally keeps the read cache (§4.2) valid
because a skipped write never arms its invalidation marker. Keeping the default at `error`
matches the CLI, so a caller who does nothing special gets the tool's own conservative
behaviour.

**`backfill` is the one exception, and it is a per-command default.** Densification (§4.8) is
defined by being resumable: it walks a window that it has usually already partly filled, so
erroring on an existing point would fail every run after the first. `on-existing` therefore
defaults to `skip` for `backfill` and to `error` everywhere else. The default is resolved
**per command** rather than as one action-wide constant, so an omitted input means "this
command's sensible default", and an explicitly set one always wins.

**PR measurements are expected to be orphaned, and that is fine.** Points are keyed by commit,
so a repository that **squash- or rebase-merges** — the default on many projects — discards
the very SHAs the PR flow measured: the branch commits never land on the trunk, and the
squashed commit is one nobody has benchmarked. PR-collected data therefore has a *shorter*
useful life than history-flow data: it exists to answer "does this change move anything?"
while the PR is open, and afterwards it is dead weight that no trunk analysis will ever
select. This is a deliberate acceptance, not an oversight. Two consequences follow, and both
are load-bearing elsewhere in this design: the trunk series is fed by the **history flow and
the densification pass**, never by PR runs, so nothing downstream depends on PR points
surviving; and because those points are disposable, a PR run has no need to *write* to the
shared store at all, which is what makes the read-only fork tiering of §6 possible.

**Analysis mode is inferred by the tool, not selected by the action.** There is no `--mode`
flag: `analyze` auto-detects **history** vs **branch** from git topology and the recorded
runs it admits (`DESIGN.md` §8.5) — the analyzed tip being its own merge-base with no dirty
run means history; anything past the merge-base means branch. The action does not expose a
mode input; instead its two analyze *commands* pre-wire the inputs that put the tool in the
right mode: `analyze-history` leaves the context at the base tip (→ history), `analyze-pr`
passes an explicit `--base` distinct from the PR-head context (→ branch). Future analysis
questions, if any, arrive as **new command values** with their own cohesive input group and
sink, never as an orthogonal mode parameter grafted onto one overloaded command.

**`examine` is a human follow-up, not a CI command.** The tool's `examine` drills into a
single `(benchmark, metric)` series and is what a maintainer runs by hand after a report
points at a finding. The action does not wrap it — it is interactive triage, outside the
collect/analyze/report loop.

### 4.6 Machine-key handoff between collect and analyze

Collection stamps **every** result with the runner's **real hardware fingerprint**, and
analysis must survey exactly those keys — but collection is a matrix across a heterogeneous
runner pool while analysis is one job that cannot re-derive those keys from its own hardware.
The action therefore treats the handoff as a first-class concern:

* `collect` exposes the leg's fingerprint as its `machine-key` output (§4.1); the caller
  uploads it as a per-platform **artifact**.
* `analyze-history` / `analyze-pr` take a `machine-keys` input — a directory of the
  downloaded per-platform key files — and thread each as a repeated `--machine-key
  <fingerprint>` argument. The action scans that directory itself rather than making the
  caller build the argument list (§5.1).
* **The download must be token-authenticated.** When a workflow is partially re-run, the
  artifacts it needs were produced by a *previous* attempt, and `actions/download-artifact`
  only resolves across attempts when it is given an explicit `github-token`. Without it a
  partial re-run 404s on the machine-key artifact and the analyze job dies. The reusable
  workflow (§4.7) wires the token in by default; a hand-assembled caller must do it.

**Fingerprints are versioned, and a version bump partitions history.** The fingerprint is
derived from the usable hardware the runner actually exposes, and it carries an explicit
version tag. When the derivation changes — as it did when the Linux side moved from the
kernel-padded ID-space *width* to the *count* of usable processors and memory regions, and
again when an unstable-across-reboots reading was dropped — every affected runner starts
filing under a **new key**. Nothing is lost, but old points sit under the old key and no
longer join the new series, so a consumer sees a coverage gap until enough new points
accumulate (which is what the nightly densification pass, §4.8, exists to shorten).

**All** engines are machine-keyed — Callgrind (instruction counts) and `alloc_tracker`
(allocation bytes/counts) no less than the wall-clock engines (criterion and `all_the_time`) —
because even integer-count metrics turned out to be machine-dependent (libraries dispatch to
microarchitecture-specific code paths). There is no ride-along exemption: a result is only
analyzed when its own machine key was threaded in, so the handoff above is what makes *any*
engine's data visible to analysis, not just the wall-clock ones. Whether the artifact
upload/download is bundled into the composite action (via `actions/upload-artifact` steps) or
left to the caller is an implementation choice (§12); either way the tool stays GitHub-agnostic
— it only ever sees `--machine-key <fingerprint>`.

### 4.7 Two consumption layers — reusable workflows over composite actions

The commands above are *building blocks*. Assembling them into a working setup means writing
the same job graph every consumer needs: a matrix `collect` across platforms, the machine-key
artifact handoff, a single `analyze` gated on the matrix, the sink lifecycle jobs, plus
concurrency, permissions, and (for PRs) the fork gate. That graph is identical everywhere
except for its parameters, so making each consumer retype it is exactly the repo-specific
bulk this design set out to remove.

The action repo therefore publishes **two layers**:

* **Reusable workflows** (`workflow_call`) — the default path. One per flow:
  `history.yml`, `pr.yml`, and `backfill.yml`. Each owns the entire job graph, and the
  consumer's whole workflow reduces to a trigger, a `uses:` line, the permissions the flow
  needs, and a few inputs:

  ```yaml
  on:
    push: { branches: [main] }
  jobs:
    bench-history:
      permissions:
        contents: read      # checkout
        actions: read       # cross-attempt artifact download (§4.6)
        id-token: write     # Entra OIDC, if using cloud storage
        issues: write       # rolling issue + failure alert
      uses: folo-rs/cargo-bench-history-action/.github/workflows/history.yml@v1
      with:
        platforms: '["ubuntu-latest", "windows-latest"]'
        azure-client-id: ${{ vars.AZURE_CLIENT_ID }}
        azure-tenant-id: ${{ vars.AZURE_TENANT_ID }}
  ```

* **Composite actions** — the escape hatch, and what the reusable workflows are built from.
  A consumer whose graph differs (extra gating, an unusual runner pool, a different sink)
  calls `collect` / `analyze-history` / `analyze-pr` / the lifecycle commands directly and
  wires the jobs themselves. Nothing is hidden from them; the reusable workflow is a
  convenience, not a privileged path.

Four platform constraints shape this split, and none is worked around:

* **The Marketplace lists actions, not workflows.** Reusable workflows are referenced by
  repository path and ref (`owner/repo/.github/workflows/x.yml@v1`) and cannot be published
  to the Marketplace. The composite action therefore stays the Marketplace-listed artifact
  and the discovery surface (§8); the reusable workflows ride the same repo and the same
  `v1` floating tag, so both layers version in lockstep with one release. Inside the
  reusable workflows the root action is referenced through the **self-repository syntax**
  (`$/`), which resolves at the exact commit the workflow is running from — a hardcoded
  `@v1` there would let a workflow pinned to `v1.2.3` silently invoke a newer action.
* **`workflow_call` inputs are scalars.** Only `string`, `number`, and `boolean` exist, so
  list-shaped inputs (the platform matrix, package scopes, labels) are passed as **JSON
  strings** and expanded with `fromJSON` inside the reusable workflow. This is why
  `platforms` above is a quoted JSON array. The composite layer, which has no such
  restriction, keeps its more forgiving newline/space lists; the reusable workflow is the
  one place that converts.
* **A caller matrix fans out the whole invocation, not one phase of it.** A matrix job *can*
  call a reusable workflow, but it re-instantiates the entire workflow per combination —
  which would give one analyze per platform instead of the single aggregate analyze the
  design requires. The collect matrix therefore lives **inside** `history.yml`, driven by the
  JSON `platforms` input, so many collect jobs converge on one analyze. This is the main
  reason the reusable-workflow layer is worth having at all: the fan-out-then-converge shape
  is the single most awkward piece for a consumer to reproduce.
* **Secrets and permissions do not flow implicitly.** `secrets: inherit` only works when
  caller and called workflow share an organization or enterprise, so it cannot appear in a
  recipe aimed at any repository; the called workflow gets `github.token` regardless, and
  anything else is an explicit input. Permissions can only be **narrowed** by a called
  workflow, never widened, so the caller job must grant them — which is why they appear in
  the example above rather than being hidden. For the same reason, cloud credentials are
  passed as **inputs** (non-secret client/tenant identifiers, with the OIDC token minted
  inside), not by expecting the consumer to run a login step first: a caller job that
  delegates to a reusable workflow cannot contain steps at all.

**The consequence worth stating plainly: the reusable workflows serve repos that need no
custom preparation to build their benchmarks.** Because a calling job cannot carry steps, a
repo that must install system packages, start a service, run a code generator, or invoke its
own setup action before `cargo bench` will work cannot express that through the `uses:` layer.
Two mechanisms cover that, and which one is right depends on how far the repo deviates:

* A **`setup-action` input** naming a repository-local action (path or `owner/repo@ref`) that
  each job runs immediately after checkout, before touching the tool. It is one hook, run at
  one point, which keeps the graph fixed while letting the environment vary — enough for the
  common "we have a `setup-environment` action" case, and it is what Folo's own migration
  (§10) needs, since its jobs depend on exactly such an action.
* **Dropping to the composite layer** for anything more structural. A repo needing steps at
  several points in the graph is telling us its graph differs, which is what the escape hatch
  is for.

Claiming otherwise would overstate the reach of the default path: the collapse to a handful of
lines is real for repos whose benchmarks build from a plain checkout plus a toolchain, and the
`setup-action` hook stretches that to most of the rest, but not to every repository.

### 4.8 The densification flow (`backfill`)

Analysis needs *neighbouring* points, not just the commit under test: a lone measurement on
a fresh machine key has nothing to be judged against. Since each push measures only its own
commit — and a fingerprint version bump (§4.6) or a newly-added runner platform starts an
empty series — a series can stay too sparse to judge for a long time. A third flow closes
that gap.

* **`backfill` replays collection across a window of recent history**, walking **newest
  first** so a run that exhausts its time budget has spent it on the most
  comparison-relevant commits rather than the oldest ones. It is **resumable by default**:
  commits already stored for this key are skipped, so a truncated run simply continues
  next time, and only an explicit overwrite re-measures.
* **It has no analysis phase and no sink.** Densification only *writes*; the next
  push-triggered `analyze-history` picks up whatever landed. This keeps the flow free of
  report-sink concerns entirely — no issue, no comment, no staleness.
* **Its natural trigger is `schedule`**, which is precisely the trigger-agnostic case §1
  calls out: a nightly densification pass and a per-push collector can target the same
  commit, so the write mode must be caller-selected rather than assumed (§4.5).

## 5. Configuration and the division of labour

This section settles two related questions: what a consumer must *supply* to the action, and
where the action's behaviour actually *lives*. Configuration comes first, because it is the
smaller half — the interesting decision is the second one.

**Configuration is a committed file, not a scatter of inputs.** The tool already reads a
committed `.cargo/bench_history.toml` that describes *where*
history lives: `[project] id` (the partition key) and the `[storage.*]` backend. A generic
consumer is **assumed to commit that file**, exactly as the tool expects when run locally,
so the action does **not** synthesise a config from a scatter of per-field inputs. The unit
of configuration is a whole, reviewable, version-controlled file — not a dozen action
inputs.

* **Default** — no `config` input: the tool discovers `.cargo/bench_history.toml` in the
  checked-out repo and the action passes no `--config`.
* **Override** — the single **`config`** input (a path): passed verbatim as `--config
  <path>`. A caller who does not want the in-repo file, or who must assemble one from
  secrets at run time, writes their own config in a prior step and points the action at it.

**Runtime behaviour still comes from action inputs**, passed as CLI flags rather than baked
into the config file: `local-path` (→ `--local=<path>`), `cache` (→ `--cache <dir>`, cloud
read-through cache, mutually exclusive with `--local`), the `collect` scope (`exclude` /
`packages` / `bench`), `best-of`, `on-existing` write mode, `recollect-commit`, `machine-key`
override and the `machine-keys` handoff directory, the `analyze-pr` `base`, and `since`.
The division is clean: the **config file says where history lives; the action inputs say what
this run does**.

### 5.1 Where the logic lives — Rust binaries, not shell

Folo's current sink layer is **PowerShell**: roughly 100 KB of `scripts/bench-history/*.psm1`
modules backed by 125 KB of Pester tests. That layer works, but it is the wrong home for this
logic, and its shape shows why. The largest module is nearly half **pure Markdown
composition** — it decides how a coverage shortfall reads in prose, how a staleness banner is
worded, how a package list is pluralised — and to do that it must restate vocabulary the Rust
side already owns. Every wire name the tool can emit (each reason a series went unjudged, for
instance) has to be mirrored by hand in a `switch` statement, so a reason added to the tool
silently renders as nothing until someone notices. That is drift by construction, and no
amount of Pester coverage fixes it: the tests can only assert what the module's author
believed the tool emits.

**The rule this design adopts: if logic can live in a Rust binary, it does.** The remaining
YAML holds wiring only — inputs to flags, files to steps — never decisions.

The split is drawn by **vocabulary ownership**, not by the more obvious-looking
"composition versus transport" line. Two questions separate cleanly:

* *What do these numbers mean?* — findings and their direction, the coverage census and the
  reasons a series went unjudged, which commit a change point sits near. This vocabulary is
  the tool's, and it changes when the analysis changes.
* *What does a GitHub report look like?* — the heading, the hidden dedup marker, the
  analyzed-commit marker, the artifact link, the staleness banner, the in-progress
  placeholder, the failure alert. This vocabulary is GitHub's, and it changes when the
  reporting changes.

**Domain rendering stays in the tool.** The tool already renders reports (`--markdown` for
the full report, `--markdown-summary` for a condensed one sized to fit a GitHub issue body,
and `--json`), and it already computes the coverage census. It keeps that job and gains one
addition: the **coverage verdict in prose** — the sentence explaining *why* the judged set
fell short — because that is the piece the shell formatter had to reproduce by hand-mirroring
the tool's own reason names, and therefore the piece that drifts. `notable` likewise becomes a
first-class output rather than something callers read back out of the JSON. Nothing
GitHub-shaped enters the tool: it never learns what a comment marker or an artifact URL is.

**The GitHub envelope and the whole sink lifecycle live in the companion binary.** It wraps
the tool's rendered summary in the report body, adds the markers and the artifact link, and
owns *every* message — including the ones with no analysis behind them: the in-progress
placeholder, the staleness banner, the terminal failure notice, and the rolling failure
issue. It then performs the API work: finding the rolling comment by its marker, deciding
create-versus-edit, deleting on cleanup, opening and closing the failure issue, and reading
the live PR head to detect a race.

**Ordering is why the envelope cannot live in `analyze`.** Two of the values the body needs
do not exist when analysis runs. The report **artifact URL** only exists after the upload
step, which necessarily follows analysis; and the **live head distance** is deliberately read
as late as possible, immediately before posting, precisely so it catches a race that analysis
could not have seen (§4.3). A body composed during `analyze` would have to be rewritten
afterwards by whatever posts it — which is exactly the string-patching this design is trying
to eliminate. Composing in the companion, at post time, is the only ordering that works.

The companion stays **separate from** `cargo-bench-history` because the tool is a general
benchmark-history tool with zero GitHub API calls, and that neutrality is worth preserving: a
repo publishing to GitLab, or to nothing at all, should not install GitHub plumbing. Both
halves are still Rust and still unit-tested — the companion separates its **pure body
rendering** from a **fakeable GitHub client**, so every message is asserted without a network,
exactly as the sink bodies are pinned in §9's Layer 1.

Drift is prevented not by keeping the prose in one binary, but by making the companion
**embed** what the tool rendered rather than re-deriving it. The companion never maps a
finding kind or an unjudged-series reason itself; those arrive already rendered. If a new
reason appears in the tool, it flows through untouched.

**What is left for YAML — and what stays outside Rust entirely.** After the move, the action's
steps are one-line invocations. Three things deliberately do not move:

* **Environment and credential wiring** — mapping federation variables into `$GITHUB_ENV`,
  creating a scratch directory. This is the runner's own contract; expressing it in Rust would
  mean shelling back out to set variables the shell must set anyway.
* **Artifact upload/download** — `actions/upload-artifact` and `download-artifact` are
  first-party actions with their own cross-attempt semantics (§4.6); reimplementing them would
  be strictly worse.
* **Job graph and gating** — `needs:`, `if:`, concurrency, and the fork check are workflow
  concepts. They are removed from the *consumer's* burden by the reusable workflows (§4.7),
  not by being rewritten in another language.

Two consequences of moving the logic are worth stating, because they replace mechanisms this
design previously relied on:

* **Nothing parses human-readable output.** Report prose is for humans and changes freely —
  change points now read *"somewhere near `<commit>`"* rather than naming a commit as the
  cause, and fields such as the old confidence figure have been dropped outright. Any value
  the action needs for a decision comes from the JSON report or from a dedicated output, never
  from scraping text. In particular `notable` — the "did anything interesting happen" signal
  that routes the whole flow — is read as a first-class output rather than extracted from a
  report by shell.
* **Retry is per operation, not per direction.** The shell layer's blanket rule — reads retry,
  writes never do — is coarser than it needs to be, and it makes ordinary transient GitHub
  failures visible to users from the very component built to absorb them. In Rust the rule is
  stated per operation: **reads** retry with backoff behind a transient-fault classifier (a
  4xx or auth failure still surfaces at once); **updating a known comment, closing an issue,
  and deleting a known comment are idempotent** and retry too (a delete that 404s on the
  second attempt has succeeded); only a **create** is genuinely ambiguous, because a failure
  may have taken effect. A create therefore does not blind-retry — it re-reads by marker
  first, and edits what it finds rather than posting a duplicate. This is why identity is a
  hidden marker rather than a displayed title: a rolling issue found by title alone would be
  abandoned the moment a consumer edited `issue-title`, and could hijack an unrelated issue
  that happened to match.

### 5.2 Standard reports, with narrow overrides

Once the message catalogue lives in one binary (§5.1), standardisation is nearly free: there
is exactly
one implementation of each message, so every consuming repo posts recognisably the same
report. That is the default and it requires no configuration. A consumer who sets nothing
gets the full standard set — the in-progress placeholder, the results comment, the staleness
banner, the terminal failure notice, the no-findings state, the coverage verdict, and the
failure-alert issue — all worded identically to every other consumer's.

Standardisation is the goal, so the override surface is deliberately **narrow and
declarative** rather than a general templating system. Only the things that genuinely differ
per repo are adjustable, and each is a value, not a format:

| Slot | Why it varies |
| --- | --- |
| Issue title & labels | Must match the repo's existing triage conventions and dedup key. |
| A short intro line | Room to say "this is advisory" or point at team-specific context. |
| A docs link | Consumers want to send readers to *their* runbook, not ours. |
| Comment marker | Lets a repo run two independent instances without them fighting. |

Beyond that, a consumer turns the built-in sink off (`sink: none`) and builds their own report
from the structured outputs (§7). This is a **deliberate cliff rather than a gradient**: either
the standard report with a few values filled in, or full ownership. What it is *not* is a
free lunch — the JSON report is a versioned interface the consumer is then coupled to, and a
custom sink inherits only the results message, leaving the lifecycle states (§4.4) to be
reproduced or forgone.

**Full templating is a deferred stretch goal, not a first release.** A user-supplied template
file (rendered by an engine such as `minijinja`) sits in the gap the cliff deliberately leaves
open, and it is the more dangerous position of the two. Opting out via `sink: none` couples a
consumer to a *versioned* report they chose to build on, and their reporting breaks in their
own job. A template instead binds arbitrary internal fields into **our** sink, so a field
rename breaks posting at run time, in CI, on someone else's repo — and every field a template
may reference becomes de facto public, which is precisely the freedom §5.1 relies on to keep
prose and data model moving together. The narrow slots plus the opt-out cover the demonstrated
needs; templating is revisited only if a concrete case appears that neither serves.

## 6. Storage backend & auth

Backend selection and credentials are resolved by the tool from the config file plus two
runtime signals; the action adds no GitHub-specific auth code and takes **no secret
inputs** — and, since the tool's Azure backend is **Entra-ID-only**, there is no secret
*storage* material for it to handle at all:

* **Local filesystem** — set `local-path`; the action passes `--local=<path>`, overriding
  any cloud backend in the config for that run.
* **Azure, Entra OIDC** — the config file's `[storage.azure]` names the account and
  container (non-secret, committable); it has no key or SAS field. Authentication self-mints
  via the tool's GitHub OIDC credential (`DESIGN.md` §6) when `AZURE_CLIENT_ID` /
  `AZURE_TENANT_ID` are present in the job env and the caller grants `id-token: write`. Those
  are **non-secret identifiers the caller sets in the job/step `env`** (or supplies a prior
  `azure/login`, which the tool's local-dev credential fallback then picks up): a composite
  action's steps inherit the job env, so the tool sees them without the action plumbing
  anything.

**Fork PRs: a trust gate, not a blanket ban.** The concern is narrow and worth stating
precisely — a PR run *executes the contributor's code*, and that run holds credentials to the
history store. A drive-by contributor must never reach them. But excluding *all* forks also
excludes trusted people who simply work from a fork, which is a normal workflow for
maintainers of many projects.

Two separate things have to line up, and conflating them is what makes this look harder than
it is:

* **Trust in the person** is answered by the PR event's own `author_association`. `OWNER`,
  `MEMBER`, and `COLLABORATOR` are people the repository already trusts to push branches and
  run CI; `CONTRIBUTOR`, `FIRST_TIME_CONTRIBUTOR`, and `NONE` are not. This is the gate the
  action exposes, as an `allowed-associations` input defaulting to those first three.
* **Availability of credentials** is a platform fact that association cannot change. A
  `pull_request` event from a fork gets a read-only token, no secrets, and — decisively — **no
  OIDC**, no matter who opened it. So "trusted author" alone does not produce a working run.

`pull_request_target` is the usual workaround and is **categorically rejected here**: it runs
in the base repo's context with full credentials, and this workflow's entire purpose is to
execute the PR's code. That combination is the textbook privilege-escalation shape, and
benchmarking is its worst case.

Two mechanisms close the gap honestly, and the design supports both:

* **Reduce what a PR run needs to a read.** A PR's own measurements are discarded on merge
  anyway (§4.5), so the PR flow does not need *write* access to the shared store — only the
  ability to read the baseline and keep its own points somewhere temporary. Running PRs
  against a **read-only identity** with a scratch store for this run's own data shrinks the
  blast radius to "can read benchmark history", which is a far smaller thing to extend to a
  trusted fork. This is the preferred answer and the one that makes fork support cheap.
* **Approval-gated credentials** for anything that still needs them. Routing the
  credential-holding job through a deployment **environment with required reviewers** means a
  maintainer explicitly approves the run before secrets exist. It costs a click, which is the
  correct price for handing credentials to code from outside the repository.

The resulting behaviour is tiered: a **same-repo PR** gets the full experience unchanged; a
**fork PR from a trusted association** gets it too, via read-only access (or after approval);
and a **fork PR from anyone else** runs no benchmarks at all. The last case is silent by
default rather than misleading, but §9's diagnosability rules apply — a reader should be able
to discover *why* nothing ran.

**PR analysis reads the same production store as `main`.** Branch mode compares the PR head
against `main`'s recorded baseline, so the PR flow must read the very store that holds it — a
separate PR store is rejected because it would have no baseline to compare against. Read
access to that baseline is therefore unavoidable for any PR run that reports anything; what
the tiering above removes is the *write* half.

**Bring-your-own infrastructure.** The action does **not** bundle the Azure provisioning
(`infra/azure-bench-history-prod/`); that stays in the monorepo as a *referenced example* the
README links to. A consumer points the action at whatever account/identity they already have.

**Caller permissions** (documented in the README): `contents: read` (checkout);
`id-token: write` (Entra OIDC self-minting); `issues: write` (history flow, when
`issue-on-regression: true`, and the `alert`/`resolve-alert` failure lifecycle); `pull-requests:
write` (branch flow's comment and its lifecycle).

## 7. Interface summary (inputs / outputs)

This section describes the **composite action** — the building-block layer. The reusable
workflows (§4.7) accept a smaller, flow-shaped subset of the same names (plus `platforms`,
the JSON-encoded runner matrix) and pass the rest through, so a consumer on the default path
sees only the inputs their flow actually varies.

**Common inputs:** `command` (`collect` | `analyze-history` | `analyze-pr` | `backfill` |
`pr-comment-preflight` | `pr-comment-cleanup` | `pr-comment-finalize` | `alert` |
`resolve-alert`, required);
`install-method` (`binstall` | `cargo-install` | `path` | `none`, default `binstall`);
`tool-version` (package version to install; defaults to the version named in the action
release's manifest; `latest` opts into the newest published release; ignored for
`install-method: none`; the companion is pinned by the release and is not overridable, §3);
`tool-path` / `companion-path` (for `install-method: path`); `config` (path to a
`bench_history.toml`; default: the tool's own `.cargo/bench_history.toml` discovery);
`local-path` (→ `--local`); `verbose` (default `true`).

**Cargo build inputs (`collect` / `backfill`):** `all-features` (default `true`, matching the
flows' need to reach benchmark targets gated behind `required-features`),
`no-default-features`, and `features` (a list). Without these a repo whose benches sit behind
a feature would silently measure nothing.

**`collect` inputs:** `packages` (newline/space list → `--package` per name; empty → whole
workspace); `exclude`, `bench` (→ repeated flags); `best-of` (→ `--best-of`, default 1);
`machine-key` (override; default auto-detect); `on-existing` (`error` (default here) | `skip` |
`overwrite` → neither / `--skip-existing` / `--overwrite`; §4.5); `recollect-commit` (a SHA →
backfill-and-overwrite that commit in a throwaway worktree; §4.1). **Output:** `machine-key`
(this leg's fingerprint, for the analyze handoff).

**`analyze-history` inputs:** `machine-keys` (directory of collected per-platform keys →
repeated `--machine-key`); `cache` (→ `--cache`; mutually exclusive with `local-path`);
`since` (look-back window; default: the tool's history default); `issue-on-regression`
(default `false`); `issue-title` (dedup key; default "Benchmark regressions detected");
`issue-labels`.

**`analyze-pr` inputs:** `pr-number` (which PR to comment on); `base` (→ `--base`; **no
built-in branch name** — the reusable workflow passes the PR event's own base ref, and the
composite layer falls back to the tool's configured default-branch resolution, so a repo whose
trunk is not `main`, or a PR targeting a release branch, is compared against the right thing);
`context` (→ `--context`; default `HEAD`); `machine-keys`; `cache`;
`comment-marker` (hidden dedup marker; default the action's own). Improvements are reported
unconditionally in branch mode, so there is no direction input.

**Lifecycle-command inputs:** `pr-comment-preflight` / `pr-comment-cleanup` /
`pr-comment-finalize` take `pr-number` and `comment-marker`, plus (preflight) the `packages`
scope to disclose and (finalize) the failed run's URL; `alert` / `resolve-alert` take
`issue-title` and `issue-labels`.

**`backfill` inputs:** the same scope inputs as `collect` (`packages`, `exclude`, `bench`,
`best-of`), plus the history window to densify and `on-existing` (which defaults to `skip`
here, §4.5, because densification must stay resumable).

**Report-wording inputs (all commands with a sink):** the narrow override slots of §5.2 —
`issue-title`, `issue-labels`, an intro line, a docs link, and `comment-marker`. There is
deliberately no template input. A `sink` input (`standard` (default) | `none`) turns the
built-in posting off entirely for a consumer who wants to publish their own report from the
outputs below; with `sink: none` the analyze commands compute and emit, and post nothing.

**Outputs (from `analyze-history` / `analyze-pr`):** `notable` (`true`/`false`);
`regressions` (count); `report-markdown` (full report path); `report-json`; `report-summary`
(condensed top-findings Markdown); `report-schema` (the JSON report's schema version). The
reusable workflows re-export these as workflow outputs, so a caller on the default path can
still add a job of their own downstream. Together with `sink: none` these are the escape hatch
§5.2 points at.

Two honest caveats attach to that hatch. The JSON report is a **versioned** interface — hence
`report-schema` — but it tracks the analysis, so a consumer building on it accepts that
findings vocabulary evolves. And a custom sink owns *only* the results report: the lifecycle
messages (§4.4) have no analysis behind them, so a consumer choosing `sink: none` either
forgoes the placeholder/staleness/failure states or reproduces them.

## 8. Versioning & Marketplace

* **Semver tags** `vX.Y.Z` on the action repo, plus a **floating major** `v1` ref
  that is force-moved to each new `v1.*` release (the standard `actions/*` major-tag dance,
  re-pointed by the action repo's `release.yml`).
* **Action version is independent of tool version.** `tool-version` defaults to a
  known-good crates.io release baked into each action release, so `@v1` installs a
  pinned, tested tool by default, while a caller can override it (or set `latest`) without
  waiting for a new action release. A new *tool* release therefore never forces a new *action*
  release; the baked-in default advances only when we deliberately cut one (§8.1).
* **README leads with the reusable workflows and keeps the hand-assembled recipes as
  reference.** The headline examples are the three `uses:` snippets of §4.7 — history, PR,
  and nightly backfill — because that is the whole consumer surface for the default path.
  Below them, for repos assembling their own graph, the README documents what those workflows
  expand to:
  * A **per-push history** workflow — a `fail-fast: false` matrix `collect` job across the
    platforms (each `on-existing: skip`, uploading its `machine-key` output as a per-platform
    artifact), then an `analyze-history` job (`needs: collect`, `fetch-depth: 0`, downloading
    the key artifacts into the `machine-keys` dir **with an explicit `github-token`** so
    partial re-runs resolve, §4.6, an `actions/cache` step feeding `cache`,
    `issue-on-regression: true`), plus `alert`/`resolve-alert` jobs.
  * A **per-PR branch** workflow — a delta preflight computing the touched benchmarkable
    packages, a `pr-comment-preflight` job (in parallel with collect), a matrix `collect` job
    scoped by `packages`, an `analyze-pr` job (checkout `head.sha`, `fetch-depth: 0`,
    restore-only cache, gated `!cancelled()` so a superseded run never posts) posting the
    comment, and a `pr-comment-cleanup` path for the empty-scope case — all gated on the
    same-repo fork check (§6).
  * A **nightly densification** workflow (§4.8) — a matrix `backfill` job over the same
    platforms and window, with no analyze job and no sink.
  * The README also shows the **concurrency + cancel-on-close** pattern: PR-driven runs
    cancel superseded runs keyed on the ref, and a tiny companion workflow triggered on PR
    close joins the same concurrency group to reclaim a run left in flight by the close
    (`cancel-pr-bench-history.yml`); the push flow deduplicates the same commit instead.
  These mirror Folo's own bench-history workflows, lifted to consume the published action.
* **Marketplace publish** from the action repo's release UI (root `action.yml` + branding)
  once a `vX.Y.Z` release exists. Only the composite action is listed; the reusable workflows
  ship in the same repo under the same tags but are referenced by path (§4.7).

### 8.1 Releasing the action (operator flow)

The action is distributed as **git tags on its own repo** — the composite action and the
reusable workflows are both plain files resolved by ref, and the binaries they invoke are
installed at run time — so "releasing" it is a small, deliberate operation, entirely separate
from the monorepo's automated package/binary releases
([`../../../docs/release-automation.md`](../../../docs/release-automation.md)). The action
repo carries its own tiny release tooling:

**`just release <version>`** (a recipe in the action repo) that:

1. checks the working tree is clean and on `main`;
2. optionally updates the **release manifest** (§3) — the tool version this action version is
   validated against, and the pinned companion version — committing that change;
3. creates the annotated tag `vX.Y.Z` and pushes it.

A **`release.yml`** workflow in the action repo, triggered on the `v*.*.*` tag push, then
does the parts that must happen server-side:

* **verifies every binary in the manifest actually resolves** — the tool and the companion, on
  each supported target — *before* anything else. Moving the floating major tag to a release
  whose companion is not yet installable would break every consumer at once, including the
  `alert` path that exists to report breakage;
* **creates a GitHub Release** for the tag — publishing a Release is the event that
  (re)publishes the Marketplace listing;
* **force-moves the major tag** (`v1` → the tagged commit) so `uses: …@v1` consumers pick
  up the release. Because the reusable workflows reference the root action through
  self-repository syntax (§4.7), moving the tag advances both layers together and cannot
  leave a workflow calling a mismatched action.

**The maintainer's responsibility** is therefore deliberately small:

* decide the semver bump and confirm `tool-version` points at a known-good package release;
* run `just release X.Y.Z`;
* **first release only:** complete the one-time Marketplace listing form in the GitHub UI
  (accept the agreement, choose a category, confirm the root `action.yml` carries
  `name` / `description` / `branding`). Every later release re-publishes automatically.

There is no crates.io or binary publishing in this flow — the *tool* is released from the
monorepo's automated pipeline
([`../../../docs/release-automation.md`](../../../docs/release-automation.md)); this action
merely pins a `tool-version` to install at runtime.

## 9. Testing the action

Most of the action's risk is not in arithmetic — the tool owns that, covered by the monorepo
suite — but in behaviour that only manifests over *time* (a trend needs many commits before it
is "notable") and in **real GitHub side effects** (filing and updating a rolling issue; posting,
re-posting, stale-bannering, and cleaning up a PR comment). A single test level cannot reach all
of that, so the action repo layers three of them, each stronger and slower than the last.

**Layer 1 — Rust unit tests (every push, seconds, no network).** The action's behaviour lives
in Rust (§5.1), so every non-trivial branch is unit-testable against fakes: install-method
selection and version resolution (§3), machine-key gathering, and the whole PR-comment
lifecycle — placeholder seeding, staleness-banner insertion/replacement, cleanup removal —
asserted against a faked GitHub transport with **no live issue or PR**. This is also where
the exact composed issue/comment *bodies* are pinned (hidden markers, scope line, coverage
verdict, banner text), so a formatting regression fails here first — and because the
composition sits beside the data model it renders, a newly added census reason cannot slip
through unrendered.

**Layer 2 — local-storage end-to-end on the CI matrix (every push, minutes, no secrets,
fork-safe).** `test.yml` runs the *real* action against **local filesystem storage**
(`local-path` under `${RUNNER_TEMP}`) across the platform matrix and across *each* real
`install-method` (`binstall`, `cargo-install`, `path`, and `none` against a pre-seeded `PATH`),
so both the install branching and the actual installs are exercised, not just mocked:

1. A tiny checked-in throwaway Rust project with one fast Criterion benchmark.
2. `command: collect` over `--local`; assert a result set was stored and the `machine-key`
   output is a valid fingerprint.
3. `command: analyze-history` over that store, threading the collected `machine-keys`; assert
   the `notable` output and that the Markdown/JSON/summary reports exist and parse.
4. A **branch fixture** — a throwaway repo whose final commit regresses the benchmark — drives
   `command: analyze-pr` (context = the tip, base = the branch point) and asserts
   `notable == true`, that the summary names the regressed series, and that the
   composed PR-comment body carries the scope line and hidden markers. The base side must be
   seeded with **enough points for branch mode to judge against** — a two-commit fixture cannot
   clear the detector's minimum evidence — so this fixture uses the faker→`import` path (§11)
   rather than real benchmark runs to populate the comparison window cheaply.
5. A **recollect** leg (`recollect-commit`) asserts a single historical point is overwritten.
6. **Caller canaries for the reusable workflows.** The action repo carries its own caller
   workflows that invoke `history.yml`, `pr.yml`, and `backfill.yml` exactly as an external
   consumer would, because none of the other levels exercise the layer that is now doing the
   most work: matrix expansion from the JSON `platforms` input, fan-out-then-converge onto one
   analyze, permission narrowing, the fork gate, artifact aggregation, and concurrency. The
   canaries deliberately include the ugly cases — a **partially failed** matrix, a **fully
   failed** matrix, a malformed `platforms` JSON, an **empty package scope** (which must route
   to cleanup, not analyze), and a cancelled run — since each is a path where the graph, not
   the binaries, decides the outcome. The two layers' input lists are contract-tested against
   each other so a new composite input cannot silently go unexposed by the workflows.

This proves the wiring and the real installs, but it deliberately never posts to GitHub and
never sees a *long* history — the two things Layer 1 could only mock.

**Layer 3 — trend-dependent, real-GitHub-write validation.** The remaining gap is the behaviour
that needs (a) a benchmark history long enough to make analysis "notable" and (b) the action
actually writing to GitHub. The tool already ships the two enablers this needs — the hidden
`cargo-bench-history import` command and the published `cargo-bench-history-faker` engine
(§11) — so none of the options below requires new tooling. Three options, roughly increasing in
fidelity and cost. The design adopts **A + B**; C is a cheaper fallback if a sandbox repo proves
impractical.

* **Option A — synthetic history in a sandbox repo (highest fidelity).** `cargo-bench-history-faker`
  writes curated per-engine output into a `target/`-shaped tree (inventing nothing — every value
  comes from its flags), and `cargo bench-history import --target-dir <tree>` stores that output
  through the exact `collect` finalize-and-store path *without running `cargo bench`*. Crucially,
  `import --commit <ancestor>` keys a stored point to any existing commit **without checking it
  out**, so a single HEAD position can fabricate a whole multi-commit series (a planted
  regression, a planted improvement) by looping faker→import over real ancestor SHAs. Both
  binaries are published and `binstall`-able, so the sandbox needs **no vendoring and no
  workspace checkout** — it installs `cargo-bench-history` and `cargo-bench-history-faker` like
  any consumer. The action then runs for real against a dedicated **sandbox repository** (a
  scratch repo with its own bot token): `analyze-history` files and later updates the rolling
  *issue*; `analyze-pr` against a scratch PR posts the comment, re-posts on a second run, applies
  the staleness banner, and finally cleans up — each asserted back by reading the live repo
  (issue exists with
  the expected title/body, comment updated in place not duplicated, banner present, comment
  removed on cleanup). Because it needs a token and mutates a real repo, this runs on a schedule
  and pre-release rather than on every push.
* **Option B — compose-only assertions against a faked transport (fast, no repo).** Reuse the
  Option A
  faker→`import` history into local storage, run `analyze-history` / `analyze-pr`, and assert the
  *exact* issue/comment body the action *would* post against a **faked GitHub transport** — no
  live posting.
  This catches body/marker/banner regressions with a realistic multi-commit trend but without a
  sandbox repo or a token, so it can run on every push. (It is Layer 1's body assertions, but fed
  a real long history instead of a one-off fixture.)
* **Option C — checked-in storage fixtures (cheapest, least fresh).** Commit a small
  pre-built store (a handful of result sets across synthetic commits) and replay it through
  `analyze-history` / `analyze-pr`, asserting `notable` and the reports. It skips faker/`import`
  entirely, but the fixtures are opaque and must be regenerated by hand when the storage format
  changes — so it is only a stopgap where the faker→`import` path is unavailable.

The Azure auth branches stay covered by the monorepo's Azure-backend test jobs (`DESIGN.md` §6),
so none of these layers needs to reach the cloud.


## 10. Dogfooding — Folo's own workflows

Folo migrates **all** of its bench-history workflows — the push flow, the PR flow, and the
nightly densification pass — to consume the published action, but with **`install-method:
path`** pointed at the workspace checkout, so its collection keeps measuring `main`'s HEAD
tool rather than waiting for a release (preserving the property that a tool change is
exercised the same push it lands). External repos use the default `binstall` install. This
dogfoods the action's entire input-driven path
— config resolution, auth wiring, the `on-existing: skip` write mode and delta-scoped
`packages`, the machine-key artifact handoff, the `--cache` read-through cache, the notable
signal, and *both* report sinks with their lifecycles.

The migration is also the clearest measure of whether this design achieved its first two
goals (§1): Folo's roughly 1,200 lines of bench-history workflow YAML should collapse to
three `uses:` blocks, and the `scripts/bench-history/*.psm1` modules with their Pester suites
should be **deleted rather than generalized** — their composition logic having moved into the
tool and their transport logic into the companion binary (§5.1). Anything that resists
deletion is a signal that some behaviour was repo-specific after all, and belongs in the
action's input surface.

If `install-method: path` proves awkward to host in a *separate* action repo (it needs the
tool's source on disk), the fallback is a **single internal thin composite action kept in the
monorepo** (`.github/actions/bench-history`) that shares its implementation with the published
action — the published action stays the source of truth for external consumers.

## 11. Tool / repo changes this design implies

The tool's *analysis* surface, its release pipeline, and the testing enablers are all already
in place. What this design adds is a **small rendering addition to the tool** and a **companion
binary** that owns the GitHub-shaped half (§5.1):

* **The tool gains the coverage verdict and a first-class `notable`.** Alongside the existing
  `--markdown`, `--markdown-summary`, and `--json`, `analyze` renders the **prose explanation
  of the judged-set shortfall** — the sentence a shell formatter previously had to reconstruct
  by mirroring the tool's own reason names by hand, and therefore the one piece guaranteed to
  drift. `notable` likewise becomes an output rather than something callers read back out of
  the JSON report. Nothing GitHub-shaped is added: no markers, no artifact links, no comment
  structure. This narrows an earlier position in this document that kept *all* presentation
  outside the tool; what changed it is that domain prose and domain vocabulary cannot be
  separated without drift, while the GitHub envelope can be, and is (below).
* **A companion binary is new.** The GitHub envelope and the whole sink lifecycle — results
  body, placeholder, staleness banner, terminal failure notice, rolling failure issue, and the
  API transport for all of them — become a small, unsupported, doc-hidden Rust package
  published beside the faker and installed by the same layer (§3). It is the only new
  *package* this design requires, and the only one a consumer installs beyond the tool itself.
* **The release pipeline needs the companion added to it.** The automated publish flow itself
  needs no new logic, but the new package must be entered into the version-group configuration
  and set up as a crates.io trusted publisher, exactly as any other published package is —
  plus the pre-tag resolvability gate of §8.1, which is new.
* **The existing CLI covers everything else.** `--config`, `--local`, `--cache`,
  `--skip-existing` / `--overwrite`, `--best-of`, `--context` / `--base`, `--machine-key`, the
  scope flags, and `backfill` (both the densification pass, §4.8, and `--overwrite` recollect)
  all already exist, and the analysis mode is inferred, so there is nothing to add there.
  Note that `--include-improvements` no longer exists — direction is now a property of the
  mode (§4.3) — so nothing should pass it.
* **The synthetic-history testing enablers already exist** (§9). The hidden
  `cargo bench-history import` command (`collect`'s finalize-and-store path minus the `cargo
  bench` run; `--target-dir` required, `--commit`/`--target-triple`/`--dirty` overrides —
  `DESIGN.md` §7.9) and the published-but-unsupported `cargo-bench-history-faker`
  engine together let a sandbox repo fabricate a realistic multi-commit history from published
  binaries alone. No new command is needed for even the highest-fidelity testing option.
* **No fake-engine handling needed** — the fake engine is its own separate package
  (`cargo-bench-history-faker`) with its own binary, so the published `cargo-bench-history`
  already ships a single binary (`DESIGN.md` §9). The action installs the plain package name.
* **The monorepo's own shell layer becomes redundant.** Folo's
  `scripts/bench-history/*.psm1` modules exist because nothing else could compose or post;
  once the tool and the companion can, they are replaced by the action rather than generalized
  into it, and Folo's four bench-history workflows collapse into calls to the reusable
  workflows (§4.7, §10).
* **Docs — the book gains a "Continuous integration" section.** The user guide currently
  documents the tool as something you run by hand (Installation, Commands, Concepts,
  Appendix), which leaves its most common *real* deployment undocumented. Running
  `cargo-bench-history` in CI is not a footnote to local use; it is the primary use, and it
  raises questions that have no local analogue. The book therefore gains a section covering:
  * **The three flows** — per-push history, per-PR branch, nightly densification — and why a
    useful setup runs all three rather than just the first.
  * **Adopting the action**, pointing at the published action for the mechanics.
  * **Deployment profiles** — the shared, rotating, ephemeral runner pool versus dedicated
    self-hosted benchmark machines. These differ in almost every way that matters (machine-key
    stability, noise floor, whether densification is needed at all, useful `best-of` values),
    and a reader choosing hardware needs that comparison before they build a pipeline around
    one of them.
  * **What CI data means** — that PR measurements are keyed to branch commits and are
    discarded by squash- and rebase-merges (§4.5), so the trunk series is fed by the history
    and densification flows alone.
  * **Reading a CI report** — in particular telling apart "no findings", "not enough baseline
    yet", and "nothing benchmarkable changed" (§9).

  The division of labour stays as it was: the **book** owns tool-level concepts and the
  deployment thinking, and the action repo's **README** owns only the action's own mechanics
  (inputs, workflow recipes, install methods) and links to the book rather than restating it.
  This file and the pointer from `DESIGN.md` §7.3 remain the design record.

## 12. Open questions

* **Whether the benchmarkable-delta filter belongs in the companion.** The PR flow needs to
  reduce a touched-package set to the packages that actually carry benches, and §5.1 rules out
  doing it in shell. Folding it into the companion avoids a third installed binary; wrapping
  the existing `cargo-detect-package` reuses working code but adds another versioned
  dependency to the manifest (§3). Until this is settled, `pr.yml` is specified but not fully
  designed.
* **Machine-key artifact plumbing.** Whether `collect` and the analyze commands bundle the
  `actions/upload-artifact` / `download-artifact` steps for the machine-key handoff (§4.6),
  or leave the upload/download to the caller and only exchange the `machine-key` output and
  `machine-keys` directory input. The reusable workflows (§4.7) make this invisible to most
  consumers either way; it matters only for hand-assembled graphs. Bundling hard-codes
  artifact names and adds bundled action dependencies.
* **Whether the PR close-cancellation workflow can be folded into `pr.yml`.** Today it is a
  separate tiny companion workflow joining the same concurrency group (§8), which sits awkwardly
  with the claim that a consumer adopts the PR flow with one `uses:` block. Having `pr.yml`
  itself accept the `closed` event and skip straight to cancellation would keep the whole PR
  story in one entry point.
* **Whether collection scope should name platforms, not just packages.** Analysis proceeds
  after a partially failed matrix (§4.2), so a report can be honest about *which packages* were
  measured while still implying more platform coverage than it had. Disclosing the
  attempted/succeeded platforms would close that gap, at the cost of a noisier report on every
  clean run.
* **How far to take real-GitHub testing (§9).** Whether to stand up a dedicated sandbox
  repository (with its own bot token) for Option A's end-to-end issue/PR-comment validation, or
  start with the no-repo Option B (compose-only assertions over a faker→`import` history) and add
  the sandbox later. The tooling for both — the `import` command and the published faker — already
  exists, so this is purely a cost/fidelity call about the sandbox repo, not about building
  anything.
