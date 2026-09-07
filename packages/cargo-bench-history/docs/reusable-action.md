# Reusable GitHub Action — design (issue #284)

This is the design for packaging the `cargo-bench-history` *collect → analyze → report*
flow as a reusable, Marketplace-published GitHub Action — a sub-component of the tool,
referenced from [`DESIGN.md`](DESIGN.md). It describes the intended end state of that
action: its command surface, distribution model, configuration surface, and hosting. The
tool's own data model, commands, storage, auth, and analysis modes live in `DESIGN.md`;
this document does not restate them, only how they are wrapped for external consumers.

**This is a working document, not a finished reference.** It exists to be *consumed* while
the design is implemented: expect it to be split, rewritten, and largely dismantled as the
implementation lands, whether as one pull request or a stack of them. Sections graduate out
of it as they find permanent homes — user-facing material into the book's GitHub-automation
section (§11) and the action repo's README, internal decisions into the owning package's
design and implementation guides — and what remains here shrinks accordingly. Treat
disagreement between this document and shipped behaviour as a defect in this document.

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
  groups, the same-repo gate, the sink lifecycle jobs — is repo-specific in *substance*, only in
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

*Where the code lives.* The split is by *language and release cadence*, not by convenience:

* **All Rust stays in the monorepo.** The tool, the companion binary (§5.1), and the faker are
  workspace packages, sharing its toolchain, lints, test conventions, release automation, and
  version groups. They are published to crates.io like any other package here and installed by
  the action at run time (§3). Nothing Rust is authored in the action repo.
* **Only the GitHub-shaped files live in the action repo**: the root `action.yml`, the reusable
  workflows (§4.7), the README, and the release tooling (§8.1). These are exactly the artefacts
  that need their own Marketplace listing and their own semver tag stream.

This keeps the boundary honest in both directions: the action repo carries no logic worth
testing in isolation, so it needs no Rust toolchain, and the monorepo keeps every piece of
behaviour under the validation it already runs. The interface between them is the published
binary plus its documented arguments.

*Testability.* Most of the action's value lives in behaviour that is awkward to
test — a history that only becomes interesting once it accrues over many commits, and real
GitHub side effects (filing an issue, posting and updating a PR comment) — so it leans on a
layered strategy (§9): fast Rust unit tests of the composition and transport (in the monorepo,
beside the code), a local-storage
end-to-end pass on every push, and a synthetic-history pass (built on the published
`cargo-bench-history-faker` engine and the tool's hidden `import` command) that drives the real
GitHub-write paths.

## 3. Binary distribution

External callers cannot `cargo run -p cargo-bench-history` against our workspace, so the
action must obtain real binaries. The published tool is the `cargo-bench-history` package,
published from CI to crates.io.
Every published release also carries **prebuilt, `cargo-binstall`-consumable binaries**
(per-target archives + `.sha256`, attached to the release; see
[`../../../docs/release-automation.md`](../../../docs/release-automation.md)), so the action
has both a from-source path and a download-a-binary fast path.

The action exposes an **`install-method`** input with four modes. **The chosen method applies
to every binary the action needs**, not just the tool: the companion (§5.1), and any other
`folo-rs/folo` binary a command depends on, are obtained the same way. A consumer who chose
`binstall` because their runners cannot afford a compile must not have that choice silently
ignored for the second binary, and one who pre-seeded `PATH` expects *nothing* to be
downloaded. Each command installs only the binaries it actually uses.

| `install-method` | How | When |
| --- | --- | --- |
| `binstall` | Install `cargo-binstall`, then `cargo binstall <package> --version <v> --locked` for each required binary | **Default.** Downloads the prebuilt archive from each package's GitHub Release (seconds), automatically falling back to a source build if no asset matches the runner target. Best for `cargo-bench-history`, whose Azure SDK + `mimalloc` dependencies are slow to compile. |
| `install` | `cargo install <package> --version <v> --locked` for each required binary | Pure source build, no extra tooling. Always works once published; pays the full cold-cache compile. The escape hatch when a prebuilt asset is unavailable or unwanted. |
| `path` | `cargo install --path <workspace>/packages/<package> --locked` for each required binary | Dogfooding (§10): build from a checkout of *this* workspace so Folo's own collection exercises `main`'s HEAD, not a release. |
| `none` | (nothing) | The caller has already put the required binaries on `PATH` — a prior step, a devcontainer, a warmed tool cache — and the action just runs them. `tool-version` is ignored; the caller owns which builds are used. |

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
location for each.

**The failure-reporting path must not itself be fragile.** `alert` (§4.4) runs *because*
something already went wrong, so it is the one command that cannot afford a flaky install: if
fetching its companion fails, the run loses not only its results but also the notification that
anything is wrong, and the failure becomes silent. Two existing mechanisms cover this, and
neither is special-cased for `alert` — they simply matter most there. Installs are cached and
retried like every other install, so a transient network fault does not reach the command; and
the release gate (§8.1) refuses to move the floating major tag until every binary named in the
manifest resolves on every supported target, so a release can never ship a manifest whose
companion cannot be installed.

**All install modes stay testable.** Method selection, version resolution, and the binary
cache are unit-tested in the same Rust layer as the rest of the action's logic (§5.1), with
mocked tool output; and the CI matrix (§9) additionally runs each *real* method (`binstall`,
`install`, `path`; `none` against a pre-seeded `PATH`) against a published version, so
both the branching and the actual installs stay covered.

**No test scaffolding is ever shipped to consumers.** `cargo install cargo-bench-history` (or a
`binstall` of it) installs **only** the real tool. The end-to-end test engine is a *separate
package*, `cargo-bench-history-faker`, with its own binary; installing the tool never pulls it
in (see `DESIGN.md` §9). The faker is **already published** — it is on crates.io today, with
`binstall`-able prebuilt binaries from the same release pipeline — but **unsupported**: its
crate root is doc-hidden and neither its library API nor its CLI carries a semver contract. It
is published purely so a test job can run it without vendoring or a workspace checkout (§9);
nothing about the testing strategy below is blocked on publishing work. A consumer of the
action never installs it: it appears in the release manifest only for
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
| `analyze-history` | Trend analysis of the default branch, → rolling **issue** (once, after the matrix). |
| `analyze-pr` | Branch-vs-base analysis of a PR, → rolling **PR comment** (once). |
| `pr-comment-preflight` | Keep the PR comment honest at run *start* (staleness + in-progress seeding). |
| `pr-comment-cleanup` | Replace the PR comment with a note when the PR touches nothing benchmarkable. |
| `pr-comment-finalize` | Retire the in-progress placeholder when the run failed (§4.4). |
| `issue-preflight` | Flag the open regression issue as stale at run *start* (§4.4). |
| `issue-cleanup` | Move the regression issue to all-clear when a run comes back clean (§4.4). |
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
   * **`--verbose` is on by default.** The tool's verbose channel is *explanatory* — it states
     the inputs and reasoning behind each decision rather than announcing conclusions — and a
     CI reader cannot re-run the job locally to find out why it did what it did. Paying for
     that reasoning in the job log on every run is therefore worth it: the log is the only
     forensic record a consumer has when a run measures nothing, skips a package, or picks an
     unexpected partition. It stays an input so a consumer can turn it down.
3. **Emit this leg's machine key.** After a *successful* collect, the action resolves this
   runner's real hardware fingerprint (`cargo-bench-history machine-key`) and exposes it as a
   `machine-key` **output**, so the caller can hand the exact keys measured this run to the
   later single analyze job (§4.6). A failed leg measured nothing, so it emits no key.

**The collect matrix does not fail fast, and the reason is structural.** Each platform writes
into **its own machine-key partition**, and analysis never compares across partitions (§4.6).
A leg that dies therefore does not corrupt or bias what the other legs produced — it simply
means that one platform has no point at this commit. Partial data is *sound*, not a
compromise, which is what makes tolerating it defensible rather than merely convenient.

The objection is worth taking seriously: if a benchmark is broken, surely every leg fails, so
letting them all run just burns runner minutes. That is true of *systematic* failures — and
they are also the cheap case, because a benchmark that does not build fails early, and a run
where every leg failed publishes nothing misleading since no new data exists. The asymmetry is
in the other class. **Platform-specific and environmental failures are real and observed**: a
runner image change, a toolchain install hitting a transient disk fault, a storage hiccup, or
a benchmark that only breaks on one OS. There, `fail-fast: true` would *cancel the surviving
legs mid-flight*, discarding hours of valid measurement and leaving a permanent hole in those
platforms' series for that commit — recoverable only by a later densification pass (§4.8),
which costs more than was saved. So the choice trades a bounded waste in the cheap case
against unrecoverable data loss in the expensive one.

Tolerating partial collection does carry one real hazard, and the design pays for it rather
than ignoring it: a report drawn from the surviving platforms can look like a broader clean
bill of health than it earned. That is why coverage is disclosed rather than assumed (§4.2) —
the two decisions are a package, and tolerating partial failure without the disclosure would
be the genuinely wrong design.

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
   * **Analysis mode is inferred, not passed.** With the context left at the analyzed branch's
     own tip (its own merge-base, clean), the tool auto-selects **history** mode — long-range
     change-point and drift detection reporting regressions only (§4.5). The action passes no
     mode flag because none exists.
   * **Nothing here is specific to a branch named `main`.** History mode is selected by
     *topology*, not by a branch name: it applies to whatever branch the run collected on, so a
     repo whose trunk is `master`, `develop`, or `trunk` works unchanged, as does a repo running
     the flow on a long-lived release branch. What the flow does assume is that it runs on **one
     branch that accumulates a continuous series** — the trunk, in practice — because a history
     is only comparable along a line of commits that share ancestry. Running the history flow
     against a second branch is supported, but it is a second history: give it its own
     `instance` (§5) so the two do not share a rolling issue, an artifact name, or a cache key.
   * **Machine keys.** The facets default to surveying every engine and triple (`all`), but
     the machine key is **not** `all`: it is the exact set of fingerprints collected this run,
     threaded from the collect matrix (§4.6), so the survey is scoped to the machines that
     actually measured this commit and never mixes in a stray key from the shared store.
   * **Coverage is disclosed, not assumed.** Because the matrix tolerates a partially failed
     collect (§4.1), the set of platforms that *contributed* can be smaller than the set that
     was *intended* — and the difference is invisible in the findings themselves. The action
     knows both sides (the intended platform list is an input; the contributing set is the
     machine-key artifacts that arrived), so when they differ the report says which platforms
     are missing, and the run's outcome is **partial** rather than clean. On a fully successful
     run this adds nothing, so the common case stays quiet. Without it, a Windows leg dying
     would leave "no regressions" standing as an unqualified claim about a platform nobody
     measured — which is the failure mode that makes silent partial coverage worse than an
     outright failed run.
   * **Cache.** `--cache <dir>` (a read-through mirror of the cloud history persisted across
     runs via `actions/cache`) turns repeated full-history downloads into a warm-cache read;
     it applies to the **cloud backend only** and **conflicts with `--local`**, so the action
     passes at most one.
   * A single pass emits all three machine-readable artefacts via the per-format output
     toggles: the full Markdown report, the full JSON report, and the **condensed
     top-findings Markdown summary** (`--markdown-summary`) sized to fit an issue body.
5. **Surface the run's outcome**, not merely a boolean. Analysis ends in one of a small set of
   named states — findings, clean, insufficient baseline, nothing in scope, partial coverage,
   failed — and that **`outcome`** is what the flow carries forward, alongside the report paths
   and a regression count (§7). A single `notable` boolean collapses states that need different
   handling: "clean" and "we could not judge anything" are both *not notable*, yet only one of
   them is good news. Naming the state serves two consumers. It is the key the companion
   selects a message with (§5.2), so the choice is made once and explicitly rather than
   re-derived from scattered checks; and it lets the test canaries (§9) assert what actually
   happened, where an assertion on `notable == false` would pass equally for a clean run and
   for a broken fixture that analyzed nothing. Callers may branch on it too, but that is a
   side benefit, not the justification.
6. **File the rolling issue** when `issue-on-regression: true` *and* the outcome is
   **findings**. The
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
  collection scope on both axes** — which packages were benchmarked *and*, when the collect
  matrix came back partial, which platforms are missing (§4.2) — so a clean result is never
  mistaken for the whole suite, or the whole runner pool, being clean. The PR flow runs the
  same `fail-fast: false` matrix as the history flow (§4.1), so it is exposed to exactly the
  same partial-coverage hazard, and this is the more widely read of the two sinks: a reviewer
  deciding whether a change is safe to merge must be able to see that a platform went
  unmeasured. A partial run therefore says so in the comment and reports `outcome: partial`.
  It records **which commit it measured** (a bare full SHA
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
  that touched one earlier and then reverted). It replaces any rolling comment a prior push
  left behind with a **one-line note saying nothing benchmarkable changed**, rather than
  deleting it outright. Silence is the wrong answer here: an absent comment is
  indistinguishable from a workflow that is broken, skipped, or still running, and a reader
  who expected benchmark feedback has no way to tell which. Stating the reason costs one line
  and removes the ambiguity. (Deleting instead remains available for repos that prefer a clean
  PR, but it is not the default.)
* **`pr-comment-finalize`** runs *last*, gated on failure, and closes the one hole the two
  above leave open. A placeholder promises results are coming; if collection or analysis then
  fails, `analyze-pr` deliberately posts nothing (§4.3), so without a terminal step the
  placeholder would sit on the PR claiming work is in progress forever — the run that would
  have replaced it is gone, and only a *further push* would ever revisit it. Finalize replaces
  the placeholder with a short terminal notice pointing at the failed run. It fires only for
  genuine failure, not cancellation: a superseded run is *expected* to leave the placeholder
  standing, because the run that cancelled it is about to refresh it.

**Regression-issue sink (history flow).** The rolling regression issue needs the same upkeep
as the PR comment, and for the same reason: it is a long-lived artefact that keeps asserting
something after the run that wrote it has become history. Without upkeep it goes stale
silently — an issue filed six commits ago still reads as a current statement about `main` —
and it never acknowledges being fixed, so a maintainer cannot tell "still broken" from
"nobody has looked". Two commands mirror the PR pair:

* **`issue-preflight`** runs at the *start* of each history run. When an open regression issue
  exists, it prepends the same sentinel-bounded staleness banner the PR sink uses — *"findings
  are N commits behind HEAD"*, from the compare distance — so a reader who arrives mid-run
  knows the issue describes an older state of `main` and that a fresher answer is on its way.
  It is a no-op when no issue is open; unlike the PR sink it seeds **no placeholder**, because
  an issue that exists only to say "benchmarking in progress" would be noise in the issue
  tracker rather than context on something already being read.
* **`issue-cleanup`** runs at the *end* of a run whose outcome is **clean** (§4.2) while an
  issue filed by an earlier run is still open. It rewrites the body to an **all-clear** state
  naming the commit that came back clean, so the issue stops asserting a regression that no
  longer reproduces. By default it **leaves the issue open**: the tool detects that the numbers
  recovered, which is not the same as the underlying problem being understood — a regression
  may have been masked, worked around, or accepted, and closing it would discard a thread a
  human may still want. An **`auto-close`** input (default `false`) closes it for maintainers
  who prefer the issue to track the *measurement* rather than the *investigation*.

This is deliberately asymmetric with the failure alert below, and the distinction is the point:
a **failure** is a machine condition that is definitively over when the next run goes green, so
it auto-closes; a **regression finding** is a statement about the code that a green run does
not by itself resolve. Same mechanism, opposite defaults, because they answer to different
readers.

**Failure-alert sink (history flow).** Separately, the *failure* of a run — distinct from a
regression *finding* — is a recurring condition on a rolling target, so it gets its own
deduplicated tracking issue that **does** auto-close on recovery:

* **`alert`** opens or refreshes a dedup failure issue when the run fails.
* **`resolve-alert`** closes it when a subsequent run succeeds.

All four use the same rolling-issue path in the companion binary as §4.2, each keyed on a
distinct identity so they never collide. Every message named in this section — placeholder,
staleness banners, all-clear, terminal failure notice, failure alert — has no analysis behind
it, which is precisely why the companion owns the whole catalogue (§5.1) rather than splitting
it with the tool.

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
select. This is a deliberate acceptance, not an oversight. Two consequences follow: the trunk
series is fed by the **history flow and
the densification pass**, never by PR runs, so nothing downstream depends on PR points
surviving; and because those points are disposable, a PR run has no need to *write* to the
shared store at all — which is the property any future fork support would build on (§6).

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
engine's data visible to analysis, not just the wall-clock ones.

**The artifact steps stay in the workflow, not inside the action.** The upload and download are
plain `actions/upload-artifact` / `download-artifact` steps in the reusable workflows, which
already hide them from everyone on the default path. Bundling them into the composite action
would save two steps for hand-assembled callers only, and would cost us a permanent dependency
on two more actions to pin and upgrade, a fixed artifact-naming scheme to keep collision-free,
and ownership of their cross-attempt and retention failure modes. Since neither arrangement is
visible to most consumers, the tie breaks on which we would rather maintain — and the answer is
the one that adds nothing. Either way the tool stays GitHub-agnostic: it only ever sees
`--machine-key <fingerprint>`.

### 4.7 Two consumption layers — reusable workflows over composite actions

The commands above are *building blocks*. Assembling them into a working setup means writing
the same job graph every consumer needs: a matrix `collect` across platforms, the machine-key
artifact handoff, a single `analyze` gated on the matrix, the sink lifecycle jobs, plus
concurrency, permissions, and (for PRs) the same-repo gate. That graph is identical everywhere
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
        platforms: ubuntu-latest, windows-latest
        azure-client-id: ${{ vars.AZURE_CLIENT_ID }}
        azure-tenant-id: ${{ vars.AZURE_TENANT_ID }}
  ```

* **Composite actions** — the escape hatch, and what the reusable workflows are built from.
  A consumer whose graph differs (extra gating, an unusual runner pool, a different sink)
  calls `collect` / `analyze-history` / `analyze-pr` / the lifecycle commands directly and
  wires the jobs themselves. Nothing is hidden from them; the reusable workflow is a
  convenience, not a privileged path.

**`pr.yml` owns the scope preflight end to end.** Deciding *which packages a PR should
benchmark* is a prerequisite for the PR flow, and leaving it to the consumer would leave the
hardest part of adoption unsolved while claiming the flow is one `uses:` line. The workflow
therefore computes it, in three steps:

1. **Changed files → owning packages.** The diff against the base names files; each file
   belongs to a package. This is what `cargo-detect-package` answers, and taking a versioned
   dependency on it is preferable to reimplementing the lookup.
2. **Expand to dependents.** A change to a package can move the numbers of anything that
   depends on it, so the set is closed over reverse dependencies within the workspace.
3. **Keep the packages that carry benchmarks.** `cargo metadata` lists every target with its
   kind, so this is a filter over data Cargo already hands us — no bespoke walk, and notably
   *not* a job for a package-detection tool.

The result is the `packages` scope that drives collect (§4.1), and an empty result routes to
the cleanup path instead of an analysis that would find nothing.

Placing this in the **workflow** layer rather than the action layer is the point. Repositories
genuinely disagree about what "affected" means — whether dev-dependencies count, whether a
workspace-wide config change touches everything — so this is exactly the kind of policy that
should be replaceable rather than baked into a published action's contract. A consumer who
disagrees with our answer drops to the composite layer, runs whatever preflight they prefer,
and passes the resulting `packages` to `collect`. They lose the one `uses:` line and gain full
control, which is the trade the two-layer split exists to offer.

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
  list-shaped inputs (the platform matrix, package scopes) are passed as
  **comma-separated strings** and split inside the reusable workflow. JSON-in-a-string is the
  other common encoding and is rejected here: it is fiddly to write in YAML (quoting a JSON
  array inside a YAML scalar), easy to get subtly wrong, and produces an unhelpful failure when
  it is — a malformed array surfaces as a matrix expansion error rather than as "your input is
  wrong". A comma-separated list is what a human would write unprompted, and the same spelling
  works at both layers, so the composite action accepts it too rather than having one list
  syntax for the workflow and another for the action.
* **A caller matrix fans out the whole invocation, not one phase of it.** A matrix job *can*
  call a reusable workflow, but it re-instantiates the entire workflow per combination —
  which would give one analyze per platform instead of the single aggregate analyze the
  design requires. The collect matrix therefore lives **inside** `history.yml`, driven by the
  `platforms` input, so many collect jobs converge on one analyze. This is the main
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

**`pr.yml` handles the PR's whole life, including its close.** A benchmark run takes hours, so
a PR closed or merged mid-run leaves one in flight producing a result nobody will read — a real
cost when each leg occupies a runner. GitHub cancels a superseded run when a *new* run joins the
same concurrency group, so reclaiming that time needs some workflow to start on the close event.
The obvious shape is a second, tiny workflow that does nothing but join the group; the monorepo
has exactly that today.

The action does not export that second file. `pr.yml` accepts the `closed` event itself and
skips every job when it fires: the workflow still starts, still joins the concurrency group, and
the in-flight run is still evicted — the mechanism is identical, but the consumer's surface
stays one `uses:` block instead of two files. The cost is one gate expression inside `pr.yml`,
paid once by us; the alternative charges every consumer an extra file whose purpose is
non-obvious and which is easy to omit, silently losing the cancellation. Trading our complexity
for theirs is the whole point of the layer.

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

**One repository may run several instances, so every derived identity is namespaced.** A
monorepo can reasonably keep more than one independent history — separate components, teams,
or hardware targets, each with its own `[project] id`. Nothing in the analysis prevents that,
but the *action* would collide with itself, because several of the identities it derives
default to a single constant:

| Derived identity | Collision if shared |
| --- | --- |
| PR comment marker | Two instances overwrite each other's comment on the same PR. |
| Failure-issue identity | One instance's `resolve-alert` closes the other's open alert. |
| Machine-key artifact name | Matrix legs of different instances clobber each other. |
| Binary/history cache key | One instance's cache is served to the other. |
| Concurrency group | One instance cancels the other's in-flight run. |

The fix is a naming rule, not new machinery: a single **`instance`** input, defaulting to the
`[project] id` the config already carries, from which *all* of the above are derived. A repo
running one instance never sets it and sees no difference; a repo running two gets two
independent sets of comments, issues, artifacts, caches, and concurrency groups for free, and
the reusable workflows (§4.7) accept it so two `uses:` blocks can coexist.

This is worth settling now rather than later: the comment marker and the issue identity *are*
the dedup keys. Introducing a namespace after consumers are live would change those keys under
them, silently abandoning the rolling comment and rolling issue each repo already has and
starting fresh ones beside the originals.

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

**The GitHub envelope and the whole sink lifecycle live in the companion binary,
`cargo-bench-history-github`.** The name states what it is — the GitHub adapter for
`cargo-bench-history` — and follows the family convention the faker already sets
(`cargo-bench-history-faker`), so the relationship and the ownership are obvious on a
crates.io listing or a `PATH` entry. Naming it after the *action* was considered and rejected:
the action is a distribution wrapper that happens to invoke this binary, while the binary
itself is a GitHub adapter that would still make sense driven by a hand-written workflow, and
tying its name to the wrapper would misstate that. It wraps
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
* **Job graph and gating** — `needs:`, `if:`, concurrency, and the same-repo check are workflow
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

  The ambiguous-create case is not hypothetical, and the storage backend already solves its
  own version of it: a conditional create can commit and then lose its response, so the SDK's
  automatic retry sees "already exists" for an object *it* just wrote. The fix there was to
  carry an opaque request identity on the object and reconcile against it — a matching identity
  proves this writer committed, anything else is a genuine collision. The sink layer faces the
  same shape with the same answer: the hidden marker *is* that identity, so a create that
  cannot confirm its outcome reconciles by reading rather than by guessing.

**No labels.** The action applies none, and offers no input to configure any. Labels look like
free triage value, but they cost more than they return here: `gh issue create` rejects an
unknown label outright rather than warning, so a label named in configuration but absent from
the repository fails the whole filing — and it fails at exactly the wrong moment, since the
paths that file issues are the paths reporting that something is already wrong. This has bitten
the monorepo in production: a label referenced only by the workflow, and never created in the
repository, took down both the regression filing and the failure alert that would have reported
it. Making labels safe means creating them on demand, tolerating the permission to do so being
absent, and exposing per-repository configuration for names that every consumer will want
different — a chain of complexity in service of decoration. Everything works with zero labels
on the issues, so that is what ships. Consumers who want labels can add them by hand or by
their own automation, and label support can arrive later without breaking anyone, because
adding labels to an issue nobody was labelling is not a behaviour change.

**Rolling issues are found by author and marker, never by label or title.** Dropping labels
removes the narrowing mechanism the monorepo's shell layer uses today — it lists issues
carrying a known label and then matches the title client-side — so the companion needs a
replacement, and the one it uses is better anyway. It lists **open issues authored by the
identity it posts as**, which on any runner is the workflow's own token identity, and matches
the **hidden marker** in the body. Author is a strong, free filter: a repository has few issues
opened by its own automation, so the candidate set stays small without anyone configuring
anything. The marker then decides identity exactly, which title-matching never did — a title is
consumer-editable, so matching on it means an edited title silently abandons the issue it was
tracking, and could adopt an unrelated issue that happens to collide. The same marker is the
identity a create reconciles against when its outcome is uncertain (above), so one mechanism
serves lookup, deduplication, and retry safety.

### 5.2 Standard reports, with narrow overrides

Once the message catalogue lives in one binary (§5.1), standardisation is nearly free: there
is exactly
one implementation of each message, so every consuming repo posts recognisably the same
report. That is the default and it requires no configuration. A consumer who sets nothing
gets the full standard set — the in-progress placeholder, the results comment, the staleness
banner, the terminal failure notice, the no-findings state, the coverage verdict, and the
failure-alert issue — all worded identically to every other consumer's.

**One rule binds the whole catalogue: a report never claims more coverage than it measured.**
Every message that reports a result states what it covered — the packages benchmarked, and the
platforms that contributed when they fall short of those intended (§4.2). This is not a
per-sink courtesy but a property of the catalogue, because the states that most need it are
the quiet ones: "no regressions" after a platform silently dropped out, or after a series had
too little history to judge, reads as reassurance nobody computed. Since the messages are
selected by the named `outcome` (§4.2), a sink cannot accidentally present a `partial` run in
the words of a `clean` one, and a sink added later inherits the rule rather than having to
remember it.

Standardisation is the goal, so the override surface is deliberately **narrow and
declarative** rather than a general templating system. Only the things that genuinely differ
per repo are adjustable, and each is a value, not a format:

| Slot | Why it varies |
| --- | --- |
| Issue title | Appears in the issue list, so it should read the way the repo's other issues do. |
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

**Fork PRs are not supported.** The PR flow is **same-repo only**. A run executes the
contributor's code and needs credentials to reach the history store, and GitHub gives a fork PR
neither secrets nor an OIDC token — so there is no identity such a run could use, and the ways
around that (a publicly readable store, or splitting the credentialed half into a second
trusted stage) each carry design and operational weight that is not worth paying before anyone
has asked for it.

So the flow **detects a fork PR and stops early with a clear message** — that benchmarking is
skipped because the PR comes from a fork, which is a supported state rather than a
malfunction — and posts nothing. The message matters more than it looks: the failure mode
this replaces is a workflow that silently does nothing, leaving a contributor to wonder whether
benchmarking is broken, queued, or deliberately off. Nothing else about the design is
fork-aware, and no input configures this.

This is a deferral, not a judgement that fork support is undesirable. Revisiting it means
choosing between the two approaches above; nothing in the current design forecloses either,
because the property they both rely on — that a PR run only ever *reads* the shared store,
since its own points are disposable (§4.5) — holds regardless.

**PR analysis reads the same production store as the trunk.** Branch mode compares the PR head
against the trunk's recorded baseline, so the PR flow must read the very store that holds it —
a separate PR store is rejected because it would have no baseline to compare against.

**Bring-your-own infrastructure.** The action does **not** bundle the Azure provisioning
(`infra/azure-bench-history-prod/`); that stays in the monorepo as a *referenced example* the
README links to. A consumer points the action at whatever account/identity they already have.

**Caller permissions** (documented in the README): `contents: read` (checkout);
`actions: read` (cross-attempt artifact download, §4.6); `id-token: write` (Entra OIDC
self-minting); `issues: write` (history flow, when `issue-on-regression: true`, and the
`alert`/`resolve-alert` failure lifecycle); `pull-requests: write` (branch flow's comment and
its lifecycle). These are listed **per command**, not as one union: a job should hold only
what the command it runs actually needs.

**No job holds both storage credentials and posting rights.** Those two capabilities are
what an attacker would want to combine, and nothing requires them together. Analysis reads
the history store; publishing writes to the repository; neither needs the other's access. The
flows therefore split them across two jobs, passing the report between them as an artifact:

| Job | Holds | Cannot |
| --- | --- | --- |
| analyze | `id-token: write` (+ `contents: read`) | post anything |
| publish | `issues:` / `pull-requests: write` | reach the history store |

The split costs one artifact hand-off and buys a real reduction in blast radius: a compromised
tool binary cannot post to the repository, and a compromised companion cannot read or corrupt
the benchmark history. It also falls out of the §5.1 division rather than being bolted on —
the companion never had a reason to see storage, and the tool never had a reason to see a
GitHub token. The same reasoning is why the release manifest pins what it pins (§8.1): these
binaries run inside credentialed jobs, so *which* binary runs is itself a security property.

## 7. Interface summary (inputs / outputs)

This section describes the **composite action** — the building-block layer. The reusable
workflows (§4.7) accept a smaller, flow-shaped subset of the same names (plus `platforms`,
the comma-separated runner matrix) and pass the rest through, so a consumer on the default path
sees only the inputs their flow actually varies.

**Common inputs:** `command` (`collect` | `analyze-history` | `analyze-pr` | `backfill` |
`pr-comment-preflight` | `pr-comment-cleanup` | `pr-comment-finalize` | `alert` |
`resolve-alert`, required);
`install-method` (`binstall` | `install` | `path` | `none`, default `binstall`; applies to
every binary the command needs, §3);
`tool-version` (package version to install; defaults to the version named in the action
release's manifest; `latest` opts into the newest published release; ignored for
`install-method: none`; the companion is pinned by the release and is not overridable, §3);
`tool-path` / `companion-path` (for `install-method: path`); `config` (path to a
`bench_history.toml`; default: the tool's own `.cargo/bench_history.toml` discovery);
`instance` (namespace for every derived identity — comment marker, failure-issue identity,
artifact names, cache keys, concurrency group; default: the config's `[project] id`, §5);
`local-path` (→ `--local`); `verbose` (default `true`, §4.1).

**Cargo build inputs (`collect` / `backfill`):** `all-features` (default `true`, matching the
flows' need to reach benchmark targets gated behind `required-features`),
`no-default-features`, and `features` (a list). Without these a repo whose benches sit behind
a feature would silently measure nothing.

**`collect` inputs:** `packages` (comma-separated list → `--package` per name; empty → whole
workspace); `exclude`, `bench` (→ repeated flags); `best-of` (→ `--best-of`, default 1);
`machine-key` (override; default auto-detect); `on-existing` (`error` (default here) | `skip` |
`overwrite` → neither / `--skip-existing` / `--overwrite`; §4.5); `recollect-commit` (a SHA →
backfill-and-overwrite that commit in a throwaway worktree; §4.1). **Output:** `machine-key`
(this leg's fingerprint, for the analyze handoff).

**`analyze-history` inputs:** `machine-keys` (directory of collected per-platform keys →
repeated `--machine-key`); `cache` (→ `--cache`; mutually exclusive with `local-path`);
`since` (look-back window; default: the tool's history default); `issue-on-regression`
(default `false`); `issue-title` (the displayed title; identity comes from the hidden marker,
not from this, §5.1).

**`analyze-pr` inputs:** `pr-number` (which PR to comment on); `base` (→ `--base`; **no
built-in branch name** — the reusable workflow passes the PR event's own base ref, and the
composite layer falls back to the tool's configured default-branch resolution, so a repo whose
trunk is not `main`, or a PR targeting a release branch, is compared against the right thing);
`context` (→ `--context`; default `HEAD`); `machine-keys`; `cache`;
`comment-marker` (hidden dedup marker; default the action's own). Improvements are reported
unconditionally in branch mode, so there is no direction input.

**Lifecycle-command inputs:** `pr-comment-preflight` / `pr-comment-cleanup` /
`pr-comment-finalize` take `pr-number` and `comment-marker`, plus (preflight) the `packages`
scope to disclose and (finalize) the failed run's URL; `issue-preflight` / `issue-cleanup` take
`issue-title`, and `issue-cleanup` additionally takes **`auto-close`**
(default `false`, §4.4); `alert` / `resolve-alert` take `issue-title`.

**`backfill` inputs:** the same scope inputs as `collect` (`packages`, `exclude`, `bench`,
`best-of`), plus the history window to densify and `on-existing` (which defaults to `skip`
here, §4.5, because densification must stay resumable).

**Report-wording inputs (all commands with a sink):** the narrow override slots of §5.2 —
`issue-title`, an intro line, a docs link, and `comment-marker`. There is
deliberately no template input and no label input (§5.1). A `sink` input (`standard` (default)
| `none`) turns the
built-in posting off entirely for a consumer who wants to publish their own report from the
outputs below; with `sink: none` the analyze commands compute and emit, and post nothing.

**Outputs (from `analyze-history` / `analyze-pr`):** `outcome` (`findings` | `clean` |
`insufficient-baseline` | `nothing-in-scope` | `partial` | `failed` — the named end state, §4.2);
`notable` (`true`/`false`, retained as the convenience boolean for simple gating, and defined
as `outcome == findings`);
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
* **README is a quick start; the book is the reference.** Two documents describing the same
  action drift, and the one that drifts is always the one a maintainer forgets — so they get
  clearly different jobs rather than overlapping scopes. The **README** answers "what is this
  and how do I switch it on": a sentence on what the action does, the three `uses:` snippets of
  §4.7 (history, PR, nightly densification), the permissions each needs, and a link onward for
  everything else. It stops there deliberately; a reader who needs more is a reader the book
  serves better. The **book's GitHub-automation section** (§11) is the reference: the input
  surface, the hand-assembled recipes for repos whose job graph differs, the deployment
  profiles, and how to read a report. It is also where the action's material sits next to the
  tool concepts it depends on — engines, comparability, analysis modes — which is the context a
  reader configuring a pipeline actually needs, and which a README cannot supply without
  restating the whole guide.

  The hand-assembled recipes the book carries are the expansions of the three workflows:
  * A **per-push history** workflow — a `fail-fast: false` matrix `collect` job across the
    platforms (each `on-existing: skip`, uploading its `machine-key` output as a per-platform
    artifact), then an `analyze-history` job (`needs: collect`, `fetch-depth: 0`, downloading
    the key artifacts into the `machine-keys` dir **with an explicit `github-token`** so
    partial re-runs resolve, §4.6, an `actions/cache` step feeding `cache`,
    `issue-on-regression: true`), plus the `issue-preflight` / `issue-cleanup` upkeep and the
    `alert` / `resolve-alert` failure lifecycle.
  * A **per-PR branch** workflow — a delta preflight computing the touched benchmarkable
    packages, a `pr-comment-preflight` job (in parallel with collect), a matrix `collect` job
    scoped by `packages`, an `analyze-pr` job (checkout `head.sha`, `fetch-depth: 0`,
    restore-only cache, gated `!cancelled()` so a superseded run never posts) posting the
    comment, plus the `pr-comment-cleanup` and `pr-comment-finalize` paths — all behind the
    same-repo check (§6).
  * A **nightly densification** workflow (§4.8) — a matrix `backfill` job over the same
    platforms and window, with no analyze job and no sink.
  * The **concurrency** pattern: PR-driven runs
    cancel superseded runs keyed on the ref, and the close event is handled by `pr.yml` itself
    (§4.7) rather than a second workflow; the push flow deduplicates the same commit instead.
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

**Layer 2 — local-storage end-to-end on the CI matrix (every push, minutes, no secrets).** `test.yml` runs the *real* action against **local filesystem storage**
(`local-path` under `${RUNNER_TEMP}`) across the platform matrix and across *each* real
`install-method` (`binstall`, `install`, `path`, and `none` against a pre-seeded `PATH`),
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
   most work: matrix expansion from the `platforms` input, fan-out-then-converge onto one
   analyze, permission narrowing, the same-repo check, artifact aggregation, and concurrency.
   The
   canaries deliberately include the ugly cases — a **partially failed** matrix, a **fully
   failed** matrix, a malformed `platforms` list, an **empty package scope** (which must route
   to cleanup, not analyze), a **fork PR** (which must stop with the skip message, §6), and a
   cancelled run — since each is a path where the graph, not
   the binaries, decides the outcome. The two layers' input lists are contract-tested against
   each other so a new composite input cannot silently go unexposed by the workflows.

This proves the wiring and the real installs, but it deliberately never posts to GitHub and
never sees a *long* history — the two things Layer 1 could only mock.

**Layer 3 — trend-dependent, real-GitHub-write validation.** The remaining gap is the behaviour
that needs (a) a benchmark history long enough to make analysis "notable" and (b) the action
actually writing to GitHub. The tool already ships the two enablers this needs — the hidden
`cargo-bench-history import` command and the published `cargo-bench-history-faker` engine
(§11) — so none of the options below requires new tooling, and none requires a credential we
would have to create or maintain. Three options, roughly increasing in
fidelity and cost. The design adopts **A + B**; C is a cheaper fallback if the real-repository
runs prove impractical.

* **Option A — synthetic history driven against a real repository (highest fidelity).**
  `cargo-bench-history-faker`
  writes curated per-engine output into a `target/`-shaped tree (inventing nothing — every value
  comes from its flags), and `cargo bench-history import --target-dir <tree>` stores that output
  through the exact `collect` finalize-and-store path *without running `cargo bench`*. Crucially,
  `import --commit <ancestor>` keys a stored point to any existing commit **without checking it
  out**, so a single HEAD position can fabricate a whole multi-commit series (a planted
  regression, a planted improvement) by looping faker→import over real ancestor SHAs. Both
  binaries are published and `binstall`-able, so the test needs **no vendoring and no
  workspace checkout** — it installs them like any consumer.

  **The repository under test is the action repo itself, not a separate sandbox**, and that
  choice is what keeps the credentials problem from existing. A workflow's built-in
  `GITHUB_TOKEN` is minted per run, expires with the job, is scoped to the repository it runs
  in, and needs no setup or rotation — so a job in the action repo that grants itself
  `issues: write` and `pull-requests: write` can exercise every write path the companion has:
  open a scratch issue, update it, resolve it to all-clear, open a throwaway PR, post the
  comment, re-post to prove in-place update, apply the staleness banner, finalize, clean up —
  each asserted by reading the live repository back. A *separate* sandbox repository is what
  would force a long-lived credential, because cross-repository access needs either a personal
  access token or a GitHub App private key, both of which are exactly the maintained secret we
  refuse to introduce. Testing in-place removes the requirement rather than managing it.
  Because these runs mutate real issues and PRs (in a repo whose purpose is to host them), they
  are scheduled and pre-release rather than on every push, and their artefacts are namespaced
  by `instance` (§5) so they cannot collide with anything real.
* **Option B — compose-only assertions against a faked transport (fast, no writes).** Reuse the
  Option A
  faker→`import` history into local storage, run `analyze-history` / `analyze-pr`, and assert the
  *exact* issue/comment body the action *would* post against a **faked GitHub transport** — no
  live posting.
  This catches body/marker/banner regressions with a realistic multi-commit trend without
  touching a real issue or PR, so it can run on every push. (It is
  Layer 1's body assertions, but fed
  a real long history instead of a one-off fixture.)
* **Option C — checked-in storage fixtures (cheapest, least fresh).** Commit a small
  pre-built store (a handful of result sets across synthetic commits) and replay it through
  `analyze-history` / `analyze-pr`, asserting `notable` and the reports. It skips faker/`import`
  entirely, but the fixtures are opaque and must be regenerated by hand when the storage format
  changes — so it is only a stopgap where the faker→`import` path is unavailable.

**Dogfooding is the fourth layer, and in practice the most valuable one.** Folo runs these
flows on every push and every PR against a real repository with a real multi-year history
(§10), which is precisely the condition the layers above simulate. It exercises the write
paths continuously, on data no fixture can imitate, and it does so with no extra credentials
because it is the repository's own `GITHUB_TOKEN` doing the writing. Its limitation is that it
only ever tests **one** configuration — the one Folo happens to use — so it can confirm the
common path works while saying nothing about the rest of the input surface. That is exactly
the gap Options A and B fill: they cover the configurations nobody's production repo happens
to have, and dogfooding covers the realism no test fixture can.

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

**What `install-method: path` requires, concretely.** The action executes inside the *caller's*
job, so the workspace it builds from is the caller's own checkout — nothing needs to be shared
between the two repositories, and the action being hosted elsewhere costs nothing here. Two
real constraints do apply, and both are properties of the input rather than obstacles:

* **The path names the package directory**, because no layout is universal. Folo keeps packages
  under `packages/<name>`, which is a convention, not a rule, so the input must not assume it.
* **All binaries come from the same checkout, or none do.** A HEAD-built tool paired with a
  released companion is a version mix nobody tested; since `path` exists precisely to exercise
  unreleased code, it applies to every binary the command needs (§3).

The cost is a source build in the job, which for Folo is already paid — the workspace is being
compiled to benchmark it. The payoff is that a tool change is exercised by the same push that
lands it, rather than waiting for a release.

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
  needs no new logic, but the new package joins the `cargo-bench-history` version group in
  `[workspace.metadata.release-plan.groups]` — so it moves in lockstep with the tool, exactly
  as the faker already does — and needs its crates.io trusted-publisher setup like any other
  published package. Its version increment then rides the ordinary process: the pull request
  that changes released content carries the increment, and merging publishes. The one genuinely
  new piece is the pre-tag resolvability gate of §8.1, which lives in the action repo.
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
  engine — **both already published** — together let a test job fabricate a realistic
  multi-commit history from published
  binaries alone. No new command is needed for even the highest-fidelity testing option.
* **No fake-engine handling needed** — the fake engine is its own separate package
  (`cargo-bench-history-faker`) with its own binary, so the published `cargo-bench-history`
  already ships a single binary (`DESIGN.md` §9). The action installs the plain package name.
* **The monorepo's own shell layer becomes redundant.** Folo's
  `scripts/bench-history/*.psm1` modules exist because nothing else could compose or post;
  once the tool and the companion can, they are replaced by the action rather than generalized
  into it, and Folo's bench-history workflows collapse into calls to the reusable
  workflows (§4.7, §10) — including `cancel-pr-bench-history.yml`, whose whole job `pr.yml`
  now absorbs (§4.7).
* **Docs — the book gains a "GitHub automation" section.** The user guide currently
  documents the tool as something you run by hand (Installation, Commands, Concepts,
  Appendix), which leaves its most common *real* deployment undocumented. Running
  `cargo-bench-history` from automation is not a footnote to local use; it is the primary use,
  and it raises questions that have no local analogue. The book therefore gains a section
  covering:
  * **The three flows** — per-push history, per-PR branch, nightly densification — and why a
    useful setup runs all three rather than just the first.
  * **Adopting the action** — the full input surface and the hand-assembled recipes, with the
    action's README reduced to a quick start that links here (§8).
  * **Deployment profiles** — the shared, rotating, ephemeral runner pool versus dedicated
    self-hosted benchmark machines. These differ in almost every way that matters (machine-key
    stability, noise floor, whether densification is needed at all, useful `best-of` values),
    and a reader choosing hardware needs that comparison before they build a pipeline around
    one of them. This is **documentation, not a supported configuration we exercise**: our own
    runs are on shared public runners, and the dedicated-hardware profile is described so a
    reader can reason about the trade rather than because we test it.
  * **What automated measurements mean** — that PR measurements are keyed to branch commits and
    are discarded by squash- and rebase-merges (§4.5), so the trunk series is fed by the history
    and densification flows alone.
  * **Reading a report** — in particular telling apart "no findings", "not enough baseline
    yet", "some platforms did not report", and "nothing benchmarkable changed" (§4.2).

  The section is titled **GitHub automation** rather than "Continuous integration": the latter
  is a vague umbrella that says nothing about what the section contains, while this section is
  specifically about driving the tool from GitHub — the flows, the action, the reports it
  posts. A reader looking for it will be looking for GitHub, and a future reader adding, say,
  a self-hosted-runner chapter will know whether it belongs here.

  The division of labour: the **book** owns tool-level concepts, the deployment thinking, and
  the action's reference material; the action repo's **README** is a quick start that links
  here. This file and the pointer from `DESIGN.md` §7.3 remain the design record until it is
  dismantled into those homes.

## 12. Implementation plan

The work splits into phases that each **land independently, prove themselves, and leave the
system working**. Two properties drive the ordering. First, every phase before the last is
invisible to consumers, so a stalled effort never leaves a half-published action in the
Marketplace. Second, the risky, hard-to-test parts are pulled early and validated *inside the
monorepo*, where the existing workflows already exercise them against real data — so by the
time anything is published, its behaviour has been running in production for weeks.

The phases are ordered so that each is testable by the layer below it, and the monorepo's own
bench-history workflows act as the integration test throughout: at every phase they keep
working, first unchanged, then progressively rewired.

**Phase 0 — Manual preparation (you).** Detailed in §12.1; the only phase requiring repository
administration. Phases 1–3 do not depend on it, so it can happen in parallel.

**Phase 1 — Tool-side rendering.** Add the coverage verdict in prose and the named `outcome`
to `analyze` (§4.2, §5.1). Pure additions to an existing command, testable as ordinary Rust
unit tests beside the renderer, with no GitHub or workflow involvement. Independently valuable:
the verdict improves local `analyze` output immediately.

**Phase 2 — The companion binary.** Create `cargo-bench-history-github` (§5.1) with its GitHub
transport behind a port trait and an in-memory fake, following the package's existing ports-and-
fakes convention. This phase covers the whole message catalogue and every lifecycle command,
and is where the bulk of the logic lands — all of it unit-testable against the fake with no
network. Add it to the version group. Nothing consumes it yet.

**Phase 3 — Cut the monorepo over to the companion.** Replace `scripts/bench-history/*.psm1`
with calls to the companion, keeping the existing workflow structure. This is the highest-value
validation in the plan: the monorepo's real push and PR flows start exercising the companion
against real issues, real comments, and real history, while the workflows around them are
unchanged and can be reverted in one commit. Deleting the PowerShell modules and their Pester
suites is the phase's completion signal. The new lifecycle commands (`issue-preflight`,
`issue-cleanup`, `pr-comment-finalize`) land here too, so their behaviour is observed on a real
repository before anyone else can adopt them.

**Phase 4 — The composite action.** Stand up the action repo with `action.yml` and the
`command` surface (§4, §7), implemented as thin wiring over the binaries from phases 1–3. Its
own CI runs Layers 1 and 2 (§9). Still unpublished, and still consumed by nobody.

**Phase 5 — The reusable workflows.** Add `history.yml`, `pr.yml`, and `backfill.yml` (§4.7),
including the scope preflight and the matrix. Validated by the caller canaries (§9), which are
the only test of this layer.

**Phase 6 — Dogfood.** Point the monorepo's workflows at the reusable workflows with
`install-method: path` (§10). Folo's ~1,200 lines of workflow YAML collapse to three `uses:`
blocks. This is the last chance to find interface problems while we are the only consumer, and
the phase where the design's central claim is either demonstrated or falsified.

**Phase 7 — Publish.** Cut `v1`, list on the Marketplace (§8.1), and write the book's
GitHub-automation section and the action README (§11). Documentation lands with the release
rather than before it, since it describes behaviour the previous phases have by then proven.

### 12.1 Phase 0 — what needs your hands

None of this can be done from a pull request, and everything else can proceed while it is
pending. In rough order of when it is needed:

| # | Action | Needed by | Notes |
| --- | --- | --- | --- |
| 1 | **Configure `folo-rs/cargo-bench-history-action`** — see §12.2 | Phase 4 | The repository exists; this is its settings. |
| 2 | **Add `cargo-bench-history-github` as a crates.io trusted publisher** | Phase 2 (before its first publish) | Same one-time setup every published package here needs. Cannot be scripted from a PR. |
| 3 | **Enable Marketplace publishing** on the action repo — accept the agreement, choose a category, verify the listing | Phase 7 | A one-time UI flow tied to the account, not to a release run (§8.1). |
| 4 | **Decide the `v1` promise** — when the floating major tag starts moving, its consumers inherit whatever it points at | Phase 7 | Worth an explicit decision rather than discovering it after the first breaking change. |

Three things I want to flag as **not** needed, because they would each be reasonable to assume:

* **No new secrets or tokens.** Every phase runs on the per-run `GITHUB_TOKEN` (§9) or the
  existing Azure federation. Nothing here introduces a credential to create or rotate.
* **No separate test repository.** Testing happens in the repository that runs it (§9), which
  is what removes the cross-repository credential problem entirely.
* **No issue labels.** The action applies none and configures none (§5.1), so no repository
  needs labels created before it can file.

### 12.2 Configuring the action repository

The repository holds YAML, Markdown, and release tooling — no Rust, no published binaries
(§2) — so its settings are about protecting the tag stream consumers depend on, not about
build validation.

* **Protect `main`**: no direct pushes, no force-pushes, no deletion. Its ref is what
  `uses: …@v1` ultimately resolves to.
* **Require a pull request to merge**, with the repository's CI as the required check. The
  repo's own CI is Layers 1–2 and the caller canaries (§9); wire the required check as a single
  fan-in job rather than naming individual matrix jobs, matching how the monorepo does it, so
  adding a canary does not require a settings change.
* **Allow the release workflow to move tags.** Releases are cut by pushing `vX.Y.Z` and
  force-moving `v1` (§8.1), so whatever protection covers tags must permit the release
  workflow — and only it — to do that. This is the one setting where getting it wrong is
  discovered late, at the first release.
* **Grant the release workflow permission to create releases** (`contents: write` in that
  workflow), since publishing a GitHub Release is what refreshes the Marketplace listing.
* **Merge policy is yours.** Squash, merge commit, or a merge queue — nothing in the design
  depends on it. The action repo has no benchmark history, so the squash-merge consideration
  of §4.5 does not apply here.

Deferred until the repository actually has content: branch protection is worth setting now,
but the required-check name cannot be chosen until Phase 4 creates the workflow that produces
it. Setting protection first and adding the check requirement in Phase 4 avoids a chicken-and-egg
block.
