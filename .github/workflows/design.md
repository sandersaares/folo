# GitHub workflows design

The high-level design of this repository's CI/CD workflows: the patterns they share,
the tenets behind them, and how the pieces relate. Per-job mechanics live in inline
YAML comments and in the `just` recipes the steps call; this document stays high-level.
Ownership of the release-validation pipeline is in [implementation.md](implementation.md).

## Job granularity and gating

Validation runs each `just` command as its own parallel job rather than one combined
`validate-local` step. Parallelism gives faster feedback and pinpoints failures by check
name instead of burying them in a monolithic log. Expensive jobs gate behind cheaper
equivalents so a fast failure short-circuits slow work — for example, Miri and mutation
testing only start once the dev Clippy pass and the base test pass have already succeeded,
since there is nothing to interpret or mutate in code that does not compile or whose tests
already fail. Clippy stands in for a bare `cargo check` here: Clippy compiles the code as a
prerequisite to linting it, so a standalone `check` job would only re-prove what a green
Clippy already guarantees.

## Selective validation

Most jobs are package-scoped and skip packages a change does not touch: a `delta` job
computes the affected set and downstream jobs consult it, so a one-package PR does not
rebuild the workspace. The complement of this pattern is the rule that a job whose inputs
are **not** Cargo packages — the workflow files themselves, or the standalone PowerShell
under `scripts/` — must run unconditionally. Delta analysis reports "nothing affected" for
such a change, so gating those jobs on it would leave the change validated by nothing.
Release-plan generation (`validate-versions`) is in that class: it compares every
publishable package's released content to that package's version anchor. Gating it on
delta's changed-package set would skip a package that already needed an increment.

## Platform strategy

Test passes are organised as an x86_64/ARM64 pair. The x64 pass carries coverage
instrumentation (which needs a nightly-only toolchain component), while the ARM pass
doubles as the MSRV pass and exists to exercise architecture-gated code that x86_64 runners
never compile. macOS is Apple Silicon, so it rides the ARM pass. Miri follows the same
shape: it is an architecture-agnostic interpreter, so a second ARM run earns its keep only
by subjecting ARM-gated paths to Miri's UB detection. Platform-agnostic checks (formatting,
workflow validation, script tests) run on a single Linux runner because their result cannot
vary by platform.

Not every check earns its place on every pull request. The full matrix runs on each push to
`main`, but pull-request validation prunes the rarely-informative legs to cut runner cost,
leaning on push-to-`main` as the backstop for what it drops. PRs run the test and docs
suites only on the x86_64 Windows and Linux runners: the whole ARM pass (which carries the
MSRV *test* run) and the macOS legs of the test and docs jobs wait for `main`, because
architecture- and OS-gated behaviour rarely diverges on a PR and re-running the
platform-independent test and doc suites on macOS almost never is informative. The base Miri
pass runs on Windows only for a PR — being an architecture-agnostic interpreter, its Linux
and ARM re-runs are a `main`-only sanity net over cfg-gated paths — and the release-profile
Clippy pass and the `careful` run are skipped entirely on PRs. The many-seeds Miri passes are
the exception to that pruning: gated by *package* rather than by event, they run on Linux —
on a PR as much as on `main` — whenever their specific package is touched, because their
worth is catching seed-dependent UB in that code, not covering a platform. The
compile-oriented passes (dev Clippy, release build, frozen-minimum check, feature `hack`)
deliberately keep their macOS leg on PRs, because a cheap macOS cross-compile still catches
macOS-specific build breaks that the pruned runtime passes would not. MSRV *compilation*
therefore stays covered on every PR by `check-frozen`, which compiles all targets on the
MSRV toolchain against the frozen minimum-version lockfile even though the ARM MSRV test
pass is `main`-only. Because a push to `main` is the first place the pruned checks can fail,
that event — unlike a PR — files a tracking issue (see Failure alerting).

The event split is expressed two ways: a job whose every leg is pruned on a PR (the ARM test
and Miri passes, `clippy-release`, `careful`) carries a whole-job `github.event_name ==
'push'` guard, while a job that keeps some legs on a PR (macOS-dropping test/docs, the
Ubuntu-dropping `miri-x64`) selects its platform list with a `fromJSON` conditional matrix
keyed on the same event. Both reduce to "the full set on push, the pruned set on a PR".
A `merge_group` (merge queue) run uses that same pruned set: those guards are false for
anything that is not `push`. Do not rewrite them as `!= 'pull_request'`, or a queue entry
would take the full matrix. Push to `main` remains the backstop.

Delta analysis on a queue run uses `merge_group.base_sha` (the commit the queue rebased
onto) rather than a freshly fetched `origin/main`, so the affected-package set cannot
drift from the version check's base. Pull requests keep today's `origin/main` baseline.
Push to `main` still skips delta and validates the whole workspace.

## External type surface

A dedicated job fails validation when a library exposes an external type — one that is
neither a standard-library type nor defined by the crate itself (a type from another crate,
first-party or not) — in its public API without that type being listed in the crate's
allow-list. The intent is to catch *accidental* additions to the external surface (a leaked
dependency type, a forgotten `pub`), not to prohibit external types outright; an intentional
exposure is admitted by adding it to the crate's
`[package.metadata.cargo_check_external_types]`. The user-facing principle and the allow-list
mechanics live in `docs/external-types.md`.

The check drives nightly rustdoc's unstable JSON output, which pins an exact schema version,
so it runs on its own pinned nightly (`RUST_NIGHTLY_EXTERNAL_TYPES`) held separate from the
general nightly and bumped only in lockstep with the tool. Like the other package jobs it is
delta-scoped, iterating the affected crates one manifest at a time and leaving the tool's
`--skip-unsupported` flag to pass over crates it cannot document (proc-macro and binary-only).
Because a public API can differ by platform through cfg-gated items, the surface is verified
on both a Unix and a Windows target, so a Windows-only or Unix-only leak cannot slip through.
Two targets suffice because platform-variant public surface here is gated only on
`cfg(windows)`/`cfg(unix)` (Linux stands in for macOS) or hidden behind a platform abstraction
layer with an identical public facade, and nothing public is `target_arch`-gated;
`docs/external-types.md` records the assumption and when the matrix must grow.


## Concurrency

Commit-driven and PR-driven workflows cancel superseded runs, keyed on the ref, so pushing
a new commit abandons the outdated run. That supersession only fires when a *new commit*
arrives on the branch, so closing or merging a PR — which pushes nothing to the PR branch —
would otherwise leave its in-flight Validation run to burn to completion. A dedicated
companion workflow closes that gap: it triggers on the PR-close event and joins the target
workflow's concurrency group so cancel-in-progress reclaims the stale run. Both the Validation
workflow and the PR benchmark-history workflow pair with such a close companion. Validation's
group (`github.head_ref || github.ref`) already distinguishes merge-queue entries: `head_ref`
is empty there and `github.ref` is the unique queue ref. The close companion stays
pull-request-only. The exception
is history collection on `main`, which is keyed on the commit **SHA**: each commit is a distinct
measurement, so distinct commits must run in parallel and only a redundant re-trigger of the
*same* commit is deduplicated. A schedule-driven workflow carries a concurrency block only when
a duplicate run would be expensive: the nightly history backfill groups on itself with
cancellation **off**, so a manual dispatch queues behind the scheduled run rather than
duplicating hours of benchmarking. Keeping it out of collection's SHA-keyed group matters for
the same reason — a scheduled run's SHA is the current tip, so a shared group would let the
nightly and that tip's own collection cancel each other.

## Thin steps

Workflow steps stay thin. Non-trivial logic lives in PowerShell `[script]` `just` recipes
the steps call, so it runs and is debugged locally instead of only by pushing to `main`.
Logic worth unit-testing goes one level deeper into a module under `scripts/` covered by a
Pester suite. Every `run:` step uses `pwsh`; the `setup-environment` composite is the sole
Bash holdout because it bootstraps PowerShell itself.

## Pull-request version readiness

Published content changes require an explicit **change level** — `breaking`, `nonbreaking`, or
`patch` — decided before release. The change level describes the substance of the change;
tooling maps it to a Cargo increment level or an exact target version. This applies to the
workspace as a whole rather than only packages selected by delta analysis: an earlier change can
remain pending even when the current pull request does not touch that package.

Release state is read from the branch that publishes, not from the branch a pull request
targets, so a stacked pull request is assessed against the same baseline as any other and a
parent branch's pending increment is never mistaken for a release. A merge-queue entry is the
exception: it is assessed against the commit the queue rebased it onto, so its scope matches
what will actually land.

Version increments follow Cargo's compatibility rule rather than plain semantic versioning:
the leftmost non-zero component acts as the major component, so a compatible change to a 0.y
package advances its patch component and a 0.0.z package has no compatible increment at all.

Automated API comparison supplies evidence for those decisions but does not replace semantic
review. It is fail-closed when its input format or execution is unsupported. Comparisons cover
only packages whose surface is a supported consumer contract. Published implementation and
test-support packages have no supported consumer contract of their own; changes in a grouped
implementation package are assessed through the owning public package instead.

The checks support a valid empty consumer-contract set without turning that case into a
workspace-wide comparison.

A change level rests on released evidence rather than on the version a manifest already declares.
A package's own released-content diff, the workspace values it inherits, the locked dependencies
an executable releases, and the decisions taken for its dependencies all participate, and any
package-metadata change establishes at least `patch`. A version group whose members disagree is
realigned mechanically and needs no change level of its own: normally onto the highest version its
members declare, so nothing is published for a change it did not make, but by a patch increment of
the whole group where that alignment would rewrite a requirement inside a member that otherwise
kept an already-published version. A package the release baseline has never published takes the
first-publication path rather than an increment. The [`increment-versions`
skill](../skills/increment-versions/SKILL.md) carries out this policy and owns the procedure.

## Required checks fan-in

Validation posts a fan-in job whose GitHub check name is the ruleset string. GitHub's
required-checks field is a string match on that name: it cannot express "this matrix
job, but only the legs that actually ran", and it cannot see a check that was skipped
rather than posted. A job with both `strategy.matrix` and a job-level `if:` that evaluates
false never expands the matrix, so contexts such as `test-x64 (ubuntu-latest)` stay on
Expected — Waiting for status to be reported forever if they are listed as required.

A ruleset that requires merge-blocking Validation therefore lists only this fan-in. The
job is `if: always()`, `needs:` every merge-blocking job in Validation (including
`validate-versions` and `semver-checks`), succeeds when every dependency reports `success` or an
allowed `skipped`, and fails on `failure`, `cancelled`, or any other result.
Unconditional gates may not skip. Advisory jobs stay off that list. `alert` stays off it
— it files issues on a failed push to `main`, it is not a merge gate.

When a new merge-blocking job is added to Validation it is added to this `needs:` list; it
is never added to the GitHub ruleset. Unconditional gates are also named in the fan-in's
must-succeed list. Matrix jobs that can skip via a job-level `if:` can only be made
required through this fan-in.

`alert` keeps a `needs:` list of its own, which also names the advisory jobs the fan-in excludes,
so a new job joins both. The two lists answer different questions — what blocks a merge, and what
is worth an issue after a push to `main` — and folding `alert` onto the fan-in would tie issue
filing to the fan-in's skip policy and begin filing issues for cancelled runs.

## Published user guides

Markdown user guides live under `packages/<package>/book/` and are published together as one
GitHub Pages site. The book workflow discovers that convention rather than keeping a second
catalog, builds each book in an independent matrix leg, and merges the resulting artifacts under
`/<package>/`. A generated landing page at the site root reads each book's title and description
from `book.toml`, so adding a book requires no workflow edit.

Pull requests build every discovered book but never deploy, providing a real rendering check
without publishing unmerged content. Pushes to `main` and explicitly dispatched runs assemble the
artifacts, generate the landing page, and deploy through the GitHub Pages artifact flow. mdBook and
its preprocessors are version-pinned in `constants.env` so local and hosted builds use the same
rendering toolchain.

## Coverage reporting

Coverage is a side effect of the ordinary test run, not a separate re-execution. A single
commit therefore produces several coverage uploads — one per platform, plus a conditional
upload from the Azure-backend job. A platform upload is itself conditional on a report
existing: a delta-scoped run can measure only packages that carry no instrumented code, which
yields nothing to report, nothing to upload and nothing to notify about. Codecov is configured
to hold all notifications until a final gate job signals that every expected upload for the
commit has landed, so the reported figure is computed from the complete set rather than
flapping as partial uploads arrive. The gate keys off "every expected upload succeeded or was
legitimately skipped", never off a hardcoded upload count, because the Azure upload is
conditional; when no coverage landed at all, it releases nothing.

## Azure backend testing

The `cargo-bench-history` Azure storage backend is validated in layers of increasing
fidelity, each an additive sibling of the last: against a local Azurite emulator (the layer
that also feeds coverage), against a real Azure Storage account with shared-key access
disabled (proving real Microsoft Entra ID signature validation the emulator fakes), and
against that same account through the tool's self-minting GitHub OIDC credential (the exact
path the production history collection depends on). The backend's network paths self-skip
when no emulator or account is reachable, so the ordinary multi-platform test jobs stay
green without one; the Azure jobs flip that skip into a hard failure so a misconfigured job
can never silently pass by testing nothing. All Azure authentication uses GitHub OIDC
workload-identity federation — no long-lived secret is stored — and is gated to same-repo
runs, since a fork cannot federate into the tenant.

## Federated identity

Every Azure sign-in in these workflows uses GitHub OIDC workload-identity federation, so no
long-lived storage secret is ever committed or held as a repository secret. A job requests a
short-lived GitHub OIDC token (`permissions: id-token: write`), and Azure exchanges it for
managed-identity credentials only when the token's *subject* matches a federated credential
registered on that identity. The subject encodes the triggering event: a push to a branch
presents `repo:folo-rs/folo:ref:refs/heads/<branch>` (e.g. `…:ref:refs/heads/main`), while a
pull-request run presents `repo:folo-rs/folo:pull_request`. The audience is always
`api://AzureADTokenExchange`. The two non-secret identifiers a job needs — the managed
identity's client id and the tenant id — live in `constants.env` and are remapped to the
standard `AZURE_*` names the tool and `azure/login` read (`AZURE_PROD_CLIENT_ID` →
`AZURE_CLIENT_ID`) by a single shared federation step, so the mapping is defined once rather
than copy-pasted per job.

Federation only works for **same-repo** runs: a fork's run cannot mint a token whose subject
names this repository, so fork PRs skip the Azure-touching jobs rather than fail.

Two managed identities exist, each registered with exactly the subjects its events present:

| Event | OIDC subject | Identity | Consumer |
| --- | --- | --- | --- |
| push to `main` | `…:ref:refs/heads/main` | prod | `bench-history.yml` |
| schedule on `main` | `…:ref:refs/heads/main` | prod | `bench-history-backfill.yml` |
| pull request | `…:pull_request` | prod | `pr-bench-history.yml` |
| push to `main` | `…:ref:refs/heads/main` | test | `test-azure` backend tests |
| pull request | `…:pull_request` | test | `test-azure` backend tests |

`merge_group` is not a trusted subject. Queue runs skip `test-azure` and `test-azure-gh`
rather than attempting an exchange that cannot succeed.

The **prod** identity backs history collection and the PR benchmark workflow; the **test**
identity backs the Azure-backend test jobs against a throwaway account. Both trust `main` and
`pull_request` so each identity's on-main and on-PR consumers can sign in. Granting the prod
identity a `pull_request` credential is a deliberate tradeoff: it widens prod's write surface
from "only pushes to `main`" to "any same-repo PR run", accepting a larger blast radius in
exchange for letting a PR's benchmarks be compared against the very store that holds `main`'s
baseline. The narrower alternative — a separate PR store — was rejected because branch-mode
analysis must read the base's accumulated history and the tool reads a single backend.

## Benchmark history

History collection runs on every push to `main` rather than on a schedule, so it captures
exactly one data point per commit instead of re-measuring an unchanged tip and missing
intermediate commits. It writes to a dedicated production storage account under a dedicated
production managed identity, kept entirely separate from the throwaway account the test jobs
use, so the long-lived data store never depends on test infrastructure. Collection is
append-only and idempotent, which is what makes a re-run safe and lets a read-through cache
of the bulk history persist between runs.

Collection stamps every engine's results with the runner's **own auto-detected hardware
fingerprint**, with no fixed key override. The GitHub-hosted pool is heterogeneous, so a
single shared key would blend genuinely different machines into one jittery series;
fingerprinting instead splits the pool into one clean series per hardware type. Because
collection is a matrix and analysis is a single job that cannot re-derive those keys from its
own hardware, each collect leg writes its fingerprint out as an artifact and the analysis job
threads exactly the keys collected this run into its selection — scoping the analysis to the
machines that actually measured this commit, without assuming anything about other data in
the shared store. The cost of that split is sparseness: consecutive commits land on whatever
hardware the pool handed out, so each per-key series sees only a fraction of `main`'s commits.
The nightly backfill below exists to densify them.

To blunt the residual within-machine jitter, collection runs the whole suite several
times per commit and keeps, per metric, the minimum sample: runner interference is one-sided
(a contended host only ever makes a benchmark slower) and the repeats are spaced apart in
time, so the minimum is the reading least perturbed by transient noise. This trades a
proportionally longer collection job for a more stable series.

Both this workflow and its PR variant benchmark only the x86_64 Linux and Windows runners.
ARM and macOS are only nominally supported — they must pass tests (see the Platform strategy
section) but their performance is not tracked — so benchmarking them would spend runner
minutes producing series no one reads.

Every selected package is benchmarked with all Cargo features enabled. This makes Cargo include
benchmark targets guarded by `required-features` and builds each selected package in its
all-features configuration. The push, PR, re-collection and nightly-backfill paths all obtain this
feature selection from the same command builder, so a stored point is never made incomparable by
one path using narrower feature coverage.

Even with those defences, a single day of a badly degraded runner can still leave one commit's
data point corrupted. Collection is therefore also manually re-runnable against a specific
historical commit: a `workflow_dispatch` with a `recollect_commit_id` re-measures just that
commit and *overwrites* its stored point instead of appending the pushed tip. The subtlety this
resolves is that the collection tool lives in the same repository as the benchmarks, so a naive
"check out that commit and re-run" would also run the tool as it shipped at that commit. Instead
the re-collection benchmarks the code *at* the target commit in a throwaway worktree while
running the current tool, so the measured code and the compiler that builds it come from that
commit while the collection logic does not. The overwrite bumps the cache-invalidation marker
so downstream analysis refreshes,
and it deliberately discards any blessings recorded at that commit, since a fresh measurement
invalidates a level that was previously accepted. Analysis is unaffected by the input and always
surveys the current `main` tip.

The stored history can also change *out of band* — a blessing or unblessing, a `prune`, or an
administrative overwrite performed from a developer machine. Those surface in the rolling issue on
the next push, which re-lists the store (so out-of-band additions are seen) while deletions and
overwrites bump the cache-invalidation marker (so those are seen too). There is deliberately no
"analysis only" dispatch mode: analysis threads the *exact machine keys collected this run* from the
collect matrix into the single analyze job (see below), so a mode that skipped collection would have
no keys to analyze. To force a refresh out of band, push a commit or dispatch a `recollect_commit_id`
run (which still collects, hence still produces keys).
A downstream analysis job reads the accumulated
history and files a single rolling, advisory issue when it detects a notable regression;
regressions never fail the run. Because a GitHub issue body is size-capped and a large
analysis can exceed it, the issue carries a **condensed summary** (the top findings) and
links to the **full Markdown and JSON reports**, which the job uploads as a run artifact — so
the issue always fits while the complete data stays one click away.

### Nightly history backfill

A scheduled companion workflow densifies the per-machine-key series the push workflow leaves
sparse. At 02:00 UTC — clear of the cache warmup's midnight slot — it runs the collection tool's
`backfill` in its default skip-existing mode over a window of recent `main` commits, on the same
two platforms, so whichever machine key its runner draws that night receives the newest commits
that key is missing. It is purely a producer: it performs no analysis and raises no alert.
Analysis stays with the push workflow, which surveys a densified series the next time one of its
runners draws that same machine key.

The window is computed per run rather than fixed. Its newest end is the newest first-parent
commit at least 24 hours old, which keeps the nightly from racing a push-collect that may still
be measuring a recent commit (collection runs for hours). Its oldest end is 14 days
back: history older than that has no comparison value against the current tip, since detection
reads only a short window of recent points, and the same bound caps how far back the
measurement-configuration caveat below can plant an odd-looking point.

Being killed by the clock is the expected outcome, not a failure. One commit costs as much as a
push-collect and more — the backfill worktree's build directory sits outside the shared
dependency cache, so it always builds cold — so a night fills roughly one gap per platform
against the six-hour hosted-runner ceiling. The job therefore carries the maximum
`timeout-minutes` *together with* `continue-on-error`, which is what turns the kill into an
unremarkable end rather than a red scheduled workflow, and it ignores per-commit errors so one
unbuildable commit does not end the walk. Because backfill works newest-first, whatever the run
managed to finish is the most comparison-relevant part of the range. A kill landing in the
seconds a commit spends writing its per-engine results leaves that commit stored for only some
engines, and later runs count it as filled; repairing it takes a `backfill --overwrite` over
that commit. The accepted cost is that a
genuinely broken nightly — bad credentials, a tool bug, every commit failing — is equally green
and silent; this is an opportunistic job, and such breakage still surfaces within hours in the
push workflow, which does alert.

It carries its own concurrency group instead of joining history collection's. A scheduled run's
SHA *is* the current tip, so sharing that SHA-keyed, cancel-in-progress group would put the
nightly and the tip commit's own collection into one group where whichever started later kills
the other — hours of benchmarking discarded in either direction. The backfill's own group merely
stops a manual dispatch from duplicating a scheduled run, queueing it instead. The dispatch
exists as an escape hatch: it can override the computed newest endpoint to step over a commit
that fails slowly and would otherwise be re-selected every night.

Two fidelity caveats ride along, both worth recognising before an unexplained step in a series
is read as a real regression. A backfilled commit is built with the toolchain that commit pins,
but its `RUSTFLAGS` and benchmark scope come from the current checkout — they are caller intent
that a general-purpose tool cannot recover from a historical worktree — so a commit older than
the newest change to either is measured slightly differently from its pushed neighbours; the
14-day window is what bounds this. Separately, the hosted runner images roll weekly and the
hardware fingerprint does not capture the image version, so a gap filled tonight may be measured
on a newer image than the neighbour it sits between — an exposure the pushed series already
carries, and strictly better than leaving the gap empty.

### PR benchmark history

The same measure-and-report loop runs on pull requests, retuned to answer "does this PR move
any benchmark relative to `main`?" instead of "is `main` trending?". It reuses the analysis
tool's **branch mode**: with the PR head as context and `main` as the base, the tool splits the
head's first-parent ancestry at the merge-base and compares the branch tip's level against the
base ancestry's clean baseline, so a finding means *this branch changed the level* rather than
that the long-range trend moved. To keep that topology intact the collect and analyze jobs
check out the PR head's real commit — not the synthetic `pull_request` merge ref, whose first
parent is `main` and would corrupt the comparison — with full history, since both the
merge-base and the first-parent walk need it.

Because a PR is transient, the findings land in a single **rolling PR comment** (deduped by a
hidden marker, updated in place on every push) rather than the rolling issue the `main`
workflow files; the comment lives and dies with the pull request. The comment is strictly
advisory — findings never affect the check's exit code, so a regression note never blocks a
merge — and it reports detected improvements alongside regressions, or a plain "no regressions"
state. It also states its **collection scope** — which packages were benchmarked — so a clean
result is never mistaken for the whole suite being clean when only the impacted subset was
measured. A run *failure* surfaces only as the red check, with no issue and no failure comment,
because a PR failure is a transient condition, not the persistent one the issue lifecycle
tracks.

An all-clear is claimed only when the analysis actually reached a verdict. The detector judges
a series only when it carries enough evidence to tell a change from noise, so "no findings" and
"nothing was assessed" are different states that would otherwise render identically. The comment
resolves them from the **series census** the report carries: a run that judged nothing, and a
placeholder report from a total collect failure, both say so plainly instead of showing a
checkmark. Silence is only reassuring when it is silence *about something*.

Because a full benchmark run takes hours and a new push *cancels* the in-flight one (see
Concurrency), on a PR's first push there is nothing on display yet, and on later pushes the comment
on display can lag the PR tip by a long way with no way for a reader to tell current numbers from
hours-old ones. A lightweight **`mark-stale` job** runs at the *start* of each new run (right after
the short delta preflight, in parallel with the multi-hour collect) and keeps the comment honest
about the run just begun. When the PR has **no comment yet**, it seeds a *"benchmarking in
progress"* placeholder — carrying the same hidden dedup marker and disclosing the collection scope,
so the author knows results are coming rather than seeing nothing for hours; it refreshes that
placeholder's scope on later pushes and steps aside once a completed analyze overwrites it with real
findings. When a comment **already carries results**, two further mechanisms flag their age: every
such comment records **which commit it measured** — a human-visible line printing the full SHA bare
(GitHub autolinks it to the commit and abbreviates it for display, so we neither truncate it
ourselves nor lose the click-through) plus a hidden full-SHA marker — and `mark-stale` prepends a
warning banner stating how far behind `HEAD` those numbers now are: *"N commits behind HEAD"*, or a
numberless *"out of date"* when the two share no history (e.g. a force-push) or the marker is
absent. The distance comes from the GitHub compare API (`ahead_by`), which needs no clone and still
resolves a commit orphaned by a force-push; any inability to compute it degrades to the numberless
wording rather than failing the run. The banner is bounded by a sentinel pair so a re-run *replaces*
rather than stacks it, and the next completed analyze — which rewrites the body from scratch with a
fresh analyzed-commit marker — drops it automatically once real new results land. The staleness pass
skips a still-empty placeholder (it has no results to age), leaving the placeholder's own upkeep to
the seeding pass. `mark-stale` is gated on the same non-empty delta as collect, so it never races the
cleanup path that *deletes* the comment when the PR no longer touches anything benchmarkable.

That start-of-run banner only helps a *later* run flag an *earlier* run's results; a run must also not
publish results that are *already* stale by the time it finishes. Two mechanisms cover the finish side.
First, the `analyze` job is gated on `!cancelled()`, **not** `always()`: a partially-failed collect
(one platform red) is not a cancellation, so analysis still runs and reports whatever landed, but a run
*superseded* by a newer push — which concurrency `cancel-in-progress` cancels — does **not** analyze or
post, because `always()` would run even when cancelled and publish results for a head SHA the PR has
already moved past. Second, as defense-in-depth for the narrow window where a push races a run's *final
post* faster than the cancellation can stop it, the `analyze` job re-reads the **live PR head** just
before posting and, when it no longer matches the analyzed (frozen) head SHA, injects that same
staleness banner into the composed body first — so a superseded result is never published looking fresh.
Both this self-check and `mark-stale` word the banner through one shared helper so they cannot drift,
and both degrade to posting/leaving the body untouched rather than failing the advisory comment if the
live head or compare distance cannot be read.

Collection is **delta-scoped**: a preflight job runs cargo-delta against `main` and benchmarks only
the impacted packages — those the PR changed, plus their dependents — since re-measuring the whole
workspace on every PR push would be wasteful. Analysis, by contrast, is deliberately **not**
package-scoped — yet it stays correctly scoped anyway, by construction rather than by a name
filter. `analyze` considers only
benchmarks **present at the context commit** (the PR head), dropping any "ghost" benchmark with
no run there *before* detection. Because only the impacted packages are collected at the PR's
branch-unique head commit, only they are present there, so every other package is excluded
as a ghost automatically — for *every* measurement engine — leaving exactly the collected set to
analyze. Package scoping thus falls out of *what gets collected*, with no need to filter analysis
by name, which would in fact be wrong: benchmark identities are engine-dependent (some engines
identify a series by bare operation name with no package prefix), so a name filter would silently
drop those series and turn a real regression into a false negative. The PR analysis therefore
relies on that ghost exclusion, which is unconditional and cannot be turned off. As a side
benefit the same filter
also drops a benchmark the PR itself *removed*, so a deletion is never mis-reported as a
regression.

When a PR impacts no benchmarkable package — including a PR that impacted one earlier and then
reverted it — a lightweight cleanup path removes any rolling comment a prior push left behind
and posts nothing, so a stale, misleading comment never lingers; it is a no-op when there was
no comment. PR runs read the shared history cache **restore-only** (never saving), keeping the
baseline warm without accumulating per-PR cache entries, which the append-only store makes
safe even when slightly stale.

## Failure alerting

The push-triggered history collection, release, and validation workflows all open a GitHub
issue on failure, but with
deliberately different lifecycles matched to what failed. A benchmark-history failure is
a recurring condition on a rolling target, so it opens a *deduplicated* tracking issue
(keyed on a fixed title) that a companion job closes automatically once the workflow is
green again — exactly one open issue per persistent failure, cleared without manual
intervention. The nightly backfill is deliberately outside this scheme and files nothing (see
Nightly history backfill). A release failure is a discrete event tied to one publish attempt, so it
opens a *per-run* issue (identified by the failing run) that stays open until a human
investigates; each failed release is tracked individually rather than folded into a
rolling issue. A push-to-`main` Validation failure follows the same per-run shape as the
release alert — a fresh `ci-failure` issue per failing run, no dedup and no auto-close —
because it now backstops the checks pruned from PR validation, so each such failure warrants
individual triage. It fires *only* on push to `main`: a PR failure is already self-evident as
the red check and needs no issue, so the alert is gated on the `main` ref (which a
`pull_request` run never presents) and on `failure()`, leaving a green or skipped-only run to
file nothing.

## Release automation

Publishing changed crates to crates.io and attaching cargo-binstall prebuilt binaries is
fully automated after the single manual version-bump step. Its full design — single-workflow
structure, crates.io Trusted Publishing, dynamic derivation of which crates receive GitHub
releases, and the self-healing reconciliation that rebuilds only missing binary assets —
lives in [`docs/release-automation.md`](../../docs/release-automation.md).

## Cache warmup

A scheduled workflow recompiles the shared dependency cache on every runner image daily so
it is never evicted for inactivity. Without it, a cold cache would force every parallel
validation job to compile all dependencies from scratch.

## Shared infrastructure

All non-trivial jobs use the `setup-environment` composite action to install a single,
consistent toolchain (`just`, PowerShell, the Rust toolchain, and release tooling);
deviating from it to hand-pick a minimal per-job toolchain costs more in maintenance than
the mostly-cached setup time it would save. Toolchain versions are defined once in
`constants.env` and `rust-toolchain.toml` and reach the workflows through the `just`
commands they call, so no version is ever duplicated into a workflow file.

The one deliberate deviation from "one identical environment everywhere" is Valgrind. It is
installed only where a job actually executes Callgrind measurements — the benchmark
collection jobs, the test jobs that smoke-run every bench target, and the cache warmup that
primes their caches — because Valgrind pulls in glibc debug symbols that are pinned to the
exact glibc build on the runner image. Installing it in every job would put every Ubuntu job
in the repository at the mercy of routine Ubuntu security updates, so the opt-in confines
that exposure to the jobs that cannot work without it. For the same reason the APT package
cache is scoped to the runner image version, so it rolls forward with the image instead of
serving debug symbols that no longer match — and because that scoping makes every image roll
resolve packages afresh, the APT index is refreshed on every Linux job rather than trusted as
the image left it.

## Transient-fault handling

CI touches unreliable infrastructure — package mirrors, the GitHub API, runner disks — where a
single blip (an HTTP 5xx, a rate-limit refusal, a dropped connection, a runner disk I/O error)
is not a real failure and must not fail a whole job. Such faults are retried automatically, and
at the lowest feasible level: one flaky download or one API read is re-attempted in place rather
than restarting the job around it. A shared helper, `scripts/utility/Retry.psm1`, is the single
place that logic lives: `Invoke-WithRetry` re-runs an action a few times with exponential backoff
capped at a ceiling, and `Test-TransientFailure` classifies an error message so callers can retry
only genuinely transient faults.

Retry wraps the operations most exposed to that risk: the in-repo Rust toolchain install (the
`rustup toolchain install` a runner disk blip once failed with no retry, dropping a whole job on
one bad sector), the actionlint/shellcheck/azcopy tool downloads (which re-fetch *and* re-verify
the checksum, so a truncated payload re-downloads rather than being trusted), and the idempotent
`gh` read calls behind the bench-history modules' `Invoke-GhCapture` seam.

Two rules bound where and how retry is applied. First, it never wraps a non-idempotent mutation —
creating, editing, closing, or deleting an issue or comment — because a retry after a fault that
already took effect would duplicate it; only reads opt in, via `Invoke-GhCapture -RetryOnFailure`.
Second, how a retryable failure is recognised depends on whether the failure is legible. A `gh` read
runs against a live API where a deterministic error (a 4xx, an auth refusal, a malformed request) is
unambiguous and should surface at once, so those reads pass `Test-TransientFailure` as the retry
predicate and re-attempt only transient-looking failures. The idempotent installs and downloads are
different: their motivating faults — a runner disk I/O error, a truncated fetch — are not reliably
classifiable from the error text, and the operations are cheap to repeat, so they retry *every*
failure within a small, bounded budget, and a genuinely deterministic failure (a bad version pin, a
checksum that never matches) costs only the capped backoff window before it surfaces. Reads that
already degrade gracefully — returning a neutral result on failure rather than aborting the job — are
left un-retried, since a soft-failing read needs no retry to keep the job alive.

First-party marketplace actions (checkout, cache, artifact up/download, and the like) are trusted
to retry their own network operations internally, so they are not wrapped in a third-party retry
action; adding one would trade the minimal-dependency stance for redundant coverage. That trust is
revisited only if a specific action is observed to flake.

## Job timeouts

Every job that runs `setup-environment` must budget for a *cold* cache. When the shared
dependency and toolchain caches miss — the warmup workflow lapses, a runner image rolls, or a
version pin changes — that step recompiles everything from scratch and can take up to ~90
minutes (which is why the warmup job, whose only work *is* that setup, is itself capped
generously). A job-level `timeout-minutes` bounds the whole job, setup included, so any
explicit cap is sized as the job's own work budget *plus* that ~90-minute cold-setup
allowance; sizing a cap to the warm-cache setup time alone would make a cache miss spuriously
fail the job. Jobs whose work is comfortably bounded carry no explicit cap and rely on
GitHub's default ceiling, which already clears a cold setup with room to spare. Explicit caps
exist only to stop a genuinely stuck run, never to bound the expected duration. The nightly
history backfill is the deliberate exception: its work is unbounded by nature (it keeps filling
gaps until it runs out of range), so it takes the ceiling as its run budget and pairs the cap
with `continue-on-error` so being cut off is an ordinary end rather than a failure.
