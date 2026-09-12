# GitHub workflows implementation

This guide maps the workflow design to the repository tools that implement it. User-visible
CI behavior and design tenets are in [design.md](design.md); command flags and step details stay
with the commands and workflow jobs.

## Workflow events and orchestration

GitHub cron times are UTC. Local App schedules are configured separately using
the installed App's scheduling controls.

| Workflow | What starts it | Responsibility |
|---|---|---|
| `deep-validation.yml` / **Deep validation** | Daily at 02:41 UTC, or no-input manual dispatch on `main`. | Plan and execute the full main-branch suite, preserve diagnostics and report failures within one run. |
| `standard-validation.yml` / **Standard validation** | Push to `main`, PR opened/synchronized/reopened/ready for review, and merge-queue events. | Shallow checks feeding the single required `required-checks` result. |
| `pr-bench-history.yml` / **PR Benchmark history** | PR opened/synchronized/reopened. | Advisory production-backed benchmark feedback for same-repository PRs. |

```text
Deep validation on main: plan -> check matrix -> report failures -> run report issue

Local App triage -> run report -> existing or new problem issues
Local App repair -> claimed problem issue -> PR -> human review and merge

PR / main push / merge queue -> Standard validation -> required-checks
```

Hosted reporting does not invoke AI or wait for triage. App automations discover
work through ordinary GitHub issues. Closing a triaged run report is independent
of resolving its linked problems. A merged repair closes its issue through the
normal PR relationship, not a main-push confirmation workflow.

Standard validation and its close companion share the `standard-validation-`
ref-specific concurrency group. The merge-blocking job/check name and ruleset
target are exactly `required-checks`.

## Standard validation structure

`standard-validation.yml` assigns each independently useful check to a separate job so GitHub reports the
outcomes in parallel. Cargo-package jobs consume the affected-package set from the `delta`
job. Non-Cargo checks use the independent path plan, supplemented by native-helper package
impact for script integration tests. Release validation remains unconditional.

Pull requests and merge-queue entries use the pruned validation set. Pushes to `main` use the
full set. Queue delta analysis takes the event's base commit so its comparison cannot drift
from the queued merge candidate.

### Non-Cargo change planning

The `changes` job runs `scripts/build/ValidationPlan.psm1` with Git and preinstalled
PowerShell, before preparing a development environment. Rust is impractical at this boundary:
installing its toolchains and build prerequisites merely to decide whether standalone lint
should run would impose the setup cost this planner is intended to avoid.

The planner reads immutable event SHAs from a full-history checkout. Pull requests compare
their head with its merge base against the event's base SHA, covering all PR commits without
including unrelated base-branch changes. Merge groups compare their exact base and combined
head directly. Git emits NUL-delimited paths with rename detection disabled, so a move
contributes both its removed path and its added path without filename quoting ambiguity.
Unavailable revisions fail. Main pushes explicitly select the full suite.

Script directories are coarse test domains. The module declares recipe ownership and
cross-domain consumers, and defaults unfamiliar script/recipe locations to the full suite.
Setup, shared utility, planner and fan-in changes select every tooling check. Fixtures select
their owning tests; workflow changes also select the Pester workflow-contract tests. Analyzer
configuration and script files select static analysis independently of the Pester scope.

The `delta` job combines the path-selected domains with affected native helpers used by
script integration tests. Dependency impact comes from Cargo delta rather than treating
every lockfile change as a full-script-suite trip wire. Live manifest changes also select the
scheduled tests that read workspace metadata. The resulting domain array is explicit even
when empty. `test-scripts` runs that union once; `just test-scripts "book release"` is the local
equivalent, while an omitted argument retains full discovery. Unknown domains or explicit
directories containing no tests fail rather than producing a successful empty run.

Recipe files follow their automation responsibility: benchmark history and release commands
have separate imports, while setup installers live beside the setup module. Workflow
entrypoints stay in GitHub's required location. Co-location reduces selection coupling but
does not replace declared dependencies.

### Workflow lint environment

`setup-workflow-lint` restores the same portable actionlint/ShellCheck cache as
`setup-environment` and invokes the same pinned, checksum-verifying installers under
`scripts/setup`. It needs neither Rust toolchain setup nor system package installation.
The workflow invokes `actionlint -color` directly, matching `just validate-workflows` without
installing Just solely to dispatch that command. Local full setup continues to install these
same binaries, and the workflow-contract tests keep the command and cache keys aligned.

## Release validation

`cargo-release-plan` compares released content with version anchors and owns the report schema
and version-readiness verdict. Its release baseline is the tip of the branch releases are made
from, which is not the branch a pull request targets, so the workflow passes the release branch
on a pull request and the merge-group base commit on a queue run.

`scripts/release/ReleasePlan.psm1` is the PowerShell boundary between that report and hosted
validation. It accepts only the report schema revision it understands and keeps separate lookups
for publishable release assessments and all tracked version targets. The report's `packages`
array supplies released-content evidence and consumer-contract selection for publishable members;
`non_publishable_packages` supplies only names, declared versions, and derived group membership.
SemVer analysis and change-level decisions use the former, while grouping and alignment use their
union. The pre-apply publication gate resolves every planned target against current workspace
metadata and queries crates.io only for targets Cargo says are publishable. Package-name patterns
do not determine whether a crate has a consumer contract or is publishable.

The module also owns the skill's deterministic mechanics: dependency-order presentation,
publication eligibility, change-level validation, version-group realignment, and
conversion to `cargo-release-plan apply` input. The just recipes remain thin command-line entry
points. Pester tests in `scripts/release/ReleasePlan.Tests.ps1` lock these boundaries.

The guided release workflow collects its decision evidence after explicit offline preparation.
It then sends semantic choices to Rust's prospective resolution preview, which completes the
version-target set and captures the resolved files before application. The PowerShell boundary does
not duplicate Cargo resolution or infer binary closure membership. Compatibility evidence is
built with the prospective manifest path and working directory, so Cargo reads its captured
configuration and resolution rather than the live tree's. A read-only comparison rejects any
input mutation by that build. The module presents the stable
expanded artifact and applies it unchanged; the Rust boundary rejects stale original inputs
and installs only the captured state. CI's report/check path and post-apply reporting remain
read-only, with no hidden preparation or dependency refresh.

There is no separate version-approval prompt. The complete pull request and its
Version/release plan section carry the human review of release impact.

The unconditional `validate-versions` job also runs `validate-binstall` against the live
workspace. Release-target and archive-shape obligations follow Cargo's discovered binary
targets, including source additions that do not edit a manifest. The version report still runs
after a binstall failure so semantic-version analysis can consume its output in the same run.

Plan generation is verified by asserting properties of the generated plan over a matrix of report
states, not only by testing individual guards. The properties are that every entry is well formed
and names a known target, that no target receives two decision kinds, that no version moves
backwards, that every version group ends on one version, and that no package keeps an
already-published version while a requirement inside it is rewritten. That last one had been
analyzed incompletely several times in review — once per release state that reached it — so it is
checked as an outcome, where an incomplete analysis fails whatever form it takes. A scenario
passes either by refusing to generate a plan or by generating one that holds every property.

On Windows, the module scopes `CARGO_TARGET_DIR` for direct `cargo-semver-checks`
invocations to a stable, workspace-specific directory beneath the user
temporary directory. This keeps the SemVer tool's nested placeholder builds independent of
checkout depth without changing target-directory behavior for unrelated Cargo commands or
non-Windows validation. The override can be reassessed when
[cargo-semver-checks issue #1725](https://github.com/obi1kenobi/cargo-semver-checks/issues/1725)
shortens the generated paths upstream.

## Release publication

`release-plz` owns registry publication only. Its committed configuration disables Git
tag and release creation, so a main-advance race in GitHub publication cannot turn a
successful crates.io publish into a publisher retry. The publish job's ambient GitHub
token is read-only; Trusted Publishing retains its independent OIDC permission.

`scripts/release/ReleasePublication.psm1` owns Git/GitHub process orchestration after
publication. It derives package/version requests from the checked-out publication source,
reads existing remote tags, and builds `release-target-check` from that same controller
checkout only when a new tag needs a source candidate. The temporary candidate worktree
is data for the verifier, not the source of the verifier executable or automation scripts.

The nonpublished `release-target-check` utility owns candidate identity and version
constraints, and delegates released-content validation to `cargo-release-plan`. It requires
a clean checkout at the supplied immutable commit on the supplied main history, exact
requested package versions, and the release invariant against that snapshot's own anchors.
Using the candidate as the validator's baseline does not relax the invariant: the clean
worktree must still match each package's version anchor within that main history.

The PowerShell boundary uses the verified SHA in GitHub reference creation, confirms
the resulting remote reference, and retries only after observing that main moved.
It does not duplicate the Rust package-content or binary-dependency comparison.
Its temporary worktree is removed on both success and failure; cleanup failures retain
the original diagnostic rather than replacing it.

Missing binary releases use `gh release create --verify-tag` against the established
reference. Asset planning resolves each tag to a commit and carries `source_sha` in
the build matrix. Checkout consumes that immutable SHA; upload consumes the versioned
tag name. Existing references are not rewritten to match a newer preferred snapshot.
See [Release-equivalent snapshots](design.md#release-equivalent-snapshots) for the
identity contract and credential rationale.

## Merge-blocking result

The `required-checks` job is the intended single ruleset target. Its `needs` graph contains
every merge-blocking Standard validation job. `scripts/build/RequiredChecks.psm1` rejects failed,
cancelled, missing, and unknown dependency results. It permits `skipped` only for jobs whose
event, platform, or package scope legitimately excludes them.

For tooling checks it reads the explicit `changes` plan and reconstructs script selection
using `delta`'s affected-package output. The execution-domain output must agree with that
selection. Every selected tooling job must succeed; every tooling dependency must be present,
even when not selected. Both planners remain must-succeed dependencies, so a failed planner
cannot turn downstream skips into merge approval.

The classifier only observes what `needs` supplies, so it also rejects an unconditional gate
that its must-succeed list names but the payload omits. A name that drifts out of the `needs:`
list therefore fails the fan-in instead of silently disappearing from it.

Azure OIDC test jobs are among the legitimate queue skips because their federated identity
trusts pull-request and `main` subjects, not merge-group subjects. Repair branches
use the same repository/event conditions as other branches.

## Scheduled validation implementation

The scheduled scripts own planning, check invocation and readable reporting.
The App skills own diagnosis and ordinary issue/PR work. There is no shared state
machine connecting these components; their handoffs are GitHub reports and issues.

### Deep execution

The workflow's `plan` job reads the full check catalog and supplies the matrix as
a job output. Each `checks` job uses an ordinary checkout of the workflow's main
commit, installs the environment and invokes the same Just recipe used locally.
The catalog defines recipes, platforms, packages and shards; there
is no hosted selection of a different source commit or reduced scope.

Each execution leg runs independently with fail-fast disabled. Always-upload steps
preserve its readable summary and raw diagnostics even after failure. The thin
capture wrapper records the exact Just command and preserves its exit status.
Generic process capture owns stream handling and child cleanup, not checker behavior.
Successful artifact
preservation does not turn failed validation green.

The Just recipes own toolchain selection, test runners, helper preparation and
configuration. Ordinary Miri therefore uses nextest and its `default-miri` profile
in both development and CI. The many-seed recipe computes its own shard range.
`just mutants` runs cargo-mutants with its native unmutated baseline and accepts an
output directory for collecting artifacts. There is no scheduled-only Cargo
argument builder, target enumeration or mutation verdict derived from result files.
A successful empty shard is reported as no mutation work; the wrapper does not
reconstruct a test invocation or claim that a baseline ran.

The full workflow executes every night, without persistent coverage receipts or
successful-run reuse. Ordinary dependency/build caches remain available.

### Manual checks

Manually starting **Deep validation** on `main` runs the same full suite as the
schedule. The workflow has no selection inputs and does not run on PR heads.

Repair authors run relevant local checks and link results with their tested SHA in the PR discussion.
These are reviewed alongside normal required checks; Standard validation does not
run a special repair gate or recreate a worker's version edits. Unavailable local
platform coverage is disclosed for human review, not claimed as a passing result.

### Failure reporting

The `report` job depends on planning and the check matrix and runs on failure.
It uses the same main checkout as the other jobs. Its normal GitHub permissions
allow reading Actions results and writing an issue. Preinstalled PowerShell and
the GitHub CLI are sufficient, even when checker/toolchain setup failed.

The reporter reads the run's effective job results, including executions reused by
a job rerun, and collects available check summaries and failed-job log excerpts.
It does not require the overall workflow to finish before reporting. Missing artifacts or
inaccessible logs are explicit gaps in the report, not reasons to omit a failure.
Issue content contains observed failures and direct links, not serialized API
inventories. Ordinary continuation comments can retain long diagnostic lists
without an encoding or reassembly protocol.

An exact visible attempt link identifies an existing report in open or closed
issues. The current workflow attempt identifies the report, while each job's
execution identifies its diagnostic artifact. Successful validation does not close earlier reports.
Reporting errors fail the reporter job and remain visible in Actions. Recovery
can rerun that job through normal Actions controls. Rerunning all failed jobs may
also rerun checks; neither path needs a separate reporting workflow or journal.

Triage and repair use the workflows described in
[scheduled validation](../../docs/scheduled-validation.md). Their GitHub comments,
labels, assignees and PR links remain understandable without App installation or
access to an executor's local files.
