# GitHub workflows implementation

This guide maps the workflow design to the repository tools that implement it. User-visible
CI behavior and design tenets are in [design.md](design.md); command flags and step details stay
with the commands and workflow jobs.

## Workflow events and orchestration

Workflow events create runs; reviewed policy and planning determine whether those
runs execute checks or write issues. A disabled execution switch does not remove a
GitHub timer, and an intentional no-work plan is not new coverage. GitHub cron times
below are UTC. Local App timers are separate and use their verified installation timezones.

| Workflow | What starts it | Responsibility and downstream calls |
|---|---|---|
| `scheduled-validation.yml` / **Scheduled validation** | Daily `schedule` at 02:41 (`41 2 * * *`), or authorized `workflow_dispatch` on `main`. | Plans immutable full-main scope and checks reusable coverage. Calls `deep-checks.yml` only when execution is authorized and needed. `force` bypasses coverage reuse, not execution authorization; `canary` permits an explicit read-only execution check. |
| `deep-checks.yml` / **Deep checks** | **Only `workflow_call`**, from `scheduled-validation.yml`, `scheduled-verify.yml`, or `validation.yml`. No timer, push trigger or manual entry point. | Reusable matrix execution of the caller's declared scope. Its jobs and artifacts belong to the calling run; it does not file issues or start a separate reporting chain. |
| `scheduled-report.yml` / **Scheduled reporting** | `workflow_run: completed` for **Scheduled validation** and **Scheduled verification** on `main`, regardless of conclusion. No timer or `workflow_dispatch`. | Validates the originating attempt, collects evidence and files/updates run-level failure intake; also maintains coverage and applicable authoritative confirmation. Does not perform AI triage or directly create diagnosed problem issues. Uses trusted default-branch code; writes require reporting authorization. Recover by rerunning the existing reporter run, not by rerunning deep checks. |
| `scheduled-verify.yml` / **Scheduled verification** | Each `push` to `main`, or authorized `workflow_dispatch` with an explicit immutable source and scope. | The cheap planner selects pending registered merged repairs, or explicit diagnostics. Calls `deep-checks.yml` only for selected authorized work; no pending repair is a normal no-work outcome. Completion triggers `scheduled-report.yml`. |
| `scheduled-health.yml` / **Scheduled health** | `schedule` at minute 11 every third hour (`11 */3 * * *`), or `workflow_dispatch` on `main`. | Read-only observation of scheduler, reporter, coverage and separate Local triage/repair health. Writes a summary/artifact and signals failure; does not call deep checks, file a finding for every red run, or trigger the reporter. |
| `validation.yml` / **Validation** | `push` to `main`; PRs targeting `main` on `opened`, `synchronize`, `reopened`, `edited`, or `ready_for_review`; merge-queue `merge_group` events for `main`. | Ordinary merge validation and the managed-repair gate. Calls `deep-checks.yml` for relevant registered repair scope, not the entire scheduled suite. Results feed `required-checks`; this workflow does not trigger scheduled issue reporting. |
| `pr-bench-history.yml` / **PR Benchmark history** | PRs targeting `main` on `opened`, `synchronize`, or `reopened`. | Independent advisory benchmark workflow; excludes managed repairs from production-backed collection. It neither calls deep checks nor triggers scheduled issue reporting. |

```text
GitHub daily timer / manual dispatch
  -> Scheduled validation -> plan -> Deep checks (workflow_call, if needed)
           |
           +-- completion, any conclusion --> Scheduled reporting
                                                 |
                                                 +-> run-level failure intake
                                                 +-> coverage / applicable confirmation

main push / manual diagnostic dispatch
  -> Scheduled verification -> plan -> Deep checks (workflow_call, if selected)
           |
           +-- completion, any conclusion --> Scheduled reporting

main push / PR event / merge-queue event
  -> Validation -> ordinary checks + managed repair scope -> required-checks
                                     |
                                     +-> Deep checks (workflow_call, if selected)

GitHub three-hour timer / manual dispatch -> Scheduled health (observation only)
Local App triage timer -> AI run analysis -> deduplicated actionable problem issues
Local App repair timer -> problem intake -> owned repair session -> PR
```

Hosted intake does not dispatch Local AI. The retained repair coordinator can
inspect registered work but cannot reserve new repairs; it reports the unavailable
triage capability even if policy is otherwise permissive. A downstream triage
consumer has the run-record contract, not a running implementation supplied by
these workflows. The saved repair entry retains cron `17 */3 * * *` and stays
disabled. Its verified App timezone is independent of GitHub's UTC health timer.
A repair session's PR activity triggers normal PR workflows. A human merge
produces the `main` push that starts targeted confirmation. Reporter completions,
health runs and reusable child jobs do not recursively trigger scheduled reporting.

## Validation structure

`validation.yml` assigns each independently useful check to a separate job so GitHub reports the
outcomes in parallel. Cargo-package jobs consume the affected-package set from the `delta`
job. Repository-wide checks run without that gate.

Pull requests and merge-queue entries use the pruned validation set. Pushes to `main` use the
full set. Queue delta analysis takes the event's base commit so its comparison cannot drift
from the queued merge candidate.

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
union. The pre-apply publication gate resolves every approved target against current workspace
metadata and queries crates.io only for targets Cargo says are publishable. Package-name patterns
do not determine whether a crate has a consumer contract or is publishable.

The module also owns the skill's deterministic mechanics: dependency-order presentation,
publication eligibility, change-level validation, version-group realignment, and
conversion to `cargo-release-plan apply` input. The just recipes remain thin command-line entry
points. Pester tests in `scripts/release/ReleasePlan.Tests.ps1` lock these boundaries.

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

## Merge-blocking result

The `required-checks` job is the intended single ruleset target. Its `needs` graph contains
every merge-blocking Validation job. `scripts/build/RequiredChecks.psm1` rejects failed,
cancelled, missing, and unknown dependency results. It permits `skipped` only for jobs whose
event, platform, or package scope legitimately excludes them.

The classifier only observes what `needs` supplies, so it also rejects an unconditional gate
that its must-succeed list names but the payload omits. A name that drifts out of the `needs:`
list therefore fails the fan-in instead of silently disappearing from it.

Azure OIDC test jobs are among the legitimate queue skips because their federated identity
trusts pull-request and `main` subjects, not merge-group subjects.

## Scheduled controller ownership

`scripts/scheduled/ScheduledContracts.psm1` owns versioned record validation and stable
identities, not AI root-cause decisions. Reporter-owned run records in run-level
issue bodies contain execution evidence; separate hosted records retain coverage
and authoritative confirmation. The downstream contract assigns triage records to dedicated comments on run-level
issues contain analysis status and problem-issue links. Triage-owned problem records
in problem-issue bodies contain diagnosis, actionable scope and supporting evidence.
Repair-session and PR records
describe repair ownership. Replacing one record preserves surrounding prose and other
owners' records.

New repair admission is rejected rather than treating a reporter-authored finding
or a supplied triage marker as authorization. An eventual admission implementation
must validate the triager's problem record and linked hosted source evidence;
labels and matching logins alone are insufficient. Existing repair records remain
available for reconciliation and authoritative confirmation.

### Immutable execution

The check plan declares every expected platform, package, shard and seed range before execution.
Its digest includes check construction, execution tooling and pins. Results bind the actual scope,
source, controller, run and attempt to raw replay evidence. The reusable deep workflow checks out
the controller and candidate separately, runs with read permissions and preserves artifacts even
on checker failure. Reporting runs only reviewed default-branch code with issue-write authority;
candidate artifacts are data, never commands or an alternate policy.
Source bytes, including Cargo versions, are bound by the source SHA rather than the checker
digest. Admission policy does not change checker compatibility; changing an allowlist or applying
a release increment must not prevent an otherwise compatible repair from confirming its finding.

Each execution leg runs independently with fail-fast disabled so a failed shard cannot cancel
evidence from the rest of the manifest. The shared timeout accommodates mutation work and
cold-cache setup. Always-upload steps include hidden artifact directories: failures still need
their plan, result and raw evidence, not just a job conclusion.

### Complete evidence and reuse

`ScheduledPlan.psm1` establishes full-scope completeness and compatible-success reuse.
Before reuse, the planner also checks the execution API for unreported failures, active work and
newer attempts on the same main commit. The asynchronous reporter's durable index cannot conceal
those observations, and skipped work does not refresh the coverage timestamp.

### Evidence decoding

`ScheduledExecution.psm1` constructs typed arguments and interprets pinned checker output.
Raw mutation outcomes come from `mutants.out`, not from a full-run `--json` option.
An unmutated baseline is mandatory and a replay that matches no mutation is an error.
Timeouts remain findings.
Ordinary empty mutation shards carry successful exact-scope discovery and explicit unmutated
baseline evidence because the mutation tool does not run its baseline for an empty selection.
The parser validates both before the leg can pass; zero-match exact replays remain failures.
The archived mutation configuration must match the controller. The nonpublished
`scheduled-mutation-config` Rust utility decodes its baseline options. Checker compatibility binds
the utility's own manifest, Rust source and reviewed `dependency-contract.json`, alongside the
trusted mutation configuration, execution wrapper and toolchain pin. The dependency contract
captures the utility's effective direct requirements/features and reachable registry dependency
identities, features and edges, rather than unrelated workspace release versions.
The graph follows normal and build dependency edges, including an edge that also has a
development role. Dev-only requirements and edges do not contribute to the controller's
`--bin` build and are excluded. Resolved features of every reachable compiled package
remain bound, so test support cannot conceal a runtime dependency or feature change.

The first decoder build obtains Cargo metadata from the pinned trusted controller. The Rust helper
normalizes that metadata and requires it to match the reviewed dependency contract before caching
the executable or decoding mutation configuration. Dependency drift requires a reviewed contract
refresh; it cannot silently reuse checker compatibility. Planning hashes the checked-in contract
without executing Rust, preserving cheap planning and empty Local scans.
Cargo's workspace-unified feature resolution is conservative: feature changes reachable through
the decoder's dependencies also require contract review. Registry dependency identity is supported;
local and Git dependencies are rejected rather than assigned an incomplete content identity.
The [maintenance procedure](../../docs/scheduled-validation.md#maintaining-decoder-dependency-identity)
refreshes the contract explicitly, never as automatic acceptance during reporting.
`ScheduledExecution.psm1` builds only that package on demand from the absolute trusted controller
root, using its pinned stable toolchain and a controller-owned target directory. The resolved
executable is reused within the module process; module import and empty Local scans do not build it.
Candidate artifacts never supply the decoder, its dependencies, output directory or build instructions.
The build disables implicit rustup installation; missing tooling is an explicit prerequisite failure.

Deep execution prepares tools through the controller's environment setup. The reporter needs
only rustup/Cargo and the hosted native compiler/linker, not the deep-check tool suite. Its
PowerShell bootstrap uses the existing `RustToolchain.psm1` helpers to prepare the controller's
pinned stable channel with the minimal profile before evidence reconciliation. Bootstrap must
work without the Rust toolchain it prepares; this is the reason for the PowerShell boundary.
The wrapper otherwise provides process orchestration around the Rust utility, while planning and
health remain available without a Rust build.
Miri's parallel seed output and post-suite diagnostics do not establish which test caused a
failure. Unattributed findings retain the original target, filter and seed-range invocation instead
of selecting the last printed test or seed. Narrowed reproduction requires independently established
execution scope.

### Serialized reporting

`ScheduledReport.psm1` and `ScheduledGitHub.psm1` separate deterministic state transitions from
GitHub persistence. Hosted reporting serializes originating workflows into one issue-writing job;
candidate execution has no writer authority. The reporter checks out default-branch controller
code, validates downloaded artifacts as data and preserves durable minimal reproduction data
after the larger artifacts expire. It creates run-level intake, not diagnosed problem issues.
`scheduled-run-record` is the nonpublished Rust owner of typed run evidence,
deterministic attempt/revision reconciliation and bounded evidence-page payloads.
It performs no GitHub operations. `ScheduledRunGitHub.psm1` supplies collected
inputs and performs the ordered API writes. `ScheduledTransport.psm1` owns bounded
native response capture and UTF-8 process transport, including setup failures
before any checker result exists. Both reporting utilities are built from the
trusted controller with its pinned toolchain and separate per-platform target
directories, never from a candidate checkout or artifact.

#### Run-level failure intake

Inventory actual Actions jobs and steps as well as the declared manifest: failed checkout,
tool setup, planning or artifact upload may produce no checker result. Persist references to
all unsuccessful jobs, their failed steps and available diagnostics. Collection or parsing
failures are explicit evidence gaps in the intake, not an empty successful result.
Retain valid parsed results even when another leg cannot be decoded.
The attempt-scoped jobs API is fully paginated and checked for duplicate IDs,
foreign-run metadata and incomplete totals. Failed-job log downloads and durable
excerpts are bounded; truncation and unavailable logs remain explicit alongside
the original Actions log URL.

The run issue key is repository/workflow/run identity. Run attempts and their evidence
revisions are distinct inputs within that issue. A repeated completion notification must
not duplicate intake; a new attempt must not inherit a prior attempt's triaged checkpoint.
The issue body holds compact run identity, a publication checkpoint and a bounded
summary. Complete evidence is split into
deterministic reporter-owned comment pages, and the index records their confirmed
references only after every page exists. Retry reconciliation matches stable
run/attempt/digest/page identity before posting, including after a lost create
response. An incomplete publication is recoverable and fails reporting; it does
not claim that unwritten pages were persisted. Existing human text and unrelated
comments are preserved.
The checkpoint binds complete revision identities and their actual comment IDs.
Reconstruction must reproduce it, or differ only by the current revision whose
pages survived an interrupted index update. Other differences require reconciliation,
preventing deleted committed history from being silently replaced.

The API adapter flushes a per-run publication journal before writes and preserves
unknown page operations across retries. Hosted reruns restore it from the preceding
reporter attempt's provenance-validated artifact. If required recovery evidence is
missing or an uncertain write is still unobservable, reporting fails without
repeating that POST. This is an operator-visible recovery condition, not an empty
queue or a successful publication.

The downstream triage contract is separate from hosted publication:
The triage record in the run-level issue's automation-owned comment binds each
analysis entry to run ID, attempt number and evidence digest. Its status is
`in-progress`, `blocked` or `complete`; only `complete` acknowledges analyzed evidence. The
`scheduled-triaged` label and closed issue state summarize that every failed revision
has a matching complete triage entry. A new failed attempt or changed failed-attempt
evidence reopens the issue and removes the label while preserving prior completions.
Queue reads paginate this repository's open and closed `scheduled-run-failure`
issues and refresh locally claimed run-level issue IDs. They compare the reporter's
run record in each body with the triage record in its owned comment, rather than
trusting the label or closed state. This is separate from problem-issue discovery.
See [marking a run triaged](../../docs/scheduled-validation.md#marking-a-run-triaged)
for idempotency, concurrent evidence and green-rerun behavior.
An unsuccessful or unexpectedly incomplete execution requires intake. A legitimate disabled
or reusable-coverage skip does not. The reporter may supply parsed observations, but it does
not decide how many real problems exist or whether different symptoms share a cause.

Coverage and registered repair confirmation remain deterministic hosted responsibilities.
An analysis marked complete in a triage record cannot mint a success receipt or declare a repair verified.
Conversely, a green rerun cannot acknowledge analysis of earlier failure evidence.

#### Local AI triage and problem publication

These are downstream consumer requirements, not an executable stage in hosted
intake. No hosted parser performs semantic diagnosis or creates problem issues.
An AI triage implementation must follow the
[problem contract](../../docs/scheduled-validation.md#problems-and-triage).
It claims an unprocessed run/attempt revision, uses a capable personally funded AI model to
analyze every unsuccessful job, and compares all extracted problems with the complete
existing problem index. Programmatic helpers validate records and normalize/search evidence;
they cannot replace semantic diagnosis and causal deduplication.

The canonical identity is numeric repository ID plus problem issue number, not a
diagnostic hash. The helper paginates all open and closed `scheduled-finding` issues
and refreshes registered issue IDs. It supplies compact complete-index summaries
to the AI, which reads plausible candidates' full records and evidence and records
its match/separation reasoning. Publication rechecks the index and pending write
intent. SHA-256 hashes bind exact evidence revisions only; different hashes can
describe one problem. The [discovery contract](../../docs/scheduled-validation.md#finding-existing-problems)
defines index content, incomplete-read handling and canonical evidence inputs.

Problem identity is independent of run/job identity, replay scope and coverage verdict.
The triager distinguishes unrelated diagnostics within a shared target and can group
different tests or shards when evidence establishes a common cause. It records that
reasoning, all contributing evidence and any uncertainty. Failed prerequisites produce
execution problems with blocked downstream scope, not fictitious per-package code defects.

One active triage session per repository serializes matching and publication. Durable
run claims, issue-write intents and per-attempt checkpoints make retries recoverable.
Before creating a problem issue, check existing identities and aliases, including resolved
records. Reconcile a lost response before retrying. Finish creating new problem issues
and updating matched issues with this analysis's evidence, diagnosis and repair disposition
before marking its triage entry complete. Unfinished analysis or uncertain publication
keeps the run-level issue open without `scheduled-triaged`; the retained triage session
resumes from its checkpoint. If analysis cannot continue, its triage record and health
output identify the blocker. A fully analyzed problem awaiting a human repair decision
remains open with that restriction on its own issue, but does not prevent closing the
run-level issue once its analysis is complete.

The repair automation consumes only validated actionable problem records. A consolidation
or materially changed diagnosis must reconcile existing worker/PR ownership before
changing repair scope. Infrastructure problems default to bounded retry/operator recovery;
triage completeness does not make every problem eligible for a source patch. Triage and
repair have separate capacity, budgets, pause controls and health checkpoints.
Until that evidence-bound handoff is supported, the Local inbox and reservation
transaction reject new repairs while retaining existing attempts and continuation
accounting.

The problem record binds a versioned set of required check/package/platform/replay
scopes to its diagnosis and hosted evidence. Admission and managed verification
consume that full set, not a single representative symptom. A materially changed
scope invalidates stale readiness evidence and requires reconciliation with the
retained worker. Problem closure requires applicable confirmation for the complete
registered resolution criteria; one green constituent result cannot erase the rest.

Evidence ordering uses the originating attempt's API start time, with creation time and run ID
breaking ties. An older run can have a genuinely newer rerun; neither its original run number nor
its completion/report delivery time establishes that attempt's order. Source ancestry and the
problem's occurrence number remain separate applicability checks.
Valid failure evidence survives incomplete legs and failed workflows. Successfully publishing
failure intake is a successful reporting operation, not a reporting outage. Unexplained
workflow failure cannot certify passing evidence or resolve a problem.
No-work verification retains the unselected catalog without inventing a global package selection.
Invalid confirmation metadata is isolated to its problem and cannot discard unrelated evidence.

### Managed repair gate

The Validation context joins registered repair metadata under the default-branch operating policy.
Queue membership uses the GitHub merge queue entries' synthetic head commits and ancestry
bounded by the event base/head. It requires the actual event candidate to exist in the API
snapshot; an expired or ambiguous queue snapshot fails rather than guessing membership.
The selected checks execute against the actual combined candidate. Their unconditional
`scheduled-repair-gate` rejects incomplete results and feeds the existing single required fan-in.
Metadata-only corrections retrigger through the PR `edited` event or a rerun at the same head.
The worker and PR records carry the same bounded causal explanation and bind the current enrolled
executor. Main confirmation uses the merged commit rather than the original pre-squash head.
Both full-main reporting and dedicated verification can confirm the live merged registration;
arrival order does not strand a problem in `needs-human`.
An unexplained pass does not disable the gate for an existing registered repair PR. Local intake
still refuses fresh admission of that disposition; the current worker can finish its causal repair
and present it for human acceptance.

### Canonical version validation

Managed release edits also pass `ScheduledVersion.psm1` on the published PR head. The worker
records an immutable pre-versioning commit, the release baseline, semantic decisions and expanded
plan. Every Cargo manifest and lockfile in that checkpoint must match the trusted release
baseline byte for byte; worker-selected pending versions cannot define the reference.
A trusted controller build of `cargo-release-plan` independently regenerates the proposal
and expansion in a new owned reference worktree, then performs real apply without publication.
Every committed Cargo manifest and lockfile is compared with that canonical result, including
dependent requirement rewrites. Source changes after the pre-versioning checkpoint or a moved
release baseline invalidate the evidence. Path/version-string restrictions are only an early
scope check; they are not proof of canonical apply. The PR description combines expanded targets
with already-sufficient pending releases from the report so human review sees the whole release.

## Operating policy

`scripts/scheduled/policy.json` authorizes hosted execution/reporting and Local
repair admission. It does not define the contents of `validate-local` or
`validate-deep-local`, or route ordinary deep jobs into PR/push validation.
The [shallow/deep split](design.md#shallow-and-deep-validation) is fixed.
The ruleset never acquires matrix names or a second required integration.

The serialized names are compatibility details. Their mapping to operating behavior is:

| Policy field or emitted value | Meaning |
|---|---|
| `rollout.hosted_execution_enabled` | Authorizes recurring full execution and, with native App readiness, automatic merged-repair confirmation. |
| `rollout.reporting_enabled` | Authorizes the reporter's issue writes independently of execution and local admission. |
| `rollout.prerequisites.execution_canary`, `reporting_canary`, `native_app_canary` | Recorded operator verification of execution, reporting and actual native App capabilities. |
| `rollout.prerequisites.benchmark_exclusion`, `azure_policy` | Recorded authorization and installation of managed-publication credential safeguards. |
| `rollout.phase` | Descriptive compatibility metadata, not authorization to execute, report or admit work. |
| `local.mode` | Repair admission selection: `observe` and `paused` do not dispatch repairs; `repair` also requires matching persisted executor mode, enrollment, approved scope, profile and budgets. It does not authorize triage. |
| Planning reason or health status `staged` | Deliberately disabled hosted operation, not proof of coverage. |

Triage has a separately reviewed authorization, installed profile and budget; the repair
mode cannot implicitly enable it. Both roles require matching persisted enrollment and
profile, and distinguish disabled/observe/paused behavior from active admission.
The new-admission capability boundary is not a policy switch: enabling any of those
settings cannot bypass the unavailable AI-triage handoff.

Safe installation defaults disable hosted execution/reporting and both Local roles,
and leave repair allowlists unconfigured. Disabled hosted execution means automatic
recurring deep coverage is off; it is not proof that PR validation covers it.
Setup preserves those settings.
Manual read-only execution uses the existing `canary` dispatch input to exercise hosted execution
without changing recurring authorization; explicit verification similarly validates approved
diagnostic scope, not problem resolution by itself. Neither action is part of merely installing the
Local automations.

The default-branch controller, workflows, policy and their helpers form one installed contract.
When neither policy nor controller is installed there, Validation runs its shallow checks
and rejects recognizable managed publication. An installed policy without its controller, or an
installed controller with missing/malformed inputs, fails validation rather than treating damage
as an unconfigured installation.

Approve package/check scope and publication safeguards before any managed PR, including a test
PR. Native profile registration is not authorization to publish. If scheduled detection
or reporting is unreliable, retain the health failure and pause affected repair admissions.
The operator can request explicit deep validation while restoring scheduled operation;
no local recipe or PR workflow silently changes its scope.

## Independent health

The hosted health workflow and personal automations independently observe scheduler, reporting,
coverage and separate triage/repair availability. They distinguish fresh evidence, reused coverage, a failed
scan, deliberately disabled/paused operation and unavailable systems. No component claims to
monitor its own total outage; the operator runbook is in the
[scheduled validation chapter](../../docs/scheduled-validation.md#health-recovery-and-rollback).
The reporter retains the actual validated planning timestamp rather than substituting run
completion time. Hosted health uses read-only permissions and persists its component report as
an artifact and step summary before signaling failure. The shared coverage/health issue contains
separate hosted coverage and role-specific personal executor health records. Triage health
tracks unprocessed run evidence and completed analysis; repair health tracks actionable
problems and existing PRs. Progress in one does not renew the other's heartbeat.
Health inventories unsuccessful reporting runs through the Actions API even after newer reports
succeed. Queue overflow, cancellation and reporting failure require rerunning the existing reporter,
not repeating expensive source checks; the operator recovery procedure is in the runbook.
The reporter processes its originating event and does not silently claim to backfill missing runs.
