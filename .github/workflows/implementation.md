# GitHub workflows implementation

This guide maps the workflow design to the repository tools that implement it. User-visible
CI behavior and design tenets are in [design.md](design.md); command flags and step details stay
with the commands and workflow jobs.

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
validation. It accepts only the report schema revision it understands and selects SemVer targets
from the `consumer_contract` field the report carries for each package. That field comes from the
package's own manifest, so adding a package to the workspace requires no edit here and no list
goes stale; the same target selection drives both the CI SemVer job and evidence collection by
the increment-versions skill. Package-name patterns do not determine whether a crate has a
consumer contract.

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

`scripts/scheduled/ScheduledContracts.psm1` owns canonical fingerprints and separate reporter,
worker, PR, coverage and health records. Each record is a versioned JSON HTML comment; replacing
one record preserves surrounding human prose and other owners' records. Intake joins the
reporter-authored issue with its actual originating run and repository identity. Worker claims
belong to the enrolled personal account, never to the hosted reporter.

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
GitHub persistence. Reporting serializes all originating workflows into one issue-writing job;
candidate execution has no writer authority. The reporter checks out default-branch controller
code, validates downloaded artifacts as data and preserves durable minimal reproduction data
after the larger artifacts expire.

Evidence ordering uses the originating attempt's API start time, with creation time and run ID
breaking ties. An older run can have a genuinely newer rerun; neither its original run number nor
its completion/report delivery time establishes that attempt's order. Source ancestry and incident
generation remain separate applicability checks.
Valid defect observations survive incomplete legs and failed workflows. Complete evidence that
accounts for workflow failure through findings is a successful reporting operation, not a reporting
outage. Unexplained workflow failure cannot certify passing evidence or close an incident.
No-work verification retains the unselected catalog without inventing a global package selection.
Invalid confirmation metadata is isolated to its incident and cannot discard unrelated findings.

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
arrival order does not strand an incident in `needs-human`.
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

`scripts/scheduled/policy.json` is the reviewed source of truth for
[deep-validation operating modes](design.md#deep-validation-operating-modes).
Scheduled enforcement is accepted only with hosted execution and reporting enabled and all
readiness prerequisites recorded. It controls the ordinary deep jobs and routine local deep
calls together. Those jobs remain in the fan-in as legitimate conditional skips; retained
validation preserves its event and platform behavior. The ruleset never acquires matrix names
or a second required integration.

The serialized names are compatibility details. Their mapping to operating behavior is:

| Policy field or emitted value | Meaning |
|---|---|
| `rollout.hosted_execution_enabled` | Authorizes recurring full execution and, with native App readiness, automatic merged-repair confirmation. |
| `rollout.reporting_enabled` | Authorizes the reporter's issue writes independently of execution and local admission. |
| `rollout.cutover` | Selects scheduled enforcement when true, ordinary-validation fallback when false. Validation exports the same selection as `cutover`. |
| `rollout.prerequisites.execution_canary`, `reporting_canary`, `native_app_canary` | Recorded operator verification of execution, reporting and actual native App capabilities. |
| `rollout.prerequisites.benchmark_exclusion`, `azure_policy` | Recorded authorization and installation of managed-publication credential safeguards. |
| `rollout.phase` | Descriptive compatibility metadata, not authorization to execute, report or admit work. |
| `local.mode` | Local admission selection: `observe` and `paused` do not dispatch repairs; `repair` also requires matching persisted executor mode, enrollment, approved scope, profile and budgets. |
| Planning reason or health status `staged` | Deliberately disabled hosted operation, not proof of coverage. |

Safe installation defaults disable hosted execution/reporting, leave Local enrollment and repair
allowlists unconfigured, and retain ordinary-validation fallback. Setup preserves those settings.
Manual read-only execution uses the existing `canary` dispatch input to exercise hosted execution
without changing recurring authorization; explicit verification similarly validates approved
diagnostic scope, not incident closure by itself. Neither action is part of merely installing the
Local automation.

The default-branch controller, workflows, policy and their helpers form one installed contract.
When neither policy nor controller is installed there, Validation retains ordinary deep gates
and rejects recognizable managed publication. An installed policy without its controller, or an
installed controller with missing/malformed inputs, fails validation rather than treating damage
as an unconfigured installation.

Approve package/check scope and publication safeguards before any managed PR, including a test
PR. Native profile registration is not authorization to publish. When scheduled detection or
reporting is unreliable, select ordinary-validation fallback before disabling the hosted path;
never leave both enforcement paths disabled.

## Independent health

The hosted health workflow and personal intake independently observe scheduler, reporting,
coverage and local scan availability. They distinguish fresh evidence, reused coverage, a failed
scan, deliberately disabled/paused operation and unavailable systems. Neither process claims to
monitor its own total outage; the operator runbook is in the
[scheduled validation chapter](../../docs/scheduled-validation.md#health-recovery-and-rollback).
The reporter retains the actual validated planning timestamp rather than substituting run
completion time. Hosted health uses read-only permissions and persists its component report as
an artifact and step summary before signaling failure. The shared coverage/health issue contains
separate hosted coverage and personal executor health records.
Health inventories unsuccessful reporting runs through the Actions API even after newer reports
succeed. Queue overflow, cancellation and reporting failure require rerunning the existing reporter,
not repeating expensive source checks; the operator recovery procedure is in the runbook.
The reporter processes its originating event and does not silently claim to backfill missing runs.
