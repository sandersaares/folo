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

The check plan declares every expected platform, package, shard and seed range before execution.
Its digest includes check construction, execution tooling and pins. Results bind the actual scope,
source, controller, run and attempt to raw replay evidence. The reusable deep workflow checks out
the controller and candidate separately, runs with read permissions and preserves artifacts even
on checker failure. Reporting runs only reviewed default-branch code with issue-write authority;
candidate artifacts are data, never commands or an alternate policy.
Source bytes, including Cargo versions, are bound by the source SHA rather than the checker
digest. Admission policy does not change checker compatibility; changing an allowlist or applying
a release increment must not prevent an otherwise compatible repair from confirming its finding.

`ScheduledPlan.psm1` establishes full-scope completeness and compatible-success reuse.
Before reuse, the planner also checks the execution API for unreported failures, active work and
newer attempts on the same main commit. The asynchronous reporter's durable index cannot conceal
those observations, and skipped work does not refresh the coverage timestamp.
`ScheduledExecution.psm1` constructs typed arguments and interprets pinned checker output.
`ScheduledReport.psm1` and `ScheduledGitHub.psm1` separate deterministic state transitions from
GitHub persistence. Reporting serializes all originating workflows into one writer; durable minimal
reproduction data survives the larger artifacts. Raw mutation outcomes come from `mutants.out`,
not from a full-run `--json` option. An unmutated baseline is mandatory and a replay that matches
no mutation is an error. Timeouts remain findings.
Ordinary empty mutation shards carry successful exact-scope discovery and explicit unmutated
baseline evidence because the mutation tool does not run its baseline for an empty selection.
The parser validates both before the leg can pass; zero-match exact replays remain failures.
The archived mutation configuration must match the controller. Its baseline options are decoded
with Python's standard TOML parser; both the decoder and configuration participate in compatibility.
Miri's parallel seed output and post-suite diagnostics do not establish which test caused a
failure. Unattributed findings retain the original target, filter and seed-range invocation instead
of selecting the last printed test or seed. Narrowed reproduction requires independently established
execution scope.
Evidence ordering uses the originating attempt's API start time, with creation time and run ID
breaking ties. An older run can have a genuinely newer rerun; neither its original run number nor
its completion/report delivery time establishes that attempt's order. Source ancestry and incident
generation remain separate applicability checks.
Valid defect observations survive incomplete legs and failed workflows. Complete evidence that
accounts for workflow failure through findings is a successful reporting operation, not a reporting
outage. Unexplained workflow failure cannot certify passing evidence or close an incident.
No-work verification retains the unselected catalog without inventing a global package selection.
Invalid confirmation metadata is isolated to its incident and cannot discard unrelated findings.

The Validation context reads the default-branch rollout and joins registered repair metadata.
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

Before the controller's initial deployment, the bootstrap leg retains every legacy gate and
rejects recognizable managed PRs. No executor can be enrolled for publication before that
deployment and the publication safeguards. Once the controller exists, its missing or malformed
inputs fail the context rather than taking that deployment-only bootstrap path.

## Rollout control

`scripts/scheduled/policy.json` is the reviewed source of truth. Hosted execution and reporting
are independent switches, so observe mode can run both while local admission remains disabled.
The `cutover` switch is accepted only with both hosted paths enabled and every prerequisite
recorded. It controls the old deep jobs and routine local deep calls together. Those jobs remain
in the fan-in as legitimate conditional skips; the ruleset never acquires matrix names or a second
required integration. Retained validation jobs preserve their existing event and platform behavior.

Deployment requires the controller/workflows, helper modules, local skills and policy to be merged
together. A read-only manual execution canary can run while scheduled execution is staged. Enable
hosted observe execution/reporting after the deterministic execution and reporting pilots, then
configure the personal Local App and prove its separate capabilities. Approve package/check
allowlists and the managed credential policy before any managed PR, including a canary. Record
those prerequisites through a reviewed policy change before cutover. Do not enable both the old
and new heavy paths indefinitely, and do not disable both during rollback.

The hosted health workflow and personal intake independently observe scheduler, reporting,
coverage and local scan availability. They distinguish fresh evidence, reused coverage, a failed
scan, staged/paused operation and unavailable systems. Neither process claims to monitor its
own total outage; the operator runbook is in the scheduled validation chapter.
The reporter retains the actual validated planning timestamp rather than substituting run
completion time. Hosted health uses read-only permissions and persists its component report as
an artifact and step summary before signaling failure. The shared coverage/health issue contains
separate hosted coverage and personal executor health records.
Health inventories unsuccessful reporting runs through the Actions API even after newer reports
succeed. Queue overflow, cancellation and reporting failure require rerunning the existing reporter,
not repeating expensive source checks; the operator recovery procedure is in the runbook.
The reporter processes its originating event and does not silently claim to backfill missing runs.
