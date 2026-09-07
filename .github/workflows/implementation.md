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

On Windows, the module scopes `CARGO_TARGET_DIR` for direct `cargo-semver-checks` and
`release-plz update` invocations to a stable, workspace-specific directory beneath the user
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
