---
name: increment-versions
description: Propose and apply crate version increments for a pull request that changed released content. Use when the validate-versions check fails, when a pull request is ready to merge, or when the user asks to increment crate versions.
---

# Scope

An **increment** raises a package's version in `Cargo.toml` so unreleased changes become
pending release. Publishing is a separate process described in
[`RELEASING.md`](../../../RELEASING.md).

A **version alignment** mechanically changes a package's declared version so every member of a
derived version group agrees. Non-publishable workspace helpers participate in alignment but do
not receive release assessments, semantic change levels, or publication checks.

A **change level** describes the substance of a package's released changes:
`breaking`, `nonbreaking`, or `patch`. This skill decides change levels; it does not choose
version numbers, and it does not decide which packages a level reaches. The tooling maps the
decided levels to version numbers, derives group membership from valid exact intra-workspace
dependency requirements, keeps every
[version group](../../../packages/cargo-release-plan/README.md#plan-and-report-schema) on a
single version, and resolves dependency requirements and `Cargo.lock` before application.
Application installs the captured resolved state without another dependency refresh.

Decide and apply the plan without a separate human approval request, for every change level.
Human review of the pull request as a whole is the approval step, including its version/release
plan. This skill does not approve or merge the pull request or publish packages.

This skill applies to a feature branch. Confirm the branch before Stage 1:

> git symbolic-ref --quiet --short HEAD

Stop and report if that command fails or prints nothing, which means the checkout has no branch
attached, and stop if it prints `main`. A branch-aware probe is what distinguishes a detached
checkout from a branch; a probe that resolves a revision instead reports a detached `HEAD` as
though it were an ordinary branch name.

Choose `{{WORK_DIR}}` as an absolute path and use that absolute form everywhere below. These
stages mix commands run from the caller's own working directory with `just` recipes, which
resolve a relative path against the repository root, so one relative working directory would name
two different places.

# Working files

Every stage reads and writes files under `{{WORK_DIR}}`, one untracked directory chosen for the
run. Write each command's output to its file as the command runs rather than reconstructing it
afterwards, and read a later stage's inputs from these files rather than from memory.

| File | Written in | Contents |
|------|------------|----------|
| `base.txt` | Stage 2 | The release baseline commit. |
| `prepared.json` | Stage 2 | The prepared input snapshot bound to the release baseline. |
| `report.json` | Stage 2 | The cargo-release-plan report after explicit offline preparation. |
| `diffs/{{PACKAGE}}.patch` | Stage 2 | A package's released-content diff against its anchor. |
| `semver-checks.log` | Stage 2 | The console output of `cargo-semver-checks`. |
| `analysis-order.json` | Stage 3 | The analysis batches. |
| `decisions.json` | Stage 4 | The change levels decided from current evidence, without a separate approval gate. |
| `plan.json` | Stage 5 | The proposed plan: change levels mapped to increment levels. |
| `preview/plan.json` | Stage 5 | The complete expanded plan with captured resolved files and original input identity. |
| `preview/report.json`, `preview/diffs/` | Stage 5 | The prospective release evidence for the resolved proposal. |
| `preview/semver-checks.log` | Stage 5 | Compatibility evidence built from the final prospective source, versions, and lockfile. |
| `preview/workspace/` | Stage 5 | The retained prospective workspace used only for compatibility evidence. |

Commit none of them.

# Placeholders

| Placeholder | Description |
|-------------|-------------|
| `WORK_DIR` | The absolute path of the untracked directory holding this run's working files. |
| `VERIFY_DIR` | A second untracked directory, also absolute, holding Stage 7 verification evidence without replacing the original preparation artifacts. |
| `DIFF_PATH` | A package's `diff_path` value from `report.json`. |
| `PACKAGE` | A package name. |
| `CHANGE_LEVEL` | A decided change level: `breaking`, `nonbreaking`, or `patch`. |
| `PREVIOUS_VERSION` | A package's version at its release anchor, before the pull request's pending increment. |
| `NEW_VERSION` | A package's resolved version from `preview/plan.json`, or its `declared_version` for a retained pending increment absent from the plan. |

# Stage 1: Run preflight checks

Prove that `cargo-semver-checks` can execute before using its output as evidence, then check for
publishable crates that still need their one-time manual first publication:

> just verify-semver-checks
>
> just check-never-published

Stop and report if either command exits non-zero. A checker that cannot execute must not be
interpreted as an absence of a required increment. `check-never-published` is an advisory scan of
the whole workspace: report its warnings, then continue. Stage 6 performs the exact fail-closed
check over the publishable targets in the resolved plan.

# Stage 2: Prepare resolution and collect evidence

Create the directory, record the release baseline, then prepare offline resolution before writing
the report, per-package diffs, and SemVer evidence:

> New-Item -ItemType Directory -Force -Path "{{WORK_DIR}}"
>
> git fetch origin main
>
> git rev-parse FETCH_HEAD > "{{WORK_DIR}}/base.txt"
>
> $env:RELEASE_PLAN_BASE = Get-Content -LiteralPath "{{WORK_DIR}}/base.txt"; just release-prepare "{{WORK_DIR}}"

Stop and report if any command exits non-zero. `just release-prepare` accepts the documented
cargo-semver-checks finding exit and fails on every other non-zero exit, so a non-zero exit
here means the evidence is incomplete.

Preparation performs the workflow's intended `cargo update --offline --workspace` resolution.
It can change the work-tree lockfile and must precede semantic grading. It does not request a
blanket third-party upgrade. If the tool must be built, the preparation launcher also allows
offline resolution so a stale lockfile does not prevent preparation from starting.

An unchanged-manifest refresh does not predict all proposal effects. Stage 5 separately resolves
prospective member versions and rewritten requirements, including re-selection among versions
already present in the lockfile. Read-only `release-report` and `validate-versions` do not perform
either refresh.

The baseline is the tip of the branch releases are made from, not the branch this pull request
targets. A stacked pull request targets an unreleased parent branch, and anchoring on it would
read that parent's pending increment as a release and hide the parent's unreleased changes. The
fetch is what makes the baseline current: a local remote-tracking ref can lag behind the release
branch, which would present an already-released increment as still pending.

`base.txt` fixes that baseline for the rest of the run, so every later `just release-report` and
`just validate-versions` invocation sets `RELEASE_PLAN_BASE` from it and no stage silently
compares against a different revision. An environment variable does not outlive the shell that
set it, so set it in the same invocation as the command that reads it.

[`report.json`](../../../packages/cargo-release-plan/README.md#plan-and-report-schema) lists
every publishable package in `packages`, with its `status`, `anchor`, `changed` array, `dependencies`,
`untracked` paths, and `diff_path`. A package has a `diff_path` when its released files differ
from its anchor. One whose `changed` entries are all `inherited` or `lockfile` has none, because
neither is a file difference.

Read each package's `untracked` entries before deciding its level. Untracked paths sit inside the
package directory but take no part in the released-content comparison, so a path this pull
request intends to publish contributes no evidence until it is tracked. Track such a path and
repeat this stage. Account for every remaining path as deliberately unreleased, so it is not
mistaken for assessed content, and carry those paths into the Stage 5 explanation.

The required `non_publishable_packages` array lists alignment-only targets with their names,
declared versions, and optional group references. These entries intentionally have no release
status, anchor, changed files, dependencies, or change level.

The `groups` object names each derived version group's sorted `members`, its highest declared
`version`, and whether the base-absence-aware consistency check passes. Members cover both
package arrays. A group key is its ordinally smallest member, even when that member is
non-publishable. `consistent: true` can reflect an exemption for a member absent from the base;
Stage 5 still aligns unequal declared versions. `just validate-versions` fails on a
non-exempt inconsistency as well as on a publishable package needing an increment.

The files describe the prepared work-tree content against the recorded release baseline. If the
source, manifests, group membership, or release baseline changes before application, repeat this
stage and the analysis and planning stages that follow. Do not combine fresh inputs with stale
decisions or an old expanded plan.

# Stage 3: Determine analysis order

Write the dependency-first analysis batches:

> just release-analysis-order "{{WORK_DIR}}/report.json" > "{{WORK_DIR}}/analysis-order.json"

Stop and report if the command exits non-zero.

`analysis-order.json` is a JSON array. Every release-assessment entry in
`report.json.packages` appears in exactly one batch; `non_publishable_packages` never appear:

```json
[
  { "order": 1, "packages": ["nm_impl"], "cyclic": false },
  { "order": 2, "packages": ["nm"], "cyclic": false }
]
```

A batch is `cyclic` only when its members genuinely depend on each other through the
relationships the report records, which is rare: a public package and the implementation package
behind it form an ordinary one-way edge, not a cycle.

| Field | Meaning |
|-------|---------|
| `order` | Ascending analysis position. Every dependency of a batch sits in a lower `order`. |
| `packages` | The batch members, in analysis order. |
| `cyclic` | `true` when the members depend on each other. |

# Stage 4: Decide change levels

Work through `analysis-order.json` in ascending `order`, deciding every package in the batch
before moving to the next one. This order guarantees that a package's workspace dependencies
are already decided when the package itself is decided. Repeat a `cyclic` batch until none of
its entries change.

Decide each package from these inputs:

* its entry in `report.json`, including `status`, the `changed` array, and the `dependencies`
  entries marked `"public": true`;
* the diff at `{{WORK_DIR}}/{{DIFF_PATH}}` when the entry has a `diff_path`;
* the package's `Cargo.toml` and the workspace `Cargo.toml` fields it inherits;
* the entries already recorded in `decisions.json` for the packages it depends on; and
* the package's floor in `semver-checks.log`.

A group whose members declare different versions needs no decision of its own, regardless of its
`consistent` verdict. Deciding change levels is this skill's only judgement, and Stage 5
realigns every group the decisions leave disagreeing. Judge only the publishable members on their
own released changes. Do not assign a semantic level to a non-publishable helper's source changes
or to group membership itself.

`semver-checks.log` closes each checked package's block with one `Summary` line. The package is
the one named in the `Checking` line that opens the block. The table lists line prefixes: a
summary that demands an increment continues with a count of the checks that failed.

| Summary line prefix | Floor |
|---------------------|-------|
| `Summary no semver update required` | None. |
| `Summary semver requires new minor version` | `nonbreaking`. |
| `Summary semver requires new major version` | `breaking`. |

A package absent from the log has no floor. An absent floor is not evidence that no increment
is required, because `cargo-semver-checks` inspects only part of the Rust API surface. Raise a
decision to at least its floor and never below it.

A change level describes the released content, not the version the manifest already declares. A
package whose version has already moved keeps that movement without it raising the level, because
Stage 5 retains an increment that is already sufficient.

Use [determining-level.md](determining-level.md) to choose `breaking`, `nonbreaking`, `patch`,
or no increment.

Rebuild `decisions.json` on each pass through this stage. Append each batch's outcome before
starting the next batch, so the next batch reads its dependency decisions from the file.
Omit a package that needs no increment.

`decisions.json` carries its own schema revision, which is independent of the plan and report
revision used below and does not move with it:

```json
{
  "schema_version": 1,
  "changes": [
    { "name": "nm", "level": "breaking" },
    { "name": "events", "level": "patch" }
  ]
}
```

# Stage 5: Resolve version groups and document the plan

A plan exists in two stages, and only the second is safe to present. A **proposed plan** records
one entry per decision, so a decision about a grouped package names that package or its group and
leaves the rest of the group implied. An **expanded plan** names every package whose version the
plan sets, including non-publishable alignment-only helpers, at the version each will carry.
Document the expanded plan, so the caller and PR reviewer see the complete version-target set
rather than one that widens during apply.

Applying the plan also rewrites requirements inside the dependents of the packages it moves. Those
dependents take no version from the plan, so they are not named here; Stage 4 gives each of them a
change level of its own, which is what puts any that would otherwise keep a published version into
the table.

Write the proposed plan, then preview its resolved effects:

> just create-release-plan "{{WORK_DIR}}/report.json" "{{WORK_DIR}}/decisions.json" "{{WORK_DIR}}/plan.json"
>
> just preview-release-plan "{{WORK_DIR}}/prepared.json" "{{WORK_DIR}}/plan.json" "{{WORK_DIR}}/preview"

Stop and report if either command exits non-zero. `create-release-plan` retains sufficient
existing pending-release increments, raises insufficient ones, raises any package whose public
API exposes a dependency that releases a breaking change, and realigns any group with unequal
declared versions that the decisions leave unnamed. Realignment usually targets the highest
version any publishable or non-publishable member declares. It instead patch-increments the whole
group when exact alignment would rewrite released content under an unchanged published version,
or when the highest version has a prerelease or build suffix and therefore cannot be used in a
valid exact group requirement. `preview-release-plan` resolves the proposed member versions,
rewritten requirements, and lockfile in a disposable workspace. It expands mechanical release
effects internally until the complete plan is stable, including transitive binary closures and
re-selection among already-locked dependency versions. It does not increment libraries merely
because their lockfile closure changed.

The preview wrapper builds compatibility evidence from the retained prospective workspace, with
both Cargo's manifest path and working directory pointing there. It then verifies that the
compatibility build did not change the captured source, manifests, or lockfile. A failed build or
state comparison invalidates the resolved artifact; stop rather than using partial evidence.

Read `preview/report.json`, its patches, and `preview/semver-checks.log` alongside the original
prepared evidence. Apply the Stage 4 floor rules to the final prospective compatibility log.
Explain every additional target and dependency effect. A mechanically required release establishes a minimum,
not a semantic compatibility judgement: apply [determining-level.md](determining-level.md) to
new dependency evidence and raise `decisions.json` where required, then repeat the commands above.
These are internal planning iterations before application, with no separate approval pause. Always generate
from the original prepared `report.json`, not the prospective report's already-incremented
versions, so repeated preview does not accumulate increments.

`preview/plan.json` uses schema revision 4, carries an explicit `version` on every entry, and
records both its expanded stage and the captured resolved state. The artifact binds application
to the original prepared inputs, not to temporary preview paths. Structural `expand-release-plan`
output does not carry that resolved evidence and must not be used as the resolved artifact.
After any input edit, repeat Stage 2 and regenerate the decisions and proposal. Never
widen an exact requirement merely to remove unexpected membership; that changes declared
grouping intent and requires the caller's direction.

There is no approval document or pause: Stage 6 applies the complete resolved plan unchanged.

Prepare a **Version/release plan** section for the pull request description. Its release set is
every package named by `preview/plan.json` plus every `pending-release` package in the final
`preview/report.json`: sufficient existing increments may be absent from the plan, but still
release on merge. Present one row per version group reached by that set, naming every member,
and one per ungrouped package. Read members and anchor versions from the report; read target
versions from the plan where present, otherwise from the report's `declared_version`. The previous
version is the anchor's version, not a version already incremented on this branch. Every other
analyzed package belongs in the no-increment summary.

| Packages / group members | Previous version(s) | Proposed version | Change level | Publication | Reason |
|--------------------------|---------------------|------------------|--------------|-------------|--------|
| `{{PACKAGE}}` | `{{PREVIOUS_VERSION}}` | `{{NEW_VERSION}}` | `{{CHANGE_LEVEL}}` | Publishable or version alignment only, not published. | Substantive released-content or alignment reason. |

A group's row lists every member and the level that governs the group, which is the highest level
required by its members, including public-dependency propagation. If members have different
previous versions, identify each member's version rather than presenting one as shared.
A group present only because it was realigned has no change level, so write `none` in that column
and give the reason in the row's explanation. Distinguish a retained version-only increment from
a substantive released-content change rather than inventing a change level. Read the
versions from `preview/plan.json` rather than assuming which form realignment took: a group usually
moves onto the highest version one of its members already declared, in which case name the
members that move, because each receives a new version while the member already there keeps the
version it has. Mark every non-publishable member as **version alignment only, not published**.
A grouped row must also show each member's current declared version when they differ, using the
original prepared report. Non-publishable helpers have no release anchor; show their current
declared version as the alignment starting point, not as a previous release.
A group whose members all move to a version none of them declared was patch-incremented instead,
because exact alignment would rewrite released content under an unchanged published version;
describe which publishable members become pending releases and which helpers only align.

Keep supporting evidence with the working explanation, citing the prepared or prospective
report, diff, or compatibility summary it rests on. In the PR section, explain substantive
reasons, including levels above the SemVer floor, prospective dependency-resolution effects,
dependent requirement or public-API movements, and group-only movement. Do not copy local
artifact paths, a changed-file inventory, or a validation log into the PR. State that the remaining
analyzed packages need no increment, and report deliberately unreleased untracked paths to the caller.

If the final evidence shows no released-content or version changes, say so explicitly in the PR
section instead of omitting it. An empty plan alone does not establish this: account for existing
pending increments and first-publication packages as well.

Report separately, and outside that table, every entry in `report.json.packages` that has no
`anchor`. Such a publishable package has never been released, so it has no version to increment.
Hand it off for a first publication as described in
[`RELEASING.md`](../../../RELEASING.md#first-publish-of-a-new-crate) rather than publishing it
from this run: bootstrap publication happens from a clean `main` checkout after these changes
merge, in dependency order, and configures Trusted Publishing. Name every such package in the
handoff.

Include any first-publication handoff in the PR section separately from increments, identifying
the initial declared version and the absence of a released predecessor.

Proceed without asking the caller to approve the levels. If further evidence or review feedback
changes a decision, return to Stage 4 to reassess it and its dependents, respecting every SemVer
floor, then regenerate the proposal and resolved plan and refresh the PR section. Report a
requested level below a floor as a conflict rather than recording it. Never edit generated plans
or captured artifacts directly.

# Stage 6: Apply the resolved plan

Confirm the working evidence and decisions still describe the current tree and refresh the
release-branch tip:

> git fetch origin main
>
> git rev-parse FETCH_HEAD

Stop and report if either command exits non-zero. Compare the returned commit with
`{{WORK_DIR}}/base.txt`. If it differs, return to Stage 2 rather than applying against a stale
release baseline. Changed source or group membership also requires Stage 2; changed decisions
alone require Stages 4 and 5. Regenerate the plan and its PR section before continuing.

`cargo-release-plan apply` raises existing versions and cannot create a crate on crates.io, so
confirm that every publishable target in the resolved expanded plan is already published.
Alignment-only helpers are validated as current Git-tracked workspace members but cause no
registry query:

> just check-increment-published "{{WORK_DIR}}/preview/plan.json"

Stop and report without applying anything if the command exits non-zero, following the
first-publication handoff above rather than publishing anything from this run.

Apply the complete resolved plan used to prepare the PR section, without a separate approval gate:

> just apply-release-plan "{{WORK_DIR}}/preview/plan.json"

A stale-input rejection requires fresh preparation and a regenerated plan. Do not bypass
it or refresh the lockfile manually to force the stale artifact to apply. An I/O failure can
leave the work tree partly edited. Inspect `git status` and `git diff`, report the affected files,
and stop rather than regenerating a proposal over an unexplained partial application.

# Stage 7: Verify the result

Verify the lockfile, then collect fresh evidence for the resulting tree into a second untracked
directory:

> just verify-lockfile
>
> New-Item -ItemType Directory -Force -Path "{{VERIFY_DIR}}"
>
> Copy-Item -LiteralPath "{{WORK_DIR}}/base.txt" -Destination "{{VERIFY_DIR}}/base.txt"
>
> $env:RELEASE_PLAN_BASE = Get-Content -LiteralPath "{{VERIFY_DIR}}/base.txt"; just release-report "{{VERIFY_DIR}}"
>
> $env:RELEASE_PLAN_BASE = Get-Content -LiteralPath "{{VERIFY_DIR}}/base.txt"; just validate-versions

Stop and report if `just verify-lockfile` exits non-zero. Application installs the captured
resolved lockfile, so a stale lockfile here is a defect rather than a reason for a late refresh.

Stop and report if `just release-report` exits non-zero as well. As in Stage 2 that means the
evidence is incomplete, and incomplete evidence cannot show that a decision was wrong. Only a
report that completed establishes the state the remaining checks are read against.

Stop and report a non-zero `just validate-versions` result. With unchanged inputs, preview has
already accounted for version, requirement, group, and binary lockfile effects, so verification
must not become a routine second versioning/application cycle. Distinguish a release-readiness
failure from an execution failure and preserve both prepared and verification evidence.

Manifest defects are not decisions to revisit. A requirement that does not name the version its
target declares, an in-scope exact requirement that is not a plain `=major.minor.patch`, or legacy
`workspace.metadata.release-plan.groups` configuration cannot be repaired by a change level.
Report the defect and restart at Stage 2 only after the underlying input is corrected, so
dependency-derived membership and all evidence are regenerated.

`just release-report` exits zero on a SemVer finding, so read `{{VERIFY_DIR}}/semver-checks.log`
as well. Report any floor above the assessed level rather than silently changing the captured
versions. Do not overwrite the original preparation artifacts with verification output; those
artifacts establish what was assessed and applied.

Commit the resulting `Cargo.toml`, dependency requirement, and `Cargo.lock` edits, and publish
the **Version/release plan** section prepared in Stage 5 in the pull request description.
Keep `[Copilot speaking]` first in an agent-authored PR body. Follow the presentation contract in
[`docs/git-workflow.md`](../../../docs/git-workflow.md#versionrelease-plan-section).

Reconcile the section with final evidence, covering every resolved target and retained pending
release. Later source, base, group, or decision changes require fresh assessment and an updated
section before the PR is ready for human review. Human review and merge of the complete PR remain
the final approval; completing this skill authorizes neither.
