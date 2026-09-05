---
name: increment-versions
description: Propose and apply crate version increments for a pull request that changed released content. Use when the validate-versions check fails, when a pull request is ready to merge, or when the user asks to increment crate versions.
---

# Scope

An **increment** raises a package's version in `Cargo.toml` so unreleased changes become
pending release. Publishing is a separate process described in
[`RELEASING.md`](../../../RELEASING.md).

A **change level** describes the substance of a package's released changes:
`breaking`, `nonbreaking`, or `patch`. This skill decides change levels; it does not choose
version numbers, and it does not decide which packages a level reaches. The tooling maps the
approved levels to version numbers, keeps every
[version group](../../../packages/cargo-release-plan/README.md#plan-and-report-schema) on a
single version, rewrites dependency requirements, and refreshes `Cargo.lock`.

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

**Temporary rule.** [`docs/git-workflow.md`](../../../docs/git-workflow.md) keeps version
increments off feature branches. While it does, a run stops after Stage 5 and reports the
approved change levels. Stages 6 and 7 apply once that rule permits increments inside a pull
request.

# Working files

Every stage reads and writes files under `{{WORK_DIR}}`, one untracked directory chosen for the
run. Write each command's output to its file as the command runs rather than reconstructing it
afterwards, and read a later stage's inputs from these files rather than from memory.

| File | Written in | Contents |
|------|------------|----------|
| `base.txt` | Stage 2 | The release baseline commit. |
| `report.json` | Stage 2 | The cargo-release-plan report. |
| `diffs/{{PACKAGE}}.patch` | Stage 2 | A package's released-content diff against its anchor. |
| `semver-checks.log` | Stage 2 | The console output of `cargo-semver-checks`. |
| `analysis-order.json` | Stage 3 | The analysis batches. |
| `decisions.json` | Stage 4 | The decided change levels. |
| `plan.json` | Stage 5 | The proposed plan: change levels mapped to increment levels. |
| `expanded.json` | Stage 5 | The expanded plan: that proposal with every package it reaches named. |

Commit none of them.

# Placeholders

| Placeholder | Description |
|-------------|-------------|
| `WORK_DIR` | The absolute path of the untracked directory holding this run's working files. |
| `VERIFY_DIR` | A second untracked directory, also absolute, written by Stage 7 and adopted as `WORK_DIR` when Stage 7 sends the run back to Stage 4. |
| `DIFF_PATH` | A package's `diff_path` value from `report.json`. |
| `PACKAGE` | A package name. |
| `CHANGE_LEVEL` | A decided change level: `breaking`, `nonbreaking`, or `patch`. |
| `NEW_VERSION` | A package's resolved version from `expanded.json`. |

# Stage 1: Verify the SemVer checker

Prove that `cargo-semver-checks` can execute before using its output as evidence:

> just verify-semver-checks

Stop and report if the command exits non-zero. A checker that cannot execute must not be
interpreted as an absence of a required increment.

# Stage 2: Collect evidence

Create the directory, record the release baseline, then write the report, the per-package diffs,
and the SemVer evidence:

> New-Item -ItemType Directory -Force -Path "{{WORK_DIR}}"
>
> git fetch origin main
>
> git rev-parse FETCH_HEAD > "{{WORK_DIR}}/base.txt"
>
> $env:RELEASE_PLAN_BASE = Get-Content -LiteralPath "{{WORK_DIR}}/base.txt"; just release-report "{{WORK_DIR}}"

Stop and report if any command exits non-zero. `just release-report` accepts the documented
cargo-semver-checks finding exit and fails on every other non-zero exit, so a non-zero exit
here means the evidence is incomplete.

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
every publishable package with its `status`, `anchor`, `changed` array, `dependencies`,
`untracked` paths, and `diff_path`. A package has a `diff_path` when its released files differ
from its anchor. One whose `changed` entries are all `inherited` or `lockfile` has none, because
neither is a file difference.

Read each package's `untracked` entries before deciding its level. Untracked paths sit inside the
package directory but take no part in the released-content comparison, so a path this pull
request intends to publish contributes no evidence until it is tracked. Track such a path and
repeat this stage. Account for every remaining path as deliberately unreleased, so it is not
mistaken for assessed content, and carry those paths into the Stage 5 proposal.

Its `groups` object names each version group's `members` and whether they currently declare one
version, in `consistent`. `just validate-versions` fails on an inconsistent group as well as on
a package needing an increment. Stage 5 resolves both, and a proposal row lists a group's
members.

The files describe the current work-tree content. Repeat this stage if that content changes
before the decisions are presented or applied.

# Stage 3: Determine analysis order

Write the dependency-first analysis batches:

> just release-analysis-order "{{WORK_DIR}}/report.json" > "{{WORK_DIR}}/analysis-order.json"

Stop and report if the command exits non-zero.

`analysis-order.json` is a JSON array. Every package in `report.json` appears in exactly one
batch:

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

* its entry in `report.json`, including `status` and the `changed` array;
* the diff at `{{WORK_DIR}}/{{DIFF_PATH}}` when the entry has a `diff_path`;
* the package's `Cargo.toml` and the workspace `Cargo.toml` fields it inherits;
* the entries already recorded in `decisions.json` for the packages it depends on; and
* the package's floor in `semver-checks.log`.

A group that `report.json` marks `"consistent": false` needs no decision of its own. Deciding
change levels is this skill's only judgement, and Stage 5 realigns every group the decisions
leave disagreeing. Judge each member on its own released changes.

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

Append each batch's outcome to `decisions.json` before starting the next batch, so the next
batch reads its dependency decisions from the file. Omit a package that needs no increment.

```json
{
  "schema_version": 1,
  "changes": [
    { "name": "nm", "level": "breaking" },
    { "name": "events", "level": "patch" }
  ]
}
```

# Stage 5: Resolve version groups and present the proposal

A plan exists in two stages, and only the second is safe to present. A **proposed plan** records
one entry per decision, so a decision about a grouped package names that package or its group and
leaves the rest of the group implied. An **expanded plan** names every package the plan reaches
at the version each will carry. Present the expanded plan, so the caller sees the whole set rather
than one that widens during apply.

Write the proposed plan, then expand it:

> just create-release-plan "{{WORK_DIR}}/report.json" "{{WORK_DIR}}/decisions.json" "{{WORK_DIR}}/plan.json"
>
> just expand-release-plan "{{WORK_DIR}}/plan.json" "{{WORK_DIR}}/expanded.json"

Stop and report if either command exits non-zero. `create-release-plan` retains sufficient
existing pending-release increments, raises insufficient ones, and realigns any inconsistent
group the decisions leave unnamed, targeting the highest version its members already declare.
`expand-release-plan` names every member of a group the plan reaches, at the single version that
group resolves to.

`expanded.json` carries an explicit `version` on every entry, and an `expanded` stamp recording
which stage it is:

```json
{
  "schema_version": 1,
  "expanded": true,
  "increments": [
    { "name": "nm", "version": "2.0.0" },
    { "name": "nm_impl", "version": "2.0.0" }
  ]
}
```

That stamp binds the document to the packages it names. Applying it after a version group gained
a member fails rather than editing a package that was never presented, so a group membership
change between approval and Stage 6 sends the run back to this stage rather than widening what
was approved. Repeat this stage from `create-release-plan` if that happens.

Approval does not produce a third document: the expanded plan the caller approves is the one
Stage 6 applies, unchanged.

Present one row per version group and one per ungrouped package, limited to what the plan moves: a
group qualifies when `expanded.json` names at least one of its members, and an ungrouped package
qualifies when `expanded.json` names it. Read the members from `report.json` and the versions from
`expanded.json`. Every other analyzed package belongs in the no-increment summary below rather
than in this table.

| Packages | Change level | New version |
|----------|--------------|-------------|
| `{{PACKAGE}}` | `{{CHANGE_LEVEL}}` | `{{NEW_VERSION}}` |

A group's row lists every member and the level that governs the group, which is the highest level
decided for any of its members. A group present only because it was realigned has no change
level, so write `none` in that column and give the reason in the row's explanation: its members
disagreed on a version and are moving onto the highest version one of them already declared. Name
the members that realignment moves, because each of them receives a new version and becomes
pending release; only the member already at that version keeps the version it has.

Follow each row with its supporting explanation. The explanation may span multiple paragraphs and
must cite the `report.json` entry, diff path, or `semver-checks.log` summary it rests on. Name any
member that is moving only because it shares a group. State that the remaining analyzed packages
need no increment, and report any `untracked` path left deliberately unreleased.

Report separately, and outside that table, every `report.json` entry that has no `anchor`. Such a
package has never been released, so it has no version to increment. Hand it off for a first
publication as described in
[`RELEASING.md`](../../../RELEASING.md#first-publish-of-a-new-crate) rather than publishing it
from this run: bootstrap publication happens from a clean `main` checkout after these changes
merge, in dependency order, and configures Trusted Publishing. Name every such package in the
handoff.

Ask the caller to approve or adjust the change levels. An adjustment is still bound by the
floors in Stage 4: report the conflict rather than recording a level below one. To record an
adjustment, edit `decisions.json` and repeat this stage from `create-release-plan`. Never edit
`plan.json` or `expanded.json` directly; a hand-edited expansion can give one group's members
different versions, which the tool rejects. Do not continue until the caller approves.

Stop here and report the approved change levels while the temporary rule in Scope applies.

# Stage 6: Apply approved changes

`cargo-release-plan apply` raises an existing version and cannot create a crate on crates.io, so
confirm that every package the approved expanded plan names is already published:

> just check-increment-published "{{WORK_DIR}}/expanded.json"

Stop and report without applying anything if the command exits non-zero, following the
first-publication handoff above rather than publishing anything from this run.

Apply the approved expanded plan, which is the document the caller saw:

> just apply-release-plan "{{WORK_DIR}}/expanded.json"

A non-zero exit here can leave the work tree partly edited: the command writes the manifests it
computed one after another and then refreshes the lockfile, so a failure part way through has
already written some of them. Treat it as a mutating failure. Inspect `git status` and `git diff`,
report which manifests and which lockfile the run touched, and stop rather than rerunning the
command over a partly edited tree.

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

Stop and report if `just verify-lockfile` exits non-zero. Refreshing the lockfile is part of
applying a plan, so a stale lockfile here is a defect to report rather than a decision to revisit.

Stop and report if `just release-report` exits non-zero as well. As in Stage 2 that means the
evidence is incomplete, and incomplete evidence cannot show that a decision was wrong. Only a
report that completed establishes the state the remaining checks are read against.

A verdict from `just validate-versions` is what sends the run back to Stage 4. Confirm from its
output that it rejected a package or a version group rather than failing to run, because the
command exits non-zero either way. Report an execution failure instead of revisiting the
decisions, which cannot repair it.

`just release-report` exits zero on a SemVer finding, so read `{{VERIFY_DIR}}/semver-checks.log`
as well. Return to Stage 4 the same way if any package's `Summary` line now demands a level
above the one that was applied to it.

Before returning to Stage 4, adopt `{{VERIFY_DIR}}` as the run's `{{WORK_DIR}}`: the applied
increments changed the report, and every later stage reads its inputs from `{{WORK_DIR}}`, so
leaving the old directory in place would regenerate a plan from pre-apply evidence and increment
the same packages a second time. The copied `base.txt` keeps the adopted directory on the
original baseline. Rerun Stage 3 against the adopted directory's `report.json` first.

Commit the resulting `Cargo.toml`, dependency requirement, and `Cargo.lock` edits, and
summarize the approved package change levels in the pull request description.
