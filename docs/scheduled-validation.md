# Scheduled validation

## Purpose

Keep ordinary PR validation fast while running expensive checks nightly. GitHub
Actions runs the checks and reports failures. A human or personally funded Local
Copilot App session triages each report into independently actionable issues and
repairs them through ordinary pull requests.

```text
Nightly or manual deep checks
              |
              v
Readable failed-run issue
              |
        Human or App triage
              |
              v
Problem issues, reusing existing ones where appropriate
              |
        Human or App repair
              |
              v
PR -> checks and review -> human approval and merge
```

GitHub issues, comments, labels, branches and PRs carry every handoff. A person
reading an issue can understand the problem, ownership and next action without
private files, encoded records or another agent's conversation. Templates guide
authors; human-written issues with equivalent information work on the same terms.

The [workflow design](../.github/workflows/design.md) and
[implementation guide](../.github/workflows/implementation.md) cover hosted
execution. [Testing](testing.md) defines mutation and Miri quality requirements.
The local shallow and deep validation commands retain their separate meanings.

## Checks and readable failure reports

**Standard validation** runs ordinary required checks. **Full deep validation**
runs the complete Miri platform matrix, many-seed Miri cases, mutation shards and
careful checks nightly at a fixed main commit. Every nightly run executes the
checks; previous success does not skip a night. Ordinary dependency/build caches
remain available.

A checker finding fails its Actions job and the validation run. Missed mutations,
mutation timeouts, setup failures and incomplete execution remain failures.
Independent matrix jobs continue after another job fails, and diagnostic uploads
run even on failure.

The completion-triggered **Scheduled reporting** workflow runs trusted
default-branch code with issue-write permission. Candidate check execution does
not have that permission. Reporting works even if planning or toolchain setup
fails before checker artifacts exist.

Each failed run attempt gets a normal `scheduled-run-failure` issue titled
**Scheduled validation failed on &lt;UTC date&gt;**. It contains the workflow and
run/attempt link, tested commit and start date, unsuccessful jobs with their
conclusions and direct links, observed error summaries and useful diagnostic
excerpts. It links full logs and tool-generated artifacts and explains missing
diagnostics or checks that never ran. The reporter describes observations, not
inferred root causes.

Reports start with `[Copilot speaking]`. Long findings may continue in readable
Markdown comments, not API response blobs or encoded pages. Useful failure details
remain on GitHub after Actions logs expire; successful-job inventories and whole
logs do not belong in issues.

The visible run URL and attempt identify a report. Reporter retries search open
and closed reports for that attempt before creating one. Each failed rerun gets
its own report; a successful rerun neither files a failure nor silently closes
older reports or problems. If reporting fails, its own Actions run fails visibly.
Rerunning it retries reporting, not the expensive checks. Resolve an ambiguous
duplicate with an ordinary linked duplicate explanation.

## Running checks manually

Manual checks need permission to run repository Actions, not Local App setup,
models or repair ownership. **Deep checks** is a reusable helper, not a manually
dispatched workflow.

To test a selected scope:

1. Open **Actions -> Selected deep validation -> Run workflow**.
2. Keep **Use workflow from** set to **main**.
3. Leave `source_sha` blank to test the fixed main commit selected for this run,
   or provide the full SHA of a repository branch/PR commit.
4. Enter check IDs in `check_ids` and exact crate names in `packages`, separated
   by commas. For example, `miri-ubuntu-latest` and `cpulist`.
5. Run the workflow and inspect its tested-commit summary, job results and
   diagnostic artifacts.

| Check | Supported IDs |
|---|---|
| Miri | `miri-ubuntu-latest`, `miri-windows-latest`, `miri-ubuntu-24.04-arm`, `miri-windows-11-arm` |
| Mutation testing | `mutants-ubuntu-latest-1` through `mutants-ubuntu-latest-8`, or `mutants-windows-latest-1` through `mutants-windows-latest-8`; each ID selects one shard |
| Careful checking | `careful-ubuntu-latest`, `careful-windows-latest` |
| Many-seed Miri | `miri-many-events_once-1` through `miri-many-events_once-4`; `miri-many-events-1` through `miri-many-events-2`; `miri-many-awaiter_set-1` through `miri-many-awaiter_set-2`; `miri-many-nm_impl-1` through `miri-many-nm_impl-2`; each ID selects its named crate |

Every requested crate must be covered by a selected check. Unknown checks,
unknown crates and incompatible selections fail rather than creating successful
empty runs. The optional source SHA selects the code under test, not the workflow
or reporter code.

For the complete deep suite, open **Full deep validation** and retain
**Use workflow from: main**. Leave `source_sha` blank to test the selected main
commit, or provide a full repository commit SHA to run the entire suite there.
Manual full and selected runs execute fresh checks and may produce failed-run
issues just like nightly runs. Their results do not themselves close reports or
problem issues.

For local execution, use the existing [build commands](build-and-tooling.md):
`just package="cpulist" validate-local` is shallow validation;
`just package="cpulist" validate-deep-local` runs the deep checks available on the
current platform. Use Selected deep validation when the required platform is not
available locally.

## Triage

Use the [scheduled-triage skill](../.github/skills/scheduled-triage/SKILL.md) or the
same workflow manually. Select open `scheduled-run-failure` issues oldest first,
claim the report, and process reports sequentially. Read the report, relevant
jobs/logs and source, then search relevant open and closed issues and related PRs,
including human-filed issues without automation labels.

Separate independently actionable problems, not jobs. A dependency download
failure affecting several jobs is one problem; unrelated defects in one job are
separate problems. A failed prerequisite explains work that never executed, not
defects in every dependent package. Compare actual problems rather than identical
log text or generated fingerprints.

Create or update a normal problem issue with `scheduled-finding`, including when
reusing a human issue. It needs an observed failure and affected scope, known cause
or explicit uncertainty, useful diagnostics and report/job links, reproduction
steps with applicable toolchain/target/seed details, and acceptance criteria.
Triage need not finish the repair investigation or prescribe a speculative patch.

Repeated unresolved failures update the existing problem with links and materially
new evidence. A supported recurrence after a fix can reopen the issue; a run of
pre-fix code is not a recurrence. Link uncertain duplicates rather than
consolidating unproven matches. Do not retarget an owned repair without reconciling
the changed diagnosis with its owner.

Add `needs-human` for permission, policy or external-intervention blockers and
explain the needed action. An infrastructure recovery can resolve an issue with
an explanation and applicable successful rerun, without an artificial source
patch. An unexplained intermittent failure is not fixed merely because a retry
passes.

Post a concise mapping of the report's failures to problem issues or explained
non-actionable outcomes. An intentional operator cancellation can be dismissed
with an explanation. Close the report only after every failure is accounted for
and the linked issues contain the handoff information. Missing decisive evidence
keeps it open with a concrete blocker. Report closure means triage is complete,
not that its problems are fixed.

## Ownership and handoff

`scheduled-run-failure` identifies triage work; `scheduled-finding` identifies the
repair backlog. `in-progress` records active ownership and `needs-human` records a
human-action blocker. Issue state and linked PRs supply the remaining lifecycle.

Before working, read current discussion, assignees and linked PRs. Claim the issue
by assigning the responsible GitHub user, adding `in-progress`, and posting an
ordinary comment, for example:

> [Copilot speaking]
>
> Claimed for repair by @owner in App session "Repair cpulist Miri failure".
> Working branch: `<branch>`. I will link the pull request here.

Use actual identities, and link the GitHub branch and PR when available. An App
session link is helpful but not required for another person to understand the
work. Session identity distinguishes agents sharing an account; an assignee alone
does not authorize adopting another worker's task.

Reread claims after posting and before work. If claims collide, the earlier
unreleased claim takes precedence and the other worker withdraws without removing
the winner's assignment or label. This is a human collaboration convention, not
an atomic locking service.

Post substantive progress, blockers, releases and handoffs, not heartbeats. A
blocked owner retains the claim. Elapsed time, machine downtime or an idle session
does not authorize takeover. The owner or a human maintainer must explicitly
release or transfer work, accounting for unpublished changes before replacing an
executor. `needs-human` blocks continuation until its stated requirement is
satisfied. Remove completed ownership status without disturbing other workers.

## Repair and PR completion

The [scheduled-intake skill](../.github/skills/scheduled-intake/SKILL.md) coordinates
repairs. It follows existing claimed issues and PRs first, then starts at most one
new unclaimed repair per invocation. Independent PRs awaiting review do not block
the entire backlog. The [scheduled-repair skill](../.github/skills/scheduled-repair/SKILL.md)
works on one issue in its native issue/PR-linked Local App session.

Confirm the failure, implement the correction and create a normal PR with
`Fixes #<issue>`. Follow repository conventions, including `increment-versions`
and the complete current **Version/release plan**. There is no separate version
attestation or managed-repair merge gate.

Normal required PR checks run. The worker also runs relevant deep checks at the
current PR commit and records the tested commit, scope, outcomes and command or
job links in a PR comment. Use Selected deep validation for unavailable local
platforms. Relevant subsequent changes require fresh results at the reviewed
head; unrelated green checks do not establish the fix. Reviewers assess the
targeted results alongside ordinary required checks and version validation.

Follow the same PR through CI/deep failures, conflicts and review feedback. Read
top-level discussion, review summaries and inline threads, including valid
low-confidence agent comments. Follow repository communication policy, including
the exception permitting responses to the original user's own human comments.
Every authored post begins with `[Copilot speaking]`. Request human decisions for
design changes or unsafe ambiguity; do not call blocked work complete.

Before the first PR is created, the repair worker uses native `rename_branch` to
name its branch `scheduled-repair-<issue-number>` (with the App's normal prefix)
and verifies the resulting branch contains `scheduled-repair-`. Branch names
containing this token opt out of external-service tests and production-backed PR
benchmarks from the initial PR event. Humans can use the same convention. This is
solely a side-effect safeguard, not repair identity, ownership, admission or
validation evidence; no account-specific prefix, body marker or registry is needed.

Final approval and merge remain human actions. A merged linked PR closes its
problem issue through normal GitHub behavior. A PR closed without merging does
not resolve the issue: record the disposition and explicitly release or block the
claim. No post-merge confirmation service is needed; later scheduled failures are
triaged normally.

## Local App setup and operation

Use separate repository-level **Local** App automations for triage and repair.
Inference uses the operator-selected personal account and model, not GitHub
Actions or a cloud coding agent. Keep one enabled entry per role and avoid
overlapping invocations. Starting at most one new repair per invocation is pacing,
not a hard financial cap.

Run the checked-in [setup prompt](../.github/prompts/setup-scheduled-remediation.prompt.md)
when installing or updating the entries. It uses `list_projects`, `list_workflows`,
`save_workflow` and the supported native editor. It lists existing entries,
updates the operator-selected disabled ones rather than blindly duplicating them,
and defaults to disabled until enabling is explicitly authorized. Model/effort,
personal account, Local environment and schedule are ordinary operator choices.
Setup neither installs tooling nor runs an automation.

| Role | Suggested App name | Skill |
|---|---|---|
| Triage | Folo scheduled failure triage | `scheduled-triage` |
| Repair coordination | Folo scheduled repair | `scheduled-intake` |

The coordinator uses native session lookup and `open_issue_session` or
`open_pr_session` to open/resume visible linked sessions. It preserves the
existing executor when available. GitHub remains the source of truth; native
runtime metadata locates the executor, not the work record.

Empty scans exit without posting or preparing Rust/WSL. Waiting for review does
not trigger repeated work. New decisions or other material information can resume
blocked work. Follow-up occurs through the repository repair automation, never
through per-PR timers or hidden watchers.

A paused machine leaves the backlog intact. After an explicit handoff, a human
or another machine can resume from GitHub without copying coordination state.
There are no Local state stores, enrollment files, profiles, dispatch tokens,
admission counters, health ledgers or mandatory issue schemas.
