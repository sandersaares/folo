# Set up Local scheduled triage and repair

Run this procedure only when the operator explicitly requests setup. Install or
update repository-level Local Copilot App automations using supported native App
tools and controls. Read [scheduled validation](../../docs/scheduled-validation.md)
and the `scheduled-triage`, `scheduled-intake` and `scheduled-repair` skills.

Do not run an automation during setup. Create and update entries **disabled**
unless the operator separately and explicitly authorizes enabling them. Do not
change accounts or billing, install tools, dispatch checks, or start repairs.
GitHub Actions does no AI inference; the App work uses the operator's personally
funded account and actual Local environment.

## Stage 1: Select the repository and ordinary App settings

Use `list_projects` to identify the repository's existing Local project. Use
`list_workflows` without an enabled filter to show existing entries, including
disabled and renamed ones. Inspect their prompts and project association with
supported App metadata or the Automations UI. Ask the operator which relevant
disabled entries to update; do not infer ownership from a name alone or create a
duplicate because a lookup is incomplete.

Confirm the actual Local environment, personal account, model/effort and schedule
for each role. When model settings need a decision, first offer one shared
model/effort choice for triage, repair coordination and new repair sessions; ask
for role-specific choices only if requested. The operator may explicitly choose
App model defaults instead of an override. Preserve existing settings unless a
change is requested; never select a paid model on the operator's behalf. Record
the chosen repair-session model in the repair prompt as ordinary prose, or
explicitly say to use the App defaults.

Review the App's available schedule description, timezone and next-run preview.
If timezone or next-run information is not exposed, ask the operator to confirm
the intended local-time interpretation and report that confirmation separately
from settings verified through App metadata. Do not assume UTC, invent an
environment ID or require UI fields the App does not expose. GitHub login alone
does not establish personal inference billing. Missing settings can be selected
in the native editor rather than reconstructed from private App storage.

The entries are:

| Suggested name | Saved prompt |
|---|---|
| Folo scheduled failure triage | Run the repository's `scheduled-triage` skill in this Local App project. Process open run reports oldest first using the selected model. Do not edit source, start repairs or change automation/account/billing settings. |
| Folo scheduled repair | Run the repository's `scheduled-intake` skill in this Local App project. Follow existing repairs and PRs first, then start at most one new repair. Use the operator-selected repair-session model settings described below; preserve existing session settings. Do not create per-PR automations or change automation/account/billing settings. |

Expand the repair prompt's model sentence with the actual operator choice, not a
placeholder or inferred model. Keep the prompts short and refer to the checked-in
skills rather than copying their procedures. There is no installation marker,
enrollment, policy file, profile registration or local-state migration.

Repair work uses ordinary branches and local native/WSL deep checks. Hosted
**Deep validation** runs only on main; App setup does not add hosted PR-head checks.

## Stage 2: Save disabled entries through supported controls

For an existing entry, present the proposed prompt and settings changes, then use
`save_workflow` with its actual `workflow_id` after operator approval. Update only
the selected entry. Preserve unrelated settings and keep redundant entries
disabled; remove one through supported App UI only when explicitly requested.
An unresolved existing claim or PR must be handed over explicitly, not discarded
as part of setup.

For a missing role, use `save_workflow` with `user_confirmation: "dialog"` to open
the native creation editor with the approved settings prefilled. Prefer asking
the operator to review and save the dialog over driving it with Computer Use.
Use Computer Use only when needed; repeated active-input interruptions require
an operator handoff rather than further retries. The following is a tool-input
example, not a file to persist or a schema for issues:

```json
{
  "project_id": "PROJECT_ID",
  "name": "ROLE_NAME",
  "prompt": "ROLE_PROMPT",
  "interval": "manual",
  "cron_expression": "OPERATOR_CRON",
  "mode": "autopilot",
  "enabled": false,
  "clear_remote_branch": true,
  "user_confirmation": "dialog"
}
```

| Placeholder | Value |
|---|---|
| `PROJECT_ID` | Actual repository project ID returned by `list_projects`. |
| `ROLE_NAME` | Operator-approved name for this role. |
| `ROLE_PROMPT` | The role's short prompt, with the operator's model choice included where needed. |
| `OPERATOR_CRON` | Operator-selected schedule, reviewed against the available App preview with local-time interpretation confirmed as described in Stage 1. |

Ask the operator to verify the actual **Local** environment, project, prompt,
approved model/effort, mode and schedule in the editor, and save disabled without
using **Create and run**. Opening the dialog does not establish that it was saved.
The dialog does not require guessing a host ID. A direct create instead requires
the actual observed `host_id`; never substitute a project ID or a machine name.
After verifying a saved entry, reuse its observed Local host ID to prefill later
dialogs targeting the same environment, rather than rediscovering or guessing it.
Only pass a `model` or `reasoning_effort` override that the operator selected and
the native tool supports.

For updates, `user_confirmation` is not supported: use the existing
`workflow_id` and the already approved changed fields. `interval: "manual"` with
`cron_expression` is the native custom-schedule representation, not a request to
run now. When changing to Local, use `clear_remote_branch: true`. To explicitly
return model/effort to App defaults on an update, use `clear_model: true` and/or
`clear_reasoning_effort: true`, omitting the corresponding value. Do not invent
account, billing, worker-model or workspace-type fields on `save_workflow`.

If a save result is uncertain, reread `list_workflows` and inspect the editor before
trying again. Report a concrete blocker when the outcome cannot be established;
do not write a setup journal or create another entry blindly.

## Stage 3: Verify and report

Reread `list_workflows` to verify the persisted repository, Local environment,
prompt, selected model/effort, mode, schedule and disabled state. Use operator
review of the native editor for settings not exposed by supported metadata.
Report the actual entry names/IDs, distinguish metadata-verified settings from
operator confirmations, and state any remaining uncertainty or decisions. If
confirmation conflicts with saved metadata, reconcile the discrepancy before
declaring setup complete.

Saving an entry does not prove unattended permissions, machine availability or
successful execution. An operator-reported empty-backlog run is only a smoke
check; it does not demonstrate issue claiming, repair-session creation or PR
publication. Do not perform a live exercise to close those evidence gaps during
setup.

Keep one enabled automation per role and avoid overlapping invocations when the
operator chooses to activate them. Enabling is a separate explicit decision, not
an implied consequence of setup. Do not invoke `run_workflow`, create a per-PR
timer or perform a live exercise as part of this procedure.
