//! The `prune` command: delete stored runs — and, on request, blessing sidecars —
//! from the data set a matching `analyze`/`list` pass resolves.
//!
//! `prune` accepts the same data-set-selection options as `analyze`/`list` and
//! resolves the identical commit topology via [`resolve_history`](super::resolve_history),
//! admitting a base-branch tip's dirty runs unconditionally
//! ([`DirtyTipPolicy::Always`](super::DirtyTipPolicy)) — a deletion tool sees every
//! stored run as a candidate, regardless of the present working-tree state.
//!
//! The caller must say what to delete: `--clean` removes clean runs, `--dirty`
//! removes dirty (uncommitted-tree) snapshots, and `--all` removes both. Pruning
//! runs never removes a blessing; `--include-blessings` additionally deletes every
//! blessing sidecar in the selected range — including on a commit with no recorded
//! run (an orphan left by a pre-emptive blessing) — and may be given on its own to
//! remove only blessings. A blessing is otherwise removed only by `unbless`.
//!
//! `prune` cleans only the *context branch's own* commits — those after the
//! merge-base with the base ref — so base-branch history is preserved by default.
//! When the context resolves onto the base branch itself (`context == base`), the
//! whole selection is base-branch history, so the destructive `--prune-base` guard
//! is required to confirm it. `--dry-run` previews what would be removed without
//! deleting anything.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use cbh_command::PruneOptions;
use cbh_config::{
    Config, cache_env, load_config, resolve_cache_path, resolve_config_path, resolve_local_path,
    resolve_project_id, resolve_repo, storage_env,
};
use cbh_diag::{Reporter, ReporterExt, StderrReporter, count_noun};
use cbh_git::{GitHistory, SystemGitHistory};
use cbh_model::DiscriminantSet;
use cbh_storage::{Storage, StorageFacade, finish_with_flush, resolve_storage};
use jiff::Timestamp;
use serde::Serialize;
use tick::Clock;

use super::announce::{AnnouncedBase, AnnouncedSince, announce_selection, selection_announcement};
use super::{
    AutoDiscriminants, DirtyTipPolicy, ReportFormat, ResolvedHistory, Selection,
    before_since_cutoff, discriminant_filtered_candidates, parse_since, resolve_auto_discriminants,
    resolve_discriminants, resolve_history, resolve_now,
};
use crate::{
    AnalyzeError, PruneBaseConfirmationRequiredError, PruneSelectionRequiredError, RenderedReports,
    ReportRequest,
};

/// Which runs a prune pass deletes.
#[derive(Clone, Copy, Eq, PartialEq)]
enum Scope {
    /// Delete clean and dirty runs.
    All,
    /// Delete only dirty (uncommitted-tree) snapshots.
    Dirty,
    /// Delete only clean runs.
    Clean,
}

impl Scope {
    /// Resolves the run-deletion scope from the `--clean`/`--dirty`/`--all`
    /// switches, or `None` when none is set (only blessings are being removed).
    ///
    /// At least one action must be requested: a run scope, or `--include-blessings`
    /// on its own. `--all` expands to both `clean` and `dirty`.
    fn from_options(options: &PruneOptions) -> Result<Option<Self>, AnalyzeError> {
        let scope = match (options.clean, options.dirty) {
            (true, true) => Some(Self::All),
            (false, true) => Some(Self::Dirty),
            (true, false) => Some(Self::Clean),
            (false, false) => None,
        };
        if scope.is_none() && !options.include_blessings {
            return Err(PruneSelectionRequiredError::new().into());
        }
        Ok(scope)
    }

    /// Whether this scope deletes clean runs (and thus durable history).
    fn touches_clean(self) -> bool {
        matches!(self, Self::All | Self::Clean)
    }

    /// Whether this scope deletes dirty runs.
    fn touches_dirty(self) -> bool {
        matches!(self, Self::All | Self::Dirty)
    }
}

/// Explains `prune`'s `--since` cutoff for the effective-selection announcement.
///
/// `prune` has no history-mode default look-back — it considers the whole selected
/// history — so the cutoff is either the explicit `--since` value or absent.
fn prune_since_reason(explicit: bool) -> &'static str {
    if explicit {
        "from the --since option"
    } else {
        "no default look-back"
    }
}

/// The real `prune`: load configuration, wire the configured storage and git
/// history, and orchestrate.
///
/// `clock_override` injects the [`tick::Clock`] that anchors a relative
/// `--since` bound (see [`resolve_now`](super::resolve_now)); production
/// passes `None` for the runtime wall clock.
// Thin real-adapter wiring: loads config from disk, builds the configured storage,
// and shells out via `SystemGitHistory`/`detect_auto_discriminants` before delegating every
// decision to the mutation-tested `prune_with`. In-crate tests cannot drive these real
// adapters deterministically; the binary's integration tests cover this edge.
#[cfg_attr(test, mutants::skip)]
pub async fn execute(
    options: &PruneOptions,
    workspace_dir: &Path,
    clock_override: Option<Clock>,
    storage_override: Option<StorageFacade>,
    auto_override: Option<AutoDiscriminants>,
) -> Result<RenderedReports, AnalyzeError> {
    let reporter = StderrReporter::new(options.verbose);

    let config_path = resolve_config_path(workspace_dir, options.config_path.as_deref());
    reporter.note_with(|| format!("loading configuration from {}", config_path.display()));
    let config = load_config(&config_path, options.config_path.is_some()).await?;

    let project_id = resolve_project_id(&config, workspace_dir);
    let local = resolve_local_path(options.local.as_ref(), storage_env().as_deref())?;
    let cache = resolve_cache_path(options.cache.as_ref(), cache_env().as_deref())?;
    let storage = resolve_storage(
        storage_override,
        local.as_deref(),
        &config,
        workspace_dir,
        cache.as_deref(),
        &reporter,
    )?;
    storage.synchronize_cache(&project_id, &reporter).await?;

    let git = SystemGitHistory::new(resolve_repo(workspace_dir, options.repo.as_deref()));
    let auto = resolve_auto_discriminants(auto_override).await?;

    let now = resolve_now(clock_override);
    let result = prune_with(
        &git,
        &storage,
        &project_id,
        &config,
        options,
        &auto,
        now,
        &reporter,
    )
    .await;
    // Flush the cache-invalidation marker even on a partial failure: any delete that
    // already reached the cloud must invalidate *other* machines' caches, so this
    // bump cannot be skipped just because a later step failed.
    let flush = storage
        .flush_pending_invalidation(&project_id, &reporter)
        .await;
    storage.report_cache_tally(&reporter);
    finish_with_flush(result, flush)
}

/// Storage- and git-generic `prune`: resolve the selected commit topology, choose
/// the objects to delete per the scope and filters, then either preview them
/// (`--dry-run`) or delete them.
#[expect(
    clippy::too_many_arguments,
    reason = "prune orchestration wires several injected ports alongside its options and discriminants"
)]
pub(crate) async fn prune_with<G, S>(
    git: &G,
    storage: &S,
    project_id: &str,
    config: &Config,
    options: &PruneOptions,
    auto: &AutoDiscriminants,
    now: Timestamp,
    reporter: &dyn Reporter,
) -> Result<RenderedReports, AnalyzeError>
where
    G: GitHistory,
    S: Storage,
{
    let request = ReportRequest::resolve(
        options.no_text,
        options.markdown.as_deref(),
        options.json.as_deref(),
    )?;
    let since = parse_since(options.since.as_deref(), now)?;
    let scope = Scope::from_options(options)?;
    let selection = Selection::from_prune(options);

    let discriminants = resolve_discriminants(&selection, Some(auto))?;
    let candidates =
        discriminant_filtered_candidates(storage, project_id, &discriminants, reporter).await?;

    let ResolvedHistory {
        target_ref,
        base_name,
        order,
        commit_times,
        admit_dirty,
        merge_base_index,
        tip_is_merge_base,
        ..
    } = resolve_history(git, config, &selection, DirtyTipPolicy::Always, reporter).await?;

    // The always-on effective-selection announcement: one line, printed regardless
    // of `--verbose`, naming the resolved (possibly auto-detected) partition, base
    // branch, and `--since` cutoff a plain run would otherwise resolve silently.
    // Emitted before the `--prune-base` guard so even that refusal states what was
    // selected.
    announce_selection(
        reporter,
        &selection_announcement(
            &discriminants,
            Some(AnnouncedBase {
                name: &base_name,
                auto: options.base.is_none(),
            }),
            None,
            Some(AnnouncedSince {
                cutoff: since,
                reason: prune_since_reason(options.since.is_some()),
            }),
        ),
    );

    // The `--prune-base` guard: when the context resolves onto the base branch
    // itself (`context == base`), the whole selection is base-branch history.
    // Refuse to delete it without explicit confirmation.
    if tip_is_merge_base && !options.prune_base {
        return Err(PruneBaseConfirmationRequiredError::new(base_name).into());
    }

    // Runs and blessing sidecars are pruned independently: `--clean`/`--dirty`/`--all`
    // select runs, while `--include-blessings` selects blessing sidecars. Partition
    // the candidates so each pass considers only its own object kind.
    let (runs, blessings): (Vec<_>, Vec<_>) = candidates
        .into_iter()
        .partition(|(_, parsed)| !parsed.is_bless());

    let mut items: Vec<RemovalItem> = Vec::new();

    if let Some(scope) = scope {
        for (key, parsed) in runs {
            let Some(index) = selected_index(
                &key,
                &parsed.commit,
                &order,
                merge_base_index,
                tip_is_merge_base,
                &options.commit,
                &target_ref,
                reporter,
            ) else {
                continue;
            };

            let kind = if parsed.is_dirty() {
                RunKind::Dirty
            } else {
                RunKind::Clean
            };

            match kind {
                RunKind::Dirty => {
                    if !scope.touches_dirty() {
                        reporter.note_with(|| {
                            format!("skipping {key}: --clean removes only clean runs")
                        });
                        continue;
                    }
                    // `--dirty` reproduces `clean`: a dirty snapshot is removed only on
                    // a commit whose dirty runs the matching analyze/list would admit.
                    // The broader scopes delete dirty runs wherever they sit.
                    if scope == Scope::Dirty
                        && !admit_dirty
                            .get(parsed.commit.as_str())
                            .copied()
                            .unwrap_or(false)
                    {
                        reporter.note_with(|| {
                            format!(
                                "skipping {key}: dirty snapshot on a base-side commit ({} admits \
                                 only clean runs)",
                                parsed.commit
                            )
                        });
                        continue;
                    }
                }
                RunKind::Clean => {
                    if !scope.touches_clean() {
                        reporter.note_with(|| {
                            format!("skipping {key}: --dirty removes only dirty runs")
                        });
                        continue;
                    }
                }
                RunKind::Bless => unreachable!("blessings were partitioned out"),
            }

            // The `--since` cutoff is a per-commit property — git's committer date —
            // which topology already resolved with the rest of the history, so deciding
            // it needs no object fetch.
            if before_since_cutoff(commit_times.get(&parsed.commit).copied(), since) {
                reporter.note_with(|| {
                    format!("skipping {key}: its commit predates the --since cutoff")
                });
                continue;
            }

            reporter.note_with(|| format!("selected {key} for removal"));
            items.push(RemovalItem {
                index,
                set: parsed.set,
                commit: parsed.commit,
                key,
                kind,
            });
        }
    }

    // Blessing sidecars are removed only with `--include-blessings`, and then every
    // one in the selected range goes — including on a commit with no recorded run (an
    // orphan left by a pre-emptive blessing) — because a blessing stands on its own
    // rather than referencing a stored run.
    if options.include_blessings {
        for (key, parsed) in blessings {
            let Some(index) = selected_index(
                &key,
                &parsed.commit,
                &order,
                merge_base_index,
                tip_is_merge_base,
                &options.commit,
                &target_ref,
                reporter,
            ) else {
                continue;
            };
            if before_since_cutoff(commit_times.get(&parsed.commit).copied(), since) {
                reporter.note_with(|| {
                    format!("skipping {key}: its commit predates the --since cutoff")
                });
                continue;
            }
            reporter.note_with(|| format!("selected {key} for removal (blessing sidecar)"));
            items.push(RemovalItem {
                index,
                set: parsed.set,
                commit: parsed.commit,
                key,
                kind: RunKind::Bless,
            });
        }
    }

    let plan = build_plan(project_id, &target_ref, &items);

    if !options.dry_run {
        for set in &plan.sets {
            for commit in &set.commits {
                for key in &commit.keys {
                    reporter.note_with(|| format!("deleting {key}"));
                    storage.delete(key).await?;
                }
            }
        }
    }

    Ok(request.render(|format| render_plan(&plan, format, options.dry_run)))
}

/// Whether a commit at `index` (its first-parent position) is eligible for
/// removal. When pruning the base branch itself (`tip_is_merge_base`), every
/// selected commit is eligible. Otherwise only the context branch's own commits —
/// those strictly after the fork point — are eligible, so base-branch history is
/// preserved. `resolve_history` reports that fork point even when the base was merged
/// into the branch (the newest shared commit, not the off-line merge-base). A `None`
/// fork point means the two lines share no first-parent commit at all, which cannot
/// arise once a merge-base exists; it is treated conservatively as base-side so a
/// degenerate topology never authorises deleting history without `--prune-base`.
fn commit_is_eligible(
    index: usize,
    merge_base_index: Option<usize>,
    tip_is_merge_base: bool,
) -> bool {
    if tip_is_merge_base {
        return true;
    }
    match merge_base_index {
        Some(merge_base_index) => index > merge_base_index,
        None => false,
    }
}

/// Whether a commit matches the `<commit>` selection (case-insensitive prefix
/// match, so a short commit ID selects the full one). An empty selection matches every
/// commit.
fn commit_matches(commit: &str, filters: &[String]) -> bool {
    if filters.is_empty() {
        return true;
    }
    let commit = commit.to_ascii_lowercase();
    filters
        .iter()
        .any(|filter| commit.starts_with(&filter.to_ascii_lowercase()))
}

/// The first-parent index of `commit` when it is in the selected range: on the
/// target's history, eligible for removal (not preserved base-branch history), and
/// matching the `<commit>` selection. Returns `None`, after noting why, when the
/// commit is out of range. `key` names the object being considered, for the note.
///
/// Both prune passes (runs and blessing sidecars) share these commit-level gates;
/// each pass then applies its own remaining checks (run-kind and `--since`).
#[allow(
    clippy::too_many_arguments,
    reason = "shared gate over resolved topology"
)]
fn selected_index(
    key: &str,
    commit: &str,
    order: &HashMap<String, usize>,
    merge_base_index: Option<usize>,
    tip_is_merge_base: bool,
    commit_filters: &[String],
    target_ref: &str,
    reporter: &dyn Reporter,
) -> Option<usize> {
    let Some(&index) = order.get(commit) else {
        reporter.note_with(|| {
            format!("skipping {key}: commit {commit} is not on {target_ref}'s history")
        });
        return None;
    };
    // Preserve base-branch history: unless this is a base-branch prune, only the
    // commits after the merge-base (the context branch's own commits) are eligible.
    if !commit_is_eligible(index, merge_base_index, tip_is_merge_base) {
        reporter.note_with(|| {
            format!(
                "skipping {key}: commit {commit} is on the base branch (preserved; prune from \
                 the base branch with --prune-base to remove it)"
            )
        });
        return None;
    }
    if !commit_matches(commit, commit_filters) {
        reporter.note_with(|| {
            format!(
                "skipping {key}: commit {commit} does not match the requested <commit> arguments"
            )
        });
        return None;
    }
    Some(index)
}

/// Which kind of object a removal item names.
#[derive(Clone, Copy, Eq, PartialEq)]
enum RunKind {
    Clean,
    Dirty,
    Bless,
}

impl RunKind {
    /// Whether this kind counts as a stored run (clean or dirty) rather than a
    /// blessing sidecar.
    fn is_run(self) -> bool {
        matches!(self, Self::Clean | Self::Dirty)
    }
}

/// One object slated for removal, with its first-parent position.
#[derive(Clone)]
struct RemovalItem {
    /// First-parent position of the commit, for oldest-first ordering.
    index: usize,
    /// The discriminant set the object belongs to.
    set: DiscriminantSet,
    /// The commit the object was measured against.
    commit: String,
    /// The storage key to delete.
    key: String,
    /// Whether the object is a clean run, dirty run, or blessing sidecar.
    kind: RunKind,
}

/// One commit's objects to remove, in first-parent order.
#[derive(Clone)]
struct CommitRemoval {
    /// The commit the runs were measured against (full commit ID, or a label in tests).
    commit: String,
    /// How many runs (clean or dirty) are removed on this commit.
    runs: usize,
    /// How many blessing sidecars are removed on this commit.
    blessings: usize,
    /// The storage keys removed on this commit, in storage-key order.
    keys: Vec<String>,
}

/// One discriminant set's slice of the removal plan.
#[derive(Clone)]
struct SetRemoval {
    /// The comparable partition this slice covers.
    set: DiscriminantSet,
    /// Runs removed in this set.
    runs: usize,
    /// Blessing sidecars removed in this set.
    blessings: usize,
    /// Per-commit breakdown, oldest first by git topology.
    commits: Vec<CommitRemoval>,
}

/// The fully resolved removal plan, ready to render.
#[derive(Clone)]
struct Plan {
    /// The project the runs belong to.
    project: String,
    /// The target ref the topology was resolved against.
    target_ref: String,
    /// Per-set breakdown, one entry per discriminant set with removable objects.
    sets: Vec<SetRemoval>,
    /// Total runs removed across every set.
    total_runs: usize,
    /// Total blessing sidecars removed across every set.
    total_blessings: usize,
}

/// Groups the selected objects by discriminant set and commit (ordered by
/// first-parent topology, oldest first).
fn build_plan(project_id: &str, target_ref: &str, items: &[RemovalItem]) -> Plan {
    let mut sets: Vec<DiscriminantSet> = items.iter().map(|item| item.set.clone()).collect();
    sets.sort();
    sets.dedup();

    let set_removals: Vec<SetRemoval> = sets
        .iter()
        .map(|set| {
            let in_set: Vec<&RemovalItem> = items.iter().filter(|item| &item.set == set).collect();

            // Key by first-parent position so commits read oldest-first.
            let mut by_commit: BTreeMap<usize, CommitRemoval> = BTreeMap::new();
            for item in &in_set {
                let entry = by_commit
                    .entry(item.index)
                    .or_insert_with(|| CommitRemoval {
                        commit: item.commit.clone(),
                        runs: 0,
                        blessings: 0,
                        keys: Vec::new(),
                    });
                if item.kind.is_run() {
                    entry.runs = entry.runs.saturating_add(1);
                } else {
                    entry.blessings = entry.blessings.saturating_add(1);
                }
                entry.keys.push(item.key.clone());
            }

            SetRemoval {
                set: set.clone(),
                runs: in_set.iter().filter(|item| item.kind.is_run()).count(),
                blessings: in_set.iter().filter(|item| !item.kind.is_run()).count(),
                commits: by_commit.into_values().collect(),
            }
        })
        .collect();

    Plan {
        project: project_id.to_owned(),
        target_ref: target_ref.to_owned(),
        sets: set_removals,
        total_runs: items.iter().filter(|item| item.kind.is_run()).count(),
        total_blessings: items.iter().filter(|item| !item.kind.is_run()).count(),
    }
}

/// The past-tense or conditional verb for the plan, matching `--dry-run`.
fn verb(dry_run: bool) -> &'static str {
    if dry_run { "Would remove" } else { "Removed" }
}

/// A `" (plus N blessings)"` fragment when blessings are removed, else empty.
fn blessing_suffix(blessings: usize) -> String {
    if blessings == 0 {
        String::new()
    } else {
        format!(" (plus {})", count_noun(blessings, "blessing"))
    }
}

/// Renders the removal plan in the requested format.
fn render_plan(plan: &Plan, format: ReportFormat, dry_run: bool) -> String {
    match format {
        ReportFormat::Text => render_plan_text(plan, dry_run),
        ReportFormat::Markdown => render_plan_markdown(plan, dry_run),
        ReportFormat::Json => render_plan_json(plan, dry_run),
    }
}

fn render_plan_text(plan: &Plan, dry_run: bool) -> String {
    let mut lines = vec![format!(
        "Prune plan for project {} (target {})",
        plan.project, plan.target_ref
    )];
    if plan.sets.is_empty() {
        lines.push(String::new());
        lines.push("No run matches the selection.".to_owned());
    } else {
        for set in &plan.sets {
            lines.push(String::new());
            lines.push(set.set.to_string());
            lines.push(format!(
                "  {}{} across {}",
                count_noun(set.runs, "run"),
                blessing_suffix(set.blessings),
                count_noun(set.commits.len(), "commit")
            ));
            for commit in &set.commits {
                lines.push(format!(
                    "    {}  {}{}",
                    commit.commit,
                    count_noun(commit.runs, "run"),
                    blessing_suffix(commit.blessings)
                ));
            }
        }
        lines.push(String::new());
        lines.push(format!(
            "{} {}{} across {}",
            verb(dry_run),
            count_noun(plan.total_runs, "run"),
            blessing_suffix(plan.total_blessings),
            count_noun(plan.sets.len(), "discriminant set")
        ));
    }
    format!("{}\n", lines.join("\n"))
}

fn render_plan_markdown(plan: &Plan, dry_run: bool) -> String {
    let mut lines = vec![format!(
        "# Prune plan for {} (target {})",
        plan.project, plan.target_ref
    )];
    if plan.sets.is_empty() {
        lines.push(String::new());
        lines.push("No run matches the selection.".to_owned());
    } else {
        for set in &plan.sets {
            lines.push(String::new());
            lines.push(format!("## {}", set.set));
            lines.push(String::new());
            lines.push(format!(
                "{}{} across {}",
                count_noun(set.runs, "run"),
                blessing_suffix(set.blessings),
                count_noun(set.commits.len(), "commit")
            ));
            lines.push(String::new());
            lines.push("| Commit | Runs | Blessings |".to_owned());
            lines.push("| --- | --- | --- |".to_owned());
            for commit in &set.commits {
                lines.push(format!(
                    "| {} | {} | {} |",
                    commit.commit, commit.runs, commit.blessings
                ));
            }
        }
        lines.push(String::new());
        lines.push(format!(
            "**{} {}{} across {}**",
            verb(dry_run),
            count_noun(plan.total_runs, "run"),
            blessing_suffix(plan.total_blessings),
            count_noun(plan.sets.len(), "discriminant set")
        ));
    }
    format!("{}\n", lines.join("\n"))
}

fn render_plan_json(plan: &Plan, dry_run: bool) -> String {
    #[derive(Serialize)]
    struct JsonCommit<'a> {
        commit: &'a str,
        runs: usize,
        blessings: usize,
        keys: &'a [String],
    }
    #[derive(Serialize)]
    struct JsonSet<'a> {
        engine: &'a str,
        target_triple: &'a str,
        machine_key: &'a str,
        runs: usize,
        blessings: usize,
        commits: Vec<JsonCommit<'a>>,
    }
    #[derive(Serialize)]
    struct JsonTotals {
        runs: usize,
        blessings: usize,
        discriminant_sets: usize,
    }
    #[derive(Serialize)]
    struct JsonPlan<'a> {
        project: &'a str,
        target_ref: &'a str,
        dry_run: bool,
        sets: Vec<JsonSet<'a>>,
        totals: JsonTotals,
    }

    let sets: Vec<JsonSet<'_>> = plan
        .sets
        .iter()
        .map(|set| JsonSet {
            engine: set.set.engine.as_str(),
            target_triple: set.set.target_triple.as_str(),
            machine_key: set.set.machine_key.as_str(),
            runs: set.runs,
            blessings: set.blessings,
            commits: set
                .commits
                .iter()
                .map(|commit| JsonCommit {
                    commit: &commit.commit,
                    runs: commit.runs,
                    blessings: commit.blessings,
                    keys: &commit.keys,
                })
                .collect(),
        })
        .collect();

    let document = JsonPlan {
        project: &plan.project,
        target_ref: &plan.target_ref,
        dry_run,
        sets,
        totals: JsonTotals {
            runs: plan.total_runs,
            blessings: plan.total_blessings,
            discriminant_sets: plan.sets.len(),
        },
    };
    serde_json::to_string_pretty(&document).expect("prune plan structures always serialize to JSON")
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    #![allow(clippy::indexing_slicing, reason = "panic is fine in tests")]

    use std::path::PathBuf;

    use cbh_config::Config;
    use cbh_diag::RecordingReporter;
    use cbh_git::FakeGitHistory;
    use cbh_model::Engine;
    use cbh_storage::{MemoryStorage, Storage};
    use futures::executor::block_on;
    use jiff::Timestamp;
    use ohno::ErrorExt as _;

    use super::*;
    use crate::{
        NoOutputSelectedError, PruneBaseConfirmationRequiredError, PruneSelectionRequiredError,
        UnresolvedRefError,
    };

    fn config() -> Config {
        Config::default()
    }

    /// The auto-detected discriminant values the tests seed their default partition under.
    fn auto() -> AutoDiscriminants {
        AutoDiscriminants {
            triple: "x86_64-unknown-linux-gnu".to_owned(),
            machine_key: "m1".into(),
        }
    }

    /// Most fixtures exercise the `--dirty` scope. Base-branch fixtures
    /// (`linear_git`, HEAD on the base) also set `prune_base` so the base-branch
    /// guard does not reject them; it is inert on feature-branch fixtures.
    fn dirty_options() -> PruneOptions {
        PruneOptions {
            dirty: true,
            prune_base: true,
            ..PruneOptions::default()
        }
    }

    fn clean_key(commit: &str) -> String {
        format!("v1/folo/objects/callgrind/x86_64-unknown-linux-gnu/m1/{commit}/clean.json")
    }

    fn dirty_key(commit: &str, unix: i64) -> String {
        format!("v1/folo/objects/callgrind/x86_64-unknown-linux-gnu/m1/{commit}/dirty-{unix}.json")
    }

    fn bless_key(commit: &str, unix: i64) -> String {
        format!("v1/folo/objects/callgrind/x86_64-unknown-linux-gnu/m1/{commit}/bless-{unix}.json")
    }

    /// Pruning selects keys and never parses bodies, so fixture bodies need no run data.
    fn store(storage: &MemoryStorage, key: &str) {
        block_on(storage.put(key, b"{}")).unwrap();
    }

    /// Stores a blessing sidecar. `prune` never parses these (it only deletes
    /// them), so any well-formed bytes under a `bless-` key suffice.
    fn store_bless(storage: &MemoryStorage, key: &str) {
        block_on(storage.put(key, b"{}")).unwrap();
    }

    fn keys(storage: &MemoryStorage) -> Vec<String> {
        let mut keys = block_on(storage.list("v1/")).unwrap();
        keys.sort();
        keys
    }

    /// A linear master history `c0 - c1 - c2 - c3`, HEAD at the tip.
    fn linear_git() -> FakeGitHistory {
        let mut git = FakeGitHistory::new();
        git.commit("c0", None)
            .commit("c1", Some("c0"))
            .commit("c2", Some("c1"))
            .commit("c3", Some("c2"))
            .branch("master", "c3")
            .head("master")
            .mark_default("master");
        git
    }

    /// A master history with a feature branch off `c1`:
    ///
    /// ```text
    /// master:  c0 - c1 - c2 - c3
    ///                \
    /// feature:        f1 - f2   (HEAD)
    /// ```
    fn feature_git() -> FakeGitHistory {
        let mut git = FakeGitHistory::new();
        git.commit("c0", None)
            .commit("c1", Some("c0"))
            .commit("c2", Some("c1"))
            .commit("c3", Some("c2"))
            .commit("f1", Some("c1"))
            .commit("f2", Some("f1"))
            .branch("master", "c3")
            .branch("feature", "f2")
            .head("feature")
            .mark_default("master");
        git
    }

    /// A feature history whose own commits carry committer dates, for the
    /// `--since` cutoff tests. Branched off the base at `c1`:
    ///
    /// ```text
    /// master:  c0 - c1 - c2 - c3
    ///                \
    /// feature:        f1 - f2 - f3   (HEAD)
    ///         dates:  100s 180s 300s
    /// ```
    fn dated_feature_git() -> FakeGitHistory {
        let at = |seconds| Timestamp::from_second(seconds).unwrap();
        let mut git = FakeGitHistory::new();
        git.commit("c0", None)
            .commit("c1", Some("c0"))
            .commit("c2", Some("c1"))
            .commit("c3", Some("c2"))
            .commit_at("f1", Some("c1"), at(100))
            .commit_at("f2", Some("f1"), at(180))
            .commit_at("f3", Some("f2"), at(300))
            .branch("master", "c3")
            .branch("feature", "f3")
            .head("feature")
            .mark_default("master");
        git
    }

    /// A fixed analysis anchor for prune tests. These exercise absolute or
    /// unset `--since` cutoffs, so the exact instant is immaterial; it
    /// only stands in for the clock reading `prune::execute` supplies in production.
    fn now() -> Timestamp {
        "2024-06-01T00:00:00Z"
            .parse()
            .expect("valid RFC 3339 instant")
    }

    /// Drives `prune_with` and unwraps the rendered message.
    fn prune(storage: &MemoryStorage, git: &FakeGitHistory, options: &PruneOptions) -> String {
        let rendered = block_on(prune_with(
            git,
            storage,
            "folo",
            &config(),
            options,
            &auto(),
            now(),
            &RecordingReporter::quiet(),
        ))
        .unwrap();
        rendered
            .text
            .expect("prune renders the text report by default")
    }

    /// Drives `prune_with` requesting the JSON report and returns the JSON text.
    /// The text report is suppressed so the JSON is the only rendered output.
    fn prune_json(storage: &MemoryStorage, git: &FakeGitHistory, options: &PruneOptions) -> String {
        let mut options = options.clone();
        options.no_text = true;
        options.markdown = None;
        options.json = Some(PathBuf::from("report.json"));
        let rendered = block_on(prune_with(
            git,
            storage,
            "folo",
            &config(),
            &options,
            &auto(),
            now(),
            &RecordingReporter::quiet(),
        ))
        .unwrap();
        rendered
            .json
            .expect("the JSON report was rendered for the requested path")
    }

    /// Drives `prune_with` requesting the Markdown report and returns the Markdown
    /// text.
    fn prune_markdown(
        storage: &MemoryStorage,
        git: &FakeGitHistory,
        options: &PruneOptions,
    ) -> String {
        let mut options = options.clone();
        options.no_text = true;
        options.json = None;
        options.markdown = Some(PathBuf::from("report.md"));
        let rendered = block_on(prune_with(
            git,
            storage,
            "folo",
            &config(),
            &options,
            &auto(),
            now(),
            &RecordingReporter::quiet(),
        ))
        .unwrap();
        rendered
            .markdown
            .expect("the Markdown report was rendered for the requested path")
    }

    #[test]
    fn prune_announces_the_effective_selection() {
        let storage = MemoryStorage::new();
        store(&storage, &dirty_key("f2", 300));
        let reporter = RecordingReporter::quiet();
        block_on(prune_with(
            &feature_git(),
            &storage,
            "folo",
            &config(),
            &dirty_options(),
            &auto(),
            now(),
            &reporter,
        ))
        .unwrap();
        // The auto-detected partition, auto-detected base branch, and the (absent,
        // no-default-look-back) `--since` cutoff are all named on the always-on line.
        assert!(
            reporter.announced("target-triple=x86_64-unknown-linux-gnu (auto-detected)"),
            "{:?}",
            reporter.announcements()
        );
        assert!(
            reporter.announced("machine-key=m1 (auto-detected)"),
            "{:?}",
            reporter.announcements()
        );
        assert!(
            reporter.announced("base=master (auto-detected)"),
            "{:?}",
            reporter.announcements()
        );
        assert!(
            reporter.announced("since=none (no default look-back)"),
            "{:?}",
            reporter.announcements()
        );
    }

    #[test]
    fn prune_announces_an_explicit_since_reason() {
        let storage = MemoryStorage::new();
        store(&storage, &dirty_key("f2", 300));
        let options = PruneOptions {
            since: Some("2024-01-01".to_owned()),
            ..dirty_options()
        };
        let reporter = RecordingReporter::quiet();
        block_on(prune_with(
            &feature_git(),
            &storage,
            "folo",
            &config(),
            &options,
            &auto(),
            now(),
            &reporter,
        ))
        .unwrap();
        assert!(
            reporter.announced("since=2024-01-01T00:00:00Z (from the --since option)"),
            "{:?}",
            reporter.announcements()
        );
    }

    #[test]
    fn prune_skips_a_run_whose_commit_is_off_history() {
        let storage = MemoryStorage::new();
        // A dirty run on a commit that is not on the analyzed (HEAD) history.
        store(&storage, &dirty_key("z9", 100));
        let reporter = RecordingReporter::new();
        block_on(prune_with(
            &linear_git(),
            &storage,
            "folo",
            &config(),
            &dirty_options(),
            &auto(),
            now(),
            &reporter,
        ))
        .unwrap();
        assert!(
            reporter.contains("is not on HEAD's history"),
            "{:?}",
            reporter.notes()
        );
        // The off-history run is left in place.
        assert!(keys(&storage).contains(&dirty_key("z9", 100)), "kept");
    }

    #[test]
    fn dirty_scope_removes_target_side_dirty_runs_on_a_feature_branch() {
        let storage = MemoryStorage::new();
        // A clean run on each side proves that pruning only removes dirty snapshots.
        for commit in ["c1", "f1"] {
            store(&storage, &clean_key(commit));
        }
        // Dirty snapshots: base-side (c1) must survive; target-side (f1, f2) go.
        store(&storage, &dirty_key("c1", 150));
        store(&storage, &dirty_key("f1", 200));
        store(&storage, &dirty_key("f2", 300));
        let git = feature_git();

        let report = prune_json(&storage, &git, &dirty_options());
        let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
        assert_eq!(
            parsed["totals"]["runs"], 2,
            "two target-side dirty runs: {report}"
        );
        assert_eq!(parsed["dry_run"], false);

        let remaining = keys(&storage);
        assert!(
            remaining.contains(&dirty_key("c1", 150)),
            "base-side dirty survives"
        );
        assert!(remaining.contains(&clean_key("f1")), "clean runs survive");
        assert!(
            !remaining.contains(&dirty_key("f1", 200)),
            "target dirty removed"
        );
        assert!(
            !remaining.contains(&dirty_key("f2", 300)),
            "target dirty removed"
        );
    }

    #[test]
    fn dirty_scope_removes_the_base_tip_dirty_unconditionally() {
        // On the base branch with a CLEAN working tree, the base-branch tip's
        // dirty runs are still removed (no working-tree-dirty guard). Earlier
        // base-side dirty runs stay untouched.
        let storage = MemoryStorage::new();
        for commit in ["c1", "c3"] {
            store(&storage, &clean_key(commit));
        }
        store(&storage, &dirty_key("c1", 150));
        store(&storage, &dirty_key("c3", 300));
        let git = linear_git(); // No mark_dirty: the working tree is clean.

        let report = prune(&storage, &git, &dirty_options());
        assert!(report.contains("Removed 1 run"), "{report}");

        let remaining = keys(&storage);
        assert!(
            !remaining.contains(&dirty_key("c3", 300)),
            "the base-tip dirty run is removed even with a clean tree"
        );
        assert!(
            remaining.contains(&dirty_key("c1", 150)),
            "an earlier base-side dirty run is left untouched"
        );
    }

    #[test]
    fn all_scope_with_include_blessings_removes_clean_dirty_and_blessings_for_a_commit() {
        let storage = MemoryStorage::new();
        store(&storage, &clean_key("f1"));
        store(&storage, &dirty_key("f1", 200));
        store_bless(&storage, &bless_key("f1", 50));
        // A second commit's data must survive a commit-scoped prune.
        store(&storage, &clean_key("f2"));
        store_bless(&storage, &bless_key("f2", 60));
        let git = feature_git();

        let opts = PruneOptions {
            clean: true,
            dirty: true,
            include_blessings: true,
            commit: vec!["f1".to_owned()],
            ..PruneOptions::default()
        };
        let report = prune_json(&storage, &git, &opts);
        let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
        assert_eq!(parsed["totals"]["runs"], 2, "clean + dirty: {report}");
        assert_eq!(parsed["totals"]["blessings"], 1, "{report}");

        let remaining = keys(&storage);
        assert!(!remaining.contains(&clean_key("f1")), "f1 clean removed");
        assert!(
            !remaining.contains(&dirty_key("f1", 200)),
            "f1 dirty removed"
        );
        assert!(
            !remaining.contains(&bless_key("f1", 50)),
            "f1 blessing removed"
        );
        assert!(remaining.contains(&clean_key("f2")), "f2 clean survives");
        assert!(
            remaining.contains(&bless_key("f2", 60)),
            "f2 blessing survives"
        );
    }

    #[test]
    fn pruning_runs_keeps_blessings_without_include_blessings() {
        let storage = MemoryStorage::new();
        store(&storage, &clean_key("f1"));
        store(&storage, &dirty_key("f1", 200));
        store_bless(&storage, &bless_key("f1", 50));
        let git = feature_git();

        // `--all` removes both runs but leaves the blessing: capturing or pruning a
        // run never removes a blessing, only `unbless` (or `--include-blessings`) does.
        let opts = PruneOptions {
            clean: true,
            dirty: true,
            commit: vec!["f1".to_owned()],
            ..PruneOptions::default()
        };
        let report = prune_json(&storage, &git, &opts);
        let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
        assert_eq!(parsed["totals"]["runs"], 2, "clean + dirty: {report}");
        assert_eq!(parsed["totals"]["blessings"], 0, "{report}");

        let remaining = keys(&storage);
        assert!(!remaining.contains(&clean_key("f1")), "f1 clean removed");
        assert!(
            !remaining.contains(&dirty_key("f1", 200)),
            "f1 dirty removed"
        );
        assert!(
            remaining.contains(&bless_key("f1", 50)),
            "the blessing survives a run prune"
        );
    }

    #[test]
    fn include_blessings_with_clean_scope_removes_clean_and_blessings_but_keeps_dirty() {
        let storage = MemoryStorage::new();
        store(&storage, &clean_key("f1"));
        store(&storage, &dirty_key("f1", 200));
        store_bless(&storage, &bless_key("f1", 50));
        let git = feature_git();

        let opts = PruneOptions {
            clean: true,
            include_blessings: true,
            commit: vec!["f1".to_owned()],
            ..PruneOptions::default()
        };
        prune(&storage, &git, &opts);

        let remaining = keys(&storage);
        assert!(!remaining.contains(&clean_key("f1")), "clean removed");
        assert!(
            !remaining.contains(&bless_key("f1", 50)),
            "blessing removed by --include-blessings"
        );
        assert!(
            remaining.contains(&dirty_key("f1", 200)),
            "--clean leaves dirty runs"
        );
    }

    #[test]
    fn include_blessings_removes_an_orphan_blessing_with_no_run() {
        let storage = MemoryStorage::new();
        // A pre-emptive blessing on an in-range commit that has no recorded run.
        store_bless(&storage, &bless_key("f1", 50));
        let git = feature_git();

        // `--include-blessings` alone (no run scope) removes the orphan blessing.
        let opts = PruneOptions {
            include_blessings: true,
            commit: vec!["f1".to_owned()],
            ..PruneOptions::default()
        };
        let report = prune_json(&storage, &git, &opts);
        let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
        assert_eq!(parsed["totals"]["runs"], 0, "no runs removed: {report}");
        assert_eq!(parsed["totals"]["blessings"], 1, "{report}");
        assert!(
            !keys(&storage).contains(&bless_key("f1", 50)),
            "orphan blessing removed"
        );
    }

    #[test]
    fn include_blessings_alone_leaves_runs_untouched() {
        let storage = MemoryStorage::new();
        store(&storage, &clean_key("f1"));
        store_bless(&storage, &bless_key("f1", 50));
        let git = feature_git();

        let opts = PruneOptions {
            include_blessings: true,
            commit: vec!["f1".to_owned()],
            ..PruneOptions::default()
        };
        prune(&storage, &git, &opts);

        let remaining = keys(&storage);
        assert!(
            remaining.contains(&clean_key("f1")),
            "the clean run is untouched"
        );
        assert!(
            !remaining.contains(&bless_key("f1", 50)),
            "the blessing is removed"
        );
    }

    #[test]
    fn prune_without_any_action_is_an_error() {
        let storage = MemoryStorage::new();
        store(&storage, &clean_key("f1"));
        let git = feature_git();

        let error = block_on(prune_with(
            &git,
            &storage,
            "folo",
            &config(),
            &PruneOptions::default(),
            &auto(),
            now(),
            &RecordingReporter::quiet(),
        ))
        .unwrap_err();
        assert!(error.find_source::<PruneSelectionRequiredError>().is_some());
    }

    #[test]
    fn include_blessings_leaves_a_blessing_whose_commit_is_off_history() {
        let storage = MemoryStorage::new();
        store(&storage, &clean_key("f1"));
        // A blessing sidecar on a commit that is not on the analyzed history; the
        // blessing pass skips it because the range only covers commits on history.
        store_bless(&storage, &bless_key("z9", 70));
        let git = feature_git();

        let opts = PruneOptions {
            clean: true,
            dirty: true,
            include_blessings: true,
            commit: vec!["f1".to_owned()],
            ..PruneOptions::default()
        };
        prune(&storage, &git, &opts);

        assert!(
            keys(&storage).contains(&bless_key("z9", 70)),
            "off-history blessing kept"
        );
    }

    #[test]
    fn prune_requires_a_scope() {
        let storage = MemoryStorage::new();
        store(&storage, &clean_key("f1"));
        let git = feature_git();

        let error = block_on(prune_with(
            &git,
            &storage,
            "folo",
            &config(),
            &PruneOptions::default(),
            &auto(),
            now(),
            &RecordingReporter::quiet(),
        ))
        .unwrap_err();
        assert!(error.find_source::<PruneSelectionRequiredError>().is_some());
        // Nothing was deleted.
        assert!(keys(&storage).contains(&clean_key("f1")));
    }

    #[test]
    fn prune_rejects_when_no_output_is_selected() {
        // Suppressing the text report without requesting a Markdown or JSON file
        // leaves nothing to produce, so the selection is rejected before any
        // pruning work — even with an otherwise-valid scope.
        let storage = MemoryStorage::new();
        store(&storage, &clean_key("f1"));
        let git = feature_git();

        let opts = PruneOptions {
            no_text: true,
            dirty: true,
            ..PruneOptions::default()
        };
        let error = block_on(prune_with(
            &git,
            &storage,
            "folo",
            &config(),
            &opts,
            &auto(),
            now(),
            &RecordingReporter::quiet(),
        ))
        .unwrap_err();
        assert!(error.find_source::<NoOutputSelectedError>().is_some());
        // Nothing was deleted.
        assert!(keys(&storage).contains(&clean_key("f1")));
    }

    #[test]
    fn all_scope_keeps_base_side_history_on_a_feature_branch() {
        let storage = MemoryStorage::new();
        for commit in ["c0", "c1", "f1", "f2"] {
            store(&storage, &clean_key(commit));
        }
        let git = feature_git();

        // `--all` (clean + dirty) on a feature branch deletes only the branch's own
        // commits; the base-branch ancestors (c0, c1) are preserved.
        let opts = PruneOptions {
            clean: true,
            dirty: true,
            ..PruneOptions::default()
        };
        prune(&storage, &git, &opts);

        let remaining = keys(&storage);
        assert!(
            remaining.contains(&clean_key("c0")),
            "base-side c0 survives"
        );
        assert!(
            remaining.contains(&clean_key("c1")),
            "base-side c1 survives"
        );
        assert!(!remaining.contains(&clean_key("f1")), "branch f1 removed");
        assert!(!remaining.contains(&clean_key("f2")), "branch f2 removed");
    }

    #[test]
    fn base_prune_requires_prune_base() {
        let storage = MemoryStorage::new();
        store(&storage, &clean_key("c3"));
        let git = linear_git(); // HEAD on master, which is the base branch.

        let opts = PruneOptions {
            clean: true,
            ..PruneOptions::default()
        };
        let error = block_on(prune_with(
            &git,
            &storage,
            "folo",
            &config(),
            &opts,
            &auto(),
            now(),
            &RecordingReporter::quiet(),
        ))
        .unwrap_err();
        let found = error
            .find_source::<PruneBaseConfirmationRequiredError>()
            .unwrap();
        assert_eq!(found.base_name, "master");
        // Nothing was deleted without confirmation.
        assert!(keys(&storage).contains(&clean_key("c3")));
    }

    #[test]
    fn prune_base_confirms_base_branch_deletion() {
        let storage = MemoryStorage::new();
        store(&storage, &clean_key("c3"));
        let git = linear_git();

        let opts = PruneOptions {
            clean: true,
            prune_base: true,
            ..PruneOptions::default()
        };
        prune(&storage, &git, &opts);
        assert!(
            !keys(&storage).contains(&clean_key("c3")),
            "--prune-base confirms deleting the base-branch run"
        );
    }

    #[test]
    fn dry_run_previews_without_deleting() {
        let storage = MemoryStorage::new();
        store(&storage, &clean_key("c0"));
        store(&storage, &clean_key("c1"));
        store(&storage, &clean_key("f1"));
        store(&storage, &dirty_key("f1", 200));
        let git = feature_git();

        let before = keys(&storage);
        let opts = PruneOptions {
            dry_run: true,
            ..dirty_options()
        };
        let report = prune_json(&storage, &git, &opts);
        let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
        assert_eq!(parsed["dry_run"], true);
        assert_eq!(parsed["totals"]["runs"], 1, "{report}");

        // Nothing was actually deleted.
        assert_eq!(keys(&storage), before, "--dry-run deletes nothing");
    }

    #[test]
    fn text_format_reports_would_remove_under_dry_run() {
        let storage = MemoryStorage::new();
        store(&storage, &clean_key("c0"));
        store(&storage, &clean_key("f1"));
        store(&storage, &dirty_key("f1", 200));
        let git = feature_git();

        let opts = PruneOptions {
            dry_run: true,
            ..dirty_options()
        };
        let report = prune(&storage, &git, &opts);
        assert!(report.contains("Prune plan for project folo"), "{report}");
        assert!(report.contains("Would remove 1 run"), "{report}");
        assert!(report.contains("1 discriminant set"), "{report}");
    }

    #[test]
    fn dirty_scope_never_touches_clean_runs() {
        let storage = MemoryStorage::new();
        for commit in ["c0", "c1", "f1", "f2"] {
            store(&storage, &clean_key(commit));
        }
        let git = feature_git();

        let report = prune(&storage, &git, &dirty_options());
        assert!(report.contains("No run matches the selection."), "{report}");
        // Every clean run survives.
        assert_eq!(keys(&storage).len(), 4, "clean runs are untouched");
    }

    #[test]
    fn prune_rejects_an_unresolved_head() {
        let storage = MemoryStorage::new();
        store(&storage, &dirty_key("c0", 100));
        let git = FakeGitHistory::new(); // No commits: HEAD does not resolve.
        let error = block_on(prune_with(
            &git,
            &storage,
            "folo",
            &config(),
            &dirty_options(),
            &auto(),
            now(),
            &RecordingReporter::quiet(),
        ))
        .unwrap_err();
        let found = error.find_source::<UnresolvedRefError>().unwrap();
        assert_eq!(found.reference, "HEAD");
    }

    #[test]
    fn engine_discriminant_restricts_removal() {
        let storage = MemoryStorage::new();
        store(&storage, &clean_key("f1"));
        store(&storage, &dirty_key("f1", 200));
        // A criterion dirty run on the same commit must survive an engine-scoped
        // callgrind prune.
        let criterion_dirty =
            "v1/folo/objects/criterion/x86_64-unknown-linux-gnu/m1/f1/dirty-200.json";
        store(&storage, criterion_dirty);
        let git = feature_git();

        let opts = PruneOptions {
            engine: vec!["callgrind".to_owned()],
            ..dirty_options()
        };
        prune(&storage, &git, &opts);

        let remaining = keys(&storage);
        assert!(
            !remaining.contains(&dirty_key("f1", 200)),
            "callgrind dirty removed"
        );
        assert!(
            remaining.contains(&criterion_dirty.to_owned()),
            "criterion dirty survives the engine-scoped prune"
        );
    }

    #[test]
    fn since_only_removes_commits_on_or_after_the_cutoff() {
        let storage = MemoryStorage::new();
        // One dirty snapshot per dated feature commit. The window is decided from
        // each commit's committer date in the topology, not from the stored object,
        // so the `dirty-<unix>` key second is irrelevant to the cutoff.
        store(&storage, &dirty_key("f1", 10));
        store(&storage, &dirty_key("f2", 20));
        store(&storage, &dirty_key("f3", 30));
        let git = dated_feature_git();

        let opts = PruneOptions {
            since: Some("1970-01-01T00:03:00Z".to_owned()), // 180s
            ..dirty_options()
        };
        prune(&storage, &git, &opts);

        let remaining = keys(&storage);
        assert!(
            remaining.contains(&dirty_key("f1", 10)),
            "f1 (committed at 100s) predates --since and is kept"
        );
        assert!(
            !remaining.contains(&dirty_key("f2", 20)),
            "f2 (committed exactly at the 180s cutoff) is removed (inclusive)"
        );
        assert!(
            !remaining.contains(&dirty_key("f3", 30)),
            "f3 (committed at 300s) is after --since and removed"
        );
    }

    #[test]
    fn include_blessings_respects_the_since_cutoff() {
        let storage = MemoryStorage::new();
        // Orphan blessings on dated feature commits: f1 (100s) predates the 180s
        // cutoff and is kept; f3 (300s) is on or after it and removed.
        store_bless(&storage, &bless_key("f1", 10));
        store_bless(&storage, &bless_key("f3", 30));
        let git = dated_feature_git();

        let opts = PruneOptions {
            include_blessings: true,
            since: Some("1970-01-01T00:03:00Z".to_owned()), // 180s
            ..PruneOptions::default()
        };
        prune(&storage, &git, &opts);

        let remaining = keys(&storage);
        assert!(
            remaining.contains(&bless_key("f1", 10)),
            "f1's blessing (committed at 100s) predates --since and is kept"
        );
        assert!(
            !remaining.contains(&bless_key("f3", 30)),
            "f3's blessing (committed at 300s) is on or after --since and removed"
        );
    }

    #[test]
    fn commit_filter_restricts_removal_to_the_named_commit() {
        let storage = MemoryStorage::new();
        store(&storage, &dirty_key("f1", 200));
        store(&storage, &dirty_key("f2", 300));
        let git = feature_git();

        let opts = PruneOptions {
            commit: vec!["f2".to_owned()],
            ..dirty_options()
        };
        prune(&storage, &git, &opts);

        let remaining = keys(&storage);
        assert!(
            remaining.contains(&dirty_key("f1", 200)),
            "an unselected commit's run survives"
        );
        assert!(
            !remaining.contains(&dirty_key("f2", 300)),
            "the selected commit's run is removed"
        );
    }

    #[test]
    fn commits_are_listed_oldest_first_by_topology() {
        let storage = MemoryStorage::new();
        store(&storage, &dirty_key("f2", 300));
        store(&storage, &dirty_key("f1", 200));
        let git = feature_git();

        let opts = PruneOptions {
            dry_run: true,
            ..dirty_options()
        };
        let report = prune_json(&storage, &git, &opts);
        let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
        let commits = parsed["sets"][0]["commits"].as_array().unwrap();
        assert_eq!(commits[0]["commit"], "f1", "oldest first: {report}");
        assert_eq!(commits[1]["commit"], "f2");
    }

    #[test]
    fn markdown_format_renders_a_table_per_set() {
        let storage = MemoryStorage::new();
        store(&storage, &dirty_key("f1", 200));
        store(&storage, &dirty_key("f2", 300));
        let git = feature_git();

        let opts = PruneOptions {
            dry_run: true,
            ..dirty_options()
        };
        let report = prune_markdown(&storage, &git, &opts);
        assert!(report.contains("# Prune plan for"), "{report}");
        assert!(report.contains("| Commit | Runs | Blessings |"), "{report}");
        assert!(report.contains("| f1 |"), "{report}");
        assert!(report.contains("**Would remove"), "{report}");
    }

    #[test]
    fn markdown_format_reports_no_match_for_an_empty_plan() {
        let storage = MemoryStorage::new();
        for commit in ["c0", "c1", "f1", "f2"] {
            store(&storage, &clean_key(commit));
        }
        let git = feature_git();

        let report = prune_markdown(&storage, &git, &dirty_options());
        assert!(report.contains("# Prune plan for"), "{report}");
        assert!(report.contains("No run matches the selection."), "{report}");
    }

    #[test]
    fn commit_is_eligible_excludes_base_side_commits_on_a_feature_branch() {
        // A feature branch merged off the base at index 1: only commits after the
        // merge-base (indices 2, 3) are the branch's own and eligible for removal.
        assert!(!commit_is_eligible(0, Some(1), false), "base-side c0");
        assert!(
            !commit_is_eligible(1, Some(1), false),
            "the merge-base itself"
        );
        assert!(commit_is_eligible(2, Some(1), false), "branch-side f1");
        assert!(commit_is_eligible(3, Some(1), false), "branch-side f2");
    }

    #[test]
    fn commit_is_eligible_admits_every_commit_on_the_base_branch() {
        // On the base branch (tip is its own merge-base), every commit is eligible.
        assert!(commit_is_eligible(0, Some(3), true));
        assert!(commit_is_eligible(3, Some(3), true));
    }

    #[test]
    fn commit_is_eligible_preserves_everything_without_a_fork_point() {
        // A degenerate topology with no shared first-parent commit yields no fork point.
        // Nothing is treated as branch-side, so no commit is deletable without the
        // explicit base-branch opt-in — a `None` fork point never authorises deletion.
        assert!(!commit_is_eligible(0, None, false));
        assert!(!commit_is_eligible(5, None, false));
    }

    #[test]
    fn blessing_suffix_is_empty_only_when_no_blessings_are_removed() {
        assert_eq!(blessing_suffix(0), "");
        assert_eq!(blessing_suffix(1), " (plus 1 blessing)");
        assert_eq!(blessing_suffix(3), " (plus 3 blessings)");
    }

    #[test]
    fn build_plan_counts_runs_and_blessings_separately() {
        let set = DiscriminantSet {
            engine: Engine::Callgrind,
            target_triple: "x86_64-unknown-linux-gnu".into(),
            machine_key: "m1".into(),
        };
        // Two clean runs (c0, c1) plus one blessing on c0: an asymmetric mix so a
        // run/blessing miscount diverges from the truth.
        let items = vec![
            RemovalItem {
                index: 0,
                set: set.clone(),
                commit: "c0".to_owned(),
                key: clean_key("c0"),
                kind: RunKind::Clean,
            },
            RemovalItem {
                index: 1,
                set: set.clone(),
                commit: "c1".to_owned(),
                key: clean_key("c1"),
                kind: RunKind::Clean,
            },
            RemovalItem {
                index: 0,
                set,
                commit: "c0".to_owned(),
                key: bless_key("c0", 10),
                kind: RunKind::Bless,
            },
        ];

        let plan = build_plan("folo", "master", &items);
        assert_eq!(plan.total_runs, 2, "two clean runs");
        assert_eq!(plan.total_blessings, 1, "one blessing sidecar");
        assert_eq!(plan.sets.len(), 1);
        assert_eq!(plan.sets[0].runs, 2);
        assert_eq!(plan.sets[0].blessings, 1);

        let commits = &plan.sets[0].commits;
        assert_eq!(commits.len(), 2, "oldest-first by first-parent index");
        assert_eq!(commits[0].commit, "c0");
        assert_eq!(commits[0].runs, 1, "c0 has one clean run");
        assert_eq!(commits[0].blessings, 1, "c0 has one blessing");
        assert_eq!(commits[1].commit, "c1");
        assert_eq!(commits[1].runs, 1);
        assert_eq!(commits[1].blessings, 0, "c1 has no blessing");
    }
}
