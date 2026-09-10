//! Resolving the git topology a `Selection` targets: the target ref's first-parent
//! ancestry, its split at the base-branch merge-base, and the dirty-tip admission policy.

use std::collections::HashMap;
use std::time::Instant;

use cbh_config::Config;
use cbh_detect::select_commits;
use cbh_diag::{Reporter, ReporterExt, count_noun};
use cbh_git::GitHistory;
use jiff::Timestamp;

use super::selection::Selection;
use crate::{
    AnalyzeError, BaseBranchUnavailableError, DefaultBranchProbeFailedError,
    FirstParentWalkFailedError, MergeBaseFailedError, MergeBaseUnavailableError,
    ResolveRefFailedError, UnresolvedRefError, WorkingTreeProbeFailedError,
};

/// How the base-branch dirty-tip exception is gated.
///
/// On a feature branch the target-side commits admit dirty runs unconditionally;
/// this policy only governs the *base* branch's tip. `analyze`/`list` admit a
/// base-tip dirty run only when the working tree is currently dirty (the
/// "evaluating the tool / accidentally on the base branch" case); `prune` admits
/// base-tip dirty runs regardless of the current working-tree state.
#[derive(Clone, Copy)]
pub(crate) enum DirtyTipPolicy {
    /// Admit a base-side tip's dirty runs only when the working tree is dirty now.
    WhenWorkingTreeDirty,
    /// Always treat a base-side tip as admitting dirty runs.
    Always,
}

/// The git topology a selection resolves to: the target ref it was resolved
/// against, the first-parent position of each selected commit, and the per-commit
/// dirty-admission flags. All maps use owned commit IDs so the borrowed
/// `selected` set can drop before the caller's load loop.
pub(crate) struct ResolvedHistory {
    /// The target ref the timeline was resolved against (for diagnostics).
    pub(crate) target_ref: String,
    /// The display name of the base ref the target's history was split against
    /// (an explicit `--base`, the configured default branch, or the detected one),
    /// for the effective-selection summary. Always present: `resolve_history`
    /// refuses to build a `ResolvedHistory` when no base can be resolved.
    pub(crate) base_name: String,
    /// The full commit ID the base ref resolved to.
    pub(crate) base_commit: String,
    /// The full commit ID the target ref resolved to — the analyzed context commit,
    /// carried into the report so it names the exact commit the findings describe.
    pub(crate) tip_commit: String,
    /// Whether the working tree carried uncommitted changes when the topology was
    /// resolved. Probed only under [`DirtyTipPolicy::WhenWorkingTreeDirty`] (and
    /// not under `--no-dirty`); `false` otherwise. The report annotates the context
    /// commit `+ uncommitted changes` when set.
    pub(crate) tip_dirty: bool,
    /// First-parent position of each selected commit, for series ordering. An
    /// object whose commit is absent is outside the analyzed history.
    pub(crate) order: HashMap<String, usize>,
    /// The selected commits in first-parent order, oldest first — the reverse of
    /// [`order`](Self::order), so `ordered_commits[order[c]] == c`. It lets a
    /// consumer name the commit at a topological index, including one that carries
    /// no data; only `examine` reads it.
    pub(crate) ordered_commits: Vec<String>,
    /// Committer timestamp of each first-parent commit, for deciding the
    /// `--since` cutoff from topology before any object is fetched. A
    /// commit absent here has an unknown time and is treated as in-window.
    pub(crate) commit_times: HashMap<String, Timestamp>,
    /// Subject line of each first-parent commit that has one, for labeling
    /// `examine`'s per-commit data points. A commit absent here has an empty
    /// subject; only `examine` reads this.
    pub(crate) commit_subjects: HashMap<String, String>,
    /// Whether each selected commit admits dirty (uncommitted-tree) snapshots.
    pub(crate) admit_dirty: HashMap<String, bool>,
    /// Whether a commit's dirty runs are admitted *only* by the base-branch
    /// dirty-tree exception, which triggers the ephemeral-data warning.
    pub(crate) dirty_base_exception: HashMap<String, bool>,
    /// First-parent topological index of the **fork point** on the context line: the
    /// newest context commit that is an ancestor of the base ref, dividing base-side
    /// history from the branch's own commits. When the branch was rebased onto the
    /// base, this is the merge-base itself. When the base was instead merged into the
    /// branch, the merge-base sits off the context's first-parent line, so this is the
    /// newest commit the two lines still shared before the branch diverged. `None` only
    /// when the two lines share no first-parent commit at all, which cannot arise once a
    /// merge-base exists.
    pub(crate) merge_base_index: Option<usize>,
    /// Whether the target's tip *is* its own merge-base with the base: the signal
    /// that this is an official base-branch view rather than a feature branch.
    pub(crate) tip_is_merge_base: bool,
}

/// Resolves the git topology for a selection: the target ref's first-parent
/// ancestry, the merge-base with the base ref, and the per-commit dirty-admission
/// flags. Requires a repository — an unresolvable target ref is an error rather
/// than an empty success, and so is a merge-base that cannot be determined (the
/// base ref does not resolve, or it shares no common ancestor with the target),
/// rather than silently falling back to a base-branch (history) view of the
/// incomplete topology.
pub(crate) async fn resolve_history<G>(
    git: &G,
    config: &Config,
    selection: &Selection<'_>,
    policy: DirtyTipPolicy,
    reporter: &dyn Reporter,
) -> Result<ResolvedHistory, AnalyzeError>
where
    G: GitHistory,
{
    // The topology comes from git history, not from stored timestamps. The git port
    // cannot distinguish a missing ref from a path that is not a repository, so the
    // error must describe only the unresolved ref.
    let target_ref = selection.context.unwrap_or("HEAD");
    let Some(target_commit_id) = git
        .resolve(target_ref)
        .await
        .map_err(|error| ResolveRefFailedError::caused_by(target_ref, error))?
    else {
        return Err(UnresolvedRefError::new(
            "resolving history",
            target_ref,
            "Check that the ref exists in the selected repository, and fetch it if it is absent. \
             Select a different repository with --repo, or select a different target ref with \
             --context.",
        )
        .into());
    };

    // A base branch is required: mode detection, branch comparisons, and base-branch
    // dirty-run admission all need a resolved base ref. Refuse before walking the
    // ancestry rather than carrying an unresolved base through. The usual cause is a
    // shallow clone or a checkout that never fetched the base branch.
    let Some(ResolvedBase {
        name: base_name,
        commit: base_commit_id,
    }) = resolve_base(git, config, selection.base).await?
    else {
        return Err(BaseBranchUnavailableError::new(target_ref).into());
    };
    let first_parent_started = Instant::now();
    let first_parent = git
        .first_parent(&target_commit_id)
        .await
        .map_err(|error| FirstParentWalkFailedError::caused_by(&target_commit_id, error))?;
    reporter.timing(
        "git.first_parent ancestry walk (target's first-parent line)",
        first_parent_started.elapsed(),
    );
    // Split the first-parent ancestry into the commit ID timeline (for commit selection
    // and the merge-base lookup) and a commit ID -> committer-time map (for the window).
    let commit_count = first_parent.len();
    let mut ancestry: Vec<String> = Vec::with_capacity(commit_count);
    let mut commit_times: HashMap<String, Timestamp> = HashMap::new();
    let mut commit_subjects: HashMap<String, String> = HashMap::new();
    for commit in first_parent {
        if let Some(time) = commit.committer_time {
            commit_times.insert(commit.commit_id.clone(), time);
        }
        if !commit.subject.is_empty() {
            commit_subjects.insert(commit.commit_id.clone(), commit.subject);
        }
        ancestry.push(commit.commit_id);
    }
    let merge_base = git
        .merge_base(&target_commit_id, &base_commit_id)
        .await
        .map_err(|error| {
            MergeBaseFailedError::caused_by(&target_commit_id, &base_commit_id, error)
        })?;

    reporter.if_enabled(|notes| {
        notes.note(&format!(
            "target ref {target_ref} resolves to {target_commit_id}; {} on its first-parent line",
            count_noun(commit_count, "commit")
        ));
        notes.note(&format!(
            "base ref {base_name} resolves to {base_commit_id}; merge-base with target is {}",
            merge_base.as_deref().unwrap_or("<none>")
        ));
    });

    // The target and base must share history. A merge-base we cannot determine leaves
    // no reliable branch relationship, and guessing a mode from the incomplete history
    // would silently mislead — so refuse and say how to supply the missing history.
    // The base resolved (checked above), so the only remaining cause is a shallow
    // clone whose depth stops short of the branch point, or a checkout that never
    // fetched the base branch.
    let Some(merge_base) = merge_base else {
        // The base resolved, but shares no common ancestor with the target. By far
        // the usual cause is a shallow clone whose depth stops short of the branch
        // point, so the primary remedy is to deepen the clone — not to pick a
        // different base. Only once the history is known-complete is a genuinely
        // disjoint base worth calling out, and even then the deliberate `--base` a
        // user passed is theirs to reconsider, not ours to second-guess.
        let remedy = match selection.base {
            Some(explicit) => format!(
                " If the history is already complete, {explicit} is genuinely unrelated \
                 to the target and cannot serve as its base."
            ),
            None => " If the history is already complete, the base branch is unrelated to \
                     the target; name the intended base with --base or \
                     project.default_branch."
                .to_owned(),
        };
        return Err(MergeBaseUnavailableError::new(
            target_ref,
            &target_commit_id,
            &base_commit_id,
            remedy,
        )
        .into());
    };

    // The base-branch dirty-tip exception: `analyze`/`list` admit a base-side tip's
    // dirty runs only when the working tree is currently dirty (`--no-dirty` skips
    // both the probe and the exception); `prune` admits them unconditionally so it
    // can remove them regardless of the present working-tree state. The probe result
    // is reused for the report's tip annotation, so `analyze` never runs it twice.
    let working_tree_dirty = match policy {
        DirtyTipPolicy::WhenWorkingTreeDirty if !selection.no_dirty => git
            .is_dirty()
            .await
            .map_err(WorkingTreeProbeFailedError::caused_by)?,
        _ => false,
    };
    let dirty_tip_exception = match policy {
        DirtyTipPolicy::Always => !selection.no_dirty,
        DirtyTipPolicy::WhenWorkingTreeDirty => {
            if working_tree_dirty {
                reporter.note_with(|| {
                    "working tree is dirty: dirty snapshots on a base-side tip will be admitted"
                        .to_owned()
                });
            }
            working_tree_dirty
        }
    };

    // The context-line consumers (list, prune, examine) and the base/branch dirty-run
    // split both divide the first-parent line at its fork point with the base ref: the
    // newest context commit that is an ancestor of the base ref. When the branch was
    // rebased onto the base, that fork point is the merge-base itself and lies on the
    // line. When the base was instead merged into the branch, the merge-base sits off the
    // line, so the fork point is the newest commit the line still shares with the base
    // ref's history — found by projecting onto the line rather than left absent, so a base
    // merged in is split just like a rebased one. Ref: book appendix "A base merged into
    // the branch is supported".
    let split_commit = if ancestry.contains(&merge_base) {
        Some(merge_base.clone())
    } else {
        base_ref_fork_point(git, &ancestry, &base_commit_id, reporter).await?
    };

    let selected = select_commits(
        &ancestry,
        split_commit.as_deref(),
        !selection.no_dirty,
        dirty_tip_exception,
    );
    let order: HashMap<String, usize> = selected
        .iter()
        .enumerate()
        .map(|(index, one)| (one.commit.clone(), index))
        .collect();
    let admit_dirty: HashMap<String, bool> = selected
        .iter()
        .map(|one| (one.commit.clone(), one.dirty.admits_dirty()))
        .collect();
    let dirty_base_exception: HashMap<String, bool> = selected
        .iter()
        .map(|one| (one.commit.clone(), one.dirty.is_base_exception()))
        .collect();

    let merge_base_index = split_commit
        .as_ref()
        .and_then(|commit| order.get(commit).copied());
    // The target's tip is its own merge-base exactly when this is an official
    // base-branch view rather than a feature branch.
    let tip_is_merge_base = merge_base == target_commit_id;

    Ok(ResolvedHistory {
        target_ref: target_ref.to_owned(),
        base_name,
        base_commit: base_commit_id,
        tip_commit: target_commit_id,
        tip_dirty: working_tree_dirty,
        order,
        ordered_commits: ancestry,
        commit_times,
        commit_subjects,
        admit_dirty,
        dirty_base_exception,
        merge_base_index,
        tip_is_merge_base,
    })
}

/// The fork point on the context's first-parent line when the merge-base lies off it.
///
/// Reached only when the merge-base is not on the context's first-parent line, which
/// happens when the base ref was merged into the branch rather than the branch rebased
/// onto the base. The fork point is the newest line commit that is an ancestor of the
/// base ref — the last commit the branch shared with the base before diverging.
///
/// The line is oldest-first and ancestry is downward-closed: if a commit is an ancestor
/// of the base ref, so is every commit before it, so the base-side commits form a prefix.
/// The prefix boundary is found by binary search — `O(log n)` merge-base probes rather
/// than one per commit. `Some(commit)` is the newest base-side commit; `None` only when
/// the line shares nothing with the base ref, which cannot arise once a merge-base exists
/// (the shared root is always base-side), so a `None` degrades to preserving the whole
/// line as base-side rather than misclassifying it.
async fn base_ref_fork_point<G>(
    git: &G,
    ancestry: &[String],
    base_commit_id: &str,
    reporter: &dyn Reporter,
) -> Result<Option<String>, AnalyzeError>
where
    G: GitHistory,
{
    let started = Instant::now();
    // Binary search for the first branch-own commit: the oldest line commit that is
    // *not* an ancestor of the base ref. Every commit before it is base-side. The loop
    // is bounded by the commit count — a strict over-estimate of the `log2` probes a
    // halving search needs — so a comparison slip can only end it early, never spin it.
    let mut low = 0_usize;
    let mut high = ancestry.len();
    for _ in 0..=ancestry.len() {
        if low >= high {
            break;
        }
        let mid = low.midpoint(high);
        let Some(candidate) = ancestry.get(mid) else {
            break;
        };
        if is_ancestor_of(git, candidate, base_commit_id).await? {
            low = mid.saturating_add(1);
        } else {
            high = mid;
        }
    }
    let fork_point = low
        .checked_sub(1)
        .and_then(|index| ancestry.get(index).cloned());
    reporter.timing(
        "base-ref fork-point binary search (merge-base probes for a merged-in base)",
        started.elapsed(),
    );
    reporter.note_with(|| match &fork_point {
        Some(commit) => format!(
            "merge-base is off the context's first-parent line (the base ref was merged into \
             the branch); the fork point is {commit}, so commits after it are the branch's own"
        ),
        None => "merge-base is off the context's first-parent line and the line shares nothing \
             with the base ref, so the whole line is preserved as base-side"
            .to_owned(),
    });
    Ok(fork_point)
}

/// Whether `commit` is an ancestor of `descendant` (or the same commit): true exactly
/// when their merge-base is `commit` itself.
async fn is_ancestor_of<G>(git: &G, commit: &str, descendant: &str) -> Result<bool, AnalyzeError>
where
    G: GitHistory,
{
    let merge_base = git
        .merge_base(commit, descendant)
        .await
        .map_err(|error| MergeBaseFailedError::caused_by(commit, descendant, error))?;
    Ok(merge_base.as_deref() == Some(commit))
}

/// The ephemeral-data warning appended when a dirty base-branch-tip run is admitted.
pub(crate) fn dirty_base_exception_warning() -> String {
    "Warning: analysis included dirty runs (with uncommitted changes) on top of the \
     base branch. These may be excluded from future analysis. Switch to a new branch \
     to persist benchmark history of your changes."
        .to_owned()
}

/// The base ref a target's history is split against, resolved to both its display
/// name and its commit ID in a single pass so a diagnostic can name the branch
/// while the analysis compares against the commit.
pub(crate) struct ResolvedBase {
    /// The base ref's display name (an explicit `--base`, the configured
    /// `project.default_branch`, or the repository's detected default branch).
    pub(crate) name: String,
    /// The commit ID the base ref resolved to.
    pub(crate) commit: String,
}

/// Resolves the base ref the target's history is split against, returning both its
/// display name and commit ID, or `None` when no base can be determined.
///
/// Precedence: an explicit `--base` (an error if it does not resolve), then the
/// configured `project.default_branch`, then the repository's detected default
/// branch (`origin/HEAD`, else `main`/`master`). A candidate that names a branch
/// which does not resolve to a commit falls through to the next source, so the
/// returned name always pairs with a real commit.
pub(crate) async fn resolve_base<G: GitHistory>(
    git: &G,
    config: &Config,
    base: Option<&str>,
) -> Result<Option<ResolvedBase>, AnalyzeError> {
    if let Some(base) = base {
        let Some(commit) = git
            .resolve(base)
            .await
            .map_err(|error| ResolveRefFailedError::caused_by(base, error))?
        else {
            return Err(UnresolvedRefError::new(
                "resolving the comparison base",
                base,
                "Check that the --base ref exists or is fetched.",
            )
            .into());
        };
        return Ok(Some(ResolvedBase {
            name: base.to_owned(),
            commit,
        }));
    }
    if let Some(default) = config.project.default_branch.as_deref()
        && let Some(commit) = git
            .resolve(default)
            .await
            .map_err(|error| ResolveRefFailedError::caused_by(default, error))?
    {
        return Ok(Some(ResolvedBase {
            name: default.to_owned(),
            commit,
        }));
    }
    if let Some(name) = git
        .default_branch()
        .await
        .map_err(DefaultBranchProbeFailedError::caused_by)?
        && let Some(commit) = git
            .resolve(&name)
            .await
            .map_err(|error| ResolveRefFailedError::caused_by(&name, error))?
    {
        return Ok(Some(ResolvedBase { name, commit }));
    }
    Ok(None)
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use cbh_diag::RecordingReporter;
    use cbh_git::FakeGitHistory;
    use futures::executor::block_on;
    use ohno::ErrorExt as _;

    use super::*;

    /// A two-commit history: `HEAD` is `c1`, whose base `master` is its parent `c0`.
    fn git() -> FakeGitHistory {
        let mut git = FakeGitHistory::new();
        git.commit("c0", None)
            .commit("c1", Some("c0"))
            .branch("master", "c0")
            .head("c1");
        git
    }

    fn selection(base: Option<&'static str>) -> Selection<'static> {
        Selection {
            context: None,
            base,
            no_dirty: false,
            since: None,
            engine: &[],
            target_triple: &[],
            machine_key: &[],
        }
    }

    /// Resolves the topology of `git` against `master`, expecting it to fail.
    fn resolve_error(git: &FakeGitHistory) -> AnalyzeError {
        resolve_error_for(git, &selection(Some("master")))
    }

    fn resolve_error_for(git: &FakeGitHistory, selection: &Selection<'_>) -> AnalyzeError {
        // `ResolvedHistory` is not `Debug`, so `unwrap_err` is unavailable here.
        block_on(resolve_history(
            git,
            &Config::default(),
            selection,
            DirtyTipPolicy::WhenWorkingTreeDirty,
            &RecordingReporter::quiet(),
        ))
        .err()
        .unwrap()
    }

    #[test]
    fn a_failed_ref_resolution_names_that_query() {
        let mut git = git();
        git.fail_resolve();

        let error = resolve_error(&git);
        assert!(error.find_source::<ResolveRefFailedError>().is_some());
    }

    #[test]
    fn a_failed_default_branch_probe_names_that_query() {
        // With no --base and no configured default branch, the base comes from the
        // repository's advertised default branch.
        let mut git = git();
        git.fail_default_branch();

        let error = resolve_error_for(&git, &selection(None));
        assert!(
            error
                .find_source::<DefaultBranchProbeFailedError>()
                .is_some()
        );
    }

    #[test]
    fn a_failed_ancestry_walk_names_that_query() {
        // The ancestry walk and the merge-base lookup sit next to each other on this
        // path, so each must name itself rather than the other.
        let mut git = git();
        git.fail_first_parent();

        let error = resolve_error(&git);
        assert!(error.find_source::<FirstParentWalkFailedError>().is_some());
    }

    #[test]
    fn a_failed_merge_base_lookup_names_that_query() {
        let mut git = git();
        git.fail_merge_base();

        let error = resolve_error(&git);
        assert!(error.find_source::<MergeBaseFailedError>().is_some());
    }

    #[test]
    fn a_failed_working_tree_probe_names_that_query() {
        let mut git = git();
        git.fail_is_dirty();

        let error = resolve_error(&git);
        assert!(error.find_source::<WorkingTreeProbeFailedError>().is_some());
    }

    /// A four-commit graph where the base ref (`c2`) forks off `c1`, alongside a
    /// feature line `root - c1 - f1 - f2`. The merge-base of the feature tip and `c2`
    /// is `c1`, which is off the feature's first-parent line only once a real merge is
    /// involved; the fake still lets the fork-point helpers be exercised directly.
    fn forked_git() -> FakeGitHistory {
        let mut git = FakeGitHistory::new();
        git.commit("root", None)
            .commit("c1", Some("root"))
            .commit("c2", Some("c1"))
            .commit("f1", Some("c1"))
            .commit("f2", Some("f1"));
        git
    }

    #[test]
    fn is_ancestor_of_holds_only_up_the_chain() {
        let git = forked_git();
        // `c1` and `root` are ancestors of `c2`; `f1` (a sibling of `c2`) and a
        // descendant relation (`c2` under `c1`) are not.
        assert!(block_on(is_ancestor_of(&git, "c1", "c2")).unwrap());
        assert!(block_on(is_ancestor_of(&git, "root", "c2")).unwrap());
        assert!(!block_on(is_ancestor_of(&git, "f1", "c2")).unwrap());
        assert!(!block_on(is_ancestor_of(&git, "c2", "c1")).unwrap());
    }

    #[test]
    fn base_ref_fork_point_is_the_newest_shared_commit() {
        let git = forked_git();
        let ancestry: Vec<String> = ["root", "c1", "f1", "f2"]
            .iter()
            .map(|commit| (*commit).to_owned())
            .collect();
        let fork = block_on(base_ref_fork_point(
            &git,
            &ancestry,
            "c2",
            &RecordingReporter::quiet(),
        ))
        .unwrap();
        assert_eq!(fork.as_deref(), Some("c1"));
    }

    #[test]
    fn base_ref_fork_point_without_shared_history_is_none() {
        // The line shares no commit with the base ref, so no fork point can be located.
        let mut git = FakeGitHistory::new();
        git.commit("x1", None).commit("x2", Some("x1"));
        let ancestry: Vec<String> = ["x1", "x2"]
            .iter()
            .map(|commit| (*commit).to_owned())
            .collect();
        let fork = block_on(base_ref_fork_point(
            &git,
            &ancestry,
            "root",
            &RecordingReporter::quiet(),
        ))
        .unwrap();
        assert_eq!(fork, None);
    }
}
