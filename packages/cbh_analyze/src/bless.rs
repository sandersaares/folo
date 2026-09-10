//! The `bless` / `unbless` commands: manually accept (or revoke acceptance of) a
//! benchmark's level on the base branch, so history analysis stops re-flagging an
//! intentional change.
//!
//! `bless` writes an append-only `BlessingRecord` sidecar for the context commit
//! (`HEAD` by default, or `--context <ref>`). Any commit that *resolves* can be
//! blessed; the hard errors are an unresolvable ref, no benchmark prefixes (and no
//! `--all`), an undeterminable base branch, and — only when the commit has no stored
//! run — an unconstrained target triple or machine key (nothing to synthesize a set
//! from). Two conditions warn and proceed rather than refuse, so the command never
//! refuses without cause:
//!
//! * **Off the base branch** — a blessing only takes effect once the commit joins
//!   the base branch's first-parent history (for example after a fast-forward), so
//!   this warns and proceeds rather than refusing.
//! * **No stored result at the commit** — a blessing may be recorded *before* data
//!   is captured. With a run present, the sidecar lands in every set selected by
//!   discriminant filters that has a stored result there. With no run present,
//!   the target sets are synthesized from the resolved discriminant filters (all
//!   four engines when `--engine` is omitted, under the resolved target triple and
//!   machine key), so whichever engine's data is captured later at that commit is
//!   accepted. This warns, because a typo'd commit id is the likelier cause.
//!
//! When blessing `HEAD`, a dirty working tree is allowed — the blessing applies to
//! the committed `clean.json` recorded at `HEAD`, which the local edits do not
//! change — but it emits a warning. `unbless` deletes every blessing recorded at
//! the context commit in the selected sets; sidecars are immutable, so narrowing a
//! blessing means unblessing and re-blessing the subset to keep. Blessings issued
//! at later commits are unaffected, so the timeline can stay blessed past the
//! context commit.

use std::collections::HashSet;
use std::path::Path;

use cbh_command::{BlessOptions, UnblessOptions};
use cbh_config::{
    Config, load_config, resolve_config_path, resolve_local_path, resolve_project_id, resolve_repo,
    storage_env,
};
use cbh_detect::{DiscriminantFilter, DiscriminantSetQuery};
use cbh_diag::{Reporter, ReporterExt, StderrReporter, count_noun};
use cbh_git::{GitHistory, SystemGitHistory};
use cbh_model::{BlessingRecord, DiscriminantSet, Engine, MachineKey, StorageKey, TargetTriple};
use cbh_storage::{Storage, build_storage, finish_with_flush};
use jiff::Timestamp;
use tick::Clock;

use super::announce::{
    AnnouncedBase, AnnouncedContext, announce_selection, selection_announcement,
};
use super::history::resolve_base;
use super::{
    AutoDiscriminants, Selection, discriminant_filtered_candidates, resolve_auto_discriminants,
    resolve_discriminants, resolve_now,
};
use crate::{
    AnalyzeError, BlessBaseRequiredError, BlessDiscriminantsRequiredError,
    BlessSelectionRequiredError, FirstParentWalkFailedError, ResolveRefFailedError,
    UnresolvedRefError, WorkingTreeProbeFailedError,
};

/// The real `bless`: load configuration, wire the configured storage and git
/// history, and orchestrate.
///
/// `clock_override` injects the [`tick::Clock`] that stamps each blessing's issue
/// time: `None` reads the runtime wall clock (`Clock::new_tokio`) in production,
/// while tests inject a frozen clock (`Clock::new_frozen_at`) so the recorded time
/// is deterministic.
// Thin real-adapter wiring: loads config from disk, builds the configured storage,
// and shells out via `SystemGitHistory`/`detect_auto_discriminants` before delegating every
// decision to the mutation-tested `bless_with`. In-crate tests cannot drive these real
// adapters deterministically; the binary's integration tests cover this edge.
#[cfg_attr(test, mutants::skip)]
pub async fn bless(
    options: &BlessOptions,
    workspace_dir: &Path,
    clock_override: Option<Clock>,
    auto_override: Option<AutoDiscriminants>,
) -> Result<String, AnalyzeError> {
    let reporter = StderrReporter::new(options.verbose);

    let config_path = resolve_config_path(workspace_dir, options.config_path.as_deref());
    reporter.note_with(|| format!("loading configuration from {}", config_path.display()));
    let config = load_config(&config_path, options.config_path.is_some()).await?;

    let project_id = resolve_project_id(&config, workspace_dir);
    let local = resolve_local_path(options.local.as_ref(), storage_env().as_deref())?;
    let storage = build_storage(local.as_deref(), &config, workspace_dir, None)?;

    let git = SystemGitHistory::new(resolve_repo(workspace_dir, options.repo.as_deref()));
    let auto = resolve_auto_discriminants(auto_override).await?;

    let now = resolve_now(clock_override);
    let result = bless_with(
        &git,
        &storage,
        &project_id,
        &config,
        options,
        &auto,
        now,
        env!("CARGO_PKG_VERSION"),
        &reporter,
    )
    .await;
    // Flush the cache-invalidation marker after success: blessing writes a fresh
    // timestamped sidecar, so it is additive and never arms the backend — a
    // read-through cache discovers the new key through its always-fresh listing. It
    // only arms (and so bumps the marker, invalidating other machines' caches) in
    // the rare case of overwriting an existing sidecar, e.g. a same-second re-bless.
    let flush = storage
        .flush_pending_invalidation(&project_id, &reporter)
        .await;
    finish_with_flush(result, flush)
}

/// The real `unbless`: load configuration, wire the configured storage and git
/// history, and orchestrate.
// Thin real-adapter wiring: loads config from disk, builds the configured storage,
// and shells out via `SystemGitHistory`/`detect_auto_discriminants` before delegating every
// decision to the mutation-tested `unbless_with`. In-crate tests cannot drive these
// real adapters deterministically; the binary's integration tests cover this edge.
#[cfg_attr(test, mutants::skip)]
pub async fn unbless(
    options: &UnblessOptions,
    workspace_dir: &Path,
    auto_override: Option<AutoDiscriminants>,
) -> Result<String, AnalyzeError> {
    let reporter = StderrReporter::new(options.verbose);

    let config_path = resolve_config_path(workspace_dir, options.config_path.as_deref());
    reporter.note_with(|| format!("loading configuration from {}", config_path.display()));
    let config = load_config(&config_path, options.config_path.is_some()).await?;

    let project_id = resolve_project_id(&config, workspace_dir);
    let local = resolve_local_path(options.local.as_ref(), storage_env().as_deref())?;
    let storage = build_storage(local.as_deref(), &config, workspace_dir, None)?;

    let git = SystemGitHistory::new(resolve_repo(workspace_dir, options.repo.as_deref()));
    let auto = resolve_auto_discriminants(auto_override).await?;

    let result = unbless_with(
        &git,
        &storage,
        &project_id,
        &config,
        options,
        &auto,
        &reporter,
    )
    .await;
    // Unblessing deletes sidecars, which arms the backend, so flush the marker to
    // invalidate other machines' caches.
    let flush = storage
        .flush_pending_invalidation(&project_id, &reporter)
        .await;
    finish_with_flush(result, flush)
}

/// Storage- and git-generic `bless`: validate the preconditions, then write a
/// blessing sidecar into every set selected by discriminant filters that has a clean result at the
/// current commit.
#[expect(
    clippy::too_many_arguments,
    reason = "blessing wires several injected ports plus the pinned issue time and tool version"
)]
pub(crate) async fn bless_with<G, S>(
    git: &G,
    storage: &S,
    project_id: &str,
    config: &Config,
    options: &BlessOptions,
    auto: &AutoDiscriminants,
    now: Timestamp,
    tool_version: &str,
    reporter: &dyn Reporter,
) -> Result<String, AnalyzeError>
where
    G: GitHistory,
    S: Storage,
{
    let prefixes = if options.all {
        // An empty prefix list accepts every benchmark, so `--all` blesses the
        // whole commit.
        Vec::new()
    } else if options.prefixes.is_empty() {
        return Err(BlessSelectionRequiredError::new().into());
    } else {
        options.prefixes.clone()
    };

    let context = options.context.as_deref().unwrap_or("HEAD");
    let head = resolve_commit(git, context).await?;
    let short = short_commit_id(&head);

    // The base branch must still be *determinable* (an undeterminable base, or a bad
    // explicit `--base`, is a real configuration problem worth surfacing). Its only
    // remaining job here is to check membership and, when the commit is not on it,
    // warn — blessing off the base branch is allowed but only takes effect once the
    // commit joins the base's first-parent history.
    let base = resolve_base(git, config, options.base.as_deref())
        .await?
        .ok_or_else(BlessBaseRequiredError::new)?;

    let selection = Selection::from_bless(options);
    let discriminants = resolve_discriminants(&selection, Some(auto))?;

    // The always-on effective-selection announcement: one line, printed regardless
    // of `--verbose`, naming the resolved (possibly auto-detected) partition, base
    // branch, and context commit, so a plain run never hides a value it defaulted.
    announce_selection(
        reporter,
        &selection_announcement(
            &discriminants,
            Some(AnnouncedBase {
                name: &base.name,
                auto: options.base.is_none(),
            }),
            Some(AnnouncedContext {
                short,
                defaulted_head: options.context.is_none(),
            }),
            None,
        ),
    );

    // Warnings are surfaced in the returned message (like the dirty-tree warning),
    // in a stable order: off-base, then no-data, then dirty.
    let mut warnings: Vec<String> = Vec::new();

    // `analyze` orders a series by the base branch's first-parent history and admits
    // a blessing only when its commit lies on that mainline, so the membership test
    // here mirrors it exactly: the context commit must appear in the base ref's
    // first-parent ancestry. An ordinary (non-first-parent) ancestor — a commit
    // merged in as a side parent — is *not* on the mainline `analyze` walks, so it
    // must warn just like an unrelated commit.
    let on_base = git
        .first_parent(&base.commit)
        .await
        .map_err(|error| FirstParentWalkFailedError::caused_by(&base.commit, error))?
        .iter()
        .any(|commit| commit.commit_id == head);
    if !on_base {
        warnings.push(format!(
            "Warning: the context commit {short} is not on the base branch {}; the blessing takes \
             effect only once this commit is part of the base branch's first-parent history (for \
             example after a fast-forward), and analyze ignores it until then.",
            short_commit_id(&base.commit)
        ));
    }

    // A blessing accepts the *committed* level recorded at the context commit
    // (`clean.json`), so a dirty working tree does not change which data point is
    // blessed — the local edits are simply irrelevant. Warn rather than refuse, so
    // an accidental uncommitted edit does not block blessing an already-recorded
    // clean run. The warning is only relevant when blessing the checked-out commit.
    let working_tree_dirty = options.context.is_none()
        && git
            .is_dirty()
            .await
            .map_err(WorkingTreeProbeFailedError::caused_by)?;

    let issued_unix = now.as_second();
    let candidates =
        discriminant_filtered_candidates(storage, project_id, &discriminants, reporter).await?;
    let clean_at_head: Vec<StorageKey> = candidates
        .into_iter()
        .filter(|(_, parsed)| parsed.commit == head && parsed.is_clean())
        .map(|(_, parsed)| parsed)
        .collect();

    // Each target is a `(discriminant set, sidecar key)` pair. With a run present the
    // sidecars land beside the stored results at the commit; with no run present they
    // are synthesized from the resolved discriminant filters so a pre-emptive blessing still has a
    // concrete home for whichever engine's data is captured there later.
    let targets: Vec<(DiscriminantSet, String)> = if clean_at_head.is_empty() {
        warnings.push(format!(
            "Warning: no stored result at the context commit {short}; blessing anyway — \
             double-check the commit id. The blessing takes effect once a run is captured at this \
             commit in a matching discriminant set."
        ));
        let sets = synthesize_target_sets(&discriminants);
        if sets.is_empty() {
            return Err(BlessDiscriminantsRequiredError::new(short).into());
        }
        sets.into_iter()
            .map(|set| {
                let key = set.bless_key(project_id, &head, issued_unix);
                (set, key)
            })
            .collect()
    } else {
        clean_at_head
            .iter()
            .map(|parsed| (parsed.set.clone(), parsed.bless_key(issued_unix)))
            .collect()
    };

    let mut sets = 0_usize;
    for (set, bless_key) in &targets {
        let record =
            BlessingRecord::new(head.clone(), now, prefixes.clone(), tool_version.to_owned());
        let json = record
            .to_json()
            .expect("a freshly built blessing always serializes to JSON");
        storage.put_overwrite(bless_key, json.as_bytes()).await?;
        reporter.note_with(|| format!("blessed set {set} at {bless_key}"));
        sets = sets.saturating_add(1);
    }

    if working_tree_dirty {
        warnings.push(format!(
            "Warning: uncommitted changes present. Blessing was applied to the existing commit at \
             HEAD ({short})."
        ));
    }

    let scope = if options.all {
        "all benchmarks".to_owned()
    } else {
        count_noun(prefixes.len(), "prefix filter")
    };
    let warnings_prefix = if warnings.is_empty() {
        String::new()
    } else {
        format!("{}\n", warnings.join("\n"))
    };
    let message = format!(
        "{warnings_prefix}Blessed {scope} across {} at commit {short}.",
        count_noun(sets, "discriminant set"),
    );
    Ok(message)
}

/// Storage- and git-generic `unbless`: delete every blessing recorded at the
/// current commit in the sets selected by discriminant filters.
pub(crate) async fn unbless_with<G, S>(
    git: &G,
    storage: &S,
    project_id: &str,
    _config: &Config,
    options: &UnblessOptions,
    auto: &AutoDiscriminants,
    reporter: &dyn Reporter,
) -> Result<String, AnalyzeError>
where
    G: GitHistory,
    S: Storage,
{
    let context = options.context.as_deref().unwrap_or("HEAD");
    let head = resolve_commit(git, context).await?;
    let short = short_commit_id(&head);

    let selection = Selection::from_unbless(options);
    let discriminants = resolve_discriminants(&selection, Some(auto))?;

    // The always-on effective-selection announcement: one line, printed regardless
    // of `--verbose`, naming the resolved (possibly auto-detected) partition and the
    // context commit whose blessings are being removed. `unbless` acts purely at a
    // commit and never resolves a base branch, so no base segment appears.
    announce_selection(
        reporter,
        &selection_announcement(
            &discriminants,
            None,
            Some(AnnouncedContext {
                short,
                defaulted_head: options.context.is_none(),
            }),
            None,
        ),
    );

    let candidates =
        discriminant_filtered_candidates(storage, project_id, &discriminants, reporter).await?;
    let blessings_at_head: Vec<String> = candidates
        .into_iter()
        .filter(|(_, parsed)| parsed.commit == head && parsed.is_bless())
        .map(|(key, _)| key)
        .collect();

    let mut removed = 0_usize;
    for key in &blessings_at_head {
        storage.delete(key).await?;
        reporter.note_with(|| format!("removed blessing {key}"));
        removed = removed.saturating_add(1);
    }

    let message = if removed == 0 {
        format!("No blessings recorded at commit {short}.")
    } else {
        format!(
            "Removed {} at commit {short}.",
            count_noun(removed, "blessing")
        )
    };
    Ok(message)
}

/// Resolves a context ref (for example `HEAD` or a commit ID) to a full commit ID.
async fn resolve_commit<G: GitHistory>(git: &G, reference: &str) -> Result<String, AnalyzeError> {
    let resolved = git
        .resolve(reference)
        .await
        .map_err(|error| ResolveRefFailedError::caused_by(reference, error))?;
    resolved
        .ok_or_else(|| {
            UnresolvedRefError::new(
                "resolving a blessing context",
                reference,
                "Check that the ref exists or is fetched, and select a repository with --repo if \
                 needed.",
            )
        })
        .map_err(Into::into)
}

/// The first twelve characters of a commit ID (all of it when shorter), for messages.
fn short_commit_id(commit_id: &str) -> &str {
    commit_id.get(..12).unwrap_or(commit_id)
}

/// Concrete discriminant sets to record a pre-emptive blessing in when the context
/// commit has no stored run to anchor to.
///
/// Analysis matches a blessing to a series by an *exact* [`DiscriminantSet`], so a
/// pre-emptive blessing must already occupy the set a future run will land in. The
/// targets are the cartesian product of the resolved discriminant filters' concrete values: an
/// omitted `--engine` expands to every [`Engine`] (there is no host default), so
/// whichever engine's data is captured later is accepted, while the target triple and
/// machine key default to the current host. The product is empty only when the triple
/// or machine-key filter is unconstrained (`all`) and so cannot be enumerated.
///
/// Repeated discriminant values (for example `--engine callgrind --engine callgrind`) or
/// values that sanitize to the same segment collapse to one set, so the caller writes
/// each sidecar key once and reports an honest count.
fn synthesize_target_sets(discriminants: &DiscriminantSetQuery) -> Vec<DiscriminantSet> {
    let engines: Vec<Engine> = match &discriminants.engine {
        DiscriminantFilter::All => Engine::ALL.to_vec(),
        DiscriminantFilter::Auto(value) => Engine::from_name(value).into_iter().collect(),
        DiscriminantFilter::Explicit(values) => values
            .iter()
            .filter_map(|value| Engine::from_name(value))
            .collect(),
    };
    let triples = concrete_discriminant_values(&discriminants.target_triple);
    let machines = concrete_discriminant_values(&discriminants.machine_key);

    let mut sets = Vec::new();
    let mut seen = HashSet::new();
    for engine in &engines {
        for triple in &triples {
            for machine in &machines {
                let set = DiscriminantSet::new(
                    *engine,
                    &TargetTriple::from(triple.as_str()),
                    &MachineKey::from(machine.as_str()),
                );
                if seen.insert(set.clone()) {
                    sets.push(set);
                }
            }
        }
    }
    sets
}

/// The concrete values a non-engine discriminant filter resolves to, or empty when it is
/// unconstrained (`all`) and so cannot be enumerated.
fn concrete_discriminant_values(filter: &DiscriminantFilter) -> Vec<String> {
    match filter {
        DiscriminantFilter::All => Vec::new(),
        DiscriminantFilter::Auto(value) => vec![value.clone()],
        DiscriminantFilter::Explicit(values) => values.iter().cloned().collect(),
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    #![allow(clippy::indexing_slicing, reason = "panic is fine in tests")]
    use cbh_diag::RecordingReporter;
    use cbh_git::FakeGitHistory;
    use cbh_model::{
        BenchmarkId, BenchmarkIdPrefix, BenchmarkResult, EnvironmentInfo, GitInfo, Metric,
        MetricKind, Run, RunContext, ToolchainInfo,
    };
    use cbh_storage::MemoryStorage;
    use futures::executor::block_on;
    use nonempty::nonempty;
    use ohno::ErrorExt as _;

    use super::*;
    use crate::testing::run_points_json;

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

    fn ts(seconds: i64) -> Timestamp {
        Timestamp::from_second(seconds).unwrap()
    }

    /// A serialized clean result set at `commit`, ready to seed storage.
    fn clean_run_json(commit: &str, effective: i64) -> String {
        let time = ts(effective);
        let context = RunContext::new(
            time,
            GitInfo {
                commit: Some(commit.to_owned()),
                branch: Some("master".to_owned()),
                dirty: false,
            },
            EnvironmentInfo::default(),
            ToolchainInfo::default(),
            "0.0.1".to_owned(),
        );
        let record = BenchmarkResult::new(
            BenchmarkId::new(nonempty!["all_the_time".to_owned(), "read_cell".to_owned()]),
            vec![Metric::new(MetricKind::InstructionCount, 100.0)],
        );
        run_points_json(&Run::new(context, vec![record]))
    }

    fn clean_key(commit: &str) -> String {
        format!("v1/folo/objects/callgrind/x86_64-unknown-linux-gnu/m1/{commit}/clean.json")
    }

    /// A linear master history `c0 - c1 - c2`, HEAD at the tip `c2`.
    fn master_git() -> FakeGitHistory {
        let mut git = FakeGitHistory::new();
        git.commit("c0", None)
            .commit("c1", Some("c0"))
            .commit("c2", Some("c1"))
            .branch("master", "c2")
            .head("master")
            .mark_default("master");
        git
    }

    fn bless_options(prefixes: &[&str]) -> BlessOptions {
        BlessOptions {
            prefixes: prefixes
                .iter()
                .map(|prefix| BenchmarkIdPrefix::new(*prefix).unwrap())
                .collect(),
            ..BlessOptions::default()
        }
    }

    /// All blessing sidecar keys stored under the project partition.
    fn stored_blessings(storage: &MemoryStorage) -> Vec<String> {
        let mut keys = block_on(storage.list("v1/folo/")).unwrap();
        keys.retain(|key| {
            key.rsplit('/')
                .next()
                .is_some_and(|name| name.starts_with("bless-"))
        });
        keys.sort();
        keys
    }

    fn drive_bless(
        storage: &MemoryStorage,
        git: &FakeGitHistory,
        options: &BlessOptions,
    ) -> Result<String, AnalyzeError> {
        block_on(bless_with(
            git,
            storage,
            "folo",
            &config(),
            options,
            &auto(),
            ts(1_700_000_000),
            "0.0.1",
            &RecordingReporter::quiet(),
        ))
    }

    #[test]
    fn bless_writes_a_sidecar_into_the_set_with_a_clean_run_at_head() {
        let storage = MemoryStorage::new();
        block_on(storage.put(&clean_key("c2"), clean_run_json("c2", 1000).as_bytes())).unwrap();
        let git = master_git();

        let message =
            drive_bless(&storage, &git, &bless_options(&["all_the_time/read_cell"])).unwrap();
        assert!(message.contains("Blessed"), "{message}");

        let blessings = stored_blessings(&storage);
        assert_eq!(blessings.len(), 1, "one sidecar written: {blessings:?}");
        // The sidecar lands in the same commit directory as the run it accepts.
        assert!(
            blessings[0].contains("/c2/bless-"),
            "sidecar in the commit dir: {}",
            blessings[0]
        );

        // The record carries the requested prefix and the blessed commit.
        let bytes = block_on(storage.get(&blessings[0])).unwrap();
        let record = BlessingRecord::from_json(&String::from_utf8(bytes).unwrap()).unwrap();
        assert_eq!(
            record.prefixes,
            vec![BenchmarkIdPrefix::new("all_the_time/read_cell").unwrap()]
        );
        assert_eq!(record.commit, "c2");
    }

    #[test]
    fn bless_requires_at_least_one_prefix() {
        let storage = MemoryStorage::new();
        block_on(storage.put(&clean_key("c2"), clean_run_json("c2", 1000).as_bytes())).unwrap();
        let error = drive_bless(&storage, &master_git(), &bless_options(&[])).unwrap_err();
        assert!(error.find_source::<BlessSelectionRequiredError>().is_some());
    }

    #[test]
    fn bless_off_the_base_branch_warns_but_succeeds() {
        let storage = MemoryStorage::new();
        block_on(storage.put(&clean_key("f1"), clean_run_json("f1", 1000).as_bytes())).unwrap();
        // A feature commit on top of master: HEAD is not on the base branch.
        let mut git = FakeGitHistory::new();
        git.commit("c0", None)
            .commit("c1", Some("c0"))
            .commit("c2", Some("c1"))
            .commit("f1", Some("c2"))
            .branch("master", "c2")
            .branch("feature", "f1")
            .head("feature")
            .mark_default("master");

        let message = drive_bless(&storage, &git, &bless_options(&["all_the_time"])).unwrap();
        // Off-base is a warning now, not a refusal, and the warning is explanatory
        // about when the blessing takes effect.
        assert!(message.contains("not on the base branch"), "{message}");
        assert!(message.contains("first-parent history"), "{message}");
        assert!(message.contains("Blessed"), "{message}");
        // The message names both the current commit and the base ref via
        // `short_commit_id`, so both must appear verbatim.
        assert!(message.contains("f1"), "names HEAD: {message}");
        assert!(message.contains("c2"), "names base: {message}");

        // The clean run present at f1 is still blessed in its own set.
        let blessings = stored_blessings(&storage);
        assert_eq!(blessings.len(), 1, "one sidecar written: {blessings:?}");
        assert!(
            blessings[0].contains("/f1/bless-"),
            "sidecar in the f1 commit dir: {}",
            blessings[0]
        );
    }

    #[test]
    fn bless_a_prefix_matching_no_benchmark_still_writes_a_sidecar() {
        let storage = MemoryStorage::new();
        block_on(storage.put(&clean_key("c2"), clean_run_json("c2", 1000).as_bytes())).unwrap();

        // The clean run only carries `all_the_time/read_cell`; this prefix matches no
        // benchmark in it. Prefixes are recorded verbatim, never validated against the
        // run, so the blessing still succeeds and stores the unmatched prefix.
        let message = drive_bless(
            &storage,
            &master_git(),
            &bless_options(&["all_the_time/nonexistent"]),
        )
        .unwrap();
        assert!(message.contains("Blessed"), "{message}");

        let blessings = stored_blessings(&storage);
        assert_eq!(blessings.len(), 1, "one sidecar written: {blessings:?}");
        let bytes = block_on(storage.get(&blessings[0])).unwrap();
        let record = BlessingRecord::from_json(&String::from_utf8(bytes).unwrap()).unwrap();
        assert_eq!(
            record.prefixes,
            vec![BenchmarkIdPrefix::new("all_the_time/nonexistent").unwrap()]
        );
    }

    #[test]
    fn bless_a_dirty_tree_warns_but_still_blesses() {
        let storage = MemoryStorage::new();
        block_on(storage.put(&clean_key("c2"), clean_run_json("c2", 1000).as_bytes())).unwrap();
        let mut git = master_git();
        git.mark_dirty();

        let message = drive_bless(&storage, &git, &bless_options(&["all_the_time"])).unwrap();
        assert!(
            message.contains("Warning: uncommitted changes present"),
            "{message}"
        );
        assert!(message.contains("Blessed"), "{message}");
        assert_eq!(
            stored_blessings(&storage).len(),
            1,
            "the committed clean run at HEAD is still blessed"
        );
    }

    #[test]
    fn bless_with_a_context_targets_an_earlier_commit() {
        let storage = MemoryStorage::new();
        // A clean run exists at c1, an earlier commit than HEAD (c2).
        block_on(storage.put(&clean_key("c1"), clean_run_json("c1", 1000).as_bytes())).unwrap();
        let options = BlessOptions {
            context: Some("c1".to_owned()),
            ..bless_options(&["all_the_time/read_cell"])
        };

        let message = drive_bless(&storage, &master_git(), &options).unwrap();
        assert!(message.contains("at commit c1"), "{message}");

        let blessings = stored_blessings(&storage);
        assert_eq!(blessings.len(), 1, "one sidecar written: {blessings:?}");
        assert!(
            blessings[0].contains("/c1/bless-"),
            "sidecar in the c1 commit dir: {}",
            blessings[0]
        );
    }

    #[test]
    fn bless_with_an_explicit_context_does_not_warn_about_a_dirty_tree() {
        let storage = MemoryStorage::new();
        block_on(storage.put(&clean_key("c1"), clean_run_json("c1", 1000).as_bytes())).unwrap();
        let mut git = master_git();
        git.mark_dirty();
        let options = BlessOptions {
            context: Some("c1".to_owned()),
            ..bless_options(&["all_the_time/read_cell"])
        };

        let message = drive_bless(&storage, &git, &options).unwrap();
        assert!(
            !message.contains("Warning"),
            "an explicit context ignores the working tree: {message}"
        );
    }

    #[test]
    fn bless_all_writes_an_empty_prefix_list_accepting_every_benchmark() {
        let storage = MemoryStorage::new();
        block_on(storage.put(&clean_key("c2"), clean_run_json("c2", 1000).as_bytes())).unwrap();
        let options = BlessOptions {
            all: true,
            ..BlessOptions::default()
        };

        let message = drive_bless(&storage, &master_git(), &options).unwrap();
        assert!(message.contains("all benchmarks"), "{message}");

        let blessings = stored_blessings(&storage);
        assert_eq!(blessings.len(), 1, "one sidecar written: {blessings:?}");
        let bytes = block_on(storage.get(&blessings[0])).unwrap();
        let record = BlessingRecord::from_json(&String::from_utf8(bytes).unwrap()).unwrap();
        // An empty prefix list accepts every benchmark.
        assert!(record.prefixes.is_empty());
    }

    #[test]
    fn unbless_with_a_context_removes_blessings_at_an_earlier_commit() {
        let storage = MemoryStorage::new();
        block_on(storage.put(&clean_key("c1"), clean_run_json("c1", 1000).as_bytes())).unwrap();
        let git = master_git();
        let bless = BlessOptions {
            context: Some("c1".to_owned()),
            ..bless_options(&["all_the_time/read_cell"])
        };
        drive_bless(&storage, &git, &bless).unwrap();
        assert_eq!(stored_blessings(&storage).len(), 1, "blessed once");

        let unbless = UnblessOptions {
            context: Some("c1".to_owned()),
            ..UnblessOptions::default()
        };
        let message = block_on(unbless_with(
            &git,
            &storage,
            "folo",
            &config(),
            &unbless,
            &auto(),
            &RecordingReporter::quiet(),
        ))
        .unwrap();
        assert!(message.contains("at commit c1"), "{message}");
        assert!(stored_blessings(&storage).is_empty(), "sidecar deleted");
    }

    #[test]
    fn bless_without_a_run_at_the_commit_warns_and_synthesizes_all_engine_sets() {
        let storage = MemoryStorage::new();
        // A clean run exists, but on an earlier commit, not HEAD.
        block_on(storage.put(&clean_key("c1"), clean_run_json("c1", 1000).as_bytes())).unwrap();

        let message =
            drive_bless(&storage, &master_git(), &bless_options(&["all_the_time"])).unwrap();
        // No data at the commit is a warning now, not a refusal.
        assert!(message.contains("no stored result"), "{message}");
        assert!(message.contains("Blessed"), "{message}");

        // With no run to anchor to, one sidecar is synthesized per engine under the
        // auto-detected triple and machine key.
        let blessings = stored_blessings(&storage);
        assert_eq!(blessings.len(), 4, "one sidecar per engine: {blessings:?}");
        assert!(
            blessings.iter().all(|key| key.contains("/c2/bless-")),
            "all at the c2 commit dir: {blessings:?}"
        );
        // The callgrind sidecar sits exactly where a future callgrind run at c2 would,
        // so the blessing will actually apply once that data lands.
        let callgrind = DiscriminantSet::new(
            Engine::Callgrind,
            &TargetTriple::from("x86_64-unknown-linux-gnu"),
            &MachineKey::from("m1"),
        )
        .bless_key("folo", "c2", 1_700_000_000);
        assert!(
            blessings.contains(&callgrind),
            "{blessings:?} lacks {callgrind}"
        );
    }

    #[test]
    fn bless_all_on_an_empty_project_synthesizes_all_engine_sets() {
        let storage = MemoryStorage::new();
        // No runs recorded anywhere: `bless --all` still succeeds pre-emptively.
        let options = BlessOptions {
            all: true,
            ..BlessOptions::default()
        };

        let message = drive_bless(&storage, &master_git(), &options).unwrap();
        assert!(message.contains("no stored result"), "{message}");
        assert!(message.contains("all benchmarks"), "{message}");

        let blessings = stored_blessings(&storage);
        assert_eq!(blessings.len(), 4, "one sidecar per engine: {blessings:?}");
        // Each carries an empty prefix list (accepting every benchmark).
        for key in &blessings {
            let bytes = block_on(storage.get(key)).unwrap();
            let record = BlessingRecord::from_json(&String::from_utf8(bytes).unwrap()).unwrap();
            assert!(record.prefixes.is_empty(), "{key} should accept all");
        }
    }

    #[test]
    fn bless_without_data_and_explicit_engine_targets_only_that_engine() {
        let storage = MemoryStorage::new();
        let options = BlessOptions {
            engine: vec!["callgrind".to_owned()],
            ..bless_options(&["all_the_time"])
        };

        let message = drive_bless(&storage, &master_git(), &options).unwrap();
        assert!(message.contains("no stored result"), "{message}");

        // An explicit engine narrows synthesis to just that engine.
        let blessings = stored_blessings(&storage);
        assert_eq!(blessings.len(), 1, "one callgrind sidecar: {blessings:?}");
        assert!(
            blessings[0].contains("/callgrind/x86_64-unknown-linux-gnu/m1/c2/bless-"),
            "{}",
            blessings[0]
        );
    }

    #[test]
    fn bless_without_data_dedupes_repeated_engine_discriminants() {
        let storage = MemoryStorage::new();
        // A repeated `--engine` value must not write the same sidecar key twice or
        // over-report the discriminant-set count.
        let options = BlessOptions {
            engine: vec!["callgrind".to_owned(), "callgrind".to_owned()],
            ..bless_options(&["all_the_time"])
        };

        let message = drive_bless(&storage, &master_git(), &options).unwrap();
        assert!(message.contains("no stored result"), "{message}");
        assert!(
            message.contains("across 1 discriminant set "),
            "the repeated engine collapses to one set: {message}"
        );

        let blessings = stored_blessings(&storage);
        assert_eq!(blessings.len(), 1, "one callgrind sidecar: {blessings:?}");
    }

    #[test]
    fn bless_without_data_and_unconstrained_machine_is_an_error() {
        let storage = MemoryStorage::new();
        // No data at the commit and the machine-key filter is `all`, so no concrete
        // discriminant set can be synthesized to anchor the blessing.
        let options = BlessOptions {
            machine_key: vec!["all".to_owned()],
            ..bless_options(&["all_the_time"])
        };

        let error = drive_bless(&storage, &master_git(), &options).unwrap_err();
        assert!(
            error
                .find_source::<BlessDiscriminantsRequiredError>()
                .is_some()
        );
        assert!(stored_blessings(&storage).is_empty());
    }

    #[test]
    fn synthesize_expands_an_auto_engine_and_explicit_discriminants() {
        // An auto-detected engine resolves to that single engine, while explicit
        // multi-value discriminants expand across each concrete value.
        let discriminants = DiscriminantSetQuery {
            engine: DiscriminantFilter::Auto("callgrind".to_owned()),
            target_triple: DiscriminantFilter::Explicit(nonempty![
                "x86_64-unknown-linux-gnu".to_owned(),
                "aarch64-apple-darwin".to_owned(),
            ]),
            machine_key: DiscriminantFilter::Auto("m1".to_owned()),
        };
        let sets = synthesize_target_sets(&discriminants);
        // One engine × two triples × one machine = two sets.
        assert_eq!(sets.len(), 2, "{sets:?}");
        assert!(
            sets.iter().all(|set| set.engine == Engine::Callgrind),
            "the auto engine is callgrind: {sets:?}"
        );
    }

    #[test]
    fn unbless_removes_every_blessing_at_head() {
        let storage = MemoryStorage::new();
        block_on(storage.put(&clean_key("c2"), clean_run_json("c2", 1000).as_bytes())).unwrap();
        let git = master_git();
        drive_bless(&storage, &git, &bless_options(&["all_the_time/read_cell"])).unwrap();
        assert_eq!(stored_blessings(&storage).len(), 1, "blessed once");

        let message = block_on(unbless_with(
            &git,
            &storage,
            "folo",
            &config(),
            &UnblessOptions::default(),
            &auto(),
            &RecordingReporter::quiet(),
        ))
        .unwrap();
        assert!(message.contains("Removed"), "{message}");
        assert!(stored_blessings(&storage).is_empty(), "sidecar deleted");
    }

    #[test]
    fn unbless_reports_when_there_is_nothing_to_remove() {
        let storage = MemoryStorage::new();
        block_on(storage.put(&clean_key("c2"), clean_run_json("c2", 1000).as_bytes())).unwrap();
        let message = block_on(unbless_with(
            &master_git(),
            &storage,
            "folo",
            &config(),
            &UnblessOptions::default(),
            &auto(),
            &RecordingReporter::quiet(),
        ))
        .unwrap();
        assert!(message.contains("No blessings"), "{message}");
    }

    #[test]
    fn bless_rejects_an_unresolved_head() {
        let storage = MemoryStorage::new();
        // No commits: HEAD does not resolve.
        let git = FakeGitHistory::new();
        let error =
            drive_bless(&storage, &git, &bless_options(&["all_the_time/read_cell"])).unwrap_err();
        let found = error.find_source::<UnresolvedRefError>().unwrap();
        assert_eq!(found.operation, "resolving a blessing context");
        assert_eq!(found.reference, "HEAD");
    }

    #[test]
    fn unbless_rejects_an_unresolved_head() {
        let storage = MemoryStorage::new();
        // No commits: HEAD does not resolve.
        let git = FakeGitHistory::new();
        let error = block_on(unbless_with(
            &git,
            &storage,
            "folo",
            &config(),
            &UnblessOptions::default(),
            &auto(),
            &RecordingReporter::quiet(),
        ))
        .unwrap_err();
        let found = error.find_source::<UnresolvedRefError>().unwrap();
        assert_eq!(found.operation, "resolving a blessing context");
        assert_eq!(found.reference, "HEAD");
    }

    #[test]
    fn bless_names_a_failed_ref_resolution() {
        // Resolving the context, walking the base's ancestry, and probing the working
        // tree are all on the bless path, so each must name itself rather than a
        // neighbour.
        let storage = MemoryStorage::new();
        let mut git = master_git();
        git.fail_resolve();

        let error =
            drive_bless(&storage, &git, &bless_options(&["all_the_time/read_cell"])).unwrap_err();
        assert!(error.find_source::<ResolveRefFailedError>().is_some());
    }

    #[test]
    fn bless_names_a_failed_ancestry_walk() {
        let storage = MemoryStorage::new();
        let mut git = master_git();
        git.fail_first_parent();

        let error =
            drive_bless(&storage, &git, &bless_options(&["all_the_time/read_cell"])).unwrap_err();
        assert!(error.find_source::<FirstParentWalkFailedError>().is_some());
    }

    #[test]
    fn bless_names_a_failed_working_tree_probe() {
        let storage = MemoryStorage::new();
        let mut git = master_git();
        git.fail_is_dirty();

        let error =
            drive_bless(&storage, &git, &bless_options(&["all_the_time/read_cell"])).unwrap_err();
        assert!(error.find_source::<WorkingTreeProbeFailedError>().is_some());
    }

    #[test]
    fn bless_without_a_resolvable_base_branch_is_an_error() {
        let storage = MemoryStorage::new();
        block_on(storage.put(&clean_key("c2"), clean_run_json("c2", 1000).as_bytes())).unwrap();
        // HEAD resolves, but no advertised default branch and no --base / config
        // default, so the base branch cannot be determined.
        let mut git = FakeGitHistory::new();
        git.commit("c0", None)
            .commit("c1", Some("c0"))
            .commit("c2", Some("c1"))
            .branch("master", "c2")
            .head("master"); // No `.mark_default(...)`.
        let error =
            drive_bless(&storage, &git, &bless_options(&["all_the_time/read_cell"])).unwrap_err();
        assert!(error.find_source::<BlessBaseRequiredError>().is_some());
    }

    #[test]
    fn bless_announces_the_effective_selection() {
        let storage = MemoryStorage::new();
        block_on(storage.put(&clean_key("c2"), clean_run_json("c2", 1000).as_bytes())).unwrap();
        let reporter = RecordingReporter::quiet();
        block_on(bless_with(
            &master_git(),
            &storage,
            "folo",
            &config(),
            &bless_options(&["all_the_time/read_cell"]),
            &auto(),
            ts(1_700_000_000),
            "0.0.1",
            &reporter,
        ))
        .unwrap();
        // The auto-detected partition, auto-detected base branch, and the context
        // commit (defaulted to HEAD) are all named on the always-on line.
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
            reporter.announced("context=c2 (defaulted to HEAD)"),
            "{:?}",
            reporter.announcements()
        );
    }

    #[test]
    fn unbless_announces_the_effective_selection_without_a_base() {
        let storage = MemoryStorage::new();
        block_on(storage.put(&clean_key("c2"), clean_run_json("c2", 1000).as_bytes())).unwrap();
        let git = master_git();
        drive_bless(&storage, &git, &bless_options(&["all_the_time/read_cell"])).unwrap();
        let reporter = RecordingReporter::quiet();
        block_on(unbless_with(
            &git,
            &storage,
            "folo",
            &config(),
            &UnblessOptions::default(),
            &auto(),
            &reporter,
        ))
        .unwrap();
        assert!(
            reporter.announced("machine-key=m1 (auto-detected)"),
            "{:?}",
            reporter.announcements()
        );
        assert!(
            reporter.announced("context=c2 (defaulted to HEAD)"),
            "{:?}",
            reporter.announcements()
        );
        // `unbless` resolves no base branch, so the line carries no base segment.
        assert!(
            !reporter.announced("base="),
            "{:?}",
            reporter.announcements()
        );
    }
}
