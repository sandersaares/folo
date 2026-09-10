use crate::harness::*;

/// `prune --dirty` removes the dirty runs a matching `list`/`analyze` would include
/// on the target side of a feature branch, leaving the clean runs untouched. The
/// end state is verified through `list`, proving the production delete path reaches
/// the configured storage.
#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn prune_dirty_removes_dirty_runs_on_a_feature_branch() {
    let workspace = Workspace::repo(&storage_only_config());
    workspace.commit_dated("2024-01-01", "c1");
    workspace.seed_callgrind("c1", 100.0);
    workspace.checkout_new_branch("feature");
    workspace.commit_dated("2024-01-02", "f1");
    workspace.seed_callgrind("f1", 100.0);
    workspace.seed_dirty_callgrind("2024-01-03", "f1", 200.0);

    // Before: the target-side dirty snapshot is part of the data set.
    let message = workspace.drive_json(&["list", "runs"]).await;
    let parsed: serde_json::Value = serde_json::from_str(&message).unwrap();
    assert_eq!(parsed["totals"]["runs"], 3, "{message}");

    // `prune --dirty` removes exactly the one dirty run.
    let message = workspace.drive_json(&["prune", "--dirty"]).await;
    let parsed: serde_json::Value = serde_json::from_str(&message).unwrap();
    assert_eq!(parsed["dry_run"], false, "{message}");
    assert_eq!(parsed["totals"]["runs"], 1, "{message}");

    // After: only the two clean runs remain; the dirty snapshot is gone.
    let message = workspace.drive_json(&["list", "runs"]).await;
    let parsed: serde_json::Value = serde_json::from_str(&message).unwrap();
    assert_eq!(parsed["totals"]["runs"], 2, "{message}");
    let commits = parsed["sets"][0]["commits"].as_array().unwrap();
    assert!(
        commits.iter().all(|commit| commit["dirty"] == 0),
        "no dirty run remains: {message}"
    );
}

/// The key divergence from `analyze`/`list`: on the base branch with a *clean*
/// working tree, `list` hides the base-tip dirty snapshot, yet `prune --dirty` still
/// removes it (the base-tip exception is unconditional for the dirty scope).
#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn prune_dirty_removes_the_base_branch_tip_dirty_with_a_clean_tree() {
    let workspace = Workspace::clean_repo(&storage_only_config());
    workspace.commit_dated("2024-01-01", "c1");
    workspace.seed_callgrind("c1", 100.0);
    workspace.seed_dirty_callgrind("2024-01-02", "c1", 200.0);

    // With a clean working tree, `list` excludes the base-tip dirty snapshot.
    let message = workspace.drive_json(&["list", "runs"]).await;
    let parsed: serde_json::Value = serde_json::from_str(&message).unwrap();
    assert_eq!(
        parsed["totals"]["runs"], 1,
        "list hides the base-tip dirty run with a clean tree: {message}"
    );

    // `prune --dirty` removes it anyway (with the base-branch guard confirmed).
    let message = workspace
        .drive_json(&["prune", "--dirty", "--prune-base"])
        .await;
    let parsed: serde_json::Value = serde_json::from_str(&message).unwrap();
    assert_eq!(parsed["totals"]["runs"], 1, "{message}");

    // A second pass finds nothing left to remove.
    let message = workspace
        .drive_json(&["prune", "--dirty", "--prune-base", "--dry-run"])
        .await;
    let parsed: serde_json::Value = serde_json::from_str(&message).unwrap();
    assert_eq!(
        parsed["totals"]["runs"], 0,
        "the base-tip dirty run was already removed: {message}"
    );
}

/// `--dry-run` previews the removal without deleting: it reports the same runs a
/// real `prune` would, but a follow-up real `prune` still finds and removes them.
#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn prune_dry_run_reports_without_deleting() {
    let workspace = Workspace::repo(&storage_only_config());
    workspace.commit_dated("2024-01-01", "c1");
    workspace.seed_callgrind("c1", 100.0);
    workspace.checkout_new_branch("feature");
    workspace.commit_dated("2024-01-02", "f1");
    workspace.seed_dirty_callgrind("2024-01-02", "f1", 200.0);

    let message = workspace
        .drive_json(&["prune", "--dirty", "--dry-run"])
        .await;
    let parsed: serde_json::Value = serde_json::from_str(&message).unwrap();
    assert_eq!(parsed["dry_run"], true, "{message}");
    assert_eq!(parsed["totals"]["runs"], 1, "{message}");

    // The dry run deleted nothing: a real prune still removes the same run.
    let message = workspace.drive_json(&["prune", "--dirty"]).await;
    let parsed: serde_json::Value = serde_json::from_str(&message).unwrap();
    assert_eq!(parsed["dry_run"], false, "{message}");
    assert_eq!(parsed["totals"]["runs"], 1, "{message}");
}

/// An engine discriminant scopes the prune: a callgrind-scoped `prune --dirty` leaves a
/// dirty criterion snapshot on the same commit untouched.
#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn prune_dirty_scopes_by_engine() {
    let workspace = Workspace::repo(&storage_only_config());
    workspace.commit_dated("2024-01-01", "c1");
    workspace.seed_callgrind("c1", 100.0);
    workspace.checkout_new_branch("feature");
    workspace.commit_dated("2024-01-02", "f1");
    workspace.seed_dirty_callgrind("2024-01-02", "f1", 200.0);
    workspace.seed_dirty_criterion("2024-01-02", "f1", "m1", 20.0);

    // Prune only the callgrind set.
    let message = workspace
        .drive_json(&["prune", "--dirty", "--engine", "callgrind"])
        .await;
    let parsed: serde_json::Value = serde_json::from_str(&message).unwrap();
    assert_eq!(
        parsed["totals"]["runs"], 1,
        "only the callgrind dirty run: {message}"
    );
    assert_eq!(parsed["sets"][0]["engine"], "callgrind", "{message}");

    // The criterion dirty snapshot survives: a criterion-scoped prune still finds it
    // once the windows triple and its non-host machine key are named explicitly.
    let message = workspace
        .drive_json(&[
            "prune",
            "--dirty",
            "--engine",
            "criterion",
            "--target-triple",
            "x86_64-pc-windows-msvc",
            "--machine-key",
            "m1",
            "--dry-run",
        ])
        .await;
    let parsed: serde_json::Value = serde_json::from_str(&message).unwrap();
    assert_eq!(
        parsed["totals"]["runs"], 1,
        "criterion dirty survived: {message}"
    );
    assert_eq!(parsed["sets"][0]["engine"], "criterion", "{message}");
}

/// A target-triple filter scopes the prune just like it scopes `analyze`/`list`:
/// the same target commit hosts a dirty Linux (callgrind) run and a dirty Windows
/// (criterion) run, and `--target-triple <linux>` removes only the former.
#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn prune_dirty_scopes_by_target_triple_discriminant() {
    let workspace = Workspace::repo(&storage_only_config());
    workspace.commit_dated("2024-01-01", "c1");
    workspace.seed_callgrind("c1", 100.0);
    workspace.checkout_new_branch("feature");
    workspace.commit_dated("2024-01-02", "f1");
    workspace.seed_dirty_callgrind("2024-01-02", "f1", 200.0);
    workspace.seed_dirty_criterion("2024-01-02", "f1", "m1", 20.0);

    // The Linux triple removes only the callgrind (linux) dirty run.
    let message = workspace
        .drive_json(&[
            "prune",
            "--dirty",
            "--target-triple",
            "x86_64-unknown-linux-gnu",
        ])
        .await;
    let parsed: serde_json::Value = serde_json::from_str(&message).unwrap();
    assert_eq!(parsed["totals"]["runs"], 1, "{message}");
    assert_eq!(
        parsed["sets"][0]["target_triple"], "x86_64-unknown-linux-gnu",
        "{message}"
    );
    assert_eq!(parsed["sets"][0]["engine"], "callgrind", "{message}");

    // The Windows (criterion) dirty run survives: a Windows-triple pass still finds
    // it once the machine discriminant is widened to its non-host machine key.
    let message = workspace
        .drive_json(&[
            "prune",
            "--dirty",
            "--target-triple",
            "x86_64-pc-windows-msvc",
            "--machine-key",
            "m1",
            "--dry-run",
        ])
        .await;
    let parsed: serde_json::Value = serde_json::from_str(&message).unwrap();
    assert_eq!(
        parsed["totals"]["runs"], 1,
        "windows dirty survived: {message}"
    );
    assert_eq!(
        parsed["sets"][0]["target_triple"], "x86_64-pc-windows-msvc",
        "{message}"
    );
}

/// `prune` spans every selected discriminant set in one pass: a dirty callgrind and
/// a dirty criterion run on the same target commit form two sets, and an unfiltered
/// `prune --dirty` removes both — exercising the multi-set plan and its plural
/// rendering.
#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn prune_dirty_removes_runs_across_multiple_discriminant_sets() {
    let workspace = Workspace::repo(&storage_only_config());
    workspace.commit_dated("2024-01-01", "c1");
    workspace.seed_callgrind("c1", 100.0);
    workspace.checkout_new_branch("feature");
    workspace.commit_dated("2024-01-02", "f1");
    workspace.seed_dirty_callgrind("2024-01-02", "f1", 200.0);
    workspace.seed_dirty_criterion("2024-01-02", "f1", "m1", 20.0);

    // Text format exercises the plural "discriminant sets" summary branch. The
    // discriminants widen to every triple/machine so the callgrind set (under the harness
    // machine key) and the `m1`-keyed criterion set are both in scope regardless of
    // host.
    let RunOutcome::Completed { message } = workspace
        .drive(&[
            "prune",
            "--dirty",
            "--target-triple",
            "all",
            "--machine-key",
            "all",
        ])
        .await
        .unwrap()
    else {
        panic!("expected a completed outcome");
    };
    assert!(
        message.contains("Removed 2 runs across 2 discriminant sets"),
        "{message}"
    );

    // Both sets are now empty: a second pass finds nothing to remove.
    let message = workspace
        .drive_json(&[
            "prune",
            "--dirty",
            "--target-triple",
            "all",
            "--machine-key",
            "all",
            "--dry-run",
        ])
        .await;
    let parsed: serde_json::Value = serde_json::from_str(&message).unwrap();
    assert_eq!(parsed["totals"]["runs"], 0, "{message}");
}

/// `--since` removes only the dirty runs on or after the cutoff — one of the two
/// scopes that read object bodies in production (to recover each run's effective
/// time).
#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn prune_dirty_since_only_removes_runs_on_or_after_the_cutoff() {
    let workspace = Workspace::repo(&storage_only_config());
    workspace.commit_dated("2024-01-01", "c1");
    workspace.seed_callgrind("c1", 100.0);
    workspace.checkout_new_branch("feature");
    workspace.commit_dated("2024-01-02", "f1");
    workspace.seed_dirty_callgrind("2024-01-02", "f1", 100.0);
    workspace.commit_dated("2024-01-05", "f2");
    workspace.seed_dirty_callgrind("2024-01-05", "f2", 200.0);

    // Only the 2024-01-05 run is on or after the cutoff.
    let message = workspace
        .drive_json(&["prune", "--dirty", "--since", "2024-01-04"])
        .await;
    let parsed: serde_json::Value = serde_json::from_str(&message).unwrap();
    assert_eq!(parsed["totals"]["runs"], 1, "{message}");

    // The on-or-after run is gone; the earlier run survives the cutoff.
    let message = workspace
        .drive_json(&["prune", "--dirty", "--since", "2024-01-04", "--dry-run"])
        .await;
    let parsed: serde_json::Value = serde_json::from_str(&message).unwrap();
    assert_eq!(
        parsed["totals"]["runs"], 0,
        "the late run was removed: {message}"
    );

    let message = workspace
        .drive_json(&["prune", "--dirty", "--dry-run"])
        .await;
    let parsed: serde_json::Value = serde_json::from_str(&message).unwrap();
    assert_eq!(
        parsed["totals"]["runs"], 1,
        "the early run survived the cutoff: {message}"
    );
}

/// The `--all` scope deletes clean *and* dirty runs for the narrowed selection. A
/// `<commit>` argument selecting one feature commit removes its
/// clean and dirty runs while leaving the base-branch run intact.
#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn prune_all_removes_clean_and_dirty_for_a_commit() {
    let workspace = Workspace::repo(&storage_only_config());
    workspace.commit_dated("2024-01-01", "c1");
    workspace.seed_callgrind("c1", 100.0);
    workspace.checkout_new_branch("feature");
    workspace.commit_dated("2024-01-02", "f1");
    workspace.seed_callgrind("f1", 100.0);
    workspace.seed_dirty_callgrind("2024-01-03", "f1", 200.0);
    let f1 = workspace.commit("f1");

    // Narrowed to f1, the `--all` scope removes its clean and dirty runs.
    let message = workspace.drive_json(&["prune", &f1, "--all"]).await;
    let parsed: serde_json::Value = serde_json::from_str(&message).unwrap();
    assert_eq!(parsed["totals"]["runs"], 2, "clean + dirty f1: {message}");

    // Only the base-branch clean run remains.
    let message = workspace
        .drive_json(&["list", "runs", "--context", "feature"])
        .await;
    let parsed: serde_json::Value = serde_json::from_str(&message).unwrap();
    assert_eq!(parsed["totals"]["runs"], 1, "{message}");
}

/// `--all` deletes the whole selected data set, but Design B preserves base-branch
/// history: pruning the feature context removes only the feature-unique commits,
/// leaving the base commit at the merge-base intact.
#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn prune_all_removes_only_the_feature_side() {
    let workspace = Workspace::repo(&storage_only_config());
    workspace.commit_dated("2024-01-01", "c1");
    workspace.seed_callgrind("c1", 100.0);
    workspace.checkout_new_branch("feature");
    workspace.commit_dated("2024-01-02", "f1");
    workspace.seed_callgrind("f1", 100.0);

    let message = workspace
        .drive_json(&["prune", "--all", "--context", "feature"])
        .await;
    let parsed: serde_json::Value = serde_json::from_str(&message).unwrap();
    assert_eq!(
        parsed["totals"]["runs"], 1,
        "only the feature-unique run is deleted: {message}"
    );

    // The base commit survives; only the base-branch baseline remains.
    let message = workspace
        .drive_json(&["list", "runs", "--context", "feature"])
        .await;
    let parsed: serde_json::Value = serde_json::from_str(&message).unwrap();
    assert_eq!(
        parsed["totals"]["runs"], 1,
        "the base-branch run is preserved: {message}"
    );
}

/// `prune` preserves base-side history even when the base was merged into the branch,
/// so the merge-base sits off the context's first-parent line. The fork point on that
/// line (the newest shared commit) still divides base-side history from the branch's
/// own commits, so a shared commit is never deleted without the base-branch opt-in.
#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn prune_preserves_base_side_when_the_base_is_merged_in() {
    let workspace = Workspace::repo(&storage_only_config());
    // master:  root - c1 - c2
    //                  \
    // feature:          f1 - M - f2   (M merges master's tip c2 into feature)
    // merge-base(feature, master) = c2, off feature's first-parent line
    // [root, c1, f1, M, f2]; the fork point is the newest shared commit, c1.
    workspace.commit("c1");
    workspace.seed_callgrind("c1", 100.0);
    workspace.checkout_new_branch("feature");
    workspace.commit("f1");
    workspace.seed_callgrind("f1", 100.0);
    workspace.checkout("master");
    workspace.commit("c2");
    workspace.checkout("feature");
    workspace.merge("master", "M");
    workspace.commit("f2");
    workspace.seed_callgrind("f2", 100.0);

    // Only the branch's own commits (f1, f2) are eligible; the shared c1 is base-side.
    let message = workspace
        .drive_json(&["prune", "--all", "--context", "feature"])
        .await;
    let parsed: serde_json::Value = serde_json::from_str(&message).unwrap();
    assert_eq!(
        parsed["totals"]["runs"], 2,
        "only the two branch-own runs are deleted, not the shared base commit: {message}"
    );

    // The shared base commit c1 survives on the context's first-parent line.
    let message = workspace
        .drive_json(&["list", "runs", "--context", "feature"])
        .await;
    let parsed: serde_json::Value = serde_json::from_str(&message).unwrap();
    assert_eq!(
        parsed["totals"]["runs"], 1,
        "the shared base-side run is preserved: {message}"
    );
}

/// `--clean` deletes clean runs while leaving dirty snapshots
/// in place — the inverse of `--dirty`.
#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn prune_clean_scope_removes_clean_and_keeps_dirty() {
    let workspace = Workspace::repo(&storage_only_config());
    workspace.commit_dated("2024-01-01", "c1");
    workspace.seed_callgrind("c1", 100.0);
    workspace.checkout_new_branch("feature");
    workspace.commit_dated("2024-01-02", "f1");
    workspace.seed_callgrind("f1", 100.0);
    workspace.seed_dirty_callgrind("2024-01-03", "f1", 200.0);
    let f1 = workspace.commit("f1");

    // `--clean` narrowed to f1 removes only its clean run.
    let message = workspace.drive_json(&["prune", &f1, "--clean"]).await;
    let parsed: serde_json::Value = serde_json::from_str(&message).unwrap();
    assert_eq!(
        parsed["totals"]["runs"], 1,
        "only the clean f1 run: {message}"
    );

    // The dirty f1 snapshot survives: a `--dirty` pass still finds it.
    let message = workspace
        .drive_json(&["prune", "--dirty", "--dry-run"])
        .await;
    let parsed: serde_json::Value = serde_json::from_str(&message).unwrap();
    assert_eq!(parsed["totals"]["runs"], 1, "dirty f1 survived: {message}");
}

/// Pruning a run never removes a blessing; only `--include-blessings` (or `unbless`)
/// does. The default `--all` prune deletes the clean run but leaves the blessing,
/// which `--include-blessings` then removes.
#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn include_blessings_is_required_to_prune_a_blessing() {
    let workspace = Workspace::clean_repo(&storage_only_config());
    workspace.seed_rising_callgrind_history();
    let head = workspace.head();

    workspace.drive(&["bless", "nm/nm::observe"]).await.unwrap();

    // The blessing is recorded at HEAD.
    let message = workspace.drive_json(&["list", "blessings"]).await;
    let parsed: serde_json::Value = serde_json::from_str(&message).unwrap();
    assert_eq!(
        parsed["blessings"].as_array().unwrap().len(),
        1,
        "{message}"
    );

    // The default `--all` prune removes HEAD's clean run but leaves the blessing.
    // The checkout is the base branch tip, so the base-branch guard must be confirmed.
    let message = workspace
        .drive_json(&["prune", &head, "--all", "--prune-base"])
        .await;
    let parsed: serde_json::Value = serde_json::from_str(&message).unwrap();
    assert_eq!(parsed["totals"]["runs"], 1, "the clean HEAD run: {message}");
    assert_eq!(parsed["totals"]["blessings"], 0, "{message}");

    // The blessing survives the run prune.
    let message = workspace.drive_json(&["list", "blessings"]).await;
    let parsed: serde_json::Value = serde_json::from_str(&message).unwrap();
    assert_eq!(
        parsed["blessings"].as_array().unwrap().len(),
        1,
        "the blessing outlived its clean run: {message}"
    );

    // `--include-blessings` removes the now-orphan blessing.
    let message = workspace
        .drive_json(&["prune", &head, "--include-blessings", "--prune-base"])
        .await;
    let parsed: serde_json::Value = serde_json::from_str(&message).unwrap();
    assert_eq!(parsed["totals"]["runs"], 0, "no runs left: {message}");
    assert_eq!(parsed["totals"]["blessings"], 1, "{message}");

    // The blessing is gone.
    let message = workspace.drive_json(&["list", "blessings"]).await;
    let parsed: serde_json::Value = serde_json::from_str(&message).unwrap();
    assert!(
        parsed["blessings"].as_array().unwrap().is_empty(),
        "the blessing was removed by --include-blessings: {message}"
    );
}

/// `--all` and `--include-blessings` are additive and combine in a single
/// invocation: the run and its blessing are both removed in one pass.
#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn all_and_include_blessings_combine_in_one_invocation() {
    let workspace = Workspace::clean_repo(&storage_only_config());
    workspace.seed_rising_callgrind_history();
    let head = workspace.head();

    workspace.drive(&["bless", "nm/nm::observe"]).await.unwrap();

    // A single pass with both flags removes the clean HEAD run and its blessing.
    let message = workspace
        .drive_json(&[
            "prune",
            &head,
            "--all",
            "--include-blessings",
            "--prune-base",
        ])
        .await;
    let parsed: serde_json::Value = serde_json::from_str(&message).unwrap();
    assert_eq!(parsed["totals"]["runs"], 1, "the clean HEAD run: {message}");
    assert_eq!(parsed["totals"]["blessings"], 1, "the blessing: {message}");

    // Both are gone.
    let message = workspace.drive_json(&["list", "blessings"]).await;
    let parsed: serde_json::Value = serde_json::from_str(&message).unwrap();
    assert!(
        parsed["blessings"].as_array().unwrap().is_empty(),
        "the blessing was removed alongside its run: {message}"
    );
}

/// Like `analyze`/`list`, `prune` requires a repository to resolve the topology;
/// without one it errors rather than removing nothing silently.
#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn prune_without_a_repository_errors() {
    let workspace = Workspace::new(&storage_only_config());

    let error = workspace.drive(&["prune", "--dirty"]).await.unwrap_err();
    assert!(error.find_source::<cbh_analyze::AnalyzeError>().is_some());
}
