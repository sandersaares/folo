use crate::harness::*;

/// An empty history analyzes cleanly and states that it tested nothing.
#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn analyze_empty_history_reports_that_nothing_was_analyzed() {
    let workspace = Workspace::repo(&storage_only_config());

    let outcome = workspace.drive(&["analyze"]).await.unwrap();
    let RunOutcome::Analyzed {
        report,
        regressions,
        ..
    } = outcome
    else {
        panic!("expected an analyzed outcome, got {outcome:?}");
    };
    assert_eq!(regressions, 0);
    // An empty history judged nothing, so the report must not dress its silence as
    // an all-clear: the verdict says nothing was analyzed, there is no coverage to
    // state, and nothing was measured against the floor. The empty-outcome hint
    // carries the explanation instead.
    assert!(
        report.contains("Nothing was analyzed, so no change could be detected."),
        "{report}"
    );
    assert!(!report.contains("No notable changes detected."), "{report}");
    assert!(!report.contains("series judged"), "{report}");
    assert!(
        !report.contains("none moved beyond the measurement floor"),
        "{report}"
    );
}

/// A rising history is flagged as a regression end to end.
#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn analyze_detects_regression_in_seeded_history() {
    let workspace = Workspace::repo(&storage_only_config());
    workspace.seed_rising_callgrind_history();

    let outcome = workspace.drive(&["analyze"]).await.unwrap();
    let RunOutcome::Analyzed {
        report,
        regressions,
        ..
    } = outcome
    else {
        panic!("expected an analyzed outcome, got {outcome:?}");
    };
    assert_eq!(regressions, 1);
    assert!(report.contains("regression"), "{report}");
    assert!(report.contains("nm::observe/pull"), "{report}");
    assert!(report.contains("instruction_count"), "{report}");
}

/// History written by an older tool can carry metric kinds since dropped (the
/// build-layout-volatile Callgrind events). `analyze` must skip those kinds and keep
/// folding the retained ones rather than aborting the whole run on an unknown-variant
/// parse error — the exact failure a stored `conditional_branch_misses` metric caused
/// on the real history. The `instruction_count` regression must still surface.
#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn analyze_tolerates_legacy_metric_kinds_in_stored_history() {
    let workspace = Workspace::repo(&storage_only_config());
    for (index, date) in sequential_dates("2024-01-01", MIN_SERIES_POINTS)
        .into_iter()
        .enumerate()
    {
        let label = format!("c{}", index + 1);
        workspace.commit_dated(&date, &label);
        let value = if index < MIN_REGIME { 100.0 } else { 130.0 };
        workspace.seed_callgrind_with_legacy_metric(&label, value);
    }

    let outcome = workspace.drive(&["analyze"]).await.unwrap();
    let RunOutcome::Analyzed {
        report,
        regressions,
        ..
    } = outcome
    else {
        panic!("expected an analyzed outcome, got {outcome:?}");
    };
    assert_eq!(regressions, 1);
    assert!(report.contains("nm::observe/pull"), "{report}");
    assert!(report.contains("instruction_count"), "{report}");
}

/// **Detector-liveness canary. Do not relax this assertion.**
///
/// A doubling is the largest, cleanest signal this tool can be shown: two full
/// regimes, flat within each, an exact 2x step between them, far above every
/// measurement floor and every practical-significance threshold. If the pipeline
/// cannot report *this*, it cannot report anything, and the tool's silence on real
/// history would mean nothing.
///
/// The gates that decide whether a series is judged at all, the family size the
/// false-discovery correction divides by, and the correction itself are all tuned
/// against false positives. Each tightening moves the tool toward reporting less.
/// This test is the floor under that: it drives the whole `analyze` pipeline — data
/// loading, selection, series reconstruction, detection, correction and rendering —
/// and fails loudly the moment a tightening switches detection off.
///
/// If this test ever fails, the fix is in the detector or its gates. Weakening the
/// assertion, seeding a larger step, or deleting the test defeats its only purpose.
#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn analyze_canary_an_unmistakable_step_is_always_reported() {
    let workspace = Workspace::repo(&storage_only_config());
    workspace.seed_stepped_callgrind("2024-01-01", "c", 100.0, 200.0);

    let report = workspace.drive_json(&["analyze"]).await;
    let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();

    // The series reached the detectors: silence here would have been vacuous.
    assert_eq!(parsed["census"]["total"], 1, "{report}");
    assert_eq!(parsed["census"]["judged"], 1, "{report}");
    assert_eq!(parsed["census"]["unjudged"], 0, "{report}");

    // And the detectors called it.
    assert_eq!(
        parsed["regressions"], 1,
        "a doubling must always be reported: {report}"
    );
    let finding = &parsed["findings"][0];
    assert_eq!(finding["direction"], "regression", "{report}");
    assert_eq!(finding["kind"], "instruction_count", "{report}");
    // Nothing on the path from the seeded values to this field approximates: the
    // change-point detector represents each regime by its median, which for a run
    // of identical values is that value itself; 200 - 100 and 100 / 100 are both
    // exact in binary floating point; and the JSON projection copies the `f64`
    // through untouched. So the doubling is asserted exactly. A tolerance would
    // let a wrong baseline or a wrong relative-delta formula pass unnoticed while
    // the canary still reported green.
    //
    // The method is pinned because these exact values are the two-regime model's.
    // The drift detector describes the same fixture with a fitted line, whose
    // endpoints straddle the two levels rather than sitting on them, so it would
    // report a materially different baseline, latest and movement. Pinning the
    // method keeps the three assertions below anchored to the model that produces
    // them.
    assert_eq!(finding["method"], "change_point", "{report}");
    assert_eq!(finding["baseline"], 100.0, "{report}");
    assert_eq!(finding["latest"], 200.0, "{report}");
    assert_eq!(
        finding["relative_delta"], 1.0,
        "the reported move must be exactly the seeded doubling: {report}"
    );
}

/// The converse of the canary: a series the pipeline cannot judge is *accounted
/// for*, not silently dropped. Five points is half the evidence the detectors
/// require, so nothing is tested — and the report says exactly that instead of
/// rendering the same "no notable changes" an all-clear would produce.
#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn analyze_reports_a_series_it_could_not_judge_rather_than_omitting_it() {
    let workspace = Workspace::repo(&storage_only_config());
    for (index, date) in sequential_dates("2024-01-01", MIN_REGIME)
        .into_iter()
        .enumerate()
    {
        let label = format!("c{}", index + 1);
        workspace.commit_dated(&date, &label);
        // The same doubling the canary drives, on a series too short to judge.
        let value = if index * 2 < MIN_REGIME { 100.0 } else { 200.0 };
        workspace.seed_callgrind(&label, value);
    }

    let report = workspace.drive_json(&["analyze"]).await;
    let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
    assert_eq!(parsed["regressions"], 0, "{report}");
    assert_eq!(parsed["census"]["total"], 1, "{report}");
    assert_eq!(parsed["census"]["judged"], 0, "{report}");
    assert_eq!(
        parsed["census"]["reasons"][0]["reason"], "too_few_points",
        "{report}"
    );
    assert_eq!(parsed["census"]["reasons"][0]["count"], 1, "{report}");

    let text = workspace.drive(&["analyze"]).await.unwrap();
    let RunOutcome::Analyzed { report, .. } = text else {
        panic!("expected an analyzed outcome, got {text:?}");
    };
    assert!(
        report.contains("in-scope series judged: 0 of 1"),
        "{report}"
    );
    assert!(
        report.contains("this silence is not evidence that nothing moved"),
        "{report}"
    );
    assert!(
        report.contains("Not judged: 1 series with too few points in the analyzed window."),
        "{report}"
    );
}

/// A rising Criterion `wall_time` history is flagged as a regression, proving the
/// analyzer reconstructs series across a machine-keyed partition too.
#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn analyze_detects_criterion_wall_time_regression() {
    let workspace = Workspace::repo(&storage_only_config());
    workspace.seed_rising_criterion_history("mk-fixed");

    let outcome = workspace
        .drive(&[
            "analyze",
            "--target-triple",
            "x86_64-pc-windows-msvc",
            "--machine-key",
            "mk-fixed",
        ])
        .await
        .unwrap();
    let RunOutcome::Analyzed {
        report,
        regressions,
        ..
    } = outcome
    else {
        panic!("expected an analyzed outcome, got {outcome:?}");
    };
    assert_eq!(regressions, 1);
    assert!(report.contains("time/capture/std_instant"), "{report}");
    assert!(report.contains("wall_time"), "{report}");
}

/// A flagged regression never fails the process: findings are advisory, so the
/// exit code reflects only whether the tool ran, not what it found.
#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn analyze_with_regression_is_always_successful() {
    let workspace = Workspace::repo(&storage_only_config());
    workspace.seed_rising_callgrind_history();

    let outcome = workspace.drive(&["analyze"]).await.unwrap();
    assert!(
        outcome.is_success(),
        "findings must never fail the build: {outcome:?}"
    );
}

/// `--json <path>` writes a structured, parseable report.
#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn analyze_json_output_is_structured() {
    let workspace = Workspace::repo(&storage_only_config());
    workspace.seed_rising_callgrind_history();

    let report = workspace.drive_json(&["analyze"]).await;
    let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
    assert_eq!(parsed["project"], "testproj");
    // The report names the analyzed tip commit (a full-length hex commit ID) so a
    // reader can identify exactly which commit it describes; the seeded checkout is
    // clean. Accept either length Git may use (40 for SHA-1, 64 for SHA-256).
    let tip = parsed["tip_commit"].as_str().unwrap();
    assert!(
        (tip.len() == 40 || tip.len() == 64)
            && tip.chars().all(|character| character.is_ascii_hexdigit()),
        "tip_commit should be a full-length hex commit ID: {tip:?}"
    );
    assert_eq!(parsed["tip_dirty"], false);
    assert_eq!(parsed["regressions"], 1);
    assert_eq!(parsed["findings"][0]["direction"], "regression");
}

/// `--markdown <path>` writes per-finding blocks mirroring the text report.
#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn analyze_markdown_output_renders_blocks() {
    let workspace = Workspace::repo(&storage_only_config());
    workspace.seed_rising_callgrind_history();

    let report = workspace.drive_markdown(&["analyze"]).await;
    assert!(
        report.contains("# Benchmark history analysis: testproj"),
        "{report}"
    );
    // The header names the analyzed tip commit so the reader knows which commit the
    // findings describe.
    assert!(report.contains("- Commit: "), "{report}");
    // Findings render as heading + bold-headline blocks; there is no table.
    assert!(!report.contains("| Change | Direction |"), "{report}");
    // Each finding leads with its benchmark id as a `###` chapter title (nested under
    // the set `##`), then a bold percentage headline naming the metric.
    assert!(report.contains("\n### `"), "{report}");
    assert!(report.contains("** `"), "{report}");
    assert!(report.contains("via change point"), "{report}");
}

/// Every requested file can be rendered from one analysis pass while text is suppressed.
#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn analyze_writes_reports_and_outcome_from_one_suppressed_text_pass() {
    let workspace = Workspace::repo(&storage_only_config());
    workspace.seed_rising_callgrind_history();

    let outcome = workspace
        .drive(&[
            "analyze",
            "--no-text",
            "--markdown",
            "report.md",
            "--json",
            "report.json",
            "--outcome",
            "outcome.txt",
        ])
        .await
        .unwrap();
    let RunOutcome::Analyzed {
        report, outcome, ..
    } = outcome
    else {
        panic!("expected an analyzed outcome, got {outcome:?}");
    };
    assert!(report.is_empty(), "{report}");
    assert_eq!(outcome, AnalysisOutcome::Findings);

    let markdown = workspace.read("report.md");
    let json = workspace.read("report.json");
    let outcome = workspace.read("outcome.txt");
    assert!(markdown.is_some(), "the Markdown report should be written");
    assert!(json.is_some(), "the JSON report should be written");
    assert_eq!(outcome.as_deref(), Some("findings"));

    let markdown = markdown.unwrap();
    assert!(
        markdown.contains("# Benchmark history analysis: testproj"),
        "{markdown}"
    );
    let json = json.unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed["project"], "testproj");
    assert_eq!(parsed["outcome"], "findings");
}

/// `--markdown-summary <path>` renders a flat, ungrouped condensed report that omits
/// the per-set headings the full Markdown report carries. With few findings there is
/// no truncation note.
#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn analyze_markdown_summary_renders_a_flat_report() {
    let workspace = Workspace::repo(&storage_only_config());
    workspace.seed_rising_callgrind_history();

    let summary = workspace.drive_markdown_summary(&["analyze"]).await;

    assert!(
        summary.contains("# Benchmark history analysis: testproj"),
        "{summary}"
    );
    // Each finding leads with its benchmark id as a `##` chapter title, then a bold
    // percentage headline naming the metric — the same block the full report
    // uses, one heading level up.
    assert!(summary.contains("\n## `"), "{summary}");
    assert!(summary.contains("** `"), "{summary}");
    // A single seeded regression is well within the top-20 cap, so no truncation note
    // is added.
    assert!(
        !summary.contains("Showing the top"),
        "an untruncated summary must not claim it is partial: {summary}"
    );
    // The per-set `## engine/triple/machine` grouping is deliberately dropped from the
    // summary, unlike the full Markdown report; only the flat finding headings remain.
    assert!(
        !summary.contains("## callgrind"),
        "the summary must be flat, without per-set headings: {summary}"
    );
    // Each flat finding instead carries the discriminant flags of its partition as a footer,
    // so a reader who loses the per-set grouping still knows how to query the exact set.
    assert!(
        summary.contains(
            "_Filter:_ `--engine callgrind --target-triple x86_64-unknown-linux-gnu --machine-key harness-auto-machine`"
        ),
        "{summary}"
    );
}

/// When an analysis produces more findings than the summary cap, `--markdown-summary`
/// keeps only the top 10 by magnitude and states the total, while a full `--markdown`
/// report from the same run still lists every finding.
#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn analyze_markdown_summary_caps_findings_and_names_the_total() {
    let workspace = Workspace::repo(&storage_only_config());
    // Seed more distinct rising benchmarks than the top-10 cap so truncation engages.
    workspace.seed_many_rising_callgrind_history(25);

    let outcome = workspace
        .drive(&[
            "analyze",
            "--no-text",
            "--markdown",
            "full.md",
            "--markdown-summary",
            "summary.md",
        ])
        .await
        .unwrap();
    let RunOutcome::Analyzed { regressions, .. } = outcome else {
        panic!("expected an analyzed outcome, got {outcome:?}");
    };
    assert_eq!(regressions, 25, "every seeded benchmark should regress");

    let summary = workspace
        .read("summary.md")
        .expect("the Markdown summary should be written");
    let full = workspace
        .read("full.md")
        .expect("the full Markdown report should be written");

    // The header carries the *true* total (all 25), not the retained subset.
    assert!(summary.contains("- Regressions: 25"), "{summary}");
    // The truncation note names how many of the total are shown.
    assert!(
        summary.contains("Showing the top 10 of 25 findings by magnitude."),
        "{summary}"
    );
    // Exactly the cap of finding headlines survives in the summary. Each finding
    // headline carries a bold metric name (`** ` before the backtick), which appears
    // nowhere else, so it counts blocks.
    let summary_blocks = summary.matches("** `").count();
    assert_eq!(
        summary_blocks, 10,
        "summary should keep only the cap: {summary}"
    );
    // The full report from the same run still lists every finding.
    let full_blocks = full.matches("** `").count();
    assert_eq!(
        full_blocks, 25,
        "full report should keep every finding: {full}"
    );
}

/// `--no-text` suppresses stdout text while still writing a JSON report.
#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn analyze_no_text_suppresses_stdout_alongside_json_file() {
    let workspace = Workspace::repo(&storage_only_config());
    workspace.seed_rising_callgrind_history();

    let outcome = workspace
        .drive(&["analyze", "--no-text", "--json", "report.json"])
        .await
        .unwrap();
    let RunOutcome::Analyzed { report, .. } = outcome else {
        panic!("expected an analyzed outcome, got {outcome:?}");
    };
    assert!(report.is_empty(), "{report}");

    let json = workspace
        .read("report.json")
        .expect("the JSON report should be written");
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed["project"], "testproj");
}

/// Text remains on by default when a JSON file is also requested.
#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn analyze_default_text_coexists_with_json_file() {
    let workspace = Workspace::repo(&storage_only_config());
    workspace.seed_rising_callgrind_history();

    let outcome = workspace
        .drive(&["analyze", "--json", "report.json"])
        .await
        .unwrap();
    let RunOutcome::Analyzed { report, .. } = outcome else {
        panic!("expected an analyzed outcome, got {outcome:?}");
    };
    assert!(!report.is_empty(), "text should be rendered by default");
    assert!(report.contains("regression"), "{report}");

    let json = workspace
        .read("report.json")
        .expect("the JSON report should be written");
    let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed["project"], "testproj");
}

/// `--since` excludes points before the cutoff, so an early spike is dropped and
/// the remaining flat history shows no change. The retained history is
/// [`MIN_SERIES_POINTS`] points — long enough that the detector actually judges
/// the series, so the "no change" result reflects a flat series rather than a
/// series too short to analyse.
#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn analyze_since_filters_old_points() {
    let workspace = Workspace::repo(&storage_only_config());
    // A flat recent history (no change), preceded long ago by an unrelated point.
    workspace.commit_dated("2020-01-01", "c0");
    workspace.seed_callgrind("c0", 999.0);
    for (index, date) in sequential_dates("2024-01-01", MIN_SERIES_POINTS)
        .into_iter()
        .enumerate()
    {
        let label = format!("c{}", index + 1);
        workspace.commit_dated(&date, &label);
        workspace.seed_callgrind(&label, 100.0);
    }

    let report = workspace
        .drive_json(&["analyze", "--since", "2024-01-01"])
        .await;
    let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
    assert_eq!(
        parsed["runs"],
        u64::try_from(MIN_SERIES_POINTS).unwrap(),
        "the pre-cutoff point is excluded, leaving exactly the judged flat window: {report}"
    );
    assert_eq!(parsed["regressions"], 0, "{report}");
}

/// `--engine` restricts analysis to one engine's partition.
#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn analyze_engine_filters_partition() {
    let workspace = Workspace::repo(&storage_only_config());
    workspace.commit_dated("2024-01-01", "c1");
    workspace.seed_callgrind("c1", 100.0);
    // A criterion-partition object that the callgrind filter must skip. Its commit
    // segment is never read because the engine discriminant excludes it from listing.
    workspace.seed(
        "v1/testproj/objects/criterion/x86_64-pc-windows-msvc/m1/abc123/clean.json",
        &ir_result_set(1, "c1", 100.0),
    );

    let report = workspace
        .drive_json(&["analyze", "--engine", "callgrind"])
        .await;
    let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
    assert_eq!(
        parsed["runs"], 1,
        "only the callgrind object is loaded: {report}"
    );
}

/// Selecting no output for `analyze` is reported as an analysis error.
#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn analyze_rejects_no_output_selected() {
    let workspace = Workspace::repo(&storage_only_config());

    let error = workspace
        .drive(&["analyze", "--no-text"])
        .await
        .unwrap_err();
    assert!(error.find_source::<cbh_analyze::AnalyzeError>().is_some());
}

/// A benchmark-id prefix narrows analysis to the matching benchmarks: a
/// regression in another benchmark is invisible when the filter selects a flat one.
#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn analyze_prefix_filter_excludes_other_benchmarks() {
    let workspace = Workspace::repo(&storage_only_config());
    // `alpha` stays flat while `beta` climbs into a regression.
    for (index, date) in sequential_dates("2024-01-01", MIN_SERIES_POINTS)
        .into_iter()
        .enumerate()
    {
        let label = format!("c{}", index + 1);
        workspace.commit_dated(&date, &label);
        let beta = if index < MIN_REGIME { 100.0 } else { 130.0 };
        workspace.seed_two_benchmarks(&label, 100.0, beta);
    }

    // Unfiltered, the `beta` climb flags as a regression.
    let RunOutcome::Analyzed { regressions, .. } = workspace.drive(&["analyze"]).await.unwrap()
    else {
        panic!("expected an analyzed outcome");
    };
    assert_eq!(regressions, 1, "the beta climb should flag unfiltered");

    // Restricting to the flat `alpha` benchmark family hides it.
    let RunOutcome::Analyzed {
        regressions,
        report,
        ..
    } = workspace.drive(&["analyze", "alpha/"]).await.unwrap()
    else {
        panic!("expected an analyzed outcome");
    };
    assert_eq!(regressions, 0, "the alpha series is flat: {report}");
    // The filtered-in series was judged, so this silence is about `alpha` being flat
    // rather than about the filter leaving nothing to test.
    assert_history_was_judged(&report);
}

/// Two benchmarks regressing by different magnitudes produce two findings that the
/// report ranks largest-relative-move first, end to end through real storage and
/// rendering.
#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn analyze_ranks_findings_by_relative_move_across_benchmarks() {
    let workspace = Workspace::repo(&storage_only_config());
    // `alpha` doubles (+100%); `beta` steps up +5%.
    for (index, date) in sequential_dates("2024-01-01", MIN_SERIES_POINTS)
        .into_iter()
        .enumerate()
    {
        let label = format!("c{}", index + 1);
        workspace.commit_dated(&date, &label);
        let (alpha, beta) = if index < MIN_REGIME {
            (100.0, 100.0)
        } else {
            (200.0, 105.0)
        };
        workspace.seed_two_benchmarks(&label, alpha, beta);
    }

    let report = workspace.drive_json(&["analyze"]).await;
    let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
    assert_eq!(
        parsed["regressions"], 2,
        "both benchmarks regress: {report}"
    );

    let findings = parsed["findings"].as_array().unwrap();
    assert_eq!(findings.len(), 2, "{report}");
    // The larger relative move (alpha, +100%) ranks ahead of the smaller (beta, +5%).
    assert_eq!(findings[0]["segments"][0], "alpha", "{report}");
    assert_eq!(findings[0]["segments"][1], "alpha::bench", "{report}");
    assert_eq!(findings[1]["segments"][0], "beta", "{report}");

    // The Markdown rendering preserves the same ranked order in its finding blocks.
    let markdown = workspace.drive_markdown(&["analyze"]).await;
    let alpha_row = markdown.find("alpha::bench").unwrap();
    let beta_row = markdown.find("beta::bench").unwrap();
    assert!(
        alpha_row < beta_row,
        "the larger move must precede the smaller:\n{markdown}"
    );
}

/// Branch mode reports a speedup as a finding in its own right: a tip that runs
/// faster than the base is an improvement, counted apart from the regressions,
/// and the improvement tally is stated because the mode looks in both directions.
#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn analyze_branch_mode_reports_an_improvement_without_regression() {
    let workspace = Workspace::repo(&storage_only_config());
    for (index, date) in sequential_dates("2024-01-01", MIN_SERIES_POINTS)
        .into_iter()
        .enumerate()
    {
        let label = format!("c{}", index + 1);
        workspace.commit_dated(&date, &label);
        workspace.seed_callgrind(&label, 100.0);
    }
    workspace.checkout_new_branch("feature");
    workspace.commit_dated("2024-02-01", "f1");
    workspace.seed_callgrind("f1", 70.0);

    let report = workspace.drive_json(&["analyze"]).await;
    let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
    assert_eq!(parsed["mode"], "branch", "{report}");
    assert_eq!(
        parsed["regressions"], 0,
        "a drop in instructions is not a regression: {report}"
    );
    assert_eq!(parsed["improvements"], 1, "{report}");
    assert_eq!(parsed["findings"][0]["direction"], "improvement");
}

/// Run-to-run jitter on a Criterion `wall_time` series whose underlying mean is
/// flat produces no finding at all: the rank-sum gate finds no real separation
/// between the regimes and the trend test finds no monotonic drift, so substantial
/// per-point noise never manufactures a spurious regression or improvement. This is
/// the core signal-to-noise guarantee for the noisy engine. The series carries
/// [`MIN_SERIES_POINTS`] points, so the detector genuinely judges it rather than
/// skipping it as too short.
#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn analyze_criterion_jitter_is_not_flagged() {
    let workspace = Workspace::repo(&storage_only_config());
    // Points oscillating roughly +/-15% around 20ns with no sustained shift, one
    // per commit, enough of them that the series clears the minimum-length gate.
    let jitter = [20.0, 23.0, 18.0, 21.0, 19.0, 22.0, 18.0, 22.0, 19.0, 21.0];
    assert!(
        jitter.len() >= MIN_SERIES_POINTS,
        "the jitter series must clear the detector's minimum length so its silence is judged"
    );
    for (index, (date, value)) in sequential_dates("2024-02-01", jitter.len())
        .into_iter()
        .zip(jitter)
        .enumerate()
    {
        let label = format!("d{}", index + 1);
        workspace.commit_dated(&date, &label);
        workspace.seed_criterion(&label, "mk", value);
    }

    let report = workspace
        .drive_json(&[
            "analyze",
            "--target-triple",
            "x86_64-pc-windows-msvc",
            "--machine-key",
            "mk",
        ])
        .await;
    let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
    assert_eq!(
        parsed["runs"],
        u64::try_from(jitter.len()).unwrap(),
        "every jittery point is loaded: {report}"
    );
    assert_eq!(
        parsed["census"]["judged"], 1,
        "the detectors reached a verdict, so this silence is one: {report}"
    );
    assert_eq!(
        parsed["regressions"], 0,
        "jitter must not read as a regression: {report}"
    );
    assert!(
        parsed["findings"].as_array().unwrap().is_empty(),
        "noisy jitter around a flat mean produces no findings at all: {report}"
    );
}

/// A slow, monotonic Criterion `wall_time` drift is flagged as a `drift` finding
/// (not a change-point): the Mann-Kendall trend is significant, the Theil-Sen line
/// fits the gentle ramp better than any single step, and the total movement clears
/// the practical-magnitude floor. This exercises the second detector and the
/// model-fit arbitration that routes ramps to drift and steps to change-point.
#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn analyze_criterion_slow_drift_is_flagged_as_drift() {
    let workspace = Workspace::repo(&storage_only_config());
    // A gentle one-nanosecond-per-commit ramp from 20ns upward across
    // `MIN_SERIES_POINTS` commits, the minimum length the drift detector considers.
    for (index, date) in sequential_dates("2024-02-01", MIN_SERIES_POINTS)
        .into_iter()
        .enumerate()
    {
        let label = format!("d{}", index + 1);
        workspace.commit_dated(&date, &label);
        // `index` spans a small fixed range; a u16 cast keeps the ramp exact.
        let value = 20.0 + f64::from(u16::try_from(index).unwrap());
        workspace.seed_criterion(&label, "mk", value);
    }

    let report = workspace
        .drive_json(&[
            "analyze",
            "--target-triple",
            "x86_64-pc-windows-msvc",
            "--machine-key",
            "mk",
        ])
        .await;
    let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
    assert_eq!(
        parsed["regressions"], 1,
        "the upward drift is a regression: {report}"
    );
    assert_eq!(parsed["findings"][0]["method"], "drift", "{report}");
    assert_eq!(parsed["findings"][0]["direction"], "regression", "{report}");
}

/// A Callgrind series whose instruction count steps up by a single count - a 0.1%
/// move - is below the practical-magnitude floor every engine now shares, so it is
/// suppressed. No engine is exact (even a CPU simulator's counts jitter run to run),
/// and a sub-percent change is not worth a human's attention regardless of how
/// certainly it was measured. The series carries [`MIN_SERIES_POINTS`] points, so
/// the suppression is the floor's doing, not a series too short to judge.
#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn analyze_callgrind_sub_floor_step_is_suppressed() {
    let workspace = Workspace::repo(&storage_only_config());
    workspace.seed_stepped_callgrind("2024-01-01", "c", 1000.0, 1001.0);

    let report = workspace.drive_json(&["analyze"]).await;
    let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
    assert_eq!(
        parsed["runs"],
        u64::try_from(MIN_SERIES_POINTS).unwrap(),
        "the full series is loaded: {report}"
    );
    assert_eq!(
        parsed["census"]["judged"], 1,
        "the detectors reached a verdict, so this silence is one: {report}"
    );
    assert_eq!(
        parsed["regressions"], 0,
        "a 0.1% step is below the practical-magnitude floor and must not flag: {report}"
    );
    let findings = parsed["findings"].as_array().expect("findings is an array");
    assert!(
        findings.is_empty(),
        "the sub-floor move is fully suppressed, so no finding of any direction reaches \
         the report: {report}"
    );
}

/// A Callgrind series that steps up by 5% clears the practical-magnitude floor and is
/// flagged as a `change_point`. Callgrind is low-noise but not exact, so it passes
/// through the same rank test, practical floor and residual gate as any other engine
/// rather than being trusted as deterministic.
#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn analyze_callgrind_step_above_the_practical_floor_is_flagged() {
    let workspace = Workspace::repo(&storage_only_config());
    workspace.seed_stepped_callgrind("2024-01-01", "c", 1000.0, 1050.0);

    let report = workspace.drive_json(&["analyze"]).await;
    let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
    assert_eq!(
        parsed["regressions"], 1,
        "a 5% Callgrind step clears the practical floor and must flag: {report}"
    );
    assert_eq!(parsed["findings"][0]["method"], "change_point", "{report}");
    assert_eq!(parsed["findings"][0]["direction"], "regression", "{report}");
}

/// An `alloc_tracker` series whose allocated-bytes figure steps up by 5% is flagged as
/// a `change_point`. Allocation figures are not deterministic - warmup and
/// buffer-resize allocations jitter the per-iteration count - so they clear the same
/// gates as any other engine; the constant allocation count produces no finding.
#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn analyze_alloc_tracker_step_is_flagged_as_change_point() {
    let workspace = Workspace::repo(&storage_only_config());
    // A flat allocated-bytes baseline that steps up by 5% at the midpoint; the
    // allocation count stays constant throughout. The series holds
    // `MIN_SERIES_POINTS` points so the step clears the change-point persistence gate.
    for (index, date) in sequential_dates("2024-03-01", MIN_SERIES_POINTS)
        .into_iter()
        .enumerate()
    {
        let label = format!("a{}", index + 1);
        workspace.commit_dated(&date, &label);
        let bytes = if index < MIN_REGIME { 200.0 } else { 210.0 };
        workspace.seed_alloc_tracker(&label, "allocate_vec", bytes, 2.0);
    }

    let report = workspace.drive_json(&["analyze"]).await;
    let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
    assert_eq!(
        parsed["regressions"], 1,
        "a 5% allocated-bytes step clears the practical floor and must flag: {report}"
    );
    assert_eq!(parsed["findings"][0]["method"], "change_point", "{report}");
    assert_eq!(parsed["findings"][0]["direction"], "regression", "{report}");
    assert_eq!(parsed["findings"][0]["kind"], "allocated_bytes", "{report}");
    assert!(report.contains("allocate_vec"), "{report}");
}

/// A gapped `alloc_tracker` history - where some commits measure a different
/// operation - reconstructs each series across only the commits that measured it,
/// treating an unmeasured commit as a missing observation rather than a drop to
/// zero. The `allocate_vec` series spanning the gaps still surfaces its real step.
#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn analyze_alloc_tracker_ignores_gaps_in_a_sparse_history() {
    let workspace = Workspace::repo(&storage_only_config());
    // `allocate_vec` is measured only at every other commit; the intervening
    // commits measure `free_box` instead, leaving `allocate_vec` unobserved there.
    // Its step spans the gaps: `MIN_REGIME` baseline observations, then
    // `MIN_REGIME` raised ones, so the observed series clears `MIN_SERIES_POINTS`.
    // The history ends on an `allocate_vec` commit so that series is present at the
    // tip and survives the ghost filter (`free_box` is the ghost here, and being
    // flat it produces no finding regardless).
    let mut allocate_vec_bytes = vec![200.0; MIN_REGIME];
    allocate_vec_bytes.extend(std::iter::repeat_n(260.0, MIN_REGIME));
    let mut day = 1;
    for bytes in allocate_vec_bytes {
        workspace.commit_dated(&format!("2024-04-{day:02}"), &format!("g{day}"));
        workspace.seed_alloc_tracker(&format!("g{day}"), "free_box", 8.0, 1.0);
        day += 1;
        workspace.commit_dated(&format!("2024-04-{day:02}"), &format!("g{day}"));
        workspace.seed_alloc_tracker(&format!("g{day}"), "allocate_vec", bytes, 2.0);
        day += 1;
    }

    // Only `allocate_vec`'s allocated-bytes step is a finding. A gap read as a drop
    // to zero would manufacture wild swings; instead the series is contiguous.
    let report = workspace.drive_json(&["analyze"]).await;
    let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
    assert_eq!(
        parsed["regressions"], 1,
        "the gapped series' real step flags: {report}"
    );
    assert_eq!(parsed["findings"][0]["method"], "change_point", "{report}");
    assert_eq!(parsed["findings"][0]["direction"], "regression", "{report}");
    assert!(report.contains("allocate_vec"), "{report}");
    assert!(
        !report.contains("free_box"),
        "the flat free_box series produces no finding: {report}"
    );
}

/// A slow, monotonic `all_the_time` processor-time drift is flagged as a `drift`
/// finding. Processor time is a noisy, hardware-dependent measurement (like
/// Criterion wall time), so a gentle ramp clears the practical-magnitude floor only
/// via the trend detector. These synthetic points are seeded mean-only (no
/// confidence interval), so the detector falls back gracefully to the Mann-Kendall
/// trend and practical floor. In practice `all_the_time` does record a confidence
/// interval, which would only ever act as an additional veto here, never change the
/// outcome.
#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn analyze_all_the_time_slow_drift_is_flagged_as_drift() {
    let workspace = Workspace::repo(&storage_only_config());
    // A gentle one-nanosecond-per-commit ramp from 20ns upward across
    // `MIN_SERIES_POINTS` commits, the minimum length the drift detector considers.
    for (index, date) in sequential_dates("2024-05-01", MIN_SERIES_POINTS)
        .into_iter()
        .enumerate()
    {
        let label = format!("t{}", index + 1);
        workspace.commit_dated(&date, &label);
        // `index` spans a small fixed range; a u16 cast keeps the ramp exact.
        let value = 20.0 + f64::from(u16::try_from(index).unwrap());
        workspace.seed_all_the_time(&label, "mk", "read_cell", value);
    }

    let report = workspace
        .drive_json(&[
            "analyze",
            "--target-triple",
            "x86_64-unknown-linux-gnu",
            "--machine-key",
            "mk",
        ])
        .await;
    let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
    assert_eq!(
        parsed["regressions"], 1,
        "the upward drift is a regression: {report}"
    );
    assert_eq!(parsed["findings"][0]["method"], "drift", "{report}");
    assert_eq!(parsed["findings"][0]["direction"], "regression", "{report}");
    assert_eq!(parsed["findings"][0]["kind"], "processor_time", "{report}");
}

/// Per-point noise in an `all_the_time` processor-time series never manufactures a
/// spurious finding: oscillation around a flat mean clears neither the trend test
/// nor the change-point gate, so the noisy engine produces nothing. The series
/// carries [`MIN_SERIES_POINTS`] points, so the detector genuinely judges it
/// rather than skipping it as too short.
#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn analyze_all_the_time_jitter_is_not_flagged() {
    let workspace = Workspace::repo(&storage_only_config());
    // Points oscillating roughly +/-15% around 20ns with no sustained shift, one
    // per commit, enough of them that the series clears the minimum-length gate.
    let jitter = [20.0, 23.0, 18.0, 21.0, 19.0, 22.0, 18.0, 22.0, 19.0, 21.0];
    assert!(
        jitter.len() >= MIN_SERIES_POINTS,
        "the jitter series must clear the detector's minimum length so its silence is judged"
    );
    for (index, (date, value)) in sequential_dates("2024-06-01", jitter.len())
        .into_iter()
        .zip(jitter)
        .enumerate()
    {
        let label = format!("j{}", index + 1);
        workspace.commit_dated(&date, &label);
        workspace.seed_all_the_time(&label, "mk", "read_cell", value);
    }

    let report = workspace
        .drive_json(&[
            "analyze",
            "--target-triple",
            "x86_64-unknown-linux-gnu",
            "--machine-key",
            "mk",
        ])
        .await;
    let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
    assert_eq!(
        parsed["runs"],
        u64::try_from(jitter.len()).unwrap(),
        "every jittery point is loaded: {report}"
    );
    assert_eq!(
        parsed["census"]["judged"], 1,
        "the detectors reached a verdict, so this silence is one: {report}"
    );
    assert_eq!(
        parsed["regressions"], 0,
        "jitter must not read as a regression: {report}"
    );
    assert!(
        parsed["findings"].as_array().unwrap().is_empty(),
        "noisy jitter around a flat mean produces no findings at all: {report}"
    );
}

/// A sustained processor-time step whose per-regime confidence intervals are
/// tight and non-overlapping clears the noise detector's CI gate and is reported
/// as a regression. This proves the confidence interval the `all_the_time` engine
/// now records flows through the adapter into the CI-non-overlap gate for
/// processor time.
#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn analyze_all_the_time_step_with_disjoint_intervals_is_a_regression() {
    let workspace = Workspace::repo(&storage_only_config());
    // Five points near 100ns then five near 130ns, each with a tight +/-2ns
    // confidence interval so the two regimes' intervals do not overlap.
    for (date, label, value) in [
        ("2024-07-01", "s1", 98.0),
        ("2024-07-02", "s2", 100.0),
        ("2024-07-03", "s3", 102.0),
        ("2024-07-04", "s4", 99.0),
        ("2024-07-05", "s5", 101.0),
        ("2024-07-06", "s6", 128.0),
        ("2024-07-07", "s7", 130.0),
        ("2024-07-08", "s8", 132.0),
        ("2024-07-09", "s9", 129.0),
        ("2024-07-10", "s10", 131.0),
    ] {
        workspace.commit_dated(date, label);
        workspace.seed_all_the_time_with_interval(label, "mk", "read_cell", value, 2.0);
    }

    let report = workspace
        .drive_json(&[
            "analyze",
            "--target-triple",
            "x86_64-unknown-linux-gnu",
            "--machine-key",
            "mk",
        ])
        .await;
    let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
    assert_eq!(
        parsed["regressions"], 1,
        "a sustained step with disjoint intervals is a regression: {report}"
    );
    assert_eq!(parsed["findings"][0]["method"], "change_point", "{report}");
    assert_eq!(parsed["findings"][0]["direction"], "regression", "{report}");
    assert_eq!(parsed["findings"][0]["kind"], "processor_time", "{report}");
}

/// The same processor-time step values, but with confidence intervals so wide
/// that the two regimes overlap, is suppressed: the CI-non-overlap gate rejects
/// it even though the point medians separate cleanly. This is the companion to
/// [`analyze_all_the_time_step_with_disjoint_intervals_is_a_regression`] — only
/// the recorded dispersion differs, proving the gate is active on processor time.
#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn analyze_all_the_time_step_with_overlapping_intervals_is_suppressed() {
    let workspace = Workspace::repo(&storage_only_config());
    for (date, label, value) in [
        ("2024-08-01", "o1", 98.0),
        ("2024-08-02", "o2", 100.0),
        ("2024-08-03", "o3", 102.0),
        ("2024-08-04", "o4", 99.0),
        ("2024-08-05", "o5", 101.0),
        ("2024-08-06", "o6", 128.0),
        ("2024-08-07", "o7", 130.0),
        ("2024-08-08", "o8", 132.0),
        ("2024-08-09", "o9", 129.0),
        ("2024-08-10", "o10", 131.0),
    ] {
        // A +/-60ns interval makes the ~100ns and ~130ns regimes overlap.
        workspace.commit_dated(date, label);
        workspace.seed_all_the_time_with_interval(label, "mk", "read_cell", value, 60.0);
    }

    let report = workspace
        .drive_json(&[
            "analyze",
            "--target-triple",
            "x86_64-unknown-linux-gnu",
            "--machine-key",
            "mk",
        ])
        .await;
    let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
    assert_eq!(
        parsed["census"]["judged"], 1,
        "the detectors reached a verdict, so this silence is one: {report}"
    );
    assert_eq!(
        parsed["regressions"], 0,
        "an overlapping-interval step is suppressed by the CI gate: {report}"
    );
    assert!(
        parsed["findings"].as_array().unwrap().is_empty(),
        "wide overlapping intervals suppress the step entirely: {report}"
    );
}

/// Without a repository to resolve topology from, `analyze` is an error rather
/// than an empty success — a series' timeline comes from git, not storage.
#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn analyze_without_a_repository_errors() {
    let workspace = Workspace::new(&storage_only_config());
    let error = workspace.drive(&["analyze"]).await.unwrap_err();
    assert!(error.find_source::<cbh_analyze::AnalyzeError>().is_some());
}

/// The official view (analyzing the default branch against itself) admits only
/// clean runs: a dirty snapshot sitting on the tip commit is excluded, so a dirty
/// spike does not flag and the run count covers only the clean points. The clean
/// history is [`MIN_SERIES_POINTS`] points, so the series is genuinely judged (and
/// found flat) rather than merely too short to analyse.
#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn analyze_official_view_excludes_dirty_runs() {
    // A clean working tree, so the base-branch dirty-tree exception does not apply.
    let workspace = Workspace::clean_repo(&storage_only_config());
    workspace.seed_stepped_callgrind("2024-01-01", "c", 100.0, 100.0);
    // A dirty snapshot on the tip commit that would spike the series if admitted.
    let tip = format!("c{MIN_SERIES_POINTS}");
    workspace.seed_dirty_callgrind("2024-02-01", &tip, 200.0);

    let report = workspace.drive_json(&["analyze"]).await;
    let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
    assert_eq!(
        parsed["census"]["judged"], 1,
        "the clean series is judged, so this silence is a verdict: {report}"
    );
    assert_eq!(
        parsed["regressions"], 0,
        "the dirty spike must be excluded: {report}"
    );
    assert_eq!(
        parsed["runs"],
        u64::try_from(MIN_SERIES_POINTS).unwrap(),
        "only the clean runs are loaded, not the dirty snapshot: {report}"
    );
}

/// When every stored run on the default branch is a dirty (uncommitted-tree)
/// snapshot — the trap a user hits when the configuration file is never committed,
/// so each `collect` records a `dirty-*.json` on the base-side tip — `analyze` finds
/// zero runs but renders a diagnostic hint that explains the exclusion, so the
/// empty result is not mistaken for "no data". This exercises the discoverability
/// fix end to end through the production dispatch.
#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn analyze_hints_when_every_run_is_a_dirty_snapshot_on_the_base() {
    // A clean working tree, so the base-branch dirty-tree exception does not apply
    // and every dirty-on-base snapshot stays excluded (yielding the hint).
    let workspace = Workspace::clean_repo(&storage_only_config());
    // Two dirty snapshots on master commits and not a single clean run anywhere.
    workspace.commit_dated("2024-01-01", "c1");
    workspace.seed_dirty_callgrind("2024-01-01", "c1", 100.0);
    workspace.commit_dated("2024-01-02", "c2");
    workspace.seed_dirty_callgrind("2024-01-02", "c2", 130.0);

    let report = workspace.drive_json(&["analyze"]).await;
    let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
    assert_eq!(
        parsed["runs"], 0,
        "every dirty-on-base snapshot is excluded: {report}"
    );
    let hint = parsed["hint"].as_str().unwrap();
    assert!(
        hint.contains("Found 2 stored runs"),
        "the hint should count the stored runs: {hint}"
    );
    assert!(
        hint.contains("dirty"),
        "the hint should explain the dirty-on-base exclusion: {hint}"
    );
}

/// The mirror of the exclusion case: when the working tree is *currently* dirty on
/// the base branch (the user is evaluating the tool, or accidentally working on top
/// of the default branch), the dirty snapshot on the tip commit IS admitted, and
/// the report ends with a warning that such data may be excluded from future
/// analysis. `--no-dirty` overrides it. Exercised end to end through the production
/// dispatch with a real dirty git tree.
#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn analyze_dirty_tree_on_the_base_admits_the_tip_with_a_warning() {
    let workspace = Workspace::clean_repo(&storage_only_config());
    // A flat clean baseline of `MIN_SERIES_POINTS` commits on the base branch. A
    // dirty tip promotes this to branch mode, which needs at least
    // `MIN_SERIES_POINTS` base-side commit levels before it will judge the tip.
    let baseline_dates = sequential_dates("2024-01-01", MIN_SERIES_POINTS);
    for (index, date) in baseline_dates.iter().enumerate() {
        let label = format!("c{}", index + 1);
        workspace.commit_dated(date, &label);
        workspace.seed_callgrind(&label, 100.0);
    }
    // `MIN_REGIME` dirty regression snapshots on the tip commit form the "after"
    // cohort: a raised level beyond the observed clean-base range.
    let tip = format!("c{MIN_SERIES_POINTS}");
    for date in sequential_dates("2024-02-01", MIN_REGIME) {
        workspace.seed_dirty_callgrind(&date, &tip, 130.0);
    }
    // Make the working tree genuinely dirty (an uncommitted, non-ignored file),
    // matching the "evaluating the tool" / "working on the base branch" scenario.
    workspace.make_dirty("uncommitted.txt");

    let report = workspace.drive_json(&["analyze"]).await;
    let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
    assert_eq!(
        parsed["regressions"], 1,
        "the dirty tip regression is admitted: {report}"
    );
    assert_eq!(
        parsed["runs"],
        u64::try_from(MIN_SERIES_POINTS + MIN_REGIME).unwrap(),
        "the dirty tip snapshots join the clean baseline runs: {report}"
    );
    let warning = parsed["warning"].as_str().unwrap();
    assert!(
        warning.contains("Switch to a new branch"),
        "the warning should advise switching to a branch: {warning}"
    );

    // `--no-dirty` skips the probe and the exception, so the dirty tip is dropped.
    let report = workspace.drive_json(&["analyze", "--no-dirty"]).await;
    let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
    assert_eq!(
        parsed["runs"],
        u64::try_from(MIN_SERIES_POINTS).unwrap(),
        "--no-dirty drops the dirty tip snapshots: {report}"
    );
    assert!(
        parsed["warning"].is_null(),
        "no warning fires under --no-dirty: {report}"
    );
}

/// The reported corner case, end to end: a *currently-dirty* working tree on the
/// base branch but with ONLY clean runs recorded (no dirty snapshot on the tip).
/// Mode auto-detection keys off the recorded data set, not the on-disk tree, so this
/// stays `history` mode — and the long-range change-point detector still flags the
/// sustained clean step. (The old behaviour keyed off `git status` and wrongly fell
/// into branch mode here.)
#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn analyze_dirty_tree_without_recorded_dirty_runs_stays_history_mode() {
    let workspace = Workspace::clean_repo(&storage_only_config());
    // A clean base branch with a sustained upward step (a regression). No dirty
    // snapshots are recorded anywhere.
    workspace.seed_stepped_callgrind("2024-01-01", "c", 100.0, 130.0);
    // Dirty the working tree, even though no dirty run was ever stored.
    workspace.make_dirty("uncommitted.txt");

    let report = workspace.drive_json(&["analyze"]).await;
    let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
    assert_eq!(
        parsed["mode"], "history",
        "a dirty tree with only clean runs is still the official history view: {report}"
    );
    assert_eq!(
        parsed["regressions"], 1,
        "history mode flags the sustained clean step: {report}"
    );
    assert_eq!(
        parsed["runs"],
        u64::try_from(MIN_SERIES_POINTS).unwrap(),
        "all clean runs participate; nothing dirty exists: {report}"
    );
    assert!(
        parsed["warning"].is_null(),
        "no dirty runs are admitted, so nothing is ephemeral: {report}"
    );
}

/// A feature view admits dirty snapshots on the branch's own commits (after the
/// merge-base): a dirty regression on the feature tip flags by default, and
/// `--no-dirty` suppresses it.
#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn analyze_feature_branch_admits_dirty_snapshots() {
    let workspace = Workspace::repo(&storage_only_config());
    // A flat clean baseline on master, long enough to give branch mode the
    // `MIN_SERIES_POINTS` base-side commit levels its observed-range comparison needs.
    for (index, date) in sequential_dates("2024-01-01", MIN_SERIES_POINTS)
        .into_iter()
        .enumerate()
    {
        let label = format!("c{}", index + 1);
        workspace.commit_dated(&date, &label);
        workspace.seed_callgrind(&label, 100.0);
    }
    // Branch off master and add a clean point plus dirty regression snapshots on it;
    // the branch tip's dirty cohort is the "after" sample branch mode compares
    // against the base level.
    workspace.checkout_new_branch("feature");
    workspace.commit_dated("2024-02-01", "f1");
    workspace.seed_callgrind("f1", 100.0);
    workspace.seed_dirty_callgrind("2024-02-02", "f1", 200.0);
    workspace.seed_dirty_callgrind("2024-02-03", "f1", 200.0);
    workspace.seed_dirty_callgrind("2024-02-04", "f1", 200.0);

    // By default (feature is HEAD; base is the detected master) the dirty snapshot
    // on the target side is admitted, so the spike flags.
    let RunOutcome::Analyzed { regressions, .. } = workspace.drive(&["analyze"]).await.unwrap()
    else {
        panic!("expected an analyzed outcome");
    };
    assert_eq!(regressions, 1, "the dirty feature snapshot should flag");

    // `--no-dirty` drops it, leaving only the flat clean history.
    let RunOutcome::Analyzed {
        regressions,
        report,
        ..
    } = workspace.drive(&["analyze", "--no-dirty"]).await.unwrap()
    else {
        panic!("expected an analyzed outcome");
    };
    assert_eq!(
        regressions, 0,
        "with --no-dirty only the flat clean series remains: {report}"
    );
    // That remaining clean series was judged, so its silence is a verdict.
    assert_history_was_judged(&report);
}

/// The series timeline follows git topology, not the runs' commit timestamps:
/// seeding the regressing commit last in topology but earliest in time still flags
/// it, proving commit time does not reorder the history.
#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn analyze_orders_by_topology_not_commit_time() {
    let workspace = Workspace::repo(&storage_only_config());
    // Topology c1 -> .. -> cN with a sustained step at the midpoint, but commit
    // times strictly descend, so a commit-time ordering would reverse the step into
    // a non-regression (a drop). A flagged regression proves topology order won.
    // The chain is `MIN_SERIES_POINTS` long so the step clears the persistence gate.
    let ascending = sequential_dates("2024-04-01", MIN_SERIES_POINTS);
    for (index, date) in ascending.iter().rev().enumerate() {
        let label = format!("c{}", index + 1);
        workspace.commit_dated(date, &label);
        let value = if index < MIN_REGIME { 100.0 } else { 130.0 };
        workspace.seed_callgrind(&label, value);
    }

    let report = workspace.drive_json(&["analyze"]).await;
    let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
    assert_eq!(
        parsed["regressions"], 1,
        "topology order must place the 130 step last and flag it: {report}"
    );
    assert_eq!(parsed["findings"][0]["baseline"], 100.0, "{report}");
    assert_eq!(parsed["findings"][0]["latest"], 130.0, "{report}");
}

/// Branch mode judges a feature branch by its *tip commit*, not its journey:
/// a branch that first improved and then regressed is reported as a regression
/// against the base, since only the tip commit lands in the base on merge. The
/// intermediate improvement is not attributed as a within-branch flip.
/// The JSON also advertises the resolved mode and the downstream `notable` signal.
#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn analyze_branch_mode_reports_the_tip_commit_state() {
    let workspace = Workspace::repo(&storage_only_config());
    // A flat clean baseline on master, long enough to give branch mode the
    // `MIN_SERIES_POINTS` base-side commit levels its observed-range comparison needs.
    for (index, date) in sequential_dates("2024-01-01", MIN_SERIES_POINTS)
        .into_iter()
        .enumerate()
    {
        let label = format!("c{}", index + 1);
        workspace.commit_dated(&date, &label);
        workspace.seed_callgrind(&label, 100.0);
    }
    // The feature branch first improves (80) then regresses hard (130): the latest
    // state is what matters, so this must be reported as a regression vs the base.
    workspace.checkout_new_branch("feature");
    workspace.commit_dated("2024-02-01", "f1");
    workspace.seed_callgrind("f1", 80.0);
    workspace.commit_dated("2024-02-02", "f2");
    workspace.seed_callgrind("f2", 80.0);
    workspace.commit_dated("2024-02-03", "f3");
    workspace.seed_callgrind("f3", 80.0);
    workspace.commit_dated("2024-02-04", "f4");
    workspace.seed_callgrind("f4", 130.0);
    workspace.commit_dated("2024-02-05", "f5");
    workspace.seed_callgrind("f5", 130.0);
    workspace.commit_dated("2024-02-06", "f6");
    workspace.seed_callgrind("f6", 130.0);

    let report = workspace.drive_json(&["analyze"]).await;
    let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
    assert_eq!(
        parsed["regressions"], 1,
        "the latest (worse) regime must be reported, not the intermediate improvement: {report}"
    );
    assert_eq!(parsed["mode"], "branch", "{report}");
    assert_eq!(parsed["notable"], true, "{report}");
    let finding = &parsed["findings"][0];
    assert_eq!(finding["direction"], "regression", "{report}");
    assert_eq!(finding["baseline"], 100.0, "{report}");
    assert_eq!(finding["latest"], 130.0, "{report}");
    assert!(
        finding.get("series").is_none(),
        "the JSON finding mirrors the text data and omits the charting series: {report}"
    );
}

/// A feature branch that holds flat at the base level produces no finding in
/// branch mode: with no change in the latest state there is nothing to report,
/// and `notable` stays false so downstream automation knows to stay quiet. The
/// base carries [`MIN_SERIES_POINTS`] commit levels, so branch mode actually
/// judges the tip against a real observed base range rather than staying silent
/// only because the base was too short to evaluate.
#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn analyze_branch_mode_stays_silent_on_a_flat_branch() {
    let workspace = Workspace::repo(&storage_only_config());
    for (index, date) in sequential_dates("2024-01-01", MIN_SERIES_POINTS)
        .into_iter()
        .enumerate()
    {
        let label = format!("c{}", index + 1);
        workspace.commit_dated(&date, &label);
        workspace.seed_callgrind(&label, 100.0);
    }
    workspace.checkout_new_branch("feature");
    workspace.commit_dated("2024-02-01", "f1");
    workspace.seed_callgrind("f1", 100.0);
    workspace.commit_dated("2024-02-02", "f2");
    workspace.seed_callgrind("f2", 100.0);
    workspace.commit_dated("2024-02-03", "f3");
    workspace.seed_callgrind("f3", 100.0);

    let report = workspace.drive_json(&["analyze"]).await;
    let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
    assert_eq!(parsed["mode"], "branch", "{report}");
    assert_eq!(
        parsed["census"]["judged"], 1,
        "branch mode judged the tip against the base, so this silence is a verdict: {report}"
    );
    assert_eq!(
        parsed["regressions"], 0,
        "a flat branch must not flag: {report}"
    );
    assert_eq!(parsed["notable"], false, "{report}");
    assert!(
        parsed["findings"].as_array().is_some_and(Vec::is_empty),
        "{report}"
    );
}

/// History mode reports only regressions: steady improvement on the base branch
/// over time is expected and not noteworthy, so a sustained drop never becomes a
/// finding — and the report states no improvement tally rather than a zero it
/// never looked for.
#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn analyze_history_mode_never_reports_improvements() {
    let workspace = Workspace::repo(&storage_only_config());
    // A clean base branch with a sustained downward step (an improvement).
    workspace.seed_stepped_callgrind("2024-01-01", "c", 100.0, 70.0);

    let report = workspace.drive_json(&["analyze"]).await;
    let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
    assert_eq!(parsed["mode"], "history", "{report}");
    assert_eq!(
        parsed["census"]["judged"], 1,
        "the improvement was judged and then withheld, not left untested: {report}"
    );
    assert_eq!(parsed["notable"], false, "{report}");
    // The key must be absent rather than null: `parsed["improvements"]` cannot tell the
    // two apart, so the object is asked directly. Both the top-level document and every
    // per-set entry carry the tally, so both must omit it.
    let object = parsed.as_object().expect("the report is a JSON object");
    assert!(
        !object.contains_key("improvements"),
        "history mode states no improvement tally: {report}"
    );
    for set in parsed["sets"]
        .as_array()
        .expect("the report lists its sets")
    {
        let set = set.as_object().expect("a set entry is a JSON object");
        assert!(
            !set.contains_key("improvements"),
            "a history-mode set states no improvement tally: {report}"
        );
    }
    assert!(
        parsed["findings"].as_array().is_some_and(Vec::is_empty),
        "improvements are never reported in history mode: {report}"
    );
}

/// A `--target-triple` filter restricts analysis to the matching set: the same
/// commits host a regressing Linux series and a flat Windows series, and each
/// triple selection sees only its own.
#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn analyze_target_triple_discriminant_isolates_linux_from_windows() {
    let workspace = Workspace::repo(&storage_only_config());
    // Each commit carries both a Linux point (rising into a regression) and a
    // Windows point (flat). The chain is `MIN_SERIES_POINTS` long so both series
    // are judged: the Linux step clears the persistence gate and the flat Windows
    // series is genuinely evaluated rather than skipped as too short.
    for (index, date) in sequential_dates("2024-01-01", MIN_SERIES_POINTS)
        .into_iter()
        .enumerate()
    {
        let label = format!("c{}", index + 1);
        workspace.commit_dated(&date, &label);
        let linux = if index < MIN_REGIME { 100.0 } else { 130.0 };
        workspace.seed_callgrind_in(
            "x86_64-unknown-linux-gnu",
            HARNESS_AUTO_MACHINE_KEY,
            &label,
            linux,
        );
        workspace.seed_callgrind_in(
            "x86_64-pc-windows-msvc",
            HARNESS_AUTO_MACHINE_KEY,
            &label,
            50.0,
        );
    }

    // The Linux triple sees the regression.
    let RunOutcome::Analyzed { regressions, .. } = workspace
        .drive(&["analyze", "--target-triple", "x86_64-unknown-linux-gnu"])
        .await
        .unwrap()
    else {
        panic!("expected an analyzed outcome");
    };
    assert_eq!(regressions, 1, "the Linux series regresses");

    // The Windows triple sees only the flat series.
    let RunOutcome::Analyzed {
        regressions,
        report,
        ..
    } = workspace
        .drive(&["analyze", "--target-triple", "x86_64-pc-windows-msvc"])
        .await
        .unwrap()
    else {
        panic!("expected an analyzed outcome");
    };
    assert_eq!(regressions, 0, "the Windows series is flat: {report}");
    // The Windows series was judged, so its silence is a verdict on the data rather
    // than the discriminant having selected an untestable set.
    assert_history_was_judged(&report);
}

/// From a feature checkout, `--context master` analyzes the default branch's
/// official line (clean only), independent of the currently checked-out branch.
#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn analyze_branch_selects_official_line_from_a_feature_checkout() {
    let workspace = Workspace::repo(&storage_only_config());
    // Master carries a clean sustained regression.
    workspace.seed_stepped_callgrind("2024-01-01", "c", 100.0, 130.0);
    // A feature branch with an unrelated dirty improvement that master must ignore.
    workspace.checkout_new_branch("feature");
    workspace.commit_dated("2024-02-01", "f1");
    workspace.seed_dirty_callgrind("2024-02-01", "f1", 10.0);

    let report = workspace
        .drive_json(&["analyze", "--context", "master"])
        .await;
    let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
    assert_eq!(
        parsed["regressions"], 1,
        "the master regression is selected, the feature snapshot ignored: {report}"
    );
    assert_eq!(parsed["findings"][0]["latest"], 130.0, "{report}");
}

/// The official line of a branch that contains merge commits follows first-parent
/// topology: a run stored on a merge commit is on the line, but runs on the
/// merged-in side branch's own commits are not. This is the common real-world
/// shape (a pull request merged into `master`), which the flat-prefix model never
/// exercised.
#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn analyze_official_line_follows_first_parent_across_a_merge() {
    // The merge takes the second position on the official line, so the plain
    // commits after it are labeled from three up to the length the detector needs.
    const FIRST_LABEL_AFTER_MERGE: usize = 3;

    let workspace = Workspace::repo(&storage_only_config());
    // master:  root - c1 - M - c3 - c4 - .. - c{MIN_SERIES_POINTS}
    //                  \   /            (M merges the side branch in)
    //  side:            sf1 - sf2
    workspace.commit("c1");
    workspace.checkout_new_branch("side");
    workspace.commit("sf1");
    workspace.commit("sf2");
    workspace.checkout("master");
    workspace.merge("side", "M");
    let after_merge: Vec<String> = (FIRST_LABEL_AFTER_MERGE..=MIN_SERIES_POINTS)
        .map(|index| format!("c{index}"))
        .collect();
    for label in &after_merge {
        workspace.commit(label);
    }

    // The first-parent line c1 -> M -> c3 -> .. carries a clean sustained
    // regression on its tail: `MIN_REGIME` points at 100 then `MIN_REGIME` at 130,
    // for `MIN_SERIES_POINTS` points in all — enough for the change-point detector.
    let line = ["c1".to_owned(), "M".to_owned()]
        .into_iter()
        .chain(after_merge);
    for (index, label) in line.enumerate() {
        let value = if index < MIN_REGIME { 100.0 } else { 130.0 };
        workspace.seed_callgrind(&label, value);
    }
    // Side-branch points sit on the second-parent side and must never leak into the
    // official line; they carry wild values that would distort the series if read.
    workspace.seed_callgrind("sf1", 999.0);
    workspace.seed_callgrind("sf2", 999.0);

    let report = workspace.drive_json(&["analyze"]).await;
    let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
    assert_eq!(
        parsed["runs"],
        u64::try_from(MIN_SERIES_POINTS).unwrap(),
        "only the first-parent line c1 -> M -> c3 -> .. is analyzed, not \
         the side branch: {report}"
    );
    assert_eq!(
        parsed["regressions"], 1,
        "the regression on the tip commit flags once the merge commit is on the line: {report}"
    );
    assert_eq!(parsed["findings"][0]["latest"], 130.0, "{report}");
}

/// When a feature branch merges the default branch *in*, branch mode still compares
/// the context commit against the base ref's own first-parent window. The
/// merge-base sits off the feature's first-parent chain, but that topology is no
/// longer the source of the base window.
#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn analyze_feature_that_merged_master_uses_the_base_ref_window() {
    for (tip_value, expected_regressions) in [(130.0, 1_u64), (100.0, 0_u64)] {
        let workspace = Workspace::repo(&storage_only_config());
        // master:  root - c1 - c2 - ... - c{MIN_SERIES_POINTS}
        //                  \
        // feature:          f1 - M - f2
        //                        /
        // M merges master's tip into feature, so merge-base(feature, master) is
        // c{MIN_SERIES_POINTS}, which is not on feature's first-parent line
        // [root, c1, f1, M, f2].
        workspace.commit("c1");
        workspace.checkout_new_branch("feature");
        workspace.commit("f1");
        workspace.checkout("master");
        for index in 2..=MIN_SERIES_POINTS {
            workspace.commit(&format!("c{index}"));
        }
        workspace.checkout("feature");
        workspace.merge("master", "M");
        workspace.commit("f2");

        for index in 1..=MIN_SERIES_POINTS {
            workspace.seed_callgrind(&format!("c{index}"), 100.0);
        }
        workspace.seed_callgrind("f2", tip_value);

        let report = workspace.drive_json(&["analyze"]).await;
        let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
        assert_eq!(parsed["mode"], "branch", "{report}");
        assert_eq!(
            parsed["regressions"], expected_regressions,
            "tip value {tip_value} is compared with master's own recent history: {report}"
        );
        if expected_regressions == 1 {
            assert_eq!(parsed["findings"][0]["baseline"], 100.0, "{report}");
            assert_eq!(parsed["findings"][0]["latest"], tip_value, "{report}");
        } else {
            assert!(
                parsed["findings"].as_array().unwrap().is_empty(),
                "{report}"
            );
        }
    }
}

/// Two machine-key partitions on the same engine/triple stay isolated: a rising
/// (regressing) series on one machine and a flat series on the other never merge
/// into one series, so exactly one set regresses. This guards the series grouping
/// key keeping `machine` — dropping it would cross-compare the two machines.
#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn analyze_criterion_machine_keys_stay_isolated() {
    let workspace = Workspace::repo(&storage_only_config());
    // Same commits, same Windows/x86_64 triple, two distinct machine keys. `mk-flat`
    // stays flat across the full d1.. history (the same commits the rising machine
    // uses), so it is present at the tip and never regresses.
    workspace.seed_rising_criterion_history("mk-rising");
    for index in 0..MIN_SERIES_POINTS {
        workspace.seed_criterion(&format!("d{}", index + 1), "mk-flat", 20.0);
    }

    let report = workspace
        .drive_json(&[
            "analyze",
            "--target-triple",
            "x86_64-pc-windows-msvc",
            "--machine-key",
            "all",
        ])
        .await;
    let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
    let sets = parsed["sets"].as_array().unwrap();
    assert_eq!(
        sets.len(),
        2,
        "the two machine keys form two comparable sets: {report}"
    );
    assert_eq!(
        parsed["regressions"], 1,
        "only the rising machine regresses; the machines do not cross-compare: {report}"
    );
}

/// `--target-triple` selects a single comparable set by its whole partition value:
/// the same commits host a regressing `x86_64-unknown-linux-gnu` series and a flat
/// `aarch64-unknown-linux-gnu` series, and each `--target-triple` selection sees
/// only its own.
#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn analyze_target_triple_discriminant_selects_one_set() {
    let workspace = Workspace::repo(&storage_only_config());
    // Each commit hosts a regressing x86_64 series and a flat aarch64 series; the
    // chain is `MIN_SERIES_POINTS` long so both series are judged.
    for (index, date) in sequential_dates("2024-01-01", MIN_SERIES_POINTS)
        .into_iter()
        .enumerate()
    {
        let label = format!("c{}", index + 1);
        workspace.commit_dated(&date, &label);
        let x64 = if index < MIN_REGIME { 100.0 } else { 130.0 };
        workspace.seed_callgrind_in(
            "x86_64-unknown-linux-gnu",
            HARNESS_AUTO_MACHINE_KEY,
            &label,
            x64,
        );
        workspace.seed_callgrind_in(
            "aarch64-unknown-linux-gnu",
            HARNESS_AUTO_MACHINE_KEY,
            &label,
            50.0,
        );
    }

    let RunOutcome::Analyzed { regressions, .. } = workspace
        .drive(&["analyze", "--target-triple", "x86_64-unknown-linux-gnu"])
        .await
        .unwrap()
    else {
        panic!("expected an analyzed outcome");
    };
    assert_eq!(regressions, 1, "the x86_64 triple regresses");

    let RunOutcome::Analyzed {
        regressions,
        report,
        ..
    } = workspace
        .drive(&["analyze", "--target-triple", "aarch64-unknown-linux-gnu"])
        .await
        .unwrap()
    else {
        panic!("expected an analyzed outcome");
    };
    assert_eq!(regressions, 0, "the aarch64 triple is flat: {report}");
    // The aarch64 series was judged, so its silence is a verdict on the data rather
    // than the discriminant having selected an untestable set.
    assert_history_was_judged(&report);
}

#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn analyze_machine_key_discriminant_selects_one_set() {
    let workspace = Workspace::repo(&storage_only_config());
    workspace.seed_rising_criterion_history("mk-rising");
    // `mk-flat` stays flat across the same d1.. commits (already created by the
    // rising history), a full-length series so its silence is a judged verdict
    // rather than a too-short skip.
    for index in 0..MIN_SERIES_POINTS {
        workspace.seed_criterion(&format!("d{}", index + 1), "mk-flat", 20.0);
    }

    let RunOutcome::Analyzed { regressions, .. } = workspace
        .drive(&[
            "analyze",
            "--target-triple",
            "x86_64-pc-windows-msvc",
            "--machine-key",
            "mk-rising",
        ])
        .await
        .unwrap()
    else {
        panic!("expected an analyzed outcome");
    };
    assert_eq!(regressions, 1, "the rising machine regresses");

    let RunOutcome::Analyzed {
        regressions,
        report,
        ..
    } = workspace
        .drive(&[
            "analyze",
            "--target-triple",
            "x86_64-pc-windows-msvc",
            "--machine-key",
            "mk-flat",
        ])
        .await
        .unwrap()
    else {
        panic!("expected an analyzed outcome");
    };
    assert_eq!(
        regressions, 0,
        "the flat machine does not regress: {report}"
    );
    // The flat machine's series was judged, so this silence is a verdict on it.
    assert_history_was_judged(&report);
}

/// The dirty/feature-branch admission path works for Criterion too (not only
/// Callgrind): a dirty Criterion regression on a feature commit flags by default
/// and is suppressed by `--no-dirty`.
#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn analyze_criterion_feature_branch_admits_dirty_snapshots() {
    let workspace = Workspace::repo(&storage_only_config());
    // A clean Criterion baseline on master with enough commits for branch mode's
    // observed-range comparison. The alternating values exercise a narrow base range;
    // branch mode would also accept a flat range.
    for (index, date) in sequential_dates("2024-02-01", MIN_SERIES_POINTS)
        .into_iter()
        .enumerate()
    {
        let label = format!("c{}", index + 1);
        workspace.commit_dated(&date, &label);
        let level = if index % 2 == 0 { 19.9 } else { 20.1 };
        workspace.seed_criterion(&label, "mk", level);
    }
    // Branch off master, add a clean point, then repeated dirty snapshots at the tip.
    // Branch mode prefers the dirty lane and collapses those runs to one commit
    // observation before comparing it with the base range.
    workspace.checkout_new_branch("feature");
    workspace.commit_dated("2024-03-01", "f1");
    workspace.seed_criterion("f1", "mk", 20.0);
    workspace.seed_dirty_criterion("2024-03-02", "f1", "mk", 40.0);
    workspace.seed_dirty_criterion("2024-03-03", "f1", "mk", 40.0);
    workspace.seed_dirty_criterion("2024-03-04", "f1", "mk", 40.0);
    workspace.seed_dirty_criterion("2024-03-05", "f1", "mk", 40.0);

    let RunOutcome::Analyzed { regressions, .. } = workspace
        .drive(&[
            "analyze",
            "--target-triple",
            "x86_64-pc-windows-msvc",
            "--machine-key",
            "mk",
        ])
        .await
        .unwrap()
    else {
        panic!("expected an analyzed outcome");
    };
    assert_eq!(
        regressions, 1,
        "the dirty Criterion feature snapshot should flag"
    );

    let RunOutcome::Analyzed {
        regressions,
        report,
        ..
    } = workspace
        .drive(&[
            "analyze",
            "--target-triple",
            "x86_64-pc-windows-msvc",
            "--machine-key",
            "mk",
            "--no-dirty",
        ])
        .await
        .unwrap()
    else {
        panic!("expected an analyzed outcome");
    };
    assert_eq!(
        regressions, 0,
        "with --no-dirty only the flat clean Criterion series remains: {report}"
    );
    // That remaining clean series was judged, so its silence is a verdict.
    assert_history_was_judged(&report);
}

/// `analyze` ignores "ghost" benchmarks — ones no longer present at the context
/// commit — so a regression on a since-removed benchmark is not re-flagged.
#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn analyze_excludes_ghost_benchmarks() {
    let workspace = Workspace::repo(&storage_only_config());
    // `bench_000` runs flat through the tip; `bench_001` climbs into a regression it
    // would flag on its own (a full `MIN_SERIES_POINTS`-long step), but it is dropped
    // from the suite at the tip commit, so it is a ghost there and excluded.
    for (index, date) in sequential_dates("2024-01-01", MIN_SERIES_POINTS)
        .into_iter()
        .enumerate()
    {
        let label = format!("c{}", index + 1);
        workspace.commit_dated(&date, &label);
        let climbing = if index < MIN_REGIME { 100.0 } else { 130.0 };
        workspace.seed_many_callgrind(&label, &[100.0, climbing]);
    }
    // The tip carries only the surviving benchmark; `bench_001` is absent here.
    workspace.commit_dated("2024-02-01", "tip");
    workspace.seed_many_callgrind("tip", &[100.0]);

    // The ghost's regression is filtered out, and the JSON reports the count.
    let report = workspace.drive_json(&["analyze"]).await;
    let parsed: serde_json::Value = serde_json::from_str(&report).unwrap();
    assert_eq!(parsed["regressions"], 0, "{report}");
    assert_eq!(parsed["ghosts_excluded"], 1, "{report}");
    assert!(
        !report.contains("many::bench_001"),
        "the ghost benchmark must not appear: {report}"
    );
    // Excluded is not the same as forgotten: the census still accounts for the
    // ghost's series, so the reader can tell this silence covers one series of two.
    assert_eq!(parsed["census"]["total"], 2, "{report}");
    assert_eq!(parsed["census"]["judged"], 1, "{report}");
    assert_eq!(
        parsed["census"]["reasons"][0]["reason"], "ghost",
        "{report}"
    );
    assert_eq!(parsed["census"]["reasons"][0]["count"], 1, "{report}");
}

/// The rendered chart's rows: every axis row carries a `┤` or `┼` tick and the
/// chart is always drawn uncolored, so filtering on those glyphs isolates the plot
/// from the surrounding (possibly colored) report prose.
fn chart_lines(report: &str) -> Vec<&str> {
    report
        .lines()
        .filter(|line| line.contains('┤') || line.contains('┼'))
        .collect()
}

/// The chart's rendered width in characters — the widest of its rows. Two charts
/// spanning the same number of topology columns share this width even when one
/// carries a trailing gap, so a width match proves the fill reached the tip column.
fn chart_width(report: &str) -> usize {
    chart_lines(report)
        .iter()
        .map(|line| line.chars().count())
        .max()
        .unwrap_or(0)
}

/// A feature branch whose comparison base lags the merge-base — the newest base
/// data sits one commit behind the branch point — both warns in prose and still
/// draws the branch chart. That trailing lag is the visual form of the warning: the
/// chart spans the base history and the tip with the gap columns in between.
#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn analyze_branch_comparison_base_lag_warns_and_charts_the_gap() {
    let workspace = Workspace::repo(&storage_only_config());
    // Base data reaches only the last seeded base commit; the merge-base carries
    // none, so the branch's comparison base ends one commit behind the branch point.
    // The base has `MIN_SERIES_POINTS` seeded commit levels, which branch mode's
    // observed-range comparison requires.
    for (index, date) in sequential_dates("2024-01-01", MIN_SERIES_POINTS)
        .into_iter()
        .enumerate()
    {
        let label = format!("c{}", index + 1);
        workspace.commit_dated(&date, &label);
        workspace.seed_callgrind(&label, 100.0);
    }
    // The merge-base commit is intentionally left unseeded, so the newest base datum
    // sits one commit behind the branch point.
    workspace.commit_dated("2024-01-20", "mb");
    workspace.checkout_new_branch("feature");
    workspace.commit_dated("2024-02-01", "f1");
    workspace.seed_callgrind("f1", 130.0);
    workspace.commit_dated("2024-02-02", "f2");
    workspace.seed_callgrind("f2", 130.0);
    workspace.commit_dated("2024-02-03", "f3");
    workspace.seed_callgrind("f3", 130.0);

    let RunOutcome::Analyzed {
        regressions,
        report,
        ..
    } = workspace.drive(&["analyze"]).await.unwrap()
    else {
        panic!("expected an analyzed outcome");
    };
    assert_eq!(
        regressions, 1,
        "the branch tip regresses against the base: {report}"
    );
    assert!(
        report.contains(
            "Warning: comparison base is 1 commit behind base \
             (no base data at more recent commits)"
        ),
        "the one-commit comparison-base lag is disclosed: {report}"
    );
    assert!(
        !chart_lines(&report).is_empty(),
        "the branch finding draws a chart: {report}"
    );
}

/// In history mode a benchmark whose latest observation for one metric lags the
/// analyzed tip renders a trailing gap: the chart still spans every commit through
/// the tip, with a blank column where the tip carries no datum for that metric. A
/// sibling metric keeps the benchmark present at the tip, so the lagging metric's
/// series is retained rather than dropped as a ghost.
#[tokio::test]
#[cfg_attr(miri, ignore)]
async fn analyze_history_no_newer_data_renders_a_trailing_gap() {
    // Two identical histories of `MIN_SERIES_POINTS` commits. `conditional_branches`
    // steps up at the midpoint (a sustained regression); `instruction_count` stays
    // flat and never flags. Only the tip commit differs: the full run records both
    // metrics there while the lagging run records instruction_count alone, so the
    // benchmark stays present at the tip (not a ghost) yet its conditional_branches
    // series ends one commit behind.
    let dates = sequential_dates("2024-01-01", MIN_SERIES_POINTS);

    let full = Workspace::repo(&storage_only_config());
    let lag = Workspace::repo(&storage_only_config());
    for workspace in [&full, &lag] {
        for (index, date) in dates.iter().enumerate() {
            let label = format!("c{}", index + 1);
            let cb = if index < MIN_REGIME { 100.0 } else { 130.0 };
            workspace.commit_dated(date, &label);
            workspace.seed_metrics(
                &label,
                vec![
                    Metric::new(MetricKind::InstructionCount, 500.0),
                    Metric::new(MetricKind::ConditionalBranches, cb),
                ],
            );
        }
    }
    // The shared tip commit: the full run records both metrics; the lagging run
    // records only the sibling, leaving conditional_branches one commit behind.
    full.commit_dated("2024-02-01", "tip");
    full.seed_metrics(
        "tip",
        vec![
            Metric::new(MetricKind::InstructionCount, 500.0),
            Metric::new(MetricKind::ConditionalBranches, 130.0),
        ],
    );
    lag.commit_dated("2024-02-01", "tip");
    lag.seed_metrics(
        "tip",
        vec![Metric::new(MetricKind::InstructionCount, 500.0)],
    );

    let RunOutcome::Analyzed {
        regressions: full_regressions,
        report: full_report,
        ..
    } = full.drive(&["analyze"]).await.unwrap()
    else {
        panic!("expected an analyzed outcome");
    };
    let RunOutcome::Analyzed {
        regressions: lag_regressions,
        report: lag_report,
        ..
    } = lag.drive(&["analyze"]).await.unwrap()
    else {
        panic!("expected an analyzed outcome");
    };

    assert_eq!(
        full_regressions, 1,
        "the full history flags the conditional_branches step: {full_report}"
    );
    assert_eq!(
        lag_regressions, 1,
        "the lagging history flags the same step even though the tip lacks the metric: {lag_report}"
    );

    // The lagging series ends at the last dated commit but its chart is filled with
    // a trailing gap column up to the analyzed tip, so it spans exactly as many
    // columns — and renders exactly as wide — as the full history's chart. Without
    // that trailing fill the lagging chart would stop one column short of the tip
    // and be one character narrower. (`rasciigraph` draws a trailing gap and a
    // continued flat plateau identically — both a blank final column — so the
    // rendered width, not the glyph content, is what proves the fill reached the
    // tip.)
    assert!(
        chart_width(&full_report) > 0,
        "the full history draws a chart: {full_report}"
    );
    assert_eq!(
        chart_width(&lag_report),
        chart_width(&full_report),
        "the trailing 'no newer data' gap extends the lagging chart to the tip\n\
         FULL:\n{full_report}\nLAG:\n{lag_report}"
    );
}
