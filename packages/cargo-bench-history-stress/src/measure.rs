//! Drives the `analyze` command once per mode and times it.
//!
//! Each mode is run through the real public [`run_with_overrides`] entry point, so
//! the measured path is exactly production data-loading and detection — only the
//! data underneath is synthetic. The discriminant filters are forced to `all` because the
//! seeded triples and the seeded machine key never match the host the harness runs
//! on; without that every object would be filtered out.

use std::num::NonZero;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};

use all_the_time::Session;
use cargo_bench_history::{
    AnalyzeOptions, CacheSelection, Command, LocalStorageSelection, Overrides, RunOutcome,
    run_with_overrides,
};
use jiff::Timestamp;
use serde::Deserialize;
use tick::Clock;

use crate::cli::ModeArg;
use crate::error::{Error, fail};
use crate::logging::Logger;
use crate::scenario::{BRANCH_FEATURE, BRANCH_MAIN};

/// A far-past `--since` cutoff guaranteeing the whole history is in window for
/// history-mode analysis (whose default look-back is only six months).
const FULL_HISTORY_SINCE: &str = "2000-01-01";

/// Where each analysis writes its JSON report inside the seeded workspace. The
/// harness reads the structured counts back from this file (text output is
/// suppressed); a relative path resolves against the workspace directory, and
/// each attempt overwrites the previous one in place.
const ANALYZE_REPORT_FILE: &str = "stress-analyze-report.json";

/// The optional storage selections forwarded into each analysis: a `--local`
/// store root and a read-through `--cache` directory. Both are `Option<&Path>`
/// (each meaningful only against its respective backend) and are grouped so the
/// per-mode measurement signature stays compact.
#[derive(Clone, Copy)]
pub(crate) struct StorageInputs<'a> {
    /// Root of a `--local` filesystem store, when the run targets local storage.
    pub(crate) local: Option<&'a Path>,
    /// Read-through cache directory, when the run targets cloud storage with a cache.
    pub(crate) cache: Option<&'a Path>,
}

/// The timed result of analyzing one mode.
#[derive(Clone, Copy, Debug)]
pub(crate) struct MeasureResult {
    /// Which mode was measured.
    pub(crate) mode: ModeArg,
    /// The fastest observed wall-clock time across the repeats.
    pub(crate) duration: Duration,
    /// Process CPU time (summed across every analysis thread) consumed by the
    /// fastest run — the work the wall time above paid for.
    pub(crate) processor_time: Duration,
    /// Fraction of the available cores the fastest run kept busy
    /// (`processor_time / (duration * cores)`). 1.0 means every core was
    /// saturated for the whole analysis; lower means cores sat idle.
    pub(crate) cpu_efficiency: f64,
    /// Stored objects loaded into the analysis.
    pub(crate) runs: usize,
    /// Distinct series compared.
    pub(crate) series: usize,
    /// Flagged regressions.
    pub(crate) regressions: usize,
    /// Flagged improvements, or `None` in a mode that does not report them.
    pub(crate) improvements: Option<usize>,
    /// Whether any finding survived (the downstream signal).
    pub(crate) notable: bool,
}

/// The subset of the analyze JSON report the harness reports on.
#[derive(Debug, Deserialize)]
struct ReportCounts {
    /// Stored objects loaded.
    runs: usize,
    /// Distinct series compared.
    series: usize,
    /// Flagged regressions.
    regressions: usize,
    /// Flagged improvements. Absent from the report in a mode that does not report
    /// improvements.
    improvements: Option<usize>,
    /// Whether any finding survived.
    notable: bool,
}

/// Runs `analyze` for `mode` `repeat` times, returning the fastest run's timing
/// and the (identical) counts it reported.
///
/// # Errors
///
/// Returns an error if the analysis fails or its report cannot be parsed.
// The only coverage of this function is the process-spawning smoke suite, which
// exceeds the 60s cargo-mutants timeout on slower Windows shards.
#[cfg_attr(test, mutants::skip)]
pub(crate) async fn measure(
    workspace: &Path,
    repo: &Path,
    mode: ModeArg,
    storage: StorageInputs<'_>,
    anchor: Timestamp,
    repeat: usize,
    logger: Logger,
) -> Result<MeasureResult, Error> {
    let options = build_options(workspace, repo, mode, storage, logger.verbose());
    logger.step(&format!("analyzing in {} mode", mode.keyword()));
    logger.detail_with(|| {
        format!(
            "context={}, base={}, discriminant filters forced to all \
             (seeded triples and the seeded machine key never match the host), repeats={repeat}",
            options.context.as_deref().unwrap_or(""),
            options.base.as_deref().unwrap_or(""),
        )
    });

    let command = Command::Analyze(options);

    // Capture process CPU time (across all analysis threads) over the exact
    // interval the wall clock times, so we can report how well the analysis
    // saturates the cores it asked for. A fresh in-process session per attempt
    // (no stdout table, no JSON file) holds exactly that attempt's one span; the
    // harness renders the result itself and the tests stay free of target files.
    //
    // The measured span is the full production `run_with_overrides`, which writes
    // the JSON report to the workspace as part of producing it. That write stays
    // inside the span deliberately — it is the production output path being
    // measured — and is a fixed, sub-percent cost: the lean report is bounded by
    // the finding count, not the history size. The structured counts are read
    // back from that file only after the span closes, so the read never distorts
    // the timing.
    let cores = thread::available_parallelism().map_or(1, NonZero::get);

    let mut best: Option<(Duration, Duration)> = None;
    let mut counts: Option<ReportCounts> = None;
    for attempt in 0..repeat.max(1) {
        let session = Session::new().no_stdout().no_file();
        let operation = session.operation(format!("analyze-{}", mode.keyword()));

        let started = Instant::now();
        let span = operation.measure_process().iterations(1);
        let outcome = run_with_overrides(&command, overrides(workspace, anchor))
            .await
            .map_err(|error| fail(format!("{} analysis failed: {error}", mode.keyword())))?;
        drop(span);
        let elapsed = started.elapsed();
        let attempt_cpu = recorded_cpu(&session);
        if !matches!(outcome, RunOutcome::Analyzed { .. }) {
            return Err(fail("analyze did not return an Analyzed outcome"));
        }
        let report = std::fs::read_to_string(workspace.join(ANALYZE_REPORT_FILE))
            .map_err(|error| fail(format!("failed to read the analyze report: {error}")))?;
        let parsed: ReportCounts = serde_json::from_str(&report)
            .map_err(|error| fail(format!("failed to parse the analyze report: {error}")))?;

        logger.detail_with(|| {
            format!(
                "attempt {} took {:.3}s wall / {:.3}s CPU = {:.0}% of {cores} cores ({} objects, \
             {} series)",
                attempt.saturating_add(1),
                elapsed.as_secs_f64(),
                attempt_cpu.as_secs_f64(),
                cpu_efficiency(attempt_cpu, elapsed, cores) * 100.0,
                parsed.runs,
                parsed.series,
            )
        });
        best = Some(keep_fastest(best, (elapsed, attempt_cpu)));
        counts = Some(parsed);
    }

    let (duration, processor_time) = best.ok_or_else(|| fail("no analysis attempts ran"))?;
    let counts = counts.ok_or_else(|| fail("no analysis attempts ran"))?;
    Ok(MeasureResult {
        mode,
        duration,
        processor_time,
        cpu_efficiency: cpu_efficiency(processor_time, duration, cores),
        runs: counts.runs,
        series: counts.series,
        regressions: counts.regressions,
        improvements: counts.improvements,
        notable: counts.notable,
    })
}

/// The process CPU time recorded by the single span held in `session`, or zero
/// if the session recorded nothing.
fn recorded_cpu(session: &Session) -> Duration {
    session
        .to_report()
        .operations()
        .next()
        .map_or(Duration::ZERO, |(_, op)| op.total_processor_time())
}

/// Keeps whichever attempt ran in less wall time, carrying its paired CPU time;
/// ties keep the incumbent. This is what makes the report show the fastest run.
fn keep_fastest(
    best: Option<(Duration, Duration)>,
    candidate: (Duration, Duration),
) -> (Duration, Duration) {
    match best {
        Some(best) if best.0 <= candidate.0 => best,
        _ => candidate,
    }
}

/// The fraction of the available cores the analysis kept busy: process CPU time
/// over the wall time times the core count, clamped to `0.0..=1.0`. 1.0 means
/// every core was saturated for the whole run; a zero or sub-zero ideal (an
/// unmeasurably short or zero-core interval) yields 0.0. Measurement skew that
/// would nudge the raw ratio just past full saturation is clamped back, so the
/// value always honours the `0.0..=1.0` contract the report renders as a
/// percentage.
#[expect(
    clippy::cast_precision_loss,
    reason = "core counts and sub-hour durations are far below 2^53"
)]
fn cpu_efficiency(processor_time: Duration, wall: Duration, cores: usize) -> f64 {
    let ideal = wall.as_secs_f64() * cores as f64;
    if ideal <= 0.0 {
        0.0
    } else {
        (processor_time.as_secs_f64() / ideal).min(1.0)
    }
}

/// Builds the analyze options for `mode`. `storage.local` is the `--local` storage
/// path for local-filesystem runs (`None` when a cloud backend is configured in the
/// seeded config file instead), and `storage.cache` is the `--cache` mirror
/// directory, set only for cloud runs that opted into the read-through cache.
///
/// `timing` requests the analyze pipeline's per-stage wall-clock breakdown
/// (under the harness's `--verbose`). The per-object note stream stays off
/// (`verbose: false`) because the stress dataset holds tens of thousands of
/// objects: one note per object would both bury the timings and distort the very
/// wall clock being measured.
fn build_options(
    workspace: &Path,
    repo: &Path,
    mode: ModeArg,
    storage: StorageInputs<'_>,
    timing: bool,
) -> AnalyzeOptions {
    let (context, base, since) = match mode {
        ModeArg::History => (
            BRANCH_MAIN,
            BRANCH_MAIN,
            Some(FULL_HISTORY_SINCE.to_owned()),
        ),
        ModeArg::Branch => (BRANCH_FEATURE, BRANCH_MAIN, None),
    };
    AnalyzeOptions {
        config_path: Some(config_path(workspace)),
        repo: Some(repo.to_path_buf()),
        local: storage
            .local
            .map(|path| LocalStorageSelection::Path(path.to_path_buf())),
        cache: storage
            .cache
            .map(|path| CacheSelection::Path(path.to_path_buf())),
        context: Some(context.to_owned()),
        base: Some(base.to_owned()),
        no_dirty: false,
        since,
        engine: vec!["all".to_owned()],
        target_triple: vec!["all".to_owned()],
        machine_key: vec!["all".to_owned()],
        prefixes: Vec::new(),
        no_text: true,
        markdown: None,
        json: Some(PathBuf::from(ANALYZE_REPORT_FILE)),
        markdown_summary: None,
        outcome: None,
        verbose: false,
        timing,
    }
}

/// The overrides anchoring the analysis to the seeded workspace and clock.
fn overrides(workspace: &Path, anchor: Timestamp) -> Overrides {
    Overrides {
        workspace_dir: Some(workspace.to_path_buf()),
        target_root: None,
        bench_command: None,
        clock: Some(Clock::new_frozen_at(anchor)),
        storage_override: None,
        auto_discriminants: None,
    }
}

/// The path to the seeded configuration file under `workspace`.
pub(crate) fn config_path(workspace: &Path) -> PathBuf {
    workspace.join(".cargo").join("bench_history.toml")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overrides_pin_the_workspace_and_clock() {
        let workspace = Path::new("/tmp/stress-ws");
        let anchor = Timestamp::from_second(1_750_000_000).expect("anchor is in range");

        let overrides = overrides(workspace, anchor);

        // The seeded workspace and the fixed analysis clock are what make the
        // measurement read synthetic data at a reproducible "now"; the other
        // overrides stay unset so production defaults apply.
        assert_eq!(overrides.workspace_dir.as_deref(), Some(workspace));
        assert_eq!(
            overrides.clock.map(|c| c.system_time_as::<Timestamp>()),
            Some(anchor)
        );
        assert!(overrides.target_root.is_none());
        assert!(overrides.bench_command.is_none());
    }

    #[test]
    fn config_path_lives_under_dot_cargo() {
        let path = config_path(Path::new("/tmp/stress-ws"));
        assert!(
            path.ends_with(".cargo/bench_history.toml"),
            "{}",
            path.display()
        );
    }

    #[test]
    fn build_options_carries_the_cache_only_when_requested() {
        let workspace = Path::new("/tmp/stress-ws");
        let repo = Path::new("/tmp/stress-repo");

        // A cloud run with a cache directory threads it into the analyze options as
        // an explicit path selection, so the measured load exercises the mirror.
        let cached = build_options(
            workspace,
            repo,
            ModeArg::History,
            StorageInputs {
                local: None,
                cache: Some(Path::new("/tmp/cache")),
            },
            false,
        );
        assert_eq!(
            cached.cache,
            Some(CacheSelection::Path(PathBuf::from("/tmp/cache")))
        );

        // Without a cache directory the field stays unset, so analyze reads the
        // cloud directly (the harness's default, uncached measurement).
        let uncached = build_options(
            workspace,
            repo,
            ModeArg::History,
            StorageInputs {
                local: None,
                cache: None,
            },
            false,
        );
        assert_eq!(uncached.cache, None);
    }

    #[test]
    fn cpu_efficiency_is_cpu_over_wall_times_cores() {
        // Four cores busy for the whole second == full saturation.
        let full = cpu_efficiency(Duration::from_secs(4), Duration::from_secs(1), 4);
        assert!((full - 1.0).abs() < 1e-9, "{full}");

        // Only one of four cores busy == a quarter of the ideal.
        let quarter = cpu_efficiency(Duration::from_secs(1), Duration::from_secs(1), 4);
        assert!((quarter - 0.25).abs() < 1e-9, "{quarter}");
    }

    #[test]
    fn cpu_efficiency_clamps_above_full_saturation() {
        // Measurement skew can report more CPU time than the wall-times-cores
        // ideal; the fraction is clamped to full saturation so it never breaches
        // the `0.0..=1.0` contract the report renders as a percentage.
        let over = cpu_efficiency(Duration::from_secs(5), Duration::from_secs(1), 4);
        assert!((over - 1.0).abs() < 1e-9, "{over}");
    }

    #[test]
    fn cpu_efficiency_is_zero_when_there_is_no_wall_time() {
        // A zero-length (or zero-core) interval has no ideal to divide by, so the
        // ratio collapses to zero rather than dividing by zero.
        assert!(cpu_efficiency(Duration::from_secs(1), Duration::ZERO, 4).abs() < 1e-12);
        assert!(cpu_efficiency(Duration::from_secs(1), Duration::from_secs(1), 0).abs() < 1e-12);
    }

    #[test]
    fn keep_fastest_takes_the_smaller_wall_and_carries_its_cpu() {
        let slow = (Duration::from_secs(5), Duration::from_secs(9));
        let fast = (Duration::from_secs(3), Duration::from_secs(7));

        // The first attempt is always kept.
        assert_eq!(keep_fastest(None, slow), slow);
        // A faster candidate replaces the incumbent, bringing its own CPU time.
        assert_eq!(keep_fastest(Some(slow), fast), fast);
        // A slower candidate leaves the incumbent (and its CPU) in place.
        assert_eq!(keep_fastest(Some(fast), slow), fast);
        // A tie keeps the incumbent rather than swapping to the newcomer.
        let tie = (Duration::from_secs(3), Duration::from_secs(1));
        assert_eq!(keep_fastest(Some(fast), tie), fast);
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "all_the_time process timing uses syscalls unsupported under Miri"
    )]
    fn recorded_cpu_reflects_a_measured_span() {
        // A session that recorded nothing has no CPU time to report.
        let empty = Session::new().no_stdout().no_file();
        assert_eq!(recorded_cpu(&empty), Duration::ZERO);

        // A span around a CPU burn records a non-zero process time, which is what
        // the harness pairs with the wall clock to compute efficiency.
        let session = Session::new().no_stdout().no_file();
        let operation = session.operation("analyze-history");
        {
            let span = operation.measure_process().iterations(1);
            let mut acc = 0_u64;
            for value in 0..50_000_000_u64 {
                acc = acc.wrapping_add(value);
            }
            assert_ne!(acc, 0, "the burn must not be optimized away");
            drop(span);
        }
        assert!(recorded_cpu(&session) > Duration::ZERO);
    }
}
