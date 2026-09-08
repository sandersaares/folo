//! A diagnostic-note sink that surfaces the step-by-step detail behind a command
//! when `--verbose` is set, so an otherwise-silent outcome (notably an empty
//! harvest that stores nothing) can be diagnosed.
//!
//! Notes are progress/diagnostic output and are distinct from a command's final
//! summary, which is returned as a `RunOutcome` and printed
//! to standard output. The real adapter writes notes to standard error so they
//! never contaminate machine-readable stdout; tests record them in memory.
//!
//! Stage timings are a *second*, independent channel ([`ReporterExt::timing`]):
//! they report the wall-clock cost of each pipeline stage so a mystery slowdown
//! can be localized. They are kept separate from per-object
//! [`note`](Notes::note)s precisely so a caller can ask for the coarse per-stage
//! breakdown *without* the per-object flood — the stress harness, which analyzes
//! tens of thousands of objects, would otherwise drown in (and be slowed by) one
//! note per object.
//!
//! Announcements are a *third*, always-on channel ([`ReporterExt::announce`]):
//! a single line stating the effective, possibly auto-detected inputs a run
//! resolved. Which inputs appear varies by command — a `collect` run announces
//! the target triple and machine key it partitioned under, while a query run
//! also names the base branch and look-back window it analyzed. Unlike notes,
//! they are emitted *regardless* of `--verbose`, so a plain run never hides which
//! values it defaulted. Like notes they go to standard error, so
//! machine-readable stdout stays clean.
//!
//! The unconditional emit primitives live on a sealed [`Sink`] trait that cannot
//! be named outside this module, so the only note-emitting surface a caller sees
//! is the guarded [`note_with`](ReporterExt::note_with) /
//! [`if_enabled`](ReporterExt::if_enabled) pair. An unconditional
//! [`note`](Notes::note) is reachable *only* on the [`Notes`] handle passed to an
//! `if_enabled` block, so a bare `note` can never escape its `--verbose` guard.

use std::time::Duration;

mod sealed {
    use std::time::Duration;

    /// The unconditional emit primitives, sealed within [`report`](super) so no
    /// caller can invoke them directly.
    ///
    /// Every note or timing a command emits flows through the guarded helpers on
    /// [`ReporterExt`](super::ReporterExt) — which are the only surface that can
    /// reach these methods — so the `--verbose` guard is applied in exactly one
    /// place and can never be bypassed or forgotten at a call site.
    pub(crate) trait Sink {
        /// Whether notes are consumed at all, gating the guarded helpers.
        fn enabled(&self) -> bool;

        /// Records a single diagnostic note unconditionally.
        fn emit_note(&self, message: &str);

        /// Records the wall-clock duration of a named pipeline `stage`
        /// unconditionally.
        fn emit_timing(&self, stage: &str, elapsed: Duration);

        /// Emits an always-on announcement unconditionally (not gated on
        /// `--verbose`).
        fn emit_announcement(&self, message: &str);
    }
}

use sealed::Sink;

/// Receives human-facing diagnostic notes emitted while a command runs.
///
/// This is the threaded reporter type (`&dyn Reporter`). It deliberately exposes
/// *no* unconditional emit method: callers reach the sink only through the
/// guarded helpers on [`ReporterExt`], so a raw `note` can never be emitted
/// without its `--verbose` guard. Any [`Sink`] is a `Reporter`.
#[expect(
    private_bounds,
    reason = "Sink is sealed within this module on purpose so its unconditional \
              emit primitives stay off the crate-facing reporter surface"
)]
pub trait Reporter: Sink {}

impl<T: Sink + ?Sized> Reporter for T {}

/// The guarded, crate-facing diagnostic API available on every [`Reporter`],
/// including `&dyn Reporter`.
///
/// These live on a separate trait rather than on [`Reporter`] itself so the base
/// trait stays dyn-compatible: a method taking a generic closure cannot be
/// dispatched through `&dyn Reporter`. A blanket impl makes the helpers available
/// on every reporter regardless.
pub trait ReporterExt {
    /// Records a diagnostic note whose formatting runs only when notes are
    /// consumed.
    ///
    /// The `build` closure — typically an allocating `format!` — is invoked only
    /// when notes are enabled. On a hot path that emits one note per scanned
    /// object this avoids building (and immediately discarding) a string for
    /// every object when `--verbose` is off, without sprinkling an
    /// `if reporter.enabled()` guard around each call site.
    fn note_with(&self, build: impl FnOnce() -> String);

    /// Runs `body` — handed a [`Notes`] emitter — only when notes are consumed.
    ///
    /// The counterpart to [`note_with`](Self::note_with) for a *block* that emits
    /// several notes (or computes intermediate values solely to note them): it
    /// pays nothing when `--verbose` is off, behind a single guard rather than
    /// one per note. Because the unconditional [`note`](Notes::note) lives on the
    /// [`Notes`] handle passed here — and nowhere else callers can reach — a bare
    /// note is only ever emittable inside such a guarded block. The closure is
    /// higher-ranked over the handle's lifetime (`for<'a>`), so the handle cannot
    /// be moved into outer state and used after the guard completes.
    fn if_enabled(&self, body: impl for<'a> FnOnce(Notes<'a, Self>));

    /// Records the wall-clock duration of a named pipeline `stage`.
    ///
    /// `stage` should name the stage *and* what it encompasses (so the breakdown
    /// reconstructs where the time went), e.g. `"phase 2/3 fetch + parallel parse +
    /// fold"`. This is a separate channel from [`note`](Notes::note) so the
    /// per-stage cost can be surfaced without the per-object note flood.
    fn timing(&self, stage: &str, elapsed: Duration);

    /// Emits an always-on, one-line announcement of a run's effective inputs.
    ///
    /// Unlike [`note_with`](Self::note_with), this channel is *not* gated on
    /// `--verbose`: it is the one diagnostic a plain run always prints, so a value
    /// that was auto-detected or defaulted (the target triple, machine key, base
    /// branch, or look-back window) is never resolved silently. Emitted to
    /// standard error so machine-readable stdout stays clean.
    fn announce(&self, message: &str);
}

impl<R: Reporter + ?Sized> ReporterExt for R {
    fn note_with(&self, build: impl FnOnce() -> String) {
        if self.enabled() {
            self.emit_note(&build());
        }
    }

    fn if_enabled(&self, body: impl for<'a> FnOnce(Notes<'a, Self>)) {
        if self.enabled() {
            body(Notes { reporter: self });
        }
    }

    fn timing(&self, stage: &str, elapsed: Duration) {
        self.emit_timing(stage, elapsed);
    }

    fn announce(&self, message: &str) {
        self.emit_announcement(message);
    }
}

/// The unconditional note emitter handed to an [`if_enabled`](ReporterExt::if_enabled)
/// block.
///
/// A block already runs only when notes are enabled, so the notes it emits need
/// no further guard — hence this is the one place an unconditional
/// [`note`](Self::note) is exposed. It cannot be constructed elsewhere, so a bare
/// note can never escape its `--verbose` guard.
#[derive(Debug)]
pub struct Notes<'a, R: ?Sized> {
    reporter: &'a R,
}

impl<R: Reporter + ?Sized> Notes<'_, R> {
    /// Records a single diagnostic note.
    pub fn note(&self, message: &str) {
        self.reporter.emit_note(message);
    }
}

/// A [`Reporter`] that writes notes to standard error when verbose mode is on,
/// and discards them otherwise.
#[derive(Clone, Copy, Debug)]
pub struct StderrReporter {
    /// Whether per-object diagnostic notes are emitted.
    verbose: bool,
    /// Whether per-stage timing notes are emitted. Independent of `verbose` so a
    /// caller can ask for the timing breakdown alone.
    timing_enabled: bool,
}

impl StderrReporter {
    /// Creates a reporter that emits notes only when `verbose` is set.
    ///
    /// Stage timings follow `verbose` too, so a plain `--verbose` run gets the
    /// per-stage breakdown alongside the per-object detail.
    #[must_use]
    pub fn new(verbose: bool) -> Self {
        Self {
            verbose,
            timing_enabled: verbose,
        }
    }

    /// Creates a reporter with independent control over the per-object note stream
    /// (`verbose`) and the per-stage timing stream (`timing`), so a caller can ask
    /// for the timing breakdown without the per-object flood.
    #[must_use]
    pub fn with_timing(verbose: bool, timing: bool) -> Self {
        Self {
            verbose,
            timing_enabled: timing,
        }
    }
}

impl Sink for StderrReporter {
    fn enabled(&self) -> bool {
        self.verbose
    }

    /// Writes the note to standard error in verbose mode.
    ///
    /// A pure standard-error side effect with no return value or observable state,
    /// so a mutation of its body cannot be caught by a test; the `verbose` flag it
    /// guards on is covered by `stderr_reporter_reports_enabled_state`.
    #[cfg_attr(test, mutants::skip)]
    fn emit_note(&self, message: &str) {
        if self.verbose {
            eprintln!("[bench-history] {message}");
        }
    }

    /// Writes the stage timing to standard error when timing is enabled.
    ///
    /// A pure standard-error side effect, untestable for the same reason as
    /// [`emit_note`](Self::emit_note); the `timing_enabled` flag it guards on is
    /// covered by `stderr_reporter_reports_timing_state`.
    #[cfg_attr(test, mutants::skip)]
    fn emit_timing(&self, stage: &str, elapsed: Duration) {
        if self.timing_enabled {
            eprintln!(
                "[bench-history] timing: {stage} took {}",
                format_elapsed(elapsed)
            );
        }
    }

    /// Writes the announcement to standard error unconditionally.
    ///
    /// A pure standard-error side effect with no return value or observable state,
    /// untestable for the same reason as [`emit_note`](Self::emit_note). Unlike a
    /// note it is not gated on `verbose`: the announcement is always shown.
    #[cfg_attr(test, mutants::skip)]
    fn emit_announcement(&self, message: &str) {
        eprintln!("[bench-history] {message}");
    }
}

/// Formats a stage duration for a timing note: seconds (with millisecond
/// precision) at or above one second, milliseconds below that, so both a
/// multi-second load and a sub-millisecond step read naturally.
fn format_elapsed(elapsed: Duration) -> String {
    let seconds = elapsed.as_secs_f64();
    if seconds >= 1.0 {
        format!("{seconds:.3} s")
    } else {
        format!("{:.1} ms", seconds * 1000.0)
    }
}

#[cfg(any(test, feature = "private-test-util"))]
#[cfg_attr(docsrs, doc(cfg(feature = "private-test-util")))]
pub use test_support::RecordingReporter;

#[cfg(any(test, feature = "private-test-util"))]
#[cfg_attr(coverage_nightly, coverage(off))]
mod test_support {
    use std::cell::RefCell;
    use std::time::Duration;

    use super::Sink;

    /// A [`Reporter`](super::Reporter) that records every note in memory so tests
    /// can assert on the diagnostic trail.
    #[derive(Debug, Default)]
    pub struct RecordingReporter {
        notes: RefCell<Vec<String>>,
        timings: RefCell<Vec<String>>,
        announcements: RefCell<Vec<String>>,
    }

    impl RecordingReporter {
        /// Creates an empty recording reporter.
        #[must_use]
        pub fn new() -> Self {
            Self::default()
        }

        /// Returns a snapshot of the notes recorded so far.
        pub fn notes(&self) -> Vec<String> {
            self.notes.borrow().clone()
        }

        /// Whether any recorded note contains `needle`.
        pub fn contains(&self, needle: &str) -> bool {
            self.notes.borrow().iter().any(|note| note.contains(needle))
        }

        /// Whether a stage timing whose label contains `needle` was recorded.
        pub fn timed(&self, needle: &str) -> bool {
            self.timings
                .borrow()
                .iter()
                .any(|stage| stage.contains(needle))
        }

        /// Returns a snapshot of the announcements recorded so far.
        pub fn announcements(&self) -> Vec<String> {
            self.announcements.borrow().clone()
        }

        /// Whether any recorded announcement contains `needle`.
        pub fn announced(&self, needle: &str) -> bool {
            self.announcements
                .borrow()
                .iter()
                .any(|line| line.contains(needle))
        }
    }

    impl Sink for RecordingReporter {
        fn enabled(&self) -> bool {
            true
        }

        fn emit_note(&self, message: &str) {
            self.notes.borrow_mut().push(message.to_owned());
        }

        fn emit_timing(&self, stage: &str, _elapsed: Duration) {
            // Record only the stage label; the elapsed time is non-deterministic, so
            // tests assert that a stage *was* timed, not how long it took.
            self.timings.borrow_mut().push(stage.to_owned());
        }

        fn emit_announcement(&self, message: &str) {
            self.announcements.borrow_mut().push(message.to_owned());
        }
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;

    #[test]
    fn stderr_reporter_reports_enabled_state() {
        assert!(StderrReporter::new(true).enabled());
        assert!(!StderrReporter::new(false).enabled());
    }

    #[test]
    fn stderr_reporter_reports_timing_state() {
        // `new` ties timing to verbose, so a plain `--verbose` run also gets timings.
        assert!(StderrReporter::new(true).timing_enabled);
        assert!(!StderrReporter::new(false).timing_enabled);

        // `with_timing` controls the two streams independently, so a caller can ask
        // for the timing breakdown without the per-object note flood.
        let timing_only = StderrReporter::with_timing(false, true);
        assert!(!timing_only.enabled());
        assert!(timing_only.timing_enabled);

        let notes_only = StderrReporter::with_timing(true, false);
        assert!(notes_only.enabled());
        assert!(!notes_only.timing_enabled);
    }

    #[test]
    fn recording_reporter_captures_timings_separately_from_notes() {
        let reporter = RecordingReporter::new();
        reporter.emit_note("a per-object note");
        reporter.emit_timing("select_dataset (full load)", Duration::from_millis(5));

        // Timings live in their own channel, so a per-object note assertion is not
        // disturbed by them and vice versa.
        assert_eq!(reporter.notes(), vec!["a per-object note".to_owned()]);
        assert!(reporter.timed("select_dataset"));
        assert!(!reporter.timed("find_changes"));
    }

    #[test]
    fn recording_reporter_captures_announcements_separately_from_notes() {
        let reporter = RecordingReporter::new();
        reporter.emit_note("a per-object note");
        reporter.emit_announcement("selection: target-triple=x86_64 (auto-detected)");

        // Announcements live in their own channel, so they neither disturb nor are
        // disturbed by the per-object note stream.
        assert_eq!(reporter.notes(), vec!["a per-object note".to_owned()]);
        assert_eq!(
            reporter.announcements(),
            vec!["selection: target-triple=x86_64 (auto-detected)".to_owned()]
        );
        assert!(reporter.announced("auto-detected"));
        assert!(!reporter.announced("machine-key"));
    }

    #[test]
    fn reporter_ext_announce_forwards_to_the_sink() {
        // `ReporterExt::announce` is the always-on, crate-facing entry point; it
        // must forward to the sink's `emit_announcement` rather than drop the line.
        let reporter = RecordingReporter::new();
        reporter.announce("selection: base=main (auto-detected)");
        assert!(reporter.announced("base=main"));
    }

    #[test]
    fn reporter_ext_timing_forwards_to_the_sink() {
        // `ReporterExt::timing` is the guarded, crate-facing entry point; it must
        // forward to the sink's `emit_timing` rather than silently drop the stage.
        let reporter = RecordingReporter::new();
        reporter.timing("find_changes (git walk)", Duration::from_millis(7));
        assert!(reporter.timed("find_changes"));
    }

    #[test]
    fn format_elapsed_uses_seconds_at_or_above_one_second() {
        assert_eq!(format_elapsed(Duration::from_secs(1)), "1.000 s");
        assert_eq!(format_elapsed(Duration::from_millis(2500)), "2.500 s");
    }

    #[test]
    fn format_elapsed_uses_milliseconds_below_one_second() {
        assert_eq!(format_elapsed(Duration::from_millis(250)), "250.0 ms");
        assert_eq!(format_elapsed(Duration::ZERO), "0.0 ms");
    }

    #[test]
    fn recording_reporter_captures_notes() {
        let reporter = RecordingReporter::new();
        assert!(reporter.enabled());
        reporter.emit_note("scanning target/criterion");
        reporter.emit_note("excluding stale.json");

        assert_eq!(
            reporter.notes(),
            vec![
                "scanning target/criterion".to_owned(),
                "excluding stale.json".to_owned()
            ]
        );
        assert!(reporter.contains("stale"));
        assert!(!reporter.contains("missing"));
    }

    #[test]
    fn note_with_skips_formatting_when_disabled() {
        use std::cell::Cell;

        // A disabled reporter must never run the formatting closure, so a per-object
        // note costs nothing when nothing will consume it.
        let built = Cell::new(0_u32);
        StderrReporter::new(false).note_with(|| {
            built.set(built.get() + 1);
            "discarded".to_owned()
        });
        assert_eq!(built.get(), 0);

        // An enabled reporter runs the closure and records the resulting note.
        let recording = RecordingReporter::new();
        recording.note_with(|| {
            built.set(built.get() + 1);
            "lazy note".to_owned()
        });
        assert_eq!(built.get(), 1);
        assert!(recording.contains("lazy note"));
    }

    #[test]
    fn if_enabled_runs_the_block_only_when_enabled() {
        use std::cell::Cell;

        // A disabled reporter must never run the block, so a multi-note diagnostic
        // section behind one guard costs nothing when `--verbose` is off.
        let ran = Cell::new(0_u32);
        StderrReporter::new(false).if_enabled(|_notes| ran.set(ran.get() + 1));
        assert_eq!(ran.get(), 0);

        // An enabled reporter runs the block, which emits several notes through the
        // handle — the only place a bare `note` is reachable.
        let recording = RecordingReporter::new();
        recording.if_enabled(|notes| {
            ran.set(ran.get() + 1);
            notes.note("first");
            notes.note("second");
        });
        assert_eq!(ran.get(), 1);
        assert_eq!(
            recording.notes(),
            vec!["first".to_owned(), "second".to_owned()]
        );
    }
}
