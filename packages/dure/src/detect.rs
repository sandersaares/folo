//! Auto-detect a session from the current directory.
//!
//! Ref: docs/design.md, "Auto-detect".

use std::path::Path;

use crate::path_display::display_path;
use crate::SessionId;
use crate::session_record::SessionRecord;
use crate::trace::{Trace, trace};

/// Result of matching live sessions against the current directory.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum DetectOutcome {
    /// No live sessions exist.
    None,
    /// Exactly one live session was launched from the current directory.
    Unique(SessionId),
    /// Zero or several matches for this directory, so the caller has to ask which\r
    /// session to take. The candidates are the live sessions the caller already\r
    /// holds, so none are carried here.
    NeedsSelection,
}

/// Chooses a session using the launch-directory rule.
///
/// `cwd` must already be the PAL-canonicalized current directory. Comparison is
/// equality of those canonical paths.
pub(crate) fn auto_detect(live: &[SessionRecord], cwd: &Path, trace: Trace) -> DetectOutcome {
    if live.is_empty() {
        trace!(trace, "auto-detect: no live sessions to choose from");
        return DetectOutcome::None;
    }

    let matches: Vec<&SessionRecord> = live
        .iter()
        .filter(|session| session.launch_directory == cwd)
        .collect();

    if trace.is_enabled() {
        trace!(
            trace,
            "auto-detect: current directory {}; {} live {}; {} launch-directory {}",
            display_path(cwd),
            live.len(),
            session_noun(live.len()),
            matches.len(),
            match_noun(matches.len()),
        );
        for session in live {
            trace!(
                trace,
                "auto-detect: session {} was launched from {} ({})",
                session.id,
                display_path(&session.launch_directory),
                match_mark(&session.launch_directory, cwd)
            );
        }
        trace!(trace, "auto-detect: {}", outcome_note(matches.len()));
    }

    match unique_match(&matches) {
        Some(id) => DetectOutcome::Unique(id),
        None => DetectOutcome::NeedsSelection,
    }
}

// Mutating this selection turns a deterministic resume into the interactive
// prompt, which hangs integration tests when mutation watchdogs are disabled.
#[cfg_attr(test, mutants::skip)]
fn unique_match(matches: &[&SessionRecord]) -> Option<SessionId> {
    let [only] = matches else {
        return None;
    };
    Some(only.session_id())
}

// Trace wording is not a behavioral contract; the selection it explains is.
#[cfg_attr(test, mutants::skip)]
fn outcome_note(matches: usize) -> &'static str {
    match matches {
        0 => "no session was launched from here, so every live session is offered",
        1 => "exactly one session was launched from here, so it is resumed",
        _ => "several sessions were launched from here, so none can be chosen for you",
    }
}

// English pluralization is not a behavioral contract.
#[cfg_attr(test, mutants::skip)]
fn session_noun(count: usize) -> &'static str {
    if count == 1 { "session" } else { "sessions" }
}

// Per-session annotation in the verbose trace. The selection itself is the
// behavioral contract; the wording that explains it is not.
#[cfg_attr(test, mutants::skip)]
fn match_mark(launch_directory: &Path, cwd: &Path) -> &'static str {
    if launch_directory == cwd {
        "match"
    } else {
        "different directory"
    }
}

// English pluralization is not a behavioral contract.
#[cfg_attr(test, mutants::skip)]
fn match_noun(count: usize) -> &'static str {
    if count == 1 { "match" } else { "matches" }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::app_command::AppCommand;

    fn record(id: u32, dir: &str) -> SessionRecord {
        SessionRecord {
            id,
            supervisor_pid: 1,
            supervisor_creation_time: 1,
            pipe_name: format!("pipe-{id}"),
            launch_directory: PathBuf::from(dir),
            command: AppCommand::for_test(&["app.exe"]),
            started_at_unix_ms: 1,
            attached: false,
        }
    }

    #[test]
    fn no_sessions_fails() {
        assert_eq!(
            auto_detect(&[], Path::new("/work"), Trace::default()),
            DetectOutcome::None
        );
    }

    #[test]
    fn unique_launch_directory_match() {
        let live = [record(1, "/a"), record(2, "/work")];
        assert_eq!(
            auto_detect(&live, Path::new("/work"), Trace::default()),
            DetectOutcome::Unique(SessionId::from_u32(2).unwrap())
        );
    }

    #[test]
    fn verbose_unique_match_is_still_unique() {
        let live = [record(1, "/work")];
        assert_eq!(
            auto_detect(&live, Path::new("/work"), Trace::new(true)),
            DetectOutcome::Unique(SessionId::from_u32(1).unwrap())
        );
    }

    #[test]
    fn verbose_reports_sessions_from_other_directories() {
        let live = [record(1, "/other"), record(2, "/work")];
        assert_eq!(
            auto_detect(&live, Path::new("/work"), Trace::new(true)),
            DetectOutcome::Unique(SessionId::from_u32(2).unwrap())
        );
    }

    #[test]
    fn verbose_reports_an_empty_store() {
        assert_eq!(
            auto_detect(&[], Path::new("/work"), Trace::new(true)),
            DetectOutcome::None
        );
    }

    #[test]
    fn several_matches_need_selection() {
        let live = [record(1, "/work"), record(2, "/work")];
        let outcome = auto_detect(&live, Path::new("/work"), Trace::default());
        assert_eq!(outcome, DetectOutcome::NeedsSelection);
    }

    #[test]
    fn single_session_in_other_directory_needs_selection() {
        let live = [record(1, "/other")];
        let outcome = auto_detect(&live, Path::new("/work"), Trace::default());
        assert_eq!(outcome, DetectOutcome::NeedsSelection);
    }
}
