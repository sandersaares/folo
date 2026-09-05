//! Windows integration tests that drive `dure` inside a test-owned pseudoconsole.
//!
//! A test runner has no interactive console, so each test builds one: the
//! *outer* pseudoconsole is the one this harness owns and reads, standing in
//! for the user's terminal. A `dure` session then creates an *inner*
//! pseudoconsole of its own for the app.
//!
//! Names beginning `harness_` exercise the harness itself; the rest run the
//! `dure` binary end to end.

#![cfg(windows)]

use std::fs;
use std::sync::mpsc::Receiver;

use dure::test_support::ConsoleProcess;
use dure_test_helper::SAMPLE_NON_ASCII_TEXT;
use tempfile::TempDir;
use testing::with_watchdog;

mod fixture;

use fixture::{
    DureCommand, FIRST_SESSION_ID, HELPER_READY, HELPER_STEM, SAMPLE_NONZERO_EXIT, Scenario,
    helper_exe, release, store_entries,
};

/// Introducer for the escape sequences a console host writes around app text.
const ESC: char = '\u{1b}';
/// Terminator the pseudoconsole uses for its window-title sequences.
const BEL: char = '\u{7}';

/// Strip the control sequences a pseudoconsole typically injects around app text.
///
/// An escape character introduces a sequence. A sequence beginning `ESC [` — the
/// form used for colors and cursor movement — runs until an ASCII letter ends
/// it. One beginning `ESC ]` — the form used to set the window title — runs
/// until the terminator above. Any other escape form consumes one following
/// character. This recovers app text from console-host noise; it is not a full
/// terminal emulator.
fn visible_text(bytes: &[u8]) -> String {
    let raw = String::from_utf8_lossy(bytes);
    let mut chars = raw.chars();
    let mut text = String::new();
    while let Some(ch) = chars.next() {
        if ch != ESC {
            text.push(ch);
            continue;
        }
        match chars.next() {
            Some('[') => {
                for next in chars.by_ref() {
                    if next.is_ascii_alphabetic() {
                        break;
                    }
                }
            }
            Some(']') => {
                for next in chars.by_ref() {
                    if next == BEL {
                        break;
                    }
                }
            }
            Some(_) | None => {}
        }
    }
    text
}

/// Text with all whitespace removed.
///
/// A pseudoconsole wraps output at the window width and can break a line in the
/// middle of a word, so ignoring whitespace is the only stable way to look for
/// a phrase in console output.
fn without_whitespace(text: &str) -> String {
    text.chars().filter(|ch| !ch.is_whitespace()).collect()
}

/// Whether `haystack` contains `needle`, ignoring how the console wrapped it.
fn says(haystack: &str, needle: &str) -> bool {
    without_whitespace(haystack).contains(&without_whitespace(needle))
}

/// One reader over a client's console output.
///
/// A pseudoconsole has one read side, so asking for the stream twice sets two
/// readers racing for the same bytes and each sees only some of them. A test
/// that looks at a client's output more than once therefore keeps one of these.
struct Console {
    chunks: Receiver<Vec<u8>>,
    seen: Vec<u8>,
}

impl Console {
    fn watching(process: &ConsoleProcess) -> Self {
        Self {
            chunks: process.output_until_exit(),
            seen: Vec::new(),
        }
    }

    /// Everything printed so far, as text.
    fn text(&self) -> String {
        visible_text(&self.seen)
    }

    /// Reads until `needle` has been printed.
    ///
    /// The stream ends when the child does, so a phrase that never arrives
    /// fails here rather than waiting for a watchdog.
    fn until(&mut self, needle: &str) -> String {
        let wanted = without_whitespace(needle);
        loop {
            let text = self.text();
            if without_whitespace(&text).contains(&wanted) {
                return text;
            }
            let Ok(chunk) = self.chunks.recv() else {
                panic!(
                    "child exited without printing {needle:?}, output was {:?}",
                    self.text()
                );
            };
            self.seen.extend(chunk);
        }
    }

    /// Reads to the end of the stream.
    fn rest(&mut self) -> String {
        while let Ok(chunk) = self.chunks.recv() {
            self.seen.extend(chunk);
        }
        self.text()
    }
}

/// Reads one client's output until `needle`, for a test that looks once.
fn collect_until(console: &ConsoleProcess, needle: &str) -> String {
    Console::watching(console).until(needle)
}

/// Everything the client printed, once it has exited.
fn collect_all(console: &ConsoleProcess) -> String {
    Console::watching(console).rest()
}

/// Whether the session banner in `output` names exactly `id`.
///
/// Whitespace is normalized away first, for the same line-wrapping reason
/// `collect_until` does it, so the digits are required to end where the id
/// does rather than merely to start with it.
fn banner_names_session(output: &str, id: u64) -> bool {
    let normalized = without_whitespace(output);
    let wanted = format!("session{id}");
    normalized.match_indices(&wanted).any(|(at, _)| {
        let after = at.saturating_add(wanted.len());
        normalized
            .get(after..)
            .and_then(|rest| rest.chars().next())
            .is_none_or(|next| !next.is_ascii_digit())
    })
}

#[cfg_attr(miri, ignore)]
#[test]
fn harness_forwards_the_helper_exit_status() {
    with_watchdog(|| {
        let dir = TempDir::new().unwrap();
        let args = vec!["exit".to_string(), SAMPLE_NONZERO_EXIT.to_string()];
        let helper = ConsoleProcess::spawn(&helper_exe(), &args, dir.path());
        let status = helper.wait();
        assert_eq!(status, SAMPLE_NONZERO_EXIT);
    });
}

#[cfg_attr(miri, ignore)]
#[test]
fn harness_gives_the_helper_a_console() {
    with_watchdog(|| {
        let dir = TempDir::new().unwrap();
        let args = vec!["has-console".to_string()];
        let helper = ConsoleProcess::spawn(&helper_exe(), &args, dir.path());
        let output = collect_until(&helper, "console");
        let status = helper.wait();
        assert_eq!(status, 0);
        assert!(
            output.contains("console"),
            "helper must report a console, got {output:?}"
        );
    });
}

#[cfg_attr(miri, ignore)]
#[test]
fn harness_relays_input_to_the_helper() {
    with_watchdog(|| {
        let dir = TempDir::new().unwrap();
        let args = vec!["echo-line".to_string()];
        let helper = ConsoleProcess::spawn(&helper_exe(), &args, dir.path());
        helper.write_input(b"hello\r\n");
        let output = collect_until(&helper, "hello");
        let status = helper.wait();
        assert_eq!(status, 0, "helper output: {output:?}");
    });
}

#[cfg_attr(miri, ignore)]
#[test]
fn run_forwards_the_app_exit_status() {
    with_watchdog(|| {
        let dir = TempDir::new().unwrap();
        let client = DureCommand::run(dir.path(), Scenario::WaitThenExit).spawn(dir.path());
        // Released before anything is asserted, so a `dure` that stops
        // announcing its session fails the assertion below rather than leaving
        // the app parked on input this test never sent.
        release(&client);
        let output = collect_all(&client);
        let status = client.wait();

        assert_eq!(status, SAMPLE_NONZERO_EXIT, "client output: {output:?}");
        assert!(
            banner_names_session(&output, FIRST_SESSION_ID),
            "a fresh store must report session {FIRST_SESSION_ID}, got {output:?}"
        );
    });
}

#[cfg_attr(miri, ignore)]
#[test]
fn run_gives_the_app_a_console() {
    with_watchdog(|| {
        let dir = TempDir::new().unwrap();
        let client =
            DureCommand::run(dir.path(), Scenario::WaitThenReportConsole).spawn(dir.path());
        release(&client);
        let output = collect_all(&client);
        let status = client.wait();

        assert_eq!(status, 0, "client output: {output:?}");
        let report = fs::read_to_string(dir.path().join("console-status.txt"))
            .expect("helper console status file");
        assert_eq!(report, "console");
    });
}

#[cfg_attr(miri, ignore)]
#[test]
fn run_explains_what_it_is_launching_when_verbose() {
    with_watchdog(|| {
        let dir = TempDir::new().unwrap();
        let client = DureCommand::run(dir.path(), Scenario::WaitThenExit)
            .verbose()
            .spawn(dir.path());
        release(&client);
        let output = collect_all(&client);
        let status = client.wait();

        assert_eq!(status, SAMPLE_NONZERO_EXIT, "client output: {output:?}");
        // The app being launched is an input a quiet run never echoes, so
        // finding it proves the trace reached the terminal.
        assert!(
            says(&output, HELPER_STEM),
            "a verbose run must say what it is launching, got {output:?}"
        );
    });
}

#[cfg_attr(miri, ignore)]
#[test]
fn relayed_output_keeps_non_ascii_text_intact() {
    with_watchdog(|| {
        let dir = TempDir::new().unwrap();
        let client = DureCommand::run(dir.path(), Scenario::PrintNonAscii).spawn(dir.path());
        let output = collect_until(&client, SAMPLE_NON_ASCII_TEXT);
        let status = client.wait();
        assert_eq!(status, 0, "client output: {output:?}");
    });
}

#[cfg_attr(miri, ignore)]
#[test]
fn relayed_input_keeps_non_ascii_text_intact() {
    with_watchdog(|| {
        let dir = TempDir::new().unwrap();
        let client = DureCommand::run(dir.path(), Scenario::EchoLine).spawn(dir.path());
        // Input and output travel different paths, so output surviving proves
        // nothing about what the app is handed. Ref: docs/console.md.
        let mut typed = SAMPLE_NON_ASCII_TEXT.as_bytes().to_vec();
        typed.extend_from_slice(b"\r\n");
        client.write_input(&typed);
        let output = collect_until(&client, SAMPLE_NON_ASCII_TEXT);
        let status = client.wait();
        assert_eq!(status, 0, "client output: {output:?}");
    });
}

#[cfg_attr(miri, ignore)]
#[test]
fn a_dropped_client_leaves_the_session_resumable() {
    with_watchdog(|| {
        // The launch directory and the store root are deliberately different
        // places: auto-detect matches on where `run` was issued, and a store
        // root that also served as that directory could not tell the two
        // apart. Ref: docs/design.md, "Auto-detect".
        let store = TempDir::new().unwrap();
        let launch = TempDir::new().unwrap();
        let client = DureCommand::run(store.path(), Scenario::PrintAndWait).spawn(launch.path());
        // The app's own greeting, so this waits on the session being up rather
        // than on anything `dure` prints.
        _ = collect_until(&client, HELPER_READY);
        // Models the SSH connection dropping: the client dies where it stands,
        // without a chance to tell the supervisor anything.
        drop(client);

        let resumed = DureCommand::resume(store.path()).spawn(launch.path());
        release(&resumed);
        let output = collect_all(&resumed);
        let status = resumed.wait();

        // Bare `resume` found the session from the launch directory alone, and
        // the app was still parked on console input across the disconnect, so
        // reaching it now is what makes the resumed session interactive rather
        // than merely alive.
        assert_eq!(status, 0, "resumed client output: {output:?}");
        assert!(
            says(&output, "session"),
            "resume must report the session it found, got {output:?}"
        );
    });
}

#[cfg_attr(miri, ignore)]
#[test]
fn a_second_client_takes_the_session_from_a_live_one() {
    with_watchdog(|| {
        let store = TempDir::new().unwrap();
        let launch = TempDir::new().unwrap();
        let first = DureCommand::run(store.path(), Scenario::PrintAndWait).spawn(launch.path());
        // One reader for the whole test: the displacement notice arrives on
        // the same stream the greeting did.
        let mut watching_first = Console::watching(&first);
        _ = watching_first.until(HELPER_READY);

        // The first client is still attached, which is what separates a steal
        // from the reconnect above.
        // Ref: docs/design.md, "Attach, detach, steal".
        let second = DureCommand::resume(store.path()).spawn(launch.path());

        // The displaced client is told why its screen went quiet, and ends
        // rather than staying attached alongside. Waiting for that here is
        // also what keeps the app alive until the steal has completed.
        let displaced = watching_first.until("another client");
        let first_status = first.wait();
        assert_ne!(
            first_status, 0,
            "a displaced client does not report success, output: {displaced:?}"
        );

        // Last attach wins: the newest client is the one that reaches the app.
        release(&second);
        let stolen = collect_all(&second);
        let second_status = second.wait();
        assert_eq!(second_status, 0, "second client output: {stolen:?}");
    });
}

#[cfg_attr(miri, ignore)]
#[test]
fn run_refuses_a_launcher_that_forbids_breakaway() {
    with_watchdog(|| {
        let dir = TempDir::new().unwrap();
        // Models a wrapper such as `cargo run`, whose job would kill the
        // supervisor along with the client it launched.
        let client =
            DureCommand::run(dir.path(), Scenario::WaitThenExit).spawn_confined(dir.path());
        let output = collect_until(&client, "forbids breakaway");
        let status = client.wait();
        assert_ne!(status, 0, "client output: {output:?}");
        assert!(
            !banner_names_session(&output, FIRST_SESSION_ID),
            "a refused launch must not report a session, got {output:?}"
        );
        // Nothing may be left behind for `resume` or `list` to find.
        let records = store_entries(dir.path());
        assert!(records.is_empty(), "store must stay empty, got {records:?}");
    });
}

#[cfg_attr(miri, ignore)]
#[test]
fn run_warns_when_an_ancestor_job_would_end_the_session() {
    with_watchdog(|| {
        let dir = TempDir::new().unwrap();
        // The immediate job permits breakaway, so the supervisor starts, but the
        // ancestor it cannot leave would kill it with the launcher.
        let client = DureCommand::run(dir.path(), Scenario::WaitThenExit)
            .spawn_confined_by_ancestor(dir.path());
        release(&client);
        let output = collect_all(&client);
        let status = client.wait();

        assert_eq!(status, SAMPLE_NONZERO_EXIT, "client output: {output:?}");
        // Checked the way every other console assertion here is, because the
        // warning is long enough for the console host to wrap it.
        assert!(
            says(&output, "will not survive a disconnect"),
            "a session tied to the launcher must say so, got {output:?}"
        );
        // Whitespace-insensitive matching erases the boundary between the word
        // and the id, so `session 10` would satisfy a search for `session 1`.
        // The banner is therefore checked again against the id a fresh store
        // must have handed out.
        assert!(
            banner_names_session(&output, FIRST_SESSION_ID),
            "a fresh store must report session {FIRST_SESSION_ID}, got {output:?}"
        );
    });
}
