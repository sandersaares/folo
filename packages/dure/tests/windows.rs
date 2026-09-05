//! Windows integration tests that drive `dure` inside a test-owned pseudoconsole.
//!
//! A test runner has no interactive console, so each test builds one: the
//! *outer* pseudoconsole is the one this harness owns and reads, standing in
//! for the user''s terminal. A `dure` session then creates an *inner*
//! pseudoconsole of its own for the app. `ConPTY` names the Windows facility
//! both are made from.
//!
//! Names beginning `harness_` exercise the harness itself; the rest run the
//! `dure` binary end to end.

#![cfg(all(windows, feature = "private-test-util"))]

use std::fs;
use std::path::{Path, PathBuf};

use dure::test_support::ConsoleProcess;
use dure_test_helper::{SAMPLE_NON_ASCII_TEXT, binary_path};
use tempfile::TempDir;
use testing::with_watchdog;

fn dure_exe() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_dure"))
}

fn helper_exe() -> PathBuf {
    PathBuf::from(binary_path())
}

/// Introducer for CSI, OSC, and other ECMA-48 sequences.
const ESC: char = '\u{1b}';
/// OSC terminator used by pseudoconsole window-title sequences.
const BEL: char = '\u{7}';

/// Strip the control sequences a pseudoconsole typically injects around app text.
///
/// ESC introduces a sequence. CSI (`ESC [`) runs until an ASCII letter, the
/// ECMA-48 final byte for SGR and cursor commands. OSC (`ESC ]`) runs until
/// BEL, the terminator the pseudoconsole uses for window-title sequences. Any
/// other ESC form consumes one following character. This recovers app text from
/// pseudoconsole cursor noise; it is not a full VT parser.
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
/// A pseudoconsole wraps output at the window width and can break a line in the middle
/// of a word, so ignoring whitespace is the only stable way to look for a
/// phrase in console output.
fn without_whitespace(text: &str) -> String {
    text.chars().filter(|ch| !ch.is_whitespace()).collect()
}

fn collect_until(console: &ConsoleProcess, needle: &str) -> String {
    let wanted = without_whitespace(needle);
    let mut collected = Vec::new();
    for chunk in console.output_until_exit() {
        collected.extend(chunk);
        let text = visible_text(&collected);
        if without_whitespace(&text).contains(&wanted) {
            return text;
        }
    }
    panic!(
        "child exited without printing {needle:?}, output was {:?}",
        visible_text(&collected)
    );
}

/// Arbitrary nonzero status used to prove forwarding, not a special value.
const SAMPLE_NONZERO_EXIT: i32 = 7;

/// The id a fresh store hands to its first session, so a test that owns an
/// empty store knows the banner to expect.
const FIRST_SESSION_ID: u64 = 1;

/// Distinctive part of the helper's file name, which a verbose run echoes and a
/// quiet one never does.
const HELPER_STEM: &str = "dure-test-helper";

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
fn run_forwards_the_app_exit_status() {
    with_watchdog(|| {
        let dir = TempDir::new().unwrap();
        let helper = helper_exe();
        let args = vec![
            "--store-root".to_string(),
            dir.path().display().to_string(),
            "run".to_string(),
            "--".to_string(),
            helper.display().to_string(),
            "wait-exit".to_string(),
            SAMPLE_NONZERO_EXIT.to_string(),
        ];
        let client = ConsoleProcess::spawn(&dure_exe(), &args, dir.path());
        let attached = collect_until(&client, "session");
        // The helper's console is in line-input mode, so a lone character is
        // not delivered to `stdin.read` until a newline arrives.
        client.write_input(b"x\r\n");
        let status = client.wait();
        assert_eq!(status, SAMPLE_NONZERO_EXIT, "client output: {attached:?}");
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
fn run_gives_the_app_a_console() {
    with_watchdog(|| {
        let dir = TempDir::new().unwrap();
        let helper = helper_exe();
        let args = vec![
            "--store-root".to_string(),
            dir.path().display().to_string(),
            "run".to_string(),
            "--".to_string(),
            helper.display().to_string(),
            "wait-has-console".to_string(),
        ];
        let client = ConsoleProcess::spawn(&dure_exe(), &args, dir.path());
        // The pseudoconsole may emit `session` and the id with intervening
        // cursor sequences instead of a literal `session `.
        _ = collect_until(&client, "session");
        // The helper's console is in line-input mode, so a lone character is
        // not delivered to `stdin.read` until a newline arrives.
        client.write_input(b"x\r\n");
        let status = client.wait();
        assert_eq!(status, 0);
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
        let helper = helper_exe();
        let args = vec![
            "--store-root".to_string(),
            dir.path().display().to_string(),
            "run".to_string(),
            "--verbose".to_string(),
            "--".to_string(),
            helper.display().to_string(),
            "wait-exit".to_string(),
            "0".to_string(),
        ];
        let client = ConsoleProcess::spawn(&dure_exe(), &args, dir.path());
        // The app being launched is an input a quiet run never echoes, so
        // finding it proves the trace reached the terminal before attach.
        let output = collect_until(&client, HELPER_STEM);
        // The helper's console is in line-input mode, so a lone character is
        // not delivered to `stdin.read` until a newline arrives.
        client.write_input(b"x\r\n");
        let status = client.wait();
        assert_eq!(status, 0, "client output: {output:?}");
    });
}

#[cfg_attr(miri, ignore)]
#[test]
fn relayed_output_keeps_non_ascii_text_intact() {
    with_watchdog(|| {
        let dir = TempDir::new().unwrap();
        let helper = helper_exe();
        let args = vec![
            "--store-root".to_string(),
            dir.path().display().to_string(),
            "run".to_string(),
            "--".to_string(),
            helper.display().to_string(),
            "print-non-ascii".to_string(),
        ];
        let client = ConsoleProcess::spawn(&dure_exe(), &args, dir.path());
        let output = collect_until(&client, SAMPLE_NON_ASCII_TEXT);
        let status = client.wait();
        assert_eq!(status, 0, "client output: {output:?}");
    });
}

#[cfg_attr(miri, ignore)]
#[test]
fn a_dropped_client_leaves_the_session_resumable() {
    with_watchdog(|| {
        let dir = TempDir::new().unwrap();
        let helper = helper_exe();
        let run_args = vec![
            "--store-root".to_string(),
            dir.path().display().to_string(),
            "run".to_string(),
            "--".to_string(),
            helper.display().to_string(),
            "wait-exit".to_string(),
            SAMPLE_NONZERO_EXIT.to_string(),
        ];
        let client = ConsoleProcess::spawn(&dure_exe(), &run_args, dir.path());
        _ = collect_until(&client, "session");
        // Models the SSH connection dropping: the client dies where it stands,
        // without a chance to tell the supervisor anything.
        drop(client);

        let resume_args = vec![
            "--store-root".to_string(),
            dir.path().display().to_string(),
            "resume".to_string(),
        ];
        let resumed = ConsoleProcess::spawn(&dure_exe(), &resume_args, dir.path());
        _ = collect_until(&resumed, "session");
        // The app was blocked on console input across the disconnect. Reaching
        // it now, and seeing its own exit status arrive, is what makes the
        // resumed session interactive rather than merely alive.
        resumed.write_input(b"x\r\n");
        let status = resumed.wait();
        assert_eq!(status, SAMPLE_NONZERO_EXIT);
    });
}

#[cfg_attr(miri, ignore)]
#[test]
fn run_refuses_a_launcher_that_forbids_breakaway() {
    with_watchdog(|| {
        let dir = TempDir::new().unwrap();
        let helper = helper_exe();
        let args = run_helper_args(dir.path(), &helper);
        // Models a wrapper such as `cargo run`, whose job would kill the
        // supervisor along with the client it launched.
        let client = ConsoleProcess::spawn_confined(&dure_exe(), &args, dir.path());
        let output = collect_until(&client, "forbids breakaway");
        let status = client.wait();
        assert_ne!(status, 0, "client output: {output:?}");
        assert!(
            !output.contains("session "),
            "a refused launch must not report a session, got {output:?}"
        );
        // Nothing may be left behind for `resume` or `list` to find.
        let records = fs::read_dir(dir.path())
            .unwrap()
            .filter_map(Result::ok)
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert!(records.is_empty(), "store must stay empty, got {records:?}");
    });
}

/// Command line for a `run` that launches the wait-exit helper.
fn run_helper_args(store_root: &Path, helper: &Path) -> Vec<String> {
    vec![
        "--store-root".to_string(),
        store_root.display().to_string(),
        "run".to_string(),
        "--".to_string(),
        helper.display().to_string(),
        "wait-exit".to_string(),
        SAMPLE_NONZERO_EXIT.to_string(),
    ]
}

/// Whether the session banner in `output` names exactly `id`.
///
/// Whitespace is normalized away first, for the same line-wrapping reason
/// `collect_until` does it, so the digits are required to end where the id
/// does rather than merely to start with it.
fn banner_names_session(output: &str, id: u64) -> bool {
    let normalized = without_whitespace(output);
    let wanted = format!("session{id}");
    normalized
        .match_indices(&wanted)
        .any(|(at, _)| {
            let after = at.saturating_add(wanted.len());
            normalized
                .get(after..)
                .and_then(|rest| rest.chars().next())
                .is_none_or(|next| !next.is_ascii_digit())
        })
}

#[cfg_attr(miri, ignore)]
#[test]
fn run_warns_when_an_ancestor_job_would_end_the_session() {
    with_watchdog(|| {
        let dir = TempDir::new().unwrap();
        let helper = helper_exe();
        let args = run_helper_args(dir.path(), &helper);
        // The immediate job permits breakaway, so the supervisor starts, but the
        // ancestor it cannot leave would kill it with the launcher.
        let client = ConsoleProcess::spawn_confined_by_ancestor(&dure_exe(), &args, dir.path());
        // The banner follows the warning, so waiting for it collects both.
        let output = collect_until(&client, &format!("session {FIRST_SESSION_ID}"));
        assert!(
            output.contains("will not survive a disconnect"),
            "a session tied to the launcher must say so, got {output:?}"
        );
        // `collect_until` removes whitespace before searching, which is what
        // makes it robust against line wrapping but also erases the boundary
        // between the word and the id: `session 10` would satisfy a search for
        // `session 1`. The banner is therefore checked again here, against the
        // id a fresh store must have handed out.
        assert!(
            banner_names_session(&output, FIRST_SESSION_ID),
            "a fresh store must report session {FIRST_SESSION_ID}, got {output:?}"
        );
        client.write_input(b"x\r\n");
        let status = client.wait();
        assert_eq!(status, SAMPLE_NONZERO_EXIT, "client output: {output:?}");
    });
}
