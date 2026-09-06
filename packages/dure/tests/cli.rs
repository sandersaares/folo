//! Process-level tests that do not require a console.
//!
//! Every test here runs the built binary with pipe-backed stdio, which is also
//! the environment the attaching commands must refuse (design.md, "Console
//! I/O"). Child waits run inside the workspace watchdog so a binary that stops
//! returning fails this suite instead of the whole job.
//!
//! `dure` is a Windows-only binary; on other platforms it only reports that it
//! is unsupported.
#![cfg(windows)]

use std::ffi::{OsStr, OsString};
use std::process::{Command, Output};

use dure::SessionId;
use tempfile::TempDir;

/// Runs the built binary to completion under the workspace watchdog.
fn run_dure<I, S>(args: I) -> Output
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let args: Vec<OsString> = args
        .into_iter()
        .map(|arg| arg.as_ref().to_os_string())
        .collect();
    testing::with_watchdog(move || {
        Command::new(env!("CARGO_BIN_EXE_dure"))
            .args(&args)
            .output()
            .unwrap()
    })
}

// Talks to the real operating system: runs the built binary as a child process.
#[cfg_attr(miri, ignore)]
#[test]
fn help_advertises_every_published_command() {
    let output = run_dure(["--help"]);
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    // The README teaches these four commands, so help has to keep offering them.
    for command in ["run", "resume", "list", "kill"] {
        assert!(stdout.contains(command), "{command} missing from {stdout}");
    }
}

// Talks to the real operating system: runs the built binary as a child process.
#[cfg_attr(miri, ignore)]
#[test]
fn list_empty_store() {
    let dir = TempDir::new().unwrap();
    let output = run_dure([
        OsStr::new("--store-root"),
        dir.path().as_os_str(),
        OsStr::new("list"),
    ]);
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("ID"));
    assert!(stdout.contains("ATTACHED"));
}

// Talks to the real operating system: runs the built binary as a child process.
#[cfg_attr(miri, ignore)]
#[test]
fn kill_missing_session_reports_the_session_as_not_live() {
    let dir = TempDir::new().unwrap();
    let id = SessionId::MIN;
    let output = run_dure([
        OsStr::new("--store-root"),
        dir.path().as_os_str(),
        OsStr::new("kill"),
        OsStr::new(&id.to_string()),
    ]);
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    // Naming the session distinguishes the lookup this test is about from an
    // argument or dispatch failure, which would also exit non-zero.
    assert!(
        stderr.contains(&format!("Session {id} is not a live session")),
        "unexpected failure: {stderr}"
    );
}

// Talks to the real operating system: runs the built binary as a child process.
#[cfg_attr(miri, ignore)]
#[test]
fn run_refuses_pipe_backed_stdio() {
    let dir = TempDir::new().unwrap();
    let output = run_dure([
        OsStr::new("--store-root"),
        dir.path().as_os_str(),
        OsStr::new("run"),
        OsStr::new("--"),
        OsStr::new("cmd.exe"),
    ]);
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Attaching requires a console"),
        "unexpected failure: {stderr}"
    );
}

// Talks to the real operating system: runs the built binary as a child process.
#[cfg_attr(miri, ignore)]
#[test]
fn list_explains_itself_only_when_verbose() {
    let dir = TempDir::new().unwrap();
    let root = dir.path();
    let list = |verbose: bool| {
        let mut args = vec![
            OsStr::new("--store-root"),
            root.as_os_str(),
            OsStr::new("list"),
        ];
        if verbose {
            args.push(OsStr::new("--verbose"));
        }
        let output = run_dure(args);
        assert!(output.status.success());
        String::from_utf8_lossy(&output.stderr).into_owned()
    };

    // The store root is an input behind every later decision, so a verbose run
    // states which store it read and a quiet one says nothing at all.
    assert!(
        list(true).contains(&root.display().to_string()),
        "verbose run must name its store"
    );
    assert!(list(false).is_empty(), "quiet run must stay silent");
}

// Talks to the real operating system: runs the built binary as a child process.
#[cfg_attr(miri, ignore)]
#[test]
fn kill_without_id_is_a_parse_error() {
    let output = run_dure(["kill"]);
    assert!(!output.status.success());
}

// Talks to the real operating system: runs the built binary as a child process.
#[cfg_attr(miri, ignore)]
#[test]
fn naming_no_command_is_a_failure() {
    let output = run_dure(Vec::<&OsStr>::new());
    assert!(!output.status.success());
}
