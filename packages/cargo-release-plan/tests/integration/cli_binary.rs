//! Subprocess coverage of the `cargo-release-plan` binary entry point.
//!
//! These cases cover only what is unique to the executable: which stream each
//! outcome is written to and which exit status it produces. Classification and
//! plan semantics are covered in-process by the other modules of this suite.

use std::fs;
use std::process::{Command, Output};

use crate::fixture::{Fixture, write_package};

#[cfg_attr(miri, ignore)] // Spawns the compiled binary; Miri cannot emulate that.
#[test]
fn help_exits_success() {
    let output = release_plan(&["--help"], None);
    assert!(output.status.success());
    assert!(stdout(&output).contains("Usage"));
}

#[cfg_attr(miri, ignore)] // Spawns the compiled binary; Miri cannot emulate that.
#[test]
fn cargo_injected_subcommand_is_stripped() {
    let output = release_plan(&["release-plan", "--help"], None);
    assert!(output.status.success());
    assert!(stdout(&output).contains("Usage"));
}

#[cfg_attr(miri, ignore)] // Spawns the compiled binary; Miri cannot emulate that.
#[test]
fn unknown_flag_exits_failure() {
    let output = release_plan(&["--definitely-not-a-flag"], None);
    assert!(!output.status.success());
}

#[cfg_attr(miri, ignore)] // Spawns the compiled binary and git; Miri cannot emulate that.
#[test]
fn passing_check_writes_stdout_and_exits_success() {
    let fixture = seeded_package();
    let base = fixture.sha("HEAD");
    let output = release_plan(&["check", "--base", &base], Some(&fixture));

    assert!(output.status.success(), "{}", stderr(&output));
    assert!(!stdout(&output).is_empty());
    assert!(stderr(&output).is_empty());
}

#[cfg_attr(miri, ignore)] // Spawns the compiled binary, git, and cargo; Miri cannot emulate that.
#[test]
fn passing_packaging_warnings_write_stderr_without_replacing_stdout() {
    let fixture = seeded_package();
    let base = fixture.sha("HEAD");
    // The tool ignores untracked files while Cargo packages them, deliberately
    // producing a non-gating warning from the packaging cross-check.
    fixture.write("packages/demo/src/extra.rs", "pub fn g() {}\n");
    let output = release_plan(
        &["check", "--base", &base, "--verify-packaging"],
        Some(&fixture),
    );

    assert!(output.status.success(), "{}", stderr(&output));
    assert!(
        stdout(&output).contains("Every release and workspace-version check passed"),
        "{}",
        stdout(&output)
    );
    assert!(
        stderr(&output).contains("warning: packaging rule mismatch"),
        "{}",
        stderr(&output)
    );
    assert!(!stdout(&output).contains("warning:"));
}

#[cfg_attr(miri, ignore)] // Spawns the compiled binary and git; Miri cannot emulate that.
#[test]
fn failing_check_writes_stderr_and_exits_failure() {
    let fixture = seeded_package();
    fixture.write("packages/demo/src/lib.rs", "pub fn f() { let _ = 3; }\n");
    fixture.commit("content without a version increment");
    let base = fixture.sha("HEAD");
    let output = release_plan(&["check", "--base", &base], Some(&fixture));

    assert!(!output.status.success());
    assert!(stderr(&output).contains("needs-increment"));
    assert!(stdout(&output).is_empty());
}

#[cfg_attr(miri, ignore)] // Spawns the compiled binary and git; Miri cannot emulate that.
#[test]
fn operational_error_writes_stderr_and_exits_failure() {
    let fixture = seeded_package();
    // A base revision no repository resolves, so classification fails before it
    // can produce a verdict.
    let output = release_plan(&["check", "--base", "definitely-not-a-rev"], Some(&fixture));

    assert!(!output.status.success());
    assert!(stderr(&output).contains("Error:"));
    assert!(stdout(&output).is_empty());
}

#[cfg_attr(miri, ignore)] // Spawns the compiled binary and git; Miri cannot emulate that.
#[test]
fn report_writes_its_summary_to_stdout() {
    let fixture = seeded_package();
    let base = fixture.sha("HEAD");
    let out_dir = fixture.path().join("out");
    let output = release_plan(
        &[
            "report",
            "--base",
            &base,
            "--out-dir",
            &out_dir.to_string_lossy(),
        ],
        Some(&fixture),
    );

    assert!(output.status.success(), "{}", stderr(&output));
    assert!(stdout(&output).contains("report.json"));
}

#[cfg_attr(miri, ignore)] // Spawns the compiled binary and git; Miri cannot emulate that.
#[test]
fn apply_writes_its_summary_to_stdout() {
    let fixture = seeded_package();
    let plan_path = fixture.path().join("plan.json");
    fs::write(
        &plan_path,
        r#"{ "schema_version": 4, "increments": [{ "name": "demo", "level": "patch" }] }"#,
    )
    .unwrap();
    let output = release_plan(
        &["apply", "--plan", &plan_path.to_string_lossy(), "--dry-run"],
        Some(&fixture),
    );

    assert!(output.status.success(), "{}", stderr(&output));
    assert!(stdout(&output).contains("Dry run"));
}

fn seeded_package() -> Fixture {
    let fixture = Fixture::new("");
    write_package(&fixture, "demo", "0.1.0", "");
    fixture.commit("seed");
    fixture
}

fn release_plan(args: &[&str], fixture: Option<&Fixture>) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_cargo-release-plan"));
    command.args(args);
    if let Some(fixture) = fixture {
        command.current_dir(fixture.path());
    }
    command.output().unwrap()
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}
