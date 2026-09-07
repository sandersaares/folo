//! Tests for the `cargo-release-plan` clap argument parser.
//!
//! These exercise the library-housed [`Cli`] directly (no subprocess), covering
//! subcommand requiredness, option defaults, and help/error early-exit.

use std::iter;
use std::path::PathBuf;

use cargo_release_plan::{CheckFormat, Cli, EarlyExit, RunInput};

fn parse(args: &[&str]) -> Result<Cli, EarlyExit> {
    Cli::from_args_os(iter::once("cargo-release-plan").chain(args.iter().copied()))
}

#[test]
fn missing_subcommand_prints_help() {
    let early = parse(&[]).unwrap_err();
    assert!(
        early.status.is_ok(),
        "clap treats a missing subcommand as a help request"
    );
    assert!(early.output.contains("Usage"));
}

#[test]
fn help_request_is_a_success_early_exit() {
    let early = parse(&["--help"]).unwrap_err();
    assert!(early.status.is_ok(), "help should be a success exit");
    assert!(early.output.contains("Usage"));
}

#[test]
fn unknown_flag_is_a_failure_early_exit() {
    let early = parse(&["--definitely-not-a-flag"]).unwrap_err();
    assert!(early.status.is_err());
}

#[test]
fn report_requires_out_dir() {
    let early = parse(&["report"]).unwrap_err();
    assert!(early.status.is_err());
}

#[test]
fn report_defers_base_and_defaults_the_manifest_path() {
    let input = parse(&["report", "--out-dir", "out"]).unwrap().into_input();
    match input {
        RunInput::Report {
            out_dir,
            base,
            manifest_path,
            verbose,
        } => {
            assert_eq!(out_dir, PathBuf::from("out"));
            assert_eq!(base, None, "an unset --base defers to the repository");
            assert_eq!(manifest_path, PathBuf::from("Cargo.toml"));
            assert!(!verbose);
        }
        other => panic!("expected report, got {other:?}"),
    }
}

#[test]
fn check_parses_github_format_and_verify_packaging() {
    let input = parse(&[
        "check",
        "--base",
        "HEAD",
        "--format",
        "github",
        "--verify-packaging",
        "--verbose",
    ])
    .unwrap()
    .into_input();
    match input {
        RunInput::Check {
            base,
            format,
            verify_packaging,
            verbose,
            ..
        } => {
            assert_eq!(base.as_deref(), Some("HEAD"));
            assert_eq!(format, CheckFormat::Github);
            assert!(verify_packaging);
            assert!(verbose);
        }
        other => panic!("expected check, got {other:?}"),
    }
}

#[test]
fn expand_requires_both_a_plan_and_an_output_path() {
    assert!(parse(&["expand"]).unwrap_err().status.is_err());
    assert!(
        parse(&["expand", "--plan", "plan.json"])
            .unwrap_err()
            .status
            .is_err()
    );
    assert!(
        parse(&["expand", "--out", "expanded.json"])
            .unwrap_err()
            .status
            .is_err()
    );
}

#[test]
fn expand_defaults_the_manifest_path() {
    let input = parse(&["expand", "--plan", "plan.json", "--out", "expanded.json"])
        .unwrap()
        .into_input();
    match input {
        RunInput::Expand {
            plan,
            out,
            manifest_path,
            verbose,
        } => {
            assert_eq!(plan, PathBuf::from("plan.json"));
            assert_eq!(out, PathBuf::from("expanded.json"));
            assert_eq!(manifest_path, PathBuf::from("Cargo.toml"));
            assert!(!verbose);
        }
        other => panic!("expected expand, got {other:?}"),
    }
}

#[test]
fn apply_requires_plan() {
    let early = parse(&["apply"]).unwrap_err();
    assert!(early.status.is_err());
}

#[test]
fn apply_parses_dry_run() {
    let input = parse(&["apply", "--plan", "plan.json", "--dry-run"])
        .unwrap()
        .into_input();
    match input {
        RunInput::Apply {
            plan,
            dry_run,
            manifest_path,
            verbose,
        } => {
            assert_eq!(plan, PathBuf::from("plan.json"));
            assert!(dry_run);
            assert_eq!(manifest_path, PathBuf::from("Cargo.toml"));
            assert!(!verbose);
        }
        other => panic!("expected apply, got {other:?}"),
    }
}
