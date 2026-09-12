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
fn inspection_requires_a_plan_and_preserves_workspace_selection() {
    assert!(parse(&["inspect-plan"]).unwrap_err().status.is_err());
    let input = parse(&[
        "inspect-plan",
        "--plan",
        "plan.json",
        "--require-resolved",
        "--manifest-path",
        "workspace.toml",
        "--verbose",
    ])
    .unwrap()
    .into_input();
    match input {
        RunInput::InspectPlan {
            plan,
            require_resolved,
            manifest_path,
            verbose,
        } => {
            assert_eq!(plan, PathBuf::from("plan.json"));
            assert_eq!(manifest_path, PathBuf::from("workspace.toml"));
            assert!(require_resolved);
            assert!(verbose);
        }
        other => panic!("unexpected input {other:?}"),
    }
}

#[test]
fn artifact_commands_require_inputs_and_do_not_accept_workspace_options() {
    for command in ["analysis-order", "semver-targets"] {
        assert!(parse(&[command]).unwrap_err().status.is_err());
        for option in ["--base", "--manifest-path"] {
            assert!(
                parse(&[command, "--report", "report.json", option, "other"])
                    .unwrap_err()
                    .status
                    .is_err()
            );
        }
        let input = parse(&[command, "--report", "report.json", "--verbose"])
            .unwrap()
            .into_input();
        match input {
            RunInput::AnalysisOrder { report, verbose }
            | RunInput::SemverTargets { report, verbose } => {
                assert_eq!(report, PathBuf::from("report.json"));
                assert!(verbose);
            }
            other => panic!("unexpected input {other:?}"),
        }
    }
}

#[test]
fn proposal_requires_report_decisions_and_output() {
    for args in [
        vec![
            "propose",
            "--report",
            "evidence",
            "--decisions",
            "decisions.json",
        ],
        vec!["propose", "--report", "evidence", "--out", "plan.json"],
        vec![
            "propose",
            "--decisions",
            "decisions.json",
            "--out",
            "plan.json",
        ],
    ] {
        assert!(parse(&args).unwrap_err().status.is_err());
    }
    let input = parse(&[
        "propose",
        "--report",
        "evidence",
        "--decisions",
        "decisions.json",
        "--out",
        "plan.json",
        "--verbose",
    ])
    .unwrap()
    .into_input();
    match input {
        RunInput::Propose {
            report,
            decisions,
            out,
            verbose,
        } => {
            assert_eq!(report, PathBuf::from("evidence"));
            assert_eq!(decisions, PathBuf::from("decisions.json"));
            assert_eq!(out, PathBuf::from("plan.json"));
            assert!(verbose);
        }
        other => panic!("unexpected input {other:?}"),
    }
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
fn expand_requires_arguments() {
    assert!(parse(&["expand"]).unwrap_err().status.is_err());
}

#[test]
fn expand_requires_an_output_path() {
    assert!(
        parse(&["expand", "--plan", "plan.json"])
            .unwrap_err()
            .status
            .is_err()
    );
}

#[test]
fn expand_requires_a_plan() {
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

#[test]
fn preparation_requires_output_and_preserves_baseline_selection() {
    assert!(parse(&["prepare"]).unwrap_err().status.is_err());
    let input = parse(&[
        "prepare",
        "--output",
        "evidence",
        "--base",
        "main",
        "--verbose",
    ])
    .unwrap()
    .into_input();
    match input {
        RunInput::Prepare {
            output,
            base,
            manifest_path,
            verbose,
        } => {
            assert_eq!(output, PathBuf::from("evidence"));
            assert_eq!(base.as_deref(), Some("main"));
            assert_eq!(manifest_path, PathBuf::from("Cargo.toml"));
            assert!(verbose);
        }
        other => panic!("expected prepare, got {other:?}"),
    }
}

#[test]
fn preview_requires_the_prepared_state_proposal_and_output() {
    for args in [
        vec![
            "preview",
            "--plan",
            "proposal.json",
            "--output",
            "candidate",
        ],
        vec![
            "preview",
            "--prepared",
            "prepared.json",
            "--output",
            "candidate",
        ],
        vec![
            "preview",
            "--prepared",
            "prepared.json",
            "--plan",
            "proposal.json",
        ],
    ] {
        assert!(parse(&args).unwrap_err().status.is_err());
    }

    let input = parse(&[
        "preview",
        "--prepared",
        "prepared.json",
        "--plan",
        "proposal.json",
        "--output",
        "candidate",
        "--manifest-path",
        "workspace/Cargo.toml",
    ])
    .unwrap()
    .into_input();
    match input {
        RunInput::Preview {
            plan,
            prepared,
            output,
            manifest_path,
            verbose,
        } => {
            assert_eq!(plan, PathBuf::from("proposal.json"));
            assert_eq!(prepared, PathBuf::from("prepared.json"));
            assert_eq!(output, PathBuf::from("candidate"));
            assert_eq!(manifest_path, PathBuf::from("workspace/Cargo.toml"));
            assert!(!verbose);
        }
        other => panic!("expected preview, got {other:?}"),
    }
}

#[test]
fn verify_preview_requires_an_explicit_candidate_manifest() {
    assert!(
        parse(&["verify-preview", "--plan", "plan.json"])
            .unwrap_err()
            .status
            .is_err()
    );
    let input = parse(&[
        "verify-preview",
        "--plan",
        "plan.json",
        "--manifest-path",
        "candidate/Cargo.toml",
    ])
    .unwrap()
    .into_input();
    match input {
        RunInput::VerifyPreview {
            plan,
            manifest_path,
            verbose,
        } => {
            assert_eq!(plan, PathBuf::from("plan.json"));
            assert_eq!(manifest_path, PathBuf::from("candidate/Cargo.toml"));
            assert!(!verbose);
        }
        other => panic!("expected verify-preview, got {other:?}"),
    }
}
