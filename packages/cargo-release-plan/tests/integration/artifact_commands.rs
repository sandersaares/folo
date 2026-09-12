//! Artifact commands work outside a checkout without Cargo, Git, or registry access.

use std::fs;
use std::path::Path;
use std::process::{Command, Output};

use serde_json::{Value, json};
use tempfile::tempdir;

fn command(directory: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_cargo-release-plan"))
        .args(args)
        .current_dir(directory)
        // No subprocess tool can be resolved; the report is the complete planning input.
        .env("PATH", "")
        .output()
        .unwrap()
}

fn write_report(directory: &Path) {
    fs::write(
        directory.join("report.json"),
        json!({
            "schema_version": 4, "head": "captured",
            "packages": [{
                "name": "api", "declared_version": "1.0.0", "status": "needs-increment",
                "anchor": {"commit": "anchor", "version": "1.0.0"},
                "changed": [{"source": "package", "path": "src/lib.rs", "change": "modified"}],
                "stat": {"files": 1, "insertions": 1, "deletions": 1},
                "dependencies": [], "dependents": [], "consumer_contract": true
            }],
            "non_publishable_packages": [], "groups": {}
        })
        .to_string(),
    )
    .unwrap();
}

#[test]
#[cfg_attr(
    miri,
    ignore = "executes the native binary against filesystem artifacts"
)]
fn queries_emit_only_json_to_stdout_without_workspace_discovery() {
    let directory = tempdir().unwrap();
    write_report(directory.path());
    for (subcommand, expected) in [
        (
            "analysis-order",
            json!([{"order": 1, "packages": ["api"], "cyclic": false}]),
        ),
        ("semver-targets", json!(["api"])),
    ] {
        let output = command(
            directory.path(),
            &[subcommand, "--report", ".", "--verbose"],
        );
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            serde_json::from_slice::<Value>(&output.stdout).unwrap(),
            expected
        );
        assert!(!output.stderr.is_empty());
        assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 1);
    }
}

#[test]
#[cfg_attr(
    miri,
    ignore = "executes the native binary against filesystem artifacts"
)]
fn proposal_writes_a_usable_plan_without_workspace_discovery() {
    let directory = tempdir().unwrap();
    write_report(directory.path());
    fs::write(
        directory.path().join("decisions.json"),
        json!({
            "schema_version": 1, "changes": [{"name": "api", "level": "nonbreaking"}]
        })
        .to_string(),
    )
    .unwrap();
    let output = command(
        directory.path(),
        &[
            "propose",
            "--report",
            "report.json",
            "--decisions",
            "decisions.json",
            "--out",
            "proposal.json",
            "--verbose",
        ],
    );
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let plan: Value =
        serde_json::from_slice(&fs::read(directory.path().join("proposal.json")).unwrap()).unwrap();
    assert_eq!(plan.get("schema_version").unwrap(), 4);
    assert_eq!(plan.get("increments").unwrap().as_array().unwrap().len(), 1);
    assert_eq!(plan.pointer("/increments/0/name").unwrap(), "api");
    assert!(plan.get("resolved").is_none());
    assert!(!output.stdout.is_empty());
    assert!(!output.stderr.is_empty());
}

#[test]
#[cfg_attr(
    miri,
    ignore = "executes the native binary against malformed artifacts"
)]
fn incompatible_artifacts_fail_without_success_output() {
    let directory = tempdir().unwrap();
    fs::write(
        directory.path().join("report.json"),
        r#"{"schema_version":3}"#,
    )
    .unwrap();
    for subcommand in ["analysis-order", "semver-targets"] {
        let output = command(directory.path(), &[subcommand, "--report", "report.json"]);
        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
        assert!(!output.stderr.is_empty());
    }
}

#[test]
#[cfg_attr(miri, ignore = "executes artifact commands against BOM-prefixed files")]
fn utf8_bom_does_not_change_report_commands_or_path_forms() {
    let directory = tempdir().unwrap();
    write_report(directory.path());
    fs::write(
        directory.path().join("decisions.json"),
        json!({
            "schema_version": 1, "changes": [{"name": "api", "level": "patch"}]
        })
        .to_string(),
    )
    .unwrap();
    let path = directory.path().join("report.json");
    let report = fs::read_to_string(&path).unwrap();
    let mut expected = Vec::new();
    for bom in ["", "\u{feff}"] {
        fs::write(&path, format!("{bom}{report}")).unwrap();
        let mut outcomes = Vec::new();
        for input in ["report.json", "."] {
            for subcommand in ["analysis-order", "semver-targets"] {
                let output = command(directory.path(), &[subcommand, "--report", input]);
                assert!(
                    output.status.success(),
                    "{}",
                    String::from_utf8_lossy(&output.stderr)
                );
                outcomes.push(serde_json::from_slice::<Value>(&output.stdout).unwrap());
            }
            let output = command(
                directory.path(),
                &[
                    "propose",
                    "--report",
                    input,
                    "--decisions",
                    "decisions.json",
                    "--out",
                    "proposal.json",
                ],
            );
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            outcomes.push(
                serde_json::from_slice::<Value>(
                    &fs::read(directory.path().join("proposal.json")).unwrap(),
                )
                .unwrap(),
            );
        }
        if bom.is_empty() {
            expected = outcomes;
        } else {
            assert_eq!(outcomes, expected);
        }
    }
}
