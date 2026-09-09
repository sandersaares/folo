//! Captured Git membership and standalone preview completion boundaries.

use std::fs;

use cargo_release_plan::{RunInput, run};
use serde_json::Value;

use crate::harness::{check, prepare, resolved_plan, seeded_package};

#[test]
#[cfg_attr(miri, ignore = "spawns Git and offline Cargo")]
fn intent_to_add_source_requires_a_decision_and_survives_preview() {
    let fixture = seeded_package();
    fixture.write("packages/demo/src/new.rs", "pub fn added() {}\n");
    fixture.git(&["add", "-N", "packages/demo/src/new.rs"]);
    let prepared = prepare(&fixture);
    let report: Value = serde_json::from_str(&fixture.read("prepared/report.json")).unwrap();
    let demo = report
        .get("packages")
        .unwrap()
        .as_array()
        .unwrap()
        .first()
        .unwrap();
    assert_eq!(demo.get("status").unwrap(), "needs-increment");
    let index = fixture.git(&["ls-files", "--stage", "-z"]);
    fixture.write("proposal.json", r#"{"schema_version":4,"increments":[]}"#);
    let proposal = fixture.path().join("proposal.json");
    run(&RunInput::Preview {
        plan: proposal.clone(),
        prepared: prepared.clone(),
        output: fixture.path().join("preview"),
        manifest_path: fixture.manifest(),
        verbose: false,
    })
    .unwrap_err();
    assert!(!fixture.path().join("preview/plan.json").exists());
    assert_eq!(fixture.git(&["ls-files", "--stage", "-z"]), index);

    fixture.write(
        "proposal.json",
        r#"{"schema_version":4,"increments":[{"name":"demo","level":"patch"}]}"#,
    );
    run(&RunInput::Preview {
        plan: proposal,
        prepared,
        output: fixture.path().join("preview"),
        manifest_path: fixture.manifest(),
        verbose: false,
    })
    .unwrap();
    let plan = fixture.path().join("preview/plan.json");
    run(&RunInput::VerifyPreview {
        plan: plan.clone(),
        manifest_path: fixture.path().join("preview/workspace/Cargo.toml"),
        verbose: false,
    })
    .unwrap();
    run(&RunInput::Apply {
        plan,
        dry_run: false,
        manifest_path: fixture.manifest(),
        verbose: false,
    })
    .unwrap();
    assert!(check(&fixture, "HEAD").0);
    assert_eq!(fixture.git(&["ls-files", "--stage", "-z"]), index);
}

#[test]
#[cfg_attr(miri, ignore = "spawns Git and offline Cargo")]
fn failed_standalone_repreview_invalidates_the_previous_completion_marker() {
    let fixture = seeded_package();
    fixture.write(
        "proposal.json",
        r#"{"schema_version":4,"increments":[{"name":"demo","level":"patch"}]}"#,
    );
    let proposal = fixture.path().join("proposal.json");
    let plan = resolved_plan(&fixture, &proposal);
    assert!(plan.exists());
    fixture.write("proposal.json", "{ invalid json");
    run(&RunInput::Preview {
        plan: proposal.clone(),
        prepared: fixture.path().join("prepared/prepared.json"),
        output: fixture.path().join("preview"),
        manifest_path: fixture.manifest(),
        verbose: false,
    })
    .unwrap_err();
    assert!(!plan.exists());
    assert!(fixture.read("packages/demo/Cargo.toml").contains("0.1.0"));

    fixture.write(
        "proposal.json",
        r#"{"schema_version":4,"increments":[{"name":"demo","level":"patch"}]}"#,
    );
    let plan = resolved_plan(&fixture, &proposal);
    fixture.write("packages/demo/src/lib.rs", "pub fn stale() {}\n");
    run(&RunInput::Preview {
        plan: proposal,
        prepared: fixture.path().join("prepared/prepared.json"),
        output: fixture.path().join("preview"),
        manifest_path: fixture.manifest(),
        verbose: false,
    })
    .unwrap_err();
    assert!(!plan.exists());
}

#[test]
#[cfg_attr(miri, ignore = "spawns Git and offline Cargo")]
fn preview_output_cannot_destroy_an_input_document() {
    let fixture = seeded_package();
    let prepared = prepare(&fixture);
    fixture.write(
        "preview/plan.json",
        r#"{"schema_version":4,"increments":[]}"#,
    );
    let plan = fixture.path().join("preview/plan.json");
    let before = fs::read(&plan).unwrap();
    run(&RunInput::Preview {
        plan: plan.clone(),
        prepared,
        output: fixture.path().join("preview"),
        manifest_path: fixture.manifest(),
        verbose: false,
    })
    .unwrap_err();
    assert_eq!(fs::read(plan).unwrap(), before);

    fixture.write(
        "preview/report.json.tmp",
        r#"{"schema_version":4,"increments":[]}"#,
    );
    let staged_report = fixture.path().join("preview/report.json.tmp");
    let before = fs::read(&staged_report).unwrap();
    run(&RunInput::Preview {
        plan: staged_report.clone(),
        prepared: fixture.path().join("prepared/prepared.json"),
        output: fixture.path().join("preview"),
        manifest_path: fixture.manifest(),
        verbose: false,
    })
    .unwrap_err();
    assert_eq!(fs::read(staged_report).unwrap(), before);
}
