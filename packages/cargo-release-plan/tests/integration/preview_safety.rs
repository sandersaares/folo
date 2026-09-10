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

#[test]
#[cfg_attr(miri, ignore = "uses owned Git and filesystem fixtures")]
fn output_subdirectories_cannot_own_preview_inputs() {
    let fixture = seeded_package();
    let prepared = prepare(&fixture);
    for directory in ["diffs", "workspace", ".prospective"] {
        let relative = format!("preview/{directory}/proposal.json");
        fixture.write(&relative, r#"{"schema_version":4,"increments":[]}"#);
        let plan = fixture.path().join(relative);
        let before = fs::read(&plan).unwrap();
        run(&RunInput::Preview {
            plan: plan.clone(),
            prepared: prepared.clone(),
            output: fixture.path().join("preview"),
            manifest_path: fixture.manifest(),
            verbose: false,
        })
        .unwrap_err();
        assert_eq!(fs::read(plan).unwrap(), before);
    }
}

#[test]
#[cfg_attr(miri, ignore = "uses owned Git and filesystem fixtures")]
fn occupied_prospective_directory_and_marker_fail_without_live_writes() {
    let fixture = seeded_package();
    fixture.write("prepared/.prospective/keep", "another owner");
    let manifest = fixture.read("packages/demo/Cargo.toml");
    run(&RunInput::Prepare {
        output: fixture.path().join("prepared"),
        base: Some("HEAD".to_owned()),
        manifest_path: fixture.manifest(),
        verbose: false,
    })
    .unwrap_err();
    assert_eq!(fixture.read("prepared/.prospective/keep"), "another owner");
    assert_eq!(fixture.read("packages/demo/Cargo.toml"), manifest);
    assert!(!fixture.path().join("Cargo.lock").exists());

    fixture.write("preview/plan.json/keep", "not a completion file");
    run(&RunInput::Preview {
        plan: fixture.path().join("proposal.json"),
        prepared: fixture.path().join("prepared/prepared.json"),
        output: fixture.path().join("preview"),
        manifest_path: fixture.manifest(),
        verbose: false,
    })
    .unwrap_err();
    assert_eq!(
        fixture.read("preview/plan.json/keep"),
        "not a completion file"
    );
    assert_eq!(fixture.read("packages/demo/Cargo.toml"), manifest);
}

#[test]
#[cfg_attr(miri, ignore = "uses real offline Cargo with an empty local source")]
fn offline_resolution_failure_never_becomes_prepared_evidence() {
    let fixture = seeded_package();
    fixture.write(
        ".cargo/config.toml",
        "[source.crates-io]\nreplace-with = \"local\"\n[source.local]\ndirectory = \"vendor\"\n",
    );
    fixture.write("vendor/.keep", "");
    fixture.git(&["add", ".cargo/config.toml", "vendor/.keep"]);
    let manifest = "[package]\nname = \"demo\"\nversion = \"0.1.0\"\nedition = \"2024\"\n\
                    [dependencies]\nfixture_only_missing_dependency = \"1.0.0\"\n";
    fixture.write("packages/demo/Cargo.toml", manifest);
    run(&RunInput::Prepare {
        output: fixture.path().join("prepared"),
        base: Some("HEAD".to_owned()),
        manifest_path: fixture.manifest(),
        verbose: false,
    })
    .unwrap_err();
    assert!(!fixture.path().join("prepared/prepared.json").exists());
    assert!(!fixture.path().join("prepared/.prospective").exists());
    assert!(!fixture.path().join("Cargo.lock").exists());
    assert_eq!(fixture.read("packages/demo/Cargo.toml"), manifest);
}

#[test]
#[cfg_attr(miri, ignore = "uses real Git and offline Cargo")]
fn preview_rejects_unknown_plan_targets_without_installing_files() {
    let fixture = seeded_package();
    let prepared = prepare(&fixture);
    let manifest = fixture.read("packages/demo/Cargo.toml");
    let lockfile = fixture.read("Cargo.lock");
    fixture.write(
        "proposal.json",
        r#"{"schema_version":4,"increments":[{"name":"missing","level":"patch"}]}"#,
    );
    run(&RunInput::Preview {
        plan: fixture.path().join("proposal.json"),
        prepared,
        output: fixture.path().join("preview"),
        manifest_path: fixture.manifest(),
        verbose: false,
    })
    .unwrap_err();
    assert!(!fixture.path().join("preview/plan.json").exists());
    assert_eq!(fixture.read("packages/demo/Cargo.toml"), manifest);
    assert_eq!(fixture.read("Cargo.lock"), lockfile);
}
