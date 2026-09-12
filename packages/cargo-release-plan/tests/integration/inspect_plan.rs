//! Expanded-plan inspection validates the same tracked targets application accepts.

use std::fs;

use cargo_release_plan::{RunInput, RunOutcome, run};
use serde_json::{Value, json};

use crate::fixture::{Fixture, write_package};
use crate::harness::resolved_plan;

#[test]
#[cfg_attr(miri, ignore = "uses Git, Cargo metadata and a filesystem workspace")]
fn inspection_selects_only_publishable_members_and_never_edits_the_workspace() {
    let fixture = Fixture::new("");
    write_package(&fixture, "api", "1.0.0", "");
    write_package(&fixture, "helper", "1.0.0", "publish = false\n");
    fixture.commit("members");
    let path = fixture.path().join("expanded.json");
    fs::write(
        &path,
        json!({
            "schema_version": 4, "expanded": true, "increments": [
                {"name": "helper", "version": "1.0.1"}, {"name": "api", "version": "1.0.1"}
            ]
        })
        .to_string(),
    )
    .unwrap();
    let input = RunInput::InspectPlan {
        plan: path.clone(),
        require_resolved: false,
        manifest_path: fixture.manifest(),
        verbose: false,
    };
    let before = fixture.git(&["status", "--porcelain"]);
    let RunOutcome::ArtifactQuery { message } = run(&input).unwrap() else {
        panic!()
    };
    assert_eq!(
        serde_json::from_str::<Value>(&message).unwrap(),
        json!({
            "publication_targets": ["api"], "evidence_manifest_path": null
        })
    );
    assert_eq!(fixture.git(&["status", "--porcelain"]), before);
    _ = run(&RunInput::InspectPlan {
        plan: path.clone(),
        require_resolved: true,
        manifest_path: fixture.manifest(),
        verbose: false,
    })
    .unwrap_err();

    let captured = resolved_plan(&fixture, &path);
    let input = RunInput::InspectPlan {
        plan: captured,
        require_resolved: true,
        manifest_path: fixture.manifest(),
        verbose: false,
    };
    let before = fixture.git(&["status", "--porcelain"]);
    let RunOutcome::ArtifactQuery { message } = run(&input).unwrap() else {
        panic!()
    };
    let inspection: Value = serde_json::from_str(&message).unwrap();
    assert_eq!(
        inspection.get("publication_targets").unwrap(),
        &json!(["api"])
    );
    assert!(
        inspection
            .get("evidence_manifest_path")
            .unwrap()
            .as_str()
            .is_some()
    );
    assert_eq!(fixture.git(&["status", "--porcelain"]), before);
    fixture.write("packages/api/src/lib.rs", "pub fn changed() {}\n");
    _ = run(&input).unwrap_err();
}

#[test]
#[cfg_attr(
    miri,
    ignore = "uses Git and Cargo metadata to validate target identity"
)]
fn unknown_and_untracked_members_cannot_supply_publication_targets() {
    let fixture = Fixture::new("");
    write_package(&fixture, "api", "1.0.0", "");
    fixture.commit("tracked member");
    write_package(&fixture, "untracked", "1.0.0", "");
    let path = fixture.path().join("plan.json");
    for name in ["absent", "untracked"] {
        fs::write(
            &path,
            json!({
                "schema_version": 4, "expanded": true,
                "increments": [{"name": name, "version": "1.0.1"}]
            })
            .to_string(),
        )
        .unwrap();
        _ = run(&RunInput::InspectPlan {
            plan: path.clone(),
            require_resolved: false,
            manifest_path: fixture.manifest(),
            verbose: false,
        })
        .unwrap_err();
    }
}
