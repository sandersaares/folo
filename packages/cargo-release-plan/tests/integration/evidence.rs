//! Retained final workspaces used for compatibility evidence before application.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use cargo_release_plan::{RunInput, run};
use ohno::AppError;
use serde_json::Value;

use crate::fixture::{Fixture, write_package};
use crate::harness::{check, resolved_plan};

fn evidence_manifest(plan: &Path) -> PathBuf {
    let plan: Value = serde_json::from_slice(&fs::read(plan).unwrap()).unwrap();
    PathBuf::from(
        plan.get("resolved")
            .unwrap()
            .get("evidence_manifest_path")
            .unwrap()
            .as_str()
            .unwrap(),
    )
}

fn verify(plan: &Path, manifest: &Path) -> Result<(), AppError> {
    run(&RunInput::VerifyPreview {
        plan: plan.to_path_buf(),
        manifest_path: manifest.to_path_buf(),
        verbose: false,
    })
    .map(|_| ())
}

#[test]
#[cfg_attr(
    miri,
    ignore = "spawns Git and offline Cargo against retained workspaces"
)]
fn final_compatibility_workspace_has_candidate_versions_and_resolution() {
    let fixture = Fixture::new("");
    write_package(&fixture, "core", "0.1.0", "");
    write_package(
        &fixture,
        "tool",
        "0.1.0",
        "\n[dependencies]\ncore = { path = \"../core\", version = \"0.1.0\" }\n",
    );
    fixture.write("packages/tool/src/main.rs", "fn main() {}\n");
    fixture.cargo(&["generate-lockfile", "--offline"]);
    fixture.commit("released workspace");
    fixture.write(
        "proposal.json",
        r#"{"schema_version":4,"increments":[{"name":"core","level":"minor"}]}"#,
    );
    let plan = resolved_plan(&fixture, &fixture.path().join("proposal.json"));
    let candidate = evidence_manifest(&plan);
    let root = candidate.parent().unwrap();
    assert_eq!(root, fixture.path().join("preview/workspace"));
    assert!(
        fs::read_to_string(root.join("packages/core/Cargo.toml"))
            .unwrap()
            .contains("0.2.0")
    );
    assert!(fixture.read("packages/core/Cargo.toml").contains("0.1.0"));
    let expected_lock = fs::read(root.join("Cargo.lock")).unwrap();
    assert_ne!(
        expected_lock,
        fs::read(fixture.path().join("Cargo.lock")).unwrap()
    );

    let output = Command::new("cargo")
        .current_dir(root)
        .args([
            "metadata",
            "--locked",
            "--offline",
            "--format-version",
            "1",
            "--manifest-path",
        ])
        .arg(&candidate)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    verify(&plan, &candidate).unwrap();
    verify(&plan, &fixture.manifest()).unwrap_err();
    assert_eq!(fs::read(root.join("Cargo.lock")).unwrap(), expected_lock);

    fs::write(root.join("Cargo.lock"), fixture.read("Cargo.lock")).unwrap();
    verify(&plan, &candidate).unwrap_err();
    fs::write(root.join("Cargo.lock"), &expected_lock).unwrap();
    let source = root.join("packages/core/src/lib.rs");
    let original_source = fs::read(&source).unwrap();
    fs::write(&source, "pub fn changed_after_evidence() {}\n").unwrap();
    verify(&plan, &candidate).unwrap_err();
    fs::write(&source, &original_source).unwrap();
    let candidate_member = root.join("packages/core/Cargo.toml");
    let original_manifest = fs::read(&candidate_member).unwrap();
    fs::write(
        &candidate_member,
        String::from_utf8(original_manifest.clone())
            .unwrap()
            .replace("0.2.0", "0.3.0"),
    )
    .unwrap();
    verify(&plan, &candidate).unwrap_err();
    fs::write(&candidate_member, original_manifest).unwrap();
    verify(&plan, &candidate).unwrap();

    let live_source = fixture.read("packages/core/src/lib.rs");
    fixture.write(
        "packages/core/src/lib.rs",
        "pub fn changed_live_source() {}\n",
    );
    verify(&plan, &candidate).unwrap_err();
    fixture.write("packages/core/src/lib.rs", &live_source);
    verify(&plan, &candidate).unwrap();

    // Application depends on the original snapshot and captured bytes, not the retained tree.
    fs::remove_dir_all(root).unwrap();
    run(&RunInput::Apply {
        plan,
        dry_run: false,
        manifest_path: fixture.manifest(),
        verbose: false,
    })
    .unwrap();
    assert!(check(&fixture, "HEAD").0);
}

#[test]
#[cfg_attr(
    miri,
    ignore = "spawns Git and offline Cargo against retained workspaces"
)]
fn repreview_replaces_owned_evidence_and_rejects_an_unowned_directory() {
    let fixture = Fixture::new("");
    write_package(&fixture, "library", "0.1.0", "");
    fixture.commit("released workspace");
    fixture.write(
        "proposal.json",
        r#"{"schema_version":4,"increments":[{"name":"library","level":"patch"}]}"#,
    );
    let proposal = fixture.path().join("proposal.json");
    let plan = resolved_plan(&fixture, &proposal);
    let candidate = evidence_manifest(&plan);
    let marker = candidate.parent().unwrap().join("old-evidence");
    fs::write(&marker, "").unwrap();
    let plan = resolved_plan(&fixture, &proposal);
    assert!(!marker.exists());
    verify(&plan, &evidence_manifest(&plan)).unwrap();

    let marker = candidate
        .parent()
        .unwrap()
        .join(".git/cargo-release-plan-preview");
    fs::remove_file(marker).unwrap();
    let retained_source = candidate
        .parent()
        .unwrap()
        .join("packages/library/src/lib.rs");
    let retained_bytes = fs::read(&retained_source).unwrap();
    run(&RunInput::Preview {
        plan: proposal,
        prepared: fixture.path().join("prepared/prepared.json"),
        output: fixture.path().join("preview"),
        manifest_path: fixture.manifest(),
        verbose: false,
    })
    .unwrap_err();
    assert_eq!(fs::read(retained_source).unwrap(), retained_bytes);
    assert!(!plan.exists());
}
