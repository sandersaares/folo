//! Captured-state rejection boundaries and nested workspace input identity.

use std::fs;
use std::path::Path;
#[cfg(windows)]
use std::path::PathBuf;
#[cfg(windows)]
use std::process::Command;

#[cfg(windows)]
use cargo_release_plan::{CheckFormat, RunOutcome};
use cargo_release_plan::{RunInput, run};
use serde_json::{Value, json};

use crate::fixture::{Fixture, write_package};
use crate::harness::{prepare, resolved_plan, seeded_package};

#[test]
#[cfg_attr(miri, ignore = "captures real Git and Cargo workspace inputs")]
fn changed_identity_and_resolved_metadata_fail_before_writing() {
    let fixture = seeded_package();
    fixture.write(
        "proposal.json",
        r#"{"schema_version":4,"increments":[{"name":"demo","level":"patch"}]}"#,
    );
    let plan = resolved_plan(&fixture, &fixture.path().join("proposal.json"));
    let original: Value = serde_json::from_slice(&fs::read(&plan).unwrap()).unwrap();
    let manifest = fixture.read("packages/demo/Cargo.toml");
    let lockfile = fixture.read("Cargo.lock");
    let changes = [
        (
            "/resolved/inputs/root",
            json!(fixture.path().join("different")),
        ),
        ("/resolved/inputs/manifest", json!("different/Cargo.toml")),
        ("/resolved/inputs/head", json!("0".repeat(40))),
        ("/resolved/inputs/base", json!("0".repeat(40))),
        ("/resolved/inputs/index", json!("different index")),
        ("/resolved/inputs/paths", json!([])),
        ("/resolved/versions", json!({"demo":"9.0.0"})),
        ("/resolved/final_digest", json!("not the captured digest")),
        ("/expanded", json!(false)),
    ];
    for (pointer, value) in changes {
        let mut changed = original.clone();
        *changed.pointer_mut(pointer).unwrap() = value;
        fs::write(&plan, serde_json::to_vec(&changed).unwrap()).unwrap();
        run(&RunInput::Apply {
            plan: plan.clone(),
            dry_run: false,
            manifest_path: fixture.manifest(),
            verbose: false,
        })
        .unwrap_err();
        assert_eq!(fixture.read("packages/demo/Cargo.toml"), manifest);
        assert_eq!(fixture.read("Cargo.lock"), lockfile);
    }
    let mut unknown_target = original;
    unknown_target
        .get_mut("increments")
        .unwrap()
        .as_array_mut()
        .unwrap()
        .push(json!({"name":"untracked-package","version":"1.0.0"}));
    fs::write(&plan, serde_json::to_vec(&unknown_target).unwrap()).unwrap();
    run(&RunInput::Apply {
        plan,
        dry_run: false,
        manifest_path: fixture.manifest(),
        verbose: false,
    })
    .unwrap_err();
    assert_eq!(fixture.read("packages/demo/Cargo.toml"), manifest);
    assert_eq!(fixture.read("Cargo.lock"), lockfile);
}

#[test]
#[cfg_attr(miri, ignore = "captures real Git and Cargo workspace inputs")]
fn captured_writes_reject_extra_duplicate_and_missing_artifacts() {
    let fixture = seeded_package();
    fixture.write(
        "proposal.json",
        r#"{"schema_version":4,"increments":[{"name":"demo","level":"patch"}]}"#,
    );
    let plan = resolved_plan(&fixture, &fixture.path().join("proposal.json"));
    let original: Value = serde_json::from_slice(&fs::read(&plan).unwrap()).unwrap();
    let manifest = fixture.read("packages/demo/Cargo.toml");
    let lockfile = fixture.read("Cargo.lock");
    let original_files = original
        .pointer("/resolved/files")
        .unwrap()
        .as_array()
        .unwrap();
    let mut duplicate = original_files.clone();
    duplicate.push(original_files.first().unwrap().clone());
    let mut source = original_files.clone();
    source.push(json!({"path":"packages/demo/src/lib.rs","contents":"unplanned source"}));
    let mut outside = original_files.clone();
    outside.push(json!({"path":"../Cargo.toml","contents":"outside"}));
    let mut missing = original_files.clone();
    missing.pop().unwrap();
    for files in [duplicate, source, outside, missing] {
        let mut changed = original.clone();
        *changed.pointer_mut("/resolved/files").unwrap() = json!(files);
        fs::write(&plan, serde_json::to_vec(&changed).unwrap()).unwrap();
        run(&RunInput::Apply {
            plan: plan.clone(),
            dry_run: false,
            manifest_path: fixture.manifest(),
            verbose: false,
        })
        .unwrap_err();
        assert_eq!(fixture.read("packages/demo/Cargo.toml"), manifest);
        assert_eq!(fixture.read("Cargo.lock"), lockfile);
    }
}

#[test]
#[cfg_attr(miri, ignore = "captures real Git and Cargo workspace inputs")]
fn modified_preparation_bytes_cannot_become_a_preview() {
    let fixture = seeded_package();
    let prepared = prepare(&fixture);
    let original: Value = serde_json::from_slice(&fs::read(&prepared).unwrap()).unwrap();
    let lockfile = fixture.read("Cargo.lock");
    fixture.write("proposal.json", r#"{"schema_version":4,"increments":[]}"#);
    for alternative_resolution in [false, true] {
        let mut changed = original.clone();
        if alternative_resolution {
            changed.as_object_mut().unwrap().insert(
                "files".to_owned(),
                json!([{"path":"Cargo.lock","contents":"different resolution"}]),
            );
        } else {
            *changed.pointer_mut("/inputs/digest").unwrap() = json!("different digest");
        }
        fs::write(&prepared, serde_json::to_vec(&changed).unwrap()).unwrap();
        run(&RunInput::Preview {
            plan: fixture.path().join("proposal.json"),
            prepared: prepared.clone(),
            output: fixture.path().join("preview"),
            manifest_path: fixture.manifest(),
            verbose: false,
        })
        .unwrap_err();
        assert!(!fixture.path().join("preview/plan.json").exists());
        assert!(!fixture.path().join("preview/.prospective").exists());
        assert_eq!(fixture.read("Cargo.lock"), lockfile);
    }
}

#[test]
#[cfg_attr(miri, ignore = "captures nested real Git and Cargo workspace inputs")]
fn nested_workspace_preparation_captures_ancestor_configuration_and_default_base() {
    let fixture = Fixture::with_workspace_manifest(
        "rust/Cargo.toml",
        "[workspace]\nmembers = [\"packages/demo\"]\nresolver = \"2\"\n",
    );
    fixture.write(
        "rust/packages/demo/Cargo.toml",
        "[package]\nname = \"demo\"\nversion = \"0.1.0\"\nedition = \"2024\"\n",
    );
    fixture.write("rust/packages/demo/src/lib.rs", "");
    fixture.write(".cargo/config.toml", "[term]\nquiet = true\n");
    fixture.commit("nested workspace");
    let base = fixture.sha("HEAD");
    fixture.git(&["update-ref", "refs/remotes/origin/main", &base]);
    run(&RunInput::Prepare {
        output: fixture.path().join("prepared"),
        base: None,
        manifest_path: fixture.manifest(),
        verbose: false,
    })
    .unwrap();
    let prepared: Value = serde_json::from_str(&fixture.read("prepared/prepared.json")).unwrap();
    let inputs = prepared.get("inputs").unwrap();
    assert_eq!(inputs.get("base").unwrap(), &json!(base));
    assert_eq!(
        Path::new(inputs.get("manifest").unwrap().as_str().unwrap()),
        Path::new("rust/Cargo.toml")
    );
    assert!(
        inputs
            .get("paths")
            .unwrap()
            .as_array()
            .unwrap()
            .iter()
            .any(|value| { Path::new(value.as_str().unwrap()) == Path::new(".cargo/config.toml") })
    );
}

#[test]
#[cfg_attr(
    miri,
    ignore = "captures a real Cargo workspace with an absolute local dependency"
)]
fn preparation_rejects_absolute_paths_before_creating_a_resolution_workspace() {
    let fixture = seeded_package();
    write_package(&fixture, "core", "0.1.0", "");
    fixture.commit("local dependency target");
    let manifest = format!(
        "[package]\nname = \"demo\"\nversion = \"0.1.0\"\nedition = \"2024\"\n\
         [dependencies]\ncore = {{ version = \"0.1.0\", path = {:?} }}\n",
        fixture.path().join("packages/core")
    );
    fixture.write("packages/demo/Cargo.toml", &manifest);
    run(&RunInput::Prepare {
        output: fixture.path().join("prepared"),
        base: Some("HEAD".to_owned()),
        manifest_path: fixture.manifest(),
        verbose: false,
    })
    .unwrap_err();
    assert!(!fixture.path().join("prepared/.prospective").exists());
    assert_eq!(fixture.read("packages/demo/Cargo.toml"), manifest);
    assert!(!fixture.path().join("Cargo.lock").exists());
}

#[test]
#[cfg(windows)]
#[cfg_attr(
    miri,
    ignore = "uses Windows short paths and native PowerShell/Git/Cargo"
)]
fn short_windows_paths_use_the_same_captured_workspace_identity() {
    let fixture = seeded_package();
    // PowerShell is part of the repository's Windows test environment. The filesystem API
    // probes actual short-name availability instead of assuming the volume provides it.
    fixture.write(
        "short-path.ps1",
        "# Returns the actual short-name spelling for this test's owned directory.\n\
         param([string] $Path)\n\
         Set-StrictMode -Version Latest\n\
         $ErrorActionPreference = 'Stop'\n\
         $PSNativeCommandUseErrorActionPreference = $true\n\
         $filesystem = New-Object -ComObject Scripting.FileSystemObject\n\
         $filesystem.GetFolder($Path).ShortPath\n",
    );
    let output = Command::new("pwsh")
        .args(["-NoProfile", "-NonInteractive", "-File"])
        .arg(fixture.path().join("short-path.ps1"))
        .arg(fixture.path())
        .output()
        .unwrap();
    assert!(output.status.success());
    let short_root = PathBuf::from(String::from_utf8(output.stdout).unwrap().trim());
    assert_eq!(
        fs::canonicalize(&short_root).unwrap(),
        fs::canonicalize(fixture.path()).unwrap()
    );
    if short_root == fixture.path() {
        eprintln!("This volume exposes no distinct short directory name.");
        return;
    }
    let manifest = short_root.join("Cargo.toml");
    let prepared = short_root.join("prepared");
    run(&RunInput::Prepare {
        output: prepared.clone(),
        base: Some("HEAD".to_owned()),
        manifest_path: manifest.clone(),
        verbose: false,
    })
    .unwrap();
    fixture.write(
        "proposal.json",
        r#"{"schema_version":4,"increments":[{"name":"demo","level":"patch"}]}"#,
    );
    let preview = short_root.join("preview");
    run(&RunInput::Preview {
        plan: short_root.join("proposal.json"),
        prepared: prepared.join("prepared.json"),
        output: preview.clone(),
        manifest_path: manifest.clone(),
        verbose: false,
    })
    .unwrap();
    let plan = preview.join("plan.json");
    let document: Value = serde_json::from_slice(&fs::read(&plan).unwrap()).unwrap();
    let candidate = document
        .pointer("/resolved/evidence_manifest_path")
        .unwrap()
        .as_str()
        .unwrap();
    run(&RunInput::VerifyPreview {
        plan: plan.clone(),
        manifest_path: PathBuf::from(candidate),
        verbose: false,
    })
    .unwrap();
    run(&RunInput::Apply {
        plan,
        dry_run: false,
        manifest_path: manifest.clone(),
        verbose: false,
    })
    .unwrap();
    assert!(fixture.read("packages/demo/Cargo.toml").contains("0.1.1"));
    let outcome = run(&RunInput::Check {
        base: Some("HEAD".to_owned()),
        manifest_path: manifest,
        format: CheckFormat::Text,
        verify_packaging: false,
        verbose: false,
    })
    .unwrap();
    assert!(matches!(outcome, RunOutcome::Check { passed: true, .. }));
}
