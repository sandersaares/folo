//! Scaffolding shared by the integration modules.
//!
//! Provides fixture workspaces that are already seeded, and thin wrappers that
//! drive one run and reduce its outcome to what a test asserts on.

use std::fs;
use std::path::{Path, PathBuf};

use cargo_release_plan::{CheckFormat, RunInput, RunOutcome, run};

use crate::fixture::{Fixture, write_package};

pub(crate) fn seeded_package() -> Fixture {
    let fixture = Fixture::new("");
    write_package(&fixture, "demo", "0.1.0", "");
    fixture.commit("seed");
    fixture
}

/// Workspace whose declared member contains a second package reached by path.
///
/// `packages/*` matches `packages/outer` only, so `inner` is a member solely
/// through the path dependency, and it sits inside the outer package directory.
pub(crate) fn nested_workspace() -> Fixture {
    let fixture = Fixture::new("");
    write_package(
        &fixture,
        "outer",
        "0.1.0",
        "\n[dependencies]\ninner = { path = \"inner\", version = \"0.1.0\" }",
    );
    fixture.write(
        "packages/outer/inner/Cargo.toml",
        "[package]\nname = \"inner\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
    );
    fixture.write("packages/outer/inner/src/lib.rs", "pub fn g() {}\n");
    fixture.commit("seed");
    fixture
}

pub(crate) fn check(fixture: &Fixture, base: &str) -> (bool, String) {
    check_workspace(base, fixture.manifest())
}

pub(crate) fn check_result(fixture: &Fixture, base: &str) -> Result<(bool, String), String> {
    match run(&RunInput::Check {
        base: Some(base.to_string()),
        manifest_path: fixture.manifest(),
        format: CheckFormat::Text,
        verify_packaging: false,
        verbose: false,
    }) {
        Ok(RunOutcome::Check {
            passed, message, ..
        }) => Ok((passed, message)),
        Ok(other) => panic!("expected check, got {other:?}"),
        Err(error) => Err(format!("{error}")),
    }
}

pub(crate) fn check_verbose(fixture: &Fixture, base: &str) -> (bool, String) {
    match run(&RunInput::Check {
        base: Some(base.to_string()),
        manifest_path: fixture.manifest(),
        format: CheckFormat::Text,
        verify_packaging: false,
        verbose: true,
    }) {
        Ok(RunOutcome::Check {
            passed, message, ..
        }) => (passed, message),
        Ok(other) => panic!("expected check, got {other:?}"),
        Err(error) => panic!("{error}"),
    }
}

pub(crate) fn check_workspace(base: &str, manifest_path: PathBuf) -> (bool, String) {
    match run(&RunInput::Check {
        base: Some(base.to_string()),
        manifest_path,
        format: CheckFormat::Text,
        verify_packaging: false,
        verbose: false,
    }) {
        Ok(RunOutcome::Check {
            passed, message, ..
        }) => (passed, message),
        Ok(other) => panic!("expected check, got {other:?}"),
        Err(error) => panic!("{error}"),
    }
}

/// Runs `check` with no baseline, leaving the tool to discover one.
pub(crate) fn check_discovering_base(fixture: &Fixture) -> Result<(bool, String), String> {
    match run(&RunInput::Check {
        base: None,
        manifest_path: fixture.manifest(),
        format: CheckFormat::Text,
        verify_packaging: false,
        verbose: true,
    }) {
        Ok(RunOutcome::Check {
            passed, message, ..
        }) => Ok((passed, message)),
        Ok(other) => panic!("expected check, got {other:?}"),
        Err(error) => Err(format!("{error}")),
    }
}

pub(crate) fn check_verifying_packaging(fixture: &Fixture, base: &str) -> (bool, String) {
    match run(&RunInput::Check {
        base: Some(base.to_string()),
        manifest_path: fixture.manifest(),
        format: CheckFormat::Text,
        verify_packaging: true,
        verbose: false,
    }) {
        Ok(RunOutcome::Check {
            passed, warnings, ..
        }) => (passed, warnings),
        Ok(other) => panic!("expected check, got {other:?}"),
        Err(error) => panic!("{error}"),
    }
}

pub(crate) fn report_json(fixture: &Fixture, base: &str) -> String {
    let out_dir = fixture.path().join("out");
    run(&RunInput::Report {
        out_dir: out_dir.clone(),
        base: Some(base.to_string()),
        manifest_path: fixture.manifest(),
        verbose: false,
    })
    .unwrap();
    fs::read_to_string(out_dir.join("report.json")).unwrap()
}

pub(crate) fn apply_increment(fixture: &Fixture, name: &str, level: &str) {
    let plan_path = fixture.path().join("plan.json");
    fs::write(
        &plan_path,
        format!(
            r#"{{ "schema_version": 4, "increments": [{{ "name": "{name}", "level": "{level}" }}] }}"#
        ),
    )
    .unwrap();

    run(&RunInput::Apply {
        plan: plan_path,
        dry_run: false,
        manifest_path: fixture.manifest(),
        verbose: true,
    })
    .unwrap();
}

pub(crate) fn resolved_plan(fixture: &Fixture, proposal: &Path) -> PathBuf {
    let prepared = prepare(fixture);
    let output = fixture.path().join("preview");
    run(&RunInput::Preview {
        plan: proposal.to_path_buf(),
        prepared,
        output: output.clone(),
        manifest_path: fixture.manifest(),
        verbose: false,
    })
    .unwrap();
    output.join("plan.json")
}

pub(crate) fn prepare(fixture: &Fixture) -> PathBuf {
    let prepared = fixture.path().join("prepared");
    run(&RunInput::Prepare {
        output: prepared.clone(),
        base: Some("HEAD".to_owned()),
        manifest_path: fixture.manifest(),
        verbose: false,
    })
    .unwrap();
    prepared.join("prepared.json")
}
