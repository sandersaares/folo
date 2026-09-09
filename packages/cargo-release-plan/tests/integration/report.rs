//! Report and check output: the JSON document and the failure renderings.

use std::fs;

use cargo_release_plan::{CheckFormat, RunInput, RunOutcome, run};
use serde_json::{Value, json};

use crate::fixture::{Fixture, write_package};
use crate::harness::{check, report_json, seeded_package};

/// A compatible edge remains valid inside a transitively derived group.
#[cfg_attr(miri, ignore)] // Spawns git and cargo, which Miri cannot emulate.
#[test]
fn check_accepts_a_compatible_requirement_within_a_transitive_group() {
    let fixture = Fixture::new("");
    write_package(&fixture, "gamma", "1.1.0", "");
    write_package(
        &fixture,
        "beta",
        "1.1.0",
        "\n[dependencies]\ngamma = { path = \"../gamma\", version = \"=1.1.0\" }\n",
    );
    write_package(
        &fixture,
        "alpha",
        "1.1.0",
        "\n[dependencies]\nbeta = { path = \"../beta\", version = \"=1.1.0\" }\ngamma = { path = \"../gamma\", version = \"1.1.0\" }\n",
    );
    fixture.commit("seed");
    let base = fixture.sha("HEAD");

    let (passed, message) = check(&fixture, &base);

    assert!(passed, "{message}");
}

/// A stale exact pin still forms a group and is reported as version drift.
#[cfg_attr(miri, ignore)] // Spawns git and cargo, which Miri cannot emulate.
#[test]
fn check_reports_a_stale_exact_pin_without_losing_its_group() {
    let fixture = group_fixture("=1.0.0");
    fixture.commit("seed");
    let base = fixture.sha("HEAD");

    let (passed, message) = check(&fixture, &base);

    assert!(!passed, "{message}");
    assert!(
        message.contains("does not name the version it declares"),
        "{message}"
    );
    let report: Value = serde_json::from_str(&report_json(&fixture, &base)).unwrap();
    assert_eq!(
        report.pointer("/groups/lib/members"),
        Some(&json!(["lib", "lib_impl"]))
    );
}

/// A workspace whose `lib` requires its group sibling `lib_impl` with the given requirement.
fn group_fixture(requirement: &str) -> Fixture {
    let fixture = Fixture::new("");
    write_package(&fixture, "lib_impl", "1.1.0", "");
    write_package(
        &fixture,
        "lib",
        "1.1.0",
        &format!(
            r#"
[dependencies]
lib_impl = {{ path = "../lib_impl", version = "{requirement}" }}
"#
        ),
    );
    fixture
}

/// A requirement that does not name its target's declared version fails the check.
///
/// The workspace pins every intra-workspace requirement to the version its target declares, so
/// a requirement that merely admits that version is drift the merge gate has to catch.
#[cfg_attr(miri, ignore)] // Spawns git and cargo, which Miri cannot emulate.
#[test]
fn check_rejects_a_requirement_that_does_not_name_the_declared_version() {
    let fixture = Fixture::new("");
    write_package(&fixture, "helper", "1.1.0", "");
    write_package(
        &fixture,
        "demo",
        "0.1.0",
        r#"
[dependencies]
helper = { path = "../helper", version = "1.0.0" }
"#,
    );
    fixture.commit("seed");
    let base = fixture.sha("HEAD");

    let (passed, message) = check(&fixture, &base);

    assert!(!passed, "{message}");
    assert!(
        message.contains("does not name the version it declares"),
        "{message}"
    );
    assert!(message.contains("1.1.0"), "{message}");
}

/// The same workspace passes once the requirement names the declared version.
#[cfg_attr(miri, ignore)] // Spawns git and cargo, which Miri cannot emulate.
#[test]
fn check_accepts_a_requirement_naming_the_declared_version() {
    let fixture = Fixture::new("");
    write_package(&fixture, "helper", "1.1.0", "");
    write_package(
        &fixture,
        "demo",
        "0.1.0",
        r#"
[dependencies]
helper = { path = "../helper", version = "1.1.0" }
"#,
    );
    fixture.commit("seed");
    let base = fixture.sha("HEAD");

    let (passed, message) = check(&fixture, &base);

    assert!(passed, "{message}");
}

/// A package exposing a dependency that breaks must break as well.
///
/// `demo` re-exports `helper` types, declared through its allow-list, so `helper` moving to an
/// incompatible version changes the identity of what `demo` exposes.
#[cfg_attr(miri, ignore)] // Spawns git and cargo, which Miri cannot emulate.
#[test]
fn check_rejects_a_public_dependency_breaking_alone() {
    let fixture = public_dependency_fixture("1.0.0", "0.1.0");
    let base = fixture.sha("HEAD");

    // `helper` releases 1.0.0 -> 2.0.0 while `demo` stays on a compatible 0.1.1.
    write_public_dependency_packages(&fixture, "2.0.0", "0.1.1");

    let (passed, message) = check(&fixture, &base);

    assert!(!passed, "{message}");
    assert!(
        message.contains("must release a breaking change of its own"),
        "{message}"
    );
}

/// The same move passes once the dependent breaks too.
#[cfg_attr(miri, ignore)] // Spawns git and cargo, which Miri cannot emulate.
#[test]
fn check_accepts_a_public_dependency_breaking_together_with_its_dependent() {
    let fixture = public_dependency_fixture("1.0.0", "0.1.0");
    let base = fixture.sha("HEAD");

    // 0.1.0 -> 0.2.0 is incompatible on a 0.x line, so `demo` breaks as well.
    write_public_dependency_packages(&fixture, "2.0.0", "0.2.0");

    let (passed, message) = check(&fixture, &base);

    assert!(
        passed,
        "a dependent breaking alongside its public dependency is accepted: {message}"
    );
}

/// A workspace whose `demo` exposes `helper` in its public API, at the given versions.
fn public_dependency_fixture(helper: &str, demo: &str) -> Fixture {
    let fixture = Fixture::new("");
    write_public_dependency_packages(&fixture, helper, demo);
    fixture.commit("seed");
    fixture
}

fn write_public_dependency_packages(fixture: &Fixture, helper: &str, demo: &str) {
    write_package(fixture, "helper", helper, "");
    write_package(
        fixture,
        "demo",
        demo,
        &format!(
            r#"
[package.metadata.cargo_check_external_types]
allowed_external_types = ["helper::*"]

[dependencies]
helper = {{ path = "../helper", version = "{helper}" }}
"#
        ),
    );
}

#[cfg_attr(miri, ignore)] // Spawns git and cargo, which Miri cannot emulate.
#[test]
fn github_format_emits_workflow_annotations() {
    let fixture = seeded_package();
    fixture.write("packages/demo/src/lib.rs", "pub fn f() { let _ = 5; }\n");
    fixture.commit("content");
    let base = fixture.sha("HEAD");

    let outcome = run(&RunInput::Check {
        base: Some(base),
        manifest_path: fixture.manifest(),
        format: CheckFormat::Github,
        verify_packaging: false,
        verbose: false,
    })
    .unwrap();
    match outcome {
        RunOutcome::Check {
            passed, message, ..
        } => {
            assert!(!passed);
            assert!(message.contains("::error"));
            assert!(message.contains("increment-versions"));
        }
        other => panic!("expected check, got {other:?}"),
    }
}

#[cfg_attr(miri, ignore)] // Spawns git and cargo, which Miri cannot emulate.
#[test]
fn report_records_group_verdicts() {
    let fixture = Fixture::new("");
    write_package(
        &fixture,
        "alpha",
        "0.1.0",
        "\n[dependencies]\nbeta = { path = \"../beta\", version = \"=0.1.0\" }\n",
    );
    write_package(&fixture, "beta", "0.1.0", "");
    fixture.commit("seed");
    let base = fixture.sha("HEAD");

    let report = report_json(&fixture, &base);

    let report: Value = serde_json::from_str(&report).unwrap();
    assert_eq!(report.get("schema_version"), Some(&json!(4)));
    assert_eq!(
        report.pointer("/groups/alpha"),
        Some(&json!({
            "members": ["alpha", "beta"],
            "consistent": true,
            "version": "0.1.0"
        }))
    );
    assert_eq!(report.get("non_publishable_packages"), Some(&json!([])));
}

/// A helper-only group is represented entirely by version-target records.
#[cfg_attr(miri, ignore)] // Spawns git and cargo, which Miri cannot emulate.
#[test]
fn report_records_an_all_non_publishable_group() {
    let fixture = Fixture::new("");
    write_package(&fixture, "z-helper", "0.1.0", "\npublish = false\n");
    write_package(
        &fixture,
        "a-helper",
        "0.1.0",
        "\npublish = false\n\n[dependencies]\nz-helper = { path = \"../z-helper\", version = \"=0.1.0\" }\n",
    );
    fixture.commit("helper group");
    let base = fixture.sha("HEAD");

    let report: Value = serde_json::from_str(&report_json(&fixture, &base)).unwrap();

    assert_eq!(report.get("packages"), Some(&json!([])));
    assert_eq!(
        report.get("non_publishable_packages"),
        Some(&json!([
            {
                "name": "a-helper",
                "declared_version": "0.1.0",
                "group": "a-helper"
            },
            {
                "name": "z-helper",
                "declared_version": "0.1.0",
                "group": "a-helper"
            }
        ]))
    );
    assert_eq!(
        report.pointer("/groups/a-helper"),
        Some(&json!({
            "members": ["a-helper", "z-helper"],
            "consistent": true,
            "version": "0.1.0"
        }))
    );
}

/// Explicit wildcard requirements survive packaging and remain report relationships.
#[cfg_attr(miri, ignore)] // Spawns git and cargo, which Miri cannot emulate.
#[test]
fn report_retains_an_explicitly_versioned_dev_dependency() {
    let fixture = Fixture::new("");
    write_package(&fixture, "wildcard_helper", "0.1.0", "");
    write_package(&fixture, "path_only_helper", "0.1.0", "");
    write_package(
        &fixture,
        "demo",
        "0.1.0",
        r#"
[dev-dependencies]
wildcard_helper = { path = "../wildcard_helper", version = "*" }
path_only_helper = { path = "../path_only_helper" }
"#,
    );
    fixture.commit("seed");
    let base = fixture.sha("HEAD");

    let report: Value = serde_json::from_str(&report_json(&fixture, &base)).unwrap();
    let demo = report
        .get("packages")
        .and_then(Value::as_array)
        .unwrap()
        .iter()
        .find(|package| package.get("name").and_then(Value::as_str) == Some("demo"))
        .unwrap();
    let dependencies = demo.get("dependencies").unwrap();

    assert_eq!(
        dependencies,
        &json!([{
            "name": "wildcard_helper",
            "req": "*",
            "exact_pin": false,
            "public": false
        }])
    );
}

/// Report replaces the diffs of an earlier run.
///
/// A report directory is reused across runs, so a diff left over from a package that no longer has
/// one would still be read as current.
#[cfg_attr(miri, ignore)] // Spawns git and cargo, which Miri cannot emulate.
#[test]
fn report_replaces_the_diffs_of_an_earlier_run() {
    let fixture = Fixture::new("");
    write_package(&fixture, "demo", "0.1.0", "");
    fixture.commit("seed");
    let base = fixture.sha("HEAD");
    let out_dir = fixture.path().join("out");
    report_json(&fixture, &base);
    let stale = out_dir.join("diffs").join("stale.diff");
    fs::write(&stale, "leftover").unwrap();

    report_json(&fixture, &base);

    assert!(!stale.exists());
    assert!(out_dir.join("report.json").exists());
    assert!(!out_dir.join("report.json.tmp").exists());
}

/// A failed rerun removes the completion marker before changing patches.
///
/// A consumer treats `report.json` as the index of one complete report. Leaving
/// an earlier marker after patch replacement fails would make a mixed artifact
/// set appear complete.
#[cfg_attr(miri, ignore)] // Spawns git and cargo, which Miri cannot emulate.
#[test]
fn a_failed_rerun_does_not_leave_the_previous_report_marker() {
    let fixture = seeded_package();
    let base = fixture.sha("HEAD");
    let out_dir = fixture.path().join("out");
    report_json(&fixture, &base);
    fs::remove_dir_all(out_dir.join("diffs")).unwrap();
    fs::write(out_dir.join("diffs"), "blocks directory creation").unwrap();

    let result = run(&RunInput::Report {
        out_dir: out_dir.clone(),
        base: Some(base),
        manifest_path: fixture.manifest(),
        verbose: false,
    });

    result.expect_err("report rerun must fail after deleting tracked content");
    assert!(!out_dir.join("report.json").exists());
}
