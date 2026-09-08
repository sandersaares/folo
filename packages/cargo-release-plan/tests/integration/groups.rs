//! Version-group consistency and closure over members.

use std::fs;

use cargo_release_plan::{CheckFormat, RunInput, RunOutcome, run};
use serde_json::{Value, json};

use crate::fixture::{Fixture, write_package};
use crate::harness::{check, report_json};

/// A group member absent from the base leaves the group consistent.
///
/// Consistency compares the versions members were released under, and a member
/// the base does not carry has none yet, so requiring it to match would fail a
/// group for a package no consumer can have seen.
/// Ref: docs/design.md, "Version groups".
#[cfg_attr(miri, ignore)] // Spawns git and cargo, which Miri cannot emulate.
#[test]
fn group_closure_with_member_absent_from_base_is_consistent() {
    let fixture = Fixture::new("");
    write_package(
        &fixture,
        "alpha",
        "0.1.0",
        "\n[dependencies]\nbeta = { path = \"../beta\", version = \"=0.1.0\" }\n",
    );
    fixture.commit("alpha only");
    let base = fixture.sha("HEAD");
    write_package(&fixture, "beta", "0.1.0", "");
    fixture.commit("add group member absent from base");

    let (passed, message) = check(&fixture, &base);
    assert!(passed, "{message}");
}

/// A group member withdrawn on the base is still held to its group.
///
/// The base carries it, and it may already have been released before it was
/// withdrawn, so restoring it at a version its group does not share has to fail
/// the group rather than be waved through as a brand new member.
/// Ref: docs/design.md, "Version groups".
#[cfg_attr(miri, ignore)] // Spawns git and cargo, which Miri cannot emulate.
#[test]
fn group_member_withdrawn_on_the_base_is_still_held_to_the_group() {
    let fixture = Fixture::new("");
    write_package(
        &fixture,
        "alpha",
        "0.1.0",
        "\n[dependencies]\nbeta = { path = \"../beta\", version = \"=0.1.0\" }\n",
    );
    write_package(&fixture, "beta", "0.1.0", "");
    fixture.commit("release both members");
    write_package(&fixture, "beta", "0.1.0", "\npublish = false");
    fixture.commit("withdraw beta");
    let base = fixture.sha("HEAD");
    write_package(&fixture, "beta", "0.2.0", "");
    write_package(
        &fixture,
        "alpha",
        "0.1.0",
        "\n[dependencies]\nbeta = { path = \"../beta\", version = \"=0.2.0\" }\n",
    );
    fixture.commit("restore beta at a version the group does not share");

    let (passed, message) = check(&fixture, &base);
    assert!(!passed, "{message}");
    assert!(message.contains("beta"), "{message}");
}

/// A non-publishable member participates in grouping without release assessment.
#[cfg_attr(miri, ignore)] // Spawns git and cargo, which Miri cannot emulate.
#[test]
fn a_non_publishable_group_member_is_a_version_target() {
    let fixture = Fixture::new("");
    write_package(
        &fixture,
        "alpha",
        "0.1.0",
        "\n[dependencies]\nbeta = { path = \"../beta\", version = \"=0.1.0\" }\n",
    );
    write_package(
        &fixture,
        "beta",
        "0.1.0",
        "\npublish = false\n\n[dependencies]\nalpha = { path = \"../alpha\", version = \"=0.1.0\" }\n",
    );
    fixture.commit("group with a package that is never published");
    let base = fixture.sha("HEAD");

    let outcome = run(&RunInput::Check {
        base: Some(base),
        manifest_path: fixture.manifest(),
        format: CheckFormat::Text,
        verify_packaging: false,
        verbose: false,
    })
    .unwrap();
    match outcome {
        RunOutcome::Check {
            passed, message, ..
        } => assert!(passed, "{message}"),
        other => panic!("expected check, got {other:?}"),
    }
}

#[cfg_attr(miri, ignore)] // Spawns git and cargo, which Miri cannot emulate.
#[test]
fn inconsistent_group_fails_check_even_when_content_is_unchanged() {
    let fixture = Fixture::new("");
    write_package(
        &fixture,
        "alpha",
        "0.1.0",
        "\n[dependencies]\nbeta = { path = \"../beta\", version = \"=0.2.0\" }\n",
    );
    write_package(&fixture, "beta", "0.2.0", "");
    fixture.commit("mismatched group versions");
    let base = fixture.sha("HEAD");

    let (passed, message) = check(&fixture, &base);
    assert!(!passed, "{message}");
    assert!(message.contains("inconsistent") || message.contains("different versions"));
    assert!(message.contains("increment-versions"));

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
        }
        other => panic!("expected check, got {other:?}"),
    }
}

/// Every dependency form used by Cargo contributes the same host-independent edge.
#[cfg_attr(miri, ignore)] // Spawns git and cargo, which Miri cannot emulate.
#[test]
fn dependency_kinds_aliases_inheritance_and_inactive_targets_derive_one_group() {
    let fixture = Fixture::new(
        r#"
        [workspace.dependencies]
        pub_c_alias = { package = "pub_c", path = "packages/pub_c", version = "=0.1.0" }
        unused = { package = "outside", path = "packages/outside", version = "=0.1.0" }
        "#,
    );
    write_package(
        &fixture,
        "bridge",
        "0.1.0",
        "\npublish = false\n\n[dependencies]\npub_a = { path = \"../pub_a\", version = \"=0.1.0\", optional = true }\n",
    );
    write_package(
        &fixture,
        "pub_a",
        "0.1.0",
        "\n[target.'cfg(target_os = \"none\")'.dependencies]\npub_b_alias = { package = \"pub_b\", path = \"../pub_b\", version = \" = 0.1.0 \", optional = true }\n",
    );
    write_package(
        &fixture,
        "pub_b",
        "0.1.0",
        "\n[build-dependencies]\npub_c_alias.workspace = true\n",
    );
    write_package(
        &fixture,
        "pub_c",
        "0.1.0",
        "\n[dev-dependencies]\npub_d = { path = \"../pub_d\", version = \"=0.1.0\" }\noutside = { path = \"../outside\" }\n",
    );
    write_package(&fixture, "pub_d", "0.1.0", "");
    write_package(&fixture, "outside", "0.1.0", "");
    fixture.commit("dependency forms");
    let base = fixture.sha("HEAD");

    let report: Value = serde_json::from_str(&report_json(&fixture, &base)).unwrap();

    assert_eq!(
        report.pointer("/groups/bridge/members"),
        Some(&json!(["bridge", "pub_a", "pub_b", "pub_c", "pub_d"]))
    );
    assert!(report.pointer("/groups/outside").is_none());
    assert_eq!(
        report.pointer("/non_publishable_packages/0"),
        Some(&json!({
            "name": "bridge",
            "declared_version": "0.1.0",
            "group": "bridge"
        }))
    );
}

/// An exact declaration from a helper is checked without becoming a release assessment.
#[cfg_attr(miri, ignore)] // Spawns git and cargo, which Miri cannot emulate.
#[test]
fn stale_exact_requirement_from_a_non_publishable_source_is_reported() {
    let fixture = Fixture::new("");
    write_package(&fixture, "library", "0.2.0", "");
    write_package(
        &fixture,
        "helper",
        "0.1.0",
        "\npublish = false\n\n[dependencies]\nlibrary = { path = \"../library\", version = \"=0.1.0\" }\n",
    );
    fixture.commit("stale helper pin");
    let base = fixture.sha("HEAD");

    let (passed, message) = check(&fixture, &base);

    assert!(!passed, "{message}");
    assert!(message.contains("helper"), "{message}");
    assert!(message.contains("=0.2.0"), "{message}");
    let report: Value = serde_json::from_str(&report_json(&fixture, &base)).unwrap();
    assert_eq!(
        report.pointer("/groups/helper/members"),
        Some(&json!(["helper", "library"]))
    );
    assert_eq!(
        report
            .get("packages")
            .and_then(Value::as_array)
            .unwrap()
            .iter()
            .filter(|package| package.get("name") == Some(&json!("helper")))
            .count(),
        0
    );

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
            assert!(
                message.contains("title=stale-workspace-requirement::"),
                "{message}"
            );
        }
        other => panic!("expected check, got {other:?}"),
    }
}

/// Invalid exact syntax is rejected by the loader shared by every command.
#[cfg_attr(miri, ignore)] // Spawns git and cargo, which Miri cannot emulate.
#[test]
fn malformed_exact_requirements_fail_all_commands_before_writes() {
    let fixture = Fixture::new("");
    write_package(&fixture, "library", "0.1.0", "");
    write_package(
        &fixture,
        "helper",
        "0.1.0",
        "\npublish = false\n\n[dependencies]\nlibrary = { path = \"../library\", version = \"=0.1\" }\n\n[dev-dependencies]\nlibrary = { path = \"../library\", version = \"=0.1.0\" }\n",
    );
    fixture.commit("malformed exact requirement");
    let base = fixture.sha("HEAD");
    let plan = fixture.path().join("plan.json");
    fs::write(
        &plan,
        r#"{ "schema_version": 3, "increments": [{ "name": "library", "level": "patch" }] }"#,
    )
    .unwrap();

    let commands = [
        RunInput::Check {
            base: Some(base.clone()),
            manifest_path: fixture.manifest(),
            format: CheckFormat::Text,
            verify_packaging: false,
            verbose: false,
        },
        RunInput::Report {
            out_dir: fixture.path().join("report"),
            base: Some(base),
            manifest_path: fixture.manifest(),
            verbose: false,
        },
        RunInput::Expand {
            plan: plan.clone(),
            out: fixture.path().join("expanded.json"),
            manifest_path: fixture.manifest(),
            verbose: false,
        },
        RunInput::Apply {
            plan,
            dry_run: false,
            manifest_path: fixture.manifest(),
            verbose: false,
        },
    ];

    for command in commands {
        let error = run(&command).expect_err("malformed exact syntax must fail shared loading");
        assert!(error.to_string().contains("=major.minor.patch"), "{error}");
    }
    assert!(
        fixture
            .read("packages/library/Cargo.toml")
            .contains("version = \"0.1.0\"")
    );
}

/// Current legacy metadata is rejected regardless of its value shape.
#[cfg_attr(miri, ignore)] // Spawns git and cargo, which Miri cannot emulate.
#[test]
fn current_legacy_group_metadata_is_always_rejected() {
    for metadata in [
        "\n[workspace.metadata.release-plan.groups]\n",
        "\n[workspace.metadata.release-plan]\ngroups = []\n",
    ] {
        let fixture = Fixture::new(metadata);
        write_package(&fixture, "demo", "0.1.0", "");
        fixture.commit("legacy metadata");
        let base = fixture.sha("HEAD");

        let error = run(&RunInput::Check {
            base: Some(base),
            manifest_path: fixture.manifest(),
            format: CheckFormat::Text,
            verify_packaging: false,
            verbose: false,
        })
        .expect_err("legacy metadata must not be silently ignored");
        assert!(error.to_string().contains("obsolete"), "{error}");
    }
}

/// Historical snapshots may retain metadata that the current workspace rejects.
#[cfg_attr(miri, ignore)] // Spawns git and cargo, which Miri cannot emulate.
#[test]
fn historical_legacy_group_metadata_remains_readable() {
    let fixture = Fixture::new("\n[workspace.metadata.release-plan.groups]\ng = [\"demo\"]\n");
    write_package(&fixture, "demo", "0.1.0", "");
    fixture.commit("historical metadata");
    let base = fixture.sha("HEAD");
    fixture.write_workspace("");
    fixture.commit("current derived groups");

    let (passed, message) = check(&fixture, &base);

    assert!(passed, "{message}");
}

/// A registry dependency never matches a same-named workspace member.
#[cfg_attr(miri, ignore)] // Spawns git and cargo, which Miri cannot emulate.
#[test]
fn registry_exact_requirement_is_outside_group_validation_and_identity() {
    let fixture = Fixture::new("");
    write_package(&fixture, "shared", "0.1.0", "");
    write_package(
        &fixture,
        "consumer",
        "0.1.0",
        "\n[dependencies]\nshared = \"=0.1\"\n",
    );
    fixture.commit("same-named registry dependency");
    let base = fixture.sha("HEAD");

    let report: Value = serde_json::from_str(&report_json(&fixture, &base)).unwrap();

    assert_eq!(report.get("groups"), Some(&json!({})));
}

/// An exact path dependency outside the workspace does not create or validate a group edge.
#[cfg_attr(miri, ignore)] // Spawns git and cargo, which Miri cannot emulate.
#[test]
fn outside_workspace_exact_requirement_is_not_a_group_edge() {
    let fixture = Fixture::new("exclude = [\"vendor/outside\"]");
    write_package(
        &fixture,
        "consumer",
        "0.1.0",
        "\n[dependencies]\noutside = { path = \"../../vendor/outside\", version = \"=0.1\" }\n",
    );
    fixture.write(
        "vendor/outside/Cargo.toml",
        r#"[package]
name = "outside"
version = "0.1.0"
edition = "2021"
"#,
    );
    fixture.write("vendor/outside/src/lib.rs", "");
    fixture.commit("outside-workspace exact dependency");
    let base = fixture.sha("HEAD");

    let report: Value = serde_json::from_str(&report_json(&fixture, &base)).unwrap();

    assert_eq!(report.get("groups"), Some(&json!({})));
}
