//! Resolving a plan's version groups into an explicit per-package plan.

use std::fs;

use cargo_release_plan::{RunInput, RunOutcome, run};
use serde_json::Value;

use crate::fixture::{Fixture, write_package};
use crate::harness::{check, resolved_plan};

/// Read-only expansion names group effects; preview completes dependency effects before apply.
#[cfg_attr(miri, ignore)] // Spawns git and cargo, which Miri cannot emulate.
#[test]
fn expand_names_moved_packages_while_apply_also_rewrites_their_dependents() {
    let fixture = Fixture::new("");
    write_package(&fixture, "helper", "1.0.0", "");
    write_package(
        &fixture,
        "app",
        "0.1.0",
        r#"
[dependencies]
helper = { path = "../helper", version = "1.0.0" }
"#,
    );
    fixture.commit("seed");

    let plan_path = fixture.path().join("plan.json");
    fs::write(
        &plan_path,
        r#"{ "schema_version": 4, "increments": [{ "name": "helper", "level": "patch" }] }"#,
    )
    .unwrap();
    let expanded_path = fixture.path().join("expanded.json");

    run(&RunInput::Expand {
        plan: plan_path,
        out: expanded_path.clone(),
        manifest_path: fixture.manifest(),
        verbose: false,
    })
    .unwrap();

    // Only the package whose version moves is named.
    let expanded: Value =
        serde_json::from_str(&fs::read_to_string(&expanded_path).unwrap()).unwrap();
    let named: Vec<&str> = expanded
        .get("increments")
        .and_then(Value::as_array)
        .unwrap()
        .iter()
        .map(|entry| entry.get("name").and_then(Value::as_str).unwrap())
        .collect();
    assert_eq!(named, vec!["helper"]);

    run(&RunInput::Apply {
        plan: resolved_plan(&fixture, &expanded_path),
        manifest_path: fixture.manifest(),
        dry_run: false,
        verbose: false,
    })
    .unwrap();

    // Preview includes the dependent's requirement rewrite and its corresponding release.
    let manifest = fixture.read("packages/app/Cargo.toml");
    assert!(manifest.contains("version = \"1.0.1\""), "{manifest}");
}

/// Expansion names every group member and produces an applicable result.
///
/// An expanded plan and the apply operation that consumes it cannot disagree
/// about which packages a decision reaches.
/// Ref: docs/design.md, "Version groups".
#[cfg_attr(miri, ignore)] // Spawns git and cargo, which Miri cannot emulate.
#[test]
fn expand_names_every_group_member_and_the_result_applies() {
    let fixture = Fixture::new(
        r#"
[workspace.dependencies]
shell_impl = { version = "=0.1.0", path = "packages/shell_impl" }
"#,
    );
    write_package(
        &fixture,
        "shell",
        "0.1.0",
        "
[dependencies]
shell_impl = { workspace = true }
",
    );
    write_package(&fixture, "shell_impl", "0.1.0", "");
    write_package(&fixture, "loner", "1.2.3", "");
    fixture.commit("grouped packages");
    let base = fixture.sha("HEAD");

    let plan_path = fixture.path().join("plan.json");
    fs::write(
        &plan_path,
        r#"{ "schema_version": 4, "increments": [
            { "name": "shell", "level": "patch" },
            { "name": "loner", "level": "minor" }
        ] }"#,
    )
    .unwrap();
    let expanded_path = fixture.path().join("expanded/plan.json");

    let outcome = run(&RunInput::Expand {
        plan: plan_path,
        out: expanded_path.clone(),
        manifest_path: fixture.manifest(),
        verbose: true,
    })
    .unwrap();
    assert!(matches!(outcome, RunOutcome::Expand { .. }));

    let expanded: Value =
        serde_json::from_str(&fs::read_to_string(&expanded_path).unwrap()).unwrap();
    let increments = expanded
        .get("increments")
        .and_then(Value::as_array)
        .unwrap();
    let versions = expanded_versions(increments);
    // `shell_impl` was never named, but shares a group with `shell`.
    assert_eq!(
        versions,
        vec![
            ("loner", "1.3.0"),
            ("shell", "0.1.1"),
            ("shell_impl", "0.1.1"),
        ]
    );
    // Levels are already resolved, so nothing is left to decide at apply time.
    assert!(increments.iter().all(|entry| entry.get("level").is_none()));

    run(&RunInput::Apply {
        plan: resolved_plan(&fixture, &expanded_path),
        dry_run: false,
        manifest_path: fixture.manifest(),
        verbose: false,
    })
    .unwrap();

    let impl_manifest =
        fs::read_to_string(fixture.path().join("packages/shell_impl/Cargo.toml")).unwrap();
    assert!(impl_manifest.contains("version = \"0.1.1\""));
    let root = fs::read_to_string(fixture.manifest()).unwrap();
    assert!(root.contains("version = \"=0.1.1\""));

    fixture.commit("apply expanded plan");
    let (passed, message) = check(&fixture, &base);
    assert!(passed, "{message}");
}

/// A helper can directly target and align a group that publishes no package.
#[cfg_attr(miri, ignore)] // Spawns git and cargo, which Miri cannot emulate.
#[test]
fn a_helper_directly_targets_an_all_non_publishable_group() {
    let fixture = Fixture::new("");
    write_package(&fixture, "z-helper", "0.1.0", "\npublish = false\n");
    write_package(
        &fixture,
        "a-helper",
        "0.1.0",
        "\npublish = false\n\n[dependencies]\nz-helper = { path = \"../z-helper\", version = \"=0.1.0\" }\n",
    );
    fixture.commit("helper group");
    let plan_path = fixture.path().join("plan.json");
    fs::write(
        &plan_path,
        r#"{ "schema_version": 4, "increments": [{ "name": "z-helper", "level": "patch" }] }"#,
    )
    .unwrap();
    let expanded_path = fixture.path().join("expanded.json");

    run(&RunInput::Expand {
        plan: plan_path,
        out: expanded_path.clone(),
        manifest_path: fixture.manifest(),
        verbose: false,
    })
    .unwrap();

    let expanded: Value =
        serde_json::from_str(&fs::read_to_string(&expanded_path).unwrap()).unwrap();
    assert_eq!(
        expanded_versions(
            expanded
                .get("increments")
                .and_then(Value::as_array)
                .unwrap()
        ),
        vec![("a-helper", "0.1.1"), ("z-helper", "0.1.1")]
    );

    run(&RunInput::Apply {
        plan: resolved_plan(&fixture, &expanded_path),
        dry_run: false,
        manifest_path: fixture.manifest(),
        verbose: false,
    })
    .unwrap();

    for helper in ["a-helper", "z-helper"] {
        let manifest = fixture.read(&format!("packages/{helper}/Cargo.toml"));
        assert!(manifest.contains("version = \"0.1.1\""), "{manifest}");
    }
}

/// A group that gains an earlier-sorting helper after resolution is rejected.
///
/// The expanded document is what resolution captured and what the publication
/// check ran over, so `apply` must not quietly reach a package it does not name.
/// Expansion resolves entries through the current exact dependency graph, which
/// is where a membership change between the two commands would otherwise widen
/// the captured set and change its derived key.
/// Ref: docs/design.md, "Version groups".
#[cfg_attr(miri, ignore)] // Spawns git and cargo, which Miri cannot emulate.
#[test]
fn apply_rejects_an_expanded_plan_whose_group_gained_a_member() {
    let fixture = Fixture::new("");
    write_package(&fixture, "shell", "0.1.0", "");
    write_package(&fixture, "aaa-helper", "0.1.0", "\npublish = false\n");
    fixture.commit("grouped packages");

    let plan_path = fixture.path().join("plan.json");
    fs::write(
        &plan_path,
        r#"{ "schema_version": 4, "increments": [{ "name": "shell", "level": "patch" }] }"#,
    )
    .unwrap();
    let expanded_path = fixture.path().join("expanded.json");
    run(&RunInput::Expand {
        plan: plan_path.clone(),
        out: expanded_path.clone(),
        manifest_path: fixture.manifest(),
        verbose: false,
    })
    .unwrap();

    // The expanded document names only the member the group held when it was
    // written.
    let expanded: Value =
        serde_json::from_str(&fs::read_to_string(&expanded_path).unwrap()).unwrap();
    assert_eq!(expanded.get("expanded"), Some(&Value::Bool(true)));
    let names: Vec<&str> = expanded
        .get("increments")
        .and_then(Value::as_array)
        .unwrap()
        .iter()
        .map(|entry| entry.get("name").and_then(Value::as_str).unwrap())
        .collect();
    assert_eq!(names, vec!["shell"]);
    let resolved = resolved_plan(&fixture, &expanded_path);
    run(&RunInput::Apply {
        plan: resolved.clone(),
        dry_run: true,
        manifest_path: fixture.manifest(),
        verbose: false,
    })
    .unwrap();
    let original_helper = fixture.read("packages/aaa-helper/Cargo.toml");

    // An exact dependency connects an earlier-sorting helper between resolution and application.
    fixture.write(
        "packages/aaa-helper/Cargo.toml",
        r#"[package]
name = "aaa-helper"
version = "0.1.0"
edition = "2021"
publish = false

[dependencies]
shell = { path = "../shell", version = "=0.1.0" }
"#,
    );

    let shell = fixture.read("packages/shell/Cargo.toml");
    let helper = fixture.read("packages/aaa-helper/Cargo.toml");
    let lockfile = fixture.read("Cargo.lock");
    run(&RunInput::Apply {
        plan: resolved.clone(),
        dry_run: false,
        manifest_path: fixture.manifest(),
        verbose: false,
    })
    .unwrap_err();

    // Nothing was written: the rejection precedes every manifest edit.
    assert_eq!(fixture.read("packages/shell/Cargo.toml"), shell);
    assert_eq!(fixture.read("packages/aaa-helper/Cargo.toml"), helper);
    assert_eq!(fixture.read("Cargo.lock"), lockfile);

    // Restoring precisely the captured inputs restores acceptance of the same plan.
    fixture.write("packages/aaa-helper/Cargo.toml", &original_helper);
    run(&RunInput::Apply {
        plan: resolved,
        dry_run: true,
        manifest_path: fixture.manifest(),
        verbose: false,
    })
    .unwrap();
    fixture.write("packages/aaa-helper/Cargo.toml", &helper);

    // The changed graph is valid; only fresh resolution may include its additional member.
    let fresh = resolved_plan(&fixture, &plan_path);
    let expanded: Value = serde_json::from_slice(&fs::read(&fresh).unwrap()).unwrap();
    let names: Vec<_> = expanded
        .get("increments")
        .unwrap()
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| entry.get("name").unwrap().as_str().unwrap())
        .collect();
    assert_eq!(names, ["aaa-helper", "shell"]);
    run(&RunInput::Apply {
        plan: fresh,
        dry_run: false,
        manifest_path: fixture.manifest(),
        verbose: false,
    })
    .unwrap();
    for member in ["shell", "aaa-helper"] {
        assert!(
            fixture
                .read(&format!("packages/{member}/Cargo.toml"))
                .contains("0.1.1")
        );
    }
}

/// A group whose members disagree on an explicit version is rejected.
///
/// Expansion is the only place a planner resolves a group, so a hand-edited
/// expanded plan that breaks group uniformity must not reach manifests.
#[cfg_attr(miri, ignore)] // Spawns git and cargo, which Miri cannot emulate.
#[test]
fn expand_rejects_disagreeing_versions_within_one_group() {
    let fixture = Fixture::new("");
    write_package(
        &fixture,
        "shell",
        "0.1.0",
        "\n[dependencies]\nshell_impl = { path = \"../shell_impl\", version = \"=0.1.0\" }\n",
    );
    write_package(&fixture, "shell_impl", "0.1.0", "");
    fixture.commit("grouped packages");

    let plan_path = fixture.path().join("plan.json");
    fs::write(
        &plan_path,
        r#"{ "schema_version": 4, "increments": [
            { "name": "shell", "version": "0.2.0" },
            { "name": "shell_impl", "version": "0.3.0" }
        ] }"#,
    )
    .unwrap();

    let error = run(&RunInput::Expand {
        plan: plan_path,
        out: fixture.path().join("expanded.json"),
        manifest_path: fixture.manifest(),
        verbose: false,
    })
    .expect_err("members of one group cannot take different versions");
    assert!(error.to_string().contains("shell"), "{error}");
}

/// A patch increment level restores a version group whose members drifted apart.
///
/// `check` fails on an inconsistent group even when no released content
/// changed, so that failure must be recoverable through the same
/// increment-level decision recorded in the plan for every other
/// case. Expansion lifts every member to the highest declared version raised by
/// the decided level, which is what returns the group to one version.
/// Ref: docs/design.md, "Version groups".
#[cfg_attr(miri, ignore)] // Spawns git and cargo, which Miri cannot emulate.
#[test]
fn a_patch_increment_level_realigns_an_inconsistent_group() {
    let fixture = Fixture::new("");
    write_package(
        &fixture,
        "shell",
        "0.1.0",
        "\n[dependencies]\nshell_impl = { path = \"../shell_impl\", version = \"=0.2.0\" }\n",
    );
    write_package(&fixture, "shell_impl", "0.2.0", "");
    fixture.commit("drifted group");
    let base = fixture.sha("HEAD");

    let (passed, message) = check(&fixture, &base);
    assert!(!passed, "a drifted group must fail the check: {message}");

    let plan_path = fixture.path().join("plan.json");
    fs::write(
        &plan_path,
        r#"{ "schema_version": 4, "increments": [{ "name": "shell", "level": "patch" }] }"#,
    )
    .unwrap();
    let expanded_path = fixture.path().join("expanded.json");
    run(&RunInput::Expand {
        plan: plan_path,
        out: expanded_path.clone(),
        manifest_path: fixture.manifest(),
        verbose: false,
    })
    .unwrap();

    let expanded: Value =
        serde_json::from_str(&fs::read_to_string(&expanded_path).unwrap()).unwrap();
    let increments = expanded
        .get("increments")
        .and_then(Value::as_array)
        .unwrap();
    assert_eq!(
        expanded_versions(increments),
        vec![("shell", "0.2.1"), ("shell_impl", "0.2.1")]
    );

    run(&RunInput::Apply {
        plan: resolved_plan(&fixture, &expanded_path),
        dry_run: false,
        manifest_path: fixture.manifest(),
        verbose: false,
    })
    .unwrap();
    fixture.commit("realign group");

    let (passed, message) = check(&fixture, &base);
    assert!(passed, "{message}");
}

/// An exact target equal to the group's highest version aligns lagging members.
///
/// Naming the highest declared version moves lagging members up to it and leaves
/// the leading member unchanged. The lagging members become pending release
/// because their declared versions advanced.
/// Ref: docs/design.md, "Version groups".
#[cfg_attr(miri, ignore)] // Spawns git and cargo, which Miri cannot emulate.
#[test]
fn an_exact_target_aligns_a_group_without_advancing_its_leader() {
    let fixture = Fixture::new("");
    write_package(
        &fixture,
        "shell",
        "1.0.0",
        "\n[dependencies]\nshell_impl = { path = \"../shell_impl\", version = \"=1.1.0\" }\n",
    );
    write_package(&fixture, "shell_impl", "1.1.0", "");
    fixture.commit("drifted group");
    let base = fixture.sha("HEAD");

    let plan_path = fixture.path().join("plan.json");
    fs::write(
        &plan_path,
        r#"{ "schema_version": 4, "increments": [{ "name": "shell", "version": "1.1.0" }] }"#,
    )
    .unwrap();
    let expanded_path = fixture.path().join("expanded.json");
    run(&RunInput::Expand {
        plan: plan_path,
        out: expanded_path.clone(),
        manifest_path: fixture.manifest(),
        verbose: false,
    })
    .unwrap();

    let expanded: Value =
        serde_json::from_str(&fs::read_to_string(&expanded_path).unwrap()).unwrap();
    let increments = expanded
        .get("increments")
        .and_then(Value::as_array)
        .unwrap();
    assert_eq!(
        expanded_versions(increments),
        vec![("shell", "1.1.0"), ("shell_impl", "1.1.0")]
    );

    run(&RunInput::Apply {
        plan: resolved_plan(&fixture, &expanded_path),
        dry_run: false,
        manifest_path: fixture.manifest(),
        verbose: false,
    })
    .unwrap();

    let leader = fs::read_to_string(fixture.path().join("packages/shell_impl/Cargo.toml")).unwrap();
    assert!(leader.contains("version = \"1.1.0\""), "{leader}");
    let laggard = fs::read_to_string(fixture.path().join("packages/shell/Cargo.toml")).unwrap();
    assert!(laggard.contains("version = \"1.1.0\""), "{laggard}");

    fixture.commit("align group");
    let (passed, message) = check(&fixture, &base);
    assert!(passed, "{message}");
}

fn expanded_versions(increments: &[Value]) -> Vec<(&str, &str)> {
    increments
        .iter()
        .map(|entry| {
            let name = entry.get("name").and_then(Value::as_str).unwrap();
            let version = entry.get("version").and_then(Value::as_str).unwrap();
            (name, version)
        })
        .collect()
}
