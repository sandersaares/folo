//! Resolving a plan's version groups into an explicit per-package plan.

use std::fs;

use cargo_release_plan::{RunInput, RunOutcome, run};
use serde_json::Value;

use crate::fixture::{Fixture, write_package};
use crate::harness::check;

/// Expansion names the packages whose versions move, not every manifest apply edits.
///
/// A dependent's requirement is rewritten whenever its target moves, because every
/// intra-workspace requirement names the version its target declares. That rewrite does not give
/// the dependent a version of its own, so expansion does not name it: the document records
/// version decisions, and inventing an entry for a package the plan does not move would claim a
/// release it is not making.
///
/// What keeps that safe is a separate rule. A dependent that would keep an already-published
/// version while its manifest is rewritten needs a change level of its own, which the plan
/// generator refuses to omit, and `check` rejects the result afterwards if one is ever missed.
/// Deciding that here is not possible: `expand` reads the work tree without classification, so it
/// has no anchors and cannot tell a published dependent from a pending or unpublished one.
/// Ref: docs/design.md, "Planning stages".
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
        r#"{ "schema_version": 2, "increments": [{ "name": "helper", "level": "patch" }] }"#,
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
        plan: expanded_path,
        manifest_path: fixture.manifest(),
        dry_run: false,
        verbose: false,
    })
    .unwrap();

    // `app` was not named, yet its requirement followed `helper` so it keeps naming the version
    // `helper` declares.
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
[workspace.metadata.release-plan.groups]
g = ["shell", "shell_impl"]

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
        r#"{ "schema_version": 2, "increments": [
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
        plan: expanded_path,
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

/// A group that gained a member after expansion is rejected at apply time.
///
/// The expanded document is what gets presented and what the publication check
/// ran over, so `apply` must not quietly reach a package it does not name.
/// Expansion resolves entries through the group configuration as it stands at
/// apply time, which is where a membership change between the two commands would
/// otherwise widen the recorded set.
/// Ref: docs/design.md, "Version groups".
#[cfg_attr(miri, ignore)] // Spawns git and cargo, which Miri cannot emulate.
#[test]
fn apply_rejects_an_expanded_plan_whose_group_gained_a_member() {
    let fixture = Fixture::new(
        r#"
[workspace.metadata.release-plan.groups]
release-family = ["shell"]
"#,
    );
    write_package(&fixture, "shell", "0.1.0", "");
    write_package(&fixture, "shell_impl", "0.1.0", "");
    fixture.commit("grouped packages");

    let plan_path = fixture.path().join("plan.json");
    fs::write(
        &plan_path,
        r#"{ "schema_version": 2, "increments": [{ "name": "shell", "level": "patch" }] }"#,
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

    // The reviewed document names only the member the group held when it was
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

    // The group gains a member between expansion and application.
    let manifest = fs::read_to_string(fixture.manifest()).unwrap();
    fs::write(
        fixture.manifest(),
        manifest.replace(
            r#"release-family = ["shell"]"#,
            r#"release-family = ["shell", "shell_impl"]"#,
        ),
    )
    .unwrap();

    let error = run(&RunInput::Apply {
        plan: expanded_path,
        dry_run: false,
        manifest_path: fixture.manifest(),
        verbose: false,
    })
    .expect_err("an expanded plan cannot widen to a newly added group member");
    assert!(error.to_string().contains("shell_impl"), "{error}");

    // Nothing was written: the rejection precedes every manifest edit.
    let shell = fs::read_to_string(fixture.path().join("packages/shell/Cargo.toml")).unwrap();
    assert!(shell.contains("version = \"0.1.0\""), "{shell}");
}

/// A group whose members disagree on an explicit version is rejected.
///
/// Expansion is the only place a planner resolves a group, so a hand-edited
/// expanded plan that breaks group uniformity must not reach manifests.
#[cfg_attr(miri, ignore)] // Spawns git and cargo, which Miri cannot emulate.
#[test]
fn expand_rejects_disagreeing_versions_within_one_group() {
    let fixture = Fixture::new(
        r#"
[workspace.metadata.release-plan.groups]
release-family = ["shell", "shell_impl"]
"#,
    );
    write_package(&fixture, "shell", "0.1.0", "");
    write_package(&fixture, "shell_impl", "0.1.0", "");
    fixture.commit("grouped packages");

    let plan_path = fixture.path().join("plan.json");
    fs::write(
        &plan_path,
        r#"{ "schema_version": 2, "increments": [
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
    assert!(error.to_string().contains("release-family"), "{error}");
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
    let fixture = Fixture::new(
        r#"
[workspace.metadata.release-plan.groups]
g = ["shell", "shell_impl"]
"#,
    );
    write_package(&fixture, "shell", "0.1.0", "");
    write_package(&fixture, "shell_impl", "0.2.0", "");
    fixture.commit("drifted group");
    let base = fixture.sha("HEAD");

    let (passed, message) = check(&fixture, &base);
    assert!(!passed, "a drifted group must fail the check: {message}");

    let plan_path = fixture.path().join("plan.json");
    fs::write(
        &plan_path,
        r#"{ "schema_version": 2, "increments": [{ "name": "shell", "level": "patch" }] }"#,
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
        plan: expanded_path,
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
    let fixture = Fixture::new(
        r#"
[workspace.metadata.release-plan.groups]
g = ["shell", "shell_impl"]
"#,
    );
    write_package(&fixture, "shell", "1.0.0", "");
    write_package(&fixture, "shell_impl", "1.1.0", "");
    fixture.commit("drifted group");
    let base = fixture.sha("HEAD");

    let plan_path = fixture.path().join("plan.json");
    fs::write(
        &plan_path,
        r#"{ "schema_version": 2, "increments": [{ "name": "g", "version": "1.1.0" }] }"#,
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
        plan: expanded_path,
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
