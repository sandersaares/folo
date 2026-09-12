use serde_json::{Value, json};

use crate::propose::tests::{assert_versions, depends, generate, needs, package, report};
use crate::report::{ReportFile, ReportPackage};

fn assert_scenario(report: &ReportFile, changes: &[(&str, &str)], expected: &Value) {
    let plan = generate(report, changes).unwrap();
    // Safety alone permits unnecessary extra increments. The exact outcome also pins
    // retention of adequate pending versions and minimal group alignment.
    assert_versions(report, &plan, expected);
}

fn drifted_packages() -> Vec<ReportPackage> {
    vec![
        package("nm", "1.1.0", Some("1.1.0")),
        package("nm_impl", "1.0.0", Some("1.0.0")),
    ]
}

#[test]
fn invariant_drifted_group_without_other_changes() {
    let report = report(drifted_packages(), vec![], &[&["nm", "nm_impl"]]);
    assert_scenario(&report, &[], &json!({"nm": "1.1.0", "nm_impl": "1.1.0"}));
}

#[test]
fn invariant_leader_pins_lagging_group_member() {
    let report = report(
        vec![
            depends(package("nm", "1.1.0", Some("1.1.0")), "nm_impl", false),
            package("nm_impl", "1.0.0", Some("1.0.0")),
        ],
        vec![],
        &[&["nm", "nm_impl"]],
    );
    assert_scenario(&report, &[], &json!({"nm": "1.1.1", "nm_impl": "1.1.1"}));
}

#[test]
fn invariant_published_outside_dependent_requires_a_decision() {
    let mut packages = drifted_packages();
    packages.push(depends(
        package("events", "2.0.0", Some("2.0.0")),
        "nm_impl",
        false,
    ));
    let report = report(packages, vec![], &[&["nm", "nm_impl"]]);
    _ = generate(&report, &[]).unwrap_err();
}

#[test]
fn invariant_pending_outside_dependent_keeps_its_version() {
    let mut packages = drifted_packages();
    packages.push(depends(
        package("events", "2.1.0", Some("2.0.0")),
        "nm_impl",
        false,
    ));
    let report = report(packages, vec![], &[&["nm", "nm_impl"]]);
    assert_scenario(
        &report,
        &[],
        &json!({"nm": "1.1.0", "nm_impl": "1.1.0", "events": "2.1.0"}),
    );
}

#[test]
fn invariant_unpublished_outside_dependent_keeps_its_version() {
    let mut packages = drifted_packages();
    packages.push(depends(package("events", "0.1.0", None), "nm_impl", false));
    let report = report(packages, vec![], &[&["nm", "nm_impl"]]);
    assert_scenario(
        &report,
        &[],
        &json!({"nm": "1.1.0", "nm_impl": "1.1.0", "events": "0.1.0"}),
    );
}

#[test]
fn invariant_outside_dependent_receives_its_own_decision() {
    let mut packages = drifted_packages();
    packages.push(needs(depends(
        package("events", "2.0.0", Some("2.0.0")),
        "nm_impl",
        false,
    )));
    let report = report(packages, vec![], &[&["nm", "nm_impl"]]);
    assert_scenario(
        &report,
        &[("events", "patch")],
        &json!({"nm": "1.1.0", "nm_impl": "1.1.0", "events": "2.0.1"}),
    );
}

#[test]
fn invariant_covered_group_decision_still_aligns() {
    let report = report(
        vec![
            package("nm", "1.1.0", Some("1.0.0")),
            package("nm_impl", "1.0.0", Some("1.0.0")),
        ],
        vec![],
        &[&["nm", "nm_impl"]],
    );
    assert_scenario(
        &report,
        &[("nm", "patch")],
        &json!({"nm": "1.1.0", "nm_impl": "1.1.0"}),
    );
}

#[test]
fn invariant_one_drifted_group_pins_another() {
    let mut packages = drifted_packages();
    packages.push(depends(
        package("events", "3.0.0", Some("3.0.0")),
        "nm_impl",
        false,
    ));
    packages.push(package("events_impl", "2.0.0", Some("2.0.0")));
    let report = report(
        packages,
        vec![],
        &[&["nm", "nm_impl"], &["events", "events_impl"]],
    );
    assert_scenario(
        &report,
        &[],
        &json!({
            "nm": "1.1.0", "nm_impl": "1.1.0",
            "events": "3.0.1", "events_impl": "3.0.1"
        }),
    );
}

#[test]
fn invariant_consistent_group_follows_one_decided_member() {
    let report = report(
        vec![
            needs(package("nm", "1.0.0", Some("1.0.0"))),
            package("nm_impl", "1.0.0", Some("1.0.0")),
        ],
        vec![],
        &[&["nm", "nm_impl"]],
    );
    assert_scenario(
        &report,
        &[("nm", "breaking")],
        &json!({"nm": "2.0.0", "nm_impl": "2.0.0"}),
    );
}

#[test]
fn invariant_non_key_member_decision_reaches_dependent_group() {
    let report = report(
        vec![
            package("nm", "1.0.0", Some("1.0.0")),
            needs(package("nm_impl", "1.0.0", Some("1.0.0"))),
            depends(package("other", "3.0.0", Some("3.0.0")), "nm", false),
            package("other_impl", "2.0.0", Some("2.0.0")),
        ],
        vec![],
        &[&["nm", "nm_impl"], &["other", "other_impl"]],
    );
    assert_scenario(
        &report,
        &[("nm_impl", "patch")],
        &json!({
            "nm": "1.0.1", "nm_impl": "1.0.1",
            "other": "3.0.1", "other_impl": "3.0.1"
        }),
    );
}

#[test]
fn invariant_changed_package_without_a_decision_is_rejected() {
    let report = report(
        vec![needs(package("nm", "1.0.0", Some("1.0.0")))],
        vec![],
        &[],
    );
    _ = generate(&report, &[]).unwrap_err();
}

#[test]
fn invariant_sibling_decision_covers_both_changed_packages() {
    let report = report(
        vec![
            needs(package("nm", "1.0.0", Some("1.0.0"))),
            needs(package("nm_impl", "1.0.0", Some("1.0.0"))),
        ],
        vec![],
        &[&["nm", "nm_impl"]],
    );
    assert_scenario(
        &report,
        &[("nm", "patch")],
        &json!({"nm": "1.0.1", "nm_impl": "1.0.1"}),
    );
}

#[test]
fn invariant_pending_ungrouped_dependent_keeps_its_version() {
    let report = report(
        vec![
            needs(package("nm", "1.0.0", Some("1.0.0"))),
            depends(package("events", "2.1.0", Some("2.0.0")), "nm", false),
        ],
        vec![],
        &[],
    );
    assert_scenario(
        &report,
        &[("nm", "patch")],
        &json!({"nm": "1.0.1", "events": "2.1.0"}),
    );
}
