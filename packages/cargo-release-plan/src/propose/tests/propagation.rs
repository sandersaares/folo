use serde_json::json;

use crate::propose::tests::{
    assert_invariants, assert_versions, depends, entries, generate, package, report,
};

#[test]
fn public_dependency_propagation_reaches_a_fixed_point() {
    let report = report(
        vec![
            depends(package("a_outer", "3.0.0", Some("3.0.0")), "middle", true),
            depends(package("middle", "2.0.0", Some("2.0.0")), "z_inner", true),
            package("z_inner", "1.0.0", Some("1.0.0")),
        ],
        vec![],
        &[],
    );
    let plan = generate(&report, &[("z_inner", "breaking")]).unwrap();
    assert_eq!(
        entries(&plan),
        json!([
            {"name": "a_outer", "level": "major"},
            {"name": "middle", "level": "major"},
            {"name": "z_inner", "level": "major"}
        ])
    );
    assert_invariants(&report, &plan);
}

#[test]
fn public_dependencies_raise_compatible_decisions() {
    assert_dependency_decision(true, "major");
}

#[test]
fn private_dependencies_retain_compatible_decisions() {
    assert_dependency_decision(false, "patch");
}

fn assert_dependency_decision(public: bool, expected: &str) {
    let report = report(
        vec![
            package("lib", "1.0.0", Some("1.0.0")),
            depends(package("app", "2.0.0", Some("2.0.0")), "lib", public),
        ],
        vec![],
        &[],
    );
    let plan = generate(&report, &[("lib", "breaking"), ("app", "patch")]).unwrap();
    let first = plan.increments.first().unwrap();
    assert_eq!(first.name, "app");
    assert_eq!(first.level.as_deref(), Some(expected));
    assert_invariants(&report, &plan);
}

#[test]
fn pending_dependency_breaks_propagate_without_repeating_adequate_increments() {
    for (current, count) in [("2.0.0", 1), ("3.0.0", 0)] {
        let report = report(
            vec![
                package("lib", "2.0.0", Some("1.0.0")),
                depends(package("app", current, Some("2.0.0")), "lib", true),
            ],
            vec![],
            &[],
        );
        let plan = generate(&report, &[]).unwrap();
        assert_eq!(plan.increments.len(), count);
        assert_invariants(&report, &plan);
    }
}

#[test]
fn consistent_group_sibling_decision_breaks_the_public_dependency() {
    assert_sibling_break("1.0.0", "2.0.0");
}

#[test]
fn drifted_group_sibling_decision_breaks_the_resolved_leader() {
    assert_sibling_break("2.0.0", "3.0.0");
}

fn assert_sibling_break(leader: &str, expected: &str) {
    let report = report(
        vec![
            package("lib", leader, Some(leader)),
            package("lib_impl", "1.0.0", Some("1.0.0")),
            depends(package("app", "5.0.0", Some("5.0.0")), "lib", true),
        ],
        vec![],
        &[&["lib", "lib_impl"]],
    );
    let plan = generate(&report, &[("lib_impl", "breaking")]).unwrap();
    assert_eq!(
        entries(&plan),
        json!([
            {"name": "app", "level": "major"},
            {"name": "lib_impl", "level": "major"}
        ])
    );
    assert_versions(
        &report,
        &plan,
        &json!({"app": "6.0.0", "lib": expected, "lib_impl": expected}),
    );
}

#[test]
fn alignment_patch_breaks_propagate_after_alignment_is_settled() {
    let report = report(
        vec![
            depends(package("lib", "0.0.5", Some("0.0.5")), "lib_impl", false),
            package("lib_impl", "0.0.4", Some("0.0.4")),
            depends(package("app", "3.0.0", Some("3.0.0")), "lib", true),
        ],
        vec![],
        &[&["lib", "lib_impl"]],
    );
    let plan = generate(&report, &[]).unwrap();
    assert_eq!(
        entries(&plan),
        json!([
            {"name": "app", "level": "major"},
            {"name": "lib", "level": "patch"}
        ])
    );
    assert_invariants(&report, &plan);
}

#[test]
fn cyclic_public_dependencies_terminate_with_one_breaking_decision() {
    let report = report(
        vec![
            depends(package("alpha", "1.0.0", Some("1.0.0")), "beta", true),
            depends(package("beta", "2.0.0", Some("2.0.0")), "alpha", true),
        ],
        vec![],
        &[],
    );
    let plan = generate(&report, &[("alpha", "breaking")]).unwrap();
    assert_eq!(plan.increments.len(), 2);
    assert!(
        plan.increments
            .iter()
            .all(|entry| entry.level.as_deref() == Some("major"))
    );
    assert_versions(&report, &plan, &json!({"alpha": "2.0.0", "beta": "3.0.0"}));
}

#[test]
fn public_propagation_does_not_add_redundant_decisions_for_group_siblings() {
    let report = report(
        vec![
            depends(package("alpha", "1.0.0", Some("1.0.0")), "lib", true),
            depends(package("beta", "1.0.0", Some("1.0.0")), "lib", true),
            package("lib", "2.0.0", Some("1.0.0")),
        ],
        vec![],
        &[&["alpha", "beta"]],
    );
    let plan = generate(&report, &[]).unwrap();
    assert_eq!(entries(&plan), json!([{"name": "alpha", "level": "major"}]));
    assert_invariants(&report, &plan);
}
