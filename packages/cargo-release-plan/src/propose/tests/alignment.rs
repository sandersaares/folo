use serde_json::json;

use crate::plan::{PlanFile, PlanStage};
use crate::propose::generate::Proposal;
use crate::propose::tests::{
    assert_invariants, assert_versions, depends, entries, generate, helper, needs, package, report,
};
use crate::verbose::Verbose;

#[test]
fn reciprocal_group_requirements_settle_without_repeated_increments() {
    assert_reciprocal_requirements(false);
}

#[test]
fn reciprocal_group_requirements_settle_in_reverse_report_order() {
    assert_reciprocal_requirements(true);
}

fn assert_reciprocal_requirements(reverse: bool) {
    let mut report = report(
        vec![
            depends(package("alpha", "1.1.0", Some("1.1.0")), "beta_impl", false),
            package("alpha_impl", "1.0.0", Some("1.0.0")),
            depends(package("beta", "2.1.0", Some("2.1.0")), "alpha_impl", false),
            package("beta_impl", "2.0.0", Some("2.0.0")),
        ],
        vec![],
        &[&["alpha", "alpha_impl"], &["beta", "beta_impl"]],
    );
    if reverse {
        report.packages.reverse();
    }
    let plan = generate(&report, &[]).unwrap();
    assert_eq!(
        entries(&plan),
        json!([
            {"name": "alpha", "level": "patch"},
            {"name": "beta", "level": "patch"}
        ])
    );
    assert_versions(
        &report,
        &plan,
        &json!({
            "alpha": "1.1.1", "alpha_impl": "1.1.1",
            "beta": "2.1.1", "beta_impl": "2.1.1"
        }),
    );
}

#[test]
fn group_requirement_changes_propagate_across_multiple_alignment_passes() {
    // Only the leaders need release assessments: each laggard moves in every alignment choice.
    // Helpers retain the complete group graph without unrelated release records in this fixture.
    let report = report(
        vec![
            depends(package("a", "1.1.0", Some("1.1.0")), "b", false),
            depends(package("b", "2.1.0", Some("2.1.0")), "c_impl", false),
            depends(package("c", "0.0.5", Some("0.0.5")), "c_impl", false),
        ],
        vec![
            helper("a_impl", "1.0.0"),
            helper("b_impl", "2.0.0"),
            helper("c_impl", "0.0.4"),
        ],
        &[&["a", "a_impl"], &["b", "b_impl"], &["c", "c_impl"]],
    );
    // This scenario isolates the alignment fixed point. Whole-proposal composition is covered
    // by smaller cases, so Miri need not repeat this long cascade through the outer fixed point.
    let alignment = Proposal::new(&report)
        .align_groups(&[], Verbose::new(false))
        .unwrap();
    let plan = PlanFile::new(PlanStage::Proposed, alignment.into_values().collect());
    assert_eq!(
        entries(&plan),
        json!([
            {"name": "a", "level": "patch"},
            {"name": "b", "level": "patch"},
            {"name": "c", "level": "patch"}
        ])
    );
    assert_versions(
        &report,
        &plan,
        &json!({
            "a": "1.1.1", "a_impl": "1.1.1",
            "b": "2.1.1", "b_impl": "2.1.1",
            "c": "0.0.6", "c_impl": "0.0.6"
        }),
    );
}

#[test]
fn exact_alignment_uses_helpers_for_the_key_and_highest_version() {
    assert_helper_alignment(&[], "5.0.0");
}

#[test]
fn semantic_alignment_uses_the_highest_helper_version() {
    assert_helper_alignment(&[("library", "patch")], "5.0.1");
}

fn assert_helper_alignment(changes: &[(&str, &str)], expected: &str) {
    let report = report(
        vec![
            needs(package("library", "1.0.0", Some("1.0.0"))),
            depends(
                package("application", "2.0.0", Some("2.0.0")),
                "library",
                true,
            ),
        ],
        vec![helper("alignment-helper", "5.0.0")],
        &[&["alignment-helper", "library"]],
    );
    let plan = generate(&report, changes).unwrap();
    assert!(
        plan.increments.iter().any(|entry| {
            entry.name == "application" && entry.level.as_deref() == Some("major")
        })
    );
    if changes.is_empty() {
        let first = plan.increments.first().unwrap();
        assert_eq!(first.name, "alignment-helper");
        assert_eq!(first.version.as_deref(), Some("5.0.0"));
    } else {
        let last = plan.increments.last().unwrap();
        assert_eq!(last.name, "library");
        assert_eq!(last.level.as_deref(), Some("patch"));
    }
    assert_versions(
        &report,
        &plan,
        &json!({"alignment-helper": expected, "library": expected, "application": "3.0.0"}),
    );
}

#[test]
fn helper_only_groups_align_without_release_assessments() {
    let mut report = report(
        vec![],
        vec![helper("helper", "1.0.0"), helper("support", "1.1.0")],
        &[&["helper", "support"]],
    );
    // The consistency exemption does not suppress alignment of different declared versions.
    report.groups.get_mut("helper").unwrap().consistent = true;
    let plan = generate(&report, &[]).unwrap();
    assert_eq!(
        entries(&plan),
        json!([{"name": "helper", "version": "1.1.0"}])
    );
    assert_invariants(&report, &plan);
}

#[test]
fn prerelease_group_maximum_becomes_a_plain_patch_version() {
    assert_nonplain_maximum("1.2.3-alpha.1", "1.2.2");
}

#[test]
fn build_metadata_group_maximum_becomes_a_plain_patch_version() {
    assert_nonplain_maximum("1.2.3+build", "1.2.2");
}

#[test]
fn equal_nonplain_group_members_become_a_plain_patch_version() {
    assert_nonplain_maximum("1.2.3+build", "1.2.3+build");
}

#[test]
fn build_metadata_tie_becomes_a_plain_patch_version() {
    assert_nonplain_maximum("1.2.3+build", "1.2.3");
}

fn assert_nonplain_maximum(leader: &str, laggard: &str) {
    let report = report(
        vec![],
        vec![helper("helper", leader), helper("support", laggard)],
        &[&["helper", "support"]],
    );
    let plan = generate(&report, &[]).unwrap();
    assert_eq!(
        entries(&plan),
        json!([{"name": "helper", "level": "patch"}])
    );
    assert_versions(
        &report,
        &plan,
        &json!({"helper": "1.2.4", "support": "1.2.4"}),
    );
}

#[test]
fn lower_nonplain_group_members_can_align_to_a_plain_highest_version() {
    let report = report(
        vec![],
        vec![
            helper("helper", "1.2.3-alpha.1"),
            helper("support", "1.2.3"),
        ],
        &[&["helper", "support"]],
    );
    let plan = generate(&report, &[]).unwrap();
    assert_eq!(
        entries(&plan),
        json!([{"name": "helper", "version": "1.2.3"}])
    );
    assert_invariants(&report, &plan);
}

#[test]
fn sufficient_group_decision_is_dropped_but_drift_is_aligned() {
    let report = report(
        vec![
            package("nm", "1.1.0", Some("1.0.0")),
            package("nm_impl", "1.0.0", Some("1.0.0")),
        ],
        vec![],
        &[&["nm", "nm_impl"]],
    );
    let plan = generate(&report, &[("nm", "patch")]).unwrap();
    assert_eq!(entries(&plan), json!([{"name": "nm", "version": "1.1.0"}]));
}
