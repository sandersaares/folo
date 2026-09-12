use std::fs;

use semver::Version;
use serde_json::json;
use tempfile::tempdir_in;

use crate::plan::{IncrementLevel, increment_version, resolve_plan};
use crate::propose::run_propose;
use crate::propose::tests::{
    assert_invariants, depends, entries, generate, helper, package, report,
};
use crate::resolved::write_json;
use crate::verbose::Verbose;

#[test]
fn semantic_member_entries_keep_their_names_and_merge_only_in_the_resolver() {
    let report = report(
        vec![
            package("nm", "1.2.0", Some("1.0.0")),
            package("nm_impl", "1.2.0", Some("1.2.0")),
        ],
        vec![],
        &[&["nm", "nm_impl"]],
    );
    let plan = generate(&report, &[("nm", "nonbreaking"), ("nm_impl", "patch")]).unwrap();
    assert_eq!(
        entries(&plan),
        json!([{"name": "nm_impl", "level": "patch"}])
    );
    let plan = generate(&report, &[("nm", "breaking"), ("nm_impl", "patch")]).unwrap();
    assert_eq!(plan.increments.len(), 2);
    assert_invariants(&report, &plan);
}

#[test]
fn anchorless_dependents_and_external_dependencies_do_not_invent_decisions() {
    let report = report(
        vec![
            package("lib", "2.0.0", Some("1.0.0")),
            depends(package("new", "0.1.0", None), "lib", true),
            depends(package("other", "1.0.0", Some("1.0.0")), "external", true),
        ],
        vec![],
        &[],
    );
    assert!(generate(&report, &[]).unwrap().increments.is_empty());
}

#[test]
fn ordered_output_sorts_unsorted_decisions() {
    assert_ordered_output(false);
}

#[test]
fn ordered_output_is_unchanged_by_reversed_report_and_decisions() {
    assert_ordered_output(true);
}

fn assert_ordered_output(reverse: bool) {
    let mut report = report(
        vec![
            package("alpha", "1.0.0", Some("1.0.0")),
            package("zeta", "1.0.0", Some("1.0.0")),
            package("laggard", "1.0.0", Some("1.0.0")),
            package("leader", "1.1.0", Some("1.1.0")),
        ],
        vec![],
        &[&["laggard", "leader"]],
    );
    let mut changes = [("zeta", "patch"), ("alpha", "patch")];
    if reverse {
        report.packages.reverse();
        changes.reverse();
    }
    let plan = generate(&report, &changes).unwrap();
    assert_eq!(
        entries(&plan),
        json!([
            {"name": "alpha", "level": "patch"},
            {"name": "laggard", "version": "1.1.0"},
            {"name": "zeta", "level": "patch"}
        ])
    );
    assert_invariants(&report, &plan);
}

#[test]
fn empty_and_consistent_reports_generate_empty_proposals() {
    for report in [
        report(vec![], vec![], &[]),
        report(
            vec![
                package("a", "1.0.0", Some("1.0.0")),
                package("b", "1.0.0", Some("1.0.0")),
            ],
            vec![],
            &[&["a", "b"]],
        ),
    ] {
        assert!(generate(&report, &[]).unwrap().increments.is_empty());
    }
}

#[test]
#[cfg_attr(miri, ignore = "reads and writes malformed report artifacts")]
fn malformed_versions_anywhere_in_the_report_invalidate_stale_output() {
    let directory = tempdir_in(".").unwrap();
    let report_path = directory.path().join("report.json");
    let decisions_path = directory.path().join("decisions.json");
    let output = directory.path().join("plan.json");
    fs::write(&decisions_path, r#"{"schema_version":1,"changes":[]}"#).unwrap();
    let report = report(
        vec![package("library", "1.0.0", Some("1.0.0"))],
        vec![helper("helper", "1.0.0"), helper("support", "1.1.0")],
        &[&["helper", "support"]],
    );
    for pointer in [
        "/packages/0/declared_version",
        "/packages/0/anchor/version",
        "/non_publishable_packages/0/declared_version",
        "/groups/helper/version",
    ] {
        let mut value = serde_json::to_value(&report).unwrap();
        *value.pointer_mut(pointer).unwrap() = json!("invalid");
        write_json(&report_path, &value).unwrap();
        fs::write(&output, "stale").unwrap();
        _ = run_propose(&report_path, &decisions_path, &output, Verbose::new(false)).unwrap_err();
        assert!(!output.exists());
    }
}

#[test]
fn mechanical_levels_use_the_original_group_base_only_once() {
    let report = report(
        vec![
            package("a", "1.0.0", Some("1.0.0")),
            package("b", "2.3.4", Some("2.3.4")),
        ],
        vec![],
        &[&["a", "b"]],
    );
    let plan = generate(&report, &[("a", "breaking"), ("b", "nonbreaking")]).unwrap();
    let resolved = resolve_plan(
        &plan,
        &report.version_groups(),
        &report.version_targets(),
        Verbose::new(false),
    )
    .unwrap();
    let expected = increment_version(&Version::new(2, 3, 4), IncrementLevel::Major).unwrap();
    assert_eq!(resolved.packages.get("a"), Some(&expected));
    assert_eq!(resolved.packages.get("b"), Some(&expected));
}
