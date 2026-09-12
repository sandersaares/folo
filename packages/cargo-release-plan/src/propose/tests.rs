use std::collections::BTreeMap;
use std::fs;

use ohno::AppError;
use semver::Version;
use serde_json::{Value, json};
use tempfile::tempdir_in;

use crate::check::compatibility_key;
use crate::classify::{AnchorJson, ChangedItem, DiffStat, PackageStatus};
use crate::metadata::{DepKind, ReportedDep};
use crate::plan::{
    IncrementLevel, PlanFile, PlanStage, SCHEMA_VERSION, increment_version, resolve_plan,
};
use crate::propose::decision::Decisions;
use crate::propose::generate::Proposal;
use crate::propose::run_propose;
use crate::report::{ReportFile, ReportGroup, ReportPackage, ReportVersionTarget};
use crate::resolved::write_json;
use crate::verbose::Verbose;

pub(crate) fn package(name: &str, version: &str, anchor: Option<&str>) -> ReportPackage {
    ReportPackage {
        name: name.to_owned(),
        declared_version: version.to_owned(),
        group: None,
        anchor: anchor.map(|version| AnchorJson {
            commit: "release".to_owned(),
            version: version.to_owned(),
        }),
        status: PackageStatus::Unchanged,
        changed: Vec::new(),
        stat: DiffStat {
            files: 0,
            insertions: 0,
            deletions: 0,
        },
        dependencies: Vec::new(),
        dependents: Vec::new(),
        consumer_contract: true,
        untracked: Vec::new(),
        diff_path: None,
    }
}

pub(crate) fn helper(name: &str, version: &str) -> ReportVersionTarget {
    ReportVersionTarget {
        name: name.to_owned(),
        declared_version: version.to_owned(),
        group: None,
    }
}

pub(crate) fn depends(mut package: ReportPackage, dependency: &str, public: bool) -> ReportPackage {
    package.dependencies.push(ReportedDep {
        name: dependency.to_owned(),
        // Proposal tests reason about version movement, independently of requirement spelling.
        req: "^1.0.0".to_owned(),
        exact_pin: false,
        kind: DepKind::Normal,
        public,
    });
    package
}

pub(crate) fn needs(mut package: ReportPackage) -> ReportPackage {
    package.status = PackageStatus::NeedsIncrement;
    package.changed.push(ChangedItem::Package {
        path: "src/lib.rs".to_owned(),
        change: "modified".to_owned(),
    });
    package
}

pub(crate) fn report(
    mut packages: Vec<ReportPackage>,
    mut helpers: Vec<ReportVersionTarget>,
    groups: &[&[&str]],
) -> ReportFile {
    let mut group_values = BTreeMap::new();
    for members in groups {
        let mut members = members.to_vec();
        members.sort_unstable();
        let name = *members.first().unwrap();
        let mut versions = Vec::new();
        for member in &members {
            let (group, declared) = if let Some(package) =
                packages.iter_mut().find(|package| package.name == *member)
            {
                (&mut package.group, &package.declared_version)
            } else {
                let helper = helpers
                    .iter_mut()
                    .find(|helper| helper.name == *member)
                    .unwrap();
                (&mut helper.group, &helper.declared_version)
            };
            *group = Some(name.to_owned());
            versions.push(declared.parse::<Version>().unwrap());
        }
        group_values.insert(
            name.to_owned(),
            ReportGroup {
                members: members.into_iter().map(str::to_owned).collect(),
                consistent: versions
                    .iter()
                    .all(|version| Some(version) == versions.first()),
                version: versions.iter().max().unwrap().to_string(),
            },
        );
    }
    ReportFile {
        schema_version: SCHEMA_VERSION,
        head: "captured-head".to_owned(),
        packages,
        non_publishable_packages: helpers,
        groups: group_values,
    }
}

pub(crate) fn generate(
    report: &ReportFile,
    changes: &[(&str, &str)],
) -> Result<PlanFile, AppError> {
    report.validate()?;
    Proposal::new(report).generate(&Decisions::for_test(changes), Verbose::new(false))
}

pub(crate) fn entries(plan: &PlanFile) -> Value {
    serde_json::to_value(&plan.increments).unwrap()
}

pub(crate) fn assert_versions(report: &ReportFile, plan: &PlanFile, expected: &Value) {
    let versions = assert_invariants(report, plan);
    let versions: BTreeMap<String, String> = versions
        .into_iter()
        .map(|(name, version)| (name, version.to_string()))
        .collect();
    assert_eq!(&serde_json::to_value(versions).unwrap(), expected);
}

fn assert_invariants(report: &ReportFile, plan: &PlanFile) -> BTreeMap<String, Version> {
    assert_eq!(plan.schema_version, SCHEMA_VERSION);
    assert_eq!(plan.stage(), PlanStage::Proposed);
    assert!(plan.resolved.is_none());
    let declared = report.version_targets();
    let groups = report.version_groups();
    let mut kinds = BTreeMap::new();
    for entry in &plan.increments {
        assert!(declared.contains_key(&entry.name));
        assert!(entry.level.is_some() ^ entry.version.is_some());
        let key = groups.group_of(&entry.name).unwrap_or(&entry.name);
        if let Some(previous) = kinds.insert(key, entry.level.is_some()) {
            assert_eq!(previous, entry.level.is_some());
        }
    }
    let mut resolved = declared.clone();
    resolved.extend(
        resolve_plan(plan, &groups, &declared, Verbose::new(false))
            .unwrap()
            .packages,
    );
    for (name, version) in &resolved {
        assert!(version >= declared.get(name).unwrap());
    }
    for group in report.groups.values() {
        let first = resolved.get(group.members.first().unwrap()).unwrap();
        assert!(first.pre.is_empty());
        assert!(first.build.is_empty());
        assert!(
            group
                .members
                .iter()
                .all(|name| resolved.get(name) == Some(first))
        );
    }
    for package in &report.packages {
        let version = resolved.get(&package.name).unwrap();
        if package.status == PackageStatus::NeedsIncrement {
            assert!(version > declared.get(&package.name).unwrap());
        }
        let Some(anchor) = &package.anchor else {
            continue;
        };
        let anchor = anchor.version.parse::<Version>().unwrap();
        if Some(version) == declared.get(&package.name) && version.cmp_precedence(&anchor).is_le() {
            for dependency in &package.dependencies {
                if let Some(dependency_version) = resolved.get(&dependency.name) {
                    assert_eq!(Some(dependency_version), declared.get(&dependency.name));
                }
            }
        }
        assert_public_dependencies(report, package, &resolved, &anchor, version);
    }
    resolved
}

fn assert_public_dependencies(
    report: &ReportFile,
    package: &ReportPackage,
    resolved: &BTreeMap<String, Version>,
    anchor: &Version,
    version: &Version,
) {
    for dependency in package
        .dependencies
        .iter()
        .filter(|dependency| dependency.public)
    {
        let Some(target) = report
            .packages
            .iter()
            .find(|target| target.name == dependency.name)
        else {
            continue;
        };
        if let Some(target_anchor) = &target.anchor {
            let target_anchor = target_anchor.version.parse::<Version>().unwrap();
            if compatibility_key(&target_anchor)
                != compatibility_key(resolved.get(&target.name).unwrap())
            {
                assert_ne!(compatibility_key(anchor), compatibility_key(version));
            }
        }
    }
}

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
