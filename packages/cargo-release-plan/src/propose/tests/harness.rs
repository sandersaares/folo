use std::collections::BTreeMap;

use ohno::AppError;
use semver::Version;
use serde_json::Value;

use crate::check::compatibility_key;
use crate::classify::PackageStatus;
use crate::plan::{PlanFile, PlanStage, SCHEMA_VERSION, resolve_plan};
use crate::propose::decision::Decisions;
use crate::propose::generate::Proposal;
use crate::report::{ReportFile, ReportPackage};
use crate::verbose::Verbose;

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

pub(crate) fn assert_invariants(report: &ReportFile, plan: &PlanFile) -> BTreeMap<String, Version> {
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
