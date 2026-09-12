use std::collections::BTreeMap;

use semver::Version;

use crate::classify::{AnchorJson, ChangedItem, DiffStat, PackageStatus};
use crate::metadata::{DepKind, ReportedDep};
use crate::plan::SCHEMA_VERSION;
use crate::report::{ReportFile, ReportGroup, ReportPackage, ReportVersionTarget};

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
