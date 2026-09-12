// Artifact-only commands consume the same report model the classifier serializes.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use ohno::AppError;
use semver::Version;

use crate::classify::PackageStatus;
use crate::groups::Groups;
use crate::plan::SCHEMA_VERSION;
use crate::report::ReportFile;
use crate::resolved::read_json;
use crate::text::Quotable as _;
use crate::{InvalidVersionError, UnsupportedPlanSchemaError};

pub(crate) fn read_report(path: &Path) -> Result<ReportFile, AppError> {
    let path = if path.is_dir() {
        path.join("report.json")
    } else {
        path.to_path_buf()
    };
    let report: ReportFile = read_json(&path)?;
    report.validate()?;
    Ok(report)
}

impl ReportFile {
    pub(crate) fn validate(&self) -> Result<(), AppError> {
        if self.schema_version != SCHEMA_VERSION {
            return Err(UnsupportedPlanSchemaError::new(self.schema_version).into());
        }
        let mut targets = BTreeMap::new();
        for (name, version, group) in self
            .packages
            .iter()
            .map(|package| (&package.name, &package.declared_version, &package.group))
            .chain(
                self.non_publishable_packages
                    .iter()
                    .map(|package| (&package.name, &package.declared_version, &package.group)),
            )
        {
            if name.trim().is_empty() || targets.contains_key(name) {
                return Err(InvalidReportPackage::new(name).into());
            }
            targets.insert(name, (group, parse_version(name, version)?));
        }
        for package in &self.packages {
            let anchor = package
                .anchor
                .as_ref()
                .map(|anchor| parse_version(&package.name, &anchor.version))
                .transpose()?;
            let (_, declared) = targets
                .get(&package.name)
                .expect("every report package was indexed with its declared version");
            if PackageStatus::from_evidence(declared, anchor.as_ref(), !package.changed.is_empty())
                != Some(package.status)
            {
                return Err(InconsistentReportStatus::new(&package.name).into());
            }
            for reference in package
                .dependencies
                .iter()
                .map(|dependency| &dependency.name)
                .chain(&package.dependents)
            {
                if !targets.contains_key(reference) {
                    return Err(InvalidReportReference::new(&package.name, reference).into());
                }
            }
        }

        let mut grouped = BTreeSet::new();
        for (name, group) in &self.groups {
            _ = parse_version(name, &group.version)?;
            if group.members.len() < 2
                || group.members.first() != Some(name)
                || !group
                    .members
                    .iter()
                    .is_sorted_by(|left, right| left < right)
            {
                return Err(InvalidReportGroup::new(name).into());
            }
            for member in &group.members {
                if !grouped.insert(member)
                    || targets
                        .get(member)
                        .is_none_or(|(reference, _)| reference.as_deref() != Some(name.as_str()))
                {
                    return Err(InvalidReportGroup::new(name).into());
                }
            }
        }
        for (name, (group, _)) in targets {
            if group.is_some() && !grouped.contains(name) {
                return Err(InvalidReportPackage::new(name).into());
            }
        }
        Ok(())
    }

    pub(crate) fn version_targets(&self) -> BTreeMap<String, Version> {
        self.packages
            .iter()
            .map(|package| (&package.name, &package.declared_version))
            .chain(
                self.non_publishable_packages
                    .iter()
                    .map(|package| (&package.name, &package.declared_version)),
            )
            .map(|(name, version)| {
                (
                    name.clone(),
                    version
                        .parse()
                        .expect("report versions were validated before resolution"),
                )
            })
            .collect()
    }

    pub(crate) fn version_groups(&self) -> Groups {
        Groups::from_edges(
            self.packages
                .iter()
                .map(|package| package.name.clone())
                .chain(
                    self.non_publishable_packages
                        .iter()
                        .map(|package| package.name.clone()),
                ),
            self.groups.iter().flat_map(|(name, group)| {
                group
                    .members
                    .iter()
                    .map(|member| (name.clone(), member.clone()))
            }),
        )
    }
}

fn parse_version(name: &str, version: &str) -> Result<Version, AppError> {
    version
        .parse()
        .map_err(|error| InvalidVersionError::caused_by(name, version, error).into())
}

/// A report needs unique package identities and reciprocal group references.
#[ohno::error]
#[display("Invalid package identity or group reference in report: {}", name.quoted())]
struct InvalidReportPackage {
    name: String,
}

/// Status cannot contradict the producer's anchor, version and change evidence.
#[ohno::error]
#[display("Report package {} has inconsistent status, anchor, version or change evidence", name.quoted())]
struct InconsistentReportStatus {
    name: String,
}

/// Reported workspace relationships must retain their referenced version targets.
#[ohno::error]
#[display("Report package {} references missing workspace package {}", package.quoted(), target.quoted())]
struct InvalidReportReference {
    package: String,
    target: String,
}

/// A group must be a sorted, disjoint set keyed by its smallest member.
#[ohno::error]
#[display("Invalid version group in report: {}", name.quoted())]
struct InvalidReportGroup {
    name: String,
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::fs;

    use serde_json::{Value, json};
    use tempfile::tempdir;

    use super::*;
    use crate::report::fixture::{package, report};

    #[test]
    fn anchored_reports_cannot_claim_pending_release_without_a_version_increase() {
        let mut data = report(vec![package("api", "needs-increment", true)]);
        data.packages.first_mut().unwrap().status = PackageStatus::PendingRelease;
        assert!(
            data.validate()
                .unwrap_err()
                .find_source::<InconsistentReportStatus>()
                .is_some()
        );
        data.packages.first_mut().unwrap().declared_version = "0.9.0".to_owned();
        assert!(
            data.validate()
                .unwrap_err()
                .find_source::<InconsistentReportStatus>()
                .is_some()
        );
    }

    #[test]
    fn anchorless_reports_have_no_comparison_evidence() {
        let mut data = report(vec![package("api", "needs-increment", true)]);
        let package = data.packages.first_mut().unwrap();
        package.anchor = None;
        package.status = PackageStatus::PendingRelease;
        assert!(
            data.validate()
                .unwrap_err()
                .find_source::<InconsistentReportStatus>()
                .is_some()
        );
        data.packages.first_mut().unwrap().changed.clear();
        data.validate().unwrap();
    }

    #[test]
    fn in_memory_reports_must_use_the_current_schema() {
        let mut report = report(Vec::new());
        report.schema_version = SCHEMA_VERSION.checked_add(1).unwrap();
        assert!(
            report
                .validate()
                .unwrap_err()
                .find_source::<UnsupportedPlanSchemaError>()
                .is_some()
        );
    }

    #[test]
    fn workspace_dependencies_must_reference_known_targets() {
        let mut data = grouped();
        let package = data.packages.first_mut().unwrap();
        package.dependencies = serde_json::from_value(json!([
            {"name": "absent", "req": "1.0.0", "public": true, "exact_pin": false}
        ]))
        .unwrap();
        let error = data.validate().unwrap_err();
        assert!(error.find_source::<InvalidReportReference>().is_some());
        data.packages
            .first_mut()
            .unwrap()
            .dependencies
            .first_mut()
            .unwrap()
            .name = "helper".to_owned();
        data.validate().unwrap();
    }

    #[test]
    fn reported_dependents_use_exact_workspace_identities() {
        let mut data = grouped();
        data.packages.first_mut().unwrap().dependents = vec!["API".to_owned()];
        assert!(
            data.validate()
                .unwrap_err()
                .find_source::<InvalidReportReference>()
                .is_some()
        );
        data.packages.first_mut().unwrap().dependents = vec!["api".to_owned()];
        data.validate().unwrap();
    }

    fn grouped() -> ReportFile {
        let mut report = report(vec![package("api", "needs-increment", true)]);
        report.packages.first_mut().unwrap().group = Some("api".to_owned());
        report.non_publishable_packages = serde_json::from_value(json!([
            {"name": "helper", "declared_version": "1.1.0", "group": "api"}
        ]))
        .unwrap();
        report.groups = serde_json::from_value(json!({
            "api": {"members": ["api", "helper"], "version": "1.1.0", "consistent": false}
        }))
        .unwrap();
        report
    }

    #[test]
    fn roundtrips_all_report_change_shapes_and_version_targets() {
        let mut report = grouped();
        report.packages.first_mut().unwrap().changed = serde_json::from_value(json!([
            {"source": "package", "path": "src/lib.rs", "change": "modified"},
            {"source": "inherited", "field": "license"},
            {"source": "lockfile", "dependency": "dependency@1.0.0", "change": "added"}
        ]))
        .unwrap();
        let json = serde_json::to_value(&report).unwrap();
        let parsed: ReportFile = serde_json::from_value(json.clone()).unwrap();
        parsed.validate().unwrap();
        assert_eq!(serde_json::to_value(&parsed).unwrap(), json);
        assert_eq!(parsed.version_groups().closure("helper"), ["api", "helper"]);
        assert_eq!(
            parsed.version_targets().get("helper"),
            Some(&Version::new(1, 1, 0))
        );
    }

    #[test]
    fn rejects_missing_required_report_fields() {
        let original = serde_json::to_value(grouped()).unwrap();
        for key in [
            "schema_version",
            "head",
            "packages",
            "non_publishable_packages",
            "groups",
        ] {
            let mut value = original.clone();
            _ = value.as_object_mut().unwrap().remove(key);
            _ = serde_json::from_value::<ReportFile>(value).unwrap_err();
        }
    }

    #[test]
    fn rejects_missing_required_package_fields() {
        check_required_package_fields(false);
    }

    #[test]
    fn rejects_null_required_package_fields() {
        check_required_package_fields(true);
    }

    fn check_required_package_fields(use_null: bool) {
        let original = serde_json::to_value(grouped()).unwrap();
        for key in [
            "name",
            "declared_version",
            "status",
            "changed",
            "dependencies",
            "dependents",
            "stat",
            "consumer_contract",
        ] {
            let mut value = original.clone();
            let package = value
                .pointer_mut("/packages/0")
                .unwrap()
                .as_object_mut()
                .unwrap();
            if use_null {
                _ = package.insert(key.to_owned(), Value::Null);
            } else {
                _ = package.remove(key);
            }
            _ = serde_json::from_value::<ReportFile>(value).unwrap_err();
        }
    }

    #[test]
    fn rejects_invalid_status_change_and_dependency_shapes() {
        let original = serde_json::to_value(grouped()).unwrap();
        for (key, invalid) in [
            ("status", json!("Needs-Increment")),
            ("changed", json!([{"source": "unknown"}])),
            (
                "dependencies",
                json!([{"name": "api", "req": "1.0.0", "exact_pin": false}]),
            ),
            ("consumer_contract", json!("true")),
        ] {
            let mut value = original.clone();
            *value
                .pointer_mut("/packages/0")
                .unwrap()
                .get_mut(key)
                .unwrap() = invalid;
            _ = serde_json::from_value::<ReportFile>(value).unwrap_err();
        }
    }

    #[test]
    fn rejects_duplicate_names_and_nonreciprocal_groups() {
        let original = grouped();
        for name in ["", "api"] {
            let mut report = original.clone();
            report.non_publishable_packages.first_mut().unwrap().name = name.to_owned();
            let error = report.validate().unwrap_err();
            assert!(error.find_source::<InvalidReportPackage>().is_some());
        }
        for members in [
            vec!["api"],
            vec!["helper", "api"],
            vec!["api", "api"],
            vec!["api", "absent"],
            vec!["helper", "missing"],
        ] {
            let mut report = original.clone();
            report.groups.get_mut("api").unwrap().members =
                members.into_iter().map(str::to_owned).collect();
            let error = report.validate().unwrap_err();
            assert!(error.find_source::<InvalidReportGroup>().is_some());
        }
        let mut report = original.clone();
        report.non_publishable_packages.first_mut().unwrap().group = None;
        assert!(
            report
                .validate()
                .unwrap_err()
                .find_source::<InvalidReportGroup>()
                .is_some()
        );
        let mut report = original;
        report.groups.clear();
        assert!(
            report
                .validate()
                .unwrap_err()
                .find_source::<InvalidReportPackage>()
                .is_some()
        );
    }

    #[test]
    fn validates_every_version_even_when_the_package_is_not_selected() {
        let original = grouped();
        for location in ["declared", "anchor", "helper", "group"] {
            let mut report = original.clone();
            let version = match location {
                "declared" => &mut report.packages.first_mut().unwrap().declared_version,
                "anchor" => {
                    &mut report
                        .packages
                        .first_mut()
                        .unwrap()
                        .anchor
                        .as_mut()
                        .unwrap()
                        .version
                }
                "helper" => {
                    &mut report
                        .non_publishable_packages
                        .first_mut()
                        .unwrap()
                        .declared_version
                }
                "group" => &mut report.groups.get_mut("api").unwrap().version,
                _ => unreachable!(),
            };
            *version = "not-a-version".to_owned();
            assert!(
                report
                    .validate()
                    .unwrap_err()
                    .find_source::<InvalidVersionError>()
                    .is_some()
            );
        }
    }

    #[test]
    #[cfg_attr(miri, ignore = "reads report artifacts from a real filesystem")]
    fn reads_file_or_directory_and_rejects_unsupported_schema() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("report.json");
        let report = grouped();
        fs::write(&path, serde_json::to_vec(&report).unwrap()).unwrap();
        assert_eq!(read_report(&path).unwrap().head, report.head);
        assert_eq!(read_report(directory.path()).unwrap().head, report.head);
        // Schema rejection precedes interpreting fields from a different protocol.
        fs::write(
            &path,
            json!({"schema_version": SCHEMA_VERSION.checked_add(1).unwrap()}).to_string(),
        )
        .unwrap();
        assert!(
            read_report(&path)
                .unwrap_err()
                .find_source::<UnsupportedPlanSchemaError>()
                .is_some()
        );
        fs::remove_file(&path).unwrap();
        _ = read_report(&path).unwrap_err();
    }
}
