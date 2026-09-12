// `report` command: write report.json and per-package diffs.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use ohno::AppError;
use serde::{Deserialize, Serialize};

use crate::WriteFileError;
use crate::classify::{
    AnchorJson, ChangedItem, Classification, DiffStat, PackageClass, PackageStatus, classify,
};
use crate::metadata::ReportedDep;
use crate::plan::SCHEMA_VERSION;
use crate::text::quote_path;
use crate::verbose::Verbose;

/// On-disk `report.json` body.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct ReportFile {
    pub(crate) schema_version: u32,
    pub(crate) head: String,
    pub(crate) packages: Vec<ReportPackage>,
    pub(crate) non_publishable_packages: Vec<ReportVersionTarget>,
    pub(crate) groups: BTreeMap<String, ReportGroup>,
}

/// One publishable package in `report.json`.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct ReportPackage {
    pub(crate) name: String,
    pub(crate) declared_version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) group: Option<String>,
    pub(crate) status: PackageStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) anchor: Option<AnchorJson>,
    pub(crate) changed: Vec<ChangedItem>,
    pub(crate) stat: DiffStat,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) diff_path: Option<String>,
    pub(crate) dependencies: Vec<ReportedDep>,
    pub(crate) dependents: Vec<String>,
    /// Whether the package's library is what consumers are meant to use.
    ///
    /// False for a package with no library target, and for one declaring
    /// `[package.metadata.release-plan] private-api = true`.
    pub(crate) consumer_contract: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) untracked: Vec<String>,
}

/// One non-publishable version target in `report.json`.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct ReportVersionTarget {
    pub(crate) name: String,
    pub(crate) declared_version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) group: Option<String>,
}

/// Version-group consistency as recorded in `report.json`.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct ReportGroup {
    pub(crate) members: Vec<String>,
    pub(crate) consistent: bool,
    pub(crate) version: String,
}

pub(crate) fn run_report(
    out_dir: &Path,
    base: Option<&str>,
    manifest_path: &Path,
    verbose: Verbose,
) -> Result<String, AppError> {
    let classification = classify(manifest_path, base, verbose)?;
    write_report(out_dir, &classification)
}

pub(crate) fn write_report(
    out_dir: &Path,
    classification: &Classification,
) -> Result<String, AppError> {
    fs::create_dir_all(out_dir).map_err(|error| WriteFileError::caused_by(out_dir, error))?;
    let diff_names: Vec<Option<String>> =
        classification.packages.iter().map(diff_file_name).collect();
    let packages = classification
        .packages
        .iter()
        .zip(&diff_names)
        .map(|(package, diff_name)| {
            report_package(
                package,
                diff_name.as_ref().map(|name| format!("diffs/{name}")),
            )
        })
        .collect();
    let mut groups = BTreeMap::new();
    for (name, verdict) in &classification.groups {
        groups.insert(
            name.clone(),
            ReportGroup {
                members: verdict.members().to_vec(),
                consistent: verdict.is_consistent(),
                version: verdict.version().to_string(),
            },
        );
    }
    let non_publishable_packages = classification
        .work_tree
        .version_targets
        .iter()
        .filter(|target| !target.publishable)
        .map(|target| ReportVersionTarget {
            name: target.name.clone(),
            declared_version: target.version.to_string(),
            group: classification
                .work_tree
                .groups
                .group_of(&target.name)
                .map(ToOwned::to_owned),
        })
        .collect();
    // The emitted field names are part of the consumer-facing layout documented
    // in the README, so they are compatibility-sensitive rather than incidental.
    let report = ReportFile {
        schema_version: SCHEMA_VERSION,
        head: classification.head.clone(),
        packages,
        non_publishable_packages,
        groups,
    };
    let report = serde_json::to_string_pretty(&report)
        .expect("the report body contains only JSON-serializable fields");

    // `report.json` is the completion marker. Remove an earlier marker before
    // touching its patch tree so an interrupted rerun cannot present stale JSON
    // alongside missing or partially replaced patches.
    let report_path = out_dir.join("report.json");
    if report_path.exists() {
        fs::remove_file(&report_path)
            .map_err(|error| WriteFileError::caused_by(&report_path, error))?;
    }
    // Consumer-facing layout: README "report".
    //
    // The subtree belongs to this tool, and the report contract is that it
    // holds the patches of the current classification, so it is replaced whole
    // rather than merged into: a reused output directory would otherwise offer
    // a patch from an earlier run as evidence for this one.
    let diffs_dir = out_dir.join("diffs");
    if diffs_dir.exists() {
        fs::remove_dir_all(&diffs_dir)
            .map_err(|error| WriteFileError::caused_by(&diffs_dir, error))?;
    }
    fs::create_dir_all(&diffs_dir).map_err(|error| WriteFileError::caused_by(&diffs_dir, error))?;

    for (package, diff_name) in classification.packages.iter().zip(&diff_names) {
        write_diff(&diffs_dir, package, diff_name.as_deref())?;
    }

    // Write through a staging path so even a partial write is not mistaken for
    // the completion marker. Keeping both paths together avoids a cross-filesystem rename.
    let staged_report_path = report_path.with_extension("json.tmp");
    fs::write(&staged_report_path, report.as_bytes())
        .map_err(|error| WriteFileError::caused_by(&staged_report_path, error))?;
    fs::rename(&staged_report_path, &report_path)
        .map_err(|error| WriteFileError::caused_by(&report_path, error))?;

    let needing_increment = classification
        .packages
        .iter()
        .filter(|package| package.status() == PackageStatus::NeedsIncrement)
        .count();
    Ok(format!(
        "Wrote {} ({} needing an increment)",
        quote_path(&report_path.display().to_string()),
        needing_increment
    ))
}

fn diff_file_name(package: &PackageClass) -> Option<String> {
    // Patches accompany a file difference; inherited-only and unchanged packages have none.
    if package.patch().is_empty() {
        return None;
    }
    Some(format!("{}.patch", package.name))
}

// Patch files are a dump of `package.patch`; bytes are covered by `naive_patch`.
#[cfg_attr(test, mutants::skip)]
fn write_diff(
    diffs_dir: &Path,
    package: &PackageClass,
    file_name: Option<&str>,
) -> Result<(), AppError> {
    let Some(file_name) = file_name else {
        return Ok(());
    };
    let path = diffs_dir.join(file_name);
    fs::write(&path, package.patch().as_bytes())
        .map_err(|error| WriteFileError::caused_by(&path, error))?;
    Ok(())
}

fn report_package(package: &PackageClass, diff_path: Option<String>) -> ReportPackage {
    ReportPackage {
        name: package.name.clone(),
        declared_version: package.declared_version.to_string(),
        group: package.group.clone(),
        status: package.status(),
        anchor: package.anchor().map(|anchor| AnchorJson {
            commit: anchor.commit.clone(),
            version: anchor.version.to_string(),
        }),
        changed: package.changed().to_vec(),
        stat: package.stat.clone(),
        diff_path,
        dependencies: package.dependencies.clone(),
        dependents: package.dependents.clone(),
        consumer_contract: package.consumer_contract,
        untracked: package.untracked.clone(),
    }
}
