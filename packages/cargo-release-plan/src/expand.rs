// `expand` command: turn a proposed plan into an expanded plan.
//
// A proposed plan may name a version group, or one member of it, instead of
// every package the release decision reaches. This command resolves it and
// writes the complete explicit package/version set, so a caller reviews the same
// document `apply` consumes.

use std::fs;
use std::path::Path;

use ohno::AppError;
use serde::Serialize;

use crate::metadata::load_tracked_work_tree;
use crate::plan::{PlanFile, SCHEMA_VERSION, resolve_plan};
use crate::text::plural;
use crate::verbose::Verbose;
use crate::{
    CreateOutputDirectoryError, ParsePlanError, ReadFileError, WriteFileError, quote_path,
};

/// On-disk body of an expanded plan.
///
/// An expanded plan is itself applyable, so a caller reviews and applies one
/// document rather than a rendering of another. Every entry carries an explicit
/// version because resolution has already applied the increment to the group's
/// highest declared member version. The `expanded` stamp records the planning
/// stage, which is what holds the document to the package set it names instead
/// of letting the group configuration of the day widen it.
#[derive(Serialize)]
struct ExpandedPlanFile {
    schema_version: u32,
    expanded: bool,
    increments: Vec<ExpandedPackageVersion>,
}

/// One package's resolved version within an expanded plan.
#[derive(Serialize)]
struct ExpandedPackageVersion {
    name: String,
    version: String,
}

pub(crate) fn run_expand(
    plan_path: &Path,
    out_path: &Path,
    manifest_path: &Path,
    verbose: Verbose,
) -> Result<String, AppError> {
    let plan = fs::read_to_string(plan_path)
        .map_err(|error| ReadFileError::caused_by(plan_path, error))?;
    let plan: PlanFile =
        serde_json::from_str(&plan).map_err(|error| ParsePlanError::caused_by(plan_path, error))?;

    let (work_tree, _) = load_tracked_work_tree(manifest_path)?;
    // Only Git-tracked publishable members are valid targets, and a group
    // increments from the highest version any of its members declares.
    // Ref: docs/implementation.md, "Plan resolution and application".
    let publishable = work_tree.publishable_versions();
    let resolved = resolve_plan(&plan, &work_tree.groups, &publishable, verbose)?;

    verbose.note(|| {
        format!(
            "{} named {} and expands to {}; any difference is group members the plan did not name",
            quote_path(&plan_path.to_string_lossy()),
            plural(plan.increments.len(), "increment"),
            plural(resolved.packages.len(), "package version")
        )
    });

    let document = ExpandedPlanFile {
        schema_version: SCHEMA_VERSION,
        expanded: true,
        increments: resolved
            .packages
            .iter()
            .map(|(name, version)| ExpandedPackageVersion {
                name: name.clone(),
                version: version.to_string(),
            })
            .collect(),
    };
    let mut json = serde_json::to_string_pretty(&document)
        .expect("an expanded plan holds only strings and a number, which always serialize");
    json.push('\n');
    // A bare filename has an empty parent and therefore needs no directory creation.
    if let Some(parent) = out_path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)
            .map_err(|error| CreateOutputDirectoryError::caused_by(parent, error))?;
    }
    fs::write(out_path, json.as_bytes())
        .map_err(|error| WriteFileError::caused_by(out_path, error))?;

    Ok(format!(
        "Expanded {} to {}",
        plural(resolved.packages.len(), "package version"),
        quote_path(&out_path.to_string_lossy())
    ))
}
