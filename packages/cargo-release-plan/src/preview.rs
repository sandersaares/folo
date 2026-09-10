// Explicit offline preparation and proposal-specific fixed-point resolution.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, absolute};

use ohno::AppError;
use semver::Version;
use serde::{Deserialize, Serialize};

use crate::WriteFileError;
use crate::apply::compute_edits;
use crate::check::{CheckFormat, releases_breaking_change, run_check};
use crate::classify::{ChangedItem, Classification, PackageStatus, classify};
use crate::command::hash_bytes;
use crate::manifest::requirement_names_version;
use crate::metadata::load_tracked_work_tree;
use crate::plan::{
    IncrementLevel, PlanFile, PlanIncrement, PlanStage, ResolvedVersions, SCHEMA_VERSION,
    increment_version, resolve_plan,
};
use crate::prospective::Prospective;
use crate::report::write_report;
use crate::resolved::{Artifact, Inputs, ResolvedState, canonical, read_json, write_json};
use crate::verbose::Verbose;

/// Post-refresh workspace inputs captured before semantic grading.
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Prepared {
    schema_version: u32,
    inputs: Inputs,
}

pub(crate) fn run_prepare(
    output: &Path,
    base: Option<&str>,
    manifest: &Path,
    verbose: Verbose,
) -> Result<String, AppError> {
    let output = absolute(output).map_err(|error| WriteFileError::caused_by(output, error))?;
    let manifest = canonical(manifest)?;
    let inputs = Inputs::capture(&manifest, base)?;
    let prospective = Prospective::new(&output, &inputs)?;
    remove_marker(&output.join("prepared.json"))?;
    prospective.resolve(verbose)?;
    let files = prospective.artifacts(&inputs)?;
    inputs.verify(&manifest, None)?;
    let (work_tree, _) = load_tracked_work_tree(&manifest)?;
    let lockfile = work_tree.workspace_root.join("Cargo.lock");
    validate_preparation_files(inputs.root(), &lockfile, &files)?;
    // Preparation is the explicit mutation boundary. Install only the successfully resolved
    // lockfile before capturing evidence so semantic checks run against this same live state.
    for file in files {
        fs::write(&lockfile, file.contents)
            .map_err(|error| WriteFileError::caused_by(&lockfile, error))?;
    }
    let inputs = Inputs::capture(&manifest, base)?;
    let classification = classify(&manifest, Some(&inputs.base), verbose)?;
    inputs.verify(&manifest, None)?;
    write_report(&output, &classification)?;
    write_json(
        &output.join("prepared.json"),
        &Prepared {
            schema_version: SCHEMA_VERSION,
            inputs,
        },
    )?;
    Ok(format!(
        "Refreshed the workspace lockfile offline and prepared release evidence in {}",
        output.display()
    ))
}

pub(crate) fn run_preview(
    plan: &Path,
    prepared: &Path,
    output: &Path,
    manifest: &Path,
    verbose: Verbose,
) -> Result<String, AppError> {
    let output = absolute(output).map_err(|error| WriteFileError::caused_by(output, error))?;
    let marker = output.join("plan.json");
    let inputs = [plan, prepared, manifest];
    if inputs.iter().any(|input| same_path(input, &marker)) {
        return Err(OutputInputCollision::new().into());
    }
    // The completion marker belongs to this invocation from its first fallible input read.
    // A failed standalone rerun must not leave an earlier resolved plan looking current.
    remove_marker(&marker)?;
    guard_output_inputs(&output, &inputs)?;
    let prepared: Prepared = read_json(prepared)?;
    prepared.inputs.verify(manifest, None)?;
    let plan: PlanFile = read_json(plan)?;
    plan.validate_schema()?;
    let prospective = Prospective::new(&output, &prepared.inputs)?;
    let initial = classify(&prospective.manifest, Some(&prepared.inputs.base), verbose)?;
    let mut resolved = resolve_plan(
        &plan,
        &initial.work_tree.groups,
        &initial.work_tree.target_versions(),
        verbose,
    )?;
    require_semantic_decisions(&initial, &resolved)?;

    // Each pass must either add a version consequence or change the resolved artifact.
    // Remember actual states, rather than imposing an arbitrary iteration deadline.
    let mut visited = BTreeSet::new();
    let mut previous_files = Vec::new();
    loop {
        let (work_tree, _) = load_tracked_work_tree(&prospective.manifest)?;
        for edit in compute_edits(&work_tree, &resolved, verbose)? {
            if edit.original != edit.updated {
                fs::write(&edit.path, edit.updated)
                    .map_err(|error| WriteFileError::caused_by(&edit.path, error))?;
            }
        }
        prospective.resolve(verbose)?;
        let classification = classify(&prospective.manifest, Some(&prepared.inputs.base), verbose)?;
        let files = prospective.artifacts(&prepared.inputs)?;
        let mut expanded = resolved.clone();
        add_consequences(&classification, &mut expanded)?;
        if expanded == resolved && files == previous_files {
            let (passed, message, _) = run_check(
                Some(&prepared.inputs.base),
                &prospective.manifest,
                CheckFormat::Text,
                false,
                verbose,
            )?;
            require_complete_preview(passed, message)?;
            prepared.inputs.verify(manifest, None)?;
            let final_digest = prepared.inputs.final_digest(&files)?;
            let evidence_manifest_path = prospective.retain(&output, prepared.inputs.root())?;
            prepared
                .inputs
                .verify_candidate(&evidence_manifest_path, &final_digest)?;
            let mut plan = explicit_plan(&resolved);
            plan.resolved = Some(ResolvedState {
                final_digest,
                versions: resolved
                    .packages
                    .iter()
                    .map(|(name, version)| (name.clone(), version.to_string()))
                    .collect(),
                inputs: prepared.inputs,
                files,
                evidence_manifest_path,
            });
            write_report(&output, &classification)?;
            write_json(&output.join("plan.json"), &plan)?;
            return Ok(format!(
                "Wrote complete resolved plan to {}",
                output.join("plan.json").display()
            ));
        }
        record_state(&mut visited, &expanded, &files, &prospective.root)?;
        previous_files = files;
        resolved = expanded;
    }
}

fn validate_preparation_files(
    root: &Path,
    lockfile: &Path,
    files: &[Artifact],
) -> Result<(), AppError> {
    if files.iter().any(|file| root.join(&file.path) != lockfile) {
        return Err(InvalidPreparation::new().into());
    }
    Ok(())
}

fn require_complete_preview(passed: bool, diagnostics: String) -> Result<(), AppError> {
    if !passed {
        return Err(IncompletePreview::new(diagnostics).into());
    }
    Ok(())
}

fn record_state(
    visited: &mut BTreeSet<String>,
    resolved: &ResolvedVersions,
    files: &[Artifact],
    root: &Path,
) -> Result<(), AppError> {
    let state = serde_json::to_vec(&(explicit_plan(resolved), files))
        .expect("version plans and artifacts contain only JSON-compatible data");
    // Keep a bounded digest per iteration, not another retained copy of every resolved file.
    // Hash the complete state so a version, path, or content change cannot look like a cycle.
    if !visited.insert(hash_bytes(&state, root)?) {
        return Err(ResolutionCycle::new().into());
    }
    Ok(())
}

fn require_semantic_decisions(
    initial: &Classification,
    resolved: &ResolvedVersions,
) -> Result<(), AppError> {
    for package in &initial.packages {
        if package.status() != PackageStatus::NeedsIncrement
            || package
                .changed()
                .iter()
                .all(|change| matches!(change, ChangedItem::Lockfile { .. }))
        {
            continue;
        }
        if !resolved.packages.get(&package.name).is_some_and(|version| {
            package
                .anchor()
                .is_some_and(|anchor| version > &anchor.version)
        }) {
            return Err(SemanticDecisionRequired::new(&package.name).into());
        }
    }
    Ok(())
}

fn add_consequences(
    classification: &Classification,
    resolved: &mut ResolvedVersions,
) -> Result<(), AppError> {
    let versions = classification.work_tree.target_versions();
    for (name, group) in &classification.groups {
        if !group.is_consistent() {
            raise(classification, resolved, name, group.version());
        }
    }
    for package in &classification.packages {
        if package.status() == PackageStatus::NeedsIncrement {
            let anchor = package.anchor().expect("needs-increment has an anchor");
            raise(
                classification,
                resolved,
                &package.name,
                &increment_version(&anchor.version, IncrementLevel::Patch)?,
            );
        }
        for dependency in &package.dependencies {
            let Some(version) = versions.get(&dependency.name) else {
                continue;
            };
            if !requirement_names_version(&dependency.req, version) {
                // Explicitly retaining the target version also schedules its requirement rewrites.
                raise(classification, resolved, &dependency.name, version);
            }
            if !dependency.public || releases_breaking_change(package) {
                continue;
            }
            let Some(anchor) = package.anchor() else {
                continue;
            };
            if classification
                .packages
                .iter()
                .any(|target| target.name == dependency.name && releases_breaking_change(target))
            {
                let level = if anchor.version.major == 0 {
                    IncrementLevel::Minor
                } else {
                    IncrementLevel::Major
                };
                raise(
                    classification,
                    resolved,
                    &package.name,
                    &increment_version(&anchor.version, level)?,
                );
            }
        }
    }
    for dependency in &classification.work_tree.exact_dependencies {
        if let Some(version) = versions.get(&dependency.target)
            && !requirement_names_version(&dependency.requirement, version)
        {
            raise(classification, resolved, &dependency.target, version);
        }
    }
    Ok(())
}

fn raise(
    classification: &Classification,
    resolved: &mut ResolvedVersions,
    target: &str,
    minimum: &Version,
) {
    let groups = &classification.work_tree.groups;
    let group = groups.group_of(target).unwrap_or(target);
    let mut members = groups.members(group).to_vec();
    if members.is_empty() {
        members.push(target.to_owned());
    }
    let versions = classification.work_tree.target_versions();
    let version = members
        .iter()
        .filter_map(|name| resolved.packages.get(name).or_else(|| versions.get(name)))
        .chain([minimum])
        .max()
        .expect("the minimum version always participates")
        .clone();
    for member in members {
        resolved.packages.insert(member, version.clone());
    }
}

fn explicit_plan(resolved: &ResolvedVersions) -> PlanFile {
    PlanFile::new(
        PlanStage::Expanded,
        resolved
            .packages
            .iter()
            .map(|(name, version)| PlanIncrement {
                name: name.clone(),
                level: None,
                version: Some(version.to_string()),
            })
            .collect(),
    )
}

fn remove_marker(path: &Path) -> Result<(), AppError> {
    if path.exists() {
        fs::remove_file(path).map_err(|error| WriteFileError::caused_by(path, error))?;
    }
    Ok(())
}

fn same_path(left: &Path, right: &Path) -> bool {
    match (fs::canonicalize(left), fs::canonicalize(right)) {
        (Ok(left), Ok(right)) => left == right,
        _ => absolute(left)
            .ok()
            .zip(absolute(right).ok())
            .is_some_and(|(left, right)| left == right),
    }
}

fn guard_output_inputs(output: &Path, inputs: &[&Path]) -> Result<(), AppError> {
    let output = fs::canonicalize(output).unwrap_or_else(|_| output.to_path_buf());
    for input in inputs {
        if ["report.json", "report.json.tmp"]
            .iter()
            .any(|name| same_path(input, &output.join(name)))
        {
            return Err(OutputInputCollision::new().into());
        }
        let input = fs::canonicalize(input)
            .or_else(|_| absolute(input))
            .map_err(|error| WriteFileError::caused_by(input, error))?;
        for directory in ["diffs", "workspace", ".prospective"] {
            if input.starts_with(output.join(directory)) {
                return Err(OutputInputCollision::new().into());
            }
        }
    }
    Ok(())
}

/// Completion artifacts must not replace the invocation's own input documents.
#[ohno::error]
#[display("preview output overlaps an input; choose a separate output location")]
struct OutputInputCollision;

/// Source-level release grading belongs to the caller, not the resolver.
#[ohno::error]
#[display("package {package} needs a semantic release decision in the proposed plan")]
struct SemanticDecisionRequired {
    package: String,
}

/// Prepared bytes must remain the state captured before grading.
#[ohno::error]
#[display("prepared resolution artifacts changed; prepare and assess the report again")]
struct InvalidPreparation;

/// Predictable expansion must satisfy the same final gate as the live workspace.
#[ohno::error]
#[display("resolved preview does not pass the release gate: {diagnostics}")]
struct IncompletePreview {
    diagnostics: String,
}

/// An offline resolver must converge before producing an applicable artifact.
#[ohno::error]
#[display("offline release preview repeated a non-final state")]
struct ResolutionCycle;

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::collections::BTreeMap;

    use tempfile::tempdir;

    use super::*;

    #[test]
    fn preparation_accepts_only_the_workspace_lockfile() {
        let root = Path::new("repository");
        let lockfile = root.join("workspace/Cargo.lock");
        validate_preparation_files(root, &lockfile, &[]).unwrap();
        let mut files = [Artifact {
            path: "workspace/Cargo.lock".into(),
            contents: String::new(),
        }];
        validate_preparation_files(root, &lockfile, &files).unwrap();
        files[0].path = "workspace/Cargo.toml".into();
        let error = validate_preparation_files(root, &lockfile, &files).unwrap_err();
        assert!(error.find_source::<InvalidPreparation>().is_some());
    }

    #[test]
    fn incomplete_preview_preserves_the_release_gate_diagnostics() {
        require_complete_preview(true, String::new()).unwrap();
        let diagnostics = "package requires an increment";
        let error = require_complete_preview(false, diagnostics.to_owned()).unwrap_err();
        assert_eq!(
            error
                .find_source::<IncompletePreview>()
                .unwrap()
                .diagnostics,
            diagnostics
        );
    }

    #[test]
    #[cfg_attr(miri, ignore = "uses Git to hash convergence states")]
    fn history_detects_repetition_without_retaining_artifact_contents() {
        let directory = tempdir().unwrap();
        let mut visited = BTreeSet::new();
        let mut resolved = ResolvedVersions {
            packages: BTreeMap::from([("tool".to_owned(), Version::new(1, 0, 0))]),
        };
        let mut files = [Artifact {
            path: "Cargo.lock".into(),
            contents: "initial resolution".to_owned(),
        }];
        record_state(&mut visited, &resolved, &files, directory.path()).unwrap();
        let key_length = visited.first().unwrap().len();
        let error = record_state(&mut visited, &resolved, &files, directory.path()).unwrap_err();
        assert!(error.find_source::<ResolutionCycle>().is_some());
        assert_eq!(visited.len(), 1);

        resolved
            .packages
            .insert("tool".to_owned(), Version::new(1, 0, 1));
        record_state(&mut visited, &resolved, &files, directory.path()).unwrap();
        files[0].path = "Cargo.toml".into();
        record_state(&mut visited, &resolved, &files, directory.path()).unwrap();
        // Larger content establishes that retained key size is independent of artifact size.
        files[0].contents = "resolved dependency\n".repeat(1024);
        record_state(&mut visited, &resolved, &files, directory.path()).unwrap();
        assert_eq!(visited.len(), 4);
        assert!(visited.iter().all(|key| key.len() == key_length));
    }
}
