// Expanded-plan inspection supplies typed facts to publication and evidence callers.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use ohno::AppError;
use serde::Serialize;

use crate::metadata::load_tracked_work_tree;
use crate::plan::{PlanFile, PlanStage, resolve_plan};
use crate::resolved::{apply_resolved, read_json};
use crate::verbose::Verbose;

/// Publication eligibility comes from tracked Cargo members, not package naming.
#[derive(Serialize)]
struct PlanInspection {
    publication_targets: Vec<String>,
    evidence_manifest_path: Option<PathBuf>,
}

pub(crate) fn run_inspect_plan(
    path: &Path,
    require_resolved: bool,
    manifest: &Path,
    verbose: Verbose,
) -> Result<String, AppError> {
    let plan: PlanFile = read_json(path)?;
    validate_expanded(&plan)?;
    if require_resolved || plan.resolved.is_some() {
        // Reuse application's captured-state validation without installing any files.
        // The registry probe must see the same target set that application will accept.
        _ = apply_resolved(&plan, manifest, true, verbose)?;
    }
    if let Some(state) = &plan.resolved {
        // Callers may run compatibility tooling immediately after consuming this path.
        state.verify_candidate(&state.evidence_manifest_path)?;
    }
    let (work_tree, _) = load_tracked_work_tree(manifest)?;
    let resolved = resolve_plan(
        &plan,
        &work_tree.groups,
        &work_tree.target_versions(),
        verbose,
    )?;
    let publication_targets = work_tree
        .version_targets
        .iter()
        .filter(|target| target.publishable && resolved.packages.contains_key(&target.name))
        .map(|target| target.name.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let inspection = PlanInspection {
        publication_targets,
        evidence_manifest_path: plan.resolved.map(|state| state.evidence_manifest_path),
    };
    Ok(serde_json::to_string(&inspection)
        .expect("plan inspection contains only JSON-compatible artifact fields"))
}

fn validate_expanded(plan: &PlanFile) -> Result<(), AppError> {
    plan.validate_schema()?;
    let mut names = BTreeSet::new();
    if plan.stage() != PlanStage::Expanded
        || plan.increments.iter().any(|increment| {
            increment.version.is_none()
                || increment.level.is_some()
                || !names.insert(&increment.name)
        })
    {
        return Err(ExpandedPlanRequired::new().into());
    }
    Ok(())
}

/// Inspection requires the complete, uniquely named package/version set.
#[ohno::error]
#[display("inspection requires an expanded plan with one explicit version per package")]
struct ExpandedPlanRequired;

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;
    use crate::plan::PlanIncrement;

    #[test]
    fn only_complete_explicit_unique_expansions_are_inspectable() {
        let increment = PlanIncrement {
            name: "api".to_owned(),
            level: None,
            version: Some("1.0.1".to_owned()),
        };
        let plan = PlanFile::new(PlanStage::Expanded, vec![increment.clone()]);
        validate_expanded(&plan).unwrap();
        for plan in [
            PlanFile::new(PlanStage::Proposed, vec![increment.clone()]),
            PlanFile::new(
                PlanStage::Expanded,
                vec![increment.clone(), increment.clone()],
            ),
            PlanFile::new(
                PlanStage::Expanded,
                vec![PlanIncrement {
                    level: Some("patch".to_owned()),
                    ..increment.clone()
                }],
            ),
            PlanFile::new(
                PlanStage::Expanded,
                vec![PlanIncrement {
                    version: None,
                    ..increment
                }],
            ),
        ] {
            assert!(
                validate_expanded(&plan)
                    .unwrap_err()
                    .find_source::<ExpandedPlanRequired>()
                    .is_some()
            );
        }
    }
}
