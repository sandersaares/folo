// Artifact-only proposal generation for the increment-versions workflow.
//
// Semantic judgement remains in the decision file. This module settles the version and
// requirement consequences using the same plan resolver as expansion and application.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

use ohno::AppError;
use semver::Version;
use serde::Serialize;

use crate::WriteFileError;
use crate::artifact_path::same_path;
use crate::check::compatibility_key;
use crate::classify::PackageStatus;
use crate::groups::Groups;
use crate::plan::{PlanFile, PlanIncrement, PlanStage, ResolvedVersions, resolve_plan};
use crate::preview::remove_marker;
use crate::propose::decision::{ChangeLevel, Decisions};
use crate::report::{ReportFile, ReportPackage, read_report};
use crate::resolved::write_json;
use crate::text::{Quotable as _, quote_path};
use crate::verbose::Verbose;

pub(crate) fn run_propose(
    report: &Path,
    decisions: &Path,
    out: &Path,
    verbose: Verbose,
) -> Result<String, AppError> {
    let report = if report.is_dir() {
        report.join("report.json")
    } else {
        report.to_path_buf()
    };
    // An invalid rerun invalidates its previous proposal, but never its own source evidence.
    // Canonical comparison also protects inputs addressed through a symlink or a relative path.
    for input in [&report, decisions] {
        if same_path(input, out)? {
            return Err(ProposalInputCollision::new().into());
        }
    }
    remove_marker(out)?;
    let report = read_report(&report)?;
    let decisions = Decisions::read(decisions)?;
    let plan = Proposal::new(&report).generate(&decisions, verbose)?;
    if let Some(parent) = out.parent().filter(|parent| !parent.as_os_str().is_empty()) {
        fs::create_dir_all(parent).map_err(|error| WriteFileError::caused_by(parent, error))?;
    }
    // Generation has no fallible steps after publication. A write failure must not leave a
    // partially written plan that another workflow step could mistake for a completed proposal.
    if let Err(error) = write_json(out, &plan) {
        remove_marker(out)?;
        return Err(error);
    }
    Ok(format!(
        "Wrote cargo-release-plan input to {}",
        quote_path(&out.display().to_string())
    ))
}

/// Validated report evidence indexed for repeated, entirely in-memory resolution.
///
/// Release assessments exclude helpers, while version targets and group maxima include them.
/// Ref: docs/design.md, "Plan from captured evidence" and "Version groups".
pub(crate) struct Proposal<'a> {
    pub(crate) report: &'a ReportFile,
    pub(crate) packages: BTreeMap<&'a str, &'a ReportPackage>,
    pub(crate) anchors: BTreeMap<&'a str, Version>,
    versions: BTreeMap<String, Version>,
    pub(crate) groups: Groups,
    highest: BTreeMap<String, Version>,
}

impl<'a> Proposal<'a> {
    pub(crate) fn new(report: &'a ReportFile) -> Self {
        let versions = report.version_targets();
        let highest = report
            .groups
            .iter()
            .map(|(name, group)| {
                let highest = group
                    .members
                    .iter()
                    .map(|member| {
                        versions
                            .get(member)
                            .expect("validated group members are tracked version targets")
                    })
                    .max()
                    .expect("validated report groups contain at least two known members");
                (name.clone(), highest.clone())
            })
            .collect();
        Self {
            report,
            packages: report
                .packages
                .iter()
                .map(|package| (package.name.as_str(), package))
                .collect(),
            anchors: report
                .packages
                .iter()
                .filter_map(|package| {
                    package.anchor.as_ref().map(|anchor| {
                        (
                            package.name.as_str(),
                            anchor
                                .version
                                .parse()
                                .expect("the report reader validates every anchor version"),
                        )
                    })
                })
                .collect(),
            versions,
            groups: report.version_groups(),
            highest,
        }
    }

    pub(crate) fn generate(
        &self,
        decisions: &Decisions,
        verbose: Verbose,
    ) -> Result<PlanFile, AppError> {
        let decisions = decisions.validate()?;
        // Validate user decisions even if group movement would otherwise hide an invalid entry.
        _ = self.decision_increments(&decisions, Verbose::new(false))?;
        let mut levels = BTreeMap::new();
        let mut alignment = BTreeMap::new();
        let mut visited = BTreeSet::new();
        loop {
            record_state(&mut visited, &(&levels, &alignment))?;
            let fresh_levels = self.public_levels(&decisions, &alignment, verbose)?;
            let increments = self.decision_increments(&fresh_levels, Verbose::new(false))?;
            let fresh_alignment = self.align_groups(&increments, verbose)?;
            if levels == fresh_levels && alignment == fresh_alignment {
                break;
            }
            levels = fresh_levels;
            alignment = fresh_alignment;
        }
        let increments = self.decision_increments(&levels, verbose)?;
        let increments = self.combine(increments, &alignment);
        let plan = PlanFile::new(PlanStage::Proposed, increments);
        let resolved = self.resolve(&plan)?;
        self.validate_result(&resolved, &levels)?;
        Ok(plan)
    }

    fn resolve(&self, plan: &PlanFile) -> Result<ResolvedVersions, AppError> {
        resolve_plan(plan, &self.groups, &self.versions, Verbose::new(false))
    }

    pub(crate) fn decision_key<'n>(&'n self, name: &'n str) -> &'n str {
        self.groups.group_of(name).unwrap_or(name)
    }

    pub(crate) fn declared(&self, name: &str) -> &Version {
        self.versions
            .get(name)
            .expect("proposal names come from validated report targets or resolved plan targets")
    }

    pub(crate) fn highest(&self, group: &str) -> &Version {
        self.highest
            .get(group)
            .expect("alignment only addresses validated report groups")
    }

    fn combine(
        &self,
        mut increments: Vec<PlanIncrement>,
        alignment: &BTreeMap<String, PlanIncrement>,
    ) -> Vec<PlanIncrement> {
        let planned: BTreeSet<String> = increments
            .iter()
            .map(|entry| self.decision_key(&entry.name).to_owned())
            .collect();
        // A fresh semantic level supersedes an earlier exact alignment for the same group.
        // Passing both to resolve_plan would conflict; a patch alignment cannot exceed a level.
        increments.extend(
            alignment
                .iter()
                .filter(|(key, _)| !planned.contains(*key))
                .map(|(_, entry)| entry.clone()),
        );
        increments.sort_by(|left, right| left.name.cmp(&right.name));
        increments
    }

    fn predicted_versions(
        &self,
        levels: &BTreeMap<String, ChangeLevel>,
        alignment: &BTreeMap<String, PlanIncrement>,
    ) -> Result<BTreeMap<String, Version>, AppError> {
        let increments = self.decision_increments(levels, Verbose::new(false))?;
        let plan = PlanFile::new(PlanStage::Proposed, self.combine(increments, alignment));
        let mut versions = self.resolve(&plan)?.packages;
        for (name, declared) in &self.versions {
            // Even before alignment has been selected, every group must eventually reach its
            // highest member. This lets public propagation see breaking laggard realignment.
            versions.entry(name.clone()).or_insert_with(|| {
                self.highest
                    .get(self.decision_key(name))
                    .unwrap_or(declared)
                    .clone()
            });
        }
        Ok(versions)
    }

    fn public_levels(
        &self,
        decisions: &BTreeMap<String, ChangeLevel>,
        alignment: &BTreeMap<String, PlanIncrement>,
        verbose: Verbose,
    ) -> Result<BTreeMap<String, ChangeLevel>, AppError> {
        let mut levels = decisions.clone();
        let mut visited = BTreeSet::new();
        loop {
            record_state(&mut visited, &levels)?;
            let mut versions = self.predicted_versions(&levels, alignment)?;
            let mut changed = false;
            for (name, package) in &self.packages {
                if !self.anchors.contains_key(name)
                    || self.breaks(name, &versions)
                    || levels.get(*name) == Some(&ChangeLevel::Breaking)
                {
                    continue;
                }
                if let Some(dependency) = package
                    .dependencies
                    .iter()
                    .filter(|dependency| dependency.public)
                    .filter(|dependency| self.packages.contains_key(dependency.name.as_str()))
                    .find(|dependency| self.breaks(&dependency.name, &versions))
                {
                    verbose.note(|| {
                        format!(
                            "Package {} is raised to change level 'breaking' because its public \
                             API exposes {}, whose resolved version {} is incompatible with \
                             anchor {}. Exposed dependency types are part of the consumer contract.",
                            quote_path(name),
                            quote_path(&dependency.name),
                            versions
                                .get(&dependency.name)
                                .expect("predicted versions include every tracked dependency"),
                            self.anchors
                                .get(dependency.name.as_str())
                                .expect("breaking dependency releases have a published anchor")
                        )
                    });
                    levels.insert((*name).to_owned(), ChangeLevel::Breaking);
                    // A sibling may now move through the same group. Resolve before inspecting
                    // the next package so that it does not receive a redundant semantic entry.
                    versions = self.predicted_versions(&levels, alignment)?;
                    changed = true;
                }
            }
            if !changed {
                return Ok(levels);
            }
        }
    }

    fn breaks(&self, name: &str, versions: &BTreeMap<String, Version>) -> bool {
        self.anchors.get(name).is_some_and(|anchor| {
            compatibility_key(anchor)
                != compatibility_key(
                    versions
                        .get(name)
                        .expect("predicted and final versions include every tracked package"),
                )
        })
    }

    pub(crate) fn moved(
        &self,
        increments: Vec<PlanIncrement>,
    ) -> Result<BTreeSet<String>, AppError> {
        Ok(self
            .resolve(&PlanFile::new(PlanStage::Proposed, increments))?
            .packages
            .into_iter()
            .filter(|(name, version)| version != self.declared(name))
            .map(|(name, _)| name)
            .collect())
    }

    pub(crate) fn ships_published_version(&self, name: &str, moved: &BTreeSet<String>) -> bool {
        !moved.contains(name)
            && self
                .anchors
                .get(name)
                .is_some_and(|anchor| self.declared(name).cmp_precedence(anchor).is_le())
    }

    fn validate_result(
        &self,
        resolved: &ResolvedVersions,
        levels: &BTreeMap<String, ChangeLevel>,
    ) -> Result<(), AppError> {
        let mut versions = self.versions.clone();
        versions.extend(resolved.packages.clone());
        let moved: BTreeSet<String> = versions
            .iter()
            .filter(|(name, version)| *version != self.declared(name))
            .map(|(name, _)| name.clone())
            .collect();
        let missing: Vec<String> = self
            .packages
            .iter()
            .filter(|(name, package)| {
                package.status == PackageStatus::NeedsIncrement && !moved.contains(**name)
            })
            .map(|(name, _)| (*name).to_owned())
            .collect();
        if !missing.is_empty() {
            return Err(MissingIncrement::new(missing).into());
        }
        let stranded: Vec<String> = self
            .packages
            .iter()
            .filter(|(name, package)| {
                self.ships_published_version(name, &moved)
                    && package
                        .dependencies
                        .iter()
                        .any(|dependency| moved.contains(&dependency.name))
            })
            .map(|(name, _)| (*name).to_owned())
            .collect();
        if !stranded.is_empty() {
            return Err(RewrittenPublishedPackage::new(stranded).into());
        }
        for (name, group) in &self.report.groups {
            let first = group
                .members
                .first()
                .expect("validated version groups contain at least two members");
            let target = versions
                .get(first)
                .expect("final versions include every tracked group member");
            if !target.pre.is_empty()
                || !target.build.is_empty()
                || group
                    .members
                    .iter()
                    .any(|member| versions.get(member) != Some(target))
            {
                return Err(UnsettledGroup::new(name).into());
            }
        }
        for (name, level) in levels {
            let anchor = self
                .anchors
                .get(name.as_str())
                .expect("semantic decisions are validated against published anchors");
            let minimum = level.minimum(anchor)?;
            let version = versions
                .get(name)
                .expect("semantic decisions only name tracked packages");
            if version.cmp_precedence(&minimum).is_lt() {
                return Err(InsufficientIncrement::new(name, minimum).into());
            }
        }
        for (name, package) in &self.packages {
            if self.anchors.contains_key(name)
                && !self.breaks(name, &versions)
                && package.dependencies.iter().any(|dependency| {
                    dependency.public
                        && self.packages.contains_key(dependency.name.as_str())
                        && self.breaks(&dependency.name, &versions)
                })
            {
                return Err(UnpropagatedPublicDependency::new(*name).into());
            }
        }
        Ok(())
    }
}

pub(crate) fn record_state(
    visited: &mut BTreeSet<String>,
    state: &impl Serialize,
) -> Result<(), AppError> {
    let state = serde_json::to_string(state)
        .expect("proposal states contain only string-keyed maps and JSON-compatible decisions");
    // Remember actual decisions, not pass counts: a repeated non-final state cannot make
    // progress, regardless of package ordering or the number of groups.
    if !visited.insert(state) {
        return Err(ProposalCycle::new().into());
    }
    Ok(())
}

/// Input evidence must survive even when an invocation has an invalid output location.
#[ohno::error]
#[display("proposal output overlaps an input; choose a separate output location")]
struct ProposalInputCollision;

/// Repeating a non-final state means semantic and mechanical consequences cannot settle.
#[ohno::error]
#[display("release proposal repeated a non-final decision state")]
struct ProposalCycle;

/// Changed released content must be paired with an effective version movement.
#[ohno::error]
#[display("packages need an increment without one: {}. Decide their change levels", packages.join(", ").quoted())]
struct MissingIncrement {
    packages: Vec<String>,
}

/// Requirement rewrites cannot change the contents of an already-published version.
#[ohno::error]
#[display("the plan rewrites published packages that keep their versions: {}. Decide their change levels", packages.join(", ").quoted())]
struct RewrittenPublishedPackage {
    packages: Vec<String>,
}

/// A proposal must end every version group on one plain version.
#[ohno::error]
#[display("version group {} did not settle on one plain version", group.quoted())]
struct UnsettledGroup {
    group: String,
}

/// A malformed report must not yield a mechanical bump below a decided minimum.
#[ohno::error]
#[display("package {} does not reach its decided minimum version {minimum}", package.quoted())]
struct InsufficientIncrement {
    package: String,
    minimum: Version,
}

/// Finished proposals obey the release gate's public dependency compatibility requirement.
#[ohno::error]
#[display("package {} does not release the breaking change required by its public dependency", package.quoted())]
struct UnpropagatedPublicDependency {
    package: String,
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use serde_json::json;
    use tempfile::tempdir_in;

    use super::*;
    use crate::propose::tests::{
        assert_versions, depends, entries, generate, needs, package, report,
    };

    #[test]
    #[cfg_attr(
        miri,
        ignore = "resolves missing output directories around real input artifacts"
    )]
    fn missing_output_parents_cannot_make_an_input_writable_as_a_proposal() {
        let directory = tempdir_in(".").unwrap();
        let report_path = directory.path().join("report.json");
        let decisions_path = directory.path().join("decisions.json");
        write_json(&report_path, &report(Vec::new(), Vec::new(), &[])).unwrap();
        fs::write(&decisions_path, r#"{"schema_version":1,"changes":[]}"#).unwrap();
        for input in [&report_path, &decisions_path] {
            let before = fs::read(input).unwrap();
            let output = directory
                .path()
                .join("missing")
                .join("..")
                .join(input.file_name().unwrap());
            let error = run_propose(&report_path, &decisions_path, &output, Verbose::new(false))
                .unwrap_err();
            assert!(error.find_source::<ProposalInputCollision>().is_some());
            assert_eq!(fs::read(input).unwrap(), before);
            assert!(!directory.path().join("missing").exists());
        }
    }

    #[test]
    fn verbose_public_propagation_explains_the_dependency_anchor() {
        let report = report(
            vec![
                depends(package("app", "1.0.0", Some("1.0.0")), "lib", true),
                package("lib", "2.0.0", Some("1.0.0")),
            ],
            vec![],
            &[],
        );
        report.validate().unwrap();
        let levels = Proposal::new(&report)
            .public_levels(&BTreeMap::new(), &BTreeMap::new(), Verbose::new(true))
            .unwrap();
        assert_eq!(
            levels,
            BTreeMap::from([("app".to_owned(), ChangeLevel::Breaking)])
        );
    }

    #[test]
    #[cfg_attr(
        miri,
        ignore = "reads proposal inputs and attempts a real filesystem write"
    )]
    fn output_write_failure_is_returned_without_a_completed_proposal() {
        let directory = tempdir_in(".").unwrap();
        let report_path = directory.path().join("report.json");
        let decisions_path = directory.path().join("decisions.json");
        // A NUL cannot form a filesystem filename on either supported platform. This reaches
        // the write-failure cleanup without permission assumptions or a concurrent mutation.
        let output = directory.path().join("invalid\0.json");
        write_json(&report_path, &report(vec![], vec![], &[])).unwrap();
        fs::write(&decisions_path, r#"{"schema_version":1,"changes":[]}"#).unwrap();
        let error =
            run_propose(&report_path, &decisions_path, &output, Verbose::new(false)).unwrap_err();
        assert!(error.find_source::<WriteFileError>().is_some());
        assert!(!output.exists());
        assert!(report_path.is_file());
        assert!(decisions_path.is_file());
    }

    #[test]
    fn a_propagated_semantic_level_supersedes_an_earlier_exact_group_alignment() {
        let report = report(
            vec![
                depends(package("app", "2.0.0", Some("2.0.0")), "lib", true),
                package("app_impl", "1.0.0", Some("1.0.0")),
                package("lib", "0.0.6", Some("0.0.5")),
            ],
            vec![],
            &[&["app", "app_impl"]],
        );
        report.validate().unwrap();
        let proposal = Proposal::new(&report);
        // Capture the transitional state directly: a dependency now breaks, but the dependent
        // group still carries the exact alignment chosen before that break was discovered.
        let alignment = BTreeMap::from([(
            "app".to_owned(),
            PlanIncrement {
                name: "app".to_owned(),
                level: None,
                version: Some("2.0.0".to_owned()),
            },
        )]);
        let levels = proposal
            .public_levels(&BTreeMap::new(), &alignment, Verbose::new(false))
            .unwrap();
        let increments = proposal
            .decision_increments(&levels, Verbose::new(false))
            .unwrap();
        let plan = PlanFile::new(
            PlanStage::Proposed,
            proposal.combine(increments, &alignment),
        );
        assert_eq!(entries(&plan), json!([{"name": "app", "level": "major"}]));
        assert_versions(
            &report,
            &plan,
            &json!({"app": "3.0.0", "app_impl": "3.0.0", "lib": "0.0.6"}),
        );
    }

    #[test]
    fn changed_and_rewritten_packages_cannot_be_left_unmoved() {
        for changed in [false, true] {
            let mut first = depends(
                package("first", "1.0.0", Some("1.0.0")),
                "dependency",
                false,
            );
            let mut second = depends(
                package("second", "1.0.0", Some("1.0.0")),
                "dependency",
                false,
            );
            if changed {
                first = needs(first);
                second = needs(second);
            }
            let report = report(
                vec![package("dependency", "1.0.0", Some("1.0.0")), first, second],
                vec![],
                &[],
            );
            let error = generate(&report, &[("dependency", "patch")]).unwrap_err();
            let names = if changed {
                &error.find_source::<MissingIncrement>().unwrap().packages
            } else {
                &error
                    .find_source::<RewrittenPublishedPackage>()
                    .unwrap()
                    .packages
            };
            assert_eq!(names, &["first", "second"]);
        }
    }

    #[test]
    fn repeated_state_detection_compares_names_and_values_not_counts() {
        let mut visited = BTreeSet::new();
        for state in [
            BTreeMap::from([("alpha", "patch"), ("beta", "breaking")]),
            BTreeMap::from([("gamma", "patch"), ("beta", "breaking")]),
            BTreeMap::from([("alpha", "breaking"), ("beta", "breaking")]),
            BTreeMap::from([("alpha", "patch")]),
        ] {
            record_state(&mut visited, &state).unwrap();
        }
        let reordered = BTreeMap::from([("beta", "breaking"), ("alpha", "patch")]);
        let error = record_state(&mut visited, &reordered).unwrap_err();
        assert!(error.find_source::<ProposalCycle>().is_some());
    }

    #[test]
    fn final_validation_rejects_an_unsettled_group_or_public_contract() {
        let report = report(
            vec![
                package("lib", "1.0.0", Some("1.0.0")),
                package("lib_impl", "2.0.0", Some("2.0.0")),
                depends(package("app", "1.0.0", Some("1.0.0")), "lib_impl", true),
            ],
            vec![],
            &[&["lib", "lib_impl"]],
        );
        let proposal = Proposal::new(&report);
        let mut resolved = ResolvedVersions {
            packages: BTreeMap::new(),
        };
        let error = proposal
            .validate_result(&resolved, &BTreeMap::new())
            .unwrap_err();
        assert!(error.find_source::<UnsettledGroup>().is_some());
        resolved
            .packages
            .insert("lib".to_owned(), Version::new(3, 0, 0));
        resolved
            .packages
            .insert("lib_impl".to_owned(), Version::new(3, 0, 0));
        resolved
            .packages
            .insert("app".to_owned(), Version::new(1, 0, 1));
        let error = proposal
            .validate_result(&resolved, &BTreeMap::new())
            .unwrap_err();
        assert!(
            error
                .find_source::<UnpropagatedPublicDependency>()
                .is_some()
        );
    }

    #[test]
    fn insufficient_decisions_from_regressed_evidence_fail_closed() {
        let report = report(vec![package("lib", "1.0.0", Some("3.0.0"))], vec![], &[]);
        let error = generate(&report, &[("lib", "breaking")]).unwrap_err();
        assert!(error.find_source::<InsufficientIncrement>().is_some());
    }

    #[test]
    #[cfg_attr(miri, ignore = "reads and writes real proposal artifacts")]
    fn failed_reruns_remove_stale_proposals_and_preserve_inputs() {
        let directory = tempdir_in(".").unwrap();
        let report_path = directory.path().join("report.json");
        let decisions_path = directory.path().join("decisions.json");
        let output = directory.path().join("output").join("plan.json");
        let report = report(
            vec![package("library", "1.0.0", Some("1.0.0"))],
            vec![],
            &[],
        );
        write_json(&report_path, &report).unwrap();
        fs::write(
            &decisions_path,
            r#"{"schema_version":1,"changes":[{"name":"library","level":"patch"}]}"#,
        )
        .unwrap();
        run_propose(
            directory.path(),
            &decisions_path,
            &output,
            Verbose::new(false),
        )
        .unwrap();
        let written: PlanFile = serde_json::from_slice(&fs::read(&output).unwrap()).unwrap();
        assert_eq!(
            entries(&written),
            json!([{"name": "library", "level": "patch"}])
        );
        fs::write(&decisions_path, "null").unwrap();
        _ = run_propose(&report_path, &decisions_path, &output, Verbose::new(false)).unwrap_err();
        assert!(!output.exists());
        let before = fs::read(&report_path).unwrap();
        let error = run_propose(
            directory.path(),
            &decisions_path,
            &report_path,
            Verbose::new(false),
        )
        .unwrap_err();
        assert!(error.find_source::<ProposalInputCollision>().is_some());
        assert_eq!(fs::read(&report_path).unwrap(), before);
        let error = run_propose(
            &report_path,
            &decisions_path,
            &decisions_path,
            Verbose::new(false),
        )
        .unwrap_err();
        assert!(error.find_source::<ProposalInputCollision>().is_some());
        assert_eq!(fs::read_to_string(&decisions_path).unwrap(), "null");
    }
}
