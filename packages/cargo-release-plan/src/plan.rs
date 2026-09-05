// Increment-plan parsing, validation, and group expansion.
//
// Plans name package or version-group decisions. Expansion resolves those
// decisions to explicit package versions for both preview and application.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Display, Formatter};
use std::str::FromStr;

use ohno::AppError;
use semver::Version;
use serde::Deserialize;

use crate::groups::Groups;
use crate::verbose::Verbose;
use crate::{
    ConflictingPlanIncrementKindError, ConflictingPlanVersionError, ExpandedPlanDriftError,
    InvalidVersionError, PlanIncrementSpecError, PlanVersionRegressionError,
    UnknownIncrementLevelError, UnknownPlanTargetError, UnsupportedPlanSchemaError,
    VersionOverflowError, quote_path,
};

/// Shared plan and report schema revision.
///
/// Plan and report formats advance together. Incompatible field, enum, or
/// path-layout changes increment this constant. Contract: package README
/// "Plan and report schema".
pub(crate) const SCHEMA_VERSION: u32 = 1;

/// On-disk plan file.
///
/// This is the wire shape both `expand` and `apply` read. It is deliberately one
/// shape for both planning stages, so an expansion can be applied directly, and
/// [`PlanFile::stage`] recovers which stage a given document belongs to.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub(crate) struct PlanFile {
    pub(crate) schema_version: u32,
    /// Set by `expand`, absent in a hand-written plan.
    ///
    /// Read through [`PlanFile::stage`] rather than directly, so the two stages
    /// are matched on by name instead of by a bare condition.
    #[serde(default)]
    expanded: bool,
    pub(crate) increments: Vec<PlanIncrement>,
}

impl PlanFile {
    pub(crate) fn stage(&self) -> PlanStage {
        if self.expanded {
            PlanStage::Expanded
        } else {
            PlanStage::Proposed
        }
    }

    #[cfg(test)]
    pub(crate) fn new(stage: PlanStage, increments: Vec<PlanIncrement>) -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            expanded: matches!(stage, PlanStage::Expanded),
            increments,
        }
    }

    #[cfg(test)]
    pub(crate) fn with_schema_version(schema_version: u32) -> Self {
        Self {
            schema_version,
            expanded: false,
            increments: Vec::new(),
        }
    }
}

/// Which stage of planning a plan document belongs to.
///
/// The two stages carry different guarantees about the packages a document
/// names, so resolving one is not the same operation as resolving the other.
/// Approval is not a third stage: the expansion a caller approves is applied
/// byte for byte, so the reviewed document and the applied document are one.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PlanStage {
    /// A planner's input, which may name a version group or a single member of
    /// one and leave resolution to reach the rest. What it names is therefore a
    /// starting point rather than the full set of packages it moves.
    Proposed,
    /// The document `expand` writes, which names every package the plan reaches
    /// at the version each will carry. Because that set is what a caller
    /// reviews, resolving it again must reproduce it exactly.
    Expanded,
}

/// Package name → resolved version.
///
/// This is the outcome of resolving a plan of either stage, not the expanded
/// document itself: `apply` resolves a proposal to exactly this shape without
/// any expansion ever being written.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ResolvedVersions {
    pub(crate) packages: BTreeMap<String, Version>,
}

/// Resolves `plan` into the version each package it reaches will carry.
///
/// `publishable` maps every package a plan may target to the version its
/// manifest declares today. A package outside it is not a valid plan target, and
/// the highest version among a group's members in it is the increment base.
///
/// The plan's stage decides whether reaching a package the document does not
/// name is expected or is a failure.
pub(crate) fn resolve_plan(
    plan: &PlanFile,
    groups: &Groups,
    publishable: &BTreeMap<String, Version>,
    verbose: Verbose,
) -> Result<ResolvedVersions, AppError> {
    if plan.schema_version != SCHEMA_VERSION {
        return Err(UnsupportedPlanSchemaError::new(plan.schema_version).into());
    }

    let mut decisions: BTreeMap<String, IncrementSpec> = BTreeMap::new();

    for increment in &plan.increments {
        let targets = resolve_targets(&increment.name, groups, publishable)?;
        let spec = increment.spec()?;
        verbose.note(|| {
            format!(
                "{} requests {} and resolves to {}",
                quote_path(&increment.name),
                spec.describe(),
                targets.join(", ")
            )
        });
        for key in decision_keys(&targets, groups) {
            let decision = match decisions.remove(&key) {
                Some(existing) => {
                    let merged = existing.merge(spec.clone(), &key)?;
                    verbose.note(|| {
                        format!(
                            "{}: folded {} into the running decision, giving {}",
                            quote_path(&key),
                            spec.describe(),
                            merged.describe()
                        )
                    });
                    merged
                }
                None => spec.clone(),
            };
            decisions.insert(key, decision);
        }
    }

    let mut packages = BTreeMap::new();
    for (key, decision) in decisions {
        let members = members_for_key(&key, groups, publishable);
        let highest = members
            .iter()
            .filter_map(|member| publishable.get(member))
            .max()
            .cloned()
            .expect(
                "every decision key comes from a target that resolved to at least one publishable member, and every publishable member has a declared version",
            );
        let new_version = match &decision {
            IncrementSpec::Version(version) => {
                // An explicit version must not regress: a lower one would
                // re-publish a released version with different content. Equality
                // is accepted because exact realignment leaves the member that
                // already declares the highest version unchanged.
                // Ref: docs/design.md, "Version monotonicity".
                if version < &highest {
                    return Err(
                        PlanVersionRegressionError::new(&key, version.clone(), highest).into(),
                    );
                }
                version.clone()
            }
            IncrementSpec::Level(level) => increment_version(&highest, *level)?,
        };
        verbose.note(|| {
            format!(
                "{}: {} declares the highest version among {}, so {} yields {}, which every member \
                 takes",
                quote_path(&key),
                highest,
                members.join(", "),
                decision.describe(),
                new_version
            )
        });
        for member in members {
            packages.insert(member, new_version.clone());
        }
    }

    match plan.stage() {
        // Reaching a package the document does not name is what a proposal is
        // for: naming a version group, or one member of it, and letting
        // resolution find the rest is how such a plan is written.
        PlanStage::Proposed => {}
        // An expansion names every package it reaches, and that set is what a
        // caller reviewed and what the publication check ran over. Resolution
        // reads the group configuration as it stands now, so a member added to a
        // group after the document was written would otherwise be picked up
        // here, widening the reviewed set without anyone seeing it.
        PlanStage::Expanded => {
            let named: BTreeSet<&str> = plan
                .increments
                .iter()
                .map(|increment| increment.name.as_str())
                .collect();
            let unnamed: Vec<String> = packages
                .keys()
                .filter(|package| !named.contains(package.as_str()))
                .cloned()
                .collect();
            if !unnamed.is_empty() {
                return Err(ExpandedPlanDriftError::new(unnamed).into());
            }
        }
    }

    Ok(ResolvedVersions { packages })
}

/// One increment entry as stored in plan JSON.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub(crate) struct PlanIncrement {
    pub(crate) name: String,
    #[serde(default)]
    pub(crate) level: Option<String>,
    #[serde(default)]
    pub(crate) version: Option<String>,
}

impl PlanIncrement {
    fn spec(&self) -> Result<IncrementSpec, AppError> {
        match (&self.level, &self.version) {
            (Some(level), None) => {
                let level = IncrementLevel::from_str(level)
                    .map_err(|()| UnknownIncrementLevelError::new(&self.name, level))?;
                Ok(IncrementSpec::Level(level))
            }
            (None, Some(version)) => {
                let version = version
                    .parse::<Version>()
                    .map_err(|error| InvalidVersionError::caused_by(&self.name, version, error))?;
                Ok(IncrementSpec::Version(version))
            }
            (None, None) | (Some(_), Some(_)) => {
                Err(PlanIncrementSpecError::new(&self.name).into())
            }
        }
    }
}

/// Requested version increment relative to the highest declared member version.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum IncrementLevel {
    Patch,
    Minor,
    Major,
}

impl FromStr for IncrementLevel {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "patch" => Ok(Self::Patch),
            "minor" => Ok(Self::Minor),
            "major" => Ok(Self::Major),
            _ => Err(()),
        }
    }
}

impl Display for IncrementLevel {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Patch => "patch",
            Self::Minor => "minor",
            Self::Major => "major",
        };
        f.write_str(name)
    }
}

/// Exactly one of `level` or `version`, parsed.
///
/// A plan entry declares one or the other, and expansion accumulates entries for
/// the same group into the same shape, so this doubles as the running decision
/// for a group or ungrouped package.
#[derive(Clone)]
enum IncrementSpec {
    Level(IncrementLevel),
    Version(Version),
}

impl IncrementSpec {
    /// Renders the decision for an explanatory note.
    fn describe(&self) -> String {
        match self {
            Self::Level(level) => format!("increment level {level}"),
            Self::Version(version) => format!("exact version {version}"),
        }
    }

    /// Folds a further plan entry for the same key into this decision.
    ///
    /// Two levels take the higher and matching explicit versions coalesce.
    /// Different explicit versions and mixed decision kinds contradict each other.
    fn merge(self, other: Self, key: &str) -> Result<Self, AppError> {
        match (self, other) {
            (Self::Level(existing), Self::Level(added)) => Ok(Self::Level(existing.max(added))),
            (Self::Version(existing), Self::Version(added)) => {
                if existing == added {
                    Ok(Self::Version(existing))
                } else {
                    Err(ConflictingPlanVersionError::new(key).into())
                }
            }
            (Self::Version(_), Self::Level(_)) | (Self::Level(_), Self::Version(_)) => {
                Err(ConflictingPlanIncrementKindError::new(key).into())
            }
        }
    }
}

fn resolve_targets(
    name: &str,
    groups: &Groups,
    publishable: &BTreeMap<String, Version>,
) -> Result<Vec<String>, AppError> {
    let group_members = groups.members(name);
    if !group_members.is_empty() {
        let targets: Vec<String> = group_members
            .iter()
            .filter(|member| publishable.contains_key(*member))
            .cloned()
            .collect();
        if targets.is_empty() {
            // Silently dropping the entry would report success while applying
            // nothing, which reads as an accepted plan that had no effect.
            return Err(UnknownPlanTargetError::new(name).into());
        }
        return Ok(targets);
    }
    if publishable.contains_key(name) {
        return Ok(groups
            .closure(name)
            .into_iter()
            .filter(|member| publishable.contains_key(member))
            .collect());
    }
    Err(UnknownPlanTargetError::new(name).into())
}

fn decision_keys(targets: &[String], groups: &Groups) -> BTreeSet<String> {
    targets
        .iter()
        .map(|target| groups.group_of(target).unwrap_or(target).to_string())
        .collect()
}

fn members_for_key(
    key: &str,
    groups: &Groups,
    publishable: &BTreeMap<String, Version>,
) -> Vec<String> {
    let group_members = groups.members(key);
    if group_members.is_empty() {
        if publishable.contains_key(key) {
            vec![key.to_string()]
        } else {
            Vec::new()
        }
    } else {
        group_members
            .iter()
            .filter(|member| publishable.contains_key(*member))
            .cloned()
            .collect()
    }
}

pub(crate) fn increment_version(
    version: &Version,
    level: IncrementLevel,
) -> Result<Version, AppError> {
    match level {
        IncrementLevel::Major => {
            let major = version
                .major
                .checked_add(1)
                .ok_or_else(|| VersionOverflowError::new(version.clone()))?;
            Ok(Version::new(major, 0, 0))
        }
        IncrementLevel::Minor => {
            let minor = version
                .minor
                .checked_add(1)
                .ok_or_else(|| VersionOverflowError::new(version.clone()))?;
            Ok(Version::new(version.major, minor, 0))
        }
        IncrementLevel::Patch => {
            let patch = version
                .patch
                .checked_add(1)
                .ok_or_else(|| VersionOverflowError::new(version.clone()))?;
            Ok(Version::new(version.major, version.minor, patch))
        }
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;

    fn v(text: &str) -> Version {
        text.parse().unwrap()
    }

    fn nm_groups() -> Groups {
        Groups::from_members(BTreeMap::from([(
            "nm".to_string(),
            vec!["nm".to_string(), "nm_impl".to_string()],
        )]))
        .unwrap()
    }

    fn current() -> BTreeMap<String, Version> {
        // Synthetic versions; tests assert relative increment arithmetic, not
        // workspace pins.
        BTreeMap::from([
            ("nm".to_string(), v("0.1.0")),
            ("nm_impl".to_string(), v("0.1.0")),
            ("events".to_string(), v("0.2.0")),
        ])
    }

    #[test]
    fn only_the_three_semver_levels_are_accepted() {
        assert_eq!("patch".parse::<IncrementLevel>(), Ok(IncrementLevel::Patch));
        assert_eq!("minor".parse::<IncrementLevel>(), Ok(IncrementLevel::Minor));
        assert_eq!("major".parse::<IncrementLevel>(), Ok(IncrementLevel::Major));
        "Patch".parse::<IncrementLevel>().unwrap_err();
        "build".parse::<IncrementLevel>().unwrap_err();
    }

    #[test]
    fn expands_group_when_one_member_is_listed() {
        let plan = PlanFile::new(
            PlanStage::Proposed,
            vec![PlanIncrement {
                name: "nm_impl".to_string(),
                level: Some("patch".to_string()),
                version: None,
            }],
        );
        let expanded = resolve_plan(&plan, &nm_groups(), &current(), Verbose::new(false)).unwrap();
        assert_eq!(expanded.packages.get("nm"), Some(&v("0.1.1")));
        assert_eq!(expanded.packages.get("nm_impl"), Some(&v("0.1.1")));
        assert!(!expanded.packages.contains_key("events"));
    }

    /// An expanded plan is rejected once its group gained a member.
    ///
    /// The expanded document is the approved set, so reaching a package it does
    /// not name means the group configuration moved underneath it. Applying it
    /// would edit a package nobody reviewed and that the publication check never
    /// saw.
    #[test]
    fn an_expanded_plan_rejects_a_member_added_after_it_was_written() {
        // Names only `nm`, as an expansion written while the group held it alone.
        let plan = PlanFile::new(
            PlanStage::Expanded,
            vec![PlanIncrement {
                name: "nm".to_string(),
                level: None,
                version: Some("0.1.1".to_string()),
            }],
        );
        // `nm_groups` has since gained `nm_impl`.
        let error = resolve_plan(&plan, &nm_groups(), &current(), Verbose::new(false)).unwrap_err();
        let drift = error
            .find_source::<ExpandedPlanDriftError>()
            .expect("a widened expanded plan reports drift");
        assert_eq!(drift.unnamed(), ["nm_impl".to_string()]);
    }

    /// An expanded plan naming every member still applies.
    ///
    /// The guard must reject only a widened set, not the ordinary case where the
    /// document already names everything expansion reaches.
    #[test]
    fn an_expanded_plan_naming_every_member_is_accepted() {
        let plan = PlanFile::new(
            PlanStage::Expanded,
            vec![
                PlanIncrement {
                    name: "nm".to_string(),
                    level: None,
                    version: Some("0.1.1".to_string()),
                },
                PlanIncrement {
                    name: "nm_impl".to_string(),
                    level: None,
                    version: Some("0.1.1".to_string()),
                },
            ],
        );
        let expanded = resolve_plan(&plan, &nm_groups(), &current(), Verbose::new(false)).unwrap();
        assert_eq!(expanded.packages.get("nm"), Some(&v("0.1.1")));
        assert_eq!(expanded.packages.get("nm_impl"), Some(&v("0.1.1")));
    }

    /// A proposed plan may still widen through its group.
    ///
    /// Naming a group, or one member of it, and letting expansion reach the rest
    /// is a proposed plan's whole purpose, so the drift guard must not apply to a
    /// document `expand` did not produce.
    #[test]
    fn a_proposed_plan_may_still_widen_through_its_group() {
        let plan = PlanFile::new(
            PlanStage::Proposed,
            vec![PlanIncrement {
                name: "nm".to_string(),
                level: Some("patch".to_string()),
                version: None,
            }],
        );
        let expanded = resolve_plan(&plan, &nm_groups(), &current(), Verbose::new(false)).unwrap();
        assert!(expanded.packages.contains_key("nm_impl"));
    }

    #[test]
    fn highest_level_wins_inside_a_group() {
        let plan = PlanFile::new(
            PlanStage::Proposed,
            vec![
                PlanIncrement {
                    name: "nm".to_string(),
                    level: Some("patch".to_string()),
                    version: None,
                },
                PlanIncrement {
                    name: "nm_impl".to_string(),
                    level: Some("minor".to_string()),
                    version: None,
                },
            ],
        );
        let expanded = resolve_plan(&plan, &nm_groups(), &current(), Verbose::new(false)).unwrap();
        assert_eq!(expanded.packages.get("nm"), Some(&v("0.2.0")));
        assert_eq!(expanded.packages.get("nm_impl"), Some(&v("0.2.0")));
    }

    #[test]
    fn explicit_version_is_applied_to_the_group() {
        let plan = PlanFile::new(
            PlanStage::Proposed,
            vec![PlanIncrement {
                name: "nm".to_string(),
                level: None,
                version: Some("0.2.0".to_string()),
            }],
        );
        let expanded = resolve_plan(&plan, &nm_groups(), &current(), Verbose::new(false)).unwrap();
        assert_eq!(expanded.packages.get("nm"), Some(&v("0.2.0")));
        assert_eq!(expanded.packages.get("nm_impl"), Some(&v("0.2.0")));
    }

    #[test]
    fn rejects_unknown_schema() {
        // Arbitrary revision distinct from the supported schema.
        let plan = PlanFile::with_schema_version(9);
        let error = resolve_plan(&plan, &nm_groups(), &current(), Verbose::new(false)).unwrap_err();
        assert!(error.find_source::<UnsupportedPlanSchemaError>().is_some());
    }

    #[test]
    fn rejects_unknown_target() {
        let plan = PlanFile::new(
            PlanStage::Proposed,
            vec![PlanIncrement {
                name: "ghost".to_string(),
                level: Some("patch".to_string()),
                version: None,
            }],
        );
        let error = resolve_plan(&plan, &nm_groups(), &current(), Verbose::new(false)).unwrap_err();
        assert!(error.find_source::<UnknownPlanTargetError>().is_some());
    }

    #[test]
    fn rejects_missing_level_and_version() {
        let plan = PlanFile::new(
            PlanStage::Proposed,
            vec![PlanIncrement {
                name: "events".to_string(),
                level: None,
                version: None,
            }],
        );
        let error = resolve_plan(&plan, &nm_groups(), &current(), Verbose::new(false)).unwrap_err();
        assert!(error.find_source::<PlanIncrementSpecError>().is_some());
    }

    #[test]
    fn max_declared_version_is_the_increment_base() {
        let mut versions = current();
        versions.insert("nm_impl".to_string(), v("0.1.50"));
        let plan = PlanFile::new(
            PlanStage::Proposed,
            vec![PlanIncrement {
                name: "nm".to_string(),
                level: Some("patch".to_string()),
                version: None,
            }],
        );
        let expanded = resolve_plan(&plan, &nm_groups(), &versions, Verbose::new(false)).unwrap();
        assert_eq!(expanded.packages.get("nm"), Some(&v("0.1.51")));
        assert_eq!(expanded.packages.get("nm_impl"), Some(&v("0.1.51")));
    }

    #[test]
    fn rejects_conflicting_explicit_versions() {
        let plan = PlanFile::new(
            PlanStage::Proposed,
            vec![
                PlanIncrement {
                    name: "nm".to_string(),
                    level: None,
                    version: Some("0.2.0".to_string()),
                },
                PlanIncrement {
                    name: "nm_impl".to_string(),
                    level: None,
                    version: Some("0.3.0".to_string()),
                },
            ],
        );
        let error = resolve_plan(&plan, &nm_groups(), &current(), Verbose::new(false)).unwrap_err();
        assert!(error.find_source::<ConflictingPlanVersionError>().is_some());
    }

    #[test]
    fn matching_explicit_versions_merge() {
        let plan = PlanFile::new(
            PlanStage::Proposed,
            vec![
                PlanIncrement {
                    name: "nm".to_string(),
                    level: None,
                    version: Some("0.2.0".to_string()),
                },
                PlanIncrement {
                    name: "nm_impl".to_string(),
                    level: None,
                    version: Some("0.2.0".to_string()),
                },
            ],
        );
        let expanded = resolve_plan(&plan, &nm_groups(), &current(), Verbose::new(false)).unwrap();
        assert_eq!(expanded.packages.get("nm"), Some(&v("0.2.0")));
        assert_eq!(expanded.packages.get("nm_impl"), Some(&v("0.2.0")));
    }

    #[test]
    fn explicit_version_and_level_conflict_in_either_order() {
        for increments in [
            vec![
                PlanIncrement {
                    name: "nm".to_string(),
                    level: Some("patch".to_string()),
                    version: None,
                },
                PlanIncrement {
                    name: "nm_impl".to_string(),
                    level: None,
                    version: Some("0.3.0".to_string()),
                },
            ],
            vec![
                PlanIncrement {
                    name: "nm".to_string(),
                    level: None,
                    version: Some("0.3.0".to_string()),
                },
                PlanIncrement {
                    name: "nm_impl".to_string(),
                    level: Some("patch".to_string()),
                    version: None,
                },
            ],
        ] {
            let plan = PlanFile::new(PlanStage::Proposed, increments);
            let error =
                resolve_plan(&plan, &nm_groups(), &current(), Verbose::new(false)).unwrap_err();
            assert!(
                error
                    .find_source::<ConflictingPlanIncrementKindError>()
                    .is_some()
            );
        }
    }

    #[test]
    fn an_unknown_decision_key_has_no_members() {
        assert!(members_for_key("ghost", &nm_groups(), &current()).is_empty());
    }

    #[test]
    fn ungrouped_package_is_incremented_alone() {
        let plan = PlanFile::new(
            PlanStage::Proposed,
            vec![PlanIncrement {
                name: "events".to_string(),
                level: Some("patch".to_string()),
                version: None,
            }],
        );
        let expanded = resolve_plan(&plan, &nm_groups(), &current(), Verbose::new(false)).unwrap();
        assert_eq!(expanded.packages.get("events"), Some(&v("0.2.1")));
        assert!(!expanded.packages.contains_key("nm"));
    }

    #[test]
    fn major_level_increments_the_major_component() {
        let plan = PlanFile::new(
            PlanStage::Proposed,
            vec![PlanIncrement {
                name: "events".to_string(),
                level: Some("major".to_string()),
                version: None,
            }],
        );
        let expanded = resolve_plan(&plan, &nm_groups(), &current(), Verbose::new(false)).unwrap();
        assert_eq!(expanded.packages.get("events"), Some(&v("1.0.0")));
    }

    #[test]
    fn increment_version_errors_when_a_component_overflows() {
        let max = Version::new(0, 0, u64::MAX);
        let error = increment_version(&max, IncrementLevel::Patch).unwrap_err();
        assert!(error.find_source::<VersionOverflowError>().is_some());
    }

    #[test]
    fn explicit_version_below_the_declared_version_is_rejected() {
        let plan = PlanFile::new(
            PlanStage::Proposed,
            vec![PlanIncrement {
                name: "events".to_string(),
                level: None,
                version: Some("0.1.0".to_string()),
            }],
        );
        let error = resolve_plan(&plan, &nm_groups(), &current(), Verbose::new(false)).unwrap_err();
        let regression = error.find_source::<PlanVersionRegressionError>().unwrap();
        assert_eq!(regression.target(), "events");
    }

    #[test]
    fn explicit_version_equal_to_the_declared_version_is_accepted() {
        // Equality is how a lagging group member is raised into alignment.
        let mut current = current();
        current.insert("nm_impl".to_string(), v("0.0.9"));
        let plan = PlanFile::new(
            PlanStage::Proposed,
            vec![PlanIncrement {
                name: "nm".to_string(),
                level: None,
                version: Some("0.1.0".to_string()),
            }],
        );
        let expanded = resolve_plan(&plan, &nm_groups(), &current, Verbose::new(false)).unwrap();
        assert_eq!(expanded.packages.get("nm_impl"), Some(&v("0.1.0")));
    }

    #[test]
    fn group_without_publishable_members_is_rejected() {
        let publishable = BTreeMap::from([("events".to_string(), v("0.2.0"))]);
        let plan = PlanFile::new(
            PlanStage::Proposed,
            vec![PlanIncrement {
                name: "nm".to_string(),
                level: Some("patch".to_string()),
                version: None,
            }],
        );
        let error =
            resolve_plan(&plan, &nm_groups(), &publishable, Verbose::new(false)).unwrap_err();
        assert!(error.find_source::<UnknownPlanTargetError>().is_some());
    }
}
