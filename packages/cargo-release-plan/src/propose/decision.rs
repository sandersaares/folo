// Semantic decision parsing and translation to the shared mechanical version algebra.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use ohno::AppError;
use semver::Version;
use serde::{Deserialize, Serialize};

use crate::ReadFileError;
use crate::plan::{IncrementLevel, PlanIncrement, increment_version};
use crate::propose::generate::Proposal;
use crate::text::{Quotable as _, quote_path};
use crate::verbose::Verbose;

/// Local decision-file revision used by the increment-versions skill.
const DECISION_SCHEMA_VERSION: u32 = 1;

/// Human semantic assessments, before resolving their mechanical consequences.
///
/// Top-level metadata is extensible; individual decisions deliberately accept only name/level.
#[derive(Debug, Deserialize)]
pub(crate) struct Decisions {
    schema_version: u32,
    changes: Vec<Change>,
}

impl Decisions {
    #[cfg(test)]
    #[cfg_attr(coverage_nightly, coverage(off))]
    pub(crate) fn for_test(changes: &[(&str, &str)]) -> Self {
        Self {
            schema_version: DECISION_SCHEMA_VERSION,
            changes: changes
                .iter()
                .map(|(name, level)| Change {
                    name: (*name).to_owned(),
                    level: match *level {
                        "breaking" => ChangeLevel::Breaking,
                        "nonbreaking" => ChangeLevel::Nonbreaking,
                        "patch" => ChangeLevel::Patch,
                        _ => panic!("unsupported test change level"),
                    },
                })
                .collect(),
        }
    }

    pub(crate) fn read(path: &Path) -> Result<Self, AppError> {
        let text =
            fs::read_to_string(path).map_err(|error| ReadFileError::caused_by(path, error))?;
        Self::parse(&text)
    }

    pub(crate) fn parse(text: &str) -> Result<Self, AppError> {
        serde_json::from_str(text.trim_start_matches('\u{feff}'))
            .map_err(|error| InvalidDecisionFile::caused_by(error).into())
    }

    pub(crate) fn validate(&self) -> Result<BTreeMap<String, ChangeLevel>, AppError> {
        if self.schema_version != DECISION_SCHEMA_VERSION {
            return Err(UnsupportedDecisionSchema::new(self.schema_version).into());
        }
        let mut levels = BTreeMap::new();
        for change in &self.changes {
            if change.name.trim().is_empty()
                || levels.insert(change.name.clone(), change.level).is_some()
            {
                return Err(InvalidDecisionName::new(&change.name).into());
            }
        }
        Ok(levels)
    }
}

/// One case-sensitive semantic judgement, not an exact version or Cargo increment level.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Change {
    name: String,
    level: ChangeLevel,
}

/// Compatibility significance relative to a package's published anchor.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum ChangeLevel {
    Breaking,
    Nonbreaking,
    Patch,
}

impl ChangeLevel {
    pub(crate) fn minimum(self, anchor: &Version) -> Result<Version, AppError> {
        // Cargo's leftmost nonzero component determines compatibility. In particular every
        // 0.0.z movement is breaking, so even a breaking judgement only advances its patch.
        // Ref: docs/design.md, "Public dependencies".
        let level = match self {
            Self::Breaking if anchor.major > 0 => IncrementLevel::Major,
            Self::Breaking if anchor.minor > 0 => IncrementLevel::Minor,
            Self::Nonbreaking if anchor.major > 0 => IncrementLevel::Minor,
            Self::Breaking | Self::Nonbreaking | Self::Patch => IncrementLevel::Patch,
        };
        increment_version(anchor, level)
    }

    fn name(self) -> &'static str {
        match self {
            Self::Breaking => "breaking",
            Self::Nonbreaking => "nonbreaking",
            Self::Patch => "patch",
        }
    }
}

impl Proposal<'_> {
    pub(crate) fn decision_increments(
        &self,
        levels: &BTreeMap<String, ChangeLevel>,
        verbose: Verbose,
    ) -> Result<Vec<PlanIncrement>, AppError> {
        let mut increments = Vec::new();
        for (name, level) in levels {
            if !self.packages.contains_key(name.as_str()) {
                return Err(UnknownDecisionTarget::new(name).into());
            }
            let anchor = self
                .anchors
                .get(name.as_str())
                .ok_or_else(|| FirstPublicationRequired::new(name))?;
            let declared = self.declared(name);
            // Component bumps cannot express dropping a prerelease suffix, so deriving a
            // mechanical level from a prerelease would silently overshoot its semantic target.
            if !anchor.pre.is_empty() || !declared.pre.is_empty() {
                return Err(PrereleaseDecision::new(name).into());
            }
            let minimum = level.minimum(anchor)?;
            if declared.cmp_precedence(&minimum).is_ge() {
                verbose.note(|| {
                    format!(
                        "Decision for package {} at semantic level '{}' is not emitted because \
                         declared version {} already satisfies minimum version {} from anchor {}.",
                        quote_path(name),
                        level.name(),
                        declared,
                        minimum,
                        anchor
                    )
                });
                continue;
            }
            let cargo_level = if minimum.major > declared.major {
                IncrementLevel::Major
            } else if minimum.minor > declared.minor {
                IncrementLevel::Minor
            } else {
                IncrementLevel::Patch
            };
            verbose.note(|| {
                format!(
                    "Decision for package {} at semantic level '{}' is emitted as '{}' because \
                     declared version {} is below minimum version {} from anchor {}.",
                    quote_path(name),
                    level.name(),
                    cargo_level,
                    declared,
                    minimum,
                    anchor
                )
            });
            increments.push(PlanIncrement {
                name: name.clone(),
                level: Some(cargo_level.to_string()),
                version: None,
            });
        }
        Ok(increments)
    }
}

/// A decision document must preserve the skill's typed name/level contract.
#[ohno::error]
#[display("invalid release change-decision document")]
struct InvalidDecisionFile;

/// The decision working-file protocol is versioned independently from the report protocol.
#[ohno::error]
#[display("unsupported change-decision schema_version {version}; expected 1")]
struct UnsupportedDecisionSchema {
    version: u32,
}

/// Names are nonempty, unique ordinal identifiers rather than normalized labels.
#[ohno::error]
#[display("change-decision name {} is empty or duplicated", name.quoted())]
struct InvalidDecisionName {
    name: String,
}

/// Only publishable packages can receive semantic assessments.
#[ohno::error]
#[display("change decision names unknown or non-publishable package {}", name.quoted())]
struct UnknownDecisionTarget {
    name: String,
}

/// First publication requires the release process rather than an inferred anchor.
#[ohno::error]
#[display("package {} has no published version anchor. Publish the package manually first; follow RELEASING.md#first-publish-of-a-new-crate and complete the full procedure, including Trusted Publishing and binary-release follow-up", name.quoted())]
struct FirstPublicationRequired {
    name: String,
}

/// Semantic component-based grading requires release versions at both endpoints.
#[ohno::error]
#[display("package {} has a prerelease version, which semantic proposal generation does not support", name.quoted())]
struct PrereleaseDecision {
    name: String,
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::VersionOverflowError;
    use crate::propose::tests::{generate, helper, package, report};

    #[test]
    fn verbose_decisions_explain_emitted_and_retained_versions() {
        let report = report(
            vec![
                package("breaking", "1.0.0", Some("1.0.0")),
                package("feature", "1.0.0", Some("1.0.0")),
                package("pending", "1.0.1", Some("1.0.0")),
            ],
            vec![],
            &[],
        );
        report.validate().unwrap();
        let levels = BTreeMap::from([
            ("breaking".to_owned(), ChangeLevel::Breaking),
            ("feature".to_owned(), ChangeLevel::Nonbreaking),
            ("pending".to_owned(), ChangeLevel::Patch),
        ]);
        let increments = Proposal::new(&report)
            .decision_increments(&levels, Verbose::new(true))
            .unwrap();
        assert_eq!(
            increments,
            [
                PlanIncrement {
                    name: "breaking".to_owned(),
                    level: Some("major".to_owned()),
                    version: None,
                },
                PlanIncrement {
                    name: "feature".to_owned(),
                    level: Some("minor".to_owned()),
                    version: None,
                },
            ]
        );
    }

    #[test]
    fn malformed_decision_shapes_fail_typed_parsing() {
        for value in [
            json!(null),
            json!([]),
            json!({"changes": []}),
            json!({"schema_version": "1", "changes": []}),
            json!({"schema_version": 1.0, "changes": []}),
            json!({"schema_version": 1}),
            json!({"schema_version": 1, "changes": {}}),
            json!({"schema_version": 1, "changes": [null]}),
            json!({"schema_version": 1, "changes": [{"name": "lib"}]}),
            json!({"schema_version": 1, "changes": [{"name": "lib", "level": "patch", "version": "9.0.0"}]}),
            json!({"schema_version": 1, "changes": [{"name": "lib", "level": "minor"}]}),
            json!({"schema_version": 1, "changes": [{"name": "lib", "level": "Breaking"}]}),
            json!({"schema_version": 1, "changes": [{"name": "lib", "level": null}]}),
        ] {
            let error = Decisions::parse(&value.to_string()).unwrap_err();
            assert!(error.find_source::<InvalidDecisionFile>().is_some());
        }
        let error = Decisions::parse(
            r#"{"schema_version":1,"changes":[{"name":"lib","name":"other","level":"patch"}]}"#,
        )
        .unwrap_err();
        assert!(error.find_source::<InvalidDecisionFile>().is_some());
    }

    #[test]
    fn schema_and_ordinal_unique_names_are_validated() {
        let decisions = Decisions::parse(r#"{"schema_version":2,"changes":[]}"#).unwrap();
        let error = decisions.validate().unwrap_err();
        assert!(error.find_source::<UnsupportedDecisionSchema>().is_some());
        for names in [["lib", "lib"], [" ", "lib"]] {
            let decisions = Decisions::parse(
                &json!({
                    "schema_version": 1,
                    "changes": names.map(|name| json!({"name": name, "level": "patch"}))
                })
                .to_string(),
            )
            .unwrap();
            let error = decisions.validate().unwrap_err();
            assert!(error.find_source::<InvalidDecisionName>().is_some());
        }
        let decisions = Decisions::parse(
            r#"{"schema_version":1,"metadata":"allowed","changes":[{"name":"lib","level":"patch"},{"name":"Lib","level":"patch"}]}"#,
        )
        .unwrap();
        assert_eq!(decisions.validate().unwrap().len(), 2);
        assert!(
            Decisions::parse("\u{feff}{\"schema_version\":1,\"changes\":[]}")
                .unwrap()
                .validate()
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn semantic_algebra_covers_stable_zero_minor_and_zero_patch_lines() {
        for (anchor, expected) in [
            ("2.4.7", ["3.0.0", "2.5.0", "2.4.8"]),
            ("0.7.14", ["0.8.0", "0.7.15", "0.7.15"]),
            ("0.0.5", ["0.0.6", "0.0.6", "0.0.6"]),
        ] {
            let anchor = anchor.parse::<Version>().unwrap();
            for (level, expected) in [
                ChangeLevel::Breaking,
                ChangeLevel::Nonbreaking,
                ChangeLevel::Patch,
            ]
            .into_iter()
            .zip(expected)
            {
                assert_eq!(level.minimum(&anchor).unwrap().to_string(), expected);
            }
        }
        let error = ChangeLevel::Patch
            .minimum(&Version::new(1, 0, u64::MAX))
            .unwrap_err();
        assert!(error.find_source::<VersionOverflowError>().is_some());
    }

    #[test]
    fn invalid_targets_and_first_publications_are_not_semantic_decisions() {
        let report = report(
            vec![package("new", "0.1.0", None)],
            vec![helper("helper", "1.0.0")],
            &[],
        );
        for target in ["absent", "helper"] {
            let error = generate(&report, &[(target, "patch")]).unwrap_err();
            assert!(error.find_source::<UnknownDecisionTarget>().is_some());
        }
        let error = generate(&report, &[("new", "patch")]).unwrap_err();
        assert!(error.find_source::<FirstPublicationRequired>().is_some());
    }

    #[test]
    fn prerelease_semantic_endpoints_are_rejected_even_when_a_group_covers_them() {
        for (declared, anchor) in [("1.1.0-alpha", "1.0.0"), ("1.1.0", "1.0.0-alpha")] {
            let report = report(
                vec![
                    package("lib", declared, Some(anchor)),
                    package("sibling", "2.0.0", Some("2.0.0")),
                ],
                vec![],
                &[&["lib", "sibling"]],
            );
            let error = generate(&report, &[("lib", "patch")]).unwrap_err();
            assert!(error.find_source::<PrereleaseDecision>().is_some());
        }
    }

    #[test]
    fn adequate_pending_breaking_increment_is_kept() {
        assert_pending_increment("0.8.0", "0.7.0", "breaking", None);
    }

    #[test]
    fn adequate_pending_patch_increment_is_kept() {
        assert_pending_increment("1.1.0", "1.0.0", "patch", None);
    }

    #[test]
    fn inadequate_pending_nonbreaking_increment_is_raised() {
        assert_pending_increment("1.0.1", "1.0.0", "nonbreaking", Some("minor"));
    }

    #[test]
    fn inadequate_pending_breaking_increment_is_raised() {
        assert_pending_increment("1.1.0", "1.0.0", "breaking", Some("major"));
    }

    #[test]
    fn build_metadata_does_not_satisfy_a_pending_patch_increment() {
        assert_pending_increment("1.0.0+build", "1.0.0", "patch", Some("patch"));
    }

    fn assert_pending_increment(
        declared: &str,
        anchor: &str,
        change: &str,
        expected: Option<&str>,
    ) {
        let report = report(vec![package("lib", declared, Some(anchor))], vec![], &[]);
        let plan = generate(&report, &[("lib", change)]).unwrap();
        assert_eq!(
            plan.increments
                .first()
                .and_then(|entry| entry.level.as_deref()),
            expected
        );
    }
}
