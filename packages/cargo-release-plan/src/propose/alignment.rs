// Group alignment reaches a fixed point over every requirement the proposal rewrites.

use std::collections::{BTreeMap, BTreeSet};

use ohno::AppError;

use crate::plan::{IncrementLevel, PlanIncrement};
use crate::propose::generate::{Proposal, record_state};
use crate::text::quote_path;
use crate::verbose::Verbose;

impl Proposal<'_> {
    pub(crate) fn align_groups(
        &self,
        increments: &[PlanIncrement],
        verbose: Verbose,
    ) -> Result<BTreeMap<String, PlanIncrement>, AppError> {
        let planned: BTreeSet<&str> = increments
            .iter()
            .map(|entry| self.decision_key(&entry.name))
            .collect();
        let unaligned: Vec<&str> = self
            .report
            .groups
            .iter()
            .filter(|(name, group)| {
                !planned.contains(name.as_str())
                    && (group
                        .members
                        .iter()
                        .any(|member| self.declared(member) != self.highest(name))
                        || self.highest_has_nonplain_member(name))
            })
            .map(|(name, _)| name.as_str())
            .collect();
        let mut alignment: BTreeMap<String, PlanIncrement> = BTreeMap::new();
        let mut visited = BTreeSet::new();
        loop {
            record_state(&mut visited, &alignment)?;
            let previous = alignment.clone();
            for name in &unaligned {
                // A group's own answer is not context for itself: exact alignment must account
                // for its own laggards separately from whatever the other entries move.
                let mut context = increments.to_vec();
                context.extend(
                    alignment
                        .iter()
                        .filter(|(key, _)| key.as_str() != *name)
                        .map(|(_, entry)| entry.clone()),
                );
                let moved = self.moved(context)?;
                alignment.insert(
                    (*name).to_owned(),
                    self.alignment_increment(name, &moved, verbose),
                );
            }
            if alignment == previous {
                return Ok(alignment);
            }
        }
    }

    fn highest_has_nonplain_member(&self, name: &str) -> bool {
        self.groups.members(name).iter().any(|member| {
            let version = self.declared(member);
            version.cmp_precedence(self.highest(name)).is_eq()
                && (!version.pre.is_empty() || !version.build.is_empty())
        })
    }

    fn alignment_increment(
        &self,
        name: &str,
        moved: &BTreeSet<String>,
        verbose: Verbose,
    ) -> PlanIncrement {
        let highest = self.highest(name);
        let members = self.groups.members(name);
        let moving: BTreeSet<&str> = members
            .iter()
            .filter(|member| self.declared(member) != highest)
            .map(String::as_str)
            .collect();
        for member in members {
            if moving.contains(member.as_str())
                || !self.ships_published_version(member, &BTreeSet::new())
            {
                continue;
            }
            let Some(package) = self.packages.get(member.as_str()) else {
                continue;
            };
            if let Some(dependency) = package.dependencies.iter().find(|dependency| {
                moving.contains(dependency.name.as_str()) || moved.contains(&dependency.name)
            }) {
                verbose.note(|| {
                    format!(
                        "Group {} cannot align on {} because member {} already publishes that \
                         version and depends on moving package {}. Patch-incrementing the group \
                         gives the rewritten requirement a new version.",
                        quote_path(name),
                        highest,
                        quote_path(member),
                        quote_path(&dependency.name)
                    )
                });
                return patch_alignment(name);
            }
        }
        if self.highest_has_nonplain_member(name) {
            verbose.note(|| {
                format!(
                    "Group {} has a non-plain member at highest version {}. Patch-incrementing \
                     produces the plain version required by exact workspace requirements.",
                    quote_path(name),
                    highest
                )
            });
            return patch_alignment(name);
        }
        verbose.note(|| {
            format!(
                "Group {} aligns on its highest declared version {} because no member keeping \
                 a published version depends on a moving package.",
                quote_path(name),
                highest
            )
        });
        PlanIncrement {
            name: name.to_owned(),
            level: None,
            version: Some(highest.to_string()),
        }
    }
}

fn patch_alignment(name: &str) -> PlanIncrement {
    PlanIncrement {
        name: name.to_owned(),
        level: Some(IncrementLevel::Patch.to_string()),
        version: None,
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;
    use crate::propose::tests::{depends, helper, package, report};

    #[test]
    fn verbose_alignment_explains_retaining_the_highest_version() {
        let report = report(
            vec![
                package("a", "1.1.0", Some("1.1.0")),
                package("b", "1.0.0", Some("1.0.0")),
            ],
            vec![],
            &[&["a", "b"]],
        );
        report.validate().unwrap();
        let increment =
            Proposal::new(&report).alignment_increment("a", &BTreeSet::new(), Verbose::new(true));
        assert_eq!(increment.name, "a");
        assert_eq!(increment.version.as_deref(), Some("1.1.0"));
        assert!(increment.level.is_none());
    }

    #[test]
    fn verbose_alignment_explains_rewritten_published_requirements() {
        let report = report(
            vec![
                depends(package("a", "1.1.0", Some("1.1.0")), "b", false),
                package("b", "1.0.0", Some("1.0.0")),
            ],
            vec![],
            &[&["a", "b"]],
        );
        report.validate().unwrap();
        let increment =
            Proposal::new(&report).alignment_increment("a", &BTreeSet::new(), Verbose::new(true));
        assert_eq!(increment.name, "a");
        assert_eq!(increment.level.as_deref(), Some("patch"));
        assert!(increment.version.is_none());
    }

    #[test]
    fn verbose_alignment_explains_plain_version_normalization() {
        let report = report(
            vec![],
            vec![helper("a", "1.2.3+build"), helper("b", "1.2.2")],
            &[&["a", "b"]],
        );
        report.validate().unwrap();
        let increment =
            Proposal::new(&report).alignment_increment("a", &BTreeSet::new(), Verbose::new(true));
        assert_eq!(increment.name, "a");
        assert_eq!(increment.level.as_deref(), Some("patch"));
        assert!(increment.version.is_none());
    }
}
