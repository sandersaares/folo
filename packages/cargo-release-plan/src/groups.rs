// Version-group derivation, membership, and consistency.

use std::collections::{BTreeMap, BTreeSet, HashSet};

use semver::Version;

/// Derived version groups keyed by their smallest member and by package name.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct Groups {
    by_name: BTreeMap<String, Vec<String>>,
    by_package: BTreeMap<String, String>,
}

impl Groups {
    /// Derives connected components from exact dependency edges.
    ///
    /// Every target is inserted before the edges, so self-edges and isolated
    /// targets remain singletons and therefore do not become groups.
    pub(crate) fn from_edges(
        targets: impl IntoIterator<Item = String>,
        edges: impl IntoIterator<Item = (String, String)>,
    ) -> Self {
        let mut adjacency: BTreeMap<String, BTreeSet<String>> = targets
            .into_iter()
            .map(|target| (target, BTreeSet::new()))
            .collect();
        for (left, right) in edges {
            if left == right || !adjacency.contains_key(&left) || !adjacency.contains_key(&right) {
                continue;
            }
            adjacency
                .get_mut(&left)
                .expect("the endpoint was checked above")
                .insert(right.clone());
            adjacency
                .get_mut(&right)
                .expect("the endpoint was checked above")
                .insert(left);
        }

        let mut unvisited: BTreeSet<String> = adjacency.keys().cloned().collect();
        let mut by_name = BTreeMap::new();
        let mut by_package = BTreeMap::new();
        while let Some(first) = unvisited.pop_first() {
            let mut pending = vec![first];
            let mut members = BTreeSet::new();
            while let Some(member) = pending.pop() {
                if !members.insert(member.clone()) {
                    continue;
                }
                if let Some(neighbors) = adjacency.get(&member) {
                    for neighbor in neighbors {
                        if unvisited.remove(neighbor) {
                            pending.push(neighbor.clone());
                        }
                    }
                }
            }
            if members.len() < 2 {
                continue;
            }
            let members: Vec<String> = members.into_iter().collect();
            let key = members
                .first()
                .expect("a multi-member component always has a first member")
                .clone();
            for member in &members {
                by_package.insert(member.clone(), key.clone());
            }
            by_name.insert(key, members);
        }
        Self {
            by_name,
            by_package,
        }
    }

    pub(crate) fn group_of(&self, package: &str) -> Option<&str> {
        self.by_package.get(package).map(String::as_str)
    }

    pub(crate) fn members(&self, group: &str) -> &[String] {
        self.by_name.get(group).map_or(&[], Vec::as_slice)
    }

    /// Packages that share a group with `package`, including `package` itself.
    pub(crate) fn closure(&self, package: &str) -> Vec<String> {
        match self.group_of(package) {
            Some(group) => self.members(group).to_vec(),
            None => vec![package.to_string()],
        }
    }

    /// Group-level consistency on work-tree declared versions.
    ///
    /// `exempt` names members that do not exist on the base revision and are
    /// therefore not required to match.
    pub(crate) fn verdicts(
        &self,
        versions: &BTreeMap<String, Version>,
        exempt: &HashSet<String>,
    ) -> BTreeMap<String, GroupVerdict> {
        self.by_name
            .iter()
            .map(|(name, members)| (name.clone(), GroupVerdict::new(members, versions, exempt)))
            .collect()
    }
}

/// Consistency outcome for one complete version group.
///
/// The outcome is derived once, at construction, from the declared versions and
/// the exemption set; there is no way to assemble a verdict that contradicts
/// those facts. That matters because `check` gates the process exit on
/// consistency while `report` and `apply` use the group version as the
/// increment base, so a verdict that reported one without the other would let
/// the two disagree. Ref: `docs/design.md`, "Version groups".
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GroupVerdict {
    members: Vec<String>,
    state: GroupState,
}

impl GroupVerdict {
    /// Derives the verdict for one group from the work tree's declared versions.
    ///
    /// Every member must have a declared version. `exempt` names members that
    /// do not exist on the base revision.
    pub(crate) fn new(
        members: &[String],
        versions: &BTreeMap<String, Version>,
        exempt: &HashSet<String>,
    ) -> Self {
        let members = members.to_vec();
        let compared: BTreeSet<&Version> = members
            .iter()
            .filter(|member| !exempt.contains(*member))
            .map(|member| {
                versions
                    .get(member)
                    .expect("every derived group member is a version target")
            })
            .collect();
        // Exemption governs consistency only. The group version is the highest
        // declared by any present member, including exempt ones, so that it
        // matches the increment base `expand_plan` computes and no member is
        // ever moved backwards.
        let highest = members
            .iter()
            .map(|member| {
                versions
                    .get(member)
                    .expect("every derived group member is a version target")
            })
            .max()
            .cloned()
            .expect("a derived group contains at least two members");
        let state = if compared.len() <= 1 {
            GroupState::Consistent { version: highest }
        } else {
            GroupState::Inconsistent { version: highest }
        };
        Self { members, state }
    }

    /// Members in ordinal name order.
    pub(crate) fn members(&self) -> &[String] {
        &self.members
    }

    /// Whether every non-exempt member declares the same version.
    pub(crate) fn is_consistent(&self) -> bool {
        !matches!(self.state, GroupState::Inconsistent { .. })
    }

    /// The highest version any member declares.
    pub(crate) fn version(&self) -> &Version {
        match &self.state {
            GroupState::Consistent { version } | GroupState::Inconsistent { version } => version,
        }
    }
}

/// The outcomes a group can actually have.
///
/// Every derived group contains complete version targets, so both outcomes carry
/// the version base planning needs.
#[derive(Clone, Debug, Eq, PartialEq)]
enum GroupState {
    /// Every non-exempt member declares the same version.
    Consistent { version: Version },
    /// Non-exempt members declare more than one version.
    Inconsistent { version: Version },
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;

    fn v(text: &str) -> Version {
        text.parse().unwrap()
    }

    fn groups() -> Groups {
        Groups::from_edges(
            ["nm", "nm_impl"].map(str::to_string),
            [("nm".to_string(), "nm_impl".to_string())],
        )
    }

    #[test]
    fn graph_shapes_produce_deterministic_components() {
        let targets = ["g", "f", "e", "d", "c", "b", "a", "solo"]
            .map(str::to_string)
            .into_iter();
        let edges = [
            ("b", "a"),
            ("a", "c"),
            ("c", "b"),
            ("d", "e"),
            ("d", "f"),
            ("e", "g"),
            ("f", "g"),
            ("a", "b"),
            ("solo", "solo"),
        ]
        .map(|(left, right)| (left.to_string(), right.to_string()));
        let groups = Groups::from_edges(targets, edges);

        assert_eq!(groups.members("a"), ["a", "b", "c"]);
        assert_eq!(groups.members("d"), ["d", "e", "f", "g"]);
        assert_eq!(groups.group_of("solo"), None);
    }

    #[test]
    fn a_bridge_connects_and_its_removal_splits_components() {
        let targets = ["a", "b", "helper", "c"].map(str::to_string);
        let connected = Groups::from_edges(
            targets.clone(),
            [("a", "b"), ("b", "helper"), ("helper", "c")]
                .map(|(left, right)| (left.to_string(), right.to_string())),
        );
        assert_eq!(connected.members("a"), ["a", "b", "c", "helper"]);

        let split = Groups::from_edges(
            targets,
            [("a", "b"), ("b", "helper")]
                .map(|(left, right)| (left.to_string(), right.to_string())),
        );
        assert_eq!(split.members("a"), ["a", "b", "helper"]);
        assert_eq!(split.group_of("c"), None);
    }

    #[test]
    fn removing_a_redundant_edge_does_not_split_a_component() {
        let targets = ["a", "b", "c"].map(str::to_string);
        let groups = Groups::from_edges(
            targets,
            [("a", "b"), ("b", "c")].map(|(left, right)| (left.to_string(), right.to_string())),
        );
        assert_eq!(groups.members("a"), ["a", "b", "c"]);
    }

    #[test]
    fn consistent_when_declared_versions_match() {
        let versions = BTreeMap::from([
            ("nm".to_string(), v("0.1.0")),
            ("nm_impl".to_string(), v("0.1.0")),
        ]);
        let verdicts = groups().verdicts(&versions, &HashSet::new());
        let nm = verdicts.get("nm").unwrap();
        assert!(nm.is_consistent());
        assert_eq!(nm.version(), &v("0.1.0"));
    }

    #[test]
    fn inconsistent_when_declared_versions_differ() {
        let versions = BTreeMap::from([
            ("nm".to_string(), v("0.1.0")),
            ("nm_impl".to_string(), v("0.1.1")),
        ]);
        let verdicts = groups().verdicts(&versions, &HashSet::new());
        assert!(!verdicts.get("nm").unwrap().is_consistent());
        // The reported version is the highest declared by any present member,
        // so it can serve as the increment base for the whole group.
        assert_eq!(verdicts.get("nm").unwrap().version(), &v("0.1.1"));
    }

    #[test]
    fn never_published_member_is_exempt_from_consistency() {
        let versions = BTreeMap::from([
            ("nm".to_string(), v("0.2.0")),
            ("nm_impl".to_string(), v("0.1.0")),
        ]);
        let exempt = HashSet::from(["nm_impl".to_string()]);
        let verdicts = groups().verdicts(&versions, &exempt);
        let nm = verdicts.get("nm").unwrap();
        assert!(nm.is_consistent());
        assert_eq!(nm.version(), &v("0.2.0"));
    }

    #[test]
    fn exempt_member_still_raises_the_group_version() {
        // Exemption suppresses the consistency failure but must not lower the
        // increment base, or `expand_plan` would move the exempt member back.
        let versions = BTreeMap::from([
            ("nm".to_string(), v("0.1.0")),
            ("nm_impl".to_string(), v("0.3.0")),
        ]);
        let exempt = HashSet::from(["nm_impl".to_string()]);
        let verdicts = groups().verdicts(&versions, &exempt);
        let nm = verdicts.get("nm").unwrap();
        assert!(nm.is_consistent());
        assert_eq!(nm.version(), &v("0.3.0"));
    }

    #[test]
    fn closure_includes_every_member() {
        assert_eq!(groups().closure("nm_impl"), vec!["nm", "nm_impl"]);
        let empty = Groups::default();
        assert_eq!(empty.closure("events"), vec!["events"]);
    }
}
