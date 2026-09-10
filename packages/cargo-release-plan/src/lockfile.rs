// Resolved dependency closures read from Cargo lockfiles.
//
// Cargo puts a lockfile into every package archive, but library consumers resolve
// the library in their own dependency graph and do not use that file. For a
// package with a binary or example target, the package-specific resolution is
// operationally relevant and therefore released content.
// Ref: docs/design.md, "Relevant lockfile closures".

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
#[cfg(any(test, feature = "private-test-util"))]
use std::iter;

use ohno::AppError;
use toml_edit::{DocumentMut, Item};

use crate::MalformedLockfileError;

/// A package's resolved dependencies, keyed by crate name.
///
/// A name maps to a set because one closure may legitimately hold several
/// versions of the same crate, and because comparing per name is what lets a
/// report say which dependency moved.
pub(crate) type Closure = BTreeMap<String, BTreeSet<String>>;

/// The locked packages of one Cargo lockfile, indexed for closure walks.
///
/// Only what a closure walk needs is retained: which entries exist, how each is
/// identified, and which entries each names as a dependency. Ref:
/// docs/implementation.md, "Lockfile closures".
#[derive(Debug)]
pub(crate) struct Lockfile {
    entries: Vec<LockEntry>,
    roots: HashMap<String, HashMap<String, usize>>,
}

impl Lockfile {
    /// Parses a lockfile, naming `label` in any diagnostic.
    pub(crate) fn parse(text: &str, label: &str) -> Result<Self, AppError> {
        let doc: DocumentMut = text
            .parse()
            .map_err(|error| MalformedLockfileError::caused_by(label, error))?;
        // A lockfile that resolved to nothing omits the array entirely, which is
        // a well-formed lockfile with an empty closure rather than a fault.
        let Some(tables) = doc.get("package").and_then(Item::as_array_of_tables) else {
            return Ok(Self {
                entries: Vec::new(),
                roots: HashMap::new(),
            });
        };
        let mut pending = Vec::with_capacity(tables.len());
        for table in tables {
            let Some(name) = table.get("name").and_then(Item::as_str) else {
                return Err(MalformedLockfileError::new(label).into());
            };
            let Some(version) = table.get("version").and_then(Item::as_str) else {
                return Err(MalformedLockfileError::new(label).into());
            };
            let mut dependencies = Vec::new();
            if let Some(item) = table.get("dependencies") {
                let Some(array) = item.as_array() else {
                    return Err(MalformedLockfileError::new(label).into());
                };
                for value in array {
                    let Some(text) = value.as_str() else {
                        return Err(MalformedLockfileError::new(label).into());
                    };
                    let Some(dependency) = DepRef::parse(text) else {
                        return Err(MalformedLockfileError::new(label).into());
                    };
                    dependencies.push(dependency);
                }
            }
            pending.push(PendingEntry {
                name: name.to_owned(),
                version: version.to_owned(),
                source: table
                    .get("source")
                    .and_then(Item::as_str)
                    .map(ToOwned::to_owned),
                dependencies,
            });
        }
        let index = DependencyIndex::from_entries(&pending);
        let mut roots: HashMap<String, HashMap<String, usize>> = HashMap::new();
        for (entry_index, entry) in pending.iter().enumerate() {
            if entry.source.is_none() {
                // Preserve the first match, matching the earlier linear lookup
                // when malformed input duplicates a source-less identity.
                roots
                    .entry(entry.name.clone())
                    .or_default()
                    .entry(entry.version.clone())
                    .or_insert(entry_index);
            }
        }
        let mut entries = Vec::with_capacity(pending.len());
        for entry in pending {
            let mut dependencies = Vec::with_capacity(entry.dependencies.len());
            for dependency in &entry.dependencies {
                let Some(entry_index) = index.matching(dependency) else {
                    return Err(MalformedLockfileError::new(label).into());
                };
                dependencies.push(entry_index);
            }
            entries.push(LockEntry {
                name: entry.name,
                version: entry.version,
                source: entry.source,
                dependencies,
            });
        }
        Ok(Self { entries, roots })
    }

    /// The locked identities `root` transitively depends on.
    ///
    /// Returns `None` when the lockfile does not resolve `root` at all, which is
    /// what a lockfile predating the package looks like.
    ///
    /// The root's own entry is left out however the walk reaches it. Its version
    /// is the declared version the invariant already tracks, so counting it
    /// would make every increment look like a further change and leave the
    /// package permanently unable to settle.
    pub(crate) fn closure(&self, root: &str, version: &str) -> Option<Closure> {
        let root_index = self.root_index(root, version)?;
        let mut seen = BTreeSet::from([root_index]);
        let mut queue = VecDeque::from([root_index]);
        let mut closure = Closure::new();
        while let Some(index) = queue.pop_front() {
            let entry = self
                .entries
                .get(index)
                .expect("every queued index was produced by a lookup into this same vector");
            if index != root_index {
                closure
                    .entry(entry.name.clone())
                    .or_default()
                    .insert(entry.identity());
            }
            for &next in &entry.dependencies {
                if seen.insert(next) {
                    queue.push_back(next);
                }
            }
        }
        Some(closure)
    }

    /// Locates the entry standing for the workspace member being classified.
    ///
    /// A registry crate may share a workspace member's name, and only the member
    /// is the package being published. Cargo records no `source` for a package
    /// it resolved from a path, so the member is the source-less entry and
    /// nothing else: a lockfile predating the member holds no such entry, which
    /// is the unresolved root this reports as `None`.
    fn root_index(&self, root: &str, version: &str) -> Option<usize> {
        self.roots
            .get(root)
            .and_then(|versions| versions.get(version))
            .copied()
    }
}

/// Parses and walks several closures for an in-workspace benchmark.
#[cfg(any(test, feature = "private-test-util"))]
#[cfg_attr(coverage_nightly, coverage(off))]
#[doc(hidden)]
#[must_use]
pub fn benchmark_lockfile_closures(
    text: &str,
    root: &str,
    version: &str,
    closure_count: usize,
) -> usize {
    let lockfile = Lockfile::parse(text, "benchmark lockfile")
        .expect("the generated benchmark lockfile is valid");
    iter::repeat_with(|| {
        lockfile
            .closure(root, version)
            .expect("the generated benchmark lockfile contains its root package")
            .len()
    })
    .take(closure_count)
    .sum()
}

/// One `[[package]]` entry of a lockfile.
#[derive(Debug)]
struct LockEntry {
    name: String,
    version: String,
    /// Where the package was resolved from; absent for a path dependency.
    source: Option<String>,
    dependencies: Vec<usize>,
}

impl LockEntry {
    /// How this entry is identified when closures are compared.
    ///
    /// The source is carried because a `[patch]` can redirect a name and version
    /// at an entirely different tree. The checksum is not, because a registry
    /// fixes it for a given name, version and source.
    fn identity(&self) -> String {
        match &self.source {
            Some(source) => format!("{} ({source})", self.version),
            None => self.version.clone(),
        }
    }
}

/// One parsed lockfile entry before dependency names become entry indices.
///
/// Parsing retains Cargo's textual dependency references only until all package
/// identities are indexed. Converting them once avoids searching the package
/// list for every edge during every lockfile-bearing package's closure walk.
#[derive(Debug)]
struct PendingEntry {
    name: String,
    version: String,
    source: Option<String>,
    dependencies: Vec<DepRef>,
}

/// Package-entry lookup built while textual dependency references are resolved.
///
/// A name can map to several versions, and a version can map to several sources.
/// The nested maps let each form Cargo writes resolve without allocating a lookup
/// key or scanning unrelated lockfile entries.
#[derive(Debug, Default)]
struct DependencyIndex {
    names: HashMap<String, NameMatches>,
}

impl DependencyIndex {
    fn from_entries(entries: &[PendingEntry]) -> Self {
        let mut index = Self::default();
        for (entry_index, entry) in entries.iter().enumerate() {
            let matches = index.names.entry(entry.name.clone()).or_default();
            matches.all.push(entry_index);
            let version = matches.versions.entry(entry.version.clone()).or_default();
            version.all.push(entry_index);
            match &entry.source {
                Some(source) => version
                    .sources
                    .entry(source.clone())
                    .or_default()
                    .push(entry_index),
                None => version.source_less.push(entry_index),
            }
        }
        index
    }

    fn matching(&self, dependency: &DepRef) -> Option<usize> {
        let name = self.names.get(&dependency.name)?;
        let matches: &[usize] = match (&dependency.version, &dependency.source) {
            (None, _) => name.all.as_slice(),
            (Some(version), None) => name.versions.get(version).map_or(&[], |matches| {
                if matches.all.len() == 1 {
                    matches.all.as_slice()
                } else {
                    // Cargo omits the source for a path dependency even when a
                    // sourced package shares its name and version.
                    matches.source_less.as_slice()
                }
            }),
            (Some(version), Some(source)) => name
                .versions
                .get(version)
                .and_then(|matches| matches.sources.get(source))
                .map_or(&[], Vec::as_slice),
        };
        let [entry_index] = matches else {
            return None;
        };
        Some(*entry_index)
    }
}

/// Lockfile entries sharing a package name.
#[derive(Debug, Default)]
struct NameMatches {
    all: Vec<usize>,
    versions: HashMap<String, VersionMatches>,
}

/// Lockfile entries sharing a package name and version.
#[derive(Debug, Default)]
struct VersionMatches {
    all: Vec<usize>,
    source_less: Vec<usize>,
    sources: HashMap<String, Vec<usize>>,
}

/// A dependency named by a lockfile entry.
///
/// Cargo spells a dependency as a bare name while the name is unambiguous, adds
/// the version once it is not, and adds the source once name and version are
/// still not enough. Every part beyond the name is therefore optional here, and
/// every part that is present has to take part in matching: Cargo only wrote it
/// because something else would otherwise answer to the same reference.
#[derive(Debug)]
struct DepRef {
    name: String,
    version: Option<String>,
    /// Where the dependency resolves from, as a `[[package]]` entry spells it.
    source: Option<String>,
}

impl DepRef {
    fn parse(text: &str) -> Option<Self> {
        let mut parts = text.split_whitespace();
        let name = parts.next()?;
        let version = parts.next();
        let source = parts.next();
        if parts.next().is_some() {
            return None;
        }
        let source = match source {
            Some(source) => Some(source.strip_prefix('(')?.strip_suffix(')')?.to_owned()),
            None => None,
        };
        Some(Self {
            name: name.to_owned(),
            version: version.map(ToOwned::to_owned),
            source,
        })
    }
}

/// How one dependency differs between two closures.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ClosureChange {
    Added,
    Deleted,
    Modified,
}

impl ClosureChange {
    /// The vocabulary `report.json` uses for a change of any source.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Added => "added",
            Self::Deleted => "deleted",
            Self::Modified => "modified",
        }
    }
}

/// Dependencies whose locked identities differ, in name order.
pub(crate) fn closure_changes(anchor: &Closure, work: &Closure) -> Vec<(String, ClosureChange)> {
    let names: BTreeSet<&String> = anchor.keys().chain(work.keys()).collect();
    names
        .into_iter()
        .filter_map(|name| {
            let change = match (anchor.get(name), work.get(name)) {
                (None, Some(_)) => ClosureChange::Added,
                (Some(_), None) => ClosureChange::Deleted,
                (Some(before), Some(after)) if before != after => ClosureChange::Modified,
                _ => return None,
            };
            Some((name.clone(), change))
        })
        .collect()
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;

    const LABEL: &str = "Cargo.lock";

    fn closure_of(text: &str, root: &str, version: &str) -> Closure {
        Lockfile::parse(text, LABEL)
            .unwrap()
            .closure(root, version)
            .expect("the fixtures all resolve the root they are asked about")
    }

    #[test]
    fn a_lockfile_without_packages_has_an_empty_closure() {
        let lockfile = Lockfile::parse("version = 4\n", LABEL).unwrap();
        assert!(lockfile.closure("tool", "1.0.0").is_none());
    }

    #[test]
    fn an_unresolved_root_has_no_closure() {
        let text = "\
[[package]]
name = \"other\"
version = \"1.0.0\"
";
        assert!(
            Lockfile::parse(text, LABEL)
                .unwrap()
                .closure("tool", "1.0.0")
                .is_none()
        );
    }

    #[test]
    fn a_closure_follows_dependencies_transitively() {
        let text = "\
[[package]]
name = \"tool\"
version = \"0.1.0\"
dependencies = [\"direct\"]

[[package]]
name = \"direct\"
version = \"1.0.0\"
dependencies = [\"indirect\"]

[[package]]
name = \"indirect\"
version = \"2.0.0\"

[[package]]
name = \"unrelated\"
version = \"9.0.0\"
";
        let closure = closure_of(text, "tool", "0.1.0");
        assert_eq!(
            closure.keys().collect::<Vec<_>>(),
            vec!["direct", "indirect"]
        );
        assert!(!closure.contains_key("unrelated"));
    }

    #[test]
    fn an_unresolved_dependency_reference_is_malformed() {
        let text = "\
[[package]]
name = \"tool\"
version = \"0.1.0\"
dependencies = [\"absent\"]
";
        let error = Lockfile::parse(text, LABEL).unwrap_err();
        assert!(error.find_source::<MalformedLockfileError>().is_some());
    }

    #[test]
    fn a_closure_excludes_the_root_itself() {
        // Otherwise incrementing the package would register as a change to its
        // own dependencies and it could never reach a settled state.
        let text = "\
[[package]]
name = \"tool\"
version = \"0.1.0\"
dependencies = [\"helper\"]

[[package]]
name = \"helper\"
version = \"1.0.0\"
dependencies = [\"tool\"]
";
        let closure = closure_of(text, "tool", "0.1.0");
        assert_eq!(closure.keys().collect::<Vec<_>>(), vec!["helper"]);
    }

    #[test]
    fn a_dependency_cycle_terminates() {
        let text = "\
[[package]]
name = \"tool\"
version = \"0.1.0\"
dependencies = [\"a\"]

[[package]]
name = \"a\"
version = \"1.0.0\"
dependencies = [\"b\"]

[[package]]
name = \"b\"
version = \"1.0.0\"
dependencies = [\"a\"]
";
        let closure = closure_of(text, "tool", "0.1.0");
        assert_eq!(closure.keys().collect::<Vec<_>>(), vec!["a", "b"]);
    }

    #[test]
    fn a_disambiguated_dependency_selects_one_version() {
        let text = "\
[[package]]
name = \"tool\"
version = \"0.1.0\"
dependencies = [\"dup 2.0.0\"]

[[package]]
name = \"dup\"
version = \"1.0.0\"
source = \"registry+https://example.invalid\"

[[package]]
name = \"dup\"
version = \"2.0.0\"
source = \"registry+https://example.invalid\"
";
        let closure = closure_of(text, "tool", "0.1.0");
        let versions = closure.get("dup").unwrap();
        assert_eq!(versions.len(), 1);
        assert!(
            versions
                .iter()
                .any(|identity| identity.starts_with("2.0.0"))
        );
    }

    #[test]
    fn an_ambiguous_dependency_reference_is_malformed() {
        let text = "\
[[package]]
name = \"tool\"
version = \"0.1.0\"
dependencies = [\"dup\"]

[[package]]
name = \"dup\"
version = \"1.0.0\"
source = \"registry+https://example.invalid\"

[[package]]
name = \"dup\"
version = \"2.0.0\"
source = \"registry+https://example.invalid\"
";
        let error = Lockfile::parse(text, LABEL).unwrap_err();
        assert!(error.find_source::<MalformedLockfileError>().is_some());
    }

    #[test]
    fn a_sourceless_reference_selects_a_path_package_on_an_identity_collision() {
        // The identity in the closure distinguishes the colliding entries without extra
        // descendants; transitive traversal has its own focused test.
        let text = "\
[[package]]
name = \"tool\"
version = \"0.1.0\"
dependencies = [\"dup 1.0.0\"]

[[package]]
name = \"dup\"
version = \"1.0.0\"

[[package]]
name = \"dup\"
version = \"1.0.0\"
source = \"registry+r\"
";
        let closure = closure_of(text, "tool", "0.1.0");
        assert_eq!(closure.keys().collect::<Vec<_>>(), vec!["dup"]);
        assert_eq!(
            closure.get("dup").unwrap().iter().collect::<Vec<_>>(),
            vec!["1.0.0"]
        );
    }

    #[test]
    fn a_workspace_member_wins_over_a_registry_crate_of_the_same_name() {
        // The published package is the member, so the walk must start there.
        let text = "\
[[package]]
name = \"tool\"
version = \"9.9.9\"
source = \"registry+https://example.invalid\"

[[package]]
name = \"tool\"
version = \"0.1.0\"
dependencies = [\"helper\"]

[[package]]
name = \"helper\"
version = \"1.0.0\"
";
        let closure = closure_of(text, "tool", "0.1.0");
        assert_eq!(closure.keys().collect::<Vec<_>>(), vec!["helper"]);
    }

    /// A registry crate alone never stands in for an absent workspace member.
    ///
    /// A lockfile predating the member resolves no member entry, and answering
    /// with a same-named registry crate would invent a closure for a package
    /// the lockfile knows nothing about.
    #[test]
    fn a_registry_crate_does_not_stand_in_for_an_absent_member() {
        let text = "\
[[package]]
name = \"tool\"
version = \"9.9.9\"
source = \"registry+https://example.invalid\"
dependencies = [\"helper\"]

[[package]]
name = \"helper\"
version = \"1.0.0\"
source = \"registry+https://example.invalid\"
";
        assert!(
            Lockfile::parse(text, LABEL)
                .unwrap()
                .closure("tool", "1.0.0")
                .is_none()
        );
    }

    /// A source-less package with the same name but another version is not the root.
    #[test]
    fn a_path_dependency_does_not_stand_in_for_an_absent_member_version() {
        let text = "\
[[package]]
name = \"tool\"
version = \"0.9.0\"

[[package]]
name = \"dependency\"
version = \"1.0.0\"
dependencies = [\"tool\"]
";
        assert!(
            Lockfile::parse(text, LABEL)
                .unwrap()
                .closure("tool", "1.0.0")
                .is_none()
        );
    }

    /// A dependency naming a source follows only the entry from that source.
    ///
    /// Cargo spells the source out precisely when the name and version do not
    /// identify one entry, so ignoring it would walk into the wrong package.
    #[test]
    fn a_dependency_naming_a_source_selects_that_source() {
        // Source identities are opaque here; the identity in the closure identifies exactly
        // which colliding entry was selected without needing descendants of each candidate.
        let text = "\
[[package]]
name = \"tool\"
version = \"0.1.0\"
dependencies = [\"dup 1.0.0 (git+g)\"]

[[package]]
name = \"dup\"
version = \"1.0.0\"
source = \"registry+r\"

[[package]]
name = \"dup\"
version = \"1.0.0\"
source = \"git+g\"
";
        let closure = closure_of(text, "tool", "0.1.0");
        assert_eq!(closure.keys().collect::<Vec<_>>(), vec!["dup"]);
        let dup = closure.get("dup").unwrap();
        assert_eq!(dup.iter().collect::<Vec<_>>(), vec!["1.0.0 (git+g)"]);
    }

    #[test]
    fn a_source_change_is_part_of_an_identity() {
        // A patched dependency keeps its name and version while resolving to a
        // different tree entirely.
        let plain = "\
[[package]]
name = \"tool\"
version = \"0.1.0\"
dependencies = [\"dep\"]

[[package]]
name = \"dep\"
version = \"1.0.0\"
source = \"registry+a\"
";
        let mut lockfile = Lockfile::parse(plain, LABEL).unwrap();
        let before = lockfile.closure("tool", "0.1.0").unwrap();
        // Only the dependency source changes; reusing the parsed graph isolates identity
        // comparison from repeated TOML parsing.
        let dependency = lockfile
            .entries
            .iter_mut()
            .find(|entry| {
                entry.name == "dep"
                    && entry.version == "1.0.0"
                    && entry.source.as_deref() == Some("registry+a")
            })
            .unwrap();
        dependency.source = Some("registry+b".to_owned());
        let after = lockfile.closure("tool", "0.1.0").unwrap();
        let changes = closure_changes(&before, &after);
        assert_eq!(changes, vec![("dep".to_owned(), ClosureChange::Modified)]);
    }

    #[test]
    fn closure_changes_name_what_moved() {
        let before = Closure::from([
            ("kept".to_owned(), BTreeSet::from(["1.0.0".to_owned()])),
            ("bumped".to_owned(), BTreeSet::from(["1.0.0".to_owned()])),
            ("dropped".to_owned(), BTreeSet::from(["1.0.0".to_owned()])),
        ]);
        let after = Closure::from([
            ("kept".to_owned(), BTreeSet::from(["1.0.0".to_owned()])),
            ("bumped".to_owned(), BTreeSet::from(["1.1.0".to_owned()])),
            ("gained".to_owned(), BTreeSet::from(["1.0.0".to_owned()])),
        ]);
        assert_eq!(
            closure_changes(&before, &after),
            vec![
                ("bumped".to_owned(), ClosureChange::Modified),
                ("dropped".to_owned(), ClosureChange::Deleted),
                ("gained".to_owned(), ClosureChange::Added),
            ]
        );
    }

    #[test]
    fn identical_closures_report_nothing() {
        let closure = Closure::from([("dep".to_owned(), BTreeSet::from(["1.0.0".to_owned()]))]);
        assert!(closure_changes(&closure, &closure).is_empty());
    }

    #[test]
    fn a_change_renders_the_vocabulary_reports_use() {
        assert_eq!(ClosureChange::Added.as_str(), "added");
        assert_eq!(ClosureChange::Deleted.as_str(), "deleted");
        assert_eq!(ClosureChange::Modified.as_str(), "modified");
    }

    fn assert_malformed(text: &str) {
        let error = Lockfile::parse(text, LABEL).unwrap_err();
        assert!(error.find_source::<MalformedLockfileError>().is_some());
    }

    #[test]
    fn malformed_toml_is_rejected() {
        assert_malformed("not = = toml");
    }

    #[test]
    fn a_package_without_a_name_is_rejected() {
        assert_malformed("[[package]]\nversion = \"1.0.0\"\n");
    }

    #[test]
    fn a_package_without_a_version_is_rejected() {
        assert_malformed("[[package]]\nname = \"a\"\n");
    }

    #[test]
    fn a_non_array_dependency_list_is_rejected() {
        assert_malformed("[[package]]\nname = \"a\"\nversion = \"1.0.0\"\ndependencies = \"b\"\n");
    }

    #[test]
    fn a_non_string_dependency_is_rejected() {
        assert_malformed("[[package]]\nname = \"a\"\nversion = \"1.0.0\"\ndependencies = [1]\n");
    }

    #[test]
    fn an_empty_dependency_is_rejected() {
        assert_malformed("[[package]]\nname = \"a\"\nversion = \"1.0.0\"\ndependencies = [\"\"]\n");
    }

    #[test]
    fn an_unparenthesized_source_is_rejected() {
        assert_malformed(
            "[[package]]\nname = \"a\"\nversion = \"1.0.0\"\ndependencies = [\"b 1.0.0 source\"]\n",
        );
    }

    #[test]
    fn trailing_dependency_fields_are_rejected() {
        assert_malformed(
            "[[package]]\nname = \"a\"\nversion = \"1.0.0\"\n\
dependencies = [\"b 1.0.0 (source) extra\"]\n",
        );
    }
}
