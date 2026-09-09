// Resolved dependency closures read from Cargo lockfiles.
//
// Cargo puts a lockfile into every package archive, but library consumers resolve
// the library in their own dependency graph and do not use that file. For a
// package with an installable binary target, the package-specific resolution is
// operationally relevant and therefore released content.
// Ref: docs/design.md, "Relevant lockfile closures".

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
#[cfg(any(test, feature = "private-test-util"))]
use std::iter;
use std::sync::Arc;

use ohno::AppError;
use semver::Version;
use toml_edit::{DocumentMut, Item};

use crate::manifest::{
    DependencyPatch, DependencyPath, DependencySource, InstallationDependencies,
    InstallationDependency, InstallationError, PackageIdentity, installation_error,
};
use crate::{LockfileClosureUnavailableError, MalformedLockfileError};

/// A package's resolved dependencies, keyed by crate name.
///
/// A name maps to a set because one closure may legitimately hold several
/// versions of the same crate, and because comparing per name is what lets a
/// report say which dependency moved.
pub(crate) type Closure = BTreeMap<String, BTreeSet<String>>;

/// Installation declarations for every tracked member at one endpoint.
///
/// Non-publishable members are included: Cargo.lock also merges their development
/// edges, which must not enter a binary's transitive installation closure.
/// A version is retained so a same-named path dependency cannot borrow another
/// package's declarations. Ref: docs/implementation.md, "Lockfile closures".
#[derive(Clone, Debug, Default)]
pub(crate) struct InstallationGraph {
    members: BTreeMap<String, (Version, InstallationDependencies)>,
    pub(crate) patches: Vec<DependencyPatch>,
    path_errors: BTreeMap<DependencyPath, InstallationError>,
    /// Cargo metadata resolves registry names without resolving package versions.
    pub(crate) registries: BTreeMap<String, String>,
    /// Historical configuration errors matter only when resolving a registry name.
    pub(crate) registry_error: Option<InstallationError>,
}

impl InstallationGraph {
    pub(crate) fn insert(
        &mut self,
        name: String,
        version: Version,
        dependencies: impl Into<InstallationDependencies>,
    ) {
        self.members.insert(name, (version, dependencies.into()));
    }

    /// Resolves direct and patched path declarations through the same endpoint.
    ///
    /// Unavailable targets retain their unresolved state and any original error.
    /// Only a closure that needs such a declaration fails, not an unrelated
    /// library assessment.
    pub(crate) fn resolve_paths(
        &mut self,
        mut identify: impl FnMut(&DependencyPath) -> Result<Option<PackageIdentity>, AppError>,
    ) {
        for declaration in self
            .members
            .values_mut()
            .filter_map(|(_, dependencies)| match dependencies {
                InstallationDependencies::Parsed(dependencies) => Some(dependencies),
                InstallationDependencies::Invalid(_) => None,
            })
            .flatten()
            .chain(
                self.patches
                    .iter_mut()
                    .filter_map(|patch| patch.replacement.as_mut().ok()),
            )
        {
            let DependencySource::UnresolvedPath(path) = &declaration.source else {
                continue;
            };
            self.path_errors.remove(path);
            match identify(path) {
                Ok(Some(identity)) if identity.name == declaration.name => {
                    declaration.source = DependencySource::Path(identity);
                }
                Ok(_) => {}
                Err(error) => {
                    self.path_errors
                        .insert(path.clone(), installation_error(error));
                }
            }
        }
    }

    fn dependencies(
        &self,
        entry: &LockEntry,
        root: &str,
    ) -> Result<Option<&[InstallationDependency]>, AppError> {
        if entry.source.is_some() {
            return Ok(None);
        }
        let Some((version, dependencies)) = self.members.get(&entry.name) else {
            return Ok(None);
        };
        if version != &entry.version {
            return Ok(None);
        }
        match dependencies {
            InstallationDependencies::Parsed(dependencies) => Ok(Some(dependencies)),
            InstallationDependencies::Invalid(error) => {
                Err(LockfileClosureUnavailableError::caused_by(
                    root,
                    "required installation declarations cannot be interpreted",
                    Arc::clone(error),
                )
                .into())
            }
        }
    }

    fn check_path_error(&self, source: &DependencySource, root: &str) -> Result<(), AppError> {
        if let DependencySource::UnresolvedPath(path) = source
            && let Some(error) = self.path_errors.get(path)
        {
            return Err(LockfileClosureUnavailableError::caused_by(
                root,
                "a required path package identity cannot be read",
                Arc::clone(error),
            )
            .into());
        }
        Ok(())
    }

    fn check_registry_error(&self, root: &str) -> Result<(), AppError> {
        if let Some(error) = &self.registry_error {
            return Err(LockfileClosureUnavailableError::caused_by(
                root,
                "required registry configuration cannot be interpreted",
                Arc::clone(error),
            )
            .into());
        }
        Ok(())
    }

    fn allows(
        &self,
        declarations: &[InstallationDependency],
        dependency: &LockEntry,
        root: &str,
    ) -> Result<Option<bool>, AppError> {
        for declared in declarations {
            if !declared.matches_package(&dependency.name, &dependency.version) {
                continue;
            }
            if matches!(declared.source, DependencySource::NamedRegistry(_)) {
                self.check_registry_error(root)?;
            }
            let Some(matches) = declared
                .source
                .matches_locked(dependency.source.as_deref(), &self.registries)
            else {
                self.check_path_error(&declared.source, root)?;
                return Ok(None);
            };
            if matches {
                return Ok(Some(true));
            }
            for patch in &self.patches {
                if patch.name != dependency.name {
                    continue;
                }
                if !matches!(
                    declared.source,
                    DependencySource::Path(_) | DependencySource::UnresolvedPath(_)
                ) && patch.origin != "crates-io"
                    && !patch.origin.contains("://")
                {
                    self.check_registry_error(root)?;
                }
                let Some(applies) = declared
                    .source
                    .accepts_patch(&patch.origin, &self.registries)
                else {
                    return Ok(None);
                };
                if !applies {
                    continue;
                }
                let replacement = patch.replacement.as_ref().map_err(|error| {
                    LockfileClosureUnavailableError::caused_by(
                        root,
                        "a required patch declaration cannot be interpreted",
                        Arc::clone(error),
                    )
                })?;
                if !replacement.matches_package(&dependency.name, &dependency.version) {
                    continue;
                }
                if matches!(replacement.source, DependencySource::NamedRegistry(_)) {
                    self.check_registry_error(root)?;
                }
                let Some(matches) = replacement
                    .source
                    .matches_locked(dependency.source.as_deref(), &self.registries)
                else {
                    self.check_path_error(&replacement.source, root)?;
                    return Ok(None);
                };
                if matches {
                    return Ok(Some(true));
                }
            }
        }
        Ok(Some(false))
    }
}

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
            let version = version
                .parse::<Version>()
                .map_err(|error| MalformedLockfileError::caused_by(label, error))?;
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
                version,
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
                    .entry(entry.version.to_string())
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
    /// Returns `Ok(None)` when the root or a required source identity is unavailable.
    /// Path targets require exact manifest identities, and named registries and
    /// nonidentical URLs require sufficient configuration and supported normalization.
    /// Stored read/parse failures are returned only when their inputs are reached.
    ///
    /// The root's own entry is left out however the walk reaches it. Its version
    /// is the declared version the invariant already tracks, so counting it
    /// would make every increment look like a further change and leave the
    /// package permanently unable to settle.
    ///
    /// Every workspace member uses its endpoint's normal and build declarations
    /// to select edges. Other packages retain their resolved lockfile edges.
    pub(crate) fn closure(
        &self,
        root: &str,
        version: &str,
        installation: &InstallationGraph,
    ) -> Result<Option<Closure>, AppError> {
        let Some(root_index) = self.root_index(root, version) else {
            return Ok(None);
        };
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
            let declarations = installation.dependencies(entry, root)?;
            if let Some(declaration) = declarations.and_then(|declarations| {
                declarations.iter().find(|declaration| {
                    matches!(declaration.source, DependencySource::UnresolvedPath(_))
                })
            }) {
                installation.check_path_error(&declaration.source, root)?;
                return Ok(None);
            }
            for &next in &entry.dependencies {
                let dependency = self
                    .entries
                    .get(next)
                    .expect("parsing resolves every dependency to an index into this same vector");
                if let Some(declarations) = declarations {
                    let Some(allowed) = installation.allows(declarations, dependency, root)? else {
                        return Ok(None);
                    };
                    if !allowed {
                        continue;
                    }
                }
                if seen.insert(next) {
                    queue.push_back(next);
                }
            }
        }
        Ok(Some(closure))
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
    let installation = InstallationGraph::default();
    iter::repeat_with(|| {
        lockfile
            .closure(root, version, &installation)
            .expect("the benchmark has no installation declarations to fail")
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
    version: Version,
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
            None => self.version.to_string(),
        }
    }
}

/// One parsed lockfile entry before dependency names become entry indices.
///
/// Parsing retains Cargo's textual dependency references only until all package
/// identities are indexed. Converting them once avoids searching the package
/// list for every edge during every binary package's closure walk.
#[derive(Debug)]
struct PendingEntry {
    name: String,
    version: Version,
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
            let version = matches
                .versions
                .entry(entry.version.to_string())
                .or_default();
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
    use crate::ReadFileError;
    use crate::manifest::DependencySource;

    const LABEL: &str = "Cargo.lock";

    fn closure_of(text: &str, root: &str, version: &str) -> Closure {
        Lockfile::parse(text, LABEL)
            .unwrap()
            .closure(root, version, &InstallationGraph::default())
            .unwrap()
            .unwrap()
    }

    #[test]
    fn a_lockfile_without_packages_has_an_empty_closure() {
        let lockfile = Lockfile::parse("version = 4\n", LABEL).unwrap();
        assert!(
            lockfile
                .closure("tool", "1.0.0", &InstallationGraph::default())
                .unwrap()
                .is_none()
        );
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
                .closure("tool", "1.0.0", &InstallationGraph::default())
                .unwrap()
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
source = \"registry+https://example.invalid\"
dependencies = [\"indirect\"]

[[package]]
name = \"indirect\"
version = \"2.0.0\"
source = \"registry+https://example.invalid\"

[[package]]
name = \"unrelated\"
version = \"9.0.0\"
source = \"registry+https://example.invalid\"
";
        let closure = closure_of(text, "tool", "0.1.0");
        assert_eq!(
            closure.keys().collect::<Vec<_>>(),
            vec!["direct", "indirect"]
        );
        assert!(!closure.contains_key("unrelated"));
    }

    #[test]
    fn installation_filters_root_and_transitive_workspace_edges() {
        let text = r#"
[[package]]
name = "tool"
version = "0.1.0"
dependencies = ["helper", "widget 1.0.0", "widget 2.0.0"]

[[package]]
name = "helper"
version = "0.1.0"
dependencies = ["build-tool", "development"]

[[package]]
name = "build-tool"
version = "1.0.0-alpha"
dependencies = ["widget 1.0.0"]

[[package]]
name = "development"
version = "1.0.0"
dependencies = ["tool"]

[[package]]
name = "widget"
version = "1.0.0"
source = "registry+https://example.invalid"

[[package]]
name = "widget"
version = "2.0.0"
source = "registry+https://example.invalid"
"#;
        let mut installation = InstallationGraph::default();
        installation.insert(
            "tool".to_owned(),
            Version::new(0, 1, 0),
            vec![
                InstallationDependency {
                    name: "helper".to_owned(),
                    requirement: None,
                    source: DependencySource::Path(PackageIdentity {
                        name: "helper".to_owned(),
                        version: Version::new(0, 1, 0),
                    }),
                },
                InstallationDependency {
                    name: "widget".to_owned(),
                    requirement: Some("1".parse().unwrap()),
                    source: DependencySource::Registry("https://example.invalid".to_owned()),
                },
            ],
        );
        installation.insert(
            "helper".to_owned(),
            Version::new(0, 1, 0),
            vec![InstallationDependency {
                name: "build-tool".to_owned(),
                // A versionless path declaration also admits prereleases.
                requirement: None,
                source: DependencySource::Path(PackageIdentity {
                    name: "build-tool".to_owned(),
                    version: "1.0.0-alpha".parse().unwrap(),
                }),
            }],
        );
        let lockfile = Lockfile::parse(text, LABEL).unwrap();
        let closure = lockfile
            .closure("tool", "0.1.0", &installation)
            .unwrap()
            .unwrap();

        assert_eq!(
            closure.keys().collect::<Vec<_>>(),
            ["build-tool", "helper", "widget"]
        );
        assert_eq!(
            closure.get("widget").unwrap(),
            &BTreeSet::from(["1.0.0 (registry+https://example.invalid)".to_owned()])
        );
    }

    #[test]
    fn workspace_filters_do_not_apply_to_other_sources_or_versions() {
        let text = r#"
[[package]]
name = "tool"
version = "0.1.0"
dependencies = ["helper 1.0.0", "helper 2.0.0"]

[[package]]
name = "helper"
version = "1.0.0"
source = "registry+https://example.invalid"
dependencies = ["leaf"]

[[package]]
name = "helper"
version = "2.0.0"
dependencies = ["leaf"]

[[package]]
name = "leaf"
version = "1.0.0"
"#;
        let lockfile = Lockfile::parse(text, LABEL).unwrap();
        let mut installation = InstallationGraph::default();
        installation.insert("helper".to_owned(), Version::new(1, 0, 0), Vec::new());

        let closure = lockfile
            .closure("tool", "0.1.0", &installation)
            .unwrap()
            .unwrap();
        assert_eq!(closure.keys().collect::<Vec<_>>(), ["helper", "leaf"]);
        assert_eq!(closure.get("helper").unwrap().len(), 2);
    }

    #[test]
    fn installation_edges_match_sources_not_just_name_and_version() {
        let lockfile = Lockfile::parse(
            r#"
[[package]]
name = "tool"
version = "0.1.0"
dependencies = ["foo 1.0.0", "foo 1.0.0 (registry+https://example.invalid)"]
[[package]]
name = "foo"
version = "1.0.0"
[[package]]
name = "foo"
version = "1.0.0"
source = "registry+https://example.invalid"
"#,
            LABEL,
        )
        .unwrap();
        for (source, identity) in [
            (
                DependencySource::Path(PackageIdentity {
                    name: "foo".to_owned(),
                    version: Version::new(1, 0, 0),
                }),
                "1.0.0",
            ),
            (
                DependencySource::Registry("https://example.invalid".to_owned()),
                "1.0.0 (registry+https://example.invalid)",
            ),
        ] {
            let mut installation = InstallationGraph::default();
            installation.insert(
                "tool".to_owned(),
                Version::new(0, 1, 0),
                vec![InstallationDependency {
                    name: "foo".to_owned(),
                    requirement: Some("1".parse().unwrap()),
                    source,
                }],
            );
            let closure = lockfile
                .closure("tool", "0.1.0", &installation)
                .unwrap()
                .unwrap();
            assert_eq!(
                closure.get("foo").unwrap(),
                &BTreeSet::from([identity.to_owned()])
            );
        }
    }

    #[test]
    fn direct_and_patched_paths_share_exact_identity_resolution() {
        let lockfile = Lockfile::parse(
            "[[package]]\nname = \"tool\"\nversion = \"0.1.0\"\n\
             dependencies = [\"foo 1.0.0\", \"foo 1.1.0\"]\n\
             [[package]]\nname = \"foo\"\nversion = \"1.0.0\"\n\
             [[package]]\nname = \"foo\"\nversion = \"1.1.0\"\n",
            LABEL,
        )
        .unwrap();
        let declaration = InstallationDependency {
            name: "foo".to_owned(),
            requirement: Some("1".parse().unwrap()),
            source: DependencySource::UnresolvedPath(DependencyPath {
                path: "foreign/foo".to_owned(),
                package_directory: None,
            }),
        };
        let mut installation = InstallationGraph::default();
        installation.insert(
            "tool".to_owned(),
            Version::new(0, 1, 0),
            vec![declaration.clone()],
        );
        installation.patches.push(DependencyPatch {
            origin: "crates-io".to_owned(),
            name: "foo".to_owned(),
            replacement: Ok(declaration),
        });
        let mut reads = 0;
        installation.resolve_paths(|path| {
            reads += 1;
            assert_eq!(path.path, "foreign/foo");
            Ok(Some(PackageIdentity {
                name: "foo".to_owned(),
                version: Version::new(1, 0, 0),
            }))
        });
        assert_eq!(reads, 2);
        let closure = lockfile
            .closure("tool", "0.1.0", &installation)
            .unwrap()
            .unwrap();
        assert_eq!(
            closure.get("foo").unwrap(),
            &BTreeSet::from(["1.0.0".to_owned()])
        );
        assert!(
            installation
                .patches
                .first()
                .unwrap()
                .replacement
                .as_ref()
                .unwrap()
                .matches_package("foo", &Version::new(1, 0, 0))
        );
        assert!(
            !installation
                .patches
                .first()
                .unwrap()
                .replacement
                .as_ref()
                .unwrap()
                .matches_package("foo", &Version::new(1, 1, 0))
        );
    }

    #[test]
    fn unresolved_path_declarations_cannot_produce_an_empty_closure() {
        let lockfile =
            Lockfile::parse("[[package]]\nname = \"tool\"\nversion = \"0.1.0\"\n", LABEL).unwrap();
        let mut installation = InstallationGraph::default();
        installation.insert(
            "tool".to_owned(),
            Version::new(0, 1, 0),
            vec![InstallationDependency {
                name: "foo".to_owned(),
                requirement: None,
                source: DependencySource::UnresolvedPath(DependencyPath {
                    path: "missing".to_owned(),
                    package_directory: None,
                }),
            }],
        );
        installation.resolve_paths(|_| Ok(None));
        assert!(
            lockfile
                .closure("tool", "0.1.0", &installation)
                .unwrap()
                .is_none()
        );
        installation.resolve_paths(|_| {
            Ok(Some(PackageIdentity {
                name: "different-package".to_owned(),
                version: Version::new(1, 0, 0),
            }))
        });
        assert!(
            lockfile
                .closure("tool", "0.1.0", &installation)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn lookup_errors_are_deferred_until_a_closure_reaches_the_member() {
        let lockfile = Lockfile::parse(
            "[[package]]\nname = \"unrelated\"\nversion = \"0.1.0\"\n\
             [[package]]\nname = \"tool\"\nversion = \"0.1.0\"\ndependencies = [\"library\"]\n\
             [[package]]\nname = \"library\"\nversion = \"0.1.0\"\n",
            LABEL,
        )
        .unwrap();
        let mut installation = InstallationGraph::default();
        installation.insert(
            "tool".to_owned(),
            Version::new(0, 1, 0),
            vec![InstallationDependency {
                name: "library".to_owned(),
                requirement: None,
                source: DependencySource::Path(PackageIdentity {
                    name: "library".to_owned(),
                    version: Version::new(0, 1, 0),
                }),
            }],
        );
        installation.insert(
            "library".to_owned(),
            Version::new(0, 1, 0),
            vec![InstallationDependency {
                name: "foo".to_owned(),
                requirement: None,
                source: DependencySource::UnresolvedPath(DependencyPath {
                    path: "external/foo".to_owned(),
                    package_directory: None,
                }),
            }],
        );
        installation.resolve_paths(|_| Err(ReadFileError::new("external/foo/Cargo.toml").into()));
        assert!(
            lockfile
                .closure("unrelated", "0.1.0", &installation)
                .unwrap()
                .unwrap()
                .is_empty()
        );
        let error = lockfile
            .closure("tool", "0.1.0", &installation)
            .unwrap_err();
        assert!(error.find_source::<ReadFileError>().is_some());
    }

    #[test]
    fn invalid_patches_only_fail_closures_needing_the_replacement() {
        let lockfile = Lockfile::parse(
            "[[package]]\nname = \"unrelated\"\nversion = \"0.1.0\"\n\
             [[package]]\nname = \"tool\"\nversion = \"0.1.0\"\ndependencies = [\"foo\"]\n\
             [[package]]\nname = \"foo\"\nversion = \"1.0.0\"\n",
            LABEL,
        )
        .unwrap();
        let mut installation = InstallationGraph::default();
        installation.insert(
            "tool".to_owned(),
            Version::new(0, 1, 0),
            vec![InstallationDependency {
                name: "foo".to_owned(),
                requirement: None,
                source: DependencySource::Registry("https://example.invalid".to_owned()),
            }],
        );
        installation.patches.push(DependencyPatch {
            origin: "https://example.invalid".to_owned(),
            name: "foo".to_owned(),
            replacement: Err(installation_error(
                ReadFileError::new("patch declaration").into(),
            )),
        });
        assert!(
            lockfile
                .closure("unrelated", "0.1.0", &installation)
                .unwrap()
                .unwrap()
                .is_empty()
        );
        let error = lockfile
            .closure("tool", "0.1.0", &installation)
            .unwrap_err();
        assert!(error.find_source::<ReadFileError>().is_some());
    }

    #[test]
    fn registry_configuration_errors_only_fail_named_source_resolution() {
        let lockfile = Lockfile::parse(
            "[[package]]\nname = \"unrelated\"\nversion = \"0.1.0\"\ndependencies = [\"foo\"]\n\
             [[package]]\nname = \"tool\"\nversion = \"0.1.0\"\ndependencies = [\"foo\"]\n\
             [[package]]\nname = \"foo\"\nversion = \"1.0.0\"\nsource = \"registry+https://example.invalid\"\n",
            LABEL,
        ).unwrap();
        let mut installation = InstallationGraph::default();
        for (name, source) in [
            (
                "unrelated",
                DependencySource::Registry("https://example.invalid".to_owned()),
            ),
            (
                "tool",
                DependencySource::NamedRegistry("private".to_owned()),
            ),
        ] {
            installation.insert(
                name.to_owned(),
                Version::new(0, 1, 0),
                vec![InstallationDependency {
                    name: "foo".to_owned(),
                    requirement: None,
                    source,
                }],
            );
        }
        installation.registry_error = Some(installation_error(
            ReadFileError::new("historical Cargo configuration").into(),
        ));
        assert_eq!(
            lockfile
                .closure("unrelated", "0.1.0", &installation)
                .unwrap()
                .unwrap()
                .len(),
            1
        );
        let error = lockfile
            .closure("tool", "0.1.0", &installation)
            .unwrap_err();
        assert!(error.find_source::<ReadFileError>().is_some());
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
        let text = "\
[[package]]
name = \"tool\"
version = \"0.1.0\"
dependencies = [\"dup 1.0.0\"]

[[package]]
name = \"dup\"
version = \"1.0.0\"
dependencies = [\"from-path\"]

[[package]]
name = \"dup\"
version = \"1.0.0\"
source = \"registry+https://example.invalid\"
dependencies = [\"from-registry\"]

[[package]]
name = \"from-path\"
version = \"1.0.0\"

[[package]]
name = \"from-registry\"
version = \"1.0.0\"
source = \"registry+https://example.invalid\"
";
        let closure = closure_of(text, "tool", "0.1.0");
        assert_eq!(closure.keys().collect::<Vec<_>>(), vec!["dup", "from-path"]);
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
                .closure("tool", "1.0.0", &InstallationGraph::default())
                .unwrap()
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
                .closure("tool", "1.0.0", &InstallationGraph::default())
                .unwrap()
                .is_none()
        );
    }

    /// A dependency naming a source follows only the entry from that source.
    ///
    /// Cargo spells the source out precisely when the name and version do not
    /// identify one entry, so ignoring it would walk into the wrong package.
    #[test]
    fn a_dependency_naming_a_source_selects_that_source() {
        let text = "\
[[package]]
name = \"tool\"
version = \"0.1.0\"
dependencies = [\"dup 1.0.0 (git+https://example.invalid/dup)\"]

[[package]]
name = \"dup\"
version = \"1.0.0\"
source = \"registry+https://example.invalid\"
dependencies = [\"from-registry\"]

[[package]]
name = \"dup\"
version = \"1.0.0\"
source = \"git+https://example.invalid/dup\"
dependencies = [\"from-git\"]

[[package]]
name = \"from-registry\"
version = \"1.0.0\"
source = \"registry+https://example.invalid\"

[[package]]
name = \"from-git\"
version = \"1.0.0\"
source = \"registry+https://example.invalid\"
";
        let closure = closure_of(text, "tool", "0.1.0");
        assert_eq!(
            closure.keys().collect::<Vec<_>>(),
            vec!["dup", "from-git"],
            "{closure:?}"
        );
        let dup = closure.get("dup").unwrap();
        assert_eq!(
            dup.iter().collect::<Vec<_>>(),
            vec!["1.0.0 (git+https://example.invalid/dup)"]
        );
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
source = \"registry+https://example.invalid\"
";
        let patched = plain.replace("example.invalid", "elsewhere.invalid");
        let changes = closure_changes(
            &closure_of(plain, "tool", "0.1.0"),
            &closure_of(&patched, "tool", "0.1.0"),
        );
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

    #[test]
    fn a_malformed_lockfile_is_rejected() {
        for text in [
            "not = = toml",
            "[[package]]\nversion = \"1.0.0\"\n",
            "[[package]]\nname = \"a\"\n",
            "[[package]]\nname = \"a\"\nversion = \"1.0.0\"\ndependencies = \"b\"\n",
            "[[package]]\nname = \"a\"\nversion = \"1.0.0\"\ndependencies = [1]\n",
            "[[package]]\nname = \"a\"\nversion = \"1.0.0\"\ndependencies = [\"\"]\n",
            "[[package]]\nname = \"a\"\nversion = \"1.0.0\"\ndependencies = [\"b 1.0.0 source\"]\n",
            "[[package]]\nname = \"a\"\nversion = \"1.0.0\"\n\
dependencies = [\"b 1.0.0 (source) extra\"]\n",
        ] {
            let error = Lockfile::parse(text, LABEL).unwrap_err();
            assert!(error.find_source::<MalformedLockfileError>().is_some());
        }
    }
}
