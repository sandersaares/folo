// Manifest parsing for versions, packaging rules, members, and pins.

use std::borrow::Cow;
use std::collections::{BTreeMap, HashSet};
use std::error::Error;
use std::path::{Component, MAIN_SEPARATOR, Path, PathBuf};
use std::sync::Arc;
use std::{fmt, fs};

use ignore::overrides::{Override, OverrideBuilder};
use ohno::AppError;
use semver::{Version, VersionReq};
use toml_edit::{DocumentMut, Item, TableLike, Value};

use crate::git::{join_git_rel, os_path};
use crate::inherited::{InheritedKeys, collect_inherited_keys, is_workspace_inherit};
use crate::packaging::PackagingRules;
use crate::text::short_type_name;
use crate::{InvalidMemberPatternError, InvalidVersionError, ParseTomlError};

/// Parsed facts about one package manifest.
#[derive(Clone, Debug)]
pub(crate) struct PackageManifest {
    pub(crate) name: String,
    pub(crate) version: Version,
    pub(crate) directory: String,
    pub(crate) packaging: PackagingRules,
    pub(crate) inherited: InheritedKeys,
    pub(crate) publish: bool,
    /// Dependency paths declared by this package, relative to its own directory.
    pub(crate) path_dependencies: Vec<String>,
    /// Dependency paths this package inherits, relative to the workspace root.
    ///
    /// `[workspace.dependencies]` declares its paths relative to the workspace
    /// root, so these cannot be joined onto the member directory the way a
    /// locally declared path is.
    pub(crate) inherited_path_dependencies: Vec<String>,
    /// Effective normal and build declarations used to filter locked workspace edges.
    pub(crate) installation_dependencies: InstallationDependencies,
    /// Packaged files named by a manifest key, package-relative.
    ///
    /// Cargo packs the file named by `readme` or `license-file` into the package
    /// root, so released content is not confined to the tree beneath the
    /// package.
    pub(crate) resource_paths: Vec<String>,
    /// Packaged files this package inherits, relative to the workspace root.
    ///
    /// `[workspace.package]` declares its paths relative to the workspace root,
    /// so a shared README several members inherit is released content for each of
    /// them without living in any of their directories.
    pub(crate) inherited_resource_paths: Vec<String>,
    /// Whether Cargo picks this package's README by probing its directory.
    ///
    /// A manifest that declares no `readme` still releases the README beside it,
    /// because Cargo probes the package directory for its default names and packs
    /// the first that exists. Which name that is depends on what the end being
    /// examined holds, so only the choice to probe is recorded here.
    pub(crate) auto_readme: bool,
    /// How Cargo discovers installable binary targets for this package.
    pub(crate) targets: TargetDiscovery,
}

impl PackageManifest {
    pub(crate) fn identity(&self) -> PackageIdentity {
        PackageIdentity {
            name: self.name.clone(),
            version: self.version.clone(),
        }
    }
}

/// Manifest controls for discovering installable binary targets.
///
/// Historical snapshots cannot ask Cargo about an old tree, so they combine
/// these controls with that tree's paths to reconstruct whether the packaged
/// lockfile's dependency closure is operationally relevant.
/// Ref: docs/design.md, "Relevant lockfile closures".
#[derive(Clone, Copy, Debug)]
pub(crate) struct TargetDiscovery {
    explicit: bool,
    autobins: bool,
}

impl TargetDiscovery {
    /// Whether the manifest or tracked file paths define an installable binary.
    pub(crate) fn has_lockfile_target<'a>(
        self,
        package_paths: impl IntoIterator<Item = &'a str>,
        case: PathCase,
    ) -> bool {
        self.explicit
            || (self.autobins
                && package_paths
                    .into_iter()
                    .any(|path| is_auto_binary(path, case)))
    }
}

impl Default for TargetDiscovery {
    fn default() -> Self {
        Self {
            // Cargo discovers conventional targets unless the package opts out.
            // Ref: Cargo reference, "Target auto-discovery".
            explicit: false,
            autobins: true,
        }
    }
}

/// An effective dependency declaration needed when installing a package.
///
/// Cargo.lock merges workspace members' development dependencies into their
/// edges. Matching normal and build declarations by package name, requirement,
/// and source recovers installation edges without resolving dependencies.
/// Ref: docs/implementation.md, "Lockfile closures".
#[derive(Clone, Debug)]
pub(crate) struct InstallationDependency {
    pub(crate) name: String,
    /// Path and Git declarations may omit a registry version constraint.
    pub(crate) requirement: Option<VersionReq>,
    pub(crate) source: DependencySource,
}

impl InstallationDependency {
    pub(crate) fn matches_package(&self, name: &str, version: &Version) -> bool {
        self.name == name
            && match &self.source {
                DependencySource::Path(identity) => {
                    identity.name == name && &identity.version == version
                }
                _ => true,
            }
            && self
                .requirement
                .as_ref()
                .is_none_or(|requirement| requirement.matches(version))
    }
}

/// Installation declarations, or a deferred error interpreting them.
///
/// Library release assessment does not consume installation facts. Historical
/// declaration errors therefore belong to the binary closure that needs them,
/// not to workspace discovery or unrelated member classification.
#[derive(Clone, Debug)]
pub(crate) enum InstallationDependencies {
    Parsed(Vec<InstallationDependency>),
    Invalid(InstallationError),
}

impl Default for InstallationDependencies {
    fn default() -> Self {
        Self::Parsed(Vec::new())
    }
}

impl From<Vec<InstallationDependency>> for InstallationDependencies {
    fn from(declarations: Vec<InstallationDependency>) -> Self {
        Self::Parsed(declarations)
    }
}

impl From<Result<Vec<InstallationDependency>, AppError>> for InstallationDependencies {
    fn from(result: Result<Vec<InstallationDependency>, AppError>) -> Self {
        match result {
            Ok(declarations) => Self::Parsed(declarations),
            Err(error) => Self::Invalid(installation_error(error)),
        }
    }
}

/// Shared original cause retained until installation facts are actually needed.
pub(crate) type InstallationError = Arc<dyn Error + Send + Sync>;

pub(crate) fn installation_error(error: AppError) -> InstallationError {
    let error: Box<dyn Error + Send + Sync> = error.into();
    Arc::from(error)
}

/// The exact package identity read from a path dependency's manifest.
///
/// A Cargo requirement can accept several path packages in the lockfile. The
/// referenced manifest, rather than that range, determines which one is used.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PackageIdentity {
    pub(crate) name: String,
    pub(crate) version: Version,
}

/// A path declaration whose target identity an endpoint must read.
///
/// Local declarations retain their package directory in that endpoint's path
/// space. Inherited dependencies and root patches are workspace-relative and
/// therefore have no package-directory override.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct DependencyPath {
    pub(crate) path: String,
    pub(crate) package_directory: Option<String>,
}

impl DependencyPath {
    /// Resolves the declaration into the repository's tracked path space.
    ///
    /// Current package directories are workspace-relative; historical package
    /// directories are already repository-relative. The caller supplies the
    /// corresponding prefix rather than reinterpreting the declaration's base.
    pub(crate) fn directory(
        &self,
        repository_root: &Path,
        workspace_prefix: &str,
        package_prefix: &str,
    ) -> Option<String> {
        let path = Path::new(&self.path);
        let directory = if path.is_absolute() {
            workspace_relative_path(repository_root, path)?
        } else {
            let base = self.package_directory.as_ref().map_or_else(
                || workspace_prefix.to_owned(),
                |directory| join_git_rel(package_prefix, directory),
            );
            join_git_rel(&base, &to_git_separators(&self.path, MAIN_SEPARATOR))
        };
        (directory != ".." && !directory.starts_with("../")).then_some(directory)
    }
}

/// The source identity of an effective dependency declaration.
///
/// Cargo.lock does not store paths: a path package is identified there by its
/// source-less name and version. Git references remain distinct while their
/// resolved commit is compared as released content, not as a declaration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum DependencySource {
    Path(PackageIdentity),
    UnresolvedPath(DependencyPath),
    Registry(String),
    NamedRegistry(String),
    Git {
        repository: String,
        reference: GitReference,
    },
}

impl DependencySource {
    pub(crate) fn matches_locked(
        &self,
        source: Option<&str>,
        registries: &BTreeMap<String, String>,
    ) -> Option<bool> {
        if source.is_some_and(|source| {
            locked_registry_index(source).is_none() && !source.starts_with("git+")
        }) {
            return None;
        }
        Some(match self {
            Self::Path(_) => source.is_none(),
            Self::UnresolvedPath(_) => return None,
            Self::Registry(index) => match source.and_then(locked_registry_index) {
                Some(locked) => same_registry_index(index, locked)?,
                None => false,
            },
            Self::NamedRegistry(name) => match source.and_then(locked_registry_index) {
                Some(locked) => same_registry_index(registries.get(name)?, locked)?,
                None => {
                    registries.get(name)?;
                    false
                }
            },
            Self::Git {
                repository,
                reference,
            } => {
                let Some(source) = source.and_then(|source| source.strip_prefix("git+")) else {
                    return Some(false);
                };
                let (locked_repository, locked_reference) = parse_locked_git_source(source)?;
                reference == &locked_reference && same_source_url(repository, locked_repository)?
            }
        })
    }

    pub(crate) fn accepts_patch(
        &self,
        origin: &str,
        registries: &BTreeMap<String, String>,
    ) -> Option<bool> {
        let origin = if origin == "crates-io" {
            CRATES_IO_INDEX
        } else {
            registries.get(origin).map_or(origin, String::as_str)
        };
        Some(match self {
            Self::Path(_) | Self::UnresolvedPath(_) => false,
            Self::Registry(index) => same_registry_index(index, origin)?,
            Self::NamedRegistry(name) => same_registry_index(registries.get(name)?, origin)?,
            Self::Git { repository, .. } => same_source_url(repository, origin)?,
        })
    }
}

/// Git's requested reference, separate from its resolved lockfile commit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum GitReference {
    Default,
    Branch(String),
    Tag(String),
    Rev(String),
}

/// A workspace-root replacement declaration and the source it applies to.
///
/// Path replacements use the same exact target-identity lookup as direct paths,
/// so another source-less package cannot stand in for the referenced directory.
/// Invalid declarations retain their target name so unrelated closures can ignore them.
#[derive(Clone, Debug)]
pub(crate) struct DependencyPatch {
    pub(crate) origin: String,
    pub(crate) name: String,
    pub(crate) replacement: Result<InstallationDependency, InstallationError>,
}

/// Cargo's canonical crates.io source identity, including sparse registry usage.
const CRATES_IO_INDEX: &str = "https://github.com/rust-lang/crates.io-index";

/// Workspace member patterns from the root manifest, compiled for repeated queries.
///
/// A historical snapshot tests every discovered manifest against the same
/// patterns, so the matchers are built once when the root manifest is parsed
/// rather than per candidate directory.
#[derive(Clone, Debug, Default)]
pub(crate) struct WorkspaceMembers {
    members: Vec<MemberPattern>,
    exclude: Vec<MemberPattern>,
}

/// How the filesystem hosting a workspace resolves path case.
///
/// Cargo opens member directories through the filesystem while Git reports the
/// spelling recorded in the tree, so member matching only agrees with Cargo when
/// it applies the same case rules. Case sensitivity is a property of the volume
/// and directory rather than of the operating system, so it is probed.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum PathCase {
    /// Two spellings that differ in case name different paths.
    #[default]
    Sensitive,
    /// Two spellings that differ only in case name the same path.
    Insensitive,
}

impl PathCase {
    /// Probes how the filesystem holding `dir` resolves path case.
    ///
    /// The probe re-opens an existing entry under a case-flipped spelling, so it
    /// writes nothing and works on a read-only checkout. A directory that cannot
    /// be read, or that holds no entry whose flipped spelling is unambiguous,
    /// yields the stricter answer, which never widens member matching.
    pub(crate) fn probe(dir: &Path) -> Self {
        let Ok(entries) = fs::read_dir(dir) else {
            return Self::Sensitive;
        };
        let names: Vec<String> = entries
            .flatten()
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .collect();
        let present: HashSet<&str> = names.iter().map(String::as_str).collect();
        for name in &names {
            let flipped = flip_case(name);
            // An entry that is already present under both spellings proves
            // nothing, and a name without cased characters cannot be flipped.
            if flipped == *name || present.contains(flipped.as_str()) {
                continue;
            }
            return if dir.join(&flipped).exists() {
                Self::Insensitive
            } else {
                Self::Sensitive
            };
        }
        Self::Sensitive
    }

    /// Whether two path components name the same path under these case rules.
    pub(crate) fn same_path(self, left: &str, right: &str) -> bool {
        match self {
            Self::Sensitive => left == right,
            // Compared as lowercase character streams rather than lowercase
            // strings: this runs once per member pattern per candidate
            // directory, and the streams give the same answer as
            // `to_lowercase` without allocating for either side.
            Self::Insensitive => left
                .chars()
                .flat_map(char::to_lowercase)
                .eq(right.chars().flat_map(char::to_lowercase)),
        }
    }
}

/// Rewrites a manifest-declared relative path into Git's `/`-separated form.
///
/// Cargo resolves a manifest-declared path through the host's own path rules, so
/// a backslash separates components where the platform says it does and is an
/// ordinary file name character everywhere else. Normalising only the native
/// separator keeps the resolved path equal to the one Cargo would open, where
/// normalising a backslash unconditionally would resolve a legal Unix name such
/// as `odd\name.md` to a different path and mis-attribute its content.
/// The separator is a parameter so both spellings are reachable from a test on
/// either host.
pub(crate) fn to_git_separators(relative: &str, native_separator: char) -> Cow<'_, str> {
    if native_separator == '/' {
        Cow::Borrowed(relative)
    } else {
        Cow::Owned(relative.replace(native_separator, "/"))
    }
}

fn flip_case(name: &str) -> String {
    name.chars()
        .flat_map(|c| {
            if c.is_uppercase() {
                c.to_lowercase().collect::<Vec<_>>()
            } else {
                c.to_uppercase().collect()
            }
        })
        .collect()
}

pub(crate) fn parse_document(path: &Path, content: &str) -> Result<DocumentMut, AppError> {
    content
        .parse()
        .map_err(|error| ParseTomlError::caused_by(path, error).into())
}

/// Reads a `[package]` manifest, resolving what it inherits from the root.
///
/// `manifest_path` is repository-relative and `/`-separated, as Git reports it,
/// so the parsed manifest is in one path space from the moment it exists rather
/// than needing a caller to correct it afterwards.
pub(crate) fn parse_package_manifest(
    content: &str,
    manifest_path: &str,
    workspace: &WorkspaceInherit<'_>,
) -> Result<Option<PackageManifest>, AppError> {
    let path = Path::new(manifest_path);
    let doc = parse_document(path, content)?;
    package_manifest_from_document(&doc, manifest_path, workspace)
}

/// Extracts package facts from an already parsed manifest document.
pub(crate) fn package_manifest_from_document(
    doc: &DocumentMut,
    manifest_path: &str,
    workspace: &WorkspaceInherit<'_>,
) -> Result<Option<PackageManifest>, AppError> {
    // A manifest without a complete `[package]` identity is not something Cargo
    // would publish: it is either a virtual workspace root or a member whose
    // version is inherited from a root that does not declare one. Both are
    // ordinary states of historical trees, so they yield "not a package" rather
    // than an error. Malformed values that Cargo would reject still error below.
    let Some(package) = doc.get("package").and_then(Item::as_table_like) else {
        return Ok(None);
    };
    let Some(PackageIdentity { name, version }) = package_identity_from_document(doc, workspace)?
    else {
        return Ok(None);
    };
    let directory = directory_of(manifest_path);
    let (resource_paths, inherited_resource_paths, auto_readme) =
        resource_paths(package, workspace);
    let targets = target_discovery(doc, package);
    Ok(Some(PackageManifest {
        name,
        version,
        directory,
        packaging: packaging_from_package(package, workspace)?,
        inherited: collect_inherited_keys(doc),
        publish: publish_allowed(package, workspace),
        path_dependencies: path_dependencies(doc),
        inherited_path_dependencies: inherited_path_dependencies(doc, workspace),
        installation_dependencies: installation_dependencies(
            doc,
            workspace,
            Path::new(manifest_path),
        )
        .into(),
        resource_paths,
        inherited_resource_paths,
        auto_readme,
        targets,
    }))
}

/// Reads only a package's declared identity, without unrelated packaging facts.
fn package_identity_from_document(
    doc: &DocumentMut,
    workspace: &WorkspaceInherit<'_>,
) -> Result<Option<PackageIdentity>, AppError> {
    let Some(package) = doc.get("package").and_then(Item::as_table_like) else {
        return Ok(None);
    };
    let Some(name) = package.get("name").and_then(Item::as_str) else {
        return Ok(None);
    };
    let Some(version_item) = package.get("version") else {
        return Ok(None);
    };
    let version = if is_workspace_inherit(version_item) {
        workspace.package_version()
    } else {
        version_item.as_str()
    };
    let Some(version) = version else {
        return Ok(None);
    };
    Ok(Some(PackageIdentity {
        name: name.to_owned(),
        version: version
            .parse()
            .map_err(|error| InvalidVersionError::caused_by(name, version, error))?,
    }))
}

/// Identifies a tracked path package and, when needed, its owning workspace.
///
/// Excluded packages may belong to a nested workspace. Their version must not
/// inherit from the caller's workspace merely because it supplies the dependency.
pub(crate) fn path_package_identity(
    manifest_path: &str,
    case: PathCase,
    mut read: impl FnMut(&str) -> Result<Option<DocumentMut>, AppError>,
) -> Result<Option<PackageIdentity>, AppError> {
    // Cargo requires this filename for both packages and workspace roots.
    const MANIFEST_FILE_NAME: &str = "Cargo.toml";

    let Some(document) = read(manifest_path)? else {
        return Ok(None);
    };
    if let Some(identity) = package_identity_from_document(&document, &WorkspaceInherit::default())?
    {
        return Ok(Some(identity));
    }
    let Some(package) = document.get("package").and_then(Item::as_table_like) else {
        return Ok(None);
    };
    let package_directory = directory_of(manifest_path);
    if let Some(workspace) = package.get("workspace").and_then(Item::as_str) {
        let workspace = join_git_rel(
            &package_directory,
            &to_git_separators(workspace, MAIN_SEPARATOR),
        );
        let Some(root) = read(&join_git_rel(&workspace, MANIFEST_FILE_NAME))? else {
            return Ok(None);
        };
        return package_identity_from_document(&document, &WorkspaceInherit::from_root(&root));
    }
    let mut directory = package_directory.clone();
    loop {
        let path = join_git_rel(&directory, MANIFEST_FILE_NAME);
        if let Some(root) = read(&path)?
            && root.get("workspace").is_some()
        {
            let members = parse_workspace_members(&root.to_string(), Path::new(&path), case)?;
            let relative = relativize_directory(&package_directory, &directory);
            if !relative.is_empty() && is_workspace_excluded(relative, &members) {
                return Ok(None);
            }
            return package_identity_from_document(&document, &WorkspaceInherit::from_root(&root));
        }
        if directory.is_empty() {
            return Ok(None);
        }
        let parent = directory_of(&directory);
        debug_assert!(parent.len() < directory.len());
        directory = parent;
    }
}

fn relativize_directory<'a>(directory: &'a str, ancestor: &str) -> &'a str {
    directory
        .strip_prefix(ancestor)
        .unwrap_or(directory)
        .trim_start_matches('/')
}

/// Conventional source path for a package's default binary.
const AUTO_BINARY_MAIN: &str = "src/main.rs";
/// Directory in which Cargo discovers additional binary targets.
const AUTO_BINARY_DIR: &str = "src/bin";
/// Rust source suffix Cargo recognises during target auto-discovery.
const RUST_SOURCE_SUFFIX: &str = ".rs";
/// File name Cargo recognises as a directory target's entry point.
const TARGET_MAIN_FILE: &str = "main.rs";

fn target_discovery(doc: &DocumentMut, package: &dyn TableLike) -> TargetDiscovery {
    let explicit = doc
        .get("bin")
        .and_then(Item::as_array_of_tables)
        .is_some_and(|targets| !targets.is_empty());
    TargetDiscovery {
        explicit,
        autobins: package
            .get("autobins")
            .and_then(Item::as_bool)
            .unwrap_or(true),
    }
}

fn is_auto_binary(path: &str, case: PathCase) -> bool {
    case.same_path(path, AUTO_BINARY_MAIN) || is_auto_directory_target(path, AUTO_BINARY_DIR, case)
}

/// Whether `path` follows either auto-discovered layout beneath `directory`.
fn is_auto_directory_target(path: &str, directory: &str, case: PathCase) -> bool {
    let path_parts: Vec<&str> = path.split('/').collect();
    let directory_parts: Vec<&str> = directory.split('/').collect();
    match (directory_parts.as_slice(), path_parts.as_slice()) {
        ([parent, directory], [held_parent, held_directory, file]) => {
            case.same_path(parent, held_parent)
                && case.same_path(directory, held_directory)
                && is_visible_target_name(file)
                && file.ends_with(RUST_SOURCE_SUFFIX)
        }
        ([parent, directory], [held_parent, held_directory, name, main]) => {
            case.same_path(parent, held_parent)
                && case.same_path(directory, held_directory)
                && is_visible_target_name(name)
                && case.same_path(main, TARGET_MAIN_FILE)
        }
        _ => false,
    }
}

/// Cargo excludes dotfile entries from automatic target discovery.
fn is_visible_target_name(name: &str) -> bool {
    !name.starts_with('.')
}

/// `[package]` keys naming a file Cargo packs alongside the sources.
///
/// Cargo rewrites both to a bare file name when it normalises a manifest for
/// packaging, and packs the named file regardless of `include` and `exclude`.
const RESOURCE_KEYS: &[&str] = &["readme", "license-file"];

/// The `[package]` key naming the README.
///
/// Singled out among [`RESOURCE_KEYS`] because Cargo also accepts a boolean
/// there and derives the value from the package directory when it is absent.
const README_KEY: &str = "readme";

/// The name Cargo prefers when it picks a README itself.
const PRIMARY_README: &str = "README.md";

/// The names Cargo probes for, in order, when a manifest declares no `readme`.
///
/// Ref: Cargo's `default_readme_from_package_root`.
pub(crate) const DEFAULT_README_FILES: &[&str] = &[PRIMARY_README, "README.txt", "README"];

/// Collects the files Cargo packs because a `[package]` key names them.
///
/// A locally declared value is relative to the package directory while an
/// inherited one is relative to the workspace root, so the two are returned
/// separately for the caller to resolve against the right base. The third
/// element reports whether Cargo picks the README by probing the package
/// directory, which it does only when the key is absent altogether: `readme =
/// false` deliberately names no file.
fn resource_paths(
    package: &dyn TableLike,
    workspace: &WorkspaceInherit<'_>,
) -> (Vec<String>, Vec<String>, bool) {
    let mut local = Vec::new();
    let mut inherited = Vec::new();
    let mut auto_readme = false;
    for key in RESOURCE_KEYS {
        let Some(item) = package.get(key) else {
            auto_readme |= *key == README_KEY;
            continue;
        };
        let (value, destination) = if is_workspace_inherit(item) {
            match workspace.package_key(key) {
                Some(value) => (value, &mut inherited),
                None => continue,
            }
        } else {
            (item, &mut local)
        };
        if let Some(path) = resource_value(key, value) {
            destination.push(path.to_string());
        }
    }
    (local, inherited, auto_readme)
}

/// The file name a resource key's value names, if any.
///
/// `readme = true` selects Cargo's preferred default name and `readme = false`
/// names no file, so a boolean there is not merely a value of the wrong type.
fn resource_value<'a>(key: &str, item: &'a Item) -> Option<&'a str> {
    if let Some(enabled) = item.as_bool().filter(|_| key == README_KEY) {
        return enabled.then_some(PRIMARY_README);
    }
    item.as_str()
}

/// The `[workspace.package]` and `[workspace.dependencies]` tables a member inherits from.
///
/// Historical snapshots parse member manifests without Cargo's help, so every
/// `.workspace = true` key has to be resolved against the root manifest of the
/// same commit or the member would be read with Cargo's defaults instead of the
/// values it actually declares.
#[derive(Clone, Copy, Default)]
pub(crate) struct WorkspaceInherit<'a> {
    package: Option<&'a dyn TableLike>,
    dependencies: Option<&'a dyn TableLike>,
}

impl<'a> WorkspaceInherit<'a> {
    pub(crate) fn from_root(root: &'a DocumentMut) -> Self {
        let workspace = root.get("workspace").and_then(Item::as_table_like);
        Self {
            package: workspace
                .and_then(|workspace| workspace.get("package"))
                .and_then(Item::as_table_like),
            dependencies: workspace
                .and_then(|workspace| workspace.get("dependencies"))
                .and_then(Item::as_table_like),
        }
    }

    fn package_version(&self) -> Option<&'a str> {
        self.package_key("version").and_then(Item::as_str)
    }

    fn package_key(&self, key: &str) -> Option<&'a Item> {
        let package = self.package?;
        package.get(key)
    }

    fn dependency(&self, name: &str) -> Option<&'a dyn TableLike> {
        self.dependency_item(name).and_then(Item::as_table_like)
    }

    fn dependency_item(&self, name: &str) -> Option<&'a Item> {
        self.dependencies?.get(name)
    }
}

/// Resolves dependency inheritance before discarding development-only edges.
fn installation_dependencies(
    doc: &DocumentMut,
    workspace: &WorkspaceInherit<'_>,
    path: &Path,
) -> Result<Vec<InstallationDependency>, AppError> {
    let mut declarations = Vec::new();
    for_each_dependency_table(doc.as_table(), &mut |kind, dependencies| {
        if kind == "dev-dependencies" {
            return;
        }
        for (alias, dependency) in dependencies.iter() {
            let inherited = is_workspace_inherit(dependency);
            let dependency = if inherited {
                workspace.dependency_item(alias)
            } else {
                Some(dependency)
            };
            declarations.push(parse_installation_dependency(
                alias, dependency, path, inherited,
            ));
        }
    });
    declarations.into_iter().collect()
}

fn parse_installation_dependency(
    alias: &str,
    dependency: Option<&Item>,
    path: &Path,
    inherited: bool,
) -> Result<InstallationDependency, AppError> {
    let dependency = dependency.ok_or_else(|| ParseTomlError::new(path))?;
    let table = dependency.as_table_like();
    let name = table
        .and_then(|table| table.get("package"))
        .and_then(Item::as_str)
        .unwrap_or(alias);
    let requirement = dependency
        .as_str()
        .or_else(|| {
            table
                .and_then(|table| table.get("version"))
                .and_then(Item::as_str)
        })
        .map(VersionReq::parse)
        .transpose()
        .map_err(|error| ParseTomlError::caused_by(path, error))?;
    Ok(InstallationDependency {
        name: name.to_owned(),
        requirement,
        source: dependency_source(table, (!inherited).then_some(path)),
    })
}

fn dependency_source(
    table: Option<&dyn TableLike>,
    declaring_manifest: Option<&Path>,
) -> DependencySource {
    let field = |key| {
        table
            .and_then(|table| table.get(key))
            .and_then(Item::as_str)
    };
    if let Some(path) = field("path") {
        return DependencySource::UnresolvedPath(DependencyPath {
            path: path.to_owned(),
            package_directory: declaring_manifest
                .map(|manifest| manifest.parent().map_or_else(String::new, os_path)),
        });
    }
    if let Some(repository) = field("git") {
        let reference = if let Some(branch) = field("branch") {
            GitReference::Branch(branch.to_owned())
        } else if let Some(tag) = field("tag") {
            GitReference::Tag(tag.to_owned())
        } else if let Some(rev) = field("rev") {
            GitReference::Rev(rev.to_owned())
        } else {
            GitReference::Default
        };
        return DependencySource::Git {
            repository: repository.to_owned(),
            reference,
        };
    }
    if let Some(index) = field("registry-index") {
        return DependencySource::Registry(index.to_owned());
    }
    if let Some(name) = field("registry").filter(|name| *name != "crates-io") {
        return DependencySource::NamedRegistry(name.to_owned());
    }
    DependencySource::Registry(CRATES_IO_INDEX.to_owned())
}

/// Reads replacement declarations from the endpoint's workspace-root patch table.
pub(crate) fn installation_patches(root: &DocumentMut) -> Vec<DependencyPatch> {
    let Some(patches) = root.get("patch").and_then(Item::as_table_like) else {
        return Vec::new();
    };
    let mut resolved = Vec::new();
    for (origin, dependencies) in patches.iter() {
        let Some(dependencies) = dependencies.as_table_like() else {
            continue;
        };
        for (alias, item) in dependencies.iter() {
            let name = item
                .as_table_like()
                .and_then(|table| table.get("package"))
                .and_then(Item::as_str)
                .unwrap_or(alias);
            let replacement =
                parse_installation_dependency(alias, Some(item), Path::new("Cargo.toml"), true)
                    .map_err(installation_error);
            resolved.push(DependencyPatch {
                origin: origin.to_owned(),
                name: name.to_owned(),
                replacement,
            });
        }
    }
    resolved
}

/// Cargo configuration candidates, from repository root to workspace directory.
///
/// Cargo prefers the extensionless filename when both names are present.
pub(crate) fn cargo_config_paths(workspace_prefix: &str) -> Vec<[String; 2]> {
    let mut directories = vec![String::new()];
    let mut directory = String::new();
    for component in workspace_prefix.split('/').filter(|part| !part.is_empty()) {
        directory = join_git_rel(&directory, component);
        directories.push(directory.clone());
    }
    directories
        .into_iter()
        .map(|directory| {
            [
                join_git_rel(&directory, ".cargo/config"),
                join_git_rel(&directory, ".cargo/config.toml"),
            ]
        })
        .collect()
}

/// Overlays registry indices declared in one Cargo configuration file.
pub(crate) fn collect_registry_indices(doc: &DocumentMut, indices: &mut BTreeMap<String, String>) {
    let Some(registries) = doc.get("registries").and_then(Item::as_table_like) else {
        return;
    };
    for (name, entry) in registries.iter() {
        if let Some(index) = entry
            .as_table_like()
            .and_then(|entry| entry.get("index"))
            .and_then(Item::as_str)
        {
            indices.insert(name.to_owned(), index.to_owned());
        }
    }
}

/// The index spelling Cargo uses for registry and sparse source identifiers.
pub(crate) fn locked_registry_index(source: &str) -> Option<&str> {
    source
        .strip_prefix("registry+")
        .filter(|index| !index.starts_with("sparse+"))
        .or_else(|| source.starts_with("sparse+").then_some(source))
}

fn same_source_url(left: &str, right: &str) -> Option<bool> {
    if left == right {
        return Some(true);
    }
    Some(canonical_source_url(left)? == canonical_source_url(right)?)
}

fn same_registry_index(left: &str, right: &str) -> Option<bool> {
    if left.starts_with("sparse+") != right.starts_with("sparse+") {
        return Some(false);
    }
    same_source_url(left, right)
}

/// Applies Cargo's source canonicalization to already URL-normalized input.
///
/// General URL parsing belongs to Cargo. When nonidentical spellings need URL
/// normalization outside this supported subset, comparison is unavailable rather
/// than guessing that a legitimate installation edge is absent. Exact URL
/// spellings remain comparable. Ref: Cargo's `util::CanonicalUrl`.
fn canonical_source_url(url: &str) -> Option<String> {
    // URL serialization omits default HTTP transport ports.
    const HTTP_PORT: u16 = 80;
    const HTTPS_PORT: u16 = 443;
    // Canonical IPv4 addresses retain every decimal octet.
    const IPV4_OCTETS: usize = 4;

    let (scheme, remainder) = url.split_once("://")?;
    if !matches!(
        scheme,
        "https" | "http" | "ssh" | "git" | "file" | "sparse+https" | "sparse+http"
    ) || !url.is_ascii()
        || url.chars().any(|ch| {
            ch.is_ascii_control() || ch.is_ascii_whitespace() || "%\\?#\"<>`{}|^[]".contains(ch)
        })
    {
        return None;
    }
    let (authority, path) = remainder
        .split_once('/')
        .map_or((remainder, ""), |(authority, path)| (authority, path));
    let host_port = authority.rsplit('@').next()?;
    if authority
        .rsplit_once('@')
        .is_some_and(|(credentials, _)| credentials.is_empty() || credentials.ends_with(':'))
    {
        return None;
    }
    let (host, port) = host_port
        .split_once(':')
        .map_or((host_port, None), |(host, port)| (host, Some(port)));
    if host.bytes().any(|byte| byte.is_ascii_uppercase())
        || (host.is_empty() && scheme != "file")
        || (scheme == "file" && !authority.is_empty())
        || path.split('/').any(|part| matches!(part, "." | ".."))
    {
        return None;
    }
    if let Some(port) = port {
        let number = port.parse::<u16>().ok()?;
        // URL parsing removes default HTTP ports and normalizes numeric spelling.
        if number.to_string() != port
            || matches!(
                (scheme, number),
                ("http", HTTP_PORT) | ("https", HTTPS_PORT)
            )
            || (host == "github.com" && number == HTTPS_PORT)
        {
            return None;
        }
    }
    let last_label = host.trim_end_matches('.').rsplit('.').next()?;
    if !last_label.is_empty()
        && (last_label.bytes().all(|byte| byte.is_ascii_digit()) || last_label.starts_with("0x"))
    {
        let octets: Vec<_> = host.split('.').collect();
        if octets.len() != IPV4_OCTETS
            || octets.iter().any(|octet| {
                octet
                    .parse::<u8>()
                    .ok()
                    .is_none_or(|number| number.to_string() != *octet)
            })
        {
            return None;
        }
    }
    let mut path = path.strip_suffix('/').unwrap_or(path).to_owned();
    let scheme = if !scheme.contains('+') && host == "github.com" {
        path.make_ascii_lowercase();
        "https"
    } else {
        scheme
    };
    if !scheme.contains('+')
        && let Some(stripped) = path.strip_suffix(".git")
    {
        path = stripped.to_owned();
    }
    Some(format!("{scheme}://{authority}/{path}"))
}

fn parse_locked_git_source(source: &str) -> Option<(&str, GitReference)> {
    let source = source.split('#').next()?;
    let Some((repository, query)) = source.split_once('?') else {
        return Some((source, GitReference::Default));
    };
    let (kind, value) = query.split_once('=')?;
    let value = decode_git_reference(value)?;
    let reference = match kind {
        "branch" => GitReference::Branch(value),
        "tag" => GitReference::Tag(value),
        "rev" => GitReference::Rev(value),
        _ => return None,
    };
    Some((repository, reference))
}

fn decode_git_reference(encoded: &str) -> Option<String> {
    // A percent escape encodes a byte as hexadecimal nibbles.
    const HEX_RADIX: u32 = 16;
    const NIBBLE_BITS: u32 = 4;

    let mut bytes = encoded.bytes();
    let mut decoded = Vec::with_capacity(encoded.len());
    while let Some(byte) = bytes.next() {
        decoded.push(match byte {
            b'%' => {
                let high = char::from(bytes.next()?).to_digit(HEX_RADIX)?;
                let low = char::from(bytes.next()?).to_digit(HEX_RADIX)?;
                u8::try_from((high << NIBBLE_BITS) | low).ok()?
            }
            b'+' => b' ',
            other => other,
        });
    }
    String::from_utf8(decoded).ok()
}

/// Collects every `path` a package reaches through `[workspace.dependencies]`.
///
/// Cargo makes an inherited path dependency a member exactly as it does a
/// locally declared one, so historical membership only matches Cargo once these
/// edges are followed as well.
fn inherited_path_dependencies(doc: &DocumentMut, workspace: &WorkspaceInherit<'_>) -> Vec<String> {
    let mut names = Vec::new();
    for_each_dependency_table(
        doc.as_table(),
        &mut |_kind, dependencies: &dyn TableLike| {
            for (name, dependency) in dependencies.iter() {
                if is_workspace_inherit(dependency) {
                    names.push(name.to_string());
                }
            }
        },
    );
    names
        .iter()
        .filter_map(|name| {
            workspace
                .dependency(name)
                .and_then(|dependency| dependency.get("path"))
                .and_then(Item::as_str)
                .map(ToOwned::to_owned)
        })
        .collect()
}

/// Collects every `path` value declared by a dependency of this package.
///
/// Cargo makes a path dependency that lives inside the workspace directory a
/// member even when the `members` list does not name it, so historical
/// membership can only match Cargo once these edges are known.
fn path_dependencies(doc: &DocumentMut) -> Vec<String> {
    let mut paths = Vec::new();
    for_each_dependency_table(
        doc.as_table(),
        &mut |_kind, dependencies: &dyn TableLike| {
            for (_, dependency) in dependencies.iter() {
                let Some(dependency) = dependency.as_table_like() else {
                    continue;
                };
                if let Some(path) = dependency.get("path").and_then(Item::as_str) {
                    paths.push(path.to_string());
                }
            }
        },
    );
    paths
}

/// Visits every table in `manifest` that Cargo reads dependencies from.
///
/// Cargo recognises dependency tables at the manifest root and one level below
/// `[target.<spec>]`, and nowhere else. Matching on the table name at any depth
/// would also collect look-alikes such as `[package.metadata.dependencies]`,
/// which carry no dependency semantics, and would then attribute workspace
/// membership and inherited keys to entries that are not dependencies at all.
/// The visitor receives the table name so consumers can preserve dependency
/// kinds when Cargo's packaging behavior differs between them.
pub(crate) fn for_each_dependency_table(
    manifest: &dyn TableLike,
    visit: &mut dyn FnMut(&str, &dyn TableLike),
) {
    for_each_dependency_table_with_context(manifest, &mut |_, kind, dependencies| {
        visit(kind, dependencies);
    });
}

/// Visits every dependency table with its manifest location.
pub(crate) fn for_each_dependency_table_with_context(
    manifest: &dyn TableLike,
    visit: &mut dyn FnMut(&str, &str, &dyn TableLike),
) {
    visit_dependency_tables(manifest, "", visit);
    let Some(target) = manifest.get("target").and_then(Item::as_table_like) else {
        return;
    };
    for (target_name, spec) in target.iter() {
        if let Some(spec) = spec.as_table_like() {
            visit_dependency_tables(spec, &format!("target.{target_name}."), visit);
        }
    }
}

fn visit_dependency_tables(
    table: &dyn TableLike,
    prefix: &str,
    visit: &mut dyn FnMut(&str, &str, &dyn TableLike),
) {
    for name in DEPENDENCY_TABLES {
        let Some(actual) = dependency_table_name(table, name) else {
            continue;
        };
        if let Some(dependencies) = table.get(actual).and_then(Item::as_table_like) {
            visit(&format!("{prefix}{actual}"), name, dependencies);
        }
    }
}

/// Selects Cargo's effective spelling, preserving the raw edit location.
///
/// Cargo prefers the hyphenated spelling even when that table is empty. The
/// underscore aliases remain valid in editions before 2024.
pub(crate) fn dependency_table_name<'a>(table: &dyn TableLike, name: &'a str) -> Option<&'a str> {
    if table.contains_key(name) {
        return Some(name);
    }
    let legacy = match name {
        "build-dependencies" => "build_dependencies",
        "dev-dependencies" => "dev_dependencies",
        _ => return None,
    };
    table.contains_key(legacy).then_some(legacy)
}

/// The dependency table names Cargo recognises, at the root and under `[target]`.
pub(crate) const DEPENDENCY_TABLES: &[&str] =
    &["dependencies", "dev-dependencies", "build-dependencies"];

pub(crate) fn parse_workspace_members(
    content: &str,
    path: &Path,
    case: PathCase,
) -> Result<WorkspaceMembers, AppError> {
    let doc = parse_document(path, content)?;
    let Some(workspace) = doc.get("workspace").and_then(Item::as_table_like) else {
        return Ok(WorkspaceMembers::default());
    };
    Ok(WorkspaceMembers {
        members: compile_patterns(&string_array(workspace.get("members")), case)?,
        exclude: compile_patterns(&string_array(workspace.get("exclude")), case)?,
    })
}

/// Whether `dir` (repo-relative, `/` separators) matches a workspace member pattern.
///
/// The directory comes from Git, which separates with `/` on every platform, so
/// a backslash in it is an ordinary character of a file name and is matched as
/// one. Only the pattern, which a manifest author writes, is normalised.
pub(crate) fn is_workspace_member(dir: &str, members: &WorkspaceMembers) -> bool {
    let dir = dir.trim_end_matches('/');
    // A non-virtual root's own package is a member whatever the lists say, so it
    // is decided before them. This is only ever asked about a directory that
    // holds a package manifest, so a virtual root cannot reach it.
    if dir.is_empty() {
        return true;
    }
    if members.exclude.iter().any(|pattern| pattern.matches(dir)) {
        return false;
    }
    // A manifest without a `members` list defines a workspace whose only member
    // is the root package, already handled above. Treating an absent list as
    // "every manifest in the repository" would pull unrelated packages into a
    // historical snapshot.
    members.members.iter().any(|pattern| pattern.matches(dir))
}

/// Whether `dir` (repo-relative, `/` separators) is excluded from the workspace.
///
/// Membership that Cargo derives from a path dependency still honours
/// `exclude`, so that list is queried separately from the `members` patterns.
/// The directory is in Git's path space, as it is for `is_workspace_member`.
pub(crate) fn is_workspace_excluded(dir: &str, members: &WorkspaceMembers) -> bool {
    let dir = dir.trim_end_matches('/');
    members.exclude.iter().any(|pattern| pattern.matches(dir))
}

fn compile_patterns(patterns: &[String], case: PathCase) -> Result<Vec<MemberPattern>, AppError> {
    patterns
        .iter()
        .map(|pattern| MemberPattern::new(pattern, case))
        .collect()
}

/// One compiled `[workspace] members` / `exclude` pattern.
struct MemberPattern {
    literal: String,
    case: PathCase,
    matcher: Override,
}

impl MemberPattern {
    fn new(pattern: &str, case: PathCase) -> Result<Self, AppError> {
        // A member list is authored by hand, and one written on Windows may
        // separate with backslashes, which Cargo accepts there. The directories
        // this is matched against come from Git and are never rewritten, so the
        // normalisation is confined to the pattern.
        let literal = to_git_separators(pattern, MAIN_SEPARATOR).into_owned();
        let mut matcher = OverrideBuilder::new("");
        if case == PathCase::Insensitive {
            matcher
                .case_insensitive(true)
                .map_err(|error| InvalidMemberPatternError::caused_by(&literal, error))?;
        }
        matcher
            .add(&anchored(&literal))
            .map_err(|error| InvalidMemberPatternError::caused_by(&literal, error))?;
        let matcher = matcher
            .build()
            .map_err(|error| InvalidMemberPatternError::caused_by(&literal, error))?;
        Ok(Self {
            literal,
            case,
            matcher,
        })
    }

    fn matches(&self, dir: &str) -> bool {
        if self.case.same_path(&self.literal, dir) {
            return true;
        }
        // `foo/**` in Cargo member lists includes the `foo` directory itself.
        if let Some(prefix) = self.literal.strip_suffix("/**")
            && self.case.same_path(prefix, dir)
        {
            return true;
        }
        self.matcher.matched(dir, true).is_whitelist()
    }
}

// `Override` has no `Clone`, and the compiled matchers are immutable after
// construction, so cloning a member set recompiles its patterns.
impl Clone for MemberPattern {
    fn clone(&self) -> Self {
        Self::new(&self.literal, self.case)
            .expect("this pattern already compiled once, and compilation depends only on the literal and the case rules that are copied here")
    }
}

impl fmt::Debug for MemberPattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Derived rather than spelled out, so a rename cannot leave the label
        // behind.
        f.debug_struct(short_type_name::<Self>())
            .field("literal", &self.literal)
            .finish_non_exhaustive()
    }
}

/// Root-anchors a member pattern for gitignore-style matching.
///
/// Cargo resolves `members` and `exclude` globs against the workspace root, but
/// a gitignore pattern with no separator matches a basename at any depth, so
/// `foo*` would otherwise pull in `packages/foo` and attribute a nested package's
/// files to a workspace that never declared it. A leading `/` restores Cargo's
/// meaning; a pattern that already carries a separator is anchored either way.
fn anchored(literal: &str) -> String {
    match literal.strip_prefix('/') {
        Some(_) => literal.to_string(),
        None => format!("/{literal}"),
    }
}

fn packaging_from_package(
    package: &dyn TableLike,
    workspace: &WorkspaceInherit<'_>,
) -> Result<PackagingRules, AppError> {
    let include = inherited_string_array(package, workspace, "include");
    let exclude = inherited_string_array(package, workspace, "exclude");
    PackagingRules::new(include.as_deref(), exclude.as_deref())
}

/// Reads a `[package]` string array, following `.workspace = true` to the root.
fn inherited_string_array(
    package: &dyn TableLike,
    workspace: &WorkspaceInherit<'_>,
    key: &str,
) -> Option<Vec<String>> {
    let item = package.get(key)?;
    if is_workspace_inherit(item) {
        return opt_string_array(workspace.package_key(key));
    }
    opt_string_array(Some(item))
}

fn publish_allowed(package: &dyn TableLike, workspace: &WorkspaceInherit<'_>) -> bool {
    let Some(item) = package.get("publish") else {
        return true;
    };
    let item = if is_workspace_inherit(item) {
        // An inherited key whose root value is absent is a manifest Cargo
        // rejects, so the publishable default keeps it under the release gate.
        let Some(item) = workspace.package_key("publish") else {
            return true;
        };
        item
    } else {
        item
    };
    match item {
        Item::Value(Value::Boolean(b)) => *b.value(),
        Item::Value(Value::Array(array)) => !array.is_empty(),
        // Cargo accepts only a boolean or a registry array here, so any other
        // shape is a manifest Cargo itself rejects. Treating it as publishable
        // keeps the package under the release gate; the opposite default would
        // silently exempt a package from classification.
        _ => true,
    }
}

fn string_array(item: Option<&Item>) -> Vec<String> {
    opt_string_array(item).unwrap_or_default()
}

fn opt_string_array(item: Option<&Item>) -> Option<Vec<String>> {
    let item = item?;
    let array = item.as_array()?;
    Some(
        array
            .iter()
            .filter_map(Value::as_str)
            .map(ToOwned::to_owned)
            .collect(),
    )
}

/// The directory part of a repository-relative manifest path.
fn directory_of(manifest_path: &str) -> String {
    match manifest_path.rsplit_once('/') {
        Some((dir, _)) => dir.to_string(),
        None => String::new(),
    }
}

/// Whether a requirement names exactly `version` rather than a range containing it.
///
/// This is the workspace's intra-workspace requirement convention in one place,
/// because `check` validates it and `apply` maintains it: if the two disagreed,
/// `apply` would either rewrite a requirement `check` accepts, editing a
/// published manifest for no reason, or leave one `check` rejects.
///
/// All three accepted spellings name the version. `cargo metadata` normalizes a
/// bare requirement to the caret form before `check` sees it, while `apply`
/// reads the manifest text, so both forms reach this predicate.
/// Ref: docs/dependencies.md, "Intra-workspace requirements name the declared version".
pub(crate) fn requirement_names_version(requirement: &str, version: &Version) -> bool {
    let trimmed = requirement.trim();
    let bare = version.to_string();
    trimmed == bare || trimmed == format!("^{bare}") || trimmed == format!("={bare}")
}

/// Workspace-relative form of a work-tree path.
///
/// A member may be outside the workspace root while remaining inside the same
/// repository. Leading parent components preserve that relationship until the
/// caller rebases the path into Git's repository-relative path space.
pub(crate) fn workspace_relative_path(workspace_root: &Path, path: &Path) -> Option<String> {
    if let Ok(relative) = path.strip_prefix(workspace_root) {
        return Some(os_path(relative));
    }

    let workspace: Vec<Component<'_>> = workspace_root.components().collect();
    let path: Vec<Component<'_>> = path.components().collect();
    let common = workspace
        .iter()
        .zip(&path)
        .take_while(|(left, right)| left == right)
        .count();
    if common == 0 {
        return None;
    }

    let mut relative = PathBuf::new();
    for component in workspace.iter().skip(common) {
        match component {
            Component::Normal(_) => relative.push(".."),
            Component::CurDir => {}
            Component::ParentDir | Component::Prefix(_) | Component::RootDir => return None,
        }
    }
    for component in path.iter().skip(common) {
        match component {
            Component::Normal(value) => relative.push(value),
            Component::CurDir => {}
            Component::ParentDir => relative.push(".."),
            Component::Prefix(_) | Component::RootDir => return None,
        }
    }
    Some(os_path(&relative))
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;

    /// A requirement names a version only when it pins exactly that version.
    ///
    /// All three spellings that name it are accepted: `apply` reads the manifest text, where a
    /// bare requirement stays bare, while `cargo metadata` normalizes the same requirement to
    /// the caret form before `check` sees it. A partial requirement names a range, not a
    /// version, so it is not accepted however close it looks.
    #[test]
    fn a_requirement_names_a_version_only_when_it_pins_exactly_that_version() {
        let version = Version::new(1, 2, 3);
        assert!(requirement_names_version("1.2.3", &version));
        assert!(requirement_names_version("^1.2.3", &version));
        assert!(requirement_names_version("=1.2.3", &version));
        assert!(requirement_names_version(" ^1.2.3 ", &version));

        assert!(!requirement_names_version("^1.2", &version));
        assert!(!requirement_names_version("^1", &version));
        assert!(!requirement_names_version("=1.2", &version));
        assert!(!requirement_names_version(">=1.2.3", &version));
        assert!(!requirement_names_version("^1.2.4", &version));
        assert!(!requirement_names_version("*", &version));
        assert!(!requirement_names_version("", &version));
    }

    fn members(patterns: &[&str]) -> WorkspaceMembers {
        cased_members(patterns, PathCase::Sensitive)
    }

    fn cased_members(patterns: &[&str], case: PathCase) -> WorkspaceMembers {
        WorkspaceMembers {
            members: compile_patterns(
                &patterns
                    .iter()
                    .map(|pattern| (*pattern).to_string())
                    .collect::<Vec<_>>(),
                case,
            )
            .unwrap(),
            exclude: Vec::new(),
        }
    }

    #[test]
    fn member_pattern_matches_one_segment_star() {
        let packages = members(&["p/*"]);
        assert!(is_workspace_member("p/a", &packages));
        assert!(!is_workspace_member("p/a/b", &packages));
        assert!(!is_workspace_member("q/a", &packages));
    }

    #[test]
    fn member_pattern_matches_a_partial_segment_star() {
        let crates = members(&["p/a-*"]);
        assert!(is_workspace_member("p/a-b", &crates));
        assert!(!is_workspace_member("p/a-b/c", &crates));
    }

    /// A member pattern matches only at the workspace root.
    ///
    /// Cargo resolves `members` and `exclude` globs against the workspace root, where the gitignore
    /// matcher backing them would otherwise let a pattern with no separator match a directory at
    /// any depth — pulling a nested package into a workspace that never declared it and anchoring
    /// the wrong package directory at that end of the comparison.
    #[test]
    fn a_member_pattern_matches_only_at_the_workspace_root() {
        let bare = members(&["a*"]);
        assert!(is_workspace_member("ab", &bare));
        assert!(!is_workspace_member("p/ab", &bare));
    }

    #[test]
    fn an_exclusion_matches_only_at_the_workspace_root() {
        let excluded = WorkspaceMembers {
            members: Vec::new(),
            exclude: compile_patterns(&["skip".to_string()], PathCase::Sensitive).unwrap(),
        };
        assert!(is_workspace_excluded("skip", &excluded));
        assert!(!is_workspace_excluded("packages/skip", &excluded));
    }

    /// An incomplete package identity is not a package.
    ///
    /// A manifest Cargo would not publish is not a package for classification, and every incomplete
    /// identity reaches that answer without erroring.
    #[test]
    fn an_incomplete_package_identity_is_not_a_package() {
        let inherit = WorkspaceInherit::default();
        let not_a_package = |content: &str| {
            parse_package_manifest(content, "packages/foo/Cargo.toml", &inherit).unwrap()
        };

        assert!(not_a_package("[workspace]\nmembers = []\n").is_none());
        assert!(not_a_package("[package]\nversion = \"0.1.0\"\n").is_none());
        assert!(not_a_package("[package]\nname = \"foo\"\n").is_none());
        assert!(not_a_package("[package]\nname = \"foo\"\nversion = 1\n").is_none());
        assert!(
            not_a_package("[package]\nname = \"foo\"\nversion.workspace = true\n").is_none(),
            "an inherited version with no root value is not a package"
        );
    }

    #[test]
    fn a_manifest_without_a_workspace_table_declares_no_members() {
        let parsed = parse_workspace_members(
            "[package]\nname = \"foo\"\n",
            Path::new("Cargo.toml"),
            PathCase::Sensitive,
        )
        .unwrap();

        assert!(parsed.members.is_empty());
        assert!(parsed.exclude.is_empty());
    }

    /// An unusable publish value keeps the package publishable.
    ///
    /// Cargo accepts only a boolean or a registry array for `publish`, so a manifest Cargo would
    /// reject stays under the release gate rather than silently exempting itself from
    /// classification.
    #[test]
    fn an_unusable_publish_value_keeps_the_package_publishable() {
        let inherit = WorkspaceInherit::default();
        let publishable = |content: &str| {
            parse_package_manifest(content, "packages/foo/Cargo.toml", &inherit)
                .unwrap()
                .unwrap()
                .publish
        };

        assert!(publishable(
            "[package]\nname = \"foo\"\nversion = \"0.1.0\"\npublish = \"yes\"\n"
        ));
        assert!(
            publishable(
                "[package]\nname = \"foo\"\nversion = \"0.1.0\"\npublish.workspace = true\n"
            ),
            "an inherited publish key with no root value stays publishable"
        );
    }

    /// A member pattern matches from the workspace root however it is spelled.
    ///
    /// A member pattern that already anchors itself against the workspace root keeps that meaning,
    /// and one that does not is anchored for it. Ref: the `anchored` documentation.
    #[test]
    fn a_member_pattern_is_anchored_to_the_workspace_root() {
        assert_root_anchored("p/*");
    }

    #[test]
    fn an_explicitly_anchored_member_pattern_stays_anchored() {
        assert_root_anchored("/p/*");
    }

    fn assert_root_anchored(literal: &str) {
        let pattern = MemberPattern::new(literal, PathCase::Sensitive).unwrap();
        assert!(pattern.matches("p/a"));
        assert!(!pattern.matches("q/p/a"));
    }

    /// A cloned member pattern matches the same directories.
    ///
    /// A member set is cloned into every historical snapshot, and the clone must keep matching
    /// exactly what the original matched.
    #[test]
    fn a_cloned_member_pattern_matches_the_same_directories() {
        // The anchored literal still queries the compiled matcher because its stored spelling
        // differs from the candidate. It avoids wildcard-regex compilation under Miri while
        // native runs retain wildcard clone coverage.
        let (literal, included, excluded) = if cfg!(miri) {
            ("/a", "a", "b")
        } else {
            ("*", "a", "p/a")
        };
        let pattern = MemberPattern::new(literal, PathCase::Sensitive).unwrap();

        let cloned = pattern.clone();

        assert_eq!(cloned.matches(included), pattern.matches(included));
        assert_eq!(cloned.matches(excluded), pattern.matches(excluded));
        assert!(cloned.matches(included));
        assert!(!cloned.matches(excluded));
        assert!(
            format!("{cloned:?}").contains(literal),
            "the compiled matcher cannot be shown, so the literal identifies the pattern"
        );
    }

    #[test]
    fn a_manifest_at_the_repository_root_has_an_empty_directory() {
        assert_eq!(directory_of("Cargo.toml"), "");
        assert_eq!(directory_of("packages/foo/Cargo.toml"), "packages/foo");
    }

    /// Probing a directory without cased names reports sensitive.
    ///
    /// The probe re-opens an existing entry under a flipped spelling, so a directory that offers no
    /// flippable entry cannot prove insensitivity and must yield the stricter answer.
    #[cfg_attr(miri, ignore)] // Reads a real directory, which Miri cannot emulate.
    #[test]
    fn probing_a_directory_without_cased_names_reports_sensitive() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("123"), "").unwrap();

        assert_eq!(PathCase::probe(temp.path()), PathCase::Sensitive);
    }

    #[cfg_attr(miri, ignore)] // Reads a real directory, which Miri cannot emulate.
    #[test]
    fn probing_an_unreadable_directory_reports_sensitive() {
        let temp = tempfile::tempdir().unwrap();

        assert_eq!(
            PathCase::probe(&temp.path().join("absent")),
            PathCase::Sensitive
        );
    }

    /// Only the native separator is rewritten.
    ///
    /// Cargo resolves a manifest-declared path with the host's own rules, so a backslash is a
    /// separator on Windows and a legal file name character elsewhere. Both spellings are asserted
    /// directly because only one of them is the native one on any given host.
    #[test]
    fn only_the_native_separator_is_rewritten() {
        assert_eq!(to_git_separators(r"..\b", '\\'), "../b");
        assert_eq!(to_git_separators(r"odd\name.md", '/'), r"odd\name.md");
        assert_eq!(to_git_separators("../b", '/'), "../b");
        assert_eq!(to_git_separators("../b", '\\'), "../b");
    }

    #[test]
    fn a_sibling_manifest_is_relative_to_the_workspace() {
        let repository = PathBuf::from("repository");
        let workspace = repository.join("workspace");
        let sibling = repository.join("outside").join("Cargo.toml");

        assert_eq!(
            workspace_relative_path(&workspace, &sibling),
            Some("../outside/Cargo.toml".to_string())
        );
        assert_eq!(
            workspace_relative_path(&workspace, &workspace.join("packages/a/Cargo.toml")),
            Some("packages/a/Cargo.toml".to_string())
        );
    }

    #[test]
    fn publish_false_excludes_package() {
        let parsed = parse_package_manifest(
            r#"
[package]
name = "priv"
version = "0.1.0"
publish = false
"#,
            "packages/priv/Cargo.toml",
            &WorkspaceInherit::default(),
        )
        .unwrap()
        .unwrap();
        assert!(!parsed.publish);
    }

    #[test]
    fn member_pattern_double_star_matches_the_prefix_directory() {
        let packages = members(&["p/**"]);
        assert!(is_workspace_member("p", &packages));
        assert!(is_workspace_member("p/a", &packages));
        assert!(!is_workspace_member("q", &packages));
    }

    #[test]
    fn invalid_member_pattern_is_rejected() {
        // An unclosed brace is not a valid path pattern; a silently non-matching
        // pattern would drop real members from a historical snapshot.
        let error = parse_workspace_members(
            "[workspace]\nmembers = [\"foo.{js,ts\"]\n",
            Path::new("Cargo.toml"),
            PathCase::Sensitive,
        )
        .unwrap_err();
        assert_eq!(
            error
                .find_source::<InvalidMemberPatternError>()
                .map(InvalidMemberPatternError::pattern),
            Some("foo.{js,ts")
        );
    }

    #[test]
    fn workspace_members_honor_exclude() {
        let declared = parse_workspace_members(
            r#"
[workspace]
members = ["p/*"]
exclude = ["p/b"]
"#,
            Path::new("Cargo.toml"),
            PathCase::Sensitive,
        )
        .unwrap();
        assert!(is_workspace_member("p/a", &declared));
        assert!(!is_workspace_member("p/b", &declared));
        assert!(!is_workspace_member("q/a", &declared));
        // A non-virtual root's own package is a member even though no pattern
        // names it.
        assert!(is_workspace_member("", &declared));
    }

    #[test]
    fn empty_workspace_members_only_selects_the_root() {
        // Without a `members` list the only member is the root package, matching
        // Cargo rather than treating every manifest in the tree as a member.
        let empty = parse_workspace_members(
            "[workspace]\n",
            Path::new("Cargo.toml"),
            PathCase::Sensitive,
        )
        .unwrap();
        assert!(is_workspace_member("", &empty));
        assert!(!is_workspace_member("anything", &empty));
    }

    #[test]
    fn workspace_exclusion_is_queried_independently_of_membership() {
        let declared = parse_workspace_members(
            "[workspace]\nmembers = [\"p/*\"]\nexclude = [\"p/b\"]\n",
            Path::new("Cargo.toml"),
            PathCase::Sensitive,
        )
        .unwrap();
        assert!(is_workspace_excluded("p/b", &declared));
        // Not named by either list: outside the workspace, but not excluded.
        assert!(!is_workspace_excluded("q/a", &declared));
    }

    /// A directory name containing a backslash is one component.
    ///
    /// A backslash is an ordinary character in a directory name on Unix, and both Git and Cargo
    /// treat it that way, so a pattern written with one names a single component there rather than
    /// a nested directory.
    ///
    /// Windows cannot hold such a name and does separate at a backslash, so the
    /// distinction is only observable on Unix.
    #[cfg(unix)]
    #[test]
    fn a_directory_name_containing_a_backslash_is_one_component() {
        let excluding = parse_workspace_members(
            "[workspace]\nmembers = [\"packages/*\"]\nexclude = [\"packages/a\\\\b\"]\n",
            Path::new("Cargo.toml"),
            PathCase::Sensitive,
        )
        .unwrap();
        // The pattern names the one-component directory it was written as, and
        // says nothing about the nested one.
        assert!(is_workspace_excluded(r"packages/a\b", &excluding));
        assert!(!is_workspace_excluded("packages/a/b", &excluding));

        let listing = parse_workspace_members(
            "[workspace]\nmembers = [\"packages/a\\\\b\"]\n",
            Path::new("Cargo.toml"),
            PathCase::Sensitive,
        )
        .unwrap();
        // The members side reads the same way: the literal name is the member,
        // and the nested directory the spelling would name on Windows is not.
        assert!(is_workspace_member(r"packages/a\b", &listing));
        assert!(!is_workspace_member("packages/a/b", &listing));
    }

    #[test]
    fn path_dependencies_are_collected_from_normal_dependencies() {
        assert_path_dependency("[dependencies]");
    }

    #[test]
    fn path_dependencies_are_collected_from_build_dependencies() {
        assert_path_dependency("[build-dependencies]");
    }

    #[test]
    fn path_dependencies_are_collected_from_dev_dependencies() {
        assert_path_dependency("[dev-dependencies]");
    }

    #[test]
    fn path_dependencies_are_collected_from_target_dependencies() {
        assert_path_dependency("[target.'cfg(windows)'.dependencies]");
    }

    #[test]
    fn path_dependencies_accumulate_across_dependency_tables() {
        // Aggregation needs all table kinds together, but not a complete package manifest.
        let doc = root_doc(
            "[dependencies]\nb.path = 'b'\n\
             [build-dependencies]\nc.path = 'c'\n\
             [dev-dependencies]\nd.path = 'd'\n\
             [target.'cfg(windows)'.dependencies]\ne.path = 'e'\n",
        );
        let mut paths = path_dependencies(&doc);
        paths.sort();
        assert_eq!(paths, vec!["b", "c", "d", "e"]);
    }

    fn assert_path_dependency(table: &str) {
        let parsed = parse_package_manifest(
            &format!(
                r#"
[package]
name = "a"
version = "0.1.0"

{table}
b = {{ path = "../b" }}
registry = "1"
"#
            ),
            "packages/a/Cargo.toml",
            &WorkspaceInherit::default(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(parsed.path_dependencies, vec!["../b"]);
    }

    /// A dependency look alike table is not a dependency table.
    ///
    /// Cargo reads dependency tables at the manifest root and under `[target.<spec>]` only, so a
    /// look-alike table elsewhere carries no dependency semantics and must not contribute
    /// membership edges.
    #[test]
    fn a_dependency_look_alike_table_is_not_a_dependency_table() {
        let parsed = parse_package_manifest(
            r#"
[package]
name = "a"
version = "0.1.0"

[package.metadata.dependencies]
ghost = { path = "../ghost" }

[package.metadata.some-tool]
dev-dependencies = { phantom = { path = "../phantom" } }

[dependencies]
b = { path = "../b" }
"#,
            "packages/a/Cargo.toml",
            &WorkspaceInherit::default(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(parsed.path_dependencies, vec!["../b"]);
    }

    #[test]
    fn case_sensitive_matching_follows_the_probed_filesystem() {
        let strict = cased_members(&["P/*"], PathCase::Sensitive);
        assert!(!is_workspace_member("p/a", &strict));
        assert!(is_workspace_member("P/a", &strict));
    }

    #[test]
    fn case_insensitive_matching_follows_the_probed_filesystem() {
        let relaxed = cased_members(&["P/*"], PathCase::Insensitive);
        assert!(is_workspace_member("p/a", &relaxed));
        assert!(is_workspace_member("P/a", &relaxed));
    }

    #[test]
    fn case_insensitive_prefix_matching_follows_the_probed_filesystem() {
        // The literal and `foo/**` prefix fast paths follow the same rules as
        // the compiled matcher.
        let prefix = cased_members(&["P/**"], PathCase::Insensitive);
        assert!(is_workspace_member("p", &prefix));
    }

    #[cfg_attr(miri, ignore)] // Reads a real directory, which Miri cannot emulate.
    #[test]
    fn path_case_probe_agrees_with_the_filesystem() {
        let dir = tempfile::TempDir::new().unwrap();
        fs::write(dir.path().join("Probe.txt"), "x").unwrap();
        let probed = PathCase::probe(dir.path());
        let observed = if dir.path().join("PROBE.TXT").exists() {
            PathCase::Insensitive
        } else {
            PathCase::Sensitive
        };
        assert_eq!(probed, observed);
    }

    #[cfg_attr(miri, ignore)] // Reads the filesystem, which Miri cannot emulate.
    #[test]
    fn unreadable_directory_probes_as_case_sensitive() {
        // The stricter answer never widens member matching, so an unreadable
        // directory must not relax it.
        assert_eq!(
            PathCase::probe(Path::new("cargo-release-plan-no-such-directory")),
            PathCase::Sensitive
        );
    }

    #[test]
    fn flip_case_inverts_cased_characters_only() {
        assert_eq!(flip_case("Cargo.toml"), "cARGO.TOML");
        assert_eq!(flip_case("123"), "123");
    }

    #[test]
    fn publish_registry_array_is_allowed_when_nonempty() {
        let allowed = parse_package_manifest(
            r#"
[package]
name = "pub"
version = "0.1.0"
publish = ["crates-io"]
"#,
            "packages/pub/Cargo.toml",
            &WorkspaceInherit::default(),
        )
        .unwrap()
        .unwrap();
        assert!(allowed.publish);
        let empty = parse_package_manifest(
            r#"
[package]
name = "nopub"
version = "0.1.0"
publish = []
"#,
            "packages/nopub/Cargo.toml",
            &WorkspaceInherit::default(),
        )
        .unwrap()
        .unwrap();
        assert!(!empty.publish);
    }

    #[test]
    fn include_array_becomes_packaging_rules() {
        let parsed = parse_package_manifest(
            r#"
[package]
name = "foo"
version = "0.1.0"
include = ["/src/", "/README.md"]
"#,
            "packages/foo/Cargo.toml",
            &WorkspaceInherit::default(),
        )
        .unwrap()
        .unwrap();
        assert!(parsed.packaging.is_released("src/lib.rs"));
        assert!(parsed.packaging.is_released("README.md"));
        assert!(!parsed.packaging.is_released("tests/x.rs"));
    }

    #[test]
    fn inherited_workspace_version_is_parsed() {
        let root = root_doc("[workspace.package]\nversion = \"0.4.0\"\n");
        let parsed = parse_package_manifest(
            r#"
[package]
name = "foo"
version.workspace = true
"#,
            "packages/foo/Cargo.toml",
            &WorkspaceInherit::from_root(&root),
        )
        .unwrap()
        .unwrap();
        assert_eq!(parsed.version.to_string(), "0.4.0");
        assert!(
            parse_package_manifest(
                r#"
[package]
name = "foo"
version.workspace = true
"#,
                "packages/foo/Cargo.toml",
                &WorkspaceInherit::default(),
            )
            .unwrap()
            .is_none()
        );
    }

    #[test]
    fn inherited_packaging_and_publish_are_resolved_from_the_root() {
        let root = root_doc(
            "[workspace.package]\ninclude = [\"src/\"]\nexclude = [\"tests/\"]\npublish = false\n",
        );
        let parsed = parse_package_manifest(
            r#"
[package]
name = "foo"
version = "0.1.0"
include.workspace = true
publish.workspace = true
"#,
            "packages/foo/Cargo.toml",
            &WorkspaceInherit::from_root(&root),
        )
        .unwrap()
        .unwrap();
        assert!(!parsed.publish);
        assert!(parsed.packaging.is_released("src/lib.rs"));
        assert!(!parsed.packaging.is_released("README.md"));
    }

    /// Manifest resources are split by where they are declared.
    ///
    /// Cargo copies `readme` and `license-file` into the crate root, so both are released content,
    /// and an inherited value names a path relative to the workspace root rather than to the
    /// package.
    #[test]
    fn manifest_resources_are_split_by_where_they_are_declared() {
        let root = root_doc("[workspace.package]\nreadme = \"README.md\"\n");
        let parsed = parse_package_manifest(
            r#"
[package]
name = "foo"
version = "0.1.0"
readme.workspace = true
license-file = "../../LICENSE"
"#,
            "packages/foo/Cargo.toml",
            &WorkspaceInherit::from_root(&root),
        )
        .unwrap()
        .unwrap();
        assert_eq!(parsed.resource_paths, vec!["../../LICENSE"]);
        assert_eq!(parsed.inherited_resource_paths, vec!["README.md"]);
        assert!(!parsed.auto_readme);
    }

    /// A manifest resource that names no file is skipped.
    ///
    /// `readme = false` disables the key rather than naming a file, and an inherited key with no
    /// root value names nothing either.
    #[test]
    fn a_manifest_resource_that_names_no_file_is_skipped() {
        let parsed = parse_package_manifest(
            r#"
[package]
name = "foo"
version = "0.1.0"
readme = false
license-file.workspace = true
"#,
            "packages/foo/Cargo.toml",
            &WorkspaceInherit::default(),
        )
        .unwrap()
        .unwrap();
        assert!(parsed.resource_paths.is_empty());
        assert!(parsed.inherited_resource_paths.is_empty());
        assert!(!parsed.auto_readme);
    }

    /// An undeclared readme is left for cargo to detect.
    ///
    /// Cargo probes the package directory only when the key is absent, and reads `readme = true` as
    /// naming its preferred default.
    #[test]
    fn an_undeclared_readme_is_left_for_cargo_to_detect() {
        let detected = parse_package_manifest(
            "[package]\nname = \"foo\"\nversion = \"0.1.0\"\n",
            "packages/foo/Cargo.toml",
            &WorkspaceInherit::default(),
        )
        .unwrap()
        .unwrap();
        assert!(detected.auto_readme);
        assert!(detected.resource_paths.is_empty());

        let enabled = parse_package_manifest(
            "[package]\nname = \"foo\"\nversion = \"0.1.0\"\nreadme = true\n",
            "packages/foo/Cargo.toml",
            &WorkspaceInherit::default(),
        )
        .unwrap()
        .unwrap();
        assert!(!enabled.auto_readme);
        assert_eq!(enabled.resource_paths, vec!["README.md"]);
    }

    #[test]
    fn inherited_path_dependencies_resolve_against_the_workspace_root() {
        let root = root_doc(
            "[workspace.dependencies]\nb = { path = \"packages/b\", version = \"0.1.0\" }\n",
        );
        let parsed = parse_package_manifest(
            r#"
[package]
name = "a"
version = "0.1.0"

[dependencies]
b.workspace = true

[dev-dependencies]
c = { path = "../c" }
"#,
            "packages/a/Cargo.toml",
            &WorkspaceInherit::from_root(&root),
        )
        .unwrap()
        .unwrap();
        assert_eq!(parsed.inherited_path_dependencies, vec!["packages/b"]);
        assert_eq!(parsed.path_dependencies, vec!["../c"]);
    }

    /// A dependency look alike table declares no inherited dependency.
    ///
    /// Inherited-key attribution reads the same dependency tables as membership, so a look-alike
    /// table must not add an inherited edge either.
    #[test]
    fn a_dependency_look_alike_table_declares_no_inherited_dependency() {
        let root = root_doc(
            "[workspace.dependencies]\nb = { path = \"packages/b\", version = \"0.1.0\" }\n",
        );
        let parsed = parse_package_manifest(
            r#"
[package]
name = "a"
version = "0.1.0"

[package.metadata.dependencies]
b.workspace = true
"#,
            "packages/a/Cargo.toml",
            &WorkspaceInherit::from_root(&root),
        )
        .unwrap()
        .unwrap();
        assert!(parsed.inherited_path_dependencies.is_empty());
    }

    /// Cargo's conventional installable binary layouts are reconstructed.
    ///
    /// Historical target shape is inferred without invoking Cargo, so every layout
    /// that can install a binary must be recognised from tree paths.
    #[test]
    fn conventional_lockfile_targets_are_discovered() {
        let targets = TargetDiscovery::default();

        for path in ["src/main.rs", "src/bin/tool.rs", "src/bin/tool/main.rs"] {
            assert!(
                targets.has_lockfile_target([path], PathCase::Sensitive),
                "{path} should be an installable binary target"
            );
        }
        for path in [
            "src/lib.rs",
            "src/bin/tool/data.rs",
            "src/bin/.scratch.rs",
            "src/bin/.scratch/main.rs",
            "examples/demo.rs",
            "examples/demo/main.rs",
            "examples/demo/data.rs",
            "examples/.scratch.rs",
            "examples/.scratch/main.rs",
            "tests/demo.rs",
            "benches/demo.rs",
            "build.rs",
        ] {
            assert!(
                !targets.has_lockfile_target([path], PathCase::Sensitive),
                "{path} should not be an installable binary target"
            );
        }
        assert!(targets.has_lockfile_target(["SRC/MAIN.rs"], PathCase::Insensitive));
        assert!(!targets.has_lockfile_target(["SRC/MAIN.rs"], PathCase::Sensitive));
    }

    /// Explicit target declarations remain targets when auto-discovery is disabled.
    #[test]
    fn explicit_targets_override_disabled_auto_discovery() {
        let explicit = parse_package_manifest(
            r#"
[package]
name = "foo"
version = "0.1.0"
autobins = false
autoexamples = false

[[bin]]
name = "demo"
path = "demo.rs"
"#,
            "packages/foo/Cargo.toml",
            &WorkspaceInherit::default(),
        )
        .unwrap()
        .unwrap();
        assert!(
            explicit
                .targets
                .has_lockfile_target(std::iter::empty(), PathCase::Sensitive)
        );

        let disabled = parse_package_manifest(
            r#"
[package]
name = "foo"
version = "0.1.0"
autobins = false
autoexamples = false
"#,
            "packages/foo/Cargo.toml",
            &WorkspaceInherit::default(),
        )
        .unwrap()
        .unwrap();
        assert!(
            !disabled
                .targets
                .has_lockfile_target(["src/main.rs", "examples/demo.rs"], PathCase::Sensitive)
        );
    }

    #[test]
    fn auxiliary_targets_do_not_create_an_installation_closure() {
        for target in ["example", "bench", "test"] {
            let content = format!(
                "[package]\nname = \"foo\"\nversion = \"0.1.0\"\nbuild = \"build.rs\"\n\
                 [[{target}]]\nname = \"demo\"\npath = \"demo.rs\"\n"
            );
            let manifest =
                parse_package_manifest(&content, "Cargo.toml", &WorkspaceInherit::default())
                    .unwrap()
                    .unwrap();
            assert!(!manifest.targets.has_lockfile_target(
                [
                    "src/lib.rs",
                    "examples/demo.rs",
                    "benches/demo.rs",
                    "tests/demo.rs",
                    "build.rs"
                ],
                PathCase::Sensitive,
            ));
        }
    }

    #[test]
    fn installation_declarations_resolve_aliases_inheritance_and_dependency_kinds() {
        let root = root_doc(
            "[workspace.dependencies]\nshared = \"1\"\n\
             renamed = { package = \"actual\", version = \"2\" }\n\
             development = { package = \"dev-only\", version = \"3\" }\n",
        );
        let manifest = parse_package_manifest(
            "[package]\nname = \"tool\"\nversion = \"0.1.0\"\n\
             [dependencies]\nshared.workspace = true\n\
             local = { path = \"../local\" }\n\
             [build-dependencies]\nbuilder = \"4\"\n\
             [target.'cfg(unix)'.dependencies]\nrenamed.workspace = true\n\
             [target.'cfg(windows)'.build-dependencies]\nwindows-builder = \"5\"\n\
             [dev-dependencies]\nshared = \"9\"\n\
             [target.'cfg(unix)'.dev-dependencies]\ndevelopment.workspace = true\n",
            "Cargo.toml",
            &WorkspaceInherit::from_root(&root),
        )
        .unwrap()
        .unwrap();
        let InstallationDependencies::Parsed(declarations) = manifest.installation_dependencies
        else {
            panic!("valid fixture declarations must parse");
        };
        let dependencies: Vec<_> = declarations
            .iter()
            .map(|dependency| {
                (
                    dependency.name.as_str(),
                    dependency.requirement.as_ref().map(ToString::to_string),
                )
            })
            .collect();
        assert_eq!(
            dependencies,
            [
                ("shared", Some("^1".to_owned())),
                ("local", None),
                ("builder", Some("^4".to_owned())),
                ("actual", Some("^2".to_owned())),
                ("windows-builder", Some("^5".to_owned())),
            ]
        );
    }

    #[test]
    fn legacy_dependency_tables_use_canonical_precedence_and_raw_locations() {
        let document = root_doc(
            "[package]\nname = \"tool\"\nversion = \"0.1.0\"\nedition = \"2021\"\n\
             [build_dependencies]\nignored = \"1\"\n\
             [build-dependencies]\n\
             [dev_dependencies]\nignored-dev = \"1\"\n\
             [dev-dependencies]\nselected-dev = \"1\"\n\
             [target.'cfg(unix)'.build_dependencies]\nbuilder = \"1\"\n\
             [target.'cfg(unix)'.dev_dependencies]\ndevelopment = \"1\"\n",
        );
        let mut visited = Vec::new();
        for_each_dependency_table_with_context(
            document.as_table(),
            &mut |location, kind, table| {
                visited.push((location.to_owned(), kind.to_owned(), table.len()));
            },
        );
        assert_eq!(
            visited,
            [
                (
                    "dev-dependencies".to_owned(),
                    "dev-dependencies".to_owned(),
                    1
                ),
                (
                    "build-dependencies".to_owned(),
                    "build-dependencies".to_owned(),
                    0
                ),
                (
                    "target.cfg(unix).dev_dependencies".to_owned(),
                    "dev-dependencies".to_owned(),
                    1
                ),
                (
                    "target.cfg(unix).build_dependencies".to_owned(),
                    "build-dependencies".to_owned(),
                    1
                ),
            ],
        );
        let declarations = installation_dependencies(
            &document,
            &WorkspaceInherit::default(),
            Path::new("Cargo.toml"),
        )
        .unwrap();
        assert_eq!(declarations.len(), 1);
        assert_eq!(declarations.first().unwrap().name, "builder");
    }

    #[test]
    fn git_source_identity_preserves_references_but_not_resolved_commits() {
        let source = DependencySource::Git {
            repository: "https://example.invalid/foo".to_owned(),
            reference: GitReference::Branch("release/next".to_owned()),
        };
        let registries = BTreeMap::new();
        assert_eq!(
            source.matches_locked(
                Some("git+https://example.invalid/foo.git?branch=release%2Fnext#aaaa"),
                &registries,
            ),
            Some(true),
        );
        assert_eq!(
            source.matches_locked(
                Some("git+https://example.invalid/foo?branch=development#aaaa"),
                &registries,
            ),
            Some(false),
        );
        assert_eq!(
            source.matches_locked(Some("git+https://example.invalid/other#aaaa"), &registries),
            Some(false),
        );
        for (query, reference) in [
            ("tag=v1%2Bnext", GitReference::Tag("v1+next".to_owned())),
            ("rev=abc", GitReference::Rev("abc".to_owned())),
        ] {
            assert_eq!(
                parse_locked_git_source(&format!("https://example.invalid/foo?{query}#123")),
                Some(("https://example.invalid/foo", reference)),
            );
        }
        assert_eq!(
            decode_git_reference("release+next"),
            Some("release next".to_owned())
        );
        for invalid in ["%", "%xy", "%ff"] {
            assert!(decode_git_reference(invalid).is_none());
        }
    }

    #[test]
    fn patches_only_apply_to_the_declared_origin() {
        let source = DependencySource::Registry(CRATES_IO_INDEX.to_owned());
        let registries = BTreeMap::from([(
            "private".to_owned(),
            "https://example.invalid/index".to_owned(),
        )]);
        assert_eq!(source.accepts_patch("crates-io", &registries), Some(true));
        assert_eq!(
            source.accepts_patch("https://example.invalid/foo", &registries),
            Some(false)
        );
        assert_eq!(
            DependencySource::Path(PackageIdentity {
                name: "foo".to_owned(),
                version: Version::new(1, 0, 0),
            })
            .accepts_patch("crates-io", &registries),
            Some(false)
        );
        let private = DependencySource::NamedRegistry("private".to_owned());
        assert_eq!(
            private.accepts_patch("https://example.invalid/index", &registries),
            Some(true)
        );
        assert_eq!(private.matches_locked(None, &BTreeMap::new()), None);
    }

    #[test]
    fn cargo_source_url_rules_do_not_guess_unsupported_normalization() {
        assert_eq!(
            same_source_url(
                "http://github.com/Owner/Repo.GIT/",
                "https://github.com/owner/repo"
            ),
            Some(true),
        );
        assert_eq!(
            same_source_url(
                "https://example.invalid/Foo.git",
                "https://example.invalid/foo"
            ),
            Some(false),
        );
        assert_eq!(
            same_source_url(
                "https://example.invalid/foo//",
                "https://example.invalid/foo/"
            ),
            Some(false),
        );
        for unsupported in [
            "https://EXAMPLE.invalid/foo",
            "https://example.invalid:443/foo",
            "https://example.invalid:0444/foo",
            "https://example.invalid:65536/foo",
            "https://example.invalid/a/../foo",
            "https://example.invalid/f%6fo",
            "https://[::1]/foo",
            "https://0x7f.0.0.1/foo",
            "https://127.1/foo",
            "https://127.0.0.256/foo",
            "https://127.0.0.01/foo",
            "https://@example.invalid/foo",
            "https://example.invalid/foo\0",
        ] {
            assert_eq!(
                same_source_url(unsupported, "https://example.invalid/foo"),
                None
            );
            assert_eq!(same_source_url(unsupported, unsupported), Some(true));
        }
        assert_eq!(
            same_source_url("https://127.0.0.1:444/foo.git", "https://127.0.0.1:444/foo"),
            Some(true)
        );
        assert_eq!(
            same_source_url(
                "https://example.invalid:444/foo",
                "https://example.invalid:445/foo"
            ),
            Some(false)
        );
    }

    #[test]
    fn malformed_git_references_are_not_treated_as_default_references() {
        let source = DependencySource::Git {
            repository: "https://example.invalid/foo".to_owned(),
            reference: GitReference::Default,
        };
        for query in ["branch", "unknown=main", "branch=%", "tag=%ff"] {
            assert_eq!(
                source.matches_locked(
                    Some(&format!("git+https://example.invalid/foo?{query}#123")),
                    &BTreeMap::new(),
                ),
                None
            );
        }
        assert_eq!(source.matches_locked(None, &BTreeMap::new()), Some(false));
    }

    #[test]
    fn configured_named_registries_reject_source_less_and_git_entries() {
        let registry = DependencySource::NamedRegistry("private".to_owned());
        let registries = BTreeMap::from([(
            "private".to_owned(),
            "https://example.invalid/index".to_owned(),
        )]);
        for source in [None, Some("git+https://example.invalid/index#123")] {
            assert_eq!(registry.matches_locked(source, &registries), Some(false));
            assert_eq!(registry.matches_locked(source, &BTreeMap::new()), None);
        }
    }

    #[test]
    fn installation_source_declarations_preserve_git_selectors_and_registry_indices() {
        let document = root_doc(
            "[dependencies]\n\
             branch = { git = \"https://example.invalid/foo\", branch = \"next\" }\n\
             tag = { git = \"https://example.invalid/foo\", tag = \"v1\" }\n\
             rev = { git = \"https://example.invalid/foo\", rev = \"abc\" }\n\
             registry = { version = \"1\", registry-index = \"https://example.invalid/index\" }\n",
        );
        let dependencies = installation_dependencies(
            &document,
            &WorkspaceInherit::default(),
            Path::new("Cargo.toml"),
        )
        .unwrap();
        let sources: BTreeMap<_, _> = dependencies
            .into_iter()
            .map(|dependency| (dependency.name, dependency.source))
            .collect();
        for (name, reference) in [
            ("branch", GitReference::Branch("next".to_owned())),
            ("tag", GitReference::Tag("v1".to_owned())),
            ("rev", GitReference::Rev("abc".to_owned())),
        ] {
            assert_eq!(
                sources.get(name),
                Some(&DependencySource::Git {
                    repository: "https://example.invalid/foo".to_owned(),
                    reference,
                })
            );
        }
        assert_eq!(
            sources.get("registry"),
            Some(&DependencySource::Registry(
                "https://example.invalid/index".to_owned()
            ))
        );
    }

    #[test]
    fn sparse_registry_sources_keep_their_protocol_identity() {
        let index = "sparse+https://example.invalid/index/";
        let registry = DependencySource::NamedRegistry("private".to_owned());
        let registries = BTreeMap::from([("private".to_owned(), index.to_owned())]);
        assert_eq!(
            registry.matches_locked(Some(index), &registries),
            Some(true)
        );
        assert_eq!(
            registry.matches_locked(Some("registry+https://example.invalid/index/"), &registries),
            Some(false),
        );
        assert_eq!(
            registry.matches_locked(
                Some("registry+sparse+https://example.invalid/index/"),
                &registries
            ),
            None,
        );
        assert_eq!(
            registry.matches_locked(Some("unknown+https://example.invalid/index/"), &registries),
            None
        );
    }

    #[test]
    fn path_patches_retain_their_workspace_relative_declaration() {
        let root =
            root_doc("[patch.crates-io]\nalias = { package = \"foo\", path = \"patches/foo\" }\n");
        let patches = installation_patches(&root);
        assert_eq!(patches.len(), 1);
        let patch = patches.first().unwrap();
        assert_eq!(patch.origin, "crates-io");
        assert_eq!(patch.name, "foo");
        let replacement = patch.replacement.as_ref().unwrap();
        assert_eq!(replacement.name, "foo");
        assert_eq!(
            replacement.source,
            DependencySource::UnresolvedPath(DependencyPath {
                path: "patches/foo".to_owned(),
                package_directory: None,
            })
        );
    }

    #[test]
    fn local_and_inherited_paths_keep_their_declaration_bases() {
        let root = root_doc("[workspace.dependencies]\nshared = { path = \"shared/foo\" }\n");
        let content = "[package]\nname = \"tool\"\nversion = \"0.1.0\"\n\
            [dependencies]\nlocal = { path = \"../foo\" }\nshared.workspace = true\n";
        for (manifest, prefix) in [
            ("packages/tool/Cargo.toml", "root"),
            ("root/packages/tool/Cargo.toml", ""),
        ] {
            let package =
                parse_package_manifest(content, manifest, &WorkspaceInherit::from_root(&root))
                    .unwrap()
                    .unwrap();
            let InstallationDependencies::Parsed(declarations) = package.installation_dependencies
            else {
                panic!("valid fixture declarations must parse");
            };
            let paths: Vec<_> = declarations
                .iter()
                .map(|dependency| {
                    let DependencySource::UnresolvedPath(path) = &dependency.source else {
                        panic!("a declared path must retain its reference");
                    };
                    path.directory(Path::new("repo"), "root", prefix).unwrap()
                })
                .collect();
            assert_eq!(paths, ["root/packages/foo", "root/shared/foo"]);
        }
    }

    #[test]
    fn excluded_path_identity_uses_its_own_workspace_version() {
        let documents = BTreeMap::from([
            (
                "foreign/foo/Cargo.toml",
                root_doc("[package]\nname = \"foo\"\nversion.workspace = true\n"),
            ),
            (
                "foreign/Cargo.toml",
                root_doc(
                    "[workspace]\nmembers = [\"foo\"]\n[workspace.package]\nversion = \"1.2.0\"\n",
                ),
            ),
            (
                "Cargo.toml",
                root_doc(
                    "[workspace]\nexclude = [\"foreign\"]\n[workspace.package]\nversion = \"9.0.0\"\n",
                ),
            ),
        ]);
        let identity =
            path_package_identity("foreign/foo/Cargo.toml", PathCase::Sensitive, |path| {
                Ok(documents.get(path).cloned())
            })
            .unwrap()
            .unwrap();
        assert_eq!(
            identity,
            PackageIdentity {
                name: "foo".to_owned(),
                version: Version::new(1, 2, 0)
            }
        );
    }

    #[test]
    fn an_explicit_path_workspace_owns_inheritance_instead_of_the_nearest_ancestor() {
        let documents = BTreeMap::from([
            (
                "packages/foo/Cargo.toml",
                root_doc(
                    "[package]\nname = \"foo\"\nversion.workspace = true\n\
                     workspace = \"../../owner\"\n",
                ),
            ),
            (
                "owner/Cargo.toml",
                root_doc("[workspace.package]\nversion = \"1.2.0\"\n"),
            ),
        ]);
        let identity =
            path_package_identity("packages/foo/Cargo.toml", PathCase::Sensitive, |path| {
                // Explicit workspace selection must not inspect an unrelated ancestor.
                assert!(documents.contains_key(path));
                Ok(documents.get(path).cloned())
            })
            .unwrap()
            .unwrap();
        assert_eq!(identity.name, "foo");
        assert_eq!(identity.version, Version::new(1, 2, 0));

        let identity =
            path_package_identity("packages/foo/Cargo.toml", PathCase::Sensitive, |path| {
                Ok((path != "owner/Cargo.toml")
                    .then(|| documents.get(path).cloned())
                    .flatten())
            })
            .unwrap();
        assert!(identity.is_none());
    }

    #[test]
    fn inheritance_stops_at_an_excluding_workspace_or_the_repository_boundary() {
        for root in [
            None,
            Some(
                "[workspace]\nexclude = [\"packages/foo\"]\n[workspace.package]\nversion = \"9.0.0\"\n",
            ),
        ] {
            let package = root_doc("[package]\nname = \"foo\"\nversion.workspace = true\n");
            let identity =
                path_package_identity("packages/foo/Cargo.toml", PathCase::Sensitive, |path| {
                    assert!(!path.starts_with("../"));
                    Ok(match path {
                        "packages/foo/Cargo.toml" => Some(package.clone()),
                        "Cargo.toml" => root.map(root_doc),
                        _ => None,
                    })
                })
                .unwrap();
            assert!(identity.is_none());
        }
    }

    #[test]
    fn a_virtual_path_target_has_no_package_identity() {
        let identity = path_package_identity("foreign/Cargo.toml", PathCase::Sensitive, |path| {
            assert_eq!(path, "foreign/Cargo.toml");
            Ok(Some(root_doc("[workspace]\n")))
        })
        .unwrap();
        assert!(identity.is_none());
    }

    #[test]
    fn path_identity_preserves_errors_from_its_explicit_workspace() {
        let error = path_package_identity("foo/Cargo.toml", PathCase::Sensitive, |path| {
            if path == "foo/Cargo.toml" {
                Ok(Some(root_doc(
                    "[package]\nname = \"foo\"\nversion.workspace = true\nworkspace = \"..\"\n",
                )))
            } else {
                parse_document(Path::new(path), "[workspace").map(Some)
            }
        })
        .unwrap_err();
        assert!(error.find_source::<ParseTomlError>().is_some());
    }

    #[test]
    fn absolute_dependency_paths_are_rebased_without_admitting_repository_siblings() {
        let repository = if cfg!(windows) {
            Path::new(r"C:\repository")
        } else {
            Path::new("/repository")
        };
        let inside = DependencyPath {
            path: repository.join("foreign").to_string_lossy().into_owned(),
            package_directory: Some("ignored".to_owned()),
        };
        assert_eq!(
            inside.directory(repository, "workspace", "workspace"),
            Some("foreign".to_owned())
        );
        let outside = DependencyPath {
            path: repository
                .parent()
                .unwrap()
                .join("sibling")
                .to_string_lossy()
                .into_owned(),
            package_directory: None,
        };
        assert!(outside.directory(repository, "workspace", "").is_none());
    }

    #[test]
    fn package_identity_survives_unusable_installation_declarations() {
        let package = parse_package_manifest(
            "[package]\nname = \"library\"\nversion = \"0.1.0\"\n\
             [dependencies]\nfoo = \"not a requirement\"\n",
            "packages/library/Cargo.toml",
            &WorkspaceInherit::default(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            package.identity(),
            PackageIdentity {
                name: "library".to_owned(),
                version: Version::new(0, 1, 0),
            }
        );
        assert!(matches!(
            package.installation_dependencies,
            InstallationDependencies::Invalid(_)
        ));
    }

    #[test]
    fn invalid_patch_declarations_retain_their_target_without_failing_discovery() {
        let root = root_doc(
            "[patch.crates-io]\nalias = { package = \"foo\", version = \"not a requirement\" }\n",
        );
        let patches = installation_patches(&root);
        let patch = patches.first().unwrap();
        assert_eq!(patch.name, "foo");
        _ = patch.replacement.as_ref().unwrap_err();
    }

    fn root_doc(content: &str) -> DocumentMut {
        content.parse().unwrap()
    }
}
