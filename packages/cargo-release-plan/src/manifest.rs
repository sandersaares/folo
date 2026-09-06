// Manifest parsing for versions, packaging rules, members, and pins.

use std::borrow::Cow;
use std::collections::HashSet;
use std::path::{Component, MAIN_SEPARATOR, Path, PathBuf};
use std::{fmt, fs};

use ignore::overrides::{Override, OverrideBuilder};
use ohno::AppError;
use semver::Version;
use toml_edit::{DocumentMut, Item, TableLike, Value};

use crate::git::os_path;
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
    /// How Cargo discovers lockfile-bearing targets for this package.
    pub(crate) targets: TargetDiscovery,
}

/// Manifest controls for discovering binary and example targets.
///
/// Historical snapshots cannot ask Cargo about an old tree, so they combine
/// these controls with that tree's paths to reconstruct whether the packaged
/// lockfile's dependency closure is operationally relevant.
/// Ref: docs/design.md, "Relevant lockfile closures".
#[derive(Clone, Copy, Debug)]
pub(crate) struct TargetDiscovery {
    explicit: bool,
    autobins: bool,
    autoexamples: bool,
}

impl TargetDiscovery {
    /// Whether `package_paths` contain any target that ships a lockfile.
    pub(crate) fn has_lockfile_target<'a>(
        self,
        package_paths: impl IntoIterator<Item = &'a str>,
        case: PathCase,
    ) -> bool {
        self.explicit
            || package_paths.into_iter().any(|path| {
                (self.autobins && is_auto_binary(path, case))
                    || (self.autoexamples && is_auto_example(path, case))
            })
    }
}

impl Default for TargetDiscovery {
    fn default() -> Self {
        Self {
            // Cargo discovers conventional targets unless the package opts out.
            // Ref: Cargo reference, "Target auto-discovery".
            explicit: false,
            autobins: true,
            autoexamples: true,
        }
    }
}

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
    // A manifest without a complete `[package]` identity is not something Cargo
    // would publish: it is either a virtual workspace root or a member whose
    // version is inherited from a root that does not declare one. Both are
    // ordinary states of historical trees, so they yield "not a package" rather
    // than an error. Malformed values that Cargo would reject still error below.
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
        let Some(version) = workspace.package_version() else {
            return Ok(None);
        };
        version
    } else {
        let Some(version) = version_item.as_str() else {
            return Ok(None);
        };
        version
    };
    let version = version
        .parse::<Version>()
        .map_err(|error| InvalidVersionError::caused_by(name, version, error))?;
    let directory = directory_of(manifest_path);
    let (resource_paths, inherited_resource_paths, auto_readme) =
        resource_paths(package, workspace);
    let targets = target_discovery(&doc, package);
    Ok(Some(PackageManifest {
        name: name.to_string(),
        version,
        directory,
        packaging: packaging_from_package(package, workspace)?,
        inherited: collect_inherited_keys(&doc),
        publish: publish_allowed(package, workspace),
        path_dependencies: path_dependencies(&doc),
        inherited_path_dependencies: inherited_path_dependencies(&doc, workspace),
        resource_paths,
        inherited_resource_paths,
        auto_readme,
        targets,
    }))
}

/// Conventional source path for a package's default binary.
const AUTO_BINARY_MAIN: &str = "src/main.rs";
/// Directory in which Cargo discovers additional binary targets.
const AUTO_BINARY_DIR: &str = "src/bin";
/// Directory in which Cargo discovers example targets.
const AUTO_EXAMPLE_DIR: &str = "examples";
/// Rust source suffix Cargo recognises during target auto-discovery.
const RUST_SOURCE_SUFFIX: &str = ".rs";
/// File name Cargo recognises as a directory target's entry point.
const TARGET_MAIN_FILE: &str = "main.rs";

fn target_discovery(doc: &DocumentMut, package: &dyn TableLike) -> TargetDiscovery {
    let explicit = ["bin", "example"].iter().any(|key| {
        doc.get(key)
            .and_then(Item::as_array_of_tables)
            .is_some_and(|targets| !targets.is_empty())
    });
    TargetDiscovery {
        explicit,
        autobins: package
            .get("autobins")
            .and_then(Item::as_bool)
            .unwrap_or(true),
        autoexamples: package
            .get("autoexamples")
            .and_then(Item::as_bool)
            .unwrap_or(true),
    }
}

fn is_auto_binary(path: &str, case: PathCase) -> bool {
    case.same_path(path, AUTO_BINARY_MAIN) || is_auto_directory_target(path, AUTO_BINARY_DIR, case)
}

fn is_auto_example(path: &str, case: PathCase) -> bool {
    is_auto_directory_target(path, AUTO_EXAMPLE_DIR, case)
}

/// Whether `path` follows either auto-discovered layout beneath `directory`.
fn is_auto_directory_target(path: &str, directory: &str, case: PathCase) -> bool {
    let path_parts: Vec<&str> = path.split('/').collect();
    let directory_parts: Vec<&str> = directory.split('/').collect();
    match (directory_parts.as_slice(), path_parts.as_slice()) {
        ([directory], [held, file]) => {
            case.same_path(directory, held)
                && is_visible_target_name(file)
                && file.ends_with(RUST_SOURCE_SUFFIX)
        }
        ([directory], [held, name, main]) => {
            case.same_path(directory, held)
                && is_visible_target_name(name)
                && case.same_path(main, TARGET_MAIN_FILE)
        }
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
        let dependencies = self.dependencies?;
        dependencies.get(name).and_then(Item::as_table_like)
    }
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
    visit_dependency_tables(manifest, visit);
    let Some(target) = manifest.get("target").and_then(Item::as_table_like) else {
        return;
    };
    for (_, spec) in target.iter() {
        if let Some(spec) = spec.as_table_like() {
            visit_dependency_tables(spec, visit);
        }
    }
}

fn visit_dependency_tables(table: &dyn TableLike, visit: &mut dyn FnMut(&str, &dyn TableLike)) {
    for name in DEPENDENCY_TABLES {
        if let Some(dependencies) = table.get(name).and_then(Item::as_table_like) {
            visit(name, dependencies);
        }
    }
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

/// Workspace-relative form of a work-tree path.
///
/// A member may be outside the workspace root while remaining inside the same
/// repository. Leading parent components preserve that relationship until the
/// caller rebases the path into Git's repository-relative path space.
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
        let packages = members(&["packages/*"]);
        assert!(is_workspace_member("packages/foo", &packages));
        assert!(!is_workspace_member("packages/foo/bar", &packages));
        assert!(!is_workspace_member("other/foo", &packages));
        let crates = members(&["crates/foo-*"]);
        assert!(is_workspace_member("crates/foo-bar", &crates));
        assert!(!is_workspace_member("crates/foo-bar/nested", &crates));
    }

    /// A member pattern matches only at the workspace root.
    ///
    /// Cargo resolves `members` and `exclude` globs against the workspace root, where the gitignore
    /// matcher backing them would otherwise let a pattern with no separator match a directory at
    /// any depth — pulling a nested package into a workspace that never declared it and anchoring
    /// the wrong package directory at that end of the comparison.
    #[test]
    fn a_member_pattern_matches_only_at_the_workspace_root() {
        let bare = members(&["foo*"]);
        assert!(is_workspace_member("foo-bar", &bare));
        assert!(!is_workspace_member("packages/foo-bar", &bare));

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
    fn a_member_pattern_matches_from_the_workspace_root_however_it_is_spelled() {
        for literal in ["packages/*", "/packages/*"] {
            let pattern = MemberPattern::new(literal, PathCase::Sensitive).unwrap();

            assert!(pattern.matches("packages/foo"), "{literal}");
            assert!(!pattern.matches("nested/packages/foo"), "{literal}");
        }
    }

    /// A cloned member pattern matches the same directories.
    ///
    /// A member set is cloned into every historical snapshot, and the clone must keep matching
    /// exactly what the original matched.
    #[test]
    fn a_cloned_member_pattern_matches_the_same_directories() {
        let pattern = MemberPattern::new("packages/*", PathCase::Sensitive).unwrap();

        let cloned = pattern.clone();

        assert_eq!(
            cloned.matches("packages/foo"),
            pattern.matches("packages/foo")
        );
        assert_eq!(cloned.matches("other/foo"), pattern.matches("other/foo"));
        assert!(cloned.matches("packages/foo"));
        assert!(!cloned.matches("other/foo"));
        assert!(
            format!("{cloned:?}").contains("packages/*"),
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
        let packages = members(&["packages/**"]);
        assert!(is_workspace_member("packages", &packages));
        assert!(is_workspace_member("packages/foo", &packages));
        assert!(!is_workspace_member("other", &packages));
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
    fn workspace_members_exclude_and_empty_members() {
        let declared = parse_workspace_members(
            r#"
[workspace]
members = ["packages/*"]
exclude = ["packages/skip"]
"#,
            Path::new("Cargo.toml"),
            PathCase::Sensitive,
        )
        .unwrap();
        assert!(is_workspace_member("packages/foo", &declared));
        assert!(!is_workspace_member("packages/skip", &declared));
        assert!(!is_workspace_member("examples/foo", &declared));
        // A non-virtual root's own package is a member even though no pattern
        // names it.
        assert!(is_workspace_member("", &declared));
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
            "[workspace]\nmembers = [\"packages/*\"]\nexclude = [\"packages/skip\"]\n",
            Path::new("Cargo.toml"),
            PathCase::Sensitive,
        )
        .unwrap();
        assert!(is_workspace_excluded("packages/skip", &declared));
        // Not named by either list: outside the workspace, but not excluded.
        assert!(!is_workspace_excluded("examples/foo", &declared));
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
    fn path_dependencies_are_collected_from_every_dependency_table() {
        let parsed = parse_package_manifest(
            r#"
[package]
name = "a"
version = "0.1.0"

[dependencies]
b = { path = "../b" }
registry = "1"

[build-dependencies]
c = { path = "../c" }

[dev-dependencies]
d = { path = "../d" }

[target.'cfg(windows)'.dependencies]
e = { path = "../e" }
"#,
            "packages/a/Cargo.toml",
            &WorkspaceInherit::default(),
        )
        .unwrap()
        .unwrap();
        let mut paths = parsed.path_dependencies;
        paths.sort();
        assert_eq!(paths, vec!["../b", "../c", "../d", "../e"]);
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
    fn case_insensitive_matching_follows_the_probed_filesystem() {
        let strict = cased_members(&["Packages/*"], PathCase::Sensitive);
        assert!(!is_workspace_member("packages/foo", &strict));
        assert!(is_workspace_member("Packages/foo", &strict));

        let relaxed = cased_members(&["Packages/*"], PathCase::Insensitive);
        assert!(is_workspace_member("packages/foo", &relaxed));
        assert!(is_workspace_member("Packages/foo", &relaxed));

        // The literal and `foo/**` prefix fast paths follow the same rules as
        // the compiled matcher.
        let prefix = cased_members(&["Packages/**"], PathCase::Insensitive);
        assert!(is_workspace_member("packages", &prefix));
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
include = ["src/**", "README.md"]
"#,
            "packages/foo/Cargo.toml",
            &WorkspaceInherit::default(),
        )
        .unwrap()
        .unwrap();
        assert!(parsed.packaging.is_released("src/lib.rs"));
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

    /// Cargo's conventional binary and example layouts are reconstructed.
    ///
    /// Historical target shape is inferred without invoking Cargo, so every layout
    /// that can make Cargo package a lockfile must be recognised from tree paths.
    #[test]
    fn conventional_lockfile_targets_are_discovered() {
        let targets = TargetDiscovery::default();

        for path in [
            "src/main.rs",
            "src/bin/tool.rs",
            "src/bin/tool/main.rs",
            "examples/demo.rs",
            "examples/demo/main.rs",
        ] {
            assert!(
                targets.has_lockfile_target([path], PathCase::Sensitive),
                "{path} should be a lockfile-bearing target"
            );
        }
        for path in [
            "src/lib.rs",
            "src/bin/tool/data.rs",
            "src/bin/.scratch.rs",
            "src/bin/.scratch/main.rs",
            "examples/demo/data.rs",
            "examples/.scratch.rs",
            "examples/.scratch/main.rs",
            "tests/demo.rs",
        ] {
            assert!(
                !targets.has_lockfile_target([path], PathCase::Sensitive),
                "{path} should not be a lockfile-bearing target"
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

[[example]]
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

    fn root_doc(content: &str) -> DocumentMut {
        content.parse().unwrap()
    }
}
