// Work-tree package discovery via `cargo metadata --no-deps`.
//
// The design forbids resolving a full graph or compiling. `--no-deps` is the
// only Cargo invocation used for classification.

use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use ohno::AppError;
use semver::{Op, Version, VersionReq};
use serde::Deserialize;
use serde_json::Value;
use toml_edit::{DocumentMut, Item};

use crate::command::run_capture;
use crate::git::{GitRepo, join_git_rel};
use crate::groups::Groups;
#[cfg(test)]
use crate::inherited::InheritedKeys;
#[cfg(test)]
use crate::manifest::TargetDiscovery;
use crate::manifest::{
    PackageManifest, PathCase, WorkspaceInherit, for_each_dependency_table,
    for_each_dependency_table_with_context, parse_document, parse_package_manifest,
    workspace_relative_path,
};
#[cfg(test)]
use crate::packaging::PackagingRules;
use crate::packaging::relativize;
use crate::{
    InvalidVersionError, LegacyVersionGroupsError, MalformedPrivateApiError, ParseMetadataError,
    ReadFileError, UnsupportedExactRequirementError,
};

/// Work-tree snapshot from `cargo metadata --no-deps`.
#[derive(Debug)]
pub(crate) struct WorkTree {
    pub(crate) workspace_root: PathBuf,
    pub(crate) packages: Vec<WorkPackage>,
    /// Every Git-tracked member whose declared version a plan may set.
    pub(crate) version_targets: Vec<VersionTarget>,
    /// Valid exact declarations used to derive groups and diagnose stale pins.
    pub(crate) exact_dependencies: Vec<ExactDependency>,
    /// Manifest paths of every member, publishable or not.
    ///
    /// `apply` rewrites dependency requirements in all of them, because a
    /// non-publishable member can still pin a package the plan increments.
    pub(crate) member_manifests: Vec<PathBuf>,
    /// Declared package name of every member, keyed by its manifest directory.
    ///
    /// `apply` rewrites a `path` dependency only after resolving that path to a
    /// member directory declaring the same package, so a same-named package
    /// living outside the workspace is left alone.
    pub(crate) members_by_dir: BTreeMap<PathBuf, String>,
    pub(crate) groups: Groups,
}

impl WorkTree {
    /// Returns declared versions for every Git-tracked version target.
    pub(crate) fn target_versions(&self) -> BTreeMap<String, Version> {
        self.version_targets
            .iter()
            .map(|target| (target.name.clone(), target.version.clone()))
            .collect()
    }
}

/// A Git-tracked workspace member whose declared version plans may set.
#[derive(Clone, Debug)]
pub(crate) struct VersionTarget {
    pub(crate) name: String,
    pub(crate) version: Version,
    pub(crate) manifest_path: PathBuf,
    pub(crate) publishable: bool,
}

/// One validated exact dependency between tracked workspace members.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ExactDependency {
    pub(crate) source: String,
    pub(crate) target: String,
    pub(crate) requirement: String,
    pub(crate) manifest_path: PathBuf,
    pub(crate) location: String,
}

/// One publishable workspace member in the work tree.
#[derive(Clone, Debug)]
pub(crate) struct WorkPackage {
    pub(crate) manifest: PackageManifest,
    pub(crate) manifest_path: PathBuf,
    pub(crate) dependencies: Vec<ReportedDep>,
    /// Whether the package presents a library API contract to consumers.
    ///
    /// True when the package has a library target and has not declared
    /// `[package.metadata.release-plan] private-api = true`. A package
    /// declares itself private to say that its library exists to serve another
    /// package rather than to be depended on directly, which is a release-policy
    /// statement the package makes about itself rather than something derivable
    /// from its code.
    ///
    /// This is reported as evidence; what to do with it is the consumer's
    /// decision.
    /// Ref: docs/design.md, "Consumer contracts".
    pub(crate) consumer_contract: bool,
    /// Whether the package builds a target that makes its locked closure relevant.
    ///
    /// Ref: docs/design.md, "Relevant lockfile closures".
    pub(crate) has_lockfile_target: bool,
    /// Files Cargo packs because a manifest key names them.
    ///
    /// Keyed by the path each takes inside the package archive.
    ///
    /// Resolution needs the repository layout, which `cargo metadata` does not
    /// describe, so classification fills this in once the repository is known.
    pub(crate) resources: BTreeMap<String, String>,
}

/// Intra-workspace dependency as exposed in `report.json`.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub(crate) struct ReportedDep {
    pub(crate) name: String,
    pub(crate) req: String,
    pub(crate) exact_pin: bool,
    /// Which dependency table declares this edge.
    ///
    /// Not reported: it decides how the edge is judged here, and the release
    /// decision reads the judgement rather than repeating it.
    #[serde(skip)]
    pub(crate) kind: DepKind,
    /// Whether the dependent's public API exposes types from this dependency.
    ///
    /// Read from the dependent's `allowed_external_types` allow-list, which
    /// names every type outside the crate that its public API may expose. The
    /// allow-list is a declaration rather than an observation, but
    /// `check-external-types` fails on an exposed type the list omits, so a
    /// passing workspace makes it a superset of what is genuinely exposed.
    /// Erring wide is the safe direction here: a dependency wrongly called
    /// public over-states a change level, while a missed one would publish a
    /// broken contract.
    /// Ref: docs/external-types.md; docs/design.md, "Public dependencies".
    pub(crate) public: bool,
}

/// The dependency kinds Cargo distinguishes, as they matter to a release.
///
/// Only a normal dependency can supply types to a library's public API. A
/// development dependency additionally does not survive packaging when it is
/// declared without a version, so it reaches no published manifest at all.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum DepKind {
    #[default]
    Normal,
    Build,
    Dev,
}

impl DepKind {
    fn from_metadata(kind: Option<&str>) -> Self {
        match kind {
            Some("dev") => Self::Dev,
            Some("build") => Self::Build,
            _ => Self::Normal,
        }
    }
}

/// Raw `cargo metadata` document before conversion to [`WorkTree`].
#[derive(Debug, Deserialize)]
struct MetadataJson {
    packages: Vec<MetadataPackage>,
    workspace_members: Vec<String>,
    workspace_root: String,
    #[serde(default)]
    metadata: Value,
}

/// One package object from the metadata document.
#[derive(Debug, Deserialize)]
struct MetadataPackage {
    name: String,
    version: String,
    id: String,
    manifest_path: String,
    #[serde(default)]
    publish: Option<Vec<String>>,
    #[serde(default)]
    dependencies: Vec<MetadataDep>,
    #[serde(default)]
    targets: Vec<MetadataTarget>,
    #[serde(default)]
    metadata: Value,
}

/// One build target from the metadata document.
#[derive(Debug, Deserialize)]
struct MetadataTarget {
    name: String,
    #[serde(default)]
    kind: Vec<String>,
}

/// One declared dependency from the metadata document.
#[derive(Debug, Deserialize)]
struct MetadataDep {
    name: String,
    req: String,
    #[serde(default)]
    rename: Option<String>,
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    kind: Option<String>,
}

/// Git-tracked inputs that constrain Cargo's work-tree metadata.
///
/// Cargo still supplies manifest normalization and dependency relationships,
/// while this scope prevents untracked manifests and auto-discovered targets
/// from entering the released-content model.
/// Ref: docs/implementation.md, "Workspace snapshots".
struct TrackedMetadata<'a> {
    git: &'a GitRepo,
    workspace_root: &'a Path,
    paths: Vec<String>,
    case: PathCase,
}

impl TrackedMetadata<'_> {
    /// Whether Cargo's member manifest is recorded in Git.
    fn contains_manifest(&self, manifest_path: &str) -> bool {
        // Cargo paths are first made workspace-relative using Cargo's own root
        // spelling, then rebased with Git's prefix. Subtracting Git's root from a
        // Cargo path would fail for equivalent 8.3, symlinked, or substituted
        // spellings of the same directory.
        let Some(workspace_path) =
            workspace_relative_path(self.workspace_root, Path::new(manifest_path))
        else {
            return false;
        };
        let manifest_path = join_git_rel(self.git.prefix(), &workspace_path);
        self.paths
            .iter()
            .any(|path| self.case.same_path(path, &manifest_path))
    }

    /// Whether tracked, present package inputs define a lockfile-bearing target.
    fn has_lockfile_target(&self, manifest: &PackageManifest) -> Result<bool, AppError> {
        let package_dir = join_git_rel(self.git.prefix(), &manifest.directory);
        let mut present = Vec::new();
        for path in &self.paths {
            let Some(relative) = relativize(path, &package_dir) else {
                continue;
            };
            match fs::symlink_metadata(self.git.root().join(path)) {
                Ok(_) => present.push(relative),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(ReadFileError::caused_by(self.git.root().join(path), error).into());
                }
            }
        }
        Ok(manifest.targets.has_lockfile_target(present, self.case))
    }
}

/// Loads the current workspace while restricting release inputs to tracked files.
pub(crate) fn load_tracked_work_tree(
    manifest_path: &Path,
) -> Result<(WorkTree, GitRepo), AppError> {
    let metadata = query_metadata(manifest_path)?;
    let workspace_root = PathBuf::from(&metadata.workspace_root);
    let git = GitRepo::discover(&workspace_root)?;
    let tracked = TrackedMetadata {
        paths: git.ls_files("")?,
        case: PathCase::probe(&workspace_root),
        git: &git,
        workspace_root: &workspace_root,
    };
    let work_tree = work_tree_from_metadata(&metadata, &tracked)?;
    Ok((work_tree, git))
}

fn query_metadata(manifest_path: &Path) -> Result<MetadataJson, AppError> {
    // Cargo resolves a relative `--manifest-path` against the child's working
    // directory, so the child inherits this process's directory and the path is
    // passed through unchanged. Deriving the directory from the path instead
    // would resolve any leading directory component twice.
    let cwd = Path::new(".");
    // `--no-deps` is the classification Cargo invocation: no graph resolve and
    // no crates.io. `--offline` is omitted so a workspace without a lockfile
    // can still be classified; no registry packages are consulted.
    // The requested schema version is pinned because the `Metadata*`
    // projections in this module deserialize exactly that documented contract.
    let metadata = run_capture(
        "cargo",
        &[
            "metadata",
            "--no-deps",
            "--format-version",
            "1",
            "--manifest-path",
            &manifest_path.to_string_lossy(),
        ],
        cwd,
    )?;
    Ok(serde_json::from_str(&metadata).map_err(ParseMetadataError::caused_by)?)
}

fn work_tree_from_metadata(
    metadata: &MetadataJson,
    tracked: &TrackedMetadata<'_>,
) -> Result<WorkTree, AppError> {
    let workspace_root = PathBuf::from(&metadata.workspace_root);
    let cargo_member_ids: HashSet<&str> = metadata
        .workspace_members
        .iter()
        .map(String::as_str)
        .collect();
    let selected_member_ids: HashSet<&str> = metadata
        .packages
        .iter()
        .filter(|package| cargo_member_ids.contains(package.id.as_str()))
        .filter(|package| tracked.contains_manifest(&package.manifest_path))
        .map(|package| package.id.as_str())
        .collect();

    reject_legacy_groups(&metadata.metadata)?;
    let tracked_members_by_dir: BTreeMap<PathBuf, String> = metadata
        .packages
        .iter()
        .filter(|package| selected_member_ids.contains(package.id.as_str()))
        .filter_map(|package| {
            Path::new(&package.manifest_path)
                .parent()
                .map(|dir| (dir.to_path_buf(), package.name.clone()))
        })
        .collect();
    // Apply visits every member Cargo can see so an untracked or ignored
    // dependent cannot retain a stale exact pin. This set is deliberately wider
    // than the tracked package set accepted as plan targets.
    // Ref: docs/implementation.md, "Plan resolution and application".
    let members_by_dir: BTreeMap<PathBuf, String> = metadata
        .packages
        .iter()
        .filter(|package| cargo_member_ids.contains(package.id.as_str()))
        .filter_map(|package| {
            Path::new(&package.manifest_path)
                .parent()
                .map(|dir| (dir.to_path_buf(), package.name.clone()))
        })
        .collect();
    // The identifier a Rust path uses for a package is its library target name, which the
    // allow-list patterns are written in. Reading it from the target rather than deriving it
    // from the package name keeps a `[lib] name` override from silently breaking the match.
    let library_crate_names: BTreeMap<&str, String> = metadata
        .packages
        .iter()
        .filter(|package| cargo_member_ids.contains(package.id.as_str()))
        .filter_map(|package| library_crate_name(package).map(|lib| (package.name.as_str(), lib)))
        .collect();
    let root_manifest_path = workspace_root.join("Cargo.toml");
    let root_manifest = fs::read_to_string(&root_manifest_path)
        .map_err(|error| ReadFileError::caused_by(&root_manifest_path, error))?;
    let root_manifest = parse_document(&root_manifest_path, &root_manifest)?;
    let workspace = WorkspaceInherit::from_root(&root_manifest);
    let mut version_targets = Vec::new();
    for package in &metadata.packages {
        if !selected_member_ids.contains(package.id.as_str()) {
            continue;
        }
        let path = PathBuf::from(&package.manifest_path);
        let manifest_text =
            fs::read_to_string(&path).map_err(|error| ReadFileError::caused_by(&path, error))?;
        let git_manifest_path = workspace_relative_path(&workspace_root, &path).expect(
            "a selected manifest already matched a tracked path after this same conversion",
        );
        let Some(manifest) =
            parse_package_manifest(&manifest_text, &git_manifest_path, &workspace)?
        else {
            continue;
        };
        let version = package.version.parse::<Version>().map_err(|error| {
            InvalidVersionError::caused_by(&package.name, &package.version, error)
        })?;
        let publishable = !matches!(&package.publish, Some(registries) if registries.is_empty())
            && manifest.publish;
        version_targets.push(VersionTarget {
            name: package.name.clone(),
            version,
            manifest_path: path,
            publishable,
        });
    }
    version_targets.sort_by(|left, right| left.name.cmp(&right.name));

    let mut exact_dependencies = discover_exact_dependencies(
        metadata,
        &selected_member_ids,
        &tracked_members_by_dir,
        &root_manifest,
        &workspace_root,
    )?;
    exact_dependencies.sort_by(|left, right| {
        (
            &left.source,
            &left.location,
            &left.target,
            &left.requirement,
        )
            .cmp(&(
                &right.source,
                &right.location,
                &right.target,
                &right.requirement,
            ))
    });
    let groups = Groups::from_edges(
        version_targets.iter().map(|target| target.name.clone()),
        exact_dependencies
            .iter()
            .map(|dependency| (dependency.source.clone(), dependency.target.clone())),
    );

    let mut packages = Vec::new();
    let mut exposed_crates_by_package: BTreeMap<String, Vec<String>> = BTreeMap::new();

    for package in &metadata.packages {
        if !selected_member_ids.contains(package.id.as_str()) {
            continue;
        }
        if matches!(&package.publish, Some(regs) if regs.is_empty()) {
            continue;
        }
        let path = PathBuf::from(&package.manifest_path);
        let manifest_text =
            fs::read_to_string(&path).map_err(|error| ReadFileError::caused_by(&path, error))?;
        let git_manifest_path = workspace_relative_path(&workspace_root, &path).expect(
            "a selected manifest already matched a tracked path after this same conversion",
        );
        let Some(mut manifest) =
            parse_package_manifest(&manifest_text, &git_manifest_path, &workspace)?
        else {
            continue;
        };
        if !manifest.publish {
            continue;
        }
        let manifest_doc = parse_document(&path, &manifest_text)?;
        manifest.version = package.version.parse::<Version>().map_err(|error| {
            InvalidVersionError::caused_by(&package.name, &package.version, error)
        })?;
        manifest.name.clone_from(&package.name);

        let exposed_crates = allowed_external_crates(&package.metadata);
        let dependencies = package
            .dependencies
            .iter()
            .filter(|dep| {
                is_intra_workspace_released(
                    dep,
                    &tracked_members_by_dir,
                    &manifest_doc,
                    &root_manifest,
                )
            })
            .map(|dep| ReportedDep {
                name: dep.name.clone(),
                req: dep.req.clone(),
                exact_pin: dep.req.starts_with('='),
                kind: DepKind::from_metadata(dep.kind.as_deref()),
                // Resolved once every package's allow-list is known, below.
                public: false,
            })
            .collect();

        exposed_crates_by_package.insert(package.name.clone(), exposed_crates);

        packages.push(WorkPackage {
            has_lockfile_target: tracked.has_lockfile_target(&manifest)?,
            consumer_contract: is_consumer_contract(package)?,
            manifest,
            manifest_path: path,
            dependencies,
            resources: BTreeMap::new(),
        });
    }

    packages.sort_by(|a, b| a.manifest.name.cmp(&b.manifest.name));

    let mut member_manifests: Vec<PathBuf> = members_by_dir
        .keys()
        .map(|dir| dir.join("Cargo.toml"))
        .collect();
    member_manifests.sort();

    mark_public_dependencies(
        &mut packages,
        &exposed_crates_by_package,
        &library_crate_names,
    );

    Ok(WorkTree {
        workspace_root,
        packages,
        version_targets,
        exact_dependencies,
        member_manifests,
        members_by_dir,
        groups,
    })
}

fn reject_legacy_groups(metadata: &Value) -> Result<(), AppError> {
    if metadata
        .get("release-plan")
        .and_then(|plan| plan.get("groups"))
        .is_some()
    {
        return Err(LegacyVersionGroupsError::new().into());
    }
    Ok(())
}

/// Effective facts needed to identify and validate one dependency declaration.
struct EffectiveDependency<'a> {
    item: &'a Item,
    path_base: &'a Path,
    location: String,
}

fn discover_exact_dependencies(
    metadata: &MetadataJson,
    selected_member_ids: &HashSet<&str>,
    tracked_members_by_dir: &BTreeMap<PathBuf, String>,
    workspace_manifest: &DocumentMut,
    workspace_root: &Path,
) -> Result<Vec<ExactDependency>, AppError> {
    let mut found = Vec::new();
    for package in &metadata.packages {
        if !selected_member_ids.contains(package.id.as_str()) {
            continue;
        }
        let manifest_path = PathBuf::from(&package.manifest_path);
        let text = fs::read_to_string(&manifest_path)
            .map_err(|error| ReadFileError::caused_by(&manifest_path, error))?;
        let manifest = parse_document(&manifest_path, &text)?;
        let manifest_dir = manifest_path
            .parent()
            .expect("Cargo reports a manifest path with a parent directory");
        let mut error = None;
        for_each_dependency_table_with_context(
            manifest.as_table(),
            &mut |location, _, dependencies| {
                if error.is_some() {
                    return;
                }
                for (alias, item) in dependencies.iter() {
                    let effective = effective_dependency(
                        item,
                        alias,
                        location,
                        manifest_dir,
                        workspace_manifest,
                        workspace_root,
                    );
                    let Some(path) = dependency_field(effective.item, "path") else {
                        continue;
                    };
                    let Some(target) =
                        resolved_member(effective.path_base, path, tracked_members_by_dir)
                    else {
                        continue;
                    };
                    let package_name = dependency_field(effective.item, "package").unwrap_or(alias);
                    if package_name != target {
                        continue;
                    }
                    let Some(requirement) = dependency_requirement(effective.item) else {
                        continue;
                    };
                    match validated_exact_requirement(requirement) {
                        Ok(None) => {}
                        Ok(Some(_)) => found.push(ExactDependency {
                            source: package.name.clone(),
                            target: target.to_string(),
                            requirement: requirement.to_string(),
                            manifest_path: manifest_path.clone(),
                            location: effective.location,
                        }),
                        Err(()) => {
                            error = Some(
                                UnsupportedExactRequirementError::new(
                                    manifest_path.to_string_lossy(),
                                    alias,
                                    &effective.location,
                                    requirement,
                                )
                                .into(),
                            );
                            break;
                        }
                    }
                }
            },
        );
        if let Some(error) = error {
            return Err(error);
        }
    }
    Ok(found)
}

fn effective_dependency<'a>(
    item: &'a Item,
    alias: &str,
    location: &str,
    manifest_dir: &'a Path,
    workspace_manifest: &'a DocumentMut,
    workspace_root: &'a Path,
) -> EffectiveDependency<'a> {
    let inherited = item
        .as_table_like()
        .and_then(|table| table.get("workspace"))
        .and_then(Item::as_bool)
        == Some(true);
    if inherited
        && let Some(item) = workspace_manifest
            .get("workspace")
            .and_then(Item::as_table_like)
            .and_then(|workspace| workspace.get("dependencies"))
            .and_then(Item::as_table_like)
            .and_then(|dependencies| dependencies.get(alias))
    {
        return EffectiveDependency {
            item,
            path_base: workspace_root,
            location: format!("{location}.{alias} -> workspace.dependencies.{alias}"),
        };
    }
    EffectiveDependency {
        item,
        path_base: manifest_dir,
        location: format!("{location}.{alias}"),
    }
}

fn dependency_field<'a>(item: &'a Item, field: &str) -> Option<&'a str> {
    item.as_table_like()
        .and_then(|table| table.get(field))
        .and_then(Item::as_str)
}

fn dependency_requirement(item: &Item) -> Option<&str> {
    item.as_str().or_else(|| dependency_field(item, "version"))
}

fn resolved_member<'a>(
    base: &Path,
    dependency_path: &str,
    members_by_dir: &'a BTreeMap<PathBuf, String>,
) -> Option<&'a str> {
    let joined = normalize_path(&base.join(dependency_path));
    if let Some(name) = members_by_dir.get(&joined) {
        return Some(name);
    }
    let resolved = fs::canonicalize(&joined).ok()?;
    members_by_dir.iter().find_map(|(member_dir, name)| {
        fs::canonicalize(member_dir)
            .is_ok_and(|member| member == resolved)
            .then_some(name.as_str())
    })
}

fn normalize_path(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                normalized.pop();
            }
            other => normalized.push(other),
        }
    }
    normalized
}

fn validated_exact_requirement(requirement: &str) -> Result<Option<Version>, ()> {
    let parsed = match VersionReq::parse(requirement) {
        Ok(parsed) => parsed,
        Err(_) if contains_exact_comparator(requirement) => return Err(()),
        Err(_) => return Ok(None),
    };
    if !parsed
        .comparators
        .iter()
        .any(|comparator| comparator.op == Op::Exact)
    {
        return Ok(None);
    }

    let Some(version) = requirement.trim().strip_prefix('=') else {
        return Err(());
    };
    let version = version.trim();
    if parsed.comparators.len() != 1 || version.split('.').count() != 3 {
        return Err(());
    }
    let version = Version::parse(version).map_err(|_error| ())?;
    if !version.pre.is_empty() || !version.build.is_empty() {
        return Err(());
    }
    Ok(Some(version))
}

fn contains_exact_comparator(requirement: &str) -> bool {
    let bytes = requirement.as_bytes();
    bytes.iter().enumerate().any(|(index, byte)| {
        *byte == b'='
            && index
                .checked_sub(1)
                .and_then(|previous| bytes.get(previous))
                .is_none_or(|previous| !matches!(previous, b'>' | b'<' | b'!'))
    })
}

/// Reports whether a dependency edge is published and points at a workspace member.
///
/// Normal and build dependencies are recorded in the published manifest, so a
/// version decision on the target cascades to this package. A dev dependency
/// survives packaging only when its manifest declaration supplies a version
/// requirement; Cargo reports both an explicit wildcard and no requirement as
/// `*`, so metadata alone cannot distinguish them.
/// Whether the package presents a library API contract to consumers.
///
/// Two facts combine. A package with no library target has no library API at
/// all. A package that declares
/// `[package.metadata.release-plan] private-api = true` has one but states that
/// it is private: an implementation partition behind a public package, or a
/// crate published only because Cargo requires a dependency to be published.
///
/// The declaration is read from the package rather than inferred, because no
/// property of the code distinguishes a library meant for consumers from one
/// meant for a sibling crate. Adjacent facts such as `[lib] doc = false`
/// correlate in some workspaces but mean something else, so keying release
/// policy on them would silently mis-classify a package whose author changed
/// one for an unrelated reason.
///
/// A package is public unless it declares otherwise. The safe direction: a new
/// package is assessed by default, and a package wrongly assessed reports a
/// finding a maintainer can see, while a package wrongly skipped reports
/// nothing at all.
fn is_consumer_contract(package: &MetadataPackage) -> Result<bool, AppError> {
    let has_library = package.targets.iter().any(|target| {
        target
            .kind
            .iter()
            .any(|kind| LIBRARY_TARGET_KINDS.contains(&kind.as_str()))
    });
    if !has_library {
        return Ok(false);
    }
    let Some(declared) = package
        .metadata
        .get("release-plan")
        .and_then(|value| value.get("private-api"))
    else {
        return Ok(true);
    };
    declared
        .as_bool()
        .map(|private| !private)
        .ok_or_else(|| MalformedPrivateApiError::new(&package.name, declared.to_string()).into())
}

/// The identifier a Rust path uses for a package's library, if it has one.
///
/// A package without a library target exposes no API for another crate to
/// re-export, so it can never be a public dependency.
fn library_crate_name(package: &MetadataPackage) -> Option<String> {
    package
        .targets
        .iter()
        .find(|target| {
            target
                .kind
                .iter()
                .any(|kind| LIBRARY_TARGET_KINDS.contains(&kind.as_str()))
        })
        // Cargo already applies this substitution when deriving a target name from a package
        // name, but a `[lib] name` written with hyphens would not be a legal path segment.
        .map(|target| target.name.replace('-', "_"))
}

/// Target kinds that produce a library another crate can name in a path.
const LIBRARY_TARGET_KINDS: &[&str] =
    &["lib", "rlib", "dylib", "cdylib", "staticlib", "proc-macro"];

/// Leading path segments of a package's `allowed_external_types` allow-list.
///
/// Each entry is a `::`-separated type path whose first segment names the crate
/// the type comes from, so the leading segments are the crates this package's
/// public API is permitted to expose. A pattern may glob, as `cbh_*::*` does.
/// Ref: docs/external-types.md.
fn allowed_external_crates(metadata: &Value) -> Vec<String> {
    let Some(patterns) = metadata
        .get("cargo_check_external_types")
        .and_then(|value| value.get("allowed_external_types"))
        .and_then(Value::as_array)
    else {
        // An absent allow-list is not an absent opinion: cargo-check-external-types then permits
        // no external type at all, so the package exposes none.
        return Vec::new();
    };
    patterns
        .iter()
        .filter_map(Value::as_str)
        .filter_map(|pattern| {
            let segment = pattern.split("::").next().unwrap_or_default().trim();
            (!segment.is_empty()).then(|| segment.to_string())
        })
        .collect()
}

/// Whether `candidate` matches a `wildmatch` pattern, where `*` spans any run
/// of characters and `?` matches exactly one.
///
/// cargo-check-external-types matches `allowed_external_types` entries with
/// `wildmatch`, so a pattern it accepts has to reach the same crates here.
/// Matching walks characters rather than bytes, so a multi-byte character is
/// one `?`, as `wildmatch` treats it.
fn glob_matches(pattern: &str, candidate: &str) -> bool {
    let pattern: Vec<char> = pattern.chars().collect();
    let candidate: Vec<char> = candidate.chars().collect();
    // Position in each, plus the last `*` seen and where the candidate had reached then, which
    // is what a failed match backtracks to instead of recursing.
    let (mut p, mut c) = (0_usize, 0_usize);
    let mut star: Option<(usize, usize)> = None;
    while c < candidate.len() {
        let current = candidate.get(c).copied();
        match pattern.get(p) {
            Some('*') => {
                star = Some((p, c));
                p = p.saturating_add(1);
            }
            Some('?') => {
                p = p.saturating_add(1);
                c = c.saturating_add(1);
            }
            Some(literal) if Some(*literal) == current => {
                p = p.saturating_add(1);
                c = c.saturating_add(1);
            }
            _ => {
                // Let the most recent `*` absorb one more character and try again.
                let Some((star_p, star_c)) = star else {
                    return false;
                };
                p = star_p.saturating_add(1);
                c = star_c.saturating_add(1);
                star = Some((star_p, c));
            }
        }
    }
    pattern
        .get(p..)
        .is_some_and(|rest| rest.iter().all(|entry| *entry == '*'))
}

/// Marks the dependency edges through which each package exposes another crate's types.
///
/// A package names the crates its public API may expose, but it does not
/// necessarily depend on them directly: an implementation crate's types
/// normally reach consumers re-exported through the public crate in front of
/// it, so `region_local` names `many_cpus_impl` while depending on `many_cpus`.
/// The re-exporting crate closes that gap, because it must itself declare the
/// crate it re-exports. Following those declarations transitively is what
/// attributes a named crate to the direct dependency that actually supplies it.
///
/// Only a normal dependency can supply types to a library's public API, so a
/// build or development dependency is never public however the allow-lists read.
/// Ref: docs/design.md, "Public dependencies".
fn mark_public_dependencies(
    packages: &mut [WorkPackage],
    exposed_crates_by_package: &BTreeMap<String, Vec<String>>,
    library_crate_names: &BTreeMap<&str, String>,
) {
    // The workspace packages each package's allow-list names outright.
    let mut named: BTreeMap<String, HashSet<String>> = BTreeMap::new();
    for (package, patterns) in exposed_crates_by_package {
        let matched = library_crate_names
            .iter()
            .filter(|(_, library)| {
                patterns
                    .iter()
                    .any(|pattern| glob_matches(pattern, library))
            })
            .map(|(name, _)| (*name).to_string())
            .collect();
        named.insert(package.clone(), matched);
    }

    let normal_dependencies: BTreeMap<String, Vec<String>> = packages
        .iter()
        .map(|package| {
            (
                package.manifest.name.clone(),
                package
                    .dependencies
                    .iter()
                    .filter(|dependency| dependency.kind == DepKind::Normal)
                    .map(|dependency| dependency.name.clone())
                    .collect(),
            )
        })
        .collect();

    // What each package publicly exposes, including what it re-exports from further down. A
    // package exposes itself, so a direct dependency is caught by the same intersection test.
    let mut exposes: BTreeMap<String, HashSet<String>> = normal_dependencies
        .keys()
        .map(|name| {
            let mut own: HashSet<String> = named.get(name).cloned().unwrap_or_default();
            own.insert(name.clone());
            (name.clone(), own)
        })
        .collect();

    // An edge admitted in one pass can widen what its dependent exposes, which can admit a
    // further edge, so the sets are grown until they stop changing. They only ever grow and are
    // bounded by the workspace, so this settles; the bound is asserted rather than assumed.
    let mut remaining_passes = normal_dependencies.len().saturating_add(1);
    let mut settled = false;
    while !settled {
        assert!(
            remaining_passes > 0,
            "public-dependency closure did not settle; this is a defect in the exposure model"
        );
        remaining_passes = remaining_passes.saturating_sub(1);
        settled = true;
        for (name, dependencies) in &normal_dependencies {
            let wanted = named.get(name).cloned().unwrap_or_default();
            let mut added: HashSet<String> = HashSet::new();
            for dependency in dependencies {
                let Some(reachable) = exposes.get(dependency) else {
                    continue;
                };
                if reachable.is_disjoint(&wanted) {
                    continue;
                }
                added.extend(reachable.iter().cloned());
            }
            let own = exposes
                .get_mut(name)
                .expect("every package was seeded above");
            let before = own.len();
            own.extend(added);
            if own.len() != before {
                settled = false;
            }
        }
    }

    for package in packages.iter_mut() {
        let wanted = named
            .get(&package.manifest.name)
            .cloned()
            .unwrap_or_default();
        for dependency in &mut package.dependencies {
            dependency.public = dependency.kind == DepKind::Normal
                && exposes
                    .get(&dependency.name)
                    .is_some_and(|reachable| !reachable.is_disjoint(&wanted));
        }
    }
}

fn is_intra_workspace_released(
    dep: &MetadataDep,
    members_by_dir: &BTreeMap<PathBuf, String>,
    manifest: &DocumentMut,
    workspace_manifest: &DocumentMut,
) -> bool {
    let Some(path) = &dep.path else {
        return false;
    };
    if !members_by_dir.contains_key(Path::new(path)) {
        return false;
    }
    dep.kind.as_deref().unwrap_or("normal") != "dev"
        || dev_dependency_declares_version(dep, manifest, workspace_manifest)
}

/// Determines whether a development dependency survives Cargo's manifest normalization.
///
/// The package declaration is authoritative. An inherited declaration delegates
/// version presence to the workspace entry with the same local dependency name.
fn dev_dependency_declares_version(
    dep: &MetadataDep,
    manifest: &DocumentMut,
    workspace_manifest: &DocumentMut,
) -> bool {
    let local_name = dep.rename.as_deref().unwrap_or(&dep.name);
    let mut declares_version = false;
    for_each_dependency_table(manifest.as_table(), &mut |kind, dependencies| {
        if kind != "dev-dependencies" || declares_version {
            return;
        }
        let Some(item) = dependencies.get(local_name) else {
            return;
        };
        if dependency_item_declares_version(item) {
            declares_version = true;
            return;
        }
        let inherits = item
            .as_table_like()
            .and_then(|table| table.get("workspace"))
            .and_then(Item::as_bool)
            == Some(true);
        declares_version =
            inherits && workspace_dependency_declares_version(workspace_manifest, local_name);
    });
    declares_version
}

/// Determines whether one dependency item explicitly carries a version requirement.
fn dependency_item_declares_version(item: &Item) -> bool {
    item.as_str().is_some()
        || item
            .as_table_like()
            .is_some_and(|table| table.get("version").is_some())
}

/// Determines whether an inherited workspace dependency carries a version requirement.
fn workspace_dependency_declares_version(manifest: &DocumentMut, name: &str) -> bool {
    manifest
        .get("workspace")
        .and_then(Item::as_table_like)
        .and_then(|workspace| workspace.get("dependencies"))
        .and_then(Item::as_table_like)
        .and_then(|dependencies| dependencies.get(name))
        .is_some_and(dependency_item_declares_version)
}

pub(crate) fn dependents_of(packages: &[WorkPackage], name: &str) -> Vec<String> {
    packages
        .iter()
        .filter(|package| package.dependencies.iter().any(|dep| dep.name == name))
        .map(|package| package.manifest.name.clone())
        .collect()
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use serde_json::json;

    use super::*;

    fn doc(text: &str) -> DocumentMut {
        parse_document(Path::new("Cargo.toml"), text).unwrap()
    }

    #[test]
    fn the_legacy_group_key_is_rejected_in_every_shape() {
        for groups in [json!({}), json!([]), json!(null)] {
            let metadata = json!({ "release-plan": { "groups": groups } });
            let error = reject_legacy_groups(&metadata).unwrap_err();
            assert!(error.find_source::<LegacyVersionGroupsError>().is_some());
        }
        reject_legacy_groups(&json!({ "release-plan": { "private-api": true } })).unwrap();
    }

    #[test]
    fn exact_requirement_validation_accepts_only_a_plain_triplet() {
        for requirement in ["=1.2.3", "= 1.2.3", " =1.2.3 "] {
            assert_eq!(
                validated_exact_requirement(requirement),
                Ok(Some("1.2.3".parse().unwrap()))
            );
        }
        for requirement in [
            "=1",
            "=1.2",
            "=1.2.3-alpha",
            "=1.2.3+build",
            "=1.2.3, <2.0.0",
            "^1.0.0, =1.2.3",
        ] {
            assert_eq!(validated_exact_requirement(requirement), Err(()));
        }
        for requirement in ["1.2.3", "^1.2.3", ">=1.2.3", "<=2.0.0"] {
            assert_eq!(validated_exact_requirement(requirement), Ok(None));
        }
    }

    #[test]
    fn released_intra_workspace_deps_require_a_member_directory() {
        let dirs = BTreeMap::from([(PathBuf::from("/ws/packages/bar"), "bar".to_string())]);
        let path_dep = MetadataDep {
            name: "bar".to_string(),
            req: "0.1.0".to_string(),
            rename: None,
            path: Some("/ws/packages/bar".to_string()),
            kind: None,
        };
        assert!(is_intra_workspace_released(
            &path_dep,
            &dirs,
            &doc(""),
            &doc("")
        ));
        let named = MetadataDep {
            name: "bar".to_string(),
            req: "0.1.0".to_string(),
            rename: None,
            path: None,
            kind: Some("normal".to_string()),
        };
        assert!(!is_intra_workspace_released(
            &named,
            &dirs,
            &doc(""),
            &doc("")
        ));
        let colliding = MetadataDep {
            name: "bar".to_string(),
            req: "0.1.0".to_string(),
            rename: None,
            path: Some("/other/bar".to_string()),
            kind: None,
        };
        assert!(!is_intra_workspace_released(
            &colliding,
            &dirs,
            &doc(""),
            &doc("")
        ));
        let build = MetadataDep {
            name: "bar".to_string(),
            req: "0.1.0".to_string(),
            rename: None,
            path: Some("/ws/packages/bar".to_string()),
            kind: Some("build".to_string()),
        };
        assert!(is_intra_workspace_released(
            &build,
            &dirs,
            &doc(""),
            &doc("")
        ));
        let dev = MetadataDep {
            name: "bar".to_string(),
            req: "0.1.0".to_string(),
            rename: None,
            path: Some("/ws/packages/bar".to_string()),
            kind: Some("dev".to_string()),
        };
        assert!(is_intra_workspace_released(
            &dev,
            &dirs,
            &doc("[dev-dependencies]\nbar = { path = \"../bar\", version = \"0.1.0\" }\n"),
            &doc("")
        ));
        let path_only_dev = MetadataDep {
            name: "bar".to_string(),
            req: "*".to_string(),
            rename: None,
            path: Some("/ws/packages/bar".to_string()),
            kind: Some("dev".to_string()),
        };
        assert!(!is_intra_workspace_released(
            &path_only_dev,
            &dirs,
            &doc("[dev-dependencies]\nbar = { path = \"../bar\" }\n"),
            &doc("")
        ));
        let wildcard_dev = MetadataDep {
            name: "bar".to_string(),
            req: "*".to_string(),
            rename: None,
            path: Some("/ws/packages/bar".to_string()),
            kind: Some("dev".to_string()),
        };
        assert!(is_intra_workspace_released(
            &wildcard_dev,
            &dirs,
            &doc("[dev-dependencies]\nbar = { path = \"../bar\", version = \"*\" }\n"),
            &doc("")
        ));
        // A normal dependency without a version requirement still survives
        // packaging, because Cargo strips only path-only dev dependencies.
        let path_only_normal = MetadataDep {
            name: "bar".to_string(),
            req: "*".to_string(),
            rename: None,
            path: Some("/ws/packages/bar".to_string()),
            kind: None,
        };
        assert!(is_intra_workspace_released(
            &path_only_normal,
            &dirs,
            &doc(""),
            &doc("")
        ));
        let foreign = MetadataDep {
            name: "serde".to_string(),
            req: "1.0.0".to_string(),
            rename: None,
            path: None,
            kind: None,
        };
        assert!(!is_intra_workspace_released(
            &foreign,
            &dirs,
            &doc(""),
            &doc("")
        ));
    }

    #[test]
    fn inherited_wildcard_dev_dependency_is_released() {
        let dirs = BTreeMap::from([(PathBuf::from("/ws/packages/bar"), "bar".to_string())]);
        let dep = MetadataDep {
            name: "bar".to_string(),
            req: "*".to_string(),
            rename: Some("bar_alias".to_string()),
            path: Some("/ws/packages/bar".to_string()),
            kind: Some("dev".to_string()),
        };
        let member = doc("[dev-dependencies]\nbar_alias.workspace = true\n");
        let versionless_workspace = doc(
            "[workspace.dependencies]\nbar_alias = { package = \"bar\", path = \"packages/bar\" }\n",
        );
        assert!(!is_intra_workspace_released(
            &dep,
            &dirs,
            &member,
            &versionless_workspace
        ));
        let versioned_workspace = doc(
            "[workspace.dependencies]\nbar_alias = { package = \"bar\", path = \"packages/bar\", version = \"*\" }\n",
        );
        assert!(is_intra_workspace_released(
            &dep,
            &dirs,
            &member,
            &versioned_workspace
        ));
    }

    /// A pattern matches the crates cargo-check-external-types would allow.
    ///
    /// The tool matches these with `wildmatch`, where `*` spans any run of characters and `?`
    /// matches exactly one, so a pattern it accepts has to reach the same crates here.
    #[test]
    fn a_glob_pattern_matches_the_crates_it_covers() {
        assert!(glob_matches("cbh_*", "cbh_model"));
        assert!(glob_matches("cbh_*", "cbh_"));
        assert!(glob_matches("*", "anything"));
        assert!(glob_matches("*", ""));
        assert!(glob_matches("nm_impl", "nm_impl"));
        assert!(glob_matches("a*c", "abc"));
        assert!(glob_matches("a*c", "ac"));
        assert!(glob_matches("", ""));

        assert!(!glob_matches("cbh_*", "nm_impl"));
        assert!(!glob_matches("nm_impl", "nm"));
        assert!(!glob_matches("nm", "nm_impl"));
        assert!(!glob_matches("a*c", "abd"));
        assert!(!glob_matches("", "nm"));

        // A `*` must be able to give characters back to a later literal.
        assert!(glob_matches("a*bc", "abxbc"));
        assert!(glob_matches("*a*b*", "xaybz"));
        assert!(!glob_matches("a*bc", "abxb"));

        // `?` matches exactly one character.
        assert!(glob_matches("cbh_?", "cbh_a"));
        assert!(!glob_matches("cbh_?", "cbh_"));
        assert!(!glob_matches("cbh_?", "cbh_ab"));

        // A multi-byte character is one character, not one byte.
        assert!(glob_matches("?", "é"));
        assert!(glob_matches("a*é", "abcé"));
    }

    /// Exposure follows re-exports to the dependency that actually supplies the named crate.
    ///
    /// `outer` names `impl` in its allow-list but depends on `facade`, which re-exports it. The
    /// edge that must be marked is `outer -> facade`, because that is the requirement whose
    /// version moves when `impl` breaks. A crate no one names stays private, and a build or
    /// development edge never counts because neither can supply types to a library's API.
    #[test]
    fn exposure_follows_re_exports_to_the_supplying_dependency() {
        fn work_package(name: &str, dependencies: Vec<ReportedDep>) -> WorkPackage {
            WorkPackage {
                manifest: PackageManifest {
                    name: name.to_string(),
                    version: "0.1.0".parse().unwrap(),
                    directory: format!("packages/{name}"),
                    packaging: PackagingRules::default(),
                    inherited: InheritedKeys::default(),
                    publish: true,
                    path_dependencies: Vec::new(),
                    inherited_path_dependencies: Vec::new(),
                    resource_paths: Vec::new(),
                    inherited_resource_paths: Vec::new(),
                    auto_readme: false,
                    targets: TargetDiscovery::default(),
                },
                manifest_path: PathBuf::from(format!("packages/{name}/Cargo.toml")),
                dependencies,
                has_lockfile_target: false,
                consumer_contract: true,
                resources: BTreeMap::new(),
            }
        }
        fn edge(name: &str, kind: DepKind) -> ReportedDep {
            ReportedDep {
                name: name.to_string(),
                req: "0.1.0".to_string(),
                exact_pin: false,
                kind,
                public: false,
            }
        }
        fn is_public(packages: &[WorkPackage], from: &str, to: &str) -> bool {
            packages
                .iter()
                .find(|package| package.manifest.name == from)
                .and_then(|package| {
                    package
                        .dependencies
                        .iter()
                        .find(|dependency| dependency.name == to)
                })
                .is_some_and(|dependency| dependency.public)
        }

        let mut packages = vec![
            work_package(
                "outer",
                vec![
                    edge("facade", DepKind::Normal),
                    edge("private", DepKind::Normal),
                    edge("tool", DepKind::Build),
                    edge("harness", DepKind::Dev),
                ],
            ),
            work_package("facade", vec![edge("implementation", DepKind::Normal)]),
            work_package("implementation", Vec::new()),
            work_package("private", Vec::new()),
            work_package("tool", Vec::new()),
            work_package("harness", Vec::new()),
        ];
        // `outer` and `harness` name the defining crate; `facade` names what it re-exports.
        let exposed = BTreeMap::from([
            ("outer".to_string(), vec!["implementation".to_string()]),
            ("facade".to_string(), vec!["implementation".to_string()]),
            ("harness".to_string(), vec!["implementation".to_string()]),
        ]);
        let libraries = BTreeMap::from([
            ("facade", "facade".to_string()),
            ("implementation", "implementation".to_string()),
            ("private", "private".to_string()),
            ("tool", "tool".to_string()),
            ("harness", "harness".to_string()),
        ]);

        mark_public_dependencies(&mut packages, &exposed, &libraries);

        // The re-export chain is public at every hop.
        assert!(is_public(&packages, "outer", "facade"));
        assert!(is_public(&packages, "facade", "implementation"));

        // A crate `outer` never names is private even though it is a normal dependency.
        assert!(!is_public(&packages, "outer", "private"));

        // Neither non-normal kind can supply types to a library's public API, so neither is
        // public even though both reach a package naming the exposed crate.
        assert!(!is_public(&packages, "outer", "tool"));
        assert!(!is_public(&packages, "outer", "harness"));
    }

    /// An absent allow-list permits no external type, so it exposes no crate.
    #[test]
    fn an_absent_allow_list_exposes_no_crate() {
        assert!(allowed_external_crates(&Value::Null).is_empty());
        assert!(allowed_external_crates(&serde_json::json!({})).is_empty());
        assert!(
            allowed_external_crates(&serde_json::json!({ "cargo_check_external_types": {} }))
                .is_empty()
        );
    }

    /// Only the leading path segment of each entry names a crate.
    #[test]
    fn an_allow_list_yields_the_leading_segment_of_each_entry() {
        let metadata = serde_json::json!({
            "cargo_check_external_types": {
                "allowed_external_types": [
                    "many_cpus_impl::system_hardware::SystemHardware",
                    "cbh_*::*",
                    "linked::family::Family",
                ]
            }
        });

        assert_eq!(
            allowed_external_crates(&metadata),
            vec![
                "many_cpus_impl".to_string(),
                "cbh_*".to_string(),
                "linked".to_string()
            ]
        );
    }

    /// A package's library target names the crate a path refers to.
    #[test]
    fn a_library_target_name_is_the_crate_name_in_a_path() {
        fn metadata_package(targets: Vec<MetadataTarget>) -> MetadataPackage {
            MetadataPackage {
                name: "demo-package".to_string(),
                version: "0.1.0".to_string(),
                id: "demo".to_string(),
                manifest_path: "packages/demo/Cargo.toml".to_string(),
                publish: None,
                dependencies: Vec::new(),
                targets,
                metadata: Value::Null,
            }
        }
        fn target(name: &str, kind: &str) -> MetadataTarget {
            MetadataTarget {
                name: name.to_string(),
                kind: vec![kind.to_string()],
            }
        }

        // A hyphenated package name becomes an underscored crate name.
        assert_eq!(
            library_crate_name(&metadata_package(vec![target("demo-package", "lib")])),
            Some("demo_package".to_string())
        );
        // A proc-macro crate is still nameable in a path.
        assert_eq!(
            library_crate_name(&metadata_package(vec![target("macros", "proc-macro")])),
            Some("macros".to_string())
        );
        // A binary-only package exposes no library to re-export.
        assert_eq!(
            library_crate_name(&metadata_package(vec![target("demo", "bin")])),
            None
        );
        assert_eq!(library_crate_name(&metadata_package(Vec::new())), None);
    }

    /// A package declares whether its library is private.
    ///
    /// No property of the code distinguishes a library meant for consumers from one meant for a
    /// sibling crate, so the package states it. Absent means public, which keeps a new package
    /// assessed rather than silently skipped.
    #[test]
    fn a_package_declares_whether_its_library_is_private() {
        fn metadata_package(kinds: &[&str], metadata: Value) -> MetadataPackage {
            MetadataPackage {
                name: "demo".to_string(),
                version: "0.1.0".to_string(),
                id: "demo".to_string(),
                manifest_path: "packages/demo/Cargo.toml".to_string(),
                publish: None,
                dependencies: Vec::new(),
                targets: kinds
                    .iter()
                    .map(|kind| MetadataTarget {
                        name: "demo".to_string(),
                        kind: vec![(*kind).to_string()],
                    })
                    .collect(),
                metadata,
            }
        }
        fn declaring(value: bool) -> Value {
            serde_json::json!({ "release-plan": { "private-api": value } })
        }

        // A library is a contract unless the package declares itself private.
        assert!(is_consumer_contract(&metadata_package(&["lib"], Value::Null)).unwrap());
        assert!(is_consumer_contract(&metadata_package(&["proc-macro"], Value::Null)).unwrap());
        assert!(is_consumer_contract(&metadata_package(&["lib"], declaring(false))).unwrap());

        // A private library is not, and neither is a package with no library at all.
        assert!(!is_consumer_contract(&metadata_package(&["lib"], declaring(true))).unwrap());
        assert!(!is_consumer_contract(&metadata_package(&["bin"], Value::Null)).unwrap());
        assert!(!is_consumer_contract(&metadata_package(&[], Value::Null)).unwrap());

        // Unrelated package metadata leaves the default alone.
        assert!(
            is_consumer_contract(&metadata_package(
                &["lib"],
                serde_json::json!({ "binstall": { "pkg-fmt": "zip" } })
            ))
            .unwrap()
        );
    }

    /// A malformed declaration fails rather than falling back to the default.
    ///
    /// Defaulting would let a typo silently decide whether the package is assessed at all.
    #[test]
    fn a_malformed_private_api_declaration_is_an_error() {
        let package = MetadataPackage {
            name: "demo".to_string(),
            version: "0.1.0".to_string(),
            id: "demo".to_string(),
            manifest_path: "packages/demo/Cargo.toml".to_string(),
            publish: None,
            dependencies: Vec::new(),
            targets: vec![MetadataTarget {
                name: "demo".to_string(),
                kind: vec!["lib".to_string()],
            }],
            metadata: serde_json::json!({ "release-plan": { "private-api": "true" } }),
        };

        let error = is_consumer_contract(&package).unwrap_err();

        assert_eq!(
            error
                .find_source::<MalformedPrivateApiError>()
                .unwrap()
                .package(),
            "demo"
        );
    }

    #[test]
    fn dependents_of_lists_packages_that_depend_on_the_name() {
        fn package(name: &str, dependencies: Vec<ReportedDep>) -> WorkPackage {
            WorkPackage {
                manifest: PackageManifest {
                    name: name.to_string(),
                    version: "0.1.0".parse().unwrap(),
                    directory: format!("packages/{name}"),
                    packaging: PackagingRules::default(),
                    inherited: InheritedKeys::default(),
                    publish: true,
                    path_dependencies: Vec::new(),
                    inherited_path_dependencies: Vec::new(),
                    resource_paths: Vec::new(),
                    inherited_resource_paths: Vec::new(),
                    auto_readme: false,
                    targets: TargetDiscovery::default(),
                },
                manifest_path: PathBuf::from(format!("packages/{name}/Cargo.toml")),
                dependencies,
                has_lockfile_target: false,
                consumer_contract: true,
                resources: BTreeMap::new(),
            }
        }

        let bar = package(
            "bar",
            vec![ReportedDep {
                name: "foo".to_string(),
                req: "0.1.0".to_string(),
                exact_pin: false,
                kind: DepKind::Normal,
                public: false,
            }],
        );
        let foo = package("foo", Vec::new());
        assert_eq!(dependents_of(&[foo, bar], "foo"), vec!["bar".to_string()]);
    }
}
