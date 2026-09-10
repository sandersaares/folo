// Captured repository inputs and the exact resolved manifest/lockfile writes.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::ErrorKind;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Component, Path, PathBuf};

use ohno::AppError;
use serde::{Deserialize, Serialize};
use toml_edit::{Item, TableLike};

use crate::command::{hash_bytes, run_capture};
use crate::manifest::{for_each_dependency_table, parse_document};
use crate::metadata::load_tracked_work_tree;
use crate::plan::{PlanFile, PlanStage, SCHEMA_VERSION, resolve_plan};
use crate::verbose::Verbose;
use crate::{ParsePlanError, ReadFileError, UnsupportedPlanSchemaError, WriteFileError};

/// Repository facts frozen before any resolver-driven release decisions.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct Inputs {
    root: PathBuf,
    pub(crate) manifest: PathBuf,
    pub(crate) head: String,
    pub(crate) base: String,
    base_revision: String,
    index: String,
    pub(crate) paths: BTreeSet<PathBuf>,
    digest: String,
}

impl Inputs {
    pub(crate) fn capture(manifest: &Path, base: Option<&str>) -> Result<Self, AppError> {
        let (work_tree, git) = load_tracked_work_tree(manifest)?;
        let root = canonical(git.root())?;
        let manifest = relative(&root, &canonical(manifest)?)?;
        let base_revision = match base {
            Some(base) => base.to_owned(),
            None => git.default_base()?.revision().to_owned(),
        };
        let mut paths: BTreeSet<PathBuf> =
            git.ls_files("")?.into_iter().map(PathBuf::from).collect();
        paths.insert(relative(
            &root,
            &work_tree.workspace_root.join("Cargo.lock"),
        )?);
        for directory in work_tree
            .workspace_root
            .ancestors()
            .take_while(|directory| directory.starts_with(&root))
        {
            paths.insert(relative(&root, &directory.join(".cargo/config"))?);
            paths.insert(relative(&root, &directory.join(".cargo/config.toml"))?);
        }
        for manifest in &work_tree.member_manifests {
            paths.insert(relative(&root, manifest)?);
            let directory = manifest
                .parent()
                .expect("a manifest has a parent directory");
            collect_sources(&root, &directory.join("src"), &mut paths)?;
            paths.insert(relative(&root, &directory.join("build.rs"))?);
        }
        capture_path_dependencies(
            &root,
            work_tree
                .member_manifests
                .iter()
                .chain([&work_tree.workspace_root.join("Cargo.toml")]),
            &mut paths,
        )?;
        let digest = fingerprint(&root, &paths, &BTreeMap::new())?;
        Ok(Self {
            root,
            manifest,
            head: git.head()?,
            base: git.rev_parse(&base_revision)?,
            base_revision,
            index: run_capture("git", &["ls-files", "--stage", "-z"], git.root())?,
            paths,
            digest,
        })
    }

    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    pub(crate) fn index(&self) -> &str {
        &self.index
    }

    /// Verifies the same captured input set in a relocated final workspace.
    pub(crate) fn verify_candidate(
        &self,
        manifest: &Path,
        final_digest: &str,
    ) -> Result<(), AppError> {
        let current = Self::capture(manifest, Some(&self.base)).map_err(StaleInputs::caused_by)?;
        if current.manifest != self.manifest
            || current.head != self.head
            || current.base != self.base
            || current.index != self.index
            || current.paths != self.paths
            || current.digest != final_digest
        {
            return Err(StaleInputs::new().into());
        }
        Ok(())
    }

    /// Accepts only the complete initial state or the complete captured final state.
    pub(crate) fn verify(
        &self,
        manifest: &Path,
        final_digest: Option<&str>,
    ) -> Result<bool, AppError> {
        let current =
            Self::capture(manifest, Some(&self.base_revision)).map_err(StaleInputs::caused_by)?;
        if current.root != self.root
            || current.manifest != self.manifest
            || current.head != self.head
            || current.base != self.base
            || current.index != self.index
            || current.paths != self.paths
        {
            return Err(StaleInputs::new().into());
        }
        if current.digest == self.digest {
            return Ok(false);
        }
        if final_digest == Some(current.digest.as_str()) {
            return Ok(true);
        }
        Err(StaleInputs::new().into())
    }

    pub(crate) fn final_digest(&self, files: &[Artifact]) -> Result<String, AppError> {
        let mut seen = BTreeSet::new();
        for file in files {
            if !self.paths.contains(&file.path)
                || !matches!(
                    file.path.file_name().and_then(|name| name.to_str()),
                    Some("Cargo.toml" | "Cargo.lock")
                )
                || !seen.insert(&file.path)
            {
                return Err(ResolutionRequired::new().into());
            }
        }
        let replacements = files
            .iter()
            .map(|file| (file.path.clone(), file.contents.as_bytes().to_vec()))
            .collect();
        fingerprint(&self.root, &self.paths, &replacements)
    }
}

fn capture_path_dependencies<'a>(
    root: &Path,
    manifests: impl IntoIterator<Item = &'a PathBuf>,
    paths: &mut BTreeSet<PathBuf>,
) -> Result<(), AppError> {
    let mut pending: BTreeSet<PathBuf> = manifests.into_iter().cloned().collect();
    let mut visited = BTreeSet::new();
    while let Some(manifest) = pending.pop_first() {
        if !visited.insert(manifest.clone()) {
            continue;
        }
        paths.insert(relative(root, &manifest)?);
        let text = fs::read_to_string(&manifest)
            .map_err(|error| ReadFileError::caused_by(&manifest, error))?;
        let document = parse_document(&manifest, &text)?;
        let mut dependencies = Vec::new();
        for_each_dependency_table(document.as_table(), &mut |_, table| {
            dependency_paths(table, &mut dependencies);
        });
        if let Some(workspace) = document.get("workspace").and_then(Item::as_table_like) {
            for_each_dependency_table(workspace, &mut |_, table| {
                dependency_paths(table, &mut dependencies);
            });
        }
        if let Some(patches) = document.get("patch").and_then(Item::as_table_like) {
            for (_, patch) in patches.iter() {
                if let Some(table) = patch.as_table_like() {
                    dependency_paths(table, &mut dependencies);
                }
            }
        }
        if let Some(replacements) = document.get("replace").and_then(Item::as_table_like) {
            dependency_paths(replacements, &mut dependencies);
        }
        for dependency in dependencies {
            let path = Path::new(&dependency);
            // Absolute paths would keep pointing into the live workspace from a disposable clone.
            if path.is_absolute() {
                return Err(UnsupportedInput::new(path).into());
            }
            let directory = manifest
                .parent()
                .expect("a manifest has a parent")
                .join(path);
            let directory = canonical(&directory)?;
            relative(root, &directory)?;
            collect_sources(root, &directory.join("src"), paths)?;
            pending.insert(directory.join("Cargo.toml"));
        }
    }
    Ok(())
}

fn dependency_paths(table: &dyn TableLike, paths: &mut Vec<String>) {
    for (_, dependency) in table.iter() {
        if let Some(path) = dependency
            .as_table_like()
            .and_then(|dependency| dependency.get("path"))
            .and_then(Item::as_str)
        {
            paths.push(path.to_owned());
        }
    }
}

/// The complete resolved state embedded in the explicit plan for application.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct ResolvedState {
    pub(crate) inputs: Inputs,
    pub(crate) files: Vec<Artifact>,
    pub(crate) final_digest: String,
    pub(crate) versions: BTreeMap<String, String>,
    pub(crate) evidence_manifest_path: PathBuf,
}

/// Exact UTF-8 bytes of one resolved manifest or workspace lockfile.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct Artifact {
    pub(crate) path: PathBuf,
    pub(crate) contents: String,
}

/// The protocol header is checked before parsing a version-specific artifact body.
#[derive(Deserialize)]
struct ArtifactSchema {
    schema_version: u32,
}

pub(crate) fn run_verify_preview(
    plan_path: &Path,
    manifest: &Path,
    verbose: Verbose,
) -> Result<String, AppError> {
    let plan: PlanFile = read_json(plan_path)?;
    let state = plan.resolved.as_ref().ok_or_else(ResolutionRequired::new)?;
    let manifest = canonical(manifest)?;
    if manifest != canonical(&state.evidence_manifest_path)?
        || manifest == canonical(&state.inputs.root.join(&state.inputs.manifest))?
    {
        return Err(WrongEvidenceWorkspace::new().into());
    }
    _ = apply_resolved(
        &plan,
        &state.inputs.root.join(&state.inputs.manifest),
        true,
        verbose,
    )?;
    state
        .inputs
        .verify_candidate(&manifest, &state.final_digest)?;
    Ok("Compatibility workspace matches the captured source, versions, and lockfile.".to_owned())
}

pub(crate) fn apply_resolved(
    plan: &PlanFile,
    manifest: &Path,
    dry_run: bool,
    verbose: Verbose,
) -> Result<String, AppError> {
    let state = plan.resolved.as_ref().ok_or_else(ResolutionRequired::new)?;
    if plan.schema_version != SCHEMA_VERSION || plan.stage() != PlanStage::Expanded {
        return Err(ResolutionRequired::new().into());
    }
    let already_applied = state.inputs.verify(manifest, Some(&state.final_digest))?;
    let (work_tree, _) = load_tracked_work_tree(manifest)?;
    let resolved = resolve_plan(
        plan,
        &work_tree.groups,
        &work_tree.target_versions(),
        verbose,
    )?;
    let versions: BTreeMap<String, String> = resolved
        .packages
        .iter()
        .map(|(name, version)| (name.clone(), version.to_string()))
        .collect();
    if versions != state.versions {
        return Err(ResolutionRequired::new().into());
    }
    let allowed: BTreeSet<PathBuf> = work_tree
        .member_manifests
        .iter()
        .chain([&work_tree.workspace_root.join("Cargo.toml")])
        .chain([&work_tree.workspace_root.join("Cargo.lock")])
        .map(|path| relative(state.inputs.root(), path))
        .collect::<Result<_, _>>()?;
    let mut seen = BTreeSet::new();
    for file in &state.files {
        if !allowed.contains(&file.path)
            || !state.inputs.paths.contains(&file.path)
            || !seen.insert(&file.path)
        {
            return Err(ResolutionRequired::new().into());
        }
    }
    if state.inputs.final_digest(&state.files)? != state.final_digest {
        return Err(ResolutionRequired::new().into());
    }
    if already_applied {
        return Ok("Resolved state is already applied; no files changed.".to_owned());
    }
    if dry_run {
        return Ok(format!(
            "Dry run: would install {} captured files; no Cargo resolution is performed.",
            state.files.len()
        ));
    }
    for file in &state.files {
        let path = state.inputs.root().join(&file.path);
        fs::write(&path, &file.contents)
            .map_err(|error| WriteFileError::caused_by(&path, error))?;
    }
    state.inputs.verify(manifest, Some(&state.final_digest))?;
    Ok(format!(
        "Installed {} captured files without Cargo resolution.",
        state.files.len()
    ))
}

pub(crate) fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T, AppError> {
    let text = fs::read_to_string(path).map_err(|error| ReadFileError::caused_by(path, error))?;
    parse_artifact(path, &text)
}

fn parse_artifact<T: for<'de> Deserialize<'de>>(path: &Path, text: &str) -> Result<T, AppError> {
    let schema: ArtifactSchema =
        serde_json::from_str(text).map_err(|error| ParsePlanError::caused_by(path, error))?;
    if schema.schema_version != SCHEMA_VERSION {
        return Err(UnsupportedPlanSchemaError::new(schema.schema_version).into());
    }
    serde_json::from_str(text).map_err(|error| ParsePlanError::caused_by(path, error).into())
}

pub(crate) fn write_json(path: &Path, value: &impl Serialize) -> Result<(), AppError> {
    let json = serde_json::to_string_pretty(value)
        .expect("release artifacts contain only JSON-compatible data");
    fs::write(path, format!("{json}\n"))
        .map_err(|error| WriteFileError::caused_by(path, error).into())
}

pub(crate) fn relative(root: &Path, path: &Path) -> Result<PathBuf, AppError> {
    let relative = path
        .strip_prefix(root)
        .map_err(|error| UnsupportedInput::caused_by(path, error))?;
    if relative
        .components()
        .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(UnsupportedInput::new(path).into());
    }
    Ok(relative.to_path_buf())
}

fn canonical(path: &Path) -> Result<PathBuf, AppError> {
    let canonical =
        fs::canonicalize(path).map_err(|error| ReadFileError::caused_by(path, error))?;
    // Git and Cargo report ordinary Windows paths, not canonicalize's verbatim spelling.
    #[cfg(windows)]
    let canonical = ordinary_windows_path(&canonical);
    Ok(canonical)
}

#[cfg(windows)]
fn ordinary_windows_path(path: &Path) -> PathBuf {
    let text = path.to_string_lossy();
    match text.strip_prefix(r"\\?\UNC\") {
        Some(path) => PathBuf::from(format!(r"\\{path}")),
        None => PathBuf::from(text.trim_start_matches(r"\\?\")),
    }
}

fn collect_sources(
    root: &Path,
    directory: &Path,
    paths: &mut BTreeSet<PathBuf>,
) -> Result<(), AppError> {
    if !directory.exists() {
        return Ok(());
    }
    for entry in
        fs::read_dir(directory).map_err(|error| ReadFileError::caused_by(directory, error))?
    {
        let entry = entry.map_err(|error| ReadFileError::caused_by(directory, error))?;
        let kind = entry
            .file_type()
            .map_err(|error| ReadFileError::caused_by(entry.path(), error))?;
        if kind.is_dir() {
            collect_sources(root, &entry.path(), paths)?;
        } else {
            paths.insert(relative(root, &entry.path())?);
        }
    }
    Ok(())
}

fn fingerprint(
    root: &Path,
    paths: &BTreeSet<PathBuf>,
    replacements: &BTreeMap<PathBuf, Vec<u8>>,
) -> Result<String, AppError> {
    let mut bytes = Vec::new();
    for relative in paths {
        let path = root.join(relative);
        let name = relative.to_string_lossy();
        append_field(&mut bytes, name.as_bytes());
        let metadata = match fs::symlink_metadata(&path) {
            Ok(metadata) => Some(metadata),
            Err(error) if error.kind() == ErrorKind::NotFound => None,
            Err(error) => return Err(ReadFileError::caused_by(&path, error).into()),
        };
        if metadata
            .as_ref()
            .is_some_and(|metadata| !metadata.is_file() || metadata.file_type().is_symlink())
        {
            return Err(UnsupportedInput::new(&path).into());
        }
        #[cfg(unix)]
        bytes.push(u8::from(metadata.as_ref().is_some_and(|metadata| {
            // Git's executable-file mode records whether any execute bit is present.
            metadata.permissions().mode() & 0o111 != 0
        })));
        let contents = if let Some(replacement) = replacements.get(relative) {
            Some(replacement.clone())
        } else if metadata.is_some() {
            Some(fs::read(&path).map_err(|error| ReadFileError::caused_by(&path, error))?)
        } else {
            None
        };
        bytes.push(u8::from(contents.is_some()));
        if let Some(contents) = contents {
            append_field(&mut bytes, &contents);
        }
    }
    hash_bytes(&bytes, root)
}

fn append_field(bytes: &mut Vec<u8>, field: &[u8]) {
    // Artifact fingerprints use a fixed-width length, independent of the executing binary's
    // pointer width. Length-prefixing also distinguishes adjacent fields with the same bytes.
    let length = u64::try_from(field.len())
        .expect("a slice on a supported Rust target cannot exceed u64::MAX bytes");
    bytes.extend_from_slice(&length.to_le_bytes());
    bytes.extend_from_slice(field);
}

/// A stale source tree must return to preparation rather than widen a captured plan.
#[ohno::error]
#[display("prepared release inputs are stale; regenerate with prepare and preview")]
pub(crate) struct StaleInputs;

/// A plain expansion has not captured the resolution effects required for application.
#[ohno::error]
#[display("apply requires the unchanged resolved plan produced by preview")]
pub(crate) struct ResolutionRequired;

/// Prospective work must remain inside the captured repository.
#[ohno::error]
#[display("cannot capture release input {}; use ordinary files inside the repository", path.display())]
pub(crate) struct UnsupportedInput {
    path: PathBuf,
}

/// Compatibility evidence must come from the recorded final candidate, not the live tree.
#[ohno::error]
#[display("verification requires the evidence_manifest_path recorded in the resolved plan")]
struct WrongEvidenceWorkspace;

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn fields_use_fixed_width_little_endian_lengths() {
        let mut bytes = Vec::new();
        append_field(&mut bytes, b"abc");
        append_field(&mut bytes, b"");
        assert_eq!(
            bytes,
            [
                3, 0, 0, 0, 0, 0, 0, 0, b'a', b'b', b'c', 0, 0, 0, 0, 0, 0, 0, 0
            ]
        );
    }

    #[test]
    fn field_boundaries_participate_in_fingerprints() {
        let mut left = Vec::new();
        append_field(&mut left, b"ab");
        append_field(&mut left, b"c");
        let mut right = Vec::new();
        append_field(&mut right, b"a");
        append_field(&mut right, b"bc");
        assert_ne!(left, right);
    }

    #[test]
    #[cfg(windows)]
    fn canonical_windows_paths_match_git_and_cargo_spellings() {
        for (canonical, ordinary) in [
            (
                r"\\?\UNC\server\share\workspace",
                r"\\server\share\workspace",
            ),
            (r"\\?\C:\workspace", r"C:\workspace"),
            (r"C:\workspace", r"C:\workspace"),
        ] {
            assert_eq!(
                ordinary_windows_path(Path::new(canonical)),
                Path::new(ordinary)
            );
        }
    }

    #[test]
    fn artifact_selection_rejects_uncaptured_non_cargo_and_duplicate_paths() {
        let inputs = Inputs {
            root: PathBuf::from("not-accessed"),
            manifest: PathBuf::from("Cargo.toml"),
            head: String::new(),
            base: String::new(),
            base_revision: String::new(),
            index: String::new(),
            paths: ["Cargo.toml", "src/lib.rs"]
                .into_iter()
                .map(PathBuf::from)
                .collect(),
            digest: String::new(),
        };
        for paths in [
            vec!["uncaptured/Cargo.toml"],
            vec!["src/lib.rs"],
            vec!["Cargo.toml", "Cargo.toml"],
        ] {
            let artifacts: Vec<_> = paths
                .into_iter()
                .map(|path| Artifact {
                    path: path.into(),
                    contents: String::new(),
                })
                .collect();
            let error = inputs.final_digest(&artifacts).unwrap_err();
            assert!(error.find_source::<ResolutionRequired>().is_some());
        }
    }

    #[test]
    fn relative_paths_cannot_escape_the_captured_root() {
        let root = Path::new("repository");
        assert_eq!(
            relative(root, &root.join("member/Cargo.toml")).unwrap(),
            Path::new("member/Cargo.toml")
        );
        for path in [
            PathBuf::from("elsewhere/Cargo.toml"),
            root.join("../Cargo.toml"),
        ] {
            let error = relative(root, &path).unwrap_err();
            assert!(error.find_source::<UnsupportedInput>().is_some());
        }
    }

    #[test]
    #[cfg_attr(miri, ignore = "reads an owned filesystem fixture")]
    fn source_collection_handles_absence_recursion_and_non_directory_errors() {
        let directory = tempdir().unwrap();
        let source = directory.path().join("src");
        let mut paths = BTreeSet::new();
        collect_sources(directory.path(), &source, &mut paths).unwrap();
        assert!(paths.is_empty());
        fs::create_dir_all(source.join("nested")).unwrap();
        fs::write(source.join("lib.rs"), "").unwrap();
        fs::write(source.join("nested/mod.rs"), "").unwrap();
        collect_sources(directory.path(), &source, &mut paths).unwrap();
        assert_eq!(
            paths,
            ["src/lib.rs", "src/nested/mod.rs"]
                .into_iter()
                .map(PathBuf::from)
                .collect()
        );
        let error =
            collect_sources(directory.path(), &source.join("lib.rs"), &mut paths).unwrap_err();
        assert!(error.find_source::<ReadFileError>().is_some());
    }

    #[test]
    #[cfg_attr(miri, ignore = "reads files and invokes Git hashing")]
    fn fingerprints_distinguish_missing_empty_content_and_paths() {
        let directory = tempdir().unwrap();
        let path = PathBuf::from("Cargo.lock");
        let paths = BTreeSet::from([path.clone()]);
        let missing = fingerprint(directory.path(), &paths, &BTreeMap::new()).unwrap();
        fs::write(directory.path().join(&path), "").unwrap();
        let empty = fingerprint(directory.path(), &paths, &BTreeMap::new()).unwrap();
        assert_ne!(missing, empty);
        let contents = b"resolved bytes".to_vec();
        let replacements = BTreeMap::from([(path.clone(), contents.clone())]);
        let replaced = fingerprint(directory.path(), &paths, &replacements).unwrap();
        assert_ne!(empty, replaced);
        fs::write(directory.path().join(&path), &contents).unwrap();
        assert_eq!(
            fingerprint(directory.path(), &paths, &BTreeMap::new()).unwrap(),
            replaced
        );
        fs::rename(
            directory.path().join(path),
            directory.path().join("Cargo.toml"),
        )
        .unwrap();
        let renamed = fingerprint(
            directory.path(),
            &BTreeSet::from([PathBuf::from("Cargo.toml")]),
            &BTreeMap::new(),
        )
        .unwrap();
        assert_ne!(renamed, replaced);
    }

    #[test]
    #[cfg_attr(miri, ignore = "uses filesystem metadata")]
    fn fingerprints_reject_a_directory_as_a_file() {
        let directory = tempdir().unwrap();
        fs::create_dir_all(directory.path().join("Cargo.lock")).unwrap();
        let error = fingerprint(
            directory.path(),
            &BTreeSet::from([PathBuf::from("Cargo.lock")]),
            &BTreeMap::new(),
        )
        .unwrap_err();
        assert!(error.find_source::<UnsupportedInput>().is_some());
    }

    #[test]
    #[cfg_attr(miri, ignore = "reads local dependency manifests")]
    fn capture_includes_workspace_and_replacement_sources() {
        let directory = tempdir().unwrap();
        let root = directory.path();
        fs::create_dir_all(root.join("replacement/src/nested")).unwrap();
        fs::write(
            root.join("Cargo.toml"),
            "[workspace.dependencies]\nhelper = { path = \"replacement\" }\n\
             [replace]\n\"helper:0.1.0\" = { path = \"replacement\" }\n",
        )
        .unwrap();
        fs::write(
            root.join("replacement/Cargo.toml"),
            "[package]\nname = \"helper\"\nversion = \"0.1.0\"\n",
        )
        .unwrap();
        fs::write(root.join("replacement/src/nested/lib.rs"), "").unwrap();
        let mut paths = BTreeSet::new();
        capture_path_dependencies(root, [&root.join("Cargo.toml")], &mut paths).unwrap();
        assert_eq!(
            paths,
            [
                "Cargo.toml",
                "replacement/Cargo.toml",
                "replacement/src/nested/lib.rs"
            ]
            .into_iter()
            .map(PathBuf::from)
            .collect()
        );
    }

    #[test]
    #[cfg_attr(miri, ignore = "reads an owned dependency manifest")]
    fn absolute_dependency_paths_cannot_leak_back_to_the_live_workspace() {
        let directory = tempdir().unwrap();
        let manifest = directory.path().join("Cargo.toml");
        fs::write(
            &manifest,
            format!(
                "[dependencies]\nhelper = {{ path = {:?} }}\n",
                directory.path()
            ),
        )
        .unwrap();
        let error = capture_path_dependencies(directory.path(), [&manifest], &mut BTreeSet::new())
            .unwrap_err();
        assert!(error.find_source::<UnsupportedInput>().is_some());
    }

    #[test]
    fn unsupported_artifact_schema_precedes_body_validation() {
        let error =
            parse_artifact::<PlanFile>(Path::new("prepared.json"), r#"{"schema_version":3}"#)
                .unwrap_err();
        assert!(error.find_source::<UnsupportedPlanSchemaError>().is_some());
    }
}
