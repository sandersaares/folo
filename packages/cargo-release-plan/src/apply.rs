// Manifest rewrites for prospective resolution and proposed manifest-only edits.

use std::collections::{BTreeMap, HashSet};
use std::fmt::Write as _;
use std::fs;
use std::path::{Component, Path, PathBuf};

use ohno::AppError;
use semver::Version;
use toml_edit::{DocumentMut, Formatted, Item, TableLike, Value};

use crate::inherited::is_workspace_inherit;
use crate::manifest::{
    DEPENDENCY_TABLES, dependency_table_name, parse_document, requirement_names_version,
};
use crate::metadata::{WorkTree, load_tracked_work_tree};
use crate::plan::{PlanFile, PlanStage, ResolvedVersions, resolve_plan};
use crate::resolved::apply_resolved;
use crate::text::plural;
use crate::verbose::Verbose;
use crate::{ParsePlanError, ReadFileError, WriteFileError, quote_path};

/// One on-disk manifest after an in-memory rewrite, waiting to be written.
pub(crate) struct ManifestEdit {
    pub(crate) path: PathBuf,
    pub(crate) original: String,
    pub(crate) updated: String,
}

/// What a `path` dependency has to resolve to before `apply` rewrites it.
///
/// A `path` key plus a matching package name is not enough: a member may depend on
/// a package outside the workspace, or on an excluded one, that happens to carry
/// the same package name. Rewriting such a requirement would corrupt an
/// unrelated dependency, so the declared path is resolved against the manifest
/// that declares it and checked against the workspace's member directories.
struct DepTargets<'a> {
    manifest_dir: PathBuf,
    members_by_dir: &'a BTreeMap<PathBuf, String>,
}

impl DepTargets<'_> {
    fn declares(&self, dep_path: &str, package_name: &str) -> bool {
        let joined = self.manifest_dir.join(dep_path);
        if let Some(declared) = self.members_by_dir.get(&normalize_lexically(&joined)) {
            return declared == package_name;
        }

        // Cargo resolves a dependency path through the filesystem, so a symbolic
        // link or a case-variant spelling can name a workspace member that the
        // lexical form above cannot match. Asking the filesystem is deferred to
        // this point because it costs a system call per candidate, and only a
        // path dependency whose package the plan already names reaches here.
        let Ok(resolved) = fs::canonicalize(&joined) else {
            return false;
        };
        self.members_by_dir.iter().any(|(dir, declared)| {
            declared == package_name && fs::canonicalize(dir).is_ok_and(|member| member == resolved)
        })
    }
}

/// Resolves `.` and `..` without touching the filesystem.
///
/// Most manifests spell a member directory the same way `cargo metadata`
/// reports it once dot components are folded, so this form answers the common
/// case without a system call.
fn normalize_lexically(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            other => normalized.push(other),
        }
    }
    normalized
}

pub(crate) fn run_apply(
    plan_path: &Path,
    dry_run: bool,
    manifest_path: &Path,
    verbose: Verbose,
) -> Result<String, AppError> {
    let plan = fs::read_to_string(plan_path)
        .map_err(|error| ReadFileError::caused_by(plan_path, error))?;
    let plan: PlanFile =
        serde_json::from_str(&plan).map_err(|error| ParsePlanError::caused_by(plan_path, error))?;
    plan.validate_schema()?;

    if plan.stage() == PlanStage::Expanded || plan.resolved.is_some() {
        return apply_resolved(&plan, manifest_path, dry_run, verbose);
    }
    let (work_tree, _) = load_tracked_work_tree(manifest_path)?;
    // Git-tracked members decide which plan targets are valid and supply their
    // increment bases. All Cargo-visible member manifests remain
    // available below for dependent-pin rewrites.
    // Ref: docs/implementation.md, "Plan resolution and application".
    let target_versions = work_tree.target_versions();
    let resolved = resolve_plan(&plan, &work_tree.groups, &target_versions, verbose)?;
    verbose.note(|| {
        format!(
            "plan expands to {}; every tracked group member is included even when the plan named \
             only one of them",
            plural(resolved.packages.len(), "package version")
        )
    });

    let edits = compute_edits(&work_tree, &resolved, verbose)?;
    let changed = changed_edit_count(&edits);

    if dry_run {
        return Ok(dry_run_summary(&edits));
    }

    for edit in &edits {
        if edit.original == edit.updated {
            continue;
        }
        fs::write(&edit.path, edit.updated.as_bytes())
            .map_err(|error| WriteFileError::caused_by(&edit.path, error))?;
        verbose.note(|| format!(
            "wrote {} after computing the full edit set in memory; remaining writes can still fail",
            quote_path(&edit.path.to_string_lossy())
        ));
    }

    Ok(format!(
        "Updated {} and left the workspace lockfile untouched; use prepare and preview for a resolved release plan",
        plural(changed, "manifest")
    ))
}

pub(crate) fn compute_edits(
    work_tree: &WorkTree,
    resolved: &ResolvedVersions,
    verbose: Verbose,
) -> Result<Vec<ManifestEdit>, AppError> {
    let mut edits = Vec::new();
    let root = work_tree.workspace_root.join("Cargo.toml");
    let root_targets = DepTargets {
        manifest_dir: work_tree.workspace_root.clone(),
        members_by_dir: &work_tree.members_by_dir,
    };
    edits.push(edit_path(&root, |doc| {
        rewrite_workspace_dependencies(doc, &root_targets, resolved, verbose);
        rewrite_package_version(doc, resolved, verbose);
        rewrite_dependency_tables(doc, &root_targets, resolved, verbose);
    })?);

    let mut seen = HashSet::new();
    seen.insert(root);
    // Every member is rewritten, not just the publishable ones: a `publish = false`
    // member can still carry an `=` pin on a package the plan increments, and
    // leaving it stale would break prospective workspace resolution.
    for manifest_path in &work_tree.member_manifests {
        if !seen.insert(manifest_path.clone()) {
            continue;
        }
        let targets = DepTargets {
            manifest_dir: manifest_path
                .parent()
                .unwrap_or(&work_tree.workspace_root)
                .to_path_buf(),
            members_by_dir: &work_tree.members_by_dir,
        };
        edits.push(edit_path(manifest_path, |doc| {
            rewrite_package_version(doc, resolved, verbose);
            rewrite_dependency_tables(doc, &targets, resolved, verbose);
        })?);
    }
    Ok(edits)
}

fn dry_run_summary(edits: &[ManifestEdit]) -> String {
    let changed = changed_edit_count(edits);
    let mut message = format!("Dry run: {} would change", plural(changed, "manifest"));
    for edit in edits {
        if edit.original != edit.updated {
            write!(message, "\n  {}", quote_path(&edit.path.to_string_lossy()))
                .expect("writing to String");
        }
    }
    message.push_str("; the workspace lockfile would be left untouched");
    message
}

fn changed_edit_count(edits: &[ManifestEdit]) -> usize {
    edits
        .iter()
        .filter(|edit| edit.original != edit.updated)
        .count()
}

fn edit_path(
    path: &Path,
    rewrite: impl FnOnce(&mut DocumentMut),
) -> Result<ManifestEdit, AppError> {
    let original =
        fs::read_to_string(path).map_err(|error| ReadFileError::caused_by(path, error))?;
    let mut doc: DocumentMut = parse_document(path, &original)?;
    rewrite(&mut doc);
    Ok(ManifestEdit {
        path: path.to_path_buf(),
        original,
        updated: doc.to_string(),
    })
}

fn rewrite_package_version(doc: &mut DocumentMut, resolved: &ResolvedVersions, verbose: Verbose) {
    let Some(package) = doc.get("package").and_then(Item::as_table_like) else {
        return;
    };
    let Some(name) = package.get("name").and_then(Item::as_str) else {
        return;
    };
    let name = name.to_string();
    let Some(new_version) = resolved.packages.get(&name) else {
        return;
    };
    let Some(package) = doc.get_mut("package").and_then(Item::as_table_like_mut) else {
        return;
    };
    if set_package_version_item(package, new_version) {
        verbose.note(|| {
            format!(
                "{}: set package.version to {new_version} because the plan assigns that \
             version to this package",
                quote_path(&name)
            )
        });
    }
}

fn rewrite_workspace_dependencies(
    doc: &mut DocumentMut,
    targets: &DepTargets<'_>,
    resolved: &ResolvedVersions,
    verbose: Verbose,
) {
    let Some(workspace) = doc.get_mut("workspace").and_then(Item::as_table_like_mut) else {
        return;
    };
    let Some(deps) = workspace
        .get_mut("dependencies")
        .and_then(Item::as_table_like_mut)
    else {
        return;
    };
    rewrite_dep_table(deps, targets, resolved, verbose, "workspace.dependencies");
}

// Walks every dependency table; entry-level rewrite is tested separately.
#[cfg_attr(test, mutants::skip)]
fn rewrite_dependency_tables(
    doc: &mut DocumentMut,
    targets: &DepTargets<'_>,
    resolved: &ResolvedVersions,
    verbose: Verbose,
) {
    for canonical in DEPENDENCY_TABLES {
        let Some(table_name) = dependency_table_name(doc.as_table(), canonical) else {
            continue;
        };
        if let Some(table) = doc.get_mut(table_name).and_then(Item::as_table_like_mut) {
            rewrite_dep_table(table, targets, resolved, verbose, table_name);
        }
    }

    let specs: Vec<String> = match doc.get("target").and_then(Item::as_table_like) {
        Some(target) => target.iter().map(|(key, _)| key.to_string()).collect(),
        None => Vec::new(),
    };
    for spec in specs {
        let Some(spec_table) = doc
            .get_mut("target")
            .and_then(Item::as_table_like_mut)
            .and_then(|target| target.get_mut(spec.as_str()))
            .and_then(Item::as_table_like_mut)
        else {
            continue;
        };
        for canonical in DEPENDENCY_TABLES {
            let Some(table_name) = dependency_table_name(spec_table, canonical) else {
                continue;
            };
            let Some(table) = spec_table
                .get_mut(table_name)
                .and_then(Item::as_table_like_mut)
            else {
                continue;
            };
            rewrite_dep_table(
                table,
                targets,
                resolved,
                verbose,
                &format!("target.{spec}.{table_name}"),
            );
        }
    }
}

fn rewrite_dep_table(
    table: &mut dyn TableLike,
    targets: &DepTargets<'_>,
    resolved: &ResolvedVersions,
    verbose: Verbose,
    where_: &str,
) {
    for (key, entry) in table.iter_mut() {
        let Some(new_version) = planned_version(entry, key.get(), targets, resolved) else {
            continue;
        };
        if rewrite_dep_entry(entry, new_version) {
            verbose.note(|| {
                format!(
                    "{}.{}: rewrote the version requirement to follow {} {new_version} \
                 (exact `=` pins keep the equals sign; requirements that already match the new \
                 version are left unchanged; only path dependencies resolving to that workspace \
                 member are rewritten)",
                    quote_path(where_),
                    quote_path(key.get()),
                    quote_path(dep_package_name(entry, key.get()))
                )
            });
        }
    }
}

/// The version a dependency entry must be rewritten to, if the plan names it.
///
/// Borrowed throughout: `apply` inspects every dependency of every workspace
/// member, while a plan usually names a handful of packages, so an unrelated
/// dependency must not cost an allocation to reject.
fn planned_version<'p>(
    entry: &Item,
    table_key: &str,
    targets: &DepTargets<'_>,
    resolved: &'p ResolvedVersions,
) -> Option<&'p Version> {
    let dep_path = dep_path(entry)?;
    let package_name = dep_package_name(entry, table_key);
    let new_version = resolved.packages.get(package_name)?;
    targets
        .declares(dep_path, package_name)
        .then_some(new_version)
}

fn dep_path(entry: &Item) -> Option<&str> {
    match entry {
        Item::Value(Value::InlineTable(table)) => table.get("path").and_then(Value::as_str),
        Item::Table(table) => table.get("path").and_then(Item::as_str),
        _ => None,
    }
}

fn dep_package_name<'e>(entry: &'e Item, table_key: &'e str) -> &'e str {
    let package = match entry {
        Item::Value(Value::InlineTable(table)) => table.get("package").and_then(Value::as_str),
        Item::Table(table) => table.get("package").and_then(Item::as_str),
        _ => None,
    };
    package.unwrap_or(table_key)
}

fn rewrite_dep_entry(entry: &mut Item, new_version: &Version) -> bool {
    match entry {
        Item::Value(Value::String(formatted)) => {
            let rewritten = rewrite_req(formatted.value(), new_version);
            set_formatted(formatted, rewritten)
        }
        Item::Value(Value::InlineTable(table)) => {
            if let Some(Value::String(formatted)) = table.get_mut("version") {
                let rewritten = rewrite_req(formatted.value(), new_version);
                set_formatted(formatted, rewritten)
            } else {
                false
            }
        }
        Item::Table(table) => set_version_item(table, "version", new_version),
        _ => false,
    }
}

fn set_package_version_item(table: &mut dyn TableLike, new_version: &Version) -> bool {
    match table.get_mut("version") {
        Some(Item::Value(Value::String(formatted))) => {
            set_formatted(formatted, new_version.to_string())
        }
        // A member that inherits its version is given a local literal rather
        // than having the shared workspace value changed: the plan increments
        // one package, while the shared value governs every member that
        // inherits it, so editing it would silently increment them all.
        // Ref: docs/implementation.md, "Plan resolution and application".
        Some(item) if is_workspace_inherit(item) => {
            *item = Item::Value(Value::from(new_version.to_string()));
            true
        }
        _ => false,
    }
}

fn set_version_item(table: &mut dyn TableLike, key: &str, new_version: &Version) -> bool {
    match table.get_mut(key) {
        Some(Item::Value(Value::String(formatted))) => {
            let rewritten = rewrite_req(formatted.value(), new_version);
            set_formatted(formatted, rewritten)
        }
        _ => false,
    }
}

fn set_formatted(formatted: &mut Formatted<String>, rewritten: String) -> bool {
    if rewritten == formatted.value().as_str() {
        return false;
    }
    let decor = formatted.decor().clone();
    let mut replacement = Formatted::new(rewritten);
    *replacement.decor_mut() = decor;
    *formatted = replacement;
    true
}

/// The requirement a dependent must declare once its target is incremented.
///
/// A requirement that already names the new version is returned untouched,
/// down to its spelling. That matters beyond tidiness: an exact group
/// alignment resolves the leading member to the version it already declares,
/// so rewriting its dependents' spelling would edit published manifests that
/// no increment covers. What counts as already naming the version is
/// [`requirement_names_version`], the same predicate `check` validates with.
///
/// Anything else is replaced, because a requirement that merely still *admits*
/// the new version would let a consumer resolve a combination this workspace
/// never built. An exact comparator is replaced by an exact pin on the new
/// version, keeping the lockstep the author asked for; anything else becomes
/// the bare new version, which is also what an unparsable requirement gets:
/// Cargo would reject that anyway, so the apply leaves behind a manifest Cargo
/// can read rather than one it cannot.
///
/// A requirement whose *form* is wrong for its edge, such as a compatible
/// requirement between two version-group members, is left for `check` to
/// report rather than silently corrected here: that is a manifest defect, not
/// a consequence of the version moving.
/// Ref: docs/implementation.md, "Plan resolution and application".
fn rewrite_req(old: &str, new_version: &Version) -> String {
    if requirement_names_version(old, new_version) {
        return old.to_string();
    }
    if old.trim().starts_with('=') {
        return format!("={new_version}");
    }
    new_version.to_string()
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    #[cfg(unix)]
    use std::os::unix::fs::symlink;

    use tempfile::tempdir;

    use super::*;
    use crate::UnsupportedPlanSchemaError;

    #[test]
    #[cfg_attr(miri, ignore = "writes a plan file through the host filesystem")]
    fn unsupported_proposed_and_expanded_schemas_fail_before_workspace_access() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("plan.json");
        for stage in [PlanStage::Proposed, PlanStage::Expanded] {
            let mut plan = PlanFile::new(stage, Vec::new());
            plan.schema_version = 3;
            fs::write(&path, serde_json::to_vec(&plan).unwrap()).unwrap();
            let error = run_apply(
                &path,
                false,
                &directory.path().join("absent").join("Cargo.toml"),
                Verbose::new(false),
            )
            .unwrap_err();
            assert!(error.find_source::<UnsupportedPlanSchemaError>().is_some());
        }
    }

    fn v(text: &str) -> Version {
        text.parse().unwrap()
    }

    fn dep_item(toml: &str) -> DocumentMut {
        toml.parse::<DocumentMut>().unwrap()
    }

    fn first_dep(doc: &mut DocumentMut) -> &mut Item {
        doc.get_mut("dependencies")
            .and_then(Item::as_table_like_mut)
            .and_then(|table| table.get_mut("foo"))
            .unwrap()
    }

    #[test]
    fn changed_edit_count_counts_only_rewritten_manifests() {
        let edits = [
            ManifestEdit {
                path: PathBuf::from("unchanged.toml"),
                original: "a".to_string(),
                updated: "a".to_string(),
            },
            ManifestEdit {
                path: PathBuf::from("changed.toml"),
                original: "a".to_string(),
                updated: "b".to_string(),
            },
        ];
        assert_eq!(changed_edit_count(&edits), 1);
        assert_eq!(changed_edit_count(&edits[..1]), 0);
        let summary = dry_run_summary(&edits);
        assert!(summary.contains("changed.toml"));
        assert!(!summary.contains("unchanged.toml"));
        assert!(summary.contains("lockfile would be left untouched"));
    }

    #[test]
    fn empty_manifest_only_dry_run_reports_no_writes() {
        let summary = dry_run_summary(&[]);
        assert_eq!(
            summary,
            "Dry run: 0 manifests would change; the workspace lockfile would be left untouched"
        );
    }

    #[test]
    fn legacy_dependency_tables_are_rewritten_at_root_and_under_targets() {
        let mut document: DocumentMut = concat!(
            "[build_dependencies]\ndemo = { path = \"../demo\", version = \"=0.1.0\" }\n",
            "[dev_dependencies]\ndemo = { path = \"../demo\", version = \"^0.1.0\" }\n",
            "[target.'cfg(unix)'.build_dependencies]\ndemo = { path = \"../demo\", version = \"=0.1.0\" }\n",
            "[target.'cfg(unix)'.dev_dependencies]\ndemo = { path = \"../demo\", version = \"^0.1.0\" }\n",
        ).parse().unwrap();
        rewrite_test_dependencies(&mut document);
        assert_eq!(
            demo_requirement(&document, &["build_dependencies"]),
            "=0.2.0"
        );
        assert_eq!(demo_requirement(&document, &["dev_dependencies"]), "0.2.0");
        assert_eq!(
            demo_requirement(&document, &["target", "cfg(unix)", "build_dependencies"]),
            "=0.2.0"
        );
        assert_eq!(
            demo_requirement(&document, &["target", "cfg(unix)", "dev_dependencies"]),
            "0.2.0"
        );
    }

    #[test]
    fn canonical_dependency_tables_override_legacy_even_when_empty() {
        let mut document: DocumentMut = concat!(
            "[build_dependencies]\ndemo = { path = \"../demo\", version = \"=0.1.0\" }\n",
            "[build-dependencies]\n",
            "[dev_dependencies]\ndemo = { path = \"../demo\", version = \"=0.1.0\" }\n",
            "[dev-dependencies]\ndemo = { path = \"../demo\", version = \"^0.1.0\" }\n",
            "[target.'cfg(unix)'.build_dependencies]\ndemo = { path = \"../demo\", version = \"=0.1.0\" }\n",
            "[target.'cfg(unix)'.build-dependencies]\n",
        ).parse().unwrap();
        rewrite_test_dependencies(&mut document);
        assert_eq!(
            demo_requirement(&document, &["build_dependencies"]),
            "=0.1.0"
        );
        assert_eq!(demo_requirement(&document, &["dev_dependencies"]), "=0.1.0");
        assert_eq!(demo_requirement(&document, &["dev-dependencies"]), "0.2.0");
        assert_eq!(
            demo_requirement(&document, &["target", "cfg(unix)", "build_dependencies"]),
            "=0.1.0"
        );
        assert!(
            document
                .get("build-dependencies")
                .unwrap()
                .as_table_like()
                .unwrap()
                .is_empty()
        );
    }

    fn rewrite_test_dependencies(document: &mut DocumentMut) {
        let members = demo_members();
        let targets = DepTargets {
            manifest_dir: PathBuf::from("/ws/packages/user"),
            members_by_dir: &members,
        };
        let resolved = ResolvedVersions {
            packages: BTreeMap::from([("demo".to_owned(), v("0.2.0"))]),
        };
        rewrite_dependency_tables(document, &targets, &resolved, Verbose::new(false));
    }

    fn demo_requirement<'a>(document: &'a DocumentMut, path: &[&str]) -> &'a str {
        let mut table: &dyn TableLike = document.as_table();
        for key in path {
            table = table.get(key).unwrap().as_table_like().unwrap();
        }
        table
            .get("demo")
            .unwrap()
            .as_table_like()
            .unwrap()
            .get("version")
            .unwrap()
            .as_str()
            .unwrap()
    }

    /// Every requirement is rewritten to name the new version unless it already does.
    ///
    /// A requirement that merely admits the new version is not good enough, because it would
    /// let a consumer resolve a combination the workspace never built. One that already names
    /// it keeps its exact spelling, so an alignment that leaves a package on its current
    /// version does not edit its dependents' manifests.
    #[test]
    fn rewrite_req_names_the_new_version_and_keeps_the_exact_comparator() {
        let new = v("0.1.1");
        assert_eq!(rewrite_req("0.1.0", &new), "0.1.1");
        assert_eq!(rewrite_req("^0.1", &new), "0.1.1");
        assert_eq!(rewrite_req("=0.1.0", &new), "=0.1.1");
        assert_eq!(rewrite_req("0.1.0", &v("0.2.0")), "0.2.0");
    }

    /// A requirement already naming the new version survives byte for byte.
    ///
    /// Exact group alignment resolves the leading member to the version it already declares, so
    /// rewriting its dependents here would edit manifests that no increment covers.
    #[test]
    fn rewrite_req_leaves_a_requirement_that_already_names_the_version() {
        let new = v("1.1.0");
        assert_eq!(rewrite_req("1.1.0", &new), "1.1.0");
        assert_eq!(rewrite_req("^1.1.0", &new), "^1.1.0");
        assert_eq!(rewrite_req("=1.1.0", &new), "=1.1.0");
        assert_eq!(rewrite_req(" ^1.1.0 ", &new), " ^1.1.0 ");
    }

    /// A partial comparator that still admits the new version is narrowed to name it.
    ///
    /// `=0.1` admits every 0.1.x, so it survived the older rewrite untouched. It no longer
    /// does: naming the declared version is what the invariant requires, and a partial
    /// comparator names a range instead.
    #[test]
    fn rewrite_req_narrows_a_partial_comparator_to_the_new_version() {
        assert_eq!(rewrite_req("=0.1", &v("0.1.1")), "=0.1.1");
        assert_eq!(rewrite_req(" =0.1 ", &v("0.1.1")), "=0.1.1");
        assert_eq!(rewrite_req("=1", &v("1.5.0")), "=1.5.0");
        assert_eq!(rewrite_req("=0.1", &v("0.2.0")), "=0.2.0");
    }

    /// An unparsable requirement cannot be asked whether it matches, so the
    /// exact-comparator branch still decides its shape.
    #[test]
    fn rewrite_req_pins_an_unparsable_exact_requirement() {
        assert_eq!(rewrite_req("=not-a-version", &v("0.2.0")), "=0.2.0");
        assert_eq!(rewrite_req("not-a-version", &v("0.2.0")), "0.2.0");
    }

    #[test]
    fn set_version_item_replaces_workspace_inherit() {
        let mut doc = dep_item(
            r#"
[package]
name = "foo"
version.workspace = true
"#,
        );
        let package = doc
            .get_mut("package")
            .and_then(Item::as_table_like_mut)
            .unwrap();
        assert!(set_package_version_item(package, &v("0.2.0")));
        assert!(doc.to_string().contains("version = \"0.2.0\""));
        assert!(!doc.to_string().contains("workspace"));
    }

    #[test]
    fn dep_path_reads_inline_and_full_dependency_tables() {
        let doc = dep_item("[dependencies]\nfoo = { version = \"0.1.0\", path = \"../foo\" }\n");
        let entry = doc
            .get("dependencies")
            .and_then(Item::as_table_like)
            .and_then(|table| table.get("foo"))
            .unwrap();
        assert_eq!(dep_path(entry), Some("../foo"));
        let doc = dep_item("[dependencies.foo]\nversion = \"0.1.0\"\npath = \"../foo\"\n");
        let entry = doc
            .get("dependencies")
            .and_then(Item::as_table_like)
            .and_then(|table| table.get("foo"))
            .unwrap();
        assert_eq!(dep_path(entry), Some("../foo"));
        let doc = dep_item("[dependencies]\nfoo = \"0.1.0\"\n");
        let entry = doc
            .get("dependencies")
            .and_then(Item::as_table_like)
            .and_then(|table| table.get("foo"))
            .unwrap();
        assert_eq!(dep_path(entry), None);
    }

    #[test]
    fn rewrite_dep_entry_updates_bare_string_and_table() {
        let mut doc = dep_item("[dependencies]\nfoo = \"0.1.0\"\n");
        assert!(rewrite_dep_entry(first_dep(&mut doc), &v("0.2.0")));
        assert!(doc.to_string().contains("0.2.0"));

        let mut doc = dep_item("[dependencies]\nfoo = \"=0.1.0\"\n");
        assert!(rewrite_dep_entry(first_dep(&mut doc), &v("0.1.1")));
        assert!(doc.to_string().contains("=0.1.1"));

        let mut doc = dep_item("[dependencies]\nfoo = { version = \"0.1.0\", path = \"../x\" }\n");
        assert!(rewrite_dep_entry(first_dep(&mut doc), &v("0.2.0")));
        assert!(doc.to_string().contains("version = \"0.2.0\""));

        let mut doc = dep_item(
            "
[dependencies.foo]
version = \"0.1.0\"
",
        );
        assert!(rewrite_dep_entry(first_dep(&mut doc), &v("0.3.0")));
        assert!(doc.to_string().contains("0.3.0"));
    }

    #[test]
    fn dep_package_name_reads_package_alias() {
        let doc =
            dep_item("[dependencies]\nfoo-alias = { package = \"foo\", version = \"0.1.0\" }\n");
        let entry = doc
            .get("dependencies")
            .and_then(Item::as_table_like)
            .and_then(|table| table.get("foo-alias"))
            .unwrap();
        assert_eq!(dep_package_name(entry, "foo-alias"), "foo");
        let doc = dep_item("[dependencies]\nfoo = \"0.1.0\"\n");
        let entry = doc
            .get("dependencies")
            .and_then(Item::as_table_like)
            .and_then(|table| table.get("foo"))
            .unwrap();
        assert_eq!(dep_package_name(entry, "foo"), "foo");
        let doc = dep_item(
            "
[dependencies.foo-alias]
package = \"foo\"
version = \"0.1.0\"
",
        );
        let entry = doc
            .get("dependencies")
            .and_then(Item::as_table_like)
            .and_then(|table| table.get("foo-alias"))
            .unwrap();
        assert_eq!(dep_package_name(entry, "foo-alias"), "foo");
    }
    /// Every dependency declaration form is rewritten.
    ///
    /// Cargo accepts a dependency as a bare requirement string, an inline table, or a table of its
    /// own, and a `=` pin in any of them has to follow the package it pins.
    #[test]
    fn every_dependency_declaration_form_is_rewritten() {
        let new = v("0.2.0");

        let mut bare = Item::Value(Value::from("=0.1.0"));
        assert!(rewrite_dep_entry(&mut bare, &new));
        assert_eq!(bare.as_str(), Some("=0.2.0"));

        let mut doc = dep_item("[dependencies.demo]\nversion = \"=0.1.0\"\npath = \"../demo\"\n");
        let entry = doc
            .get_mut("dependencies")
            .and_then(Item::as_table_like_mut)
            .and_then(|deps| deps.get_mut("demo"))
            .unwrap();
        assert!(rewrite_dep_entry(entry, &new));
        assert!(doc.to_string().contains("version = \"=0.2.0\""), "{doc}");
    }

    /// A dependency without a version is left alone.
    ///
    /// A path dependency can omit the version entirely, which leaves nothing to rewrite rather than
    /// being an error.
    #[test]
    fn a_dependency_without_a_version_is_left_alone() {
        let new = v("0.2.0");

        let mut doc = dep_item("[dependencies.demo]\npath = \"../demo\"\n");
        let entry = doc
            .get_mut("dependencies")
            .and_then(Item::as_table_like_mut)
            .and_then(|deps| deps.get_mut("demo"))
            .unwrap();
        assert!(!rewrite_dep_entry(entry, &new));

        let mut inline_doc = dep_item("[dependencies]\ndemo = { path = \"../demo\" }\n");
        let inline = inline_doc
            .get_mut("dependencies")
            .and_then(Item::as_table_like_mut)
            .and_then(|deps| deps.get_mut("demo"))
            .unwrap();
        assert!(!rewrite_dep_entry(inline, &new));
    }

    /// Manifests outside the plan are left untouched.
    ///
    /// A rewrite pass runs over every manifest in the workspace, including ones that declare no
    /// package, no plan target, or no workspace table at all.
    #[test]
    fn manifests_outside_the_plan_are_left_untouched() {
        let resolved = ResolvedVersions {
            packages: BTreeMap::from([("demo".to_string(), v("0.2.0"))]),
        };
        let members = demo_members();
        let targets = targets_for("/ws/packages/demo", &members);
        let verbose = Verbose::new(false);
        let unchanged = |text: &str| {
            let mut doc = dep_item(text);
            rewrite_workspace_dependencies(&mut doc, &targets, &resolved, verbose);
            rewrite_package_version(&mut doc, &resolved, verbose);
            assert_eq!(doc.to_string(), text);
        };

        unchanged("[workspace]\nmembers = [\"packages/*\"]\n");
        unchanged("[package]\nversion = \"0.1.0\"\n");
        unchanged("[package]\nname = \"other\"\nversion = \"0.1.0\"\n");
        unchanged("[dependencies]\ndemo = { version = \"0.1.0\" }\n");
    }

    /// A dependency without a path or a plan entry is not rewritten.
    ///
    /// Only path dependencies follow a plan: a registry dependency on a package of the same name is
    /// a different package as far as this workspace goes.
    #[test]
    fn a_dependency_without_a_path_or_a_plan_entry_is_not_rewritten() {
        let resolved = ResolvedVersions {
            packages: BTreeMap::from([("demo".to_string(), v("0.2.0"))]),
        };
        let members = demo_members();
        let targets = targets_for("/ws/packages/caller", &members);
        let text = "[dependencies]\ndemo = \"0.1.0\"\nother = { version = \"0.1.0\", path = \"../other\" }\n";
        let mut doc = dep_item(text);

        rewrite_dependency_tables(&mut doc, &targets, &resolved, Verbose::new(false));

        assert_eq!(doc.to_string(), text);
    }

    /// A path resolving to the declaring member is rewritten.
    ///
    /// A path dependency follows the plan only when its path resolves to the member directory that
    /// declares that package, so resolution is what admits the rewrite rather than the package name
    /// alone.
    #[test]
    fn a_path_resolving_to_the_declaring_member_is_rewritten() {
        let resolved = ResolvedVersions {
            packages: BTreeMap::from([("demo".to_string(), v("0.2.0"))]),
        };
        let members = demo_members();
        let targets = targets_for("/ws/packages/caller", &members);

        let mut inside =
            dep_item("[dependencies]\ndemo = { version = \"0.1.0\", path = \"../demo\" }\n");
        rewrite_dependency_tables(&mut inside, &targets, &resolved, Verbose::new(false));

        assert!(inside.to_string().contains("version = \"0.2.0\""));
    }

    /// A path outside the workspace keeps its own requirement.
    ///
    /// A same-named package living outside the workspace is a different package, so its requirement
    /// must survive a plan that names ours.
    // The lexical form does not match, so the rewrite falls through to asking the filesystem, which
    // Miri's isolation refuses.
    #[cfg_attr(miri, ignore)]
    #[test]
    fn a_path_outside_the_workspace_keeps_its_own_requirement() {
        let resolved = ResolvedVersions {
            packages: BTreeMap::from([("demo".to_string(), v("0.2.0"))]),
        };
        let members = demo_members();
        let targets = targets_for("/ws/packages/caller", &members);

        let outside_text =
            "[dependencies]\ndemo = { version = \"0.1.0\", path = \"../../vendor/demo\" }\n";
        let mut outside = dep_item(outside_text);
        rewrite_dependency_tables(&mut outside, &targets, &resolved, Verbose::new(false));

        assert_eq!(outside.to_string(), outside_text);
    }

    /// A path is folded lexically.
    ///
    /// The helper folds `.` and `..` itself because the filesystem is not consulted, so both must
    /// be recognised wherever a manifest spells them.
    #[test]
    fn a_path_is_folded_lexically() {
        assert_eq!(
            normalize_lexically(Path::new("./packages/./demo")),
            PathBuf::from("packages/demo")
        );
        assert_eq!(
            normalize_lexically(Path::new("/ws/packages/caller/../demo")),
            PathBuf::from("/ws/packages/demo")
        );
    }

    /// A path spelled with redundant components still resolves to the member.
    ///
    /// A manifest may spell the same member directory with `./` or a redundant `..` hop, which the
    /// filesystem would accept and a plain string comparison would not.
    #[test]
    fn a_path_spelled_with_redundant_components_still_resolves_to_the_member() {
        let resolved = ResolvedVersions {
            packages: BTreeMap::from([("demo".to_string(), v("0.2.0"))]),
        };
        let members = demo_members();
        let targets = targets_for("/ws/packages/caller", &members);

        for path in ["./../demo", "../caller/../demo"] {
            let mut item = dep_item(&format!(
                "[dependencies]\ndemo = {{ version = \"0.1.0\", path = \"{path}\" }}\n"
            ));
            rewrite_dependency_tables(&mut item, &targets, &resolved, Verbose::new(false));
            assert!(
                item.to_string().contains("version = \"0.2.0\""),
                "path {path} did not resolve to the member"
            );
        }
    }

    /// A path reaching a member through a link still resolves to the member.
    ///
    /// Cargo resolves a dependency path through the filesystem, so a link that reaches a workspace
    /// member declares that member and its requirement must follow the member's new version.
    #[cfg(unix)]
    #[cfg_attr(miri, ignore)] // tempdir and symlinks are host filesystem, which Miri cannot emulate.
    #[test]
    fn a_path_reaching_a_member_through_a_link_still_resolves_to_the_member() {
        let dir = tempdir().unwrap();
        let root = dir.path();
        fs::create_dir_all(root.join("packages/demo")).unwrap();
        fs::create_dir_all(root.join("packages/caller")).unwrap();
        symlink(root.join("packages/demo"), root.join("packages/demo-link")).unwrap();

        let resolved = ResolvedVersions {
            packages: BTreeMap::from([("demo".to_string(), v("0.2.0"))]),
        };
        let members = BTreeMap::from([(root.join("packages/demo"), "demo".to_string())]);
        let targets = DepTargets {
            manifest_dir: root.join("packages/caller"),
            members_by_dir: &members,
        };

        let mut item =
            dep_item("[dependencies]\ndemo = { version = \"=0.1.0\", path = \"../demo-link\" }\n");
        rewrite_dependency_tables(&mut item, &targets, &resolved, Verbose::new(false));

        assert!(item.to_string().contains("=0.2.0"), "{item}");
    }

    /// A path that does not exist declares no member.
    ///
    /// A path that reaches nothing on disk names no member, whatever its spelling, so an outside
    /// dependency stays untouched.
    #[cfg_attr(miri, ignore)] // tempdir is host filesystem, which Miri cannot emulate.
    #[test]
    fn a_path_that_does_not_exist_declares_no_member() {
        let dir = tempdir().unwrap();
        let members = BTreeMap::from([(dir.path().join("packages/demo"), "demo".to_string())]);
        let targets = DepTargets {
            manifest_dir: dir.path().join("packages/caller"),
            members_by_dir: &members,
        };

        assert!(!targets.declares("../gone", "demo"));
    }
    /// A workspace whose only member is `demo`.
    ///
    /// It is laid out under a shared root so the rewrite tests can express both
    /// in-workspace and outside paths.
    fn demo_members() -> BTreeMap<PathBuf, String> {
        BTreeMap::from([(PathBuf::from("/ws/packages/demo"), "demo".to_string())])
    }

    fn targets_for<'a>(
        manifest_dir: &str,
        members: &'a BTreeMap<PathBuf, String>,
    ) -> DepTargets<'a> {
        DepTargets {
            manifest_dir: PathBuf::from(manifest_dir),
            members_by_dir: members,
        }
    }

    #[test]
    fn a_version_that_is_not_a_string_is_left_alone() {
        let mut doc = dep_item("[package]\nname = \"demo\"\nversion = 1\n");
        let package = doc
            .get_mut("package")
            .and_then(Item::as_table_like_mut)
            .unwrap();

        assert!(!set_package_version_item(package, &v("0.2.0")));
    }

    #[test]
    fn an_empty_dependency_item_is_not_rewritten() {
        let mut absent = Item::None;

        assert!(!rewrite_dep_entry(&mut absent, &v("0.2.0")));
        assert_eq!(dep_path(&absent), None);
        assert_eq!(dep_package_name(&absent, "demo"), "demo");
    }

    #[test]
    fn a_requirement_that_already_matches_is_not_rewritten() {
        let mut bare = Item::Value(Value::from("=0.2.0"));
        assert!(!rewrite_dep_entry(&mut bare, &v("0.2.0")));
    }
}
