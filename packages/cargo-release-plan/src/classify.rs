// Classification of publishable packages against their anchors.

use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::{MAIN_SEPARATOR, Path, PathBuf};
use std::rc::Rc;
use std::{fs, io, str};

use ohno::AppError;
use semver::Version;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use toml_edit::DocumentMut;

use crate::anchor::{Anchor, Presence, TimelineEntry, resolve_anchor};
use crate::diff::{FileVersion, file_diff, mode_change_diff};
use crate::git::{DefaultBase, GitRepo, TreeEntry, WorkTreeModes, join_git_rel, tree_mode};
use crate::groups::GroupVerdict;
use crate::inherited::{InheritedChange, inherited_changes};
use crate::lockfile::{Closure, ClosureChange, InstallationGraph, Lockfile, closure_changes};
use crate::manifest::{
    DEFAULT_README_FILES, PackageIdentity, PackageManifest, PathCase, WorkspaceInherit,
    WorkspaceMembers, cargo_config_paths, collect_registry_indices, installation_error,
    installation_patches, is_workspace_excluded, is_workspace_member, parse_document,
    parse_package_manifest, parse_workspace_members, path_package_identity, to_git_separators,
};
use crate::metadata::{ReportedDep, WorkPackage, WorkTree, dependents_of, load_tracked_work_tree};
use crate::packaging::{PackagingRules, relativize};
use crate::text::{plural, quote_path, short_type_name};
use crate::verbose::Verbose;
use crate::{
    LockfileClosureUnavailableError, MalformedLockfileError, ReadFileError, SymlinkReleasedError,
    VersionRegressionError, short_commit,
};

/// Name Cargo requires for a workspace lockfile.
const LOCKFILE_FILE_NAME: &str = "Cargo.lock";
/// Name Cargo requires for a package manifest.
const MANIFEST_FILE_NAME: &str = "Cargo.toml";

/// Workspace classification: every publishable package plus its group verdicts.
///
/// This is the result the `report` and `check` commands render.
#[derive(Debug)]
pub(crate) struct Classification {
    pub(crate) head: String,
    /// The revision every anchor was resolved against.
    ///
    /// Retained as the revision the caller named - or the configured default
    /// when it named none - rather than the commit it resolved to, so a
    /// diagnostic can quote a command that reproduces this run.
    pub(crate) base: String,
    pub(crate) packages: Vec<PackageClass>,
    pub(crate) groups: BTreeMap<String, GroupVerdict>,
    pub(crate) work_tree: WorkTree,
    pub(crate) git: GitRepo,
    /// The case rules probed for the volume hosting the work tree.
    ///
    /// Carried so a later packaging probe resolves paths exactly as
    /// classification did.
    pub(crate) case: PathCase,
}

/// Per-package classification: its status and the evidence behind it.
///
/// The status, anchor, and change evidence are not independent: a package that
/// does not exist on the base line has no anchor and no evidence, while an
/// anchored package always has one and carries a patch whenever its released
/// files differ, whatever its status. The classifier produces only those
/// combinations, so they are held in one closed
/// [`Verdict`] rather than as separately writable fields. `check` gates the
/// process exit on the status while `report` emits the anchor and evidence
/// beside it, and the two must never disagree.
/// Ref: `docs/design.md`, "Package status".
#[derive(Clone, Debug)]
pub(crate) struct PackageClass {
    pub(crate) name: String,
    pub(crate) declared_version: Version,
    pub(crate) group: Option<String>,
    verdict: Verdict,
    pub(crate) stat: DiffStat,
    pub(crate) untracked: Vec<String>,
    pub(crate) dependencies: Vec<ReportedDep>,
    pub(crate) dependents: Vec<String>,
    /// Whether the package's library is documented for consumers.
    ///
    /// Ref: `crate::metadata::WorkPackage::consumer_contract`.
    pub(crate) consumer_contract: bool,
    pub(crate) manifest_path: PathBuf,
}

impl PackageClass {
    /// Classification status, as reported and as gated on.
    pub(crate) fn status(&self) -> PackageStatus {
        match &self.verdict {
            Verdict::New | Verdict::PendingRelease { .. } => PackageStatus::PendingRelease,
            Verdict::Unchanged { .. } => PackageStatus::Unchanged,
            Verdict::NeedsIncrement { .. } => PackageStatus::NeedsIncrement,
        }
    }

    /// The commit the released content was compared against, if there is one.
    pub(crate) fn anchor(&self) -> Option<&Anchor> {
        match &self.verdict {
            Verdict::New => None,
            Verdict::PendingRelease { anchor, .. }
            | Verdict::Unchanged { anchor }
            | Verdict::NeedsIncrement { anchor, .. } => Some(anchor),
        }
    }

    /// Released-content and inherited-value differences against the anchor.
    pub(crate) fn changed(&self) -> &[ChangedItem] {
        match &self.verdict {
            Verdict::New | Verdict::Unchanged { .. } => &[],
            Verdict::PendingRelease { changed, .. } | Verdict::NeedsIncrement { changed, .. } => {
                changed
            }
        }
    }

    /// The rendered file-difference patch.
    ///
    /// Empty when the package has no released file difference. Pending-release
    /// packages retain the patch because judging whether an existing increment
    /// covers accumulated changes needs the same evidence as choosing a new
    /// increment. Inherited workspace values and locked dependency identities are
    /// not file differences and never appear here.
    pub(crate) fn patch(&self) -> &str {
        match &self.verdict {
            Verdict::PendingRelease { patch, .. } | Verdict::NeedsIncrement { patch, .. } => patch,
            Verdict::New | Verdict::Unchanged { .. } => "",
        }
    }
}

/// Constructors for tests in other modules, which cannot name [`Verdict`].
///
/// They take only the evidence their outcome admits, so a test cannot assemble
/// a state the classifier would never produce. Everything the assertions do not
/// observe is left empty.
#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
impl PackageClass {
    pub(crate) fn unchanged(
        name: &str,
        declared_version: Version,
        anchor: Anchor,
        manifest_path: PathBuf,
    ) -> Self {
        Self::with_verdict(
            name,
            declared_version,
            Verdict::Unchanged { anchor },
            manifest_path,
        )
    }

    pub(crate) fn pending_release(
        name: &str,
        declared_version: Version,
        anchor: Anchor,
        manifest_path: PathBuf,
    ) -> Self {
        Self::with_verdict(
            name,
            declared_version,
            Verdict::PendingRelease {
                anchor,
                changed: Vec::new(),
                patch: String::new(),
            },
            manifest_path,
        )
    }

    pub(crate) fn needs_increment(
        name: &str,
        declared_version: Version,
        anchor: Anchor,
        changed: Vec<ChangedItem>,
        manifest_path: PathBuf,
    ) -> Self {
        Self::with_verdict(
            name,
            declared_version,
            Verdict::NeedsIncrement {
                anchor,
                changed,
                patch: String::new(),
            },
            manifest_path,
        )
    }

    /// A package created on this branch, with no anchor to compare against.
    pub(crate) fn new_package(
        name: &str,
        declared_version: Version,
        manifest_path: PathBuf,
    ) -> Self {
        Self::with_verdict(name, declared_version, Verdict::New, manifest_path)
    }

    fn with_verdict(
        name: &str,
        declared_version: Version,
        verdict: Verdict,
        manifest_path: PathBuf,
    ) -> Self {
        Self {
            name: name.to_string(),
            declared_version,
            group: None,
            verdict,
            stat: DiffStat {
                files: 0,
                insertions: 0,
                deletions: 0,
            },
            untracked: Vec::new(),
            dependencies: Vec::new(),
            dependents: Vec::new(),
            consumer_contract: true,
            manifest_path,
        }
    }
}

/// The classifier's outcome for one package, with the evidence it implies.
///
/// Each alternative carries exactly what that outcome can be justified by, so
/// there is no way to express an anchorless failure or an unchanged package that
/// still holds a patch. Ref: `docs/design.md`, "Package status".
#[derive(Clone, Debug)]
enum Verdict {
    /// The package was created on this branch.
    ///
    /// It is absent from the base line and from its earlier first-parent
    /// history, so its creation counts as a version increase and there is
    /// nothing to compare against.
    New,
    /// The declared version increased over the anchor's.
    PendingRelease {
        anchor: Anchor,
        changed: Vec<ChangedItem>,
        patch: String,
    },
    /// The declared version did not increase, and neither did the content.
    Unchanged { anchor: Anchor },
    /// Released content differs from the anchor without a version increase.
    NeedsIncrement {
        anchor: Anchor,
        changed: Vec<ChangedItem>,
        patch: String,
    },
}

/// Classification status of one publishable package.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum PackageStatus {
    PendingRelease,
    NeedsIncrement,
    Unchanged,
}

impl PackageStatus {
    /// Derives the status shared by classification and report validation.
    ///
    /// Missing anchors have no comparison evidence; anchored versions must not regress.
    pub(crate) fn from_evidence(
        declared: &Version,
        anchor: Option<&Version>,
        has_changes: bool,
    ) -> Option<Self> {
        match anchor {
            None if has_changes => None,
            None => Some(Self::PendingRelease),
            Some(anchor) => match declared.cmp(anchor) {
                Ordering::Less => None,
                Ordering::Greater => Some(Self::PendingRelease),
                Ordering::Equal if has_changes => Some(Self::NeedsIncrement),
                Ordering::Equal => Some(Self::Unchanged),
            },
        }
    }
}

/// A released-content, inherited-value or locked-dependency change.
///
/// Serialized as the report.json object with `path`/`change`, `field`, or
/// `dependency`/`change`, plus `source`, so callers keep a stable JSON shape
/// without optional nulls.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(tag = "source", rename_all = "kebab-case")]
pub(crate) enum ChangedItem {
    Package { path: String, change: String },
    Inherited { field: String },
    Lockfile { dependency: String, change: String },
}

impl Serialize for ChangedItem {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        // The struct name reaches self-describing formats, so derive it from the
        // type rather than repeating its spelling in a literal.
        let name = short_type_name::<Self>();
        match self {
            Self::Package { path, change } => {
                let mut state = serializer.serialize_struct(name, 3)?;
                state.serialize_field("path", path)?;
                state.serialize_field("change", change)?;
                state.serialize_field("source", "package")?;
                state.end()
            }
            Self::Inherited { field } => {
                let mut state = serializer.serialize_struct(name, 2)?;
                state.serialize_field("field", field)?;
                state.serialize_field("source", "inherited")?;
                state.end()
            }
            Self::Lockfile { dependency, change } => {
                let mut state = serializer.serialize_struct(name, 3)?;
                state.serialize_field("dependency", dependency)?;
                state.serialize_field("change", change)?;
                state.serialize_field("source", "lockfile")?;
                state.end()
            }
        }
    }
}

/// Insertion/deletion counts for one package in `report.json`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct DiffStat {
    pub(crate) files: usize,
    pub(crate) insertions: usize,
    pub(crate) deletions: usize,
}

/// Anchor identity as serialized in `report.json`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct AnchorJson {
    pub(crate) commit: String,
    pub(crate) version: String,
}

pub(crate) fn classify(
    manifest_path: &Path,
    base: Option<&str>,
    verbose: Verbose,
) -> Result<Classification, AppError> {
    let (mut work_tree, git) = load_tracked_work_tree(manifest_path)?;
    let base = match base {
        Some(base) => base.to_owned(),
        None => {
            let default = git.default_base()?;
            verbose.note(|| match &default {
                DefaultBase::RemoteHead(revision) => format!(
                    "no --base given; the remote records {} as its default branch, so that \
                     is taken to be the branch releases are made from",
                    quote_path(revision)
                ),
                DefaultBase::Convention(revision) => format!(
                    "no --base given and the remote records no default branch, so the \
                     conventional {} is assumed to be the branch releases are made from",
                    quote_path(revision)
                ),
            });
            default.revision().to_owned()
        }
    };
    for package in &mut work_tree.packages {
        package.manifest.directory = join_git_rel(git.prefix(), &package.manifest.directory);
        package.resources =
            resolve_resources(&package.manifest, &package.manifest.directory, git.prefix());
    }
    let head = git.head()?;
    let base_sha = git.rev_parse(&base)?;
    verbose.note(|| {
        format!(
            "classifying {} against base {} ({base_sha}); \
         anchors are the last parsed version change on that revision's first-parent line, \
         not on the work tree's branch",
            plural(work_tree.packages.len(), "publishable package"),
            quote_path(&base)
        )
    });

    let mut cache = SnapshotCache::new(
        &work_tree.workspace_root,
        work_tree.installation.registries.clone(),
    );
    let base_snapshot = cache.snapshot(&git, &base_sha)?;
    let work_root_path = work_tree.workspace_root.join("Cargo.toml");
    let work_root_doc = parse_document(
        &work_root_path,
        &fs::read_to_string(&work_root_path)
            .map_err(|error| ReadFileError::caused_by(&work_root_path, error))?,
    )?;

    let commits = git.first_parent_manifest_commits(&base_sha)?;
    let mut classes = Vec::new();
    let versions = work_tree.target_versions();
    let exempt: HashSet<String> = work_tree
        .version_targets
        .iter()
        .filter(|target| is_new_on_base(&base_snapshot, &target.name))
        .map(|target| target.name.clone())
        .collect();
    let mut lockfiles = LockfileCache::default();

    for package in &work_tree.packages {
        let class = classify_one(
            package,
            &work_tree,
            &git,
            &base_sha,
            &commits,
            &base_snapshot,
            &work_root_doc,
            &mut cache,
            &mut lockfiles,
            verbose,
        )?;
        classes.push(class);
    }

    let group_verdicts = work_tree.groups.verdicts(&versions, &exempt);
    for (name, verdict) in &group_verdicts {
        let members: Vec<Cow<'_, str>> = verdict
            .members()
            .iter()
            .map(String::as_str)
            .map(quote_path)
            .collect();
        verbose.note(|| {
            format!(
                "version group {} is derived from exact workspace dependencies: members [{}]; \
                 consistent={} (members absent from the baseline are exempt from matching \
                 declared versions but remain version targets)",
                quote_path(name),
                members.join(", "),
                verdict.is_consistent()
            )
        });
    }

    Ok(Classification {
        head,
        base,
        packages: classes,
        groups: group_verdicts,
        work_tree,
        git,
        case: cache.case(),
    })
}

#[expect(
    clippy::too_many_arguments,
    reason = "classification needs the work-tree package, both trees, the first-parent walk, and both snapshot caches together"
)]
fn classify_one(
    package: &WorkPackage,
    work_tree: &WorkTree,
    git: &GitRepo,
    base_sha: &str,
    commits: &[String],
    base_snapshot: &CommitSnapshot,
    work_root_doc: &DocumentMut,
    cache: &mut SnapshotCache,
    lockfiles: &mut LockfileCache,
    verbose: Verbose,
) -> Result<PackageClass, AppError> {
    let name = &package.manifest.name;
    // Diagnostics never render a repository-controlled name raw.
    // Ref: docs/implementation.md, "Diagnostics".
    let shown = quote_path(name);
    let group = work_tree.groups.group_of(name).map(ToOwned::to_owned);
    let dependents = dependents_of(&work_tree.packages, name);

    let anchor = if base_snapshot.packages.contains_key(name) {
        let timeline = build_timeline(git, name, commits, cache)?;
        resolve_anchor(name, &timeline)?
    } else {
        // A package the baseline does not publish has no released version to
        // compare against, whether it never existed or once did and was
        // withdrawn. Reconciling a name that was published before is left to
        // whoever restores it: guessing which older release a restored
        // directory continues would make the tool's verdict depend on history
        // a fetch may not even carry.
        // Ref: docs/design.md, "Packages the baseline does not publish".
        verbose.note(|| {
            format!(
                "{shown}: not published by the baseline {base_sha}, so it is treated as a new \
                 package whose first release this branch prepares"
            )
        });
        let side = work_tree_side(package, cache.case());
        let resource_paths: Vec<&str> = side.resources.values().map(String::as_str).collect();
        let tracked_paths = git.tracked_paths(&resource_paths, side.case)?;
        let tracked_resources = tracked_resources(&side, &tracked_paths);
        let content = released_in_work_tree(git, &side, &tracked_resources)?;
        let work_modes = work_tree_modes(git, &side, &tracked_resources)?;
        _ = validated_work_tree_files(git, name, &content.released, &work_modes)?;
        let untracked =
            untracked_released(git, &side, &tracked_resources, &content.present_tracked)?;
        log_untracked(verbose, name, untracked.len());
        return Ok(PackageClass {
            name: name.clone(),
            declared_version: package.manifest.version.clone(),
            group,
            verdict: Verdict::New,
            stat: DiffStat {
                files: 0,
                insertions: 0,
                deletions: 0,
            },
            untracked,
            dependencies: package.dependencies.clone(),
            dependents,
            consumer_contract: package.consumer_contract,
            manifest_path: package.manifest_path.clone(),
        });
    };
    let short_anchor = short_commit(&anchor.commit);
    verbose.note(|| format!(
        "{shown}: anchor {short_anchor} declared {}; work tree declares {}; the package counts as \
         pending release only when the work-tree version is greater than the anchor version \
         (parsed, not textual)",
        anchor.version,
        package.manifest.version
    ));

    let anchor_snapshot = cache.snapshot(git, &anchor.commit)?;
    let anchor_pkg = anchor_snapshot
        .packages
        .get(name)
        .expect("the anchor commit is the newest commit at which the anchor version was observed, and both the timeline and this snapshot read that version from the same cache, so the package is present here");

    let (changed_files, patch, stat, untracked) = diff_package(
        git,
        name,
        &anchor.commit,
        &PackageSide {
            dir: &anchor_pkg.directory,
            rules: &anchor_pkg.packaging,
            resources: &anchor_pkg.resources,
            auto_readme: anchor_pkg.auto_readme,
            case: cache.case(),
        },
        &work_tree_side(package, cache.case()),
    )?;

    let mut changed = changed_files;
    let inherited: Vec<InheritedChange> = inherited_changes(
        &package.manifest.inherited,
        &anchor_snapshot.root_doc,
        work_root_doc,
    );
    for item in inherited {
        verbose.note(|| {
            format!(
                "{shown}: inherited {} changed between the anchor and the work tree, so the root \
             manifest is in scope for this package",
                quote_path(&item.field)
            )
        });
        changed.push(ChangedItem::Inherited { field: item.field });
    }

    log_untracked(verbose, name, untracked.len());

    if anchor_pkg.has_lockfile_target || package.has_lockfile_target {
        for (dependency, change) in lockfile_closure_changes(
            lockfiles,
            git,
            work_tree,
            name,
            anchor_pkg,
            package,
            &anchor.commit,
            &anchor_snapshot.installation,
        )? {
            verbose.note(|| {
                format!(
                    "{shown}: the locked identity of {} is {} between the anchor and the work \
                     tree, and this package has an installable binary target at one or both \
                     endpoints, so the dependency is released content",
                    quote_path(&dependency),
                    change.as_str()
                )
            });
            changed.push(ChangedItem::Lockfile {
                dependency,
                change: change.as_str().to_owned(),
            });
        }
    }

    // A declared version below the anchor cannot describe a release: the anchor
    // version is already on the base line, so the work tree would re-publish an
    // existing version with different content. Ref: docs/design.md,
    // "Version monotonicity".
    if package.manifest.version < anchor.version {
        return Err(VersionRegressionError::new(
            name,
            package.manifest.version.clone(),
            anchor.version.clone(),
            &anchor.commit,
        )
        .into());
    }

    let version_increased = package.manifest.version > anchor.version;
    let status = PackageStatus::from_evidence(
        &package.manifest.version,
        Some(&anchor.version),
        !changed.is_empty(),
    )
    .expect("the anchor is present and version regression was rejected");
    let verdict = match status {
        PackageStatus::PendingRelease => Verdict::PendingRelease {
            anchor,
            changed,
            patch,
        },
        PackageStatus::Unchanged => Verdict::Unchanged { anchor },
        PackageStatus::NeedsIncrement => Verdict::NeedsIncrement {
            anchor,
            changed,
            patch,
        },
    };
    let class = PackageClass {
        name: name.clone(),
        declared_version: package.manifest.version.clone(),
        group,
        verdict,
        stat,
        untracked,
        dependencies: package.dependencies.clone(),
        dependents,
        consumer_contract: package.consumer_contract,
        manifest_path: package.manifest_path.clone(),
    };
    verbose.note(|| {
        format!(
            "{shown}: status {:?} because version_increased={version_increased} and \
         changed_items={}",
            class.status(),
            class.changed().len()
        )
    });

    Ok(class)
}

fn build_timeline(
    git: &GitRepo,
    name: &str,
    commits: &[String],
    cache: &mut SnapshotCache,
) -> Result<Vec<TimelineEntry>, AppError> {
    let mut timeline = Vec::with_capacity(commits.len());
    for (index, commit) in commits.iter().enumerate() {
        let snapshot = cache.snapshot(git, commit)?;
        let presence = snapshot.packages.get(name).map_or_else(
            || {
                if snapshot.unpublished.contains(name) {
                    Presence::Unpublished
                } else {
                    Presence::Absent
                }
            },
            |pkg| Presence::Published(pkg.version.clone()),
        );
        let is_last = index
            .checked_add(1)
            .is_some_and(|next| next == commits.len());
        let has_parent = if is_last {
            git.has_parent_or_is_shallow_boundary(commit)?
        } else {
            true
        };
        timeline.push(TimelineEntry {
            commit: commit.clone(),
            presence,
            has_parent,
        });
        // Stop once we have observed a version different from the base (the
        // resolver only needs the first change plus whether the last entry is
        // a true root). Keep going until a change appears so creation vs
        // shallow can be distinguished.
        if can_stop_timeline(&timeline) {
            break;
        }
    }
    Ok(timeline)
}

/// One end of a released-content comparison.
///
/// The anchor and the work tree are resolved independently, so each end carries
/// its own package directory and packaging rules.
struct PackageSide<'a> {
    dir: &'a str,
    rules: &'a PackagingRules,
    /// Files Cargo packs because a manifest key names them.
    ///
    /// Keyed by the path each takes inside the package archive and valued by
    /// its git-root-relative path.
    resources: &'a BTreeMap<String, String>,
    /// Whether Cargo picks this package's README by probing its directory.
    auto_readme: bool,
    /// How the volume hosting the workspace resolves path case.
    ///
    /// This decides whether a tracked spelling answers a README candidate Cargo
    /// probes for.
    case: PathCase,
}

/// Resolves the files a manifest key names for packaging.
///
/// Cargo packs the file named by `readme` or `license-file` regardless of
/// `include` and `exclude`, and from outside `package_dir` if that is where it
/// lives. A package's released content is therefore neither confined to its own
/// directory nor fully described by its packaging rules: a workspace-level
/// README that several members inherit is released content for every one of
/// them, and so is a README the package's own `include` leaves out.
/// Ref: docs/design.md, "Released content".
///
/// A resource that already lives inside the package directory keeps its own
/// package-relative path, matching where Cargo puts it. `package_dir`,
/// `workspace_prefix`, and the resolved values are all git-root-relative.
fn resolve_resources(
    manifest: &PackageManifest,
    package_dir: &str,
    workspace_prefix: &str,
) -> BTreeMap<String, String> {
    let workspace_prefix = workspace_prefix.trim_end_matches('/');
    let mut resolved = BTreeMap::new();
    let declared = manifest
        .resource_paths
        .iter()
        .map(|relative| (package_dir, relative))
        .chain(
            manifest
                .inherited_resource_paths
                .iter()
                .map(|relative| (workspace_prefix, relative)),
        );
    for (base, relative) in declared {
        let Some(full) = join_relative(base, relative) else {
            continue;
        };
        // Cargo leaves a resource that is already inside the package where it
        // is and only flattens one from outside into the crate root. Both are
        // recorded, because `include` and `exclude` do not apply to either: a
        // README the package excludes is still released content.
        let key = match relativize(&full, package_dir) {
            Some(rel) => rel.to_string(),
            // A resource from outside is flattened into the crate root under its
            // file name, which is everything after the last separator.
            None => full
                .rsplit_once('/')
                .map_or(full.as_str(), |(_, name)| name)
                .to_string(),
        };
        resolved.insert(key, full);
    }
    resolved
}

fn diff_package(
    git: &GitRepo,
    name: &str,
    anchor_commit: &str,
    anchor: &PackageSide<'_>,
    work_side: &PackageSide<'_>,
) -> Result<(Vec<ChangedItem>, String, DiffStat, Vec<String>), AppError> {
    // Released content is defined from git-tracked files, and a manifest
    // resource may sit outside the package directory or outside its packaging
    // rules, so the directory listing does not cover it. Querying Git for those
    // paths keeps an untracked README from being read off disk and reported as
    // a content change. Ref: docs/design.md, "Released content".
    let resource_paths: Vec<&str> = work_side.resources.values().map(String::as_str).collect();
    let tracked_paths = git.tracked_paths(&resource_paths, work_side.case)?;
    let tracked_resources = tracked_resources(work_side, &tracked_paths);

    let anchor_tree = anchor_tree_entries(git, anchor_commit, anchor)?;
    let anchor_files = released_at_commit(&anchor_tree, anchor);
    let work = released_in_work_tree(git, work_side, &tracked_resources)?;
    let work_files = &work.released;

    reject_anchor_symlinks(name, &anchor_tree, &anchor_files)?;

    // Git converts content on its way into the object database, so a file on
    // disk and the blob recording it need not hold the same bytes. Comparing
    // content identity rather than raw bytes puts both ends in the one
    // representation Git itself compares by, which is what keeps an LFS-tracked
    // asset or a line-ending rule from making an untouched package look
    // changed. Ref: docs/implementation.md, "Classification".
    let anchor_ids: HashMap<&str, &str> = anchor_tree
        .iter()
        .map(|entry| (entry.path.as_str(), entry.id.as_str()))
        .collect();
    let work_modes = work_tree_modes(git, work_side, &tracked_resources)?;
    let work_ids = work_blob_ids(git, name, work_files, &work_modes)?;

    // Cargo copies the executable bit into the archive, so a file made
    // executable without an edit is released content that changed even though
    // its blob is untouched. Ref: docs/design.md, "Released content".
    let anchor_exec: HashSet<&str> = anchor_tree
        .iter()
        .filter(|entry| entry.is_executable())
        .map(|entry| entry.path.as_str())
        .collect();
    let rels: BTreeSet<&str> = anchor_files
        .keys()
        .chain(work_files.keys())
        .map(String::as_str)
        .collect();

    let mut changed = Vec::new();
    let mut patch = String::new();
    let mut insertions = 0_usize;
    let mut deletions = 0_usize;

    for rel in rels {
        let old_id = anchor_files
            .get(rel)
            .and_then(|path| anchor_ids.get(path.as_str()).copied());
        let new_id = work_ids.get(rel).map(String::as_str);
        // The mode is only a change while the file exists at both ends: an
        // addition or a deletion is already reported by presence alone.
        let mode_change = match (anchor_files.get(rel), work_files.get(rel)) {
            (Some(old_path), Some(new_path)) if old_id.is_some() && new_id.is_some() => {
                let old_mode = tree_mode(anchor_exec.contains(old_path.as_str()));
                let new_mode = tree_mode(work_modes.is_executable(new_path));
                (old_mode != new_mode).then_some((old_mode, new_mode))
            }
            _ => None,
        };
        if old_id == new_id && mode_change.is_none() {
            continue;
        }
        let kind = match (old_id.is_some(), new_id.is_some()) {
            (false, true) => "added",
            (true, false) => "deleted",
            _ => "modified",
        };
        changed.push(ChangedItem::Package {
            path: rel.to_string(),
            change: kind.to_string(),
        });
        if let Some((old_mode, new_mode)) = mode_change {
            patch.push_str(&mode_change_diff(rel, old_mode, new_mode).text);
        }
        // Equal object ids prove the bytes are unchanged. This check comes after
        // mode rendering so a mode-only binary change cannot gain a false
        // "Binary files differ" line from the content renderer.
        if old_id == new_id {
            continue;
        }
        // The content itself is only needed to render an identity change.
        let old = match anchor_files.get(rel).filter(|_| old_id.is_some()) {
            Some(path) => git.show_file_bytes(anchor_commit, path)?,
            None => None,
        };
        let new = new_id.map(|id| git.show_blob_bytes(id)).transpose()?;
        let old_side = old.as_deref().map(|content| FileVersion {
            content,
            mode: tree_mode(
                anchor_files
                    .get(rel)
                    .is_some_and(|path| anchor_exec.contains(path.as_str())),
            ),
        });
        let new_side = new.as_deref().map(|content| FileVersion {
            content,
            mode: tree_mode(
                work_files
                    .get(rel)
                    .is_some_and(|path| work_modes.is_executable(path)),
            ),
        });
        let file_diff = file_diff(rel, old_side, new_side);
        insertions = insertions.saturating_add(file_diff.insertions);
        deletions = deletions.saturating_add(file_diff.deletions);
        patch.push_str(&file_diff.text);
    }

    let untracked = untracked_released(git, work_side, &tracked_resources, &work.present_tracked)?;

    let stat = DiffStat {
        files: changed.len(),
        insertions,
        deletions,
    };
    Ok((changed, patch, stat, untracked))
}

/// Git modes for released work-tree paths.
///
/// The package directory does not cover a manifest resource that lives outside
/// it, so those paths are asked for alongside the directory. Only tracked
/// resources are asked for, because an untracked one is not released content
/// and Git records no mode for it.
fn work_tree_modes(
    git: &GitRepo,
    side: &PackageSide<'_>,
    tracked_resources: &BTreeMap<String, String>,
) -> Result<WorkTreeModes, AppError> {
    let mut pathspecs = vec![side.dir];
    pathspecs.extend(tracked_resources.values().map(String::as_str));
    git.work_tree_modes(&pathspecs)
}

/// Object ids the released work-tree files would be stored under.
///
/// Keyed by the path each takes inside the package archive.
///
/// A tracked path the work tree no longer holds is left out, which is what makes
/// it read as deleted. A symbolic link stops the run here rather than being
/// hashed, because Git would hash the file it points at while the tree records
/// the link itself.
fn work_blob_ids(
    git: &GitRepo,
    name: &str,
    released: &HashMap<String, String>,
    modes: &WorkTreeModes,
) -> Result<HashMap<String, String>, AppError> {
    let files = validated_work_tree_files(git, name, released, modes)?;
    let paths: Vec<&str> = files.iter().map(|(_, path)| *path).collect();
    let ids = git.hash_objects(&paths)?;
    Ok(files
        .into_iter()
        .map(|(rel, _)| rel.to_string())
        .zip(ids)
        .collect())
}

/// Released work-tree files that are present and safe to hash.
///
/// Symbolic links stop classification before Git can dereference them. Both
/// index modes and filesystem metadata are required because either one may be
/// the only place that identifies a link in the current checkout.
fn validated_work_tree_files<'a>(
    git: &GitRepo,
    name: &str,
    released: &'a HashMap<String, String>,
    modes: &WorkTreeModes,
) -> Result<Vec<(&'a str, &'a str)>, AppError> {
    let mut files = Vec::with_capacity(released.len());
    for (rel, path) in released {
        if modes.is_symlink(path) {
            return Err(SymlinkReleasedError::new(name, path).into());
        }
        match fs::symlink_metadata(git.root().join(path)) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(SymlinkReleasedError::new(name, path).into());
            }
            Ok(_) => {}
            Err(error) if is_not_found(&error) => continue,
            Err(error) => {
                return Err(ReadFileError::caused_by(git.root().join(path), error).into());
            }
        }
        files.push((rel.as_str(), path.as_str()));
    }
    Ok(files)
}

/// Stops when the anchor released a symbolic link.
///
/// The tree's modes are the only place the distinction survives, so the paths
/// released at the anchor are matched against the links the tree records.
fn reject_anchor_symlinks(
    name: &str,
    entries: &[TreeEntry],
    released: &HashMap<String, String>,
) -> Result<(), AppError> {
    let links: HashSet<&str> = entries
        .iter()
        .filter(|entry| entry.is_symlink())
        .map(|entry| entry.path.as_str())
        .collect();
    if links.is_empty() {
        return Ok(());
    }
    // The released paths are a hash map, so the lowest matching path is chosen
    // to keep the reported one stable across runs.
    let offender = released
        .values()
        .filter(|path| links.contains(path.as_str()))
        .min()
        .cloned();
    match offender {
        Some(path) => Err(SymlinkReleasedError::new(name, &path).into()),
        None => Ok(()),
    }
}

/// Lists the tree at `commit` for everything a package could release.
///
/// The package directory alone does not cover a manifest resource that lives
/// outside it, so those paths are asked for in the same listing.
fn anchor_tree_entries(
    git: &GitRepo,
    commit: &str,
    side: &PackageSide<'_>,
) -> Result<Vec<TreeEntry>, AppError> {
    let mut pathspecs: Vec<&str> = vec![side.dir];
    pathspecs.extend(side.resources.values().map(String::as_str));
    let entries = git.ls_tree(commit, &pathspecs)?;
    let all_resources_found = side.resources.values().all(|resource| {
        entries
            .iter()
            .any(|entry| side.case.same_path(&entry.path, resource))
    });
    if side.case == PathCase::Insensitive && !all_resources_found {
        // Unlike `ls-files`, `ls-tree` rejects the `icase` pathspec magic.
        // Listing the tree is the only reliable way to resolve an external
        // resource whose recorded spelling differs from the manifest.
        return git.ls_tree(commit, &[""]);
    }
    Ok(entries)
}

fn released_at_commit(entries: &[TreeEntry], side: &PackageSide<'_>) -> HashMap<String, String> {
    let paths: Vec<String> = entries
        .iter()
        .map(|entry| entry.path.clone())
        .collect::<Vec<_>>();
    let mut released = released_from_paths(&paths, &paths, side);
    let resources = tracked_resources(side, &paths);
    // Reading a resource back from the commit yields nothing when the commit
    // did not track it, so the tree itself performs the tracked-only filter the
    // work tree needs `tracked_paths` for.
    add_resources(&mut released, resources.iter());
    released
}

/// Adds the files Cargo packs because a manifest key names them.
///
/// The packaging rules are not consulted: Cargo packs these regardless of
/// `include` and `exclude`. An entry never displaces a file the directory
/// listing already claimed at that path, matching Cargo, which keeps the first
/// claim on a path and warns rather than overwriting it.
fn add_resources<'a>(
    released: &mut HashMap<String, String>,
    resources: impl Iterator<Item = (&'a String, &'a String)>,
) {
    for (name, path) in resources {
        released.entry(name.clone()).or_insert_with(|| path.clone());
    }
}

/// Resolves manifest-declared resources to the spelling Git records.
///
/// Cargo follows the checkout's case rules when it opens the declared path.
/// Git's index and trees preserve their own spelling, which must be retained so
/// blob, mode, and historical lookups address the entry Git actually returned.
fn tracked_resources(side: &PackageSide<'_>, tracked_paths: &[String]) -> BTreeMap<String, String> {
    side.resources
        .iter()
        .filter_map(|(name, declared)| {
            tracked_paths
                .iter()
                .filter(|tracked| side.case.same_path(tracked, declared))
                .min()
                .map(|tracked| (name.clone(), tracked.clone()))
        })
        .collect()
}

/// Lists the untracked paths a package's rules would release.
///
/// Paths are package-relative.
///
/// These are advisory only: released content is defined from git-tracked files,
/// so an untracked path is never a change. Ref: docs/design.md, "Released
/// content".
fn untracked_released(
    git: &GitRepo,
    side: &PackageSide<'_>,
    tracked_resources: &BTreeMap<String, String>,
    tracked: &[String],
) -> Result<Vec<String>, AppError> {
    let listed: Vec<String> = git.ls_untracked(side.dir)?;
    // The same nested-package boundary the tracked listing observes applies
    // here, or a file under a nested package would be advertised as content
    // Cargo would pack for the outer one. The manifest drawing that boundary
    // may itself still be untracked, so both listings feed the scan.
    let mut boundary_paths = listed.clone();
    boundary_paths.extend_from_slice(tracked);
    let nested = nested_package_dirs(&boundary_paths, side.dir);

    let mut untracked: Vec<String> = listed
        .iter()
        .filter(|full| !is_inside_any(full, &nested))
        .filter_map(|full| {
            let rel = relativize(full, side.dir)?.to_string();
            side.rules.is_released(&rel).then_some(rel)
        })
        .collect();
    // The listing above is filtered by the packaging rules and stops at the
    // package directory, so a resource that those rules exclude or that is
    // declared from outside would go unmentioned even though Cargo would pack
    // it. It is advisory in exactly the same way, and it is named by the path
    // it takes inside the package archive.
    untracked.extend(side.resources.iter().filter_map(|(name, path)| {
        let present = git.root().join(path).symlink_metadata().is_ok();
        (present && !tracked_resources.contains_key(name)).then(|| name.clone())
    }));
    if side.auto_readme {
        // A README Cargo would detect is packed whatever the packaging rules
        // say. Detect across both sets because a higher-priority untracked name
        // can outrank a tracked fallback; only the selected untracked path is
        // advisory.
        let listed_set: HashSet<&str> = listed.iter().map(String::as_str).collect();
        let present: HashSet<&str> = tracked.iter().chain(&listed).map(String::as_str).collect();
        if let Some((name, full)) = detected_readme(side.dir, &present, side.case)
            && listed_set.contains(full.as_str())
        {
            untracked.push(name);
        }
    }
    untracked.sort_unstable();
    untracked.dedup();
    Ok(untracked)
}

/// The package-relative paths of one work-tree package's released content.
///
/// The packaging verifier compares Cargo's own listing against this, so it has
/// to be the very selection classification compares — reconstructing the set
/// from `include` and `exclude` alone would drop a README Cargo detects for
/// itself and take in the files of a nested package, reporting a mismatch on a
/// package whose rules are in fact right.
pub(crate) fn released_work_tree_paths(
    git: &GitRepo,
    package: &WorkPackage,
    case: PathCase,
) -> Result<BTreeSet<String>, AppError> {
    let side = work_tree_side(package, case);
    let resource_paths: Vec<&str> = side.resources.values().map(String::as_str).collect();
    let tracked_paths = git.tracked_paths(&resource_paths, side.case)?;
    let tracked_resources = tracked_resources(&side, &tracked_paths);
    let content = released_in_work_tree(git, &side, &tracked_resources)?;
    Ok(content.released.into_keys().collect())
}

/// The work-tree end of `package`'s released-content comparison.
fn work_tree_side(package: &WorkPackage, case: PathCase) -> PackageSide<'_> {
    PackageSide {
        dir: &package.manifest.directory,
        rules: &package.manifest.packaging,
        resources: &package.resources,
        auto_readme: package.manifest.auto_readme,
        case,
    }
}

/// One work-tree package's released content and the listing it was drawn from.
///
/// The tracked listing is reused for the advisory untracked scan, which needs
/// the same nested-package boundary this selection observed.
struct WorkTreeContent {
    released: HashMap<String, String>,
    present_tracked: Vec<String>,
}

fn released_in_work_tree(
    git: &GitRepo,
    side: &PackageSide<'_>,
    tracked_resources: &BTreeMap<String, String>,
) -> Result<WorkTreeContent, AppError> {
    let tracked = git.ls_files(side.dir)?;
    let present = present_in_work_tree(git, &tracked)?;
    let mut released = released_from_paths(&tracked, &present, side);
    add_resources(&mut released, tracked_resources.iter());
    Ok(WorkTreeContent {
        released,
        present_tracked: present,
    })
}

/// Whether a group member is too new on the base revision to be held to its
/// group's declared version.
///
/// The exemption is base membership, not anchor presence: a reintroduced package
/// is absent from the base yet still resolves an older anchor, and the documented
/// rule exempts every member that does not exist on the base revision from
/// matching its group (docs/design.md, "Version groups"). A member the base
/// carries but does not publish does exist there, and may well have been released
/// before it was withdrawn, so it stays bound to its group.
fn is_new_on_base(base: &CommitSnapshot, name: &str) -> bool {
    !base.packages.contains_key(name) && !base.unpublished.contains(name)
}

/// The subset of `paths` the work tree still holds on disk.
///
/// Git lists a tracked file whose work-tree copy has been deleted, but Cargo
/// packages what is on disk: a nested manifest that is gone no longer stops
/// packing, and a deleted default README is no longer there to be found. The
/// tracked listing still decides eligibility — released content is defined from
/// git-tracked files — so this narrower listing is used only for structure.
/// A dangling symbolic link counts as present, because Git tracks the link
/// itself rather than what it points at.
///
/// An untracked nested manifest draws no boundary because untracked paths never
/// enter the release verdict. The optional packaging probe still reports when
/// Cargo would treat one as structural input under `--allow-dirty`.
/// Ref: docs/design.md, "Released content".
/// Only a path that is not there is absent; any other failure stops the run,
/// because reading it as a deletion would silently change what the package
/// releases.
fn present_in_work_tree(git: &GitRepo, paths: &[String]) -> Result<Vec<String>, AppError> {
    let mut present = Vec::with_capacity(paths.len());
    for path in paths {
        let full = git.root().join(path);
        match fs::symlink_metadata(&full) {
            Ok(_) => present.push(path.clone()),
            Err(error) if is_not_found(&error) => {}
            Err(error) => return Err(ReadFileError::caused_by(&full, error).into()),
        }
    }
    Ok(present)
}

/// Selects one package's released content from the tracked paths beneath it.
///
/// `tracked` decides eligibility and `present` decides structure: which
/// directories draw a nested package boundary, and whether a default README is
/// there to detect. The two coincide at a commit and differ in a work tree that
/// has deleted a tracked file.
///
/// Keys are package-relative, values are the git-root-relative paths the caller
/// reads the content back from.
fn released_from_paths(
    tracked: &[String],
    present: &[String],
    side: &PackageSide<'_>,
) -> HashMap<String, String> {
    let nested = nested_package_dirs(present, side.dir);
    let mut map = HashMap::new();
    for full in tracked {
        if is_inside_any(full, &nested) {
            continue;
        }
        let Some(rel) = relativize(full, side.dir) else {
            continue;
        };
        if side.rules.is_released(rel) {
            map.insert(rel.to_string(), full.clone());
        }
    }
    if side.auto_readme {
        let present: HashSet<&str> = present.iter().map(String::as_str).collect();
        if let Some((name, full)) = detected_readme(side.dir, &present, side.case) {
            map.entry(name).or_insert(full);
        }
    }
    map
}

/// The default README this end holds for `dir`.
///
/// Keyed by the name it takes in the package archive.
///
/// Cargo probes the package directory for its default names in order and packs
/// the first that exists without consulting `include` or `exclude`, so a package
/// that names no README still releases the one beside it. Detection runs over
/// the same tracked listing the rest of the comparison uses, because released
/// content is defined from git-tracked files. Cargo's probe goes through the
/// filesystem, so on a case-insensitive volume a tracked `readme.md` answers the
/// `README.md` candidate and the probed case rules decide the match; the key is
/// the tracked spelling, which keeps a re-spelling of the file visible as the
/// content change it is.
/// Ref: docs/design.md, "Released content".
fn detected_readme(dir: &str, present: &HashSet<&str>, case: PathCase) -> Option<(String, String)> {
    DEFAULT_README_FILES.iter().find_map(|name| {
        let candidate = join_relative(dir, name)?;
        let full = match case {
            PathCase::Sensitive => present.contains(candidate.as_str()).then_some(candidate),
            // A volume cannot hold two spellings that differ only in case, so at
            // most one entry can match; `min` only keeps the scan deterministic.
            PathCase::Insensitive => present
                .iter()
                .filter(|held| case.same_path(held, &candidate))
                .min()
                .map(|held| (*held).to_string()),
        }?;
        let rel = relativize(&full, dir)?.to_string();
        Some((rel, full))
    })
}

/// Package directories nested strictly inside `dir`, read off the tracked paths.
///
/// `cargo package` stops at a nested package boundary: a directory beneath the
/// package that carries its own `Cargo.toml` contributes nothing to the outer
/// package's released content, and Cargo applies that regardless of workspace
/// membership or of an explicit `include`. Reading the boundaries off the
/// manifests tracked on the side being examined therefore matches Cargo, where
/// reading them off the member list would attribute the files of an excluded or
/// otherwise non-member nested package to the outer package and report changes it
/// will never release.
fn nested_package_dirs(paths: &[String], dir: &str) -> Vec<String> {
    let prefix = if dir.is_empty() {
        String::new()
    } else {
        format!("{dir}/")
    };
    paths
        .iter()
        .filter_map(|path| {
            let (parent, file) = path.rsplit_once('/')?;
            (file == "Cargo.toml").then_some(parent)
        })
        .filter(|parent| *parent != dir && parent.starts_with(&prefix))
        .map(ToOwned::to_owned)
        .collect()
}

fn is_inside_any(path: &str, dirs: &[String]) -> bool {
    dirs.iter().any(|dir| {
        path.strip_prefix(dir.as_str())
            .is_some_and(|rest| rest.starts_with('/'))
    })
}

/// Package facts reconstructed from a historical tree, keyed by package name.
#[derive(Clone, Debug)]
/// One publishable member as it existed at a historical commit.
struct HistoricalPackage {
    directory: String,
    version: Version,
    packaging: PackagingRules,
    /// Files Cargo packs because a manifest key names them.
    ///
    /// Keyed by the path each takes inside the package archive.
    resources: BTreeMap<String, String>,
    /// Whether Cargo picks this package's README by probing its directory.
    auto_readme: bool,
    /// Whether an installable binary makes this endpoint's closure relevant.
    has_lockfile_target: bool,
}

/// Workspace members and root manifest at one commit.
#[derive(Clone, Debug)]
struct CommitSnapshot {
    packages: BTreeMap<String, HistoricalPackage>,
    /// Members that declared `publish = false` at this commit.
    ///
    /// Nothing could be released from them, so they carry no packaging facts,
    /// but the anchor walk still has to tell "withdrawn here" from "absent
    /// here". Ref: docs/implementation.md, "Anchor and change set".
    unpublished: BTreeSet<String>,
    root_doc: DocumentMut,
    installation: InstallationGraph,
}

/// Cache of [`CommitSnapshot`] values so a first-parent walk does not re-parse.
struct SnapshotCache {
    inner: HashMap<String, Rc<CommitSnapshot>>,
    case: PathCase,
    registries: BTreeMap<String, String>,
}

impl SnapshotCache {
    /// Probes the work tree once; every snapshot matches members the same way.
    fn new(workspace_root: &Path, registries: BTreeMap<String, String>) -> Self {
        Self {
            inner: HashMap::new(),
            case: PathCase::probe(workspace_root),
            registries,
        }
    }

    /// The probed case rules, shared by member matching and README detection.
    fn case(&self) -> PathCase {
        self.case
    }

    fn snapshot(&mut self, git: &GitRepo, commit: &str) -> Result<Rc<CommitSnapshot>, AppError> {
        if let Some(existing) = self.inner.get(commit) {
            return Ok(Rc::clone(existing));
        }
        let built = Rc::new(load_snapshot(git, commit, self.case, &self.registries)?);
        self.inner.insert(commit.to_string(), Rc::clone(&built));
        Ok(built)
    }
}

fn load_snapshot(
    git: &GitRepo,
    commit: &str,
    case: PathCase,
    registries: &BTreeMap<String, String>,
) -> Result<CommitSnapshot, AppError> {
    let root_rel = root_manifest_rel(git);
    // History before the workspace existed has no root manifest. An empty
    // `[workspace]` reproduces that state exactly: no members, so every current
    // package is absent from the snapshot and classified as newly created.
    let root_content = git
        .show_file(commit, &root_rel)?
        .unwrap_or_else(|| "[workspace]\n".to_string());
    let root_doc = parse_document(Path::new(&root_rel), &root_content)?;
    let members = parse_workspace_members(&root_content, Path::new(&root_rel), case)?;
    // `members` globs are written relative to the workspace root, which need not be
    // the git root, while Git yields git-root-relative paths. Rebase before
    // matching, or a nested workspace would find no members and silently classify
    // every package as absent from the base revision.
    let workspace_prefix = root_rel.strip_suffix(MANIFEST_FILE_NAME).unwrap_or("");
    let workspace = WorkspaceInherit::from_root(&root_doc);
    let tree_paths = git.ls_tree_paths(commit)?;

    let mut manifest_paths: BTreeMap<String, String> = BTreeMap::new();
    for path in tree_paths
        .iter()
        .filter(|path| path.rsplit('/').next() == Some(MANIFEST_FILE_NAME))
    {
        let dir = path.rsplit_once('/').map_or("", |(dir, _)| dir);
        let member_dir = workspace_relative_dir(dir, workspace_prefix);
        manifest_paths.insert(member_dir, path.clone());
    }

    let mut manifests = GitManifestSource {
        git,
        commit,
        workspace_prefix,
        workspace,
        paths: manifest_paths,
        parsed: BTreeMap::new(),
    };
    let member_dirs = resolve_members(&mut manifests, &members)?;
    let mut packages = BTreeMap::new();
    let mut unpublished = BTreeSet::new();
    let mut installation = InstallationGraph::default();
    let mut path_identities = BTreeMap::new();
    for member_dir in &member_dirs {
        let Some(parsed) = manifests.manifest(member_dir)? else {
            continue;
        };
        installation.insert(
            parsed.name.clone(),
            parsed.version.clone(),
            parsed.installation_dependencies.clone(),
        );
        path_identities.insert(
            join_git_rel(&parsed.directory, MANIFEST_FILE_NAME),
            Some(parsed.identity()),
        );
        if !parsed.publish {
            unpublished.insert(parsed.name.clone());
            continue;
        }
        packages.insert(
            parsed.name.clone(),
            HistoricalPackage {
                directory: parsed.directory.clone(),
                version: parsed.version.clone(),
                packaging: parsed.packaging.clone(),
                resources: resolve_resources(parsed, &parsed.directory, workspace_prefix),
                auto_readme: parsed.auto_readme,
                has_lockfile_target: parsed.targets.has_lockfile_target(
                    tree_paths
                        .iter()
                        .filter_map(|path| relativize(path, &parsed.directory)),
                    case,
                ),
            },
        );
    }
    if packages.values().any(|package| package.has_lockfile_target) {
        match historical_registries(git, commit, registries, &tree_paths) {
            Ok(registries) => installation.registries = registries,
            Err(error) => installation.registry_error = Some(installation_error(error)),
        }
        installation.patches = installation_patches(&root_doc);
        resolve_historical_installation_paths(
            &mut installation,
            path_identities,
            git,
            commit,
            &tree_paths,
            case,
        );
    }
    Ok(CommitSnapshot {
        packages,
        unpublished,
        root_doc,
        installation,
    })
}

fn resolve_historical_installation_paths(
    installation: &mut InstallationGraph,
    mut identities: BTreeMap<String, Option<PackageIdentity>>,
    git: &GitRepo,
    commit: &str,
    tree_paths: &[String],
    case: PathCase,
) {
    let mut documents = BTreeMap::<String, Option<DocumentMut>>::new();
    installation.resolve_paths(|reference| {
        let Some(directory) = reference.directory(git.root(), git.prefix(), "") else {
            return Ok(None);
        };
        let path = join_git_rel(&directory, MANIFEST_FILE_NAME);
        if let Some((_, identity)) = identities
            .iter()
            .find(|(candidate, _)| case.same_path(candidate, &path))
        {
            return Ok(identity.clone());
        }
        let identity = path_package_identity(&path, case, |path| {
            let Some(path) = tree_paths
                .iter()
                .find(|candidate| case.same_path(candidate, path))
            else {
                return Ok(None);
            };
            if let Some(document) = documents.get(path) {
                return Ok(document.clone());
            }
            let document = git
                .show_file(commit, path)?
                .map(|content| parse_document(Path::new(path), &content))
                .transpose()?;
            documents.insert(path.clone(), document.clone());
            Ok(document)
        })?;
        identities.insert(path, identity.clone());
        Ok(identity)
    });
}

/// Reconstructs tracked registry configuration over Cargo's ambient configuration.
///
/// Cargo loads ancestor configurations from outermost to innermost and prefers
/// the extensionless filename when both names exist in the same directory.
fn historical_registries(
    git: &GitRepo,
    commit: &str,
    ambient: &BTreeMap<String, String>,
    tree_paths: &[String],
) -> Result<BTreeMap<String, String>, AppError> {
    let mut registries = ambient.clone();
    for candidates in cargo_config_paths(git.prefix()) {
        for path in candidates {
            if !tree_paths.contains(&path) {
                continue;
            }
            let Some(content) = git.show_file(commit, &path)? else {
                continue;
            };
            let doc = parse_document(Path::new(&path), &content)?;
            collect_registry_indices(&doc, &mut registries);
            break;
        }
    }
    Ok(registries)
}

/// Supplies historical member manifests to membership resolution.
///
/// Cargo loads only the manifests that can become members of the selected
/// workspace, so resolution pulls manifests through this abstraction instead of
/// parsing every `Cargo.toml` in the tree up front. An unrelated or excluded
/// nested workspace holding a manifest Cargo would never read must not be able
/// to fail classification. Ref: docs/implementation.md, "Classification".
trait ManifestSource {
    /// Workspace-relative directories that hold a manifest, none of them parsed yet.
    fn candidate_dirs(&self) -> Vec<String>;

    /// The parsed manifest in `dir`, or `None` when `dir` holds no package.
    fn manifest(&mut self, dir: &str) -> Result<Option<&PackageManifest>, AppError>;

    /// Directories the path dependencies of the manifest in `dir` point at.
    ///
    /// Locally declared paths resolve against the member's own directory;
    /// inherited ones are declared in `[workspace.dependencies]` and so resolve
    /// against the workspace root. A path climbing above the workspace root
    /// names nothing inside it and is dropped. The result is owned rather than
    /// borrowed because following an edge parses further manifests through this
    /// same source.
    fn path_edges(&mut self, dir: &str) -> Result<Vec<String>, AppError> {
        let Some(parsed) = self.manifest(dir)? else {
            return Ok(Vec::new());
        };
        Ok(parsed
            .path_dependencies
            .iter()
            .map(|relative| join_relative(dir, relative))
            .chain(
                parsed
                    .inherited_path_dependencies
                    .iter()
                    .map(|relative| join_relative("", relative)),
            )
            .flatten()
            .collect())
    }
}

/// Reads member manifests out of one commit, parsing each at most once.
struct GitManifestSource<'a> {
    git: &'a GitRepo,
    commit: &'a str,
    workspace_prefix: &'a str,
    workspace: WorkspaceInherit<'a>,
    /// Git-root-relative manifest path of every candidate directory.
    paths: BTreeMap<String, String>,
    parsed: BTreeMap<String, Option<PackageManifest>>,
}

impl ManifestSource for GitManifestSource<'_> {
    fn candidate_dirs(&self) -> Vec<String> {
        self.paths.keys().cloned().collect()
    }

    fn manifest(&mut self, dir: &str) -> Result<Option<&PackageManifest>, AppError> {
        if !self.parsed.contains_key(dir) {
            let parsed = match self.paths.get(dir) {
                Some(path) => match self.git.show_file(self.commit, path)? {
                    Some(content) => parse_package_manifest(&content, path, &self.workspace)?,
                    None => None,
                },
                None => None,
            };
            self.parsed.insert(dir.to_string(), parsed);
        }
        Ok(self.parsed.get(dir).and_then(Option::as_ref))
    }

    fn path_edges(&mut self, dir: &str) -> Result<Vec<String>, AppError> {
        let workspace_prefix = self.workspace_prefix.trim_end_matches('/');
        let Some(parsed) = self.manifest(dir)? else {
            return Ok(Vec::new());
        };
        let package_dir = parsed.directory.clone();
        let local = parsed.path_dependencies.clone();
        let inherited = parsed.inherited_path_dependencies.clone();
        Ok(local
            .iter()
            .map(|relative| join_relative(&package_dir, relative))
            .chain(
                inherited
                    .iter()
                    .map(|relative| join_relative(workspace_prefix, relative)),
            )
            .flatten()
            .map(|target| workspace_relative_dir(&target, workspace_prefix))
            // Cargo implicitly adds path dependencies only when they live below
            // the workspace root. Explicit member patterns add outside members.
            .filter(|target| target != ".." && !target.starts_with("../"))
            .collect())
    }
}

/// Reconstructs the workspace-relative directories Cargo would treat as members.
///
/// Beyond the declared `members` patterns, Cargo makes every path dependency of
/// a member that lives inside the workspace a member too, so the closure is
/// followed until it stops growing. Membership derived this way still honours
/// `exclude`.
fn resolve_members(
    manifests: &mut dyn ManifestSource,
    members: &WorkspaceMembers,
) -> Result<BTreeSet<String>, AppError> {
    let mut resolved: BTreeSet<String> = manifests
        .candidate_dirs()
        .into_iter()
        .filter(|dir| is_workspace_member(dir, members))
        .collect();
    let mut pending: Vec<String> = resolved.iter().cloned().collect();
    while let Some(dir) = pending.pop() {
        for target in manifests.path_edges(&dir)? {
            if is_workspace_excluded(&target, members) {
                continue;
            }
            if manifests.manifest(&target)?.is_none() {
                continue;
            }
            if resolved.insert(target.clone()) {
                pending.push(target);
            }
        }
    }
    Ok(resolved)
}

/// Resolves a manifest-relative path against a workspace-relative directory.
///
/// Returns `None` when the path climbs above the workspace root, because such a
/// dependency is outside the workspace and therefore not an implicit member.
fn join_relative(base: &str, relative: &str) -> Option<String> {
    let relative = to_git_separators(relative, MAIN_SEPARATOR);
    let mut segments: Vec<&str> = if base.is_empty() {
        Vec::new()
    } else {
        base.split('/').collect()
    };
    for segment in relative.split('/') {
        match segment {
            "" | "." => {}
            ".." => {
                segments.pop()?;
            }
            other => segments.push(other),
        }
    }
    Some(segments.join("/"))
}

/// Parsed lockfiles shared by binary package classifications.
///
/// The work-tree endpoint is common to every package, while packages that share
/// an anchor commit also share its historical endpoint. Retaining both avoids
/// reparsing workspace-sized lockfiles for every binary package.
/// Ref: docs/implementation.md, "Lockfile closures".
#[derive(Debug, Default)]
struct LockfileCache {
    work: Option<Lockfile>,
    anchors: HashMap<String, Lockfile>,
}

impl LockfileCache {
    fn anchor<'a>(
        &'a mut self,
        git: &GitRepo,
        name: &str,
        commit: &str,
        path: &str,
    ) -> Result<&'a Lockfile, AppError> {
        if !self.anchors.contains_key(commit) {
            let Some(bytes) = git.show_file_bytes(commit, path)? else {
                return Err(LockfileClosureUnavailableError::new(
                    name,
                    "the anchor commit does not track a workspace Cargo.lock",
                )
                .into());
            };
            let lockfile = Lockfile::parse(&decode_lockfile(bytes, path)?, path)?;
            self.anchors.insert(commit.to_owned(), lockfile);
        }
        Ok(self
            .anchors
            .get(commit)
            .expect("the requested anchor lockfile was inserted above when absent"))
    }

    fn work<'a>(
        &'a mut self,
        work_tree: &WorkTree,
        name: &str,
        git_path: &str,
    ) -> Result<&'a Lockfile, AppError> {
        if self.work.is_none() {
            let work_path = work_tree.workspace_root.join(LOCKFILE_FILE_NAME);
            let Some(bytes) = read_optional_bytes(&work_path, name, git_path)? else {
                return Err(LockfileClosureUnavailableError::new(
                    name,
                    "the work tree has no workspace Cargo.lock; restore or refresh Cargo.lock",
                )
                .into());
            };
            self.work = Some(Lockfile::parse(
                &decode_lockfile(bytes, git_path)?,
                git_path,
            )?);
        }
        Ok(self
            .work
            .as_ref()
            .expect("the work-tree lockfile was initialized above when absent"))
    }
}

/// Dependencies whose locked identity changed between the anchor and the work tree.
///
/// Each endpoint that has an installable binary target must have a lockfile that
/// resolves the package at its corresponding declared version. An endpoint without
/// that target releases no closure and therefore contributes an empty closure.
/// Ref: docs/design.md, "Relevant lockfile closures".
#[expect(
    clippy::too_many_arguments,
    reason = "each endpoint supplies its target, lockfile and installation declarations"
)]
fn lockfile_closure_changes(
    cache: &mut LockfileCache,
    git: &GitRepo,
    work_tree: &WorkTree,
    name: &str,
    anchor_package: &HistoricalPackage,
    work_package: &WorkPackage,
    anchor_commit: &str,
    anchor_installation: &InstallationGraph,
) -> Result<Vec<(String, ClosureChange)>, AppError> {
    let git_path = join_git_rel(git.prefix(), LOCKFILE_FILE_NAME);
    let anchor = if anchor_package.has_lockfile_target {
        let lockfile = cache.anchor(git, name, anchor_commit, &git_path)?;
        let Some(closure) = lockfile.closure(
            name,
            &anchor_package.version.to_string(),
            anchor_installation,
        )?
        else {
            return Err(LockfileClosureUnavailableError::new(
                name,
                "the anchor Cargo.lock does not identify an installation closure at the declared version and configured sources",
            )
            .into());
        };
        closure
    } else {
        Closure::new()
    };
    let work = if work_package.has_lockfile_target {
        let lockfile = cache.work(work_tree, name, &git_path)?;
        let Some(closure) = lockfile.closure(
            name,
            &work_package.manifest.version.to_string(),
            &work_tree.installation,
        )?
        else {
            return Err(LockfileClosureUnavailableError::new(
                name,
                "the work-tree Cargo.lock does not identify an installation closure at the declared version and configured sources; refresh Cargo.lock",
            )
            .into());
        };
        closure
    } else {
        Closure::new()
    };
    Ok(closure_changes(&anchor, &work))
}

/// Reads lockfile bytes as text, naming `path` if they are not text at all.
fn decode_lockfile(bytes: Vec<u8>, path: &str) -> Result<String, AppError> {
    String::from_utf8(bytes).map_err(|error| MalformedLockfileError::caused_by(path, error).into())
}

fn log_untracked(verbose: Verbose, name: &str, count: usize) {
    if count == 0 {
        return;
    }
    verbose.note(|| {
        format!(
            "{}: {} match packaging rules and are advisory only; released content is defined as \
         the git-tracked files under the package, so untracked paths are never counted as changes \
         even where Cargo would pack them (an untracked nested manifest therefore does not draw a \
         package boundary either; the optional packaging probe warns about that divergence)",
            quote_path(name),
            plural(count, "untracked path")
        )
    });
}

// Early-exit is equivalent to walking the rest of first-parent history.
#[cfg_attr(test, mutants::skip)]
fn can_stop_timeline(timeline: &[TimelineEntry]) -> bool {
    let [.., last] = timeline else {
        return false;
    };
    // The anchor walk starts at the newest commit that released the package: the
    // base itself when the base releases it, the reintroduction point otherwise.
    // Until an older commit declares a different version the anchor is still
    // undetermined, and a package no commit has released yet needs the whole
    // history to tell creation from truncation.
    let Some(reference) = timeline
        .iter()
        .find_map(|entry| entry.presence.released_version())
    else {
        return false;
    };
    // A commit at which the package was withdrawn is invisible to the walk, so
    // it can never be the change that ends it. An absent package is visible: its
    // reappearance is itself a version change.
    !last.presence.is_unpublished() && last.presence.released_version() != Some(reference)
}

/// Reads a tracked work-tree path the way Cargo would pack it.
///
/// A symbolic link stops the run rather than being compared. Cargo dereferences
/// a link when it builds a package archive, while Git stores the link as a blob holding
/// the target path, so neither reading the target text nor following the link
/// yields a comparison that is right at both ends. Ref: docs/design.md,
/// "Released content".
fn read_optional_bytes(path: &Path, name: &str, rel: &str) -> Result<Option<Vec<u8>>, AppError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            return Err(SymlinkReleasedError::new(name, rel).into());
        }
        Ok(_) => {}
        Err(error) if is_not_found(&error) => return Ok(None),
        Err(error) => return Err(ReadFileError::caused_by(path, error).into()),
    }
    match fs::read(path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(error) if is_not_found(&error) => Ok(None),
        Err(error) => Err(ReadFileError::caused_by(path, error).into()),
    }
}

fn is_not_found(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::NotFound
}

/// Git-root-relative path of the workspace root manifest.
///
/// The prefix comes from Git rather than from subtracting Cargo's
/// `workspace_root` from the repository root, because the two tools can spell
/// the same directory differently and a failed subtraction would silently pick
/// the repository-root manifest for a nested workspace. Ref:
/// docs/implementation.md, "Classification".
fn root_manifest_rel(git: &GitRepo) -> String {
    let prefix = git.prefix();
    if prefix.is_empty() {
        "Cargo.toml".to_string()
    } else {
        format!("{prefix}/Cargo.toml")
    }
}

/// Rebases a git-root-relative directory onto the workspace root.
///
/// `workspace_prefix` is the git-root-relative workspace directory with a trailing
/// separator, or empty when the workspace root is the git root. Leading parent
/// components preserve members that use `[package] workspace` from beside a
/// nested workspace root.
fn workspace_relative_dir(dir: &str, workspace_prefix: &str) -> String {
    let workspace: Vec<&str> = workspace_prefix
        .trim_matches('/')
        .split('/')
        .filter(|component| !component.is_empty())
        .collect();
    let directory: Vec<&str> = dir
        .trim_matches('/')
        .split('/')
        .filter(|component| !component.is_empty())
        .collect();
    let common = workspace
        .iter()
        .zip(&directory)
        .take_while(|(left, right)| left == right)
        .count();
    let mut relative = vec![".."; workspace.len().saturating_sub(common)];
    relative.extend(directory.iter().skip(common).copied());
    relative.join("/")
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    #[cfg(unix)]
    use std::os::unix::fs::symlink;

    use tempfile::tempdir;

    use super::*;
    use crate::inherited::InheritedKeys;
    use crate::manifest::{InstallationDependencies, TargetDiscovery};

    #[test]
    fn status_requires_consistent_anchor_version_and_change_evidence() {
        let anchor = Version::new(1, 0, 0);
        for (declared, previous, changed, expected) in [
            (
                Version::new(1, 0, 0),
                None,
                false,
                Some(PackageStatus::PendingRelease),
            ),
            (Version::new(1, 0, 0), None, true, None),
            (
                Version::new(1, 0, 0),
                Some(&anchor),
                false,
                Some(PackageStatus::Unchanged),
            ),
            (
                Version::new(1, 0, 0),
                Some(&anchor),
                true,
                Some(PackageStatus::NeedsIncrement),
            ),
            (
                Version::new(1, 0, 1),
                Some(&anchor),
                false,
                Some(PackageStatus::PendingRelease),
            ),
            (
                Version::new(1, 0, 1),
                Some(&anchor),
                true,
                Some(PackageStatus::PendingRelease),
            ),
            (Version::new(0, 9, 0), Some(&anchor), false, None),
            (Version::new(0, 9, 0), Some(&anchor), true, None),
        ] {
            assert_eq!(
                PackageStatus::from_evidence(&declared, previous, changed),
                expected
            );
        }
    }

    #[test]
    fn workspace_relative_dir_rebases_onto_the_workspace_root() {
        // Workspace root is the git root: paths pass through unchanged.
        assert_eq!(workspace_relative_dir("packages/a", ""), "packages/a");
        assert_eq!(workspace_relative_dir("", ""), "");

        // Workspace root is nested: the prefix is stripped so member globs match.
        assert_eq!(
            workspace_relative_dir("rust/packages/a", "rust/"),
            "packages/a"
        );
        assert_eq!(workspace_relative_dir("rust", "rust/"), "");

        // Explicit members beside a nested root retain their parent traversal.
        assert_eq!(
            workspace_relative_dir("dotnet/packages/a", "rust/"),
            "../dotnet/packages/a"
        );
        assert_eq!(workspace_relative_dir("", "rust/"), "..");
    }

    #[test]
    fn only_a_package_the_base_never_carried_is_new_on_it() {
        let mut base = CommitSnapshot {
            packages: BTreeMap::new(),
            unpublished: BTreeSet::new(),
            root_doc: DocumentMut::new(),
            installation: InstallationGraph::default(),
        };
        base.packages.insert(
            "released".to_string(),
            HistoricalPackage {
                directory: "packages/released".to_string(),
                version: Version::new(0, 1, 0),
                packaging: PackagingRules::default(),
                resources: BTreeMap::new(),
                auto_readme: false,
                has_lockfile_target: false,
            },
        );
        base.unpublished.insert("withdrawn".to_string());

        assert!(!is_new_on_base(&base, "released"));
        // Withdrawn on the base is still present on it, so the group binds it.
        assert!(!is_new_on_base(&base, "withdrawn"));
        assert!(is_new_on_base(&base, "added"));
    }

    #[test]
    fn every_changed_item_source_serializes_its_payload() {
        let items = [
            ChangedItem::Package {
                path: "src/lib.rs".to_string(),
                change: "modified".to_string(),
            },
            ChangedItem::Inherited {
                field: "workspace.package.license".to_string(),
            },
            ChangedItem::Lockfile {
                dependency: "serde".to_string(),
                change: "modified".to_string(),
            },
        ];
        let values: Vec<serde_json::Value> = items
            .iter()
            .map(|item| serde_json::to_value(item).unwrap())
            .collect();

        let package = values.first().unwrap();
        let inherited = values.get(1).unwrap();
        let lockfile = values.get(2).unwrap();
        assert_eq!(package.get("source").unwrap(), "package");
        assert_eq!(package.get("path").unwrap(), "src/lib.rs");
        assert_eq!(inherited.get("source").unwrap(), "inherited");
        assert_eq!(inherited.get("field").unwrap(), "workspace.package.license");
        assert_eq!(lockfile.get("source").unwrap(), "lockfile");
        assert_eq!(lockfile.get("dependency").unwrap(), "serde");
    }

    #[test]
    fn is_not_found_matches_not_found_kind() {
        assert!(is_not_found(&io::Error::new(
            io::ErrorKind::NotFound,
            "missing",
        )));
        assert!(!is_not_found(&io::Error::new(
            io::ErrorKind::PermissionDenied,
            "denied",
        )));
    }

    #[cfg_attr(miri, ignore)] // tempfile::tempdir is host filesystem, which Miri cannot emulate.
    #[test]
    fn read_optional_bytes_missing_is_none() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("gone.txt");
        assert_eq!(read_optional_bytes(&path, "pkg", "gone.txt").unwrap(), None);
    }

    #[cfg_attr(miri, ignore)] // tempfile::tempdir is host filesystem, which Miri cannot emulate.
    #[test]
    fn read_optional_bytes_reads_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("here.txt");
        fs::write(&path, "hi").unwrap();
        assert_eq!(
            read_optional_bytes(&path, "pkg", "here.txt")
                .unwrap()
                .as_deref(),
            Some(b"hi".as_slice())
        );
    }

    #[cfg_attr(miri, ignore)] // tempfile::tempdir is host filesystem, which Miri cannot emulate.
    #[test]
    fn read_optional_bytes_rejects_non_not_found_errors() {
        let dir = tempdir().unwrap();
        let _ = read_optional_bytes(dir.path(), "pkg", "dir").unwrap_err();
    }

    /// Read optional bytes rejects a symbolic link.
    ///
    /// A link cannot be compared against history, because Cargo would pack the target's bytes while
    /// Git stores the target's path.
    #[cfg(unix)]
    #[cfg_attr(miri, ignore)] // tempfile::tempdir is host filesystem, which Miri cannot emulate.
    #[test]
    fn read_optional_bytes_rejects_a_symbolic_link() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("real.txt"), "hi").unwrap();
        let link = dir.path().join("link.txt");
        symlink("real.txt", &link).unwrap();

        let error = read_optional_bytes(&link, "pkg", "a/link.txt").unwrap_err();
        let reported = error
            .find_source::<SymlinkReleasedError>()
            .expect("a link is refused with the dedicated condition")
            .to_string();
        assert!(reported.contains("a/link.txt"), "{reported}");
        assert!(reported.contains("pkg"), "{reported}");
    }

    #[test]
    fn join_relative_resolves_against_the_member_directory() {
        assert_eq!(
            join_relative("packages/a", "../b"),
            Some("packages/b".to_string())
        );
        assert_eq!(
            join_relative("packages/a", "./nested/"),
            Some("packages/a/nested".to_string())
        );
        assert_eq!(join_relative("", "vendored"), Some("vendored".to_string()));
        // Cargo resolves a manifest-declared path with the host's own rules, so
        // a backslash separates components only where the platform says it does
        // and is an ordinary file name character everywhere else.
        let expected = if MAIN_SEPARATOR == '\\' {
            "packages/b".to_string()
        } else {
            r"packages/a/..\b".to_string()
        };
        assert_eq!(join_relative("packages/a", r"..\b"), Some(expected));
        // Climbing above the workspace root leaves the workspace entirely.
        assert_eq!(join_relative("packages", "../../outside"), None);
    }

    #[test]
    fn nested_package_dirs_selects_strict_descendants() {
        let paths = vec![
            "Cargo.toml".to_string(),
            "packages/a/Cargo.toml".to_string(),
            "packages/a/src/lib.rs".to_string(),
            "packages/a/inner/Cargo.toml".to_string(),
            "packages/ab/Cargo.toml".to_string(),
        ];
        assert_eq!(
            nested_package_dirs(&paths, "packages/a"),
            vec!["packages/a/inner".to_string()]
        );
        assert_eq!(
            nested_package_dirs(&paths, "packages/a/inner"),
            Vec::<String>::new()
        );
        // A root package contains every other manifest.
        assert_eq!(
            nested_package_dirs(&paths, ""),
            vec![
                "packages/a".to_string(),
                "packages/a/inner".to_string(),
                "packages/ab".to_string(),
            ]
        );
    }

    #[test]
    fn nested_package_dirs_ignores_files_that_merely_end_in_the_manifest_name() {
        let paths = vec![
            "packages/a/Cargo.toml".to_string(),
            "packages/a/inner/NotCargo.toml".to_string(),
            "packages/a/inner/Cargo.toml.bak".to_string(),
        ];
        assert_eq!(
            nested_package_dirs(&paths, "packages/a"),
            Vec::<String>::new()
        );
    }

    #[test]
    fn released_from_paths_stops_at_a_nested_manifest() {
        let rules = PackagingRules::default();
        let resources = BTreeMap::new();
        let side = PackageSide {
            dir: "packages/a",
            rules: &rules,
            resources: &resources,
            auto_readme: false,
            case: PathCase::Sensitive,
        };
        // `fixture` is a package of its own, so Cargo packs none of its files with
        // `packages/a` even though the workspace never lists it as a member.
        let paths = vec![
            "packages/a/Cargo.toml".to_string(),
            "packages/a/src/lib.rs".to_string(),
            "packages/a/fixture/Cargo.toml".to_string(),
            "packages/a/fixture/src/lib.rs".to_string(),
        ];
        let released = released_from_paths(&paths, &paths, &side);
        assert_eq!(
            released.keys().map(String::as_str).collect::<BTreeSet<_>>(),
            BTreeSet::from(["Cargo.toml", "src/lib.rs"])
        );
        assert_eq!(released.get("Cargo.toml").unwrap(), "packages/a/Cargo.toml");
    }

    /// A backslash in a reported path is not a directory boundary.
    ///
    /// Git's `-z` listings separate directories with `/` on every platform, so a `\` in a reported
    /// path belongs to a file's name. Rewriting it would file the content under a directory that
    /// does not exist and, worse, could make two distinct files collide on one package-relative
    /// key.
    #[test]
    fn a_backslash_in_a_reported_path_is_not_a_directory_boundary() {
        let rules = PackagingRules::default();
        let resources = BTreeMap::new();
        let side = PackageSide {
            dir: "packages/a",
            rules: &rules,
            resources: &resources,
            auto_readme: false,
            case: PathCase::Sensitive,
        };
        let paths = vec![
            "packages/a/Cargo.toml".to_string(),
            r"packages/a/src/odd\name.rs".to_string(),
        ];
        let released = released_from_paths(&paths, &paths, &side);
        assert_eq!(
            released.keys().map(String::as_str).collect::<BTreeSet<_>>(),
            BTreeSet::from(["Cargo.toml", r"src/odd\name.rs"])
        );
        assert_eq!(
            released.get(r"src/odd\name.rs").unwrap(),
            r"packages/a/src/odd\name.rs"
        );
    }

    /// A detected readme outranks the packaging rules.
    ///
    /// Cargo packs the README it detects itself even when `include` omits it, and prefers the first
    /// of its default names that the end being examined holds.
    #[test]
    fn a_detected_readme_outranks_the_packaging_rules() {
        let rules = PackagingRules::new(Some(&["/src/".to_string()]), None).unwrap();
        let resources = BTreeMap::new();
        let paths = vec![
            "packages/a/src/lib.rs".to_string(),
            "packages/a/README.md".to_string(),
            "packages/a/README.txt".to_string(),
        ];
        let side = PackageSide {
            dir: "packages/a",
            rules: &rules,
            resources: &resources,
            auto_readme: true,
            case: PathCase::Sensitive,
        };

        let released = released_from_paths(&paths, &paths, &side);
        assert_eq!(
            released.keys().map(String::as_str).collect::<BTreeSet<_>>(),
            BTreeSet::from(["src/lib.rs", "README.md"])
        );

        // A package that names its README, or disables the key, gets no detection.
        let declared = PackageSide {
            auto_readme: false,
            ..side
        };
        assert_eq!(
            released_from_paths(&paths, &paths, &declared)
                .keys()
                .map(String::as_str)
                .collect::<BTreeSet<_>>(),
            BTreeSet::from(["src/lib.rs"])
        );
    }

    /// A detected readme follows the probed case rules.
    ///
    /// Cargo probes the filesystem for its default README names, so on a case-insensitive volume a
    /// tracked `readme.md` answers the `README.md` candidate and its content is released. Matching
    /// the spelling exactly there would report such a package as having released nothing.
    #[test]
    fn a_detected_readme_follows_the_probed_case_rules() {
        let rules = PackagingRules::new(Some(&["/src/".to_string()]), None).unwrap();
        let resources = BTreeMap::new();
        let paths = vec![
            "packages/a/src/lib.rs".to_string(),
            "packages/a/readme.md".to_string(),
        ];
        let strict = PackageSide {
            dir: "packages/a",
            rules: &rules,
            resources: &resources,
            auto_readme: true,
            case: PathCase::Sensitive,
        };
        assert_eq!(
            released_from_paths(&paths, &paths, &strict)
                .keys()
                .map(String::as_str)
                .collect::<BTreeSet<_>>(),
            BTreeSet::from(["src/lib.rs"])
        );

        let relaxed = PackageSide {
            case: PathCase::Insensitive,
            ..strict
        };
        let released = released_from_paths(&paths, &paths, &relaxed);
        assert_eq!(
            released.keys().map(String::as_str).collect::<BTreeSet<_>>(),
            BTreeSet::from(["src/lib.rs", "readme.md"])
        );
        // Keying by the tracked spelling keeps a re-spelling of the file
        // visible as the released-content change it is.
        assert_eq!(released.get("readme.md").unwrap(), "packages/a/readme.md");
    }

    /// A declared resource follows the probed case rules while retaining Git's spelling.
    ///
    /// Cargo opens the manifest's spelling through the filesystem. Git path and
    /// blob lookups must use the recorded spelling after deciding that both name
    /// the same resource.
    #[test]
    fn a_declared_resource_follows_the_probed_case_rules() {
        let rules = PackagingRules::default();
        let resources =
            BTreeMap::from([("README.md".to_string(), "packages/a/README.md".to_string())]);
        let tracked = vec!["packages/a/readme.md".to_string()];
        let strict = PackageSide {
            dir: "packages/a",
            rules: &rules,
            resources: &resources,
            auto_readme: false,
            case: PathCase::Sensitive,
        };
        assert!(tracked_resources(&strict, &tracked).is_empty());

        let relaxed = PackageSide {
            case: PathCase::Insensitive,
            ..strict
        };
        assert_eq!(
            tracked_resources(&relaxed, &tracked),
            BTreeMap::from([("README.md".to_string(), "packages/a/readme.md".to_string())])
        );
    }

    /// A deleted default README is no longer detected.
    ///
    /// Git still lists a tracked file the work tree has deleted, but Cargo packages what is on
    /// disk, so README detection must use the paths that remain present.
    #[test]
    fn a_deleted_readme_is_no_longer_detected() {
        let resources = BTreeMap::new();
        // `include` omits the README, so only detection can bring it in.
        let rules = PackagingRules::new(Some(&["/src/".to_string()]), None).unwrap();
        let side = PackageSide {
            dir: "packages/a",
            rules: &rules,
            resources: &resources,
            auto_readme: true,
            case: PathCase::Sensitive,
        };
        let tracked = vec![
            "packages/a/src/lib.rs".to_string(),
            "packages/a/README.md".to_string(),
        ];
        assert!(
            released_from_paths(&tracked, &tracked, &side).contains_key("README.md"),
            "a README on disk is detected"
        );
        let present = vec!["packages/a/src/lib.rs".to_string()];
        assert!(
            !released_from_paths(&tracked, &present, &side).contains_key("README.md"),
            "a README the work tree deleted is not"
        );
    }

    /// A deleted nested manifest no longer stops packing its directory.
    #[test]
    fn a_deleted_nested_manifest_no_longer_limits_released_content() {
        let resources = BTreeMap::new();
        let rules = PackagingRules::default();
        let side = PackageSide {
            dir: "packages/a",
            rules: &rules,
            resources: &resources,
            auto_readme: false,
            case: PathCase::Sensitive,
        };
        let tracked = vec![
            "packages/a/Cargo.toml".to_string(),
            "packages/a/fixture/Cargo.toml".to_string(),
            "packages/a/fixture/src/lib.rs".to_string(),
        ];
        let present = vec!["packages/a/Cargo.toml".to_string()];
        // Without the nested manifest on disk the boundary is gone and the
        // files beneath it become the outer package's released content.
        assert_eq!(
            released_from_paths(&tracked, &present, &side)
                .keys()
                .map(String::as_str)
                .collect::<BTreeSet<_>>(),
            BTreeSet::from(["Cargo.toml", "fixture/Cargo.toml", "fixture/src/lib.rs"])
        );
    }

    /// Resources resolve against the end that declared them.
    ///
    /// A resource outside the package directory is released content under the name it takes at the
    /// crate root; one already inside keeps its own path.
    #[test]
    fn resources_resolve_against_the_end_that_declared_them() {
        let manifest = manifest_with_resources(
            "packages/a",
            &["../../LICENSE", "docs/GUIDE.md"],
            &["README.md"],
        );

        let resolved = resolve_resources(&manifest, "packages/a", "");

        assert_eq!(
            resolved,
            BTreeMap::from([
                ("LICENSE".to_string(), "LICENSE".to_string()),
                ("README.md".to_string(), "README.md".to_string()),
                (
                    "docs/GUIDE.md".to_string(),
                    "packages/a/docs/GUIDE.md".to_string()
                ),
            ])
        );
    }

    /// Inherited resources resolve against the workspace prefix.
    ///
    /// A nested workspace declares inherited resources relative to its own root, not to the git
    /// root.
    #[test]
    fn inherited_resources_resolve_against_the_workspace_prefix() {
        let manifest = manifest_with_resources("inner/packages/a", &[], &["README.md"]);

        let resolved = resolve_resources(&manifest, "inner/packages/a", "inner/");

        assert_eq!(
            resolved,
            BTreeMap::from([("README.md".to_string(), "inner/README.md".to_string())])
        );
    }

    /// A resource outside the repository is dropped.
    ///
    /// A path climbing above the git root names no file in the repository, so it contributes no
    /// released content rather than failing classification.
    #[test]
    fn a_resource_outside_the_repository_is_dropped() {
        let manifest = manifest_with_resources("packages/a", &["../../../elsewhere/LICENSE"], &[]);

        assert!(resolve_resources(&manifest, "packages/a", "").is_empty());
    }

    fn manifest_with_resources(
        directory: &str,
        local: &[&str],
        inherited: &[&str],
    ) -> PackageManifest {
        PackageManifest {
            name: "a".to_string(),
            version: "0.1.0".parse().unwrap(),
            directory: directory.to_string(),
            packaging: PackagingRules::default(),
            inherited: InheritedKeys::default(),
            publish: true,
            path_dependencies: Vec::new(),
            inherited_path_dependencies: Vec::new(),
            installation_dependencies: InstallationDependencies::default(),
            resource_paths: local.iter().map(|path| (*path).to_string()).collect(),
            inherited_resource_paths: inherited.iter().map(|path| (*path).to_string()).collect(),
            auto_readme: false,
            targets: TargetDiscovery::default(),
        }
    }

    #[test]
    fn can_stop_timeline_waits_for_a_change_from_the_newest_carried_version() {
        fn entry(commit: &str, version: Option<&str>) -> TimelineEntry {
            TimelineEntry {
                commit: commit.to_string(),
                presence: version.map_or(Presence::Absent, |text| {
                    Presence::Published(text.parse().unwrap())
                }),
                has_parent: true,
            }
        }

        fn unpublished(commit: &str) -> TimelineEntry {
            TimelineEntry {
                commit: commit.to_string(),
                presence: Presence::Unpublished,
                has_parent: true,
            }
        }

        assert!(!can_stop_timeline(&[]));
        assert!(!can_stop_timeline(&[entry("c2", Some("0.1.0"))]));
        assert!(can_stop_timeline(&[
            entry("c2", Some("0.1.0")),
            entry("c1", Some("0.0.9")),
        ]));
        // Absent at the base and never carried: creation cannot be told from
        // truncation until the walk reaches a root.
        assert!(!can_stop_timeline(&[entry("c2", None), entry("c1", None)]));
        // Absent at the base but carried earlier: the reintroduction anchor is
        // still undetermined while the carried version keeps repeating.
        assert!(!can_stop_timeline(&[
            entry("c3", None),
            entry("c2", Some("0.3.0")),
        ]));
        assert!(!can_stop_timeline(&[
            entry("c3", None),
            entry("c2", Some("0.3.0")),
            entry("c1", Some("0.3.0")),
        ]));
        assert!(can_stop_timeline(&[
            entry("c3", None),
            entry("c2", Some("0.3.0")),
            entry("c1", Some("0.2.0")),
        ]));
        // A withdrawn commit releases nothing, so the walk must continue past it
        // to find the version change that actually ends the search.
        assert!(!can_stop_timeline(&[
            entry("c2", Some("0.3.0")),
            unpublished("c1"),
        ]));
        assert!(!can_stop_timeline(&[unpublished("c1")]));
    }

    #[test]
    fn is_inside_any_requires_a_directory_boundary() {
        let dirs = vec!["packages/a".to_string()];
        assert!(is_inside_any("packages/a/src/lib.rs", &dirs));
        assert!(!is_inside_any("packages/a", &dirs));
        assert!(!is_inside_any("packages/ab/src/lib.rs", &dirs));
    }

    /// Serves pre-parsed manifests.
    ///
    /// Membership resolution can then be exercised without a repository.
    struct FakeManifests {
        manifests: BTreeMap<String, PackageManifest>,
        /// Directories whose manifest was actually read, in order of first read.
        read: Vec<String>,
    }

    impl FakeManifests {
        fn new(entries: &[(&str, &str)]) -> Self {
            let manifests = entries
                .iter()
                .map(|(dir, content)| {
                    let path = format!("{dir}/Cargo.toml");
                    let parsed =
                        parse_package_manifest(content, &path, &WorkspaceInherit::default())
                            .unwrap()
                            .unwrap();
                    ((*dir).to_string(), parsed)
                })
                .collect();
            Self {
                manifests,
                read: Vec::new(),
            }
        }
    }

    impl ManifestSource for FakeManifests {
        fn candidate_dirs(&self) -> Vec<String> {
            self.manifests.keys().cloned().collect()
        }

        fn manifest(&mut self, dir: &str) -> Result<Option<&PackageManifest>, AppError> {
            if !self.read.iter().any(|seen| seen == dir) {
                self.read.push(dir.to_string());
            }
            Ok(self.manifests.get(dir))
        }
    }

    #[test]
    fn resolve_members_follows_path_dependencies() {
        let root = Path::new("Cargo.toml");
        let members = parse_workspace_members(
            "[workspace]\nmembers = [\"a\"]\nexclude = [\"c\"]\n",
            root,
            PathCase::Sensitive,
        )
        .unwrap();
        // Traversal consumes parsed models. Seed the package identities from one small document;
        // dependency-table parsing and aggregation have their own manifest tests.
        let mut manifests =
            FakeManifests::new(&[("a", "[package]\nname = \"a\"\nversion = \"0.1.0\"\n")]);
        let package = manifests.manifests.get("a").unwrap().clone();
        for name in ["b", "c"] {
            let mut package = package.clone();
            package.name = name.to_string();
            package.directory = name.to_string();
            manifests.manifests.insert(name.to_string(), package);
        }
        manifests.manifests.get_mut("a").unwrap().path_dependencies =
            vec!["../b".to_string(), "../c".to_string()];
        let resolved = resolve_members(&mut manifests, &members).unwrap();
        // `b` is reachable only as a path dependency; `c` is excluded even though
        // a member depends on it.
        assert_eq!(
            resolved.into_iter().collect::<Vec<_>>(),
            vec!["a".to_string(), "b".to_string()]
        );
    }

    /// Resolve members does not read unreachable manifests.
    ///
    /// A manifest Cargo would never load for this workspace is never parsed, so an unrelated nested
    /// workspace cannot fail classification.
    #[test]
    fn resolve_members_does_not_read_unreachable_manifests() {
        let root = Path::new("Cargo.toml");
        let members = parse_workspace_members(
            "[workspace]\nmembers = [\"packages/a\"]\n",
            root,
            PathCase::Sensitive,
        )
        .unwrap();
        let mut manifests = FakeManifests::new(&[
            (
                "packages/a",
                "[package]\nname = \"a\"\nversion = \"0.1.0\"\n",
            ),
            (
                "vendor/unrelated",
                "[package]\nname = \"unrelated\"\nversion = \"0.1.0\"\n",
            ),
        ]);

        let resolved = resolve_members(&mut manifests, &members).unwrap();

        assert_eq!(
            resolved.into_iter().collect::<Vec<_>>(),
            vec!["packages/a".to_string()]
        );
        assert_eq!(manifests.read, vec!["packages/a".to_string()]);
    }

    /// Resolve members ignores a path dependency outside the repository.
    ///
    /// A path dependency that climbs out of the repository is not a workspace member, and following
    /// it would name a directory outside the tree.
    #[test]
    fn resolve_members_ignores_a_path_dependency_outside_the_repository() {
        let root = Path::new("Cargo.toml");
        let members = parse_workspace_members(
            "[workspace]\nmembers = [\"packages/a\"]\n",
            root,
            PathCase::Sensitive,
        )
        .unwrap();
        let mut manifests = FakeManifests::new(&[(
            "packages/a",
            "[package]\nname = \"a\"\nversion = \"0.1.0\"\n\n[dependencies]\nout = { path = \"../../../outside\" }\n",
        )]);

        let resolved = resolve_members(&mut manifests, &members).unwrap();

        assert_eq!(
            resolved.into_iter().collect::<Vec<_>>(),
            vec!["packages/a".to_string()]
        );
    }
}
