// Git access via `git` subprocesses.
//
// The design forbids git2/gix; every read of history, trees, and diffs goes
// through this type. Ref: docs/implementation.md, "Subprocess boundaries".

use std::collections::HashSet;
use std::mem;
use std::path::{MAIN_SEPARATOR, Path, PathBuf};

use ohno::AppError;

use crate::command::{run_capture, run_capture_bytes, run_capture_ok, run_capture_os_bytes};
use crate::manifest::PathCase;
use crate::{
    CommandFailedError, NonUtf8BlobError, NonUtf8PathError, PathTooLongError, UnresolvedBaseError,
};

/// Name Cargo requires for a manifest.
const MANIFEST_FILE_NAME: &str = "Cargo.toml";

/// Pathspec matching manifests below the repository root.
///
/// The `glob` magic makes `**` cross directory boundaries, which the default
/// pathspec syntax does not do.
const MANIFEST_GLOB_PATHSPEC: &str = ":(glob)**/Cargo.toml";

/// Tree mode Git records for a symbolic link.
///
/// Ref: the `git ls-tree` documentation, which fixes the set of modes a tree
/// entry may carry.
const SYMLINK_TREE_MODE: &str = "120000";

/// Tree mode Git records for an executable regular file.
///
/// Cargo copies the bit into the archive it builds, so the mode is part of a
/// package's released content rather than a local detail.
/// Ref: docs/design.md, "Released content".
const EXECUTABLE_TREE_MODE: &str = "100755";

/// Tree mode Git records for a regular file without the executable bit.
const REGULAR_TREE_MODE: &str = "100644";

/// The mode Git records for a regular file, chosen by its executable bit.
///
/// Reports name the mode rather than the bit, so that what they print is the
/// vocabulary Git and Cargo both use.
pub(crate) fn tree_mode(executable: bool) -> &'static str {
    if executable {
        EXECUTABLE_TREE_MODE
    } else {
        REGULAR_TREE_MODE
    }
}

/// Remote whose recorded head names the branch releases are made from.
const DEFAULT_REMOTE_HEAD_REF: &str = "refs/remotes/origin/HEAD";

/// Revision used when the remote records no default branch.
///
/// Only reached by a repository that has never recorded a remote head, so the
/// most widely used name for a default branch is the one remaining guess.
const CONVENTIONAL_BASE: &str = "origin/main";

/// Where the release baseline came from when the caller did not name one.
///
/// Which of the two answered decides how much a run should trust the baseline,
/// so the distinction is carried to the caller rather than collapsed into a
/// string: a recorded remote head is the repository's own statement of the
/// branch it releases from, while the convention is a guess that a repository
/// releasing from a differently named branch would silently make wrong.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum DefaultBase {
    /// The remote's recorded default branch.
    RemoteHead(String),
    /// The conventional name, because the remote records no default branch.
    Convention(String),
}

impl DefaultBase {
    pub(crate) fn revision(&self) -> &str {
        match self {
            Self::RemoteHead(revision) | Self::Convention(revision) => revision,
        }
    }
}

/// Interprets what the recorded remote head lookup answered.
///
/// A repository whose remote head is unset fails the lookup rather than
/// answering emptily, but a configuration that answers with nothing names no
/// branch either, so both reach the convention by the same route.
fn decode_default_base(recorded: Option<String>) -> DefaultBase {
    let recorded = recorded
        .map(|branch| branch.trim().to_owned())
        .filter(|branch| !branch.is_empty());
    match recorded {
        Some(branch) => DefaultBase::RemoteHead(branch),
        None => DefaultBase::Convention(CONVENTIONAL_BASE.to_owned()),
    }
}

/// The tool's access to one Git repository.
///
/// Every historical fact a verdict rests on — the commits on the base
/// first-parent line, the manifests and file contents at a commit, the tracked
/// state of the work tree — is read through this type by spawning `git`, so it
/// is the single place where repository state enters classification
/// (implementation.md, "Subprocess boundaries").
///
/// It also owns the translation between the two path spaces the tool works in:
/// commands run at the repository root, while packages are addressed relative
/// to it, so `prefix` records where the invoked workspace sits inside the
/// repository and every query is expressed in repository-relative Git paths.
#[derive(Clone, Debug)]
pub(crate) struct GitRepo {
    root: PathBuf,
    prefix: String,
}

impl GitRepo {
    /// Discovers the repository containing `start` using `git rev-parse`.
    pub(crate) fn discover(start: &Path) -> Result<Self, AppError> {
        let dir = if start.is_file() {
            start.parent().unwrap_or(start)
        } else {
            start
        };
        // Both answers are asked of the same directory rather than derived from
        // one another: stripping the root from a path Cargo reported would
        // compare two spellings of the same directory that need not match, since
        // Windows hands out 8.3 short names for some paths and symlinked or
        // substituted roots differ on every platform.
        //
        // Each answer is read from its own invocation because `git` separates
        // them with a newline, which is a legal character in a path name and so
        // cannot be told apart from one inside an answer.
        let root = run_capture("git", &["rev-parse", "--show-toplevel"], dir)?;
        // Empty when the repository root is the directory itself.
        let prefix = run_capture("git", &["rev-parse", "--show-prefix"], dir)?;
        let prefix = strip_terminator(&prefix);
        Ok(Self {
            root: PathBuf::from(strip_terminator(&root)),
            prefix: prefix.strip_suffix('/').unwrap_or(prefix).to_string(),
        })
    }

    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    /// The repository-relative directory the repository was discovered from.
    ///
    /// Empty when that directory is the repository root.
    pub(crate) fn prefix(&self) -> &str {
        &self.prefix
    }

    pub(crate) fn rev_parse(&self, rev: &str) -> Result<String, AppError> {
        match run_capture(
            "git",
            &["rev-parse", "--verify", "--end-of-options", rev],
            &self.root,
        ) {
            Ok(stdout) => Ok(stdout.trim().to_string()),
            Err(error) => Err(UnresolvedBaseError::caused_by(rev, error).into()),
        }
    }

    /// The branch releases are made from, used when the caller names no baseline.
    ///
    /// `git clone` records the remote's default branch, and `git remote
    /// set-head` refreshes it, so the repository already knows the branch it
    /// releases from and the tool does not have to assume what it is called.
    pub(crate) fn default_base(&self) -> Result<DefaultBase, AppError> {
        let recorded = run_capture_ok(
            "git",
            &["symbolic-ref", "--short", DEFAULT_REMOTE_HEAD_REF],
            &self.root,
        )?;
        Ok(decode_default_base(recorded))
    }

    // HEAD is only used as a report label; tests do not pin the exact SHA string.
    #[cfg_attr(test, mutants::skip)]
    pub(crate) fn head(&self) -> Result<String, AppError> {
        Ok(run_capture("git", &["rev-parse", "HEAD"], &self.root)?
            .trim()
            .to_string())
    }

    /// First-parent commits reachable from `rev`, newest first, as full hashes.
    pub(crate) fn first_parent_commits(&self, rev: &str) -> Result<Vec<String>, AppError> {
        let stdout = run_capture("git", &["rev-list", "--first-parent", rev], &self.root)?;
        Ok(stdout
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .map(ToOwned::to_owned)
            .collect())
    }

    /// Whether `commit` has a parent that the walk must not treat as a true root.
    ///
    /// A true root commit has no `parent` header. A shallow-boundary commit has
    /// one even though Git cannot resolve `commit^`, and reports `true` here so
    /// the caller reports truncated history instead of a root.
    pub(crate) fn has_parent_or_is_shallow_boundary(&self, commit: &str) -> Result<bool, AppError> {
        if !self.commit_has_parent_header(commit)? {
            return Ok(false);
        }
        let spec = format!("{commit}^");
        if run_capture_ok("git", &["rev-parse", "--verify", &spec], &self.root)?.is_some() {
            return Ok(true);
        }
        self.is_shallow()
    }

    fn commit_has_parent_header(&self, commit: &str) -> Result<bool, AppError> {
        // `cat-file -p` prints the `parent` header even when the parent object
        // was not fetched (shallow boundary). `rev-list --parents` omits that
        // parent when it cannot be resolved.
        let stdout = run_capture("git", &["cat-file", "-p", commit], &self.root)?;
        // A blank line terminates the header block and the message follows, so
        // only the headers are inspected: a message body line that happens to
        // start with `parent ` must not make a root commit look parented.
        Ok(stdout
            .lines()
            .take_while(|line| !line.is_empty())
            .any(|line| line.starts_with("parent ")))
    }

    /// First-parent commits reachable from `rev` that touch a `Cargo.toml`.
    ///
    /// Version and membership can change only on those commits, so classification
    /// reconstructs historical workspaces from this subset rather than every
    /// first-parent commit.
    pub(crate) fn first_parent_manifest_commits(&self, rev: &str) -> Result<Vec<String>, AppError> {
        let all = self.first_parent_commits(rev)?;
        let stdout = run_capture(
            "git",
            &[
                "rev-list",
                "--first-parent",
                rev,
                "--",
                MANIFEST_FILE_NAME,
                MANIFEST_GLOB_PATHSPEC,
            ],
            &self.root,
        )?;
        let touching: HashSet<&str> = stdout
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .collect();
        // Keep the base revision and the oldest first-parent commit even when they
        // do not touch a manifest, so the timeline still observes HEAD and can
        // distinguish a true root from truncated history.
        let newest = all.first().cloned();
        let oldest = all.last().cloned();
        Ok(all
            .into_iter()
            .filter(|commit| {
                touching.contains(commit.as_str())
                    || newest.as_deref() == Some(commit.as_str())
                    || oldest.as_deref() == Some(commit.as_str())
            })
            .collect())
    }

    fn is_shallow(&self) -> Result<bool, AppError> {
        let stdout = run_capture("git", &["rev-parse", "--is-shallow-repository"], &self.root)?;
        Ok(stdout.trim() == "true")
    }

    /// Text at `commit:rel_path`, or `None` if the path is absent.
    ///
    /// Callers parse the result as TOML. Replacing invalid bytes would turn
    /// content Cargo could never have parsed into a different, parseable
    /// document and classify a package against text Git does not store, so a
    /// blob that is not valid UTF-8 is reported instead.
    pub(crate) fn show_file(
        &self,
        commit: &str,
        rel_path: &str,
    ) -> Result<Option<String>, AppError> {
        match self.show_file_bytes(commit, rel_path)? {
            Some(bytes) => String::from_utf8(bytes)
                .map(Some)
                .map_err(|error| NonUtf8BlobError::caused_by(commit, rel_path, error).into()),
            None => Ok(None),
        }
    }

    /// Raw bytes at `commit:rel_path`, or `None` if the path is absent.
    pub(crate) fn show_file_bytes(
        &self,
        commit: &str,
        rel_path: &str,
    ) -> Result<Option<Vec<u8>>, AppError> {
        let spec = format!("{commit}:{rel_path}");
        match run_capture_bytes("git", &["show", &spec], &self.root) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(error) => {
                if error
                    .find_source::<CommandFailedError>()
                    .is_some_and(|failed| is_absent_git_path(failed.stderr()))
                {
                    Ok(None)
                } else {
                    Err(error)
                }
            }
        }
    }

    /// Paths under `pathspec` that Git records in the index.
    ///
    /// A recorded path need not exist in the work tree, because a deletion is
    /// tracked until it is staged. Callers that need work-tree presence check
    /// for it separately, so that a deleted released file is still recognised
    /// as one.
    pub(crate) fn ls_files(&self, pathspec: &str) -> Result<Vec<String>, AppError> {
        let stdout = run_capture_bytes(
            "git",
            &["ls-files", "-z", "--", &dir_pathspec(pathspec)],
            &self.root,
        )?;
        split_z(&stdout)
    }

    /// Tracked entries matching `paths`, using the spelling Git records.
    ///
    /// Files a manifest names from outside its package directory are not
    /// covered by any per-directory listing, so their tracked state is asked
    /// for by path. Matching follows the checkout's case rules because Cargo
    /// opens manifest-declared resources through that checkout. An empty input
    /// answers without invoking Git, because `git ls-files` with no pathspec
    /// lists the whole repository.
    pub(crate) fn tracked_paths(
        &self,
        paths: &[&str],
        case: PathCase,
    ) -> Result<Vec<String>, AppError> {
        if paths.is_empty() {
            return Ok(Vec::new());
        }
        let mut args = vec!["ls-files".to_string(), "-z".to_string(), "--".to_string()];
        args.extend(paths.iter().map(|path| cased_pathspec(path, case)));
        let stdout = run_capture_os_bytes("git", &args, &self.root)?;
        split_z(&stdout)
    }

    /// Modes under `pathspecs` that affect packaged work-tree content.
    ///
    /// The index supplies the baseline mode. `git diff-files` then overlays
    /// changes that Git observes in the work tree, including an unstaged
    /// executable-bit change when `core.fileMode` is active. With that setting
    /// disabled, as it normally is on Windows, Git reports no mode change and
    /// the index remains the portable fallback.
    /// Ref: docs/implementation.md, "Classification".
    ///
    /// An empty input answers without invoking Git, because `git ls-files` with
    /// no pathspec lists the whole repository.
    pub(crate) fn work_tree_modes(&self, pathspecs: &[&str]) -> Result<WorkTreeModes, AppError> {
        if pathspecs.is_empty() {
            return Ok(WorkTreeModes::default());
        }
        let mut args = vec![
            "ls-files".to_string(),
            "-s".to_string(),
            "-z".to_string(),
            "--".to_string(),
        ];
        args.extend(pathspecs.iter().map(|pathspec| dir_pathspec(pathspec)));
        let stdout = run_capture_os_bytes("git", &args, &self.root)?;
        let mut modes = WorkTreeModes::default();
        for record in split_z(&stdout)? {
            if let Some((mode, path)) = staged_path_mode(&record) {
                modes.set(path, mode);
            }
        }

        let mut args = vec![
            "diff-files".to_string(),
            "--raw".to_string(),
            "-z".to_string(),
            "--no-renames".to_string(),
            "--".to_string(),
        ];
        args.extend(pathspecs.iter().map(|pathspec| dir_pathspec(pathspec)));
        let stdout = run_capture_os_bytes("git", &args, &self.root)?;
        overlay_work_tree_modes(&stdout, &mut modes)?;
        Ok(modes)
    }

    /// Untracked, non-ignored paths under `pathspec`.
    // Advisory-only listing; classification does not fail on untracked files.
    #[cfg_attr(test, mutants::skip)]
    pub(crate) fn ls_untracked(&self, pathspec: &str) -> Result<Vec<String>, AppError> {
        let stdout = run_capture_bytes(
            "git",
            &[
                "ls-files",
                "-z",
                "--others",
                "--exclude-standard",
                "--",
                &dir_pathspec(pathspec),
            ],
            &self.root,
        )?;
        split_z(&stdout)
    }

    /// Tree entries under `pathspecs` at `commit`.
    ///
    /// The mode and object id are kept alongside the path because both answer
    /// questions the path cannot. A link's blob holds its target path, which is
    /// indistinguishable from a regular file's content once read, so the mode is
    /// the only place that distinction survives; and the object id is the
    /// content identity Git itself compares by, which is what a work-tree file
    /// has to be compared against once a filter stands between the two.
    pub(crate) fn ls_tree(
        &self,
        commit: &str,
        pathspecs: &[&str],
    ) -> Result<Vec<TreeEntry>, AppError> {
        let mut args = vec![
            "ls-tree".to_string(),
            "-r".to_string(),
            "-z".to_string(),
            commit.to_string(),
            "--".to_string(),
        ];
        args.extend(pathspecs.iter().map(|pathspec| dir_pathspec(pathspec)));
        let stdout = run_capture_os_bytes("git", &args, &self.root)?;
        Ok(split_z(&stdout)?
            .iter()
            .filter_map(|record| TreeEntry::parse(record))
            .collect())
    }

    /// Object ids the work-tree files at `rel_paths` would be stored under.
    ///
    /// Git converts content on its way into the object database, so a file on
    /// disk and the blob recording it need not hold the same bytes: an LFS
    /// pointer and a line-ending rule both make the two diverge. Asking Git to
    /// hash the work-tree file applies the same conversion the file would get if
    /// it were staged, which puts both ends of a comparison in one
    /// representation. The converted blobs are retained in Git's object database
    /// so patch rendering can read those exact bytes without running a potentially
    /// stateful clean filter twice. This changes no ref, index entry, or work-tree
    /// path; unreachable blobs remain subject to ordinary Git garbage collection.
    /// Ids come back in the order the paths were given.
    pub(crate) fn hash_objects(&self, rel_paths: &[&str]) -> Result<Vec<String>, AppError> {
        let mut ids = Vec::with_capacity(rel_paths.len());
        for chunk in command_line_batches(rel_paths)? {
            // The paths are handed to the child borrowed: they outlive the call
            // and copying them would duplicate every path in the request.
            let args = ["hash-object", "-w", "--"].into_iter().chain(chunk);
            let stdout = run_capture_os_bytes("git", args, &self.root)?;
            ids.extend(
                String::from_utf8_lossy(&stdout)
                    .lines()
                    .map(str::trim)
                    .filter(|line| !line.is_empty())
                    .map(ToOwned::to_owned),
            );
        }
        Ok(ids)
    }

    /// Bytes of a blob already present in Git's object database.
    pub(crate) fn show_blob_bytes(&self, id: &str) -> Result<Vec<u8>, AppError> {
        run_capture_bytes("git", &["cat-file", "blob", id], &self.root)
    }

    /// Every path at `commit`, used to reconstruct historical package metadata.
    pub(crate) fn ls_tree_paths(&self, commit: &str) -> Result<Vec<String>, AppError> {
        let stdout = run_capture_bytes(
            "git",
            &["ls-tree", "-r", "--name-only", "-z", commit],
            &self.root,
        )?;
        split_z(&stdout)
    }
}

/// Strips the single record terminator `git` writes after a value.
///
/// Only the terminator goes: a path may legitimately begin or end with a space,
/// so trimming whitespace would silently rename it.
fn strip_terminator(value: &str) -> &str {
    value.strip_suffix('\n').unwrap_or(value)
}

/// Command-line budget one `git hash-object` invocation may spend on paths.
///
/// Windows renders a child's arguments into a single command line that
/// `CreateProcessW` caps at 32,767 UTF-16 code units, which is the smallest
/// limit among supported platforms. The remainder of that cap is left for the
/// executable path, the fixed arguments, and separators.
const PATH_ARG_BUDGET: usize = 30_000;

/// Splits `rel_paths` into batches whose rendered command line fits the budget.
///
/// A path count cannot bound a command line, because paths vary in length. Each
/// path is charged its worst-case rendered cost, so an ordinary package still
/// takes one round trip while a deeply nested one is split instead of failing
/// to spawn.
fn command_line_batches<'p>(rel_paths: &[&'p str]) -> Result<Vec<Vec<&'p str>>, AppError> {
    let mut batches = Vec::new();
    let mut current: Vec<&'p str> = Vec::new();
    let mut spent = 0_usize;
    for path in rel_paths {
        let cost = rendered_arg_cost(path);
        if cost > PATH_ARG_BUDGET {
            return Err(PathTooLongError::new(*path).into());
        }
        if spent.saturating_add(cost) > PATH_ARG_BUDGET && !current.is_empty() {
            batches.push(mem::take(&mut current));
            spent = 0;
        }
        current.push(path);
        spent = spent.saturating_add(cost);
    }
    if !current.is_empty() {
        batches.push(current);
    }
    Ok(batches)
}

/// Worst-case command-line cost of one path argument, in UTF-16 code units.
///
/// Quoting can at most double a path's length (every character escaped) and
/// adds surrounding quotes and a separator, so charging that upper bound keeps
/// the estimate on the safe side of the platform cap without modelling any
/// particular quoting rule.
fn rendered_arg_cost(path: &str) -> usize {
    path.encode_utf16()
        .count()
        .saturating_mul(2)
        .saturating_add(3)
}

/// One file recorded in a commit's tree.
///
/// Classification needs more of a tree record than the path: the mode says
/// whether the entry is a symbolic link, and the object id is the content
/// identity a work-tree file is compared against.
#[derive(Clone, Debug)]
pub(crate) struct TreeEntry {
    pub(crate) path: String,
    pub(crate) id: String,
    mode: String,
}

impl TreeEntry {
    /// Reads a `<mode> <type> <object>\t<path>` record, skipping anything else.
    fn parse(record: &str) -> Option<Self> {
        let (metadata, path) = record.split_once('\t')?;
        let mut fields = metadata.split(' ');
        let mode = fields.next()?;
        let _kind = fields.next()?;
        let id = fields.next()?;
        Some(Self {
            path: path.to_string(),
            id: id.to_string(),
            mode: mode.to_string(),
        })
    }

    /// Whether the entry is a symbolic link, which Git marks by a fixed mode.
    pub(crate) fn is_symlink(&self) -> bool {
        self.mode == SYMLINK_TREE_MODE
    }

    /// Whether the entry carries the executable bit.
    pub(crate) fn is_executable(&self) -> bool {
        self.mode == EXECUTABLE_TREE_MODE
    }
}

/// Work-tree modes that affect released artifacts or their interpretation.
///
/// Executable paths provide the mode Cargo copies into the archive. Symlink
/// paths must be rejected before hashing because `core.symlinks=false` can
/// materialize an indexed link as an ordinary work-tree file.
/// Ref: docs/implementation.md, "Content identity and file modes".
#[derive(Debug, Default, Eq, PartialEq)]
pub(crate) struct WorkTreeModes {
    executable: HashSet<String>,
    symlinks: HashSet<String>,
}

impl WorkTreeModes {
    /// Whether Git considers `path` an executable regular file.
    pub(crate) fn is_executable(&self, path: &str) -> bool {
        self.executable.contains(path)
    }

    /// Whether Git records `path` as a symbolic link.
    pub(crate) fn is_symlink(&self, path: &str) -> bool {
        self.symlinks.contains(path)
    }

    /// Replaces the artifact-relevant mode for `path`.
    fn set(&mut self, path: &str, mode: &str) {
        match mode {
            EXECUTABLE_TREE_MODE => {
                self.symlinks.remove(path);
                self.executable.insert(path.to_string());
            }
            SYMLINK_TREE_MODE => {
                self.executable.remove(path);
                self.symlinks.insert(path.to_string());
            }
            _ => {
                self.executable.remove(path);
                self.symlinks.remove(path);
            }
        }
    }
}

/// Reads a `<mode> <object> <stage>\t<path>` index record.
///
/// The mode is read off the record's own first field rather than matched
/// anywhere in the record, so a path that itself looks like a mode cannot be
/// mistaken for one.
fn staged_path_mode(record: &str) -> Option<(&str, &str)> {
    let (metadata, path) = record.split_once('\t')?;
    let mode = metadata.split(' ').next()?;
    Some((mode, path))
}

/// Applies the work-tree modes from NUL-delimited `git diff-files --raw` records.
fn overlay_work_tree_modes(stdout: &[u8], modes: &mut WorkTreeModes) -> Result<(), AppError> {
    let fields = split_z(stdout)?;
    for [header, path] in fields.as_chunks::<2>().0 {
        let mut metadata = header.split_whitespace();
        _ = metadata.next();
        if let Some(mode) = metadata.next() {
            modes.set(path, mode);
        }
    }
    Ok(())
}

/// Rewrites an operating-system path into the `/`-separated form Git reports.
///
/// Only the platform's own separator is rewritten. A backslash is an ordinary
/// character in a file name on Unix, so rewriting one would name a different
/// file, and paths Git itself reports already use `/` on every platform and are
/// therefore taken verbatim.
pub(crate) fn os_path(path: &Path) -> String {
    let path = path.to_string_lossy();
    if MAIN_SEPARATOR == '/' {
        path.into_owned()
    } else {
        path.replace(MAIN_SEPARATOR, "/")
    }
}

/// Turns a directory into a pathspec Git matches literally.
///
/// Directory names come out of the repository, so a name containing pathspec
/// syntax — a `*`, or a leading `:` Git would read as magic — would otherwise
/// select a sibling package's files or fail to select the package's own,
/// producing a release verdict for content that is not the package's.
/// `:(literal)` turns the whole value back into a plain path.
///
/// The repository root is the empty string in every path this tool computes,
/// but Git rejects an empty pathspec, so the root becomes `.`.
fn dir_pathspec(dir: &str) -> String {
    let dir = if dir.is_empty() { "." } else { dir };
    format!(":(literal){dir}")
}

/// Turns a path into a literal pathspec that follows the checkout's case rules.
///
/// Cargo opens manifest-declared resources through the filesystem, while Git
/// pathspecs are case-sensitive by default even on a case-insensitive checkout.
fn cased_pathspec(path: &str, case: PathCase) -> String {
    let path = if path.is_empty() { "." } else { path };
    match case {
        PathCase::Sensitive => format!(":(literal){path}"),
        PathCase::Insensitive => format!(":(icase,literal){path}"),
    }
}

/// Joins a workspace-relative path onto the workspace's git prefix.
///
/// The result is a pathspec that `git ls-files` and `git show` resolve against
/// the repository root. Parent components allow Cargo members beside a nested
/// workspace root to remain addressable. Both operands are already in Git's
/// `/`-separated space.
pub(crate) fn join_git_rel(prefix: &str, workspace_rel: &str) -> String {
    let prefix = prefix.trim_end_matches('/');
    let rel = workspace_rel.trim_end_matches('/');
    let joined = if prefix.is_empty() || prefix == "." {
        rel.to_string()
    } else if rel.is_empty() || rel == "." {
        prefix.to_string()
    } else {
        format!("{prefix}/{rel}")
    };
    let mut normalized = Vec::new();
    for component in joined.split('/') {
        match component {
            "" | "." => {}
            ".." if normalized.last().is_some_and(|previous| *previous != "..") => {
                normalized.pop();
            }
            other => normalized.push(other),
        }
    }
    normalized.join("/")
}

/// Whether Git reported a path as absent from the revision.
///
/// The alternative is an operational failure, which the caller must not mistake
/// for an absent path.
///
/// Git has no machine-readable signal for this, so the wording is matched. That
/// is only sound because [`crate::command`] pins the child locale to `C`; a
/// translated Git would report a routine package creation or deletion as an
/// operational error.
fn is_absent_git_path(stderr: &str) -> bool {
    let stderr = stderr.to_ascii_lowercase();
    stderr.contains("does not exist")
        || stderr.contains("exists on disk, but not in")
        || stderr.contains("did not match")
}

/// Splits NUL-terminated `git` path output into paths.
///
/// With `-z` the NUL already delimits every record, so whitespace inside a field
/// is filename data and must survive. Only the empty field after the final
/// terminator is dropped.
///
/// # Errors
///
/// Returns [`NonUtf8PathError`] if any field is not valid UTF-8.
fn split_z(stdout: &[u8]) -> Result<Vec<String>, AppError> {
    stdout
        .split(|byte| *byte == 0)
        .filter(|part| !part.is_empty())
        .map(|part| {
            str::from_utf8(part)
                .map(ToOwned::to_owned)
                .map_err(|_ignored| {
                    NonUtf8PathError::new(String::from_utf8_lossy(part).into_owned()).into()
                })
        })
        .collect()
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::fs;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt as _;

    use tempfile::tempdir;

    use super::*;

    #[test]
    fn a_recorded_remote_head_is_the_default_base() {
        assert_eq!(
            decode_default_base(Some("origin/trunk".to_owned())),
            DefaultBase::RemoteHead("origin/trunk".to_owned())
        );
        assert_eq!(
            decode_default_base(Some("origin/main\n".to_owned())),
            DefaultBase::RemoteHead("origin/main".to_owned())
        );
    }

    #[test]
    fn an_unrecorded_remote_head_falls_back_to_the_convention() {
        assert_eq!(
            decode_default_base(None),
            DefaultBase::Convention(CONVENTIONAL_BASE.to_owned())
        );
        // An answer that names no branch is as unusable as no answer.
        assert_eq!(
            decode_default_base(Some(String::new())),
            DefaultBase::Convention(CONVENTIONAL_BASE.to_owned())
        );
        assert_eq!(
            decode_default_base(Some("  \n".to_owned())),
            DefaultBase::Convention(CONVENTIONAL_BASE.to_owned())
        );
    }

    #[test]
    fn either_default_base_yields_its_revision() {
        assert_eq!(
            DefaultBase::RemoteHead("origin/trunk".to_owned()).revision(),
            "origin/trunk"
        );
        assert_eq!(
            DefaultBase::Convention("origin/main".to_owned()).revision(),
            "origin/main"
        );
    }

    #[test]
    fn only_the_record_terminator_is_stripped() {
        assert_eq!(strip_terminator("packages/demo\n"), "packages/demo");
        // A path may end in a space, so only the terminator goes.
        assert_eq!(strip_terminator("packages/demo \n"), "packages/demo ");
        // Git omits the terminator on the last record of some outputs.
        assert_eq!(strip_terminator("packages/demo"), "packages/demo");
        // A newline is a legal character in a path, so an inner one stays.
        assert_eq!(strip_terminator("odd\nname\n"), "odd\nname");
    }

    #[test]
    fn absent_git_path_matches_known_stderr() {
        assert!(is_absent_git_path(
            "fatal: path 'x' does not exist in 'abc'"
        ));
        assert!(is_absent_git_path(
            "fatal: path 'x' exists on disk, but not in 'abc'"
        ));
        assert!(is_absent_git_path(
            "fatal: Path 'x' did not match any files"
        ));
        assert!(!is_absent_git_path("fatal: bad object abc"));
    }

    /// Tree records are parsed into mode and object.
    ///
    /// The mode and object id are read off the record's own fields, so a path that itself looks
    /// like a mode cannot be mistaken for one.
    #[test]
    fn tree_records_are_parsed_into_mode_and_object() {
        let link = TreeEntry::parse("120000 blob abc\tpackages/foo/link.txt").unwrap();
        assert!(link.is_symlink());
        assert_eq!(link.path, "packages/foo/link.txt");
        assert_eq!(link.id, "abc");

        let file = TreeEntry::parse("100644 blob def\tpackages/foo/real.txt").unwrap();
        assert!(!file.is_symlink());
        assert_eq!(file.id, "def");

        // A regular file whose mode merely starts with the link mode's digits.
        assert!(
            !TreeEntry::parse("1200001 blob abc\tx")
                .unwrap()
                .is_symlink()
        );
        // Not a record at all: no field separator.
        assert!(TreeEntry::parse("120000 blob abc packages/foo").is_none());
        assert!(TreeEntry::parse("120000 blob\tpackages/foo").is_none());
    }

    /// Tree entries report the executable bit off the mode field.
    #[test]
    fn tree_records_report_the_executable_bit() {
        let script = TreeEntry::parse("100755 blob abc\tpackages/foo/run.sh").unwrap();
        assert!(script.is_executable());
        let plain = TreeEntry::parse("100644 blob def\tpackages/foo/lib.rs").unwrap();
        assert!(!plain.is_executable());
        // A symbolic link is not a regular file and so is never executable.
        assert!(
            !TreeEntry::parse("120000 blob abc\tpackages/foo/link")
                .unwrap()
                .is_executable()
        );
    }

    /// Index records yield their modes and paths.
    ///
    /// The mode is read off the record's own first field, so a path that itself looks like the
    /// executable mode cannot be mistaken for one.
    #[test]
    fn index_records_yield_modes_and_paths() {
        assert_eq!(
            staged_path_mode("100755 abc 0\tpackages/foo/run.sh"),
            Some(("100755", "packages/foo/run.sh"))
        );
        assert_eq!(
            staged_path_mode("120000 abc 0\tpackages/foo/link"),
            Some(("120000", "packages/foo/link"))
        );
        // The path names the executable mode without carrying it.
        assert_eq!(
            staged_path_mode("100644 abc 0\t100755"),
            Some(("100644", "100755"))
        );
        // Not a record at all: no field separator.
        assert_eq!(staged_path_mode("100755 abc 0 packages/foo"), None);
    }

    /// A regular file's mode is chosen by its executable bit.
    #[test]
    fn a_regular_files_mode_is_chosen_by_its_executable_bit() {
        assert_eq!(tree_mode(true), EXECUTABLE_TREE_MODE);
        assert_eq!(tree_mode(false), REGULAR_TREE_MODE);
    }

    /// An empty pathspec list never reaches Git.
    ///
    /// `git ls-files` with no pathspec lists the whole repository, which would report executable
    /// paths from every package rather than only the one being classified.
    #[cfg_attr(miri, ignore)] // Spawns git, which Miri cannot emulate.
    #[test]
    fn work_tree_modes_include_the_index() {
        let temp = tempdir().unwrap();
        let root = temp.path();
        fs::create_dir_all(root.join("packages/foo")).unwrap();
        fs::write(root.join("packages/foo/run.sh"), "echo\n").unwrap();
        fs::write(root.join("packages/foo/lib.rs"), "x").unwrap();
        let repo = init_repo(root);
        // Set through the index, because a Windows checkout has no executable
        // permission to set and turns `core.fileMode` off.
        run_capture(
            "git",
            &["update-index", "--chmod=+x", "packages/foo/run.sh"],
            root,
        )
        .unwrap();
        #[cfg(unix)]
        {
            // Unix work-tree permissions are authoritative, so keep them aligned
            // with the index state this cross-platform test establishes.
            let path = root.join("packages/foo/run.sh");
            let mut permissions = fs::metadata(&path).unwrap().permissions();
            permissions.set_mode(permissions.mode() | 0o111);
            fs::set_permissions(path, permissions).unwrap();
        }

        let modes = repo.work_tree_modes(&["packages/foo"]).unwrap();
        assert!(modes.is_executable("packages/foo/run.sh"));
        assert!(!modes.is_executable("packages/foo/lib.rs"));
        assert!(!modes.is_symlink("packages/foo/run.sh"));
        assert_eq!(repo.work_tree_modes(&[]).unwrap(), WorkTreeModes::default());
    }

    #[cfg_attr(miri, ignore)] // Spawns git, which Miri cannot emulate.
    #[test]
    fn an_option_like_revision_resolves_only_as_a_ref() {
        let temp = tempdir().unwrap();
        let repo = init_repo(temp.path());
        let expected = repo.rev_parse("HEAD").unwrap();
        // Git accepts this full ref even though the shorthand is also an option.
        run_capture(
            "git",
            &["update-ref", "refs/heads/--all", "HEAD"],
            temp.path(),
        )
        .unwrap();

        assert_eq!(repo.rev_parse("--all").unwrap(), expected);
    }

    /// Work-tree mode records overlay the index without confusing content edits.
    #[test]
    fn work_tree_modes_overlay_index_modes() {
        let mut modes = WorkTreeModes::default();
        modes.set("made-plain.sh", EXECUTABLE_TREE_MODE);
        modes.set("unchanged.sh", EXECUTABLE_TREE_MODE);
        modes.set("made-regular-link", SYMLINK_TREE_MODE);
        modes.set("unchanged-link", SYMLINK_TREE_MODE);
        let records = b":100644 100755 old new M\0made-executable.sh\0\
                        :100755 100644 old new M\0made-plain.sh\0\
                        :120000 100644 old new M\0made-regular-link\0\
                        :100644 120000 old new M\0made-link\0\
                        :100644 100644 old new M\0content-only.sh\0";

        overlay_work_tree_modes(records, &mut modes).unwrap();

        assert_eq!(
            modes.executable,
            HashSet::from(["made-executable.sh".to_string(), "unchanged.sh".to_string(),])
        );
        assert_eq!(
            modes.symlinks,
            HashSet::from(["made-link".to_string(), "unchanged-link".to_string()])
        );
    }

    /// A malformed mode record cannot invent an executable path.
    #[test]
    fn work_tree_mode_without_modes_is_ignored() {
        let mut modes = WorkTreeModes::default();

        overlay_work_tree_modes(b":\0script.sh\0", &mut modes).unwrap();

        assert_eq!(modes, WorkTreeModes::default());
    }

    /// Os path rewrites only the platform separator.
    ///
    /// A backslash is an ordinary character in a file name on Unix, so only the platform's own
    /// separator may be rewritten when an operating-system path enters Git's path space.
    #[test]
    fn os_path_rewrites_only_the_platform_separator() {
        let native = PathBuf::from("packages").join("foo").join("Cargo.toml");
        assert_eq!(os_path(&native), "packages/foo/Cargo.toml");

        if MAIN_SEPARATOR != '\\' {
            assert_eq!(os_path(Path::new(r"odd\name.rs")), r"odd\name.rs");
        }
    }

    /// Short paths all fit one batch.
    ///
    /// Ordinary packages must still take one subprocess, or every classification would pay for
    /// extra round trips.
    #[test]
    fn short_paths_all_fit_one_batch() {
        let paths: Vec<&str> = vec!["packages/foo/src/lib.rs"; 256];
        let batches = command_line_batches(&paths).unwrap();
        assert_eq!(batches.len(), 1);
        let only = batches.first().expect("one batch was just asserted");
        assert_eq!(only.len(), paths.len());
    }

    /// Batching is bounded by the rendered command line, not by a path count.
    #[test]
    fn long_paths_are_split_across_batches() {
        // Long enough that two paths cannot share a command line.
        let long = "x".repeat(PATH_ARG_BUDGET.div_euclid(3));
        let paths: Vec<&str> = vec![long.as_str(); 4];
        let batches = command_line_batches(&paths).unwrap();
        assert!(batches.len() > 1);
        assert_eq!(batches.iter().map(Vec::len).sum::<usize>(), paths.len());
    }

    #[test]
    fn an_empty_request_produces_no_batches() {
        assert!(command_line_batches(&[]).unwrap().is_empty());
    }

    /// A path longer than the budget is rejected.
    ///
    /// A path that cannot fit alone would otherwise loop or fail to spawn with an operating-system
    /// message that names no path.
    #[test]
    fn a_path_longer_than_the_budget_is_rejected() {
        let huge = "x".repeat(PATH_ARG_BUDGET);
        let error = command_line_batches(&[huge.as_str()]).unwrap_err();
        assert!(error.find_source::<PathTooLongError>().is_some());
    }

    #[test]
    fn join_git_rel_prefixes_when_workspace_is_nested() {
        assert_eq!(join_git_rel("inner", "packages/foo"), "inner/packages/foo");
        assert_eq!(join_git_rel("inner", "../outside"), "outside");
        assert_eq!(join_git_rel("inner/nested", "../../outside"), "outside");
        assert_eq!(join_git_rel("inner", "../../outside"), "../outside");
        assert_eq!(join_git_rel("", "packages/foo"), "packages/foo");
        assert_eq!(join_git_rel("inner", ""), "inner");
        assert_eq!(join_git_rel("inner", "."), "inner");
        assert_eq!(join_git_rel("inner/", "packages/foo"), "inner/packages/foo");
        assert_eq!(join_git_rel("", ""), "");
        assert_eq!(join_git_rel(".", "packages/foo"), "packages/foo");
    }

    /// Discover reports a prefix for an uncanonical directory.
    ///
    /// Cargo and Git need not spell the same directory identically: Windows hands out 8.3 short
    /// names for some paths and both tools accept uncanonical spellings, so the prefix must come
    /// from Git rather than from subtracting one reported path from the other.
    #[cfg_attr(miri, ignore)] // Spawns git, which Miri cannot emulate.
    #[test]
    fn discover_reports_a_prefix_for_an_uncanonical_directory() {
        let temp = tempdir().unwrap();
        run_capture("git", &["init", "-q"], temp.path()).unwrap();
        let nested = temp.path().join("inner");
        fs::create_dir_all(&nested).unwrap();

        let repo = GitRepo::discover(&nested.join("..").join("inner")).unwrap();

        assert_eq!(repo.prefix(), "inner");
    }

    #[cfg_attr(miri, ignore)] // Spawns git, which Miri cannot emulate.
    #[test]
    fn discover_reports_an_empty_prefix_at_the_repository_root() {
        let temp = tempdir().unwrap();
        run_capture("git", &["init", "-q"], temp.path()).unwrap();

        let repo = GitRepo::discover(temp.path()).unwrap();

        assert_eq!(repo.prefix(), "");
        assert!(
            repo.root().ends_with(
                temp.path()
                    .file_name()
                    .expect("a temporary directory always has a final component")
            )
        );
    }

    #[cfg_attr(miri, ignore)] // Spawns git, which Miri cannot emulate.
    #[test]
    fn discover_starts_from_the_directory_holding_a_file() {
        let temp = tempdir().unwrap();
        run_capture("git", &["init", "-q"], temp.path()).unwrap();
        let nested = temp.path().join("inner");
        fs::create_dir_all(&nested).unwrap();
        let manifest = nested.join(MANIFEST_FILE_NAME);
        fs::write(&manifest, "").unwrap();

        let repo = GitRepo::discover(&manifest).unwrap();

        assert_eq!(repo.prefix(), "inner");
    }

    #[cfg_attr(miri, ignore)] // Spawns git, which Miri cannot emulate.
    #[test]
    fn discover_fails_outside_a_repository() {
        let temp = tempdir().unwrap();

        GitRepo::discover(temp.path()).unwrap_err();
    }

    /// Listings fail when the root is not a repository.
    ///
    /// Every listing runs `git` in the repository root, so a root that is not a repository must
    /// surface the failure rather than an empty listing.
    #[cfg_attr(miri, ignore)] // Spawns git, which Miri cannot emulate.
    #[test]
    fn listings_fail_when_the_root_is_not_a_repository() {
        let temp = tempdir().unwrap();
        let repo = GitRepo {
            root: temp.path().to_path_buf(),
            prefix: String::new(),
        };

        repo.first_parent_commits("HEAD").unwrap_err();
        repo.ls_files("").unwrap_err();
        repo.ls_untracked("").unwrap_err();
        repo.ls_tree("HEAD", &[""]).unwrap_err();
        repo.ls_tree_paths("HEAD").unwrap_err();
        repo.hash_objects(&["Cargo.toml"]).unwrap_err();
        repo.rev_parse("HEAD").unwrap_err();
    }

    #[cfg_attr(miri, ignore)] // Spawns git, which Miri cannot emulate.
    #[test]
    fn a_commit_with_a_reachable_parent_is_not_a_root() {
        let temp = tempdir().unwrap();
        let repo = init_repo_with_two_commits(temp.path());
        let head = repo.rev_parse("HEAD").unwrap();
        let root = repo.rev_parse("HEAD~1").unwrap();

        assert!(repo.has_parent_or_is_shallow_boundary(&head).unwrap());
        assert!(!repo.has_parent_or_is_shallow_boundary(&root).unwrap());
    }

    /// A root commit whose message mentions a parent is still a root.
    ///
    /// The commit message follows the headers in `cat-file -p` output, so a message body that
    /// mentions a parent must not be read as a header.
    #[cfg_attr(miri, ignore)] // Spawns git, which Miri cannot emulate.
    #[test]
    fn a_root_commit_whose_message_mentions_a_parent_is_still_a_root() {
        let temp = tempdir().unwrap();
        let root = temp.path();
        run_capture("git", &["init", "-q"], root).unwrap();
        run_capture("git", &["config", "user.name", "test"], root).unwrap();
        run_capture("git", &["config", "user.email", "test@example.com"], root).unwrap();
        run_capture("git", &["config", "commit.gpgsign", "false"], root).unwrap();
        fs::write(root.join("first.txt"), "one\n").unwrap();
        run_capture("git", &["add", "-A"], root).unwrap();
        run_capture(
            "git",
            &[
                "commit",
                "-q",
                "-m",
                "subject",
                "-m",
                "parent 0123456789012345678901234567890123456789",
            ],
            root,
        )
        .unwrap();
        let repo = GitRepo::discover(root).unwrap();
        let head = repo.rev_parse("HEAD").unwrap();

        assert!(!repo.has_parent_or_is_shallow_boundary(&head).unwrap());
    }

    /// Show file distinguishes an absent path from a failure.
    ///
    /// A path absent at a commit is an ordinary answer, while any other `git show` failure is a
    /// real error the caller must see.
    #[cfg_attr(miri, ignore)] // Spawns git, which Miri cannot emulate.
    #[test]
    fn show_file_distinguishes_an_absent_path_from_a_failure() {
        let temp = tempdir().unwrap();
        let repo = init_repo_with_two_commits(temp.path());

        assert_eq!(
            repo.show_file("HEAD", "first.txt").unwrap().as_deref(),
            Some("one\n")
        );
        assert_eq!(repo.show_file("HEAD", "absent.txt").unwrap(), None);
        repo.show_file("no-such-revision", "first.txt").unwrap_err();
    }

    fn init_repo_with_two_commits(root: &Path) -> GitRepo {
        let repo = init_repo(root);
        fs::write(root.join("second.txt"), "two\n").unwrap();
        run_capture("git", &["add", "-A"], root).unwrap();
        run_capture("git", &["commit", "-q", "-m", "second"], root).unwrap();
        repo
    }

    /// Initialises a hermetic repository and commits whatever `root` holds.
    fn init_repo(root: &Path) -> GitRepo {
        run_capture("git", &["init", "-q"], root).unwrap();
        run_capture("git", &["config", "user.name", "test"], root).unwrap();
        run_capture("git", &["config", "user.email", "test@example.com"], root).unwrap();
        run_capture("git", &["config", "commit.gpgsign", "false"], root).unwrap();
        fs::write(root.join("first.txt"), "one\n").unwrap();
        run_capture("git", &["add", "-A"], root).unwrap();
        run_capture("git", &["commit", "-q", "-m", "first"], root).unwrap();
        GitRepo::discover(root).unwrap()
    }

    #[test]
    fn split_z_drops_empty_trailing_field() {
        assert_eq!(split_z(b"a\0b\0").unwrap(), vec!["a", "b"]);
        assert!(split_z(b"").unwrap().is_empty());
    }

    #[test]
    fn split_z_preserves_whitespace_inside_paths() {
        // With `-z` these characters are filename data, not record separators.
        assert_eq!(
            split_z(b" leading.rs\0trailing.rs \0mid\nline.rs\0").unwrap(),
            vec![" leading.rs", "trailing.rs ", "mid\nline.rs"]
        );
    }

    #[test]
    fn split_z_rejects_a_path_that_is_not_utf8() {
        // A Unix file name is an arbitrary byte string. Substituting the invalid
        // byte would name a file that is not the one Git reported.
        let error = split_z(b"ok.rs\0bad\xffname.rs\0").unwrap_err();
        assert!(error.find_source::<NonUtf8PathError>().is_some());
    }

    #[test]
    fn dir_pathspec_is_literal_and_names_the_repository_root() {
        assert_eq!(dir_pathspec(""), ":(literal).");
        assert_eq!(dir_pathspec("packages/foo"), ":(literal)packages/foo");
        // Without the magic prefix this would select every sibling package.
        assert_eq!(dir_pathspec("packages/foo*"), ":(literal)packages/foo*");
    }

    #[test]
    fn cased_pathspec_follows_the_checkout_case_rules() {
        assert_eq!(
            cased_pathspec("Packages/README.md", PathCase::Sensitive),
            ":(literal)Packages/README.md"
        );
        assert_eq!(
            cased_pathspec("Packages/README.md", PathCase::Insensitive),
            ":(icase,literal)Packages/README.md"
        );
        assert_eq!(
            cased_pathspec("", PathCase::Insensitive),
            ":(icase,literal)."
        );
    }

    /// A directory named like a pattern lists only its own files.
    ///
    /// The literal pathspec must survive the round trip through Git itself: the escaping is only
    /// correct if Git reads it back as one plain path.
    #[cfg_attr(miri, ignore)] // Spawns git, which Miri cannot emulate.
    #[test]
    fn a_directory_named_like_a_pattern_lists_only_its_own_files() {
        let temp = tempdir().unwrap();
        let root = temp.path();
        // `de[m]o` is a legal directory name on every supported platform and is
        // also a glob that matches its sibling `demo`.
        for dir in ["packages/de[m]o", "packages/demo"] {
            fs::create_dir_all(root.join(dir)).unwrap();
            fs::write(root.join(dir).join("lib.rs"), "x").unwrap();
        }
        let repo = init_repo(root);

        assert_eq!(
            repo.ls_files("packages/de[m]o").unwrap(),
            vec!["packages/de[m]o/lib.rs".to_string()]
        );
        let entries = repo.ls_tree("HEAD", &["packages/de[m]o"]).unwrap();
        assert_eq!(
            entries
                .iter()
                .map(|entry| entry.path.clone())
                .collect::<Vec<_>>(),
            vec!["packages/de[m]o/lib.rs".to_string()]
        );
    }

    /// A work tree file hashes to the id its tree entry records.
    ///
    /// Git converts content on its way into the object database, so the id a work-tree file hashes
    /// to is the representation both ends of a comparison have to be expressed in. It must agree
    /// with the id the tree records for an unmodified file.
    #[cfg_attr(miri, ignore)] // Spawns git, which Miri cannot emulate.
    #[test]
    fn a_work_tree_file_hashes_to_the_id_its_tree_entry_records() {
        let temp = tempdir().unwrap();
        let root = temp.path();
        fs::create_dir_all(root.join("packages/demo")).unwrap();
        fs::write(root.join("packages/demo/lib.rs"), "x").unwrap();
        fs::write(root.join("packages/demo/other.rs"), "y").unwrap();
        let repo = init_repo(root);

        let entries = repo.ls_tree("HEAD", &["packages/demo"]).unwrap();
        let recorded: Vec<String> = entries.iter().map(|entry| entry.id.clone()).collect();
        let hashed = repo
            .hash_objects(&["packages/demo/lib.rs", "packages/demo/other.rs"])
            .unwrap();

        assert_eq!(hashed, recorded);
    }
}
